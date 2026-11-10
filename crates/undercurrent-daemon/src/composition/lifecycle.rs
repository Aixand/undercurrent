// SPDX-License-Identifier: Apache-2.0

//! Graceful admission drain and deterministic daemon resource teardown.

use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use undercurrent_transport_local::{LocalServer, LocalServerRunReport};

use super::{DaemonProcess, DaemonProcessRunError};
use crate::{
    BoundedDaemonOperations, DaemonOperationsPort, DaemonProductShutdownError, DaemonShutdownPhase,
    DaemonShutdownSignal,
};

const SHUTDOWN_WATCH_INTERVAL: Duration = Duration::from_millis(25);
const DRAIN_WATCH_INTERVAL: Duration = Duration::from_millis(10);

/// Cloneable, non-owning control edge for one composed daemon process.
#[derive(Clone)]
pub struct DaemonProcessControl {
    listener: Weak<LocalServer>,
    operations: Weak<BoundedDaemonOperations>,
}

impl DaemonProcessControl {
    /// Begins the same bounded graceful shutdown exposed by the product protocol.
    ///
    /// Semantic mutation admission closes immediately. Physical admission stays
    /// available for bounded queries and recovery until host shutdown settles;
    /// established connections may then drain until the returned deadline.
    /// This method performs no socket IO itself and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed duration or source-unavailable failure.
    pub fn begin_shutdown(
        &self,
        drain_timeout: Duration,
    ) -> Result<i64, DaemonProductShutdownError> {
        self.operations
            .upgrade()
            .ok_or(DaemonProductShutdownError::NotConfigured)?
            .begin_product_shutdown(drain_timeout)
    }

    /// Immediately closes listener admission and established connections.
    ///
    /// Returns `false` after the owning process has already been released.
    pub fn force_shutdown(&self) -> bool {
        let Some(listener) = self.listener.upgrade() else {
            return false;
        };
        listener.shutdown();
        true
    }
}

impl Debug for DaemonProcessControl {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonProcessControl")
            .field("available", &(self.listener.strong_count() > 0))
            .finish()
    }
}

impl DaemonProcess {
    /// Returns a non-owning shutdown control edge.
    pub fn control(&self) -> DaemonProcessControl {
        DaemonProcessControl {
            listener: self.server.as_ref().map_or_else(Weak::new, Arc::downgrade),
            operations: self.operations.clone(),
        }
    }

    /// Runs the blocking listener until graceful, forced, or fatal shutdown.
    ///
    /// A single supervisor closes physical admission after durable host shutdown,
    /// watches only O(1) connection pressure, and force-closes remaining clients
    /// at the fixed deadline. The method then tears components down in dependency
    /// order and removes the owned socket node. It performs socket and thread IO
    /// and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed listener or supervisor failure after all components have
    /// been shut down and joined.
    pub fn run(mut self) -> Result<LocalServerRunReport, DaemonProcessRunError> {
        let Some(server) = self.server.as_ref().cloned() else {
            self.shutdown_ordered();
            return Err(DaemonProcessRunError::LifecycleInvariant);
        };
        let supervisor_stop = Arc::new(AtomicBool::new(false));
        let supervisor = match start_shutdown_supervisor(
            server.clone(),
            self.shutdown.clone(),
            supervisor_stop.clone(),
            self.supervisor_stack_bytes,
        ) {
            Ok(supervisor) => supervisor,
            Err(source) => {
                self.shutdown_ordered();
                return Err(DaemonProcessRunError::ShutdownSupervisorStart(source));
            }
        };

        let listener_result = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(DaemonProcessRunError::ShutdownSupervisorStart)
            .and_then(|runtime| {
                runtime
                    .block_on(server.run())
                    .map_err(DaemonProcessRunError::LocalListener)
            });
        supervisor_stop.store(true, Ordering::Release);
        let supervisor_result = supervisor.join();
        self.shutdown_ordered();

        match (listener_result, supervisor_result) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(_)) => Err(DaemonProcessRunError::ShutdownSupervisorPanicked),
            (Ok(report), Ok(())) => Ok(report),
        }
    }

    fn shutdown_ordered(&mut self) {
        if let Some(server) = self.server.take() {
            server.shutdown();
            drop(server);
        }
        if let Some(dispatcher) = self.dispatcher.take() {
            dispatcher.shutdown();
            drop(dispatcher);
        }
        if let Some(background) = self.background.take() {
            background.shutdown();
            drop(background);
        }
        if let Some(gateway) = self.agent_bridge_gateway.take() {
            drop(gateway);
        }
        if let Some(runtime) = self.process_runtime.take() {
            runtime.begin_shutdown();
            drop(runtime);
        }
    }
}

impl Debug for DaemonProcess {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonProcess")
            .field("epoch", &self.epoch)
            .field("started_at_millis", &self.started_at_millis)
            .field("ready", &self.server.is_some())
            .finish_non_exhaustive()
    }
}

impl Drop for DaemonProcess {
    fn drop(&mut self) {
        self.shutdown_ordered();
    }
}

fn start_shutdown_supervisor(
    server: Arc<LocalServer>,
    shutdown: Arc<DaemonShutdownSignal>,
    stop: Arc<AtomicBool>,
    stack_bytes: usize,
) -> Result<JoinHandle<()>, std::io::Error> {
    thread::Builder::new()
        .name("undercurrent-shutdown".to_owned())
        .stack_size(stack_bytes)
        .spawn(move || {
            'supervise: while !stop.load(Ordering::Acquire) {
                let mut phase = shutdown.wait_phase_timeout(SHUTDOWN_WATCH_INTERVAL);
                if phase == DaemonShutdownPhase::Running {
                    continue;
                }
                let deadline = loop {
                    match phase {
                        DaemonShutdownPhase::Finalizing(deadline) => break deadline,
                        DaemonShutdownPhase::Draining => {
                            if stop.load(Ordering::Acquire) {
                                return;
                            }
                            phase = shutdown.wait_for_finalization(SHUTDOWN_WATCH_INTERVAL);
                        }
                        DaemonShutdownPhase::Running => continue 'supervise,
                    }
                };
                server.stop_accepting();
                while !stop.load(Ordering::Acquire) {
                    if server.pressure().connections.connections == 0 {
                        return;
                    }
                    let now = unix_timestamp_millis();
                    if shutdown.reset_response_finished() || now >= deadline {
                        server.shutdown();
                        return;
                    }
                    let remaining = u64::try_from(deadline.saturating_sub(now)).unwrap_or(u64::MAX);
                    thread::sleep(DRAIN_WATCH_INTERVAL.min(Duration::from_millis(remaining)));
                }
                return;
            }
        })
}

fn unix_timestamp_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or_default()
}
