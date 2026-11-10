// SPDX-License-Identifier: Apache-2.0

//! Cheap aggregate runtime status without retaining the transport graph.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, RwLock, Weak};

use undercurrent_application::SessionSubscriptionHub;
use undercurrent_transport_local::LocalServer;

use crate::{
    DaemonOperationalPressure, DaemonOperationsError, DaemonStatusSource, ProductStreamPump,
};

/// Aggregate listener state required by daemon status.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DaemonListenerPressure {
    accepting: bool,
    physical_connections: usize,
}

/// Narrow status edge implemented by the physical listener adapter.
pub(crate) trait DaemonListenerStatus: Send + Sync {
    /// Returns lock-consistent aggregate listener pressure without payload IO.
    fn listener_pressure(&self) -> DaemonListenerPressure;
}

impl DaemonListenerStatus for LocalServer {
    fn listener_pressure(&self) -> DaemonListenerPressure {
        let pressure = self.pressure();
        DaemonListenerPressure {
            accepting: pressure.accepting,
            physical_connections: pressure.connections.connections,
        }
    }
}

/// Failure attaching the one listener owned by a composed daemon process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DaemonRuntimeStatusAttachError;

impl Display for DaemonRuntimeStatusAttachError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("daemon runtime status already has a listener")
    }
}

impl Error for DaemonRuntimeStatusAttachError {}

/// O(1) aggregate status source with an attach-once weak listener edge.
pub(crate) struct DaemonRuntimeStatus {
    projections: Arc<SessionSubscriptionHub>,
    streams: Arc<ProductStreamPump>,
    listener: RwLock<Option<Weak<dyn DaemonListenerStatus>>>,
}

impl DaemonRuntimeStatus {
    /// Composes prepared-session and logical-stream counters before listener bind.
    pub(crate) fn new(
        projections: Arc<SessionSubscriptionHub>,
        streams: Arc<ProductStreamPump>,
    ) -> Self {
        Self {
            projections,
            streams,
            listener: RwLock::new(None),
        }
    }

    /// Attaches the process listener without creating a transport ownership cycle.
    ///
    /// The status source retains only a weak trait-object reference. A second
    /// attachment is rejected even when the first listener has already been
    /// dropped, because one status source belongs to one process composition.
    pub(crate) fn attach_listener<T>(
        &self,
        listener: &Arc<T>,
    ) -> Result<(), DaemonRuntimeStatusAttachError>
    where
        T: DaemonListenerStatus + 'static,
    {
        let listener: Arc<dyn DaemonListenerStatus> = listener.clone();
        let mut attached = self
            .listener
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if attached.is_some() {
            return Err(DaemonRuntimeStatusAttachError);
        }
        *attached = Some(Arc::downgrade(&listener));
        Ok(())
    }

    fn listener_pressure(&self) -> DaemonListenerPressure {
        let listener = self
            .listener
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        listener
            .and_then(|listener| listener.upgrade())
            .map_or_else(DaemonListenerPressure::default, |listener| {
                listener.listener_pressure()
            })
    }
}

impl DaemonStatusSource for DaemonRuntimeStatus {
    fn pressure(&self) -> Result<DaemonOperationalPressure, DaemonOperationsError> {
        let listener = self.listener_pressure();
        let streams = self.streams.pressure();
        let logical_streams = streams
            .preparing_streams
            .checked_add(streams.active_streams)
            .ok_or(DaemonOperationsError::SourceInvariant)?;
        let (_, managed_sessions) = self
            .projections
            .catalog()
            .status()
            .map_err(|_| DaemonOperationsError::SourceUnavailable)?;
        Ok(DaemonOperationalPressure {
            accepting_connections: listener.accepting,
            physical_connections: listener.physical_connections,
            logical_streams,
            managed_sessions,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use undercurrent_application::PreparedReadModels;

    use super::*;
    use crate::ProductStreamPumpLimits;

    struct Listener {
        connections: AtomicUsize,
    }

    impl DaemonListenerStatus for Listener {
        fn listener_pressure(&self) -> DaemonListenerPressure {
            DaemonListenerPressure {
                accepting: true,
                physical_connections: self.connections.load(Ordering::Acquire),
            }
        }
    }

    fn status() -> (Arc<DaemonRuntimeStatus>, Arc<ProductStreamPump>) {
        let projections = Arc::new(
            SessionSubscriptionHub::new(PreparedReadModels::new(), 8).expect("projection limits"),
        );
        let streams = Arc::new(
            ProductStreamPump::new(ProductStreamPumpLimits::new(8).expect("stream limits"))
                .expect("stream pump"),
        );
        (
            Arc::new(DaemonRuntimeStatus::new(projections, streams.clone())),
            streams,
        )
    }

    #[test]
    fn unattached_and_dropped_listeners_are_non_accepting_without_retention() {
        let (status, streams) = status();
        assert_eq!(
            status.pressure().expect("unattached pressure"),
            DaemonOperationalPressure {
                accepting_connections: false,
                physical_connections: 0,
                logical_streams: 0,
                managed_sessions: 0,
            }
        );

        let listener = Arc::new(Listener {
            connections: AtomicUsize::new(3),
        });
        status.attach_listener(&listener).expect("first attachment");
        assert_eq!(
            status.pressure().expect("attached pressure"),
            DaemonOperationalPressure {
                accepting_connections: true,
                physical_connections: 3,
                logical_streams: 0,
                managed_sessions: 0,
            }
        );
        drop(listener);
        assert!(
            !status
                .pressure()
                .expect("dropped pressure")
                .accepting_connections
        );
        streams.shutdown();
    }

    #[test]
    fn listener_attachment_is_single_assignment() {
        let (status, streams) = status();
        let listener = Arc::new(Listener {
            connections: AtomicUsize::new(0),
        });
        status.attach_listener(&listener).expect("first attachment");
        assert_eq!(
            status.attach_listener(&listener),
            Err(DaemonRuntimeStatusAttachError)
        );
        streams.shutdown();
    }
}
