// SPDX-License-Identifier: Apache-2.0

//! Typed, payload-redacted daemon process failures.

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

use undercurrent_transport_local::LocalServerError;

/// Stable daemon startup stages suitable for redacted diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DaemonProcessStartStage {
    /// Generate the process epoch used to invalidate stale replay.
    GenerateIdentity,
    /// Validate the private directory that owns the daemon instance lock.
    PrepareInstanceLockPath,
    /// Acquire the process-lifetime advisory daemon instance lock.
    AcquireInstanceLock,
    /// Open and schema-validate durable SQLite state.
    OpenPersistence,
    /// Construct the bounded in-process telemetry recorder.
    ComposeTelemetry,
    /// Construct prepared query and subscription state.
    ComposeProjections,
    /// Start the native PTY and process runtime.
    StartProcessRuntime,
    /// Start the protected local agent bridge gateway.
    StartAgentBridgeGateway,
    /// Construct a bounded in-memory application adapter.
    ComposeApplicationAdapter,
    /// Construct or activate foreground detection profiles and patterns.
    ComposeDetection,
    /// Construct the bounded foreground evidence adapter.
    ComposeForeground,
    /// Start the fixed health-check worker pool.
    StartHealthChecks,
    /// Reconcile durable state and start background application workers.
    StartBackgroundRuntime,
    /// Start the shared logical-stream pump.
    StartStreamPump,
    /// Compose authorized product read ports.
    ComposeProductReads,
    /// Start bounded finite-request dispatcher workers.
    StartDispatcher,
    /// Bind the authenticated Local listener.
    BindLocalListener,
    /// Attach the listener to the weak aggregate-status edge.
    AttachStatus,
}

impl DaemonProcessStartStage {
    /// Complete stable startup-stage registry.
    ///
    /// These are diagnostic categories, not an execution trace. Composition
    /// can use one category at multiple construction points; its actual order
    /// is defined by `DaemonProcess::compose_inner`.
    pub const ALL: [Self; 18] = [
        Self::GenerateIdentity,
        Self::PrepareInstanceLockPath,
        Self::AcquireInstanceLock,
        Self::OpenPersistence,
        Self::ComposeTelemetry,
        Self::ComposeProjections,
        Self::StartProcessRuntime,
        Self::StartAgentBridgeGateway,
        Self::ComposeApplicationAdapter,
        Self::ComposeDetection,
        Self::ComposeForeground,
        Self::StartHealthChecks,
        Self::StartBackgroundRuntime,
        Self::StartStreamPump,
        Self::ComposeProductReads,
        Self::StartDispatcher,
        Self::BindLocalListener,
        Self::AttachStatus,
    ];

    /// Returns the stable snake-case startup-stage name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GenerateIdentity => "generate_identity",
            Self::PrepareInstanceLockPath => "prepare_instance_lock_path",
            Self::AcquireInstanceLock => "acquire_instance_lock",
            Self::OpenPersistence => "open_persistence",
            Self::ComposeTelemetry => "compose_telemetry",
            Self::ComposeProjections => "compose_projections",
            Self::StartProcessRuntime => "start_process_runtime",
            Self::StartAgentBridgeGateway => "start_agent_bridge_gateway",
            Self::ComposeApplicationAdapter => "compose_application_adapter",
            Self::ComposeDetection => "compose_detection",
            Self::ComposeForeground => "compose_foreground",
            Self::StartHealthChecks => "start_health_checks",
            Self::StartBackgroundRuntime => "start_background_runtime",
            Self::StartStreamPump => "start_stream_pump",
            Self::ComposeProductReads => "compose_product_reads",
            Self::StartDispatcher => "start_dispatcher",
            Self::BindLocalListener => "bind_local_listener",
            Self::AttachStatus => "attach_status",
        }
    }
}

/// Redacted daemon composition failure retaining its typed source internally.
pub struct DaemonProcessStartError {
    stage: DaemonProcessStartStage,
    source: Box<dyn Error + Send + Sync>,
}

impl DaemonProcessStartError {
    pub(super) fn new(
        stage: DaemonProcessStartStage,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            stage,
            source: Box::new(source),
        }
    }

    /// Returns the stable failed composition stage.
    pub const fn stage(&self) -> DaemonProcessStartStage {
        self.stage
    }
}

impl Debug for DaemonProcessStartError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonProcessStartError")
            .field("stage", &self.stage)
            .finish_non_exhaustive()
    }
}

impl Display for DaemonProcessStartError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "daemon startup failed at {}",
            self.stage.as_str()
        )
    }
}

impl Error for DaemonProcessStartError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// Stable process-run failure category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DaemonProcessRunErrorCategory {
    /// The Local listener failed after readiness opened.
    LocalListener,
    /// The shutdown supervisor thread could not start or panicked.
    ShutdownSupervisor,
    /// Process ownership violated an internal lifecycle invariant.
    LifecycleInvariant,
}

impl DaemonProcessRunErrorCategory {
    /// Complete process-run error registry.
    pub const ALL: [Self; 3] = [
        Self::LocalListener,
        Self::ShutdownSupervisor,
        Self::LifecycleInvariant,
    ];

    /// Returns the stable snake-case error category.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalListener => "local_listener",
            Self::ShutdownSupervisor => "shutdown_supervisor",
            Self::LifecycleInvariant => "lifecycle_invariant",
        }
    }
}

/// Fatal process-run failure with bounded listener context where available.
pub enum DaemonProcessRunError {
    /// Local listener failed after readiness opened.
    LocalListener(LocalServerError),
    /// The shutdown supervisor thread could not start.
    ShutdownSupervisorStart(std::io::Error),
    /// The shutdown supervisor panicked and was contained.
    ShutdownSupervisorPanicked,
    /// A composed process no longer owned its required listener.
    LifecycleInvariant,
}

impl DaemonProcessRunError {
    /// Returns the stable payload-safe failure category.
    pub const fn category(&self) -> DaemonProcessRunErrorCategory {
        match self {
            Self::LocalListener(_) => DaemonProcessRunErrorCategory::LocalListener,
            Self::ShutdownSupervisorStart(_) | Self::ShutdownSupervisorPanicked => {
                DaemonProcessRunErrorCategory::ShutdownSupervisor
            }
            Self::LifecycleInvariant => DaemonProcessRunErrorCategory::LifecycleInvariant,
        }
    }
}

impl Debug for DaemonProcessRunError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonProcessRunError")
            .field("category", &self.category())
            .finish_non_exhaustive()
    }
}

impl Display for DaemonProcessRunError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LocalListener(_) => formatter.write_str("daemon Local listener failed"),
            Self::ShutdownSupervisorStart(_) => {
                formatter.write_str("daemon shutdown supervisor could not start")
            }
            Self::ShutdownSupervisorPanicked => {
                formatter.write_str("daemon shutdown supervisor panicked")
            }
            Self::LifecycleInvariant => {
                formatter.write_str("daemon process lifecycle invariant was violated")
            }
        }
    }
}

impl Error for DaemonProcessRunError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::LocalListener(error) => Some(error),
            Self::ShutdownSupervisorStart(error) => Some(error),
            Self::ShutdownSupervisorPanicked | Self::LifecycleInvariant => None,
        }
    }
}
