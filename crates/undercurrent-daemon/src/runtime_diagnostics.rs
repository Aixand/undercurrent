// SPDX-License-Identifier: Apache-2.0

//! Bounded payload-free diagnostics over concrete daemon runtime adapters.
//!
//! Sources sample their owners independently; the capture is not a global
//! transaction. The daemon operations layer retains the resulting row vector
//! for client-owned pagination. Weak listener and dispatcher edges avoid keeping
//! process resources alive after composition teardown.

mod rows;
mod services;
mod streams;

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::sync::{Arc, RwLock, Weak};

use undercurrent_application::SessionSubscriptionHub;
use undercurrent_runtime::{
    BoundedObservationQueue, BoundedRestartQueue, BoundedSessionEventJournal,
    BoundedTelemetryRecorder, HostedProcessRuntime, InMemoryDetectionProfileCatalog,
    KeyedSessionMutationCoordinator, NativeHealthCheckExecutor, RegexPatternEngine,
    TerminalForegroundEvidenceCollector,
};
use undercurrent_store_sqlite::SqliteSessionRepository;
use undercurrent_transport_local::LocalServer;

use crate::{
    BoundedMultiplexTelemetry, BoundedProductDispatchTelemetry, BoundedProductDispatcher,
    DaemonDiagnosticRow, DaemonDiagnosticSection, DaemonDiagnosticSource, DaemonLogPressure,
    DaemonOperationsError, ProductStreamPump, ResolvedDaemonConfig, blocking::block_on,
};

/// Persistence, process, telemetry, and validated settings diagnostic owners.
pub(crate) struct DaemonRuntimeDiagnosticCore {
    config: ResolvedDaemonConfig,
    repository: Arc<SqliteSessionRepository>,
    coordinator: Arc<KeyedSessionMutationCoordinator>,
    processes: Arc<HostedProcessRuntime>,
    observability: DaemonRuntimeDiagnosticObservability,
}

impl DaemonRuntimeDiagnosticCore {
    /// Groups long-lived core runtime adapters without performing IO.
    pub(crate) fn new(
        config: ResolvedDaemonConfig,
        repository: Arc<SqliteSessionRepository>,
        coordinator: Arc<KeyedSessionMutationCoordinator>,
        processes: Arc<HostedProcessRuntime>,
        observability: DaemonRuntimeDiagnosticObservability,
    ) -> Self {
        Self {
            config,
            repository,
            coordinator,
            processes,
            observability,
        }
    }
}

/// Bounded telemetry and logging diagnostic owners.
pub(crate) struct DaemonRuntimeDiagnosticObservability {
    telemetry: Arc<BoundedTelemetryRecorder>,
    product_dispatch: Arc<BoundedProductDispatchTelemetry>,
    multiplex: Arc<BoundedMultiplexTelemetry>,
    log_pressure: Option<DaemonLogPressure>,
}

impl DaemonRuntimeDiagnosticObservability {
    /// Groups observability adapters without reading retained records.
    pub(crate) fn new(
        telemetry: Arc<BoundedTelemetryRecorder>,
        product_dispatch: Arc<BoundedProductDispatchTelemetry>,
        multiplex: Arc<BoundedMultiplexTelemetry>,
        log_pressure: Option<DaemonLogPressure>,
    ) -> Self {
        Self {
            telemetry,
            product_dispatch,
            multiplex,
            log_pressure,
        }
    }
}

/// Selective subscription and replay diagnostic owners.
pub(crate) struct DaemonRuntimeDiagnosticStreams {
    pump: Arc<ProductStreamPump>,
    projections: Arc<SessionSubscriptionHub>,
    events: Arc<BoundedSessionEventJournal>,
}

impl DaemonRuntimeDiagnosticStreams {
    /// Groups shared stream sources without performing IO.
    pub(crate) fn new(
        pump: Arc<ProductStreamPump>,
        projections: Arc<SessionSubscriptionHub>,
        events: Arc<BoundedSessionEventJournal>,
    ) -> Self {
        Self {
            pump,
            projections,
            events,
        }
    }
}

/// Observation, detection, and health diagnostic owners.
pub(crate) struct DaemonRuntimeDiagnosticObservation {
    restarts: Arc<BoundedRestartQueue>,
    observations: Arc<BoundedObservationQueue>,
    foreground: Arc<TerminalForegroundEvidenceCollector>,
    health: Arc<NativeHealthCheckExecutor>,
    profiles: Arc<InMemoryDetectionProfileCatalog>,
    patterns: Arc<RegexPatternEngine>,
}

impl DaemonRuntimeDiagnosticObservation {
    /// Groups observation resources without evaluating detection or health.
    pub(crate) fn new(
        restarts: Arc<BoundedRestartQueue>,
        observations: Arc<BoundedObservationQueue>,
        foreground: Arc<TerminalForegroundEvidenceCollector>,
        health: Arc<NativeHealthCheckExecutor>,
        profiles: Arc<InMemoryDetectionProfileCatalog>,
        patterns: Arc<RegexPatternEngine>,
    ) -> Self {
        Self {
            restarts,
            observations,
            foreground,
            health,
            profiles,
            patterns,
        }
    }
}

#[derive(Default)]
struct RuntimeDiagnosticEdges {
    listener: Option<Weak<LocalServer>>,
    dispatcher: Option<Weak<BoundedProductDispatcher>>,
}

/// Failure attaching the post-composition transport edges more than once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DaemonRuntimeDiagnosticsAttachError;

impl Display for DaemonRuntimeDiagnosticsAttachError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("daemon runtime diagnostics already have transport edges")
    }
}

impl Error for DaemonRuntimeDiagnosticsAttachError {}

/// Fixed-row diagnostic source over bounded aggregate pressure APIs.
pub(crate) struct DaemonRuntimeDiagnostics {
    core: DaemonRuntimeDiagnosticCore,
    streams: DaemonRuntimeDiagnosticStreams,
    observation: DaemonRuntimeDiagnosticObservation,
    edges: RwLock<RuntimeDiagnosticEdges>,
}

impl DaemonRuntimeDiagnostics {
    /// Composes diagnostics before operations, dispatcher, and listener exist.
    pub(crate) fn new(
        core: DaemonRuntimeDiagnosticCore,
        streams: DaemonRuntimeDiagnosticStreams,
        observation: DaemonRuntimeDiagnosticObservation,
    ) -> Self {
        Self {
            core,
            streams,
            observation,
            edges: RwLock::new(RuntimeDiagnosticEdges::default()),
        }
    }

    /// Attaches weak transport edges after the complete request graph is built.
    pub(crate) fn attach_transport(
        &self,
        listener: &Arc<LocalServer>,
        dispatcher: &Arc<BoundedProductDispatcher>,
    ) -> Result<(), DaemonRuntimeDiagnosticsAttachError> {
        let mut edges = self
            .edges
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if edges.listener.is_some() || edges.dispatcher.is_some() {
            return Err(DaemonRuntimeDiagnosticsAttachError);
        }
        edges.listener = Some(Arc::downgrade(listener));
        edges.dispatcher = Some(Arc::downgrade(dispatcher));
        Ok(())
    }

    /// Upgrades transport observers without extending their ownership lifetime.
    fn transport_edges(
        &self,
    ) -> (
        Option<Arc<LocalServer>>,
        Option<Arc<BoundedProductDispatcher>>,
    ) {
        let edges = self
            .edges
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        (
            edges.listener.as_ref().and_then(Weak::upgrade),
            edges.dispatcher.as_ref().and_then(Weak::upgrade),
        )
    }
}

impl Debug for DaemonRuntimeDiagnostics {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let (listener, dispatcher) = self.transport_edges();
        formatter
            .debug_struct("DaemonRuntimeDiagnostics")
            .field("listener_attached", &listener.is_some())
            .field("dispatcher_attached", &dispatcher.is_some())
            .finish_non_exhaustive()
    }
}

impl DaemonDiagnosticSource for DaemonRuntimeDiagnostics {
    fn snapshot(
        &self,
        sections: &BTreeSet<DaemonDiagnosticSection>,
    ) -> Result<Vec<DaemonDiagnosticRow>, DaemonOperationsError> {
        let (listener, dispatcher) = self.transport_edges();
        let needs_persistence = sections.contains(&DaemonDiagnosticSection::SupervisorQueues)
            || sections.contains(&DaemonDiagnosticSection::Persistence);
        let persistence = needs_persistence
            .then(|| block_on(self.core.repository.pressure()))
            .transpose()
            .map_err(|_| DaemonOperationsError::SourceUnavailable)?;
        let section_row_limit = self
            .core
            .config
            .transport()
            .operations()
            .maximum_snapshot_rows()
            .checked_div(sections.len().max(1))
            .unwrap_or(1)
            .max(1);
        let mut rows = Vec::new();
        if sections.contains(&DaemonDiagnosticSection::Connections) {
            rows.extend(self.connection_rows(listener.as_deref(), section_row_limit)?);
        }
        if sections.contains(&DaemonDiagnosticSection::Streams) {
            rows.extend(self.stream_rows(
                listener.as_deref(),
                DaemonDiagnosticSection::Streams,
                section_row_limit,
            )?);
        }
        if sections.contains(&DaemonDiagnosticSection::SlowConsumers) {
            rows.extend(self.stream_rows(
                listener.as_deref(),
                DaemonDiagnosticSection::SlowConsumers,
                section_row_limit,
            )?);
        }
        if sections.contains(&DaemonDiagnosticSection::OutputProducers) {
            rows.extend(self.output_rows(section_row_limit)?);
        }
        if sections.contains(&DaemonDiagnosticSection::SupervisorQueues) {
            rows.extend(self.supervisor_rows(
                dispatcher.as_deref(),
                persistence.ok_or(DaemonOperationsError::SourceInvariant)?,
                section_row_limit,
            )?);
        }
        if sections.contains(&DaemonDiagnosticSection::DetectionEvidence) {
            rows.extend(self.detection_rows(section_row_limit)?);
        }
        if sections.contains(&DaemonDiagnosticSection::Persistence) {
            rows.extend(self.persistence_rows(
                persistence.ok_or(DaemonOperationsError::SourceInvariant)?,
                section_row_limit,
            ));
        }
        if sections.contains(&DaemonDiagnosticSection::Configuration) {
            rows.extend(services::configuration_rows(&self.core.config));
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use crate::{ConfigDiscovery, PathEnvironment};
    use tempfile::TempDir;

    use super::*;
    use crate::{DaemonConfigOverrides, load_daemon_config};

    #[test]
    fn configuration_rows_are_fixed_bounded_and_path_free() {
        let temp = TempDir::new().expect("temporary directory");
        let config = load_daemon_config(
            &ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(temp.path())),
            DaemonConfigOverrides::new(),
        )
        .expect("daemon config");
        let rows = services::configuration_rows(&config);

        assert_eq!(rows.len(), 3);
        assert!(
            rows.iter()
                .all(|row| row.section == DaemonDiagnosticSection::Configuration)
        );
        let rendered = format!("{rows:?}");
        assert!(!rendered.contains(&temp.path().display().to_string()));
    }
}
