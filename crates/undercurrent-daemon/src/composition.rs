// SPDX-License-Identifier: Apache-2.0

//! Process-wide ownership and lifecycle composition for the local daemon.

use std::fs;
use std::io;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Weak};
#[cfg(windows)]
use undercurrent_platform_windows::BlockingPipeStream as UnixStream;

use undercurrent_application::{
    Clock, DurableProcessRuntime, PreparedReadModels, ProfilePatternEngine,
    RuntimeCancellationAuthorizer, SecretResolver, SessionSubscriptionHub,
    UnavailableSecretResolver,
};
use undercurrent_runtime::{
    BoundedObservationQueue, BoundedRestartQueue, BoundedSessionEventJournal,
    BoundedTelemetryRecorder, HostedOperationAttemptEffectObserver, HostedProcessRuntime,
    HostedProcessRuntimeConfig, HostedSessionCommandExecutor, InMemoryDetectionProfileCatalog,
    InMemoryProcessDirectory, KeyedSessionMutationCoordinator, MonotonicSessionIdSource,
    NativeHealthCheckExecutor, RegexPatternEngine, SystemClock,
    TerminalForegroundEvidenceCollector,
};
use undercurrent_store_sqlite::{SqliteInputDeliveryStore, SqliteSessionRepository};
use undercurrent_transport_core::BoundedRequestIdempotencyStore;
use undercurrent_transport_local::LocalServer;
use undercurrent_wire::ServerEpoch;

use crate::attempt_tracking::{
    CompensationRecoveryAttemptObserver, TrackedOperationAttemptRepository,
};
use crate::profile_files::{ProfileReload, ProfileSessions};
use crate::runtime_diagnostics::{
    DaemonRuntimeDiagnosticCore, DaemonRuntimeDiagnosticObservability,
    DaemonRuntimeDiagnosticObservation, DaemonRuntimeDiagnosticStreams, DaemonRuntimeDiagnostics,
};
use crate::runtime_status::DaemonRuntimeStatus;
use crate::{
    AgentBridgeGateway, AgentEventNotificationHub, ApplicationProductExecutor,
    AuthenticatedTransportAuthorization, BoundedDaemonOperations, BoundedMultiplexTelemetry,
    BoundedProductDispatchTelemetry, BoundedProductDispatcher, BoundedProfileCursorStore,
    BoundedScreenCheckpointStore, BoundedSessionCursorStore, DaemonApplicationRuntime,
    DaemonApplicationRuntimeConfig, DaemonBackgroundPorts, DaemonCorePorts, DaemonDiagnosticSource,
    DaemonLogPressure, DaemonObservationPorts, DaemonProcessExitPorts, DaemonRestartPorts,
    DaemonShutdownSignal, DaemonTelemetry, DaemonWorkerPorts, OperationAttemptReadinessGate,
    ProductApplicationPorts, ProductCursorStores, ProductEventPorts, ProductExecutionPorts,
    ProductReadPorts, ProductStreamPump, ProductTerminalPorts, ResolvedDaemonConfig,
    UNDERCURRENT_AGENT_BRIDGE_SOCKET_FILE,
};

mod error;
mod identity;
mod lifecycle;

const SESSION_HOST_AUTH_EXTENSION: &str = "auth";
const SESSION_HOST_EXECUTION_EXTENSION: &str = "execution-jobs";
const SESSION_HOST_RECEIPT_EXTENSION: &str = "attempt-receipts";
const EXECUTION_OUTPUT_DIRECTORY: &str = "execution-output";

use crate::process_instance::ProcessInstanceLock;
pub use error::*;
pub use identity::{DaemonInstanceState, daemon_instance_state};
use identity::{
    acquire_daemon_instance_lock, generate_server_epoch, prepare_daemon_instance_scope,
};
pub use lifecycle::*;

/// Fully reconciled, bound daemon process and deterministic resource owner.
pub struct DaemonProcess {
    pub(super) epoch: ServerEpoch,
    pub(super) started_at_millis: i64,
    pub(super) server: Option<Arc<LocalServer>>,
    pub(super) dispatcher: Option<Arc<BoundedProductDispatcher>>,
    pub(super) background: Option<Arc<DaemonApplicationRuntime>>,
    pub(super) agent_bridge_gateway: Option<AgentBridgeGateway>,
    pub(super) process_runtime: Option<Arc<HostedProcessRuntime>>,
    pub(super) operations: Weak<BoundedDaemonOperations>,
    pub(super) shutdown: Arc<DaemonShutdownSignal>,
    pub(super) supervisor_stack_bytes: usize,
    // Declared last so every runtime resource is dropped before ownership is
    // released and another daemon can acquire the instance.
    _instance_lock: ProcessInstanceLock,
}

impl DaemonProcess {
    /// Composes every daemon adapter and opens readiness only after recovery.
    ///
    /// The process-lifetime instance-lock directory is validated
    /// before durable state is opened. A fresh process epoch is shared by
    /// process supervision, replay, status, and handshake. Startup
    /// reconciliation completes before the local IPC listener is bound, so clients
    /// cannot observe a partial prepared catalog. This operation performs
    /// filesystem, database, thread, and socket IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a redacted stage error retaining the concrete typed source.
    pub fn compose(config: ResolvedDaemonConfig) -> Result<Self, DaemonProcessStartError> {
        Self::compose_inner(
            config,
            None,
            Arc::new(UnavailableSecretResolver),
            Arc::new(AuthenticatedTransportAuthorization::new()),
        )
    }

    /// Composes the external daemon with an authorized credential-resolution adapter.
    ///
    /// The adapter receives only durable opaque handles. Returned bytes are
    /// confined to the spawn path and never enter product persistence, events,
    /// diagnostics, or attempt receipts.
    pub fn compose_with_secret_resolver(
        config: ResolvedDaemonConfig,
        secret_resolver: Arc<dyn SecretResolver>,
    ) -> Result<Self, DaemonProcessStartError> {
        Self::compose_inner(
            config,
            None,
            secret_resolver,
            Arc::new(AuthenticatedTransportAuthorization::new()),
        )
    }

    /// Composes the external daemon with a semantic runtime-cancellation authorizer.
    ///
    /// The injected adapter owns verification of source product, actor context,
    /// authority reference, target/control scope, tenant, and Graceful versus
    /// Force permission. This public seam accepts only the application port and
    /// does not link a Trust product implementation into the daemon. Ordinary
    /// composition retains the fail-closed transport-only default.
    pub fn compose_with_runtime_cancellation_authorizer(
        config: ResolvedDaemonConfig,
        cancellation_authorizer: Arc<dyn RuntimeCancellationAuthorizer>,
    ) -> Result<Self, DaemonProcessStartError> {
        Self::compose_inner(
            config,
            None,
            Arc::new(UnavailableSecretResolver),
            cancellation_authorizer,
        )
    }

    /// Composes both external secret resolution and semantic cancellation authorization.
    pub fn compose_with_external_adapters(
        config: ResolvedDaemonConfig,
        secret_resolver: Arc<dyn SecretResolver>,
        cancellation_authorizer: Arc<dyn RuntimeCancellationAuthorizer>,
    ) -> Result<Self, DaemonProcessStartError> {
        Self::compose_inner(config, None, secret_resolver, cancellation_authorizer)
    }

    /// Composes the daemon and exposes the installed log queue in diagnostics.
    ///
    /// This has the same IO, panic, and error contract as [`Self::compose`].
    pub fn compose_with_log_pressure(
        config: ResolvedDaemonConfig,
        log_pressure: DaemonLogPressure,
    ) -> Result<Self, DaemonProcessStartError> {
        Self::compose_inner(
            config,
            Some(log_pressure),
            Arc::new(UnavailableSecretResolver),
            Arc::new(AuthenticatedTransportAuthorization::new()),
        )
    }

    /// Owns the shared composition sequence for embedded and process entrypoints.
    ///
    /// `log_pressure` is absent only when callers intentionally compose without
    /// installing the process logging adapter. This function has the same IO,
    /// panic, and error contract as [`Self::compose`].
    fn compose_inner(
        config: ResolvedDaemonConfig,
        log_pressure: Option<DaemonLogPressure>,
        secret_resolver: Arc<dyn SecretResolver>,
        cancellation_authorizer: Arc<dyn RuntimeCancellationAuthorizer>,
    ) -> Result<Self, DaemonProcessStartError> {
        let epoch = generate_server_epoch().map_err(|source| {
            DaemonProcessStartError::new(DaemonProcessStartStage::GenerateIdentity, source)
        })?;
        let instance_scope = prepare_daemon_instance_scope(config.paths().instance_lock())
            .map_err(|source| {
                DaemonProcessStartError::new(
                    DaemonProcessStartStage::PrepareInstanceLockPath,
                    source,
                )
            })?;
        let instance_lock = acquire_daemon_instance_lock(instance_scope).map_err(|source| {
            DaemonProcessStartError::new(DaemonProcessStartStage::AcquireInstanceLock, source)
        })?;
        verify_coupled_persistence_state(
            config.paths().database(),
            config.paths().session_host(),
            config.paths().product_home(),
        )
        .map_err(|source| {
            DaemonProcessStartError::new(DaemonProcessStartStage::OpenPersistence, source)
        })?;
        for directory in ["state", "state/session-host", "run"] {
            crate::prepare_private_directory(&config.paths().product_home().join(directory))
                .map_err(|source| {
                    DaemonProcessStartError::new(DaemonProcessStartStage::OpenPersistence, source)
                })?;
        }
        let resources = config.services().resources();
        let retained_telemetry = Arc::new(
            BoundedTelemetryRecorder::new(
                resources.telemetry_diagnostic_records(),
                resources.telemetry_metric_records(),
            )
            .map_err(|source| {
                DaemonProcessStartError::new(DaemonProcessStartStage::ComposeTelemetry, source)
            })?,
        );
        let telemetry = Arc::new(DaemonTelemetry::new(
            retained_telemetry.clone(),
            config.telemetry().diagnostics_enabled,
        ));
        let multiplex_telemetry = Arc::new(BoundedMultiplexTelemetry::new(telemetry.clone()));
        let product_dispatch_telemetry = Arc::new(
            BoundedProductDispatchTelemetry::with_application_metrics(
                resources.telemetry_diagnostic_records(),
                config.telemetry().diagnostics_enabled,
                config.telemetry().debug_enabled,
                telemetry.clone(),
            )
            .map_err(|source| {
                DaemonProcessStartError::new(DaemonProcessStartStage::ComposeTelemetry, source)
            })?,
        );
        let repository = Arc::new(
            SqliteSessionRepository::open_with_telemetry(
                config.sqlite().clone(),
                telemetry.clone(),
            )
            .map_err(|source| {
                DaemonProcessStartError::new(DaemonProcessStartStage::OpenPersistence, source)
            })?,
        );
        let clock = Arc::new(SystemClock);
        let started_at_millis = clock.now().value();
        let projections = Arc::new(
            SessionSubscriptionHub::new(
                PreparedReadModels::new(),
                resources.session_subscriptions(),
            )
            .map_err(|source| {
                DaemonProcessStartError::new(DaemonProcessStartStage::ComposeProjections, source)
            })?,
        );
        let observations = Arc::new(
            BoundedObservationQueue::new(resources.observation_jobs()).map_err(|source| {
                DaemonProcessStartError::new(
                    DaemonProcessStartStage::ComposeApplicationAdapter,
                    source,
                )
            })?,
        );
        let native_config = config
            .native_process(epoch.as_str().to_owned())
            .map_err(|source| {
                DaemonProcessStartError::new(DaemonProcessStartStage::StartProcessRuntime, source)
            })?;
        let session_host_executable = resolve_session_host_executable().map_err(|source| {
            DaemonProcessStartError::new(DaemonProcessStartStage::StartProcessRuntime, source)
        })?;
        let local_client = config.client().session_host();
        let hosted_config = HostedProcessRuntimeConfig::new(
            native_config,
            config.paths().session_host(),
            session_host_executable,
            local_client.dial_timeout(),
            local_client.cancellation_interval(),
            local_client.dial_timeout(),
            local_client
                .handshake_timeout()
                .max(local_client.write_timeout()),
        )
        .and_then(|hosted| {
            hosted.with_journal_base(
                config
                    .paths()
                    .product_home()
                    .join("state/session-host/session-host"),
            )
        })
        .map_err(|source| {
            DaemonProcessStartError::new(DaemonProcessStartStage::StartProcessRuntime, source)
        })?;
        let process_runtime = HostedProcessRuntime::start_with_foreground_wake_and_secret_resolver(
            hosted_config,
            clock.clone(),
            telemetry.clone(),
            observations.clone(),
            secret_resolver,
        )
        .map_err(|source| {
            DaemonProcessStartError::new(DaemonProcessStartStage::StartProcessRuntime, source)
        })?;
        let execution_output_bytes = u64::try_from(config.terminal_output().per_session_bytes)
            .map_err(|_| {
                DaemonProcessStartError::new(
                    DaemonProcessStartStage::OpenPersistence,
                    io::Error::other("execution output bound does not fit this platform"),
                )
            })?;
        let execution_output_store = Arc::new(
            crate::execution_output_store::FilesystemExecutionOutputStore::new(
                config.paths().product_home().join("state/execution-output"),
                execution_output_bytes,
            )
            .map_err(|source| {
                DaemonProcessStartError::new(DaemonProcessStartStage::OpenPersistence, source)
            })?,
        );
        let command_executor =
            HostedSessionCommandExecutor::new(process_runtime.clone(), execution_output_store);
        let hosted_attempt_observer = Arc::new(HostedOperationAttemptEffectObserver::new(
            process_runtime.clone(),
            command_executor.clone(),
        ));
        let command_executor = Arc::new(command_executor);
        let process_runtime = Arc::new(process_runtime);
        let processes = Arc::new(InMemoryProcessDirectory::default());
        let attempt_observer = Arc::new(CompensationRecoveryAttemptObserver::new(
            hosted_attempt_observer.clone(),
            repository.clone(),
            processes.clone(),
            process_runtime.clone(),
        ));
        let attempt_readiness = Arc::new(OperationAttemptReadinessGate::new());
        let tracked_attempts = Arc::new(TrackedOperationAttemptRepository::new(
            repository.clone(),
            repository.clone(),
            attempt_readiness.clone(),
        ));
        let durable_process_runtime = Arc::new(DurableProcessRuntime::new(
            process_runtime.clone(),
            tracked_attempts.clone(),
            clock.clone(),
        ));
        let coordinator = Arc::new(KeyedSessionMutationCoordinator::with_telemetry(
            config.mutation(),
            telemetry.clone(),
        ));
        let restarts = Arc::new(BoundedRestartQueue::new(resources.restart_jobs()).map_err(
            |source| {
                DaemonProcessStartError::new(
                    DaemonProcessStartStage::ComposeApplicationAdapter,
                    source,
                )
            },
        )?);
        let deliveries = Arc::new(
            SqliteInputDeliveryStore::new(
                repository.as_ref().clone(),
                resources.input_delivery_records(),
            )
            .map_err(|source| {
                DaemonProcessStartError::new(
                    DaemonProcessStartStage::ComposeApplicationAdapter,
                    source,
                )
            })?,
        );
        let events = Arc::new(
            BoundedSessionEventJournal::new(
                resources.session_event_records(),
                resources.session_event_subscriptions(),
            )
            .map_err(|source| {
                DaemonProcessStartError::new(
                    DaemonProcessStartStage::ComposeApplicationAdapter,
                    source,
                )
            })?,
        );
        let patterns = Arc::new(
            RegexPatternEngine::new(config.services().regex_pattern()).map_err(|source| {
                DaemonProcessStartError::new(DaemonProcessStartStage::ComposeDetection, source)
            })?,
        );
        let profiles = Arc::new(
            InMemoryDetectionProfileCatalog::new(resources.detection_profiles()).map_err(
                |source| {
                    DaemonProcessStartError::new(DaemonProcessStartStage::ComposeDetection, source)
                },
            )?,
        );
        let profile_reload = ProfileReload::start(
            config.profile_directory().to_path_buf(),
            config.services().profile_definition(),
            profiles.clone(),
            patterns.clone(),
            ProfileSessions {
                recovery: repository.clone(),
                processes: processes.clone(),
                effective: projections.clone(),
                wake: observations.clone(),
                clock: clock.clone(),
                max_sessions: config.sqlite().limits().max_sessions(),
                batch_size: config.reconciliation().batch_size(),
            },
        )
        .map_err(|source| {
            tracing::error!(
                target: "undercurrent.profiles",
                error = %source,
                "YAML profile startup rejected"
            );
            DaemonProcessStartError::new(
                DaemonProcessStartStage::ComposeDetection,
                std::io::Error::other(source),
            )
        })?;
        let profile_reload = Arc::new(profile_reload);
        let screens = process_runtime.screen_store();
        let foreground = Arc::new(
            TerminalForegroundEvidenceCollector::new(
                clock.clone(),
                profiles.clone(),
                patterns.clone(),
                screens.clone(),
                process_runtime.cooperative_signal_store(),
                config.services().foreground_evidence(),
            )
            .map_err(|source| {
                DaemonProcessStartError::new(DaemonProcessStartStage::ComposeForeground, source)
            })?
            .with_foreground_processes(process_runtime.clone()),
        );
        let health = Arc::new(
            NativeHealthCheckExecutor::new(
                screens.clone(),
                patterns.clone(),
                config.health_checks(),
            )
            .map_err(|source| {
                DaemonProcessStartError::new(DaemonProcessStartStage::StartHealthChecks, source)
            })?,
        );

        let restart_ports = DaemonRestartPorts::new(restarts.clone());
        let observation_ports = DaemonObservationPorts::new(observations.clone());
        let background_ports = Arc::new(DaemonBackgroundPorts::new(
            DaemonCorePorts::new(
                clock.clone(),
                coordinator.clone(),
                repository.clone(),
                durable_process_runtime.clone(),
                processes.clone(),
                restart_ports.clone(),
                projections.clone(),
                projections.clone(),
                telemetry.clone(),
                tracked_attempts.clone(),
                attempt_observer.clone(),
                attempt_readiness.clone(),
            ),
            DaemonWorkerPorts::new(
                repository.clone(),
                events.clone(),
                observation_ports,
                foreground.clone(),
                process_runtime.clone(),
                profiles.clone(),
                patterns.clone(),
                health.clone(),
                repository.clone(),
                process_runtime.clone(),
                DaemonProcessExitPorts::new(process_runtime.clone()),
            )
            .with_profile_reload(profile_reload.clone()),
        ));
        let background = Arc::new(
            DaemonApplicationRuntime::start(
                background_ports,
                DaemonApplicationRuntimeConfig::from_resolved(&config),
            )
            .map_err(|source| {
                DaemonProcessStartError::new(
                    DaemonProcessStartStage::StartBackgroundRuntime,
                    source,
                )
            })?,
        );
        let streams = Arc::new(
            ProductStreamPump::with_telemetry(config.transport().stream_pump(), telemetry.clone())
                .map_err(|source| {
                    DaemonProcessStartError::new(DaemonProcessStartStage::StartStreamPump, source)
                })?,
        );
        let status = Arc::new(DaemonRuntimeStatus::new(
            projections.clone(),
            streams.clone(),
        ));
        let runtime_diagnostics = Arc::new(DaemonRuntimeDiagnostics::new(
            DaemonRuntimeDiagnosticCore::new(
                config.clone(),
                repository.clone(),
                coordinator.clone(),
                process_runtime.clone(),
                DaemonRuntimeDiagnosticObservability::new(
                    retained_telemetry,
                    product_dispatch_telemetry.clone(),
                    multiplex_telemetry.clone(),
                    log_pressure,
                ),
            ),
            DaemonRuntimeDiagnosticStreams::new(
                streams.clone(),
                projections.clone(),
                events.clone(),
            ),
            DaemonRuntimeDiagnosticObservation::new(
                restarts.clone(),
                observations,
                foreground,
                health,
                profiles.clone(),
                patterns.clone(),
            ),
        ));
        let shutdown = Arc::new(DaemonShutdownSignal::default());
        let diagnostic_sources: Vec<Arc<dyn DaemonDiagnosticSource>> =
            vec![background.clone(), runtime_diagnostics.clone()];
        let operations = Arc::new(
            BoundedDaemonOperations::new(
                epoch.clone(),
                started_at_millis,
                clock.clone(),
                status.clone(),
                diagnostic_sources,
                shutdown.clone(),
                config.transport().operations(),
            )
            .with_process_host_shutdown(tracked_attempts.clone(), process_runtime.clone()),
        );
        let output = process_runtime.output_store();
        let reads = ProductReadPorts::new(
            Arc::new(AuthenticatedTransportAuthorization::new()),
            ProductEventPorts::new(events.clone(), events),
            ProductTerminalPorts::new(output, screens, config.transport().terminal_stream_bytes())
                .map_err(|source| {
                    DaemonProcessStartError::new(
                        DaemonProcessStartStage::ComposeProductReads,
                        source,
                    )
                })?,
            profiles,
            patterns as Arc<dyn ProfilePatternEngine>,
            config.services().profile_service(),
        )
        .map_err(|source| {
            DaemonProcessStartError::new(DaemonProcessStartStage::ComposeProductReads, source)
        })?;
        let ids = Arc::new(
            MonotonicSessionIdSource::new(format!("session-{}", epoch.as_str())).map_err(
                |source| {
                    DaemonProcessStartError::new(
                        DaemonProcessStartStage::ComposeApplicationAdapter,
                        source,
                    )
                },
            )?,
        );
        let application_authorization = Arc::new(AuthenticatedTransportAuthorization::new());
        let application_ports = ProductApplicationPorts::new(
            clock.clone(),
            ids,
            coordinator,
            repository.clone(),
            durable_process_runtime,
            process_runtime.clone(),
            processes,
            restarts,
            deliveries,
            projections,
            telemetry,
            reads,
        )
        .with_execution(
            ProductExecutionPorts::new(
                tracked_attempts.clone(),
                command_executor,
                tracked_attempts,
                hosted_attempt_observer.clone(),
                application_authorization.clone(),
                repository.clone(),
                application_authorization.clone(),
                attempt_readiness,
                config.reconciliation().batch_size(),
                execution_output_bytes,
            )
            .map_err(|source| {
                DaemonProcessStartError::new(
                    DaemonProcessStartStage::ComposeApplicationAdapter,
                    source,
                )
            })?
            .with_runtime_cancellation(repository.clone(), cancellation_authorizer)
            .with_agent_channel(repository.clone()),
        );
        let cursors = ProductCursorStores::new(
            Arc::new(BoundedSessionCursorStore::new(config.transport().cursor())),
            Arc::new(BoundedProfileCursorStore::new(config.transport().cursor())),
            Arc::new(BoundedScreenCheckpointStore::new(
                config.transport().screen_checkpoint(),
            )),
        );
        let agent_event_notifications = Arc::new(AgentEventNotificationHub::default());
        let executor = Arc::new(
            ApplicationProductExecutor::new(
                config.services().boundary(),
                config.transport().product_codec(),
                application_ports,
                cursors,
                streams,
                operations.clone(),
                background.clone(),
            )
            .with_agent_event_notifications(agent_event_notifications.clone())
            .with_profile_gate(profile_reload.gate.clone()),
        );
        let dispatcher = Arc::new(
            BoundedProductDispatcher::new(
                config.transport().product_codec(),
                config.transport().terminal_codec(),
                executor,
                Arc::new(BoundedRequestIdempotencyStore::new(
                    config.transport().idempotency(),
                )),
                product_dispatch_telemetry,
                config.transport().dispatcher(),
            )
            .map_err(|source| {
                DaemonProcessStartError::new(DaemonProcessStartStage::StartDispatcher, source)
            })?,
        );
        let agent_bridge_executable = resolve_agent_bridge_executable().map_err(|source| {
            DaemonProcessStartError::new(DaemonProcessStartStage::StartAgentBridgeGateway, source)
        })?;
        let agent_bridge_gateway = AgentBridgeGateway::start(
            config
                .paths()
                .product_home()
                .join("run")
                .join(UNDERCURRENT_AGENT_BRIDGE_SOCKET_FILE),
            agent_bridge_executable,
            process_runtime.clone(),
            repository.clone(),
            agent_event_notifications,
            clock,
            config.worker_thread_stack_bytes(),
        )
        .map_err(|source| {
            DaemonProcessStartError::new(DaemonProcessStartStage::StartAgentBridgeGateway, source)
        })?;
        let server = Arc::new(
            LocalServer::bind_with_telemetry(
                config.local_endpoint().clone(),
                epoch.clone(),
                config.transport().policy().clone(),
                config.transport().local_server(),
                dispatcher.clone(),
                multiplex_telemetry,
            )
            .map_err(|source| {
                DaemonProcessStartError::new(DaemonProcessStartStage::BindLocalListener, source)
            })?,
        );
        operations.attach_reset_participants(&dispatcher, &background);
        status.attach_listener(&server).map_err(|source| {
            DaemonProcessStartError::new(DaemonProcessStartStage::AttachStatus, source)
        })?;
        runtime_diagnostics
            .attach_transport(&server, &dispatcher)
            .map_err(|source| {
                DaemonProcessStartError::new(DaemonProcessStartStage::AttachStatus, source)
            })?;

        Ok(Self {
            _instance_lock: instance_lock,
            epoch,
            started_at_millis,
            server: Some(server),
            dispatcher: Some(dispatcher),
            background: Some(background),
            agent_bridge_gateway: Some(agent_bridge_gateway),
            process_runtime: Some(process_runtime),
            operations: Arc::downgrade(&operations),
            shutdown,
            supervisor_stack_bytes: config.worker_thread_stack_bytes(),
        })
    }

    /// Returns the fresh process epoch shared by handshake, replay, and status.
    pub const fn epoch(&self) -> &ServerEpoch {
        &self.epoch
    }

    /// Returns daemon composition time as Unix epoch milliseconds.
    pub const fn started_at_millis(&self) -> i64 {
        self.started_at_millis
    }
}

// Refuse to recreate an empty session catalog beside surviving host effects or
// recovery evidence. This must run before opening SQLite, which can create the
// missing database; an existing database still needs repository validation.
// See [coupled persistence](../docs/internals/lifecycle/startup-and-shutdown.md#identity-and-coupled-persistence).
fn verify_coupled_persistence_state(
    database: &Path,
    session_host: &Path,
    product_home: &Path,
) -> Result<(), io::Error> {
    match fs::symlink_metadata(database) {
        Ok(_) => return Ok(()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => {}
        Err(source) => return Err(source),
    }
    if active_session_host(session_host)?
        || retained_file(&session_host.with_extension(SESSION_HOST_AUTH_EXTENSION))?
        || retained_file(&with_suffix(database, "-wal"))?
        || retained_file(&with_suffix(database, "-shm"))?
        || retained_file(&with_suffix(database, "-journal"))?
        || retained_directory_entry(
            &product_home
                .join("state/session-host/session-host")
                .with_extension(SESSION_HOST_RECEIPT_EXTENSION),
        )?
        || retained_directory_entry(
            &product_home
                .join("state/session-host/session-host")
                .with_extension(SESSION_HOST_EXECUTION_EXTENSION),
        )?
        || retained_directory_entry(&product_home.join("state").join(EXECUTION_OUTPUT_DIRECTORY))?
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "database is absent while coupled process-host state remains; run a confirmed local reset before restarting",
        ));
    }
    Ok(())
}

fn active_session_host(socket: &Path) -> Result<bool, io::Error> {
    match UnixStream::connect(socket) {
        Ok(_) => Ok(true),
        Err(source)
            if matches!(
                source.kind(),
                io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
            ) =>
        {
            Ok(false)
        }
        Err(source) => Err(source),
    }
}

fn retained_file(path: &Path) -> Result<bool, io::Error> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(true),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "coupled persistence artifact is not a regular file",
        )),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(source),
    }
}

fn retained_directory_entry(path: &Path) -> Result<bool, io::Error> {
    match fs::read_dir(path) {
        Ok(mut entries) => Ok(entries.next().transpose()?.is_some()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(source),
    }
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn resolve_session_host_executable() -> io::Result<PathBuf> {
    let current = std::env::current_exe()?;
    let sibling =
        current.with_file_name(format!("uc-session-host{}", std::env::consts::EXE_SUFFIX));
    if sibling.is_file() {
        return Ok(sibling);
    }
    if let Some(profile_directory) = current.parent().and_then(|parent| parent.parent()) {
        let profile_sibling =
            profile_directory.join(format!("uc-session-host{}", std::env::consts::EXE_SUFFIX));
        if profile_sibling.is_file() {
            return Ok(profile_sibling);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "uc-session-host is not installed beside uc-daemon",
    ))
}

fn resolve_agent_bridge_executable() -> io::Result<PathBuf> {
    let current = std::env::current_exe()?;
    let sibling = current.with_file_name(format!(
        "operation-agent-mcp-bridge{}",
        std::env::consts::EXE_SUFFIX
    ));
    if current
        .parent()
        .is_some_and(|parent| parent.ends_with("deps"))
        && let Some(profile_directory) = current.parent().and_then(|parent| parent.parent())
    {
        return Ok(profile_directory.join(format!(
            "operation-agent-mcp-bridge{}",
            std::env::consts::EXE_SUFFIX
        )));
    }
    Ok(sibling)
}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::*;
    use crate::{ConfigDiscovery, DaemonConfigOverrides, PathEnvironment, load_daemon_config};
    #[test]
    fn native_daemon_composes_with_real_host_and_releases_ownership_on_shutdown() {
        let root = tempfile::tempdir().unwrap();
        let discovery = ConfigDiscovery::from_path_env(
            PathEnvironment::default().with_undercurrent_home(root.path().join("product")),
        );
        let config = load_daemon_config(&discovery, DaemonConfigOverrides::new()).unwrap();
        let scope = config.paths().instance_lock().to_owned();
        let authentication = config.paths().session_host().with_extension("auth");
        let process = DaemonProcess::compose(config).unwrap_or_else(|error| {
            let mut text = error.to_string();
            let mut source = std::error::Error::source(&error);
            while let Some(error) = source {
                text.push_str(&format!(": {error}"));
                source = error.source();
            }
            panic!("{text}");
        });
        assert_eq!(
            daemon_instance_state(&scope).unwrap(),
            DaemonInstanceState::Owned
        );
        assert!(authentication.exists());
        process
            .control()
            .begin_shutdown(std::time::Duration::from_millis(100))
            .unwrap();
        process.run().unwrap();
        assert_eq!(
            daemon_instance_state(&scope).unwrap(),
            DaemonInstanceState::Available
        );
        assert!(!authentication.exists());
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixListener;
    use std::path::Path;
    use std::sync::Mutex;
    use std::time::Duration;

    use crate::{ConfigDiscovery, PathEnvironment};
    use undercurrent_wire::ClientId;

    use super::*;
    use crate::{
        DaemonConfigOverrides, DaemonDiagnosticQuery, DaemonDiagnosticSection,
        DaemonOperationsPort, load_daemon_config,
    };

    // macOS's default temp directory can exceed AF_UNIX limits once the product's
    // run paths are appended. Keep IPC fixtures short and free of symlink ancestors.
    fn physical_tempdir() -> std::io::Result<tempfile::TempDir> {
        #[cfg(target_os = "macos")]
        let base = std::path::PathBuf::from("/tmp");
        #[cfg(not(target_os = "macos"))]
        let base = std::env::temp_dir();
        tempfile::tempdir_in(std::fs::canonicalize(base)?)
    }

    // Each fixture composes the production-sized native worker pools. Running
    // two complete daemon process trees concurrently tests host capacity rather
    // than composition behavior and can exhaust bounded test-host resources.
    static PROCESS_RUNTIME_FIXTURE: Mutex<()> = Mutex::new(());

    fn compose_test_daemon(config: ResolvedDaemonConfig) -> DaemonProcess {
        DaemonProcess::compose(config).unwrap_or_else(|error| {
            let mut chain = vec![error.to_string()];
            let mut source = std::error::Error::source(&error);
            while let Some(current) = source {
                chain.push(current.to_string());
                source = current.source();
            }
            panic!("{}", chain.join(": "));
        })
    }

    #[test]
    fn missing_database_with_retained_host_receipt_is_rejected_before_recreation() {
        let temp = physical_tempdir().expect("temporary directory");
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700))
            .expect("private runtime directory");
        let database = temp.path().join("undercurrent.db");
        let instance_lock = temp.path().join("daemon.lock");
        let config = test_config(temp.path(), &database, &instance_lock);
        let receipt_directory = config
            .paths()
            .product_home()
            .join("state/session-host/session-host")
            .with_extension(SESSION_HOST_RECEIPT_EXTENSION);
        fs::create_dir_all(&receipt_directory).expect("receipt directory");
        fs::set_permissions(
            config.paths().product_home(),
            fs::Permissions::from_mode(0o700),
        )
        .expect("private product home");
        fs::write(receipt_directory.join("receipts.json"), b"retained").expect("retained receipt");

        let error =
            DaemonProcess::compose(config).expect_err("inconsistent state must fail closed");

        assert_eq!(error.stage(), DaemonProcessStartStage::OpenPersistence);
        assert!(!database.exists());
    }

    #[test]
    fn missing_database_with_live_session_host_is_rejected_before_recreation() {
        let temp = physical_tempdir().expect("temporary directory");
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700))
            .expect("private runtime directory");
        let database = temp.path().join("undercurrent.db");
        let session_host = temp.path().join("session-host.sock");
        let _listener = UnixListener::bind(&session_host).expect("live session host");

        let error = verify_coupled_persistence_state(&database, &session_host, temp.path())
            .expect_err("live host without its database must fail closed");

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(!database.exists());
    }

    #[test]
    fn composed_daemon_opens_readiness_and_shuts_down_in_order() {
        let _fixture = PROCESS_RUNTIME_FIXTURE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let temp = physical_tempdir().expect("temporary directory");
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700))
            .expect("private runtime directory");
        let database = temp.path().join("undercurrent.db");
        let instance_lock = temp.path().join("daemon.lock");
        let config = test_config(temp.path(), &database, &instance_lock);
        let session_host_socket = config.paths().session_host().to_path_buf();
        let session_host_auth = session_host_socket.with_extension("auth");
        let session_host_executions = config
            .paths()
            .product_home()
            .join("state/session-host/session-host.execution-jobs");
        let session_host_receipts = config
            .paths()
            .product_home()
            .join("state/session-host/session-host.attempt-receipts");
        let endpoint = config.local_endpoint().path().to_path_buf();
        let process = compose_test_daemon(config);
        let epoch = process.epoch().clone();
        let control = process.control();

        assert!(
            !instance_lock.exists(),
            "instance lock must be released and removed"
        );
        let metadata = fs::metadata(&endpoint).expect("local socket");
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        assert_eq!(
            process
                .server
                .as_ref()
                .expect("local listener")
                .endpoint()
                .path(),
            endpoint
        );
        assert!(!temp.path().join("websocket.token").exists());
        assert!(process.started_at_millis() > 0);
        for artifact in [
            &session_host_socket,
            &session_host_auth,
            &session_host_executions,
            &session_host_receipts,
        ] {
            assert!(
                artifact.exists(),
                "composed session host must own {}",
                artifact.display()
            );
        }
        let diagnostics = process
            .operations
            .upgrade()
            .expect("operations owner")
            .diagnostics(
                &ClientId::new("composition-test").expect("client ID"),
                DaemonDiagnosticQuery {
                    sections: BTreeSet::new(),
                    page_size: 100,
                    cursor: None,
                },
            )
            .expect("complete runtime diagnostics");
        assert!(diagnostics.next_cursor.is_none());
        for section in DaemonDiagnosticSection::ALL {
            assert!(
                diagnostics.items.iter().any(|row| row.section == section),
                "missing {section:?} diagnostic section"
            );
            let selected = process
                .operations
                .upgrade()
                .expect("operations owner")
                .diagnostics(
                    &ClientId::new("section-test").expect("client ID"),
                    DaemonDiagnosticQuery {
                        sections: BTreeSet::from([section]),
                        page_size: 100,
                        cursor: None,
                    },
                )
                .expect("selected runtime diagnostics");
            assert!(!selected.items.is_empty(), "empty {section:?} diagnostics");
            assert!(selected.items.iter().all(|row| row.section == section));
        }
        let logging = diagnostics
            .items
            .iter()
            .find(|row| row.identity == "structured_logging")
            .expect("structured logging pressure");
        assert_eq!(
            logging.fields.get(&crate::DaemonDiagnosticField::Limit),
            Some(&crate::DaemonDiagnosticValue::Unsigned(8_192))
        );
        assert_eq!(
            logging.fields.get(&crate::DaemonDiagnosticField::Dropped),
            Some(&crate::DaemonDiagnosticValue::Unsigned(0))
        );
        let profiles = diagnostics
            .items
            .iter()
            .find(|row| row.identity == "profiles")
            .expect("profile catalog pressure");
        assert_eq!(
            profiles.fields.get(&crate::DaemonDiagnosticField::Items),
            Some(&crate::DaemonDiagnosticValue::Unsigned(
                crate::profile_builtins::EMBEDDED.len() as u64,
            ))
        );
        control
            .begin_shutdown(Duration::from_millis(100))
            .expect("graceful shutdown");
        let report = process.run().expect("daemon run");

        assert_eq!(report.accepted, 0);
        assert!(!instance_lock.exists());
        assert!(!control.force_shutdown());
        assert!(ServerEpoch::new(epoch.as_str()).is_ok());
        for artifact in [
            &session_host_socket,
            &session_host_auth,
            &session_host_executions,
            &session_host_receipts,
        ] {
            assert!(
                !artifact.exists(),
                "ordered shutdown must remove {}",
                artifact.display()
            );
        }
        std::thread::sleep(Duration::from_millis(100));
        for artifact in [
            &session_host_socket,
            &session_host_auth,
            &session_host_executions,
            &session_host_receipts,
        ] {
            assert!(
                !artifact.exists(),
                "retired session host must not be replaced at {}",
                artifact.display()
            );
        }
    }

    #[test]
    fn insecure_instance_lock_parent_is_rejected_before_database_creation() {
        let temp = physical_tempdir().expect("temporary directory");
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o755))
            .expect("public runtime directory");
        let database = temp.path().join("undercurrent.db");
        let instance_lock = temp.path().join("daemon.lock");
        let config = test_config(temp.path(), &database, &instance_lock);

        let error = DaemonProcess::compose(config).expect_err("public instance-lock parent");

        assert_eq!(
            error.stage(),
            DaemonProcessStartStage::PrepareInstanceLockPath
        );
        assert!(!database.exists());
    }

    #[test]
    fn one_instance_lock_allows_only_one_composing_daemon_process() {
        let _fixture = PROCESS_RUNTIME_FIXTURE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let temp = physical_tempdir().expect("temporary directory");
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700))
            .expect("private runtime directory");
        let database = temp.path().join("undercurrent.db");
        let instance_lock = temp.path().join("daemon.lock");
        let first = compose_test_daemon(test_config(temp.path(), &database, &instance_lock));

        let error = DaemonProcess::compose(test_config(temp.path(), &database, &instance_lock))
            .expect_err("second daemon must not compose");

        assert_eq!(error.stage(), DaemonProcessStartStage::AcquireInstanceLock);
        first
            .control()
            .begin_shutdown(Duration::from_millis(100))
            .expect("first daemon product shutdown");
        drop(first);
        let replacement = compose_test_daemon(test_config(temp.path(), &database, &instance_lock));
        replacement
            .control()
            .begin_shutdown(Duration::from_millis(100))
            .expect("replacement daemon product shutdown");
        drop(replacement);
    }

    #[test]
    fn ordinary_restart_recovers_local_socket_and_rotates_server_epoch() {
        let _fixture = PROCESS_RUNTIME_FIXTURE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let temp = physical_tempdir().expect("temporary directory");
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700))
            .expect("private runtime directory");
        let database = temp.path().join("undercurrent.db");
        let instance_lock = temp.path().join("daemon.lock");
        let first_config = test_config(temp.path(), &database, &instance_lock);
        let endpoint = first_config.local_endpoint().path().to_path_buf();
        let first = compose_test_daemon(first_config);
        let first_epoch = first.epoch().clone();
        first
            .control()
            .begin_shutdown(Duration::from_millis(100))
            .expect("first daemon product shutdown");
        drop(first);

        let replacement = compose_test_daemon(test_config(temp.path(), &database, &instance_lock));
        assert_ne!(
            first_epoch,
            replacement.epoch().clone(),
            "each daemon process must receive a fresh server epoch"
        );
        assert_eq!(
            fs::metadata(endpoint)
                .expect("replacement local socket")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert!(!temp.path().join("websocket.token").exists());

        replacement
            .control()
            .begin_shutdown(Duration::from_millis(100))
            .expect("replacement daemon product shutdown");
        drop(replacement);
    }

    fn test_config(home: &Path, database: &Path, instance_lock: &Path) -> ResolvedDaemonConfig {
        let discovery =
            ConfigDiscovery::from_path_env(PathEnvironment::with_home_dir(home.to_path_buf()));
        load_daemon_config(
            &discovery,
            DaemonConfigOverrides::new()
                .with_database_path(database)
                .with_instance_lock_path(instance_lock),
        )
        .expect("resolved daemon config")
    }
}
