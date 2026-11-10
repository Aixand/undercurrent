// SPDX-License-Identifier: Apache-2.0

//! One-invocation authenticated Local product connection adapter.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use undercurrent_sdk::{
    ClientRecoveryError, DeterministicClientRecoveryJitter, LocalClientConfig,
    LocalClientConnector, LocalClientLimits, LocalTransportError, LocalTransportStage,
    ProductClient, ProductClientError, ResilientClientConnectError, StandaloneClientProfile,
};

use crate::{
    CliConfigurationError, CliError, CliInvocationContext, CliProcessOperation, ConfigDiscovery,
    ResolvedCliConfig, load_cli_config,
};

static CLIENT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Config-backed factory that creates at most one live product client per call.
pub struct NativeProductClientFactory {
    configuration: Option<Arc<ResolvedCliConfig>>,
    discovery: ConfigDiscovery,
    explicit_config_path: Option<PathBuf>,
}

impl NativeProductClientFactory {
    /// Builds a factory from one validated CLI invocation context.
    ///
    /// This function reads process environment only through common config
    /// discovery. It performs no filesystem, socket, or process IO and does not
    /// panic.
    pub fn from_context(context: &CliInvocationContext) -> Self {
        let explicit_config_path = context.config_path().map(Path::to_path_buf);
        let mut discovery = ConfigDiscovery::from_process_env(explicit_config_path.clone());
        if let Some(selection) = &context.selection {
            selection.apply_paths(&mut discovery.path_env);
        }
        Self {
            configuration: context.configuration.clone(),
            discovery,
            explicit_config_path,
        }
    }

    /// Resolves and validates the complete shared daemon/client configuration.
    ///
    /// The operation may read the selected config file. It does not create
    /// directories, connect sockets, or spawn processes.
    ///
    /// # Errors
    ///
    /// Returns typed common discovery, path, decoding, or component validation failure.
    pub fn resolve(&self) -> Result<ResolvedCliConfig, CliError> {
        if let Some(config) = &self.configuration {
            return Ok((**config).clone());
        }
        load_cli_config(&self.discovery).map_err(Into::into)
    }

    pub(crate) fn explicit_config_path(&self) -> Option<&Path> {
        self.explicit_config_path.as_deref()
    }

    /// Establishes one resilient multiplexed product connection.
    ///
    /// All requests and streams for the command must clone the returned client;
    /// handlers must not call this factory again per session or terminal. An
    /// unavailable daemon remains a typed connection failure; only the
    /// daemon lifecycle adapter may create a process. This method performs
    /// config, network, and thread IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns configuration, identity, handshake, transport, or typed
    /// resilient connection failure.
    pub fn connect(&self) -> Result<ProductClient, CliError> {
        let config = self.resolve()?;
        self.connect_resolved(&config)
    }

    /// Establishes one resilient multiplexed product connection from an
    /// already resolved configuration.
    ///
    /// The caller can use this boundary to resolve configuration exactly once,
    /// derive CLI execution limits from that same value, and then share the
    /// returned client across every finite request and logical stream. This
    /// method only connects to an existing daemon; it never creates a
    /// process. It performs network and thread IO, does not read configuration
    /// files, and does not panic.
    ///
    /// # Errors
    ///
    /// Returns identity, handshake, transport, or typed resilient connection
    /// failure.
    pub fn connect_resolved(&self, config: &ResolvedCliConfig) -> Result<ProductClient, CliError> {
        connect_product(config, None)
    }

    /// Connects only when a UCMP-0.1.0-beta daemon owns the resolved endpoint.
    ///
    /// An absent local endpoint returns `None`. Negotiation,
    /// permission, resource, and SDK-runtime failures remain errors and never
    /// trigger process creation. This method performs bounded local-IPC and thread
    /// IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns typed connection failures other than an absent local listener.
    pub fn connect_existing_resolved(
        &self,
        config: &ResolvedCliConfig,
    ) -> Result<Option<ProductClient>, CliError> {
        self.connect_existing_resolved_with_transport_budget(config, None)
    }

    pub(crate) fn connect_existing_resolved_with_timeout(
        &self,
        config: &ResolvedCliConfig,
        timeout: std::time::Duration,
    ) -> Result<Option<ProductClient>, CliError> {
        self.connect_existing_resolved_with_transport_budget(config, Some(timeout))
    }

    fn connect_existing_resolved_with_transport_budget(
        &self,
        config: &ResolvedCliConfig,
        transport_budget: Option<std::time::Duration>,
    ) -> Result<Option<ProductClient>, CliError> {
        match connect_product(config, transport_budget) {
            Ok(client) => Ok(Some(client)),
            Err(error) if endpoint_absence_cause(&error).is_some() => Ok(None),
            Err(error) => Err(error),
        }
    }
}

fn connect_product(
    config: &ResolvedCliConfig,
    transport_budget: Option<std::time::Duration>,
) -> Result<ProductClient, CliError> {
    let sequence = CLIENT_SEQUENCE
        .fetch_add(1, Ordering::AcqRel)
        .saturating_add(1);
    let client_id = format!("uc-{}-{sequence}", std::process::id());
    let profile = StandaloneClientProfile::with_request_limit(
        client_id.clone(),
        config.document().requests.request_bytes,
    )
    .map_err(|source| CliConfigurationError::InvalidStaticProfile {
        component: source.component(),
    })?;
    // Lifecycle polling owns the retry loop and its total deadline. A probe gets
    // one physical attempt so SDK backoff cannot extend that outer budget; the
    // established client still uses the standard recovery policy.
    // See [readiness](../../../docs/guides/daemon/cli-lifecycle.md#start-and-verify-readiness).
    let initial_policy = if transport_budget.is_some() {
        StandaloneClientProfile::single_connection_attempt(client_id)
            .map_err(|source| CliConfigurationError::InvalidStaticProfile {
                component: source.component(),
            })?
            .product()
            .recovery_policy()
    } else {
        profile.product().recovery_policy()
    };
    let local_limits = match transport_budget {
        None => profile.local(),
        Some(timeout) => lifecycle_transport_limits(profile.local(), timeout)?,
    };
    let local = LocalClientConfig::new(
        config.connection().endpoint().to_owned(),
        profile.hello().clone(),
        local_limits,
    )
    .map_err(CliError::LocalTransport)?;
    let connector = LocalClientConnector::new(local);
    let client = ProductClient::connect_with_initial_policy(
        Arc::new(connector),
        profile.product(),
        initial_policy,
        Arc::new(DeterministicClientRecoveryJitter::new(
            u64::from(std::process::id()) ^ sequence.rotate_left(17),
        )),
    )
    .map_err(CliError::from)?;
    if let Some(recorder) = undercurrent_diagnostics::process() {
        // The generated reference is already validated by the incident owner.
        // An unexpected metadata failure must not change request behavior.
        Ok(client
            .clone()
            .with_correlation_id(recorder.incident_id())
            .unwrap_or(client))
    } else {
        Ok(client)
    }
}

fn lifecycle_transport_limits(
    base: LocalClientLimits,
    timeout: std::time::Duration,
) -> Result<LocalClientLimits, CliError> {
    // Connect and handshake run sequentially. Giving each the full remainder
    // would let one probe consume twice the lifecycle caller's remaining time.
    let connect_timeout = timeout / 2;
    let handshake_timeout = timeout.saturating_sub(connect_timeout);
    if connect_timeout.is_zero() || handshake_timeout.is_zero() {
        return Err(CliError::Process {
            operation: CliProcessOperation::ProbeDaemonInstance,
            source: std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "daemon endpoint probe deadline elapsed",
            ),
        });
    }
    LocalClientLimits::new(
        base.handshake_frame_bytes(),
        connect_timeout,
        handshake_timeout,
        base.maximum_message_bytes(),
        base.outbound_queue_messages(),
        base.inbound_queue_messages(),
    )
    .map_err(CliError::LocalTransport)
}

/// Redacted operating-system proof that a previously authenticated endpoint
/// is no longer accepting or retaining its transport connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EndpointAbsenceCause {
    NotFound,
    ConnectionRefused,
    ConnectionReset,
}

/// Returns an explicit absence cause without treating TLS, PKI, DNS, HTTP,
/// protocol, authentication, or timeout failures as endpoint disappearance.
pub(crate) fn endpoint_absence_cause(error: &CliError) -> Option<EndpointAbsenceCause> {
    let CliError::Product(ProductClientError::Connect(ResilientClientConnectError::Recovery(
        ClientRecoveryError::AttemptsExhausted { source, .. },
    ))) = error
    else {
        return None;
    };
    let local = std::error::Error::source(source)
        .and_then(|source| source.downcast_ref::<LocalTransportError>())?;
    endpoint_absence_from_transport(local.stage(), std::error::Error::source(local)?)
}

fn endpoint_absence_from_transport(
    stage: LocalTransportStage,
    source: &(dyn std::error::Error + 'static),
) -> Option<EndpointAbsenceCause> {
    if stage != LocalTransportStage::Connect {
        return None;
    }
    endpoint_absence_from_source(source)
}

fn endpoint_absence_from_source(
    mut source: &(dyn std::error::Error + 'static),
) -> Option<EndpointAbsenceCause> {
    loop {
        if let Some(error) = source.downcast_ref::<std::io::Error>() {
            return match error.kind() {
                std::io::ErrorKind::NotFound => Some(EndpointAbsenceCause::NotFound),
                std::io::ErrorKind::ConnectionRefused => {
                    Some(EndpointAbsenceCause::ConnectionRefused)
                }
                std::io::ErrorKind::ConnectionReset => Some(EndpointAbsenceCause::ConnectionReset),
                _ => None,
            };
        }
        source = source.source()?;
    }
}
