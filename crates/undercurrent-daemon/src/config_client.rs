// SPDX-License-Identifier: Apache-2.0

//! Validated native SDK, local dial, reconnect, and resume settings.

pub use undercurrent_config::DaemonClientConfig;

use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};
use std::time::Duration;

use undercurrent_sdk::{
    ClientRecoveryPolicy, ClientRuntimeLimits, LocalClientLimits,
    PRODUCT_CLIENT_REQUIRED_CAPABILITIES, ProductClientRuntime,
};
use undercurrent_wire::core::NamespacedName;
use undercurrent_wire::{ClientId, ConnectionContractError, ConnectionHello, ConnectionPolicy};

use crate::{
    DaemonConfigError, DaemonTransportSettings, config_transport::WIRE_ENVELOPE_RESERVATION_BYTES,
};

/// Private local session-host dial settings retained inside the daemon runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionHostClientLimits {
    dial_timeout: Duration,
    cancellation_interval: Duration,
    handshake_timeout: Duration,
    write_timeout: Duration,
}

impl SessionHostClientLimits {
    fn new(
        dial_timeout: Duration,
        cancellation_interval: Duration,
        handshake_timeout: Duration,
        write_timeout: Duration,
    ) -> Result<Self, DaemonConfigError> {
        for (field, value) in [
            ("client.dial_timeout", dial_timeout),
            ("client.cancellation_interval", cancellation_interval),
            ("client.handshake_timeout", handshake_timeout),
            ("client.write_timeout", write_timeout),
        ] {
            if value.is_zero() {
                return Err(DaemonConfigError::InvalidRelationship {
                    rule: match field {
                        "client.dial_timeout" => "client dial timeout must be non-zero",
                        "client.cancellation_interval" => {
                            "client cancellation interval must be non-zero"
                        }
                        "client.handshake_timeout" => "client handshake timeout must be non-zero",
                        _ => "client write timeout must be non-zero",
                    },
                });
            }
        }
        if cancellation_interval > dial_timeout {
            return Err(DaemonConfigError::InvalidRelationship {
                rule: "client cancellation interval must not exceed dial timeout",
            });
        }
        Ok(Self {
            dial_timeout,
            cancellation_interval,
            handshake_timeout,
            write_timeout,
        })
    }

    /// Returns the complete private session-host dial deadline.
    pub const fn dial_timeout(self) -> Duration {
        self.dial_timeout
    }

    /// Returns the cancellation observation interval.
    pub const fn cancellation_interval(self) -> Duration {
        self.cancellation_interval
    }

    /// Returns the private UCHS exchange deadline.
    pub const fn handshake_timeout(self) -> Duration {
        self.handshake_timeout
    }

    /// Returns the private session-host write deadline.
    pub const fn write_timeout(self) -> Duration {
        self.write_timeout
    }
}

/// Fully validated client-side projection of the daemon transport contract.
#[derive(Clone)]
pub struct DaemonClientSettings {
    policy: ConnectionPolicy,
    required_capabilities: BTreeSet<NamespacedName>,
    session_host: SessionHostClientLimits,
    local: LocalClientLimits,
    product: ProductClientRuntime,
    bulk_parallel_requests: usize,
}

impl DaemonClientSettings {
    /// Validates native-client bounds against the server negotiation policy.
    pub(crate) fn validate(
        config: DaemonClientConfig,
        transport: &DaemonTransportSettings,
    ) -> Result<Self, DaemonConfigError> {
        let policy = transport.policy().clone();
        let required_capabilities = PRODUCT_CLIENT_REQUIRED_CAPABILITIES
            .into_iter()
            .map(NamespacedName::new)
            .collect::<Result<BTreeSet<_>, _>>()
            .map_err(|source| DaemonConfigError::component("client.capabilities", source))?;
        let session_host = SessionHostClientLimits::new(
            Duration::from_millis(config.dial_timeout_millis),
            Duration::from_millis(config.cancellation_interval_millis),
            transport.local_server().handshake_timeout(),
            transport.local_server().write_timeout(),
        )?;
        let local = LocalClientLimits::new(
            transport.local_server().handshake_frame_bytes(),
            Duration::from_millis(config.dial_timeout_millis),
            transport.local_server().handshake_timeout(),
            usize::try_from(policy.limits().max_frame_bytes())
                .unwrap_or(usize::MAX)
                .max(
                    usize::try_from(transport.local_server().handshake_frame_bytes())
                        .unwrap_or(usize::MAX),
                ),
            config.writer_queue_messages,
            config.stream_mailbox_messages,
        )
        .map_err(|source| DaemonConfigError::component("client.local", source))?;
        let maximum_payload = policy
            .limits()
            .max_frame_bytes()
            .saturating_sub(WIRE_ENVELOPE_RESERVATION_BYTES);
        let client = ClientRuntimeLimits::new(
            maximum_payload,
            config.writer_queue_messages,
            config.stream_mailbox_messages,
            config.stream_mailbox_bytes,
        )
        .map_err(|source| DaemonConfigError::component("client.runtime", source))?;
        if config.bulk_parallel_requests == 0 {
            return Err(DaemonConfigError::InvalidRelationship {
                rule: "client bulk parallel requests must be non-zero",
            });
        }
        if config.bulk_parallel_requests > config.writer_queue_messages {
            return Err(DaemonConfigError::InvalidRelationship {
                rule: "client bulk parallel requests must not exceed the writer queue",
            });
        }
        if config.bulk_parallel_requests
            > transport.policy().limits().max_pending_requests() as usize
        {
            return Err(DaemonConfigError::InvalidRelationship {
                rule: "client bulk parallel requests must not exceed negotiated pending requests",
            });
        }
        if config.stream_mailbox_bytes > policy.limits().stream_queue_bytes() {
            return Err(DaemonConfigError::InvalidRelationship {
                rule: "client stream mailbox bytes must not exceed negotiated stream queue bytes",
            });
        }
        let recovery = ClientRecoveryPolicy::new(
            Duration::from_millis(config.reconnect_initial_delay_millis),
            Duration::from_millis(config.reconnect_maximum_delay_millis),
            config.reconnect_backoff_basis_points,
            config.reconnect_jitter_basis_points,
            config.reconnect_maximum_attempts,
            config.maximum_parallel_resumes,
        )
        .map_err(|source| DaemonConfigError::component("client.recovery", source))?;
        let product = ProductClientRuntime::new(
            client,
            recovery,
            Duration::from_millis(config.resume_timeout_millis),
            transport.product_codec(),
            transport.terminal_codec(),
        )
        .map_err(|source| DaemonConfigError::component("client.product", source))?;
        Ok(Self {
            policy,
            required_capabilities,
            session_host,
            local,
            product,
            bulk_parallel_requests: config.bulk_parallel_requests,
        })
    }

    /// Builds one client-owned handshake offer from the validated server policy.
    ///
    /// The typed SDK capability registry supplies the required set; unrelated
    /// future server capabilities do not become accidental client requirements.
    ///
    /// # Errors
    ///
    /// Returns a protocol contract failure if the policy projection becomes
    /// internally inconsistent.
    pub fn hello(&self, client_id: ClientId) -> Result<ConnectionHello, ConnectionContractError> {
        ConnectionHello::new(
            client_id,
            self.policy.protocol_version(),
            self.required_capabilities.clone(),
            BTreeSet::new(),
            self.policy.limits(),
        )
    }

    /// Returns bounded Local dial and handshake settings.
    pub const fn local(&self) -> LocalClientLimits {
        self.local
    }

    /// Returns private local session-host dial and UCHS settings.
    pub const fn session_host(&self) -> SessionHostClientLimits {
        self.session_host
    }

    /// Returns bounded typed SDK codec and recovery settings.
    pub const fn product(&self) -> ProductClientRuntime {
        self.product
    }

    /// Returns the bounded concurrency used by one native bulk command.
    pub const fn bulk_parallel_requests(&self) -> usize {
        self.bulk_parallel_requests
    }
}

impl Debug for DaemonClientSettings {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonClientSettings")
            .field("policy", &self.policy)
            .field("required_capabilities", &self.required_capabilities)
            .field("session_host", &self.session_host)
            .field("local", &self.local)
            .field("product", &self.product)
            .field("bulk_parallel_requests", &self.bulk_parallel_requests)
            .finish()
    }
}
