// SPDX-License-Identifier: Apache-2.0

//! Physical-connection handshake and bounded multiplex limit negotiation.
//!
//! Constructors and deserialization validate local value invariants.
//! [`negotiate_connection`] additionally checks exact-current versions and
//! required capabilities, then chooses capability intersection and minimum
//! limits. Neither value construction nor negotiation registers a connection,
//! authenticates an identity, starts heartbeat timers, or creates replay state.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{Display, Formatter};

use crate::{ClientId, ConnectionId, NamespacedName, ProtocolVersion, ServerEpoch};
use serde::{Deserialize, Serialize};

use crate::CURRENT_PROTOCOL_VERSION;

/// Maximum capability names accepted in one handshake message.
pub const MAX_CONNECTION_CAPABILITIES: usize = 128;

/// Stable Undercurrent connection capability names.
///
/// These names belong to Undercurrent connection protocol 0.1.0-beta. Older
/// connection versions are rejected before selecting capabilities.
pub mod connection_capabilities {
    /// Multiple finite requests and logical streams share one connection.
    pub const MULTIPLEX: &str = "undercurrent.connection.multiplex";
    /// Per-stream and connection-wide byte credit is supported.
    pub const FLOW_CONTROL: &str = "undercurrent.connection.flow_control";
    /// Logical streams may request replay after reconnect.
    pub const STREAM_RESUME: &str = "undercurrent.connection.stream_resume";
    /// Payload-bearing frames may use a compact binary channel.
    pub const BINARY_PAYLOADS: &str = "undercurrent.connection.binary_payloads";
    /// Finite requests and logical-stream activation carry distributed trace context.
    pub const TRACE_CONTEXT: &str = "undercurrent.connection.trace_context";

    /// Complete Undercurrent connection-capability registry.
    pub const ALL: [&str; 5] = [
        MULTIPLEX,
        FLOW_CONTROL,
        STREAM_RESUME,
        BINARY_PAYLOADS,
        TRACE_CONTEXT,
    ];
}

/// Invalid bounded connection contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionContractError {
    /// A required numeric limit was zero.
    ZeroLimit {
        /// Stable invalid field name.
        field: &'static str,
    },
    /// A child byte budget exceeded its containing budget.
    InvalidLimitRelationship {
        /// Stable violated relationship.
        rule: &'static str,
    },
    /// Capability count exceeded the connection handshake bound.
    TooManyCapabilities {
        /// Observed distinct capability count.
        actual: usize,
        /// Inclusive maximum capability count.
        maximum: usize,
    },
    /// Heartbeat timeout did not exceed its send interval.
    InvalidHeartbeat,
    /// Resume retention was zero when present.
    ZeroResumeRetention,
    /// Resume was advertised without a replay-retention window or vice versa.
    InconsistentResumeCapability,
}

impl Display for ConnectionContractError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroLimit { field } => {
                write!(formatter, "connection limit {field} must be non-zero")
            }
            Self::InvalidLimitRelationship { rule } => formatter.write_str(rule),
            Self::TooManyCapabilities { actual, maximum } => write!(
                formatter,
                "handshake capability count {actual} exceeds maximum {maximum}"
            ),
            Self::InvalidHeartbeat => {
                formatter.write_str("heartbeat timeout must exceed its non-zero interval")
            }
            Self::ZeroResumeRetention => {
                formatter.write_str("resume retention must be non-zero when present")
            }
            Self::InconsistentResumeCapability => formatter.write_str(
                "stream-resume capability and replay retention must be configured together",
            ),
        }
    }
}

impl Error for ConnectionContractError {}

/// Negotiable hard resource limits for one physical connection.
///
/// Validation requires positive values and nested queue/window budgets. It
/// does not require a frame to fit a queue or window, or even the codec header;
/// callers must also construct a codec and choose workable runtime budgets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ConnectionLimitsWire")]
pub struct ConnectionLimits {
    max_frame_bytes: u32,
    max_logical_streams: u32,
    max_pending_requests: u32,
    connection_queue_bytes: u64,
    stream_queue_bytes: u64,
    initial_stream_window_bytes: u64,
}

#[derive(Deserialize)]
struct ConnectionLimitsWire {
    max_frame_bytes: u32,
    max_logical_streams: u32,
    max_pending_requests: u32,
    connection_queue_bytes: u64,
    stream_queue_bytes: u64,
    initial_stream_window_bytes: u64,
}

impl ConnectionLimits {
    /// Constructs validated hard limits.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectionContractError`] when a value is zero, a stream queue
    /// exceeds its connection queue, or initial credit exceeds its stream queue.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub const fn new(
        max_frame_bytes: u32,
        max_logical_streams: u32,
        max_pending_requests: u32,
        connection_queue_bytes: u64,
        stream_queue_bytes: u64,
        initial_stream_window_bytes: u64,
    ) -> Result<Self, ConnectionContractError> {
        if max_frame_bytes == 0 {
            return Err(ConnectionContractError::ZeroLimit {
                field: "max_frame_bytes",
            });
        }
        if max_logical_streams == 0 {
            return Err(ConnectionContractError::ZeroLimit {
                field: "max_logical_streams",
            });
        }
        if max_pending_requests == 0 {
            return Err(ConnectionContractError::ZeroLimit {
                field: "max_pending_requests",
            });
        }
        if connection_queue_bytes == 0 {
            return Err(ConnectionContractError::ZeroLimit {
                field: "connection_queue_bytes",
            });
        }
        if stream_queue_bytes == 0 {
            return Err(ConnectionContractError::ZeroLimit {
                field: "stream_queue_bytes",
            });
        }
        if initial_stream_window_bytes == 0 {
            return Err(ConnectionContractError::ZeroLimit {
                field: "initial_stream_window_bytes",
            });
        }
        if stream_queue_bytes > connection_queue_bytes {
            return Err(ConnectionContractError::InvalidLimitRelationship {
                rule: "stream queue cannot exceed connection queue",
            });
        }
        if initial_stream_window_bytes > stream_queue_bytes {
            return Err(ConnectionContractError::InvalidLimitRelationship {
                rule: "initial stream window cannot exceed stream queue",
            });
        }
        Ok(Self {
            max_frame_bytes,
            max_logical_streams,
            max_pending_requests,
            connection_queue_bytes,
            stream_queue_bytes,
            initial_stream_window_bytes,
        })
    }

    /// Returns maximum encoded frame bytes.
    pub const fn max_frame_bytes(self) -> u32 {
        self.max_frame_bytes
    }

    /// Returns maximum active logical streams.
    pub const fn max_logical_streams(self) -> u32 {
        self.max_logical_streams
    }

    /// Returns maximum pending finite requests.
    pub const fn max_pending_requests(self) -> u32 {
        self.max_pending_requests
    }

    /// Returns total outbound queued-byte budget.
    pub const fn connection_queue_bytes(self) -> u64 {
        self.connection_queue_bytes
    }

    /// Returns one logical stream's outbound queued-byte budget.
    pub const fn stream_queue_bytes(self) -> u64 {
        self.stream_queue_bytes
    }

    /// Returns initial per-stream flow-control credit.
    pub const fn initial_stream_window_bytes(self) -> u64 {
        self.initial_stream_window_bytes
    }

    /// Returns the component-wise minimum of two valid offers.
    ///
    /// This is valid because each nested budget was already bounded by its
    /// containing budget. It performs no IO and does not panic.
    pub const fn minimum(self, other: Self) -> Self {
        Self {
            max_frame_bytes: min_u32(self.max_frame_bytes, other.max_frame_bytes),
            max_logical_streams: min_u32(self.max_logical_streams, other.max_logical_streams),
            max_pending_requests: min_u32(self.max_pending_requests, other.max_pending_requests),
            connection_queue_bytes: min_u64(
                self.connection_queue_bytes,
                other.connection_queue_bytes,
            ),
            stream_queue_bytes: min_u64(self.stream_queue_bytes, other.stream_queue_bytes),
            initial_stream_window_bytes: min_u64(
                self.initial_stream_window_bytes,
                other.initial_stream_window_bytes,
            ),
        }
    }
}

impl TryFrom<ConnectionLimitsWire> for ConnectionLimits {
    type Error = ConnectionContractError;

    fn try_from(value: ConnectionLimitsWire) -> Result<Self, Self::Error> {
        Self::new(
            value.max_frame_bytes,
            value.max_logical_streams,
            value.max_pending_requests,
            value.connection_queue_bytes,
            value.stream_queue_bytes,
            value.initial_stream_window_bytes,
        )
    }
}

/// Server-selected heartbeat interval and dead-peer timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "HeartbeatPolicyWire")]
pub struct HeartbeatPolicy {
    interval_millis: u64,
    timeout_millis: u64,
}

#[derive(Deserialize)]
struct HeartbeatPolicyWire {
    interval_millis: u64,
    timeout_millis: u64,
}

impl HeartbeatPolicy {
    /// Constructs a heartbeat policy with a timeout greater than its interval.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectionContractError::InvalidHeartbeat`] for zero or
    /// inverted values.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub const fn new(
        interval_millis: u64,
        timeout_millis: u64,
    ) -> Result<Self, ConnectionContractError> {
        if interval_millis == 0 || timeout_millis <= interval_millis {
            return Err(ConnectionContractError::InvalidHeartbeat);
        }
        Ok(Self {
            interval_millis,
            timeout_millis,
        })
    }

    /// Returns heartbeat send interval in milliseconds.
    pub const fn interval_millis(self) -> u64 {
        self.interval_millis
    }

    /// Returns dead-peer timeout in milliseconds.
    pub const fn timeout_millis(self) -> u64 {
        self.timeout_millis
    }
}

impl TryFrom<HeartbeatPolicyWire> for HeartbeatPolicy {
    type Error = ConnectionContractError;

    fn try_from(value: HeartbeatPolicyWire) -> Result<Self, Self::Error> {
        Self::new(value.interval_millis, value.timeout_millis)
    }
}

/// Client connection handshake offer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ConnectionHelloWire")]
pub struct ConnectionHello {
    /// Stable logical client identity.
    client_id: ClientId,
    /// Exact current connection protocol version.
    protocol_version: ProtocolVersion,
    /// Capabilities without which the client cannot safely operate.
    required_capabilities: BTreeSet<NamespacedName>,
    /// Additional capabilities the client can use.
    optional_capabilities: BTreeSet<NamespacedName>,
    /// Client-side resource maxima.
    limits: ConnectionLimits,
}

#[derive(Deserialize)]
struct ConnectionHelloWire {
    client_id: ClientId,
    protocol_version: ProtocolVersion,
    required_capabilities: BTreeSet<NamespacedName>,
    optional_capabilities: BTreeSet<NamespacedName>,
    limits: ConnectionLimits,
}

impl ConnectionHello {
    /// Constructs a bounded client offer and removes required capabilities from
    /// the optional set.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectionContractError::TooManyCapabilities`] above the
    /// connection handshake bound.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn new(
        client_id: ClientId,
        protocol_version: ProtocolVersion,
        required_capabilities: BTreeSet<NamespacedName>,
        mut optional_capabilities: BTreeSet<NamespacedName>,
        limits: ConnectionLimits,
    ) -> Result<Self, ConnectionContractError> {
        optional_capabilities.retain(|name| !required_capabilities.contains(name));
        validate_capability_count(required_capabilities.len() + optional_capabilities.len())?;
        Ok(Self {
            client_id,
            protocol_version,
            required_capabilities,
            optional_capabilities,
            limits,
        })
    }

    /// Returns stable logical client identity.
    pub const fn client_id(&self) -> &ClientId {
        &self.client_id
    }

    /// Returns the exact protocol version.
    pub const fn protocol_version(&self) -> ProtocolVersion {
        self.protocol_version
    }

    /// Returns capabilities required for safe operation.
    pub const fn required_capabilities(&self) -> &BTreeSet<NamespacedName> {
        &self.required_capabilities
    }

    /// Returns optional client capabilities.
    pub const fn optional_capabilities(&self) -> &BTreeSet<NamespacedName> {
        &self.optional_capabilities
    }

    /// Returns client-side hard resource maxima.
    pub const fn limits(&self) -> ConnectionLimits {
        self.limits
    }
}

impl TryFrom<ConnectionHelloWire> for ConnectionHello {
    type Error = ConnectionContractError;

    fn try_from(value: ConnectionHelloWire) -> Result<Self, Self::Error> {
        Self::new(
            value.client_id,
            value.protocol_version,
            value.required_capabilities,
            value.optional_capabilities,
            value.limits,
        )
    }
}

/// Server connection negotiation policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ConnectionPolicyWire")]
pub struct ConnectionPolicy {
    /// Exact current connection protocol version.
    protocol_version: ProtocolVersion,
    /// Supported connection and product capability names.
    capabilities: BTreeSet<NamespacedName>,
    /// Server-side resource maxima.
    limits: ConnectionLimits,
    /// Server-selected heartbeat behavior.
    heartbeat: HeartbeatPolicy,
    /// In-memory replay retention after disconnect, in milliseconds.
    resume_retention_millis: Option<u64>,
}

#[derive(Deserialize)]
struct ConnectionPolicyWire {
    protocol_version: ProtocolVersion,
    capabilities: BTreeSet<NamespacedName>,
    limits: ConnectionLimits,
    heartbeat: HeartbeatPolicy,
    resume_retention_millis: Option<u64>,
}

impl ConnectionPolicy {
    /// Constructs a bounded server policy.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectionContractError`] for excessive capabilities or an
    /// inconsistent resume capability and retention window.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn new(
        protocol_version: ProtocolVersion,
        capabilities: BTreeSet<NamespacedName>,
        limits: ConnectionLimits,
        heartbeat: HeartbeatPolicy,
        resume_retention_millis: Option<u64>,
    ) -> Result<Self, ConnectionContractError> {
        validate_capability_count(capabilities.len())?;
        if resume_retention_millis == Some(0) {
            return Err(ConnectionContractError::ZeroResumeRetention);
        }
        let supports_resume = capabilities
            .iter()
            .any(|name| name.as_str() == connection_capabilities::STREAM_RESUME);
        if supports_resume != resume_retention_millis.is_some() {
            return Err(ConnectionContractError::InconsistentResumeCapability);
        }
        Ok(Self {
            protocol_version,
            capabilities,
            limits,
            heartbeat,
            resume_retention_millis,
        })
    }

    /// Returns the exact server protocol version.
    pub const fn protocol_version(&self) -> ProtocolVersion {
        self.protocol_version
    }

    /// Returns supported server capabilities.
    pub const fn capabilities(&self) -> &BTreeSet<NamespacedName> {
        &self.capabilities
    }

    /// Returns server-side hard resource maxima.
    pub const fn limits(&self) -> ConnectionLimits {
        self.limits
    }

    /// Returns selected heartbeat policy.
    pub const fn heartbeat(&self) -> HeartbeatPolicy {
        self.heartbeat
    }

    /// Returns replay retention when resume is supported.
    pub const fn resume_retention_millis(&self) -> Option<u64> {
        self.resume_retention_millis
    }
}

impl TryFrom<ConnectionPolicyWire> for ConnectionPolicy {
    type Error = ConnectionContractError;

    fn try_from(value: ConnectionPolicyWire) -> Result<Self, Self::Error> {
        Self::new(
            value.protocol_version,
            value.capabilities,
            value.limits,
            value.heartbeat,
            value.resume_retention_millis,
        )
    }
}

/// Successful physical-connection handshake result.
///
/// Deserialization validates internal limits, capability count, and retention
/// consistency. It does not rerun negotiation against a prior client offer or
/// enforce exact-current protocol compatibility; those need caller context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ConnectionAgreementWire")]
pub struct ConnectionAgreement {
    /// Stable physical connection identity.
    connection_id: ConnectionId,
    /// Server process epoch governing replay validity.
    server_epoch: ServerEpoch,
    /// Exact protocol version selected by negotiation.
    protocol_version: ProtocolVersion,
    /// Mutually supported capabilities enabled on the connection.
    capabilities: BTreeSet<NamespacedName>,
    /// Component-wise minimum hard limits.
    limits: ConnectionLimits,
    /// Server-selected heartbeat behavior.
    heartbeat: HeartbeatPolicy,
    /// Replay retention when stream resume was negotiated.
    resume_retention_millis: Option<u64>,
}

#[derive(Deserialize)]
struct ConnectionAgreementWire {
    connection_id: ConnectionId,
    server_epoch: ServerEpoch,
    protocol_version: ProtocolVersion,
    capabilities: BTreeSet<NamespacedName>,
    limits: ConnectionLimits,
    heartbeat: HeartbeatPolicy,
    resume_retention_millis: Option<u64>,
}

impl ConnectionAgreement {
    /// Returns stable physical connection identity.
    pub const fn connection_id(&self) -> &ConnectionId {
        &self.connection_id
    }

    /// Returns server process epoch governing replay validity.
    pub const fn server_epoch(&self) -> &ServerEpoch {
        &self.server_epoch
    }

    /// Returns selected protocol version.
    pub const fn protocol_version(&self) -> ProtocolVersion {
        self.protocol_version
    }

    /// Returns enabled capabilities.
    pub const fn capabilities(&self) -> &BTreeSet<NamespacedName> {
        &self.capabilities
    }

    /// Returns negotiated hard resource limits.
    pub const fn limits(&self) -> ConnectionLimits {
        self.limits
    }

    /// Returns selected heartbeat policy.
    pub const fn heartbeat(&self) -> HeartbeatPolicy {
        self.heartbeat
    }

    /// Returns negotiated replay retention when resume is enabled.
    pub const fn resume_retention_millis(&self) -> Option<u64> {
        self.resume_retention_millis
    }
}

impl TryFrom<ConnectionAgreementWire> for ConnectionAgreement {
    type Error = ConnectionContractError;

    fn try_from(value: ConnectionAgreementWire) -> Result<Self, Self::Error> {
        validate_capability_count(value.capabilities.len())?;
        if value.resume_retention_millis == Some(0) {
            return Err(ConnectionContractError::ZeroResumeRetention);
        }
        let supports_resume = value
            .capabilities
            .iter()
            .any(|name| name.as_str() == connection_capabilities::STREAM_RESUME);
        if supports_resume != value.resume_retention_millis.is_some() {
            return Err(ConnectionContractError::InconsistentResumeCapability);
        }
        Ok(Self {
            connection_id: value.connection_id,
            server_epoch: value.server_epoch,
            protocol_version: value.protocol_version,
            capabilities: value.capabilities,
            limits: value.limits,
            heartbeat: value.heartbeat,
            resume_retention_millis: value.resume_retention_millis,
        })
    }
}

/// Connection handshake rejection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionNegotiationError {
    /// Client and server do not select the same current connection protocol version.
    IncompatibleVersions {
        /// Client offer.
        client: ProtocolVersion,
        /// Server policy.
        server: ProtocolVersion,
    },
    /// Server does not implement required client capabilities.
    MissingRequiredCapabilities {
        /// Missing names in deterministic order.
        missing: Vec<NamespacedName>,
    },
}

impl Display for ConnectionNegotiationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IncompatibleVersions { client, server } => write!(
                formatter,
                "client protocol {client} is incompatible with server protocol {server}"
            ),
            Self::MissingRequiredCapabilities { missing } => write!(
                formatter,
                "server is missing {} required connection capabilities",
                missing.len()
            ),
        }
    }
}

impl Error for ConnectionNegotiationError {}

/// Negotiates one physical connection without allocating runtime resources.
///
/// # Errors
///
/// Returns [`ConnectionNegotiationError`] when versions are incompatible or a
/// required client capability is missing. This function performs no IO and
/// does not panic.
pub fn negotiate_connection(
    hello: &ConnectionHello,
    policy: &ConnectionPolicy,
    connection_id: ConnectionId,
    server_epoch: ServerEpoch,
) -> Result<ConnectionAgreement, ConnectionNegotiationError> {
    // Matching offers alone could admit an unimplemented version. Both sides
    // must select the sole current contract; this is not version-range negotiation.
    // See [version scope](../docs/reference/scalars.md#versions).
    if hello.protocol_version != CURRENT_PROTOCOL_VERSION
        || policy.protocol_version != CURRENT_PROTOCOL_VERSION
        || hello.protocol_version != policy.protocol_version
    {
        return Err(ConnectionNegotiationError::IncompatibleVersions {
            client: hello.protocol_version,
            server: policy.protocol_version,
        });
    }
    let missing: Vec<_> = hello
        .required_capabilities
        .difference(&policy.capabilities)
        .cloned()
        .collect();
    if !missing.is_empty() {
        return Err(ConnectionNegotiationError::MissingRequiredCapabilities { missing });
    }
    let requested: BTreeSet<_> = hello
        .required_capabilities
        .union(&hello.optional_capabilities)
        .cloned()
        .collect();
    let capabilities: BTreeSet<_> = requested
        .intersection(&policy.capabilities)
        .cloned()
        .collect();
    let resume_retention_millis = capabilities
        .iter()
        .any(|name| name.as_str() == connection_capabilities::STREAM_RESUME)
        .then_some(policy.resume_retention_millis)
        .flatten();
    Ok(ConnectionAgreement {
        connection_id,
        server_epoch,
        protocol_version: hello.protocol_version,
        capabilities,
        limits: hello.limits.minimum(policy.limits),
        heartbeat: policy.heartbeat,
        resume_retention_millis,
    })
}

fn validate_capability_count(actual: usize) -> Result<(), ConnectionContractError> {
    if actual > MAX_CONNECTION_CAPABILITIES {
        return Err(ConnectionContractError::TooManyCapabilities {
            actual,
            maximum: MAX_CONNECTION_CAPABILITIES,
        });
    }
    Ok(())
}

const fn min_u32(left: u32, right: u32) -> u32 {
    if left < right { left } else { right }
}

const fn min_u64(left: u64, right: u64) -> u64 {
    if left < right { left } else { right }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capability(value: &str) -> NamespacedName {
        NamespacedName::new(value).expect("valid capability")
    }

    fn limits(frame: u32, streams: u32) -> ConnectionLimits {
        ConnectionLimits::new(frame, streams, 64, 1_000_000, 64_000, 32_000).expect("valid limits")
    }

    #[test]
    fn undercurrent_connection_capability_registry_is_exact() {
        assert_eq!(
            connection_capabilities::ALL,
            [
                "undercurrent.connection.multiplex",
                "undercurrent.connection.flow_control",
                "undercurrent.connection.stream_resume",
                "undercurrent.connection.binary_payloads",
                "undercurrent.connection.trace_context",
            ]
        );
    }

    #[test]
    fn negotiation_accepts_current_protocol_and_selects_capabilities_and_minimum_limits() {
        let required = BTreeSet::from([
            capability(connection_capabilities::MULTIPLEX),
            capability(connection_capabilities::FLOW_CONTROL),
        ]);
        let optional = BTreeSet::from([
            capability(connection_capabilities::STREAM_RESUME),
            capability("undercurrent.query_watch"),
        ]);
        let hello = ConnectionHello::new(
            ClientId::new("client-1").expect("client"),
            CURRENT_PROTOCOL_VERSION,
            required,
            optional,
            limits(1_000_000, 500),
        )
        .expect("hello");
        let policy = ConnectionPolicy::new(
            CURRENT_PROTOCOL_VERSION,
            BTreeSet::from([
                capability(connection_capabilities::MULTIPLEX),
                capability(connection_capabilities::FLOW_CONTROL),
                capability(connection_capabilities::STREAM_RESUME),
            ]),
            limits(256_000, 300),
            HeartbeatPolicy::new(5_000, 15_000).expect("heartbeat"),
            Some(30_000),
        )
        .expect("policy");

        let agreement = negotiate_connection(
            &hello,
            &policy,
            ConnectionId::new("connection-1").expect("connection"),
            ServerEpoch::new("epoch-1").expect("epoch"),
        )
        .expect("compatible");

        assert_eq!(agreement.protocol_version(), CURRENT_PROTOCOL_VERSION);
        assert_eq!(agreement.capabilities().len(), 3);
        assert_eq!(agreement.limits().max_frame_bytes(), 256_000);
        assert_eq!(agreement.limits().max_logical_streams(), 300);
        assert_eq!(agreement.resume_retention_millis(), Some(30_000));
    }

    #[test]
    fn missing_required_capability_rejects_handshake() {
        let hello = ConnectionHello::new(
            ClientId::new("client-1").expect("client"),
            CURRENT_PROTOCOL_VERSION,
            BTreeSet::from([capability(connection_capabilities::FLOW_CONTROL)]),
            BTreeSet::new(),
            limits(1_000, 10),
        )
        .expect("hello");
        let policy = ConnectionPolicy::new(
            CURRENT_PROTOCOL_VERSION,
            BTreeSet::new(),
            limits(1_000, 10),
            HeartbeatPolicy::new(1_000, 3_000).expect("heartbeat"),
            None,
        )
        .expect("policy");

        let error = negotiate_connection(
            &hello,
            &policy,
            ConnectionId::new("connection-1").expect("connection"),
            ServerEpoch::new("epoch-1").expect("epoch"),
        )
        .expect_err("capability must be required");

        assert!(matches!(
            error,
            ConnectionNegotiationError::MissingRequiredCapabilities { .. }
        ));
    }

    #[test]
    fn retired_and_future_versions_are_rejected_even_when_both_peers_match() {
        for unsupported in [
            ProtocolVersion::new(0, 1),
            ProtocolVersion::new(1, 0),
            ProtocolVersion::new(1, 1),
            ProtocolVersion::new(2, 1),
            ProtocolVersion::new(3, 0),
        ] {
            for (client, server) in [
                (unsupported, CURRENT_PROTOCOL_VERSION),
                (CURRENT_PROTOCOL_VERSION, unsupported),
                (unsupported, unsupported),
            ] {
                let hello = ConnectionHello::new(
                    ClientId::new("client-1").expect("client"),
                    client,
                    BTreeSet::new(),
                    BTreeSet::new(),
                    limits(1_000, 10),
                )
                .expect("bounded hello");
                let policy = ConnectionPolicy::new(
                    server,
                    BTreeSet::new(),
                    limits(1_000, 10),
                    HeartbeatPolicy::new(1_000, 3_000).expect("heartbeat"),
                    None,
                )
                .expect("policy");

                let error = negotiate_connection(
                    &hello,
                    &policy,
                    ConnectionId::new("connection-1").expect("connection"),
                    ServerEpoch::new("epoch-1").expect("epoch"),
                )
                .expect_err("only the current connection protocol may negotiate");

                assert!(
                    matches!(
                        error,
                        ConnectionNegotiationError::IncompatibleVersions { .. }
                    ),
                    "client {client}, server {server}: {error:?}"
                );
            }
        }
    }

    #[test]
    fn deserialization_reapplies_limit_and_heartbeat_invariants() {
        let invalid_limits = r#"{
            "max_frame_bytes":0,
            "max_logical_streams":1,
            "max_pending_requests":1,
            "connection_queue_bytes":100,
            "stream_queue_bytes":50,
            "initial_stream_window_bytes":25
        }"#;
        assert!(serde_json::from_str::<ConnectionLimits>(invalid_limits).is_err());
        assert!(
            serde_json::from_str::<HeartbeatPolicy>(
                r#"{"interval_millis":5000,"timeout_millis":1000}"#
            )
            .is_err()
        );
    }
}
