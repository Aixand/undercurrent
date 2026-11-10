// SPDX-License-Identifier: Apache-2.0

//! Internal client/daemon framing and connection-negotiation primitives.
//!
//! [`HandshakeCodec`] encodes bounded UCHS setup frames; [`negotiate_connection`]
//! selects agreement values without admitting a live connection. [`WireCodec`]
//! encodes UCMP requests, controls, responses, and stream items after setup.
//! Product payloads remain opaque bytes here.
//!
//! Both codecs are stateless: callers retain incomplete input, consume exactly
//! the returned frame length, and own I/O, deadlines, queues, cancellation,
//! replay, and shutdown. Decoding a request does not establish execution or
//! durable effects. Trace context and counter values carry metadata; they do
//! not provide instrumentation or enforce stream ordering by themselves.
//!
//! These public Rust exports support workspace composition. Supported external
//! clients use `undercurrent-sdk` and `undercurrent-protocol`. This crate's UCHS
//! handshake is distinct from the runtime's private session-host protocol,
//! which uses a different layout despite sharing the magic bytes.

mod codec;
mod connection;
mod error;
mod handshake;
mod identifiers;
mod message;
mod name;
mod ordering;
mod registry;
mod trace;
mod version;

pub use codec::*;
pub use connection::*;
pub use error::*;
pub use handshake::*;
pub use identifiers::*;
pub use message::*;
pub use name::*;
pub use ordering::*;
pub use registry::*;
pub use trace::*;
pub use version::*;

/// Namespaced access to the UCMP 0.1.0-beta scalar contract.
pub mod core {
    pub use crate::{
        CausationId, ClientId, ConnectionId, CorrelationId, CounterExhausted, IdempotencyKey,
        IdentifierParseError, IdentifierParseErrorReason, MAX_IDENTIFIER_LEN, NameParseError,
        NameParseErrorReason, NamespacedName, PaginationValueError, ProtocolVersion, RequestId,
        ServerEpoch, SpanId, StreamId, StreamSequence, TraceId,
    };
}

/// Namespaced access to the client/daemon UCHS 0.1.0-beta connection contract.
pub mod negotiation {
    pub use crate::{
        ConnectionAgreement, ConnectionContractError, ConnectionHello, ConnectionLimits,
        ConnectionNegotiationError, ConnectionPolicy, HeartbeatPolicy, connection_capabilities,
        negotiate_connection,
    };
}

/// Namespaced access to the UCMP 0.1.0-beta trace contract.
pub mod observability {
    pub use crate::TraceContext;
}
