// SPDX-License-Identifier: Apache-2.0

//! Verified caller context and correlated pre-product capacity rejection.
//!
//! The adapter supplies authenticated identity; a logical client ID alone is not
//! authentication. Product payload encoding remains the dispatcher's responsibility.

use crate::{ServerAction, ServerConnectionSession, ServerDispatchError};
use std::sync::Arc;
use undercurrent_multiplex::MultiplexOverload;
use undercurrent_protocol::AuthenticatedRequestContext;
use undercurrent_wire::RequestId;

/// Product payload shape required for one connection-level admission rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerAdmissionResponseKind {
    /// A finite product response payload.
    FiniteRequest,
    /// A product stream-open response payload.
    StreamOpen,
}

/// Correlated, payload-free capacity rejection below product dispatch admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerAdmissionRejection {
    request_id: RequestId,
    response_kind: ServerAdmissionResponseKind,
    overload: MultiplexOverload,
}

impl ServerAdmissionRejection {
    /// Constructs one rejection from a validated request ID and identifier-free overload context.
    pub const fn new(
        request_id: RequestId,
        response_kind: ServerAdmissionResponseKind,
        overload: MultiplexOverload,
    ) -> Self {
        Self {
            request_id,
            response_kind,
            overload,
        }
    }

    /// Returns the connection-epoch request identity to settle.
    pub const fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    /// Returns the product response codec family required by the request.
    pub const fn response_kind(&self) -> ServerAdmissionResponseKind {
        self.response_kind
    }

    /// Returns identifier- and payload-free capacity context.
    pub const fn overload(&self) -> MultiplexOverload {
        self.overload
    }
}

/// Authenticated daemon dispatch boundary below a verified transport identity adapter.
pub trait AuthenticatedActionDispatcher: Send + Sync {
    /// Accepts one action with immutable credential-free caller identity.
    ///
    /// Success transfers ownership to bounded product execution; it does not
    /// establish operation completion. The implementation owns product decoding,
    /// authorization, cancellation and response settlement. The local adapter
    /// treats an error as connection-fatal because work ownership is ambiguous.
    fn dispatch_authenticated(
        &self,
        authentication: Arc<AuthenticatedRequestContext>,
        connection: Arc<ServerConnectionSession>,
        action: ServerAction,
    ) -> Result<(), ServerDispatchError>;

    /// Settles one request rejected by connection-level capacity before product work admission.
    ///
    /// Returning `Ok(true)` means a correlated typed response was queued. The
    /// default returns `Ok(false)`, causing the socket adapter to use its typed
    /// replacement-connection fallback rather than inventing a product payload.
    fn reject_authenticated_admission(
        &self,
        _authentication: Arc<AuthenticatedRequestContext>,
        _connection: Arc<ServerConnectionSession>,
        _rejection: ServerAdmissionRejection,
    ) -> Result<bool, ServerDispatchError> {
        Ok(false)
    }
}
