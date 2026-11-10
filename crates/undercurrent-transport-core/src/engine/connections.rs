// SPDX-License-Identifier: Apache-2.0

//! Bounded ownership registry enforcing one physical connection per client.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::{Arc, Mutex, Weak};

use undercurrent_multiplex::MultiplexError;
use undercurrent_wire::core::PaginationValueError;
use undercurrent_wire::{
    ClientId, ConnectionAgreement, ConnectionId, ConnectionLimits, PageSize, ProtocolVersion,
    ServerEpoch,
};

use crate::{
    ServerConnectionError, ServerConnectionPressure, ServerConnectionSession,
    ServerDiagnosticLimits,
};

/// Current active physical-connection registry pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveConnectionPressure {
    /// Registered logical clients.
    pub clients: usize,
    /// Registered physical connection IDs.
    pub connections: usize,
    /// Inclusive configured connection capacity.
    pub limit: usize,
}

/// Payload-free negotiated metadata for one registered physical connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiatedConnectionDiagnostic {
    /// Stable server-assigned physical connection identity.
    pub connection_id: ConnectionId,
    /// Server process epoch governing replay validity.
    pub server_epoch: ServerEpoch,
    /// Selected Undercurrent connection protocol version.
    pub protocol_version: ProtocolVersion,
    /// Negotiated connection resource bounds.
    pub limits: ConnectionLimits,
    /// Replay retention when logical stream resume was negotiated.
    pub resume_retention_millis: Option<u64>,
}

/// One bounded, payload-free active connection diagnostic row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveConnectionDiagnostic {
    /// Stable logical client owning the physical connection.
    pub client_id: ClientId,
    /// Immutable negotiated connection metadata.
    pub negotiated: NegotiatedConnectionDiagnostic,
    /// Current session pressure, absent during handshake or after session drop.
    pub pressure: Option<ServerConnectionPressure>,
}

/// Work performed to construct one active connection diagnostic page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveConnectionDiagnosticCost {
    /// Ordered registry entries examined, including one look-ahead entry.
    pub registry_entries_examined: usize,
    /// Live sessions sampled for connection-local pressure.
    pub sessions_sampled: usize,
}

/// Stable client-ID-ordered page of active connection diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveConnectionDiagnosticPage {
    /// Payload-free rows in ascending client-ID order.
    pub items: Vec<ActiveConnectionDiagnostic>,
    /// Exclusive client-ID cursor for the next page when more rows exist.
    pub next_after: Option<ClientId>,
    /// Registry pressure captured with page selection.
    pub registry: ActiveConnectionPressure,
    /// Exact bounded work performed by this read.
    pub cost: ActiveConnectionDiagnosticCost,
}

/// Bounded daemon registry for active client-to-connection ownership.
#[derive(Clone)]
pub struct ActiveConnectionRegistry {
    maximum: usize,
    diagnostic_limits: ServerDiagnosticLimits,
    state: Arc<Mutex<ActiveConnectionState>>,
}

impl ActiveConnectionRegistry {
    /// Constructs an empty registry with an explicit physical connection bound.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed invalid-limit failure when `maximum` is zero.
    pub fn new(
        maximum: usize,
        diagnostic_limits: ServerDiagnosticLimits,
    ) -> Result<Self, ServerConnectionError> {
        if maximum == 0 {
            return Err(ServerConnectionError::InvalidConfigurationLimit {
                field: "active_connections",
            });
        }
        Ok(Self {
            maximum,
            diagnostic_limits,
            state: Arc::new(Mutex::new(ActiveConnectionState::default())),
        })
    }

    /// Atomically reserves one client and physical connection identity pair.
    ///
    /// The returned lease owns the registration and removes it on drop. This
    /// function performs no transport IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns client/connection conflict or configured capacity failure.
    pub fn register(
        &self,
        client_id: ClientId,
        agreement: &ConnectionAgreement,
    ) -> Result<ActiveConnectionLease, ServerConnectionError> {
        self.register_entry(client_id, agreement, Weak::new())
    }

    /// Atomically registers one fully composed connection session.
    ///
    /// The registry retains only a weak session reference, so a forgotten lease
    /// cannot keep connection-local requests, streams, queues, or replay handles
    /// alive. This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns client/connection conflict or configured capacity failure.
    pub fn register_session(
        &self,
        session: &Arc<ServerConnectionSession>,
    ) -> Result<ActiveConnectionLease, ServerConnectionError> {
        self.register_entry(
            session.client_id().clone(),
            session.agreement(),
            Arc::downgrade(session),
        )
    }

    /// Applies shared identity, capacity, metadata, and lease registration.
    fn register_entry(
        &self,
        client_id: ClientId,
        agreement: &ConnectionAgreement,
        session: Weak<ServerConnectionSession>,
    ) -> Result<ActiveConnectionLease, ServerConnectionError> {
        let connection_id = agreement.connection_id().clone();
        let mut state = self.lock_state();
        if let Some(existing) = state.clients.get(&client_id) {
            return Err(ServerConnectionError::ClientAlreadyConnected {
                client_id,
                connection_id: existing.negotiated.connection_id.clone(),
            });
        }
        if state.connections.contains(&connection_id) {
            return Err(ServerConnectionError::ConnectionIdInUse { connection_id });
        }
        if state.clients.len() >= self.maximum {
            return Err(MultiplexError::CapacityExceeded {
                resource: "active_connections",
                limit: self.maximum,
            }
            .into());
        }
        state.clients.insert(
            client_id.clone(),
            ActiveConnectionEntry {
                negotiated: NegotiatedConnectionDiagnostic {
                    connection_id: connection_id.clone(),
                    server_epoch: agreement.server_epoch().clone(),
                    protocol_version: agreement.protocol_version(),
                    limits: agreement.limits(),
                    resume_retention_millis: agreement.resume_retention_millis(),
                },
                session,
            },
        );
        state.connections.insert(connection_id.clone());
        Ok(ActiveConnectionLease {
            client_id,
            connection_id,
            registry: Arc::downgrade(&self.state),
            released: false,
        })
    }

    /// Reads one bounded, stable client-ID-ordered diagnostic page.
    ///
    /// `after` is an exclusive seek cursor. The read examines at most one more
    /// registry entry than requested and samples pressure only for returned live
    /// sessions. It does not retain sessions, inspect protocol payloads, or
    /// perform transport IO. Concurrent registration changes may appear between
    /// pages; this operational view is intentionally not a historical snapshot.
    ///
    /// # Errors
    ///
    /// Returns a typed pagination failure for zero or excessive page size.
    pub fn diagnostic_page(
        &self,
        after: Option<&ClientId>,
        requested_page_size: u32,
    ) -> Result<ActiveConnectionDiagnosticPage, PaginationValueError> {
        let page_size = PageSize::new(
            requested_page_size,
            self.diagnostic_limits.connection_page_size(),
        )?
        .get() as usize;
        // Copy the bounded page before sampling sessions so waiting for a session
        // lock does not also block registration or lease release. Registry counts
        // and session pressure can therefore describe different instants.
        // See [page consistency](../../docs/reference/diagnostics.md#pagination-and-consistency).
        let (mut selected, registry) = {
            let state = self.lock_state();
            let registry = ActiveConnectionPressure {
                clients: state.clients.len(),
                connections: state.connections.len(),
                limit: self.maximum,
            };
            let selected: Vec<_> = match after {
                Some(after) => state
                    .clients
                    .range((Excluded(after.clone()), Unbounded))
                    .take(page_size.saturating_add(1))
                    .map(|(client_id, entry)| (client_id.clone(), entry.clone()))
                    .collect(),
                None => state
                    .clients
                    .iter()
                    .take(page_size.saturating_add(1))
                    .map(|(client_id, entry)| (client_id.clone(), entry.clone()))
                    .collect(),
            };
            (selected, registry)
        };

        let registry_entries_examined = selected.len();
        let has_more = selected.len() > page_size;
        if has_more {
            selected.pop();
        }
        let next_after = if has_more {
            selected.last().map(|(client_id, _)| client_id.clone())
        } else {
            None
        };
        let mut sessions_sampled = 0;
        let items = selected
            .into_iter()
            .map(|(client_id, entry)| {
                let pressure = entry.session.upgrade().map(|session| {
                    sessions_sampled += 1;
                    session.pressure()
                });
                ActiveConnectionDiagnostic {
                    client_id,
                    negotiated: entry.negotiated,
                    pressure,
                }
            })
            .collect();

        Ok(ActiveConnectionDiagnosticPage {
            items,
            next_after,
            registry,
            cost: ActiveConnectionDiagnosticCost {
                registry_entries_examined,
                sessions_sampled,
            },
        })
    }

    /// Reads one bounded logical-stream diagnostic page for an active client.
    ///
    /// The registry lock is released before the connection-local page is read.
    /// A missing registration or unavailable weak session returns `None`. A live
    /// closed session may still return an empty page. This method performs no
    /// transport IO and does not retain the session beyond the read.
    ///
    /// # Errors
    ///
    /// Returns typed page-size or connection registry failures.
    pub fn stream_diagnostic_page(
        &self,
        client_id: &ClientId,
        after: Option<&undercurrent_wire::StreamId>,
        requested_page_size: u32,
    ) -> Result<Option<crate::ServerStreamDiagnosticPage>, ServerConnectionError> {
        let session = self
            .lock_state()
            .clients
            .get(client_id)
            .map(|entry| entry.session.clone())
            .and_then(|session| session.upgrade());
        session
            .map(|session| session.stream_diagnostic_page(after, requested_page_size))
            .transpose()
    }

    /// Returns a lock-consistent bounded registry pressure snapshot.
    pub fn pressure(&self) -> ActiveConnectionPressure {
        let state = self.lock_state();
        ActiveConnectionPressure {
            clients: state.clients.len(),
            connections: state.connections.len(),
            limit: self.maximum,
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, ActiveConnectionState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Drop lease owning one active client-to-connection registration.
pub struct ActiveConnectionLease {
    client_id: ClientId,
    connection_id: ConnectionId,
    registry: Weak<Mutex<ActiveConnectionState>>,
    released: bool,
}

impl ActiveConnectionLease {
    /// Returns the registered logical client identity.
    pub const fn client_id(&self) -> &ClientId {
        &self.client_id
    }

    /// Returns the registered physical connection identity.
    pub const fn connection_id(&self) -> &ConnectionId {
        &self.connection_id
    }

    /// Releases ownership before drop when an adapter completes teardown.
    ///
    /// Repeated release is idempotent. This function performs no IO and does
    /// not panic.
    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        let mut state = registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state
            .clients
            .get(&self.client_id)
            .is_some_and(|entry| entry.negotiated.connection_id == self.connection_id)
        {
            state.clients.remove(&self.client_id);
            state.connections.remove(&self.connection_id);
        }
    }
}

impl Drop for ActiveConnectionLease {
    fn drop(&mut self) {
        self.release_inner();
    }
}

#[derive(Default)]
struct ActiveConnectionState {
    clients: BTreeMap<ClientId, ActiveConnectionEntry>,
    connections: BTreeSet<ConnectionId>,
}

#[derive(Clone)]
struct ActiveConnectionEntry {
    negotiated: NegotiatedConnectionDiagnostic,
    session: Weak<ServerConnectionSession>,
}
