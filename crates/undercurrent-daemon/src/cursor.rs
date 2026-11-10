// SPDX-License-Identifier: Apache-2.0

//! Bounded, client-scoped opaque pagination cursor retention.

use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use undercurrent_application::SessionPageCursor;
use undercurrent_wire::ClientId;

const TOKEN_PREFIX: char = 'c';
const TOKEN_HEX_BYTES: usize = 16;
const TOKEN_BYTES: usize = 1 + TOKEN_HEX_BYTES;
pub(crate) const SESSION_CURSOR_TOKEN_PLACEHOLDER: &str = "c0000000000000000";

/// Hard limits for retained opaque session-page cursors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CursorStoreLimits {
    clients: usize,
    cursors_per_client: usize,
}

impl CursorStoreLimits {
    /// Constructs non-zero client and per-client cursor bounds.
    pub const fn new(clients: usize, cursors_per_client: usize) -> Result<Self, CursorStoreError> {
        if clients == 0 {
            return Err(CursorStoreError::InvalidLimits { field: "clients" });
        }
        if cursors_per_client == 0 {
            return Err(CursorStoreError::InvalidLimits {
                field: "cursors_per_client",
            });
        }
        Ok(Self {
            clients,
            cursors_per_client,
        })
    }

    /// Returns the maximum distinct clients with retained cursors.
    pub const fn clients(self) -> usize {
        self.clients
    }

    /// Returns the maximum cursors retained for one client.
    pub const fn cursors_per_client(self) -> usize {
        self.cursors_per_client
    }
}

/// Cursor registration or lookup failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorStoreError {
    /// A configured hard bound is zero.
    InvalidLimits { field: &'static str },
    /// A token is not the fixed opaque token format.
    InvalidToken,
    /// The token is absent, evicted, or owned by another client.
    NotFound,
    /// No additional client can retain cursor state.
    ClientCapacity { limit: usize },
    /// The monotonic token space was exhausted.
    TokenExhausted,
}

impl Display for CursorStoreError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLimits { field } => {
                write!(formatter, "cursor limit {field} must be non-zero")
            }
            Self::InvalidToken => formatter.write_str("session cursor is malformed"),
            Self::NotFound => formatter.write_str("session cursor is absent or expired"),
            Self::ClientCapacity { limit } => {
                write!(formatter, "cursor client capacity of {limit} is exhausted")
            }
            Self::TokenExhausted => formatter.write_str("session cursor identity is exhausted"),
        }
    }
}

impl Error for CursorStoreError {}

/// Current payload-free cursor store pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CursorStorePressure {
    /// Clients with at least one retained cursor.
    pub clients: usize,
    /// Maximum client entries.
    pub client_limit: usize,
    /// Total retained cursors across clients.
    pub cursors: usize,
    /// Inclusive aggregate bound derived from configured limits.
    pub cursor_limit: usize,
    /// Tokens evicted from per-client oldest-first retention.
    pub evictions: u64,
}

/// Thread-safe bounded retention for opaque session pagination cursors.
#[derive(Debug)]
pub struct BoundedSessionCursorStore {
    inner: BoundedCursorStore<SessionPageCursor>,
}

impl BoundedSessionCursorStore {
    /// Creates an empty cursor store without performing IO.
    pub fn new(limits: CursorStoreLimits) -> Self {
        Self {
            inner: BoundedCursorStore::new(limits),
        }
    }

    /// Retains one application cursor and returns a fixed-size opaque token.
    ///
    /// The oldest token for the same client is evicted when its bound is full.
    /// Tokens are meaningful only together with the client identity supplied to
    /// [`Self::resolve`]. This function performs no external IO and does not
    /// panic when the mutex is poisoned.
    ///
    /// # Errors
    ///
    /// Returns client capacity when a new client would exceed the configured
    /// distinct-client bound.
    pub fn register(
        &self,
        client_id: ClientId,
        cursor: SessionPageCursor,
    ) -> Result<String, CursorStoreError> {
        self.inner.register(client_id, cursor)
    }

    /// Resolves one cursor only within its owning logical client.
    ///
    /// The returned value is cloned so no lock is held while the prepared
    /// catalog evaluates the query. This function performs no external IO and
    /// does not panic when the mutex is poisoned.
    ///
    /// # Errors
    ///
    /// Returns malformed token or absent/expired ownership errors.
    pub fn resolve(
        &self,
        client_id: &ClientId,
        token: &str,
    ) -> Result<SessionPageCursor, CursorStoreError> {
        self.inner.resolve(client_id, token)
    }

    /// Releases every retained cursor for one logical client.
    pub fn close_client(&self, client_id: &ClientId) {
        self.inner.close_client(client_id);
    }

    /// Returns one payload-free pressure snapshot.
    pub fn pressure(&self) -> CursorStorePressure {
        self.inner.pressure()
    }
}

/// Profile-list continuation retained behind an opaque client-scoped token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfilePageCursor {
    after_name: String,
}

impl ProfilePageCursor {
    /// Creates a continuation after one already returned stable profile name.
    pub fn new(after_name: impl Into<String>) -> Self {
        Self {
            after_name: after_name.into(),
        }
    }

    /// Returns the final stable name represented by this continuation.
    pub fn after_name(&self) -> &str {
        &self.after_name
    }
}

/// Thread-safe bounded retention for opaque profile pagination cursors.
#[derive(Debug)]
pub struct BoundedProfileCursorStore {
    inner: BoundedCursorStore<ProfilePageCursor>,
}

impl BoundedProfileCursorStore {
    /// Creates an empty profile cursor store without performing IO.
    pub fn new(limits: CursorStoreLimits) -> Self {
        Self {
            inner: BoundedCursorStore::new(limits),
        }
    }

    /// Retains one profile continuation for a logical client.
    pub fn register(
        &self,
        client_id: ClientId,
        cursor: ProfilePageCursor,
    ) -> Result<String, CursorStoreError> {
        self.inner.register(client_id, cursor)
    }

    /// Resolves one profile continuation only for its owning client.
    pub fn resolve(
        &self,
        client_id: &ClientId,
        token: &str,
    ) -> Result<ProfilePageCursor, CursorStoreError> {
        self.inner.resolve(client_id, token)
    }

    /// Releases every retained profile cursor for one logical client.
    pub fn close_client(&self, client_id: &ClientId) {
        self.inner.close_client(client_id);
    }

    /// Returns one payload-free pressure snapshot.
    pub fn pressure(&self) -> CursorStorePressure {
        self.inner.pressure()
    }
}

#[derive(Debug)]
struct BoundedCursorStore<T> {
    limits: CursorStoreLimits,
    next_token: AtomicU64,
    state: Mutex<CursorState<T>>,
}

impl<T: Clone> BoundedCursorStore<T> {
    fn new(limits: CursorStoreLimits) -> Self {
        Self {
            limits,
            next_token: AtomicU64::new(1),
            state: Mutex::new(CursorState::default()),
        }
    }

    fn register(&self, client_id: ClientId, cursor: T) -> Result<String, CursorStoreError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !state.clients.contains_key(&client_id) && state.clients.len() == self.limits.clients {
            return Err(CursorStoreError::ClientCapacity {
                limit: self.limits.clients,
            });
        }
        let token = self
            .next_token
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| CursorStoreError::TokenExhausted)?;
        let evicted = {
            let cursors = state.clients.entry(client_id).or_default();
            let evicted = if cursors.entries.len() == self.limits.cursors_per_client {
                cursors
                    .order
                    .pop_front()
                    .is_some_and(|expired| cursors.entries.remove(&expired).is_some())
            } else {
                false
            };
            cursors.order.push_back(token);
            cursors.entries.insert(token, cursor);
            evicted
        };
        if evicted {
            state.evictions = state.evictions.saturating_add(1);
        }
        state.cursor_count = state
            .clients
            .values()
            .map(|entry| entry.entries.len())
            .sum();
        Ok(encode_token(token))
    }

    fn resolve(&self, client_id: &ClientId, token: &str) -> Result<T, CursorStoreError> {
        let token = decode_token(token)?;
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clients
            .get(client_id)
            .and_then(|cursors| cursors.entries.get(&token))
            .cloned()
            .ok_or(CursorStoreError::NotFound)
    }

    fn close_client(&self, client_id: &ClientId) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(removed) = state.clients.remove(client_id) {
            state.cursor_count = state.cursor_count.saturating_sub(removed.entries.len());
        }
    }

    fn pressure(&self) -> CursorStorePressure {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        CursorStorePressure {
            clients: state.clients.len(),
            client_limit: self.limits.clients,
            cursors: state.cursor_count,
            cursor_limit: self
                .limits
                .clients
                .saturating_mul(self.limits.cursors_per_client),
            evictions: state.evictions,
        }
    }
}

#[derive(Debug)]
struct CursorState<T> {
    clients: BTreeMap<ClientId, ClientCursors<T>>,
    cursor_count: usize,
    evictions: u64,
}

impl<T> Default for CursorState<T> {
    fn default() -> Self {
        Self {
            clients: BTreeMap::new(),
            cursor_count: 0,
            evictions: 0,
        }
    }
}

#[derive(Debug)]
struct ClientCursors<T> {
    entries: BTreeMap<u64, T>,
    order: VecDeque<u64>,
}

impl<T> Default for ClientCursors<T> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            order: VecDeque::new(),
        }
    }
}

fn encode_token(token: u64) -> String {
    format!("{TOKEN_PREFIX}{token:016x}")
}

fn decode_token(value: &str) -> Result<u64, CursorStoreError> {
    if value.len() != TOKEN_BYTES || !value.starts_with(TOKEN_PREFIX) {
        return Err(CursorStoreError::InvalidToken);
    }
    u64::from_str_radix(&value[1..], 16).map_err(|_| CursorStoreError::InvalidToken)
}
