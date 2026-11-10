// SPDX-License-Identifier: Apache-2.0

//! Bounded client-scoped terminal screen checkpoints for on-demand diffs.

use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use undercurrent_application::TerminalScreenSnapshot;
use undercurrent_domain::SessionId;
use undercurrent_wire::ClientId;

const TOKEN_PREFIX: char = 's';
const TOKEN_HEX_BYTES: usize = 16;
const TOKEN_BYTES: usize = 1 + TOKEN_HEX_BYTES;

/// Hard ownership, entry, and private-payload bounds for screen checkpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScreenCheckpointLimits {
    clients: usize,
    checkpoints_per_client: usize,
    bytes_per_client: usize,
}

impl ScreenCheckpointLimits {
    /// Constructs non-zero limits for client-scoped checkpoint retention.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a typed configuration error naming the zero field.
    pub const fn new(
        clients: usize,
        checkpoints_per_client: usize,
        bytes_per_client: usize,
    ) -> Result<Self, ScreenCheckpointStoreError> {
        if clients == 0 {
            return Err(ScreenCheckpointStoreError::InvalidLimits { field: "clients" });
        }
        if checkpoints_per_client == 0 {
            return Err(ScreenCheckpointStoreError::InvalidLimits {
                field: "checkpoints_per_client",
            });
        }
        if bytes_per_client == 0 {
            return Err(ScreenCheckpointStoreError::InvalidLimits {
                field: "bytes_per_client",
            });
        }
        Ok(Self {
            clients,
            checkpoints_per_client,
            bytes_per_client,
        })
    }

    /// Returns the maximum clients retaining checkpoints concurrently.
    pub const fn clients(self) -> usize {
        self.clients
    }

    /// Returns the maximum checkpoints retained by one client.
    pub const fn checkpoints_per_client(self) -> usize {
        self.checkpoints_per_client
    }

    /// Returns the maximum logical private screen bytes retained per client.
    pub const fn bytes_per_client(self) -> usize {
        self.bytes_per_client
    }
}

/// Screen checkpoint registration or lookup failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScreenCheckpointStoreError {
    /// A configured hard bound is zero.
    InvalidLimits { field: &'static str },
    /// A token does not use the fixed screen-checkpoint format.
    InvalidToken,
    /// The token is absent, evicted, or owned by another client.
    NotFound,
    /// The token belongs to another stable session.
    SessionMismatch,
    /// No additional client can retain screen checkpoints.
    ClientCapacity { limit: usize },
    /// One screen cannot fit the configured per-client byte budget.
    ScreenCapacity { observed: usize, limit: usize },
    /// The monotonic token identity space was exhausted.
    TokenExhausted,
}

impl Display for ScreenCheckpointStoreError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLimits { field } => {
                write!(
                    formatter,
                    "screen checkpoint limit {field} must be non-zero"
                )
            }
            Self::InvalidToken => formatter.write_str("screen checkpoint is malformed"),
            Self::NotFound => formatter.write_str("screen checkpoint is absent or expired"),
            Self::SessionMismatch => {
                formatter.write_str("screen checkpoint belongs to another session")
            }
            Self::ClientCapacity { limit } => write!(
                formatter,
                "screen checkpoint client capacity of {limit} is exhausted"
            ),
            Self::ScreenCapacity { observed, limit } => write!(
                formatter,
                "screen checkpoint bytes {observed} exceed per-client limit {limit}"
            ),
            Self::TokenExhausted => formatter.write_str("screen checkpoint identity is exhausted"),
        }
    }
}

impl Error for ScreenCheckpointStoreError {}

/// One resolved private screen baseline and its stable target session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScreenCheckpoint {
    session_id: SessionId,
    snapshot: TerminalScreenSnapshot,
}

impl ScreenCheckpoint {
    /// Returns the stable session the checkpoint was issued for.
    pub const fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    /// Returns the private parser-independent screen baseline.
    pub const fn snapshot(&self) -> &TerminalScreenSnapshot {
        &self.snapshot
    }
}

/// Payload-free pressure for bounded screen checkpoint retention.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScreenCheckpointPressure {
    /// Clients currently retaining at least one checkpoint.
    pub clients: usize,
    /// Configured inclusive client capacity.
    pub client_limit: usize,
    /// Checkpoints currently retained across all clients.
    pub checkpoints: usize,
    /// Inclusive aggregate checkpoint count bound.
    pub checkpoint_limit: usize,
    /// Logical private screen bytes currently retained.
    pub bytes: usize,
    /// Inclusive aggregate logical byte bound.
    pub byte_limit: usize,
    /// Checkpoints evicted oldest-first to admit newer client state.
    pub evictions: u64,
}

/// Thread-safe client-scoped screen checkpoint retention.
pub struct BoundedScreenCheckpointStore {
    limits: ScreenCheckpointLimits,
    next_token: AtomicU64,
    state: Mutex<CheckpointState>,
}

impl BoundedScreenCheckpointStore {
    /// Creates an empty checkpoint store without retaining private state.
    pub fn new(limits: ScreenCheckpointLimits) -> Self {
        Self {
            limits,
            next_token: AtomicU64::new(1),
            state: Mutex::new(CheckpointState::default()),
        }
    }

    /// Retains a screen baseline and returns its opaque client-scoped token.
    ///
    /// Registration evicts the owning client's oldest checkpoints until both
    /// count and byte limits admit the new screen. Payload bytes are counted
    /// logically even when immutable allocations are shared. This function
    /// performs no external IO and does not panic when the lock is poisoned.
    ///
    /// # Errors
    ///
    /// Returns client, screen-byte, or token capacity failure without retaining
    /// the supplied checkpoint.
    pub fn register(
        &self,
        client_id: ClientId,
        session_id: SessionId,
        snapshot: TerminalScreenSnapshot,
    ) -> Result<String, ScreenCheckpointStoreError> {
        let bytes = snapshot.contents().len();
        if bytes > self.limits.bytes_per_client {
            return Err(ScreenCheckpointStoreError::ScreenCapacity {
                observed: bytes,
                limit: self.limits.bytes_per_client,
            });
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !state.clients.contains_key(&client_id) && state.clients.len() == self.limits.clients {
            return Err(ScreenCheckpointStoreError::ClientCapacity {
                limit: self.limits.clients,
            });
        }
        let token = self
            .next_token
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| ScreenCheckpointStoreError::TokenExhausted)?;

        let (evicted_checkpoints, evicted_bytes) = {
            let checkpoints = state.clients.entry(client_id).or_default();
            let mut evicted_checkpoints = 0usize;
            let mut evicted_bytes = 0usize;
            while checkpoints.entries.len() >= self.limits.checkpoints_per_client
                || checkpoints.bytes.saturating_add(bytes) > self.limits.bytes_per_client
            {
                let Some(expired) = checkpoints.order.pop_front() else {
                    break;
                };
                if let Some(removed) = checkpoints.entries.remove(&expired) {
                    checkpoints.bytes = checkpoints.bytes.saturating_sub(removed.bytes);
                    evicted_checkpoints = evicted_checkpoints.saturating_add(1);
                    evicted_bytes = evicted_bytes.saturating_add(removed.bytes);
                }
            }
            checkpoints.order.push_back(token);
            checkpoints.bytes = checkpoints.bytes.saturating_add(bytes);
            checkpoints.entries.insert(
                token,
                RetainedCheckpoint {
                    checkpoint: ScreenCheckpoint {
                        session_id,
                        snapshot,
                    },
                    bytes,
                },
            );
            (evicted_checkpoints, evicted_bytes)
        };
        state.checkpoints = state
            .checkpoints
            .saturating_sub(evicted_checkpoints)
            .saturating_add(1);
        state.bytes = state
            .bytes
            .saturating_sub(evicted_bytes)
            .saturating_add(bytes);
        state.evictions = state
            .evictions
            .saturating_add(u64::try_from(evicted_checkpoints).unwrap_or(u64::MAX));
        Ok(encode_token(token))
    }

    /// Resolves one screen baseline only within its owning logical client.
    ///
    /// The returned immutable snapshot is cloned so the lock is not held while
    /// a parser computes a diff. This function performs no external IO and does
    /// not panic when the lock is poisoned.
    ///
    /// # Errors
    ///
    /// Returns malformed, absent, expired, or cross-client token failure.
    pub fn resolve(
        &self,
        client_id: &ClientId,
        token: &str,
        session_id: &SessionId,
    ) -> Result<ScreenCheckpoint, ScreenCheckpointStoreError> {
        let token = decode_token(token)?;
        let checkpoint = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clients
            .get(client_id)
            .and_then(|checkpoints| checkpoints.entries.get(&token))
            .map(|retained| retained.checkpoint.clone())
            .ok_or(ScreenCheckpointStoreError::NotFound)?;
        if checkpoint.session_id() != session_id {
            return Err(ScreenCheckpointStoreError::SessionMismatch);
        }
        Ok(checkpoint)
    }

    /// Releases every private checkpoint retained for one logical client.
    pub fn close_client(&self, client_id: &ClientId) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(removed) = state.clients.remove(client_id) {
            state.checkpoints = state.checkpoints.saturating_sub(removed.entries.len());
            state.bytes = state.bytes.saturating_sub(removed.bytes);
        }
    }

    /// Returns one lock-consistent payload-free pressure snapshot.
    pub fn pressure(&self) -> ScreenCheckpointPressure {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        ScreenCheckpointPressure {
            clients: state.clients.len(),
            client_limit: self.limits.clients,
            checkpoints: state.checkpoints,
            checkpoint_limit: self
                .limits
                .clients
                .saturating_mul(self.limits.checkpoints_per_client),
            bytes: state.bytes,
            byte_limit: self
                .limits
                .clients
                .saturating_mul(self.limits.bytes_per_client),
            evictions: state.evictions,
        }
    }
}

#[derive(Default)]
struct CheckpointState {
    clients: BTreeMap<ClientId, ClientCheckpoints>,
    checkpoints: usize,
    bytes: usize,
    evictions: u64,
}

#[derive(Default)]
struct ClientCheckpoints {
    entries: BTreeMap<u64, RetainedCheckpoint>,
    order: VecDeque<u64>,
    bytes: usize,
}

struct RetainedCheckpoint {
    checkpoint: ScreenCheckpoint,
    bytes: usize,
}

fn encode_token(token: u64) -> String {
    format!("{TOKEN_PREFIX}{token:016x}")
}

fn decode_token(value: &str) -> Result<u64, ScreenCheckpointStoreError> {
    if value.len() != TOKEN_BYTES || !value.starts_with(TOKEN_PREFIX) {
        return Err(ScreenCheckpointStoreError::InvalidToken);
    }
    u64::from_str_radix(&value[1..], 16).map_err(|_| ScreenCheckpointStoreError::InvalidToken)
}
