// SPDX-License-Identifier: Apache-2.0

//! Ephemeral secret resolution at the application adapter boundary.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

use undercurrent_domain::{SecretEnvironmentVariables, SecretReference, SessionId};

use crate::{PortError, PortErrorKind, PortFuture, PortOperation};

/// Ephemeral resolved material that redacts diagnostics and zeroes on drop.
pub struct EphemeralSecret(Vec<u8>);

impl EphemeralSecret {
    /// Takes ownership of bytes returned by an authorized adapter.
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
    /// Returns byte length without exposure.
    pub fn len(&self) -> usize {
        self.0.len()
    }
    /// Returns whether material is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Exposes bytes only for an explicit adapter callback.
    pub fn expose<R>(&self, use_secret: impl FnOnce(&[u8]) -> R) -> R {
        use_secret(&self.0)
    }
}

impl Debug for EphemeralSecret {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EphemeralSecret")
            .field("byte_len", &self.0.len())
            .field("redacted", &true)
            .finish()
    }
}

impl Drop for EphemeralSecret {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

/// Resolved environment overlay with no serialization or value-bearing debug.
pub struct ResolvedSecretEnvironment(BTreeMap<String, EphemeralSecret>);

impl ResolvedSecretEnvironment {
    /// Constructs an ephemeral resolved overlay.
    pub fn new(values: BTreeMap<String, EphemeralSecret>) -> Self {
        Self(values)
    }
    /// Returns one ephemeral value.
    pub fn get(&self, name: &str) -> Option<&EphemeralSecret> {
        self.0.get(name)
    }
    /// Returns entry count.
    pub fn len(&self) -> usize {
        self.0.len()
    }
    /// Returns whether no values are present.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterates ephemeral values for an explicitly authorized runtime adapter.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &EphemeralSecret)> {
        self.0.iter().map(|(name, secret)| (name.as_str(), secret))
    }
}

impl Debug for ResolvedSecretEnvironment {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResolvedSecretEnvironment")
            .field("entry_count", &self.0.len())
            .finish_non_exhaustive()
    }
}

/// Resolves one durable handle into short-lived secret bytes.
pub trait SecretResolver: Send + Sync {
    /// Resolves without exposing material in errors or diagnostics.
    fn resolve<'a>(
        &'a self,
        reference: &'a SecretReference,
    ) -> PortFuture<'a, Result<EphemeralSecret, PortError>>;
}

/// Fail-closed resolver used when no authorized credential adapter is composed.
#[derive(Debug, Default, Clone, Copy)]
pub struct UnavailableSecretResolver;

impl SecretResolver for UnavailableSecretResolver {
    fn resolve<'a>(
        &'a self,
        _reference: &'a SecretReference,
    ) -> PortFuture<'a, Result<EphemeralSecret, PortError>> {
        Box::pin(async {
            Err(PortError::new(
                PortErrorKind::Unsupported,
                PortOperation::ResolveSecretEnvironment,
                None,
                SecretResolutionFailure::AdapterUnavailable,
            ))
        })
    }
}

/// Resolves every durable handle into one bounded ephemeral child overlay.
///
/// Resolution is intentionally deferred until the runtime spawn edge. Raw
/// bytes are rejected when empty, NUL-bearing, or over the configured process
/// environment bound. Resolver errors must themselves be payload-safe and are
/// returned without interpolating handles or material.
pub async fn resolve_secret_environment(
    resolver: &dyn SecretResolver,
    references: &SecretEnvironmentVariables,
    session_id: &SessionId,
    maximum_entries: usize,
    maximum_bytes: usize,
) -> Result<ResolvedSecretEnvironment, PortError> {
    if references.len() > maximum_entries {
        return Err(secret_failure(
            PortErrorKind::CapacityExceeded,
            session_id,
            SecretResolutionFailure::EntryCapacity,
        ));
    }
    let mut total_bytes = 0usize;
    let mut values = BTreeMap::new();
    for (name, reference) in references.iter() {
        let secret = resolver.resolve(reference).await.map_err(|error| {
            let kind = error.kind();
            // PortError redacts its own formatting but still exposes its source
            // chain. Discard that chain so a resolver's secret-bearing error
            // cannot escape through recursive error reporting.
            // See ../docs/internals/execution/commands-and-secrets.md#resolve-secrets-at-the-spawn-edge.
            drop(error);
            secret_failure(kind, session_id, SecretResolutionFailure::AdapterFailure)
        })?;
        if secret.is_empty() {
            return Err(secret_failure(
                PortErrorKind::InvalidData,
                session_id,
                SecretResolutionFailure::EmptyMaterial,
            ));
        }
        let contains_nul = secret.expose(|bytes| bytes.contains(&0));
        if contains_nul {
            return Err(secret_failure(
                PortErrorKind::InvalidData,
                session_id,
                SecretResolutionFailure::NulMaterial,
            ));
        }
        total_bytes = total_bytes
            .checked_add(name.len())
            .and_then(|total| total.checked_add(secret.len()))
            .ok_or_else(|| {
                secret_failure(
                    PortErrorKind::CapacityExceeded,
                    session_id,
                    SecretResolutionFailure::ByteCapacity,
                )
            })?;
        if total_bytes > maximum_bytes {
            return Err(secret_failure(
                PortErrorKind::CapacityExceeded,
                session_id,
                SecretResolutionFailure::ByteCapacity,
            ));
        }
        values.insert(name.to_owned(), secret);
    }
    Ok(ResolvedSecretEnvironment::new(values))
}

fn secret_failure(
    kind: PortErrorKind,
    session_id: &SessionId,
    source: SecretResolutionFailure,
) -> PortError {
    PortError::new(
        kind,
        PortOperation::ResolveSecretEnvironment,
        Some(session_id.clone()),
        source,
    )
}

#[derive(Debug, Clone, Copy)]
enum SecretResolutionFailure {
    AdapterUnavailable,
    AdapterFailure,
    EmptyMaterial,
    NulMaterial,
    EntryCapacity,
    ByteCapacity,
}

impl Display for SecretResolutionFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::AdapterUnavailable => "no authorized secret resolver is configured",
            Self::AdapterFailure => "authorized secret resolution failed",
            Self::EmptyMaterial => "secret resolver returned empty material",
            Self::NulMaterial => "secret resolver returned process-incompatible material",
            Self::EntryCapacity => "resolved secret environment exceeds its entry limit",
            Self::ByteCapacity => "resolved secret environment exceeds its byte limit",
        })
    }
}

impl Error for SecretResolutionFailure {}
