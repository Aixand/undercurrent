// SPDX-License-Identifier: Apache-2.0
//! Entropy-backed daemon identity and instance ownership.
use crate::process_instance::{
    PreparedProcessInstanceScope, ProcessInstanceLock, ProcessInstanceState,
    acquire_prepared_process_instance_lock, prepare_process_instance_scope, process_instance_state,
};
use std::{
    error::Error,
    fmt::{Display, Formatter},
    path::Path,
};
use undercurrent_wire::ServerEpoch;
const SERVER_EPOCH_ENTROPY_BYTES: usize = 16;
pub(super) fn generate_server_epoch() -> Result<ServerEpoch, IdentityError> {
    let mut entropy = [0_u8; SERVER_EPOCH_ENTROPY_BYTES];
    getrandom::fill(&mut entropy)
        .map_err(|error| IdentityError::Entropy(std::io::Error::other(error)))?;
    server_epoch_from_entropy(entropy).map_err(IdentityError::Invalid)
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DaemonInstanceState {
    /// No process owns the operating-system-released instance lock.
    Available,
    /// One daemon process owns the instance lock.
    Owned,
}
fn server_epoch_from_entropy(
    entropy: [u8; SERVER_EPOCH_ENTROPY_BYTES],
) -> Result<ServerEpoch, undercurrent_wire::core::IdentifierParseError> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(2 + SERVER_EPOCH_ENTROPY_BYTES * 2);
    value.push_str("e-");
    for byte in entropy {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    ServerEpoch::new(value)
}

/// Creates or validates the owner-only daemon instance-scope parent.
pub(super) fn prepare_daemon_instance_scope(
    path: &Path,
) -> Result<PreparedProcessInstanceScope, std::io::Error> {
    prepare_process_instance_scope(path)
}

/// Acquires the OS-released exclusive daemon process lock.
pub(super) fn acquire_daemon_instance_lock(
    scope: PreparedProcessInstanceScope,
) -> Result<ProcessInstanceLock, std::io::Error> {
    acquire_prepared_process_instance_lock(scope)
}

/// Probes daemon process ownership without connecting to a semantic endpoint.
///
/// A missing lock file is available. An existing file is opened without
/// creation and tested using the same exclusive advisory lock as daemon
/// composition. A successful temporary lock is released before this function
/// returns. This operation performs bounded filesystem IO and does not panic.
///
/// # Errors
///
/// Returns invalid scope structure or lock-file open/locking failure.
pub fn daemon_instance_state(path: &Path) -> Result<DaemonInstanceState, std::io::Error> {
    process_instance_state(path).map(|state| match state {
        ProcessInstanceState::Available => DaemonInstanceState::Available,
        ProcessInstanceState::Owned => DaemonInstanceState::Owned,
    })
}

#[derive(Debug)]
pub(super) enum IdentityError {
    Entropy(std::io::Error),
    Invalid(undercurrent_wire::core::IdentifierParseError),
}

impl Display for IdentityError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("could not generate daemon process identity")
    }
}

impl Error for IdentityError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Entropy(error) => Some(error),
            Self::Invalid(error) => Some(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn entropy_maps_to_a_fixed_wire_safe_epoch() {
        assert_eq!(
            server_epoch_from_entropy([0xab; SERVER_EPOCH_ENTROPY_BYTES])
                .unwrap()
                .as_str(),
            "e-abababababababababababababababab"
        );
    }
}
