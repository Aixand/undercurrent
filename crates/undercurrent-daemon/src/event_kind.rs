// SPDX-License-Identifier: Apache-2.0

//! Exhaustive translation between domain and public session-event registries.

use undercurrent_domain::DomainEventKind;
use undercurrent_protocol::SessionEventKindDto;

/// Translates one public protocol event into the domain registry.
///
/// This function performs no IO and does not panic.
pub(crate) const fn to_domain(kind: SessionEventKindDto) -> DomainEventKind {
    match kind {
        SessionEventKindDto::SessionCreated => DomainEventKind::SessionCreated,
        SessionEventKindDto::SessionRenamed => DomainEventKind::SessionRenamed,
        SessionEventKindDto::SessionConfigurationChanged => {
            DomainEventKind::SessionConfigurationChanged
        }
        SessionEventKindDto::SessionRemoved => DomainEventKind::SessionRemoved,
        SessionEventKindDto::LifecycleChanged => DomainEventKind::LifecycleChanged,
        SessionEventKindDto::ProcessOwnershipChanged => DomainEventKind::ProcessOwnershipChanged,
        SessionEventKindDto::ForegroundChanged => DomainEventKind::ForegroundChanged,
        SessionEventKindDto::ForegroundEvidenceChanged => {
            DomainEventKind::ForegroundEvidenceChanged
        }
        SessionEventKindDto::HealthChanged => DomainEventKind::HealthChanged,
        SessionEventKindDto::HealthEvidenceChanged => DomainEventKind::HealthEvidenceChanged,
        SessionEventKindDto::WorkContextChanged => DomainEventKind::WorkContextChanged,
        SessionEventKindDto::RestartScheduled => DomainEventKind::RestartScheduled,
        SessionEventKindDto::RestartCancelled => DomainEventKind::RestartCancelled,
        SessionEventKindDto::RestartStateChanged => DomainEventKind::RestartStateChanged,
        SessionEventKindDto::RestartExhausted => DomainEventKind::RestartExhausted,
        SessionEventKindDto::TerminalResized => DomainEventKind::TerminalResized,
    }
}

/// Translates one domain event into the public protocol registry.
///
/// This function performs no IO and does not panic.
pub(crate) const fn from_domain(kind: DomainEventKind) -> SessionEventKindDto {
    match kind {
        DomainEventKind::SessionCreated => SessionEventKindDto::SessionCreated,
        DomainEventKind::SessionRenamed => SessionEventKindDto::SessionRenamed,
        DomainEventKind::SessionConfigurationChanged => {
            SessionEventKindDto::SessionConfigurationChanged
        }
        DomainEventKind::SessionRemoved => SessionEventKindDto::SessionRemoved,
        DomainEventKind::LifecycleChanged => SessionEventKindDto::LifecycleChanged,
        DomainEventKind::ProcessOwnershipChanged => SessionEventKindDto::ProcessOwnershipChanged,
        DomainEventKind::ForegroundChanged => SessionEventKindDto::ForegroundChanged,
        DomainEventKind::ForegroundEvidenceChanged => {
            SessionEventKindDto::ForegroundEvidenceChanged
        }
        DomainEventKind::HealthChanged => SessionEventKindDto::HealthChanged,
        DomainEventKind::HealthEvidenceChanged => SessionEventKindDto::HealthEvidenceChanged,
        DomainEventKind::WorkContextChanged => SessionEventKindDto::WorkContextChanged,
        DomainEventKind::RestartScheduled => SessionEventKindDto::RestartScheduled,
        DomainEventKind::RestartCancelled => SessionEventKindDto::RestartCancelled,
        DomainEventKind::RestartStateChanged => SessionEventKindDto::RestartStateChanged,
        DomainEventKind::RestartExhausted => SessionEventKindDto::RestartExhausted,
        DomainEventKind::TerminalResized => SessionEventKindDto::TerminalResized,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_protocol_and_domain_event_round_trips() {
        assert_eq!(SessionEventKindDto::ALL.len(), DomainEventKind::ALL.len());
        for protocol in SessionEventKindDto::ALL {
            assert_eq!(from_domain(to_domain(protocol)), protocol);
        }
        for domain in DomainEventKind::ALL {
            assert_eq!(to_domain(from_domain(domain)), domain);
        }
    }
}
