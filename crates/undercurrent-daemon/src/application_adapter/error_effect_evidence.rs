// SPDX-License-Identifier: Apache-2.0

use super::*;
use undercurrent_domain::EffectCertainty;
use undercurrent_protocol::EffectCertaintyDto;

fn capacity(operation: PortOperation) -> PortError {
    PortError::new(
        PortErrorKind::CapacityExceeded,
        operation,
        None,
        std::io::Error::other("private persistence detail"),
    )
}

#[test]
fn initial_capacity_retains_category_and_only_proven_absence() {
    for known_absent in [false, true] {
        let mut error = capacity(PortOperation::InsertSession);
        if known_absent {
            error = error.with_known_absent_effect();
        }
        let ProductResponse::Error(result) = initial_session_commit(&error) else {
            panic!("error")
        };
        assert_eq!(result.operation, Some(ProductOperation::StartSession));
        assert_eq!(result.session_id, None);
        assert_eq!(result.category, ProductErrorCategory::CapacityExceeded);
        assert_eq!(
            result.disposition,
            Some(ProductErrorDisposition::CapacityRejected)
        );
        assert_eq!(
            result.effect_certainty,
            known_absent.then_some(EffectCertaintyDto::KnownAbsent)
        );
        assert!(!result.message.contains("private persistence detail"));
    }
}

#[test]
fn later_port_absence_never_becomes_whole_command_absence() {
    let error = capacity(PortOperation::CommitSession).with_known_absent_effect();
    let ProductResponse::Error(result) = initial_session_commit(&error) else {
        panic!("error")
    };
    assert_eq!(result.effect_certainty, None);
    let ProductResponse::Error(result) = application(
        ProductOperation::StopSession,
        Some("session-1".into()),
        error.category(),
        &error,
    ) else {
        panic!("error")
    };
    assert_eq!(result.effect_certainty, None);
    assert_eq!(error.effect_certainty(), Some(EffectCertainty::KnownAbsent));
}

#[test]
fn authorization_refusals_bind_absence_to_the_operation_and_target() {
    for (operation, target) in [
        (ProductOperation::StartSession, None),
        (ProductOperation::StopSession, Some("session-1".to_owned())),
        (
            ProductOperation::SendTerminalInput,
            Some("session-1".to_owned()),
        ),
    ] {
        for reason in [
            AccessDenialReason::Forbidden,
            AccessDenialReason::Unauthenticated,
        ] {
            let ProductResponse::Error(result) =
                authorization_denied(operation, target.clone(), reason)
            else {
                panic!("error")
            };
            assert_eq!(result.operation, Some(operation));
            assert_eq!(result.session_id, target);
            assert_eq!(
                result.effect_certainty,
                Some(EffectCertaintyDto::KnownAbsent)
            );
            assert_eq!(
                result.disposition,
                Some(ProductErrorDisposition::AccessRejected)
            );
            assert!(!result.retryable);
        }
    }
}

#[test]
fn response_capacity_does_not_claim_pre_execution_rejection() {
    let ProductResponse::Error(result) = response_capacity(ProductOperation::StartSession, 1)
    else {
        panic!("error")
    };
    assert_eq!(result.category, ProductErrorCategory::CapacityExceeded);
    assert_ne!(
        result.effect_certainty,
        Some(EffectCertaintyDto::KnownAbsent)
    );
}

#[test]
fn duplicate_start_names_are_refused_before_creation() {
    let ProductResponse::Error(result) =
        name_conflict(ProductOperation::StartSession, "name in use".into())
    else {
        panic!("error")
    };
    assert_eq!(result.category, ProductErrorCategory::NameConflict);
    assert_eq!(
        result.effect_certainty,
        Some(EffectCertaintyDto::KnownAbsent)
    );
    let ProductResponse::Error(other) =
        name_conflict(ProductOperation::RenameSession, "name in use".into())
    else {
        panic!("error")
    };
    assert_eq!(
        other.effect_certainty, None,
        "A different command needs its own whole-command evidence"
    );
}
