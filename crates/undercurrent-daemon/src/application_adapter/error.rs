// SPDX-License-Identifier: Apache-2.0

use std::error::Error;

use undercurrent_application::ApplicationErrorCategory;
use undercurrent_application::{AccessDenialReason, PortError, PortErrorKind, PortOperation};
use undercurrent_protocol::{
    ProductError, ProductErrorCategory, ProductErrorDisposition, ProductOperation, ProductResponse,
    ResynchronizationDto,
};

use crate::{
    CursorStoreError, DaemonOperationsError, ProductBoundaryError, ProductPresentationError,
    ScreenCheckpointStoreError,
};

pub(super) fn cancelled(operation: ProductOperation) -> ProductResponse {
    response(ProductError {
        effect_certainty: None,
        operation: Some(operation),
        category: ProductErrorCategory::Cancelled,
        disposition: Some(ProductErrorDisposition::Cancellation),
        message: "request was cancelled".to_owned(),
        session_id: None,
        retryable: false,
        retry_after_millis: None,
        limit: None,
        resynchronization: None,
    })
}

pub(super) fn reset_cancelled(operation: ProductOperation) -> ProductResponse {
    response(ProductError {
        effect_certainty: Some(undercurrent_protocol::EffectCertaintyDto::Unknown),
        operation: Some(operation),
        category: ProductErrorCategory::Cancelled,
        disposition: Some(ProductErrorDisposition::UnknownInconclusive),
        message: "request interrupted by daemon shutdown; effects may already have occurred"
            .to_owned(),
        session_id: None,
        retryable: false,
        retry_after_millis: None,
        limit: None,
        resynchronization: None,
    })
}

pub(super) fn boundary(
    operation: ProductOperation,
    session_id: Option<String>,
    error: ProductBoundaryError,
) -> ProductResponse {
    response(error.into_product_error(operation, session_id))
}

pub(super) fn application(
    operation: ProductOperation,
    session_id: Option<String>,
    category: ApplicationErrorCategory,
    error: &(dyn Error + 'static),
) -> ProductResponse {
    application_with_optional_disposition(operation, session_id, category, None, error)
}

/// Initial session persistence precedes process activation and attempt admission.
/// Only the repository's explicit refusal evidence can establish whole-command
/// absence here; a generic persistence error may follow an unobserved commit.
pub(super) fn initial_session_commit(error: &PortError) -> ProductResponse {
    let mut result = application(
        ProductOperation::StartSession,
        None,
        error.category(),
        error,
    );
    if error.operation() == PortOperation::InsertSession
        && error.effect_certainty() == Some(undercurrent_domain::EffectCertainty::KnownAbsent)
        && let ProductResponse::Error(response) = &mut result
    {
        response.effect_certainty = Some(undercurrent_protocol::EffectCertaintyDto::KnownAbsent);
    }
    result
}

pub(super) fn application_with_disposition(
    operation: ProductOperation,
    session_id: Option<String>,
    category: ApplicationErrorCategory,
    disposition: ProductErrorDisposition,
    error: &(dyn Error + 'static),
) -> ProductResponse {
    application_with_optional_disposition(operation, session_id, category, Some(disposition), error)
}

fn application_with_optional_disposition(
    operation: ProductOperation,
    session_id: Option<String>,
    category: ApplicationErrorCategory,
    disposition: Option<ProductErrorDisposition>,
    error: &(dyn Error + 'static),
) -> ProductResponse {
    let port = find_port_error(error);
    if let Some(recorder) = undercurrent_diagnostics::process() {
        let mut record = recorder.record("application", category.as_str(), Some(error));
        record.operation = Some(operation.as_str().into());
        // Port-level evidence is deliberately not promoted to whole-command
        // effect certainty. The existing operation-specific response owns that.
        let _ = recorder.save(&record);
    }
    let effect_certainty = port.and_then(PortError::effect_certainty);
    let semantic_attempt_rejection = port.is_some_and(|error| {
        error.operation() == PortOperation::AcceptOperationAttempt
            && error.kind() == PortErrorKind::Conflict
            && effect_certainty == Some(undercurrent_domain::EffectCertainty::KnownAbsent)
    });
    let public_category = if semantic_attempt_rejection {
        ProductErrorCategory::FailedPrecondition
    } else {
        port.and_then(port_category)
            .unwrap_or_else(|| product_category(category))
    };
    let public_disposition =
        disposition.unwrap_or_else(|| match (semantic_attempt_rejection, effect_certainty) {
            (true, _) => ProductErrorDisposition::SemanticRejection,
            (false, Some(undercurrent_domain::EffectCertainty::Unknown)) => {
                ProductErrorDisposition::UnknownInconclusive
            }
            (false, _) => ProductErrorDisposition::default_for_category(public_category),
        });
    let internal = matches!(
        category,
        ApplicationErrorCategory::InvariantViolation | ApplicationErrorCategory::Internal
    );
    response(ProductError {
        // A nested port's absence evidence covers only that step. Earlier
        // command effects may already have committed, so whole-command evidence
        // must come from an operation-specific mapper such as initial_session_commit.
        // See [effect evidence](../../docs/internals/requests/effects-and-recovery.md#classify-the-evidence-before-retrying).
        effect_certainty: None,
        operation: Some(operation),
        category: public_category,
        disposition: Some(public_disposition),
        message: if let Some(port) = port {
            port.to_string()
        } else if internal {
            "operation failed internally".to_owned()
        } else {
            error.to_string()
        },
        session_id,
        retryable: matches!(
            category,
            ApplicationErrorCategory::RateLimited
                | ApplicationErrorCategory::Timeout
                | ApplicationErrorCategory::Interrupted
                | ApplicationErrorCategory::Unavailable
        ),
        retry_after_millis: None,
        limit: None,
        resynchronization: None,
    })
}

pub(super) fn name_conflict(operation: ProductOperation, message: String) -> ProductResponse {
    response(ProductError {
        effect_certainty: (operation == ProductOperation::StartSession)
            .then_some(undercurrent_protocol::EffectCertaintyDto::KnownAbsent),
        operation: Some(operation),
        category: ProductErrorCategory::NameConflict,
        disposition: Some(ProductErrorDisposition::SemanticRejection),
        message,
        session_id: None,
        retryable: false,
        retry_after_millis: None,
        limit: None,
        resynchronization: None,
    })
}

/// This category is reserved for a precondition checked before any mutation.
/// Commit/settlement conflicts after effects must retain their own uncertainty.
pub(super) fn revision_precondition_failed(
    operation: ProductOperation,
    session_id: Option<String>,
) -> ProductResponse {
    response(ProductError {
        effect_certainty: Some(undercurrent_protocol::EffectCertaintyDto::KnownAbsent),
        operation: Some(operation),
        category: ProductErrorCategory::RevisionPreconditionFailed,
        disposition: Some(ProductErrorDisposition::SemanticRejection),
        message: "session revision changed before mutation; inspect and review the current session"
            .to_owned(),
        session_id,
        retryable: false,
        retry_after_millis: None,
        limit: None,
        resynchronization: None,
    })
}

pub(super) fn authorization_denied(
    operation: ProductOperation,
    session_id: Option<String>,
    reason: AccessDenialReason,
) -> ProductResponse {
    let (category, message) = match reason {
        AccessDenialReason::Unauthenticated => (
            ProductErrorCategory::Unauthenticated,
            "authentication is required for this operation",
        ),
        AccessDenialReason::Forbidden => (
            ProductErrorCategory::PermissionDenied,
            "permission denied for this operation",
        ),
    };
    response(ProductError {
        effect_certainty: Some(undercurrent_protocol::EffectCertaintyDto::KnownAbsent),
        operation: Some(operation),
        category,
        disposition: Some(ProductErrorDisposition::default_for_category(category)),
        message: message.to_owned(),
        session_id,
        retryable: false,
        retry_after_millis: None,
        limit: None,
        resynchronization: None,
    })
}

pub(super) fn authorization_failure(
    operation: ProductOperation,
    session_id: Option<String>,
    error: &PortError,
) -> ProductResponse {
    application(operation, session_id, error.category(), error)
}

pub(super) fn profile_unavailable(
    operation: ProductOperation,
    session_id: Option<String>,
    profile: &str,
) -> ProductResponse {
    response(ProductError {
        effect_certainty: Some(undercurrent_protocol::EffectCertaintyDto::KnownAbsent),
        operation: Some(operation),
        category: ProductErrorCategory::FailedPrecondition,
        disposition: Some(ProductErrorDisposition::SemanticRejection),
        message: format!("detection profile {profile} is unavailable; restore its YAML definition"),
        session_id,
        retryable: false,
        retry_after_millis: None,
        limit: None,
        resynchronization: None,
    })
}

pub(super) fn invalid_configuration(operation: ProductOperation) -> ProductResponse {
    response(ProductError {
        effect_certainty: None,
        operation: Some(operation),
        category: ProductErrorCategory::InvalidConfiguration,
        disposition: Some(ProductErrorDisposition::SemanticRejection),
        message: "operation limits are invalid".to_owned(),
        session_id: None,
        retryable: false,
        retry_after_millis: None,
        limit: None,
        resynchronization: None,
    })
}

pub(super) fn invalid_configuration_with_source(
    operation: ProductOperation,
    _source: &(dyn Error + 'static),
) -> ProductResponse {
    invalid_configuration(operation)
}

pub(super) fn cursor(operation: ProductOperation, error: CursorStoreError) -> ProductResponse {
    let (category, retryable, limit, recovery) = match error {
        CursorStoreError::InvalidToken => (ProductErrorCategory::InvalidRequest, false, None, None),
        CursorStoreError::NotFound => (
            ProductErrorCategory::CursorExpired,
            false,
            None,
            Some(ResynchronizationDto::RestartQuery),
        ),
        CursorStoreError::ClientCapacity { limit } => (
            ProductErrorCategory::CapacityExceeded,
            true,
            Some(u64::try_from(limit).unwrap_or(u64::MAX)),
            None,
        ),
        CursorStoreError::InvalidLimits { .. } | CursorStoreError::TokenExhausted => {
            (ProductErrorCategory::Internal, false, None, None)
        }
    };
    response(ProductError {
        effect_certainty: None,
        operation: Some(operation),
        category,
        disposition: Some(ProductErrorDisposition::default_for_category(category)),
        message: if category == ProductErrorCategory::Internal {
            "cursor operation failed internally".to_owned()
        } else {
            error.to_string()
        },
        session_id: None,
        retryable,
        retry_after_millis: None,
        limit,
        resynchronization: recovery,
    })
}

pub(super) fn screen_checkpoint(
    operation: ProductOperation,
    error: ScreenCheckpointStoreError,
) -> ProductResponse {
    let (category, retryable, limit, recovery) = match error {
        ScreenCheckpointStoreError::InvalidToken | ScreenCheckpointStoreError::SessionMismatch => {
            (ProductErrorCategory::InvalidRequest, false, None, None)
        }
        ScreenCheckpointStoreError::NotFound => (
            ProductErrorCategory::CursorExpired,
            false,
            None,
            Some(ResynchronizationDto::RequestSnapshot),
        ),
        ScreenCheckpointStoreError::ClientCapacity { limit } => (
            ProductErrorCategory::CapacityExceeded,
            true,
            Some(u64::try_from(limit).unwrap_or(u64::MAX)),
            None,
        ),
        ScreenCheckpointStoreError::ScreenCapacity { limit, .. } => (
            ProductErrorCategory::CapacityExceeded,
            false,
            Some(u64::try_from(limit).unwrap_or(u64::MAX)),
            None,
        ),
        ScreenCheckpointStoreError::InvalidLimits { .. }
        | ScreenCheckpointStoreError::TokenExhausted => {
            (ProductErrorCategory::Internal, false, None, None)
        }
    };
    response(ProductError {
        effect_certainty: None,
        operation: Some(operation),
        category,
        disposition: Some(ProductErrorDisposition::default_for_category(category)),
        message: if category == ProductErrorCategory::Internal {
            "screen checkpoint operation failed internally".to_owned()
        } else {
            error.to_string()
        },
        session_id: None,
        retryable,
        retry_after_millis: None,
        limit,
        resynchronization: recovery,
    })
}

pub(super) fn presentation(
    operation: ProductOperation,
    _error: ProductPresentationError,
) -> ProductResponse {
    response(ProductError {
        effect_certainty: None,
        operation: Some(operation),
        category: ProductErrorCategory::Internal,
        disposition: Some(ProductErrorDisposition::InfrastructureFailure),
        message: "operation result could not be represented".to_owned(),
        session_id: None,
        retryable: false,
        retry_after_millis: None,
        limit: None,
        resynchronization: None,
    })
}

pub(super) fn result_encoding(operation: ProductOperation) -> ProductResponse {
    response(ProductError {
        effect_certainty: None,
        operation: Some(operation),
        category: ProductErrorCategory::Internal,
        disposition: Some(ProductErrorDisposition::InfrastructureFailure),
        message: "operation result could not be encoded".to_owned(),
        session_id: None,
        retryable: false,
        retry_after_millis: None,
        limit: None,
        resynchronization: None,
    })
}

pub(super) fn response_capacity(operation: ProductOperation, limit: usize) -> ProductResponse {
    response(ProductError {
        effect_certainty: None,
        operation: Some(operation),
        category: ProductErrorCategory::CapacityExceeded,
        disposition: Some(ProductErrorDisposition::CapacityRejected),
        message: "one result row exceeds the response byte limit".to_owned(),
        session_id: None,
        retryable: false,
        retry_after_millis: None,
        limit: Some(u64::try_from(limit).unwrap_or(u64::MAX)),
        resynchronization: None,
    })
}

pub(super) fn daemon_operations(
    operation: ProductOperation,
    error: DaemonOperationsError,
) -> ProductResponse {
    let (category, retryable, limit, resynchronization) = match error {
        DaemonOperationsError::InvalidPageSize { maximum, .. } => (
            ProductErrorCategory::InvalidRequest,
            false,
            Some(u64::try_from(maximum).unwrap_or(u64::MAX)),
            None,
        ),
        DaemonOperationsError::InvalidDrainTimeout { maximum, .. } => (
            ProductErrorCategory::InvalidRequest,
            false,
            Some(u64::try_from(maximum.as_millis()).unwrap_or(u64::MAX)),
            None,
        ),
        DaemonOperationsError::InvalidCursor => {
            (ProductErrorCategory::InvalidRequest, false, None, None)
        }
        DaemonOperationsError::TerminalContentRequired => {
            (ProductErrorCategory::InvalidRequest, false, None, None)
        }
        DaemonOperationsError::CursorExpired => (
            ProductErrorCategory::CursorExpired,
            false,
            None,
            Some(ResynchronizationDto::RestartQuery),
        ),
        DaemonOperationsError::ClientCapacity { limit }
        | DaemonOperationsError::SnapshotCapacity { limit } => (
            ProductErrorCategory::CapacityExceeded,
            true,
            Some(u64::try_from(limit).unwrap_or(u64::MAX)),
            None,
        ),
        DaemonOperationsError::TerminalContentCapacity { limit } => (
            ProductErrorCategory::CapacityExceeded,
            false,
            Some(u64::try_from(limit).unwrap_or(u64::MAX)),
            None,
        ),
        DaemonOperationsError::SourceUnavailable => {
            (ProductErrorCategory::Internal, true, None, None)
        }
        DaemonOperationsError::Draining => {
            (ProductErrorCategory::FailedPrecondition, false, None, None)
        }
        DaemonOperationsError::InvalidLimit { .. }
        | DaemonOperationsError::TokenExhausted
        | DaemonOperationsError::SourceInvariant => {
            (ProductErrorCategory::Internal, false, None, None)
        }
    };
    let internal = category == ProductErrorCategory::Internal;
    response(ProductError {
        effect_certainty: None,
        operation: Some(operation),
        category,
        disposition: Some(ProductErrorDisposition::default_for_category(category)),
        message: if internal {
            "daemon operation failed internally".to_owned()
        } else {
            error.to_string()
        },
        session_id: None,
        retryable,
        retry_after_millis: None,
        limit,
        resynchronization,
    })
}

pub(super) const fn product_category(category: ApplicationErrorCategory) -> ProductErrorCategory {
    match category {
        ApplicationErrorCategory::InvalidArgument => ProductErrorCategory::InvalidRequest,
        ApplicationErrorCategory::Unauthenticated => ProductErrorCategory::Unauthenticated,
        ApplicationErrorCategory::PermissionDenied => ProductErrorCategory::PermissionDenied,
        ApplicationErrorCategory::NotFound => ProductErrorCategory::NotFound,
        ApplicationErrorCategory::FailedPrecondition | ApplicationErrorCategory::Conflict => {
            ProductErrorCategory::FailedPrecondition
        }
        ApplicationErrorCategory::CapacityExceeded | ApplicationErrorCategory::RateLimited => {
            ProductErrorCategory::CapacityExceeded
        }
        ApplicationErrorCategory::Cancelled => ProductErrorCategory::Cancelled,
        ApplicationErrorCategory::Timeout => ProductErrorCategory::TimedOut,
        ApplicationErrorCategory::ReplayUnavailable => ProductErrorCategory::ReplayUnavailable,
        ApplicationErrorCategory::CursorExpired => ProductErrorCategory::CursorExpired,
        ApplicationErrorCategory::ProtocolViolation | ApplicationErrorCategory::Unsupported => {
            ProductErrorCategory::ProtocolMismatch
        }
        ApplicationErrorCategory::Interrupted
        | ApplicationErrorCategory::Unavailable
        | ApplicationErrorCategory::InvariantViolation
        | ApplicationErrorCategory::Internal => ProductErrorCategory::Internal,
    }
}

pub(super) fn product_category_for_error(
    category: ApplicationErrorCategory,
    error: &(dyn Error + 'static),
) -> ProductErrorCategory {
    find_port_error(error)
        .and_then(port_category)
        .unwrap_or_else(|| product_category(category))
}

const fn port_category(error: &PortError) -> Option<ProductErrorCategory> {
    match (error.operation(), error.kind()) {
        (_, PortErrorKind::Cancelled) => Some(ProductErrorCategory::Cancelled),
        (_, PortErrorKind::Timeout) => Some(ProductErrorCategory::TimedOut),
        (PortOperation::PrepareLaunch, PortErrorKind::InvalidData) => {
            Some(ProductErrorCategory::InvalidConfiguration)
        }
        (
            PortOperation::PrepareLaunch | PortOperation::InsertSession,
            PortErrorKind::CapacityExceeded,
        ) => Some(ProductErrorCategory::CapacityExceeded),
        (PortOperation::PrepareLaunch, PortErrorKind::Unsupported) => {
            Some(ProductErrorCategory::ProtocolMismatch)
        }
        _ => infrastructure_category(error.operation()),
    }
}

fn find_port_error<'a>(error: &'a (dyn Error + 'static)) -> Option<&'a PortError> {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(port) = error.downcast_ref::<PortError>() {
            return Some(port);
        }
        current = error.source();
    }
    None
}

const fn infrastructure_category(operation: PortOperation) -> Option<ProductErrorCategory> {
    match operation {
        PortOperation::PrepareLaunch
        | PortOperation::SpawnProcess
        | PortOperation::ArmProcessExit
        | PortOperation::StopProcess
        | PortOperation::ForceProcess
        | PortOperation::ReadProcessExit
        | PortOperation::InspectProcessRuntime
        | PortOperation::CollectForegroundEvidence
        | PortOperation::RunHealthChecks
        | PortOperation::BindProcess
        | PortOperation::LookupProcess
        | PortOperation::UnbindProcess
        | PortOperation::ObserveProcessTruth
        | PortOperation::ObserveForegroundProcessGroup
        | PortOperation::ResolveExecutionContext
        | PortOperation::ResolveSecretEnvironment
        | PortOperation::ExecuteInSession
        | PortOperation::ShutdownProcessHost
        | PortOperation::ObserveOperationAttemptEffect => Some(ProductErrorCategory::Process),
        PortOperation::WriteInput
        | PortOperation::CloseInput
        | PortOperation::ResizeTerminal
        | PortOperation::ReadTerminalOutput
        | PortOperation::ReadTerminalScreen
        | PortOperation::DiffTerminalScreen
        | PortOperation::RetainTerminalOutput => Some(ProductErrorCategory::Terminal),
        PortOperation::InsertSession
        | PortOperation::LoadSession
        | PortOperation::FindSessionByName
        | PortOperation::CommitSession
        | PortOperation::DeleteSession
        | PortOperation::ReserveInputDelivery
        | PortOperation::SettleInputDelivery
        | PortOperation::ReadOutbox
        | PortOperation::AcknowledgeOutbox
        | PortOperation::PublishSessionEvent
        | PortOperation::UpdateProjection
        | PortOperation::ScheduleRestart
        | PortOperation::ClaimDueRestarts
        | PortOperation::CancelSessionRestarts
        | PortOperation::ScheduleObservation
        | PortOperation::ClaimDueObservations
        | PortOperation::CancelSessionObservations
        | PortOperation::ReconcileSessionObservations
        | PortOperation::ReadRecoverySessions
        | PortOperation::InspectPersistence
        | PortOperation::ReadDetectionProfiles
        | PortOperation::ReadSessionEvents
        | PortOperation::SubscribeSessionEvents
        | PortOperation::AcceptOperationAttempt
        | PortOperation::SettleOperationAttempt
        | PortOperation::PrepareOperationAttemptCompensation
        | PortOperation::CompensateOperationAttempt
        | PortOperation::ReconcileOperationAttempt
        | PortOperation::LoadOperationAttempt
        | PortOperation::ReadPendingOperationAttempts
        | PortOperation::RepairOperationAttempt
        | PortOperation::InspectOperationAttemptReadiness
        | PortOperation::GetRuntimeAttemptStatus
        | PortOperation::AdmitRuntimeCancellation
        | PortOperation::MarkRuntimeCancellationEffectStarted
        | PortOperation::SettleRuntimeCancellation
        | PortOperation::GetRuntimeCancellationStatus
        | PortOperation::EnqueueAgentEvent
        | PortOperation::PollAgentEvent
        | PortOperation::ReplyAgentEvent
        | PortOperation::ReadAgentEventReply
        | PortOperation::ResolveAgentChannelTarget => Some(ProductErrorCategory::Persistence),
        PortOperation::StoreExecutionOutput => Some(ProductErrorCategory::Persistence),
        PortOperation::Authorize
        | PortOperation::GenerateSessionId
        | PortOperation::AcquireSessionMutation
        | PortOperation::ReadEffectiveProfile
        | PortOperation::UpdateEffectiveProfile
        | PortOperation::EvaluateProfilePattern => None,
    }
}

fn response(error: ProductError) -> ProductResponse {
    ProductResponse::Error(error)
}

#[cfg(test)]
mod tests {
    use std::io;

    use undercurrent_application::{CancelRuntimeAttemptError, PortErrorKind, PortOperation};

    use super::*;

    #[test]
    fn infrastructure_errors_preserve_typed_public_categories_without_source_detail() {
        for (port_operation, expected) in [
            (PortOperation::SpawnProcess, ProductErrorCategory::Process),
            (PortOperation::WriteInput, ProductErrorCategory::Terminal),
            (
                PortOperation::CommitSession,
                ProductErrorCategory::Persistence,
            ),
        ] {
            let error = PortError::new(
                PortErrorKind::Unavailable,
                port_operation,
                None,
                io::Error::other("private adapter detail"),
            );
            let ProductResponse::Error(response) = application(
                ProductOperation::StartSession,
                None,
                error.category(),
                &error,
            ) else {
                panic!("expected product error");
            };
            assert_eq!(response.category, expected);
            assert_eq!(
                response.disposition,
                Some(ProductErrorDisposition::default_for_category(expected))
            );
            assert!(response.retryable);
            assert!(!response.message.contains("private adapter detail"));
            assert!(response.message.contains(port_operation.as_str()));
        }
    }

    #[test]
    fn non_infrastructure_port_errors_keep_their_decision_category() {
        let error = PortError::new(
            PortErrorKind::PermissionDenied,
            PortOperation::Authorize,
            None,
            io::Error::other("private policy detail"),
        );
        assert_eq!(
            product_category_for_error(error.category(), &error),
            ProductErrorCategory::PermissionDenied
        );
    }

    #[test]
    fn launch_preparation_rejections_are_not_reported_as_runtime_failures() {
        for (kind, expected) in [
            (
                PortErrorKind::InvalidData,
                ProductErrorCategory::InvalidConfiguration,
            ),
            (
                PortErrorKind::CapacityExceeded,
                ProductErrorCategory::CapacityExceeded,
            ),
            (PortErrorKind::Unavailable, ProductErrorCategory::Process),
            (
                PortErrorKind::Unsupported,
                ProductErrorCategory::ProtocolMismatch,
            ),
        ] {
            let error = PortError::new(
                kind,
                PortOperation::PrepareLaunch,
                None,
                io::Error::other("private preparation detail"),
            );

            assert_eq!(
                product_category_for_error(error.category(), &error),
                expected
            );
        }
    }

    #[test]
    fn known_absent_attempt_admission_is_a_semantic_rejection() {
        let error = PortError::new(
            PortErrorKind::Conflict,
            PortOperation::AcceptOperationAttempt,
            None,
            io::Error::other("private attempt detail"),
        )
        .with_attempt_effect(
            undercurrent_domain::OperationAttemptId::new("expired-dispatch").expect("attempt ID"),
            undercurrent_domain::EffectCertainty::KnownAbsent,
        );

        let ProductResponse::Error(response) = application(
            ProductOperation::StartSession,
            None,
            error.category(),
            &error,
        ) else {
            panic!("expected product error");
        };
        assert_eq!(response.category, ProductErrorCategory::FailedPrecondition);
        assert_eq!(
            response.disposition,
            Some(ProductErrorDisposition::SemanticRejection)
        );
        assert!(!response.retryable);
        assert!(!response.message.contains("private attempt detail"));
    }

    #[test]
    fn pre_admission_cancellation_deadline_is_known_absent() {
        let source = CancelRuntimeAttemptError::DeadlineExpired;
        let ProductResponse::Error(response) = application(
            ProductOperation::CancelRuntimeAttempt,
            Some("session-1".to_owned()),
            source.category(),
            &source,
        ) else {
            panic!("expected product error");
        };

        assert_eq!(response.category, ProductErrorCategory::FailedPrecondition);
        assert_eq!(
            response.disposition,
            Some(ProductErrorDisposition::SemanticRejection)
        );
        assert!(!response.retryable);
    }

    #[test]
    fn known_absent_runtime_and_cancellation_errors_keep_their_typed_classification() {
        for (operation, kind, expected_category, expected_disposition) in [
            (
                PortOperation::SpawnProcess,
                PortErrorKind::Unavailable,
                ProductErrorCategory::Process,
                ProductErrorDisposition::RuntimeFailure,
            ),
            (
                PortOperation::ExecuteInSession,
                PortErrorKind::Cancelled,
                ProductErrorCategory::Cancelled,
                ProductErrorDisposition::Cancellation,
            ),
        ] {
            let error = PortError::new(
                kind,
                operation,
                None,
                io::Error::other("private typed failure"),
            )
            .with_attempt_effect(
                undercurrent_domain::OperationAttemptId::new("known-absent-non-admission")
                    .expect("attempt ID"),
                undercurrent_domain::EffectCertainty::KnownAbsent,
            );
            let ProductResponse::Error(response) = application(
                ProductOperation::StartSession,
                None,
                error.category(),
                &error,
            ) else {
                panic!("expected product error");
            };
            assert_eq!(response.category, expected_category);
            assert_eq!(response.disposition, Some(expected_disposition));
        }
    }
}

#[cfg(test)]
#[path = "error_effect_evidence.rs"]
mod effect_evidence_tests;
