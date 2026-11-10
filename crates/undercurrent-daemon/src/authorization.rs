// SPDX-License-Identifier: Apache-2.0

//! Application authorization seam behind authenticated Local admission.

use undercurrent_application::{
    AccessAction, AccessDecision, AccessResource, AuthorizationPort, OperationAttemptRepairActor,
    OperationAttemptRepairAuthorization, OperationAttemptRepairAuthorizer, PortError, PortFuture,
    RuntimeAttemptStatusActor, RuntimeAttemptStatusAuthorization, RuntimeAttemptStatusAuthorizer,
    RuntimeCancellationActor, RuntimeCancellationAuthorization, RuntimeCancellationAuthorizer,
};
use undercurrent_domain::OperationAttemptId;

/// Authorizes a principal admitted by the daemon's configured Local policy.
///
/// The transport must already have authenticated and admitted the peer. General
/// application operations, attempt repair, and attempt status are permitted here;
/// this adapter does not provide per-resource tenant policy. It never receives
/// credentials and must not be composed behind an unauthenticated transport.
///
/// Semantic runtime cancellation and cancellation-status queries are denied by
/// default. Local admission cannot establish the product actor, tenant, target,
/// or force authority required by those operations; a composition needing them
/// must provide an appropriate [`RuntimeCancellationAuthorizer`].
#[derive(Debug, Default, Clone, Copy)]
pub struct AuthenticatedTransportAuthorization;

impl AuthenticatedTransportAuthorization {
    /// Constructs the stateless post-transport policy.
    ///
    /// This function performs no IO and does not verify filesystem modes. The
    /// composition root must pair it with authenticated and authorized
    /// Local admission. This function does not panic.
    pub const fn new() -> Self {
        Self
    }
}

impl AuthorizationPort for AuthenticatedTransportAuthorization {
    fn authorize<'a>(
        &'a self,
        _action: AccessAction,
        _resource: &'a AccessResource,
    ) -> PortFuture<'a, Result<AccessDecision, PortError>> {
        Box::pin(async { Ok(AccessDecision::Permit) })
    }
}

impl OperationAttemptRepairAuthorizer for AuthenticatedTransportAuthorization {
    fn authorize_repair<'a>(
        &'a self,
        _actor: &'a OperationAttemptRepairActor,
        _attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<OperationAttemptRepairAuthorization, PortError>> {
        Box::pin(async { Ok(OperationAttemptRepairAuthorization::Authorized) })
    }
}

impl RuntimeAttemptStatusAuthorizer for AuthenticatedTransportAuthorization {
    fn authorize_status<'a>(
        &'a self,
        _actor: &'a RuntimeAttemptStatusActor,
        _attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<RuntimeAttemptStatusAuthorization, PortError>> {
        Box::pin(async { Ok(RuntimeAttemptStatusAuthorization::Authorized) })
    }
}

impl RuntimeCancellationAuthorizer for AuthenticatedTransportAuthorization {
    fn authorize_cancel<'a>(
        &'a self,
        actor: &'a RuntimeCancellationActor,
        control_id: &'a OperationAttemptId,
        target_attempt_id: &'a OperationAttemptId,
        mode: undercurrent_application::RuntimeCancellationMode,
    ) -> PortFuture<'a, Result<RuntimeCancellationAuthorization, PortError>> {
        Box::pin(async move {
            Ok(runtime_cancellation_decision(
                actor,
                control_id,
                target_attempt_id,
                Some(mode),
            ))
        })
    }

    fn authorize_status<'a>(
        &'a self,
        actor: &'a RuntimeCancellationActor,
        control_id: &'a OperationAttemptId,
        target_attempt_id: &'a OperationAttemptId,
    ) -> PortFuture<'a, Result<RuntimeCancellationAuthorization, PortError>> {
        Box::pin(async move {
            Ok(runtime_cancellation_decision(
                actor,
                control_id,
                target_attempt_id,
                None,
            ))
        })
    }
}

fn runtime_cancellation_decision(
    actor: &RuntimeCancellationActor,
    control_id: &OperationAttemptId,
    target_attempt_id: &OperationAttemptId,
    mode: Option<undercurrent_application::RuntimeCancellationMode>,
) -> RuntimeCancellationAuthorization {
    // Transport authentication proves only the peer connection. It cannot
    // prove the caller-supplied product, actor-context, semantic authority,
    // attempt/control scope, or Force permission. Until composition injects a
    // Passport/Charter-backed implementation of RuntimeCancellationAuthorizer,
    // fail closed without consulting cancellation or session state.
    let _ = (actor, control_id, target_attempt_id, mode);
    RuntimeCancellationAuthorization::Denied
}

#[cfg(test)]
mod tests {
    use undercurrent_application::{RuntimeCancellationActor, RuntimeCancellationMode};
    use undercurrent_domain::{OperationAttemptId, SessionId};

    use super::*;

    #[test]
    fn every_registered_action_and_resource_is_permitted_without_state() {
        let authorization = AuthenticatedTransportAuthorization::new();
        let resources = [
            AccessResource::Sessions,
            AccessResource::Session(SessionId::new("session-1").expect("valid session ID")),
            AccessResource::Configuration,
            AccessResource::Daemon,
            AccessResource::LocalListener,
            AccessResource::Diagnostics,
        ];

        for action in AccessAction::ALL {
            for resource in &resources {
                assert_eq!(
                    crate::blocking::block_on(authorization.authorize(action, resource))
                        .expect("local authorization cannot fail"),
                    AccessDecision::Permit
                );
            }
        }
    }

    #[test]
    fn adapter_is_zero_state_and_copyable() {
        assert_eq!(
            std::mem::size_of::<AuthenticatedTransportAuthorization>(),
            0
        );
        let original = AuthenticatedTransportAuthorization::new();
        let copy = original;
        assert_eq!(format!("{original:?}"), format!("{copy:?}"));
    }

    #[test]
    fn transport_authentication_alone_never_authorizes_cancellation_scope_or_force() {
        let authorization = AuthenticatedTransportAuthorization::new();
        let actor = RuntimeCancellationActor::new(
            "example-caller".to_owned(),
            Some("tenant-1".to_owned()),
            "example/caller".to_owned(),
            "example/workflow".to_owned(),
            "tenant-1".to_owned(),
            "actor://caller/1".to_owned(),
            "authority://runtime/cancel/1".to_owned(),
        )
        .expect("bounded actor");
        let control = OperationAttemptId::new("control-1").expect("control ID");
        let target = OperationAttemptId::new("target-1").expect("target ID");

        for mode in [
            RuntimeCancellationMode::Graceful,
            RuntimeCancellationMode::Force,
        ] {
            assert_eq!(
                crate::blocking::block_on(
                    authorization.authorize_cancel(&actor, &control, &target, mode)
                )
                .expect("fail-closed decision"),
                RuntimeCancellationAuthorization::Denied
            );
        }
        assert_eq!(
            crate::blocking::block_on(RuntimeCancellationAuthorizer::authorize_status(
                &authorization,
                &actor,
                &control,
                &target,
            ))
            .expect("fail-closed status decision"),
            RuntimeCancellationAuthorization::Denied
        );
    }
}
