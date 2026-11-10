// SPDX-License-Identifier: Apache-2.0

use undercurrent_domain::{
    DispatchCellIdentity, FencingToken, OperationAttempt, OperationAttemptId, PayloadDigest,
    ProcessMutationKind, RuntimeCallbackRef, RuntimeDispatchBinding, RuntimeDispatchLimits,
    RuntimeDispatchPolicy, RuntimeOutputContract, RuntimeTelemetryPolicyRef, SessionId,
    TimestampMillis,
};
use undercurrent_foundation::Classification;

#[test]
fn direct_product_callback_contract_accepts_an_explicit_owner_and_version() {
    let value = "runtime-callback://example/workflow/v0.1.0-beta/terminal";
    let reference = RuntimeCallbackRef::new(value).expect("direct callback contract");
    assert_eq!(reference.as_str(), value);
}

#[test]
fn direct_callback_contract_rejects_obsolete_routes_and_noncanonical_identity() {
    for (value, valid) in [
        (
            "runtime-callback://example/workflow/v0.1.0-beta/terminal",
            true,
        ),
        ("runtime-callback://intent/a/v0.1.0-beta/0", true),
        (
            "runtime-callback://insight/report-reader/v0.1.0-beta/result_1.json",
            true,
        ),
        (
            "runtime-callback://environment/worktree/v0.1.0-beta/ready",
            true,
        ),
        (
            "runtime-callback://trust/charter/v0.1.0-beta/decision",
            true,
        ),
        (
            "runtime-callback://operation/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/v0.1.0-beta/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            true,
        ),
        ("example-coordinator://callbacks/terminal", false),
        (
            "runtime-callback://operation/example-coordinator/v0.1.0-beta/terminal",
            true,
        ),
        (
            "runtime-callback://trust/example-coordinator/v0.1.0-beta/terminal",
            true,
        ),
        ("https://caller.invalid/callback", false),
        ("example-app://callbacks/direct-product", false),
        (
            "runtime-callback://custom/workflow/v0.1.0-beta/terminal",
            true,
        ),
        (
            "runtime-callback://custom-scope/client-1/v0.1.0-beta/terminal",
            true,
        ),
        (
            "runtime-callback://Custom/workflow/v0.1.0-beta/terminal",
            false,
        ),
        ("runtime-callback:///workflow/0.1.0-beta/terminal", false),
        (
            "runtime-callback://custom-/workflow/v0.1.0-beta/terminal",
            false,
        ),
        (
            "runtime-callback://example/Workflow/v0.1.0-beta/terminal",
            false,
        ),
        (
            "runtime-callback://example/1workflow/v0.1.0-beta/terminal",
            false,
        ),
        (
            "runtime-callback://example/workflow-/v0.1.0-beta/terminal",
            false,
        ),
        ("runtime-callback://operation//0.1.0-beta/terminal", false),
        (
            "runtime-callback://example/workflow/0.1.0-beta/terminal",
            false,
        ),
        ("runtime-callback://example/workflow/v0.1.0-beta/", false),
        (
            "runtime-callback://example/workflow/v0.1.0-beta/../terminal",
            false,
        ),
        (
            "runtime-callback://example/workflow/v0.1.0-beta/%74erminal",
            false,
        ),
        (
            "runtime-callback://example/workflow/v0.1.0-beta/terminal/extra",
            false,
        ),
        (
            "runtime-callback://example/workflow/v0.1.0-beta/terminal?token=secret",
            false,
        ),
        (
            "runtime-callback://example/workflow/v0.1.0-beta/terminal#fragment",
            false,
        ),
        (
            "runtime-callback://example/user@workflow/v0.1.0-beta/terminal",
            false,
        ),
        (
            "runtime-callback://example/workflow/v0.1.0-beta/terminal\n",
            false,
        ),
        ("runtime-callback://example/workflow/v0.1.0-beta/é", false),
        (
            "runtime-callback://operation/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/v0.1.0-beta/id",
            false,
        ),
        (
            "runtime-callback://example/workflow/v0.1.0-beta/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            false,
        ),
    ] {
        assert_eq!(RuntimeCallbackRef::new(value).is_ok(), valid, "{value:?}");
    }
}

#[test]
fn immutable_dispatch_binding_is_part_of_attempt_semantics() {
    assert!(DispatchCellIdentity::new("cell/unsafe").is_err());
    assert!(DispatchCellIdentity::new("cell:build-1").is_ok());

    let original = attempt("input-a");
    let exact_retry = attempt("input-a");
    let changed_input = attempt("input-b");
    assert!(original.has_same_semantics(&exact_retry));
    assert!(!original.has_same_semantics(&changed_input));

    let changed_request = OperationAttempt::accepted_with_dispatch(
        original.id().clone(),
        original.target_session().cloned(),
        original.kind(),
        original.fencing_token(),
        original.payload_digest(),
        Some(RuntimeDispatchBinding::new(
            DispatchCellIdentity::new("cell:build").expect("cell"),
            PayloadDigest::of_bytes(b"different caller request"),
            PayloadDigest::of_bytes(b"launch"),
            PayloadDigest::of_bytes(b"environment"),
            PayloadDigest::of_bytes(b"input-a"),
            policy(1_024),
        )),
        original.accepted_at(),
    );
    assert!(!original.has_same_semantics(&changed_request));

    let mut changed_policy = attempt("input-a");
    let binding = RuntimeDispatchBinding::new(
        DispatchCellIdentity::new("cell:build").expect("cell"),
        PayloadDigest::of_bytes(b"caller request"),
        PayloadDigest::of_bytes(b"launch"),
        PayloadDigest::of_bytes(b"environment"),
        PayloadDigest::of_bytes(b"input-a"),
        policy(2_048),
    );
    changed_policy = OperationAttempt::accepted_with_dispatch(
        changed_policy.id().clone(),
        changed_policy.target_session().cloned(),
        changed_policy.kind(),
        changed_policy.fencing_token(),
        changed_policy.payload_digest(),
        Some(binding),
        changed_policy.accepted_at(),
    );
    assert!(!original.has_same_semantics(&changed_policy));

    assert!(RuntimeDispatchLimits::new(0, 1, 1, None, None).is_err());
    assert!(RuntimeDispatchLimits::new(1, 1, 1, Some(0), None).is_err());
    assert!(
        RuntimeDispatchPolicy::new(
            RuntimeDispatchLimits::new(1, 1, 1, None, None).expect("limits"),
            RuntimeOutputContract::new(
                "example.output/0.1.0-beta",
                PayloadDigest::of_bytes(b"schema"),
                Classification::Internal,
            )
            .expect("output contract"),
            RuntimeTelemetryPolicyRef::new("trust://telemetry/policies/internal")
                .expect("telemetry ref"),
            undercurrent_domain::RuntimeRetentionPolicyRef::new(
                "trust://retention/policies/internal",
            )
            .expect("retention ref"),
            vec![
                RuntimeCallbackRef::new("runtime-callback://example/workflow/v0.1.0-beta/z")
                    .expect("callback"),
                RuntimeCallbackRef::new("runtime-callback://example/workflow/v0.1.0-beta/a")
                    .expect("callback"),
            ],
            TimestampMillis::new(1_000),
            TimestampMillis::new(2_000),
        )
        .is_err()
    );
    assert!(
        policy(1_024)
            .validate_at(TimestampMillis::new(1_000))
            .is_err()
    );
}

#[test]
fn output_schema_bound_matches_the_upstream_contract() {
    assert!(
        RuntimeOutputContract::new(
            "a".repeat(128),
            PayloadDigest::of_bytes(b"schema-128"),
            Classification::Internal,
        )
        .is_ok()
    );
    assert!(
        RuntimeOutputContract::new(
            "a".repeat(129),
            PayloadDigest::of_bytes(b"schema-129"),
            Classification::Internal,
        )
        .is_err()
    );
    assert!(
        RuntimeOutputContract::new(
            "secret.output/0.1.0-beta",
            PayloadDigest::of_bytes(b"secret-schema"),
            Classification::Secret,
        )
        .is_err(),
        "inline runtime results cannot safely carry Secret output"
    );
}

#[test]
fn policy_references_are_strict_and_debug_is_redacted() {
    let telemetry_canary = "trust://telemetry/policies/debug-canary";
    let callback_canary = "runtime-callback://example/workflow/v0.1.0-beta/debug-canary";
    let digest_canary = PayloadDigest::of_bytes(b"debug-schema-canary").to_hex();
    let telemetry = RuntimeTelemetryPolicyRef::new(telemetry_canary).expect("telemetry ref");
    let callback = RuntimeCallbackRef::new(callback_canary).expect("callback ref");
    let value = RuntimeDispatchPolicy::new(
        RuntimeDispatchLimits::new(30_000, 1_024, 2_048, None, None).expect("limits"),
        RuntimeOutputContract::new(
            "debug.output/0.1.0-beta",
            PayloadDigest::from_hex(&digest_canary).expect("digest"),
            Classification::Restricted,
        )
        .expect("output contract"),
        telemetry.clone(),
        undercurrent_domain::RuntimeRetentionPolicyRef::new(
            "trust://retention/policies/debug-canary",
        )
        .expect("retention ref"),
        vec![callback.clone()],
        TimestampMillis::new(10_000),
        TimestampMillis::new(20_000),
    )
    .expect("runtime policy");

    for debug in [
        format!("{telemetry:?}"),
        format!("{callback:?}"),
        format!("{value:?}"),
    ] {
        assert!(!debug.contains(telemetry_canary));
        assert!(!debug.contains(callback_canary));
        assert!(!debug.contains(&digest_canary));
    }

    assert!(
        RuntimeTelemetryPolicyRef::new("trust://telemetry/policy?token=debug-canary").is_err(),
        "Foundation opaque references reject credential-like query material"
    );
    assert!(
        RuntimeCallbackRef::new("example-app://callbacks/direct-product").is_err(),
        "unversioned callback routes are not the public callback contract"
    );
}

fn attempt(input: &str) -> OperationAttempt {
    OperationAttempt::accepted_with_dispatch(
        OperationAttemptId::new("attempt:7").expect("attempt"),
        Some(SessionId::new("session-7").expect("session")),
        ProcessMutationKind::Spawn,
        FencingToken::new(7).expect("fence"),
        PayloadDigest::of_bytes(b"canonical request"),
        Some(RuntimeDispatchBinding::new(
            DispatchCellIdentity::new("cell:build").expect("cell"),
            PayloadDigest::of_bytes(b"caller request"),
            PayloadDigest::of_bytes(b"launch"),
            PayloadDigest::of_bytes(b"environment"),
            PayloadDigest::of_bytes(input.as_bytes()),
            policy(1_024),
        )),
        TimestampMillis::new(100),
    )
}

fn policy(stdout_bytes: u64) -> RuntimeDispatchPolicy {
    RuntimeDispatchPolicy::new(
        RuntimeDispatchLimits::new(30_000, stdout_bytes, 2_048, Some(4_096), Some(500))
            .expect("limits"),
        RuntimeOutputContract::new(
            "example.output/0.1.0-beta",
            PayloadDigest::of_bytes(b"schema"),
            Classification::Internal,
        )
        .expect("output contract"),
        RuntimeTelemetryPolicyRef::new("trust://telemetry/policies/internal")
            .expect("telemetry ref"),
        undercurrent_domain::RuntimeRetentionPolicyRef::new("trust://retention/policies/internal")
            .expect("retention ref"),
        vec![
            RuntimeCallbackRef::new("runtime-callback://example/workflow/v0.1.0-beta/terminal")
                .expect("callback"),
        ],
        TimestampMillis::new(1_000),
        TimestampMillis::new(2_000),
    )
    .expect("runtime policy")
}
