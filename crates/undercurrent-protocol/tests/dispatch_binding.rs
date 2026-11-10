// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use undercurrent_foundation::Classification;
use undercurrent_protocol::{
    CommandSpecDto, EnvironmentSpecDto, ExecutionEnvironmentBindingDto, ExecutionPolicyDto,
    HealthRulesDto, MutationContext, OperationAttemptDto, OutputCaptureModeDto, ProductRequest,
    ProfileBindingModeDto, RuntimeDispatchBindingDto, RuntimeDispatchPolicyDto,
    RuntimeOutputContractDto, RuntimeResourceLimitsDto, StartSessionRequest, TerminalSizeDto,
    canonical_product_request_digest, product_capabilities,
};

#[test]
fn launch_mode_is_bound_to_request_digest_and_legacy_defaults_to_sealed() {
    let sealed = dispatched_start("3");
    let mut native = sealed.clone();
    native.launch_mode = undercurrent_protocol::LaunchModeDto::Native;
    assert_ne!(
        canonical_product_request_digest(&ProductRequest::StartSession(sealed.clone())).unwrap(),
        canonical_product_request_digest(&ProductRequest::StartSession(native.clone())).unwrap()
    );
    let encoded = serde_json::to_value(&sealed).unwrap();
    assert!(encoded.get("launch_mode").is_none());
    let decoded: StartSessionRequest = serde_json::from_value(encoded).unwrap();
    assert_eq!(
        decoded.launch_mode,
        undercurrent_protocol::LaunchModeDto::Sealed
    );
    let decoded: StartSessionRequest =
        serde_json::from_slice(&serde_json::to_vec(&native).unwrap()).unwrap();
    assert_eq!(
        decoded.launch_mode,
        undercurrent_protocol::LaunchModeDto::Native
    );
}

#[test]
fn dispatch_binding_round_trips_and_changes_the_canonical_start_fingerprint() {
    let request = dispatched_start("3");
    let encoded = serde_json::to_vec(&request).expect("encode dispatched start");
    let decoded: StartSessionRequest =
        serde_json::from_slice(&encoded).expect("decode dispatched start");
    assert!(decoded == request);

    let mut missing_request_digest =
        serde_json::to_value(request.dispatch.as_ref().expect("dispatch"))
            .expect("encode dispatch binding");
    missing_request_digest
        .as_object_mut()
        .expect("dispatch object")
        .remove("request_digest_sha256");
    assert!(
        serde_json::from_value::<RuntimeDispatchBindingDto>(missing_request_digest).is_err(),
        "the caller request digest is required in a 0.1.0-beta attached start"
    );

    let original = canonical_product_request_digest(&ProductRequest::StartSession(request.clone()))
        .expect("canonical digest");
    let changed =
        canonical_product_request_digest(&ProductRequest::StartSession(dispatched_start("4")))
            .expect("changed canonical digest");
    assert_ne!(original, changed);

    let mut changed_executable = request.clone();
    changed_executable
        .dispatch
        .as_mut()
        .expect("dispatch")
        .executable_digest_sha256 = Some("5".repeat(64));
    assert_ne!(
        original,
        canonical_product_request_digest(&ProductRequest::StartSession(changed_executable))
            .expect("executable pin digest")
    );

    let mut changed_environment_root = request.clone();
    changed_environment_root
        .environment
        .binding
        .as_mut()
        .expect("environment binding")
        .working_directory_root = "/different-workspace".to_owned();
    assert_ne!(
        original,
        canonical_product_request_digest(&ProductRequest::StartSession(changed_environment_root))
            .expect("environment root digest")
    );

    let mut changed_policy = request.clone();
    changed_policy
        .dispatch
        .as_mut()
        .expect("dispatch")
        .runtime_policy
        .as_mut()
        .expect("runtime policy")
        .limits
        .stdout_bytes += 1;
    assert_ne!(
        original,
        canonical_product_request_digest(&ProductRequest::StartSession(changed_policy))
            .expect("runtime policy digest")
    );

    let mut changed_callback = request.clone();
    changed_callback
        .dispatch
        .as_mut()
        .expect("dispatch")
        .runtime_policy
        .as_mut()
        .expect("policy")
        .callback_refs =
        vec!["runtime-callback://operation/another-caller/v0.1.0-beta/terminal".into()];
    assert_ne!(
        original,
        canonical_product_request_digest(&ProductRequest::StartSession(changed_callback))
            .expect("callback ownership participates in the immutable request fingerprint")
    );

    let mut changed_caller_request = request.clone();
    changed_caller_request
        .dispatch
        .as_mut()
        .expect("dispatch")
        .request_digest_sha256 = "8".repeat(64);
    assert_ne!(
        original,
        canonical_product_request_digest(&ProductRequest::StartSession(changed_caller_request))
            .expect("caller request digest")
    );

    let mut retry_metadata_only = request;
    retry_metadata_only.mutation = MutationContext {
        idempotency_key: Some("different-transport-retry".to_owned()),
        attempt: Some(OperationAttemptDto {
            attempt_id: "different-attempt-metadata".to_owned(),
            fencing_token: 99,
            payload_digest_sha256: "f".repeat(64),
        }),
        ..MutationContext::default()
    };
    assert_eq!(
        original,
        canonical_product_request_digest(&ProductRequest::StartSession(retry_metadata_only))
            .expect("retry digest"),
        "transport retry metadata stays outside the semantic payload fingerprint"
    );
}

#[test]
fn runtime_policy_fixture_is_strict_and_debug_is_redacted() {
    let policy: RuntimeDispatchPolicyDto =
        serde_json::from_str(include_str!("fixtures/runtime_dispatch_policy.json"))
            .expect("decode policy fixture");
    let debug = format!("{policy:?}");
    assert!(!debug.contains("trust://telemetry/policies/debug-canary"));
    assert!(!debug.contains("trust://retention/policies/debug-canary"));
    assert!(!debug.contains("runtime-callback://example/workflow/v0.1.0-beta/debug-canary"));
    assert!(!debug.contains(&"6".repeat(64)));

    let mut missing_callbacks =
        serde_json::to_value(&policy).expect("encode complete runtime policy");
    missing_callbacks
        .as_object_mut()
        .expect("policy object")
        .remove("callback_refs");
    assert!(
        serde_json::from_value::<RuntimeDispatchPolicyDto>(missing_callbacks).is_err(),
        "a present policy must not silently default omitted callback ownership"
    );

    let mut missing_retention =
        serde_json::to_value(&policy).expect("encode complete runtime policy");
    missing_retention
        .as_object_mut()
        .expect("policy object")
        .remove("retention_policy_ref");
    assert!(
        serde_json::from_value::<RuntimeDispatchPolicyDto>(missing_retention).is_err(),
        "a present policy must not silently default omitted retention ownership"
    );

    for required_optional_limit in ["memory_bytes", "cpu_millis"] {
        let mut missing_limit_member =
            serde_json::to_value(&policy).expect("encode complete runtime policy");
        missing_limit_member["limits"]
            .as_object_mut()
            .expect("limits object")
            .remove(required_optional_limit);
        assert!(
            serde_json::from_value::<RuntimeDispatchPolicyDto>(missing_limit_member).is_err(),
            "{required_optional_limit} must be explicitly null or numeric"
        );
    }

    let mut secret_output = serde_json::to_value(&policy).expect("encode complete runtime policy");
    secret_output["output_contract"]["classification"] = serde_json::json!("secret");
    assert!(
        serde_json::from_value::<RuntimeDispatchPolicyDto>(secret_output).is_err(),
        "Secret output is unsupported by the inline runtime-result protocol"
    );
}

#[test]
fn output_capture_mode_defaults_compatibly_and_changes_the_start_fingerprint() {
    let terminal = dispatched_start("3");
    let terminal_json = serde_json::to_value(&terminal).expect("encode terminal start");
    assert!(terminal_json.get("output_capture_mode").is_none());
    let decoded: StartSessionRequest =
        serde_json::from_value(terminal_json).expect("decode legacy-compatible start");
    assert_eq!(decoded.output_capture_mode, OutputCaptureModeDto::Terminal);

    let mut opaque = terminal.clone();
    opaque.output_capture_mode = OutputCaptureModeDto::Opaque;
    let opaque_json = serde_json::to_value(&opaque).expect("encode opaque start");
    assert_eq!(opaque_json["output_capture_mode"], "opaque");
    assert_ne!(
        canonical_product_request_digest(&ProductRequest::StartSession(terminal))
            .expect("terminal digest"),
        canonical_product_request_digest(&ProductRequest::StartSession(opaque))
            .expect("opaque digest")
    );
    assert_eq!(
        product_capabilities::OPAQUE_OUTPUT_CAPTURE,
        "undercurrent.opaque-output-capture"
    );
}

fn dispatched_start(input_digit: &str) -> StartSessionRequest {
    StartSessionRequest {
        launch_mode: undercurrent_protocol::LaunchModeDto::Sealed,
        mutation: MutationContext::default(),
        dispatch: Some(Box::new(RuntimeDispatchBindingDto {
            caller_cell_identity: "cell:build".to_owned(),
            request_digest_sha256: "7".repeat(64),
            launch_profile_digest_sha256: "1".repeat(64),
            executable_digest_sha256: Some("4".repeat(64)),
            environment_binding_digest_sha256: "2".repeat(64),
            input_digest_sha256: input_digit.repeat(64),
            runtime_policy: Some(Box::new(RuntimeDispatchPolicyDto {
                limits: RuntimeResourceLimitsDto {
                    wall_time_millis: 30_000,
                    stdout_bytes: 1_024,
                    stderr_bytes: 2_048,
                    memory_bytes: Some(4_096),
                    cpu_millis: Some(500),
                },
                output_contract: RuntimeOutputContractDto {
                    schema: "example.output/0.1.0-beta".to_owned(),
                    schema_digest_sha256: "6".repeat(64),
                    classification: Classification::Internal,
                },
                telemetry_policy_ref: "trust://telemetry/policies/internal".to_owned(),
                retention_policy_ref: "trust://retention/policies/internal".to_owned(),
                callback_refs: vec![
                    "runtime-callback://example/workflow/v0.1.0-beta/terminal".to_owned(),
                ],
                launch_profile_expires_at_millis: 10_000,
                environment_binding_expires_at_millis: 20_000,
            })),
            agent: None,
        })),
        name: "build".to_owned(),
        command: CommandSpecDto {
            program: "sh".to_owned(),
            arguments: Vec::new(),
        },
        working_directory: None,
        environment: EnvironmentSpecDto {
            binding: Some(ExecutionEnvironmentBindingDto {
                binding_digest_sha256: "2".repeat(64),
                working_directory_root: "/workspace".to_owned(),
            }),
            ..EnvironmentSpecDto::default()
        },
        terminal: TerminalSizeDto {
            columns: 80,
            rows: 24,
        },
        output_capture_mode: OutputCaptureModeDto::Terminal,
        execution: ExecutionPolicyDto::default(),
        health: HealthRulesDto::default(),
        profile: None,
        profile_binding: ProfileBindingModeDto::Fixed,
        labels: BTreeMap::new(),
        metadata: BTreeMap::new(),
    }
}
