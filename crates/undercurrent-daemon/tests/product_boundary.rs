// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use undercurrent_application::RemovalConfirmation;
use undercurrent_daemon::{
    ProductBoundary, ProductBoundaryError, ProductBoundaryLimits, ProductEventBoundaryLimits,
    ProductProfileBoundaryLimits,
};
use undercurrent_domain::{
    DomainEventKind, ExecutionLimits, ExecutionState, HealthStatus, InputItem,
    InputObservationPolicy, InteractionState, Key, LifecycleKind, OutputCaptureMode, RestartPolicy,
    SessionProjection, SessionSortField, SortDirection, TimestampMillis, WorkRelationKind,
    WorkStatus, WorkTarget,
};
use undercurrent_protocol::{
    CloseStandardInputRequest, CommandSpecDto, EnvironmentSchemaVersionDto, EnvironmentSpecDto,
    ExecutionEnvironmentBindingDto, ExecutionPolicyDto, ExecutionStateDto, HealthRuleDto,
    HealthRulesDto, HealthStatusDto, InputObservationPolicyDto, InteractionStateDto, KeyDto,
    LifecycleKindDto, ListProfilesRequest, ListSessionsRequest, MatchProfileRequest,
    ModifiableKeyDto, MutationContext, OperationAttemptDto, OutputCaptureModeDto, ProductRequest,
    ReadSessionEventsRequest, RemovalConfirmationDto, RemoveSessionRequest, RestartPolicyDto,
    RuntimeDispatchBindingDto, RuntimeDispatchPolicyDto, RuntimeOutputContractDto,
    RuntimeResourceLimitsDto, SecretReferenceDto, SendTerminalInputRequest, SessionEventKindDto,
    SessionFilterDto, SessionProjectionDto, SessionSortDto, SessionSortFieldDto, SortDirectionDto,
    StartSessionRequest, StopPolicyDto, StopSignalDto, TerminalInputDto, TerminalSizeDto,
    TestProfileRequest, UpsertWorkItemRequest, WorkItemDto, WorkRelationshipDto, WorkStatusDto,
    WorkTargetDto, canonical_product_request_digest,
};

fn limits() -> ProductBoundaryLimits {
    ProductBoundaryLimits {
        execution: ExecutionLimits {
            max_arguments: 8,
            max_command_bytes: 1_024,
            max_environment_entries: 8,
            max_environment_bytes: 2_048,
            max_path_bytes: 256,
            max_health_rules: 8,
            max_health_rule_bytes: 256,
        },
        max_terminal_columns: 400,
        max_terminal_rows: 200,
        max_input_items: 8,
        max_input_bytes: 1_024,
        max_query_page_size: 100,
        max_work_items: 32,
        events: ProductEventBoundaryLimits {
            max_filter_sessions: 8,
            max_filter_kinds: 16,
            max_page_size: 100,
        },
        profiles: ProductProfileBoundaryLimits {
            max_page_size: 50,
            max_environment_name_bytes: 128,
            max_sample_bytes: 1_024,
        },
    }
}

fn boundary() -> ProductBoundary {
    ProductBoundary::new(limits()).expect("boundary limits")
}

#[test]
fn label_patch_boundary_rejects_invalid_keys_values_and_ambiguous_intent() {
    use undercurrent_protocol::UpdateSessionLabelsRequest;
    let request = UpdateSessionLabelsRequest {
        mutation: MutationContext::default(),
        session_id: "session-1".into(),
        set: BTreeMap::from([("aixand.my_terminal".into(), "true".into())]),
        remove: vec![],
    };
    let command = boundary().update_session_labels(request.clone()).unwrap();
    assert_eq!(command.session_id.as_str(), "session-1");
    for invalid in [
        UpdateSessionLabelsRequest {
            session_id: "".into(),
            ..request.clone()
        },
        UpdateSessionLabelsRequest {
            set: BTreeMap::from([("x".into(), "v".repeat(257))]),
            ..request.clone()
        },
        UpdateSessionLabelsRequest {
            remove: vec!["aixand.my_terminal".into()],
            ..request.clone()
        },
        UpdateSessionLabelsRequest {
            remove: vec!["bad\nkey".into()],
            ..request.clone()
        },
        UpdateSessionLabelsRequest {
            remove: vec!["key".into(); 65],
            ..request.clone()
        },
    ] {
        assert!(boundary().update_session_labels(invalid).is_err());
    }
}

#[test]
fn complete_start_translation_preserves_policy_and_redacts_private_values() {
    let request = StartSessionRequest {
        launch_mode: undercurrent_protocol::LaunchModeDto::Sealed,
        mutation: MutationContext::default(),
        dispatch: None,
        name: "build".to_owned(),
        command: CommandSpecDto {
            program: "sh".to_owned(),
            arguments: vec!["-lc".to_owned(), "echo ready".to_owned()],
        },
        working_directory: Some("/workspace".to_owned()),
        environment: EnvironmentSpecDto {
            schema_version: EnvironmentSchemaVersionDto::Current,
            variables: BTreeMap::from([("PUBLIC_MODE".to_owned(), "do-not-log".to_owned())]),
            secret_variables: BTreeMap::from([(
                "SERVICE_CREDENTIAL".to_owned(),
                SecretReferenceDto::new("passport-handle-boundary-canary")
                    .expect("secret reference"),
            )]),
            file: Some("/run/config.env".to_owned()),
            binding: Some(ExecutionEnvironmentBindingDto {
                binding_digest_sha256: "4".repeat(64),
                working_directory_root: "/workspace".to_owned(),
            }),
        },
        terminal: TerminalSizeDto {
            columns: 120,
            rows: 40,
        },
        output_capture_mode: OutputCaptureModeDto::Opaque,
        execution: ExecutionPolicyDto {
            restart: RestartPolicyDto::OnFailure {
                max_attempts: 4,
                initial_delay_millis: 25,
                max_delay_millis: 3_000,
                backoff_basis_points: 20_000,
                min_uptime_millis: 10_000,
            },
            stop: StopPolicyDto {
                signal: StopSignalDto::Interrupt,
                timeout_millis: 2_000,
            },
            detection_enabled: true,
            auto_remove: true,
        },
        health: HealthRulesDto {
            rules: vec![
                HealthRuleDto::TcpPort { port: 8_080 },
                HealthRuleDto::OutputContains {
                    pattern: "ready".to_owned(),
                },
            ],
        },
        profile: Some("shell".to_owned()),
        profile_binding: undercurrent_protocol::ProfileBindingModeDto::Fixed,
        labels: BTreeMap::from([("team".to_owned(), "platform".to_owned())]),
        metadata: BTreeMap::from([("private".to_owned(), "secret-note".to_owned())]),
    };

    let command = boundary()
        .start_session(request, TimestampMillis::new(100))
        .expect("valid start");
    let configuration = &command.configuration;
    assert_eq!(command.name.as_str(), "build");
    assert_eq!(command.max_work_items, 32);
    assert_eq!(configuration.launch().command().program(), "sh");
    assert_eq!(configuration.launch().environment().len(), 1);
    assert_eq!(
        configuration
            .launch()
            .secret_environment()
            .names()
            .collect::<Vec<_>>(),
        ["SERVICE_CREDENTIAL"]
    );
    assert_eq!(configuration.launch().terminal_size().columns(), 120);
    assert_eq!(
        configuration.launch().output_capture_mode(),
        OutputCaptureMode::Opaque
    );
    assert_eq!(
        configuration.execution().restart().policy(),
        RestartPolicy::OnFailure
    );
    assert_eq!(
        configuration.execution().restart().backoff_basis_points(),
        20_000
    );
    assert!(configuration.execution().auto_remove());
    assert_eq!(configuration.health_rules().len(), 2);
    assert_eq!(configuration.profile().expect("profile").as_str(), "shell");
    assert_eq!(configuration.labels().get("team"), Some("platform"));

    let debug = format!("{command:?}");
    assert!(!debug.contains("do-not-log"));
    assert!(!debug.contains("secret-note"));
    assert!(!debug.contains("passport-handle-boundary-canary"));
}

#[test]
fn automatic_profile_target_uses_start_facts_and_honors_explicit_controls() {
    let mut request = StartSessionRequest {
        launch_mode: undercurrent_protocol::LaunchModeDto::Sealed,
        mutation: MutationContext::default(),
        dispatch: None,
        name: "agent".to_owned(),
        command: CommandSpecDto {
            program: "/usr/local/bin/codex".to_owned(),
            arguments: vec!["--model".to_owned(), "test".to_owned()],
        },
        working_directory: None,
        environment: EnvironmentSpecDto {
            schema_version: EnvironmentSchemaVersionDto::Current,
            variables: BTreeMap::from([("CODEX_HOME".to_owned(), "private".to_owned())]),
            secret_variables: BTreeMap::new(),
            file: None,
            binding: None,
        },
        terminal: TerminalSizeDto {
            columns: 80,
            rows: 24,
        },
        output_capture_mode: OutputCaptureModeDto::Terminal,
        execution: ExecutionPolicyDto::default(),
        health: HealthRulesDto::default(),
        profile: None,
        profile_binding: undercurrent_protocol::ProfileBindingModeDto::Fixed,
        labels: BTreeMap::new(),
        metadata: BTreeMap::new(),
    };

    let target = boundary()
        .automatic_profile_match_target(&request)
        .expect("target is valid")
        .expect("automatic matching is enabled");
    assert_eq!(target.basename(), "codex");
    assert_eq!(target.arguments(), &["--model", "test"]);
    assert!(
        target
            .environment_names()
            .iter()
            .any(|name| name.as_str() == "CODEX_HOME")
    );
    assert!(!format!("{target:?}").contains("private"));

    request.profile = Some("codex".to_owned());
    assert!(
        boundary()
            .automatic_profile_match_target(&request)
            .expect("explicit profile is valid")
            .is_none()
    );
    request.profile = None;
    request.execution.detection_enabled = false;
    assert!(
        boundary()
            .automatic_profile_match_target(&request)
            .expect("disabled detection is valid")
            .is_none()
    );
}

#[test]
fn invalid_start_and_boundary_limits_fail_before_application_io() {
    let mut invalid_limits = limits();
    invalid_limits.max_input_bytes = 0;
    assert!(ProductBoundary::new(invalid_limits).is_err());

    let request = StartSessionRequest {
        launch_mode: undercurrent_protocol::LaunchModeDto::Sealed,
        mutation: MutationContext::default(),
        dispatch: None,
        name: "build".to_owned(),
        command: CommandSpecDto {
            program: "sh".to_owned(),
            arguments: Vec::new(),
        },
        working_directory: None,
        environment: EnvironmentSpecDto::default(),
        terminal: TerminalSizeDto {
            columns: 120,
            rows: 40,
        },
        output_capture_mode: OutputCaptureModeDto::Terminal,
        execution: ExecutionPolicyDto {
            stop: StopPolicyDto {
                signal: StopSignalDto::Terminate,
                timeout_millis: 0,
            },
            ..ExecutionPolicyDto::default()
        },
        health: HealthRulesDto::default(),
        profile: None,
        profile_binding: undercurrent_protocol::ProfileBindingModeDto::Fixed,
        labels: BTreeMap::new(),
        metadata: BTreeMap::new(),
    };
    let error = boundary()
        .start_session(request, TimestampMillis::new(100))
        .expect_err("zero stop timeout");
    assert_eq!(
        error.category(),
        undercurrent_protocol::ProductErrorCategory::InvalidRequest
    );
}

#[test]
fn dispatched_start_requires_complete_safe_binding_and_canonical_payload_digest() {
    let mut request = StartSessionRequest {
        launch_mode: undercurrent_protocol::LaunchModeDto::Sealed,
        mutation: MutationContext {
            idempotency_key: Some("dispatch:retry:7".to_owned()),
            attempt: Some(OperationAttemptDto {
                attempt_id: "dispatch:attempt:7".to_owned(),
                fencing_token: 7,
                payload_digest_sha256: String::new(),
            }),
            ..MutationContext::default()
        },
        dispatch: Some(Box::new(RuntimeDispatchBindingDto {
            caller_cell_identity: "cell:build".to_owned(),
            request_digest_sha256: "7".repeat(64),
            launch_profile_digest_sha256: "1".repeat(64),
            executable_digest_sha256: Some("4".repeat(64)),
            environment_binding_digest_sha256: "2".repeat(64),
            input_digest_sha256: "3".repeat(64),
            runtime_policy: Some(Box::new(runtime_policy())),
            agent: None,
        })),
        name: "dispatched-build".to_owned(),
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
        profile_binding: undercurrent_protocol::ProfileBindingModeDto::Fixed,
        labels: BTreeMap::new(),
        metadata: BTreeMap::new(),
    };
    seal_dispatch_payload(&mut request);

    let command = boundary()
        .start_session(request.clone(), TimestampMillis::new(321))
        .expect("complete dispatch");
    assert_eq!(
        command
            .configuration
            .launch()
            .command()
            .expected_executable_digest(),
        Some(undercurrent_domain::PayloadDigest::from_hex(&"4".repeat(64)).expect("digest"))
    );
    assert_eq!(
        command
            .configuration
            .launch()
            .environment_binding()
            .expect("environment binding")
            .digest(),
        undercurrent_domain::PayloadDigest::from_hex(&"2".repeat(64)).expect("digest")
    );
    let admission = command.dispatch.expect("dispatch admission");
    assert_eq!(
        admission.supplied_fencing_token().expect("fence").value(),
        7
    );
    assert_eq!(
        admission
            .dispatch_binding()
            .expect("binding")
            .caller_cell_identity()
            .as_str(),
        "cell:build"
    );
    assert_eq!(
        admission
            .dispatch_binding()
            .expect("binding")
            .request_digest(),
        Some(undercurrent_domain::PayloadDigest::from_hex(&"7".repeat(64)).expect("digest"))
    );
    let policy = admission
        .dispatch_binding()
        .expect("binding")
        .runtime_policy()
        .expect("runtime policy");
    assert_eq!(policy.limits().wall_time_millis(), 30_000);
    assert_eq!(
        policy.output_contract().classification().to_string(),
        "confidential"
    );
    assert_eq!(policy.callback_refs().len(), 2);

    let mut native_dispatch = request.clone();
    native_dispatch.launch_mode = undercurrent_protocol::LaunchModeDto::Native;
    seal_dispatch_payload(&mut native_dispatch);
    assert!(matches!(
        boundary().start_session(native_dispatch, TimestampMillis::new(321)),
        Err(ProductBoundaryError::Domain(_))
    ));

    let mut missing_executable_digest = request.clone();
    missing_executable_digest
        .dispatch
        .as_mut()
        .expect("binding")
        .executable_digest_sha256 = None;
    seal_dispatch_payload(&mut missing_executable_digest);
    assert!(matches!(
        boundary().start_session(missing_executable_digest, TimestampMillis::new(321)),
        Err(ProductBoundaryError::MissingExecutableDigest)
    ));

    let mut missing_policy = request.clone();
    missing_policy
        .dispatch
        .as_mut()
        .expect("binding")
        .runtime_policy = None;
    seal_dispatch_payload(&mut missing_policy);
    assert!(matches!(
        boundary().start_session(missing_policy, TimestampMillis::new(321)),
        Err(ProductBoundaryError::MissingRuntimePolicy)
    ));

    let mut replayable_expired_profile = request.clone();
    replayable_expired_profile
        .dispatch
        .as_mut()
        .expect("binding")
        .runtime_policy
        .as_mut()
        .expect("runtime policy")
        .launch_profile_expires_at_millis = 321;
    seal_dispatch_payload(&mut replayable_expired_profile);
    let translated = boundary()
        .start_session(replayable_expired_profile, TimestampMillis::new(321))
        .expect("expiry is checked only after durable exact-replay lookup");
    assert_eq!(
        translated
            .dispatch
            .as_ref()
            .and_then(|dispatch| dispatch.dispatch_binding())
            .and_then(|binding| binding.runtime_policy())
            .expect("retained runtime policy")
            .launch_profile_expires_at()
            .value(),
        321
    );

    let mut non_positive_expiry = request.clone();
    non_positive_expiry
        .dispatch
        .as_mut()
        .expect("binding")
        .runtime_policy
        .as_mut()
        .expect("runtime policy")
        .launch_profile_expires_at_millis = 0;
    seal_dispatch_payload(&mut non_positive_expiry);
    assert!(
        boundary()
            .start_session(non_positive_expiry, TimestampMillis::new(321))
            .is_err(),
        "wire translation still rejects a structurally invalid expiry"
    );

    let mut unsupported_callback = request.clone();
    unsupported_callback
        .dispatch
        .as_mut()
        .expect("binding")
        .runtime_policy
        .as_mut()
        .expect("runtime policy")
        .callback_refs = vec!["example-coordinator://callbacks/terminal".to_owned()];
    seal_dispatch_payload(&mut unsupported_callback);
    assert!(
        boundary()
            .start_session(unsupported_callback, TimestampMillis::new(321))
            .is_err(),
        "a correctly sealed request must still reject an unsupported callback scheme"
    );

    let mut unsorted_callbacks = request.clone();
    unsorted_callbacks
        .dispatch
        .as_mut()
        .expect("binding")
        .runtime_policy
        .as_mut()
        .expect("runtime policy")
        .callback_refs
        .reverse();
    seal_dispatch_payload(&mut unsorted_callbacks);
    assert!(
        boundary()
            .start_session(unsorted_callbacks, TimestampMillis::new(321))
            .is_err(),
        "callback references must already be canonical and strictly sorted"
    );

    let mut secret_output = request.clone();
    secret_output
        .dispatch
        .as_mut()
        .expect("binding")
        .runtime_policy
        .as_mut()
        .expect("runtime policy")
        .output_contract
        .classification = serde_json::from_str("\"secret\"").expect("Secret classification");
    seal_dispatch_payload(&mut secret_output);
    let secret_error = boundary()
        .start_session(secret_output, TimestampMillis::new(321))
        .expect_err("Secret inline output");
    assert_eq!(
        secret_error.category(),
        undercurrent_protocol::ProductErrorCategory::InvalidRequest
    );

    let mut missing_environment = request.clone();
    missing_environment.environment.binding = None;
    seal_dispatch_payload(&mut missing_environment);
    assert!(matches!(
        boundary().start_session(missing_environment, TimestampMillis::new(321)),
        Err(ProductBoundaryError::MissingEnvironmentBinding)
    ));

    let mut mismatched_environment = request.clone();
    mismatched_environment
        .environment
        .binding
        .as_mut()
        .expect("environment binding")
        .binding_digest_sha256 = "5".repeat(64);
    seal_dispatch_payload(&mut mismatched_environment);
    assert!(matches!(
        boundary().start_session(mismatched_environment, TimestampMillis::new(321)),
        Err(ProductBoundaryError::EnvironmentBindingMismatch)
    ));

    let mut changed = request.clone();
    changed
        .dispatch
        .as_mut()
        .expect("binding")
        .input_digest_sha256 = "4".repeat(64);
    assert!(
        boundary()
            .start_session(changed, TimestampMillis::new(321))
            .is_err(),
        "changed binding with the old canonical payload digest must fail"
    );

    let mut uppercase_request_digest = request.clone();
    uppercase_request_digest
        .dispatch
        .as_mut()
        .expect("binding")
        .request_digest_sha256 = "A".repeat(64);
    seal_dispatch_payload(&mut uppercase_request_digest);
    assert!(
        boundary()
            .start_session(uppercase_request_digest, TimestampMillis::new(321))
            .is_err(),
        "the separately retained caller request digest must be canonical lowercase hex"
    );

    let mut unsafe_cell = request;
    unsafe_cell
        .dispatch
        .as_mut()
        .expect("binding")
        .caller_cell_identity = "cell/unsafe".to_owned();
    seal_dispatch_payload(&mut unsafe_cell);
    assert!(
        boundary()
            .start_session(unsafe_cell, TimestampMillis::new(321))
            .is_err(),
        "unsafe opaque identity must fail validation"
    );
}

fn seal_dispatch_payload(request: &mut StartSessionRequest) {
    let digest = canonical_product_request_digest(&ProductRequest::StartSession(request.clone()))
        .expect("canonical request digest")
        .to_string();
    request
        .mutation
        .attempt
        .as_mut()
        .expect("attempt")
        .payload_digest_sha256 = digest
        .strip_prefix("sha256:")
        .expect("sha-256 prefix")
        .to_owned();
}

fn runtime_policy() -> RuntimeDispatchPolicyDto {
    RuntimeDispatchPolicyDto {
        limits: RuntimeResourceLimitsDto {
            wall_time_millis: 30_000,
            stdout_bytes: 1_024,
            stderr_bytes: 2_048,
            memory_bytes: Some(4_096),
            cpu_millis: Some(500),
        },
        output_contract: RuntimeOutputContractDto {
            schema: "project-test-result/0.1.0-beta".to_owned(),
            schema_digest_sha256: "6".repeat(64),
            classification: serde_json::from_str("\"confidential\"")
                .expect("Confidential classification"),
        },
        telemetry_policy_ref: "trust://telemetry/policies/internal".to_owned(),
        retention_policy_ref: "trust://retention/policies/runtime-output".to_owned(),
        callback_refs: vec![
            "runtime-callback://example/workflow/v0.1.0-beta/progress".to_owned(),
            "runtime-callback://example/workflow/v0.1.0-beta/terminal".to_owned(),
        ],
        launch_profile_expires_at_millis: 10_000,
        environment_binding_expires_at_millis: 20_000,
    }
}

#[test]
fn typed_input_translation_validates_keys_mouse_and_observation_policy() {
    let request = SendTerminalInputRequest {
        expected_revision: None,
        mutation: MutationContext::default(),
        session_id: "session-1".to_owned(),
        delivery_id: Some("delivery-1".to_owned()),
        items: vec![
            TerminalInputDto::Text {
                text: "hello".to_owned(),
            },
            TerminalInputDto::Key {
                key: KeyDto::Modified {
                    base: ModifiableKeyDto::Function { number: 2 },
                    modifiers: undercurrent_protocol::KeyModifiersDto {
                        shift: true,
                        control: false,
                        alt: true,
                    },
                },
            },
            TerminalInputDto::Mouse {
                event: undercurrent_protocol::MouseEventDto {
                    column: 80,
                    row: 24,
                    button: undercurrent_protocol::MouseButtonDto::Left,
                    event: undercurrent_protocol::MouseEventKindDto::Press,
                },
            },
        ],
        observation_policy: InputObservationPolicyDto::Preserve,
    };

    let command = boundary()
        .send_terminal_input(request)
        .expect("valid input");
    assert_eq!(command.observation_policy, InputObservationPolicy::Preserve);
    assert_eq!(command.input.items().len(), 3);
    assert!(matches!(
        command.input.items()[1],
        InputItem::Key(Key::Modified { .. })
    ));

    let invalid = SendTerminalInputRequest {
        expected_revision: None,
        mutation: MutationContext::default(),
        session_id: "session-1".to_owned(),
        delivery_id: None,
        items: vec![TerminalInputDto::Key {
            key: KeyDto::Function { number: 13 },
        }],
        observation_policy: InputObservationPolicyDto::ForceActive,
    };
    assert!(boundary().send_terminal_input(invalid).is_err());
}

#[test]
fn typed_standard_input_close_validates_identity_without_constructing_terminal_bytes() {
    let command = boundary()
        .close_standard_input(CloseStandardInputRequest {
            mutation: MutationContext::default(),
            session_id: "session-1".to_owned(),
        })
        .expect("valid standard-input close");

    assert_eq!(command.session_id.as_str(), "session-1");
    assert!(
        boundary()
            .close_standard_input(CloseStandardInputRequest {
                mutation: MutationContext::default(),
                session_id: "session unsafe".to_owned(),
            })
            .is_err()
    );
}

#[test]
fn prepared_query_translation_retains_selectivity_and_enforces_page_bound() {
    let request = ListSessionsRequest {
        filter: SessionFilterDto {
            ids: vec!["session-1".to_owned(), "session-1".to_owned()],
            name: None,
            name_contains: Some("build".to_owned()),
            lifecycles: vec![LifecycleKindDto::Running],
            executions: vec![ExecutionStateDto::Quiescent],
            interactions: vec![InteractionStateDto::Requested],
            health: vec![HealthStatusDto::Healthy],
            profile: Some("shell".to_owned()),
            labels: BTreeMap::from([("team".to_owned(), "platform".to_owned())]),
            has_active_work: Some(true),
        },
        sort: SessionSortDto {
            field: SessionSortFieldDto::UpdatedAt,
            direction: SortDirectionDto::Descending,
        },
        projection: SessionProjectionDto::WithLabels,
        page_size: 25,
        cursor: None,
        include_total: false,
    };

    let query = boundary().session_query(&request).expect("valid query");
    assert_eq!(query.filter.ids.len(), 1);
    assert!(query.filter.name.is_none());
    assert!(query.filter.lifecycles.contains(&LifecycleKind::Running));
    assert!(query.filter.executions.contains(&ExecutionState::Quiescent));
    assert!(
        query
            .filter
            .interactions
            .contains(&InteractionState::Requested)
    );
    assert!(query.filter.health.contains(&HealthStatus::Healthy));
    assert_eq!(query.projection, SessionProjection::WithLabels);
    assert_eq!(query.sort.field, SessionSortField::UpdatedAt);
    assert_eq!(query.sort.direction, SortDirection::Descending);

    let mut oversized = request;
    oversized.page_size = 101;
    assert!(boundary().session_query(&oversized).is_err());
}

#[test]
fn work_translation_preserves_linkage_targets_and_provenance() {
    let request = UpsertWorkItemRequest {
        mutation: MutationContext::default(),
        session_id: "session-1".to_owned(),
        item: WorkItemDto {
            owner: "workflow".to_owned(),
            item_id: "task-1".to_owned(),
            relationship: WorkRelationshipDto::DependsOn,
            title: "Compile release".to_owned(),
            description: Some("waiting for artifact".to_owned()),
            status: WorkStatusDto::Blocked,
            targets: vec![
                WorkTargetDto::SessionId {
                    session_id: "dependency".to_owned(),
                },
                WorkTargetDto::Labels {
                    labels: BTreeMap::from([("kind".to_owned(), "build".to_owned())]),
                },
                WorkTargetDto::External {
                    namespace: "ci".to_owned(),
                    selector: "run-42".to_owned(),
                },
            ],
            provenance: "example://workflow/tasks/task-1".to_owned(),
            created_at_millis: 10,
            updated_at_millis: 20,
            expires_at_millis: Some(1_000),
            metadata: BTreeMap::new(),
        },
    };

    let command = boundary()
        .upsert_work_item(request)
        .expect("valid work item");
    assert_eq!(command.item.relation(), WorkRelationKind::DependsOn);
    assert_eq!(command.item.status(), WorkStatus::Blocked);
    assert_eq!(
        command.item.provenance().as_str(),
        "example://workflow/tasks/task-1"
    );
    assert_eq!(command.item.targets().count(), 3);
    assert!(
        command
            .item
            .targets()
            .any(|target| matches!(target, WorkTarget::Labels(_)))
    );
}

#[test]
fn removal_translation_preserves_confirmation_evidence() {
    let command = boundary()
        .remove_session(RemoveSessionRequest {
            mutation: MutationContext::default(),
            session_id: "session-1".to_owned(),
            confirmation: RemovalConfirmationDto::Forced,
        })
        .expect("valid removal");

    assert_eq!(command.confirmation, RemovalConfirmation::Forced);
}

#[test]
fn event_translation_uses_the_central_kind_registry_and_enforces_bounds() {
    let request = ReadSessionEventsRequest {
        tail: false,
        include_observation: false,
        session_ids: vec!["session-1".to_owned(), "session-1".to_owned()],
        kinds: vec![SessionEventKindDto::LifecycleChanged],
        after_sequence: Some(41),
        limit: 25,
    };

    let translated = boundary()
        .session_event_read(&request)
        .expect("event request is valid");

    assert_eq!(translated.filter().sessions().len(), 1);
    assert!(
        translated
            .filter()
            .kinds()
            .contains(&DomainEventKind::LifecycleChanged)
    );
    assert_eq!(translated.limit(), 25);

    let mut oversized = request;
    oversized.limit = 101;
    assert!(boundary().session_event_read(&oversized).is_err());
}

#[test]
fn profile_translation_bounds_command_facts_pages_and_private_samples() {
    let page = boundary()
        .profile_page_size(&ListProfilesRequest {
            page_size: 25,
            cursor: None,
        })
        .expect("page size is valid");
    assert_eq!(page.get(), 25);

    let target = boundary()
        .profile_match_target(MatchProfileRequest {
            executable: "/usr/bin/agent".to_owned(),
            arguments: vec!["run".to_owned()],
            environment_names: vec!["CI".to_owned(), "CI".to_owned()],
        })
        .expect("match target is valid");
    assert_eq!(target.basename(), "agent");
    assert_eq!(target.environment_names().len(), 1);

    let request = TestProfileRequest {
        name: "agent".to_owned(),
        screen_text: "private sample".to_owned(),
        last_line: "private last line".to_owned(),
        cursor_at_end: true,
    };
    let (_, sample) = boundary().profile_test(request).expect("sample is valid");
    assert!(!format!("{sample:?}").contains("private"));

    let oversized = TestProfileRequest {
        name: "agent".to_owned(),
        screen_text: "x".repeat(1_025),
        last_line: String::new(),
        cursor_at_end: true,
    };
    assert!(boundary().profile_test(oversized).is_err());
}
