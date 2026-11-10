// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use undercurrent_protocol::{
    AgentEventsSpec, CloseStandardInputRequest, CreateDiagnosticBundleRequest, DiagnosticBundleDto,
    DiagnosticBundleTerminalContentDto, DiagnosticBundleTerminalContentRequestDto,
    DiagnosticPageDto, DiagnosticSectionDto, ExecutionContextDto, ForegroundAxisDto,
    ForegroundClaimDto, GetDiagnosticsRequest, GetExecutionContextRequest, InteractionModalityDto,
    InteractionStateDto, ListSessionsRequest, MutationContext, NoRestartReasonDto, ProductCodec,
    ProductCodecErrorCategory, ProductCodecLimits, ProductCodecStage, ProductErrorCategory,
    ProductOperation, ProductRequest, ProductResponse, ProductStreamError, ProductStreamItem,
    ProductStreamKind, ProductStreamOpenResponse, ProductStreamSpec, ProductSuccess,
    ProfileTestDispositionDto, ProfileTestDto, ProfileTestEvaluationDto, ProfileTestRuleKindDto,
    ProfileTestSelectionDto, ReadTerminalScreenDiffRequest, RestartStatusDto, ResynchronizationDto,
    SessionDeltaBatchDto, SessionEventKindDto, SessionEventsSpec, SessionFilterDto,
    SessionProjectionDto, SessionSortDto, SessionWatchSpec, TerminalOutputChunk,
    TerminalOutputCodec, TerminalOutputPageDto, TerminalScreenDiffDto, TerminalScreenSnapshot,
    TerminalScreenSnapshotDto, TerminalScreenUpdateDto, TerminalStreamItem,
};

fn codec(limit: usize) -> ProductCodec {
    ProductCodec::new(ProductCodecLimits::new(limit, limit, limit, limit).unwrap())
}

fn list_request() -> ProductRequest {
    ProductRequest::ListSessions(ListSessionsRequest {
        filter: SessionFilterDto {
            name: Some("worker-299".to_owned()),
            ..SessionFilterDto::default()
        },
        sort: SessionSortDto::default(),
        projection: SessionProjectionDto::Compact,
        page_size: 25,
        cursor: None,
        include_total: false,
    })
}

#[test]
fn restart_dispositions_have_stable_tagged_wire_shapes() {
    let cases = [
        (
            RestartStatusDto::Idle,
            serde_json::json!({ "state": "idle" }),
        ),
        (
            RestartStatusDto::Scheduled {
                attempt: 3,
                not_before_millis: 42,
            },
            serde_json::json!({
                "state": "scheduled",
                "attempt": 3,
                "not_before_millis": 42
            }),
        ),
        (
            RestartStatusDto::NotScheduled {
                reason: NoRestartReasonDto::SuccessfulExit,
            },
            serde_json::json!({
                "state": "not_scheduled",
                "reason": "successful_exit"
            }),
        ),
        (
            RestartStatusDto::Exhausted { attempts: 5 },
            serde_json::json!({ "state": "exhausted", "attempts": 5 }),
        ),
    ];

    for (status, expected) in cases {
        let encoded = serde_json::to_value(status).expect("restart status serializes");
        assert_eq!(encoded, expected);
        assert_eq!(
            serde_json::from_value::<RestartStatusDto>(encoded)
                .expect("restart status deserializes"),
            status
        );
    }
}

#[test]
fn finite_request_round_trip_preserves_typed_operation() {
    let codec = codec(4_096);
    let request = list_request();

    let encoded = codec.encode_request(&request).unwrap();
    let decoded = codec.decode_request(&encoded).unwrap();

    assert_eq!(decoded, request);
    assert_eq!(decoded.operation(), ProductOperation::ListSessions);
}

#[test]
fn standard_input_close_round_trips_as_a_distinct_typed_operation() {
    let codec = codec(4_096);
    let request = ProductRequest::CloseStandardInput(CloseStandardInputRequest {
        mutation: MutationContext::current(ProductOperation::CloseStandardInput),
        session_id: "session-eof".to_owned(),
    });

    let encoded = codec
        .encode_request(&request)
        .expect("close request encodes");
    let decoded = codec
        .decode_request(&encoded)
        .expect("close request decodes");

    assert_eq!(decoded, request);
    assert_eq!(decoded.operation(), ProductOperation::CloseStandardInput);
    assert!(String::from_utf8_lossy(&encoded).contains("close_standard_input"));
    assert!(
        !encoded.contains(&0x04),
        "EOF must not be smuggled as input bytes"
    );
}

#[test]
fn product_messages_expose_only_single_session_context() {
    let request = ProductRequest::GetExecutionContext(GetExecutionContextRequest {
        session_id: "session-1".to_owned(),
    });
    assert_eq!(request.target_session_id(), Some("session-1"));
    assert_eq!(list_request().target_session_id(), None);

    let terminal = ProductStreamSpec::TerminalOutput(undercurrent_protocol::TerminalOutputSpec {
        session_id: "session-2".to_owned(),
        initial: undercurrent_protocol::TerminalInitialModeDto::None,
    });
    assert_eq!(terminal.target_session_id(), Some("session-2"));
    assert_eq!(
        ProductStreamSpec::SessionEvents(SessionEventsSpec {
            session_ids: vec!["session-1".to_owned()],
            kinds: Vec::new(),
            after_sequence: None,
        })
        .target_session_id(),
        None
    );

    let response = ProductResponse::Error(undercurrent_protocol::ProductError {
        effect_certainty: None,
        operation: Some(ProductOperation::GetExecutionContext),
        category: ProductErrorCategory::NotFound,
        disposition: Some(undercurrent_protocol::ProductErrorDisposition::SemanticRejection),
        message: "session was not found".to_owned(),
        session_id: Some("session-1".to_owned()),
        retryable: false,
        retry_after_millis: None,
        limit: None,
        resynchronization: None,
    });
    assert_eq!(response.session_id(), Some("session-1"));
}

#[test]
fn private_execution_context_round_trips_without_debug_payload_exposure() {
    let codec = codec(4_096);
    let request = ProductRequest::GetExecutionContext(GetExecutionContextRequest {
        session_id: "session-1".to_owned(),
    });
    let decoded = codec
        .decode_request(&codec.encode_request(&request).expect("request encodes"))
        .expect("request decodes");
    assert_eq!(decoded, request);
    assert_eq!(decoded.operation(), ProductOperation::GetExecutionContext);

    let response = ProductResponse::Success(Box::new(ProductSuccess::GetExecutionContext(
        ExecutionContextDto {
            environment: BTreeMap::from([(
                "API_TOKEN".to_owned(),
                "private-environment-value".to_owned(),
            )]),
            working_directory: Some("/private/working-directory".to_owned()),
        },
    )));
    let decoded = codec
        .decode_response(&codec.encode_response(&response).expect("response encodes"))
        .expect("response decodes");
    assert_eq!(decoded, response);
    let debug = format!("{response:?}");
    assert!(!debug.contains("private-environment-value"));
    assert!(!debug.contains("working-directory"));
}

#[test]
fn startup_reconciliation_is_an_explicit_diagnostic_selection() {
    let codec = codec(4_096);
    let request = ProductRequest::GetDiagnostics(GetDiagnosticsRequest {
        sections: vec![DiagnosticSectionDto::Reconciliation],
        page_size: 25,
        cursor: None,
    });

    let decoded = codec
        .decode_request(&codec.encode_request(&request).unwrap())
        .unwrap();

    assert_eq!(decoded, request);
}

#[test]
fn diagnostic_bundle_is_bounded_redacted_by_default_and_explicit_for_one_session() {
    let codec = codec(8_192);
    let safe_request = CreateDiagnosticBundleRequest {
        sections: vec![DiagnosticSectionDto::Connections],
        page_size: 25,
        cursor: None,
        terminal_content: None,
    };
    safe_request.validate().expect("safe bundle request");
    let decoded = codec
        .decode_request(
            &codec
                .encode_request(&ProductRequest::CreateDiagnosticBundle(
                    safe_request.clone(),
                ))
                .expect("safe bundle encodes"),
        )
        .expect("safe bundle decodes");
    assert_eq!(
        decoded,
        ProductRequest::CreateDiagnosticBundle(safe_request)
    );

    let explicit_request = CreateDiagnosticBundleRequest {
        sections: DiagnosticSectionDto::ALL.to_vec(),
        page_size: 100,
        cursor: Some("bundle-page-2".to_owned()),
        terminal_content: Some(DiagnosticBundleTerminalContentRequestDto {
            session_id: "session-private".to_owned(),
            from_offset: 0,
            max_bytes: 16_384,
        }),
    };
    explicit_request
        .validate()
        .expect("explicit bundle request");
    assert_eq!(
        codec
            .decode_request(
                &codec
                    .encode_request(&ProductRequest::CreateDiagnosticBundle(
                        explicit_request.clone(),
                    ))
                    .expect("continued explicit bundle encodes"),
            )
            .expect("continued explicit bundle decodes"),
        ProductRequest::CreateDiagnosticBundle(explicit_request)
    );
    let private_bytes = b"private-terminal".to_vec();
    let response = ProductResponse::Success(Box::new(ProductSuccess::CreateDiagnosticBundle(
        DiagnosticBundleDto {
            diagnostics: DiagnosticPageDto {
                items: Vec::new(),
                next_cursor: None,
                captured_at_millis: 42,
            },
            terminal_content: Some(DiagnosticBundleTerminalContentDto {
                session_id: "session-private".to_owned(),
                page: TerminalOutputPageDto::Data {
                    from_offset: 0,
                    to_offset: u64::try_from(private_bytes.len()).expect("bounded length"),
                    next_offset: u64::try_from(private_bytes.len()).expect("bounded length"),
                    has_more: false,
                    payload_digest_sha256:
                        undercurrent_protocol::execution_output_payload_digest_sha256(
                            &private_bytes,
                        ),
                    bytes: private_bytes.clone(),
                    truncation: None,
                },
            }),
            terminal_content_redacted: false,
        },
    )));
    let encoded = codec
        .encode_response(&response)
        .expect("bundle response encodes");
    assert_eq!(
        codec
            .decode_response(&encoded)
            .expect("bundle response decodes"),
        response
    );
    let debug = format!("{response:?}");
    assert!(!debug.contains("private-terminal"));
    assert!(!debug.contains("session-private"));
}

#[test]
fn profile_test_response_round_trip_preserves_rule_explanation() {
    let codec = codec(4_096);
    let response =
        ProductResponse::Success(Box::new(ProductSuccess::TestProfile(ProfileTestDto {
            profile: "agent".to_owned(),
            evaluations: vec![ProfileTestEvaluationDto {
                rule: 2,
                kind: ProfileTestRuleKindDto::Foreground {
                    axis: ForegroundAxisDto::Interaction,
                },
                sample: Some(0),
                disposition: ProfileTestDispositionDto::Matched,
                matched_patterns: vec![1],
                excluded_by: None,
                matched_name_patterns: Vec::new(),
            }],
            selected_foreground: vec![ProfileTestSelectionDto {
                rule: 2,
                sample: 0,
                claim: ForegroundClaimDto::Interaction {
                    state: InteractionStateDto::Requested,
                    modality: Some(InteractionModalityDto::Options),
                    semantic: Some("agent.permission".to_owned()),
                },
                confidence_basis_points: 9_000,
            }],
        })));

    let encoded = codec.encode_response(&response).unwrap();
    let decoded = codec.decode_response(&encoded).unwrap();

    assert_eq!(decoded, response);
}

#[test]
fn screen_diff_request_and_update_round_trip_without_debug_payload_exposure() {
    let codec = codec(8 * 1_024);
    let request = ProductRequest::ReadTerminalScreenDiff(ReadTerminalScreenDiffRequest {
        session_id: "session-1".into(),
        checkpoint: "s0000000000000001".into(),
    });
    assert_eq!(
        codec
            .decode_request(&codec.encode_request(&request).unwrap())
            .unwrap(),
        request
    );
    assert_eq!(
        request.operation(),
        ProductOperation::ReadTerminalScreenDiff
    );

    let response = ProductResponse::Success(Box::new(ProductSuccess::ReadTerminalScreenDiff(
        TerminalScreenUpdateDto::Diff(TerminalScreenDiffDto {
            checkpoint: "s0000000000000002".into(),
            from_output_offset: 40,
            to_output_offset: 48,
            columns: 80,
            rows: 24,
            cursor_row: 2,
            cursor_column: 8,
            cursor_visible: true,
            alternate_screen: false,
            contents: b"private-screen-diff".to_vec(),
        }),
    )));
    let decoded = codec
        .decode_response(&codec.encode_response(&response).unwrap())
        .unwrap();
    assert_eq!(decoded, response);
    assert!(!format!("{response:?}").contains("private-screen-diff"));

    let fallback = TerminalScreenUpdateDto::Snapshot(TerminalScreenSnapshotDto {
        checkpoint: "s0000000000000003".into(),
        output_offset: 48,
        columns: 100,
        rows: 30,
        cursor_row: 0,
        cursor_column: 0,
        cursor_visible: true,
        alternate_screen: false,
        contents: b"private-replacement".to_vec(),
    });
    assert!(!format!("{fallback:?}").contains("private-replacement"));
}

#[test]
fn stream_spec_round_trip_preserves_one_selective_interest() {
    let codec = codec(4_096);
    let spec = ProductStreamSpec::SessionWatch(SessionWatchSpec {
        query: match list_request() {
            ProductRequest::ListSessions(request) => request,
            _ => unreachable!(),
        },
    });

    let encoded = codec.encode_stream_spec(&spec).unwrap();
    let decoded = codec.decode_stream_spec(&encoded).unwrap();

    assert_eq!(decoded, spec);
    assert_eq!(decoded.kind(), ProductStreamKind::SessionWatch);
}

#[test]
fn event_stream_checkpoint_is_optional_and_round_trips_when_present() {
    let codec = codec(4_096);
    let resumed = ProductStreamSpec::SessionEvents(SessionEventsSpec {
        session_ids: vec!["session-1".into()],
        kinds: vec![SessionEventKindDto::LifecycleChanged],
        after_sequence: Some(42),
    });
    let encoded = codec.encode_stream_spec(&resumed).unwrap();
    assert_eq!(codec.decode_stream_spec(&encoded).unwrap(), resumed);

    let live = codec
        .decode_stream_spec(br#"{"stream":"session_events","input":{"session_ids":[],"kinds":[]}}"#)
        .unwrap();
    assert!(matches!(
        live,
        ProductStreamSpec::SessionEvents(SessionEventsSpec {
            after_sequence: None,
            ..
        })
    ));
}

#[test]
fn agent_event_stream_round_trip_preserves_the_exact_fenced_attempt() {
    let codec = codec(4_096);
    let stream = ProductStreamSpec::AgentEvents(AgentEventsSpec {
        tenant_id: "tenant_0198a5bd-4a06-7ae9-9a0c-42532d56e7f7".into(),
        source_product: "example/workflow".into(),
        actor_context_ref: "trust://actors/operator-1".into(),
        authority_ref: "trust://decisions/agent-1".into(),
        undercurrent_provider_id: "prv_0198a5bd-2afd-7dd1-b6a3-1fb90c6f0a21".into(),
        undercurrent_provider_generation: 7,
        session_id: "session-agent-1".into(),
        attempt_id: "attempt-agent-1".into(),
        attempt_revision: 2,
        recovery_epoch: 3,
        fence: 4,
        request_digest_sha256: "a".repeat(64),
        after_sequence: 5,
    });

    let encoded = codec.encode_stream_spec(&stream).unwrap();
    let decoded = codec.decode_stream_spec(&encoded).unwrap();

    assert_eq!(decoded, stream);
    assert_eq!(decoded.kind(), ProductStreamKind::AgentEvents);
    assert_eq!(decoded.target_session_id(), Some("session-agent-1"));
}

#[test]
fn session_event_registry_is_complete_and_unknown_wire_names_are_rejected() {
    let names = SessionEventKindDto::ALL
        .into_iter()
        .map(SessionEventKindDto::as_str)
        .collect::<BTreeSet<_>>();
    assert_eq!(names.len(), SessionEventKindDto::ALL.len());
    let error = codec(4_096)
        .decode_stream_spec(
            br#"{"stream":"session_events","input":{"session_ids":[],"kinds":["invented_event"]}}"#,
        )
        .expect_err("unknown event kind must fail decoding");
    assert_eq!(error.category(), ProductCodecErrorCategory::Malformed);
}

#[test]
fn stream_open_error_keeps_stream_context_separate_from_finite_operations() {
    let codec = codec(4_096);
    let response = ProductStreamOpenResponse::Error(ProductStreamError {
        kind: Some(ProductStreamKind::TerminalOutput),
        category: ProductErrorCategory::ReplayUnavailable,
        disposition: Some(undercurrent_protocol::ProductErrorDisposition::Partial),
        message: "requested terminal offset is no longer retained".into(),
        session_id: Some("session-1".into()),
        retryable: true,
        retry_after_millis: None,
        limit: None,
        resynchronization: Some(ResynchronizationDto::RestartFromAvailableOffset),
    });

    let encoded = codec.encode_stream_open_response(&response).unwrap();
    assert_eq!(
        codec.decode_stream_open_response(&encoded).unwrap(),
        response
    );
}

#[test]
fn watched_query_transition_preserves_order_and_total_without_entity_deltas() {
    let codec = codec(4_096);
    let item = ProductStreamItem::SessionDeltaBatch(SessionDeltaBatchDto {
        from_revision: 41,
        to_revision: 42,
        deltas: Vec::new(),
        order: vec!["session-2".into(), "session-1".into()],
        total: Some(300),
    });

    let encoded = codec.encode_stream_item(&item).unwrap();
    let decoded = codec.decode_stream_item(&encoded).unwrap();

    assert!(decoded == item);
}

#[test]
fn payload_limits_apply_before_json_decoding() {
    let codec = codec(8);
    let error = codec
        .decode_request(br#"{"private":"payload"}"#)
        .unwrap_err();

    assert_eq!(error.stage(), ProductCodecStage::Request);
    assert_eq!(error.category(), ProductCodecErrorCategory::Capacity);
    assert_eq!(error.limit(), Some(8));
    assert!(!format!("{error:?}").contains("private"));
    assert!(!error.to_string().contains("private"));
}

#[test]
fn malformed_request_diagnostics_do_not_echo_payload() {
    let codec = codec(4_096);
    let error = codec
        .decode_request(br#"{"operation":"start_session","secret":"do-not-log"}"#)
        .unwrap_err();

    assert_eq!(error.category(), ProductCodecErrorCategory::Malformed);
    assert!(!format!("{error:?}").contains("do-not-log"));
    assert!(!error.to_string().contains("do-not-log"));
}

#[test]
fn terminal_output_is_raw_bounded_binary() {
    let codec = TerminalOutputCodec::new(32).unwrap();
    let chunk = TerminalOutputChunk::new(41, Arc::from(b"hello\0terminal".as_slice()));

    let encoded = codec.encode(&chunk).unwrap();
    let decoded = codec.decode(&encoded).unwrap();

    assert_eq!(decoded, chunk);
    assert_eq!(decoded.start_offset(), 41);
    assert_eq!(decoded.end_offset(), Some(55));
    assert!(encoded.ends_with(b"hello\0terminal"));
    assert_eq!(encoded.len(), TerminalOutputCodec::header_bytes() + 14);
}

#[test]
fn terminal_output_parts_are_byte_identical_for_empty_and_multipart_payloads() {
    let codec = TerminalOutputCodec::new(64).unwrap();
    let empty = codec
        .encode_output_parts(7, 0, std::iter::empty())
        .expect("empty parts encode");
    assert_eq!(
        empty,
        codec
            .encode(&TerminalOutputChunk::new(7, Arc::from(&b""[..])))
            .expect("empty chunk encodes")
    );

    let parts: [&[u8]; 4] = [b"hello", b"\0", b"term", b"inal"];
    let expected = codec
        .encode(&TerminalOutputChunk::new(
            41,
            Arc::from(&b"hello\0terminal"[..]),
        ))
        .expect("contiguous chunk encodes");
    let encoded = codec
        .encode_output_parts(41, 14, parts)
        .expect("multipart chunk encodes");
    assert_eq!(encoded, expected);
    assert_eq!(
        codec
            .decode(&encoded)
            .expect("multipart chunk decodes")
            .bytes(),
        b"hello\0terminal"
    );
}

#[test]
fn terminal_output_parts_enforce_declared_capacity_offset_and_redaction_rules() {
    let codec = TerminalOutputCodec::new(64).unwrap();
    let private = &b"private-terminal-payload"[..];
    let underdeclared = codec
        .encode_output_parts(0, private.len() - 1, [private])
        .expect_err("underdeclared parts must fail");
    let overdeclared = codec
        .encode_output_parts(0, private.len() + 1, [private])
        .expect_err("overdeclared parts must fail");
    for error in [&underdeclared, &overdeclared] {
        assert_eq!(error.stage(), ProductCodecStage::TerminalOutput);
        assert_eq!(error.category(), ProductCodecErrorCategory::Malformed);
        assert!(!format!("{error:?}").contains("private-terminal-payload"));
        assert!(!error.to_string().contains("private-terminal-payload"));
    }

    let capacity = TerminalOutputCodec::new(4)
        .unwrap()
        .encode_output_parts(0, private.len(), [private])
        .expect_err("declared capacity must fail");
    assert_eq!(capacity.category(), ProductCodecErrorCategory::Capacity);
    assert_eq!(capacity.limit(), Some(4));
    assert!(!format!("{capacity:?}").contains("private-terminal-payload"));

    let offset = codec
        .encode_output_parts(u64::MAX, 1, [&b"x"[..]])
        .expect_err("exhausted output offset must fail");
    assert_eq!(offset.stage(), ProductCodecStage::TerminalOutput);
    assert_eq!(offset.category(), ProductCodecErrorCategory::Malformed);
}

#[test]
fn terminal_output_rejects_declared_length_mismatch() {
    let codec = TerminalOutputCodec::new(32).unwrap();
    let mut encoded = codec
        .encode(&TerminalOutputChunk::new(0, Arc::from(b"data".as_slice())))
        .unwrap();
    encoded.pop();

    let error = codec.decode(&encoded).unwrap_err();
    assert_eq!(error.stage(), ProductCodecStage::TerminalOutput);
    assert_eq!(error.category(), ProductCodecErrorCategory::Malformed);
}

#[test]
fn terminal_screen_is_a_distinct_bounded_binary_item() {
    let codec = TerminalOutputCodec::new(64).unwrap();
    let snapshot = TerminalScreenSnapshot::new(
        41,
        80,
        24,
        3,
        12,
        true,
        false,
        Arc::from(b"\x1b[H\x1b[Jhello".as_slice()),
    )
    .unwrap();

    let encoded = codec
        .encode_item(&TerminalStreamItem::Screen(snapshot.clone()))
        .unwrap();
    let decoded = codec.decode_item(&encoded).unwrap();

    assert_eq!(decoded, TerminalStreamItem::Screen(snapshot));
    assert_eq!(encoded[4], 2);
    assert_eq!(
        encoded.len(),
        TerminalOutputCodec::screen_header_bytes() + 11
    );
    assert!(codec.decode(&encoded).is_err());
}

#[test]
fn terminal_screen_rejects_invalid_flags_and_geometry() {
    let codec = TerminalOutputCodec::new(64).unwrap();
    let snapshot = TerminalScreenSnapshot::new(
        0,
        80,
        24,
        0,
        0,
        true,
        false,
        Arc::from(b"screen".as_slice()),
    )
    .unwrap();
    let mut encoded = codec
        .encode_item(&TerminalStreamItem::Screen(snapshot))
        .unwrap();
    encoded[21] |= 0x80;
    assert_eq!(
        codec.decode_item(&encoded).unwrap_err().category(),
        ProductCodecErrorCategory::Malformed
    );

    encoded[21] = 0;
    encoded[13..15].copy_from_slice(&0_u16.to_be_bytes());
    assert_eq!(
        codec.decode_item(&encoded).unwrap_err().category(),
        ProductCodecErrorCategory::Malformed
    );
}

#[test]
fn terminal_screen_capacity_is_enforced_without_payload_diagnostics() {
    let codec = TerminalOutputCodec::new(4).unwrap();
    let snapshot = TerminalScreenSnapshot::new(
        0,
        80,
        24,
        0,
        0,
        true,
        false,
        Arc::from(b"private-screen".as_slice()),
    )
    .unwrap();
    let error = codec
        .encode_item(&TerminalStreamItem::Screen(snapshot))
        .unwrap_err();

    assert_eq!(error.category(), ProductCodecErrorCategory::Capacity);
    assert_eq!(error.limit(), Some(4));
    assert!(!format!("{error:?}").contains("private-screen"));
}

#[test]
fn operation_and_stream_registries_are_unique_and_complete() {
    let mut operations = ProductOperation::ALL
        .into_iter()
        .map(ProductOperation::as_str)
        .collect::<Vec<_>>();
    operations.sort_unstable();
    operations.dedup();
    assert_eq!(operations.len(), ProductOperation::ALL.len());

    let mut streams = ProductStreamKind::ALL
        .into_iter()
        .map(ProductStreamKind::as_str)
        .collect::<Vec<_>>();
    streams.sort_unstable();
    streams.dedup();
    assert_eq!(streams.len(), ProductStreamKind::ALL.len());
}

#[test]
fn product_error_registry_is_unique() {
    let names = ProductErrorCategory::ALL
        .map(ProductErrorCategory::as_str)
        .into_iter()
        .collect::<BTreeSet<_>>();
    assert_eq!(names.len(), ProductErrorCategory::ALL.len());
}
