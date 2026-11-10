// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde::Deserialize;
use undercurrent_daemon::{
    BoundedProductDispatcher, NoopProductDispatchTelemetry, PreparedProductStream,
    ProductActionExecutor, ProductDispatchEvent, ProductDispatchEventName,
    ProductDispatchTelemetry, ProductDispatcherLimits, ProductRequestContext, ProductStreamContext,
    ProductStreamPublisher,
};
use undercurrent_multiplex::{
    ClientMessage, InboundStreamItem, ReplayJournal, ReplayLimits, ReplayStreamKey,
    SchedulerLimits, SchedulerMessageLimits, ServerMessage, StreamGapReason, StreamRecoveryAction,
};
use undercurrent_protocol::{
    AuthenticatedPrincipalRef, AuthenticatedRequestContext, AuthorityRefDto,
    CancelRuntimeAttemptRequest, CancelRuntimeAttemptResultDto, CommandSpecDto, DaemonStatusDto,
    EnvironmentSpecDto, ExecutionPolicyDto, HealthRulesDto, LifecycleStateDto, MutationContext,
    OutputCaptureModeDto, ProductCodec, ProductCodecLimits, ProductError, ProductErrorCategory,
    ProductErrorDisposition, ProductOperation, ProductRequest, ProductResponse, ProductStreamError,
    ProductStreamItem, ProductStreamKind, ProductStreamOpenResponse, ProductStreamSpec,
    ProfileBindingModeDto, RepairOperationAttemptRequest, ResynchronizationDto,
    RuntimeCancellationCommandDigestMaterial, RuntimeCancellationControlBindingDto,
    RuntimeCancellationModeDto, RuntimeCancellationPropagationPolicyDto,
    RuntimeCancellationSessionSnapshotDto, RuntimeCancellationStateDto,
    RuntimeCancellationTargetBindingDto, SessionEventDto, SessionEventKindDto, SessionEventsSpec,
    ShutdownAcceptedDto, ShutdownDaemonRequest, StartSessionRequest, TerminalOutputCodec,
    TerminalSizeDto, product_capabilities,
};
use undercurrent_sdk::{
    ClientRecoveryJitter, LocalClientConfig, LocalClientConnector, ProductClient,
    StandaloneClientProfile,
};
use undercurrent_transport_local::{AuthenticatedActionDispatcher, LocalServer, LocalServerLimits};
use undercurrent_transport_local::{
    BoundedRequestIdempotencyStore, RequestIdempotencyLimits, ServerAdmissionRejection,
    ServerAdmissionResponseKind, ServerConnectionSession, ServerDiagnosticLimits,
};
use undercurrent_wire::core::NamespacedName;
use undercurrent_wire::{
    CURRENT_PROTOCOL_VERSION, ClientId, ConnectionAgreement, ConnectionHello, ConnectionId,
    ConnectionLimits, ConnectionPolicy, HeartbeatPolicy, RequestId, ServerEpoch, SpanId, StreamId,
    StreamSequence, TraceContext, TraceId, connection_capabilities, negotiate_connection,
};
use undercurrent_wire::{DecodeStatus, WireCodec};

#[test]
fn typed_finite_request_executes_and_settles_through_one_bounded_worker() {
    let executor = Arc::new(RecordingExecutor::default());
    let telemetry = Arc::new(RecordingDispatchTelemetry::default());
    let dispatcher = dispatcher_with_telemetry(executor.clone(), telemetry.clone(), 1, 4);
    let connection = Arc::new(session(8, 8));
    let request = ProductRequest::GetDaemonStatus;

    dispatch_request(&dispatcher, &connection, request_id(1), &request).unwrap();
    let response = next_response(&connection);

    assert!(matches!(
        product_codec().decode_response(&response).unwrap(),
        ProductResponse::Success(success)
            if matches!(*success, undercurrent_protocol::ProductSuccess::GetDaemonStatus(_))
    ));
    assert_eq!(executor.executions.load(Ordering::Acquire), 1);
    assert_eq!(connection.pressure().pending_requests, 0);
    assert!(telemetry.events().iter().any(|event| {
        event.name == ProductDispatchEventName::RequestCompleted
            && event.response_bytes == Some(response.len())
    }));
    dispatcher.shutdown();
}

#[test]
fn runtime_cancellation_replay_reauthorizes_instead_of_using_dispatch_cache() {
    let executor = Arc::new(RecordingExecutor::runtime_cancellation_revocation());
    let dispatcher = dispatcher(executor.clone(), 1, 4);
    let connection = Arc::new(session(8, 8));
    let request = runtime_cancellation_request();

    dispatch_request(&dispatcher, &connection, request_id(1), &request).unwrap();
    assert!(matches!(
        product_codec().decode_response(&next_response(&connection)).unwrap(),
        ProductResponse::Success(success)
            if matches!(*success, undercurrent_protocol::ProductSuccess::CancelRuntimeAttempt(_))
    ));

    dispatch_request(&dispatcher, &connection, request_id(2), &request).unwrap();
    let ProductResponse::Error(error) = product_codec()
        .decode_response(&next_response(&connection))
        .expect("revoked cancellation response")
    else {
        panic!("the second cancellation must reach the revoked authorizer")
    };
    assert_eq!(
        error.operation,
        Some(ProductOperation::CancelRuntimeAttempt)
    );
    assert_eq!(error.category, ProductErrorCategory::PermissionDenied);
    assert_eq!(executor.executions.load(Ordering::Acquire), 2);
    dispatcher.shutdown();
}

#[test]
fn session_revision_preconditions_require_negotiation_before_dispatch() {
    let executor = Arc::new(RecordingExecutor::default());
    let dispatcher = dispatcher(executor.clone(), 1, 4);
    let connection = Arc::new(session(8, 8));
    let requests = [
        ProductRequest::StopSession(undercurrent_protocol::StopSessionRequest {
            graceful_only: false,
            mutation: MutationContext::current(ProductOperation::StopSession),
            session_id: "guarded-session".into(),
            expected_revision: Some(3),
            force: false,
        }),
        ProductRequest::SendTerminalInput(undercurrent_protocol::SendTerminalInputRequest {
            mutation: MutationContext::current(ProductOperation::SendTerminalInput),
            session_id: "guarded-session".into(),
            expected_revision: Some(3),
            delivery_id: Some("guarded-delivery".into()),
            items: vec![undercurrent_protocol::TerminalInputDto::Text {
                text: "input".into(),
            }],
            observation_policy: Default::default(),
        }),
    ];
    for (index, request) in requests.into_iter().cycle().take(4).enumerate() {
        dispatch_request(&dispatcher, &connection, request_id(index + 1), &request).unwrap();
        let ProductResponse::Error(error) = product_codec()
            .decode_response(&next_response(&connection))
            .unwrap()
        else {
            panic!("unguaranteed revision checks must be rejected");
        };
        assert_eq!(error.operation, Some(request.operation()));
        assert_eq!(error.category, ProductErrorCategory::ProtocolMismatch);
        assert!(!error.retryable);
    }
    assert_eq!(executor.executions.load(Ordering::Acquire), 0);
    assert_eq!(connection.pressure().pending_requests, 0);
    dispatcher.shutdown();
}

#[test]
fn opaque_start_without_negotiated_capability_is_rejected_before_idempotency_and_execution() {
    let executor = Arc::new(RecordingExecutor::default());
    let dispatcher = dispatcher(executor.clone(), 1, 4);
    let connection = Arc::new(session(8, 8));
    let request = opaque_start_request();

    for index in 1..=2 {
        dispatch_request(&dispatcher, &connection, request_id(index), &request).unwrap();
        let ProductResponse::Error(error) = product_codec()
            .decode_response(&next_response(&connection))
            .expect("protocol rejection")
        else {
            panic!("opaque start must be rejected")
        };
        assert_eq!(error.operation, Some(ProductOperation::StartSession));
        assert_eq!(error.category, ProductErrorCategory::ProtocolMismatch);
        assert!(!error.retryable);
    }
    assert_eq!(executor.executions.load(Ordering::Acquire), 0);
    assert_eq!(connection.pressure().pending_requests, 0);
    dispatcher.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn daemon_control_public_values_are_bounded_and_path_free_over_sdk_local() {
    let executor = Arc::new(RecordingExecutor::live_local());
    let dispatcher = Arc::new(dispatcher(executor.clone(), 1, 4));
    let profile = StandaloneClientProfile::standard("daemon-control-live-client")
        .expect("standalone product profile");
    let offered = profile.hello().limits();
    let policy = ConnectionPolicy::new(
        CURRENT_PROTOCOL_VERSION,
        BTreeSet::from([
            capability(connection_capabilities::MULTIPLEX),
            capability(connection_capabilities::FLOW_CONTROL),
            capability(connection_capabilities::STREAM_RESUME),
            capability(connection_capabilities::BINARY_PAYLOADS),
            capability(connection_capabilities::TRACE_CONTEXT),
            capability(product_capabilities::ERROR_DISPOSITION),
        ]),
        ConnectionLimits::new(
            offered.max_frame_bytes(),
            offered.max_logical_streams(),
            offered.max_pending_requests(),
            offered.connection_queue_bytes(),
            offered.stream_queue_bytes(),
            offered.initial_stream_window_bytes(),
        )
        .expect("server connection limits"),
        HeartbeatPolicy::new(60_000, 120_000).expect("server heartbeat"),
        Some(60_000),
    )
    .expect("server connection policy");
    let limits = LocalServerLimits::new(
        65_536,
        Duration::from_secs(2),
        Duration::from_secs(2),
        offered.max_frame_bytes() as usize,
        8,
        32,
        16_384,
        SchedulerMessageLimits::new(16_384, 1_024, [2_048, 4_096, 4_096, 6_144])
            .expect("scheduler message limits"),
        ReplayLimits::new(
            1_024,
            u64::from(offered.max_frame_bytes()),
            u64::from(offered.max_frame_bytes()) * 2,
            u64::from(offered.max_frame_bytes()),
        )
        .expect("replay limits"),
        ServerDiagnosticLimits::new(512, 512).expect("server diagnostic limits"),
        &policy,
    )
    .expect("Local server limits");
    let local_home = physical_tempdir().expect("local home");
    let address = undercurrent_transport_local::LocalEndpoint::new(
        local_home.path().join("run/control.sock"),
    )
    .expect("endpoint");
    let server = Arc::new(
        LocalServer::bind(
            address,
            ServerEpoch::new("epoch-1").expect("server epoch"),
            policy,
            limits,
            dispatcher.clone(),
        )
        .expect("bind live product server"),
    );
    let endpoint = server.endpoint().path().to_path_buf();
    let running = server.clone();
    let server_worker = tokio::spawn(async move { running.run().await });
    assert!(server.wait_until_running(Duration::from_secs(2)).await);

    let product_runtime = profile.product();
    let response_limit = product_runtime.product_codec().limits().response_bytes();
    let config = LocalClientConfig::new(endpoint, profile.hello().clone(), profile.local())
        .expect("live product client configuration");
    let client = tokio::task::spawn_blocking(move || {
        ProductClient::connect(
            Arc::new(LocalClientConnector::new(config)),
            product_runtime,
            Arc::new(ZeroJitter),
        )
    })
    .await
    .expect("product client connector task")
    .expect("live product client");

    let undercurrent_protocol::ProductSuccess::GetDaemonStatus(status) = client
        .request(ProductRequest::GetDaemonStatus, Duration::from_secs(2))
        .expect("typed daemon status over Local")
    else {
        panic!("unexpected daemon-status response")
    };
    let status_payload = product_codec()
        .encode_response(&ProductResponse::Success(Box::new(
            undercurrent_protocol::ProductSuccess::GetDaemonStatus(status.clone()),
        )))
        .expect("encode bounded public status");
    let status_text = std::str::from_utf8(&status_payload).expect("status JSON");
    assert!(status_payload.len() <= response_limit);
    assert!(status.server_epoch.len() <= 64);
    assert!(status.physical_connections <= 8);
    assert!(status.logical_streams <= offered.max_logical_streams());
    assert!(status.managed_sessions <= 512);
    assert!(!status_text.contains(['/', '\\']));
    assert!(!status_text.contains("path"));
    assert!(!status_text.contains("endpoint"));

    let undercurrent_protocol::ProductSuccess::ShutdownDaemon(accepted) = client
        .request(
            ProductRequest::ShutdownDaemon(ShutdownDaemonRequest {
                mode: Default::default(),
                mutation: MutationContext {
                    idempotency_key: Some("live-shutdown-once".into()),
                    ..MutationContext::current(ProductOperation::ShutdownDaemon)
                },
                drain_timeout_millis: 1_000,
            }),
            Duration::from_secs(2),
        )
        .expect("typed shutdown acknowledgement over Local")
    else {
        panic!("unexpected daemon-shutdown response")
    };
    let shutdown_payload = product_codec()
        .encode_response(&ProductResponse::Success(Box::new(
            undercurrent_protocol::ProductSuccess::ShutdownDaemon(accepted),
        )))
        .expect("encode bounded shutdown acknowledgement");
    let shutdown_text = std::str::from_utf8(&shutdown_payload).expect("shutdown JSON");
    assert!(shutdown_payload.len() <= response_limit);
    assert_eq!(accepted.drain_deadline_millis, 1_000);
    assert!(!shutdown_text.contains(['/', '\\']));
    assert!(!shutdown_text.contains("path"));
    assert!(!shutdown_text.contains("endpoint"));
    assert_eq!(executor.executions.load(Ordering::Acquire), 2);

    client.close();
    server.shutdown();
    let report = server_worker
        .await
        .expect("live product server task")
        .expect("live product server shutdown");
    assert_eq!(report.accepted, 1);
    assert_eq!(report.rejected, 0);
    assert_eq!(report.failed, 0);
    dispatcher.shutdown();
}

#[test]
fn malformed_product_bytes_return_a_typed_error_without_executor_access() {
    let executor = Arc::new(RecordingExecutor::default());
    let dispatcher = dispatcher(executor.clone(), 1, 4);
    let connection = Arc::new(session(8, 8));
    let request_id = request_id(1);
    let action = connection
        .accept(ClientMessage::Request {
            request_id: request_id.clone(),
            trace_context: trace_context(1),
            payload: Arc::from(br#"{"private":"do-not-log"}"#.as_slice()),
        })
        .unwrap()
        .remove(0);

    dispatcher
        .dispatch_authenticated(authentication(), connection.clone(), action)
        .unwrap();
    let response = product_codec()
        .decode_response(&next_response(&connection))
        .unwrap();

    assert!(matches!(
        response,
        ProductResponse::Error(error)
            if error.operation.is_none()
                && error.category == undercurrent_protocol::ProductErrorCategory::InvalidRequest
    ));
    assert_eq!(executor.executions.load(Ordering::Acquire), 0);
    dispatcher.shutdown();
}

#[test]
fn repair_contract_rejections_precede_request_decoded_and_executor_access() {
    let executor = Arc::new(RecordingExecutor::default());
    let telemetry = Arc::new(RecordingDispatchTelemetry::default());
    let dispatcher = dispatcher_with_telemetry(executor.clone(), telemetry.clone(), 1, 8);
    let connection = Arc::new(session(16, 8));
    let current = serde_json::to_value(ProductRequest::RepairOperationAttempt(
        RepairOperationAttemptRequest::new("repair-contract-gate".to_owned())
            .expect("current repair request"),
    ))
    .expect("current repair JSON");

    let mut missing = current.clone();
    missing["input"]
        .as_object_mut()
        .expect("repair input")
        .remove("contract");

    let mut retired_v1 = current.clone();
    retired_v1["input"]["contract"]["contract_version"] = serde_json::json!("99.0.0");

    let mut future = current.clone();
    future["input"]["contract"]["contract_version"] = serde_json::json!("4.0.0");

    let mut mismatched = current.clone();
    mismatched["input"]["contract"]["capability"] = serde_json::json!("start_session");

    let mut malformed = current.clone();
    malformed["input"]["contract"] = serde_json::json!("not-a-contract-object");

    let mut peer_effect_claim = current;
    peer_effect_claim["input"]
        .as_object_mut()
        .expect("repair input")
        .insert(
            "effect_certainty".to_owned(),
            serde_json::json!("known_applied"),
        );

    for (index, invalid) in [
        missing,
        retired_v1,
        future,
        mismatched,
        malformed,
        peer_effect_claim,
    ]
    .into_iter()
    .enumerate()
    {
        let request_number = index + 1;
        let action = connection
            .accept(ClientMessage::Request {
                request_id: request_id(request_number),
                trace_context: trace_context(request_number),
                payload: Arc::from(
                    serde_json::to_vec(&invalid)
                        .expect("invalid repair JSON")
                        .into_boxed_slice(),
                ),
            })
            .expect("connection admission")
            .remove(0);
        dispatcher
            .dispatch_authenticated(authentication(), connection.clone(), action)
            .expect("bounded dispatcher admission");
        let ProductResponse::Error(error) = product_codec()
            .decode_response(&next_response(&connection))
            .expect("typed contract rejection")
        else {
            panic!("invalid repair contract must be rejected");
        };
        assert_eq!(error.category, ProductErrorCategory::InvalidRequest);
    }

    assert_eq!(executor.executions.load(Ordering::Acquire), 0);
    assert!(
        telemetry
            .events()
            .iter()
            .all(|event| event.name != ProductDispatchEventName::RequestDecoded),
        "invalid repair metadata reached RequestDecoded"
    );
    dispatcher.shutdown();
}

#[test]
fn exact_idempotent_retry_replays_response_without_second_execution() {
    let executor = Arc::new(RecordingExecutor::default());
    let dispatcher = dispatcher(executor.clone(), 1, 4);
    let connection = Arc::new(session(8, 8));
    let request = ProductRequest::ShutdownDaemon(ShutdownDaemonRequest {
        mode: Default::default(),
        mutation: MutationContext {
            idempotency_key: Some("shutdown-once".into()),
            ..MutationContext::current(ProductOperation::ShutdownDaemon)
        },
        drain_timeout_millis: 1_000,
    });

    dispatch_request(&dispatcher, &connection, request_id(1), &request).unwrap();
    let first = next_response(&connection);
    dispatch_request(&dispatcher, &connection, request_id(2), &request).unwrap();
    let second = next_response(&connection);

    assert_eq!(first, second);
    assert_eq!(executor.executions.load(Ordering::Acquire), 1);
    dispatcher.shutdown();
}

#[test]
fn negotiated_error_disposition_preserves_strict_legacy_finite_stream_and_fallback_shapes() {
    let executor = Arc::new(RecordingExecutor::default());
    let profile_dispatcher = dispatcher(executor.clone(), 1, 8);

    for (index, client_offers, server_supports, negotiated) in [
        (1, false, false, false),
        (2, false, true, false),
        (3, true, false, false),
        (4, true, true, true),
    ] {
        let connection = Arc::new(session_for_profile(8, 8, client_offers, server_supports));
        assert_eq!(
            connection
                .agreement()
                .capabilities()
                .iter()
                .any(|value| value.as_str() == product_capabilities::ERROR_DISPOSITION),
            negotiated
        );

        let action = connection
            .accept(ClientMessage::Request {
                request_id: request_id(index * 2),
                trace_context: trace_context(index * 2),
                payload: Arc::from(br#"{"private":"do-not-log"}"#.as_slice()),
            })
            .unwrap()
            .remove(0);
        profile_dispatcher
            .dispatch_authenticated(authentication(), connection.clone(), action)
            .unwrap();
        let finite = next_response(&connection);
        if negotiated {
            let ProductResponse::Error(error) = product_codec().decode_response(&finite).unwrap()
            else {
                panic!("malformed request must return an error");
            };
            assert_eq!(
                error.disposition,
                Some(ProductErrorDisposition::SemanticRejection)
            );
        } else {
            let StrictLegacyProductResponse::Error(error) =
                serde_json::from_slice(&finite).expect("strict legacy finite reader")
            else {
                panic!("malformed request must return a legacy error");
            };
            assert_eq!(error.category, ProductErrorCategory::InvalidRequest);
        }

        let action = connection
            .accept(ClientMessage::OpenStream {
                request_id: request_id(index * 2 + 1),
                stream_id: stream_id(index),
                trace_context: trace_context(index * 2 + 1),
                payload: Arc::from(br#"{"private":"do-not-log"}"#.as_slice()),
            })
            .unwrap()
            .remove(0);
        profile_dispatcher
            .dispatch_authenticated(authentication(), connection.clone(), action)
            .unwrap();
        let stream = next_response(&connection);
        if negotiated {
            let ProductStreamOpenResponse::Error(error) = product_codec()
                .decode_stream_open_response(&stream)
                .unwrap()
            else {
                panic!("malformed stream must return an error");
            };
            assert_eq!(
                error.disposition,
                Some(ProductErrorDisposition::SemanticRejection)
            );
        } else {
            let StrictLegacyStreamOpenResponse::Error(error) =
                serde_json::from_slice(&stream).expect("strict legacy stream reader")
            else {
                panic!("malformed stream must return a legacy error");
            };
            assert_eq!(error.category, ProductErrorCategory::InvalidRequest);
        }
    }
    assert_eq!(executor.executions.load(Ordering::Acquire), 0);
    profile_dispatcher.shutdown();

    let executor = Arc::new(RecordingExecutor::oversized_error());
    let dispatcher = dispatcher(executor, 1, 4);
    for (index, negotiated) in [(1, false), (2, true)] {
        let connection = Arc::new(session_for_profile(8, 8, negotiated, negotiated));
        dispatch_request(
            &dispatcher,
            &connection,
            request_id(index),
            &ProductRequest::GetDaemonStatus,
        )
        .unwrap();
        let fallback = next_response(&connection);
        if negotiated {
            let ProductResponse::Error(error) = product_codec().decode_response(&fallback).unwrap()
            else {
                panic!("oversized response must use the typed fallback");
            };
            assert_eq!(error.category, ProductErrorCategory::Internal);
            assert_eq!(
                error.disposition,
                Some(ProductErrorDisposition::InfrastructureFailure)
            );
        } else {
            let StrictLegacyProductResponse::Error(error) =
                serde_json::from_slice(&fallback).expect("strict legacy fallback reader")
            else {
                panic!("oversized response must use the legacy fallback");
            };
            assert_eq!(error.category, ProductErrorCategory::Internal);
        }
    }
    dispatcher.shutdown();
}

#[test]
fn negotiated_admission_rejections_preserve_strict_legacy_finite_and_stream_shapes() {
    let dispatcher = dispatcher(Arc::new(RecordingExecutor::default()), 1, 4);
    for (profile, negotiated) in [(1, false), (2, true)] {
        for (offset, response_kind) in [
            (0, ServerAdmissionResponseKind::FiniteRequest),
            (1, ServerAdmissionResponseKind::StreamOpen),
        ] {
            let connection = Arc::new(session_for_profile(1, 8, negotiated, negotiated));
            connection
                .accept(ClientMessage::Request {
                    request_id: request_id(profile * 10 + offset * 2),
                    trace_context: trace_context(profile * 10 + offset * 2),
                    payload: Arc::from(b"pending".as_slice()),
                })
                .expect("filler request enters pending work");
            let rejected_id = request_id(profile * 10 + offset * 2 + 1);
            let rejected_message = match response_kind {
                ServerAdmissionResponseKind::FiniteRequest => ClientMessage::Request {
                    request_id: rejected_id.clone(),
                    trace_context: trace_context(profile * 10 + offset * 2 + 1),
                    payload: Arc::from(b"finite".as_slice()),
                },
                ServerAdmissionResponseKind::StreamOpen => ClientMessage::OpenStream {
                    request_id: rejected_id.clone(),
                    stream_id: stream_id(profile * 10 + offset),
                    trace_context: trace_context(profile * 10 + offset * 2 + 1),
                    payload: Arc::from(b"stream".as_slice()),
                },
            };
            let overload = connection
                .accept(rejected_message)
                .expect_err("pending limit rejects before product admission")
                .overload()
                .expect("pending capacity failure exposes overload");
            assert!(
                dispatcher
                    .reject_authenticated_admission(
                        authentication(),
                        connection.clone(),
                        ServerAdmissionRejection::new(rejected_id, response_kind, overload,),
                    )
                    .expect("typed admission rejection")
            );
            let payload = next_response(&connection);
            match (response_kind, negotiated) {
                (ServerAdmissionResponseKind::FiniteRequest, true) => {
                    let ProductResponse::Error(error) =
                        product_codec().decode_response(&payload).unwrap()
                    else {
                        panic!("admission rejection must be finite error");
                    };
                    assert_eq!(
                        error.disposition,
                        Some(ProductErrorDisposition::CapacityRejected)
                    );
                }
                (ServerAdmissionResponseKind::StreamOpen, true) => {
                    let ProductStreamOpenResponse::Error(error) = product_codec()
                        .decode_stream_open_response(&payload)
                        .unwrap()
                    else {
                        panic!("admission rejection must be stream error");
                    };
                    assert_eq!(
                        error.disposition,
                        Some(ProductErrorDisposition::CapacityRejected)
                    );
                }
                (ServerAdmissionResponseKind::FiniteRequest, false) => {
                    let StrictLegacyProductResponse::Error(error) =
                        serde_json::from_slice(&payload).expect("strict legacy admission reader")
                    else {
                        panic!("legacy admission rejection must be finite error");
                    };
                    assert_eq!(error.category, ProductErrorCategory::CapacityExceeded);
                }
                (ServerAdmissionResponseKind::StreamOpen, false) => {
                    let StrictLegacyStreamOpenResponse::Error(error) =
                        serde_json::from_slice(&payload)
                            .expect("strict legacy stream admission reader")
                    else {
                        panic!("legacy admission rejection must be stream error");
                    };
                    assert_eq!(error.category, ProductErrorCategory::CapacityExceeded);
                }
            }
        }
    }
    dispatcher.shutdown();
}

#[test]
fn idempotent_response_loss_replays_once_across_capability_upgrade_and_downgrade() {
    let executor = Arc::new(RecordingExecutor::unknown_effect_error());
    let dispatcher = dispatcher(executor.clone(), 1, 8);

    let downgrade = ProductRequest::ShutdownDaemon(ShutdownDaemonRequest {
        mode: Default::default(),
        mutation: MutationContext {
            idempotency_key: Some("capable-to-legacy".into()),
            ..MutationContext::current(ProductOperation::ShutdownDaemon)
        },
        drain_timeout_millis: 1_000,
    });
    let capable = Arc::new(session_for_profile(8, 8, true, true));
    dispatch_request(&dispatcher, &capable, request_id(1), &downgrade).unwrap();
    wait_until(|| {
        executor.executions.load(Ordering::Acquire) == 1 && capable.pressure().pending_requests == 0
    });
    capable.close();

    let legacy = Arc::new(session_for_profile(8, 8, false, true));
    dispatch_request(&dispatcher, &legacy, request_id(2), &downgrade).unwrap();
    let StrictLegacyProductResponse::Error(error) =
        serde_json::from_slice(&next_response(&legacy)).expect("strict legacy replay")
    else {
        panic!("downgraded replay must remain an error");
    };
    assert_eq!(error.category, ProductErrorCategory::FailedPrecondition);
    assert_eq!(executor.executions.load(Ordering::Acquire), 1);

    let upgrade = ProductRequest::ShutdownDaemon(ShutdownDaemonRequest {
        mode: Default::default(),
        mutation: MutationContext {
            idempotency_key: Some("legacy-to-capable".into()),
            ..MutationContext::current(ProductOperation::ShutdownDaemon)
        },
        drain_timeout_millis: 1_000,
    });
    let legacy = Arc::new(session_for_profile(8, 8, false, true));
    dispatch_request(&dispatcher, &legacy, request_id(3), &upgrade).unwrap();
    wait_until(|| {
        executor.executions.load(Ordering::Acquire) == 2 && legacy.pressure().pending_requests == 0
    });
    legacy.close();

    let capable = Arc::new(session_for_profile(8, 8, true, true));
    dispatch_request(&dispatcher, &capable, request_id(4), &upgrade).unwrap();
    let ProductResponse::Error(error) = product_codec()
        .decode_response(&next_response(&capable))
        .expect("capable replay")
    else {
        panic!("upgraded replay must remain an error");
    };
    assert_eq!(
        error.disposition,
        Some(ProductErrorDisposition::UnknownInconclusive)
    );
    assert_eq!(executor.executions.load(Ordering::Acquire), 2);
    dispatcher.shutdown();
}

#[test]
fn cancellation_is_visible_without_waiting_behind_the_worker_queue() {
    let executor = Arc::new(RecordingExecutor::blocking());
    let dispatcher = dispatcher(executor.clone(), 1, 2);
    let connection = Arc::new(session(8, 8));
    let request = ProductRequest::GetDaemonStatus;
    let request_id = request_id(1);

    dispatch_request(&dispatcher, &connection, request_id.clone(), &request).unwrap();
    executor.wait_started();
    let action = connection
        .accept(ClientMessage::CancelRequest {
            request_id: request_id.clone(),
        })
        .unwrap()
        .remove(0);
    dispatcher
        .dispatch_authenticated(authentication(), connection.clone(), action)
        .unwrap();

    let (response_id, payload) = next_correlated_response(&connection);
    assert_eq!(response_id, request_id);
    let ProductResponse::Error(error) = product_codec()
        .decode_response(&payload)
        .expect("cancellation response")
    else {
        panic!("cancellation must be a typed terminal error");
    };
    assert_eq!(error.operation, None);
    assert_eq!(error.category, ProductErrorCategory::Cancelled);
    assert_eq!(
        error.disposition,
        Some(ProductErrorDisposition::UnknownInconclusive)
    );
    assert!(
        connection
            .accept(ClientMessage::CancelRequest {
                request_id: request_id.clone(),
            })
            .unwrap()
            .is_empty()
    );

    wait_until(|| executor.cancel_observed.load(Ordering::Acquire));
    wait_until(|| dispatcher.pressure().active_workers == 0);
    assert_eq!(connection.pressure().pending_requests, 0);
    assert!(!connection.is_closed());
    dispatcher.shutdown();
}

#[test]
fn saturated_worker_queue_rejects_admission_with_a_bounded_error() {
    let executor = Arc::new(RecordingExecutor::gated());
    let dispatcher = dispatcher(executor.clone(), 1, 1);
    let connection = Arc::new(session(8, 8));
    let request = ProductRequest::ListProfiles(undercurrent_protocol::ListProfilesRequest {
        page_size: 1,
        cursor: None,
    });

    dispatch_request(&dispatcher, &connection, request_id(1), &request).unwrap();
    executor.wait_started();
    dispatch_request(&dispatcher, &connection, request_id(2), &request).unwrap();
    let error = dispatch_request(&dispatcher, &connection, request_id(3), &request).unwrap_err();

    assert_eq!(
        error.source().expect("dispatch source").to_string(),
        "product dispatcher queue capacity 1 is exhausted"
    );
    assert_eq!(dispatcher.pressure().queued_actions, 1);
    assert_eq!(dispatcher.pressure().rejected_actions, 1);
    connection.close();
    executor.release_gate();
    dispatcher.shutdown();
}

#[test]
fn prepared_stream_activates_then_publishes_on_the_same_connection() {
    let executor = Arc::new(RecordingExecutor::default());
    let dispatcher = dispatcher(executor.clone(), 1, 4);
    let connection = Arc::new(session(8, 8));
    let request_id = request_id(1);
    let stream_id = stream_id(1);
    let specification = ProductStreamSpec::SessionEvents(SessionEventsSpec {
        session_ids: vec!["session-1".into()],
        kinds: Vec::new(),
        after_sequence: None,
    });
    let payload = Arc::from(product_codec().encode_stream_spec(&specification).unwrap());
    let action = connection
        .accept(ClientMessage::OpenStream {
            request_id: request_id.clone(),
            stream_id: stream_id.clone(),
            trace_context: trace_context(1),
            payload,
        })
        .unwrap()
        .remove(0);

    dispatcher
        .dispatch_authenticated(authentication(), connection.clone(), action)
        .unwrap();
    let open_response = next_response(&connection);
    assert!(matches!(
        product_codec()
            .decode_stream_open_response(&open_response)
            .unwrap(),
        ProductStreamOpenResponse::Opened(opened)
            if opened.kind == undercurrent_protocol::ProductStreamKind::SessionEvents
    ));
    let publisher = executor.wait_publisher();
    let source_gap = publisher.publish_source_gap().unwrap();
    assert_eq!(source_gap.reason, StreamGapReason::SourceRetentionEvicted);
    assert_eq!(source_gap.recovery, StreamRecoveryAction::RequestSnapshot);
    let gap_write = connection.next_write().unwrap().expect("source gap");
    assert!(matches!(
        decode_server(gap_write.bytes()),
        ServerMessage::StreamItem {
            item: InboundStreamItem::Gap(gap),
            ..
        } if gap.first_missing.value() == 1
            && gap.last_missing.value() == 1
            && gap.reason == StreamGapReason::SourceRetentionEvicted
    ));
    gap_write.confirm_written().unwrap();
    let item = event_item("connection-1");
    publisher.publish(&item).unwrap();

    let (received_stream, sequence, payload) = next_stream_item(&connection);
    assert_eq!(received_stream, stream_id);
    assert_eq!(sequence.value(), 2);
    assert!(product_codec().decode_stream_item(&payload).unwrap() == item);
    assert_eq!(connection.pressure().logical_streams, 1);
    dispatcher.shutdown();
}

#[test]
fn resumed_dispatch_publisher_continues_after_a_partially_evicted_journal() {
    let journal = Arc::new(ReplayJournal::new(
        epoch(),
        ReplayLimits::new(4, 8_192, 8_192, 8_192).unwrap(),
    ));
    let stream = stream_id(1);
    let key = ReplayStreamKey::new(client_id(), stream.clone());
    journal
        .register_stream(key.clone(), StreamRecoveryAction::RequestSnapshot)
        .unwrap();
    let item = event_item("session-1");
    let payload: Arc<[u8]> = Arc::from(product_codec().encode_stream_item(&item).unwrap());
    let mut frame_bytes = 0;
    for sequence in 1..=3 {
        let encoded = WireCodec::new(8_192)
            .unwrap()
            .encode_server(&ServerMessage::StreamItem {
                stream_id: stream.clone(),
                item: InboundStreamItem::Data {
                    sequence: StreamSequence::new(sequence),
                    bytes: payload.clone(),
                    flow_controlled_bytes: 0,
                },
            })
            .unwrap();
        frame_bytes = encoded.len();
        journal
            .record(&key, StreamSequence::new(sequence), Arc::from(encoded))
            .unwrap();
    }
    // Another stream exhausts global retention, leaving exactly frames 2 and 3.
    let other = ReplayStreamKey::new(client_id(), stream_id(2));
    journal
        .register_stream(other.clone(), StreamRecoveryAction::RestartQuery)
        .unwrap();
    let eviction = journal
        .record(
            &other,
            StreamSequence::new(1),
            Arc::from(vec![0; 8_192 - 2 * frame_bytes]),
        )
        .unwrap();
    assert_eq!(eviction.evicted_items, 1);
    journal.deactivate_stream(&key, Instant::now() + Duration::from_secs(60));

    let executor = Arc::new(RecordingExecutor::default());
    let dispatcher = dispatcher(executor.clone(), 1, 4);
    let connection = Arc::new(session_with_replay(
        agreement_for_profile(8, 8, true, true),
        journal,
    ));
    let specification = ProductStreamSpec::SessionEvents(SessionEventsSpec {
        session_ids: vec!["session-1".into()],
        kinds: Vec::new(),
        after_sequence: None,
    });
    let action = connection
        .accept(ClientMessage::ResumeStream {
            request_id: request_id(1),
            stream_id: stream.clone(),
            server_epoch: epoch(),
            after: StreamSequence::INITIAL,
            trace_context: trace_context(1),
            payload: Arc::from(product_codec().encode_stream_spec(&specification).unwrap()),
        })
        .unwrap()
        .remove(0);
    dispatcher
        .dispatch_authenticated(authentication(), connection.clone(), action)
        .unwrap();
    wait_until(|| executor.publisher.0.lock().unwrap().is_some());
    let publisher = executor.wait_publisher();
    publisher.publish(&item).expect("next live event queues");

    let response = connection.next_write().unwrap().expect("open response");
    let ServerMessage::Response { payload, .. } = decode_server(response.bytes()) else {
        panic!("response must precede replay");
    };
    assert!(matches!(
        product_codec()
            .decode_stream_open_response(&payload)
            .unwrap(),
        ProductStreamOpenResponse::Opened(_)
    ));
    response.confirm_written().unwrap();
    let gap = connection.next_write().unwrap().expect("eviction gap");
    assert!(
        matches!(decode_server(gap.bytes()), ServerMessage::StreamItem {
        item: InboundStreamItem::Gap(gap), ..
    } if gap.reason == StreamGapReason::ReplayEvicted
        && gap.first_missing.value() == 1 && gap.last_missing.value() == 1)
    );
    assert!(gap.confirm_written().unwrap().is_none());
    let mut sequences = Vec::new();
    while let Some(write) = connection.next_write().unwrap() {
        let ServerMessage::StreamItem {
            item: InboundStreamItem::Data {
                sequence, bytes, ..
            },
            ..
        } = decode_server(write.bytes())
        else {
            panic!("expected retained or live event");
        };
        assert!(product_codec().decode_stream_item(&bytes).unwrap() == item);
        let recorded = write
            .confirm_written()
            .expect("publisher output must confirm after resume");
        assert_eq!(recorded.is_some(), sequence.value() == 4);
        sequences.push(sequence.value());
    }
    assert_eq!(sequences, vec![2, 3, 4]);
    assert!(!connection.is_closed());
    dispatcher.shutdown();
}

#[test]
fn reliable_publisher_retries_backpressure_without_consuming_a_sequence() {
    let executor = Arc::new(RecordingExecutor::default());
    let dispatcher = dispatcher(executor.clone(), 1, 4);
    let connection = Arc::new(session(8, 8));
    let request_id = request_id(1);
    let stream_id = stream_id(1);
    let specification = ProductStreamSpec::SessionEvents(SessionEventsSpec {
        session_ids: Vec::new(),
        kinds: Vec::new(),
        after_sequence: None,
    });
    let action = connection
        .accept(ClientMessage::OpenStream {
            request_id,
            stream_id: stream_id.clone(),
            trace_context: trace_context(1),
            payload: Arc::from(product_codec().encode_stream_spec(&specification).unwrap()),
        })
        .unwrap()
        .remove(0);
    dispatcher
        .dispatch_authenticated(authentication(), connection.clone(), action)
        .unwrap();
    next_response(&connection);
    let publisher = executor.wait_publisher();
    let item = event_item(&"x".repeat(400));

    let mut accepted = 0_u64;
    loop {
        match publisher.publish(&item) {
            Ok(_) => accepted += 1,
            Err(error) => {
                assert!(error.is_backpressured());
                break;
            }
        }
    }
    assert!(!publisher.is_closed());
    assert_eq!(
        connection
            .stream_diagnostic_page(None, 1)
            .unwrap()
            .items
            .len(),
        1
    );

    connection
        .next_write()
        .unwrap()
        .expect("queued stream item")
        .confirm_written()
        .unwrap();
    publisher.publish(&item).expect("same sequence retries");
    assert_eq!(
        connection
            .stream_diagnostic_page(None, 1)
            .unwrap()
            .items
            .first()
            .and_then(|item| match &item.state {
                undercurrent_transport_local::ServerStreamDiagnosticState::Active {
                    pressure,
                    ..
                } => {
                    Some(pressure.last_sequence.value())
                }
                undercurrent_transport_local::ServerStreamDiagnosticState::Opening { .. } => None,
            }),
        Some(accepted + 1)
    );
    dispatcher.shutdown();
}

#[test]
fn dispatch_registries_are_unique_and_stable() {
    let mut events = undercurrent_daemon::ProductDispatchEventName::ALL
        .into_iter()
        .map(undercurrent_daemon::ProductDispatchEventName::as_str)
        .collect::<Vec<_>>();
    events.sort_unstable();
    events.dedup();
    assert_eq!(
        events.len(),
        undercurrent_daemon::ProductDispatchEventName::ALL.len()
    );
    let mut results = undercurrent_daemon::ProductDispatchResultCategory::ALL
        .into_iter()
        .map(undercurrent_daemon::ProductDispatchResultCategory::as_str)
        .collect::<Vec<_>>();
    results.sort_unstable();
    results.dedup();
    assert_eq!(
        results.len(),
        undercurrent_daemon::ProductDispatchResultCategory::ALL.len()
    );
    let mut metrics = undercurrent_daemon::ProductDispatchMetricName::ALL
        .into_iter()
        .map(undercurrent_daemon::ProductDispatchMetricName::as_str)
        .collect::<Vec<_>>();
    metrics.sort_unstable();
    metrics.dedup();
    assert_eq!(
        metrics.len(),
        undercurrent_daemon::ProductDispatchMetricName::ALL.len()
    );
}

#[derive(Default)]
struct RecordingExecutor {
    expected_connection_id: Option<&'static str>,
    executions: AtomicUsize,
    blocking: AtomicBool,
    gated: AtomicBool,
    started: (Mutex<bool>, Condvar),
    gate: (Mutex<bool>, Condvar),
    cancel_observed: AtomicBool,
    error_message_bytes: AtomicUsize,
    error_effect_certainty: Option<undercurrent_protocol::EffectCertaintyDto>,
    deny_runtime_cancellation_after_first: bool,
    publisher: Arc<(Mutex<Option<ProductStreamPublisher>>, Condvar)>,
}

impl RecordingExecutor {
    fn live_local() -> Self {
        Self {
            expected_connection_id: Some("local-1"),
            ..Self::default()
        }
    }

    fn blocking() -> Self {
        Self {
            blocking: AtomicBool::new(true),
            ..Self::default()
        }
    }

    fn gated() -> Self {
        Self {
            gated: AtomicBool::new(true),
            ..Self::default()
        }
    }

    fn unknown_effect_error() -> Self {
        Self {
            error_message_bytes: AtomicUsize::new(1),
            ..Self::default()
        }
    }

    fn oversized_error() -> Self {
        Self {
            error_message_bytes: AtomicUsize::new(8_192),
            ..Self::default()
        }
    }

    fn runtime_cancellation_revocation() -> Self {
        Self {
            deny_runtime_cancellation_after_first: true,
            ..Self::default()
        }
    }

    fn wait_started(&self) {
        let mut started = self.started.0.lock().unwrap();
        while !*started {
            started = self.started.1.wait(started).unwrap();
        }
    }

    fn release_gate(&self) {
        *self.gate.0.lock().unwrap() = true;
        self.gate.1.notify_all();
    }

    fn wait_publisher(&self) -> ProductStreamPublisher {
        let mut publisher = self.publisher.0.lock().unwrap();
        while publisher.is_none() {
            publisher = self.publisher.1.wait(publisher).unwrap();
        }
        publisher.clone().unwrap()
    }
}

impl ProductActionExecutor for RecordingExecutor {
    fn execute(&self, context: &ProductRequestContext, request: ProductRequest) -> ProductResponse {
        assert_eq!(
            context.connection_id.as_str(),
            self.expected_connection_id.unwrap_or("connection-1")
        );
        assert_eq!(context.server_epoch.as_str(), "epoch-1");
        let prior_executions = self.executions.fetch_add(1, Ordering::AcqRel);
        *self.started.0.lock().unwrap() = true;
        self.started.1.notify_all();
        if self.blocking.load(Ordering::Acquire) {
            while !context.cancellation.is_cancelled() {
                thread::sleep(Duration::from_millis(1));
            }
            self.cancel_observed.store(true, Ordering::Release);
        }
        if self.gated.load(Ordering::Acquire) {
            let mut released = self.gate.0.lock().unwrap();
            while !*released {
                released = self.gate.1.wait(released).unwrap();
            }
        }
        let error_message_bytes = self.error_message_bytes.load(Ordering::Acquire);
        if error_message_bytes != 0 {
            let message = if error_message_bytes == 1 {
                "operation effect remains unknown".to_owned()
            } else {
                "x".repeat(error_message_bytes)
            };
            return ProductResponse::Error(ProductError {
                effect_certainty: self.error_effect_certainty,
                operation: Some(request.operation()),
                category: if self.error_effect_certainty.is_some() {
                    ProductErrorCategory::CapacityExceeded
                } else {
                    ProductErrorCategory::FailedPrecondition
                },
                disposition: Some(if self.error_effect_certainty.is_some() {
                    ProductErrorDisposition::CapacityRejected
                } else {
                    ProductErrorDisposition::UnknownInconclusive
                }),
                message,
                session_id: None,
                retryable: false,
                retry_after_millis: None,
                limit: None,
                resynchronization: None,
            });
        }
        if self.deny_runtime_cancellation_after_first
            && prior_executions > 0
            && matches!(&request, ProductRequest::CancelRuntimeAttempt(_))
        {
            return ProductResponse::Error(ProductError {
                effect_certainty: None,
                operation: Some(ProductOperation::CancelRuntimeAttempt),
                category: ProductErrorCategory::PermissionDenied,
                disposition: Some(ProductErrorDisposition::AccessRejected),
                message: "runtime cancellation authorization was revoked".to_owned(),
                session_id: None,
                retryable: false,
                retry_after_millis: None,
                limit: None,
                resynchronization: None,
            });
        }
        match request {
            ProductRequest::GetDaemonStatus => ProductResponse::Success(Box::new(
                undercurrent_protocol::ProductSuccess::GetDaemonStatus(DaemonStatusDto {
                    server_epoch: "epoch-1".into(),
                    started_at_millis: 1,
                    accepting_connections: true,
                    physical_connections: 1,
                    logical_streams: 0,
                    managed_sessions: 0,
                }),
            )),
            ProductRequest::ShutdownDaemon(_) => ProductResponse::Success(Box::new(
                undercurrent_protocol::ProductSuccess::ShutdownDaemon(ShutdownAcceptedDto {
                    drain_deadline_millis: 1_000,
                }),
            )),
            ProductRequest::CancelRuntimeAttempt(request) => {
                let target = request
                    .target_binding()
                    .expect("validated cancellation target");
                let material = request
                    .digest_material()
                    .expect("validated cancellation material");
                let control = RuntimeCancellationControlBindingDto::new(
                    &material,
                    request.command_digest_sha256().to_owned(),
                )
                .expect("validated cancellation control");
                let state = RuntimeCancellationStateDto::AcceptedStopping {
                    session: RuntimeCancellationSessionSnapshotDto::new(
                        request.session_id().to_owned(),
                        LifecycleStateDto::Stopping,
                        request.expected_session_revision() + 1,
                        request.expected_lifecycle_revision() + 1,
                        10,
                    )
                    .expect("valid stopping snapshot"),
                };
                ProductResponse::Success(Box::new(
                    undercurrent_protocol::ProductSuccess::CancelRuntimeAttempt(
                        CancelRuntimeAttemptResultDto::new(target, control, state)
                            .expect("valid cancellation result"),
                    ),
                ))
            }
            other => panic!("unexpected operation {}", other.operation().as_str()),
        }
    }

    fn prepare_stream(
        &self,
        context: &ProductStreamContext,
        _specification: ProductStreamSpec,
    ) -> Result<Box<dyn PreparedProductStream>, ProductStreamError> {
        assert_eq!(
            context.connection_id.as_str(),
            self.expected_connection_id.unwrap_or("connection-1")
        );
        assert_eq!(context.server_epoch.as_str(), "epoch-1");
        Ok(Box::new(RecordingPreparedStream {
            publisher: self.publisher.clone(),
        }))
    }

    fn close_stream(&self, _client_id: &ClientId, _stream_id: &StreamId) {}

    fn close_connection(&self, _client_id: &ClientId) {}

    fn shutdown(&self) {
        self.release_gate();
    }
}

struct RecordingPreparedStream {
    publisher: Arc<(Mutex<Option<ProductStreamPublisher>>, Condvar)>,
}

struct ZeroJitter;

impl ClientRecoveryJitter for ZeroJitter {
    fn sample_basis_points(&self, _failed_attempt: u32, _maximum: u16) -> u16 {
        0
    }
}

impl PreparedProductStream for RecordingPreparedStream {
    fn available_from(&self) -> Option<u64> {
        Some(0)
    }

    fn activate(self: Box<Self>, publisher: ProductStreamPublisher) {
        *self.publisher.0.lock().unwrap() = Some(publisher);
        self.publisher.1.notify_all();
    }
}

fn dispatcher(
    executor: Arc<RecordingExecutor>,
    workers: usize,
    queued_actions: usize,
) -> BoundedProductDispatcher {
    dispatcher_with_telemetry(
        executor,
        Arc::new(NoopProductDispatchTelemetry),
        workers,
        queued_actions,
    )
}

fn dispatcher_with_telemetry(
    executor: Arc<RecordingExecutor>,
    telemetry: Arc<dyn ProductDispatchTelemetry>,
    workers: usize,
    queued_actions: usize,
) -> BoundedProductDispatcher {
    let idempotency = Arc::new(BoundedRequestIdempotencyStore::new(
        RequestIdempotencyLimits::new(64, 1_048_576, 4_096, 4_096).unwrap(),
    ));
    BoundedProductDispatcher::new(
        product_codec(),
        TerminalOutputCodec::new(512).unwrap(),
        executor,
        idempotency,
        telemetry,
        ProductDispatcherLimits::new(workers, queued_actions, Duration::from_secs(60)).unwrap(),
    )
    .unwrap()
}

#[derive(Default)]
struct RecordingDispatchTelemetry {
    events: Mutex<Vec<ProductDispatchEvent>>,
}

impl RecordingDispatchTelemetry {
    fn events(&self) -> Vec<ProductDispatchEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl ProductDispatchTelemetry for RecordingDispatchTelemetry {
    fn emit(&self, event: ProductDispatchEvent) {
        self.events.lock().unwrap().push(event);
    }
}

fn dispatch_request(
    dispatcher: &BoundedProductDispatcher,
    connection: &Arc<ServerConnectionSession>,
    request_id: RequestId,
    request: &ProductRequest,
) -> Result<(), undercurrent_transport_local::ServerDispatchError> {
    let action = connection
        .accept(ClientMessage::Request {
            request_id,
            trace_context: trace_context(1),
            payload: Arc::from(product_codec().encode_request(request).unwrap()),
        })
        .unwrap()
        .remove(0);
    dispatcher.dispatch_authenticated(authentication(), connection.clone(), action)
}

fn next_response(connection: &ServerConnectionSession) -> Arc<[u8]> {
    next_correlated_response(connection).1
}

fn next_correlated_response(connection: &ServerConnectionSession) -> (RequestId, Arc<[u8]>) {
    loop {
        if let Some(write) = connection.next_write().unwrap() {
            let message = decode_server(write.bytes());
            write.confirm_written().unwrap();
            if let ServerMessage::Response {
                request_id,
                payload,
            } = message
            {
                return (request_id, payload);
            }
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn next_stream_item(
    connection: &ServerConnectionSession,
) -> (StreamId, undercurrent_wire::StreamSequence, Arc<[u8]>) {
    loop {
        if let Some(write) = connection.next_write().unwrap() {
            let message = decode_server(write.bytes());
            write.confirm_written().unwrap();
            if let ServerMessage::StreamItem {
                stream_id,
                item:
                    InboundStreamItem::Data {
                        sequence, bytes, ..
                    },
            } = message
            {
                return (stream_id, sequence, bytes);
            }
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn wait_until(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while !predicate() {
        assert!(Instant::now() < deadline, "condition did not become true");
        thread::sleep(Duration::from_millis(1));
    }
}

fn event_item(session_id: &str) -> ProductStreamItem {
    ProductStreamItem::SessionEvent(SessionEventDto {
        observation: None,
        sequence: 1,
        session_id: session_id.to_owned(),
        session_revision: 1,
        kind: SessionEventKindDto::SessionCreated,
        occurred_at_millis: 1,
    })
}

fn product_codec() -> ProductCodec {
    ProductCodec::new(ProductCodecLimits::new(4_096, 4_096, 4_096, 4_096).unwrap())
}

fn runtime_cancellation_request() -> ProductRequest {
    let target = RuntimeCancellationTargetBindingDto::new(
        "runtime-start-attempt-1".to_owned(),
        "11".repeat(32),
        7,
        "cell-1".to_owned(),
        "session-1".to_owned(),
        2,
        2,
        "prv_0198a5bd-2b01-7ae9-9a0c-42532d56e7f7".to_owned(),
        9,
    )
    .expect("runtime cancellation target");
    let material = RuntimeCancellationCommandDigestMaterial::new(
        target,
        "runtime-cancel-control-1".to_owned(),
        "runtime-cancel-idempotency-1".to_owned(),
        RuntimeCancellationModeDto::Graceful,
        "user requested cancellation".to_owned(),
        RuntimeCancellationPropagationPolicyDto::TargetOnly,
        10_000,
        "example/workflow".to_owned(),
        "tenant_0198a5bd-2afd-7dd1-b6a3-1fb90c6f0a21".to_owned(),
        "actor-context-1".to_owned(),
        "authority-1".to_owned(),
    )
    .expect("runtime cancellation material");
    let digest = material
        .canonical_digest_sha256()
        .expect("runtime cancellation digest");
    ProductRequest::CancelRuntimeAttempt(
        CancelRuntimeAttemptRequest::attached(material, digest)
            .expect("runtime cancellation request"),
    )
}

fn opaque_start_request() -> ProductRequest {
    ProductRequest::StartSession(StartSessionRequest {
        launch_mode: undercurrent_protocol::LaunchModeDto::Sealed,
        mutation: MutationContext {
            idempotency_key: Some("opaque-capture-rejection".to_owned()),
            ..MutationContext::current(ProductOperation::StartSession)
        },
        dispatch: None,
        name: "opaque-session".to_owned(),
        command: CommandSpecDto {
            program: "true".to_owned(),
            arguments: Vec::new(),
        },
        working_directory: None,
        environment: EnvironmentSpecDto::default(),
        terminal: TerminalSizeDto {
            columns: 80,
            rows: 24,
        },
        output_capture_mode: OutputCaptureModeDto::Opaque,
        execution: ExecutionPolicyDto::default(),
        health: HealthRulesDto::default(),
        profile: None,
        profile_binding: ProfileBindingModeDto::Fixed,
        labels: BTreeSet::new().into_iter().collect(),
        metadata: BTreeSet::new().into_iter().collect(),
    })
}

fn session(max_pending_requests: u32, max_streams: u32) -> ServerConnectionSession {
    session_for_profile(max_pending_requests, max_streams, true, true)
}

fn session_for_profile(
    max_pending_requests: u32,
    max_streams: u32,
    client_offers_error_disposition: bool,
    server_supports_error_disposition: bool,
) -> ServerConnectionSession {
    let agreement = agreement_for_profile(
        max_pending_requests,
        max_streams,
        client_offers_error_disposition,
        server_supports_error_disposition,
    );
    session_with_agreement(agreement)
}

fn session_with_agreement(agreement: ConnectionAgreement) -> ServerConnectionSession {
    session_with_replay(
        agreement,
        Arc::new(ReplayJournal::new(
            epoch(),
            ReplayLimits::new(1_000, 65_536, 65_536_000, 8_192).unwrap(),
        )),
    )
}

fn session_with_replay(
    agreement: ConnectionAgreement,
    replay: Arc<ReplayJournal>,
) -> ServerConnectionSession {
    let limits = agreement.limits();
    let scheduler = SchedulerLimits::new(
        limits,
        [64 * 1_024, 128 * 1_024, 256 * 1_024, 512 * 1_024],
        limits.connection_queue_bytes(),
        u64::from(limits.max_frame_bytes()) * 4,
        undercurrent_multiplex::SchedulerMessageLimits::new(
            8_192,
            1_024,
            [1_024, 2_048, 2_048, 3_072],
        )
        .expect("scheduler message limits"),
    )
    .unwrap();
    ServerConnectionSession::new(
        client_id(),
        agreement,
        scheduler,
        replay,
        ServerDiagnosticLimits::new(100, 100).unwrap(),
    )
    .unwrap()
}

fn agreement_for_profile(
    max_pending_requests: u32,
    max_streams: u32,
    client_offers_error_disposition: bool,
    server_supports_error_disposition: bool,
) -> ConnectionAgreement {
    agreement_for_effect_profile(
        max_pending_requests,
        max_streams,
        client_offers_error_disposition,
        server_supports_error_disposition,
        false,
        false,
    )
}

fn agreement_for_effect_profile(
    max_pending_requests: u32,
    max_streams: u32,
    client_offers_error_disposition: bool,
    server_supports_error_disposition: bool,
    client_offers_effect_certainty: bool,
    server_supports_effect_certainty: bool,
) -> ConnectionAgreement {
    let limits = ConnectionLimits::new(
        8_192,
        max_streams,
        max_pending_requests,
        1_000_000,
        65_536,
        16_384,
    )
    .unwrap();
    let required_capabilities = BTreeSet::from([
        capability(connection_capabilities::MULTIPLEX),
        capability(connection_capabilities::FLOW_CONTROL),
        capability(connection_capabilities::STREAM_RESUME),
        capability(connection_capabilities::BINARY_PAYLOADS),
        capability(connection_capabilities::TRACE_CONTEXT),
    ]);
    let mut optional_capabilities = if client_offers_error_disposition {
        BTreeSet::from([capability(product_capabilities::ERROR_DISPOSITION)])
    } else {
        BTreeSet::new()
    };
    let mut server_capabilities = required_capabilities.clone();
    if server_supports_error_disposition {
        server_capabilities.insert(capability(product_capabilities::ERROR_DISPOSITION));
    }
    if client_offers_effect_certainty {
        optional_capabilities.insert(capability(product_capabilities::ERROR_EFFECT_CERTAINTY));
    }
    if server_supports_effect_certainty {
        server_capabilities.insert(capability(product_capabilities::ERROR_EFFECT_CERTAINTY));
    }
    let hello = ConnectionHello::new(
        client_id(),
        CURRENT_PROTOCOL_VERSION,
        required_capabilities,
        optional_capabilities,
        limits,
    )
    .unwrap();
    let policy = ConnectionPolicy::new(
        CURRENT_PROTOCOL_VERSION,
        server_capabilities,
        limits,
        HeartbeatPolicy::new(5_000, 15_000).unwrap(),
        Some(30_000),
    )
    .unwrap();
    negotiate_connection(
        &hello,
        &policy,
        ConnectionId::new("connection-1").unwrap(),
        epoch(),
    )
    .unwrap()
}

#[derive(Debug, Deserialize)]
#[serde(tag = "outcome", content = "value", rename_all = "snake_case")]
pub enum StrictLegacyProductResponse {
    Success(serde_json::Value),
    Error(StrictLegacyProductError),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrictLegacyProductError {
    pub operation: Option<ProductOperation>,
    pub category: ProductErrorCategory,
    pub message: String,
    pub session_id: Option<String>,
    pub retryable: bool,
    pub retry_after_millis: Option<u64>,
    pub limit: Option<u64>,
    pub resynchronization: Option<ResynchronizationDto>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "outcome", content = "value", rename_all = "snake_case")]
pub enum StrictLegacyStreamOpenResponse {
    Opened(serde_json::Value),
    Error(StrictLegacyStreamError),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrictLegacyStreamError {
    pub kind: Option<ProductStreamKind>,
    pub category: ProductErrorCategory,
    pub message: String,
    pub session_id: Option<String>,
    pub retryable: bool,
    pub retry_after_millis: Option<u64>,
    pub limit: Option<u64>,
    pub resynchronization: Option<ResynchronizationDto>,
}

fn decode_server(bytes: &[u8]) -> ServerMessage {
    match WireCodec::new(8_192).unwrap().decode_server(bytes).unwrap() {
        DecodeStatus::Complete(frame) => frame.message,
        DecodeStatus::NeedMore { .. } => panic!("expected complete server frame"),
    }
}

fn capability(value: &str) -> NamespacedName {
    NamespacedName::new(value).unwrap()
}

fn trace_context(index: usize) -> TraceContext {
    TraceContext::new(
        TraceId::new(format!("trace-{index}")).expect("trace ID"),
        SpanId::new(format!("span-{index}")).expect("span ID"),
    )
}

fn client_id() -> ClientId {
    ClientId::new("client-1").unwrap()
}

fn epoch() -> ServerEpoch {
    ServerEpoch::new("epoch-1").unwrap()
}

fn authentication() -> Arc<AuthenticatedRequestContext> {
    Arc::new(AuthenticatedRequestContext::new(
        AuthenticatedPrincipalRef::new("test-principal").unwrap(),
        None,
        AuthorityRefDto::new("operation/test").unwrap(),
    ))
}

fn request_id(index: usize) -> RequestId {
    RequestId::new(format!("request-{index}")).unwrap()
}

fn stream_id(index: usize) -> StreamId {
    StreamId::new(format!("stream-{index}")).unwrap()
}

#[path = "product_dispatch/effect_evidence.rs"]
mod effect_evidence;

// macOS's default temp directory can exceed AF_UNIX limits once the product's
// run paths are appended. Keep IPC fixtures short and free of symlink ancestors.
fn physical_tempdir() -> std::io::Result<tempfile::TempDir> {
    #[cfg(target_os = "macos")]
    let base = std::path::PathBuf::from("/tmp");
    #[cfg(not(target_os = "macos"))]
    let base = std::env::temp_dir();
    tempfile::tempdir_in(std::fs::canonicalize(base)?)
}
