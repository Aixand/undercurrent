// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeSet, VecDeque};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use undercurrent_multiplex::{ClientMessage, InboundStreamItem, ServerMessage};
use undercurrent_protocol::{
    CommandSpecDto, CreateDiagnosticBundleRequest, DaemonStatusDto, DiagnosticBundleDto,
    DiagnosticPageDto, DiagnosticSectionDto, EnvironmentSpecDto, ExecutionPolicyDto,
    HealthRulesDto, MutationContext, OutputCaptureModeDto, ProductCodec, ProductCodecLimits,
    ProductError, ProductErrorCategory, ProductErrorDisposition, ProductOperation, ProductRequest,
    ProductResponse, ProductStreamError, ProductStreamOpenResponse, ProductStreamOpened,
    ProductStreamSpec, ProductSuccess, ProfileBindingModeDto, StartSessionRequest,
    TerminalInitialModeDto, TerminalOutputChunk, TerminalOutputCodec, TerminalOutputSpec,
    TerminalSizeDto, product_capabilities,
};
use undercurrent_sdk::{
    ClientConnectionAgreement, ClientConnectionLimits, ClientConnector, ClientRecoveryCancellation,
    ClientRecoveryJitter, ClientRecoveryPolicy, ClientRuntimeLimits, ClientTraceContext,
    ClientTransport, ClientTransportError, ClientTransportInbound, ClientTransportOperation,
    ClientTransportOutbound, NegotiatedClientTransport, PRODUCT_CLIENT_OPTIONAL_CAPABILITIES,
    ProductClient, ProductClientError, ProductClientErrorCategory, ProductClientRuntime,
    ProductSubscriptionItem, StandaloneClientProfile,
};
use undercurrent_wire::core::NamespacedName;
use undercurrent_wire::{
    CURRENT_PROTOCOL_VERSION, ClientId, ConnectionAgreement, ConnectionHello, ConnectionId,
    ConnectionLimits, ConnectionPolicy, DecodeStatus, HandshakeCodec, HandshakeResponse,
    HeartbeatPolicy, ServerEpoch, StreamSequence, WireCodec, connection_capabilities,
    negotiate_connection,
};

#[test]
fn product_client_error_registry_is_unique_and_complete() {
    let mut names = ProductClientErrorCategory::ALL
        .into_iter()
        .map(ProductClientErrorCategory::as_str)
        .collect::<Vec<_>>();
    names.sort_unstable();
    names.dedup();

    assert_eq!(names.len(), ProductClientErrorCategory::ALL.len());
}

#[test]
fn opaque_start_preflights_capability_before_encoding_and_terminal_start_is_unchanged() {
    let codec = ProductCodec::new(ProductCodecLimits::new(4_096, 4_096, 4_096, 4_096).unwrap());
    let terminal_codec = TerminalOutputCodec::new(1_024).unwrap();
    let transport = Arc::new(ProductTransport::new(codec));
    let connector = Arc::new(SingleConnector {
        agreement: agreement(),
        transport: transport.clone(),
        attempts: AtomicUsize::new(0),
    });
    let runtime = ProductClientRuntime::new(
        ClientRuntimeLimits::new(8_192, 32, 32, 64_000).unwrap(),
        ClientRecoveryPolicy::new(
            Duration::from_millis(1),
            Duration::from_millis(2),
            10_000,
            1,
            2,
            4,
        )
        .unwrap(),
        Duration::from_secs(1),
        codec,
        terminal_codec,
    )
    .unwrap();
    let client = ProductClient::connect(connector, runtime, Arc::new(ZeroJitter)).unwrap();

    let mut opaque = start_request(OutputCaptureModeDto::Opaque);
    opaque.command.program = "x".repeat(8_192);
    let error = match client.request_cancellable(ProductRequest::StartSession(opaque)) {
        Err(error) => error,
        Ok(_) => panic!("opaque start must fail capability preflight"),
    };
    assert!(matches!(
        error,
        ProductClientError::MissingCapability { capability }
            if capability == product_capabilities::OPAQUE_OUTPUT_CAPTURE
    ));
    assert_eq!(transport.request_count.load(Ordering::Acquire), 0);

    transport.defer_next_request();
    let pending = client
        .request_cancellable(ProductRequest::StartSession(start_request(
            OutputCaptureModeDto::Terminal,
        )))
        .expect("terminal start remains encodable without the optional capability");
    pending
        .cancel()
        .expect("terminal request cancellation enters the connection");
    let error = match pending.wait(Duration::from_secs(1)) {
        Err(error) => error,
        Ok(_) => panic!("deferred terminal request must settle through cancellation"),
    };
    assert_eq!(error.category(), ProductClientErrorCategory::Cancelled);
    assert_eq!(transport.request_count.load(Ordering::Acquire), 1);
    client.close();
}

#[test]
fn cancellable_product_request_waits_for_correlated_server_terminal_on_one_connection() {
    let codec = ProductCodec::new(ProductCodecLimits::new(4_096, 4_096, 4_096, 4_096).unwrap());
    let terminal_codec = TerminalOutputCodec::new(1_024).unwrap();
    let transport = Arc::new(ProductTransport::new(codec));
    transport.defer_next_request();
    let connector = Arc::new(SingleConnector {
        agreement: agreement(),
        transport: transport.clone(),
        attempts: AtomicUsize::new(0),
    });
    let runtime = ProductClientRuntime::new(
        ClientRuntimeLimits::new(8_192, 32, 32, 64_000).unwrap(),
        ClientRecoveryPolicy::new(
            Duration::from_millis(1),
            Duration::from_millis(2),
            10_000,
            1,
            2,
            4,
        )
        .unwrap(),
        Duration::from_secs(1),
        codec,
        terminal_codec,
    )
    .unwrap();
    let client = ProductClient::connect(connector.clone(), runtime, Arc::new(ZeroJitter)).unwrap();

    let pending = client
        .request_cancellable(ProductRequest::GetDaemonStatus)
        .expect("request starts");
    pending
        .cancel()
        .expect("cancel frame enters existing connection");
    pending
        .cancel()
        .expect("duplicate local cancel is idempotent");
    let error = match pending.wait(Duration::from_secs(1)) {
        Err(error) => error,
        Ok(_) => panic!("server cancellation must be terminal"),
    };
    assert_eq!(error.category(), ProductClientErrorCategory::Cancelled);
    assert_eq!(
        error.product_error_category(),
        Some(ProductErrorCategory::Cancelled)
    );
    assert_eq!(
        error.product_error_disposition(),
        Some(ProductErrorDisposition::UnknownInconclusive)
    );

    let normal = client
        .request_cancellable(ProductRequest::GetDaemonStatus)
        .expect("second request starts");
    normal
        .cancel()
        .expect("normal terminal may win cancellation race");
    assert!(matches!(
        normal
            .wait(Duration::from_secs(1))
            .expect("normal terminal wins"),
        ProductSuccess::GetDaemonStatus(_)
    ));
    assert_eq!(connector.attempts.load(Ordering::Acquire), 1);
    assert_eq!(transport.request_count.load(Ordering::Acquire), 2);
    assert_eq!(transport.cancel_count.load(Ordering::Acquire), 2);
    client.close();
}

#[test]
fn uref_034_sdk_preserves_exact_server_category_and_disposition() {
    let error = ProductClientError::Server(ProductError {
        effect_certainty: None,
        operation: Some(ProductOperation::ShutdownDaemon),
        category: ProductErrorCategory::FailedPrecondition,
        disposition: Some(ProductErrorDisposition::UnknownInconclusive),
        message: "shutdown effect remains unknown".to_owned(),
        session_id: None,
        retryable: false,
        retry_after_millis: None,
        limit: None,
        resynchronization: None,
    });

    assert_eq!(
        error.product_error_category(),
        Some(ProductErrorCategory::FailedPrecondition)
    );
    assert_eq!(
        error.product_error_disposition(),
        Some(ProductErrorDisposition::UnknownInconclusive)
    );

    let legacy = ProductClientError::Server(ProductError {
        effect_certainty: None,
        operation: Some(ProductOperation::ShutdownDaemon),
        category: ProductErrorCategory::FailedPrecondition,
        disposition: None,
        message: "legacy rejection".to_owned(),
        session_id: None,
        retryable: false,
        retry_after_millis: None,
        limit: None,
        resynchronization: None,
    });
    assert_eq!(
        legacy.product_error_category(),
        Some(ProductErrorCategory::FailedPrecondition)
    );
    assert_eq!(legacy.product_error_disposition(), None);

    let legacy_stream = ProductClientError::StreamRejected(ProductStreamError {
        kind: None,
        category: ProductErrorCategory::FailedPrecondition,
        disposition: None,
        message: "legacy stream rejection".to_owned(),
        session_id: None,
        retryable: false,
        retry_after_millis: None,
        limit: None,
        resynchronization: None,
    });
    assert_eq!(
        legacy_stream.product_error_category(),
        Some(ProductErrorCategory::FailedPrecondition)
    );
    assert_eq!(legacy_stream.product_error_disposition(), None);

    let profile = StandaloneClientProfile::standard("uref-034-client").expect("profile");
    assert_eq!(
        profile.hello().optional_capabilities(),
        PRODUCT_CLIENT_OPTIONAL_CAPABILITIES
    );
}

#[test]
fn retained_recovery_profile_preserves_the_replay_window_with_larger_request_frames() {
    const PREVIOUS_STANDARD_STREAM_QUEUE_BYTES: usize = 512 * 1_024;
    const CHUNK_BYTES: usize = 8 * 1_024;
    const CHUNK_COUNT: usize = 80;
    const RETAINED_BYTES: usize = CHUNK_BYTES * CHUNK_COUNT;

    const { assert!(RETAINED_BYTES > PREVIOUS_STANDARD_STREAM_QUEUE_BYTES) };
    let standard = StandaloneClientProfile::standard("standard-replay").expect("standard profile");
    assert_eq!(
        standard.hello().limits().stream_queue_bytes(),
        32 * 1024 * 1024
    );
    let profile = StandaloneClientProfile::retained_recovery_qualification("retained-replay")
        .expect("retained-recovery profile");
    assert_eq!(
        profile.hello().limits().stream_queue_bytes(),
        32 * 1024 * 1024
    );
    assert_eq!(
        profile.hello().limits().connection_queue_bytes(),
        standard.hello().limits().connection_queue_bytes()
    );

    let codec = ProductCodec::new(ProductCodecLimits::new(4_096, 4_096, 4_096, 4_096).unwrap());
    let terminal_codec = TerminalOutputCodec::new(131_072).unwrap();
    let transport = Arc::new(ProductTransport::new(codec));
    let connector = Arc::new(SingleConnector {
        agreement: agreement_for_client_limits(profile.hello().limits()),
        transport: transport.clone(),
        attempts: AtomicUsize::new(0),
    });
    let client =
        ProductClient::connect(connector, profile.product(), Arc::new(ZeroJitter)).unwrap();
    let opened = client
        .open_subscription(
            ProductStreamSpec::TerminalOutput(TerminalOutputSpec {
                session_id: "retained-session".into(),
                initial: TerminalInitialModeDto::Resume { after_offset: 0 },
            }),
            Duration::from_secs(1),
        )
        .expect("retained subscription");
    let stream_id =
        undercurrent_wire::StreamId::new(opened.subscription.stream_id()).expect("stream ID");
    let expected = (0..RETAINED_BYTES)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect::<Vec<_>>();
    let mut retained = Vec::with_capacity(RETAINED_BYTES);

    for index in 0..CHUNK_COUNT {
        let start = index * CHUNK_BYTES;
        let end = start + CHUNK_BYTES;
        let encoded = terminal_codec
            .encode(&TerminalOutputChunk::new(
                u64::try_from(start).unwrap(),
                Arc::<[u8]>::from(expected[start..end].to_vec()),
            ))
            .unwrap();
        transport.inject(ServerMessage::StreamItem {
            stream_id: stream_id.clone(),
            item: InboundStreamItem::Data {
                sequence: StreamSequence::new(u64::try_from(index + 1).unwrap()),
                flow_controlled_bytes: u64::try_from(encoded.len()).unwrap(),
                bytes: Arc::from(encoded),
            },
        });
        match opened
            .subscription
            .recv_timeout(Duration::from_secs(1))
            .expect("retained replay item")
        {
            ProductSubscriptionItem::Terminal {
                sequence,
                item: undercurrent_protocol::TerminalStreamItem::Output(chunk),
            } => {
                assert_eq!(sequence, u64::try_from(index + 1).unwrap());
                assert_eq!(chunk.start_offset(), u64::try_from(retained.len()).unwrap());
                retained.extend_from_slice(chunk.bytes());
            }
            other => panic!("retained replay produced a second gap or unexpected item: {other:?}"),
        }
    }

    assert_eq!(retained, expected);
    assert_eq!(client.pressure().physical_connection_attempts, 1);
    opened.subscription.close();
    client.close();
}

#[test]
fn typed_rpc_and_binary_terminal_stream_share_one_physical_connection() {
    let codec = ProductCodec::new(ProductCodecLimits::new(4_096, 4_096, 4_096, 4_096).unwrap());
    let terminal_codec = TerminalOutputCodec::new(1_024).unwrap();
    let transport = Arc::new(ProductTransport::new(codec));
    let connector = Arc::new(SingleConnector {
        agreement: agreement(),
        transport: transport.clone(),
        attempts: AtomicUsize::new(0),
    });
    let runtime = ProductClientRuntime::new(
        ClientRuntimeLimits::new(8_192, 32, 32, 64_000).unwrap(),
        ClientRecoveryPolicy::new(
            Duration::from_millis(1),
            Duration::from_millis(2),
            10_000,
            1,
            2,
            4,
        )
        .unwrap(),
        Duration::from_secs(1),
        codec,
        terminal_codec,
    )
    .unwrap();
    let client = ProductClient::connect(connector.clone(), runtime, Arc::new(ZeroJitter)).unwrap();
    let request_trace = trace_context("request");
    let stream_trace = trace_context("stream");

    let status = client
        .request_with_trace_context(
            ProductRequest::GetDaemonStatus,
            request_trace.clone(),
            Duration::from_secs(1),
        )
        .expect("typed daemon status");
    assert!(matches!(
        status,
        ProductSuccess::GetDaemonStatus(DaemonStatusDto {
            managed_sessions: 300,
            physical_connections: 1,
            ..
        })
    ));

    let bundle = client
        .create_diagnostic_bundle(
            CreateDiagnosticBundleRequest {
                sections: vec![DiagnosticSectionDto::Connections],
                page_size: 1,
                cursor: None,
                terminal_content: None,
            },
            Duration::from_secs(1),
        )
        .expect("typed diagnostic bundle");
    assert!(bundle.terminal_content_redacted);
    assert!(bundle.terminal_content.is_none());

    let opened = client
        .open_subscription_with_trace_context(
            ProductStreamSpec::TerminalOutput(TerminalOutputSpec {
                session_id: "session-300".into(),
                initial: TerminalInitialModeDto::None,
            }),
            stream_trace.clone(),
            Duration::from_secs(1),
        )
        .expect("terminal subscription");
    let encoded = terminal_codec
        .encode(&TerminalOutputChunk::new(9, Arc::from(&b"ready"[..])))
        .unwrap();
    transport.inject(ServerMessage::StreamItem {
        stream_id: undercurrent_wire::StreamId::new(opened.subscription.stream_id())
            .expect("stream ID"),
        item: InboundStreamItem::Data {
            sequence: StreamSequence::new(1),
            flow_controlled_bytes: u64::try_from(encoded.len()).unwrap(),
            bytes: Arc::from(encoded),
        },
    });

    assert!(matches!(
        opened
            .subscription
            .recv_timeout(Duration::from_secs(1))
            .expect("binary terminal item"),
        ProductSubscriptionItem::Terminal {
            sequence,
            item: undercurrent_protocol::TerminalStreamItem::Output(chunk),
        } if sequence == 1
            && chunk.start_offset() == 9
            && chunk.bytes() == b"ready"
    ));
    assert_eq!(connector.attempts.load(Ordering::Acquire), 1);
    assert_eq!(client.pressure().physical_connection_attempts, 1);
    transport.inject(ServerMessage::StreamItem {
        stream_id: undercurrent_wire::StreamId::new(opened.subscription.stream_id())
            .expect("stream ID"),
        item: InboundStreamItem::Data {
            sequence: StreamSequence::new(2),
            flow_controlled_bytes: 3,
            bytes: Arc::from(&b"bad"[..]),
        },
    });
    assert_eq!(
        opened
            .subscription
            .recv_timeout(Duration::from_secs(1))
            .unwrap_err()
            .category(),
        ProductClientErrorCategory::ProtocolViolation
    );
    assert!(matches!(
        client
            .request(ProductRequest::GetDaemonStatus, Duration::from_secs(1))
            .expect("peer request remains available"),
        ProductSuccess::GetDaemonStatus(_)
    ));

    assert_eq!(transport.request_count.load(Ordering::Acquire), 3);
    assert_eq!(transport.stream_count.load(Ordering::Acquire), 1);
    let observed_traces = transport.trace_contexts.lock().unwrap();
    assert_eq!(
        observed_traces[0],
        (
            request_trace.trace_id().to_owned(),
            request_trace.span_id().to_owned()
        )
    );
    assert_eq!(
        observed_traces[2],
        (
            stream_trace.trace_id().to_owned(),
            stream_trace.span_id().to_owned()
        )
    );
    drop(observed_traces);

    opened.subscription.close();
    client.close();
}

struct SingleConnector {
    agreement: ClientConnectionAgreement,
    transport: Arc<ProductTransport>,
    attempts: AtomicUsize,
}

impl ClientConnector for SingleConnector {
    fn connect(
        &self,
        cancellation: &ClientRecoveryCancellation,
    ) -> Result<NegotiatedClientTransport, ClientTransportError> {
        if cancellation.is_cancelled() {
            return Err(transport_error(ClientTransportOperation::Connect));
        }
        self.attempts.fetch_add(1, Ordering::AcqRel);
        Ok(NegotiatedClientTransport::new(
            self.agreement.clone(),
            self.transport.clone(),
        ))
    }
}

struct ProductTransport {
    codec: ProductCodec,
    incoming: Mutex<VecDeque<ServerMessage>>,
    changed: Condvar,
    closed: AtomicBool,
    request_count: AtomicUsize,
    cancel_count: AtomicUsize,
    stream_count: AtomicUsize,
    trace_contexts: Mutex<Vec<(String, String)>>,
    correlation_ids: Mutex<Vec<Option<String>>>,
    defer_next_request: AtomicBool,
    deferred_request: Mutex<Option<(undercurrent_wire::RequestId, ProductOperation)>>,
    resume_response: Mutex<Option<Arc<[u8]>>>,
}

impl ProductTransport {
    fn new(codec: ProductCodec) -> Self {
        Self {
            codec,
            incoming: Mutex::new(VecDeque::new()),
            changed: Condvar::new(),
            closed: AtomicBool::new(false),
            request_count: AtomicUsize::new(0),
            cancel_count: AtomicUsize::new(0),
            stream_count: AtomicUsize::new(0),
            trace_contexts: Mutex::new(Vec::new()),
            correlation_ids: Mutex::new(Vec::new()),
            defer_next_request: AtomicBool::new(false),
            deferred_request: Mutex::new(None),
            resume_response: Mutex::new(None),
        }
    }

    fn defer_next_request(&self) {
        self.defer_next_request.store(true, Ordering::Release);
    }

    fn inject(&self, message: ServerMessage) {
        self.incoming.lock().unwrap().push_back(message);
        self.changed.notify_all();
    }

    fn respond(&self, request_id: undercurrent_wire::RequestId, response: ProductResponse) {
        self.inject(ServerMessage::Response {
            request_id,
            payload: Arc::from(self.codec.encode_response(&response).unwrap()),
        });
    }
}

impl ClientTransport for ProductTransport {
    fn send(&self, message: ClientTransportOutbound) -> Result<(), ClientTransportError> {
        let message = decode_outbound(message);
        match message {
            ClientMessage::Request {
                request_id,
                trace_context,
                payload,
            } => {
                self.correlation_ids.lock().unwrap().push(
                    trace_context
                        .correlation_id
                        .as_ref()
                        .map(|id| id.as_str().to_owned()),
                );
                self.trace_contexts.lock().unwrap().push((
                    trace_context.trace_id.as_str().to_owned(),
                    trace_context.span_id.as_str().to_owned(),
                ));
                self.request_count.fetch_add(1, Ordering::AcqRel);
                let request = self.codec.decode_request(&payload).unwrap();
                if self.defer_next_request.swap(false, Ordering::AcqRel) {
                    *self.deferred_request.lock().unwrap() =
                        Some((request_id, request.operation()));
                    return Ok(());
                }
                let success = match request {
                    ProductRequest::GetDaemonStatus => {
                        ProductSuccess::GetDaemonStatus(DaemonStatusDto {
                            server_epoch: "epoch-1".into(),
                            started_at_millis: 1,
                            accepting_connections: true,
                            physical_connections: 1,
                            logical_streams: 0,
                            managed_sessions: 300,
                        })
                    }
                    ProductRequest::CreateDiagnosticBundle(request) => {
                        request.validate().expect("valid bundle request");
                        ProductSuccess::CreateDiagnosticBundle(DiagnosticBundleDto {
                            diagnostics: DiagnosticPageDto {
                                items: Vec::new(),
                                next_cursor: None,
                                captured_at_millis: 2,
                            },
                            terminal_content: None,
                            terminal_content_redacted: true,
                        })
                    }
                    other => panic!("unexpected request: {:?}", other.operation()),
                };
                self.respond(request_id, ProductResponse::Success(Box::new(success)));
            }
            ClientMessage::CancelRequest { request_id } => {
                self.cancel_count.fetch_add(1, Ordering::AcqRel);
                if let Some((pending_id, _operation)) = self.deferred_request.lock().unwrap().take()
                {
                    assert_eq!(pending_id, request_id);
                    self.respond(
                        request_id,
                        ProductResponse::Error(ProductError {
                            effect_certainty: None,
                            operation: None,
                            category: ProductErrorCategory::Cancelled,
                            disposition: Some(ProductErrorDisposition::UnknownInconclusive),
                            message: "request cancellation was accepted".into(),
                            session_id: None,
                            retryable: false,
                            retry_after_millis: None,
                            limit: None,
                            resynchronization: None,
                        }),
                    );
                }
            }
            ClientMessage::OpenStream {
                request_id,
                stream_id: _,
                trace_context,
                payload,
            } => {
                self.correlation_ids.lock().unwrap().push(
                    trace_context
                        .correlation_id
                        .as_ref()
                        .map(|id| id.as_str().to_owned()),
                );
                self.trace_contexts.lock().unwrap().push((
                    trace_context.trace_id.as_str().to_owned(),
                    trace_context.span_id.as_str().to_owned(),
                ));
                self.stream_count.fetch_add(1, Ordering::AcqRel);
                let specification = self.codec.decode_stream_spec(&payload).unwrap();
                let response = ProductStreamOpenResponse::Opened(ProductStreamOpened {
                    kind: specification.kind(),
                    initial_sequence: 0,
                    resumable: true,
                    available_from: Some(0),
                });
                self.inject(ServerMessage::Response {
                    request_id,
                    payload: Arc::from(self.codec.encode_stream_open_response(&response).unwrap()),
                });
            }
            ClientMessage::Ping => self.inject(ServerMessage::Pong),
            ClientMessage::ResumeStream {
                request_id,
                payload,
                ..
            } => {
                self.stream_count.fetch_add(1, Ordering::AcqRel);
                let response = self
                    .resume_response
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or_else(|| {
                        let specification = self.codec.decode_stream_spec(&payload).unwrap();
                        Arc::from(
                            self.codec
                                .encode_stream_open_response(&ProductStreamOpenResponse::Opened(
                                    ProductStreamOpened {
                                        kind: specification.kind(),
                                        initial_sequence: 0,
                                        resumable: true,
                                        available_from: Some(0),
                                    },
                                ))
                                .unwrap(),
                        )
                    });
                self.inject(ServerMessage::Response {
                    request_id,
                    payload: response,
                });
            }
            ClientMessage::CloseStream { .. }
            | ClientMessage::WindowUpdate { .. }
            | ClientMessage::Close => {}
        }
        Ok(())
    }

    fn receive(&self) -> Result<ClientTransportInbound, ClientTransportError> {
        let mut incoming = self.incoming.lock().unwrap();
        loop {
            if let Some(message) = incoming.pop_front() {
                return Ok(encode_inbound(message));
            }
            if self.closed.load(Ordering::Acquire) {
                return Err(transport_error(ClientTransportOperation::Receive));
            }
            incoming = self.changed.wait(incoming).unwrap();
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.changed.notify_all();
    }
}

struct ZeroJitter;

impl ClientRecoveryJitter for ZeroJitter {
    fn sample_basis_points(&self, _failed_attempt: u32, _maximum: u16) -> u16 {
        0
    }
}

#[derive(Debug)]
struct PrivateTransportError;

impl Display for PrivateTransportError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("private transport detail")
    }
}

impl Error for PrivateTransportError {}

fn transport_error(operation: ClientTransportOperation) -> ClientTransportError {
    ClientTransportError::new(operation, PrivateTransportError)
}

fn agreement() -> ClientConnectionAgreement {
    agreement_with_limits(ConnectionLimits::new(16_384, 400, 64, 1_000_000, 64_000, 4_096).unwrap())
}

#[test]
fn guarded_input_and_stop_require_negotiation_before_entering_the_connection() {
    let codec = ProductCodec::new(ProductCodecLimits::new(4_096, 4_096, 4_096, 4_096).unwrap());
    let transport = Arc::new(ProductTransport::new(codec));
    let connector = Arc::new(SingleConnector {
        agreement: agreement(),
        transport: transport.clone(),
        attempts: AtomicUsize::new(0),
    });
    let runtime = ProductClientRuntime::new(
        ClientRuntimeLimits::new(8_192, 32, 32, 64_000).unwrap(),
        ClientRecoveryPolicy::new(
            Duration::from_millis(1),
            Duration::from_millis(2),
            10_000,
            1,
            2,
            4,
        )
        .unwrap(),
        Duration::from_secs(1),
        codec,
        TerminalOutputCodec::new(1_024).unwrap(),
    )
    .unwrap();
    let client = ProductClient::connect(connector, runtime, Arc::new(ZeroJitter)).unwrap();
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
                text: "x".repeat(8_192),
            }],
            observation_policy: Default::default(),
        }),
    ];
    for request in requests {
        let error = match client.request_cancellable(request) {
            Err(error) => error,
            Ok(_) => panic!("unguaranteed revision checks must not reach the connection"),
        };
        assert!(
            matches!(error, ProductClientError::MissingCapability { capability }
            if capability == product_capabilities::SESSION_REVISION_PRECONDITION)
        );
    }
    assert_eq!(transport.request_count.load(Ordering::Acquire), 0);
    client.close();
}

fn start_request(output_capture_mode: OutputCaptureModeDto) -> StartSessionRequest {
    StartSessionRequest {
        launch_mode: undercurrent_protocol::LaunchModeDto::Sealed,
        mutation: MutationContext::current(ProductOperation::StartSession),
        dispatch: None,
        name: "capture-session".to_owned(),
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
        output_capture_mode,
        execution: ExecutionPolicyDto::default(),
        health: HealthRulesDto::default(),
        profile: None,
        profile_binding: ProfileBindingModeDto::Fixed,
        labels: BTreeSet::new().into_iter().collect(),
        metadata: BTreeSet::new().into_iter().collect(),
    }
}

fn agreement_for_client_limits(limits: ClientConnectionLimits) -> ClientConnectionAgreement {
    agreement_with_limits(
        ConnectionLimits::new(
            limits.max_frame_bytes(),
            limits.max_logical_streams(),
            limits.max_pending_requests(),
            limits.connection_queue_bytes(),
            limits.stream_queue_bytes(),
            limits.initial_stream_window_bytes(),
        )
        .unwrap(),
    )
}

fn agreement_with_limits(limits: ConnectionLimits) -> ClientConnectionAgreement {
    let capabilities = BTreeSet::from([
        capability(connection_capabilities::MULTIPLEX),
        capability(connection_capabilities::FLOW_CONTROL),
        capability(connection_capabilities::STREAM_RESUME),
        capability(connection_capabilities::BINARY_PAYLOADS),
        capability(connection_capabilities::TRACE_CONTEXT),
    ]);
    let hello = ConnectionHello::new(
        ClientId::new("typed-sdk").unwrap(),
        CURRENT_PROTOCOL_VERSION,
        capabilities.clone(),
        BTreeSet::new(),
        limits,
    )
    .unwrap();
    let policy = ConnectionPolicy::new(
        CURRENT_PROTOCOL_VERSION,
        capabilities,
        limits,
        HeartbeatPolicy::new(60_000, 120_000).unwrap(),
        Some(60_000),
    )
    .unwrap();
    sdk_agreement(
        negotiate_connection(
            &hello,
            &policy,
            ConnectionId::new("connection-1").unwrap(),
            ServerEpoch::new("epoch-1").unwrap(),
        )
        .unwrap(),
    )
}

const TEST_FRAME_BYTES: u32 = 16_384;

fn sdk_agreement(value: ConnectionAgreement) -> ClientConnectionAgreement {
    let bytes = HandshakeCodec::new(TEST_FRAME_BYTES)
        .unwrap()
        .encode_response(&HandshakeResponse::Accepted(value))
        .unwrap();
    ClientConnectionAgreement::decode_accepted(&bytes, TEST_FRAME_BYTES).unwrap()
}

fn decode_outbound(value: ClientTransportOutbound) -> ClientMessage {
    let bytes = value.encode(TEST_FRAME_BYTES).unwrap();
    match WireCodec::new(TEST_FRAME_BYTES)
        .unwrap()
        .decode_client(&bytes)
        .unwrap()
    {
        DecodeStatus::Complete(frame) => frame.message,
        DecodeStatus::NeedMore { .. } => panic!("complete outbound frame expected"),
    }
}

fn encode_inbound(value: ServerMessage) -> ClientTransportInbound {
    let bytes = WireCodec::new(TEST_FRAME_BYTES)
        .unwrap()
        .encode_server(&value)
        .unwrap();
    ClientTransportInbound::decode(&bytes, TEST_FRAME_BYTES).unwrap()
}

fn trace_context(label: &str) -> ClientTraceContext {
    ClientTraceContext::new(format!("trace-{label}"), format!("span-{label}"))
        .expect("trace context")
}

fn capability(value: &str) -> NamespacedName {
    NamespacedName::new(value).unwrap()
}

#[test]
fn support_reference_links_clones_and_streams_without_reusing_trace_identity() {
    let codec = ProductCodec::new(ProductCodecLimits::new(4_096, 4_096, 4_096, 4_096).unwrap());
    let transport = Arc::new(ProductTransport::new(codec));
    let connector = Arc::new(SingleConnector {
        agreement: agreement(),
        transport: transport.clone(),
        attempts: AtomicUsize::new(0),
    });
    let runtime = ProductClientRuntime::new(
        ClientRuntimeLimits::new(8_192, 32, 32, 64_000).unwrap(),
        ClientRecoveryPolicy::new(
            Duration::from_millis(1),
            Duration::from_millis(2),
            10_000,
            1,
            2,
            4,
        )
        .unwrap(),
        Duration::from_secs(1),
        codec,
        TerminalOutputCodec::new(1_024).unwrap(),
    )
    .unwrap();
    let client = ProductClient::connect(connector, runtime, Arc::new(ZeroJitter))
        .unwrap()
        .with_correlation_id("UC-0123456789abcdef0123456789abcdef")
        .unwrap();
    for client in [client.clone(), client.clone()] {
        client
            .request(ProductRequest::GetDaemonStatus, Duration::from_secs(1))
            .unwrap();
    }
    let opened = client
        .open_subscription(
            ProductStreamSpec::TerminalOutput(TerminalOutputSpec {
                session_id: "session-300".into(),
                initial: TerminalInitialModeDto::None,
            }),
            Duration::from_secs(1),
        )
        .unwrap();
    let explicit = trace_context("explicit")
        .with_correlation_id("explicit-reference")
        .unwrap();
    client
        .request_with_trace_context(
            ProductRequest::GetDaemonStatus,
            explicit.clone(),
            Duration::from_secs(1),
        )
        .unwrap();
    let ids = transport.correlation_ids.lock().unwrap();
    assert_eq!(ids.len(), 4);
    assert!(
        ids[..3]
            .iter()
            .all(|id| id.as_deref() == Some("UC-0123456789abcdef0123456789abcdef"))
    );
    assert_eq!(ids[3].as_deref(), Some("explicit-reference"));
    let traces = transport.trace_contexts.lock().unwrap();
    assert_ne!(traces[0], traces[1]);
    assert_ne!(traces[1], traces[2]);
    assert_eq!(traces[3].0, explicit.trace_id());
    drop(traces);
    drop(ids);
    opened.subscription.cancel();
    client.close();
}

struct RecoveryConnector {
    first: Arc<ProductTransport>,
    replacement: Arc<ProductTransport>,
    attempts: AtomicUsize,
}
impl ClientConnector for RecoveryConnector {
    fn connect(
        &self,
        _: &ClientRecoveryCancellation,
    ) -> Result<NegotiatedClientTransport, ClientTransportError> {
        let transport = if self.attempts.fetch_add(1, Ordering::AcqRel) == 0 {
            self.first.clone()
        } else {
            self.replacement.clone()
        };
        Ok(NegotiatedClientTransport::new(agreement(), transport))
    }
}

fn recovering_product_stream(
    response: Option<Arc<[u8]>>,
) -> (
    ProductClient,
    Arc<ProductTransport>,
    undercurrent_sdk::OpenedProductSubscription,
) {
    let codec = ProductCodec::new(ProductCodecLimits::new(4096, 4096, 4096, 4096).unwrap());
    let first = Arc::new(ProductTransport::new(codec));
    let replacement = Arc::new(ProductTransport::new(codec));
    *replacement.resume_response.lock().unwrap() = response;
    let connector = Arc::new(RecoveryConnector {
        first: first.clone(),
        replacement: replacement.clone(),
        attempts: AtomicUsize::new(0),
    });
    let runtime = ProductClientRuntime::new(
        ClientRuntimeLimits::new(8192, 32, 32, 64000).unwrap(),
        ClientRecoveryPolicy::new(
            Duration::from_millis(1),
            Duration::from_millis(2),
            10000,
            1,
            2,
            4,
        )
        .unwrap(),
        Duration::from_secs(1),
        codec,
        TerminalOutputCodec::new(1024).unwrap(),
    )
    .unwrap();
    let client = ProductClient::connect(connector, runtime, Arc::new(ZeroJitter)).unwrap();
    let opened = client
        .open_subscription(
            ProductStreamSpec::TerminalOutput(TerminalOutputSpec {
                session_id: "recovery-session".into(),
                initial: TerminalInitialModeDto::Resume { after_offset: 0 },
            }),
            Duration::from_secs(1),
        )
        .unwrap();
    first.close();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while client.pressure().generation < 2 {
        assert!(
            std::time::Instant::now() < deadline,
            "replacement not installed"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(replacement.stream_count.load(Ordering::Acquire), 1);
    assert_eq!(
        client.pressure().phase,
        undercurrent_sdk::ResilientClientPhase::Connected
    );
    (client, replacement, opened)
}

fn resume_activation_result(response: Arc<[u8]>) -> ProductClientError {
    let (client, _, opened) = recovering_product_stream(Some(response));
    let error = opened
        .subscription
        .recv_timeout(Duration::from_secs(3))
        .expect_err("invalid activation must terminate the subscription");
    client
        .request(ProductRequest::GetDaemonStatus, Duration::from_secs(1))
        .expect("peer requests survive rejected stream");
    client.close();
    error
}

#[test]
fn resumed_product_rejection_reaches_the_caller_without_losing_details() {
    let codec = ProductCodec::new(ProductCodecLimits::new(4096, 4096, 4096, 4096).unwrap());
    let response = ProductStreamOpenResponse::Error(ProductStreamError {
        kind: None,
        category: ProductErrorCategory::FailedPrecondition,
        disposition: None,
        message: "resume explicitly rejected".into(),
        session_id: None,
        retryable: false,
        retry_after_millis: None,
        limit: None,
        resynchronization: None,
    });
    let error = resume_activation_result(Arc::from(
        codec.encode_stream_open_response(&response).unwrap(),
    ));
    assert!(matches!(error, ProductClientError::StreamRejected(error)
        if error.category == ProductErrorCategory::FailedPrecondition && error.message == "resume explicitly rejected"));
}

#[test]
fn resumed_product_kind_mismatch_is_rejected() {
    let codec = ProductCodec::new(ProductCodecLimits::new(4096, 4096, 4096, 4096).unwrap());
    let response = ProductStreamOpenResponse::Opened(ProductStreamOpened {
        kind: undercurrent_protocol::ProductStreamKind::SessionEvents,
        initial_sequence: 0,
        resumable: true,
        available_from: Some(0),
    });
    let error = resume_activation_result(Arc::from(
        codec.encode_stream_open_response(&response).unwrap(),
    ));
    assert!(matches!(
        error,
        ProductClientError::StreamKindMismatch { .. }
    ));
}

#[test]
fn malformed_resume_activation_is_rejected() {
    let error = resume_activation_result(Arc::from(&b"not product JSON"[..]));
    assert!(matches!(error, ProductClientError::Codec(_)));
}

#[test]
fn valid_resumed_product_activation_delivers_output() {
    let (client, replacement, opened) = recovering_product_stream(None);
    let terminal = TerminalOutputCodec::new(1024).unwrap();
    let bytes = terminal
        .encode(&TerminalOutputChunk::new(0, Arc::from(&b"resumed"[..])))
        .unwrap();
    replacement.inject(ServerMessage::StreamItem {
        stream_id: undercurrent_wire::StreamId::new(opened.subscription.stream_id()).unwrap(),
        item: InboundStreamItem::Data {
            sequence: StreamSequence::new(1),
            flow_controlled_bytes: bytes.len() as u64,
            bytes: Arc::from(bytes),
        },
    });
    let item = opened
        .subscription
        .recv_timeout(Duration::from_secs(1))
        .expect("valid resumed stream delivers data");
    assert_eq!(item.sequence(), 1);
    client.close();
}
