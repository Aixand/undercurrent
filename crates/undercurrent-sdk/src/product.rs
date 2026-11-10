// SPDX-License-Identifier: Apache-2.0

//! Typed product encoding and correlation over the blocking resilient runtime.
//!
//! Finite operations use protocol DTOs and verify the response operation. Initial
//! stream opens verify product kind; item decoding is owned by `subscription`.
//! Capability preflight rejects guarded and opaque requests before encoding.
//! This layer does not retry mutations or own daemon-side effects.

mod error;
mod subscription;

use std::sync::Arc;
use std::time::Duration;

use undercurrent_protocol::{
    CancelRuntimeAttemptRequest, CancelRuntimeAttemptResultDto, CreateDiagnosticBundleRequest,
    DiagnosticBundleDto, GetAgentEventRequest, GetAgentEventResultDto,
    GetRuntimeCancellationStatusRequest, OutputCaptureModeDto, ProductCodec, ProductCodecLimits,
    ProductErrorCategory, ProductOperation, ProductRequest, ProductResponse,
    ProductStreamOpenResponse, ProductStreamSpec, ProductSuccess, ReplyAgentEventRequest,
    ReplyAgentEventResultDto, RuntimeCancellationStatusResultDto, TerminalOutputCodec,
};

use crate::{
    ClientConnector, ClientRecoveryJitter, ClientRecoveryPolicy, ClientRuntimeLimits,
    ResilientClient, ResilientPendingRequest,
};

pub use error::*;
pub use subscription::*;

/// Undercurrent connection capabilities required by the complete typed product client.
pub const PRODUCT_CLIENT_REQUIRED_CAPABILITIES: [&str; 5] = [
    undercurrent_wire::connection_capabilities::MULTIPLEX,
    undercurrent_wire::connection_capabilities::FLOW_CONTROL,
    undercurrent_wire::connection_capabilities::STREAM_RESUME,
    undercurrent_wire::connection_capabilities::BINARY_PAYLOADS,
    undercurrent_wire::connection_capabilities::TRACE_CONTEXT,
];

/// Product-owned capabilities offered opportunistically by the typed client.
pub const PRODUCT_CLIENT_OPTIONAL_CAPABILITIES: [&str; 4] = [
    undercurrent_protocol::product_capabilities::ERROR_DISPOSITION,
    undercurrent_protocol::product_capabilities::ERROR_EFFECT_CERTAINTY,
    undercurrent_protocol::product_capabilities::OPAQUE_OUTPUT_CAPTURE,
    undercurrent_protocol::product_capabilities::SESSION_REVISION_PRECONDITION,
];

/// Validated codecs, client bounds, and recovery settings for a product client.
#[derive(Debug, Clone, Copy)]
pub struct ProductClientRuntime {
    client_limits: ClientRuntimeLimits,
    recovery_policy: ClientRecoveryPolicy,
    resume_timeout: Duration,
    product_codec: ProductCodec,
    terminal_codec: TerminalOutputCodec,
}

impl ProductClientRuntime {
    /// Validates product codecs against the SDK payload boundary.
    ///
    /// This function performs no IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns the first product payload class that exceeds the SDK bound or a
    /// zero stream-resume timeout.
    pub fn new(
        client_limits: ClientRuntimeLimits,
        recovery_policy: ClientRecoveryPolicy,
        resume_timeout: Duration,
        product_codec: ProductCodec,
        terminal_codec: TerminalOutputCodec,
    ) -> Result<Self, ProductClientLimitError> {
        if resume_timeout.is_zero() {
            return Err(ProductClientLimitError::ZeroResumeTimeout);
        }
        validate_codec_limits(client_limits, product_codec.limits(), terminal_codec)?;
        Ok(Self {
            client_limits,
            recovery_policy,
            resume_timeout,
            product_codec,
            terminal_codec,
        })
    }

    /// Returns generic shared-connection memory limits.
    pub const fn client_limits(self) -> ClientRuntimeLimits {
        self.client_limits
    }

    /// Returns bounded physical reconnect and stream-resume policy.
    pub const fn recovery_policy(self) -> ClientRecoveryPolicy {
        self.recovery_policy
    }

    /// Returns the deadline applied to each logical stream resume.
    pub const fn resume_timeout(self) -> Duration {
        self.resume_timeout
    }

    /// Returns the finite and JSON-stream product codec.
    pub const fn product_codec(self) -> ProductCodec {
        self.product_codec
    }

    /// Returns the compact binary terminal stream codec.
    pub const fn terminal_codec(self) -> TerminalOutputCodec {
        self.terminal_codec
    }
}

/// Cheaply clonable typed client sharing one resilient physical connection.
#[derive(Clone)]
pub struct ProductClient {
    inner: ResilientClient,
    product_codec: ProductCodec,
    terminal_codec: TerminalOutputCodec,
    correlation_id: Option<undercurrent_wire::CorrelationId>,
}

/// Opaque ownership of one correlated finite product request.
#[must_use = "a pending product request must be awaited or explicitly cancelled"]
pub struct PendingProductRequest {
    inner: ResilientPendingRequest,
    codec: ProductCodec,
    expected: ProductOperation,
}

impl PendingProductRequest {
    /// Sends an explicit cancellation for this request on its existing connection.
    ///
    /// Success means only that the cancellation frame entered the local outbound
    /// queue. [`Self::wait`] returns the server-correlated terminal result.
    ///
    /// # Errors
    ///
    /// Returns a typed current-connection or bounded-queue failure.
    pub fn cancel(&self) -> Result<(), ProductClientError> {
        self.inner.cancel().map_err(ProductClientError::Client)
    }

    /// Waits for the request's correlated normal or cancellation terminal result.
    ///
    /// The timeout starts here, after request admission. It excludes connection
    /// setup and time spent holding this handle before calling `wait`. Timeout
    /// sends best-effort cancellation and does not prove a mutation was rolled back.
    ///
    /// # Errors
    ///
    /// Returns codec, connection, timeout, typed server, or correlation failure.
    pub fn wait(self, timeout: Duration) -> Result<ProductSuccess, ProductClientError> {
        let response = self
            .inner
            .wait(timeout)
            .map_err(ProductClientError::Client)?;
        decode_product_response(self.codec, self.expected, &response)
    }
}

impl ProductClient {
    /// Connects one resilient physical client through a concrete connector.
    ///
    /// All clones, finite operations, and returned subscriptions share the
    /// resulting connection and its recovery supervisor. This function performs
    /// bounded transport, handshake, thread, and resume IO and does not panic.
    ///
    /// # Errors
    ///
    /// Returns a source-retaining typed connection failure.
    pub fn connect(
        connector: Arc<dyn ClientConnector>,
        runtime: ProductClientRuntime,
        jitter: Arc<dyn ClientRecoveryJitter>,
    ) -> Result<Self, ProductClientError> {
        let inner = ResilientClient::connect(
            connector,
            runtime.client_limits(),
            runtime.recovery_policy(),
            jitter,
            runtime.resume_timeout(),
        )
        .map_err(ProductClientError::Connect)?;
        Self::from_resilient(inner, runtime)
    }

    /// Connects with a separate initial retry budget, retaining normal recovery.
    ///
    /// # Errors
    /// Returns a source-retaining typed connection or capability failure.
    pub fn connect_with_initial_policy(
        connector: Arc<dyn ClientConnector>,
        runtime: ProductClientRuntime,
        initial_policy: ClientRecoveryPolicy,
        jitter: Arc<dyn ClientRecoveryJitter>,
    ) -> Result<Self, ProductClientError> {
        let inner = ResilientClient::connect_with_initial_policy(
            connector,
            runtime.client_limits(),
            initial_policy,
            runtime.recovery_policy(),
            jitter,
            runtime.resume_timeout(),
        )
        .map_err(ProductClientError::Connect)?;
        Self::from_resilient(inner, runtime)
    }

    /// Wraps an already connected resilient runtime with typed product codecs.
    ///
    /// Successful wrapping performs no transport IO. The caller must supply
    /// codecs and runtime bounds consistent with the existing resilient client;
    /// this method does not reconfigure it. Missing capabilities close that
    /// shared client, including resources retained by other clones.
    ///
    /// # Errors
    ///
    /// Returns and closes the client when the installed agreement omits a
    /// capability required by the typed product layer.
    pub fn from_resilient(
        inner: ResilientClient,
        runtime: ProductClientRuntime,
    ) -> Result<Self, ProductClientError> {
        for capability in PRODUCT_CLIENT_REQUIRED_CAPABILITIES {
            if !inner
                .agreement()
                .is_some_and(|agreement| agreement.capabilities().contains(&capability))
            {
                inner.close();
                return Err(ProductClientError::MissingCapability { capability });
            }
        }
        Ok(Self {
            inner,
            product_codec: runtime.product_codec(),
            terminal_codec: runtime.terminal_codec(),
            correlation_id: None,
        })
    }

    /// Associates subsequent requests and subscriptions with a support reference.
    ///
    /// Each operation still gets its own generated trace/span identity. Clones
    /// retain the reference, stream recovery retains its original context, and
    /// explicitly supplied trace context takes precedence. This identifier is
    /// diagnostic metadata, never an idempotency key or retry authorization.
    ///
    /// # Errors
    /// Returns a boundary error for an invalid correlation identifier.
    pub fn with_correlation_id(
        mut self,
        id: impl Into<String>,
    ) -> Result<Self, crate::ClientBoundaryError> {
        self.correlation_id = Some(
            undercurrent_wire::CorrelationId::new(id.into())
                .map_err(|error| crate::ClientBoundaryError::new("correlation_id", error))?,
        );
        Ok(self)
    }

    fn correlated_context(
        &self,
        explicit: Option<crate::ClientTraceContext>,
    ) -> Result<Option<crate::ClientTraceContext>, ProductClientError> {
        match (explicit, &self.correlation_id) {
            (Some(context), _) => Ok(Some(context)),
            (None, Some(id)) => self
                .inner
                .correlated_context(id.clone())
                .map(Some)
                .map_err(ProductClientError::Client),
            (None, None) => Ok(None),
        }
    }

    /// Executes one typed finite request and returns its typed success value.
    ///
    /// Server rejections remain typed and operation correlation is verified
    /// after decoding. Private request fields never enter the returned error.
    /// This method blocks, never automatically replays the request, and applies
    /// `timeout` to the response wait after admission, not to connection setup.
    ///
    /// # Errors
    ///
    /// Returns codec, connection, server rejection, or protocol-correlation
    /// failures.
    pub fn request(
        &self,
        request: ProductRequest,
        timeout: Duration,
    ) -> Result<ProductSuccess, ProductClientError> {
        self.request_cancellable(request)?.wait(timeout)
    }

    /// Starts one typed request and returns its opaque cancellation boundary.
    ///
    /// The handle retains the original request correlation on the current
    /// physical connection. It exposes neither transport state nor request IDs.
    ///
    /// # Errors
    ///
    /// Returns codec, recovery-state, connection, or bounded-queue failure.
    pub fn request_cancellable(
        &self,
        request: ProductRequest,
    ) -> Result<PendingProductRequest, ProductClientError> {
        self.request_cancellable_inner(request, None)
    }

    /// Creates one bounded diagnostic bundle over the authenticated product connection.
    ///
    /// Terminal content remains absent unless the request explicitly selects one
    /// session and the daemon grants the additional export and subscription
    /// authorizations. This method performs one finite product request and does
    /// not expose wire or multiplex state.
    ///
    /// # Errors
    ///
    /// Returns codec, connection, authorization, server, or protocol-correlation
    /// failures.
    pub fn create_diagnostic_bundle(
        &self,
        request: CreateDiagnosticBundleRequest,
        timeout: Duration,
    ) -> Result<DiagnosticBundleDto, ProductClientError> {
        match self.request(ProductRequest::CreateDiagnosticBundle(request), timeout)? {
            ProductSuccess::CreateDiagnosticBundle(bundle) => Ok(bundle),
            other => Err(ProductClientError::OperationMismatch {
                expected: ProductOperation::CreateDiagnosticBundle,
                actual: Some(other.operation()),
            }),
        }
    }

    /// Durably admits and applies cancellation to one exact attached runtime attempt.
    ///
    /// Retries are authorized by the daemon before its durable cancellation ledger
    /// decides exact replay. A timeout or response loss must be reconciled with
    /// [`Self::get_runtime_cancellation_status`]; this client does not auto-redrive.
    ///
    /// # Errors
    ///
    /// Returns codec, connection, authorization, typed server, or correlation failures.
    pub fn cancel_runtime_attempt(
        &self,
        request: CancelRuntimeAttemptRequest,
        timeout: Duration,
    ) -> Result<CancelRuntimeAttemptResultDto, ProductClientError> {
        match self.request(ProductRequest::CancelRuntimeAttempt(request), timeout)? {
            ProductSuccess::CancelRuntimeAttempt(result) => Ok(result),
            other => Err(ProductClientError::OperationMismatch {
                expected: ProductOperation::CancelRuntimeAttempt,
                actual: Some(other.operation()),
            }),
        }
    }

    /// Reads one exact durable runtime-cancellation status without process IO or redrive.
    ///
    /// # Errors
    ///
    /// Returns codec, connection, authorization, typed server, or correlation failures.
    pub fn get_runtime_cancellation_status(
        &self,
        request: GetRuntimeCancellationStatusRequest,
        timeout: Duration,
    ) -> Result<RuntimeCancellationStatusResultDto, ProductClientError> {
        match self.request(
            ProductRequest::GetRuntimeCancellationStatus(request),
            timeout,
        )? {
            ProductSuccess::RuntimeCancellationStatus(result) => Ok(result),
            other => Err(ProductClientError::OperationMismatch {
                expected: ProductOperation::GetRuntimeCancellationStatus,
                actual: Some(other.operation()),
            }),
        }
    }

    /// Polls the daemon's durable queue for one exact agent-runtime event.
    ///
    /// The request carries the full caller/provider fence. The daemon rejects a
    /// stale or expired binding instead of returning another attempt's event.
    pub fn get_agent_event(
        &self,
        request: GetAgentEventRequest,
        timeout: Duration,
    ) -> Result<GetAgentEventResultDto, ProductClientError> {
        match self.request(ProductRequest::GetAgentEvent(request), timeout)? {
            ProductSuccess::GetAgentEvent(result) => Ok(result),
            other => Err(ProductClientError::OperationMismatch {
                expected: ProductOperation::GetAgentEvent,
                actual: Some(other.operation()),
            }),
        }
    }

    /// Retains one exact Caller-owned reply for a durable agent event.
    pub fn reply_agent_event(
        &self,
        request: ReplyAgentEventRequest,
        timeout: Duration,
    ) -> Result<ReplyAgentEventResultDto, ProductClientError> {
        match self.request(ProductRequest::ReplyAgentEvent(request), timeout)? {
            ProductSuccess::ReplyAgentEvent(result) => Ok(result),
            other => Err(ProductClientError::OperationMismatch {
                expected: ProductOperation::ReplyAgentEvent,
                actual: Some(other.operation()),
            }),
        }
    }

    /// Executes one typed request under caller-supplied distributed trace context.
    ///
    /// # Errors
    ///
    /// Returns codec, connection, server rejection, or protocol-correlation
    /// failures.
    pub fn request_with_trace_context(
        &self,
        request: ProductRequest,
        trace_context: crate::ClientTraceContext,
        timeout: Duration,
    ) -> Result<ProductSuccess, ProductClientError> {
        self.request_cancellable_with_trace_context(request, trace_context)?
            .wait(timeout)
    }

    /// Starts one cancellable request under caller-supplied trace context.
    ///
    /// # Errors
    ///
    /// Returns codec, recovery-state, connection, or bounded-queue failure.
    pub fn request_cancellable_with_trace_context(
        &self,
        request: ProductRequest,
        trace_context: crate::ClientTraceContext,
    ) -> Result<PendingProductRequest, ProductClientError> {
        self.request_cancellable_inner(request, Some(trace_context))
    }

    /// Encodes and correlates one request with generated or supplied context.
    fn request_cancellable_inner(
        &self,
        request: ProductRequest,
        trace_context: Option<crate::ClientTraceContext>,
    ) -> Result<PendingProductRequest, ProductClientError> {
        if request.session_revision_precondition().is_some()
            && !self.inner.agreement().is_some_and(|agreement| {
                agreement.capabilities().contains(
                    &undercurrent_protocol::product_capabilities::SESSION_REVISION_PRECONDITION,
                )
            })
        {
            return Err(ProductClientError::MissingCapability {
                capability:
                    undercurrent_protocol::product_capabilities::SESSION_REVISION_PRECONDITION,
            });
        }
        if matches!(
            &request,
            ProductRequest::StartSession(start)
                if start.output_capture_mode == OutputCaptureModeDto::Opaque
        ) && !self.inner.agreement().is_some_and(|agreement| {
            agreement
                .capabilities()
                .contains(&undercurrent_protocol::product_capabilities::OPAQUE_OUTPUT_CAPTURE)
        }) {
            return Err(ProductClientError::MissingCapability {
                capability: undercurrent_protocol::product_capabilities::OPAQUE_OUTPUT_CAPTURE,
            });
        }
        let expected = request.operation();
        let payload = self
            .product_codec
            .encode_request(&request)
            .map(Arc::from)
            .map_err(ProductClientError::Codec)?;
        let inner = match self.correlated_context(trace_context)? {
            Some(context) => self
                .inner
                .start_request_with_trace_context(payload, context),
            None => self.inner.start_request(payload),
        }
        .map_err(ProductClientError::Client)?;
        Ok(PendingProductRequest {
            inner,
            codec: self.product_codec,
            expected,
        })
    }

    /// Opens one typed persistent logical stream on the shared connection.
    ///
    /// The stream specification is retained only by the resilient SDK for
    /// bounded resume. A rejected or mismatched initial activation cancels the
    /// logical stream. If its close command cannot enter the bounded writer,
    /// physical teardown can also trigger peer recovery.
    ///
    /// # Errors
    ///
    /// Returns codec, connection, typed server rejection, or kind-correlation
    /// failures.
    pub fn open_subscription(
        &self,
        specification: ProductStreamSpec,
        timeout: Duration,
    ) -> Result<OpenedProductSubscription, ProductClientError> {
        self.open_subscription_inner(specification, None, timeout)
    }

    /// Opens one typed persistent stream under caller-supplied trace context.
    ///
    /// # Errors
    ///
    /// Returns codec, connection, typed server rejection, or kind-correlation
    /// failures.
    pub fn open_subscription_with_trace_context(
        &self,
        specification: ProductStreamSpec,
        trace_context: crate::ClientTraceContext,
        timeout: Duration,
    ) -> Result<OpenedProductSubscription, ProductClientError> {
        self.open_subscription_inner(specification, Some(trace_context), timeout)
    }

    /// Encodes and validates one stream open with generated or supplied context.
    fn open_subscription_inner(
        &self,
        specification: ProductStreamSpec,
        trace_context: Option<crate::ClientTraceContext>,
        timeout: Duration,
    ) -> Result<OpenedProductSubscription, ProductClientError> {
        let expected = specification.kind();
        let payload = self
            .product_codec
            .encode_stream_spec(&specification)
            .map(Arc::from)
            .map_err(ProductClientError::Codec)?;
        let context = match self.correlated_context(trace_context)? {
            Some(context) => context,
            None => self
                .inner
                .next_trace_context()
                .map_err(ProductClientError::Client)?,
        };
        let codec = self.product_codec;
        let validator = Arc::new(move |response: &[u8]| {
            matches!(
                codec.decode_stream_open_response(response),
                Ok(ProductStreamOpenResponse::Opened(metadata)) if metadata.kind == expected
            )
        });
        let opened = self
            .inner
            .open_subscription_validated(payload, context, timeout, Some(validator))
            .map_err(ProductClientError::Client)?;
        let response = match self
            .product_codec
            .decode_stream_open_response(&opened.response)
        {
            Ok(response) => response,
            Err(error) => {
                opened.subscription.cancel();
                return Err(ProductClientError::Codec(error));
            }
        };
        match response {
            ProductStreamOpenResponse::Error(error) => {
                opened.subscription.cancel();
                Err(ProductClientError::StreamRejected(error))
            }
            ProductStreamOpenResponse::Opened(metadata) if metadata.kind != expected => {
                opened.subscription.cancel();
                Err(ProductClientError::StreamKindMismatch {
                    expected,
                    actual: metadata.kind,
                })
            }
            ProductStreamOpenResponse::Opened(metadata) => Ok(OpenedProductSubscription {
                subscription: ProductSubscription::new(
                    metadata.kind,
                    opened.subscription,
                    self.product_codec,
                    self.terminal_codec,
                ),
                metadata,
            }),
        }
    }

    /// Returns bounded current recovery pressure without transport IO.
    pub fn pressure(&self) -> crate::ResilientClientPressure {
        self.inner.pressure()
    }

    /// Returns the current negotiated connection agreement when connected.
    pub fn agreement(&self) -> Option<crate::ClientConnectionAgreement> {
        self.inner.agreement()
    }

    /// Returns default bounded SDK diagnostics, or `None` for a custom sink.
    pub fn telemetry_diagnostics(&self) -> Option<Vec<crate::ClientDiagnosticName>> {
        self.inner.telemetry_diagnostics()
    }

    /// Returns default fixed-cardinality SDK metrics, or `None` for a custom sink.
    pub fn telemetry_metric_series(&self) -> Option<Vec<crate::ClientMetricSeries>> {
        self.inner.telemetry_metric_series()
    }

    /// Returns default SDK telemetry pressure, or `None` for a custom sink.
    pub fn telemetry_pressure(&self) -> Option<crate::ClientTelemetryPressure> {
        self.inner.telemetry_pressure()
    }

    /// Stops recovery and closes the connection for all clones and subscriptions.
    ///
    /// This does not stop daemon-owned sessions or wait for a remote shutdown
    /// acknowledgment. Dropping one clone alone does not close other owners.
    pub fn close(&self) {
        self.inner.close();
    }
}

fn decode_product_response(
    codec: ProductCodec,
    expected: ProductOperation,
    response: &[u8],
) -> Result<ProductSuccess, ProductClientError> {
    match codec
        .decode_response(response)
        .map_err(ProductClientError::Codec)?
    {
        ProductResponse::Success(success) if success.operation() == expected => Ok(*success),
        ProductResponse::Success(success) => Err(ProductClientError::OperationMismatch {
            expected,
            actual: Some(success.operation()),
        }),
        // The daemon can acknowledge cancellation without decoding the request,
        // returning Cancelled with no operation. The physical request ID still
        // correlates it; other operation-less errors remain mismatches.
        // See [dispatch cancellation](../../undercurrent-daemon/docs/internals/requests/request-flow.md#admission-concurrency-and-lifetime).
        ProductResponse::Error(error)
            if error.operation == Some(expected)
                || (error.operation.is_none()
                    && error.category == ProductErrorCategory::Cancelled) =>
        {
            Err(ProductClientError::Server(error))
        }
        ProductResponse::Error(error) => Err(ProductClientError::OperationMismatch {
            expected,
            actual: error.operation,
        }),
    }
}

fn validate_codec_limits(
    client: ClientRuntimeLimits,
    product: ProductCodecLimits,
    terminal: TerminalOutputCodec,
) -> Result<(), ProductClientLimitError> {
    let maximum = usize::try_from(client.max_payload_bytes()).unwrap_or(usize::MAX);
    for (field, value) in [
        ("request_bytes", product.request_bytes()),
        ("response_bytes", product.response_bytes()),
        ("stream_spec_bytes", product.stream_spec_bytes()),
        ("stream_item_bytes", product.stream_item_bytes()),
        (
            "terminal_item_bytes",
            terminal
                .max_data_bytes()
                .saturating_add(TerminalOutputCodec::screen_header_bytes()),
        ),
    ] {
        if value > maximum {
            return Err(ProductClientLimitError::PayloadExceedsClient { field, maximum });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn product_runtime_rejects_payload_classes_above_the_shared_client_bound() {
        let client = ClientRuntimeLimits::new(128, 8, 8, 128).unwrap();
        let recovery = ClientRecoveryPolicy::new(
            Duration::from_millis(1),
            Duration::from_millis(2),
            10_000,
            1,
            1,
            1,
        )
        .unwrap();
        let product = ProductCodec::new(ProductCodecLimits::new(129, 128, 128, 128).unwrap());
        let terminal = TerminalOutputCodec::new(64).unwrap();

        assert!(matches!(
            ProductClientRuntime::new(client, recovery, Duration::from_secs(1), product, terminal,),
            Err(ProductClientLimitError::PayloadExceedsClient {
                field: "request_bytes",
                maximum: 128,
            })
        ));
    }
}
