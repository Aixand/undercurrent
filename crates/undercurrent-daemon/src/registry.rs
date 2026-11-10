// SPDX-License-Identifier: Apache-2.0

//! Stable daemon dispatch diagnostic registry.

/// Product dispatch events emitted without request or terminal payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProductDispatchEventName {
    /// Action entered the bounded executor queue.
    ActionAdmitted,
    /// A worker dequeued one admitted action.
    ActionStarted,
    /// A worker finished or cancelled one dequeued action.
    ActionFinished,
    /// Bounded executor admission failed.
    AdmissionRejected,
    /// Finite request decoded into a typed operation.
    RequestDecoded,
    /// Finite request completed and its response was queued.
    RequestCompleted,
    /// Finite request could not be settled safely.
    RequestFailed,
    /// Stream specification decoded and prepared selective interest.
    StreamPrepared,
    /// Prepared logical stream became active.
    StreamActivated,
    /// Stream specification or preparation was rejected.
    StreamRejected,
    /// Request cancellation became visible to product execution.
    RequestCancelled,
    /// Product resources for one connection were released.
    ConnectionReleased,
    /// A product executor panicked and was contained at the adapter boundary.
    ExecutorPanicked,
}

impl ProductDispatchEventName {
    /// Complete stable event registry.
    pub const ALL: [Self; 13] = [
        Self::ActionAdmitted,
        Self::ActionStarted,
        Self::ActionFinished,
        Self::AdmissionRejected,
        Self::RequestDecoded,
        Self::RequestCompleted,
        Self::RequestFailed,
        Self::StreamPrepared,
        Self::StreamActivated,
        Self::StreamRejected,
        Self::RequestCancelled,
        Self::ConnectionReleased,
        Self::ExecutorPanicked,
    ];

    /// Returns the stable snake-case event name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ActionAdmitted => "product_action_admitted",
            Self::ActionStarted => "product_action_started",
            Self::ActionFinished => "product_action_finished",
            Self::AdmissionRejected => "product_admission_rejected",
            Self::RequestDecoded => "product_request_decoded",
            Self::RequestCompleted => "product_request_completed",
            Self::RequestFailed => "product_request_failed",
            Self::StreamPrepared => "product_stream_prepared",
            Self::StreamActivated => "product_stream_activated",
            Self::StreamRejected => "product_stream_rejected",
            Self::RequestCancelled => "product_request_cancelled",
            Self::ConnectionReleased => "product_connection_released",
            Self::ExecutorPanicked => "product_executor_panicked",
        }
    }
}

/// Fixed-cardinality product-dispatch metric registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProductDispatchMetricName {
    /// Finite and stream-open actions currently executing.
    RequestsInFlight,
    /// Time from bounded queue admission until worker dequeue.
    RequestQueueWaitSeconds,
    /// Time spent decoding, executing, encoding, and settling one finite RPC.
    RpcDurationSeconds,
    /// Terminal finite RPC result count.
    RpcResults,
    /// Encoded finite RPC response bytes.
    RpcResponseBytes,
}

impl ProductDispatchMetricName {
    /// Complete product-dispatch metric registry.
    pub const ALL: [Self; 5] = [
        Self::RequestsInFlight,
        Self::RequestQueueWaitSeconds,
        Self::RpcDurationSeconds,
        Self::RpcResults,
        Self::RpcResponseBytes,
    ];

    /// Returns the stable snake-case metric name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RequestsInFlight => "requests_in_flight",
            Self::RequestQueueWaitSeconds => "request_queue_wait_seconds",
            Self::RpcDurationSeconds => "rpc_duration_seconds",
            Self::RpcResults => "rpc_results",
            Self::RpcResponseBytes => "rpc_response_bytes",
        }
    }
}

/// Payload-free reason attached to dispatch failure diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProductDispatchResultCategory {
    /// Operation completed successfully.
    Success,
    /// Request or stream specification was invalid.
    InvalidRequest,
    /// Product executor rejected typed work.
    Rejected,
    /// Work was cancelled before settlement.
    Cancelled,
    /// Bounded capacity was exhausted.
    Capacity,
    /// Response could not be encoded within negotiated limits.
    Encoding,
    /// Connection could not accept final settlement.
    Connection,
    /// Executor violated an adapter invariant or panicked.
    Internal,
}

impl ProductDispatchResultCategory {
    /// Complete stable result registry.
    pub const ALL: [Self; 8] = [
        Self::Success,
        Self::InvalidRequest,
        Self::Rejected,
        Self::Cancelled,
        Self::Capacity,
        Self::Encoding,
        Self::Connection,
        Self::Internal,
    ];

    /// Returns the stable snake-case result category.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::InvalidRequest => "invalid_request",
            Self::Rejected => "rejected",
            Self::Cancelled => "cancelled",
            Self::Capacity => "capacity",
            Self::Encoding => "encoding",
            Self::Connection => "connection",
            Self::Internal => "internal",
        }
    }
}
