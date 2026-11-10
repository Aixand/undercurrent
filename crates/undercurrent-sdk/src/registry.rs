// SPDX-License-Identifier: Apache-2.0

//! Central SDK connection diagnostic and metric registries.

/// Stable SDK diagnostic events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ClientDiagnosticName {
    /// Shared physical connection workers started.
    ConnectionStarted,
    /// Finite request entered the bounded writer.
    RequestQueued,
    /// Correlated response reached its pending caller.
    ResponseRouted,
    /// Response arrived after timeout or cancellation.
    LateResponseIgnored,
    /// Logical subscription was opened.
    SubscriptionOpened,
    /// Logical subscription was independently closed.
    SubscriptionClosed,
    /// Stream item reached its selective mailbox.
    StreamItemRouted,
    /// Bounded client stream mailbox rejected data.
    SlowConsumerDetected,
    /// Credit return was deferred by writer pressure.
    CreditDeferred,
    /// Bounded writer rejected a command.
    WriterSaturated,
    /// Shared connection teardown failed pending work.
    ConnectionClosed,
    /// Persistent supervisor began replacement connection recovery.
    RecoveryStarted,
    /// Replacement connection and live logical streams were installed.
    RecoverySucceeded,
    /// Bounded replacement connection recovery stopped.
    RecoveryFailed,
    /// One logical stream was rejected while its peers resumed.
    SubscriptionResumeFailed,
}

impl ClientDiagnosticName {
    /// Complete SDK diagnostic registry.
    pub const ALL: [Self; 15] = [
        Self::ConnectionStarted,
        Self::RequestQueued,
        Self::ResponseRouted,
        Self::LateResponseIgnored,
        Self::SubscriptionOpened,
        Self::SubscriptionClosed,
        Self::StreamItemRouted,
        Self::SlowConsumerDetected,
        Self::CreditDeferred,
        Self::WriterSaturated,
        Self::ConnectionClosed,
        Self::RecoveryStarted,
        Self::RecoverySucceeded,
        Self::RecoveryFailed,
        Self::SubscriptionResumeFailed,
    ];

    /// Returns the stable snake-case event name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConnectionStarted => "connection_started",
            Self::RequestQueued => "request_queued",
            Self::ResponseRouted => "response_routed",
            Self::LateResponseIgnored => "late_response_ignored",
            Self::SubscriptionOpened => "subscription_opened",
            Self::SubscriptionClosed => "subscription_closed",
            Self::StreamItemRouted => "stream_item_routed",
            Self::SlowConsumerDetected => "slow_consumer_detected",
            Self::CreditDeferred => "credit_deferred",
            Self::WriterSaturated => "writer_saturated",
            Self::ConnectionClosed => "connection_closed",
            Self::RecoveryStarted => "recovery_started",
            Self::RecoverySucceeded => "recovery_succeeded",
            Self::RecoveryFailed => "recovery_failed",
            Self::SubscriptionResumeFailed => "subscription_resume_failed",
        }
    }
}

/// Stable SDK metrics without client or stream identifiers as labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ClientMetricName {
    /// Active shared physical client connections.
    PhysicalConnections,
    /// Active pending finite requests.
    PendingRequests,
    /// Active logical subscriptions.
    LogicalSubscriptions,
    /// Semantic messages waiting for the one writer.
    WriterQueuedMessages,
    /// Bytes retained by selective stream mailboxes.
    StreamMailboxBytes,
    /// Consumed bytes waiting for a credit update.
    DeferredCreditBytes,
    /// Finite RPC duration.
    RequestDurationSeconds,
    /// Reconnection attempts.
    ReconnectAttempts,
    /// Logical stream resume attempts.
    ResumeAttempts,
    /// Logical stream resume successes.
    ResumeSucceeded,
    /// Logical stream resume failures.
    ResumeFailed,
    /// Client-side bounded-capacity rejections.
    OverloadRejections,
}

impl ClientMetricName {
    /// Complete SDK metric registry.
    pub const ALL: [Self; 12] = [
        Self::PhysicalConnections,
        Self::PendingRequests,
        Self::LogicalSubscriptions,
        Self::WriterQueuedMessages,
        Self::StreamMailboxBytes,
        Self::DeferredCreditBytes,
        Self::RequestDurationSeconds,
        Self::ReconnectAttempts,
        Self::ResumeAttempts,
        Self::ResumeSucceeded,
        Self::ResumeFailed,
        Self::OverloadRejections,
    ];

    /// Returns the stable snake-case metric name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PhysicalConnections => "physical_connections",
            Self::PendingRequests => "pending_requests",
            Self::LogicalSubscriptions => "logical_subscriptions",
            Self::WriterQueuedMessages => "writer_queued_messages",
            Self::StreamMailboxBytes => "stream_mailbox_bytes",
            Self::DeferredCreditBytes => "deferred_credit_bytes",
            Self::RequestDurationSeconds => "request_duration_seconds",
            Self::ReconnectAttempts => "reconnect_attempts",
            Self::ResumeAttempts => "resume_attempts",
            Self::ResumeSucceeded => "resume_succeeded",
            Self::ResumeFailed => "resume_failed",
            Self::OverloadRejections => "overload_rejections",
        }
    }
}
