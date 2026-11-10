// SPDX-License-Identifier: Apache-2.0

//! Typed product stream decoding and explicit discontinuity reporting.
//!
//! JSON kinds are checked against initial activation; terminal payloads use the
//! binary codec. The underlying delivery cursor advances before typed decoding
//! and application processing, so it is not a durable application acknowledgment.
//! Codec failures cancel the interest; close-queue pressure can close the shared
//! physical connection as described by the runtime's teardown behavior.

use std::fmt::{Debug, Formatter};
use std::time::Duration;

use undercurrent_multiplex::{InboundStreamItem, StreamGap, StreamGapReason, StreamRecoveryAction};
use undercurrent_protocol::{
    ProductCodec, ProductStreamItem, ProductStreamKind, ProductStreamOpened, TerminalOutputCodec,
    TerminalStreamItem,
};

use super::ProductClientError;
use crate::{ResilientSubscription, ResilientSubscriptionError};

/// Successful typed logical-stream activation and its independent handle.
pub struct OpenedProductSubscription {
    /// Server-declared replay and activation metadata.
    pub metadata: ProductStreamOpened,
    /// Persistent typed stream sharing the product client's connection.
    pub subscription: ProductSubscription,
}

/// Product-facing reason a logical stream cannot provide contiguous data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProductStreamGapReason {
    /// Droppable live data exceeded a bounded scheduler queue.
    LiveQueueOverflow,
    /// Requested sequence was evicted from bounded replay retention.
    ReplayEvicted,
    /// No retained journal exists for the requested stream.
    ReplayUnavailable,
    /// Server process epoch changed and invalidated in-memory replay.
    ServerEpochChanged,
    /// The application source evicted data before its checkpoint was consumed.
    SourceRetentionEvicted,
}

impl ProductStreamGapReason {
    /// Returns the stable machine and diagnostic spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LiveQueueOverflow => "live_queue_overflow",
            Self::ReplayEvicted => "replay_evicted",
            Self::ReplayUnavailable => "replay_unavailable",
            Self::ServerEpochChanged => "server_epoch_changed",
            Self::SourceRetentionEvicted => "source_retention_evicted",
        }
    }
}

/// Product-facing deterministic recovery required after a stream gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProductStreamRecovery {
    /// Request an authoritative state or screen snapshot.
    RequestSnapshot,
    /// Restart the filtered query from a new snapshot revision.
    RestartQuery,
    /// Restart terminal output from the earliest available byte offset.
    RestartFromAvailableOffset,
}

impl ProductStreamRecovery {
    /// Returns the stable machine and diagnostic spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RequestSnapshot => "request_snapshot",
            Self::RestartQuery => "restart_query",
            Self::RestartFromAvailableOffset => "restart_from_available_offset",
        }
    }
}

/// Product-level logical-stream discontinuity independent of transport types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductStreamGap {
    /// Affected client-scoped logical stream.
    pub stream_id: String,
    /// First missing stream-local sequence.
    pub first_missing: u64,
    /// Last missing stream-local sequence.
    pub last_missing: u64,
    /// Stable reason data is unavailable.
    pub reason: ProductStreamGapReason,
    /// Required deterministic recovery action.
    pub recovery: ProductStreamRecovery,
}

/// One decoded logical-stream item or explicit sequence gap.
// Preserve the public Json variant with its by-value ProductStreamItem field.
#[allow(clippy::large_enum_variant)]
pub enum ProductSubscriptionItem {
    /// Bounded JSON item for query, event, or diagnostic streams.
    Json {
        /// Monotonic stream-local sequence.
        sequence: u64,
        /// Typed product item validated against the activated stream kind.
        item: ProductStreamItem,
    },
    /// Compact binary terminal output or screen state.
    Terminal {
        /// Monotonic stream-local sequence.
        sequence: u64,
        /// Raw terminal item decoded without JSON or base64.
        item: TerminalStreamItem,
    },
    /// Explicit replay or live-delivery discontinuity.
    Gap(ProductStreamGap),
}

impl ProductSubscriptionItem {
    /// Returns the sequence delivered or the last missing sequence for a gap.
    pub const fn sequence(&self) -> u64 {
        match self {
            Self::Json { sequence, .. } | Self::Terminal { sequence, .. } => *sequence,
            Self::Gap(gap) => gap.last_missing,
        }
    }
}

impl Debug for ProductSubscriptionItem {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json { sequence, item } => formatter
                .debug_struct("ProductSubscriptionItem::Json")
                .field("sequence", sequence)
                .field("kind", &stream_item_kind(item))
                .finish(),
            Self::Terminal { sequence, item } => formatter
                .debug_struct("ProductSubscriptionItem::Terminal")
                .field("sequence", sequence)
                .field("kind", &terminal_item_name(item))
                .finish_non_exhaustive(),
            Self::Gap(gap) => formatter
                .debug_tuple("ProductSubscriptionItem::Gap")
                .field(gap)
                .finish(),
        }
    }
}

/// Persistent typed stream whose handle survives physical reconnection.
pub struct ProductSubscription {
    kind: ProductStreamKind,
    inner: ResilientSubscription,
    product_codec: ProductCodec,
    terminal_codec: TerminalOutputCodec,
}

impl ProductSubscription {
    pub(super) fn new(
        kind: ProductStreamKind,
        inner: ResilientSubscription,
        product_codec: ProductCodec,
        terminal_codec: TerminalOutputCodec,
    ) -> Self {
        Self {
            kind,
            inner,
            product_codec,
            terminal_codec,
        }
    }

    /// Returns the activated product stream kind.
    pub const fn kind(&self) -> ProductStreamKind {
        self.kind
    }

    /// Returns the stable client-scoped logical stream identity.
    pub fn stream_id(&self) -> &str {
        self.inner.stream_id_raw().as_str()
    }

    /// Returns the last outer sequence consumed by the underlying SDK stream.
    ///
    /// This is not a terminal byte offset or durable application checkpoint.
    /// Consumption precedes typed decoding and application processing.
    pub fn last_sequence(&self) -> u64 {
        self.inner.last_sequence_raw().value()
    }

    /// Blocks across reconnection and decodes the next bounded typed item.
    ///
    /// Malformed or kind-inconsistent data cancels this logical stream.
    /// A saturated close-command queue can also force physical reconnection.
    ///
    /// # Errors
    ///
    /// Returns persistent stream, codec, or product-kind failures.
    pub fn recv(&self) -> Result<ProductSubscriptionItem, ProductClientError> {
        let item = self
            .inner
            .recv_raw()
            .map_err(|error| self.receive_error(error))?;
        self.decode_or_cancel(item)
    }

    /// Waits across reconnection up to one caller deadline and decodes an item.
    ///
    /// # Errors
    ///
    /// Returns timeout, persistent stream, codec, or product-kind failures.
    pub fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<ProductSubscriptionItem, ProductClientError> {
        let item = self
            .inner
            .recv_timeout_raw(timeout)
            .map_err(|error| self.receive_error(error))?;
        self.decode_or_cancel(item)
    }

    fn receive_error(&self, error: ResilientSubscriptionError) -> ProductClientError {
        if let ResilientSubscriptionError::ResumeActivationRejected(response) = error {
            return match self.product_codec.decode_stream_open_response(&response) {
                Err(error) => ProductClientError::Codec(error),
                Ok(undercurrent_protocol::ProductStreamOpenResponse::Error(error)) => {
                    ProductClientError::StreamRejected(error)
                }
                Ok(undercurrent_protocol::ProductStreamOpenResponse::Opened(metadata)) => {
                    ProductClientError::StreamKindMismatch {
                        expected: self.kind,
                        actual: metadata.kind,
                    }
                }
            };
        }
        ProductClientError::Subscription(error)
    }

    /// Cancels this logical stream through a shared reference.
    ///
    /// This operation wakes blocked receivers and removes the interest from
    /// future recovery. Normally peers remain active; if the close command
    /// cannot be queued, the physical client closes to avoid an untracked remote
    /// interest. Repeated cancellation is idempotent.
    pub fn cancel(&self) {
        self.inner.cancel();
    }

    /// Closes this interest with the same pressure caveat as [`Self::cancel`].
    pub fn close(self) {
        self.inner.close();
    }

    fn decode_or_cancel(
        &self,
        inbound: InboundStreamItem,
    ) -> Result<ProductSubscriptionItem, ProductClientError> {
        let decoded = self.decode(inbound);
        if decoded.is_err() {
            self.inner.cancel();
        }
        decoded
    }

    fn decode(
        &self,
        inbound: InboundStreamItem,
    ) -> Result<ProductSubscriptionItem, ProductClientError> {
        let (sequence, bytes) = match inbound {
            InboundStreamItem::Gap(gap) => {
                return Ok(ProductSubscriptionItem::Gap(gap.into()));
            }
            InboundStreamItem::Data {
                sequence, bytes, ..
            } => (sequence.value(), bytes),
        };
        if self.kind == ProductStreamKind::TerminalOutput {
            return self
                .terminal_codec
                .decode_item(&bytes)
                .map(|item| ProductSubscriptionItem::Terminal { sequence, item })
                .map_err(ProductClientError::Codec);
        }
        let item = self
            .product_codec
            .decode_stream_item(&bytes)
            .map_err(ProductClientError::Codec)?;
        let actual = stream_item_kind(&item);
        if actual != self.kind {
            return Err(ProductClientError::StreamItemMismatch {
                expected: self.kind,
                actual,
            });
        }
        Ok(ProductSubscriptionItem::Json { sequence, item })
    }
}

impl From<StreamGap> for ProductStreamGap {
    fn from(value: StreamGap) -> Self {
        Self {
            stream_id: value.stream_id.as_str().to_owned(),
            first_missing: value.first_missing.value(),
            last_missing: value.last_missing.value(),
            reason: match value.reason {
                StreamGapReason::LiveQueueOverflow => ProductStreamGapReason::LiveQueueOverflow,
                StreamGapReason::ReplayEvicted => ProductStreamGapReason::ReplayEvicted,
                StreamGapReason::ReplayUnavailable => ProductStreamGapReason::ReplayUnavailable,
                StreamGapReason::ServerEpochChanged => ProductStreamGapReason::ServerEpochChanged,
                StreamGapReason::SourceRetentionEvicted => {
                    ProductStreamGapReason::SourceRetentionEvicted
                }
            },
            recovery: match value.recovery {
                StreamRecoveryAction::RequestSnapshot => ProductStreamRecovery::RequestSnapshot,
                StreamRecoveryAction::RestartQuery => ProductStreamRecovery::RestartQuery,
                StreamRecoveryAction::RestartFromAvailableOffset => {
                    ProductStreamRecovery::RestartFromAvailableOffset
                }
            },
        }
    }
}

impl From<ResilientSubscriptionError> for ProductClientError {
    fn from(value: ResilientSubscriptionError) -> Self {
        Self::Subscription(value)
    }
}

const fn stream_item_kind(item: &ProductStreamItem) -> ProductStreamKind {
    match item {
        ProductStreamItem::SessionSnapshot(_) | ProductStreamItem::SessionDeltaBatch(_) => {
            ProductStreamKind::SessionWatch
        }
        ProductStreamItem::SessionEvent(_) => ProductStreamKind::SessionEvents,
        ProductStreamItem::AgentEvent(_) => ProductStreamKind::AgentEvents,
    }
}

const fn terminal_item_name(item: &TerminalStreamItem) -> &'static str {
    match item {
        TerminalStreamItem::Output(_) => "output",
        TerminalStreamItem::Screen(_) => "screen",
    }
}

#[cfg(test)]
mod tests {
    use undercurrent_wire::{StreamId, StreamSequence};

    use super::*;

    #[test]
    fn every_transport_gap_value_maps_to_a_stable_product_value() {
        let reasons = [
            StreamGapReason::LiveQueueOverflow,
            StreamGapReason::ReplayEvicted,
            StreamGapReason::ReplayUnavailable,
            StreamGapReason::ServerEpochChanged,
            StreamGapReason::SourceRetentionEvicted,
        ];
        let recoveries = [
            StreamRecoveryAction::RequestSnapshot,
            StreamRecoveryAction::RestartQuery,
            StreamRecoveryAction::RestartFromAvailableOffset,
        ];

        for (index, reason) in reasons.into_iter().enumerate() {
            for recovery in recoveries {
                let mapped = ProductStreamGap::from(StreamGap {
                    stream_id: StreamId::new(format!("gap-{index}")).expect("stream ID"),
                    first_missing: StreamSequence::new(2),
                    last_missing: StreamSequence::new(4),
                    reason,
                    recovery,
                });

                assert!(!mapped.reason.as_str().is_empty());
                assert!(!mapped.recovery.as_str().is_empty());
                assert_eq!(mapped.first_missing, 2);
                assert_eq!(mapped.last_missing, 4);
            }
        }
    }
}
