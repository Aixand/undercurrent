// SPDX-License-Identifier: Apache-2.0

//! Product stream preparation and bounded connection publication.

mod pump;

pub use pump::*;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use undercurrent_multiplex::{
    DeliverySemantics, EnqueueOutcome, StreamGap, StreamKind, StreamRecoveryAction, TrafficClass,
};
use undercurrent_protocol::{
    ProductCodec, ProductStreamItem, ProductStreamKind, TerminalOutputChunk, TerminalOutputCodec,
    TerminalScreenSnapshot, TerminalStreamItem,
};
use undercurrent_transport_core::{
    ServerConnectionErrorCategory, ServerConnectionSession, ServerQueueDrainWake,
    ServerStreamConfiguration,
};
use undercurrent_wire::{StreamId, StreamSequence};

use crate::{ProductStreamPublishError, ProductStreamPublishErrorKind};

/// Product stream policy translated into transport scheduling semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductStreamPolicy {
    /// Negotiated scheduler configuration.
    pub configuration: ServerStreamConfiguration,
    /// Whether a reconnecting client may request this stream identity.
    pub resumable: bool,
}

impl ProductStreamPolicy {
    /// Resolves scheduling and recovery for one complete stream specification.
    ///
    /// A screen-seeded terminal cannot continue across missing raw bytes, so it
    /// requires another authoritative snapshot. Other terminal modes can restart
    /// from retained output. This mapping performs no IO and does not panic.
    pub const fn for_specification(
        specification: &undercurrent_protocol::ProductStreamSpec,
    ) -> Self {
        let mut policy = Self::for_kind(specification.kind());
        if matches!(
            specification,
            undercurrent_protocol::ProductStreamSpec::TerminalOutput(
                undercurrent_protocol::TerminalOutputSpec {
                    initial: undercurrent_protocol::TerminalInitialModeDto::Screen,
                    ..
                }
            )
        ) {
            policy.configuration.recovery = StreamRecoveryAction::RequestSnapshot;
        }
        policy
    }

    /// Resolves the central scheduling and recovery policy for one stream kind.
    ///
    /// This mapping performs no IO, cannot fail, and does not panic.
    pub const fn for_kind(kind: ProductStreamKind) -> Self {
        match kind {
            ProductStreamKind::SessionWatch => Self {
                configuration: ServerStreamConfiguration {
                    kind: StreamKind::SessionQuery,
                    class: TrafficClass::State,
                    delivery: DeliverySemantics::Replaceable,
                    recovery: StreamRecoveryAction::RestartQuery,
                    retain_for_resume: true,
                },
                resumable: true,
            },
            ProductStreamKind::SessionEvents => Self {
                configuration: ServerStreamConfiguration {
                    kind: StreamKind::SessionEvents,
                    class: TrafficClass::Lifecycle,
                    delivery: DeliverySemantics::Reliable,
                    recovery: StreamRecoveryAction::RequestSnapshot,
                    retain_for_resume: true,
                },
                resumable: true,
            },
            ProductStreamKind::TerminalOutput => Self {
                configuration: ServerStreamConfiguration {
                    kind: StreamKind::TerminalOutput,
                    class: TrafficClass::Bulk,
                    delivery: DeliverySemantics::Reliable,
                    recovery: StreamRecoveryAction::RestartFromAvailableOffset,
                    retain_for_resume: true,
                },
                resumable: true,
            },
            ProductStreamKind::AgentEvents => Self {
                configuration: ServerStreamConfiguration {
                    kind: StreamKind::SessionEvents,
                    class: TrafficClass::Lifecycle,
                    delivery: DeliverySemantics::Reliable,
                    recovery: StreamRecoveryAction::RequestSnapshot,
                    retain_for_resume: true,
                },
                resumable: true,
            },
        }
    }
}

/// Selective product subscription prepared before transport activation.
pub trait PreparedProductStream: Send {
    /// Returns the first retained byte or revision available to the product,
    /// when that concept is meaningful for the stream kind.
    fn available_from(&self) -> Option<u64>;

    /// Attaches an already activated publisher to the prepared interest.
    ///
    /// Implementations must complete all fallible capacity and authorization
    /// work before returning from preparation. This method must be non-blocking,
    /// perform no external IO, and must not panic.
    fn activate(self: Box<Self>, publisher: ProductStreamPublisher);
}

/// Clonable publisher for one already activated logical stream.
#[derive(Clone)]
pub struct ProductStreamPublisher {
    inner: Arc<ProductStreamPublisherInner>,
}

impl ProductStreamPublisher {
    /// Constructs a publisher after successful server stream activation.
    ///
    /// The initial sequence is the highest sequence already accepted through
    /// replay or gap scheduling. Construction performs no IO and does not
    /// panic.
    pub fn new(
        connection: Arc<ServerConnectionSession>,
        stream_id: StreamId,
        initial_sequence: StreamSequence,
        codec: ProductCodec,
        terminal_codec: TerminalOutputCodec,
    ) -> Self {
        Self {
            inner: Arc::new(ProductStreamPublisherInner {
                connection,
                stream_id,
                codec,
                terminal_codec,
                sequence: Mutex::new(initial_sequence.value()),
                closed: AtomicBool::new(false),
            }),
        }
    }

    /// Returns the stable connection-local stream identity.
    pub fn stream_id(&self) -> &StreamId {
        &self.inner.stream_id
    }

    /// Returns whether publication has ended after closure or a connection error.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire) || self.inner.connection.is_closed()
    }

    /// Stops future publication attempts without affecting peer streams.
    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
    }

    pub(crate) fn set_queue_drain_wake(
        &self,
        wake: Arc<dyn ServerQueueDrainWake>,
    ) -> Result<(), ProductStreamPublishError> {
        self.inner
            .connection
            .set_queue_drain_wake(wake)
            .map_err(|source| {
                self.close();
                ProductStreamPublishError::new(ProductStreamPublishErrorKind::Connection(source))
            })
    }

    /// Encodes and publishes one non-terminal JSON product item.
    ///
    /// The underlying scheduler applies the stream's configured delivery and
    /// gap policy. This method performs bounded encoding and scheduler admission,
    /// no physical transport IO, and does not panic.
    ///
    /// # Errors
    ///
    /// Returns redacted closure, encoding, sequence, or scheduler failure.
    pub fn publish(
        &self,
        item: &ProductStreamItem,
    ) -> Result<EnqueueOutcome, ProductStreamPublishError> {
        let payload = self
            .inner
            .codec
            .encode_stream_item(item)
            .map(Arc::from)
            .map_err(|source| {
                ProductStreamPublishError::new(ProductStreamPublishErrorKind::Encoding(source))
            })?;
        self.publish_encoded(payload)
    }

    /// Publishes raw terminal bytes without JSON or base64 expansion.
    ///
    /// # Errors
    ///
    /// Returns redacted closure, encoding, sequence, or scheduler failure.
    pub fn publish_terminal(
        &self,
        chunk: &TerminalOutputChunk,
    ) -> Result<EnqueueOutcome, ProductStreamPublishError> {
        let payload = self
            .inner
            .terminal_codec
            .encode(chunk)
            .map(Arc::from)
            .map_err(|source| {
                ProductStreamPublishError::new(ProductStreamPublishErrorKind::Encoding(source))
            })?;
        self.publish_encoded(payload)
    }

    /// Publishes a terminal payload already validated by the configured binary
    /// codec without re-encoding it during a backpressure retry.
    pub(crate) fn publish_encoded_terminal(
        &self,
        payload: Arc<[u8]>,
    ) -> Result<EnqueueOutcome, ProductStreamPublishError> {
        self.publish_encoded(payload)
    }

    /// Publishes one complete screen seed on a selected terminal stream.
    ///
    /// # Errors
    ///
    /// Returns redacted closure, encoding, sequence, or scheduler failure.
    pub fn publish_terminal_screen(
        &self,
        snapshot: &TerminalScreenSnapshot,
    ) -> Result<EnqueueOutcome, ProductStreamPublishError> {
        let payload = self
            .inner
            .terminal_codec
            .encode_item(&TerminalStreamItem::Screen(snapshot.clone()))
            .map(Arc::from)
            .map_err(|source| {
                ProductStreamPublishError::new(ProductStreamPublishErrorKind::Encoding(source))
            })?;
        self.publish_encoded(payload)
    }

    /// Publishes one ordered checkpoint whose application source data was
    /// evicted before this stream consumed it.
    ///
    /// The transport derives the recovery action from the activated stream
    /// policy. This method allocates no placeholder payload, performs no
    /// physical transport IO, and does not panic.
    ///
    /// # Errors
    ///
    /// Returns redacted closure, sequence, or scheduler failure.
    pub fn publish_source_gap(&self) -> Result<StreamGap, ProductStreamPublishError> {
        self.admit(|sequence| {
            self.inner
                .connection
                .publish_source_gap(&self.inner.stream_id, sequence)
        })
    }

    fn publish_encoded(
        &self,
        payload: Arc<[u8]>,
    ) -> Result<EnqueueOutcome, ProductStreamPublishError> {
        self.admit(|sequence| {
            self.inner
                .connection
                .publish_stream(&self.inner.stream_id, sequence, payload)
        })
    }

    fn admit<T>(
        &self,
        operation: impl FnOnce(
            StreamSequence,
        ) -> Result<T, undercurrent_transport_core::ServerConnectionError>,
    ) -> Result<T, ProductStreamPublishError> {
        if self.is_closed() {
            return Err(ProductStreamPublishError::new(
                ProductStreamPublishErrorKind::Closed,
            ));
        }
        let mut sequence = self
            .inner
            .sequence
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let next = sequence.checked_add(1).ok_or_else(|| {
            self.close();
            ProductStreamPublishError::new(ProductStreamPublishErrorKind::SequenceExhausted)
        })?;
        // Advance the sequence only when the scheduler accepts the item. The
        // source retains an overloaded item for retry; consuming a sequence on
        // rejection would turn temporary queue pressure into an apparent gap.
        // See [publication retries](../docs/internals/output-and-streams.md#why-one-slow-stream-does-not-allocate-a-worker).
        match operation(StreamSequence::new(next)) {
            Ok(value) => {
                *sequence = next;
                Ok(value)
            }
            Err(source) => {
                if source.category() != ServerConnectionErrorCategory::Overloaded {
                    self.close();
                }
                Err(ProductStreamPublishError::new(
                    ProductStreamPublishErrorKind::Connection(source),
                ))
            }
        }
    }
}

struct ProductStreamPublisherInner {
    connection: Arc<ServerConnectionSession>,
    stream_id: StreamId,
    codec: ProductCodec,
    terminal_codec: TerminalOutputCodec,
    sequence: Mutex<u64>,
    closed: AtomicBool,
}

#[cfg(test)]
mod tests {
    use undercurrent_multiplex::{
        DeliverySemantics, StreamKind, StreamRecoveryAction, TrafficClass,
    };
    use undercurrent_protocol::{
        ProductStreamKind, ProductStreamSpec, TerminalInitialModeDto, TerminalOutputSpec,
    };

    use super::ProductStreamPolicy;

    #[test]
    fn session_event_lifecycle_is_reliable_and_replay_retained_while_watch_state_is_replaceable() {
        let lifecycle = ProductStreamPolicy::for_kind(ProductStreamKind::SessionEvents);
        assert_eq!(lifecycle.configuration.kind, StreamKind::SessionEvents);
        assert_eq!(lifecycle.configuration.class, TrafficClass::Lifecycle);
        assert_eq!(
            lifecycle.configuration.delivery,
            DeliverySemantics::Reliable
        );
        assert_eq!(
            lifecycle.configuration.recovery,
            StreamRecoveryAction::RequestSnapshot
        );
        assert!(lifecycle.configuration.retain_for_resume);
        assert!(lifecycle.resumable);

        let state = ProductStreamPolicy::for_kind(ProductStreamKind::SessionWatch);
        assert_eq!(state.configuration.kind, StreamKind::SessionQuery);
        assert_eq!(state.configuration.class, TrafficClass::State);
        assert_eq!(state.configuration.delivery, DeliverySemantics::Replaceable);
    }

    #[test]
    fn terminal_recovery_depends_on_the_initial_representation() {
        for initial in [
            TerminalInitialModeDto::None,
            TerminalInitialModeDto::Tail { max_bytes: 1 },
            TerminalInitialModeDto::Resume { after_offset: 0 },
        ] {
            let specification = ProductStreamSpec::TerminalOutput(TerminalOutputSpec {
                session_id: "session-1".into(),
                initial,
            });
            assert_eq!(
                ProductStreamPolicy::for_specification(&specification)
                    .configuration
                    .delivery,
                DeliverySemantics::Reliable
            );
            assert_eq!(
                ProductStreamPolicy::for_specification(&specification)
                    .configuration
                    .recovery,
                StreamRecoveryAction::RestartFromAvailableOffset
            );
        }

        let screen = ProductStreamSpec::TerminalOutput(TerminalOutputSpec {
            session_id: "session-1".into(),
            initial: TerminalInitialModeDto::Screen,
        });
        assert_eq!(
            ProductStreamPolicy::for_specification(&screen)
                .configuration
                .recovery,
            StreamRecoveryAction::RequestSnapshot
        );
    }
}
