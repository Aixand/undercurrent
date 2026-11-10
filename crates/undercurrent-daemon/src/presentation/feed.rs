// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use undercurrent_application::{
    OutputGapRecovery, OutputReplay, OutputSlice, SessionEventReadBatch, SessionOutboxRecord,
    TerminalOutputStreamReplay, TerminalOutputStreamSlice, TerminalScreenSnapshot,
    TerminalScreenUpdate,
};
use undercurrent_protocol::{
    EventGapDto, ResynchronizationDto, SessionEventDto, SessionEventPageDto, TerminalOutputCodec,
    TerminalOutputPageDto, TerminalScreenDiffDto, TerminalScreenSnapshotDto,
    TerminalScreenUpdateDto,
};

use super::{ProductPresentation, ProductPresentationError, TerminalOutputStreamPresentation};
use crate::event_kind::from_domain;

impl ProductPresentation {
    /// Formats one parser-consistent current terminal screen.
    pub fn terminal_screen(
        snapshot: &TerminalScreenSnapshot,
        checkpoint: String,
    ) -> TerminalScreenSnapshotDto {
        TerminalScreenSnapshotDto {
            checkpoint,
            output_offset: snapshot.output_offset().value(),
            columns: snapshot.columns(),
            rows: snapshot.rows(),
            cursor_row: snapshot.cursor_row(),
            cursor_column: snapshot.cursor_column(),
            cursor_visible: snapshot.cursor_visible(),
            alternate_screen: snapshot.alternate_screen(),
            contents: snapshot.contents().to_vec(),
        }
    }

    /// Formats one bounded screen update with its newly retained checkpoint.
    pub fn terminal_screen_update(
        update: &TerminalScreenUpdate,
        checkpoint: String,
    ) -> TerminalScreenUpdateDto {
        match update {
            TerminalScreenUpdate::Diff(diff) => {
                let current = diff.current();
                TerminalScreenUpdateDto::Diff(TerminalScreenDiffDto {
                    checkpoint,
                    from_output_offset: diff.from_output_offset().value(),
                    to_output_offset: current.output_offset().value(),
                    columns: current.columns(),
                    rows: current.rows(),
                    cursor_row: current.cursor_row(),
                    cursor_column: current.cursor_column(),
                    cursor_visible: current.cursor_visible(),
                    alternate_screen: current.alternate_screen(),
                    contents: diff.contents().to_vec(),
                })
            }
            TerminalScreenUpdate::Snapshot(snapshot) => {
                TerminalScreenUpdateDto::Snapshot(Self::terminal_screen(snapshot, checkpoint))
            }
        }
    }

    /// Formats a current screen for the compact tagged terminal stream codec.
    pub fn terminal_screen_stream(
        snapshot: &TerminalScreenSnapshot,
    ) -> Result<undercurrent_protocol::TerminalScreenSnapshot, ProductPresentationError> {
        undercurrent_protocol::TerminalScreenSnapshot::new(
            snapshot.output_offset().value(),
            snapshot.columns(),
            snapshot.rows(),
            snapshot.cursor_row(),
            snapshot.cursor_column(),
            snapshot.cursor_visible(),
            snapshot.alternate_screen(),
            std::sync::Arc::from(snapshot.contents()),
        )
        .map_err(|_| ProductPresentationError {
            field: "terminal_screen",
        })
    }

    /// Formats retained output for the binary terminal stream without
    /// calculating finite-response integrity metadata that the stream codec
    /// does not carry.
    pub(crate) fn terminal_output_stream(
        replay: &TerminalOutputStreamReplay,
        codec: TerminalOutputCodec,
    ) -> Result<TerminalOutputStreamPresentation, ProductPresentationError> {
        Ok(match replay {
            TerminalOutputStreamReplay::Gap { .. } => TerminalOutputStreamPresentation::Gap,
            TerminalOutputStreamReplay::Data {
                slices,
                from,
                to,
                has_more,
                ..
            } => {
                let declared_len = to
                    .value()
                    .checked_sub(from.value())
                    .and_then(|length| usize::try_from(length).ok())
                    .ok_or(ProductPresentationError {
                        field: "terminal_output_bytes",
                    })?;
                let encoded = codec
                    .encode_output_parts(
                        from.value(),
                        declared_len,
                        slices.iter().map(TerminalOutputStreamSlice::bytes),
                    )
                    .map_err(|_| ProductPresentationError {
                        field: "terminal_output_bytes",
                    })?;
                let payload = if declared_len == 0 {
                    None
                } else {
                    Some(Arc::from(encoded))
                };
                TerminalOutputStreamPresentation::Data {
                    payload,
                    has_more: *has_more,
                }
            }
        })
    }

    /// Formats one bounded retained terminal-output read.
    pub fn terminal_output(
        replay: &OutputReplay,
    ) -> Result<TerminalOutputPageDto, ProductPresentationError> {
        Ok(match replay {
            OutputReplay::Gap {
                requested,
                available_from,
                next_offset,
                recovery,
            } => TerminalOutputPageDto::Gap {
                requested_offset: requested.value(),
                available_from: available_from.value(),
                next_offset: next_offset.value(),
                recovery: match recovery {
                    OutputGapRecovery::RestartAtAvailableOffset => {
                        ResynchronizationDto::RestartFromAvailableOffset
                    }
                    OutputGapRecovery::RequestScreenSnapshot => {
                        ResynchronizationDto::RequestSnapshot
                    }
                },
            },
            OutputReplay::Data {
                slices,
                from,
                to,
                next_offset,
                has_more,
            } => {
                let bytes = terminal_output_bytes(slices)?;
                TerminalOutputPageDto::Data {
                    from_offset: from.value(),
                    to_offset: to.value(),
                    next_offset: next_offset.value(),
                    has_more: *has_more,
                    payload_digest_sha256:
                        undercurrent_protocol::execution_output_payload_digest_sha256(&bytes),
                    truncation: None,
                    bytes,
                }
            }
        })
    }

    /// Formats one validated bounded durable event batch.
    pub fn session_events(batch: &SessionEventReadBatch) -> SessionEventPageDto {
        SessionEventPageDto {
            items: batch
                .records()
                .iter()
                .map(|record| {
                    let mut event = session_event(record);
                    event.observation = record.snapshot.as_ref().map(|session| {
                        undercurrent_protocol::SessionEventObservationDto {
                            restart_status: Some(super::restart_status(session.restart_status())),
                            restoration_directory: session
                                .restoration_directory()
                                .map(|p| p.as_str().to_owned()),
                            version: undercurrent_protocol::ReleaseVersion::Current,
                            lifecycle: super::lifecycle(session.lifecycle()),
                            lifecycle_revision: session.lifecycle_revision().value(),
                            foreground: super::foreground_observation(session.foreground()),
                            health: super::health_summary(session.health()),
                        }
                    });
                    event
                })
                .collect(),
            checkpoint: batch.checkpoint().map(|sequence| sequence.value()),
            has_more: batch.has_more(),
            gap: batch.gap().map(|gap| EventGapDto {
                missing_from: gap.missing_from().value(),
                missing_through: gap.missing_through().value(),
                first_available: gap.first_available().value(),
            }),
        }
    }
}

fn terminal_output_bytes(slices: &[OutputSlice]) -> Result<Vec<u8>, ProductPresentationError> {
    let byte_len = slices.iter().try_fold(0usize, |total, slice| {
        total
            .checked_add(slice.len())
            .ok_or(ProductPresentationError {
                field: "terminal_output_bytes",
            })
    })?;
    let mut bytes = Vec::with_capacity(byte_len);
    for slice in slices {
        bytes.extend_from_slice(slice.bytes());
    }
    Ok(bytes)
}

pub(super) fn session_event(record: &SessionOutboxRecord) -> SessionEventDto {
    SessionEventDto {
        observation: None,
        sequence: record.sequence.value(),
        session_id: record.event.session_id.as_str().to_owned(),
        session_revision: record.event.revision.value(),
        kind: from_domain(record.event.kind),
        occurred_at_millis: record.event.occurred_at.value(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use undercurrent_application::{
        OutputGapRecovery, OutputOffset, SubscriptionWake, TerminalOutputLimits,
        TerminalOutputStore, TerminalOutputStreamReplay,
    };
    use undercurrent_domain::SessionId;
    use undercurrent_protocol::{TerminalOutputCodec, TerminalOutputPageDto};

    use super::{ProductPresentation, TerminalOutputStreamPresentation};

    struct NoopWake;

    impl SubscriptionWake for NoopWake {
        fn wake(&self) {}
    }

    #[test]
    fn stream_projection_encodes_multiple_retained_slices_while_bounded_page_preserves_digest() {
        let output = TerminalOutputStore::new(TerminalOutputLimits {
            per_session_bytes: 64,
            global_bytes: 64,
            chunk_bytes: 4,
            append_bytes: 64,
            replay_bytes: 64,
            sessions: 1,
            subscriptions: 1,
        })
        .expect("output limits");
        let session_id = SessionId::new("session-1").expect("session ID");
        let bytes = b"terminal-stream-payload";
        output.append(&session_id, bytes).expect("output append");
        let opened = output
            .subscribe_stream(
                session_id.clone(),
                OutputOffset::ZERO,
                64,
                OutputGapRecovery::RestartAtAvailableOffset,
                Arc::new(NoopWake),
            )
            .expect("output stream subscription");

        let TerminalOutputStreamReplay::Data { slices, .. } = opened.initial() else {
            panic!("expected retained terminal stream data")
        };
        assert!(slices.len() > 1, "fixture must cross retained chunks");
        let codec = TerminalOutputCodec::new(64).expect("terminal codec");
        let stream = ProductPresentation::terminal_output_stream(opened.initial(), codec)
            .expect("stream presentation");
        let TerminalOutputStreamPresentation::Data {
            payload: Some(payload),
            has_more,
        } = stream
        else {
            panic!("expected one terminal stream chunk")
        };
        let chunk = codec.decode(&payload).expect("terminal stream payload");
        assert_eq!(chunk.start_offset(), 0);
        assert_eq!(chunk.bytes(), bytes);
        assert!(!has_more);

        let replay = output
            .replay(
                &session_id,
                OutputOffset::ZERO,
                64,
                OutputGapRecovery::RestartAtAvailableOffset,
            )
            .expect("output replay");
        let bounded =
            ProductPresentation::terminal_output(&replay).expect("bounded response presentation");
        let TerminalOutputPageDto::Data {
            payload_digest_sha256,
            bytes: presented_bytes,
            ..
        } = bounded
        else {
            panic!("expected bounded output data")
        };
        assert_eq!(presented_bytes, bytes);
        assert_eq!(
            payload_digest_sha256,
            undercurrent_protocol::execution_output_payload_digest_sha256(bytes)
        );
    }
}
