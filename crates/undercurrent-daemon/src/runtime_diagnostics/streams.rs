// SPDX-License-Identifier: Apache-2.0

//! Connection, stream, slow-consumer, and output-producer diagnostics.

use undercurrent_transport_core::{
    ActiveConnectionDiagnostic, ServerStreamDiagnostic, ServerStreamDiagnosticState,
};
use undercurrent_transport_local::LocalServer;
use undercurrent_wire::{ClientId, StreamId};

use super::DaemonRuntimeDiagnostics;
use super::rows::{boolean, count, number, row, text};
use crate::{
    DaemonDiagnosticField, DaemonDiagnosticRow, DaemonDiagnosticSection, DaemonOperationsError,
};

impl DaemonRuntimeDiagnostics {
    /// Reports listener pressure and a bounded page of negotiated connections.
    pub(super) fn connection_rows(
        &self,
        listener: Option<&LocalServer>,
        row_limit: usize,
    ) -> Result<Vec<DaemonDiagnosticRow>, DaemonOperationsError> {
        let pressure = listener.map(LocalServer::pressure);
        let connections = pressure
            .map(|value| value.connections.connections)
            .unwrap_or(0);
        let clients = pressure.map(|value| value.connections.clients).unwrap_or(0);
        let limit = pressure
            .map(|value| value.connections.limit)
            .unwrap_or_else(|| {
                self.core
                    .config
                    .transport()
                    .local_server()
                    .maximum_connections()
            });
        let workers = pressure.map(|value| value.connection_workers).unwrap_or(0);
        let accepting = pressure.is_some_and(|value| value.accepting);
        let replay = pressure.map(|value| value.replay);
        let mut rows = vec![row(
            DaemonDiagnosticSection::Connections,
            "local_listener",
            [
                boolean(DaemonDiagnosticField::Accepting, accepting),
                count(DaemonDiagnosticField::Clients, clients),
                count(DaemonDiagnosticField::Connections, connections),
                count(DaemonDiagnosticField::ConnectionLimit, limit),
                count(DaemonDiagnosticField::Workers, workers),
                count(
                    DaemonDiagnosticField::Streams,
                    replay.map(|value| value.streams).unwrap_or(0),
                ),
                number(
                    DaemonDiagnosticField::Bytes,
                    replay.map(|value| value.bytes).unwrap_or(0),
                ),
            ],
        )];
        let Some(listener) = listener else {
            return Ok(rows);
        };
        if rows.len() >= row_limit {
            return Ok(rows);
        }

        let registry = listener.connection_registry();
        let page_limit = self
            .core
            .config
            .transport()
            .local_server()
            .diagnostic_limits()
            .connection_page_size();
        let mut after = None;
        loop {
            let page = registry
                .diagnostic_page(after.as_ref(), page_limit)
                .map_err(|_| DaemonOperationsError::SourceUnavailable)?;
            for connection in page.items {
                if rows.len() >= row_limit {
                    return Ok(rows);
                }
                rows.push(connection_row(&connection));
            }
            let Some(next) = page.next_after else {
                return Ok(rows);
            };
            after = Some(next);
        }
    }

    /// Reports bounded aggregate and entity-level selective-stream pressure.
    pub(super) fn stream_rows(
        &self,
        listener: Option<&LocalServer>,
        section: DaemonDiagnosticSection,
        row_limit: usize,
    ) -> Result<Vec<DaemonDiagnosticRow>, DaemonOperationsError> {
        let mut rows = self.aggregate_stream_rows(section)?;
        let Some(listener) = listener else {
            return Ok(rows);
        };
        if rows.len() >= row_limit {
            rows.truncate(row_limit);
            return Ok(rows);
        }

        let registry = listener.connection_registry();
        let limits = self
            .core
            .config
            .transport()
            .local_server()
            .diagnostic_limits();
        let mut after_client: Option<ClientId> = None;
        loop {
            let connections = registry
                .diagnostic_page(after_client.as_ref(), limits.connection_page_size())
                .map_err(|_| DaemonOperationsError::SourceUnavailable)?;
            for connection in connections.items {
                let mut after_stream: Option<StreamId> = None;
                loop {
                    let page = registry
                        .stream_diagnostic_page(
                            &connection.client_id,
                            after_stream.as_ref(),
                            limits.stream_page_size(),
                        )
                        .map_err(|_| DaemonOperationsError::SourceUnavailable)?;
                    let Some(page) = page else {
                        break;
                    };
                    for stream in page.items {
                        if rows.len() >= row_limit {
                            return Ok(rows);
                        }
                        if section == DaemonDiagnosticSection::Streams || is_slow_consumer(&stream)
                        {
                            rows.push(stream_row(section, &connection.client_id, &stream));
                        }
                    }
                    let Some(next) = page.next_after else {
                        break;
                    };
                    after_stream = Some(next);
                }
            }
            let Some(next) = connections.next_after else {
                return Ok(rows);
            };
            after_client = Some(next);
        }
    }

    fn aggregate_stream_rows(
        &self,
        section: DaemonDiagnosticSection,
    ) -> Result<Vec<DaemonDiagnosticRow>, DaemonOperationsError> {
        let pump = self.streams.pump.pressure();
        let projections = self.streams.projections.pressure();
        let events = self.streams.events.pressure();
        let output = self
            .core
            .processes
            .output_store()
            .status()
            .map_err(|_| DaemonOperationsError::SourceUnavailable)?;
        Ok(match section {
            DaemonDiagnosticSection::Streams => vec![
                row(
                    section,
                    "product_stream_pump",
                    [
                        count(DaemonDiagnosticField::Preparing, pump.preparing_streams),
                        count(DaemonDiagnosticField::Active, pump.active_streams),
                        count(DaemonDiagnosticField::Ready, pump.ready_streams),
                        count(
                            DaemonDiagnosticField::Backpressured,
                            pump.backpressured_streams,
                        ),
                        number(DaemonDiagnosticField::Failed, pump.failed_streams),
                        count(DaemonDiagnosticField::Limit, pump.limit),
                        boolean(DaemonDiagnosticField::Closed, pump.closed),
                    ],
                ),
                row(
                    section,
                    "session_queries",
                    [
                        count(DaemonDiagnosticField::Active, projections.subscriptions),
                        count(
                            DaemonDiagnosticField::Dirty,
                            projections.dirty_subscriptions,
                        ),
                        count(DaemonDiagnosticField::Limit, projections.limit),
                    ],
                ),
                row(
                    section,
                    "session_events",
                    [
                        count(DaemonDiagnosticField::Active, events.subscriptions),
                        count(DaemonDiagnosticField::Dirty, events.dirty_subscriptions),
                        count(DaemonDiagnosticField::Limit, events.subscription_limit),
                    ],
                ),
                row(
                    section,
                    "terminal_output",
                    [
                        count(DaemonDiagnosticField::Active, output.subscriptions),
                        count(DaemonDiagnosticField::Dirty, output.dirty_subscriptions),
                        count(DaemonDiagnosticField::Limit, output.subscription_limit),
                    ],
                ),
            ],
            DaemonDiagnosticSection::SlowConsumers => vec![
                row(
                    section,
                    "product_stream_pump",
                    [count(
                        DaemonDiagnosticField::Backpressured,
                        pump.backpressured_streams,
                    )],
                ),
                row(
                    section,
                    "session_queries",
                    [count(
                        DaemonDiagnosticField::Dirty,
                        projections.dirty_subscriptions,
                    )],
                ),
                row(
                    section,
                    "session_events",
                    [count(
                        DaemonDiagnosticField::Dirty,
                        events.dirty_subscriptions,
                    )],
                ),
                row(
                    section,
                    "terminal_output",
                    [count(
                        DaemonDiagnosticField::Dirty,
                        output.dirty_subscriptions,
                    )],
                ),
            ],
            _ => Vec::new(),
        })
    }

    /// Reports aggregate runtime pressure and bounded top retained-output sessions.
    pub(super) fn output_rows(
        &self,
        row_limit: usize,
    ) -> Result<Vec<DaemonDiagnosticRow>, DaemonOperationsError> {
        let processes = self
            .core
            .processes
            .pressure()
            .map_err(|_| DaemonOperationsError::SourceUnavailable)?;
        let mut rows = vec![
            row(
                DaemonDiagnosticSection::OutputProducers,
                "terminal_output",
                [
                    count(DaemonDiagnosticField::Sessions, processes.output.sessions),
                    count(
                        DaemonDiagnosticField::Bytes,
                        processes.output.retained_bytes,
                    ),
                    count(
                        DaemonDiagnosticField::Items,
                        processes.output.retained_chunks,
                    ),
                    count(
                        DaemonDiagnosticField::Active,
                        processes.output.subscriptions,
                    ),
                    count(
                        DaemonDiagnosticField::Dirty,
                        processes.output.dirty_subscriptions,
                    ),
                ],
            ),
            row(
                DaemonDiagnosticSection::OutputProducers,
                "terminal_screens",
                [
                    count(DaemonDiagnosticField::Sessions, processes.screens.sessions),
                    count(
                        DaemonDiagnosticField::Limit,
                        processes.screens.session_limit,
                    ),
                    count(
                        DaemonDiagnosticField::ByteLimit,
                        processes.screens.snapshot_byte_limit,
                    ),
                ],
            ),
            row(
                DaemonDiagnosticSection::OutputProducers,
                "process_runtime",
                [
                    count(
                        DaemonDiagnosticField::Active,
                        processes.active_process_entries,
                    ),
                    count(DaemonDiagnosticField::Bytes, processes.queued_input_bytes),
                    count(DaemonDiagnosticField::Workers, processes.input_workers),
                    boolean(DaemonDiagnosticField::Closed, processes.closed),
                ],
            ),
        ];
        rows.truncate(row_limit);
        let remaining = row_limit.saturating_sub(rows.len());
        if remaining == 0 {
            return Ok(rows);
        }
        let output = self.core.processes.output_store();
        let producer_limit = remaining.min(output.limits().sessions);
        for (session_id, status) in output
            .top_session_statuses(producer_limit)
            .map_err(|_| DaemonOperationsError::SourceUnavailable)?
        {
            rows.push(row(
                DaemonDiagnosticSection::OutputProducers,
                format!("session:{}", session_id.as_str()),
                [
                    text(DaemonDiagnosticField::SessionId, session_id.as_str()),
                    count(DaemonDiagnosticField::Bytes, status.retained_bytes),
                    count(DaemonDiagnosticField::Items, status.retained_chunks),
                    number(
                        DaemonDiagnosticField::OutputOffset,
                        status.next_offset.value(),
                    ),
                ],
            ));
        }
        Ok(rows)
    }
}

fn connection_row(connection: &ActiveConnectionDiagnostic) -> DaemonDiagnosticRow {
    let limits = connection.negotiated.limits;
    let pressure = connection.pressure;
    row(
        DaemonDiagnosticSection::Connections,
        format!(
            "connection:{}",
            connection.negotiated.connection_id.as_str()
        ),
        [
            text(
                DaemonDiagnosticField::ClientId,
                connection.client_id.as_str(),
            ),
            text(
                DaemonDiagnosticField::ConnectionId,
                connection.negotiated.connection_id.as_str(),
            ),
            text(
                DaemonDiagnosticField::ServerEpoch,
                connection.negotiated.server_epoch.as_str(),
            ),
            text(
                DaemonDiagnosticField::ProtocolVersion,
                connection.negotiated.protocol_version.to_string(),
            ),
            number(
                DaemonDiagnosticField::MaxFrameBytes,
                u64::from(limits.max_frame_bytes()),
            ),
            number(
                DaemonDiagnosticField::Streams,
                pressure.map_or(0, |value| value.logical_streams as u64),
            ),
            number(
                DaemonDiagnosticField::Limit,
                u64::from(limits.max_logical_streams()),
            ),
            number(
                DaemonDiagnosticField::Pending,
                pressure.map_or(0, |value| value.pending_requests as u64),
            ),
            number(
                DaemonDiagnosticField::MaxPendingRequests,
                u64::from(limits.max_pending_requests()),
            ),
            number(
                DaemonDiagnosticField::Bytes,
                pressure.map_or(0, |value| value.scheduler.queued_bytes),
            ),
            number(
                DaemonDiagnosticField::ByteLimit,
                limits.connection_queue_bytes(),
            ),
            number(
                DaemonDiagnosticField::QueuedMessages,
                pressure.map_or(0, |value| value.scheduler.queued_messages as u64),
            ),
            number(
                DaemonDiagnosticField::ResumeRetentionMillis,
                connection.negotiated.resume_retention_millis.unwrap_or(0),
            ),
            boolean(DaemonDiagnosticField::Active, pressure.is_some()),
            boolean(
                DaemonDiagnosticField::Closed,
                pressure.is_some_and(|value| value.closed),
            ),
        ],
    )
}

fn is_slow_consumer(stream: &ServerStreamDiagnostic) -> bool {
    match stream.state {
        ServerStreamDiagnosticState::Opening { .. } => false,
        ServerStreamDiagnosticState::Active { pressure, .. } => {
            pressure.gap_pending || (pressure.queued_bytes > 0 && pressure.credit_bytes == 0)
        }
    }
}

fn stream_row(
    section: DaemonDiagnosticSection,
    client_id: &ClientId,
    stream: &ServerStreamDiagnostic,
) -> DaemonDiagnosticRow {
    let identity = format!(
        "stream:{}:{}",
        client_id.as_str(),
        stream.stream_id.as_str()
    );
    match stream.state {
        ServerStreamDiagnosticState::Opening { ref request_id } => row(
            section,
            identity,
            [
                text(DaemonDiagnosticField::ClientId, client_id.as_str()),
                text(DaemonDiagnosticField::StreamId, stream.stream_id.as_str()),
                text(DaemonDiagnosticField::StreamKind, "opening"),
                text(DaemonDiagnosticField::RequestId, request_id.as_str()),
                number(
                    DaemonDiagnosticField::QueueByteLimit,
                    stream.queue_byte_limit,
                ),
                number(
                    DaemonDiagnosticField::QueueMessageLimit,
                    stream.queue_message_limit,
                ),
                boolean(DaemonDiagnosticField::Pending, true),
            ],
        ),
        ServerStreamDiagnosticState::Active {
            configuration,
            pressure,
        } => row(
            section,
            identity,
            [
                text(DaemonDiagnosticField::ClientId, client_id.as_str()),
                text(DaemonDiagnosticField::StreamId, stream.stream_id.as_str()),
                text(
                    DaemonDiagnosticField::StreamKind,
                    configuration.kind.as_str(),
                ),
                text(
                    DaemonDiagnosticField::TrafficClass,
                    configuration.class.as_str(),
                ),
                number(DaemonDiagnosticField::Bytes, pressure.queued_bytes),
                number(
                    DaemonDiagnosticField::QueuedMessages,
                    pressure.queued_messages as u64,
                ),
                number(
                    DaemonDiagnosticField::QueueByteLimit,
                    stream.queue_byte_limit,
                ),
                number(
                    DaemonDiagnosticField::QueueMessageLimit,
                    stream.queue_message_limit,
                ),
                number(DaemonDiagnosticField::CreditBytes, pressure.credit_bytes),
                number(
                    DaemonDiagnosticField::StreamSequence,
                    pressure.last_sequence.value(),
                ),
                boolean(DaemonDiagnosticField::GapPending, pressure.gap_pending),
                number(DaemonDiagnosticField::Coalesced, pressure.coalesced_items),
                number(DaemonDiagnosticField::Dropped, pressure.dropped_items),
            ],
        ),
    }
}

#[cfg(test)]
mod tests {
    use undercurrent_multiplex::{
        DeliverySemantics, SchedulerPressure, StreamKind, StreamPressure, StreamRecoveryAction,
        TrafficClass,
    };
    use undercurrent_transport_core::{
        NegotiatedConnectionDiagnostic, ServerConnectionPressure, ServerStreamConfiguration,
    };
    use undercurrent_wire::{
        ConnectionId, ConnectionLimits, ProtocolVersion, RequestId, ServerEpoch, StreamSequence,
    };

    use super::*;

    #[test]
    fn negotiated_connection_rows_retain_pressure_without_payload_fields() {
        let connection = ActiveConnectionDiagnostic {
            client_id: ClientId::new("client-1").expect("client ID"),
            negotiated: NegotiatedConnectionDiagnostic {
                connection_id: ConnectionId::new("connection-1").expect("connection ID"),
                server_epoch: ServerEpoch::new("epoch-1").expect("server epoch"),
                protocol_version: ProtocolVersion::new(1, 2),
                limits: connection_limits(),
                resume_retention_millis: Some(30_000),
            },
            pressure: Some(ServerConnectionPressure {
                pending_requests: 2,
                request_ids_used: 5,
                request_id_limit: undercurrent_transport_core::REQUEST_ID_EPOCH_LEDGER_LIMIT,
                logical_streams: 3,
                scheduler: scheduler_pressure(),
                closed: false,
            }),
        };

        let row = connection_row(&connection);

        assert_eq!(row.identity, "connection:connection-1");
        assert_eq!(
            row.fields.get(&DaemonDiagnosticField::QueuedMessages),
            Some(&crate::DaemonDiagnosticValue::Unsigned(5))
        );
        assert_eq!(
            row.fields.get(&DaemonDiagnosticField::ProtocolVersion),
            Some(&crate::DaemonDiagnosticValue::Text("1.2.0".to_owned()))
        );
        assert!(!format!("{row:?}").contains("payload"));
    }

    #[test]
    fn slow_consumer_rows_are_current_pressure_not_historical_drop_counts() {
        let blocked = active_stream(64, 0, false, 0);
        assert!(is_slow_consumer(&blocked));
        let row = stream_row(
            DaemonDiagnosticSection::SlowConsumers,
            &ClientId::new("client-1").expect("client ID"),
            &blocked,
        );
        assert_eq!(
            row.fields.get(&DaemonDiagnosticField::TrafficClass),
            Some(&crate::DaemonDiagnosticValue::Text("bulk".to_owned()))
        );
        assert_eq!(
            row.fields.get(&DaemonDiagnosticField::QueueByteLimit),
            Some(&crate::DaemonDiagnosticValue::Unsigned(1_024))
        );
        assert_eq!(
            row.fields.get(&DaemonDiagnosticField::QueueMessageLimit),
            Some(&crate::DaemonDiagnosticValue::Unsigned(8))
        );
        assert!(blocked.queue_byte_limit > 0);
        assert!(blocked.queue_message_limit > 0);
        let ServerStreamDiagnosticState::Active { pressure, .. } = blocked.state else {
            panic!("fixture must be active");
        };
        assert!(pressure.queued_bytes <= blocked.queue_byte_limit);
        assert!(
            u64::try_from(pressure.queued_messages).expect("queued message count fits u64")
                <= blocked.queue_message_limit
        );
        assert!(!format!("{row:?}").contains("payload"));

        let historical_drop_only = active_stream(0, 64, false, 9);
        assert!(!is_slow_consumer(&historical_drop_only));
        let opening = ServerStreamDiagnostic {
            stream_id: StreamId::new("stream-2").expect("stream ID"),
            queue_byte_limit: 1_024,
            queue_message_limit: 8,
            state: ServerStreamDiagnosticState::Opening {
                request_id: RequestId::new("request-2").expect("request ID"),
            },
        };
        assert!(!is_slow_consumer(&opening));
    }

    fn active_stream(
        queued_bytes: u64,
        credit_bytes: u64,
        gap_pending: bool,
        dropped_items: u64,
    ) -> ServerStreamDiagnostic {
        ServerStreamDiagnostic {
            stream_id: StreamId::new("stream-1").expect("stream ID"),
            queue_byte_limit: 1_024,
            queue_message_limit: 8,
            state: ServerStreamDiagnosticState::Active {
                configuration: ServerStreamConfiguration {
                    kind: StreamKind::TerminalOutput,
                    class: TrafficClass::Bulk,
                    delivery: DeliverySemantics::Droppable,
                    recovery: StreamRecoveryAction::RequestSnapshot,
                    retain_for_resume: true,
                },
                pressure: StreamPressure {
                    kind: StreamKind::TerminalOutput,
                    queued_bytes,
                    queued_messages: usize::from(queued_bytes > 0),
                    credit_bytes,
                    last_sequence: StreamSequence::new(7),
                    gap_pending,
                    coalesced_items: 2,
                    dropped_items,
                },
            },
        }
    }

    fn connection_limits() -> ConnectionLimits {
        ConnectionLimits::new(1024, 300, 32, 4096, 1024, 512).expect("connection limits")
    }

    fn scheduler_pressure() -> SchedulerPressure {
        SchedulerPressure {
            queued_bytes: 128,
            queued_messages: 5,
            active_streams: 3,
            connection_credit_bytes: 2048,
            class_queued_bytes: [0, 0, 0, 128],
            class_queued_messages: [0, 0, 0, 5],
            coalesced_items: 2,
            dropped_items: 1,
        }
    }
}
