// SPDX-License-Identifier: Apache-2.0

//! Validated multiplex transport, request, replay, and control-plane settings.

pub use undercurrent_config::{DaemonRequestConfig, DaemonTransportConfig};

use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};
use std::time::Duration;

use undercurrent_application::TerminalOutputLimits;
use undercurrent_multiplex::{ReplayLimits, SchedulerMessageLimits};
use undercurrent_protocol::{
    ProductCodec, ProductCodecLimits, TerminalOutputCodec, product_capabilities,
};
use undercurrent_runtime::NativeProcessRuntimeLimits;
use undercurrent_transport_core::{RequestIdempotencyLimits, ServerDiagnosticLimits};
use undercurrent_transport_local::LocalServerLimits;
use undercurrent_wire::WireCodec;
use undercurrent_wire::core::NamespacedName;
use undercurrent_wire::{
    CURRENT_PROTOCOL_VERSION, ConnectionLimits, ConnectionPolicy, HeartbeatPolicy,
    connection_capabilities,
};

use crate::{
    CursorStoreLimits, DaemonConfigError, DaemonOperationsLimits, ProductDispatcherLimits,
    ProductStreamPumpLimits, ScreenCheckpointLimits,
};

pub(crate) const WIRE_ENVELOPE_RESERVATION_BYTES: u32 =
    undercurrent_wire::MAX_CLIENT_FRAME_ENVELOPE_BYTES;

/// Fully validated transport and request settings used by daemon composition.
#[derive(Clone)]
pub struct DaemonTransportSettings {
    policy: ConnectionPolicy,
    local_server: LocalServerLimits,
    product_codec: ProductCodec,
    terminal_codec: TerminalOutputCodec,
    terminal_stream_bytes: usize,
    dispatcher: ProductDispatcherLimits,
    idempotency: RequestIdempotencyLimits,
    cursor: CursorStoreLimits,
    screen_checkpoint: ScreenCheckpointLimits,
    stream_pump: ProductStreamPumpLimits,
    operations: DaemonOperationsLimits,
}

impl DaemonTransportSettings {
    /// Validates the entire connection and request graph before socket or
    /// worker creation.
    ///
    /// # Errors
    ///
    /// Returns a typed daemon configuration error preserving the concrete
    /// component failure or naming a cross-component relationship.
    pub(crate) fn validate(
        transport: DaemonTransportConfig,
        requests: DaemonRequestConfig,
        terminal: TerminalOutputLimits,
        process: NativeProcessRuntimeLimits,
    ) -> Result<Self, DaemonConfigError> {
        let connection = ConnectionLimits::new(
            transport.max_frame_bytes,
            transport.max_logical_streams,
            transport.max_pending_requests,
            transport.connection_queue_bytes,
            transport.stream_queue_bytes,
            transport.initial_stream_window_bytes,
        )
        .map_err(|source| DaemonConfigError::component("transport.connection", source))?;
        WireCodec::new(transport.max_frame_bytes)
            .map_err(|source| DaemonConfigError::component("transport.wire", source))?;
        validate_payload_fit(transport.max_frame_bytes, &requests)?;

        let heartbeat = HeartbeatPolicy::new(
            transport.heartbeat_interval_millis,
            transport.heartbeat_timeout_millis,
        )
        .map_err(|source| DaemonConfigError::component("transport.heartbeat", source))?;
        let capabilities = [
            connection_capabilities::MULTIPLEX,
            connection_capabilities::FLOW_CONTROL,
            connection_capabilities::STREAM_RESUME,
            connection_capabilities::BINARY_PAYLOADS,
            connection_capabilities::TRACE_CONTEXT,
            product_capabilities::ERROR_DISPOSITION,
            product_capabilities::OPAQUE_OUTPUT_CAPTURE,
            product_capabilities::SESSION_REVISION_PRECONDITION,
            product_capabilities::ERROR_EFFECT_CERTAINTY,
        ]
        .into_iter()
        .map(NamespacedName::new)
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|source| DaemonConfigError::component("transport.capabilities", source))?;
        let policy = ConnectionPolicy::new(
            CURRENT_PROTOCOL_VERSION,
            capabilities,
            connection,
            heartbeat,
            Some(transport.resume_retention_millis),
        )
        .map_err(|source| DaemonConfigError::component("transport.policy", source))?;
        let replay = ReplayLimits::new(
            transport.replay_streams,
            transport.replay_bytes_per_stream,
            transport.replay_bytes_per_connection,
            transport.replay_item_bytes,
        )
        .map_err(|source| DaemonConfigError::component("transport.replay", source))?;
        let scheduler_messages = SchedulerMessageLimits::new(
            transport.connection_queue_messages,
            transport.stream_queue_messages,
            [
                transport.control_queue_messages,
                transport.lifecycle_queue_messages,
                transport.state_queue_messages,
                transport.bulk_queue_messages,
            ],
        )
        .map_err(|source| DaemonConfigError::component("transport.scheduler_messages", source))?;
        let diagnostics = ServerDiagnosticLimits::new(
            transport.connection_diagnostic_page_size,
            transport.stream_diagnostic_page_size,
        )
        .map_err(|source| DaemonConfigError::component("transport.diagnostics", source))?;
        let local_server = LocalServerLimits::new(
            transport.handshake_frame_bytes,
            Duration::from_millis(transport.handshake_timeout_millis),
            Duration::from_millis(transport.write_timeout_millis),
            usize::try_from(transport.max_frame_bytes)
                .unwrap_or(usize::MAX)
                .max(usize::try_from(transport.handshake_frame_bytes).unwrap_or(usize::MAX)),
            transport.maximum_connections,
            transport.maximum_retained_issues,
            transport.connection_queue_messages,
            scheduler_messages,
            replay,
            diagnostics,
            &policy,
        )
        .map_err(|source| DaemonConfigError::component("transport.local_server", source))?;

        let product_codec_limits = ProductCodecLimits::new(
            requests.request_bytes as usize,
            requests.response_bytes as usize,
            requests.stream_spec_bytes as usize,
            requests.stream_item_bytes as usize,
        )
        .map_err(|source| DaemonConfigError::component("requests.codec", source))?;
        let product_codec = ProductCodec::new(product_codec_limits);
        let terminal_payload_bytes = process
            .screen_snapshot_bytes
            .max(requests.terminal_stream_bytes);
        if requests.terminal_stream_bytes > terminal.replay_bytes {
            return Err(DaemonConfigError::InvalidRelationship {
                rule: "terminal stream bytes must not exceed retained output replay bytes",
            });
        }
        validate_terminal_payload_fit(transport.max_frame_bytes, terminal_payload_bytes)?;
        let terminal_codec = TerminalOutputCodec::new(terminal_payload_bytes)
            .map_err(|source| DaemonConfigError::component("requests.terminal_codec", source))?;

        let dispatcher = ProductDispatcherLimits::new(
            requests.dispatcher_workers,
            requests.dispatcher_queue_capacity,
            Duration::from_millis(requests.idempotency_retention_millis),
        )
        .map_err(|source| DaemonConfigError::component("requests.dispatcher", source))?;
        let idempotency = RequestIdempotencyLimits::new(
            requests.idempotency_records,
            requests.idempotency_total_bytes,
            requests.request_bytes,
            requests.response_bytes,
        )
        .map_err(|source| DaemonConfigError::component("requests.idempotency", source))?;
        let cursor = CursorStoreLimits::new(requests.cursor_clients, requests.cursors_per_client)
            .map_err(|source| DaemonConfigError::component("requests.cursors", source))?;
        let screen_checkpoint = ScreenCheckpointLimits::new(
            requests.screen_checkpoint_clients,
            requests.screen_checkpoints_per_client,
            requests.screen_checkpoint_bytes_per_client,
        )
        .map_err(|source| DaemonConfigError::component("requests.screen_checkpoints", source))?;
        let stream_pump = ProductStreamPumpLimits::new(requests.product_streams)
            .map_err(|source| DaemonConfigError::component("requests.streams", source))?;
        let operations = DaemonOperationsLimits::new(
            requests.diagnostic_clients,
            requests.diagnostic_cursors_per_client,
            requests.diagnostic_page_size,
            requests.diagnostic_snapshot_rows,
            Duration::from_millis(requests.maximum_shutdown_drain_millis),
        )
        .map_err(|source| DaemonConfigError::component("requests.diagnostics", source))?;

        validate_capacity_relationships(&transport, &requests, process)?;
        Ok(Self {
            policy,
            local_server,
            product_codec,
            terminal_codec,
            terminal_stream_bytes: requests.terminal_stream_bytes,
            dispatcher,
            idempotency,
            cursor,
            screen_checkpoint,
            stream_pump,
            operations,
        })
    }

    /// Returns the server connection negotiation policy.
    pub const fn policy(&self) -> &ConnectionPolicy {
        &self.policy
    }

    /// Returns Local listener, handshake, replay, and diagnostic bounds.
    pub const fn local_server(&self) -> LocalServerLimits {
        self.local_server
    }

    /// Returns the bounded product JSON codec.
    pub const fn product_codec(&self) -> ProductCodec {
        self.product_codec
    }

    /// Returns the bounded raw terminal binary codec.
    pub const fn terminal_codec(&self) -> TerminalOutputCodec {
        self.terminal_codec
    }

    /// Returns maximum raw output bytes read into one terminal stream item.
    pub const fn terminal_stream_bytes(&self) -> usize {
        self.terminal_stream_bytes
    }

    /// Returns bounded product execution worker settings.
    pub const fn dispatcher(&self) -> ProductDispatcherLimits {
        self.dispatcher
    }

    /// Returns bounded client-scoped mutation retry settings.
    pub const fn idempotency(&self) -> RequestIdempotencyLimits {
        self.idempotency
    }

    /// Returns bounded session and profile cursor settings.
    pub const fn cursor(&self) -> CursorStoreLimits {
        self.cursor
    }

    /// Returns bounded terminal screen checkpoint settings.
    pub const fn screen_checkpoint(&self) -> ScreenCheckpointLimits {
        self.screen_checkpoint
    }

    /// Returns the daemon-wide product logical-stream cap.
    pub const fn stream_pump(&self) -> ProductStreamPumpLimits {
        self.stream_pump
    }

    /// Returns bounded daemon status, diagnostics, and shutdown settings.
    pub const fn operations(&self) -> DaemonOperationsLimits {
        self.operations
    }
}

impl Debug for DaemonTransportSettings {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonTransportSettings")
            .field("policy", &self.policy)
            .field("local_server", &self.local_server)
            .field("product_codec", &self.product_codec)
            .field("terminal_codec", &self.terminal_codec)
            .field("terminal_stream_bytes", &self.terminal_stream_bytes)
            .field("dispatcher", &self.dispatcher)
            .field("idempotency", &self.idempotency)
            .field("cursor", &self.cursor)
            .field("screen_checkpoint", &self.screen_checkpoint)
            .field("stream_pump", &self.stream_pump)
            .field("operations", &self.operations)
            .finish()
    }
}

fn validate_payload_fit(
    max_frame_bytes: u32,
    requests: &DaemonRequestConfig,
) -> Result<(), DaemonConfigError> {
    let available = max_frame_bytes.saturating_sub(WIRE_ENVELOPE_RESERVATION_BYTES);
    for (field, payload) in [
        ("request_bytes", requests.request_bytes),
        ("response_bytes", requests.response_bytes),
        ("stream_spec_bytes", requests.stream_spec_bytes),
        ("stream_item_bytes", requests.stream_item_bytes),
    ] {
        if payload > available {
            return Err(DaemonConfigError::InvalidRelationship {
                rule: match field {
                    "request_bytes" => "request payload plus wire envelope must fit one frame",
                    "response_bytes" => "response payload plus wire envelope must fit one frame",
                    "stream_spec_bytes" => {
                        "stream specification plus wire envelope must fit one frame"
                    }
                    _ => "stream item plus wire envelope must fit one frame",
                },
            });
        }
    }
    Ok(())
}

fn validate_terminal_payload_fit(
    max_frame_bytes: u32,
    terminal_payload_bytes: usize,
) -> Result<(), DaemonConfigError> {
    let payload = u64::try_from(terminal_payload_bytes).unwrap_or(u64::MAX);
    let envelope = u64::from(WIRE_ENVELOPE_RESERVATION_BYTES)
        .saturating_add(TerminalOutputCodec::screen_header_bytes() as u64);
    if payload.saturating_add(envelope) > u64::from(max_frame_bytes) {
        return Err(DaemonConfigError::InvalidRelationship {
            rule: "terminal payload plus binary and wire envelopes must fit one frame",
        });
    }
    Ok(())
}

fn validate_capacity_relationships(
    transport: &DaemonTransportConfig,
    requests: &DaemonRequestConfig,
    process: NativeProcessRuntimeLimits,
) -> Result<(), DaemonConfigError> {
    if transport.replay_streams < transport.max_logical_streams as usize {
        return Err(DaemonConfigError::InvalidRelationship {
            rule: "replay stream capacity must cover one connection's logical stream capacity",
        });
    }
    if requests.product_streams < transport.max_logical_streams as usize {
        return Err(DaemonConfigError::InvalidRelationship {
            rule: "global product stream capacity must cover one complete connection",
        });
    }
    for (clients, rule) in [
        (
            requests.cursor_clients,
            "cursor client capacity must cover active connection capacity",
        ),
        (
            requests.screen_checkpoint_clients,
            "screen checkpoint client capacity must cover active connection capacity",
        ),
        (
            requests.diagnostic_clients,
            "diagnostic client capacity must cover active connection capacity",
        ),
    ] {
        if clients < transport.maximum_connections {
            return Err(DaemonConfigError::InvalidRelationship { rule });
        }
    }
    if requests.screen_checkpoint_bytes_per_client < process.screen_snapshot_bytes {
        return Err(DaemonConfigError::InvalidRelationship {
            rule: "one terminal screen snapshot must fit a client checkpoint byte budget",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use undercurrent_application::TerminalOutputLimits;

    use super::*;

    fn terminal() -> TerminalOutputLimits {
        TerminalOutputLimits {
            per_session_bytes: 2_097_152,
            global_bytes: 268_435_456,
            chunk_bytes: 16_384,
            append_bytes: 65_536,
            replay_bytes: 1_048_576,
            sessions: 2_048,
            subscriptions: 4_096,
        }
    }

    fn process() -> NativeProcessRuntimeLimits {
        NativeProcessRuntimeLimits {
            max_processes: 2_048,
            spawn_workers: 8,
            spawn_queue_capacity: 512,
            input_queue_sequences: 64,
            input_queue_bytes_per_process: 1_048_576,
            output_read_bytes: 16_384,
            screen_snapshot_bytes: 131_072,
            environment_file_bytes: 1_048_576,
            environment_entries: 1_024,
            environment_bytes: 2_097_152,
            worker_thread_stack_bytes: 524_288,
        }
    }

    #[test]
    fn transport_rejects_zero_and_inconsistent_scheduler_message_budgets() {
        let requests = DaemonRequestConfig::default();
        let zero = DaemonTransportConfig {
            connection_queue_messages: 0,
            ..DaemonTransportConfig::default()
        };
        assert!(
            DaemonTransportSettings::validate(zero, requests.clone(), terminal(), process(),)
                .is_err()
        );

        let defaults = DaemonTransportConfig::default();
        let inconsistent = DaemonTransportConfig {
            stream_queue_messages: defaults.connection_queue_messages + 1,
            ..defaults
        };
        assert!(
            DaemonTransportSettings::validate(inconsistent, requests, terminal(), process(),)
                .is_err()
        );
    }

    #[test]
    fn defaults_validate_for_three_hundred_streams_on_one_connection() {
        let settings = DaemonTransportSettings::validate(
            DaemonTransportConfig::default(),
            DaemonRequestConfig::default(),
            terminal(),
            process(),
        )
        .expect("default transport");

        assert!(settings.policy().limits().max_logical_streams() >= 300);
        assert!(settings.stream_pump().streams() >= 300);
        assert_eq!(settings.terminal_stream_bytes(), 131_072);
        assert!(settings.policy().capabilities().iter().any(|capability| {
            capability.as_str() == product_capabilities::OPAQUE_OUTPUT_CAPTURE
        }));
    }

    #[test]
    fn default_policy_reserves_each_traffic_class_for_a_large_input_frame() {
        let transport = DaemonTransportConfig::default();
        assert_eq!(transport.connection_queue_bytes, 134_217_728);
        assert_eq!(transport.stream_queue_bytes, 33_554_432);
        assert_eq!(transport.connection_queue_messages, 16_384);
        assert_eq!(transport.stream_queue_messages, 1_024);
        assert_eq!(transport.initial_stream_window_bytes, 262_144);

        let settings = DaemonTransportSettings::validate(
            transport,
            DaemonRequestConfig::default(),
            terminal(),
            process(),
        )
        .expect("default transport");
        assert_eq!(
            settings.policy().limits().connection_queue_bytes(),
            134_217_728
        );
        assert_eq!(settings.policy().limits().stream_queue_bytes(), 33_554_432);
        assert_eq!(
            settings.policy().limits().initial_stream_window_bytes(),
            262_144
        );
    }

    #[test]
    fn terminal_snapshot_must_fit_the_negotiated_frame() {
        let mut process = process();
        process.screen_snapshot_bytes = DaemonTransportConfig::default().max_frame_bytes as usize;

        let error = DaemonTransportSettings::validate(
            DaemonTransportConfig::default(),
            DaemonRequestConfig::default(),
            terminal(),
            process,
        )
        .expect_err("oversized screen must fail");

        assert_eq!(
            error.to_string(),
            "terminal payload plus binary and wire envelopes must fit one frame"
        );
    }

    #[test]
    fn per_client_state_must_cover_active_connections() {
        let requests = DaemonRequestConfig {
            cursor_clients: 127,
            ..DaemonRequestConfig::default()
        };

        let error = DaemonTransportSettings::validate(
            DaemonTransportConfig::default(),
            requests,
            terminal(),
            process(),
        )
        .expect_err("undersized client state must fail");

        assert_eq!(
            error.to_string(),
            "cursor client capacity must cover active connection capacity"
        );
    }
}
