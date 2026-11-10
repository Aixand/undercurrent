// SPDX-License-Identifier: Apache-2.0
//! Pure document constraints, shared by both binary adapters.
//!
//! Shape checking uses the serialized Rust defaults to reject unknown keys and
//! wrong types without echoing source values. Validation then checks expanded
//! numeric ranges from the embedded schema, shortcut grammar, and component
//! capacity relationships. This is not a general JSON Schema evaluator or a
//! replacement for the receiving components' own constructors.
use crate::{ConfigError, MAX_SCROLLBACK_CELLS, UndercurrentProductConfig, load::invalid};
use serde_json::Value;

pub(crate) fn validate_shape(
    value: &Value,
    defaults: &Value,
    path: &str,
) -> Result<(), ConfigError> {
    if let Some(fields) = value.as_object() {
        if !defaults.is_object() {
            return Err(invalid(path, "unexpected mapping"));
        }
        for (key, value) in fields {
            let field = if path.is_empty() {
                key.clone()
            } else {
                format!("{path}.{key}")
            };
            // Never echo an unknown, user-controlled key in errors.
            let default = defaults.get(key).ok_or_else(|| {
                invalid(
                    if path.is_empty() { "document" } else { path },
                    "unknown field",
                )
            })?;
            validate_shape(value, default, &field)?;
        }
    } else if (defaults.is_boolean() && !value.is_boolean())
        || (defaults.is_number() && value.as_u64().is_none())
        || (defaults.is_string() && !value.is_string())
        || defaults.is_object()
    {
        return Err(invalid(path, "wrong value type"));
    }
    Ok(())
}

// Mirror component constraints so the CLI can reject incompatible settings offline
// without importing daemon constructors. Adapters still validate directly constructed
// settings; see [ownership](../docs/internals/loading.md#ownership-and-dependencies).
pub(crate) fn validate(config: &UndercurrentProductConfig) -> Result<(), ConfigError> {
    let value = serde_json::to_value(config).expect("serializable configuration");
    let schema: Value = serde_json::from_str(include_str!("../../../schemas/config.schema.json"))
        .expect("checked configuration schema");
    ranges(&value, &schema, &schema, "")?;
    let prefix = &config.shortcuts.prefix.to_ascii_lowercase();
    let letter = prefix
        .strip_prefix("alt-")
        .or_else(|| prefix.strip_prefix("ctrl-"));
    if !((prefix.len() == 1 && prefix.as_bytes()[0].is_ascii())
        || letter.is_some_and(|s| s.len() == 1 && s.as_bytes()[0].is_ascii_lowercase()))
    {
        return Err(invalid(
            "shortcuts.prefix",
            "expected alt-a through alt-z, ctrl-a through ctrl-z, or an ASCII character",
        ));
    }
    let cells = config
        .terminal
        .scrollback
        .memory_lines
        .checked_mul(usize::from(config.services.max_terminal_columns));
    if cells.is_none_or(|v| v > MAX_SCROLLBACK_CELLS) {
        return Err(invalid(
            "terminal.scrollback.memory_lines",
            "history rows times maximum columns exceeds 5000000 cells",
        ));
    }
    let p = &config.process;
    let t = &config.terminal;
    let w = &config.workers;
    let s = &config.services;
    let r = &config.requests;
    let tr = &config.transport;
    let c = &config.client;
    macro_rules! require {
        ($condition:expr, $field:expr, $rule:expr) => {
            if !$condition {
                return Err(invalid($field, $rule));
            }
        };
    }
    require!(
        config.logging.max_file_bytes >= 4096,
        "logging.max_file_bytes",
        "must be at least 4096"
    );
    require!(
        p.worker_thread_stack_bytes >= 65536,
        "process.worker_thread_stack_bytes",
        "must be at least 65536"
    );
    require!(
        w.health_check_detail_bytes >= 32,
        "workers.health_check_detail_bytes",
        "must be at least 32"
    );
    require!(
        t.chunk_bytes <= t.per_session_bytes && t.chunk_bytes <= t.global_bytes,
        "terminal.chunk_bytes",
        "must fit per-session and global retention"
    );
    require!(
        p.max_processes <= config.persistence.max_sessions,
        "process.max_processes",
        "must fit durable session capacity"
    );
    require!(
        p.output_read_bytes <= t.append_bytes,
        "process.output_read_bytes",
        "must fit terminal append capacity"
    );
    require!(
        p.max_processes <= t.sessions,
        "terminal.sessions",
        "must cover process capacity"
    );
    require!(
        w.outbox_batch_size <= config.persistence.max_read_batch,
        "workers.outbox_batch_size",
        "must fit persistence read batch"
    );
    require!(
        w.reconciliation_batch_size <= config.persistence.max_read_batch,
        "workers.reconciliation_batch_size",
        "must fit persistence read batch"
    );
    require!(
        w.restart_batch_size <= s.restart_jobs,
        "workers.restart_batch_size",
        "must fit restart queue"
    );
    require!(
        w.health_check_workers <= p.max_processes,
        "workers.health_check_workers",
        "must fit process capacity"
    );
    require!(
        w.health_check_screen_text_bytes <= s.regex_match_text_bytes,
        "services.regex_match_text_bytes",
        "must contain a health screen sample"
    );
    require!(
        s.observation_jobs >= p.max_processes.saturating_mul(2),
        "services.observation_jobs",
        "must cover health and foreground work for every process"
    );
    require!(
        s.session_event_records >= w.outbox_batch_size,
        "services.session_event_records",
        "must contain an outbox batch"
    );
    require!(
        s.detection_profiles >= s.max_profile_page_size,
        "services.detection_profiles",
        "must cover a maximum profile page"
    );
    require!(
        s.session_subscriptions >= tr.max_logical_streams as usize
            && s.session_event_subscriptions >= tr.max_logical_streams as usize
            && t.subscriptions >= tr.max_logical_streams as usize,
        "services",
        "subscription capacities must cover a connection's streams"
    );
    require!(
        config.cli.list_page_size as usize <= s.max_query_page_size,
        "cli.list_page_size",
        "must fit services.max_query_page_size"
    );
    require!(
        config.cli.logs_max_bytes as usize <= t.replay_bytes,
        "cli.logs_max_bytes",
        "must fit terminal.replay_bytes"
    );
    require!(
        s.max_query_page_size <= u32::MAX as usize && t.replay_bytes <= u32::MAX as usize,
        "services",
        "CLI page limits must fit u32"
    );
    require!(
        r.terminal_stream_bytes <= t.replay_bytes,
        "requests.terminal_stream_bytes",
        "must fit retained replay capacity"
    );
    require!(
        tr.heartbeat_timeout_millis > tr.heartbeat_interval_millis,
        "transport.heartbeat_timeout_millis",
        "must exceed heartbeat interval"
    );
    require!(
        tr.stream_queue_bytes <= tr.connection_queue_bytes,
        "transport.stream_queue_bytes",
        "must fit connection queue"
    );
    require!(
        u64::from(tr.max_frame_bytes) <= tr.stream_queue_bytes,
        "transport.max_frame_bytes",
        "must fit stream queue"
    );
    require!(
        tr.initial_stream_window_bytes <= tr.stream_queue_bytes,
        "transport.initial_stream_window_bytes",
        "must fit stream queue"
    );
    require!(
        tr.replay_bytes_per_stream <= tr.replay_bytes_per_connection,
        "transport.replay_bytes_per_stream",
        "must fit connection replay budget"
    );
    require!(
        tr.replay_item_bytes <= tr.replay_bytes_per_stream,
        "transport.replay_item_bytes",
        "must fit stream replay budget"
    );
    require!(
        tr.stream_queue_messages <= tr.connection_queue_messages,
        "transport.stream_queue_messages",
        "must fit connection queue"
    );
    for limit in [
        tr.control_queue_messages,
        tr.lifecycle_queue_messages,
        tr.state_queue_messages,
        tr.bulk_queue_messages,
    ] {
        require!(
            limit <= tr.connection_queue_messages,
            "transport",
            "class queue must fit connection message budget"
        );
    }
    require!(
        c.cancellation_interval_millis <= c.dial_timeout_millis,
        "client.cancellation_interval_millis",
        "must not exceed dial timeout"
    );
    require!(
        c.reconnect_initial_delay_millis <= c.reconnect_maximum_delay_millis,
        "client.reconnect_initial_delay_millis",
        "must not exceed maximum delay"
    );
    require!(
        c.reconnect_backoff_basis_points >= 10000,
        "client.reconnect_backoff_basis_points",
        "must be at least 10000"
    );
    for value in [
        config.persistence.max_sessions,
        config.persistence.max_outbox_events,
        config.persistence.max_record_bytes,
        config.persistence.max_read_batch,
    ] {
        require!(
            value <= i64::MAX as usize,
            "persistence",
            "SQLite limits must fit a signed 64-bit integer"
        );
    }
    require!(
        p.spawn_workers <= p.max_processes,
        "process.spawn_workers",
        "must fit process capacity"
    );
    require!(
        w.foreground_activation_delay_millis > 0 && w.health_activation_delay_millis > 0,
        "workers",
        "activation delays must be nonzero"
    );
    // UCMP 0.1.0-beta reserves 12 header bytes, seven length-prefixed 256-byte IDs,
    // a 2-byte count and a u64. Keep the constants local because architecture
    // policy forbids internal crate dependencies here. When codecs change, review
    // these reservations and the daemon's config_validation_parity tests together.
    // See [payload envelopes](../../../docs/reference/configuration-fields.md#payload-envelopes).
    const ENVELOPE: u64 =
        12 + undercurrent_foundation::RELEASE_VERSION.len() as u64 + 7 * (2 + 256) + 2 + 8;
    const SCREEN_HEADER: u64 = 26;
    require!(
        tr.connection_queue_bytes >= u64::from(tr.max_frame_bytes) * 4,
        "transport.connection_queue_bytes",
        "must reserve one maximum frame for each of four traffic classes"
    );
    require!(
        s.max_input_bytes <= p.input_queue_bytes_per_process,
        "services.max_input_bytes",
        "must fit process.input_queue_bytes_per_process"
    );
    require!(
        s.max_input_bytes < r.request_bytes as usize,
        "services.max_input_bytes",
        "must leave room for JSON metadata in requests.request_bytes; escaped input may need more"
    );
    for payload in [
        r.request_bytes,
        r.response_bytes,
        r.stream_spec_bytes,
        r.stream_item_bytes,
    ] {
        require!(
            u64::from(payload) + ENVELOPE <= u64::from(tr.max_frame_bytes),
            "requests",
            "payload and wire envelope must fit a frame"
        );
    }
    require!(
        (p.screen_snapshot_bytes.max(r.terminal_stream_bytes) as u64)
            .saturating_add(ENVELOPE + SCREEN_HEADER)
            <= u64::from(tr.max_frame_bytes),
        "process.screen_snapshot_bytes",
        "terminal payload and envelopes must fit a frame"
    );
    require!(
        u64::from(tr.handshake_frame_bytes)
            >= 12 + undercurrent_foundation::RELEASE_VERSION.len() as u64,
        "transport.handshake_frame_bytes",
        "must fit a handshake header"
    );
    require!(
        tr.replay_streams >= tr.max_logical_streams as usize,
        "transport.replay_streams",
        "must cover a connection's streams"
    );
    require!(
        r.product_streams >= tr.max_logical_streams as usize,
        "requests.product_streams",
        "must cover a connection's streams"
    );
    require!(
        tr.replay_item_bytes >= u64::from(tr.max_frame_bytes),
        "transport.replay_item_bytes",
        "must fit a frame"
    );
    for capacity in [
        r.cursor_clients,
        r.screen_checkpoint_clients,
        r.diagnostic_clients,
    ] {
        require!(
            capacity >= tr.maximum_connections,
            "requests",
            "client capacities must cover active connections"
        );
    }
    require!(
        r.screen_checkpoint_bytes_per_client >= p.screen_snapshot_bytes,
        "requests.screen_checkpoint_bytes_per_client",
        "must contain one screen snapshot"
    );
    require!(
        r.idempotency_total_bytes >= u64::from(r.request_bytes) + u64::from(r.response_bytes),
        "requests.idempotency_total_bytes",
        "must contain one request and result"
    );
    require!(
        r.diagnostic_snapshot_rows >= r.diagnostic_page_size,
        "requests.diagnostic_snapshot_rows",
        "must contain one diagnostic page"
    );
    require!(
        c.bulk_parallel_requests <= c.writer_queue_messages
            && c.bulk_parallel_requests <= tr.max_pending_requests as usize,
        "client.bulk_parallel_requests",
        "must fit writer queue and pending request limits"
    );
    require!(
        c.stream_mailbox_bytes <= tr.stream_queue_bytes,
        "client.stream_mailbox_bytes",
        "must fit stream queue"
    );
    require!(
        c.reconnect_jitter_basis_points > 0 && c.reconnect_maximum_attempts > 0,
        "client",
        "recovery jitter and attempts must be nonzero"
    );
    require!(
        s.max_work_items <= u32::MAX as usize,
        "services.max_work_items",
        "must fit u32"
    );
    require!(
        s.regex_match_text_bytes >= s.max_profile_sample_bytes
            && s.regex_match_text_bytes >= p.screen_snapshot_bytes,
        "services.regex_match_text_bytes",
        "must contain profile and screen samples"
    );
    require!(
        s.foreground_evidence_items >= 4,
        "services.foreground_evidence_items",
        "must contain every runtime evidence source"
    );
    require!(
        s.foreground_motion_confidence_basis_points > 0
            && s.foreground_motion_confidence_basis_points
                >= s.foreground_minimum_confidence_basis_points
            && s.foreground_server_idle_confidence_basis_points > 0
            && s.foreground_server_idle_confidence_basis_points
                >= s.foreground_minimum_confidence_basis_points,
        "services",
        "runtime confidence must be nonzero and meet decision minimum"
    );
    require!(
        s.profile_input_submission_delay_millis > 0,
        "services.profile_input_submission_delay_millis",
        "must be nonzero"
    );
    Ok(())
}

// The typed document handles structure; apply the shipped schema's numeric ranges
// to the default-expanded values so omitted fields receive the same validation.
fn ranges(
    value: &Value,
    definition: &Value,
    schema: &Value,
    path: &str,
) -> Result<(), ConfigError> {
    if let Some(reference) = definition.get("$ref").and_then(Value::as_str) {
        let target = schema
            .pointer(reference.trim_start_matches('#'))
            .expect("local schema reference");
        return ranges(value, target, schema, path);
    }
    if let Some(number) = value.as_u64()
        && (definition
            .get("minimum")
            .and_then(Value::as_u64)
            .is_some_and(|min| number < min)
            || definition
                .get("maximum")
                .and_then(Value::as_u64)
                .is_some_and(|max| number > max))
    {
        return Err(invalid(path, "outside the supported range"));
    }
    if let Some(fields) = value.as_object() {
        for (key, value) in fields {
            if let Some(child) = definition.get("properties").and_then(|p| p.get(key)) {
                let field = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                ranges(value, child, schema, &field)?;
            }
        }
    }
    Ok(())
}
