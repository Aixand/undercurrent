// SPDX-License-Identifier: Apache-2.0

//! Shared serializable configuration; component adapters remain in their owners.
//!
//! Omitted sections and fields use these Rust defaults; unknown fields are
//! rejected by Serde. Deserializing a struct does not check the schema's numeric
//! ranges or cross-component relationships: use [`crate::load_config`] for file
//! loading with those checks. Counts, deadlines, and retention bounds are data
//! for receiving components, not resources or timers created by this crate.

use serde::{Deserialize, Serialize};

/// Serializable runtime settings at the root of the standalone document.
///
/// Bootstrap fields such as `schema_version` and `telemetry` are held separately
/// in [`crate::BootstrapConfig`]. This type carries defaults and field shape;
/// direct construction or deserialization does not validate capacity relations.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct UndercurrentProductConfig {
    /// Saved command preferences.
    pub cli: CliPreferences,
    /// Interactive keyboard preferences.
    pub shortcuts: ShortcutPreferences,
    /// Reserved profile settings; YAML files live beneath the home.
    pub profiles: DaemonProfileConfig,
    /// Reserved path settings; storage derives from the home.
    pub paths: DaemonPathConfig,
    /// Bounded structured daemon logging.
    pub logging: DaemonLoggingConfig,
    /// Protected platform-local client endpoint.
    pub local: LocalTransportConfig,
    /// Durable SQLite bounds.
    pub persistence: PersistenceConfig,
    /// Native PTY and process bounds.
    pub process: ProcessRuntimeConfig,
    /// Terminal retention and screen bounds.
    pub terminal: TerminalRuntimeConfig,
    /// Application and background-worker bounds.
    pub workers: DaemonWorkerConfig,
    /// Physical connection, flow-control, and replay bounds.
    pub transport: DaemonTransportConfig,
    /// Product request, cursor, stream, and control-plane bounds.
    pub requests: DaemonRequestConfig,
    /// Native SDK dial, queue, reconnect, and resume bounds.
    pub client: DaemonClientConfig,
    /// Domain boundary, application registry, and observation bounds.
    pub services: DaemonServiceConfig,
}

/// Reserved profile settings. Editable YAML always lives in home/profiles.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DaemonProfileConfig {}

/// Reserved local IPC settings. The endpoint always lives in home/run.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LocalTransportConfig {}

/// Serializable bounded structured daemon-log settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DaemonLoggingConfig {
    /// Maximum bytes retained in one active or rotated JSONL file.
    pub max_file_bytes: u64,
    /// Maximum total files retained, including the active file.
    pub retained_files: usize,
    /// Maximum complete log lines buffered outside daemon worker threads.
    pub queue_lines: usize,
}

impl Default for DaemonLoggingConfig {
    fn default() -> Self {
        Self {
            max_file_bytes: 16 * 1_024 * 1_024,
            retained_files: 5,
            queue_lines: 8_192,
        }
    }
}

/// Reserved daemon path table. Storage derives exclusively from the home.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DaemonPathConfig {}

/// Serializable SQLite durability and capacity settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PersistenceConfig {
    /// Maximum durable session aggregates.
    pub max_sessions: usize,
    /// Maximum pending transactional outbox records.
    pub max_outbox_events: usize,
    /// Maximum encoded bytes in one aggregate record.
    pub max_record_bytes: usize,
    /// Maximum records read by one bounded storage operation.
    pub max_read_batch: usize,
    /// Maximum admitted requests for the dedicated SQLite worker.
    pub command_queue_capacity: usize,
    /// SQLite lock wait deadline in milliseconds.
    pub busy_timeout_millis: u64,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            max_sessions: 2_048,
            max_outbox_events: 65_536,
            max_record_bytes: 1_048_576,
            max_read_batch: 256,
            command_queue_capacity: 1_024,
            busy_timeout_millis: 5_000,
        }
    }
}

/// Serializable native PTY and process settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProcessRuntimeConfig {
    /// Maximum process slots, including pending exit events.
    pub max_processes: usize,
    /// Maximum concurrent blocking spawn workers.
    pub spawn_workers: usize,
    /// Maximum admitted spawn requests not yet executing.
    pub spawn_queue_capacity: usize,
    /// Maximum atomic input sequences queued per process.
    pub input_queue_sequences: usize,
    /// Maximum queued input bytes per process.
    pub input_queue_bytes_per_process: usize,
    /// Maximum bytes read by one PTY read.
    pub output_read_bytes: usize,
    /// Maximum ANSI bytes rendered by one current-screen snapshot.
    pub screen_snapshot_bytes: usize,
    /// Maximum bytes loaded from one environment file.
    pub environment_file_bytes: usize,
    /// Maximum merged environment entry count.
    pub environment_entries: usize,
    /// Maximum merged environment key and value bytes.
    pub environment_bytes: usize,
    /// Stack reserved for each blocking adapter worker thread.
    pub worker_thread_stack_bytes: usize,
    /// Deadline for confirming forced process termination.
    pub force_confirmation_timeout_millis: u64,
}

impl Default for ProcessRuntimeConfig {
    fn default() -> Self {
        Self {
            max_processes: 2_048,
            spawn_workers: 8,
            spawn_queue_capacity: 512,
            input_queue_sequences: 64,
            input_queue_bytes_per_process: 8_388_608,
            output_read_bytes: 16_384,
            screen_snapshot_bytes: 131_072,
            environment_file_bytes: 1_048_576,
            environment_entries: 1_024,
            environment_bytes: 2_097_152,
            worker_thread_stack_bytes: 524_288,
            force_confirmation_timeout_millis: 2_000,
        }
    }
}

/// Serializable terminal retention and screen settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TerminalRuntimeConfig {
    /// Local normal-screen history.
    pub scrollback: ScrollbackConfig,
    /// Maximum retained output bytes for one session.
    pub per_session_bytes: usize,
    /// Maximum retained output bytes across all sessions.
    pub global_bytes: usize,
    /// Maximum bytes copied into one shared output chunk.
    pub chunk_bytes: usize,
    /// Maximum bytes accepted by one output append.
    pub append_bytes: usize,
    /// Maximum bytes returned by one replay extraction.
    pub replay_bytes: usize,
    /// Maximum distinct terminal output session records.
    pub sessions: usize,
    /// Maximum registered output subscriptions.
    pub subscriptions: usize,
}

impl Default for TerminalRuntimeConfig {
    fn default() -> Self {
        Self {
            scrollback: ScrollbackConfig::default(),
            per_session_bytes: 2_097_152,
            global_bytes: 268_435_456,
            chunk_bytes: 16_384,
            append_bytes: 65_536,
            replay_bytes: 1_048_576,
            sessions: 2_048,
            subscriptions: 4_096,
        }
    }
}

/// Serializable coordination, recovery, and background-worker settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DaemonWorkerConfig {
    /// Maximum session keys held or contended by mutation coordination.
    pub mutation_active_sessions: usize,
    /// Maximum callers waiting behind one session mutation.
    pub mutation_waiters_per_session: usize,
    /// Maximum mutation diagnostic page size.
    pub mutation_diagnostic_page_size: usize,
    /// Maximum transactional outbox records dispatched per cycle.
    pub outbox_batch_size: usize,
    /// Maximum due automatic restart jobs claimed per cycle.
    pub restart_batch_size: usize,
    /// Delay before a newly changed session receives foreground observation.
    pub foreground_activation_delay_millis: u64,
    /// Delay before a newly changed session receives health observation.
    pub health_activation_delay_millis: u64,
    /// Maximum scheduled observation jobs claimed per cycle.
    pub observation_batch_size: usize,
    /// Maximum payload-safe observation issues retained per cycle.
    pub observation_retained_issues: usize,
    /// Fixed blocking workers shared by health checks for every session.
    pub health_check_workers: usize,
    /// Health requests admitted while every health worker is occupied.
    pub health_check_queue_capacity: usize,
    /// Per-rule TCP, HTTP, and command deadline in milliseconds.
    pub health_check_timeout_millis: u64,
    /// Maximum private visible-screen text bytes examined per health request.
    pub health_check_screen_text_bytes: usize,
    /// Maximum payload-safe detail bytes retained per health result.
    pub health_check_detail_bytes: usize,
    /// Normal foreground observation interval.
    pub foreground_interval_millis: u64,
    /// Normal health observation interval.
    pub health_interval_millis: u64,
    /// Retry delay after transient or stale observation work.
    pub observation_retry_interval_millis: u64,
    /// Maximum durable sessions read per startup reconciliation batch.
    pub reconciliation_batch_size: usize,
    /// Maximum payload-safe startup reconciliation issues retained.
    pub reconciliation_retained_issues: usize,
    /// Idle delay between transactional outbox drain cycles.
    pub outbox_poll_interval_millis: u64,
    /// Idle delay between scheduled restart cycles.
    pub restart_poll_interval_millis: u64,
    /// Idle delay between scheduled observation cycles.
    pub observation_poll_interval_millis: u64,
    /// Retry backoff after the process-exit source or settlement fails.
    pub process_exit_error_backoff_millis: u64,
}

impl Default for DaemonWorkerConfig {
    fn default() -> Self {
        Self {
            mutation_active_sessions: 2_048,
            mutation_waiters_per_session: 64,
            mutation_diagnostic_page_size: 256,
            outbox_batch_size: 256,
            restart_batch_size: 256,
            foreground_activation_delay_millis: 100,
            health_activation_delay_millis: 100,
            observation_batch_size: 256,
            observation_retained_issues: 128,
            health_check_workers: 8,
            health_check_queue_capacity: 512,
            health_check_timeout_millis: 2_000,
            health_check_screen_text_bytes: 131_072,
            health_check_detail_bytes: 128,
            foreground_interval_millis: 2_000,
            health_interval_millis: 5_000,
            observation_retry_interval_millis: 1_000,
            reconciliation_batch_size: 256,
            reconciliation_retained_issues: 256,
            outbox_poll_interval_millis: 25,
            restart_poll_interval_millis: 25,
            observation_poll_interval_millis: 100,
            process_exit_error_backoff_millis: 100,
        }
    }
}

/// Serializable native client resource and recovery settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DaemonClientConfig {
    /// Complete client connection dial deadline in milliseconds.
    pub dial_timeout_millis: u64,
    /// Maximum delay before dial or handshake cancellation is rechecked.
    pub cancellation_interval_millis: u64,
    /// Maximum semantic messages waiting for the SDK writer.
    pub writer_queue_messages: usize,
    /// Maximum concurrent finite requests issued by one native bulk command.
    pub bulk_parallel_requests: usize,
    /// Maximum encoded items retained by one logical stream mailbox.
    pub stream_mailbox_messages: usize,
    /// Maximum encoded bytes retained by one logical stream mailbox.
    pub stream_mailbox_bytes: u64,
    /// Delay after the first failed physical connection attempt.
    pub reconnect_initial_delay_millis: u64,
    /// Inclusive reconnect backoff ceiling.
    pub reconnect_maximum_delay_millis: u64,
    /// Exponential reconnect multiplier where 10,000 is 1.0x.
    pub reconnect_backoff_basis_points: u32,
    /// Maximum additive reconnect jitter in basis points.
    pub reconnect_jitter_basis_points: u16,
    /// Inclusive physical connection attempts per recovery pass.
    pub reconnect_maximum_attempts: u32,
    /// Maximum logical streams resumed concurrently on one replacement connection.
    pub maximum_parallel_resumes: usize,
    /// Deadline for each stream activation or resume response in milliseconds.
    pub resume_timeout_millis: u64,
}

impl Default for DaemonClientConfig {
    fn default() -> Self {
        Self {
            dial_timeout_millis: 5_000,
            cancellation_interval_millis: 50,
            writer_queue_messages: 1_024,
            bulk_parallel_requests: 16,
            stream_mailbox_messages: 64,
            stream_mailbox_bytes: 262_144,
            reconnect_initial_delay_millis: 50,
            reconnect_maximum_delay_millis: 2_000,
            reconnect_backoff_basis_points: 20_000,
            reconnect_jitter_basis_points: 2_000,
            reconnect_maximum_attempts: 8,
            maximum_parallel_resumes: 8,
            resume_timeout_millis: 5_000,
        }
    }
}

/// Serializable physical connection and multiplex resource settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DaemonTransportConfig {
    /// Maximum complete encoded frame bytes after negotiation.
    pub max_frame_bytes: u32,
    /// Maximum active logical streams on one physical connection.
    pub max_logical_streams: u32,
    /// Maximum pending finite requests on one physical connection.
    pub max_pending_requests: u32,
    /// Maximum total queued outbound bytes on one connection.
    pub connection_queue_bytes: u64,
    /// Maximum queued outbound bytes for one logical stream.
    pub stream_queue_bytes: u64,
    /// Maximum retained outbound frames on one connection.
    pub connection_queue_messages: usize,
    /// Maximum retained outbound frames for one logical stream.
    pub stream_queue_messages: usize,
    /// Retained control-class frame reservation.
    pub control_queue_messages: usize,
    /// Retained lifecycle-class frame reservation.
    pub lifecycle_queue_messages: usize,
    /// Retained state-class frame reservation.
    pub state_queue_messages: usize,
    /// Retained bulk-class frame reservation.
    pub bulk_queue_messages: usize,
    /// Initial data credit for one logical stream.
    pub initial_stream_window_bytes: u64,
    /// Server heartbeat emission interval in milliseconds.
    pub heartbeat_interval_millis: u64,
    /// Dead-peer timeout in milliseconds.
    pub heartbeat_timeout_millis: u64,
    /// Reconnect replay retention after disconnect in milliseconds.
    pub resume_retention_millis: u64,
    /// Maximum complete pre-negotiation handshake frame bytes.
    pub handshake_frame_bytes: u32,
    /// Complete handshake exchange deadline in milliseconds.
    pub handshake_timeout_millis: u64,
    /// Complete physical frame write deadline in milliseconds.
    pub write_timeout_millis: u64,
    /// Maximum active local physical connections.
    pub maximum_connections: usize,
    /// Maximum connection failures retained by one server run report.
    pub maximum_retained_issues: usize,
    /// Maximum replay journals retained for one connection epoch.
    pub replay_streams: usize,
    /// Maximum reconnect replay bytes for one logical stream.
    pub replay_bytes_per_stream: u64,
    /// Maximum reconnect replay bytes for one physical connection.
    pub replay_bytes_per_connection: u64,
    /// Maximum one replay item bytes.
    pub replay_item_bytes: u64,
    /// Maximum active-connection diagnostic rows per page.
    pub connection_diagnostic_page_size: u32,
    /// Maximum logical-stream diagnostic rows per page.
    pub stream_diagnostic_page_size: u32,
}

impl Default for DaemonTransportConfig {
    fn default() -> Self {
        Self {
            max_frame_bytes: 33_554_432,
            max_logical_streams: 1_024,
            max_pending_requests: 256,
            connection_queue_bytes: 134_217_728,
            stream_queue_bytes: 33_554_432,
            connection_queue_messages: 16_384,
            stream_queue_messages: 1_024,
            control_queue_messages: 2_048,
            lifecycle_queue_messages: 4_096,
            state_queue_messages: 4_096,
            bulk_queue_messages: 6_144,
            initial_stream_window_bytes: 262_144,
            heartbeat_interval_millis: 5_000,
            heartbeat_timeout_millis: 15_000,
            resume_retention_millis: 30_000,
            handshake_frame_bytes: 65_536,
            handshake_timeout_millis: 5_000,
            write_timeout_millis: 5_000,
            maximum_connections: 128,
            maximum_retained_issues: 128,
            replay_streams: 1_024,
            replay_bytes_per_stream: 33_554_432,
            replay_bytes_per_connection: 67_108_864,
            replay_item_bytes: 33_554_432,
            connection_diagnostic_page_size: 128,
            stream_diagnostic_page_size: 256,
        }
    }
}

/// Serializable product request and daemon control-plane settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DaemonRequestConfig {
    /// Maximum JSON bytes in one finite product request.
    pub request_bytes: u32,
    /// Maximum JSON bytes in one finite product response.
    pub response_bytes: u32,
    /// Maximum JSON bytes in one product stream specification.
    pub stream_spec_bytes: u32,
    /// Maximum JSON bytes in one non-terminal product stream item.
    pub stream_item_bytes: u32,
    /// Maximum raw terminal output bytes read into one stream item.
    pub terminal_stream_bytes: usize,
    /// Product execution worker count.
    pub dispatcher_workers: usize,
    /// Maximum product actions waiting for an execution worker.
    pub dispatcher_queue_capacity: usize,
    /// Completed mutation idempotency retention in milliseconds.
    pub idempotency_retention_millis: u64,
    /// Maximum pending and completed mutation idempotency records.
    pub idempotency_records: usize,
    /// Maximum total idempotency request and result bytes.
    pub idempotency_total_bytes: u64,
    /// Maximum logical clients retaining session or profile cursors.
    pub cursor_clients: usize,
    /// Maximum session or profile cursors retained per logical client.
    pub cursors_per_client: usize,
    /// Maximum logical clients retaining terminal screen checkpoints.
    pub screen_checkpoint_clients: usize,
    /// Maximum terminal screen checkpoints retained per logical client.
    pub screen_checkpoints_per_client: usize,
    /// Maximum private terminal screen bytes retained per logical client.
    pub screen_checkpoint_bytes_per_client: usize,
    /// Maximum active product streams across every physical connection.
    pub product_streams: usize,
    /// Maximum logical clients retaining diagnostic cursors.
    pub diagnostic_clients: usize,
    /// Maximum diagnostic cursors retained per logical client.
    pub diagnostic_cursors_per_client: usize,
    /// Maximum operational diagnostic rows returned in one page.
    pub diagnostic_page_size: usize,
    /// Maximum rows retained in one point-in-time diagnostic snapshot.
    pub diagnostic_snapshot_rows: usize,
    /// Maximum accepted graceful shutdown drain timeout in milliseconds.
    pub maximum_shutdown_drain_millis: u64,
}

impl Default for DaemonRequestConfig {
    fn default() -> Self {
        Self {
            // Fit worst-case JSON escaping of the default input allowance while
            // leaving the wire envelope inside a 32 MiB frame. See the
            // [input-size decision](../../../docs/architecture/decisions/0004-input-size-limits.md#decision).
            request_bytes: 33_552_594,
            // Finite output and screen responses serialize bytes as JSON integer
            // arrays (up to four encoded bytes per raw byte). Cover a 1 MiB
            // retained-output page plus its metadata, not just a short viewport.
            response_bytes: 5 * 1_024 * 1_024,
            stream_spec_bytes: 65_536,
            stream_item_bytes: 131_072,
            // Attach sends a complete screen as its initial terminal item.
            // This must admit the default 128 KiB screen including history.
            terminal_stream_bytes: 131_072,
            dispatcher_workers: 8,
            dispatcher_queue_capacity: 1_024,
            idempotency_retention_millis: 300_000,
            idempotency_records: 1_024,
            idempotency_total_bytes: 268_435_456,
            cursor_clients: 256,
            cursors_per_client: 16,
            screen_checkpoint_clients: 256,
            screen_checkpoints_per_client: 8,
            screen_checkpoint_bytes_per_client: 1_048_576,
            product_streams: 8_192,
            diagnostic_clients: 256,
            diagnostic_cursors_per_client: 8,
            diagnostic_page_size: 128,
            diagnostic_snapshot_rows: 4_096,
            maximum_shutdown_drain_millis: 30_000,
        }
    }
}

/// Serializable domain, application-registry, and observation limits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DaemonServiceConfig {
    /// Maximum command arguments excluding the program.
    pub max_command_arguments: usize,
    /// Maximum total program and argument bytes.
    pub max_command_bytes: usize,
    /// Maximum explicit environment entry count.
    pub max_environment_entries: usize,
    /// Maximum explicit environment key and value bytes.
    pub max_environment_bytes: usize,
    /// Maximum path-reference bytes.
    pub max_path_bytes: usize,
    /// Maximum configured health rule count.
    pub max_health_rules: usize,
    /// Maximum bytes in one textual health rule.
    pub max_health_rule_bytes: usize,
    /// Maximum terminal columns admitted from a client.
    pub max_terminal_columns: u16,
    /// Maximum terminal rows admitted from a client.
    pub max_terminal_rows: u16,
    /// Maximum typed items in one atomic terminal input request.
    pub max_input_items: usize,
    /// Maximum estimated encoded terminal input bytes.
    pub max_input_bytes: usize,
    /// Maximum compact session rows in one prepared query page.
    pub max_query_page_size: usize,
    /// Maximum WorkContext declarations retained by one session.
    pub max_work_items: usize,
    /// Maximum session identities in one event filter.
    pub max_event_filter_sessions: usize,
    /// Maximum registered event kinds in one event filter.
    pub max_event_filter_kinds: usize,
    /// Maximum durable event records in one finite page.
    pub max_event_page_size: usize,
    /// Maximum compact detection profiles in one page.
    pub max_profile_page_size: usize,
    /// Maximum bytes in one profile match environment name.
    pub max_profile_environment_name_bytes: usize,
    /// Maximum bytes in one profile test screen view.
    pub max_profile_sample_bytes: usize,
    /// Maximum active prepared session query subscriptions.
    pub session_subscriptions: usize,
    /// Maximum pending scheduled restart jobs.
    pub restart_jobs: usize,
    /// Maximum pending scheduled observation jobs and session plans.
    pub observation_jobs: usize,
    /// Maximum retained semantic input delivery records.
    pub input_delivery_records: usize,
    /// Maximum retained published session events.
    pub session_event_records: usize,
    /// Maximum active selective session event subscriptions.
    pub session_event_subscriptions: usize,
    /// Maximum retained payload-safe diagnostic records.
    pub telemetry_diagnostic_records: usize,
    /// Maximum retained metric observations.
    pub telemetry_metric_records: usize,
    /// Maximum active detection profiles.
    pub detection_profiles: usize,
    /// Maximum pre-extracted screen views in one profile test.
    pub profile_test_samples: usize,
    /// Maximum evidence items in one foreground decision.
    pub foreground_evidence_items: usize,
    /// Maximum accepted foreground evidence age in milliseconds.
    pub foreground_evidence_age_millis: u64,
    /// Minimum accepted foreground evidence confidence in basis points.
    pub foreground_minimum_confidence_basis_points: u16,
    /// Confidence assigned to meaningful terminal motion in basis points.
    pub foreground_motion_confidence_basis_points: u16,
    /// Confidence assigned to quiet-server inference in basis points.
    pub foreground_server_idle_confidence_basis_points: u16,
    /// Maximum bytes in a detection profile description.
    pub profile_description_bytes: usize,
    /// Maximum executable basename wildcard count per profile.
    pub profile_binary_patterns: usize,
    /// Maximum argument regular expression count per profile.
    pub profile_argument_patterns: usize,
    /// Maximum required environment name count per profile.
    pub profile_environment_names: usize,
    /// Maximum bytes in one required environment name.
    pub profile_environment_name_bytes: usize,
    /// Maximum prompt-classification rules per profile.
    pub profile_foreground_rules: usize,
    /// Maximum background observation rules per profile.
    pub profile_background_rules: usize,
    /// Maximum expressions in one detection rule collection.
    pub profile_patterns_per_rule: usize,
    /// Maximum bytes in one detection expression.
    pub profile_pattern_bytes: usize,
    /// Maximum distinct compiled detection expressions retained.
    pub regex_cached_patterns: usize,
    /// Maximum approximate compiled regex bytes per expression.
    pub regex_compiled_size_bytes: usize,
    /// Maximum determinized automaton bytes per expression.
    pub regex_determinize_size_bytes: usize,
    /// Maximum private input bytes examined by one pattern match.
    pub regex_match_text_bytes: usize,
    /// Maximum structural trait needles in one collection.
    pub profile_trait_needles: usize,
    /// Maximum bytes in one structural trait needle or label.
    pub profile_trait_text_bytes: usize,
    /// Maximum profile-configured delay before a trailing submit key.
    pub profile_input_submission_delay_millis: u64,
}

impl Default for DaemonServiceConfig {
    fn default() -> Self {
        Self {
            max_command_arguments: 256,
            max_command_bytes: 65_536,
            max_environment_entries: 1_024,
            max_environment_bytes: 2_097_152,
            max_path_bytes: 4_096,
            max_health_rules: 16,
            max_health_rule_bytes: 8_192,
            max_terminal_columns: 500,
            max_terminal_rows: 500,
            max_input_items: 128,
            // This covers the reported routine bulk workload; it is configurable,
            // not a measured failure threshold. See the
            // [decision](../../../docs/architecture/decisions/0004-input-size-limits.md#context-and-evidence).
            max_input_bytes: 5_242_880,
            max_query_page_size: 256,
            max_work_items: 128,
            max_event_filter_sessions: 256,
            max_event_filter_kinds: 32,
            max_event_page_size: 256,
            max_profile_page_size: 256,
            max_profile_environment_name_bytes: 256,
            max_profile_sample_bytes: 16_384,
            session_subscriptions: 8_192,
            restart_jobs: 2_048,
            observation_jobs: 4_096,
            input_delivery_records: 65_536,
            session_event_records: 65_536,
            session_event_subscriptions: 8_192,
            telemetry_diagnostic_records: 65_536,
            telemetry_metric_records: 65_536,
            detection_profiles: 512,
            profile_test_samples: 8,
            foreground_evidence_items: 32,
            foreground_evidence_age_millis: 10_000,
            foreground_minimum_confidence_basis_points: 5_000,
            foreground_motion_confidence_basis_points: 8_500,
            foreground_server_idle_confidence_basis_points: 7_000,
            profile_description_bytes: 4_096,
            profile_binary_patterns: 32,
            profile_argument_patterns: 32,
            profile_environment_names: 32,
            profile_environment_name_bytes: 256,
            profile_foreground_rules: 64,
            profile_background_rules: 64,
            profile_patterns_per_rule: 32,
            profile_pattern_bytes: 4_096,
            regex_cached_patterns: 4_096,
            regex_compiled_size_bytes: 1_048_576,
            regex_determinize_size_bytes: 2_097_152,
            regex_match_text_bytes: 131_072,
            profile_trait_needles: 64,
            profile_trait_text_bytes: 1_024,
            profile_input_submission_delay_millis: 2_000,
        }
    }
}

/// Saved defaults for native CLI invocations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CliPreferences {
    /// Default command deadline, overridden by --timeout.
    pub command_timeout_millis: u64,
    /// Deadline for starting and connecting to the daemon.
    pub daemon_startup_timeout_millis: u64,
    /// Default page size for ls, top and watch.
    pub list_page_size: u32,
    /// Default finite log read size.
    pub logs_max_bytes: u32,
}
impl Default for CliPreferences {
    fn default() -> Self {
        Self {
            command_timeout_millis: 30_000,
            daemon_startup_timeout_millis: 10_000,
            list_page_size: 100,
            logs_max_bytes: 65_536,
        }
    }
}

/// Keyboard preferences shared by direct attachments and groups.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ShortcutPreferences {
    /// Prefix followed by a local action key.
    pub prefix: String,
}
impl Default for ShortcutPreferences {
    fn default() -> Self {
        Self {
            prefix: "alt-u".into(),
        }
    }
}

/// Bounded session and attached-view history, independent of raw-output retention.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ScrollbackConfig {
    /// Maximum retained normal-screen rows; zero disables history.
    pub memory_lines: usize,
}
impl Default for ScrollbackConfig {
    fn default() -> Self {
        Self {
            memory_lines: 2_000,
        }
    }
}

/// Maximum history cells per renderer, before its frozen history copy.
/// The limit is checked against the configured maximum terminal width.
pub const MAX_SCROLLBACK_CELLS: usize = 5_000_000;
