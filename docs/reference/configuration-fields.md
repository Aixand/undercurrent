# Look up the shared configuration contract

Use this reference to interpret a configuration release 0.1.0-beta document or a shared validation
failure. It describes the current loader and its checks, not runtime health or
an installed daemon's active settings. For a complete task, follow
[configuration inspection](../guides/configuration/diagnose.md).

The authoritative inputs are [config.schema.json](../../schemas/config.schema.json)
for the shipped JSON Schema, [load.rs](../../crates/undercurrent-config/src/load.rs) for file/bootstrap policy,
[document.rs](../../crates/undercurrent-config/src/document.rs) for typed field meanings and defaults, and
[validate.rs](../../crates/undercurrent-config/src/validate.rs) for shared runtime constraints. The
[package manifest](../../undercurrent-package.json) declares root-level
YAML/TOML configuration and schema 0.1.0-beta. Generate
[Rust API documentation](../../crates/undercurrent-config/docs/development/contributing.md#verification) to browse field-level
definitions; this page explains how those fields work together.

## Find a rule

- [Bootstrap defaults and profile compatibility](#document-structure-and-defaults).
- [Product sections and receiving components](#sections-and-component-boundaries).
- [Supported formats and parser limits](#formats-and-parser-budgets).
- [Scalar values and exceptions](#scalar-constraints).
- [Related capacities](#relationships-between-settings): [process and output](#process-output-and-storage), [services](#service-capacities), [transport](#transport-and-replay), [clients](#client-and-request-capacities), and [frame overhead](#payload-envelopes).
- [Retired environment variables and document keys](#retired-inputs).

## Document structure and defaults

Configuration is one root mapping with bootstrap fields and product sections
beside each other. There is no `[products.undercurrent]` wrapper. All sections
are optional. Missing product sections and fields use their Rust `Default`
values; unknown fields are rejected, including in nested tables. Explicit
values override defaults rather than merging multiple files. Null is not a
way to reset a product field.

| Bootstrap field | Default, validation, and meaning |
| --- | --- |
| `schema_version` | Omitted means `"0.1.0-beta"`; an explicit value must be that exact string. Numeric versions are rejected. No automatic migration. |
| `default_profile` | Omitted means `None`; strings are accepted without a profile lookup. Default detection profile for `uc start` without `--profile`; not part of `settings`. |
| `telemetry` | Whole section omitted: `diagnostics_enabled = true`, `debug_enabled = false`. Preferences passed to owners; loading emits no telemetry. |
| Fields inside a present `telemetry` | Omitted fields use the same defaults as an omitted section: diagnostics enabled, debug disabled. Explicit values override them. |

For example, `telemetry: {debug_enabled: true}` enables debug logging while
retaining the default diagnostic capture. Set `diagnostics_enabled: false`
explicitly to disable that capture; section presence alone does not disable it.

The loader and schema accept `default_profile: null` as `None`. A string is
applied by `uc start` when `--profile` is absent; an explicit flag takes
precedence. The selected profile follows the normal start-time validation and
binding rules. The daemon and SDK do not inject this CLI preference into direct
API requests.

The bootstrap `ConfigPaths` and product `DaemonPathConfig` types are both
empty reserved types. The `paths` table in a loaded file remains in product
settings; it is not an independent path override for bootstrap configuration.

## Sections and component boundaries

Every product section below is defined and defaulted in
[document.rs](../../crates/undercurrent-config/src/document.rs). This table locates behavior rather than
duplicating the generated field inventory. Numeric counts describe configured
capacities, not preallocated memory or guaranteed throughput.

| Section and receiving owner | Settings and boundary |
| --- | --- |
| `cli`<br>[CLI adapter](../../crates/undercurrent-cli/src/runtime_config.rs) | Command/startup deadlines, default list page size, finite output read size. Explicit command flags can override preferences. |
| `shortcuts`<br>[CLI matcher](../../crates/undercurrent-cli/src/shortcuts.rs) | Local prefix followed by an attachment/group action key; default `alt-u`. Set `shortcuts.prefix` in the main configuration file; omitted values use the default. See [prefix loading](../../crates/undercurrent-config/docs/internals/loading.md#resolve-the-prefix). |
| `profiles`<br>Profile directory is home-derived | Empty reserved table. Editable profile contents belong to the daemon. |
| `paths` | Empty reserved table. Home-derived storage, with no document relocation keys. |
| `local` | Empty reserved table. Local endpoint placement belongs to binary/transport adapters, not a host/port setting. |
| `logging`<br>[Logging adapter](../../crates/undercurrent-daemon/src/structured_logging.rs) | Per-file bytes, active-plus-rotated file count, buffered complete lines. `undercurrent-config` does not create log files. |
| `persistence`<br>[Daemon SQLite projection](../../crates/undercurrent-daemon/src/config.rs) | Sessions, pending outbox records, record size, read batch, command queue, SQLite lock wait. Caps and a busy deadline do not constitute a transaction. |
| `process`<br>[Process projection](../../crates/undercurrent-daemon/src/config.rs) | Process/spawn capacity, queued input sequences/bytes, PTY read and screen sizes, merged environment/file bounds, worker stack, forced-termination confirmation deadline. Merged-environment limits are distinct from explicit-input limits in `services`. |
| `terminal`<br>[Daemon output projection](../../crates/undercurrent-daemon/src/config.rs) and [CLI renderer](../../crates/undercurrent-cli/src/group/renderer.rs) | Retained raw-output bytes per session/globally, chunk/append/replay sizes, output records/subscriptions, and bounded session and attachment scrollback rows. Screen history is separate from raw-output replay. |
| `workers`<br>[Worker projection](../../crates/undercurrent-daemon/src/config.rs) | Mutation coordination bounds; outbox/restart/observation/reconciliation batches; health-check pool, queue, timeout and private sample/detail limits; observation delays/intervals; polling and process-exit error backoff. Saved numbers neither create workers nor schedule jobs while loading. |
| `transport`<br>[Transport projection](../../crates/undercurrent-daemon/src/config_transport.rs) | Connection/stream/request admission, frame and queue bounds by class, flow credit, heartbeat/handshake/write timing, disconnect replay retention, replay budgets, diagnostic page sizes. Wire replay retention is separate from terminal output retention. |
| `requests`<br>[Request projection](../../crates/undercurrent-daemon/src/config_transport.rs) | Finite JSON and stream payload bounds; dispatcher pool/queue; idempotency record/byte/time retention; per-client cursors and private screen checkpoints; product stream count; diagnostic snapshots/pages; maximum shutdown drain. Retention limits alone do not define safe mutation retries. |
| `client`<br>[Client projection](../../crates/undercurrent-daemon/src/config_client.rs) | Dial/cancellation timing, writer/bulk/mailbox bounds, reconnect backoff/jitter/attempts, parallel resumes and response deadline. Native CLI connection uses a [compiled profile](../../crates/undercurrent-config/docs/internals/loading.md#client-configuration-boundary). |
| `services`<br>[Service projection](../../crates/undercurrent-daemon/src/config_services.rs) | Admission bounds for command/environment/path/health/terminal/input/work/query/event/profile data; registry capacities; foreground evidence age/confidence; profile rules, traits and input delay; regex cache, compilation and input limits. Validates application/domain bounds separately from process or transport limits. |

`terminal.scrollback.memory_lines` defaults to 2,000; zero disables
normal-screen history in session parsers and attached renderers. Snapshots
restore the newest rows that fit `process.screen_snapshot_bytes`, preserving
the complete live viewport. This does not disable output capture, increase reconnect
replay, or erase retained daemon output. `MAX_SCROLLBACK_CELLS` is 5,000,000 per
parser or renderer before its frozen history copy; the guard uses configured maximum
columns, not the current terminal width.

Durations with `_millis` are milliseconds. Byte limits count bytes, not
characters. Basis points use 10,000 for 1.0 (or 100% for confidence).
The `u16`, `u32`, `u64`, and `usize` types in the Rust document also constrain
values; `usize` bounds depend on target pointer width. The schema alone does
not encode all of those upper bounds.

## Formats and parser budgets

The main document must be a readable regular file containing a root mapping.
Metadata follows symlinks; the loader is not a symlink-containment or file
permission enforcement layer. Extension matching is case-sensitive.

| Input | Supported behavior |
| --- | --- |
| `.toml` | Decode into a TOML table, then JSON, then the shared typed document. Parser/normalization failures use fixed `Invalid` rules. |
| `.yaml`, `.yml` | Event pre-scan followed by `serde-saphyr` decoding into JSON. Both feed the same typed settings and defaults as TOML. |
| Other extensions, including `.json` | Rejected even if the content could be parsed. JSON-shaped content may be read through a `.yaml` file. |
| Main file byte limit | 1,048,576 (`MAX_CONFIG_BYTES`); read limit plus one detects overflow. |
| Separate `shortcuts.toml` | Not read. Set `shortcuts.prefix` in the main configuration file. |
| YAML documents | One; multiple-document streams are rejected. Use `{}` for an empty YAML mapping. |
| YAML nesting | At most 32 levels of mapping/sequence containers. |
| YAML parser work | At most 32,768 events and 16,384 decoded nodes; total scalar bytes at most the main-file limit. |
| YAML advanced forms | Anchors, aliases, explicit tags and merge keys are unsupported; duplicate mapping keys are errors. |
| YAML booleans and errors | Strict boolean decoding; no parser snippet included in loader errors. |

The event scanner enforces tags/anchors/aliases, document count, depth, and
event count before the second decoder runs. The decoder repeats relevant
budgets and adds node, scalar-byte, merge-key, and duplicate-key restrictions.
These controls bound work on potentially malformed documents; they do not
provide a wall-clock loading deadline.

## Scalar constraints

Runtime field names/types are checked against serialized defaults before typed
deserialization. Number fields must be unsigned integers: negatives, floats,
booleans and numeric strings fail. Object/string/boolean fields must have
their matching types. The final validator applies schema numeric minima and
maxima to default-expanded settings, then stronger component relationships.

Most numeric settings reference the schema's `positiveInteger` (at least 1).
The following exceptions or stronger scalar checks require attention:

| Setting | Effective shared constraint |
| --- | --- |
| `cli.command_timeout_millis`, `cli.daemon_startup_timeout_millis` | 1 through 86,400,000 inclusive |
| `cli.list_page_size`, `cli.logs_max_bytes` | 1 through `u32::MAX`, plus capacity relationships below |
| `terminal.scrollback.memory_lines` | 0 through 5,000,000; product with maximum columns must also fit the cell budget |
| `shortcuts.prefix` | After ASCII lowercasing: `alt-a` through `alt-z`, `ctrl-a` through `ctrl-z`, or one ASCII character; empty/multicharacter/non-ASCII values fail |
| `logging.max_file_bytes` | At least 4,096 |
| `process.worker_thread_stack_bytes` | At least 65,536 |
| `workers.health_check_detail_bytes` | At least 32 |
| `workers.foreground_activation_delay_millis`, `workers.health_activation_delay_millis` | Strictly positive even though schema permits zero |
| `client.reconnect_backoff_basis_points` | At least 10,000; Rust `u32` bound also applies |
| `client.reconnect_jitter_basis_points` | 1 through 10,000; schema permits zero, shared validation does not |
| `client.reconnect_maximum_attempts` | Strictly positive; schema permits zero, shared validation does not |
| `services.foreground_minimum_confidence_basis_points` | 0 through 10,000 |
| `services.foreground_motion_confidence_basis_points`, `services.foreground_server_idle_confidence_basis_points` | 1 through 10,000 and at least the minimum confidence |
| `services.foreground_evidence_items` | At least 4, covering runtime evidence sources |
| `services.profile_input_submission_delay_millis` | Strictly positive even though schema permits zero |
| `transport.handshake_frame_bytes` | At least 22 bytes for the handshake header |
| `services.max_work_items`, `services.max_query_page_size`, `terminal.replay_bytes` | At most `u32::MAX` in addition to their positive ranges |
| `persistence.max_sessions`, `max_outbox_events`, `max_record_bytes`, `max_read_batch` | At most `i64::MAX` on the current 64-bit targets, for SQLite representation; `usize` may be narrower |

The prefix check validates a lowercased temporary string; it does not rewrite
the stored value or apply a keyboard action.

## Relationships between settings

These are shared load-time rejection rules from [validate.rs](../../crates/undercurrent-config/src/validate.rs).
They ensure a producer's bounded unit can fit in its receiving component.
They do not guarantee availability when resources are already occupied, or
that all per-session maxima fit concurrently within a global limit.

### Process, output, and storage

| Capacity and purpose | Required relationship |
| --- | --- |
| **Screen history**<br>Bound cell allocation and reject overflow | `terminal.scrollback.memory_lines * services.max_terminal_columns <= 5,000,000`, with checked multiplication |
| **Process admission**<br>Every process can have a durable aggregate and output record | `process.max_processes <= persistence.max_sessions` and `<= terminal.sessions` |
| **Spawn/health workers**<br>Pools stay within process capacity | `process.spawn_workers <= process.max_processes`; `workers.health_check_workers <= process.max_processes` |
| **Output append**<br>A PTY read fits the append interface | `process.output_read_bytes <= terminal.append_bytes` |
| **Output chunks**<br>A single chunk fits both retention budgets | `terminal.chunk_bytes <= terminal.per_session_bytes` and `<= terminal.global_bytes` |
| **Finite and streaming reads**<br>One requested extraction fits the replay bound | `cli.logs_max_bytes <= terminal.replay_bytes`; `requests.terminal_stream_bytes <= terminal.replay_bytes` |
| **Storage batches**<br>A worker batch fits bounded repository reads | `workers.outbox_batch_size` and `workers.reconciliation_batch_size <= persistence.max_read_batch` |
| **Restart batches**<br>A claimed batch fits the job registry | `workers.restart_batch_size <= services.restart_jobs` |

### Service capacities

| Capacity and purpose | Required relationship |
| --- | --- |
| **Observation jobs**<br>Health and foreground work each need coverage | `services.observation_jobs >= process.max_processes.saturating_mul(2)` |
| **Event publication**<br>Retain an outbox batch | `services.session_event_records >= workers.outbox_batch_size` |
| **Profiles/pages**<br>Pages fit their owning capacities | `services.detection_profiles >= services.max_profile_page_size`; `cli.list_page_size <= services.max_query_page_size` |
| **Subscription coverage**<br>Each registry can cover a connection's stream bound | Each of `services.session_subscriptions`, `services.session_event_subscriptions`, `terminal.subscriptions >= transport.max_logical_streams` |
| **Pattern input**<br>A matcher can examine a maximum sample from every source | `services.regex_match_text_bytes >= workers.health_check_screen_text_bytes`, `>= services.max_profile_sample_bytes`, and `>= process.screen_snapshot_bytes` |

### Transport and replay

| Capacity and purpose | Required relationship |
| --- | --- |
| **Heartbeat**<br>Peer timeout exceeds one heartbeat interval | `transport.heartbeat_timeout_millis > transport.heartbeat_interval_millis` |
| **Outbound bytes**<br>Stream allowance and a frame fit the containing budgets | `transport.connection_queue_bytes >= 4 * transport.max_frame_bytes`; `transport.stream_queue_bytes <= transport.connection_queue_bytes`; `transport.max_frame_bytes <= transport.stream_queue_bytes`; `transport.initial_stream_window_bytes <= transport.stream_queue_bytes` |
| **Outbound messages**<br>Individual class and stream bounds fit the connection; this does **not** require the class limits' sum to fit | `transport.stream_queue_messages <= transport.connection_queue_messages`; each of the control/lifecycle/state/bulk queue message limits also fits the connection limit |
| **Replay bytes**<br>A complete frame fits an item; an item fits both journal budgets | `transport.replay_item_bytes <= transport.replay_bytes_per_stream <= transport.replay_bytes_per_connection`; `transport.replay_item_bytes >= transport.max_frame_bytes` |
| **Stream registries**<br>Replay and product stream registries cover a connection | `transport.replay_streams >= transport.max_logical_streams`; `requests.product_streams >= transport.max_logical_streams` |

### Client and request capacities

| Capacity and purpose | Required relationship |
| --- | --- |
| **Logical clients**<br>Client registries cover physical connection capacity | Each of `requests.cursor_clients`, `requests.screen_checkpoint_clients`, `requests.diagnostic_clients >= transport.maximum_connections` |
| **Screen checkpoints**<br>One full screen fits a client's checkpoint budget | `requests.screen_checkpoint_bytes_per_client >= process.screen_snapshot_bytes` |
| **Idempotency**<br>Retain one maximum request and result | `requests.idempotency_total_bytes >= requests.request_bytes + requests.response_bytes` |
| **Terminal input**<br>A complete estimated sequence fits its process queue and leaves JSON metadata space | `services.max_input_bytes <= process.input_queue_bytes_per_process`; `services.max_input_bytes < requests.request_bytes`. JSON escaping can require a larger request bound. See [input sizing](../guides/configuration/README.md#configure-terminal-input-size). |
| **Diagnostic snapshots**<br>A snapshot holds a complete diagnostic page | `requests.diagnostic_snapshot_rows >= requests.diagnostic_page_size` |
| **Dial cancellation**<br>Cancellation observation interval fits the dial deadline | `client.cancellation_interval_millis <= client.dial_timeout_millis` |
| **Backoff**<br>The initial delay fits the configured ceiling | `client.reconnect_initial_delay_millis <= client.reconnect_maximum_delay_millis` |
| **Bulk requests**<br>Parallel finite requests fit writer and pending-request bounds | `client.bulk_parallel_requests <= client.writer_queue_messages` and `<= transport.max_pending_requests` |
| **Mailboxes**<br>A logical mailbox's byte bound fits its stream queue | `client.stream_mailbox_bytes <= transport.stream_queue_bytes` |

### Payload envelopes

The shared validator reserves **1,838 bytes** for the UCMP 0.1.0-beta client envelope:
22 header bytes, seven length-prefixed identifiers each bounded to 256 bytes,
a 2-byte count, and a `u64`. It reserves a further **26 bytes** for a terminal
screen header. Each of `requests.request_bytes`, `response_bytes`,
`stream_spec_bytes`, and `stream_item_bytes`, plus 1,838, must fit
`transport.max_frame_bytes`.

For terminal payloads,
`max(process.screen_snapshot_bytes, requests.terminal_stream_bytes) + 1,838 + 26`
must fit that same frame bound; addition is saturating. The shared error for
this combined check names `process.screen_snapshot_bytes` even if the terminal
stream size caused it. The constants are repeated in the shared validator
to avoid a wire-crate dependency; [wire-envelope parity tests](../../crates/undercurrent-daemon/tests/config_validation_parity.rs)
bind representative limits to real codecs. A codec change must trigger review
of this validator and those tests.

## Retired inputs

Presence of any of these process variables causes `RetiredPath` before file
selection: `UNDERCURRENT_CONFIG`, `UNDERCURRENT_DATABASE_PATH`,
`UNDERCURRENT_INSTANCE_LOCK_PATH`, and `UNDERCURRENT_LOCAL_ENDPOINT`.
`ConfigDiscovery.undercurrent_config_env` is rejected too. Choose the complete
home with `UNDERCURRENT_HOME` instead.

Recognized retired document fields also produce `RetiredPath`:

| Table | Rejected keys |
| --- | --- |
| `paths` | `state_dir`, `logs_dir`, `cache_dir`, `database`, `instance_lock` |
| `profiles` | `directory` |
| `local` | `endpoint` |

Other keys in those reserved empty tables fail as unknown fields. Unknown root
tables, including the former `websocket` table and `products` wrapper, produce
`UnknownSetting`. Low-level explicit-file/path helper APIs do not reinstate
retired binary environment variables or document keys.

The [CLI adapter](../../crates/undercurrent-cli/src/runtime_config.rs) and
[daemon adapter](../../crates/undercurrent-daemon/src/config.rs) additionally reject
`UNDERCURRENT_ENDPOINT`, `UNDERCURRENT_TOKEN_FILE`, `UNDERCURRENT_TLS_CA_FILE`,
`UNDERCURRENT_TLS_CLIENT_CERT_FILE`, and `UNDERCURRENT_TLS_CLIENT_KEY_FILE`.
That separate policy is outside `load_config`; a shared-load success alone
does not prove that a binary can resolve its configuration.

See [typed errors and recovery](../guides/configuration/diagnose.md#diagnose-a-rejection)
and [extension guidance](../../crates/undercurrent-config/docs/development/contributing.md#change-a-setting-without-splitting-the-contract).
