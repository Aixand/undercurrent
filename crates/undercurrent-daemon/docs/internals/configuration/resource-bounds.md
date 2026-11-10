# Configure daemon resource bounds

This reference explains how the current daemon validates and uses configuration.
It applies to package 0.1.0-beta and configuration schema 0.1.0-beta. The exact
field/default contract is [undercurrent-config's document](../../../../undercurrent-config/src/document.rs)
and its [shared validator](../../../../undercurrent-config/src/validate.rs).
The daemon's [config.rs](../../../src/config.rs),
[config_transport.rs](../../../src/config_transport.rs),
[config_client.rs](../../../src/config_client.rs), and
[config_services.rs](../../../src/config_services.rs) project that document into
validated adapter settings. These are configuration contracts, not SDK options
that retroactively configure arbitrary connected clients.

## Home and input selection

Use the shared [home and active-file procedure](../../../../../docs/guides/configuration/README.md#select-the-home-and-active-file)
for installation selection, and the [configuration reference](../../../../../docs/reference/configuration-fields.md)
for parser budgets, defaults, retired inputs, and exact field constraints.
The `bootstrap_config` and `bootstrap_paths` modules re-export that shared
policy; they are not separate loaders.

Low-level composition tests and deployment fixtures can supply an explicit
configuration file and absolute database/instance-scope overrides. The shipped
binary exposes none of those overrides. Legacy type variants such as
`ConfigSource::Environment` and `RuntimePathSource::Project` do not mean the
binary accepts retired discovery inputs.

## Installation layout

Locations below are relative to the selected home. Runtime configuration
requires distinct absolute database, instance-scope, private-host, and log paths.

| Location | Owner and lifetime |
| --- | --- |
| `config.yaml`, `config.yml`, or `config.toml` | One operator-authored main configuration. Changes apply to the next CLI invocation or daemon restart. |
| `profiles/` | YAML profile catalog, separately loaded and watched by the daemon. |
| `state/undercurrent.db` and SQLite sidecars | Durable repository; preserve as part of a consistent state set. |
| `run/daemon.lock` | Instance *scope*. The advisory lock is held on `.daemon.lock.instance.lock` in its parent. Do not infer ownership from either filename's existence. |
| `run/control.sock` | Logical public local endpoint. The Windows local transport maps its address to a protected named pipe. |
| `run/session-host.sock` and `run/session-host.auth` | Private host endpoint and authentication artifact, never a public SDK attachment route. |
| `state/session-host/session-host.execution-jobs` and `.attempt-receipts` | Retained private-host evidence coupled to the database. |
| `state/execution-output/` | Bounded retained command-execution output, distinct from live terminal output. |
| `run/agent-bridge.sock` | Private authenticated agent bridge gateway. |
| `logs/daemon.jsonl` | Bounded structured daemon log and rotated siblings. |

On Unix, composition validates owner-private runtime directories and the
instance scope; the lock file must be a single-link regular file owned by the
effective UID with mode 0600. It opens the parent without following a symlink,
pins its descriptor, and verifies device/inode identity around acquisition.
Windows uses pinned handles, private ACL setup, and reparse-point rejection via
the [Windows adapter](../../../../undercurrent-platform-windows/src/filesystem.rs).
See [process_instance.rs](../../../src/process_instance.rs) and
[process_instance_windows.rs](../../../src/process_instance_windows.rs).

## Resource relationships

Each component validates its own nonzero counts, durations, and internal bounds.
The daemon additionally validates relationships because a valid producer bound
can still exceed a downstream consumer's capacity. Loading reads configuration
but does not open SQLite, start workers, spawn a host, or bind a listener.

The shared reference owns the individual relationships. Use its focused tables
when a valid local bound still exceeds a downstream capacity:

| Investigating | Authoritative rules |
| --- | --- |
| Process, output, storage or worker batches | [Process, output, and storage](../../../../../docs/reference/configuration-fields.md#process-output-and-storage) |
| Subscriptions, observations, event retention, profiles or regex input | [Service capacities](../../../../../docs/reference/configuration-fields.md#service-capacities) |
| Frames, replay, byte/message queues or stream registries | [Transport and replay](../../../../../docs/reference/configuration-fields.md#transport-and-replay) |
| Cursor/checkpoint clients, bulk requests, mailbox bytes or reconnect | [Client and request capacities](../../../../../docs/reference/configuration-fields.md#client-and-request-capacities) |
| Binary or product payload overhead | [Payload envelopes](../../../../../docs/reference/configuration-fields.md#payload-envelopes) |
| Logging minima, evidence count or confidence thresholds | [Scalar constraints](../../../../../docs/reference/configuration-fields.md#scalar-constraints) |

Validation is not a reservation of all OS memory or processes. Startup may still
fail to create threads, open files, or acquire ownership. Increasing producer
limits alone may turn into validation failures or pressure elsewhere. Use
diagnostic queue, byte, eviction, and drop counters to identify the resource
that is actually saturated.

The native SDK projection in `config_client.rs` constructs a hello from the
SDK's required-capability registry and server policy. Future optional server
capabilities do not automatically become client requirements. The private host
uses dial/cancellation limits from this projection, a second dial bound, and an
exchange bound equal to the maximum of handshake and write timeouts. These
UCHS settings do not make the private socket a public transport.

## Apply a change

Follow [edit, validate, inspect, and apply configuration](../../../../../docs/guides/configuration/README.md#edit-and-inspect).
That procedure includes expected output, restoring a rejected edit, and the
restart boundary. For the log-retention fields, use
[bounded structured logs](../../../../../docs/guides/daemon/diagnostics.md#interpret-logs-and-telemetry-correctly).

Maintainer evidence: [config validation parity](../../../tests/config_validation_parity.rs)
samples numeric field boundaries and exact wire-envelope edges; module tests
cover default retention, path privacy, unknown/retired fields, component limits,
and relationship failures. Add a setting to shared validation and the daemon
projection together, then expand parity tests around its actual boundary.

[Operating guide](../../../../../docs/guides/daemon/operations.md) · [Crate entry point](../../../README.md)

[Crate documentation](../../README.md)
