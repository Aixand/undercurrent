# Navigate by responsibility

Use this map to locate the CLI source and tests for the behavior you need to change.

| Area | Owning files and investigation questions |
| --- | --- |
| Entry and routing | [main](../../src/main.rs), [lib](../../src/lib.rs), [native](../../src/native.rs), [dispatch](../../src/dispatch.rs): which path owns IO, connection lifetime, and exit? |
| Grammar and policy | [arguments](../../src/arguments.rs), [registry](../../src/registry.rs), [help](../../src/help.rs): does the command's identity, mode, output shape, help, and auto-start policy agree? |
| Invocation values | [cli_support](../../src/cli_support.rs), [context](../../src/context.rs), [duration](../../src/duration.rs), [deadline](../../src/deadline.rs), [execution_limits](../../src/execution_limits.rs): is a value validated, bounded, and shared consistently? |
| Configuration and connection | [runtime_config](../../src/runtime_config.rs), [client](../../src/client.rs), [bootstrap_config](../../src/bootstrap_config.rs), [bootstrap_paths](../../src/bootstrap_paths.rs): use the shared config owner; the bootstrap modules are compatibility reexports, not another loader |
| Requests and bulk work | [preparation](../../src/preparation.rs), [executor](../../src/executor.rs), [operations](../../src/executor/operations.rs), [batch](../../src/executor/batch.rs): where is a name fixed to an ID, and where can a mutation begin? |
| Interactive selection | [shared picker](../../src/session_picker.rs), [stop](../../src/stop.rs): paged navigation, terminal restoration before stopping, and stable-ID selection |
| Persistent session workspace | [controller](../../src/tui/mod.rs), [model](../../src/tui/model.rs), [forms](../../src/tui/forms.rs), [view](../../src/tui/view.rs), [worker](../../src/tui/worker.rs): background snapshots, stable selection, modal actions, read-only previews, and attach/return under one terminal guard |
| Local commands | [local](../../src/local.rs), [session_context](../../src/session_context.rs), [shell_integration](../../src/shell_integration.rs), [hook documents](../../src/shell_hooks/): which paths must remain daemon-independent? |
| Subscription orchestration | [streams](../../src/streams.rs), [stream_dispatch](../../src/stream_dispatch.rs), [signals](../../src/signals.rs), [top](../../src/top.rs): activation budget, stream cancellation, recovery, and terminal cleanup |
| Direct interactive views | [attach](../../src/attach.rs), [workspace](../../src/attach/workspace.rs), [layout](../../src/attach/layout.rs), [picker](../../src/attach/picker.rs), [overlays](../../src/attach/shortcuts.rs), [detach](../../src/detach.rs), [shortcuts](../../src/shortcuts.rs), [interactive terminal](../../src/interactive_terminal.rs): focus, input ordering, geometry, and stream ownership |
| Saved groups | [command surface](../../src/group/mod.rs), [headless adapter](../../src/group/api.rs), [service](../../src/group/service.rs), [store](../../src/group/store.rs), [restoration](../../src/group/restoration.rs), [workspace](../../src/group/workspace.rs): local publication, pending effects, and reconciliation |
| Shared cell rendering | [renderer](../../src/group/renderer.rs), [selection](../../src/group/renderer/selection.rs): confined escapes, bounded history, frozen copy views, and explicit clipboard effects |
| Presentation | [facade](../../src/presentation/mod.rs), [operations](../../src/presentation/operations.rs), [session](../../src/presentation/session.rs), [stream](../../src/presentation/stream.rs), [terminal](../../src/presentation/terminal.rs), [utility](../../src/presentation/utility.rs): stable machine shape versus human formatting |
| Failures and observability | [error](../../src/error.rs), [diagnostics](../../src/diagnostics.rs): preserve categories and effect certainty without emitting nested payloads |
| Native lifecycle/reset | [daemon_lifecycle](../../src/daemon_lifecycle.rs), [process_launch](../../src/daemon_lifecycle/process_launch.rs), [Windows ownership](../../src/daemon_lifecycle/windows_instance.rs), [reset](../../src/reset.rs), [Windows reset](../../src/reset/windows.rs): authenticated readiness, object ownership, and partial filesystem effects |

Generated Rustdoc is the symbol inventory. Keep behavioral definitions in the
protocol/configuration owners and use this map for ownership rather than copying
every request or method signature into prose.

[CLI maintainer index](../README.md)
