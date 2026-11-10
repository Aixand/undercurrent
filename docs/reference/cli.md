# Look up CLI commands and output contracts

Use this reference for `uc` package 0.1.0-beta. The authoritative grammar is
[arguments.rs](../../crates/undercurrent-cli/src/arguments.rs); the
[command registry](../../crates/undercurrent-cli/src/registry.rs) and
[conformance fixture](../../tests/conformance/package/cli-commands.json)
define the 25 retained commands. Parser-generated help is available through
`uc COMMAND --help` without a daemon connection.

| Task | Commands | Detailed reference |
| --- | --- | --- |
| Session lifecycle | `start`, `stop`, `restart`, `clean` | [Selection, launch options, confirmation, bulk results](cli-commands.md#start-stop-restart-and-remove-sessions) |
| State and output | `ls`, `inspect`, `watch`, `top`, `logs`, `peek`, `explain` | [Queries](cli-commands.md#list-and-inspect), [observation commands](cli-commands.md#wait-and-observe) |
| Automation | `send`, `exec`, `wait` | [Input and execution](cli-commands.md#deliver-input-or-run-a-separate-command), [wait predicates](cli-commands.md#wait-and-observe) |
| Interactive workspace | `attach`, `group` | [Attachment, panes, history](../guides/cli/interactive.md), [saved groups](../guides/cli/groups.md) |
| Local configuration/shell | `context`, `init`, `config`, `completion`, `events` | [Local utilities](cli-commands.md#interactive-and-local-utilities), [shell hooks](../guides/cli/shell-integration.md) |
| Profiles and service administration | `profile`, `daemon`, `reset`, `debug` | [Profile/diagnostic options](cli-commands.md#profile-and-diagnostic-tools), [daemon lifecycle/reset](../guides/daemon/cli-lifecycle.md) |

## Before scripting

| Question | Authoritative reference |
| --- | --- |
| Which formats can I request? | [Output shapes, stdout, and stderr](cli-output.md#choose-the-matching-output-shape) |
| Which options conflict, and what does timeout cover? | [Global options and duration budgets](cli-commands.md#global-options-and-durations) |
| Can I capture all retained output? | [Finite pagination and follow limits](cli-output.md#read-retained-output-without-assuming-complete-history) |
| Does a failed mutation have effects? | [Exit categories](cli-output.md#exit-categories-and-machine-errors) and [reconciliation](cli-output.md#reconcile-a-failed-mutation-before-repeating-it) |
| Which accepted flags and cursors have limits? | [Cursor lifetime](cli-commands.md#cursor-lifetime-across-invocations), [watch/top](cli-commands.md#watch-and-top), [diagnostic bundles](cli-commands.md#diagnostic-sections-and-bundles) |
| Will group creation save restoration recipes? | [Group creation paths](../guides/cli/groups.md#create-and-reopen-a-saved-group) |

For tasks, use [session control](../guides/cli/sessions.md) or
[daemon operation](../guides/daemon/README.md). For reusable integration, choose the
[SDK](../sdk/README.md) instead of parsing human tables.

[Documentation index](../README.md)
