# Choose and configure a CLI command

Use this reference to select a `uc` command and understand what its options
actually request. It describes the native `uc` binary from package version
`0.1.0-beta`. Run commands on the daemon's machine, with the same OS user and
connection profile or `UNDERCURRENT_HOME` as the sessions you intend to manage. Examples below assume
an installed `uc` on `PATH`; session names such as `dev` must already exist
unless the example starts them.

The owning command grammar is [arguments.rs](../../crates/undercurrent-cli/src/arguments.rs), with the
complete 27-command set in [registry.rs](../../crates/undercurrent-cli/src/registry.rs). Typed requests
come from [preparation.rs](../../crates/undercurrent-cli/src/preparation.rs) and the
[product protocol](../../crates/undercurrent-protocol/src/dto.rs). `uc COMMAND --help`
is the local option lookup. See [output and errors](cli-output.md) before scripting.

Jump to [global options](#global-options-and-durations),
[session lifecycle](#start-stop-restart-and-remove-sessions),
[queries](#list-and-inspect), [input](#deliver-input-or-run-a-separate-command),
[exec](#run-a-separate-command-with-exec), [observation](#wait-and-observe),
[local utilities](#interactive-and-local-utilities), or
[profiles and diagnostics](#profile-and-diagnostic-tools).

## Global options and durations

| Option | Meaning |
| --- | --- |
| `--json` | One finite JSON result on stdout. |
| `--jsonl` | Continuing JSON Lines records on stdout. Mutually exclusive with `--json`. |
| `-q`, `--quiet` | Suppress ordinary human presentation; preserve failures and explicit machine output. |
| `-v`, `--verbose` | Add operational fields to human session summaries. |
| `--diagnostic` | Verbose presentation plus best-effort payload-free diagnostic records on stderr. |
| `--timeout DURATION` | Finite request or stream-activation bound; default comes from `cli.command_timeout_millis` (30 seconds by default). |
| `--connection NAME` | Select one saved local connection for this invocation; overrides `UNDERCURRENT_HOME` and the saved selection. |
| `--help`, `--version` | Print parser help or the package version without connecting. |

Choose at most one of quiet, verbose, and diagnostic.

| Duration option | Grammar and local bounds |
| --- | --- |
| Common `--timeout` | One positive integer plus lowercase `ms`, `s`, `m`, or `h`, at most `24h`. Examples: `250ms`, `30s`, `2m`. Rejects `1.5s`, `1m30s`, and unitless `30`. |
| Execution policy (`--restart-delay`, `--max-restart-delay`, `--min-uptime`, `--stop-timeout`) and `--drain-timeout` | Also allow adjacent integer segments such as `1m30s`, with a 24-hour local ceiling. Restart delays and minimum uptime allow zero; stop and drain durations do not. The daemon can impose tighter bounds. |

Owners: [duration parser](../../crates/undercurrent-cli/src/duration.rs) and
[common timeout grammar](../../crates/undercurrent-cli/src/cli_support.rs).

### What the timeout covers

`--timeout` is not a universal wall-clock limit for the CLI process.

| Route | Use of the timeout |
| --- | --- |
| `stop` with an explicit selector, `clean`, `exec` | Shared deadline across lookup and operation |
| Interactive `stop` picker | Each catalog request and the selected stop get a fresh bound; choosing time is excluded |
| Stream activation | Shared deadline across target lookups and activation |
| `wait` | Includes waiting and resynchronization |
| Other finite commands | May give each lookup and request the full bound |
| Configuration, stdin, daemon startup, active continuing streams | Separate lifetimes; see [startup budgets](../guides/daemon/cli-lifecycle.md#start-and-verify-readiness) |

There is no native `--config`, `--socket`, `--no-auto-daemon`, `gateway`, or
`stack` command/flag. Select local storage with `UNDERCURRENT_HOME` or a
[saved connection profile](../guides/cli/connections.md). `uc settings show`,
`uc settings profiles`, and `uc settings set profile NAME` operate offline. The public
Rust exports of `undercurrent-cli` include adapter types that the binary does not expose
as options; they are not an additional supported SDK surface.

## Start, stop, restart, and remove sessions

| Command | Result and important selection rules |
| --- | --- |
| `uc start NAME -- PROGRAM ARG...` | Start a directly supervised program with its ordered arguments. The CLI does not interpret the command as a shell expression. |
| `uc start NAME --interactive` | Start a persistent shell. Cannot be combined with a command after `--`. |
| `uc stop` | Pick one session interactively, then stop its stable ID. Requires terminal stdin/stdout and text output. |
| `uc stop NAME` | Resolve an exact name, then stop that stable session ID. |
| `uc stop --all` | Select every session before performing bounded parallel stops. |
| `uc stop --label KEY=VALUE` | Select one exact label on the daemon, then stop the selected IDs. |
| `uc restart NAME` | Restart one exact named session using its saved session configuration. |
| `uc clean [NAME] [-y]` | Remove a confirmed session; without a name, select stopped, exited, and failed sessions only. `--all` is the same default selection. |

`stop` accepts at most one of a name, `--all`, or `--label`. Without a selector,
it opens the same paged picker as `attach`: Up/Down selects, Enter stops, lowercase
`n` advances a page, `r` refreshes, and Escape cancels. Ctrl-C exits 130 without
stopping a session while selecting. Rows show lifecycle and restart status;
terminal sessions remain selectable because stopping can cancel a pending restart.
Selection restores the terminal before sending the stop request. Time spent
choosing does not consume the stop request timeout. A stop failure exits with
the ordinary error; it does not automatically retry or reopen the picker.
Pipes and machine output require an explicit selector.

Stop and restart force termination by default, including the stop picker.
`--force` (`-f`) explicitly selects that default; `--graceful` allows the configured
stop signal and timeout before escalation. The two flags are mutually exclusive. `clean` accepts a name or `--all`, never both.
`clean` and `reset` prompt on terminal stdin in text mode; `y` or `yes`
(case-insensitive) confirms. In machine mode or with redirected stdin, pass
`--yes` (`-y`) explicitly. Confirmation does not authorize removing an active
session through `clean`; the daemon still checks removal preconditions.

For bulk operations, all selected IDs are collected before the first mutation.
Invalid/repeated cursors, duplicate IDs, a deadline, or exceeding the selection
capacity abort selection. Once mutations begin, successful changes remain even
if other sessions fail. JSON reports retain `selected`, `succeeded`, and
`failures`; an incomplete bulk report exits 3. Inspect failures before deciding
which individual actions remain necessary. A bulk command is not a transaction.

### Start options

Start options are grouped by what they configure:

| Options | Defaults and constraints |
| --- | --- |
| `--launch-mode native\|sealed` | Native by default. Sealed requests verified immutable execution; runtime/platform support is enforced by the daemon. |
| `-i`, `--interactive`; `--shell PATH` | Explicit shell overrides discovery. Unix uses nonempty `SHELL`, then `/bin/sh`; Windows prefers a valid absolute `pwsh.exe` on `PATH`, then system Windows PowerShell. `--shell` is used only for interactive starts. |
| `--cwd PATH`, `--env-file PATH` | Relative paths are joined to the CLI invocation directory. Default cwd is that directory. The daemon reads the environment file. |
| `-e`, `--env KEY=VALUE` | Repeatable explicit environment values. |
| `-l`, `--label KEY=VALUE`; `-M`, `--metadata KEY=VALUE` | Repeatable query-visible labels and private metadata. Nonempty keys are required; later occurrences of the same key replace earlier ones locally. Empty values are accepted by this parser. |
| `--restart never\|on-failure\|always` | Default `never`. |
| `--max-restarts`, `--restart-delay`, `--max-restart-delay`, `--backoff-basis-points`, `--min-uptime` | Defaults: `3`, `1s`, `30s`, `20000` (2×), `10s`. Active restart policies require a multiplier at least `10000` and maximum delay at least initial delay. These policy fields are ignored when restart is `never`. |
| `--profile NAME`, `--profile-mode auto\|fixed\|foreground`, `--no-detect` | Auto follows foreground processes for interactive shells and direct launches, including wrappers. `--profile` overrides the configured `default_profile`; either selected profile defaults to fixed binding. Use fixed mode to keep the launch-selected profile, or foreground mode to adapt even with an explicit base profile. Disabling detection forces fixed binding. Existing sessions retain their saved binding. |
| `--healthy-when RULE` | Repeatable typed health rules, described below. |
| `--stop-signal terminate\|interrupt\|hangup\|quit\|user1\|user2`, `--stop-timeout` | Interactive shells default to hangup; direct launches to terminate. Stop timeout defaults to `5s`. Platform support remains a runtime decision. |
| `--output-capture terminal\|opaque` | Terminal by default. Opaque retains PTY bytes without VT screen projection. |
| `--columns`, `--rows` | Initial terminal geometry, `80` × `24`; both must be positive. |
| `--rm` | Request automatic removal when the session finishes; do not rely on later inspection of an auto-removed session. |
| `--attach`, `--detach-keys KEYS` | Enter the interactive terminal after a successful start. Both stdin and stdout must be terminals; text output only. A later attach failure does not undo the start. |

### Health rules

Health-rule syntax is translated by [health_rule](../../crates/undercurrent-cli/src/preparation.rs).
These are declarations evaluated by the daemon, not commands run by `uc` itself.

| Rule | Meaning |
| --- | --- |
| `port:8080` or `tcp:8080` | Positive TCP port number. |
| `http:http://127.0.0.1:8080/health` | Nonempty HTTP endpoint string after the first `http:`. |
| `log-match:ready` | Require matching output. |
| `log-error:ERROR` | Reject matching output. |
| `error-color` or `error-color:24` | Red terminal cell threshold; default 12, explicit threshold must be positive. |
| `exit-command:program arg` or `exit:program arg` | Program plus whitespace-separated arguments; shell quoting inside the rule is not parsed. Use a script for complex commands. |

## List and inspect

`uc ls` (alias `uc list`) requests one server-filtered page. It does not fetch
the entire catalog and filter locally. The default page size is
`cli.list_page_size` (100). A missing total is not a zero total.

### Cursor lifetime across invocations

The parser accepts `--cursor`, but a cursor from one `uc` invocation cannot be
used to continue the query in a new invocation. The [CLI client factory](../../crates/undercurrent-cli/src/client.rs)
creates a process-specific client identity, and the daemon's
[cursor store](../../crates/undercurrent-daemon/src/cursor.rs) scopes session and
profile cursors to that identity. Connection cleanup releases those cursors.
Diagnostic cursors have the same practical CLI limitation.

Use narrower filters or a larger permitted page size for a single CLI read.
For complete multi-page traversal, keep one [SDK client](../sdk/README.md) open,
retain the query parameters, and follow its cursors. A CLI page is not proof
that the whole catalog was read.

### List filters and order

| Option | Accepted selection |
| --- | --- |
| `--id ID` | Repeatable stable session IDs. |
| `--name NAME`, `--name-contains TEXT` | Exact unique name or case-sensitive name substring. |
| `--lifecycle` | Repeatable `starting`, `running`, `stopping`, `stopped`, `exited`, `failed`. |
| `--execution` | Repeatable `active`, `quiescent`, `unknown`. |
| `--interaction` | Repeatable `none`, `available`, `requested`. |
| `--health` | Repeatable `healthy`, `unhealthy`, `unknown`, `no-check`. |
| `--profile NAME`, `--label KEY=VALUE` | Exact profile or repeatable required labels. |
| `--has-active-work true\|false` | Presence of active WorkContext. |
| `--sort` | `name` (default), `lifecycle`, `execution`, `interaction`, `health`, `updated-at`, `restart-count`. |
| `--descending` | Reverse the server sort. |
| `--projection` | `compact` (default), `with-labels`, `operational`. |
| `--page-size N`, `--cursor CURSOR`, `--total` | Positive bounded page size, opaque continuation, optional exact total request. |

For example, `uc ls --label project=demo --lifecycle running --json` returns a
page whose `items` contain matching summaries. An empty selection succeeds with
an empty array (human output: `no sessions`).

### Inspect and explain detail

`uc inspect NAME` requests all authorized detail sections: configuration,
WorkContext, process, foreground evidence, and health results. Hidden legacy
projection flags are accepted but do not narrow that request. `uc explain NAME`
requests WorkContext, foreground evidence, and health results, leaving out
configuration and process detail. Text uses the same detail renderer for both.

Lifecycle, foreground execution, interaction, health, and WorkContext are
different observations. `quiescent` does not mean that application work
succeeded, and `requested` denotes observed demand for input rather than a
completed result. Text's `LINKED` column counts active/total work targets.
`--verbose` adds IDs, revisions, restart count, projected labels, profile
selection origins, profile epoch, current directory, and server-role evidence.

## Deliver input or run a separate command

`uc send NAME "echo hello"` builds one ordered input request: body, explicit
`--key` items in order, then Enter if the body is nonempty. `--no-newline`
suppresses that final Enter. Key-only input adds no implicit Enter. Without a
body or keys, `send` reads bounded UTF-8 stdin; empty input, invalid UTF-8, or
exceeding `requests.request_bytes` fails. Additional daemon input bounds apply.

| Option | Effect |
| --- | --- |
| `--paste` | Send a typed paste body. |
| `--raw` | Send literal text instead of the profile-selected text mode; mutually exclusive with paste. This still accepts text, not arbitrary binary stdin. |
| `--key KEY` | Repeatable named key/chord. |
| `--delivery-id ID` | Supply a semantic delivery identity to the daemon's input contract. It does not make every CLI mutation retryable. |
| `--preserve-observation` | Preserve the foreground observation after delivery instead of the default force-active policy. |
| `--eof` | Distinct idempotent close-standard-input request, ordered after previously accepted writes by the daemon. Exclusive with all body, key, and delivery controls; it never reads the CLI's stdin. |

A PTY cannot universally half-close standard input. The native adapter maps
`--eof` to terminal EOF behavior, which does not guarantee that every child exits.

Keys are case-insensitive: `enter`/`return`, `tab`, `backtab`/`back-tab`/`shift-tab`,
`escape`/`esc`, `backspace`, `delete`/`del`, `space`, arrows, `home`, `end`,
`pageup`/`page-up`, `pagedown`/`page-down`, and `f1` through `f24`.
`ctrl-` plus one character creates a control key. Arrow, home/end, page, and
function keys can combine `shift-`, `ctrl-`/`control-`, and `alt-`, once each.
Unsupported forms fail with the key's index without echoing its contents.

### Run a separate command with exec

`uc exec NAME -- PROGRAM ARG...` runs a separate daemon-owned command using the
session's observed live directory and captured root-shell exports. It does not
type into the existing program. `--dir-launch` and `--env-launch` independently
select saved launch settings. Missing required live context is a failure;
current shell integration is needed for captured exports.

The CLI accepts only text mode for `exec`. It validates both returned stdout
and stderr as complete inline payloads with matching SHA-256 digests before
printing either. Unavailable/truncated output exits 3; failed integrity exits
70. On success it writes each child channel to the corresponding CLI channel
and returns the child's exit code if representable in 0–255, otherwise 1.
This is a finite result, not progressive output while the child runs. Quiet
does not suppress this explicitly requested child output.

## Wait and observe

| Command | Scope and result |
| --- | --- |
| `uc wait NAME --lifecycle exited --timeout 30s` | Return the first summary satisfying the condition, or fail on deadline/removal. Supports finite `--json`. |
| `uc watch [NAME...] --jsonl` | One bounded watched query. Names are resolved once and deduplicated; all selected IDs must fit in `--page-size`. Without names, watch the first page. |
| `uc top` | The same bounded query model with total count requested; interactive dashboard on terminal stdin/stdout, streaming records when redirected or using `--jsonl`. |
| `uc logs NAME` | One retained-output page from byte offset 0, bounded by `cli.logs_max_bytes` (65,536 raw bytes by default). Use `--max-bytes 32768` with standard response limits; see [encoded limits](cli-output.md#read-retained-output-without-assuming-complete-history). |
| `uc logs NAME --follow` | Emit one finite page, then stream at its producer-head `next_offset` for the same ID. A short seed can skip retained bytes; see [output limits](cli-output.md#read-retained-output-without-assuming-complete-history). |
| `uc peek NAME` | Render the current VT screen as plain text. |

### Wait predicates

Wait accepts any of `--execution`, `--interaction`, `--lifecycle`, and `--health`
from the value table above. At least one is required. Alternatives within a
field are ORed; selected fields are ANDed. There is no stability window: a
matching initial snapshot can complete immediately. A gap triggers an exact-ID
read within the same deadline. A removed session fails even if a new session
later reuses its name. Subscription closure does not stop the process.

### Watch and top

Watch and top accept `--page-size` and `--projection`. The accepted watch
`--work-context` and `--health` flags currently do not alter query or rendering;
the summary already carries those dimensions. Do not interpret a bounded
watched page as every session. In interactive top, `q` exits, Ctrl-C interrupts,
and Up/Down scroll; quiet suppresses the text dashboard.

### Logs and screen reads

Logs accepts `--from-offset N`, `--max-bytes N`, and `-f`/`--follow`. Byte offsets
are distinct from stream sequences. A retention gap is explicit; follow the
[retained-output procedure](../guides/cli/status-and-output.md#save-a-retained-transcript)
rather than treating the producer head as the end of the returned page. A finite
gap result can exit 0: inspect the result discriminator. `peek --raw` writes
ANSI screen bytes and rejects machine output. `peek --checkpoint CHECKPOINT`
requests a diff or replacement snapshot: its text output is ANSI update bytes
even without `--raw`. Checkpoints are scoped by the server; a later CLI
connection may need a replacement snapshot.

## Interactive and local utilities

`uc tui` (alias `uc workspace`) opens the interactive session catalog with live
refresh, attach/return, creation, group assignment, labels, and output previews.
It requires terminal stdin/stdout and text output. See the [workspace guide](../guides/cli/workspace.md)
for shortcuts, selection semantics, and recovery behavior.

`uc attach [NAME]` opens a terminal or a picker if no name is given. It requires
terminal stdin and stdout and text mode. `--read-only` suppresses forwarding
input; `--from-offset N` requests retained replay instead of the default screen
seed. `--detach-keys` overrides the configured prefix/detach sequence.
`uc group` manages saved local workspaces of ordinary daemon sessions.
See [attachment and pane shortcuts](../guides/cli/interactive.md) for read-only
limits, and [saved groups](../guides/cli/groups.md) for persistence and recovery.

| Command | Behavior |
| --- | --- |
| `uc context [--name]` | Read inherited managed-session context locally. `--name` is text-only. |
| `uc init SHELL [--prompt off\|simple\|session]` | Install shell integration; supported shells: `bash`, `zsh`, `fish`, `powershell` (alias `pwsh`). Prompt defaults to off. |
| `uc init SHELL --print` | Print integration text without editing files; text-only, preserved under quiet. |
| `uc init powershell --profile "$PROFILE"` | Explicit absolute startup file; `--profile` cannot accompany `--print`. |
| `uc completion SHELL` | Exact shell-completion document; text-only and preserved under quiet. Redirect or load it using the target shell's completion mechanism. |
| `uc config show` | Show redacted effective on-disk configuration, including defaults. This is not an inspection of the running daemon's loaded settings. |
| `uc config validate` | Validate that same configuration without launching a daemon. |
| `uc events` | Sorted CLI and SDK diagnostic-event vocabulary, not a live event subscription. |

Use [shell integration and context](../guides/cli/shell-integration.md) for installation and
verification, and [configuration and daemon operation](../guides/daemon/cli-lifecycle.md) for home
selection, lifecycle, and reset recovery.

## Profile and diagnostic tools

Profile commands use the daemon's configured profiles and may automatically
start an absent daemon. They do not start a test session.

| Command | Lookup/evaluation |
| --- | --- |
| `uc profile list [--page-size N] [--cursor CURSOR]` | One summary page, default 100; subject to [cursor lifetime](#cursor-lifetime-across-invocations). |
| `uc profile inspect NAME` | Profile contract, including input delivery policy; JSON includes full typed detail. |
| `uc profile match --executable PROGRAM [--arg ARG] [--env-name KEY]` | Match process facts; repeat arguments and environment *names*, not values. |
| `uc profile lint [NAME]` | Lint all profiles or one profile. Check `valid` and diagnostics: a returned invalid lint report is still a successfully executed CLI request. |
| `uc profile test NAME [--screen-text TEXT] [--last-line TEXT] [--cursor-not-at-end]` | Evaluate bounded sample text; defaults are empty samples with cursor at end. |

### Diagnostic sections and bundles

`uc debug collect [--output PATH]` writes a private support ZIP (default
`uc-support.zip`) and never overwrites an existing file or starts a daemon.
`--offline` skips live reads; collection still works with invalid configuration.
Use `--incident UC_REFERENCE` to select retained failure records,
`--session SESSION_ID` for typed session state, or `--profile NAME` for active-profile
counts. Terminal content and panic backtraces require explicit
`--include-terminal-content SESSION_ID` and `--include-backtraces` options. The terminal option conflicts with
`--offline`. Successful collection can be partial; inspect its manifest and follow
the [support-report procedure](../guides/daemon/diagnostics.md#collect-a-support-report)
before sharing it.

`uc debug SECTION` returns one bounded diagnostic page (default 100) with
`--page-size` and `--cursor`. Sections are `connections`, `streams`,
`slow-consumers`, `output-producers`, `supervisors`, `detection`, `persistence`,
`reconciliation`, and `configuration`. Bare `uc debug` lacks a section/action
and fails; it does not imply all sections. See [cursor lifetime](#cursor-lifetime-across-invocations)
before using a returned continuation.

`uc debug bundle --json` requests all diagnostic sections with terminal content
redacted by default. `--include-terminal-content SESSION_ID` explicitly adds one
bounded retained-output page; `--from-offset` and `--max-bytes` require that
selection. Despite the help metavariable `SESSION`, the implementation forwards
this value as a stable session ID without a name lookup. Obtain the ID from
`uc inspect NAME --json`. Text bundles encode included bytes as hex rather than
executing terminal controls; JSON includes the typed byte page. Bundle paging
uses `--page-size` and `--cursor`.

### Repair a durable Unknown attempt

`uc debug repair-attempt ATTEMPT_ID` asks the daemon to resolve a durable Unknown
operation attempt using its retained evidence. It supplies identity only, with
no caller assertion about effects. A successful result is `known_absent` (no
result digest) or `known_applied` (with its result digest), plus unresolved
readiness information; insufficient
evidence is a failure. It is not a generic way to rerun a lost CLI command.

### Daemon commands and reset

Debug commands, `daemon status`, `daemon stop`, `reset`, and local utilities
never automatically start the daemon. `uc daemon start` explicitly starts it;
`uc daemon restart` performs immediate replacement, while `uc daemon stop`
requests immediate shutdown and waits for confirmed ownership release. Both
preserve saved state. Add `--graceful [--drain-timeout DURATION]` to drain accepted
work and clients; the graceful drain defaults to `5s`. `uc reset [-y]` is destructive local
state removal, not routine session cleanup.

## Source and verification entry points

[Native routing](../../crates/undercurrent-cli/src/native.rs) owns output modes, stdin, confirmation, and
resource cleanup. [Finite execution](../../crates/undercurrent-cli/src/executor.rs),
[bulk execution](../../crates/undercurrent-cli/src/executor/batch.rs), [operation adapters](../../crates/undercurrent-cli/src/executor/operations.rs),
and [stream execution](../../crates/undercurrent-cli/src/streams.rs) own the request sequences.
Their adjacent unit tests cover parser/registry parity, input ordering, profile
binding, ID selection before mutations, partial reports, and wait predicates.
Use the [maintainer verification guide](../../crates/undercurrent-cli/docs/development/contributing.md)
to select and run checks for a change.

[Back to crate overview](../../crates/undercurrent-cli/README.md) · [Machine output and errors](cli-output.md)
