# Attach to terminals and arrange panes

Use this guide to interact with a running session, arrange temporary panes, or
[save a group of shells](groups.md) to reopen later. It describes package 0.1.0-beta. Install the
three native binaries using [the installation guide](../../getting-started/installation.md), and run
commands in a terminal with both stdin and stdout attached. Interactive attach
and group views require text output; use [logs and watch](../../reference/cli-commands.md) for pipes
and automation. Commands below run from your project directory and use your
selected `UNDERCURRENT_HOME`.

## Open and leave a session

1. Start an interactive shell and open it:

   ```sh
   uc start dev --interactive --attach
   ```

   The status row identifies the managed session and shows the detach shortcut.
2. Press **Alt-U**, release it, then press lowercase **d** (the default keys).
   Your caller's terminal returns; the shell continues running.
3. Run `uc inspect dev` to verify that the session remains, then `uc attach dev`
   to reopen it.

### Select a session or read-only view

`uc attach` without a name opens a session picker. Use Up/Down or `k`/`j` to
move and Enter to select. Use `h` for the previous catalog page, `l` or `n`
for the next, `r` to refresh from the first page, and `q` or Escape to leave.
Refreshing clears the previous-page history. Letter shortcuts are lowercase
keystrokes. A failed selection retains the picker so you can refresh or choose
another session.

These bindings also work in the `uc stop` picker and the session/split pickers
inside an attachment. Vim keys apply only while choosing; they remain ordinary
input in a running terminal or a group name field. Enter confirms the selected
action, and `q` only closes or cancels the view.

`uc attach dev --read-only` prevents forwarding typed input, pastes, mouse
input, and terminal-query replies. It **still changes the session's terminal
geometry** to fit the viewport, including on resize and pane rearrangement.
It is a viewing mode, not an authorization restriction or a size-neutral observer.

### Recover output and recognize session exit

Normal attach starts with a current screen checkpoint. `--from-offset BYTE_OFFSET`
starts replay at a retained byte offset instead. The shared stream may recover
connections, but evicted output cannot be recreated. On an output gap, attach
reads the authoritative current screen for the same session ID. This restores
the viewport and resets local history; it does not recover the missing bytes.
See [output retention and gaps](../../reference/cli-output.md) and the
[attach recovery diagram](../../../crates/undercurrent-cli/docs/internals/terminal-and-streams.md#recover-the-view-from-the-right-evidence).

Opaque output capture has no VT screen projection. `start --attach` selects
replay from offset 0 for an opaque session; when reopening one, select a retained
`--from-offset` explicitly rather than relying on a screen seed. Screen-based
gap recovery can fail when no screen is available. Use `logs` to inspect raw
retained output and its explicit gaps.

Attach watches both output and lifecycle. It remains attached across a scheduled
automatic restart. A stopped, exited, or failed session ends its pane only when
no restart is scheduled; it drains queued and retained final output first.
Attach's successful exit reports that the view ended, not the child's exit code.
Use `uc inspect dev --json` to examine lifecycle and exit status.

### Stop and clean up the demonstration

When finished with the actual session:

```sh
uc stop dev
uc clean dev --yes
```

If the stop result is uncertain, inspect the session before cleaning or repeating
the mutation. Detaching, closing a pane, and cancelling a subscription do not
send a stop request.

## Use shortcuts, panes, history, and copy

The configured `shortcuts.prefix` defaults to `alt-u`. The following keys come
after the prefix. Press the prefix twice to send one literal prefix to the child;
Escape cancels a pending prefix. Unknown prefix combinations pass through in
order. The local matcher buffers partial input without a timer.

With the default prefix, plain Ctrl-U passes through to the attached application.
Press Alt-U twice to send a literal Alt-U. Existing explicit prefixes remain in effect; to retain
the former Ctrl-B leader, set `shortcuts.prefix` to `ctrl-b` in your main YAML
configuration (or `prefix = "ctrl-b"` under `[shortcuts]` in TOML). See
[configuration](../configuration/README.md) for loading and precedence.

| Following key | Direct attachment | Group view |
| --- | --- | --- |
| `d` | Detach all panes | Detach the group |
| `s` | Pick a session for the focused pane | Pick a saved group member |
| `?` | Show shortcut help | Show shortcut help |
| `[` / `]` | Enter copy mode / paste the local buffer | Same |
| `%` / `"` | Split into columns / rows | No group action |
| `o` / `z` / `x` | Focus next pane / zoom / close pane | No group action |
| `c` / `n` / `p` | No direct-attach action | Create shell / next member / previous member |

### Split and focus panes

To split, open the picker with prefix then `%` or `"`, select an existing session,
and press Enter. A session already visible in a pane cannot be selected twice.
Inside the split picker, `c` creates a new managed interactive shell named
`pane-<UUID>` using the CLI's current directory and default shell. That shell
survives closing its pane. Read-only mode disables this creation shortcut.

Layouts have at most 16 panes and require enough space for every pane. Shrinking
the terminal below that size shows the focused pane alone until space is
available.

Clicking another pane focuses it; that focus click is consumed
locally. Subsequent mouse events use the selected pane's coordinates. Pane
layouts and copy buffers belong to this CLI process and are not saved groups.

### Read history and copy text

Use Shift-PageUp/Shift-PageDown for local history and Shift-End or Escape to
return to live output. Typing normally leaves history and forwards the input
unless read-only.

Mouse wheel scrolling uses local history when the child is in
its primary screen without mouse capture; applications with mouse capture or an
alternate screen normally receive their own scrolling.

Normal-screen history is retained with the managed session and restored when
you attach again. `terminal.scrollback.memory_lines` bounds the rows kept by
each session and attached renderer (default 2,000; zero disables it). Screen
snapshots include the newest history that fits the snapshot byte budget;
older rows may be unavailable. This is separate from retained raw output.

Full-screen applications using the alternate screen own their transcript
scrolling. In the normal attached view, the wheel reaches the application when
it enables mouse input. Scroll to the desired text there before entering copy
mode; the frozen alternate-screen view contains only the displayed screen.

Enter copy mode with prefix then `[`. Its controls are:

| Action | Key or gesture |
| --- | --- |
| Move | Arrows or `h/j/k/l`, PageUp/PageDown, `g/G`, and `0/$` |
| Select characters | `v` or Space |
| Select lines | Uppercase `V` |
| Copy selection | `y` or Enter; a mouse drag selects and copies on release |
| Cancel | Escape or `q` |

Dragging also starts copy mode directly when the application has not enabled
mouse input, or when browsing local history. A click alone does not enter copy
mode. For applications that own the mouse, use prefix then `[` explicitly.

The copy view freezes its cells while incoming output continues updating the
live screen. The wheel scrolls the frozen viewport immediately. Copying leaves
the view open so you can select another passage; Escape returns to live output.
Copied text enters a process-local buffer
and is sent to the outer terminal using OSC 52; system clipboard support depends
on your terminal.

Prefix then `]` pastes the local buffer into the focused
session, respecting the child's bracketed-paste mode.

### Override detach keys

Explicit `--detach-keys 'ctrl-a,d'` replaces prefix shortcuts with a detach-only
sequence. A nonempty sequence must have 2–16 comma-separated keys: `ctrl-a`
through `ctrl-z`, `alt-a` through `alt-z`, or one ASCII literal each. An empty
`--detach-keys ''` disables local detach matching.

An overlapping mismatch
releases original input in order; bracketed paste is one input item and does
not trigger a key sequence inside its text.

## Create and reopen a saved group

Follow [saved-group creation and reopening](groups.md#create-and-reopen-a-saved-group)
for persistent shell recipes and existing-session membership.

## Understand restoration and pending operations

Use [group restoration and pending-operation recovery](groups.md#understand-restoration-and-pending-operations)
to distinguish a closed shell from an uncertain start or missing session.

## Delete a group or stop its members

Choose [forgetting a group or stopping its members](groups.md#delete-a-group-or-stop-its-members)
and verify cleanup there.

## Implementation and verification

Maintainer details, owning sources, and tests are in
[terminal and stream handling](../../../crates/undercurrent-cli/docs/internals/terminal-and-streams.md)
and [group recovery](../../../crates/undercurrent-cli/docs/internals/groups-and-recovery.md).
Continue with [shell integration](shell-integration.md) to capture live exports.
