# Manage sessions in the terminal workspace

Run `uc tui` (or its alias `uc workspace`) in a terminal to browse and manage the
sessions shown by `uc ls`. The workspace uses the selected connection, including
`--connection NAME` and `UNDERCURRENT_HOME`. Stdin and stdout must be terminals;
`--json` and `--jsonl` are rejected before connecting.

The list refreshes in the background about once a second. Each refresh reads one
complete paginated snapshot within the configured session limit and request
deadline. Selection follows stable session IDs when rows move. A failed refresh
marks the retained view stale and disables new actions until a refresh succeeds.
Read-only snapshots retry brief connection interruptions within that same
deadline; process mutations are never retried by the workspace.

## Navigate and attach

| Key | Action |
| --- | --- |
| Up/Down or `k`/`j` | Move through sessions or sidebar filters |
| Tab / Shift-Tab | Switch focus between sessions and sidebar |
| PageUp / PageDown | Move a screenful |
| Enter | Attach to the highlighted session; in the sidebar, return focus to sessions |
| Space | Mark or unmark a session |
| `u` | Clear all marks |
| `/` | Search names, profiles, IDs, and `key=value` labels; Enter applies |
| `1` / `2` / `3` | Details, screen preview, or recent logs |
| `[` / `]` | Scroll details or preview |
| `n` / `g` / `l` | New session, grouping, or label editor |
| `a` | Stop gracefully, force stop, or restart, with confirmation |
| `r` | Refresh the catalog |
| `e` | Expand the last operation result; arrows scroll |
| `?` | Help |
| `q` / Escape | Leave the workspace; Escape first closes an open form |

The normal configured detach shortcut returns from an attachment to the same
workspace selection, marks, search, and filter. Its default is **Alt-U, then d**.
Ordinary keys, including `q` and Ctrl-C, go to the child while attached. Outside
an attachment, Ctrl-C interrupts the workspace. Leaving a view never stops a
managed process. Final process exit returns to the workspace after final output
is drained; scheduled automatic restarts keep the attachment open.

Screen and log previews issue read requests only. They do not send input or
change terminal geometry. Logs show a bounded recent tail, up to 16 KiB, and may
omit older output. Screen preview requires terminal-mode output capture; logs
remain useful for opaque output. Resize the outer terminal to at least 33 columns
by 12 rows; wider terminals show the sidebar and more session columns.

## Start and organize sessions

Press `n` to choose an interactive shell or a command, name, executable,
directory, optional existing group, labels, environment, profile, and restart
policy. Tab or arrows move between fields and buttons; Space cycles Type and
Restart. Ctrl-U clears a field. Choose **Start** or **Start and attach**.

Command arguments support single or double quotes around values with spaces.
Arguments are passed directly to the executable: variables, pipes, redirects,
and command substitution are not expanded. Choose an explicit shell command if
you need shell interpretation. Labels and environment entries use `key=value`,
with quoted values where needed.

Space marks sessions for a group or label action. Marks outside the current
filter remain selected and are included in the displayed selection count. With
no marks, the highlighted session is the target. Dialogs capture exact session
IDs before a mutation, so a later list reorder or name reuse cannot retarget it.

`g` creates an empty group or a group from the selection, assigns sessions to an
existing group, or removes membership. Each session belongs to at most one
group. Opening a sidebar group only filters the catalog; it does not restore or
launch shells. Groups created here retain membership without enabling automatic
shell restoration. Custom launches made through this form are attachment-only
members, preserving their launch configuration; use `uc group new` for saved
shell recipes and restoration.

`l` patches labels: unmentioned labels remain unchanged, and a key cannot be set
and removed in the same action. The `uc.*` namespace is reserved for recovery.
Successful patches also update an existing saved shell's recipe. If the local
recipe update fails after the daemon commits the labels, the result reports that
partial outcome.

Group assignment and label changes report success per session; a batch is not
one transaction. Grouped starts save pending creation before sending a request.
Uncertain starts and other failed mutations are not automatically retried.
Inspect the result with `e` and the session list before repeating an action;
saved group pending operations use the existing [group recovery rules](groups.md).

[Attachment shortcuts](interactive.md) · [CLI reference](../../reference/cli-commands.md)
