# Reconcile CLI operations before retrying

Use this reference for CLI recovery; MCP mode uses its own
[control reference](mcp-control.md). Recovery does not change the selected interface.

Preserve the exact name, stable ID when known, selected home, command response,
and stderr. For setup failures, use `uc config validate` and
`uc daemon status --json` first. Ordinary session reads can auto-start a daemon.

Machine-mode failures go to stderr. Inspect `result_category`,
`product_error_category` when present, `error_disposition`, and `effect_certainty`.
A numeric exit code alone does not establish whether a mutation happened.

| Uncertain operation | Next evidence |
| --- | --- |
| Start | Inspect the original unique name and run labels; recover its ID and configuration before considering another start. |
| Send | Inspect screen, retained output, and an application acknowledgment. PTY echo is insufficient. |
| Exec | Check the command's external effects or expected artifacts; re-execution can duplicate them. |
| Stop | Inspect the same session and restart intent; a timeout does not cancel an accepted stop. |
| Wait | Inspect current state, then wait again if appropriate. The worker remains alive. |

An empty bounded list, connection failure, or missing response is not universal
proof that a start had no effect. Preserve unresolved state and report the
uncertainty if evidence cannot determine the outcome. Do not reset the home,
remove locks, or delete database/host evidence to make a retry possible.

`send --delivery-id ID` supplies a semantic input identity. Retain that identity
with the exact payload if used. It does not make arbitrary CLI commands or an
unknown delivery safe to repeat; reconcile application evidence first.

If names have changed, query `uc ls --id SESSION_ID --json`, recover the current
name, and check `uc inspect NAME --json`. A different `summary.id` means a
different session; do not deliver follow-up work or cleanup to that replacement.

For classification problems, read `uc explain NAME --json`, inspect the current
terminal with `uc peek NAME`, and check the selected profile. Unknown execution
or health is missing evidence, not proof of failure. Altering a detection profile
is a separate configuration task; do not fabricate a completion classification.

Preserve retained output before cleaning terminal records. Leave shared daemons
and unrelated sessions alone. If cleanup is uncertain, report the exact names,
IDs, and home that still need inspection.
