# Check the selected CLI installation

Use this reference in CLI mode or for a separately scoped setup task. A missing
MCP connection does not by itself select CLI mode or authorize setup changes.

Run local checks in the user's chosen environment:

```sh
uc --version
uc-daemon --version
uc-session-host --version
uc config validate
uc daemon status --json
```

The binaries should come from the same release/build. This skill targets 0.1.0-beta;
for source builds, use matching revisions. Help and version checks do
not connect. Configuration validation and daemon status do not start a daemon.
Status exits nonzero when the selected daemon is not running; that is expected
for a new installation, and ordinary session commands can start it. Diagnose
configuration, missing-helper, or connection errors before launching work.

`UNDERCURRENT_HOME` must be absolute when set. Otherwise the default is
`$HOME/.aixand/undercurrent` on Unix or `%USERPROFILE%\.aixand\undercurrent` on
Windows. Preserve this choice across invocations. `uc config show --json`
describes effective on-disk settings, not necessarily the running daemon's
loaded settings.

Ordinary session commands, including `ls`, `inspect`, and `logs`, can start an
absent daemon automatically. For an offline investigation, stay with validation,
status, and local files until startup is appropriate.

If `uc` or a helper is missing, use the installation instructions for the user's
chosen release. Do not infer release availability from this skill's version.
The source guides are [installation](https://github.com/Aixand/undercurrent/blob/main/docs/getting-started/installation.md)
and [compatibility](https://github.com/Aixand/undercurrent/blob/main/docs/reference/compatibility.md).
Use the corresponding tag or revision when diagnosing an older installation.

## Shell integration, when needed

Direct program starts do not require shell hooks. Hooks provide interactive
shell readiness and exported-environment capture for live-context `uc exec`.
If shell setup is part of the task, preview the appropriate hook first:

```sh
uc init bash --print
```

Install using `uc init bash` (or `zsh`, `fish`, or `powershell`) in the actual
startup file. `--profile` selects an absolute startup path. PowerShell commonly
uses `uc init powershell --profile "$PROFILE"`. Installation edits that startup
file; existing managed shells retain their old hooks. Verify with a new managed
shell and `uc context` inside it.

## Disposable exercises

For a new, isolated POSIX-shell exercise, keep every command in the same shell:

```sh
uc_skill_dir="$(mktemp -d /tmp/uc-skill.XXXXXX)"
export UNDERCURRENT_HOME="$(cd "$uc_skill_dir" && pwd -P)"
uc config validate
uc daemon start
uc daemon status --json
```

Use the physical path because macOS `/tmp` is a symlink and private IPC rejects
symlink ancestors. Record the generated home so later commands can reconnect.
This is for a new exercise, not recovery of the user's existing sessions. On completion, stop and
inspect every exercise session before stopping this dedicated daemon. Preserve
the home if effects or cleanup remain uncertain. Never stop a shared daemon as
part of an isolated session's cleanup.
