# Edit and verify a detection profile

Use this guide to customize the daemon's active profile catalog without
restarting supervised sessions. It applies to profile YAML schema **1** in
Undercurrent 0.1.0-beta; the main configuration uses the same release identity.
You need a running local daemon, matching `uc`, and write access to that
installation's `profiles/` directory. The commands use a POSIX shell, run from
any directory, and require `UNDERCURRENT_HOME` to explicitly identify the home
you intend to edit.

Profiles supply best-effort execution, interaction, constraint, and background
observations plus an input-delivery policy. They cannot prove that a process
completed its intended work. Use [the operator guide](../daemon/operations.md) to create an
isolated installation before trying this example.

## Inspect the active catalog

```sh
uc profile list --page-size 100
uc profile inspect codex
uc profile lint codex
uc profile match --executable codex
```

These commands inspect the **active** daemon catalog. They do not validate an
unactivated file directly. A rejected edit can coexist with a valid active
profile of the same name. Profile summaries report `built_in` or `user` source;
files seeded on first startup are user overrides of the embedded defaults.

## Add a bounded demonstration profile

This example does not launch a managed process or change any existing profile.
It adds a uniquely named profile with one whole-screen interaction rule. The
pattern is intentionally narrow and has no automatic executable match.

### 1. Write a complete candidate

```sh
(
  set -eu
  : "${UNDERCURRENT_HOME:?Select the intended absolute installation first}"
  test -d "$UNDERCURRENT_HOME/profiles"
  test ! -e "$UNDERCURRENT_HOME/profiles/docs-demo.yaml"
  uc_profile_tmp=$(mktemp "$UNDERCURRENT_HOME/profiles/.docs-demo.XXXXXX")
  trap 'rm -f -- "$uc_profile_tmp"' EXIT
  cat > "$uc_profile_tmp" <<'YAML'
schema_version: 0.1.0-beta
name: docs-demo
description: Demonstration of an explicit interaction observation
archetype: unclassified
input_delivery:
  type: immediate
foreground_rules:
  - claim:
      type: interaction
      state: available
      modality: text
      semantic: docs-demo.prompt
    patterns: ['^DOCS> $']
    confidence_basis_points: 9000
YAML
  mv "$uc_profile_tmp" "$UNDERCURRENT_HOME/profiles/docs-demo.yaml"
)
```

### 2. Verify the activated profile

The reload worker needs two identical successive snapshots, then an uncontended
activation gate and a final matching disk snapshot. Its nominal interval is one
second; activation is not guaranteed within exactly two seconds. After allowing
the worker to run, verify:

```sh
uc profile inspect docs-demo
uc profile lint docs-demo
uc --json profile test docs-demo \
  --screen-text 'DOCS> ' --last-line 'DOCS> '
uc --json profile test docs-demo \
  --screen-text 'working' --last-line 'working'
```

Expected results from the schema and evaluator: inspection reports source
`user` and one foreground rule. Lint can report a warning that the profile has no
automatic selection rule while remaining valid. The first test selects rule 0
with available text interaction and semantic `docs-demo.prompt`; the second has
no selected foreground rule. These are expected results, not outputs observed
during authoring. Returned evaluation records omit the sample text.

If inspection initially says the profile is missing, retry the read after a
worker cycle. If it remains absent, inspect `uc --json debug supervisors` for
`profile_reload` failures and the daemon log. Do not treat a failed edit as
activated merely because an older catalog still passes lint.

### 3. Remove the demonstration profile

Remove only this unreferenced sample after testing:

```sh
rm -- "$UNDERCURRENT_HOME/profiles/docs-demo.yaml"
```

After successful reload, `uc profile inspect docs-demo` should report absence.
If you assigned the profile to a live session or a session awaiting automatic
restart, restore the file until that reference is no longer needed. Removal of a
referenced custom profile is rejected to keep live observation policy coherent.

## Built-in profiles and selection

The embedded catalog currently has fifteen names, defined by
[profile_builtins.rs](../../../crates/undercurrent-daemon/src/profile_builtins.rs) and its linked YAML files:

| Family | Names and relevant behavior |
| --- | --- |
| Shells | `bash`, `zsh`, `fish`, `cmd`, `powershell`: fallback matching and immediate input. |
| REPLs | `python`, `node`: fallback matching and immediate input. |
| Servers | `server` is explicitly selected; `dev-server` matches specific development commands, not generic `npm start`. |
| Agent TUIs | `codex`, `claude-code`, `copilot-cli`, `antigravity` (the `agy` executable), `opencode`, `aider`: bounded paced trailing submission, currently 120 ms. Codex uses paste text mode. |

Codex deliberately has no unconditional available-interaction baseline. Its
composer rule requires affirmative idle-footer evidence and excludes startup,
queued-message, and permission screens. These are the distribution's tested
patterns, not a compatibility promise for every future version of an external
TUI. Preserve a failing sample when investigating a mismatch.

Codex execution becomes `active` when the visible composer and model/directory
footer frame an interrupt or queue hint, or an elapsed-time working indicator
immediately above the composer. This supports activity between terminal updates;
it does not establish task progress or completion. The rule excludes startup
and approval screens and requires the surrounding UI, so a hint mentioned in
ordinary output is insufficient. Unrecognized or incomplete layouts retain
`unknown` execution unless another evidence source establishes activity.
The tested indicator shapes follow Codex's
[status row](https://github.com/openai/codex/blob/main/codex-rs/tui/src/status_indicator_widget.rs)
and [queue footer](https://github.com/openai/codex/blob/main/codex-rs/tui/src/bottom_pane/footer.rs);
custom footers, remapped keys, or future layouts may need profile changes.

The current Codex **Folder access** dialog is reported as interaction
`requested`, modality `options`, and semantic `codex.workspace_trust`. Its rule
checks the visible screen for the title, trust question, **Trust and continue** /
**Quit** choices, and keyboard footer. This also detects the dialog near the top
of a tall terminal. Older trust prompts retain their separate bottom-row rule.
Detection reports the pending choice; it does not grant folder access.

Codex footers accept Unix paths, Windows drive paths, and Windows home-relative
paths such as `~\workspace`. The complete **Update Model Permissions** menu
reports `requested` / `options` with semantic `codex.permission_settings`.
Detection requires all four choices and the keyboard footer; it does not select
a permission mode. Existing seeded profile files need the updated YAML.

Claude Code 2.1.299 and Antigravity CLI (`agy`) 1.3.5 folder-trust dialogs are
also reported as `requested` / `options`, with `claude-code.workspace_trust`
and `antigravity.workspace_trust` semantics. These rules examine the whole
visible screen and require the dialog's question and choices; trust dialogs
near the top of a tall terminal must not fall back to an available composer.
Claude's first-run theme selector reports `claude-code.theme_selection`.
Captured real PTY screens are retained in the daemon's
[profile fixtures](../../../crates/undercurrent-daemon/src/profile_builtins/fixtures/).
Startup/profile selection and these pending decisions were tested through
direct CLI launches and external SDK launches; no model turn was submitted.

When a launcher spawns an agent child, new CLI sessions without an explicit
profile follow the foreground process group and can detect that child.
Earlier CLI builds defaulted direct launches to fixed binding, matching only the
launch command (`agent`). Existing sessions retain that saved binding, including
across restarts. Add `--profile-mode foreground` when using an earlier build,
or select `--profile codex` to keep that profile fixed.
From the intended project directory, with `agent` on `PATH`:

```sh
uc start codex-work -- agent codex start
uc explain codex-work
uc peek codex-work
```

After Codex starts, expect profile `codex`; a visible folder-access dialog should
show interaction `requested`. If the profile remains unclassified, inspect
`uc --json explain codex-work` for incomplete or ambiguous foreground evidence.
If the profile is correct but interaction is missing, compare `uc peek` with the
active profile and verify that its YAML edit loaded. Existing seeded profile
files survive binary upgrades, so update the installed `profiles/codex.yaml` to
adopt new patterns. When finished, `uc stop codex-work` stops this session.

## Customize safely

Edit the existing profile file and use an atomic rename from a non-YAML temporary
file, as in the example. Duplicate `name` values across two user files reject the
whole candidate catalog. Deleting a built-in's override reveals its embedded
definition during live reload; startup also seeds missing built-in filenames
without overwriting existing files. To disable automatic selection persistently,
keep an override with `disabled: true` rather than deleting it.

The exact authoring contract is the strict boundary in
[profile_yaml.rs](../../../crates/undercurrent-daemon/src/profile_yaml.rs), followed by domain constructors in
[profile.rs](../../../crates/undercurrent-domain/src/profile.rs), and application linting
in [profiles.rs](../../../crates/undercurrent-application/src/profiles.rs).

| YAML area | Contract and useful limits |
| --- | --- |
| Root | Required `schema_version: 0.1.0-beta`, `name`, `archetype`, and tagged `input_delivery`. Unknown fields are rejected, including inside nested records. |
| `matching` | Signatures have binary patterns, argument regexes, and required environment **names**. `matching_fallback` defaults false; enabled primary matches take precedence over fallback profiles. Multiple eligible profiles can remain ambiguous. |
| `archetype` | `unclassified`, `line_oriented`, `animated_agent`, or `server`; domain behavior supplies the meaning. |
| `input_delivery` | `immediate` or `paced_trailing_submit` with a positive bounded `delay_millis`. `text_mode` is `raw` by default or explicit `paste`. A pacing policy is not an acknowledgement that the application consumed input. |
| `baseline_interaction` and foreground `claim` | Execution: `active`, `quiescent`, `unknown`. Interaction: `none`, `available`, `requested`, with allowed modality/semantic combinations. Constraint: `clear`, `present`. Domain constructors validate combinations, not only spelling. |
| `foreground_rules` | Patterns, exclusions, prompt traits, confidence in basis points, and an optional screen region. A claim is selected from evidence, not a durable business-result declaration. |
| `background_rules` | `process_group` or `agent`, patterns/exclusions, name patterns, optional label/region, and confidence. These report observations about background work. |
| Regions | Bottom rows with `any`, `cursor`, `last`, or `near_cursor` anchor; above/below cursor; optional bounded columns. Rows and supplied columns are nonzero. A sample must carry the matching region for rule evaluation. |
| Display assumptions | `alternate_screen_is_normal`, `silent_until_input`, and top/bottom output exclusions describe interpretation of terminal evidence. |

Files are UTF-8, at most 256 KiB, and one YAML document. Tags, anchors, aliases,
merge keys, duplicate keys, permissive booleans such as `yes`, excessive depth,
and oversized parser event/node/scalar budgets are rejected. Domain and regex
limits come from validated `services` configuration. A directory snapshot admits
at most 128 YAML files, 1024 directory entries, and 8 MiB of YAML contents;
subdirectories are not recursively loaded and YAML symlinks are rejected.

The CLI's simple profile test supplies a whole-screen sample. A region-specific
rule can return `sample_unavailable` for that input rather than `not_matched`.
Use the owning evaluator/runtime tests to exercise actual cursor-region sampling;
do not remove a production region guard merely to make a synthetic test match.

For concurrency and failure details, see [profile activation](../../../crates/undercurrent-daemon/docs/internals/configuration/profile-reload.md).

[Configuration](../../../crates/undercurrent-daemon/docs/internals/configuration/resource-bounds.md) · [Crate entry point](../../../crates/undercurrent-daemon/README.md)
