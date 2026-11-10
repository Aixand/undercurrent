# Follow foreground processes by default in the CLI

Status: **Accepted and implemented**, 2026-10-01.

Scope: the default binding chosen by `uc start`. This decision does not change
SDK request defaults or bindings already saved on existing sessions.

## Context

The CLI previously selected foreground-adaptive binding automatically for
interactive shells and fixed binding for direct launches. A fixed binding kept
the profile selected from the launch command throughout the session.

That left a direct launch such as `agent codex start` unclassified: the launcher
had no matching profile, even though a Codex process owned the terminal. Users
had to select a profile or request foreground mode explicitly. The same running
application could therefore receive different detection depending on whether
it was started through an interactive shell or a direct wrapper command.

## Decision

Without an explicit profile, the CLI's `auto` mode follows foreground processes
for both interactive shells and direct launches. Explicit profiles continue to
default to fixed binding. The complete override rules belong to the
[CLI start reference](../../reference/cli-commands.md).

Foreground selection uses the existing process-group matcher. It gives
application profiles priority over generic shell and interpreter fallback
profiles, and reports ambiguous or incomplete evidence as unclassified.
It does not select arbitrary background descendants. The
[profile lifetime diagram](../../../crates/undercurrent-domain/docs/internals/observations-and-profiles.md#base-profile-and-effective-profile-have-different-lifetimes)
explains the relationship between saved configuration and runtime selection.

## Consequences and alternatives

Wrapper launches can discover their actual application without maintaining a
signature for every launcher. Applications that later take foreground ownership
can also receive their own profile. Discovery requires an observation cycle;
startup or unavailable process evidence can temporarily leave the session
unclassified.

Profile selection affects prompt interpretation and input delivery. For example,
Codex supplies paste encoding and delayed submission. When no effective profile
is selected, input delivery uses the existing immediate policy. Callers that need
a stable policy for a known application can select its profile explicitly or use
fixed mode. Adaptive mode continues to expose uncertainty instead of retaining a
potentially incorrect application profile.

Keeping fixed binding as the direct-launch default would preserve predictable
policy through gaps in process evidence, but leave wrappers unclassified unless
callers configured them. Adding individual wrapper signatures would address
known launchers while requiring ongoing launcher-specific maintenance.

Existing sessions retain their saved binding across restarts. This change adds
no automatic migration of those sessions.

## Implementation and verification

[CLI request preparation](../../../crates/undercurrent-cli/src/preparation.rs)
owns the default and tests direct, wrapped, interactive, fixed, explicit-profile,
and disabled-detection starts. The
[real-daemon profile test](../../../tests/e2e/configuration/test-yaml-profiles.py)
verifies default wrapper discovery, Codex folder-access interaction, ambiguity,
and explicit fixed selection in isolated state.

[Foreground observation](../../../crates/undercurrent-application/src/observation.rs)
owns runtime selection; [terminal input](../../../crates/undercurrent-application/src/commands/terminal.rs)
uses that selection's input policy.

[Decision index](README.md)
