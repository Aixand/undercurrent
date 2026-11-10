# Install Undercurrent skills for your agent

These skills teach a local coding agent to use Undercurrent in your projects.
Both skills support CLI, MCP, and automatic interface selection. Keep the same
skill names and installation folders for every mode. Configure the optional
[MCP server](mcp.md) for typed session tools.
Install either skill independently, or both:

| Skill | Use it for |
| --- | --- |
| [undercurrent](skills/undercurrent/SKILL.md) | Persistent processes, session input, health checks, output, and recovery. |
| [undercurrent-orchestrate](skills/undercurrent-orchestrate/SKILL.md) | Delegating tasks to agent processes supervised by `uc`, collecting results, and resuming existing workers. |

The orchestration skill includes its own CLI and MCP guidance. It launches
workers directly as Undercurrent sessions. The Python SDK is optional;
CLI mode needs no MCP server. The parent agent assigns tasks and evaluates
results.

## Choose an interface

State a preference in your request or project guidance, or leave selection
automatic. These are skill instructions, not new executable flags or settings.

| Choice | Behavior |
| --- | --- |
| CLI | Use `uc` commands, even when MCP is configured. |
| MCP | Use configured Undercurrent MCP tools for session control. Report missing prerequisites or unsupported operations without switching to CLI. |
| Automatic (default) | Prefer a compatible MCP server for the selected home; use CLI when MCP is unconfigured. |

A configured but unavailable, incompatible, or incorrectly targeted MCP server
needs diagnosis. A read-only server remains observation-only; the skills do not
bypass it through CLI. Automatic mode permits a deliberate CLI handoff for an
operation outside the MCP tool inventory after explaining the gap, verifying the
same machine/user/home and session identity, and resolving outstanding effects.
An error or timeout is not permission to replay a mutation through another interface.

The selected interface's reference provides command/tool details. Shared guidance
covers verification, evidence, and recovery without assuming CLI commands. MCP
mode still allows ordinary authorized file, Git, and shell work for preparing
tasks and reading results. Session control mode does not install binaries,
register MCP, or start a daemon implicitly through MCP; setup remains separate.
Orchestration records the preference, selected interface, target, versions, and
any deliberate interface changes so interrupted runs can resume consistently.

## Prerequisites

Install [Undercurrent](../getting-started/installation.md) separately. These
instructions target the **0.1.0-beta CLI and MCP contracts**. Use matching native
builds, including `uc-mcp` for MCP mode. When building from source, use skills and
binaries from the same revision.

The MCP server or CLI needs access to the same machine, OS user, and Undercurrent
home as the managed sessions. CLI workflows require local command execution.
Orchestration also requires task-file/worktree access and an
installed, authenticated worker agent CLI. Check its local help before launching
workers. Worker invocations use that agent's configured account and resources.

## Install from a checkout

Choose a destination supported by your agent:

| Agent | Personal installation | Project installation |
| --- | --- | --- |
| Codex | `~/.agents/skills/` | `YOUR_PROJECT/.agents/skills/` |
| Claude Code | `~/.claude/skills/` | `YOUR_PROJECT/.claude/skills/` |

These locations follow the official [Codex skill documentation](https://learn.chatgpt.com/docs/build-skills)
and [Claude Code skill documentation](https://code.claude.com/docs/en/skills).
For another Agent Skills client, use its documented skill directory.

From the **Undercurrent repository root**, this POSIX-shell example installs
both skills for your Codex user. Change `uc_skills_dir` to select another
destination. It refuses to merge into an existing installation.

```sh
uc_skills_dir="$HOME/.agents/skills"
(
  set -eu
  mkdir -p "$uc_skills_dir"
  for skill in undercurrent undercurrent-orchestrate; do
    if [ -e "$uc_skills_dir/$skill" ] || [ -L "$uc_skills_dir/$skill" ]; then
      printf 'Already exists: %s\n' "$uc_skills_dir/$skill" >&2
      exit 1
    fi
  done
  for skill in undercurrent undercurrent-orchestrate; do
    cp -R "docs/agent/skills/$skill" "$uc_skills_dir/$skill"
  done
)
```

On Windows, run this from the repository root in PowerShell, changing the
destination as needed:

```powershell
$ucSkillsDir = Join-Path $HOME '.agents/skills'
$ucSkillNames = @('undercurrent', 'undercurrent-orchestrate')
New-Item -ItemType Directory -Force -Path $ucSkillsDir -ErrorAction Stop | Out-Null
foreach ($skill in $ucSkillNames) {
    $destination = Join-Path $ucSkillsDir $skill
    if (Test-Path -LiteralPath $destination) { throw "Already exists: $destination" }
}
foreach ($skill in $ucSkillNames) {
    Copy-Item -LiteralPath "docs/agent/skills/$skill" -Destination $ucSkillsDir -Recurse -ErrorAction Stop
}
```

Copy the **whole skill folder**, including its references and assets. Each
folder works outside this checkout; no shared sibling directory is required.
`docs/agent/skills/` is the distribution source, not an automatic discovery
location. Start a new agent session after installation and check its skill list.
Product artifacts also carry complete copies under `share/undercurrent/skills/`;
copy those to your agent's skill directory using the same procedure. Installing
the product does not register a server or overwrite installed skill customizations.

For updates, preserve any local edits, move the old skill folders outside the
discovery directory, and install complete replacements from the chosen revision.
To uninstall, remove only these two installed folders. Neither action changes
Undercurrent binaries, sessions, or daemon data.

## Try the skills

Use natural language, or explicitly select a skill in your agent:

- "Use the undercurrent skill to keep this development server running and verify its health."
- "Use the undercurrent skill to inspect my existing build session and collect its output."
- "Use the undercurrent skill through CLI to inspect my build session, even if MCP is available."
- "Use the undercurrent skill through MCP to start my development server and verify readiness."
- "Use the undercurrent-orchestrate skill to run architecture and test-coverage reviews in parallel through uc."
- "Use the undercurrent-orchestrate skill to split this implementation across two workers in separate worktrees, then review and integrate their changes."
- "Use the undercurrent-orchestrate skill to resume the workers recorded in this run manifest."
- "Use the undercurrent-orchestrate skill through MCP to run two independent reviews and verify their results."

For orchestration, supply the task, preferred worker agent if any, and acceptance
checks. The parent records worker names, IDs, directories, and result locations
in a run manifest. Review workers can share a stable checkout; concurrent writers
use isolated worktrees or otherwise explicitly separated working directories.

## Maintain the skills

Keep the entrypoints focused on shared workflows and interface selection; put
command syntax and tool arguments in their respective control/launch references.
Load only the selected interface and needed workflow details. Keep selection
rules consistent across both independently installable skills.
Add helper scripts when a repeated operation warrants executable automation.
The run manifest is an orchestration convention, not an Undercurrent protocol.

When changing commands or supported versions, compare the skill instructions
with [CLI commands](../reference/cli-commands.md),
[output shapes](../reference/cli-output.md),
[lifecycle](../architecture/lifecycle.md), and
[recovery](../guides/troubleshooting.md). Validate frontmatter and relative links
after copying each skill to a temporary directory without its sibling or the
repository. Parse manifest/example JSON and compare MCP examples to discovered
tool schemas. Review these behavior scenarios for both skills:

| Scenario | Expected decision |
| --- | --- |
| No MCP configured, no preference | Select CLI and load CLI details. |
| Compatible MCP for the selected home, no preference | Select MCP and load MCP details. |
| Both available, explicit CLI | Keep CLI; do not select MCP merely because it is available. |
| Explicit MCP with no configured server | Report the missing prerequisite; do not start the work through CLI. |
| Configured MCP offline, incompatible, or on another home | Diagnose the selected target; no silent fallback or home change. |
| Read-only MCP, request includes mutations | Continue supported reads; report the restriction without bypassing it. |
| Operation outside MCP's inventory | Explicit MCP reports the gap; automatic mode may use a deliberate handoff under the documented rules. |
| Input response lost | Retain identity and payload, inspect effects, and avoid replay through either interface. |
| Resume after available interfaces changed | Keep the saved interface and worker identities; reconcile pending work. |
| Older run manifest | Recover missing interface/target fields from evidence without recreating workers. |

Smoke-test CLI examples in an isolated home; use real agent runs
only when that validation is in scope. Installation syntax does not establish
runtime qualification on every platform or agent client.

[Documentation index](../README.md)
