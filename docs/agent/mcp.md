# Use Undercurrent through MCP

`uc-mcp` exposes nine session tools over local stdio. It uses the public Rust SDK
to connect to the selected home, and leaves daemon-owned sessions running when
the assistant disconnects. Pair it with either or both [agent skills](README.md).
It can also be used without those skills; each tool describes its inputs and
observation limits.

On Windows, stopping requires an explicit `graceful_then_force` or `force` mode.
The runtime cannot deliver POSIX graceful signals, so MCP rejects the default
`graceful_only` before submitting a stop or changing session state. Bounded stop
waits the configured grace interval before native force. Use application-specific
shutdown when a force-free exit is required.

The same two skills support explicit CLI, explicit MCP, and automatic selection;
there are no separate skill editions to install. Ask for MCP mode to keep session
control on these tools, or leave selection automatic to prefer configured MCP.
An unavailable or read-only server does not trigger silent CLI fallback. See
[interface selection](README.md#choose-an-interface) for capability handoffs and
resume behavior.

## Build and select the target

The portable product artifact includes `uc-mcp` alongside the other native
executables. Source installations through `./dev.sh install --prod` also include
it. To build only the relevant binaries from this checkout:

```sh
cargo build --locked -p undercurrent-mcp -p undercurrent-cli -p undercurrent-daemon --bins
target/debug/uc-mcp --version
```

Use matching builds. Start the selected daemon explicitly using the CLI:

```sh
UNDERCURRENT_HOME=/absolute/runtime/home uc daemon start
```

On PowerShell, set `$env:UNDERCURRENT_HOME` to the absolute home, then run
`uc daemon start`. Use the native binaries for that platform.

Configure your MCP client's stdio server entry with an **absolute executable**,
arguments `--home /absolute/runtime/home`, and no shell wrapper. For clients
using the `mcpServers` JSON convention:

```json
{
  "mcpServers": {
    "undercurrent": {
      "command": "/absolute/installation/bin/uc-mcp",
      "args": ["--home", "/absolute/runtime/home"]
    }
  }
}
```

Client registration formats differ; translate those command and arguments into
your client's documented configuration. Windows uses `uc-mcp.exe` and absolute
Windows paths. Product installation does not edit assistant configuration.

`--home` overrides `UNDERCURRENT_HOME`; otherwise shared native home discovery
applies. Configuration loading validates the selected home without creating it.
`get_status` reports the resolved target. An unavailable daemon does not prevent
tool discovery and is never started implicitly. Restart the adapter after
changing home/configuration.

Add `--read-only` to expose only the six observation tools. The server rejects
mutation calls even if their names are supplied manually. Full mode executes
processes and terminal input under its OS user; it does not add a sandbox.

## Tool and result contract

The independently installable [MCP control reference](skills/undercurrent/references/mcp-control.md)
describes the nine tools, result fields, input semantics, stop modes, and recovery.
Existing sessions are targeted by stable ID. Input and stop support daemon-checked
revision guards. Starts use native executable resolution and restart policy
`never`; caller argv and environment are not printed in adapter diagnostics.

The adapter has eight finite-call slots and four separate wait slots. Saturation
returns an overload error before submission. Requests have bounded SDK response
waits (10 seconds); initial dial/handshake and admission are separate. State waits
are bounded at 60 seconds after connection setup and release subscriptions on
completion, cancellation, or failure. They observe sampled state and cannot prove
that every intermediate transition was observed.

Input MCP frames and encoded tool results are capped at 256 KiB; tool argument
objects are capped at 64 KiB. Output defaults to 8192 bytes, with a 32768 maximum.
Large list/detail results may require smaller pages or fewer projections. Errors
preserve available product effect certainty; unknown effects must be reconciled.
Canceling a finite request may leave accepted work running. No mutation is
automatically replayed after cancellation, timeout, or reconnection.

This release supports tools over stdio. Runtime execution, storage, daemon
administration, task planning, and the attached-worker `operation-agent-mcp-bridge`
remain separate responsibilities. Remote HTTP, MCP resources/prompts, separate
context execution, restart, and deletion are outside this tool inventory.

## Qualification

Run unit tests and a real isolated-daemon probe:

```sh
cargo test --locked -p undercurrent-mcp
python3 tests/conformance/mcp_test.py --binary-dir target/debug
```

The probe uses standard-library Python as an MCP client and a supervised PTY
worker. It checks protocol revisions 2024-11-05, 2025-11-25, and 2026-07-28,
offline discovery, explicit input, guarded mutations, pagination, bounded waits,
cancellation, and read-only enforcement. Installed-product verification repeats
the live probe. This is protocol qualification, not a claim of manual UI testing
in every assistant application. Run the native CI probe before claiming a new
platform qualified.
