# Find Windows implementation and verification paths

Start a session or find the Windows component you need to investigate. Native
implementations are present; source presence alone does not establish release
or Python bridge qualification.

| Task | Owner and guide |
| --- | --- |
| Run your first session | [PowerShell tutorial](../getting-started/first-session-windows.md) |
| Public local client connection | [Local endpoint protection](../../crates/undercurrent-transport-local/docs/internals/endpoints.md) |
| Native pipes, path pinning and access control | [Windows architecture](../../crates/undercurrent-platform-windows/docs/internals/ownership.md), [file security](../../crates/undercurrent-platform-windows/docs/internals/filesystem-security.md) |
| Native process/job and identity behavior | [Windows processes](../../crates/undercurrent-platform-windows/docs/internals/processes.md) |
| Private session-host channels | [Private IPC](../../crates/undercurrent-platform-windows/docs/internals/private-ipc.md), [runtime host](../../crates/undercurrent-runtime/docs/internals/host/session-host.md) |
| Build and native tests | [Windows development](../../crates/undercurrent-platform-windows/docs/development/contributing.md) |

Do not use the Unix `dev.sh` installer on Windows. Follow the native guide's
toolchain, target and shell requirements. Pipe per-call timeouts are not a total
operation deadline; clones share deadline and shutdown state. Job cleanup on
drop is best effort, not a durable application result.

The Windows job in [Native platform CI](../../.github/workflows/platform-contracts.yml)
defines its target checks.
The [platform coverage checklist](../development/platform-testing.md) distinguishes
checked-in jobs from observed runs. The runtime capability helper currently
understates several implemented Windows paths, as described in
[platform scope](../reference/platforms.md). Establish an exact target/run result
before relying on a native guarantee.

[Documentation index](../README.md)
