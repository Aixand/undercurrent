# Run and verify real process workflows

Follow actual SDK calls through private daemons and managed worker processes.
Each example checks the application result and its owned-process cleanup.
Start with [runtime setup](setup.md).

| Outcome | Walkthrough | Evidence |
| --- | --- | --- |
| Accept dependent work | [Parallel report](parallel-report.md) | Concurrent workers, successful exits and exact report bytes |
| Reject invalid work | [Failed prerequisite](failed-prerequisite.md) | Exit 7, no report artifact and no report session |
| Resume output | [Byte checkpoint](output-checkpoint.md) | Two clients, six lines and no lost or duplicate bytes |
| Recover after a crash | [Daemon reconnect](daemon-reconnect.md) | Same native process/session, explicit output gap and saved prefix |
| Inspect through tools | [Read-only MCP](mcp-observer.md) | Real stdio tools, forbidden mutation and unchanged process |

These are deterministic application fixtures using real Undercurrent execution;
they make no model calls. Keep runtime communication separate from package
imports when tracing the implementation. General [architecture](../../architecture/README.md)
explains the process boundaries; the [development map](../../development/workspace-guide.md)
locates each crate and the separate Python SDK.
