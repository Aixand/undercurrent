# Changelog

This checkout contains the initial `0.1.0-beta` development snapshot.
No published release is announced here. Future entries will record user-visible
changes from this baseline.

## Unreleased

### Changed

- External pull requests are not currently accepted because maintainer review
  capacity is limited. Issues remain welcome. Removed the commit sign-off
  requirement and its tooling.

### Added

- Named process sessions with restart policies, health checks, and retained output.
- Interactive terminals with detach, reattach, split panes, and saved groups.
- Detection profiles for shells, REPLs, servers, and coding agents, including
  observed activity and requests for input.
- CLI commands to inspect sessions, send input, wait for state changes, and run
  commands using a session's live working directory and shell environment.
- Rust and Python SDKs for session control, output streams, and recovery.
- A local MCP server and installable agent skills for session control and
  worker orchestration.
- Native runtime adapters for Linux, macOS, and Windows, with source installation
  and portable product packaging.

### Compatibility

- The beta uses exact protocol and storage versions. Existing development state
  is not automatically migrated.
- Clients connect to a daemon running as the same OS user through local IPC.
  Remote network access is unsupported.
- Release artifacts and hosted installation routes require a separately
  published release. See [installation](docs/getting-started/installation.md)
  and [compatibility](docs/reference/compatibility.md) for current availability
  and limitations.
