# Interpret platform validation coverage

This checklist is included in native CI summaries. It describes the checked-in
jobs, not their latest results. Associate any result with its revision, target,
toolchain and retained logs before making a support claim.

The [Native platform CI workflow](../../.github/workflows/platform-contracts.yml)
runs for diagnostic tags matching `ci/**` and through the version-tag release
workflow. A small Ubuntu job first checks that the tagged
commit is in `main`'s history, including its current tip. Tags on commits outside
`main` skip every native test job. Branch pushes, pull requests and manual
dispatches do not trigger native CI.

For qualifying tags, names matching `ci/windows-*` select only the Windows
native job; other diagnostic tags and release tags select Linux, macOS and Windows together.

To test a commit on Windows only, create and push a new tag at a commit on `main`,
for example `ci/windows-20261008-1`. This starts CI without publishing a release.
The separate repository and product-installer checks run on pushes to `main`
and maintainer pull requests.

An all-platform run starts four native jobs: Ubuntu 24.04, macOS 15 (Arm64),
macOS 15 Intel, and Windows Server 2022. Each job uses its platform's existing
test commands, the pinned Rust toolchain, isolated installations, and retained
logs and bundle reports. The jobs can run in parallel.

| Evidence group | Checked-in coverage | What remains distinct |
| --- | --- | --- |
| Installer | Host/failure contracts, three binary versions | Source installation is not release publication |
| CLI native behavior | Platform contract and Bash/Zsh/Fish detection scripts | Shell observations are not arbitrary application completion |
| IPC and terminal | Endpoint tests, foreground evidence, terminal queries | Windows has separate native paths and a dedicated job |
| Identity and cleanup | Execute-host, ancestry, pinned cwd, shell integration, reset and host socket tests | Native fixtures do not prove every adversarial OS race |
| macOS adapter | Platform-macos and runtime macOS observation tests | A non-macOS build omits the native implementation |
| Live environment | Execute-environment smoke | Captured shell context and terminal input are different operations |
| Bundles | Manifest validation, adversarial verifier, platform bundle installation proof | No distribution availability follows from a source job definition |

See [Windows source and verification](../guides/windows.md) for its native checks.
For pre-push Windows checks, use the [shared native runner and VM procedure](../../crates/undercurrent-platform-windows/docs/development/contributing.md#run-the-windows-workflow-before-pushing).
Python's maintained mock tests exercise convenience behavior, not live bridge
transport or native platform equivalence. Rust doctests, warning-denied Rustdoc,
and Mermaid render/visual review also need their own recorded results.

For commands, prerequisites and result scope, use
[maintainer verification](verification.md) and the linked owning
crate's development guide. This page is deliberately not a static “all passed”
badge: current run artifacts are the authority for execution evidence.

[Platform scope](../reference/platforms.md) · [Documentation index](../README.md)
