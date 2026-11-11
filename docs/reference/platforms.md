# Determine platform scope before building or qualifying

This reference distinguishes checked-in implementations, executable validation
paths and release evidence. A target-specific source file is not a passing
native run. Use target-native results tied to the revision being qualified,
rather than inferring qualification from this map.

| Area | Linux | macOS | Windows |
| --- | --- | --- | --- |
| Public local transport | Unix socket adapter | Unix socket adapter | Named-pipe adapter |
| Native process/runtime paths | Implemented | Implemented with macOS observation adapter | Implemented ConPTY/job/process paths |
| Public installer `installer.aixand.org/undercurrent` | x86_64 with GNU libc, published bundle required | arm64 and x86_64, published bundle required | x86_64 through `/install.ps1`, published Windows bundle required |
| Source installer `dev.sh` | x86_64 with GNU libc | arm64 and x86_64, Xcode tools required | Not supported by this script |
| Checked-in native CI | Ubuntu 24.04 job | macOS 14 and macOS 15 Intel jobs | Windows Server 2022 job in the same workflow |
| Python live qualification | No result established here | No result established here | No result established here |

The hosted installer needs no system Python or Rust. See
[installation prerequisites](../getting-started/installation.md#choose-an-installation-route)
for native tools. Compiler requirements apply to source builds; Windows local
development uses `dev.ps1`.

Owners: [local transport](../../crates/undercurrent-transport-local/README.md),
[runtime](../../crates/undercurrent-runtime/README.md), [installer](../../dev.sh),
[native workflow](../../.github/workflows/platform-contracts.yml), and
[Windows guide](../guides/windows.md). The package manifest's target and release
declarations remain separate from this implementation table; consult
[version compatibility](compatibility.md).

[`RuntimePlatformSupport::current`](../../crates/undercurrent-runtime/src/platform.rs)
reports hosted sessions and supervised commands on Windows as well as the Unix
adapters. Its foreground-inspection flag specifically describes Unix-style
foreground inspection; Windows uses managed job/process evidence. These flags
identify implemented adapters and do not establish native release qualification.

macOS foreground collection currently leaves managed-root and managed-tree
listener evidence unavailable. Native observations establish bounded evidence,
not application readiness. The macOS crate's library body is cfg-disabled on
other targets, so a Linux pass does not validate it. Windows native file, pipe
and process guarantees likewise require execution on Windows.

Use [platform test coverage](../development/platform-testing.md) to choose native evidence,
or [installation](../getting-started/installation.md) for first use.

[Documentation index](../README.md)
