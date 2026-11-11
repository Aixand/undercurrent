# Install matching standalone artifacts

Use the [hosted installer](installation.md#install-a-published-release)
at [installer.aixand.org/undercurrent](https://installer.aixand.org/undercurrent)
for published releases. It verifies a pinned bundle and installs the complete
portable product. Follow the installation guide for installation,
platform prerequisites, verification, and shell setup.

Use this guide for alternative source builds and separately supplied bundles.
Keep `uc`, `uc-daemon`, and `uc-session-host` from the same release or source build.

For a local source checkout, use [the development installer](installation.md#install-for-source-development):
`./dev.sh install --prod` on Linux/macOS or `./dev.ps1 install --prod` on Windows.
Python additionally needs the matching bridge described in
[Python setup](../sdk/python/quickstart.md).

For an independently supplied bundle, use its accompanying manifest, checksums,
license and verification instructions before installation. The current
[bundle qualification guide](../development/releases.md) describes the
repository's verifier and isolated installation proof. Do not assemble a mixed
set of artifacts from different builds.

Consult [platform scope](../reference/platforms.md) for the distinction between source
adapters and qualified artifacts. For existing state, read
[home-layout diagnosis](../guides/configuration/diagnose.md#resolve-a-legacy-home-rejection) and [storage compatibility](../guides/daemon/storage.md)
before changing binaries or directories. Keep the old matching installation and
verified backup until the new installation has passed its required checks.

[Documentation index](../README.md)
