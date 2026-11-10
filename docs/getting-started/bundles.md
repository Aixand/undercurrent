# Install matching standalone artifacts

Hosted downloads require maintainer-confirmed working endpoints and a published
bundle for your platform. Availability is not established for this source
snapshot. Use [source installation](installation.md#install-for-source-development)
or a supplied [portable product artifact](product-installer.md) instead.


Use this guide to install a published release, build from source, or verify a
separately supplied bundle. Keep `uc`, `uc-daemon`, and `uc-session-host` from
the same release or source build.

For a published Linux/macOS release, use the public installer:

```sh
curl -fsSL https://install.aixand.org | sh -s -- undercurrent
```

It downloads the matching bundle from `downloads.aixand.org` and verifies it
before installation. See [installation and first use](installation.md) for
curl/Python prerequisites, published platform availability, license acceptance,
install paths, verification, and a first-session walkthrough.

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
