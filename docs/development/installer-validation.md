# Qualify a portable product installation

Use this guide before distributing a native product artifact. Run commands from
the repository root on the target OS, using the pinned packaging tools in
[build prerequisites](../getting-started/product-installer.md#build-and-qualify).
A local pass qualifies the tested artifact and host; it does not publish a
release or establish support for another platform or minimum OS version.

## Build and verify an artifact

Choose unused output directories and report filenames. The examples use ignored
`dist/` and `target/` paths. Replace them consistently for subsequent runs.

```sh
./dev.sh package --output dist/installer-check/release
./dev.sh verify --artifact dist/installer-check/release --install \
  --report target/installer-check/verification.json
python3 scripts/prepare_product_runtime.py --bootstrap-output dist/installer-check/bootstrap
python3 scripts/qualify_product_installation.py dist/installer-check/release \
  --exercise-upgrade --bootstrap dist/installer-check/bootstrap \
  --report target/installer-check/lifecycle.json
```

On Windows, use `.\dev.ps1` for developer commands and `python` or `py -3`
for Python commands. Each command must exit zero. The lifecycle report must
contain `verified: true`, the expected `target`, and a `manifest_sha256` matching
the tested artifact's `release-manifest.json`. Retain both reports with that artifact and its bootstrap.
Inspect failures before retrying; a partial run does not establish qualification.

## Understand the coverage

The [product verifier](../../scripts/verify_product.py) imports the installed SDK,
starts a disposable daemon, creates a managed process, exchanges PTY input and
output, and shuts down the process and daemon.

The [lifecycle qualifier](../../scripts/qualify_product_installation.py) tests:

- License acceptance before store mutation and installation without system
  Python, Rust, uv, or dependency downloads.
- Shared and application-private ownership, repeated setup, owner conflicts,
  and simultaneous isolated daemon homes.
- Native PTY and SDK access, connection profiles, reinstall, and corrupt-upgrade
  rejection.
- Quiesced backup, stateful-rollback rejection, stopped uninstall, and database
  preservation when owned software is removed.
- Generated bootstrap installation with Python absent from `PATH`, when
  `--bootstrap` is supplied.

`--exercise-upgrade` creates a marked assembly fixture with a different product
version. Native code and database schema remain unchanged. This tests installer
activation and retention, not compatibility with another native release or
storage migration. Do not export the derived fixture.

## Retain evidence and clean up

Record the source revision, native target, build-host OS/libc baseline, artifact
digest, tool versions, and executed commands with the reports. Keep transient
logs and reports under ignored output directories, rather than adding run
counts or debugging history to this guide.

The qualifier uses temporary software stores and isolated runtime homes. Its
successful path stops its daemons and removes temporary installations. If a run
fails, review the error and confirm its processes have stopped before removing
remaining test state. Packaging outputs and reports remain for inspection.
Remove those chosen output directories when you no longer need the evidence.

Repeat qualification natively for each distributed target. Clean-machine and
minimum-OS checks require their own evidence. Follow
[release qualification](releases.md) for publication gates and
[portable installation](../getting-started/product-installer.md) for end-user steps.
