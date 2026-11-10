# Verify product behavior and developer tools

Run commands from the repository root. [Developer tools](../scripts/README.md)
live in `scripts/`; this directory owns test scenarios, fixtures, and suite runners.

| Location | Coverage |
| --- | --- |
| [architecture/](architecture/README.md) | Crate dependency boundaries and supported external consumers. |
| `conformance/` | Package, installation, and CLI contracts with their fixtures. |
| [e2e/](e2e/README.md) | Live product behavior, grouped by CLI, lifecycle, configuration, and platform. |
| [tooling/](tooling/README.md) | Developer-tool correctness, including test discovery, cleanup, and failure reporting. |

Rust integration tests remain beside their owners in `crates/*/tests/`;
Python package tests remain in `python/tests/`.

The main entry point stays `python3 scripts/validate.py`. Use `--list` to
inspect its stages or `--stage tooling` to run tool self-tests. Discover live
regressions with `python3 tests/e2e/run.py --list`; build matching binaries before
running them. Discovery alone does not execute tests.

Bundle suites live in `conformance/package/`:

- [Adversarial conformance](conformance/package/release_bundle_qualification_test.py)
  checks synthetic invalid bundles without executing their artifacts.
- [Native platform bundle tests](conformance/package/test-platform-bundle.py)
  exercise verification, installation, and installed product behavior.
- [Linux installation qualification](conformance/package/run-release-bundle-qualification.py)
  verifies isolated installation, runtime boundaries, and cleanup.
- [Windows bundle tests](conformance/package/windows_release_bundle_test.py)
  check native permission and filesystem behavior on Windows.

Follow [verification](../docs/development/verification.md),
[platform testing](../docs/development/platform-testing.md), and
[bundle qualification](../docs/development/releases.md) for prerequisites and scope.
