# Find a test suite or fixture

Use the [verification guide](../verification.md) to select evidence, then use
this map to locate the owning test. Run test commands from the repository root.

| Location | Coverage |
| --- | --- |
| [`tests/architecture/`](architecture.md) | Crate dependency boundaries and supported external consumers. |
| `tests/conformance/` | Package, installation, and CLI contracts with their fixtures. |
| [`tests/e2e/`](end-to-end.md) | Live CLI, lifecycle, configuration, and platform behavior. |
| [`tests/tooling/`](../tooling/self-tests.md) | Developer tooling, discovery, cleanup, and failure reporting. |

Rust integration tests stay with their owners in `crates/*/tests/`. Python SDK
tests live in `python/tests/`.

## Package conformance

- [Adversarial conformance](../../../tests/conformance/package/release_bundle_qualification_test.py)
  checks synthetic invalid bundles without executing their artifacts.
- [Native platform bundle tests](../../../tests/conformance/package/test-platform-bundle.py)
  exercise verification, installation, and installed product behavior.
- [Linux installation qualification](../../../tests/conformance/package/run-release-bundle-qualification.py)
  verifies isolated installation, runtime boundaries, and cleanup.
- [Windows bundle tests](../../../tests/conformance/package/windows_release_bundle_test.py)
  check native permission and filesystem behavior on Windows.

Follow the [release bundle guide](../distribution/releases.md) for prerequisites
and the [platform evidence guide](../platform-testing.md) for check scope.
