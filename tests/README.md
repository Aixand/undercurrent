# Undercurrent tests

This directory contains repository-level scenarios, fixtures, and suite runners.
Run commands from the repository root. Start with the
[verification guide](../docs/development/verification.md) to select a check.

- [Architecture tests](architecture/README.md) check dependency boundaries.
- [End-to-end tests](e2e/README.md) exercise matching native binaries.
- [Tooling tests](tooling/README.md) check scripts and runners.
- [Package conformance](../docs/development/testing/suites.md#package-conformance)
  covers bundle verification, installation, and native filesystem behavior.

Rust integration tests live in `crates/*/tests/`. Python SDK tests live in
`python/tests/`.
