# Undercurrent developer tooling

Run `python3 scripts/validate.py --all` for project validation, or
use `--list` and `--stage NAME` for focused checks. Cargo uses the pinned
toolchain offline; dependencies must already be cached. Logs and reports live
under `target/standalone-validation/`.

Test scenarios and fixtures live under [tests/](../tests/README.md).
See [verification](../docs/development/verification.md) and
[platform coverage](../docs/development/platform-testing.md) for check selection.

| Location | Responsibility |
| --- | --- |
| `dev/` | Adopted installation-template revision-2 build, artifact, and lifecycle helpers. |
| `product_payload.py` | Native resources/notices and the independently exportable SDK wheel. |
| `prepare_product_runtime.py` | Checksum-pinned interpreter preparation and bootstrap generation. |
| `verify_product.py` | Installed native daemon, SDK, and real PTY verification in a disposable home. |
| `qualify_product_installation.py` | Shared/private lifecycle, isolated daemons, backup, upgrade fixture, and bootstrap proof. |
| `validate.py` | Current validation stages, including script self-tests. |
| `checks/validate-package-manifest.py` | Validator declared by package metadata and schema. |
| `check-architecture.py` | Public architecture-check entrypoint. |
| `checks/` | Formatting, dependency, IPC security, package and vendor integrity checks. |
| `ci/` | CI logging, shared native Windows stages, and disposable VM validation. |
| `release/` | Bundle construction, installation and adversarial verification. |
| `release/third_party_licenses.py` | [Reviewed dependency inventory and bundled notices](../licenses/README.md); offline freshness checks and regeneration. |
| `release/automation.py` | [Version-tag release publication](../docs/development/releases.md#publish-a-version-tag), download archives, checksums, and crates.io recovery. |
| `docs/render_demos.py` | [Record and verify terminal demos](../docs/assets/demos/README.md). |

Use Python 3.11+ for developer tooling. Shared runners use the standard library;
individual E2E tests may need platform tools such as PowerShell or `pywinpty`.
The [Windows workflow runner](ci/windows-native.py) requires Python 3.13 to match
CI; the optional VM media preparer uses `uv` to install its pinned `pycdlib` dependency.
Process timeouts and cleanup live in `_support/`. Linux process-identity checks remain
Linux-specific. Shell and PowerShell snippets exercise actual shell behavior;
the native development installers and their installer-specific tests retain
their shell implementations. CI's Python logger executes generated Bash steps.

The project validator calls suites under `tests/`. Run
`python3 tests/tooling/run.py` for tool self-tests, or use the
[E2E catalog and runner](../tests/e2e/README.md) for live product regressions.
[Bundle conformance](../docs/development/releases.md) uses drivers in
`tests/conformance/package/` and the reusable tools in `scripts/release/`.
