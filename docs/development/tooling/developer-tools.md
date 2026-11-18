# Undercurrent developer tooling

Use this map to find the script that owns a build, check, or qualification step.
Run commands from the repository root.

The [test suite map](../testing/suites.md) locates scenarios and fixtures.
See [verification](../verification.md) and
[platform coverage](../platform-testing.md) for check selection.

| Location | Responsibility |
| --- | --- |
| [`dev/`](../../../scripts/dev/) | Adopted installation-template revision-2 build, artifact, and lifecycle helpers. |
| [`product_payload.py`](../../../scripts/product_payload.py) | Native resources/notices and the independently exportable SDK wheel. |
| [`prepare_product_runtime.py`](../../../scripts/prepare_product_runtime.py) | Checksum-pinned interpreter preparation and bootstrap generation. |
| [`verify_product.py`](../../../scripts/verify_product.py) | Installed native daemon, SDK, and real PTY verification in a disposable home. |
| [`qualify_product_installation.py`](../../../scripts/qualify_product_installation.py) | Shared/private lifecycle, isolated daemons, backup, upgrade fixture, and bootstrap proof. |
| [`validate.py`](../../../scripts/validate.py) | Current validation stages, including script self-tests. |
| [`checks/validate-package-manifest.py`](../../../scripts/checks/validate-package-manifest.py) | Validator declared by package metadata and schema. |
| [`check-architecture.py`](../../../scripts/check-architecture.py) | Public architecture-check entrypoint. |
| [`checks/`](../../../scripts/checks/) | Formatting, dependency, IPC security, package and vendor integrity checks. |
| [`ci/`](../../../scripts/ci/) | CI logging, shared native Windows stages, and disposable VM validation. |
| [`release/`](../../../scripts/release/) | Bundle construction, installation and adversarial verification. |
| [`release/third_party_licenses.py`](../../../scripts/release/third_party_licenses.py) | [Reviewed dependency inventory and bundled notices](../../../licenses/README.md); offline freshness checks and regeneration. |
| [`release/automation.py`](../../../scripts/release/automation.py) | [Version-tag release publication](../distribution/releases.md#publish-a-version-tag), download archives, checksums, and crates.io recovery. |
| [`docs/render_demos.py`](../../../scripts/docs/render_demos.py) | [Record and verify terminal demos](../../assets/demos/README.md). |

Use Python 3.11+ for developer tooling. Shared runners use the standard library;
individual E2E tests may need platform tools such as PowerShell or `pywinpty`.
The [Windows workflow runner](../../../scripts/ci/windows-native.py) requires Python 3.13 to match
CI; the optional VM media preparer uses `uv` to install its pinned `pycdlib` dependency.
Process timeouts and cleanup live in [`scripts/_support/`](../../../scripts/_support/). Linux process-identity checks remain
Linux-specific. Shell and PowerShell snippets exercise actual shell behavior;
the native development installers and their installer-specific tests retain
their shell implementations. CI's Python logger executes generated Bash steps.

The [project validator](../verification.md#project-validation-runner) calls suites
under `tests/`. See the [tooling self-test guide](self-tests.md) and the
[E2E catalog and runner](../testing/end-to-end.md) for live product regressions.
[Bundle conformance](../distribution/releases.md) uses drivers in
`tests/conformance/package/` and the reusable tools in `scripts/release/`.
