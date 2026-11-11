# Assemble and verify release bundles

Use this maintainer guide to assemble a release bundle from matching build
artifacts, verify its contents, and test installation into an isolated prefix
before distribution.

For automated publication from this checkout, follow
[Publish a version tag](#publish-a-version-tag). The procedures below also cover
the separate native bundle qualification tooling.

The owner is [release_bundle_qualification.py](../../scripts/release/release_bundle_qualification.py).
It provides `assemble`, `verify`, `archive`, `unpack`, `install`, and
`adversarial` subcommands. The verifier checks the expected payload and hashes;
the installer requires explicit license acceptance and a destination prefix.
These commands do not confer trust on artifacts from an unknown supplier.

## Review licenses before distribution

Apache-2.0 covers Undercurrent's original material. Vendored and other
third-party components retain their own licenses. Do not replace their notices
with the project's license or imply that all bundled material is Apache-2.0.
Keep the canonical root `LICENSE` unchanged; company and project attribution
belongs in the relevant documentation or attribution notices.

The [dependency review and notice collection](../../licenses/README.md) records
the checked source graph, selected license alternatives, native components,
and packaging evidence. Check that this collection still matches the checkout:

```sh
python3 scripts/release/third_party_licenses.py check
```

For each release, record the source commit, lockfiles, actual compiler,
target platforms, and enabled features alongside the inventory hash. The
collection conservatively includes normal/build dependencies for supported
targets. Match it to the actual build, including native libraries, generated
assets, SDK packaging, and any components outside Cargo. Follow the collection's
regeneration instructions after changing those inputs. Declared package
metadata alone does not establish permission or satisfy notice duties.

Before publication, maintainers must:

1. Review the actual licenses and provenance of distributed components. Record
   each component's name, version/source revision, applicable license terms,
   and, for an `OR` expression, the chosen license. Meet every applicable term
   of an `AND` expression and examine any exceptions or nonstandard declarations.
2. Preserve required copyright, license, and attribution notices, including
   the [foundation notice](../../crates/undercurrent-foundation/NOTICE) and the
   licenses and provenance files under `vendor/`. Record local modifications
   where the applicable license requires it.
3. Include required license texts and notices with each distributed artifact,
   and satisfy any additional obligations imposed by its components. Check
   source archives, binary bundles, and SDK packages individually.
4. Retain the inventory and review evidence alongside the release's validation
   evidence. Resolve missing permission, unclear terms, or incompatible use
   before distributing the affected material.

The bundle verifier requires the root license, foundation notice, vendored
license/provenance payloads, and the complete reviewed dependency collection.
`license.json` links the inventory, combined notices, and Rust standard-library
notices. Their contents must match the reviewed collection, whose lockfile,
manifest, and vendor inputs must still be current. If a distribution review
identifies additional required files, extend the collection and bundle verifier
before publication; do not bypass expected-file or freshness checks.

Follow the [security support policy](../../SECURITY.md#supported-versions) for
each release and document any additional maintained release lines there.
Report uncertain licensing or missing attribution to
[corporate@aixand.org](mailto:corporate@aixand.org).

## Inspect a supplied bundle

From the repository root with Python 3.11+, use the unpacked bundle directory
as `<BUNDLE_DIR>` and select its actual declared platform:

```sh
python3 scripts/release/release_bundle_qualification.py verify --bundle '<BUNDLE_DIR>' --platform linux-x86_64-gnu
```

The placeholder must be replaced; it is not a download location. Other parser
choices are `macos-arm64`, `macos-x86_64`, and `windows-x86_64-msvc`.
Successful verification exits zero and reports verification results. A missing,
extra, unsafe, or digest-mismatched payload must be investigated against the
supplier's matching evidence. Do not remove a required file from the expected
set just to make verification pass. Read-only verification needs no process cleanup.

## Run isolated installation proof

The Linux-focused wrapper expects existing release binaries and Linux process
identity facilities; it does not build, publish or fetch a release. From the
repository root on its supported host:

```sh
python3 tests/conformance/package/run-release-bundle-qualification.py --artifact-dir "$PWD/target/release" --report "$PWD/target/release-bundle-check.json"
```

This starts processes and installs into a temporary prefix. Run it only in a
development/qualification environment, after matching binaries are built. It
checks bundle assembly, adversarial validation, installation and real installed
behavior, with owned-process cleanup. Inspect the exit status, report and
retained diagnostics. If cleanup is unresolved, preserve its evidence and
runtime root until process identity is reconciled.

The [native platform bundle driver](../../tests/conformance/package/test-platform-bundle.py)
and [platform workflow](../../.github/workflows/platform-contracts.yml) cover the
target-specific route. The validation runner's `release_bundle` stage is a
separate executed-stage result, not a claim that all stages passed.

## Publish a version tag

The [release workflow](../../.github/workflows/release.yml) runs when you push a
`v<SemVer>` tag. It requires the tagged commit to belong to `main` and all
declared product versions to match. Diagnostic tags under `ci/` do not publish.
Branch and pull-request checks remain read-only.

### Rehearse installer qualification without publishing

Normal pushes and pull requests run the four-platform
[product checks workflow](../../.github/workflows/product-checks.yml). They do not
build shipping packages or run full installer lifecycle qualification.

The [product installer workflow](../../.github/workflows/product-installer.yml)
first runs those checks, then packages and verifies the product and SDK on all
four platforms. It exercises installation, upgrades, rollback rejection, and
uninstall. The release workflow calls it for version tags.

To run the same installer qualification manually, use GitHub Actions' **Run
workflow** control for **Product installer**, or run from the repository root
with an authenticated GitHub CLI:

```sh
gh workflow run product-installer.yml --ref main
```

A manual run uploads qualification artifacts. It does not publish crates or a
GitHub Release. Versioned release archives remain gated on version-tag refs;
publication is owned by the release workflow.

### Configure publication

Before the first release:

1. Confirm ownership or availability of all eight crate names listed below.
2. Create a crates.io API token with permission to publish those crates.
3. Create the GitHub environment `release`. Allow version tags in its deployment rules.
4. Add `CARGO_REGISTRY_TOKEN` as an Actions secret in that environment or repository.
5. Allow GitHub Actions to create releases using the job's `contents: write` permission.

The workflow uses token authentication. Trusted Publishing is an alternative
that requires a separate crates.io publisher configuration and workflow change.
Use the token crate-name pattern `aixand-undercurrent*` to cover the SDK and its
supporting crates. Enable publishing new crates for the first release and new
versions for subsequent releases.
Do not put tokens in tracked files. Environment reviewers are optional; if you
configure them, publication waits for their approval.

The explicit crates.io publication set is:

| Crate | Purpose |
| --- | --- |
| `aixand-undercurrent-foundation` | Shared values and privacy primitives |
| `aixand-undercurrent-wire` | Framing and handshake codecs |
| `aixand-undercurrent-protocol` | Product requests, responses, and events |
| `aixand-undercurrent-multiplex` | Stream multiplexing |
| `aixand-undercurrent-platform-windows` | Windows dependency of local transport |
| `aixand-undercurrent-transport-core` | Transport session engine |
| `aixand-undercurrent-transport-local` | Local IPC transport |
| `aixand-undercurrent` | Public Rust client SDK |

The CLI, daemon, storage implementation, and modified vendored terminal crates
are not published to crates.io. Executables ship in the product downloads.
Cargo orders the selected dependency graph and waits for registry availability.

### Prepare and push the tag

Run commands from the repository root with Git, Python 3.11+, and Rust 1.95.0.
Complete the license review above before distributing any artifacts.

Update the workspace version in `Cargo.toml`, the exact internal dependency
versions in the published crates, product versions in `installation.toml`, and
the product version in `undercurrent-package.json`. Regenerate and review the
[license collection](../../licenses/README.md#verify-and-update) after manifest
changes. Refresh `Cargo.lock` and the external consumer lockfile when crate
versions change. Schema and protocol versions follow their own contracts.

Update `python/pyproject.toml` with the matching Python version. Stable versions
keep `X.Y.Z`; `-alpha`, `-beta`, and `-rc` map to `a0`, `b0`, and `rc0`.
Numbered suffixes retain their number: `0.1.0-rc.1` maps to `0.1.0rc1`.
Other suffixes fail release validation.

Configure a PyPI trusted publisher for `aixand-undercurrent-sdk`: owner `Aixand`,
repository `undercurrent`, workflow `release.yml`, environment `release`.
For the first publication, create a pending publisher in your PyPI account.
The release job uses GitHub OIDC with `id-token: write`; no PyPI token is needed.
See [PyPI publisher setup](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/).

Merge the prepared changes into `main`. Start from a clean checkout of that
commit and fetch the current remote references. For the current beta version:

```sh
git fetch origin main --tags
git tag v0.1.0-beta
python3 scripts/release/automation.py check --tag v0.1.0-beta
python3 scripts/release/automation.py crates-check --tag v0.1.0-beta
git push origin v0.1.0-beta
```

Replace the tag consistently for another release. The two Python commands
validate locally without publishing. The push starts the release workflow.
The workflow never changes versions or creates version commits.

Publication waits for documentation, file policy, secret scanning, dependency
advisories, licensing, product installation, and deep native qualification.
Product jobs also package and compile the crates.io graph on each native host.
The publication job prepares a draft release, uploads downloads, publishes
crates and the Python SDK, then makes the GitHub Release public. Python packaging
CI tests the installed wheel on Python 3.11 through 3.14.

### Publish only the Python SDK

To publish the Python package from a commit on `main`, push the changes without
creating or moving a version tag. In GitHub Actions, open **Release**, select
**Run workflow**, and choose `main`. The manual run tests Python packaging on
Python 3.11 through 3.14, then publishes the tested Python 3.12 wheel to PyPI.
It uses the same `release.yml` trusted publisher and `release` environment.
It does not run native builds, publish crates, or create or modify a GitHub Release.
The run uses the commit selected when the workflow starts.

Use this path for the initial `aixand-undercurrent-sdk` publication at `0.1.0b0`.
This publishes the current Python source; it does not replace the SDK wheel
inside the existing `v0.1.0-beta` archives. Install the matching bridge separately.
The manual path checks wheel installation and SDK unit tests; it does not repeat
native runtime qualification. Check compatibility before publishing SDK behavior changes.

PyPI versions are immutable. Rerun the failed job in the original run to reuse
its tested wheel. A new run can rebuild different bytes; if PyPI already has
that version, use a new version for changed contents. For the next full release,
bump the product and Python versions together, then push the new version tag.
Tag-triggered releases continue publishing all artifacts, including PyPI.

### Find release downloads

Each release contains product and SDK archives for `linux-x86_64-gnu`,
`macos-arm64`, `macos-x86_64`, and `windows-x86_64-msvc`.
Names use `undercurrent-<version>-<target>.tar.gz` and
`undercurrent-<version>-<target>-sdk.tar.gz`; Windows uses `.zip`.
`SHA256SUMS` covers all eight archives. GitHub also provides tagged-source archives.

Product archives contain `artifacts/`, `bootstrap/`, the descriptor,
installation and lifecycle evidence, and release metadata. SDK archives contain
their own `artifacts/`, the bootstrap, a consumer verification report, and release metadata. Payloads preserve
their license notices. Metadata records the commit, compiler, target, and license
inventory hash. No signing or builder attestation is implied.

For a product archive, extract it and use its `bootstrap/` with its `artifacts/` directory,
following the [product installation guide](../getting-started/product-installer.md#install-without-source-or-language-tools).
The bootstrap fetches a pinned helper interpreter; the product runtime is bundled.
SDK components use an application's Python environment rather than a product
store. Their wheels are under `artifacts/component/wheelhouse/`; install a wheel
with that environment's pip. `INSTALL.txt` explains component verification.
These downloads are portable bundles, not MSI, PKG, DEB, or RPM installers.
The workflow also publishes the qualified Linux SDK archive's universal wheel
to PyPI as `aixand-undercurrent-sdk`. The Python import remains `undercurrent`.
The wheel needs a separately installed matching runtime and `uc-python-bridge`.

Versions containing a prerelease suffix, including `-beta` and `-rc.1`, become
GitHub prereleases and are not marked as the latest stable release.

### Recover a failed publication

Inspect the failed job before retrying. Checks and builds publish nothing.
A publication failure can leave a draft GitHub Release, published crates, and a PyPI wheel.
Crates.io publication is incremental and cannot be rolled back as a transaction.

Rerun the failed jobs on the original workflow run and original tag. Matching
draft assets can be replaced while the release remains private. The publisher
checks an existing crate's registry checksum, source commit, and packaged bytes
before skipping it. `Cargo.lock` is excluded from the byte comparison because
Cargo's dry-run registry changes lockfile sources. All other files must match.
Yanked versions and conflicting contents stop publication. The PyPI upload uses
`uv publish`, which skips identical files on retries and rejects conflicting bytes.

Do not move a published tag or overwrite a crate version. Fix source defects
with a new version and tag. Once the GitHub Release is public, reruns refuse to
replace its assets. A failure after the final publish request needs inspection
of the release page before further action.

The helper and its rejection/recovery tests can be checked offline:

```sh
python3 tests/tooling/checks/release_automation_test.py
python3 scripts/checks/check-docs.py --root .
git diff --check
```

[Installation](../getting-started/bundles.md) · [Verification](verification.md) · [Documentation index](../README.md)
