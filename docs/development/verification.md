# Select checks that prove the changed behavior

Run commands from the repository root after [development setup](setup.md).
Choose the owning package's tests first, then integration gates affected by the
change. A successful check establishes only its executed scope and target.
The [test directory guide](../../tests/README.md) maps suites and fixtures;
[scripts/](../../scripts/README.md) contains developer commands and shared tools.

| Change or claim | Useful evidence | Limit |
| --- | --- | --- |
| Docs navigation | Relative-link, anchor, referenced-path and whitespace checks | Does not compile code or validate behavior |
| Mermaid | Parse, render, and visually inspect labels, arrows, clipping and supported themes | Parsing alone does not prove readability |
| Pure lifecycle or values | Domain/Foundation unit and conformance tests | No OS or transport behavior |
| Commit/effect ordering | Application command/attempt tests with controlled ports | Does not prove native process termination |
| Persistence and recovery | SQLite transaction, maintenance and crash-injection tests | Distinguish returned-error rollback from abrupt-process and power-loss behavior |
| Public clients | SDK product/recovery tests, Python mock tests, isolated live examples | Python mocks do not exercise its bridge subprocess |
| Native OS behavior | Target-native transport/runtime tests and platform smoke scripts | Compiling a cfg-disabled adapter proves nothing about it |
| Release installation | Package conformance and bundle qualification | No publication authority or invented release availability |

## Focused checks

These examples exercise representative owners without selecting every workspace
test. They require the pinned toolchain and dependency cache. Check crate guides
for native-process fixture requirements before execution.

```sh
cargo test --locked -p undercurrent-application --lib commands::command_tests
cargo test --locked -p aixand-undercurrent --test product_client
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=python/src python3 -m unittest discover -s python/tests -v
git diff --check
```

Expected result: each command exits zero. Preserve the failing test name, target,
revision, and typed stage when reporting a failure. Avoid copying terminal or
environment payloads into public issue reports. Cargo retains build artifacts;
the Python mocks and whitespace check create no runtime state. Real-daemon
fixtures have their own cleanup and retain evidence if termination is uncertain.

## Project validation runner

The normal developer commands also run the gates used by the
[product checks workflow](../../.github/workflows/product-checks.yml):

```sh
./dev.sh check
./dev.sh test
```

`check` includes naming policy and locked, warning-denied workspace Clippy with
all targets and features, alongside formatting, documentation, repository policy,
package metadata, and dependency notices. `test` first builds the native binaries,
then runs workspace unit, binary, integration and documentation tests with all
features, Python SDK tests, and every tooling test group. Cargo continues through
other test targets after a failure. Platform-specific tests still require their
owning OS. The Linux CI job also installs Zsh/Fish fixtures and runs group,
live-environment, and shell-detection conformance.

The maintained [runner](../../scripts/validate.py) exposes these stages:
`metadata`, `formatting`, `documentation`, `tooling`, `architecture`, `local_identity`, `clippy`, `tests`,
`rustdoc`, `package`, `release`, `configuration`, `smoke`, `recovery`, and
`release_bundle`. Inspect the stage implementation before executing it: several
build the workspace or start isolated native processes.

```sh
python3 scripts/validate.py --list
python3 scripts/validate.py --stage rustdoc --report target/standalone-validation/rustdoc.json
```

The Rustdoc stage uses `RUSTDOCFLAGS=-Dwarnings` with locked workspace docs,
all features, and no dependency docs. The `tests` stage separately includes
doctests. This runner forces offline Cargo, records logs and an
`executed_stages_only` report, and stops on the first failed stage. A passed
single-stage report is not a passed full validation run.

The `tooling` stage checks the vendor integrity policy, E2E catalog and runner,
subprocess cleanup, CI logging, and validation reporting.
Discover feature regressions with `python3 tests/e2e/run.py --list`; select
a test with `--test NAME --bin-dir target/debug`. Tests marked manual are not
implied by the validation runner.

For native scope, consult [platform coverage](platform-testing.md).
For bundles, follow [release qualification](releases.md).

## Review the reader outcome

Follow the [documentation workflow](documentation.md) and run
`python3 scripts/checks/check-docs.py --root .` for selected pages, local links
and anchors, structure, and diagram accessibility metadata. The `documentation`
stage also runs the checker's regression suite. External URLs, raw HTML links,
example execution, and rendered-diagram review need separate verification.

Check that commands name their directory, prerequisites, placeholders, expected
result, failure handling and cleanup. Compare every guarantee with its owning
schema, registry, implementation and meaningful tests. Follow all inbound links
when moving a page. Keep one source for examples and link to it from guides.

Report exact commands actually run separately from suggested or pending checks.
When another job owns a check, mark it unverified until its results arrive.

The metadata stage also runs [the naming policy](../../scripts/checks/check-names.py).
Product schemas, connection capabilities, telemetry, and product environment
variables use Undercurrent names. Company branding, user label namespaces,
organization links, and shared installer/checker names remain allowed. The
Foundation NOTICE preserves source attribution. Retired product namespaces and
company suite branding are rejected. Dependency and architecture checks enforce
standalone build boundaries. The naming policy
scans tracked and untracked non-ignored text, including documentation and tests.

[Documentation index](../README.md)
