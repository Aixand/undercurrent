# Offline Rust crate examples

Use these exercises to trace mechanisms in the Undercurrent development
workspace at `0.1.0-beta`. Each page includes a source excerpt, a command,
and the expected evidence. All exercises run without a daemon. Treat
implementation crates as workspace internals. For external integration, use
the [SDK documentation](../../sdk/README.md) and the
[protocol crate documentation](../../../crates/undercurrent-protocol/docs/README.md).

| Mechanism | Walkthroughs |
| --- | --- |
| Values and state | [Canonical values](foundation-values.md), [session model and reconciliation](model-walkthrough.md) |
| Product and wire formats | [Product payloads](inspect-payload.md), [cancellation validation](inspect-cancellation.md), [wire decoding](inspect-frames.md), [partial I/O framing](framing-roundtrip.md) |
| Delivery and recovery | [Consumed credit and replay](bounded-delivery.md), [retained results](retained-result.md), [retained output gaps](output-gap.md) |
| Local configuration and storage | [Effective configuration](inspect-config.md), [SQLite and outbox recovery](repository-roundtrip.md) |

## Verify all examples

Run the [verifier](../../../scripts/docs/verify_crate_examples.py) from the
repository root with Python 3.11+, the
[pinned Rust toolchain](../../../rust-toolchain.toml), and cached dependencies.
On Linux or macOS, use the paths below or choose another writable build
directory and fresh evidence directory.

```sh
python3 scripts/docs/verify_crate_examples.py \
  --target-dir /tmp/uc-crate-examples-target \
  --evidence-dir /tmp/uc-crate-examples-evidence
```

The verifier checks exit status and stdout. It also checks the configuration
report and that inspection leaves the fixture home unchanged. It prints the
evidence directory and writes a JSON record for each Cargo invocation there.
Diagnose Cargo failures using the captured stderr. Use a fresh evidence
directory for each rerun. Build artifacts remain in the target directory.
