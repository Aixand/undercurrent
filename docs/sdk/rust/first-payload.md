# Encode and inspect a product payload

Learn how to build a current mutation, check its contract, fingerprint its
semantic content, and round-trip terminal bytes without connecting to a daemon.
This tutorial targets Rust integrators using this checkout's protocol crate
`0.1.0-beta` and product contract `0.1.0-beta`.

You need the complete checkout and the
Rust `1.95.0` toolchain pinned in [rust-toolchain.toml](../../../rust-toolchain.toml).
Run commands from the repository root. The crate has no optional Cargo features
and this example uses only existing dependencies. It has no OS-specific runtime
requirements, credentials, environment variables, or daemon prerequisite.

1. Read the maintained [inspect_payload example](../../../crates/undercurrent-protocol/examples/inspect_payload.rs).
   It builds a conditional stop request for a synthetic session but only encodes
   it in memory. `MutationContext::current` supplies the exact operation contract;
   `expected_revision` is semantic input and participates in its digest.
2. Run the example:

   ```sh
   cargo run --locked -p aixand-undercurrent-protocol --example inspect_payload
   ```

3. Check for this expected output (shown as an example, not a recorded run):

   ```text
   stop_session: contract valid; JSON round trip matched
   retry metadata: same digest; changed revision: different digest
   terminal: 3 bytes at offsets 41..44; digest verified
   ```

The example's assertions check that a different idempotency key leaves the
semantic fingerprint unchanged, whereas changing the expected session revision
changes it. This demonstrates digest construction, **not permission to retry**.
The [recovery guide](../../reference/protocol/recovery.md) explains the additional durable evidence needed
when a mutation may have run.

If Cargo reports an unavailable toolchain or dependency, use the repository's
approved development setup before retrying. `--locked` deliberately refuses to
rewrite dependency resolution. A codec capacity error means the configured
payload limit is too small; select bounds that fit the negotiated SDK payload
budget, rather than assuming a larger JSON message will fit a frame.

The program starts no processes other than itself and creates no sessions or
persistent runtime state. No runtime cleanup is required; Cargo build artifacts
remain under the configured target directory.

To send real requests, use [the Rust SDK](../../../crates/undercurrent-sdk/README.md) with
an authenticated local connection. Its finite calls and stream receives block;
sharing cloned clients shares a connection. The [Python package](../../../python/README.md)
provides an `asyncio` interface through `uc-python-bridge`, not these Rust methods
as Python coroutines. See [encoding boundaries](../../reference/protocol/encoding.md) before implementing
an adapter and [streams](../../reference/protocol/streams.md) before consuming output.
