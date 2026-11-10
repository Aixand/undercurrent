# Locate the credential-resolution boundary

This overview locates the credential-resolution contract and its implementation.
Use [version ownership](../reference/compatibility.md)
when matching artifacts.

Ordinary daemon composition installs `UnavailableSecretResolver`. Supplying
secret references therefore does not configure an external credential provider.
A trusted deployment can inject a resolver through daemon composition. That is
an internal composition boundary, not a public SDK credential-storage service.

The current flow and its lifetime guarantees are owned by
[execution and secrets](../../crates/undercurrent-application/docs/internals/execution/commands-and-secrets.md)
and [runtime launch](../../crates/undercurrent-runtime/docs/internals/processes/launch.md). Retained
launch configuration stores opaque references. Resolved values are ephemeral
at launch and must not enter diagnostic payloads or durable configuration.
`SecretReference` validates shape; it does not verify authority or provenance.
Do not infer broad memory-zeroization guarantees from bounded wrapper cleanup.

Review [secrets.rs](../../crates/undercurrent-application/src/secrets.rs), the
resolver port in [ports.rs](../../crates/undercurrent-application/src/ports.rs), and
[composition](../../crates/undercurrent-daemon/src/composition.rs) before adding an
adapter. Authorization, resolution, execution and response delivery have
separate failure boundaries; losing a response after launch is not permission
to rerun with a new credential or attempt identity.

[Extension guide](../development/extending.md) · [Documentation index](../README.md)
