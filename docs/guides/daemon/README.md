# Operate and diagnose the local daemon

Choose a task for the installation you intend to operate. The daemon controls
one local Undercurrent home; its private session host supervises the managed
processes. These guides assume `uc` is installed; run it as the owning OS user.

## Start and verify

- [Start the daemon](operations.md#start-the-daemon), for example before connecting an SDK client.
- [Check status and diagnostics](operations.md#check-status-and-diagnostics) for your current home.
- **Unexpected automatic startup:** [check which CLI commands start a daemon](cli-lifecycle.md).
  That guide explains endpoint classification and ownership checks.

## Inspect bounded diagnostics

Use [diagnostics and evidence bundles](diagnostics.md) to choose a section,
collect a private bundle, and interpret counters and cursor limits. For main
settings, start with [configuration](../configuration/README.md); for live
observation rules, use [profile editing](../configuration/profiles.md).

## Shut down or replace intentionally

Follow [stop and restart](operations.md#stop-or-restart-with-managed-work-in-mind)
when you intend to terminate the service and its supervised work. To leave
sessions running, [detach an attached view](../cli/interactive.md) or close your SDK
client. Shutdown timeout and response loss require investigation in the same
home; see [startup and shutdown diagnosis](operations.md#diagnose-startup-and-shutdown-failures).

Maintainers can trace the [durable shutdown and final drain](../../../crates/undercurrent-daemon/docs/internals/lifecycle/startup-and-shutdown.md#graceful-product-shutdown).

## Preserve state during recovery

Start with [troubleshooting](../troubleshooting.md) and [storage recovery limits](storage.md).
Keep the database, host receipts, authentication files, and locks together while
investigating. Reset deliberately discards state; it does not resolve unknown
mutation effects. The [operating guide](operations.md#diagnose-startup-and-shutdown-failures)
explains the relevant failure categories.

[Documentation index](../../README.md)
