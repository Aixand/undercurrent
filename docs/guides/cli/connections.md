# Save and switch local connections

Use connection profiles when several Undercurrent instances run under your OS
account. Switching selects another instance; sessions and data stay where they
were created. These profiles belong to the native `uc` CLI and are separate from
the process detection profiles managed by `uc profile`.

```sh
uc settings show
uc settings profiles list
uc settings profiles add testing --home /absolute/path/to/uc-testing
uc settings set profile testing
uc ls
uc settings set profile default
```

On Windows, use an absolute native path such as `C:\dev\uc-testing`. The built-in
`default` always selects `$HOME/.aixand/undercurrent`, or
`%USERPROFILE%\.aixand\undercurrent` on Windows. It cannot be removed or redirected.

For one invocation, without changing the saved selection:

```sh
uc --connection testing ls
uc settings profiles show testing
```

Selection order is an explicit `--connection`, then `UNDERCURRENT_HOME`, then
the saved active profile, then `default`. An empty or invalid environment home
is an error. `uc settings show` reports the saved selection and effective source.
No global environment variable or shell profile is edited.

Settings live in `$HOME/.aixand/undercurrent-cli/settings.toml`, using
`%USERPROFILE%` on Windows. Updates use a lock and atomic file replacement.
Read-only settings commands do not create a store. Settings commands do not
load a selected workspace's configuration or start its daemon, so a broken or
missing workspace cannot prevent `uc settings set profile default`.

## Connections owned by applications

For an existing application-managed instance, disable ordinary automatic startup:

```sh
uc settings profiles add workspace --home /absolute/workspace/uc --auto-start never
uc settings set profile workspace
```

Commands connect to that instance and fail if it is unavailable; they do not
fall back to another profile or start an empty instance. Explicit `uc daemon`
operations still operate on the selected instance. Open the owning application
to start its complete workspace.

A fresh independent UC installation uses `default`; reinstalling software
preserves saved CLI settings.

Applications can register a connection atomically:

```sh
uc settings profiles register workspace --home /absolute/workspace/uc --owner example-app --auto-start never --select-if-new
```

Registration updates only a profile with the same recorded owner. It rejects
unowned profiles or another owner. `--select-if-new` selects only a newly created
profile; retries do not reclaim the user's active selection. This is ownership
metadata for applications under the same OS account, not an authorization boundary.

```sh
uc settings profiles remove testing
```

Removal forgets the definition and preserves instance data. Removing the active
profile selects `default`. Running commands keep their original connection.
Daemons, Python clients, and other SDK clients do not read the CLI settings file;
they continue using their explicitly supplied home or their normal defaults.

If the global settings file is malformed, `uc --connection default` bypasses
saved selection without changing that file. `uc debug collect --offline` can
still produce a partial report; it does not fall back to another instance.
