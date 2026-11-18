# aixand-undercurrent

Control and observe a running Undercurrent daemon with the blocking Rust SDK.
When version `0.1.0-beta` is available from your package source, add it with this
dependency alias:

```toml
[dependencies]
undercurrent = { package = "aixand-undercurrent", version = "0.1.0-beta" }
```

Use the API as `undercurrent::`. The source crate directory is `undercurrent-sdk`
and its library target is `undercurrent_sdk`.

[Connect, query, and watch](https://github.com/Aixand/undercurrent/blob/main/docs/sdk/rust/connect-and-query.md) with a
matching daemon. See the [public API reference](docs/reference/public-api.md),
[maintainer guides](docs/README.md), or [upstream repository](https://github.com/Aixand/undercurrent).
