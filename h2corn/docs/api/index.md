# API reference

The public Python API is intentionally small. Most users only touch
the top-level [`serve()`][h2corn.serve] function and a
[`Config`][h2corn.Config] instance.

::: h2corn
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

| Symbol                          | What it is                                                                  |
| ------------------------------- | --------------------------------------------------------------------------- |
| [`serve`][h2corn.serve]         | Start the server through the multi-worker supervisor (or in-process on Windows). |
| [`Server`][h2corn.Server]       | Embed a single-worker server in your own event loop.                        |
| [`Config`][h2corn.Config]       | Frozen dataclass holding every server option.                               |
| [`ProxyProtocolMode`][h2corn.ProxyProtocolMode] | Literal type for the `proxy_protocol` option.               |
| [`ASGIApp`][h2corn.ASGIApp]     | Callable type alias for an ASGI 3 application.                              |

For per-option descriptions, defaults, environment variables, and CLI
flags, see the [Configuration reference](../configuration.md).
