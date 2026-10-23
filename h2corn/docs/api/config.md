---
description: Reference for Config, RequestPolicy, construction helpers, and option literal types.
---

# Config

Use [`Config`][h2corn.Config] to configure the server and
[`RequestPolicy`][h2corn.RequestPolicy] for path-specific limits.
Constructor options are keyword-only. The
[Configuration reference](../configuration.md) lists their types, defaults,
CLI flags, environment variables, TOML keys, and precedence.

::: h2corn.Config
    options:
      show_signature: false
      members:
        - from_env
        - from_mapping
        - from_toml

::: h2corn.RequestPolicy
    options:
      show_signature: false

::: h2corn.ProxyProtocolMode
    options:
      show_signature: false

::: h2corn.LifespanMode
    options:
      show_signature: false

::: h2corn.CertReqsMode
    options:
      show_signature: false

::: h2corn.LoopImpl
    options:
      show_signature: false

::: h2corn.ServerHeaderMode
    options:
      show_signature: false

::: h2corn.LogFormat
    options:
      show_signature: false
