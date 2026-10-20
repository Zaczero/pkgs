---
description: Reference for Config, RequestPolicy, construction helpers, and option literal types.
---

# Config

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

`Config(...)` options are keyword-only. Accepted Python input types, normalized
stored types, defaults, CLI flags, environment variables, TOML keys, and
precedence are listed in the [Configuration reference](../configuration.md).

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
