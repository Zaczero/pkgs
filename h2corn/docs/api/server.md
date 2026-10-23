---
description: Reference for h2corn.serve() and the Server class.
---

# Server entrypoints

Use [`serve()`][h2corn.serve] to run a standalone server, or
[`Server`][h2corn.Server] to control one inside an existing event loop.
For startup, readiness, and shutdown examples, see [Embedding](../embedding.md).

::: h2corn.serve

::: h2corn.Server
    options:
      members:
        - addresses
        - releasing
        - serve
        - shutdown
        - wait_started
