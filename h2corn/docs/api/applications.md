---
description: ASGI application aliases, callables, and message unions exported by h2corn.
---

# ASGI applications and aliases

These aliases describe the callable boundary between an ASGI application and
h2corn. `Application` accepts either the precise `ASGIApp` contract or the
broader mutable-mapping annotations used by framework applications.

::: h2corn.ASGIApp
    options:
      show_signature: false

::: h2corn.FrameworkASGIApp
    options:
      show_signature: false

::: h2corn.Application
    options:
      show_signature: false

::: h2corn.Receive
    options:
      show_signature: false

::: h2corn.Send
    options:
      show_signature: false

::: h2corn.Scope
    options:
      show_signature: false

::: h2corn.ReceiveMessage
    options:
      show_signature: false

::: h2corn.SendMessage
    options:
      show_signature: false

::: h2corn.Message
    options:
      show_signature: false
