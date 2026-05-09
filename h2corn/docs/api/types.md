# Types

::: h2corn.ASGIApp
    options:
      show_signature: false

The ASGI 3 contract is documented in full at the
[ASGI specification](https://asgi.readthedocs.io/en/latest/specs/main.html).
`h2corn` accepts any callable matching `ASGIApp` — including
[FastAPI](https://fastapi.tiangolo.com/),
[Starlette](https://www.starlette.io/),
[Django](https://docs.djangoproject.com/en/stable/howto/deployment/asgi/) (`asgi.application`),
[Litestar](https://litestar.dev/), and
[Quart](https://quart.palletsprojects.com/).
