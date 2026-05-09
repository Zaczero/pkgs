"""
High-performance HTTP/2 ASGI server for FastAPI, Starlette, and similar apps.

`h2corn` is optimized for `h2c` behind a trusted reverse proxy, with optional
direct TLS support for TCP deployments. Application traffic stays on HTTP/2
end-to-end instead of being downgraded to HTTP/1.1 inside the trust boundary.

The two entrypoints are:

- [`serve`][h2corn.serve] — start the server through the multi-worker supervisor
  (Unix) or in-process (Windows). This matches the behavior of the `h2corn` CLI.
- [`Server`][h2corn.Server] — embed a single-worker server in your own event loop.

Configuration is provided through [`Config`][h2corn.Config], which is also
constructable from environment variables or a TOML file.
"""

from ._config import Config, ProxyProtocolMode
from ._types import ASGIApp

TYPE_CHECKING = False

if TYPE_CHECKING:
    from ._server import Server, serve

__all__ = (
    'ASGIApp',
    'Config',
    'ProxyProtocolMode',
    'Server',
    'serve',
)


def __getattr__(name: str):
    if name in {'Server', 'serve'}:
        from . import _server

        return getattr(_server, name)
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
