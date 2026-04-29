"""
High-performance HTTP/2 ASGI server for
FastAPI, Starlette, and similar apps.

h2corn is optimized for h2c behind a trusted reverse proxy,
with optional direct TLS support for TCP deployments.
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
