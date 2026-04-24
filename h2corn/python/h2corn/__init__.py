"""
High-performance HTTP/2 ASGI server for
FastAPI, Starlette, and similar apps.

h2corn is optimized for h2c behind a trusted reverse proxy,
with optional direct TLS support for TCP deployments.
"""

from ._config import Config, ProxyProtocolMode
from ._server import Server, serve
from ._types import ASGIApp

__all__ = (
    'ASGIApp',
    'Config',
    'ProxyProtocolMode',
    'Server',
    'serve',
)
