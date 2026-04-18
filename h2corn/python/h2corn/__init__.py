"""
High-performance ASGI server optimized for use behind a trusted reverse proxy.

h2corn prioritizes raw throughput and ASGI standard compatibility. By design,
it delegates TLS termination and edge security to a trusted proxy layer.
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
