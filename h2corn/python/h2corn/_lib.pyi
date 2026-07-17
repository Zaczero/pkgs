from collections.abc import Awaitable, Callable
from typing import final

from ._config import Config
from ._types import Application, State

__all__ = ['LifespanHandoff', 'emit_banner', 'serve_fds', 'validate_config']

@final
class LifespanHandoff:
    """Exact, immutable Python-to-Rust ownership handoff for an active primary
    lifespan runner. The Python wrapper remains usable by generic callbacks;
    h2corn consumes this object to call the original app directly and install
    the loop-local state dictionary on the main shard.
    """
    def __new__(
        cls,
        app: Application,
        state: State,
        required: bool,
        startup_timeout: float | None,
        shutdown_timeout: float | None,
    ) -> LifespanHandoff:
        """Create and return a new object.  See help(type) for accurate signature."""

def emit_banner(config: Config) -> None:
    """Print the startup banner for a validated server configuration."""

def serve_fds(
    app: Application,
    fds: list[int],
    config: Config,
    shutdown_trigger: Awaitable[str],
    retire_trigger: Callable[[], None] | None = None,
    lifespan_handoff: LifespanHandoff | None = None,
    ready_trigger: Callable[[], None] | None = None,
    quiesce_fd: int | None = None,
) -> Awaitable[None]:
    """Adopt listener file descriptors and run one worker until shutdown."""

def validate_config(config: Config) -> None:
    """Validate and normalize a Python `Config`, raising on invalid combinations."""
