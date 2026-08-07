from collections.abc import Awaitable, Callable
from typing import final

from h2corn._config import Config
from h2corn._server import TlsMaterial
from h2corn._types import Application, State

__all__ = [
    '_LifespanHandoff',
    '_PreparedTls',
    'emit_banner',
    'prepare_tls',
    'serve_fds',
]

@final
class _LifespanHandoff:
    """Exact, immutable Python-to-Rust ownership handoff for an active primary
    lifespan runner. Implementation object for secondary-loop state — not public
    API; the Python name is private.
    """
    def __new__(
        cls,
        app: Application,
        state: State,
        required: bool,
        startup_timeout: float | None,
        shutdown_timeout: float | None,
    ) -> _LifespanHandoff:
        """Create and return a new object.  See help(type) for accurate signature."""

@final
class _PreparedTls:
    """Validated, immutable TLS acceptor state prepared once from PEM bytes.

    Built while the process can still read the key files; workers reuse the
    same value after privilege drop and never reopen the paths.
    """

def prepare_tls(
    config: Config,
    tls_material: TlsMaterial | None = None,
) -> _PreparedTls:
    """Convert PEM material into an immutable native TLS acceptor, or plaintext.

    Runs the same `server_config` extraction serving uses so `--check-config`
    rejects bad cert/key/CA bytes before any worker starts.

    Parameters
    ----------
    config : Config
        The server configuration to extract from.
    tls_material : _TlsMaterial, optional
        PEM certificate, key and CA bytes; ``None`` serves plaintext.

    Returns
    -------
    _PreparedTls
        The prepared acceptor, to be handed to every worker.
    """

def emit_banner(config: Config, tls: _PreparedTls) -> None:
    """Print the startup banner for a validated server configuration.

    Parameters
    ----------
    config : Config
        The validated server configuration.
    tls : _PreparedTls
        The prepared acceptor, which decides whether the banner says HTTPS.

    Returns
    -------
    None
    """

def serve_fds(
    app: Application,
    fds: list[int],
    config: Config,
    shutdown_trigger: Awaitable[str],
    retire_trigger: Callable[[], None] | None = None,
    lifespan_handoff: _LifespanHandoff | None = None,
    ready_trigger: Callable[[], None] | None = None,
    quiesce_fd: int | None = None,
    *,
    prepared_tls: _PreparedTls,
) -> Awaitable[None]:
    """Adopt listener file descriptors and run one worker until shutdown.

    Takes ownership of every descriptor in `fds` and of `quiesce_fd`: they are
    closed when serving ends, and also when startup fails. Callers pass a
    descriptor they have already detached, never one they still hold — see
    `_socket._CreatedListener.transfer`.

    `prepared_tls` is required: PEM is converted once in `prepare_tls` and
    reused here. There is no path that reopens certificate files in a worker.

    Parameters
    ----------
    app : object
        The ASGI application to serve.
    fds : list of int
        Listener descriptors to adopt; ownership transfers to this call.
    config : Config
        The validated server configuration.
    shutdown_trigger : object
        Awaited to begin a graceful shutdown.
    retire_trigger : object, optional
        Awaited to stop accepting while draining in-flight requests.
    lifespan_handoff : _LifespanHandoff, optional
        Carries lifespan state from the parent process.
    ready_trigger : object, optional
        Resolved once the worker is accepting connections.
    quiesce_fd : int, optional
        Descriptor signalling quiesce; ownership transfers to this call.
    prepared_tls : _PreparedTls
        The acceptor built once by ``prepare_tls``.

    Returns
    -------
    Awaitable[None]
        Completes when the worker has stopped serving.
    """
