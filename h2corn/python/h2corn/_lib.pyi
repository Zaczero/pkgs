from collections.abc import Awaitable, Callable
from typing import final

from h2corn._config import Config
from h2corn._lifespan import PreparedApplication
from h2corn._server import TlsMaterial

__all__ = [
    '_PreparedTls',
    'emit_banner',
    'prepare_tls',
    'serve_fds',
]

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

    Args:
        config (Config): The server configuration to extract from.
        tls_material (TlsMaterial | None): PEM certificate, key and CA bytes;
            ``None`` serves plaintext.

    Returns:
        _PreparedTls: The prepared acceptor, to be handed to every worker.
    """

def emit_banner(config: Config, tls: _PreparedTls) -> None:
    """Print the startup banner for a validated server configuration.

    Args:
        config (Config): The validated server configuration.
        tls (_PreparedTls): The prepared acceptor, which decides whether the banner says HTTPS.

    Returns:
        None: The banner has been emitted.
    """

def serve_fds(
    prepared_application: PreparedApplication,
    fds: list[int],
    config: Config,
    shutdown_trigger: Awaitable[str],
    retire_trigger: Callable[[], None] | None = None,
    ready_trigger: Callable[[BaseException | None], None] | None = None,
    quiesce_fd: int | None = None,
    *,
    prepared_tls: _PreparedTls,
) -> Awaitable[None]:
    """Adopt listener file descriptors and run one worker until shutdown.

    Borrows every descriptor in `fds` and `quiesce_fd` for this synchronous call,
    duplicating them before returning the awaitable. The caller retains and may
    close the sources immediately after return; native duplicates are closed
    when serving ends or startup fails.

    `prepared_tls` is required: PEM is converted once in `prepare_tls` and
    reused here. There is no path that reopens certificate files in a worker.

    Args:
        prepared_application (PreparedApplication): The primary loop's prepared app and source.
        fds (list[int]): Listener descriptors to borrow and duplicate.
        config (Config): The validated server configuration.
        shutdown_trigger (Awaitable[str]): Awaited to begin a graceful shutdown.
        retire_trigger (Callable[[], None] | None): Called when accepting stops
            while in-flight requests drain.
        ready_trigger (Callable[[BaseException | None], None] | None): Called with
            ``None`` when the worker accepts connections, or a startup error
            before retained rollback.
        quiesce_fd (int | None): Descriptor signalling quiesce; borrowed and duplicated on Unix.
        prepared_tls (_PreparedTls): The acceptor built once by ``prepare_tls``.

    Returns:
        Awaitable[None]: Completes when the worker has stopped serving.
    """
