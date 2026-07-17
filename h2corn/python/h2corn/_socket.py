from __future__ import annotations

import io
import os
import signal
import socket
import stat
import sys
from contextlib import contextmanager

from ._config import (
    Config,
    FdBindSpec,
    TcpBindSpec,
    UnixBindSpec,
    format_tcp_bind,
    parse_bind_spec,
)

TYPE_CHECKING = False

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path
    from typing import Any


@contextmanager
def swap_signal_handlers(handlers: Mapping[int, Any]):
    previous = {sig: signal.signal(sig, handler) for sig, handler in handlers.items()}
    try:
        yield
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def nonblocking_pipe() -> tuple[int, int]:
    if hasattr(os, 'pipe2'):
        return os.pipe2(os.O_NONBLOCK | os.O_CLOEXEC)
    read_fd, write_fd = os.pipe()
    try:
        for fd in (read_fd, write_fd):
            os.set_blocking(fd, False)
            os.set_inheritable(fd, False)
    except BaseException:
        os.close(read_fd)
        os.close(write_fd)
        raise
    return read_fd, write_fd


@contextmanager
def signal_wakeup_pipe():
    read_fd, write_fd = nonblocking_pipe()
    previous = signal.set_wakeup_fd(write_fd, warn_on_full_buffer=False)
    try:
        yield read_fd
    finally:
        signal.set_wakeup_fd(previous)
        os.close(read_fd)
        os.close(write_fd)


def drain_fd(fd: int) -> None:
    try:
        while True:
            if not os.read(fd, io.DEFAULT_BUFFER_SIZE):
                return
    except BlockingIOError:
        return


def _cleanup_bound_resources(
    sockets: Sequence[socket.socket],
    owned_socket_paths: Sequence[Path],
):
    for sock in sockets:
        sock.close()
    for path in owned_socket_paths:
        try:
            if stat.S_ISSOCK(os.lstat(path).st_mode):
                os.unlink(path)
        except FileNotFoundError:
            pass


def _build_unix_socket(
    path: str | os.PathLike[str],
    config: Config,
    *,
    owner_uid: int | None = None,
    owner_gid: int | None = None,
):
    uds_path = os.fspath(path)
    if sys.platform == 'win32':
        raise OSError('Unix sockets are not supported on Windows')

    try:
        existing = os.lstat(uds_path)
    except FileNotFoundError:
        existing = None
    if existing is not None:
        if not stat.S_ISSOCK(existing.st_mode):
            raise OSError(f'unix socket path exists and is not a socket: {uds_path}')
        os.unlink(uds_path)

    sock = socket.socket(
        socket.AF_UNIX,
        socket.SOCK_STREAM | getattr(socket, 'SOCK_NONBLOCK', 0),
    )
    sock.bind(uds_path)
    if owner_uid is not None or owner_gid is not None:
        os.chown(
            uds_path,
            -1 if owner_uid is None else owner_uid,
            -1 if owner_gid is None else owner_gid,
        )
    sock.listen(config.backlog)
    if config.uds_permissions is not None:
        os.chmod(uds_path, config.uds_permissions)
    if not getattr(socket, 'SOCK_NONBLOCK', 0):
        sock.setblocking(False)
    return sock


def _adopt_socket(fd: int):
    sock = socket.socket(fileno=fd)
    sock.setblocking(False)
    return sock


def _prepare_tcp_listener(sock: socket.socket, *, reuse_port: bool = False):
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    if reuse_port:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

    value = 512 if sys.platform == 'linux' else 1
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_FASTOPEN, value)
    except (OSError, AttributeError):
        pass

    if sys.platform == 'linux':
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_DEFER_ACCEPT, 1)
        except OSError:
            pass


def _finish_bound_socket(sock: socket.socket, backlog: int):
    sock.listen(backlog)
    if not getattr(socket, 'SOCK_NONBLOCK', 0):
        sock.setblocking(False)
    return sock


def _build_tcp_socket(host: str, port: int, config: Config):
    last_error = None
    for family, _, _, _, sockaddr in socket.getaddrinfo(
        host,
        port,
        type=socket.SOCK_STREAM,
        flags=socket.AI_PASSIVE,
    ):
        sock = socket.socket(
            family,
            socket.SOCK_STREAM | getattr(socket, 'SOCK_NONBLOCK', 0),
        )
        try:
            if family == socket.AF_INET6:
                try:
                    sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
                except OSError:
                    pass
            _prepare_tcp_listener(sock, reuse_port=config.reuse_port)
            sock.bind(sockaddr)
            return _finish_bound_socket(sock, config.backlog)
        except OSError as exc:
            sock.close()
            last_error = exc
    if last_error is None:
        raise OSError(f'could not resolve bind address: {host!r}')
    raise last_error


def _build_sockets(
    config: Config,
    *,
    socket_owner: tuple[int | None, int | None] = (None, None),
):
    sockets: list[socket.socket] = []
    owned_socket_paths: list[Path] = []
    shared_tcp_port: int | None = None
    owner_uid, owner_gid = socket_owner
    try:
        for bind in config.bind:
            spec = parse_bind_spec(bind)
            match spec:
                case TcpBindSpec(host, port):
                    sock = _build_tcp_socket(host, shared_tcp_port or port, config)
                    bound_port = sock.getsockname()[1]
                    if port == 0 and shared_tcp_port is None:
                        shared_tcp_port = bound_port
                    sockets.append(sock)
                case UnixBindSpec(path):
                    sockets.append(
                        _build_unix_socket(
                            path,
                            config,
                            owner_uid=owner_uid,
                            owner_gid=owner_gid,
                        )
                    )
                    owned_socket_paths.append(path)
                case FdBindSpec(fd):
                    sock = _adopt_socket(fd)
                    # AF_UNIX is absent on Windows; an adopted fd there is never
                    # a unix socket, so a missing constant compares unequal.
                    af_unix = getattr(socket, 'AF_UNIX', None)
                    if config.certfile is not None and sock.family == af_unix:
                        sock.close()
                        raise OSError('TLS is supported only on TCP listeners')
                    sockets.append(sock)
    except Exception:
        _cleanup_bound_resources(sockets, owned_socket_paths)
        raise
    return tuple(sockets), tuple(owned_socket_paths)


def bound_addresses(sockets: Sequence[socket.socket]) -> tuple[str, ...]:
    """Resolved bind addresses of `sockets`, in `Config.bind` string form.

    Unlike the configured bind strings, these carry the port the kernel
    actually assigned — meaningful when binding port 0.
    """
    addresses: list[str] = []
    for sock in sockets:
        name = sock.getsockname()
        if sock.family in {socket.AF_INET, socket.AF_INET6}:
            addresses.append(format_tcp_bind(name[0], name[1]))
        else:
            addresses.append(f'unix:{os.fsdecode(name)}')
    return tuple(addresses)


@contextmanager
def bound_sockets(
    config: Config,
    *,
    socket_owner: tuple[int | None, int | None] = (None, None),
):
    sockets, owned_socket_paths = _build_sockets(config, socket_owner=socket_owner)
    try:
        yield sockets
    finally:
        _cleanup_bound_resources(sockets, owned_socket_paths)
