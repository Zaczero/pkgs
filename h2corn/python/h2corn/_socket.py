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
    _format_tcp_bind,
    _parse_bind_spec,
    _sync_bind_convenience_fields,
)

TYPE_CHECKING = False

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path
    from typing import Any


@contextmanager
def _swap_signal_handlers(handlers: Mapping[int, Any]):
    previous = {sig: signal.signal(sig, handler) for sig, handler in handlers.items()}
    try:
        yield
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def _nonblocking_pipe():
    if hasattr(os, 'pipe2'):
        return os.pipe2(os.O_NONBLOCK)
    read_fd, write_fd = os.pipe()
    os.set_blocking(read_fd, False)
    os.set_blocking(write_fd, False)
    return read_fd, write_fd


@contextmanager
def _signal_wakeup_pipe():
    read_fd, write_fd = _nonblocking_pipe()
    previous = signal.set_wakeup_fd(write_fd, warn_on_full_buffer=False)
    try:
        yield read_fd
    finally:
        signal.set_wakeup_fd(previous)
        os.close(read_fd)
        os.close(write_fd)


def _drain_fd(fd: int):
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


def _build_unix_socket(path: str | os.PathLike[str], config: Config):
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
        socket.SOCK_STREAM | (socket.SOCK_NONBLOCK if sys.platform == 'linux' else 0),
    )
    sock.bind(uds_path)
    sock.listen(config.backlog)
    if config.uds_permissions is not None:
        os.chmod(uds_path, config.uds_permissions)
    if sys.platform != 'linux':
        sock.setblocking(False)
    return sock


def _adopt_socket(fd: int):
    sock = socket.socket(fileno=fd)
    sock.setblocking(False)
    return sock


def _prepare_tcp_listener(sock: socket.socket):
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_TCP, socket.TCP_FASTOPEN, 512)
    if sys.platform == 'linux':
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_DEFER_ACCEPT, 1)
        except OSError:
            pass


def _finish_bound_socket(sock: socket.socket, backlog: int):
    sock.listen(backlog)
    if sys.platform != 'linux':
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
            socket.SOCK_STREAM | (socket.SOCK_NONBLOCK if sys.platform == 'linux' else 0),
        )
        try:
            if family == socket.AF_INET6:
                try:
                    sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
                except OSError:
                    pass
            _prepare_tcp_listener(sock)
            sock.bind(sockaddr)
            return _finish_bound_socket(sock, config.backlog)
        except OSError as exc:
            sock.close()
            last_error = exc
    if last_error is None:
        raise OSError(f'could not resolve bind address: {host!r}')
    raise last_error


def _build_sockets(config: Config):
    sockets: list[socket.socket] = []
    resolved_binds: list[str] = []
    owned_socket_paths: list[Path] = []
    bind_fd_is_unix: list[bool] = []
    shared_tcp_port: int | None = None
    try:
        for bind in config.bind:
            match _parse_bind_spec(bind):
                case TcpBindSpec(host, port):
                    sock = _build_tcp_socket(host, shared_tcp_port or port, config)
                    bound_port = sock.getsockname()[1]
                    if port == 0 and shared_tcp_port is None:
                        shared_tcp_port = bound_port
                    sockets.append(sock)
                    resolved_binds.append(_format_tcp_bind(host, bound_port))
                    bind_fd_is_unix.append(False)
                case UnixBindSpec(path):
                    sockets.append(_build_unix_socket(path, config))
                    resolved_binds.append(f'unix:{path}')
                    owned_socket_paths.append(path)
                    bind_fd_is_unix.append(False)
                case FdBindSpec(fd):
                    sock = _adopt_socket(fd)
                    sockets.append(sock)
                    resolved_binds.append(f'fd://{fd}')
                    bind_fd_is_unix.append(sock.family == socket.AF_UNIX)
    except Exception:
        _cleanup_bound_resources(sockets, owned_socket_paths)
        raise
    object.__setattr__(config, 'bind', tuple(resolved_binds))
    object.__setattr__(config, '_owned_socket_paths', tuple(owned_socket_paths))
    object.__setattr__(config, '_bind_fd_is_unix', tuple(bind_fd_is_unix))
    _sync_bind_convenience_fields(config)
    return tuple(sockets)


@contextmanager
def _bound_sockets(config: Config):
    sockets = _build_sockets(config)
    try:
        yield sockets
    finally:
        _cleanup_bound_resources(sockets, config._owned_socket_paths)
