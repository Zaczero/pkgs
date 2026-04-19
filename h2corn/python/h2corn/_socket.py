from __future__ import annotations

import io
import os
import signal
import socket
import stat
import sys
from contextlib import contextmanager

TYPE_CHECKING = False

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping
    from typing import Any

    from ._config import Config


@contextmanager
def _swap_signal_handlers(handlers: Mapping[int, Any]) -> Iterator[None]:
    previous = {sig: signal.signal(sig, handler) for sig, handler in handlers.items()}
    try:
        yield
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


@contextmanager
def _signal_wakeup_pipe() -> Iterator[int]:
    if sys.platform != 'darwin':
        read_fd, write_fd = os.pipe2(os.O_NONBLOCK)
    else:
        read_fd, write_fd = os.pipe()
        for fd in (read_fd, write_fd):
            os.set_blocking(fd, False)

    previous = signal.set_wakeup_fd(write_fd, warn_on_full_buffer=False)
    try:
        yield read_fd
    finally:
        signal.set_wakeup_fd(previous)
        os.close(read_fd)
        os.close(write_fd)


def _drain_fd(fd: int) -> None:
    try:
        while True:
            if not os.read(fd, io.DEFAULT_BUFFER_SIZE):
                return
    except BlockingIOError:
        return


def _build_unix_socket(config: Config) -> socket.socket:
    uds_path = config.uds
    if uds_path is None:
        raise ValueError('unix socket configuration is required')
    if sys.platform == 'win32':
        raise OSError('Unix sockets are not supported on Windows')

    try:
        existing = uds_path.lstat()
    except FileNotFoundError:
        existing = None
    if existing is not None:
        if not stat.S_ISSOCK(existing.st_mode):
            raise OSError(f'unix socket path exists and is not a socket: {uds_path}')
        uds_path.unlink()

    sock = socket.socket(
        socket.AF_UNIX,
        socket.SOCK_STREAM | (socket.SOCK_NONBLOCK if sys.platform == 'linux' else 0),
    )
    sock.bind(os.fspath(uds_path))
    sock.listen(config.backlog)
    if config.uds_permissions is not None:
        os.chmod(uds_path, config.uds_permissions)
    if sys.platform != 'linux':
        sock.setblocking(False)
    return sock


def _adopt_socket(config: Config) -> socket.socket:
    if config.fd is None:
        raise ValueError('inherited fd configuration is required')

    sock = socket.socket(fileno=config.fd)
    object.__setattr__(config, '_fd_is_unix', sock.family == socket.AF_UNIX)
    sock.setblocking(False)
    return sock


def _new_tcp_socket() -> socket.socket:
    return socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM | (socket.SOCK_NONBLOCK if sys.platform == 'linux' else 0),
    )


def _prepare_tcp_listener(sock: socket.socket) -> None:
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_TCP, socket.TCP_FASTOPEN, 512)
    if sys.platform == 'linux':
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_DEFER_ACCEPT, 1)
        except OSError:
            pass


def _finish_bound_socket(sock: socket.socket, backlog: int) -> socket.socket:
    sock.listen(backlog)
    if sys.platform != 'linux':
        sock.setblocking(False)
    return sock


def _build_tcp_socket(config: Config) -> socket.socket:
    sock = _new_tcp_socket()
    _prepare_tcp_listener(sock)
    sock.bind((config.host, config.port))
    if config.port == 0:
        object.__setattr__(config, 'port', sock.getsockname()[1])
    return _finish_bound_socket(sock, config.backlog)


def _build_socket(config: Config) -> socket.socket:
    if config.fd is not None:
        return _adopt_socket(config)
    if config.uds is not None:
        return _build_unix_socket(config)
    return _build_tcp_socket(config)


@contextmanager
def _bound_socket(config: Config) -> Iterator[socket.socket]:
    sock = _build_socket(config)
    try:
        yield sock
    finally:
        sock.close()
        if config.fd is None and config.uds is not None:
            try:
                if stat.S_ISSOCK(config.uds.lstat().st_mode):
                    config.uds.unlink()
            except FileNotFoundError:
                pass
