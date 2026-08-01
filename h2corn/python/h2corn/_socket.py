from __future__ import annotations

import io
import os
import signal
import socket
import stat
import sys
from contextlib import contextmanager
from dataclasses import dataclass

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
    from typing import Any, Self


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


@dataclass(frozen=True, slots=True)
class _SignalWakeupPipe:
    read_fd: int
    write_fd: int


@contextmanager
def signal_wakeup_pipe():
    read_fd, write_fd = nonblocking_pipe()
    try:
        # Documented to raise off the main thread; the pipe is ours either
        # way, so it is closed here rather than left to the caller who never
        # received it.
        previous = signal.set_wakeup_fd(write_fd, warn_on_full_buffer=False)
    except BaseException:
        os.close(read_fd)
        os.close(write_fd)
        raise
    try:
        yield _SignalWakeupPipe(read_fd, write_fd)
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


@dataclass(frozen=True, slots=True)
class _OwnedSocketPath:
    """A socket path together with the inode this process put there.

    Carried as one value so cleanup cannot unlink by name alone: a later
    generation that rebinds the same path owns a different inode, and deleting
    it would take a live endpoint out from under it.

    The identity is the path's own `lstat`, taken immediately after `bind`. A
    Unix socket's descriptor and its directory entry report different inodes on
    Linux, so `fstat` on the socket cannot be compared against it.
    """

    path: str
    device: int
    inode: int

    @classmethod
    def claim(cls, path: str) -> Self:
        entry = os.lstat(path)
        return cls(path, entry.st_dev, entry.st_ino)

    def release(self) -> None:
        """Unlink the path, but only while it is still the inode we bound."""
        try:
            entry = os.lstat(self.path)
        except OSError:
            return
        if (
            stat.S_ISSOCK(entry.st_mode)
            and entry.st_dev == self.device
            and entry.st_ino == self.inode
        ):
            try:
                os.unlink(self.path)
            except FileNotFoundError:
                pass


@dataclass(slots=True)
class _CreatedListener:
    """A listener this process opened: socket and optional Unix-path claim."""

    socket: socket.socket | None
    path: _OwnedSocketPath | None

    def transfer(self) -> int:
        sock = self.socket
        if sock is None:
            raise RuntimeError('listener already transferred')
        self.socket = None
        return sock.detach()

    def release(self) -> None:
        # Untransferred: close the socket. Transferred: native already owns the
        # fd; only the Unix-path claim remains until serving ends.
        sock = self.socket
        self.socket = None
        if sock is not None:
            sock.close()
        path = self.path
        self.path = None
        if path is not None:
            path.release()


@dataclass(slots=True)
class _BorrowedListener:
    """A listener handed in through `fd://` — never closed by this process."""

    socket: socket.socket | None

    def transfer(self) -> int:
        sock = self.socket
        if sock is None:
            raise RuntimeError('listener already transferred')
        self.socket = None
        # `_adopt_listener()` owns a duplicate, so native serving can consume
        # this fd without changing the caller's descriptor lifetime.
        return sock.detach()

    def release(self) -> None:
        # We detach before closing our duplicate, so this code cannot ever
        # close the fd supplied by the caller.
        sock = self.socket
        self.socket = None
        if sock is not None:
            os.close(sock.detach())


@dataclass(slots=True)
class _InheritedListener:
    """A descriptor a supervisor already transferred to this worker.

    Deliberately *not* wrapped in a `socket.socket`. `O_NONBLOCK` lives on the
    open file description, which every forked worker shares, and PyPy's
    `socket.socket(fileno=fd)` clears it -- so a worker still in lifespan
    startup would silently make `accept` blocking for its siblings that are
    already serving, parking their runtime threads in `accept(2)`. Holding the
    bare descriptor keeps that state unreachable rather than repairing it after
    the fact, and saves a socket object per listener per worker start.
    """

    fd: int | None

    def transfer(self) -> int:
        fd = self.fd
        if fd is None:
            raise RuntimeError('listener already transferred')
        self.fd = None
        return fd

    def release(self) -> None:
        fd = self.fd
        self.fd = None
        if fd is not None:
            os.close(fd)


# Spec writes `type ListenerLease = ...` (PEP 695 / 3.12+); floor is 3.11.
ListenerLease = _CreatedListener | _BorrowedListener | _InheritedListener


def _build_unix_listener(
    path: str | os.PathLike[str],
    config: Config,
    *,
    owner_uid: int | None = None,
    owner_gid: int | None = None,
) -> _CreatedListener:
    uds_path = os.fspath(path)
    if sys.platform == 'win32':
        raise OSError('Unix sockets are not supported on Windows')
    listener = _CreatedListener(socket=None, path=None)
    try:
        try:
            existing = os.lstat(uds_path)
        except FileNotFoundError:
            existing = None
        if existing is not None:
            if not stat.S_ISSOCK(existing.st_mode):
                raise OSError(
                    f'unix socket path exists and is not a socket: {uds_path}'
                )
            os.unlink(uds_path)

        listener.socket = socket.socket(
            socket.AF_UNIX,
            socket.SOCK_STREAM | getattr(socket, 'SOCK_NONBLOCK', 0),
        )
        listener.socket.bind(uds_path)
        # The owner exists before bind, and claims the path immediately
        # afterwards. Any later asynchronous exception releases both parts.
        listener.path = _OwnedSocketPath.claim(uds_path)
        if owner_uid is not None or owner_gid is not None:
            os.chown(
                uds_path,
                -1 if owner_uid is None else owner_uid,
                -1 if owner_gid is None else owner_gid,
            )
        listener.socket.listen(config.backlog)
        if config.uds_permissions is not None:
            os.chmod(uds_path, config.uds_permissions)
        if not getattr(socket, 'SOCK_NONBLOCK', 0):
            listener.socket.setblocking(False)
    except BaseException:
        listener.release()
        raise
    return listener


def _adopt_listener(fd: int) -> _BorrowedListener:
    """
    Wrap an inherited descriptor as a listener.

    The descriptor belongs to whoever handed it over. The lease therefore
    wraps a duplicate: validation, rollback, and native serving can consume
    that duplicate while the caller retains its original descriptor.
    """
    duplicated_fd = os.dup(fd)
    try:
        sock = socket.socket(fileno=duplicated_fd)
    except BaseException:
        os.close(duplicated_fd)
        raise
    listener = _BorrowedListener(socket=sock)
    # `os.dup` shares the open file description with the caller's descriptor,
    # and wrapping it can clear `O_NONBLOCK` on that shared description. Restore
    # it before anything can fail: a validation error below must not leave the
    # embedding caller's own listener blocking.
    sock.setblocking(False)
    try:
        if sock.type != socket.SOCK_STREAM:
            raise ValueError(f'fd://{fd} is not a stream socket')
        accept_conn = getattr(socket, 'SO_ACCEPTCONN', None)
        if accept_conn is not None and not sock.getsockopt(
            socket.SOL_SOCKET, accept_conn
        ):
            raise ValueError(f'fd://{fd} is not listening')
    except BaseException:
        listener.release()
        raise
    return listener


#: `socket.TCP_FASTOPEN` is missing from PyPy's socket module, but the option
#: is a stable part of the Linux UAPI and the kernel still honours it. Falling
#: back to the numeric value keeps Fast Open working there instead of silently
#: dropping it.
_TCP_FASTOPEN = getattr(socket, 'TCP_FASTOPEN', 23)


def _prepare_tcp_listener(sock: socket.socket, *, reuse_port: bool = False):
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    if reuse_port:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

    value = 512 if sys.platform == 'linux' else 1
    try:
        sock.setsockopt(socket.IPPROTO_TCP, _TCP_FASTOPEN, value)
    except OSError:
        # The kernel or its configuration refused the option. A binding that
        # simply does not name the constant is a different thing, and is
        # handled by `_TCP_FASTOPEN` rather than swallowed here.
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


def _build_tcp_listener(host: str, port: int, config: Config) -> _CreatedListener:
    last_error = None
    for family, _, _, _, sockaddr in socket.getaddrinfo(
        host,
        port,
        type=socket.SOCK_STREAM,
        flags=socket.AI_PASSIVE,
    ):
        listener = _CreatedListener(socket=None, path=None)
        try:
            listener.socket = socket.socket(
                family,
                socket.SOCK_STREAM | getattr(socket, 'SOCK_NONBLOCK', 0),
            )
            if family == socket.AF_INET6:
                try:
                    listener.socket.setsockopt(
                        socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1
                    )
                except OSError:
                    pass
            _prepare_tcp_listener(listener.socket, reuse_port=config.reuse_port)
            listener.socket.bind(sockaddr)
            _finish_bound_socket(listener.socket, config.backlog)
        except OSError as exc:
            listener.release()
            last_error = exc
        except BaseException:
            listener.release()
            raise
        else:
            return listener
    if last_error is None:
        raise OSError(f'could not resolve bind address: {host!r}')
    raise OSError(f'could not bind {host}:{port}: {last_error}') from last_error


def lease_owned_fds(fds: Sequence[int]) -> tuple[_InheritedListener, ...]:
    """Lease fds a supervisor already transferred to this worker.

    These descriptors are this process's responsibility from entry, unlike
    ``fd://`` descriptors supplied by an embedding caller.
    """
    return tuple(_InheritedListener(fd=fd) for fd in fds)


def _build_sockets(
    config: Config,
    *,
    socket_owner: tuple[int | None, int | None] = (None, None),
) -> tuple[ListenerLease, ...]:
    leases: list[ListenerLease] = []
    pending: ListenerLease | None = None
    shared_tcp_port: int | None = None
    owner_uid, owner_gid = socket_owner
    try:
        for bind in config.bind:
            spec = parse_bind_spec(bind)
            match spec:
                case TcpBindSpec(host, port):
                    pending = _build_tcp_listener(host, shared_tcp_port or port, config)
                    sock = pending.socket
                    assert sock is not None
                    bound_port = sock.getsockname()[1]
                    if port == 0 and shared_tcp_port is None:
                        shared_tcp_port = bound_port
                    leases.append(pending)
                    pending = None
                case UnixBindSpec(path):
                    pending = _build_unix_listener(
                        path,
                        config,
                        owner_uid=owner_uid,
                        owner_gid=owner_gid,
                    )
                    leases.append(pending)
                    pending = None
                case FdBindSpec(fd):
                    pending = _adopt_listener(fd)
                    sock = pending.socket
                    assert sock is not None
                    # AF_UNIX is absent on Windows; an adopted fd there is never
                    # a unix socket, so a missing constant compares unequal.
                    af_unix = getattr(socket, 'AF_UNIX', None)
                    if config.certfile is not None and sock.family == af_unix:
                        raise OSError('TLS is supported only on TCP listeners')
                    leases.append(pending)
                    pending = None
    except BaseException:
        # BaseException, not Exception: a KeyboardInterrupt between two binds
        # is exactly when a half-built set of listeners — and any unix socket
        # path already created — must still be rolled back. Reverse order so
        # later leases cannot depend on earlier ones during teardown.
        if pending is not None:
            pending.release()
        for lease in reversed(leases):
            lease.release()
        raise
    return tuple(leases)


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
    leases = _build_sockets(config, socket_owner=socket_owner)
    try:
        yield leases
    finally:
        for lease in reversed(leases):
            lease.release()
