"""Readiness notification for `systemd` units declared `Type=notify`.

`systemd` cannot tell "the process started" from "the process is serving": the
first is true the moment `exec` returns, while the second needs the listeners
bound, the lifespan startup complete and — under a supervisor — every worker
reporting in. Only the server knows when that has happened, which is why this
lives here rather than in a unit file's `ExecStartPost`.

Everything here is a no-op when `$NOTIFY_SOCKET` is unset, which is every run
outside a `Type=notify` unit.
"""

from __future__ import annotations

import os
import socket

_MESSAGE_READY = b'READY=1'
_MESSAGE_STOPPING = b'STOPPING=1'


def _address() -> str | None:
    """The notification socket, or `None` when not running under `systemd`.

    `systemd` also defines VSOCK addresses for notifying a hypervisor from
    inside a VM; h2corn deliberately supports only the ordinary `AF_UNIX`
    forms, since nothing here runs as a VM payload. An address starting with
    `@` names the abstract namespace, which Python spells as a leading NUL.
    """
    address = os.environ.get('NOTIFY_SOCKET')
    if not address:
        return None
    if address.startswith('@'):
        return '\0' + address[1:]
    # Anything else must be an absolute filesystem path. A relative one would
    # resolve against a working directory this process may have changed, so it
    # names nothing dependable.
    if not address.startswith('/'):
        return None
    return address


def _notify(message: bytes) -> bool:
    """Send one notification. Returns whether it was actually delivered.

    Best-effort by design, and the error boundary spans the whole operation —
    creating the socket can fail with `EMFILE` just as readily as sending can.
    A status datagram that cannot be delivered must never take the server with
    it: refusing to serve, or skipping shutdown cleanup, because `systemd` did
    not receive a progress report would be the worse failure by far.

    Delivery failure is still real information — a misconfigured unit, a wrong
    address, descriptor exhaustion — so the caller is told, and decides whether
    to retry or report it. This function does not log, because it is called
    from both a signal-handling supervisor and an event loop.
    """
    address = _address()
    if address is None:
        return False
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
            # Non-blocking: a full receive queue, or a `systemd` that has
            # stopped draining, would otherwise park startup or shutdown here
            # indefinitely. `EAGAIN` is simply an undelivered notification.
            sock.setblocking(False)
            _ = sock.sendto(message, address)
    except OSError:
        return False
    return True


def notify_ready() -> bool:
    """Report that the server is now accepting requests."""
    return _notify(_MESSAGE_READY)


def notify_stopping() -> bool:
    """Report that shutdown has begun.

    Sent as draining starts rather than after it, so the unit's published state
    matches what the process is actually doing. This reports state; it does not
    start or extend `TimeoutStopSec`, which `systemd` runs independently.
    """
    return _notify(_MESSAGE_STOPPING)


def notification_configured() -> bool:
    """Whether a `Type=notify` endpoint is present at all.

    Lets a caller tell "nothing to notify" from "tried and failed", which are
    the same `False` from `notify_ready`.
    """
    return _address() is not None
