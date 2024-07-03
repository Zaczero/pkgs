from __future__ import annotations

from io import BytesIO
from os import urandom
from time import time_ns
from typing import Final

RANDOM_BUFFER_SIZE: int = 128 * 1024  # 128 KiB
"""
The size of the buffer when requesting random bytes from the operating system.
"""

_random_buffer: Final[BytesIO] = BytesIO()

_last_time: int = -1
_last_sequence: int = -1


def zid() -> int:
    """
    Generate a unique identifier.
    """
    global _last_time, _last_sequence

    # UNIX timestamp in milliseconds
    time: int = time_ns() // 1_000_000
    if time > 0x7FFFFFFFFFFF:
        raise OverflowError('Time value is too large')

    # CSPRNG-initialized sequence numbers
    sequence: int
    if _last_time == time:
        _last_sequence = sequence = (_last_sequence + 1) & 0xFFFF
    else:
        rand: bytes = _random_buffer.read(2)
        if len(rand) < 2:
            _random_buffer.write(urandom(RANDOM_BUFFER_SIZE))
            _random_buffer.seek(0)
            rand = _random_buffer.read(2)
        _last_sequence = sequence = int.from_bytes(rand)
        _last_time = time

    return (time << 16) | sequence


def _zid_simple() -> int:
    global _last_time, _last_sequence

    # UNIX timestamp in milliseconds
    time: int = time_ns() // 1_000_000
    if time > 0x7FFFFFFFFFFF:
        raise OverflowError('Time value is too large')

    # CSPRNG-initialized sequence numbers
    sequence: int
    if _last_time == time:
        _last_sequence = sequence = (_last_sequence + 1) & 0xFFFF
    else:
        _last_sequence = sequence = int.from_bytes(urandom(2))
        _last_time = time

    return (time << 16) | sequence


def parse_zid_timestamp(zid: int) -> int:
    """
    Extract the UNIX timestamp in milliseconds from a ZID.
    """
    return zid >> 16
