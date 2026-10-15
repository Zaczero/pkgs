# ZID

[![PyPI - Python Version](https://shields.monicz.dev/pypi/pyversions/zid)](https://pypi.org/p/zid)

**zid** generates sortable, signed-64-bit-compatible integer identifiers for
Python 3.10 and later. A ZID stores a 47-bit logical UNIX millisecond timestamp
and a 16-bit sequence. Generation reserves increasing identifiers within one
process; independently initialized processes can collide.

Install the package below, try [basic usage](#basic-usage), and see the
[API reference](#api-reference) for inputs and errors.
[Report issues on GitHub](https://github.com/Zaczero/pkgs/issues).

## Installation

```sh
pip install zid
```

## Basic usage

```py
from zid import parse_zid_timestamp, zid, zids

identifier = zid()
batch = zids(3)
assert identifier < batch[0]
assert batch == list(range(batch[0], batch[0] + 3))
assert len({parse_zid_timestamp(value) for value in batch}) == 1
print(identifier, parse_zid_timestamp(identifier))  # Values vary on each run.
```

## Reservation and clock behavior

The native generator shares one atomic reservation state across threads in a
process. Each call reserves an interval after all earlier reservations.
`zids(n)` reserves one contiguous interval at one timestamp; other threads
cannot interleave identifiers inside it. Reservation order determines numerical
order, not the order concurrent calls finish.

At a new physical millisecond, the first sequence is chosen uniformly from the
starts that leave room for the entire batch. Subsequent reservations increment
the sequence. When the requested interval no longer fits, the generator advances
its logical timestamp by one millisecond and chooses a new bounded random start.
It does not wrap the sequence or wait for the clock. Backward clock movement
retains the last logical timestamp, so parsed timestamps can lead physical time.
Times before the UNIX epoch are treated as zero.

The largest supported timestamp is `2**47 - 1` milliseconds since the UNIX epoch
(approximately year 6429). A physical time beyond this horizon, or a reservation
requiring logical time beyond it, raises `OverflowError` without reserving an
interval. Every generated identifier is between `0` and `2**63 - 1`.

## Installation (copy & paste)

This standalone Python implementation provides the same format, logical-time
policy, batch reservation and thread synchronization. Its random values need
not match the native generator's. Keep one copy of this state for all callers
that need a shared reservation order.

```py
from operator import index
from secrets import randbelow
from threading import Lock
from time import time_ns

_lock = Lock()
_last_zid = -1


def zids(n: int, /) -> list[int]:
    global _last_zid
    n = index(n)
    if n < 0:
        raise OverflowError('Count must be nonnegative')
    if n > 65536:
        raise ValueError('Count must be at most 65536')
    if n == 0:
        return []
    with _lock:
        now = max(0, time_ns() // 1_000_000)
        if now > 0x7FFF_FFFF_FFFF:
            raise OverflowError('ZID timestamp exhausted')
        timestamp = max(now, _last_zid >> 16)
        max_start = 65536 - n
        if _last_zid >= 0 and timestamp == _last_zid >> 16:
            start = (_last_zid & 0xFFFF) + 1
            if start > max_start:
                if timestamp == 0x7FFF_FFFF_FFFF:
                    raise OverflowError('ZID timestamp exhausted')
                timestamp += 1
                start = randbelow(max_start + 1)
        else:
            start = randbelow(max_start + 1)
        first = (timestamp << 16) | start
        _last_zid = first + n - 1
    return list(range(first, first + n))


def zid() -> int:
    return zids(1)[0]


def parse_zid_timestamp(zid: int) -> int:
    zid = index(zid)
    if not 0 <= zid <= 0xFFFF_FFFF_FFFF_FFFF:
        raise OverflowError('Expected an unsigned 64-bit integer')
    return zid >> 16
```

## API reference

### `zid()`

Return one integer identifier using the shared reservation state.
Raise `OverflowError` when the timestamp horizon is exhausted.

### `zids(n, /)`

Accept an integer count from `0` through `65536` and return a list of that many
contiguous increasing integers at one logical timestamp. Zero returns an empty
list without reading the clock or reserving state. A negative count raises
`OverflowError`; a count above `65536` raises `ValueError`. Timestamp exhaustion
raises `OverflowError`.

The native package converts counts to a platform-sized unsigned integer first;
integers too large for that representation raise `OverflowError` during argument
conversion. Both implementations require integer inputs (`__index__` is
accepted); non-integer inputs raise `TypeError`.

### `parse_zid_timestamp(zid)`

Accept an unsigned 64-bit integer and return its upper 48 bits as an integer
UNIX millisecond timestamp. For generated ZIDs only 47 timestamp bits are used.
This function extracts bits; it does not validate that its input was generated
by ZID. Out-of-range integers raise `OverflowError`.

## Format specification

ZID uses 63 bits of a 64-bit integer, leaving the sign bit zero. The next 47 bits
encode the logical UNIX millisecond timestamp. The final 16 bits encode the
CSPRNG-initialized sequence.

```text
| sign: 0 (1 bit) | logical timestamp (47 bits) | sequence (16 bits) |
```

## Limitations

- Uniqueness and increasing reservation order apply to one generator state,
  not to all processes on a machine, independent standalone copies, process
  restarts or different machines. No coordinator or process namespace is
  encoded. Independent processes have probabilistic starts; two full-size
  batches at the same logical timestamp contain identical identifiers.
- Start child generators with a fresh process image, using `spawn` or `exec`.
  Calling either generator in a forked child before `exec` is unsupported:
  `fork` copies the reservation state and native buffered randomness, and can
  leave the standalone lock held by a vanished thread. There is no automatic
  fork reset. A fresh process does not guarantee uniqueness against its parent.
- Do not use ZIDs as authentication secrets. Their timestamps are visible,
  sequence numbers within a timestamp increase predictably, and the randomized
  origin has at most 16 bits of entropy.
