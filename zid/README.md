# ZID

[![PyPI - Python Version](https://shields.monicz.dev/pypi/pyversions/zid)](https://pypi.org/p/zid)

**zid** generates sortable, signed-64-bit-compatible integer identifiers for
Python 3.10 and later. A ZID stores a 47-bit logical UNIX millisecond timestamp
and a 16-bit sequence. Generation reserves increasing identifiers within one
process; independently initialized processes can collide.

## Installation

```sh
pip install zid
```

## Basic usage

```py
from zid import parse_zid_timestamp, zid, zids

identifier = zid()
batch = zids(3)
print(identifier, parse_zid_timestamp(identifier))  # Values vary on each run.
```

## Reservation and clock behavior

Calls share a thread-safe generator. `zids(n)` reserves a contiguous batch at
one logical timestamp; other threads cannot interleave identifiers inside it.
Reservation order determines numerical order, not the order calls finish.

The sequence starts randomly at each new millisecond. When it fills, the
generator advances its logical clock rather than waiting. Backward clock
movement also preserves increasing IDs, so decoded timestamps can lead wall
time. All generated IDs fit a signed 64-bit integer.

## Installation (copy & paste)

Prefer no dependency? This standalone implementation uses the same format
and reservation policy. Keep one copy of its state for all callers.

<details>
<summary>Standalone Python implementation</summary>

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

</details>

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

## Support

[Issues and questions](https://github.com/Zaczero/pkgs/issues) ·
[Private security reports](https://github.com/Zaczero/pkgs/security/advisories/new) ·
[Premium support](https://monicz.dev/#get-in-touch)
