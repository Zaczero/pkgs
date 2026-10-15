import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from itertools import pairwise
from time import time_ns

import pytest
from zid import parse_zid_timestamp, zid, zids

# Python's `time_ns()` and the Rust extension's `SystemTime::now()` can differ by a few milliseconds
# on some platforms (notably Windows) due to different clock sources / resolution.
MAX_CLOCK_SKEW_MS = 50


def assert_ms_in_window(
    value_ms: int,
    *,
    start_ms: int,
    end_ms: int,
    skew_ms: int = MAX_CLOCK_SKEW_MS,
) -> None:
    assert start_ms - skew_ms <= value_ms <= end_ms + skew_ms


def test_zid_reservations_increase():
    ids = [zid() for _ in range(4096)]
    assert all(left < right for left, right in pairwise(ids))


def test_zids_uniqueness():
    batch = zids(65536)
    assert batch == list(range(batch[0], batch[0] + 65536))
    assert zid() > batch[-1]


def test_overlapping_reservations():
    def reserve(_):
        return [zid(), *zids(1024), zid()]

    with ThreadPoolExecutor(max_workers=8) as executor:
        batches = list(executor.map(reserve, range(64)))
    ids = [value for batch in batches for value in batch]
    assert len(set(ids)) == 64 * 1026
    for batch in batches:
        assert all(left < right for left, right in pairwise(batch))


def test_zero_batch():
    assert zids(0) == []


def test_zids_n_underflow():
    with pytest.raises(OverflowError):
        zids(-1)


def test_zids_n_too_large():
    with pytest.raises(ValueError):
        zids(65537)


def test_zids_n_conversion_overflow():
    with pytest.raises(OverflowError):
        zids(1 << 64)


@pytest.mark.parametrize('value', [-1, 1 << 64])
def test_timestamp_out_of_range(value):
    with pytest.raises(OverflowError):
        parse_zid_timestamp(value)


def test_timestamp_field():
    assert parse_zid_timestamp((123456789 << 16) | 54321) == 123456789
    assert parse_zid_timestamp((1 << 63) - 1) == (1 << 47) - 1
    assert parse_zid_timestamp((1 << 64) - 1) == (1 << 48) - 1


@pytest.mark.skipif(
    not hasattr(os, 'fork'), reason='fork/exec is a POSIX process boundary'
)
def test_fork_exec_starts_a_usable_generator():
    child = (
        'from zid import zid, zids; '
        'first = zid(); batch = zids(65536); '
        'assert first < batch[0]; '
        'assert batch == list(range(batch[0], batch[0] + 65536)); '
        'assert zid() > batch[-1]'
    )
    # A fresh single-threaded launcher avoids forking pytest's worker threads.
    launcher = f"""
import os
import sys
from zid import zid, zids
zid()
zids(65536)
pid = os.fork()
if pid == 0:
    os.execv(sys.executable, [sys.executable, '-c', {child!r}])
_, status = os.waitpid(pid, 0)
assert os.waitstatus_to_exitcode(status) == 0
"""
    subprocess.run([sys.executable, '-c', launcher], check=True, timeout=30)


def test_zid_timestamp():
    ts = time_ns() // 1_000_000
    zid_ = zid()
    te = time_ns() // 1_000_000
    zid_ts = parse_zid_timestamp(zid_)
    assert_ms_in_window(zid_ts, start_ms=ts, end_ms=te)


def test_zids_timestamp():
    ts = time_ns() // 1_000_000
    zids_ = zids(65536)
    te = time_ns() // 1_000_000
    zid_ts = parse_zid_timestamp(zids_[0])
    assert_ms_in_window(zid_ts, start_ms=ts, end_ms=te)
    assert len(set(map(parse_zid_timestamp, zids_))) == 1
