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


def test_zid_uniqueness():
    assert len({zid() for _ in range(1_000_000)}) == 1_000_000


def test_zids_uniqueness():
    assert len(set(zids(65536))) == 65536


def test_zids_n_underflow():
    with pytest.raises(OverflowError):
        zids(-1)


def test_zids_n_too_large():
    with pytest.raises(ValueError):
        zids(65537)


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
