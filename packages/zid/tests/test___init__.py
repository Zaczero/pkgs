from time import time_ns

import pytest
from zid import parse_zid_timestamp, zid, zids


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
    assert ts <= parse_zid_timestamp(zid_) <= te


def test_zids_timestamp():
    ts = time_ns() // 1_000_000
    zids_ = zids(65536)
    te = time_ns() // 1_000_000
    assert ts <= parse_zid_timestamp(zids_[0]) <= te
    assert len(set(map(parse_zid_timestamp, zids_))) == 1
