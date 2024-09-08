from time import time_ns

from zid import parse_zid_timestamp, zid


def test_zid_uniqueness():
    zids: set[int] = {zid() for _ in range(1_000_000)}
    assert len(zids) == 1_000_000


def test_zid_timestamp():
    ts = time_ns() // 1_000_000
    zid_ts = parse_zid_timestamp(zid())
    te = time_ns() // 1_000_000
    assert ts <= zid_ts <= te
