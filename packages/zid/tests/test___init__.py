from time import time_ns

from zid import _zid_simple, parse_zid_timestamp
from zid import zid as _zid


def pytest_generate_tests(metafunc):
    if 'zid' in metafunc.fixturenames:
        metafunc.parametrize('zid', (_zid, _zid_simple))


def test_zid_uniqueness(zid):
    zids: set[int] = {zid() for _ in range(1_000_000)}
    assert len(zids) == 1_000_000


def test_zid_timestamp(zid):
    ts = time_ns() // 1_000_000
    zid_ts = parse_zid_timestamp(zid())
    te = time_ns() // 1_000_000
    assert ts <= zid_ts <= te
