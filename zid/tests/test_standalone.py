from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import ModuleType

import pytest


@pytest.fixture
def standalone():
    """Execute the distributed copy-and-paste generator in fresh state."""
    readme = (Path(__file__).parents[1] / 'README.md').read_text()
    section = readme.split('## Standalone implementation', 1)[1]
    code = section.split('```py\n', 1)[1].split('```', 1)[0]
    module = ModuleType('standalone_zid')
    exec(compile(code, 'README.md', 'exec'), module.__dict__)  # ruff: ignore[exec-builtin] - Execute the public runnable example.
    return module


def test_clock_and_sequence_transitions(standalone, monkeypatch):
    monkeypatch.setattr(standalone, 'time_ns', lambda: 0)
    monkeypatch.setattr(standalone, 'randbelow', lambda bound: bound - 2)
    assert standalone.zid() == 65534
    assert standalone.zid() == 65535
    assert standalone.zid() == (1 << 16) | 65534
    assert standalone.zid() == (1 << 16) | 65535
    monkeypatch.setattr(standalone, 'time_ns', lambda: 9_000_000)
    assert standalone.zid() == (9 << 16) | 65534


def test_contiguous_batch_rollover_and_rollback(standalone, monkeypatch):
    monkeypatch.setattr(standalone, 'time_ns', lambda: 7_000_000)
    monkeypatch.setattr(standalone, 'randbelow', lambda _bound: 0)
    assert standalone.zids(65536) == list(range(7 << 16, 8 << 16))
    monkeypatch.setattr(standalone, 'time_ns', lambda: 0)
    assert standalone.zids(65535) == list(range(8 << 16, (9 << 16) - 1))
    assert standalone.zid() == (9 << 16) - 1
    monkeypatch.setattr(standalone, 'randbelow', lambda bound: bound - 1)
    assert standalone.zids(2) == [(10 << 16) - 2, (10 << 16) - 1]


def test_horizon_failure_preserves_remaining_slot(standalone, monkeypatch):
    monkeypatch.setattr(standalone, 'time_ns', lambda: (1 << 47) * 1_000_000)
    with pytest.raises(OverflowError):
        standalone.zid()
    monkeypatch.setattr(standalone, 'time_ns', lambda: ((1 << 47) - 1) * 1_000_000)
    monkeypatch.setattr(standalone, 'randbelow', lambda _bound: 0)
    batch = standalone.zids(65535)
    assert batch[0] == (1 << 63) - 65536
    assert batch[-1] == (1 << 63) - 2

    def unavailable(_bound):
        raise AssertionError('exhausted timestamp consumed randomness')

    monkeypatch.setattr(standalone, 'randbelow', unavailable)
    with pytest.raises(OverflowError):
        standalone.zids(2)
    assert standalone.zid() == (1 << 63) - 1
    with pytest.raises(OverflowError):
        standalone.zid()


def test_zero_count_does_not_read_clock(standalone, monkeypatch):
    def unavailable():
        raise AssertionError('zero-count reservation read the clock')

    monkeypatch.setattr(standalone, 'time_ns', unavailable)
    assert standalone.zids(0) == []


def test_overlapping_batches_are_disjoint(standalone, monkeypatch):
    monkeypatch.setattr(standalone, 'time_ns', lambda: 11_000_000)
    monkeypatch.setattr(standalone, 'randbelow', lambda _bound: 0)
    with ThreadPoolExecutor(max_workers=8) as executor:
        batches = list(executor.map(lambda _: standalone.zids(1024), range(256)))
    ids = sorted(value for batch in batches for value in batch)
    assert ids == list(range(11 << 16, 15 << 16))


@pytest.mark.parametrize(
    ('count', 'error'),
    [(-1, OverflowError), (65537, ValueError), (1.5, TypeError)],
)
def test_invalid_count(standalone, count, error):
    with pytest.raises(error):
        standalone.zids(count)


def test_pre_epoch_time(standalone, monkeypatch):
    monkeypatch.setattr(standalone, 'time_ns', lambda: -9_000_000)
    monkeypatch.setattr(standalone, 'randbelow', lambda _bound: 123)
    assert standalone.zid() == 123


@pytest.mark.parametrize('value', [-1, 1 << 64])
def test_timestamp_out_of_range(standalone, value):
    with pytest.raises(OverflowError):
        standalone.parse_zid_timestamp(value)


def test_timestamp_field(standalone):
    assert standalone.parse_zid_timestamp((123456789 << 16) | 54321) == 123456789
    assert standalone.parse_zid_timestamp((1 << 64) - 1) == (1 << 48) - 1
