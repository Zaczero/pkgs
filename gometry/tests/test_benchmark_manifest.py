"""Manifest invariants for the public RELEASE benchmark set (Lane 1)."""

from __future__ import annotations

import importlib
import sys
from collections import Counter
from pathlib import Path

import pytest

_BENCHES = Path(__file__).resolve().parents[1] / 'benches'
_SUPPORT = _BENCHES / 'support'
_PYTHON = _BENCHES / 'python'
for _path in (_SUPPORT, _PYTHON):
    path_s = str(_path)
    if path_s not in sys.path:
        sys.path.insert(0, path_s)

_bench_pairs = importlib.import_module('_bench_pairs')
_bench_registry = importlib.import_module('_bench_registry')

DOMAIN_ORDER = _bench_registry.DOMAIN_ORDER
RELEASE = _bench_registry.RELEASE
RELEASE_OPERATIONS = _bench_registry.RELEASE_OPERATIONS
SMOKE = _bench_registry.SMOKE
SCRIPTS = _bench_registry.SCRIPTS
expand_filter_to_pairs = _bench_registry.expand_filter_to_pairs
operation_for_row = _bench_registry.operation_for_row
find_competitor = _bench_pairs.find_competitor
pair_units = _bench_pairs.pair_units


def test_direct_worker_rejects_explicitly_empty_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit empty selection cannot become a successful zero-row worker."""
    config = importlib.import_module('_bench_config')
    monkeypatch.setenv('GOMETRY_BENCH_FILTER', ' , ')
    with pytest.raises(SystemExit, match='must select at least one benchmark'):
        config.selected_benchmarks('gometry')


# This is an inventory contract, not a projection of whatever happens to be
# left in the registry. A deleted suite must fail before a release run omits it.
EXPECTED_RELEASE_SCRIPTS = {
    'gometry': 'bench_gometry.py',
    'competitors': 'bench_competitors.py',
    'real_world': 'bench_real_world.py',
}


def test_release_operation_counts() -> None:
    assert len(RELEASE_OPERATIONS) == 32
    paired = [op for op in RELEASE_OPERATIONS if op.paired]
    solo = [op for op in RELEASE_OPERATIONS if not op.paired]
    assert len(paired) == 31
    assert len(solo) == 1
    names: list[str] = []
    for op in RELEASE_OPERATIONS:
        names.extend(op.rows)
    assert len(names) == 63
    assert len(set(names)) == 63


def test_release_script_inventory_is_complete() -> None:
    assert SCRIPTS == EXPECTED_RELEASE_SCRIPTS, (
        'release script inventory changed: '
        f'missing={EXPECTED_RELEASE_SCRIPTS.keys() - SCRIPTS.keys()}, '
        f'unexpected={SCRIPTS.keys() - EXPECTED_RELEASE_SCRIPTS.keys()}'
    )


@pytest.mark.parametrize(
    'script', EXPECTED_RELEASE_SCRIPTS.values(), ids=EXPECTED_RELEASE_SCRIPTS
)
def test_release_scripts_import_before_a_release_run(script: str) -> None:
    """Every script dispatched by the release manifest must construct cleanly."""
    importlib.import_module(Path(script).stem)


def test_domain_counts_and_contiguous_order() -> None:
    counts = Counter(op.domain for op in RELEASE_OPERATIONS)
    assert [counts[d] for d in DOMAIN_ORDER] == [6, 11, 4, 4, 4, 3]
    seen: list[str] = []
    for op in RELEASE_OPERATIONS:
        if not seen or seen[-1] != op.domain:
            seen.append(op.domain)
    assert tuple(seen) == DOMAIN_ORDER


def test_only_s2_is_solo() -> None:
    solo = [op for op in RELEASE_OPERATIONS if not op.paired]
    assert len(solo) == 1
    assert 's2_cover' in solo[0].gometry
    assert solo[0].competitor is None
    assert solo[0].suite == 'gometry'


def test_suite_inventory() -> None:
    assert len(RELEASE.rows('competitors')) == 56
    assert len(RELEASE.rows('real_world')) == 6
    assert len(RELEASE.rows('gometry')) == 1


def test_smoke_shares_operation_tuple() -> None:
    assert (
        SMOKE.operations is RELEASE.operations or SMOKE.operations == RELEASE.operations
    )
    assert SMOKE.rows('gometry') == RELEASE.rows('gometry')
    assert SMOKE.rows('competitors') == RELEASE.rows('competitors')
    assert SMOKE.rows('real_world') == RELEASE.rows('real_world')
    assert SMOKE.sampling_args == ('--debug-single-value',)
    assert SMOKE.paired_sampling_args == ('--debug-single-value',)
    assert RELEASE.sampling_args != SMOKE.sampling_args


def test_no_duplicate_or_reused_names() -> None:
    names: list[str] = []
    for op in RELEASE_OPERATIONS:
        names.extend(op.rows)
    assert len(names) == len(set(names))
    competitors = [op.competitor for op in RELEASE_OPERATIONS if op.competitor]
    assert len(competitors) == len(set(competitors))


def test_public_pairing_via_manifest() -> None:
    available = set(RELEASE.rows('competitors'))
    for op in RELEASE_OPERATIONS:
        if op.suite != 'competitors':
            continue
        found = find_competitor(op.gometry, available)
        assert found == op.competitor
    s2 = next(op for op in RELEASE_OPERATIONS if not op.paired)
    assert find_competitor(s2.gometry, set(RELEASE.rows('gometry'))) is None


def test_pair_units_do_not_split_public_pairs() -> None:
    rows = RELEASE.rows('competitors')
    units = pair_units(rows, suite='competitors')
    assert all(len(u) == 2 for u in units)
    assert sum(len(u) for u in units) == 56


def test_filter_selects_both_pair_members() -> None:
    op = next(o for o in RELEASE_OPERATIONS if o.paired)
    only_gometry = expand_filter_to_pairs({op.gometry})
    assert op.gometry in only_gometry
    assert op.competitor in only_gometry
    only_competitor = expand_filter_to_pairs({op.competitor})  # type: ignore[arg-type]
    assert op.gometry in only_competitor
    assert op.competitor in only_competitor


def test_operation_for_row_resolves_both_sides() -> None:
    op = next(o for o in RELEASE_OPERATIONS if o.paired)
    assert operation_for_row(op.gometry) is op
    assert operation_for_row(op.competitor) is op  # type: ignore[arg-type]
    assert operation_for_row('not.a.benchmark/row') is None


def test_real_world_pairs_via_manifest() -> None:
    rows = RELEASE.rows('real_world')
    units = pair_units(rows, suite='real_world')
    assert len(units) == 3
    assert all(len(u) == 2 for u in units)
    area_op = next(o for o in RELEASE_OPERATIONS if 'geodesic_area' in o.gometry)
    assert area_op.competitor is not None
    assert 'pyproj' in area_op.competitor
    assert 'shapely' not in area_op.competitor


@pytest.mark.parametrize(
    'suite,expected',
    [
        ('competitors', 56),
        ('real_world', 6),
        ('gometry', 1),
    ],
)
def test_profile_rows_counts(suite: str, expected: int) -> None:
    assert len(RELEASE.rows(suite)) == expected
    assert len(SMOKE.rows(suite)) == expected
