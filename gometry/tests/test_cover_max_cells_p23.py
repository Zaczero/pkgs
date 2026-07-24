"""Regressions: overridable max_cells on cover factories + P23 pickle transforms."""

from __future__ import annotations

import pickle
import resource
import subprocess
import sys
import textwrap
import time

import gometry as gm
import pytest


def _assert_not_panic(exc: BaseException) -> None:
    assert type(exc).__name__ != "PanicException", exc


# ---------------------------------------------------------------------------
# max_cells on factories
# ---------------------------------------------------------------------------


def test_default_max_cells_names_knob_on_runaway():
    """Default 1_000_000 still rejects a runaway and names max_cells."""
    # Full-longitude world band (not a |Δlon|>180 seam edge — those normalize
    # to a narrow strip and no longer runaway under geographic covering).
    src = gm.box(-180.0, -85.0, 180.0, 85.0, crs=4326)
    with pytest.raises(gm.GeometryError, match="max_cells") as excinfo:
        gm.tile_cover(src, zoom=12, cell_rule="bbox")
    _assert_not_panic(excinfo.value)
    assert "1000000" in str(excinfo.value) or "1_000_000" in str(excinfo.value)


def test_max_cells_none_allows_past_old_wall():
    """max_cells=None (or a large value) succeeds where the hard wall used to fire."""
    # A covering that sits just above 1M cells with a tight budget must fail,
    # then succeed with None / a raised budget.
    src = gm.box(-10.0, -10.0, 10.0, 10.0, crs=4326)
    with pytest.raises(gm.GeometryError, match="max_cells"):
        gm.tile_cover(src, zoom=10, cell_rule="bbox", max_cells=100)
    cov = gm.tile_cover(src, zoom=10, cell_rule="bbox", max_cells=None)
    assert len(cov) > 100
    cov2 = gm.tile_cover(src, zoom=10, cell_rule="bbox", max_cells=1_000_000)
    assert len(cov2) == len(cov)


@pytest.mark.parametrize(
    "factory",
    [
        lambda src, **kw: gm.h3_cover(src, 3, **kw),
        lambda src, **kw: gm.s2_cover(src, level=6, **kw),
        lambda src, **kw: gm.geohash_cover(src, 4, **kw),
        lambda src, **kw: gm.tile_cover(src, 6, **kw),
    ],
)
def test_max_cells_nonpositive_typed_error(factory):
    src = gm.box(0, 0, 1, 1, crs=4326)
    for bad in (0, -1):
        with pytest.raises(gm.GeometryError, match="max_cells") as excinfo:
            factory(src, max_cells=bad)
        _assert_not_panic(excinfo.value)


def test_s2_fixed_level_max_cells_is_a_hard_cap_like_other_grids():
    """Every fixed-level coverer raises when its hard cap is too small."""
    area = gm.box(0, 0, 2, 2, crs=4326)
    factories = (
        lambda: gm.h3_cover(area, resolution=6, max_cells=1),
        lambda: gm.s2_cover(area, level=10, max_cells=1),
        lambda: gm.geohash_cover(area, precision=5, max_cells=1),
        lambda: gm.tile_cover(area, zoom=8, max_cells=1),
    )
    for factory in factories:
        with pytest.raises(gm.GeometryError, match="max_cells") as excinfo:
            factory()
        _assert_not_panic(excinfo.value)


def test_s2_target_cells_and_max_cells_have_separate_meanings():
    """Adaptive approximation and hard output caps are independent."""
    area = gm.box(0, 0, 2, 2, crs=4326)
    adaptive = gm.s2_cover(area, min_level=4, max_level=8, target_cells=64)
    assert adaptive.max_cells == 1_000_000
    assert adaptive.target_cells == 64
    assert 0 < len(adaptive) <= 64
    fixed = gm.s2_cover(area, level=6, max_cells=10_000)
    assert fixed.max_cells == 10_000
    assert len(fixed) > 0
    unlimited = gm.s2_cover(area, level=6, max_cells=None)
    assert unlimited.max_cells is None
    assert len(unlimited) == len(fixed)


def test_s2_cover_budget_equals_unlimited_fit_threshold():
    """Revert-sensitive B2: max_cells charges proven emissions, not DFS frontier.

    Short line unlimited L10 cover is exactly one cell; budgets 1..K+2 must
    succeed with the same tokens (previously 1..4 false-rejected because the
    transient fail-open frontier was charged against max_cells).
    """
    line = gm.LineString([(-75.0, 40.0), (-74.99, 40.01)], crs=4326)
    unlimited = gm.s2_cover(line, level=10, max_cells=None)
    tokens = [c.token for c in unlimited.cells]
    k = len(tokens)
    assert k == 1
    assert tokens == ['89c6b5']
    for m in range(1, k + 3):
        got = gm.s2_cover(line, level=10, max_cells=m)
        assert [c.token for c in got.cells] == tokens, m
    # Matrix: for several short geoms, M>=K succeeds equal; M<K raises.
    samples = [
        gm.Point(-75.0, 40.0, crs=4326),
        gm.LineString([(-75.0, 40.0), (-74.99, 40.01)], crs=4326),
        gm.LineString([(-75.0, 40.0), (-74.5, 40.2)], crs=4326),
        gm.box(-75.0, 40.0, -74.9, 40.1, crs=4326),
    ]
    for geom in samples:
        unlim = gm.s2_cover(geom, level=10, max_cells=None)
        kk = len(unlim.cells)
        assert kk >= 1
        unlim_ids = {c.id for c in unlim.cells}
        for m in range(kk, kk + 3):
            got = gm.s2_cover(geom, level=10, max_cells=m)
            assert {c.id for c in got.cells} == unlim_ids
            assert len(got) <= m
        if kk > 1:
            with pytest.raises(gm.GeometryError, match='max_cells') as excinfo:
                gm.s2_cover(geom, level=10, max_cells=kk - 1)
            _assert_not_panic(excinfo.value)
            assert isinstance(excinfo.value, gm.GeometryError)


def test_s2_adaptive_target_cells_matches_requested_detail():
    """Adaptive target_cells charges proven-productive cells only.

    box(-75,40,-74.9,40.1) at target 4 is exactly 4 L10 cells; previously
    small targets false-rejected because fail-open Boundary queue
    entries were charged before deeper pruning to Outside.
    """
    area = gm.box(-75.0, 40.0, -74.9, 40.1, crs=4326)
    detailed = gm.s2_cover(area, min_level=1, max_level=10, target_cells=4)
    tokens = [c.token for c in detailed.cells]
    k = len(tokens)
    assert k == 4
    assert tokens == ['89c14b', '89c14d', '89c6b3', '89c6b5']
    assert {c.level for c in detailed.cells} == {10}
    # A target at or above K preserves the detailed covering.
    for m in range(k, k + 3):
        got = gm.s2_cover(area, min_level=1, max_level=10, target_cells=m)
        assert [c.token for c in got.cells] == tokens, m
        assert len(got) <= m
    # Smaller targets coarsen only inside the requested adaptive range.
    for m in range(1, k):
        got = gm.s2_cover(area, min_level=1, max_level=10, target_cells=m)
        assert 0 < len(got) <= m, m
        assert all(c.level >= 1 for c in got.cells)
    coarse = gm.s2_cover(area, min_level=0, max_level=10, target_cells=1)
    assert len(coarse) <= 1


def test_s2_fixed_level_soft_budget_never_emits_below_min_level():
    """F7: max_cells soft budget must not emit coarser than a fixed level.

    Regression: after N9, s2_cover(box, level=10, max_cells=256) mixed in
    level-9 interior cells even though the true level-10 cover (225 cells)
    fits in 256. Fixed-level purity requires every cell at exactly L when
    the level-L cover fits.
    """
    src = gm.box(0, 0, 1, 1, crs=4326)
    # Repro: level-10 unlimited is 225 cells — must match max_cells=256.
    unlimited = gm.s2_cover(src, level=10, max_cells=None)
    capped = gm.s2_cover(src, level=10, max_cells=256)
    assert len(unlimited) == 225
    assert len(capped) == len(unlimited)
    assert {c.level for c in capped.cells} == {10}
    assert {c.id for c in capped.cells} == {c.id for c in unlimited.cells}
    assert capped.min_level == capped.max_level == 10
    # Pickle round-trip preserves pure level-10 set.
    restored = pickle.loads(pickle.dumps(capped))
    assert {c.level for c in restored.cells} == {10}
    assert restored.min_level == restored.max_level == 10
    assert list(restored.cells) == list(capped.cells)


@pytest.mark.parametrize("level", [4, 8, 10, 14])
@pytest.mark.parametrize("cell_rule", ["overlap", "within", "center", "bbox"])
def test_s2_fixed_level_matrix_only_emits_level_l(level, cell_rule):
    """F7 matrix: when the level-L cover fits in M, every cell is level L."""
    src = gm.box(0, 0, 1, 1, crs=4326)
    unlimited = gm.s2_cover(src, level=level, cell_rule=cell_rule, max_cells=None)
    # max_cells budgets the exact coverer before cell_rule filtering, so size
    # M from the underlying overlap cover (within/center are subsets).
    overlap_fit = len(gm.s2_cover(src, level=level, cell_rule="overlap", max_cells=None))
    assert all(c.level == level for c in unlimited.cells)
    # Finite M above the coverer fit — must not coarsen below level L.
    budget = max(overlap_fit, 1) + 64
    capped = gm.s2_cover(
        src, level=level, cell_rule=cell_rule, max_cells=budget
    )
    assert len(capped) == len(unlimited)
    assert all(c.level == level for c in capped.cells)
    assert capped.min_level == capped.max_level == level
    assert {c.id for c in capped.cells} == {c.id for c in unlimited.cells}
    # Array path agrees with scalar for one-row GeometryArray (Groups of cells).
    arr = gm.GeometryArray([src])
    arr_cov = gm.s2_cover(arr, level=level, cell_rule=cell_rule, max_cells=budget)
    assert isinstance(arr_cov, gm.Groups)
    assert len(arr_cov) == 1
    row = arr_cov[0]
    assert {c.id for c in row} == {c.id for c in unlimited.cells}
    assert all(c.level == level for c in row)


def test_s2_target_cells_respects_min_level_and_max_cells_stays_hard():
    """Adaptive targeting stays within its range; max_cells remains a hard cap."""
    src = gm.box(0, 0, 1, 1, crs=4326)
    tight = gm.s2_cover(src, min_level=0, max_level=12, target_cells=1)
    assert len(tight) <= 1
    # Adaptive with min_level floor: every emitted cell is >= min_level.
    adaptive = gm.s2_cover(src, min_level=4, max_level=12, target_cells=64)
    assert 0 < len(adaptive) <= 64
    levels = {c.level for c in adaptive.cells}
    assert levels
    assert min(levels) >= 4
    assert max(levels) <= 12
    # Fixed-level that cannot fit raises a typed GeometryError naming max_cells.
    with pytest.raises(gm.GeometryError, match="max_cells") as excinfo:
        gm.s2_cover(src, level=14, max_cells=1)
    _assert_not_panic(excinfo.value)


# ---------------------------------------------------------------------------
# Defect 2 — budget-aware forced descent (no frontier flood)
# ---------------------------------------------------------------------------


def _rss_kib() -> int:
    """Current max RSS in KiB (Linux)."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def test_s2_level30_tiny_budget_rejects_without_frontier_flood():
    """Defect 2: level=30 + max_cells=1 must reject fast, without BFS flood.

    Pre-fix: ~2.3s / ~85 MiB building a fine frontier before the budget
    error. Post-fix: projected-count DFS raises before that work.
    """
    line = gm.LineString([(0.0, 0.0), (0.1, 0.0)], crs=4326)
    # Warm classifier / PROJ paths so the timed call measures cover work.
    gm.s2_cover(gm.Point(0.0, 0.0, crs=4326), level=4, max_cells=8)

    rss_before = _rss_kib()
    started = time.perf_counter()
    with pytest.raises(gm.GeometryError, match="max_cells") as excinfo:
        gm.s2_cover(line, level=30, max_cells=1)
    elapsed = time.perf_counter() - started
    rss_after = _rss_kib()

    _assert_not_panic(excinfo.value)
    assert elapsed < 0.5, f"budget rejection took {elapsed:.3f}s (frontier flood?)"
    # Linux ru_maxrss is KiB; incremental peak must stay well under the old ~85 MiB.
    if sys.platform.startswith("linux"):
        delta_mib = max(0, rss_after - rss_before) / 1024.0
        assert delta_mib < 32.0, f"incremental RSS {delta_mib:.1f} MiB (frontier flood?)"


def test_s2_level30_tiny_budget_rejects_in_subprocess():
    """Same flood gate in a clean subprocess (3s outer timeout, no import noise)."""
    script = textwrap.dedent(
        """
        import resource
        import sys
        import time
        import gometry as gm

        line = gm.LineString([(0.0, 0.0), (0.1, 0.0)], crs=4326)
        rss0 = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        t0 = time.perf_counter()
        try:
            gm.s2_cover(line, level=30, max_cells=1)
        except gm.GeometryError as e:
            if type(e).__name__ == "PanicException" or "max_cells" not in str(e):
                print(f"BAD_EXC {type(e).__name__}:{e}", flush=True)
                sys.exit(2)
        else:
            print("NO_RAISE", flush=True)
            sys.exit(3)
        elapsed = time.perf_counter() - t0
        rss1 = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        delta_mib = max(0, rss1 - rss0) / 1024.0
        print(f"OK elapsed={elapsed:.4f} delta_mib={delta_mib:.2f}", flush=True)
        if elapsed >= 0.5:
            sys.exit(4)
        if delta_mib >= 32.0:
            sys.exit(5)
        """
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=3.0,
        check=False,
    )
    assert proc.returncode == 0, (
        f"subprocess failed rc={proc.returncode}\n"
        f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    )
    assert "OK" in proc.stdout


def test_s2_world_line_l20_tiny_budget_rejects_fast():
    """World-spanning line at L20 + tiny budget stays time/memory bounded."""
    line = gm.LineString([(-179.0, 0.0), (179.0, 0.0)], crs=4326)
    started = time.perf_counter()
    with pytest.raises(gm.GeometryError, match="max_cells") as excinfo:
        gm.s2_cover(line, level=20, max_cells=1)
    elapsed = time.perf_counter() - started
    _assert_not_panic(excinfo.value)
    assert elapsed < 0.5, f"world L20 tiny budget took {elapsed:.3f}s"


def test_s2_point_level30_max_cells_one_succeeds():
    """Ordinary point at level=30 with max_cells=1 is a single cell, fast."""
    pt = gm.Point(13.4, 52.5, crs=4326)
    started = time.perf_counter()
    cov = gm.s2_cover(pt, level=30, max_cells=1)
    elapsed = time.perf_counter() - started
    assert len(cov) == 1
    assert cov.cells[0].level == 30
    assert elapsed < 0.5


def test_s2_adaptive_max_level30_target_one_returns_coarse():
    """Adaptive min=0/max=30/target_cells=1 returns one coarse cell quickly."""
    area = gm.box(0, 0, 1, 1, crs=4326)
    started = time.perf_counter()
    cov = gm.s2_cover(area, min_level=0, max_level=30, target_cells=1)
    elapsed = time.perf_counter() - started
    assert len(cov) <= 1
    assert elapsed < 0.5


def test_s2_fixed_level_threshold_n_identical_n_minus_one_raises():
    """F7 threshold: budget=N matches unlimited; budget=N-1 raises fast."""
    src = gm.box(0, 0, 1, 1, crs=4326)
    unlimited = gm.s2_cover(src, level=10, max_cells=None)
    n = len(unlimited)
    assert n > 1
    exact = gm.s2_cover(src, level=10, max_cells=n)
    assert len(exact) == n
    assert {c.id for c in exact.cells} == {c.id for c in unlimited.cells}
    assert all(c.level == 10 for c in exact.cells)
    started = time.perf_counter()
    with pytest.raises(gm.GeometryError, match="max_cells") as excinfo:
        gm.s2_cover(src, level=10, max_cells=n - 1)
    elapsed = time.perf_counter() - started
    _assert_not_panic(excinfo.value)
    assert elapsed < 0.5


@pytest.mark.parametrize("level_mod", [1, 2, 3])
def test_s2_level_mod_target_cells_conforming_and_bounded(level_mod):
    """level_mod fanout uses actual K; outputs stay conforming and ≤ target."""
    src = gm.box(0, 0, 2, 2, crs=4326)
    target = 32
    cov = gm.s2_cover(
        src,
        min_level=4,
        max_level=12,
        level_mod=level_mod,
        target_cells=target,
    )
    assert 0 < len(cov) <= target
    for cell in cov.cells:
        assert cell.level >= 4
        assert cell.level <= 12
        assert (cell.level - 4) % level_mod == 0


def test_s2_interior_subtree_rejects_before_enumeration():
    """Interior 4^Δ preflight rejects a huge fixed-level areal cover early."""
    # Large box + fine min_level: interior faces expand to 4^Δ terminals.
    src = gm.box(-60.0, -40.0, 60.0, 40.0, crs=4326)
    started = time.perf_counter()
    with pytest.raises(gm.GeometryError, match="max_cells") as excinfo:
        gm.s2_cover(src, level=16, max_cells=100)
    elapsed = time.perf_counter() - started
    _assert_not_panic(excinfo.value)
    assert elapsed < 0.5, f"interior multiplicity preflight took {elapsed:.3f}s"


def test_with_parents_large_uncapped_and_pickle_roundtrip():
    """Explicit with_parents may produce >1M cells; pickle restores them."""
    # Exact ±180 full-longitude band: geographic cover keeps the world extent.
    src = gm.box(-180.0, -82.0, 180.0, 82.0, crs=4326)
    cov = gm.tile_cover(src, zoom=10, cell_rule="bbox")
    wp = cov.with_parents(min_zoom=0)
    assert len(wp) > 1_000_000
    out = pickle.loads(pickle.dumps(wp))
    assert len(out) == len(wp)
    assert list(out.cells) == list(wp.cells)


# ---------------------------------------------------------------------------
# P23 — pickle of transformed coverages
# ---------------------------------------------------------------------------


def _roundtrip_exact(cov):
    out = pickle.loads(pickle.dumps(cov))
    assert len(out) == len(cov)
    assert list(out.cells) == list(cov.cells)
    if len(cov) > 0:
        assert out.to_polygon().bounds == cov.to_polygon().bounds
    return out


@pytest.mark.parametrize(
    "label,build",
    [
        (
            "h3",
            lambda: gm.h3_cover(gm.box(-1, -1, 1, 1, crs=4326), 4, cell_rule="center"),
        ),
        (
            "s2",
            lambda: gm.s2_cover(
                gm.box(-1, -1, 1, 1, crs=4326), level=6, max_cells=128, cell_rule="center"
            ),
        ),
        (
            "geohash",
            lambda: gm.geohash_cover(gm.box(-1, -1, 1, 1, crs=4326), 4, cell_rule="center"),
        ),
        (
            "tile",
            lambda: gm.tile_cover(gm.box(-1, -1, 1, 1, crs=4326), 6, cell_rule="center"),
        ),
    ],
)
def test_p23_transform_pickle_roundtrips(label, build):
    base = build()
    uncompact_depth = {
        "h3": 5,
        "s2": 8,
        "geohash": 5,
        "tile": 7,
    }[label]
    for _form, cov in (
        ("base", base),
        ("compact", base.compact()),
        ("uncompact", base.uncompact(uncompact_depth)),
        ("with_parents", base.with_parents()),
    ):
        _roundtrip_exact(cov)


def test_p23_empty_center_uncompact_pickle():
    """Empty center cover uncompacted to a finer depth round-trips (the P23 repro)."""
    source = gm.box(-0.1, -0.1, 0.1, 0.1, crs=4326)
    # H3: center@4 is empty; uncompact(5) stays empty; must not recompute as center@5.
    cov = gm.h3_cover(source, 4, cell_rule="center").uncompact(5)
    assert len(cov) == 0
    assert cov.resolution == 5
    out = _roundtrip_exact(cov)
    assert out.resolution == 5

    gh = gm.geohash_cover(source, 2, cell_rule="center").uncompact(5)
    assert len(gh) == 0
    out_gh = _roundtrip_exact(gh)
    assert out_gh.precision == 5


def test_p23_uncompact_past_million_pickle_roundtrips():
    """Uncompact past UNCOMPACT_MAX_CELLS must still pickle-round-trip.

    Coverage transforms are uncapped; pickle reconstruction must use the same
    unlimited path (not re-hit the uncompact budget).
    """
    # One world tile at z0 → 4^10 = 1_048_576 cells at z10 (>1M).
    cov = gm.tile_cover(gm.Point(10, 10, crs=4326), zoom=0).uncompact(10)
    assert len(cov) > 1_000_000
    # Must not raise "exceed the uncompact budget" during reconstruction.
    out = pickle.loads(pickle.dumps(cov))
    assert len(out) == len(cov)
    assert list(out.cells) == list(cov.cells)
    assert out.to_polygon().bounds == cov.to_polygon().bounds


def test_p23_projected_source_roundtrips():
    """Projected sources (D26a false positive) still round-trip."""
    proj = gm.box(0, 0, 1000, 1000, crs=3857)
    for cov in (
        gm.h3_cover(proj, 3).uncompact(4),
        gm.tile_cover(proj, 4).with_parents(),
        gm.s2_cover(proj, level=5, max_cells=64).compact(),
        gm.geohash_cover(proj, 3).compact(),
    ):
        _roundtrip_exact(cov)


def test_p23_empty_nonempty_parity():
    """Empty/nonempty parity: empty factory stays empty; nonempty stays nonempty."""
    src = gm.box(-0.1, -0.1, 0.1, 0.1, crs=4326)
    empty = gm.h3_cover(src, 4, cell_rule="center")
    assert len(empty) == 0
    _roundtrip_exact(empty)
    nonempty = gm.h3_cover(src, 5, cell_rule="center")
    assert len(nonempty) > 0
    _roundtrip_exact(nonempty)
