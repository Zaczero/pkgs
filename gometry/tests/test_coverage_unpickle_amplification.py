"""Revert-sensitive regressions for coverage-unpickle amplification / non-termination.

D07 — deep factory depth with empty cells must reject under the factory max_cells
      budget (not recompute billions of cells).
D09 — infinite iterables in surviving cell-id slots must TypeError immediately;
      discarded cells_all/interior/outer fields are gone from the wire format.

Each negative test terminates under a hard wall-clock bound via the shared
subprocess helper pattern from ``test_nonterm_iterables``.
"""

from __future__ import annotations

import pickle
import subprocess
import sys
import textwrap

import gometry as gm
import pytest
from gometry import _lib


def _run_child(script: str, *, timeout: float = 8.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        capture_output=True,
        check=False,
        text=True,
        timeout=timeout,
    )


def _assert_terminates(script: str, *, timeout: float = 8.0) -> subprocess.CompletedProcess[str]:
    try:
        completed = _run_child(script, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        pytest.fail(
            f"child hung past {timeout}s:\nstdout={exc.stdout!r}\nstderr={exc.stderr!r}"
        )
    assert completed.returncode != -6, f"SIGABRT:\n{completed.stderr}"
    assert completed.returncode != 134, f"abort exit 134:\n{completed.stderr}"
    return completed


def _assert_not_panic(exc: BaseException) -> None:
    name = type(exc).__name__
    assert "Panic" not in name, f"unexpected panic-like exception: {name}: {exc}"


# ---------------------------------------------------------------------------
# D07 — factory max_cells bounds partition recompute
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "script",
    [
        pytest.param(
            """
            import gometry as gm
            from gometry import _lib
            try:
                _lib._unpickle_tile_coverage(
                    gm.box(-180, -85, 180, 85, crs=4326),
                    [],
                    "overlap",
                    14,
                    None,
                    1_000_000,
                )
            except Exception as exc:
                assert "Panic" not in type(exc).__name__
                print("ok", type(exc).__name__)
            else:
                raise SystemExit("expected rejection")
            """,
            id="tile-z14",
        ),
        pytest.param(
            """
            import gometry as gm
            from gometry import _lib
            try:
                _lib._unpickle_h3_coverage(
                    gm.box(-180, -85, 180, 85, crs=4326),
                    [],
                    "overlap",
                    15,
                    None,
                    1_000_000,
                )
            except Exception as exc:
                assert "Panic" not in type(exc).__name__
                print("ok", type(exc).__name__)
            else:
                raise SystemExit("expected rejection")
            """,
            id="h3-res15",
        ),
        pytest.param(
            """
            import gometry as gm
            from gometry import _lib
            try:
                _lib._unpickle_s2_coverage(
                    gm.box(-180, -85, 180, 85, crs=4326),
                    [],
                    "overlap",
                    30,
                    30,
                    1,
                    1_000_000,
                    8,
                )
            except Exception as exc:
                assert "Panic" not in type(exc).__name__
                print("ok", type(exc).__name__)
            else:
                raise SystemExit("expected rejection")
            """,
            id="s2-level30",
        ),
        pytest.param(
            """
            import gometry as gm
            from gometry import _lib
            try:
                _lib._unpickle_geohash_coverage(
                    gm.box(-180, -85, 180, 85, crs=4326),
                    [],
                    "overlap",
                    12,
                    None,
                    1_000_000,
                )
            except Exception as exc:
                assert "Panic" not in type(exc).__name__
                print("ok", type(exc).__name__)
            else:
                raise SystemExit("expected rejection")
            """,
            id="geohash-prec12",
        ),
    ],
)
def test_d07_deep_empty_cells_terminates(script: str):
    completed = _assert_terminates(script)
    assert completed.returncode == 0, completed.stderr
    assert "ok" in completed.stdout


def test_d07_inprocess_tile_z14_budget_message():
    with pytest.raises(gm.GeometryError, match="max_cells") as excinfo:
        _lib._unpickle_tile_coverage(
            gm.box(-180, -85, 180, 85, crs=4326),
            [],
            "overlap",
            14,
            None,
            1_000_000,
        )
    _assert_not_panic(excinfo.value)
    assert "coverage reconstruction exceeds its recorded max_cells=" in str(excinfo.value)


def test_d07_honest_finite_max_cells_roundtrip():
    src = gm.box(0, 0, 1, 1, crs=4326)
    for cov in (
        gm.tile_cover(src, zoom=6, max_cells=10_000),
        gm.geohash_cover(src, precision=5, max_cells=10_000),
        gm.h3_cover(src, resolution=3, max_cells=10_000),
        gm.s2_cover(src, level=6, max_cells=10_000),
    ):
        out = pickle.loads(pickle.dumps(cov))
        assert list(out.cells) == list(cov.cells)
        assert len(out) == len(cov)


# ---------------------------------------------------------------------------
# D09 — exact list extraction; discarded fields removed
# ---------------------------------------------------------------------------


def test_d09_infinite_cells_iterable_terminates():
    """Surviving cells slot rejects itertools.repeat immediately (TypeError)."""
    completed = _assert_terminates(
        """
        import itertools
        import gometry as gm
        from gometry import _lib
        try:
            _lib._unpickle_tile_coverage(
                gm.Point(0, 0, crs=4326),
                itertools.repeat(0),
                "overlap",
                0,
                None,
                1_000_000,
            )
        except TypeError as exc:
            print("ok", type(exc).__name__)
        except Exception as exc:
            assert "Panic" not in type(exc).__name__
            print("ok", type(exc).__name__)
        else:
            raise SystemExit("expected rejection")
        """
    )
    assert completed.returncode == 0, completed.stderr
    assert "ok" in completed.stdout


def test_d09_inprocess_repeat_cells_typeerror():
    import itertools

    with pytest.raises(TypeError, match="list") as excinfo:
        _lib._unpickle_tile_coverage(
            gm.Point(0, 0, crs=4326),
            itertools.repeat(0),
            "overlap",
            0,
            None,
            1_000_000,
        )
    _assert_not_panic(excinfo.value)


def test_d09_reduce_arity_has_no_discarded_fields():
    """Honest reducers no longer serialize partition all/interior (or S2 outer)."""
    src = gm.box(0, 0, 1, 1, crs=4326)
    tile_args = gm.tile_cover(src, zoom=4).__reduce__()[1]
    # (geom, cells, rule, factory_zoom, visible_depth, max_cells)
    assert len(tile_args) == 6
    assert isinstance(tile_args[1], list)

    gh_args = gm.geohash_cover(src, precision=4).__reduce__()[1]
    assert len(gh_args) == 6

    h3_args = gm.h3_cover(src, resolution=2).__reduce__()[1]
    # (geom, cells, rule, factory_res, visible_depth, max_cells)
    assert len(h3_args) == 6

    s2_args = gm.s2_cover(src, level=4).__reduce__()[1]
    # (geom, cells, rule, min_level, max_level, level_mod, max_cells, target_cells)
    assert len(s2_args) == 8


# ---------------------------------------------------------------------------
# Semantics preserved — compact/with_parents/uncompact pickle round-trips
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cov",
    [
        lambda: gm.tile_cover(gm.box(0, 0, 1, 1, crs=4326), zoom=8).compact(),
        lambda: gm.tile_cover(gm.box(0, 0, 1, 1, crs=4326), zoom=6).with_parents(),
        lambda: gm.tile_cover(gm.box(0, 0, 1, 1, crs=4326), zoom=5).uncompact(6),
        lambda: gm.geohash_cover(gm.box(0, 0, 1, 1, crs=4326), precision=5).compact(),
        lambda: gm.geohash_cover(gm.box(0, 0, 1, 1, crs=4326), precision=4).with_parents(),
        lambda: gm.h3_cover(gm.box(0, 0, 1, 1, crs=4326), resolution=4).compact(),
        lambda: gm.h3_cover(gm.box(0, 0, 1, 1, crs=4326), resolution=3).with_parents(),
        lambda: gm.s2_cover(gm.box(0, 0, 1, 1, crs=4326), level=8).compact(),
        lambda: gm.s2_cover(gm.box(0, 0, 1, 1, crs=4326), level=6).with_parents(),
        # N7 — composed transforms (decorative ancestors + compact/uncompact)
        lambda: gm.h3_cover(gm.Point(0.123, 0.123, crs=4326), resolution=1)
        .with_parents()
        .uncompact(2),
        lambda: gm.s2_cover(gm.Point(0.123, 0.123, crs=4326), level=1)
        .with_parents()
        .uncompact(2),
        lambda: gm.geohash_cover(gm.Point(0.123, 0.123, crs=4326), precision=2)
        .with_parents()
        .compact(),
        lambda: gm.tile_cover(gm.Point(0.123, 0.123, crs=4326), zoom=1)
        .with_parents()
        .compact(),
        lambda: gm.tile_cover(gm.box(0, 0, 1, 1, crs=4326), zoom=5)
        .with_parents()
        .compact()
        .uncompact(6),
    ],
)
def test_transform_pickle_roundtrip_all_grids(cov):
    original = cov()
    out = pickle.loads(pickle.dumps(original))
    assert list(out.cells) == list(original.cells)
    assert len(out) == len(original)
    assert out.to_polygon().bounds == original.to_polygon().bounds


# ---------------------------------------------------------------------------
# N8 — S2 compact/with_parents keep factory provenance (partition stable)
# ---------------------------------------------------------------------------


def test_n8_s2_compact_pickle_preserves_interior_boundary_partition():
    """Compact must not overwrite factory min/max; partitions survive pickle."""
    source = gm.box(-2, -2, 2, 2, crs=4326)
    before = gm.s2_cover(source, level=7, max_cells=1000).compact()
    assert before.min_level == 7
    assert before.max_level == 7
    after = pickle.loads(pickle.dumps(before))
    assert before == after
    assert after.min_level == 7
    assert after.max_level == 7
    assert list(after.interior_cells) == list(before.interior_cells)
    assert list(after.boundary_cells) == list(before.boundary_cells)


def test_n8_s2_with_parents_pickle_preserves_factory_and_partition():
    source = gm.box(-2, -2, 2, 2, crs=4326)
    before = gm.s2_cover(source, level=7, max_cells=1000).with_parents()
    assert before.min_level == 7
    assert before.max_level == 7
    after = pickle.loads(pickle.dumps(before))
    assert after.min_level == 7
    assert after.max_level == 7
    assert list(after.interior_cells) == list(before.interior_cells)
    assert list(after.boundary_cells) == list(before.boundary_cells)
    assert list(after.cells) == list(before.cells)


# ---------------------------------------------------------------------------
# N9 — adaptive S2 applies the hard max_cells limit directly
# ---------------------------------------------------------------------------


def test_n9_s2_adaptive_max_cells_one_honored_or_typed_error():
    """max_cells=1 must not silently emit more cells than requested."""
    pt = gm.Point(0, 0, crs=4326)
    try:
        cov = gm.s2_cover(pt, max_cells=1)
    except gm.GeometryError as exc:
        _assert_not_panic(exc)
        if "max_cells" not in str(exc):
            raise AssertionError(exc) from exc
        return
    assert cov.max_cells == 1
    assert len(cov) <= 1


def test_n9_s2_adaptive_max_cells_none_unlimited():
    cov = gm.s2_cover(gm.Point(0, 0, crs=4326), max_cells=None)
    assert cov.max_cells is None
    assert len(cov) > 0


def test_n9_s2_adaptive_target_cells_is_separate_from_hard_cap():
    area = gm.box(0, 0, 2, 2, crs=4326)
    adaptive = gm.s2_cover(area, min_level=4, max_level=8, target_cells=64)
    assert adaptive.max_cells == 1_000_000
    assert adaptive.target_cells == 64
    assert 0 < len(adaptive) <= 64


def test_coverage_pickle_roundtrip_preserves_all_grid_partitions():
    local_src = gm.box(0, 0, 1, 1, crs=4326)
    for local in (
        gm.tile_cover(local_src, zoom=4),
        gm.geohash_cover(local_src, precision=3),
        gm.h3_cover(local_src, resolution=2),
        gm.s2_cover(local_src, level=4),
    ):
        honest = pickle.loads(pickle.dumps(local))
        assert honest == local
        assert list(honest.interior_cells) == list(local.interior_cells)
        assert list(honest.boundary_cells) == list(local.boundary_cells)
