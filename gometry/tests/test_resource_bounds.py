"""Deterministic resource-bound regressions for amplifying operations."""

from __future__ import annotations

import subprocess
import sys
import textwrap

import gometry as gm
import pytest


def test_geometry_amplification_returns_errors_before_allocation() -> None:
    line = gm.LineString([(0.0, 0.0), (1.0, 0.0)])
    with pytest.raises(gm.GeometryError, match='max_length'):
        line.segmentize(float.fromhex('0x0.0000000000001p-1022'))
    with pytest.raises(gm.GeometryError, match='count'):
        line.sample_points(16_000_001, seed=0)
    with pytest.raises(gm.GeometryError, match='count'):
        line.line_interpolate(count=16_000_001)


def test_h3_fanout_and_traversal_have_explicit_output_budgets() -> None:
    cell = gm.H3Cell(0.0, 0.0, resolution=0)
    assert len(cell.children(2)) == cell.children_count(2)
    with pytest.raises(gm.GeometryError, match='max_cells'):
        cell.grid_disk(1_000, max_cells=100)


def test_cell_array_neighbors_cross_the_old_ragged_collector_limit() -> None:
    """Ragged cell output is governed by allocation, not a hidden cell cap."""
    cell = gm.H3Cell(13.4, 52.5, resolution=7)
    rows = 167_000
    cells = gm.CellArray([cell] * rows)

    neighbors = cells.neighbors

    assert len(neighbors) == rows
    assert len(neighbors.values) > 1_000_000


@pytest.mark.skipif(
    sys.platform in {'darwin', 'win32'}, reason='requires enforced RLIMIT_AS'
)
def test_untrusted_iterable_oom_is_a_complete_memoryerror_outcome() -> None:
    """A constrained child must report one complete, catchable outcome."""
    script = textwrap.dedent(
        """
        import itertools
        import resource
        import gometry as gm
        import numpy  # preload native dependencies before applying the cap

        limit = 192 * 1024 * 1024
        _, hard = resource.getrlimit(resource.RLIMIT_AS)
        if hard != resource.RLIM_INFINITY:
            limit = min(limit, hard)
        resource.setrlimit(resource.RLIMIT_AS, (limit, limit))

        try:
            point = gm.Point(0.0, 0.0)
            gm.GeometryArray(point for _ in itertools.count())
        except MemoryError:
            print("OUTCOME:MemoryError")
        except BaseException as exc:
            print(f"OUTCOME:{type(exc).__name__}:{exc}")
            raise SystemExit(1)
        else:
            print("OUTCOME:unexpected-success")
            raise SystemExit(2)
        """
    )
    try:
        completed = subprocess.run(
            [sys.executable, '-c', script],
            capture_output=True,
            check=False,
            text=True,
            timeout=30.0,
        )
    except subprocess.TimeoutExpired as exc:
        pytest.fail(
            f'constrained allocation child hung: stdout={exc.stdout!r} stderr={exc.stderr!r}'
        )
    assert completed.returncode == 0, (
        f'child did not return cleanly: rc={completed.returncode} '
        f'stdout={completed.stdout!r} stderr={completed.stderr!r}'
    )
    assert completed.stdout.strip() == 'OUTCOME:MemoryError', (
        f'child produced a partial or different outcome: '
        f'stdout={completed.stdout!r} stderr={completed.stderr!r}'
    )
    assert 'PanicException' not in completed.stderr
    assert 'memory allocation of' not in completed.stderr


def test_constrained_refinement_rejects_unbounded_or_nonterminating_requests() -> None:
    polygon = gm.box(0.0, 0.0, 10.0, 10.0)
    with pytest.raises(gm.GeometryError, match='30 degrees'):
        polygon.triangulate(method='constrained', min_angle=31.0)
    with pytest.raises(gm.GeometryError, match='max_area'):
        polygon.triangulate(method='constrained', max_area=1.0e-20)
