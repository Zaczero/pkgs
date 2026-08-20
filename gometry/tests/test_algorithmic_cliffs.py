"""R5-L1 P0 algorithmic cliffs: noding dedup, validity bounds sweep, prepared XY.

These are behavior + structural regressions for the three P0s from the
round-5 blind-spot audit — not timing gates (host-sensitive). Absolute ms
targets live under ``benches/cases/``; this file locks identity and the
structural property that the cliffs stay dead.
"""

from __future__ import annotations

import gometry as gm
import numpy as np
import pytest


def _ring32(cx: float = 0.0, cy: float = 0.0, s: float = 1.0) -> gm.Polygon:
    n = 32
    coords = [
        (cx + s * np.cos(2 * np.pi * i / n), cy + s * np.sin(2 * np.pi * i / n))
        for i in range(n)
    ]
    coords.append(coords[0])
    return gm.Polygon(coords)


def _poly_with_n_holes(n: int, spacing: float = 3.0) -> gm.Polygon:
    shell = [
        (-1.0, -1.0),
        (n * spacing + 2.0, -1.0),
        (n * spacing + 2.0, spacing + 1.0),
        (-1.0, spacing + 1.0),
        (-1.0, -1.0),
    ]
    holes = [
        [
            (i * spacing, 0.0),
            (i * spacing + 1.0, 0.0),
            (i * spacing + 1.0, 1.0),
            (i * spacing, 1.0),
            (i * spacing, 0.0),
        ]
        for i in range(n)
    ]
    return gm.Polygon(shell, holes)


def _multi_n_disjoint(n: int, spacing: float = 3.0) -> gm.MultiPolygon:
    return gm.MultiPolygon([
        gm.Polygon([
            (i * spacing, 0.0),
            (i * spacing + 1.0, 0.0),
            (i * spacing + 1.0, 1.0),
            (i * spacing, 1.0),
            (i * spacing, 0.0),
        ])
        for i in range(n)
    ])


# --- P0-A: duplicate noding identity + multi-source provenance ---


def test_duplicate_parts_union_all_identity():
    """160 identical rings dissolve to one; bit-stable vs a single part."""
    poly = _ring32()
    single = gm.GeometryArray([poly]).union_all()
    multi = gm.GeometryArray([poly] * 160).union_all()
    assert gm.equals(single, multi)
    assert single.to_wkb() == multi.to_wkb()


def test_binary_overlay_shared_edge_provenance():
    """Shared undirected edge must keep both operands after sourced dedup."""
    left = gm.box(0, 0, 2, 2)
    right = gm.box(2, 0, 4, 2)  # share the x=2 edge
    # Union of side-by-side boxes is the outer rectangle (provenance-sensitive
    # when the shared edge is deduped undirected before noding).
    u = gm.union(left, right)
    assert gm.equals(u, gm.box(0, 0, 4, 2))
    inter = gm.intersection(left, right)
    # Shared edge / touch only — area intersection empty or line.
    assert inter.area == 0.0
    # Opposite overlap still correct.
    a = gm.box(0, 0, 2, 2)
    b = gm.box(1, 1, 3, 3)
    assert gm.equals(gm.intersection(a, b), gm.box(1, 1, 2, 2))
    assert gm.union(a, b).area == pytest.approx(7.0)


# Wall-clock scaling for duplicate-rich noding lives in
# `benches/cases/case_duplicate_noding_scale.py` (not a pytest gate).


# --- P0-B: validity bounds sweep ---


def test_many_disjoint_holes_valid():
    """2k disjoint holes stay valid (sweep candidate-pair visitor; not O(N²))."""
    n = 2000
    p = _poly_with_n_holes(n)
    assert p.is_valid is True
    report = p.validate()
    assert report is None or bool(getattr(report, 'valid', False))


def test_many_disjoint_parts_valid():
    """2k disjoint multipolygon parts stay valid under the same visitor."""
    n = 2000
    m = _multi_n_disjoint(n)
    assert m.is_valid is True
    report = m.validate()
    assert report is None or bool(getattr(report, 'valid', False))


# Wall-clock for many-disjoint validity lives in
# `benches/cases/case_validity_disjoint_scale.py` (not a pytest gate).


def test_validity_verdicts_unchanged_on_corpus():
    cases = {
        'simple_ok': (gm.box(0, 0, 1, 1), True),
        'hole_ok': (
            gm.Polygon(
                [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)],
                [[(2, 2), (4, 2), (4, 4), (2, 4), (2, 2)]],
            ),
            True,
        ),
        'nested_holes_bad': (
            gm.Polygon(
                [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)],
                [
                    [(1, 1), (9, 1), (9, 9), (1, 9), (1, 1)],
                    [(3, 3), (5, 3), (5, 5), (3, 5), (3, 3)],
                ],
            ),
            False,
        ),
        'bowtie': (gm.from_wkt('POLYGON((0 0, 1 1, 0 1, 1 0, 0 0))'), False),
        'multi_ok': (gm.MultiPolygon([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)]), True),
        'multi_overlap': (
            gm.MultiPolygon([gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3)]),
            False,
        ),
    }
    for name, (geom, expected) in cases.items():
        assert geom.is_valid is expected, name
        # validate() / repair() remain callable and stable on the corpus
        report = geom.validate()
        assert (
            (report is None) == expected
            or bool(getattr(report, 'valid', report is None)) == expected
            or (expected is False and report is not None)
        )
        repaired = geom.repair()
        assert repaired is not None


# --- P0-C: prepared XY always uses tester ---


def _big_polygon(n_edge: int = 5000) -> gm.Polygon:
    theta = np.linspace(0, 2 * np.pi, n_edge, endpoint=False)
    coords = list(zip(np.cos(theta), np.sin(theta), strict=True))
    coords.append(coords[0])
    return gm.Polygon(coords)


# Frozen probe corpus: each prefix exercises the 1/8/63/64 dispatch boundary
# without a seeded generator becoming a second, unreviewed test input source.
_PREPARED_PROBES = np.array(
    [
        (-1.4, -1.3),
        (-1.0, -1.3),
        (-0.6, -1.3),
        (-0.2, -1.3),
        (0.2, -1.3),
        (0.6, -1.3),
        (1.0, -1.3),
        (1.4, -1.3),
        (-1.4, -0.9),
        (-1.0, -0.9),
        (-0.6, -0.9),
        (-0.2, -0.9),
        (0.2, -0.9),
        (0.6, -0.9),
        (1.0, -0.9),
        (1.4, -0.9),
        (-1.4, -0.5),
        (-1.0, -0.5),
        (-0.6, -0.5),
        (-0.2, -0.5),
        (0.2, -0.5),
        (0.6, -0.5),
        (1.0, -0.5),
        (1.4, -0.5),
        (-1.4, -0.1),
        (-1.0, -0.1),
        (-0.6, -0.1),
        (-0.2, -0.1),
        (0.2, -0.1),
        (0.6, -0.1),
        (1.0, -0.1),
        (1.4, -0.1),
        (-1.4, 0.1),
        (-1.0, 0.1),
        (-0.6, 0.1),
        (-0.2, 0.1),
        (0.2, 0.1),
        (0.6, 0.1),
        (1.0, 0.1),
        (1.4, 0.1),
        (-1.4, 0.5),
        (-1.0, 0.5),
        (-0.6, 0.5),
        (-0.2, 0.5),
        (0.2, 0.5),
        (0.6, 0.5),
        (1.0, 0.5),
        (1.4, 0.5),
        (-1.4, 0.9),
        (-1.0, 0.9),
        (-0.6, 0.9),
        (-0.2, 0.9),
        (0.2, 0.9),
        (0.6, 0.9),
        (1.0, 0.9),
        (1.4, 0.9),
        (-1.4, 1.3),
        (-1.0, 1.3),
        (-0.6, 1.3),
        (-0.2, 1.3),
        (0.2, 1.3),
        (0.6, 1.3),
        (1.0, 1.3),
        (1.4, 1.3),
    ],
    dtype=np.float64,
)


def test_prepared_contains_xy_matches_free_at_every_probe_count():
    """Prepared receiver uses its tester at every count: results match free.

    Pre-fix cliff discarded the prepared tester below 64 probes (572x).
    The retained policy is plan-aware: this 10,000-edge shape crosses over
    at four one-shot probes. Observable without a clock: identical results
    at 1/2/3/4 probes. Wall-clock scale lives in
    ``benches/cases/case_prepared_contains_xy_scale.py``.
    """
    big = _big_polygon(10_000)
    prep = big.prepare()

    for n in (1, 2, 3, 4):
        xs, ys = _PREPARED_PROBES[:n].T
        np.testing.assert_array_equal(
            gm.contains_xy(prep, xs, ys),
            gm.contains_xy(big, xs, ys),
            err_msg=f'contains_xy mismatch at n={n}',
        )
        np.testing.assert_array_equal(
            gm.intersects_xy(prep, xs, ys),
            gm.intersects_xy(big, xs, ys),
            err_msg=f'intersects_xy mismatch at n={n}',
        )


def test_prepared_and_free_contains_xy_results_match():
    big = _big_polygon(2000)
    prep = big.prepare()
    xs, ys = _PREPARED_PROBES.T
    np.testing.assert_array_equal(gm.contains_xy(prep, xs, ys), gm.contains_xy(big, xs, ys))
    np.testing.assert_array_equal(
        gm.intersects_xy(prep, xs, ys), gm.intersects_xy(big, xs, ys)
    )
    # Scalar too
    assert gm.contains_xy(prep, 0.0, 0.0) == gm.contains_xy(big, 0.0, 0.0)
    assert gm.intersects_xy(prep, 2.0, 2.0) == gm.intersects_xy(big, 2.0, 2.0)


def test_free_fn_contains_xy_agrees_across_threshold():
    """Free contains_xy is correct on both sides of the four-probe crossover.

    The timing shape lives in ``benches/cases/case_prepared_contains_xy_scale.py``.
    Here only result agreement with prepared (and across the threshold).
    """
    big = _big_polygon(10_000)
    prep = big.prepare()
    xs64, ys64 = _PREPARED_PROBES.T
    xs3, ys3 = xs64[:3], ys64[:3]
    xs4, ys4 = xs64[:4], ys64[:4]
    free3 = gm.contains_xy(big, xs3, ys3)
    free4 = gm.contains_xy(big, xs4, ys4)
    np.testing.assert_array_equal(free3, gm.contains_xy(prep, xs3, ys3))
    np.testing.assert_array_equal(free4, gm.contains_xy(prep, xs4, ys4))
    np.testing.assert_array_equal(free3, free4[:3])
