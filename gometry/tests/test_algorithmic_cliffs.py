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
    Post-fix: prepared always uses the hierarchical tester; free keeps
    MIN_PROBES=64 for one-shot batches. Observable without a clock: identical
    results at 1/8/63/64 probes, and ``explain()`` names the hierarchical
    kernel. Wall-clock scale lives in
    ``benches/cases/case_prepared_contains_xy_scale.py``.
    """
    big = _big_polygon(10_000)
    prep = big.prepare()
    plan = prep.explain()
    assert plan[0] == 'prepared geometry: Polygon'
    assert any('hierarchical Y-stabbing' in line for line in plan)

    for n in (1, 8, 63, 64):
        xs, ys = _PREPARED_PROBES[:n].T
        np.testing.assert_array_equal(
            prep.contains_xy(xs, ys),
            gm.contains_xy(big, xs, ys),
            err_msg=f'contains_xy mismatch at n={n}',
        )
        np.testing.assert_array_equal(
            prep.intersects_xy(xs, ys),
            gm.intersects_xy(big, xs, ys),
            err_msg=f'intersects_xy mismatch at n={n}',
        )


def test_prepared_and_free_contains_xy_results_match():
    big = _big_polygon(2000)
    prep = big.prepare()
    xs, ys = _PREPARED_PROBES.T
    np.testing.assert_array_equal(prep.contains_xy(xs, ys), gm.contains_xy(big, xs, ys))
    np.testing.assert_array_equal(
        prep.intersects_xy(xs, ys), gm.intersects_xy(big, xs, ys)
    )
    # Scalar too
    assert prep.contains_xy(0.0, 0.0) == gm.contains_xy(big, 0.0, 0.0)
    assert prep.intersects_xy(2.0, 2.0) == gm.intersects_xy(big, 2.0, 2.0)


def test_free_fn_contains_xy_agrees_across_threshold():
    """Free contains_xy is correct on both sides of MIN_PROBES (63 and 64).

    The free path still thresholds tester build at 64 probes; that timing
    shape lives in ``benches/cases/case_prepared_contains_xy_scale.py``.
    Here only result agreement with prepared (and across the threshold).
    """
    big = _big_polygon(10_000)
    prep = big.prepare()
    xs64, ys64 = _PREPARED_PROBES.T
    xs63, ys63 = xs64[:63], ys64[:63]
    free63 = gm.contains_xy(big, xs63, ys63)
    free64 = gm.contains_xy(big, xs64, ys64)
    np.testing.assert_array_equal(free63, prep.contains_xy(xs63, ys63))
    np.testing.assert_array_equal(free64, prep.contains_xy(xs64, ys64))
    # Shared prefix: 63-probe free (edge walk) matches the first 63 of the
    # 64-probe free path (tester-built) — both sides of MIN_PROBES agree.
    np.testing.assert_array_equal(free63, free64[:63])
