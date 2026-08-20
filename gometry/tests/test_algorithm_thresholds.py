"""Answer-stable algorithm thresholds (R14-A A1/A2).

Size/count thresholds must not select a structurally different metric or
predicate answer. Fixtures exercise both sides of the historical cliffs.
"""

from __future__ import annotations

import math

import gometry as gm
import pytest


def test_hausdorff_equidistant_metric_stable_above_feature_param_budget() -> None:
    """Asymmetric multipoint target: continuous max is at an interior equidistant
    root (not the param-interval midpoint). Padding near the two sites inflates
    feature x param products past the old 16_384 full-pairs budget without
    changing the continuous answer.
    """
    # Source base; target points at different heights so equidistant ≠ midpoint.
    # True continuous directed max ≈ 5.836 at x=4.25; midpoint sample ≈ 5.099.
    src = gm.LineString([(0.0, 0.0), (10.0, 0.0)])
    true = math.sqrt(100.0 * (4.25 / 10.0) ** 2 + 16.0)
    mid_wrong = min(math.hypot(5.0, 4.0), math.hypot(5.0, 1.0))
    assert true == pytest.approx(5.836308764964376)
    assert mid_wrong == pytest.approx(5.0990195135927845)

    def distance_for(n_extra: int) -> float:
        pts: list[gm.Point] = [gm.Point(0.0, 4.0), gm.Point(10.0, 1.0)]
        for i in range(n_extra):
            eps = (i + 1) * 1e-12
            pts.append(gm.Point(0.0 + eps, 4.0))
            pts.append(gm.Point(10.0 + eps, 1.0))
        return float(gm.hausdorff_distance(src, gm.GeometryCollection(pts)))

    # Below, at, and well above the old feature x param cliff.
    for n_extra in (0, 60, 90, 128, 129, 200):
        got = distance_for(n_extra)
        assert got == pytest.approx(true, abs=1e-6), (
            f'n_extra={n_extra}: got {got}, want continuous {true} '
            f'(must not collapse to midpoint sample {mid_wrong})'
        )
        assert got != pytest.approx(mid_wrong, abs=1e-3)


_PREDICATES = (
    'intersects',
    'disjoint',
    'contains',
    'contains_properly',
    'within',
    'covers',
    'covered_by',
    'touches',
    'crosses',
    'overlaps',
    'equals',
)


def test_predicate_batch_length_agrees_scalar_15_16() -> None:
    """Scalar, 15-row, and 16-row batches must agree for every named predicate.

    Historical PREPARED_PREDICATE_MIN=16 omitted the geographic point/seam
    kernel on short batches and answered from planar bounds — flipping at ±180.
    """
    poly = gm.Polygon(
        [(-170.0, 0.0), (170.0, 0.0), (170.0, 10.0), (-170.0, 10.0)],
        crs=4326,
    )
    # 17 points: mix interior, boundary, exterior, and antimeridian longitudes.
    lons = [
        -180.0,
        -170.0,
        -160.0,
        0.0,
        160.0,
        170.0,
        180.0,
        -175.0,
        175.0,
        5.0,
        -5.0,
        90.0,
        -90.0,
        45.0,
        -45.0,
        135.0,
        -135.0,
    ]
    pts = [gm.Point(lon, 5.0, crs=4326) for lon in lons]
    assert len(pts) >= 16

    # Planar control corpus (no CRS) — same sizes, no geo seam.
    poly_planar = gm.Polygon([(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)])
    planar_pts = [
        gm.Point(5.0, 5.0),
        gm.Point(0.0, 0.0),
        gm.Point(10.0, 5.0),
        gm.Point(20.0, 20.0),
        gm.Point(1.0, 1.0),
        gm.Point(9.0, 9.0),
        gm.Point(-1.0, 5.0),
        gm.Point(5.0, -1.0),
        gm.Point(5.0, 0.0),
        gm.Point(0.0, 5.0),
        gm.Point(10.0, 10.0),
        gm.Point(2.5, 7.5),
        gm.Point(7.5, 2.5),
        gm.Point(3.0, 3.0),
        gm.Point(8.0, 8.0),
        gm.Point(4.0, 6.0),
        gm.Point(6.0, 4.0),
    ]

    for name in _PREDICATES:
        fn = getattr(gm, name)
        for scalar, rows in (
            (poly, pts),
            (poly_planar, planar_pts),
        ):
            scalars = [bool(fn(scalar, row)) for row in rows[:16]]
            for n in (15, 16):
                batch = [bool(v) for v in fn(scalar, gm.GeometryArray(rows[:n]))]
                assert batch == scalars[:n], (
                    f'{name} n={n} scalar={scalar.crs!r}: batch={batch} scalars={scalars[:n]}'
                )
            # Single-row free form equals the scalar loop (first row).
            assert bool(fn(scalar, rows[0])) == scalars[0]


def test_prepared_predicate_matches_free_across_batch_threshold() -> None:
    poly = gm.Polygon(
        [(-170.0, 0.0), (170.0, 0.0), (170.0, 10.0), (-170.0, 10.0)],
        crs=4326,
    )
    pts = [
        gm.Point(lon, 5.0, crs=4326)
        for lon in (
            -180.0,
            -170.0,
            0.0,
            170.0,
            180.0,
            90.0,
            -90.0,
            45.0,
            -45.0,
            135.0,
            -135.0,
            160.0,
            -160.0,
            5.0,
            -5.0,
            175.0,
            -175.0,
        )
    ]
    prep = poly.prepare()
    for n in (1, 15, 16):
        arr = gm.GeometryArray(pts[:n])
        free = [bool(v) for v in gm.contains(poly, arr)]
        prepared = [bool(v) for v in gm.contains(prep, arr)]
        assert free == prepared
        assert free[0] is True  # ±180 / interior seam membership
