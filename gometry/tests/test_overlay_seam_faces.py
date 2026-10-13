"""Geographic source intervals and seam faces retain their source attribution."""

from __future__ import annotations

import json
import math
from pathlib import Path

import gometry as gm
import numpy as np
import pytest


def _thin_seam_polygon(west: float, east: float, height: float) -> gm.Polygon:
    return gm.Polygon(
        [
            (west, 0.0),
            (east, 0.0),
            (east, height),
            (west, height),
            (west, 0.0),
        ],
        crs=4326,
    )


def _meridian_arc_length(start_latitude: float, end_latitude: float) -> float:
    """Independent composite-Simpson integral of the authored ellipsoid."""
    semi_major = 7_000_000.0
    flattening = 1.0 / 150.0
    eccentricity_squared = flattening * (2.0 - flattening)
    start = math.radians(start_latitude)
    end = math.radians(end_latitude)
    intervals = 4096
    step = (end - start) / intervals

    def meridian_radius(latitude: float) -> float:
        denominator = 1.0 - eccentricity_squared * math.sin(latitude) ** 2
        return semi_major * (1.0 - eccentricity_squared) / denominator**1.5

    weighted = [
        (1 if index in (0, intervals) else 4 if index % 2 else 2)
        * meridian_radius(start + index * step)
        for index in range(intervals + 1)
    ]
    return abs(step * math.fsum(weighted) / 3.0)


def _source_zm_at_latitude(latitude: float) -> tuple[float, float]:
    fraction = _meridian_arc_length(-30.0, latitude) / _meridian_arc_length(-30.0, 70.0)
    return 7.0 + 40.0 * fraction, 101.0 + 600.0 * fraction


def _noded_geographic_linework(crs: str) -> gm.GeometryCollection:
    cut_z, _ = _source_zm_at_latitude(-5.0)
    right_z, _ = _source_zm_at_latitude(20.0)
    return gm.GeometryCollection(
        [
            gm.from_wkt(
                'LINESTRING ZM (0 -30 7 101, 0 70 47 101)',
                crs=crs,
            ),
            gm.from_wkt('LINESTRING ZM (0 -30 7 101, 2 -30 7 101)', crs=crs),
            gm.from_wkt(
                'LINESTRING ZM '
                f'(2 -30 7 101, 2 -5 {cut_z} 101, '
                f'2 20 {right_z} 101, 2 70 47 101)',
                crs=crs,
            ),
            gm.from_wkt('LINESTRING ZM (2 70 47 101, 0 70 47 101)', crs=crs),
            gm.from_wkt('LINESTRING (0 -5, 2 -5)', crs=crs),
        ],
        crs=crs,
    )


def test_q_intersection_carries_zm_by_actual_non_wgs_meridian_arclength() -> None:
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    source = gm.from_wkt(
        'LINESTRING ZM (179 -10 10 100, 179 20 70 700)',
        crs=crs,
    )
    seam_crossing = gm.from_wkt('LINESTRING (170 0, -170 0)', crs=crs)

    result = gm.intersection(source, seam_crossing)

    fraction = _meridian_arc_length(-10.0, 0.0) / _meridian_arc_length(-10.0, 20.0)
    assert abs(fraction - 1.0 / 3.0) > 1.0e-5
    assert result.coordinate_axes == 'XYZM'
    assert (result.x, result.y) == pytest.approx((179.0, 0.0), rel=0.0, abs=1.0e-12)
    assert result.z == pytest.approx(10.0 + 60.0 * fraction, rel=0.0, abs=2.0e-9)
    assert result.m == pytest.approx(100.0 + 600.0 * fraction, rel=0.0, abs=2.0e-8)
    assert abs(result.z - 30.0) > 1.0e-4
    assert abs(result.m - 300.0) > 1.0e-3


@pytest.mark.parametrize(
    (
        'source_wkt',
        'source_first',
        'start_z',
        'end_z',
        'start_m',
        'end_m',
        'chart_parameter',
        'fraction',
    ),
    [
        pytest.param(
            'LINESTRING ZM (0 -30 7 101, 0 70 47 701)',
            True,
            7.0,
            47.0,
            101.0,
            701.0,
            0.25,
            _meridian_arc_length(-30.0, -5.0) / _meridian_arc_length(-30.0, 70.0),
            id='forward-source-first',
        ),
        pytest.param(
            'LINESTRING ZM (0 70 47 701, 0 -30 7 101)',
            True,
            47.0,
            7.0,
            701.0,
            101.0,
            0.75,
            _meridian_arc_length(-5.0, 70.0) / _meridian_arc_length(-30.0, 70.0),
            id='reverse-source-first',
        ),
        pytest.param(
            'LINESTRING ZM (0 -30 7 101, 0 70 47 701)',
            False,
            7.0,
            47.0,
            101.0,
            701.0,
            0.25,
            _meridian_arc_length(-30.0, -5.0) / _meridian_arc_length(-30.0, 70.0),
            id='forward-source-second',
        ),
        pytest.param(
            'LINESTRING ZM (0 70 47 701, 0 -30 7 101)',
            False,
            47.0,
            7.0,
            701.0,
            101.0,
            0.75,
            _meridian_arc_length(-5.0, 70.0) / _meridian_arc_length(-30.0, 70.0),
            id='reverse-source-second',
        ),
    ],
)
def test_ordinary_geographic_intersection_carries_zm_by_source_arclength(
    source_wkt: str,
    source_first: bool,
    start_z: float,
    end_z: float,
    start_m: float,
    end_m: float,
    chart_parameter: float,
    fraction: float,
) -> None:
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    source = gm.from_wkt(source_wkt, crs=crs)
    crossing = gm.from_wkt('LINESTRING (-1 -5, 1 -5)', crs=crs)
    operands = (source, crossing) if source_first else (crossing, source)

    result = gm.intersection(*operands)

    assert abs(fraction - chart_parameter) > 1.0e-5
    assert result.geometry_type == 'Point'
    assert result.coordinate_axes == 'XYZM'
    assert (result.x, result.y) == pytest.approx((0.0, -5.0), rel=0.0, abs=1.0e-12)
    assert result.z == pytest.approx(
        start_z + (end_z - start_z) * fraction,
        rel=0.0,
        abs=2.0e-12,
    )
    assert result.m == pytest.approx(
        start_m + (end_m - start_m) * fraction,
        rel=0.0,
        abs=2.0e-11,
    )


def test_ordinary_geographic_node_uses_the_operation_metric_for_interior_sources() -> (
    None
):
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    fraction = _meridian_arc_length(-30.0, -5.0) / _meridian_arc_length(-30.0, 70.0)
    # A point adds the intended XY cut without contributing unsourced line
    # endpoints that would reduce the noded output axes to XY.
    sources = gm.GeometryCollection(
        [
            gm.LineString([(0, -30, 7, 101), (0, 70, 47, 701)], crs=crs),
            gm.Point(0, -5, crs=crs),
        ],
        crs=crs,
    )

    result = sources.node()

    assert result.geometry_type == 'MultiLineString'
    assert result.coordinate_axes == 'XYZM'
    crossing = [
        row
        for row in np.asarray(result.coords, dtype=np.float64).tolist()
        if row[:2] == [0.0, -5.0]
    ]
    carried = [
        row for row in crossing if math.isfinite(row[2]) and math.isfinite(row[3])
    ]
    assert len(carried) == 2
    expected_z = 7.0 + (47.0 - 7.0) * fraction
    expected_m = 101.0 + (701.0 - 101.0) * fraction
    for _, _, z, m in carried:
        assert z == pytest.approx(expected_z, rel=0.0, abs=2.0e-9)
        assert m == pytest.approx(expected_m, rel=0.0, abs=2.0e-8)


def test_geographic_node_uses_the_seam_junction_not_greenwich() -> None:
    seam = gm.MultiLineString(
        [[(170, 0), (-170, 0)], [(180, -1), (180, 1)]], crs='OGC:CRS84'
    )
    greenwich = gm.MultiLineString(
        [[(170, 0), (-170, 0)], [(0, -1), (0, 1)]], crs='OGC:CRS84'
    )

    seam_nodes = seam.node()
    greenwich_nodes = greenwich.node()
    array_nodes = gm.GeometryArray([seam, None, greenwich]).node()

    seam_points = np.asarray(seam_nodes.coords, dtype=np.float64)
    greenwich_points = np.asarray(greenwich_nodes.coords, dtype=np.float64)
    assert any(row[:2] in ([180.0, 0.0], [-180.0, 0.0]) for row in seam_points.tolist())
    assert not any(row[:2] == [0.0, 0.0] for row in greenwich_points.tolist())
    assert array_nodes[1] is None
    assert array_nodes[0] == seam_nodes
    assert array_nodes[2] == greenwich_nodes


def test_geographic_polygonize_closes_six_rows_across_the_longitude_alias() -> None:
    parts = [
        [(170, 0), (180, 0)],
        [(-180, 0), (-170, 0)],
        [(-170, 0), (-170, 10)],
        [(-170, 10), (-180, 10)],
        [(180, 10), (170, 10)],
        [(170, 10), (170, 0)],
    ]
    linework = [gm.LineString(part, crs='OGC:CRS84') for part in parts]

    polygons = gm.polygonize_all(linework)
    groups = gm.GeometryArray(linework).polygonize()

    assert len(polygons) == 1
    assert gm.covers(polygons[0], gm.Point(179, 5, crs='OGC:CRS84'))
    assert not gm.covers(polygons[0], gm.Point(0, 5, crs='OGC:CRS84'))
    assert len(groups[0]) == 1
    assert len(groups[1]) == 0
    assert groups[0][0].to_wkb() == polygons[0].to_wkb()


def test_geographic_build_area_selects_the_cap_named_by_its_winding() -> None:
    linework = gm.LineString(
        [(-135, 80), (-45, 80), (45, 80), (135, 80), (-135, 80)],
        crs='OGC:CRS84',
    )

    area = linework.build_area()
    array_area = gm.GeometryArray([linework]).build_area()[0]

    assert gm.covers(area, gm.Point(0, 85, crs='OGC:CRS84'))
    assert not gm.covers(area, gm.Point(0, 0, crs='OGC:CRS84'))
    assert array_area.to_wkb() == area.to_wkb()


def test_ordinary_geographic_single_cut_subdivide_carries_source_arclength_zm() -> None:
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    polygon = gm.Polygon(
        [
            (0, -30, 7, 101),
            (0.25, -30, 8, 151),
            (0.5, -30, 9, 201),
            (0.75, -30, 10, 251),
            (1, -30, 11, 301),
            (1, 70, 31, 501),
            (0.75, 70, 35, 551),
            (0.5, 70, 39, 601),
            (0.25, 70, 43, 651),
            (0, 70, 47, 701),
            (0, -30, 7, 101),
        ],
        crs=crs,
    )

    parts = polygon.subdivide(max_vertices=8)

    fraction = _meridian_arc_length(-30.0, 20.0) / _meridian_arc_length(-30.0, 70.0)
    assert abs(fraction - 0.5) > 1.0e-5
    assert polygon.num_coordinates == 11
    assert len(parts) == 2
    assert [part.num_coordinates for part in parts] == [8, 8]
    assert all(part.coordinate_axes == 'XYZM' for part in parts)
    cut_nodes = [
        row
        for row in np.asarray(parts.coords, dtype=np.float64).tolist()
        if row[:2] == [0.0, 20.0]
    ]
    right_cut_nodes = [
        row
        for row in np.asarray(parts.coords, dtype=np.float64).tolist()
        if row[:2] == [1.0, 20.0]
    ]
    assert cut_nodes and right_cut_nodes
    for _, _, z, m in cut_nodes:
        assert z == pytest.approx(7.0 + 40.0 * fraction, rel=0.0, abs=2.0e-9)
        assert m == pytest.approx(101.0 + 600.0 * fraction, rel=0.0, abs=2.0e-8)
    for _, _, z, m in right_cut_nodes:
        assert z == pytest.approx(11.0 + 20.0 * fraction, rel=0.0, abs=2.0e-9)
        assert m == pytest.approx(301.0 + 200.0 * fraction, rel=0.0, abs=2.0e-8)

    groups = gm.GeometryArray([polygon, None, polygon]).subdivide(max_vertices=8)
    assert len(groups) == 3
    assert [
        None if groups[index] is None else len(groups[index]) for index in range(3)
    ] == [2, None, 2]
    for index in (0, 2):
        assert all(part.coordinate_axes == 'XYZM' for part in groups[index])
        coordinates = np.asarray(groups[index].coords, dtype=np.float64).tolist()
        for x, expected_z, expected_m in (
            (0.0, 7.0 + 40.0 * fraction, 101.0 + 600.0 * fraction),
            (1.0, 11.0 + 20.0 * fraction, 301.0 + 200.0 * fraction),
        ):
            cut_nodes = [row for row in coordinates if row[:2] == [x, 20.0]]
            assert cut_nodes
            for _, _, z, m in cut_nodes:
                assert z == pytest.approx(expected_z, rel=0.0, abs=2.0e-9)
                assert m == pytest.approx(expected_m, rel=0.0, abs=2.0e-8)


def test_ordinary_geographic_shared_paths_keeps_arclength_zm_at_overlap_start() -> None:
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    source = gm.from_wkt(
        'LINESTRING ZM (0 -30 7 101, 0 70 47 701)',
        crs=crs,
    )
    overlap = gm.from_wkt('LINESTRING (0 -5, 0 70)', crs=crs)

    same, opposite = gm.shared_paths(source, overlap).parts

    expected_z, expected_m = _source_zm_at_latitude(-5.0)
    assert same.coordinate_axes == 'XYZM'
    assert opposite.is_empty
    coordinates = np.asarray(same.coords, dtype=np.float64).tolist()
    assert coordinates[0][:2] == [0.0, -5.0]
    assert coordinates[0][2] == pytest.approx(expected_z, rel=0.0, abs=2.0e-9)
    assert coordinates[0][3] == pytest.approx(expected_m, rel=0.0, abs=2.0e-8)
    end = [row for row in coordinates if row[:2] == [0.0, 70.0]]
    assert end
    assert all((row[2], row[3]) == (47.0, 701.0) for row in end)


def test_ordinary_geographic_nullable_shared_paths_array_uses_source_arclength() -> (
    None
):
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    source = gm.from_wkt(
        'LINESTRING ZM (0 -30 7 101, 0 70 47 701)',
        crs=crs,
    )
    overlap = gm.from_wkt('LINESTRING (0 -5, 0 70)', crs=crs)
    rows = gm.GeometryArray([source, None, source])

    shared = gm.shared_paths(rows, overlap)

    assert len(shared) == 3
    assert shared[1] is None
    expected_z, expected_m = _source_zm_at_latitude(-5.0)
    for row in (shared[0], shared[2]):
        same, opposite = row.parts
        assert same.coordinate_axes == 'XYZM'
        assert opposite.is_empty
        start = np.asarray(same.coords, dtype=np.float64).tolist()[0]
        assert start[:2] == [0.0, -5.0]
        assert start[2] == pytest.approx(expected_z, rel=0.0, abs=2.0e-9)
        assert start[3] == pytest.approx(expected_m, rel=0.0, abs=2.0e-8)


def test_ordinary_geographic_line_merge_reconciles_a_split_source_vertex_by_arclength() -> (
    None
):
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    cut_z, _ = _source_zm_at_latitude(-5.0)
    lines = gm.from_wkt(
        'MULTILINESTRING ZM ('
        '(0 -30 7 101, 0 70 47 101), '
        f'(0 -30 7 101, 0 -5 {cut_z} 101), '
        f'(0 -5 {cut_z} 101, 0 70 47 101))',
        crs=crs,
    )

    merged = lines.line_merge()

    assert merged.coordinate_axes == 'XYZM'
    cut_nodes = [
        row
        for row in np.asarray(merged.coords, dtype=np.float64).tolist()
        if row[:2] == [0.0, -5.0]
    ]
    assert cut_nodes
    for _, _, z, m in cut_nodes:
        assert z == pytest.approx(cut_z, rel=0.0, abs=2.0e-9)
        assert m == 101.0


@pytest.mark.parametrize(
    ('conflict_axis', 'reverse_input_order', 'expected_axes'),
    [
        pytest.param('z', False, 'XYM', id='z-conflict'),
        pytest.param('m', False, 'XYZ', id='m-conflict'),
        pytest.param('z', True, 'XYM', id='z-conflict-reversed'),
        pytest.param('m', True, 'XYZ', id='m-conflict-reversed'),
    ],
)
def test_ordinary_geographic_line_merge_detects_only_conflicting_source_axis(
    conflict_axis: str,
    reverse_input_order: bool,
    expected_axes: str,
) -> None:
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    cut_z, _ = _source_zm_at_latitude(-5.0)
    wrong_z = cut_z + 3.0 if conflict_axis == 'z' else cut_z
    wrong_m = 131.0 if conflict_axis == 'm' else 101.0
    members = [
        '(0 -30 7 101, 0 70 47 101)',
        f'(0 -30 7 101, 0 -5 {wrong_z} {wrong_m})',
        f'(0 -5 {wrong_z} {wrong_m}, 0 70 47 101)',
    ]
    if reverse_input_order:
        members.reverse()
    lines = gm.from_wkt(f'MULTILINESTRING ZM ({", ".join(members)})', crs=crs)

    merged = lines.line_merge()

    assert merged.coordinate_axes == expected_axes
    cut_nodes = [
        row
        for row in np.asarray(merged.coords, dtype=np.float64).tolist()
        if row[:2] == [0.0, -5.0]
    ]
    assert cut_nodes
    if conflict_axis == 'z':
        assert all(row[2] == 101.0 for row in cut_nodes)
    else:
        assert all(
            row[2] == pytest.approx(cut_z, rel=0.0, abs=2.0e-9) for row in cut_nodes
        )


def test_geographic_shared_paths_conflicting_signed_zero_is_order_independent() -> None:
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    positive = gm.LineString(
        [(0.0, -30.0, 0.0, 101.0), (0.0, 70.0, 47.0, 701.0)],
        crs=crs,
    )
    negative = gm.LineString(
        [(0.0, -30.0, -0.0, 101.0), (0.0, 70.0, 47.0, 701.0)],
        crs=crs,
    )

    for left, right in ((positive, negative), (negative, positive)):
        same, opposite = gm.shared_paths(left, right).parts

        assert same.coordinate_axes == 'XYM'
        assert opposite.is_empty
        start = [
            row
            for row in np.asarray(same.coords, dtype=np.float64).tolist()
            if row[:2] == [0.0, -30.0]
        ]
        assert start
        assert all(row[2] == 101.0 for row in start)


def test_ordinary_geographic_polygonize_and_full_use_source_arclength_before_axes() -> (
    None
):
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    linework = _noded_geographic_linework(crs)
    expected_z, _ = _source_zm_at_latitude(-5.0)

    polygons = gm.polygonize_all(linework)
    full = gm.polygonize_all(linework, diagnostics=True)

    assert len(polygons) == 2
    assert len(full.polygons) == 2
    assert all(polygon.coordinate_axes == 'XYZM' for polygon in polygons)
    assert all(polygon.coordinate_axes == 'XYZM' for polygon in full.polygons)
    cut_nodes = [
        row
        for row in np.asarray(full.polygons.coords, dtype=np.float64).tolist()
        if row[:2] == [0.0, -5.0]
    ]
    assert cut_nodes
    for _, _, z, m in cut_nodes:
        assert z == pytest.approx(expected_z, rel=0.0, abs=2.0e-9)
        assert m == 101.0


def test_ordinary_geographic_nullable_polygonize_array_uses_source_arclength() -> None:
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    linework = _noded_geographic_linework(crs)
    groups = gm.GeometryArray([linework, None, linework]).polygonize()

    assert len(groups) == 3
    assert [
        None if groups[index] is None else len(groups[index]) for index in range(3)
    ] == [2, None, 2]
    expected_z, _ = _source_zm_at_latitude(-5.0)
    for index in (0, 2):
        polygons = groups[index]
        assert all(polygon.coordinate_axes == 'XYZM' for polygon in polygons)
        cut_nodes = [
            row
            for row in np.asarray(polygons.coords, dtype=np.float64).tolist()
            if row[:2] == [0.0, -5.0]
        ]
        assert cut_nodes
        for _, _, z, m in cut_nodes:
            assert z == pytest.approx(expected_z, rel=0.0, abs=2.0e-9)
            assert m == 101.0


def test_ordinary_geographic_build_area_keeps_a_source_metric_vertex() -> None:
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    linework = _noded_geographic_linework(crs)
    expected_z, _ = _source_zm_at_latitude(-5.0)

    area = linework.build_area()

    assert area.coordinate_axes == 'XYZM'
    cut_nodes = [
        row
        for row in np.asarray(area.coords, dtype=np.float64).tolist()
        if row[:2] == [0.0, -5.0]
    ]
    assert cut_nodes
    for _, _, z, m in cut_nodes:
        assert z == pytest.approx(expected_z, rel=0.0, abs=2.0e-9)
        assert m == 101.0


def test_ordinary_geographic_repair_uses_source_arclength_at_a_self_crossing() -> None:
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    cut_z, _ = _source_zm_at_latitude(-5.0)
    crossing = gm.from_wkt(
        'POLYGON ZM ('
        f'(0 -30 7 101, 0 70 47 101, 2 20 {cut_z} 101, '
        f'-2 -30 {cut_z} 101, 0 -30 7 101))',
        crs=crs,
    )

    repaired = crossing.repair()

    assert repaired.is_valid
    assert repaired.coordinate_axes == 'XYZM'
    cut_nodes = [
        row
        for row in np.asarray(repaired.coords, dtype=np.float64).tolist()
        if row[:2] == [0.0, -5.0]
    ]
    assert cut_nodes
    for _, _, z, m in cut_nodes:
        assert z == pytest.approx(cut_z, rel=0.0, abs=2.0e-9)
        assert m == 101.0


def test_ordinary_geographic_nullable_repair_array_uses_source_arclength() -> None:
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    cut_z, _ = _source_zm_at_latitude(-5.0)
    crossing = gm.from_wkt(
        'POLYGON ZM ('
        f'(0 -30 7 101, 0 70 47 101, 2 20 {cut_z} 101, '
        f'-2 -30 {cut_z} 101, 0 -30 7 101))',
        crs=crs,
    )

    repaired = gm.GeometryArray([crossing, None, crossing]).repair()

    assert repaired[1] is None
    for index in (0, 2):
        assert repaired[index].is_valid
        assert repaired[index].coordinate_axes == 'XYZM'
        cut_nodes = [
            row
            for row in np.asarray(repaired[index].coords, dtype=np.float64).tolist()
            if row[:2] == [0.0, -5.0]
        ]
        assert cut_nodes
        for _, _, z, m in cut_nodes:
            assert z == pytest.approx(cut_z, rel=0.0, abs=2.0e-9)
            assert m == 101.0


def test_ordinary_geographic_coverage_clean_keeps_source_arclength_at_shared_nodes() -> (
    None
):
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    cut_z, cut_m = _source_zm_at_latitude(-5.0)
    outer = gm.from_wkt(
        'POLYGON ZM ((0 -30 7 101, 2 -30 7 101, 2 70 47 701, '
        '0 70 47 701, 0 -30 7 101))',
        crs=crs,
    )
    inner = gm.from_wkt('POLYGON ((0 -5, 1 -5, 1 20, 0 20, 0 -5))', crs=crs)

    cleaned = gm.coverage_clean(
        [outer, inner],
        grid_size=0.0,
        overlap_rule='min_index',
    )

    assert cleaned[0].coordinate_axes == 'XYZM'
    cut_nodes = [
        row
        for row in np.asarray(cleaned[0].coords, dtype=np.float64).tolist()
        if row[:2] == [0.0, -5.0]
    ]
    assert cut_nodes
    for _, _, z, m in cut_nodes:
        assert z == pytest.approx(cut_z, rel=0.0, abs=2.0e-9)
        assert m == pytest.approx(cut_m, rel=0.0, abs=2.0e-8)


def test_ordinary_geographic_nullable_line_merge_array_carries_source_arclength() -> (
    None
):
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    cut_z, _ = _source_zm_at_latitude(-5.0)
    lines = gm.from_wkt(
        'MULTILINESTRING ZM ('
        '(0 -30 7 101, 0 70 47 101), '
        f'(0 -30 7 101, 0 -5 {cut_z} 101), '
        f'(0 -5 {cut_z} 101, 0 70 47 101))',
        crs=crs,
    )
    rows = gm.GeometryArray([lines, None, lines])

    merged = rows.line_merge()

    assert len(merged) == 3
    assert merged[1] is None
    for row in (merged[0], merged[2]):
        assert row.coordinate_axes == 'XYZM'
        cut_nodes = [
            coordinate
            for coordinate in np.asarray(row.coords, dtype=np.float64).tolist()
            if coordinate[:2] == [0.0, -5.0]
        ]
        assert cut_nodes
        for _, _, z, m in cut_nodes:
            assert z == pytest.approx(cut_z, rel=0.0, abs=2.0e-9)
            assert m == 101.0


@pytest.mark.parametrize(
    ('seam_latitude', 'expected'),
    [
        pytest.param(-10.0, (179.0, -10.0, 10.0, 100.0), id='source-t0'),
        pytest.param(20.0, (179.0, 20.0, 70.0, 700.0), id='source-t1'),
    ],
)
def test_q_intersection_keeps_the_authentic_source_endpoint_record(
    seam_latitude: float,
    expected: tuple[float, float, float, float],
) -> None:
    crs = '+proj=longlat +a=7000000 +rf=150 +type=crs'
    source = gm.from_wkt(
        'LINESTRING ZM (179 -10 10 100, 179 20 70 700)',
        crs=crs,
    )
    seam_crossing = gm.from_wkt(
        f'LINESTRING (170 {seam_latitude}, -170 {seam_latitude})',
        crs=crs,
    )

    result = gm.intersection(source, seam_crossing)

    assert result.coordinate_axes == 'XYZM'
    assert (result.x, result.y, result.z, result.m) == expected


@pytest.mark.parametrize(
    ('west', 'east', 'height'),
    [
        pytest.param(179.0, -179.0, 1e-8, id='two-degree-below-probe-step'),
        pytest.param(179.0, -179.0, 1e-9, id='two-degree-well-below-probe-step'),
        pytest.param(170.0, -170.0, 1e-6, id='twenty-degree-at-probe-step'),
        pytest.param(170.0, -170.0, 1e-7, id='twenty-degree-at-half-step'),
        pytest.param(170.0, -170.0, 1e-8, id='twenty-degree-below-probe-step'),
    ],
)
def test_thin_seam_faces_keep_source_support_below_probe_step(
    west: float, east: float, height: float
) -> None:
    polygon = _thin_seam_polygon(west, east, height)
    union = gm.union_all([polygon])

    assert union.bounds == polygon.bounds
    for longitude in ((west + 180.0) / 2.0, (east - 180.0) / 2.0):
        assert gm.contains(union, gm.Point(longitude, height / 2.0, crs=4326))
    assert not gm.contains(union, gm.Point(0.0, height / 2.0, crs=4326))


def test_thin_seam_union_keeps_exclusions_and_disjoint_area() -> None:
    thin = _thin_seam_polygon(170.0, -170.0, 1e-6)
    far = gm.box(0.0, 40.0, 1.0, 41.0, crs=4326)
    union = gm.union_all([thin, far])

    assert gm.contains(union, gm.Point(175.0, 0.5e-6, crs=4326))
    assert gm.contains(union, gm.Point(-175.0, 0.5e-6, crs=4326))
    assert not gm.contains(union, gm.Point(0.5, 0.5, crs=4326))

    holed = gm.Polygon(
        [
            (170.0, -1.0),
            (-170.0, -1.0),
            (-170.0, 1.0),
            (170.0, 1.0),
            (170.0, -1.0),
        ],
        holes=[
            [
                (175.0, -0.5),
                (-175.0, -0.5),
                (-175.0, 0.5),
                (175.0, 0.5),
                (175.0, -0.5),
            ]
        ],
        crs=4326,
    )
    holed_union = gm.union_all([holed])
    assert gm.contains(holed_union, gm.Point(179.0, 0.75, crs=4326))
    assert not gm.contains(holed_union, gm.Point(179.0, 0.0, crs=4326))

    degenerate = gm.Polygon(
        [(0.0, 0.0), (1.0, 0.0), (2.0, 0.0), (1.0, 0.0), (0.0, 0.0)],
        crs=4326,
    )
    assert gm.union_all([degenerate]).area == 0.0

    ordinary = gm.Polygon(
        [(0.0, 0.0), (2.0, 0.0), (2.0, 1e-8), (0.0, 1e-8), (0.0, 0.0)],
        crs=4326,
    )
    assert gm.GeometryCollection([ordinary], crs=4326).area == pytest.approx(
        ordinary.area
    )


def _native_seam_overlap_pair() -> tuple[gm.Polygon, gm.Polygon]:
    return (
        gm.Polygon(
            [
                (170.0, -1.0),
                (-170.0, -1.0),
                (-170.0, 2.0),
                (170.0, 2.0),
                (170.0, -1.0),
            ],
            crs=4326,
        ),
        gm.Polygon(
            [
                (177.0, 0.0),
                (-174.0, 1.0),
                (-174.0, 3.0),
                (177.0, 2.0),
                (177.0, 0.0),
            ],
            crs=4326,
        ),
    )


def test_native_overlap_seam_union_keeps_both_selected_regions() -> None:
    left, right = _native_seam_overlap_pair()
    overlap = gm.Point(179.0, 1.0, crs=4326)
    assert gm.contains(left, overlap)
    assert gm.contains(right, overlap)

    union = gm.union_all([left, right])

    assert union.is_valid
    assert gm.contains(union, overlap)
    assert gm.contains(union, gm.Point(-176.0, 2.5, crs=4326))
    assert not gm.contains(union, gm.Point(0.0, 1.0, crs=4326))


def test_native_union_closes_multiple_west_seam_walls() -> None:
    left, right = _native_seam_overlap_pair()
    higher_west = gm.Polygon(
        [(-180.0, 60.0), (-179.0, 61.0), (-180.0, 62.0), (-180.0, 60.0)],
        crs=4326,
    )
    west = gm.Polygon(
        [(-180.0, 50.0), (-179.0, 51.0), (-180.0, 52.0), (-180.0, 50.0)],
        crs=4326,
    )
    operands = [left, right, higher_west, west]

    overlap = gm.Point(179.0, 1.0, crs=4326)
    assert gm.contains(left, overlap)
    assert gm.contains(right, overlap)
    union = gm.union_all(operands)

    assert union.is_valid
    assert gm.contains(union, overlap)
    assert gm.contains(union, gm.Point(-176.0, 1.5, crs=4326))
    assert gm.contains(union, gm.Point(-179.95, 61.0, crs=4326))
    assert gm.contains(union, gm.Point(-179.95, 51.0, crs=4326))
    assert not gm.contains(union, gm.Point(-179.95, 56.0, crs=4326))
    assert not gm.contains(union, gm.Point(0.0, 1.0, crs=4326))
    output_parts = [union] if union.geometry_type == 'Polygon' else list(union.parts)
    emitted = {
        (float(coordinate[0]), float(coordinate[1]))
        for polygon in output_parts
        for ring in (polygon.exterior, *polygon.interiors)
        for coordinate in ring.coords
    }
    assert {
        (-180.0, 50.0),
        (-180.0, 52.0),
        (-180.0, 60.0),
        (-180.0, 62.0),
    } <= emitted
    assert union.to_wkb() == gm.union_all(list(reversed(operands))).to_wkb()


def test_native_union_closes_an_upward_east_seam_wall() -> None:
    left, right = _native_seam_overlap_pair()
    east = gm.Polygon(
        [(180.0, 60.0), (180.0, 62.0), (179.0, 61.0), (180.0, 60.0)],
        crs=4326,
    )

    union = gm.union_all([left, right, east])

    assert union.is_valid
    assert gm.contains(union, gm.Point(179.95, 61.0, crs=4326))
    output_parts = [union] if union.geometry_type == 'Polygon' else list(union.parts)
    emitted = {
        (float(coordinate[0]), float(coordinate[1]))
        for polygon in output_parts
        for ring in (polygon.exterior, *polygon.interiors)
        for coordinate in ring.coords
    }
    assert {(180.0, 60.0), (180.0, 62.0)} <= emitted


def test_native_union_preserves_a_seam_touching_hole() -> None:
    left, right = _native_seam_overlap_pair()
    holed = gm.Polygon(
        [(170.0, 70.0), (-170.0, 70.0), (-170.0, 74.0), (170.0, 74.0), (170.0, 70.0)],
        holes=[[(-180.0, 71.0), (-180.0, 73.0), (-179.0, 72.0), (-180.0, 71.0)]],
        crs=4326,
    )
    assert gm.contains(holed, gm.Point(-179.95, 70.5, crs=4326))
    assert not gm.contains(holed, gm.Point(-179.95, 72.0, crs=4326))

    union = gm.union_all([left, right, holed])

    assert union.is_valid
    assert gm.contains(union, gm.Point(-179.95, 70.5, crs=4326))
    assert not gm.contains(union, gm.Point(-179.95, 72.0, crs=4326))
    output_parts = [union] if union.geometry_type == 'Polygon' else list(union.parts)
    emitted = {
        (float(coordinate[0]), float(coordinate[1]))
        for polygon in output_parts
        for ring in (polygon.exterior, *polygon.interiors)
        for coordinate in ring.coords
    }
    assert {(-180.0, 71.0), (-180.0, 73.0)} <= emitted


# OpenStreetMap Argentina and Uruguay borders near the Rio de la Plata, clipped
# to a 0.03-degree box. Two border vertices lie one ULP apart, so the dissolve
# has a sliver face containing no representable point.
_ARGENTINA_CLIP = (
    'MULTIPOLYGON (((-58.10222180941994 -33.010937543229446, -58.084289 -33.0009615, '
    '-58.085477253434156 -32.99, -58.089648910878246 -32.99, '
    '-58.0878711 -32.9965279, -58.10222180941994 -33.010937543229446)), '
    '((-58.11 -33.00166081596568, -58.10769305699579 -32.99, -58.11 -32.99, '
    '-58.11 -33.00166081596568)))'
)
_URUGUAY_CLIP = (
    'POLYGON ((-58.11 -33.02, -58.08 -33.02, -58.08 -32.99, '
    '-58.085477253434156 -32.99, -58.084289 -33.0009615, '
    '-58.10256757394891 -33.011129892448544, -58.0878711 -32.9965279, '
    '-58.089648910878246 -32.99, -58.10765630407339 -32.99, '
    '-58.11 -33.001904214459806, -58.11 -33.02))'
)


def test_dissolve_keeps_sliver_faces_without_representable_interior() -> None:
    argentina = gm.from_wkt(_ARGENTINA_CLIP).with_crs(4326)
    uruguay = gm.from_wkt(_URUGUAY_CLIP).with_crs(4326)
    # A disjoint seam member needs membership probes for faces it does not bound.
    seam = _thin_seam_polygon(179.0, -179.0, 1.0)

    dissolved = gm.GeometryCollection([seam, argentina, uruguay], crs=4326).area

    assert dissolved == pytest.approx(
        gm.union_all([argentina, uruguay]).area + gm.union_all([seam]).area,
        rel=1e-12,
    )


def _assert_geographic_overlay_area_identity(
    left: gm.Geometry, right: gm.Geometry
) -> gm.Geometry:
    intersection = gm.intersection(left, right)
    union = gm.union(left, right)
    symmetric_difference = gm.symmetric_difference(left, right)

    assert intersection.is_valid
    assert union.is_valid
    assert symmetric_difference.is_valid
    assert intersection.length > 0.0
    # Areas are geodesic square metres; allow 1e-12 relative and 1e-3 absolute
    # floating-point roundoff in the overlay identity.
    assert intersection.area + symmetric_difference.area == pytest.approx(
        union.area, rel=1e-12, abs=1e-3
    )
    return intersection


def test_minimized_country_shared_border_intersection_keeps_its_source_interval() -> (
    None
):
    russia = gm.Polygon(
        [
            (-179.5551997, 70.0996777),
            (179.8176414, 69.7948444),
            (-179.8683419985996, 69.94636166206753),
            (-179.5551997, 70.0996777),
        ],
        crs=4326,
    )
    norway = gm.Polygon(
        [
            (179.8176414, 69.7948444),
            (-179.5551997, 70.0996777),
            (-179.86921630140046, 69.94816043793249),
            (179.8176414, 69.7948444),
        ],
        crs=4326,
    )

    intersection = _assert_geographic_overlay_area_identity(russia, norway)

    assert intersection.area == pytest.approx(0.0, abs=1e-3)
    assert gm.covers(intersection, gm.Point(179.8176414, 69.7948444, crs=4326))


def test_synthetic_seam_shared_edge_intersection_is_valid() -> None:
    left = gm.Polygon(
        [(179.0, 0.0), (-179.0, 0.0), (-179.8, -1.0), (179.0, 0.0)],
        crs=4326,
    )
    right = gm.Polygon(
        [(-179.0, 0.0), (179.0, 0.0), (179.8, 1.0), (-179.0, 0.0)],
        crs=4326,
    )

    intersection = _assert_geographic_overlay_area_identity(left, right)

    assert intersection.area == pytest.approx(0.0, abs=1e-3)
    assert gm.covers(intersection, gm.Point(179.0, 0.0, crs=4326))


def test_russia_norway_intersection_emits_the_shared_country_border() -> None:
    fixture = Path(__file__).resolve().parents[1] / 'fixtures/osm_countries_0_1.geojson'
    source = json.loads(fixture.read_text(encoding='utf-8'))
    wanted = {'RU', 'NO'}
    features = {
        feature['properties']['tags'].get('ISO3166-1:alpha2'): feature
        for feature in source['features']
        if feature['properties']['tags'].get('ISO3166-1:alpha2') in wanted
    }
    countries = dict(
        zip(
            ('RU', 'NO'),
            gm.from_geojson(
                json.dumps({
                    'type': 'FeatureCollection',
                    'features': [features['RU'], features['NO']],
                }),
                crs=4326,
            ),
            strict=True,
        )
    )

    intersection = _assert_geographic_overlay_area_identity(
        countries['RU'], countries['NO']
    )

    assert intersection.area == pytest.approx(0.0, abs=1e-3)
    assert gm.covers(intersection, gm.Point(31.4448003, 70.0996777, crs=4326))
