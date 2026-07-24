"""Validation and repair — reports with location/path, self-intersection
repair, non-finite rejection, and quantize.
"""

import math
import struct
from typing import Any, cast

import gometry as gm
import numpy as np
import pytest


def _canon_geometries(value: gm.GeometryArray) -> list[str]:
    return [g.to_wkt() for g in value]


def test_single_vertex_lines_are_rejected_at_every_boundary() -> None:
    message = 'line string requires at least two vertices'
    with pytest.raises(ValueError, match=message):
        gm.LineString([(0, 0)])
    with pytest.raises(ValueError, match=message):
        gm.MultiLineString([[(0, 0)], [(1, 1), (2, 2)]])
    with pytest.raises(ValueError, match=message):
        gm.from_wkt('LINESTRING (1 2)')
    with pytest.raises(ValueError, match=message):
        gm.from_geojson('{"type": "LineString", "coordinates": [[1.0, 2.0]]}')
    with pytest.raises(ValueError, match=message):
        gm.from_wkb(
            b'\x01'
            + struct.pack('<I', 2)
            + struct.pack('<I', 1)
            + struct.pack('<2d', 1.0, 2.0)
        )
    singleton = gm.from_polyline('_p~iF~ps|U')
    assert singleton.geometry_type == 'Point'
    assert singleton.to_wkt() == 'POINT (-120.2 38.5)'
    assert singleton.to_polyline() == '_p~iF~ps|U'
    assert gm.LineString([]).is_empty
    assert gm.LineString([(0, 0), (1, 1)]).length > 0


def test_geojson_rejects_dimensionally_mixed_coordinate_sequences() -> None:
    message = 'dimensionally uniform'
    with pytest.raises(ValueError, match=message):
        gm.from_geojson(
            '{"type": "LineString", "coordinates": [[0.0, 0.0], [1.0, 1.0, 5.0]]}'
        )
    with pytest.raises(ValueError, match=message):
        gm.from_geojson(
            '{"type": "MultiPoint", "coordinates": [[0.0, 0.0, 1.0], [1.0, 1.0]]}'
        )
    with pytest.raises(ValueError, match=message):
        gm.from_geojson(
            '{"type": "Polygon", "coordinates": [[[0, 0], [1, 0, 3], [1, 1], [0, 0]]]}'
        )
    line = gm.from_geojson(
        '{"type": "LineString", "coordinates": [[0.0, 0.0, 2.0], [1.0, 1.0, 5.0]]}'
    )
    assert line.has_z


def test_validation_reports_wkb_boundary_ring_location_and_path() -> None:
    """Short WKB rings (< MIN_VERTICES_CLOSED) are rejected at parse (R03).

    Previously accepted then diagnosed by validate(); empty/short rings are now
    a structural parse failure with a typed ParseError (never PanicException).
    """
    wkb = (
        b'\x01'
        + struct.pack('<I', 3)
        + struct.pack('<I', 1)
        + struct.pack('<I', 3)
        + struct.pack('<6d', 0, 0, 1, 0, 1, 1)
    )
    with pytest.raises(gm.ParseError, match=r'ring|coordinates|vertices') as raised:
        gm.from_wkb(wkb)
    assert type(raised.value).__name__ != 'PanicException'
    # Content-level shell diagnosis still works for rings that clear the
    # structural floor but fail topology (unclosed ≥4 vertices).
    unclosed = (
        b'\x01'
        + struct.pack('<I', 3)
        + struct.pack('<I', 1)
        + struct.pack('<I', 4)
        + struct.pack('<8d', 0, 0, 1, 0, 1, 1, 0.5, 1)
    )
    report = gm.from_wkb(unclosed).validate()
    assert not report
    assert report.reason is not None


def test_validation_reports_and_repairs_self_intersecting_polygons() -> None:
    bowtie = gm.Polygon([(0, 0), (2, 2), (0, 2), (2, 0), (0, 0)])
    report = bowtie.validate()
    assert not report
    assert report.reason is not None
    assert 'self-intersection' in report.reason.lower()
    repaired = report.repair()
    assert repaired.geometry_type == 'MultiPolygon'
    assert repaired.validate().valid
    assert repaired.area == pytest.approx(2)


def test_repair_accepts_geometry_directly_and_preserves_crs() -> None:
    bowtie = gm.Polygon([(0, 0), (2, 2), (0, 2), (2, 0), (0, 0)], crs=3857)
    repaired = bowtie.repair()
    assert repaired.crs == 'EPSG:3857'
    assert repaired.geometry_type == 'MultiPolygon'
    assert repaired.validate().valid
    assert repaired.area == pytest.approx(2)
    with pytest.raises(gm.GeometryError, match='unknown repair method'):
        bowtie.repair(method=cast('Any', 'unknown'))


def test_rejects_non_finite_coordinates() -> None:
    with pytest.raises(gm.InvalidGeometryError, match='x must be finite'):
        gm.Point(math.nan, 0)


def test_quantize_preserves_crs_and_rounds_coordinates() -> None:
    point = gm.Point(1.23456, 2.34567, crs=4326).quantize(3)
    values = gm.points([1.23456, 9.87654], [2.34567, 8.76543], crs=4326).quantize(2)
    assert point.crs == 'EPSG:4326'
    assert point.coords.to_nested() == [1.235, 2.346]
    assert values[0].coords.to_nested() == [1.23, 2.35]
    assert [geometry.coords.to_nested() for geometry in values] == [
        [1.23, 2.35],
        [9.88, 8.77],
    ]
    with pytest.raises(gm.GeometryError, match='precision must be between 0 and 15'):
        point.quantize(16)
    huge = gm.Point(1e300, 2e300).quantize(15)
    assert huge.coords.to_nested() == [1e300, 2e300]


def test_is_valid_packed_arrays_match_mixed() -> None:
    """Packed point/line/polygon columns agree with mixed-row arrays."""
    packed_pts = gm.points([0.0, 1.0], [0.0, 1.0], crs=4326)
    mixed_pts = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)], crs=4326)
    assert packed_pts.to_arrow().type.extension_name == 'geoarrow.point'
    np.testing.assert_array_equal(packed_pts.is_valid, [True, True])
    np.testing.assert_array_equal(mixed_pts.is_valid, [True, True])
    np.testing.assert_array_equal(packed_pts.is_valid, mixed_pts.is_valid)
    packed_lines = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 1)]),
        gm.from_wkt('LINESTRING EMPTY'),
    ])
    mixed_lines = gm.from_wkt(['LINESTRING (0 0, 1 1)', 'LINESTRING EMPTY'])
    assert packed_lines.to_arrow().type.extension_name == 'geoarrow.linestring'
    np.testing.assert_array_equal(packed_lines.is_valid, [True, True])
    np.testing.assert_array_equal(mixed_lines.is_valid, [True, True])
    np.testing.assert_array_equal(packed_lines.is_valid, mixed_lines.is_valid)
    square = gm.box(0, 0, 1, 1)
    bowtie = gm.from_wkt('POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))')
    packed_polys = gm.GeometryArray([square, bowtie], crs=4326)
    mixed_polys = gm.from_wkt(
        ['POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 'POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))'],
        crs=4326,
    )
    assert packed_polys.to_arrow().type.extension_name == 'geoarrow.polygon'
    np.testing.assert_array_equal(packed_polys.is_valid, [True, False])
    np.testing.assert_array_equal(mixed_polys.is_valid, [True, False])
    np.testing.assert_array_equal(packed_polys.is_valid, [True, False])


def test_is_valid_is_uniform_across_scalar_and_array() -> None:
    bowtie = gm.from_wkt('POLYGON ((0 0, 1 1, 1 0, 0 1, 0 0))')
    square = gm.box(0, 0, 1, 1)
    assert square.is_valid is True
    assert bowtie.is_valid is False
    array = gm.GeometryArray([square, bowtie], crs=4326)
    np.testing.assert_array_equal(array.is_valid, [True, False])
    reports = array.validate()
    np.testing.assert_array_equal([bool(report) for report in reports], [True, False])
    assert reports[1].reason
    repaired = reports[1].repair()
    assert repaired.is_valid and repaired.crs == 'EPSG:4326'


BOWTIE_WKT = 'POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))'


def test_repair_is_deterministic() -> None:
    bowtie = gm.from_wkt(BOWTIE_WKT)
    lines = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 0)]),
        gm.LineString([(1, 0), (1, 1)]),
        gm.LineString([(1, 1), (0, 0)]),
    ])
    assert len({bowtie.repair().to_wkt() for _ in range(20)}) == 1
    assert len({bowtie.repair(method='structure').to_wkt() for _ in range(20)}) == 1
    assert len({'|'.join(gm.polygonize(list(lines)).to_wkt()) for _ in range(20)}) == 1


def test_repair_methods_are_genuinely_distinct() -> None:
    overlap = gm.from_wkt(
        'MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 0 0)), ((2 2, 6 2, 6 6, 2 6, 2 2)))'
    )
    linework = overlap.repair()
    structure = overlap.repair(method='structure')
    assert linework.geometry_type == 'MultiPolygon'
    assert linework.area == pytest.approx(24)
    assert structure.geometry_type == 'Polygon'
    assert structure.area == pytest.approx(28)
    square = '((0 0, 1 0, 1 1, 0 1, 0 0))'
    duplicated = gm.from_wkt(f'MULTIPOLYGON ({square}, {square})')
    assert duplicated.repair().is_empty
    assert duplicated.repair(method='structure').area == pytest.approx(1)


def test_repair_structure_fills_wound_shells() -> None:
    import math

    n = 600
    ring = []
    for k in range(n):
        a = k * 7 * 2 * math.pi / n % (2 * math.pi)
        r = 1.0 + 0.4 * math.sin(k * 60 * math.pi / n)
        ring.append((math.cos(a) * r * 50 + 50, math.sin(a) * r * 30 + 50))
    poly = gm.Polygon(ring)
    linework = poly.repair()
    structure = poly.repair(method='structure')
    assert linework.is_valid and structure.is_valid
    assert structure.area > linework.area * 1.3
    assert gm.covers(structure, linework)


def test_repair_parity_and_valid_fast_path() -> None:
    bowtie = gm.from_wkt(BOWTIE_WKT)
    square = gm.box(0, 0, 1, 1)
    expected = bowtie.repair().to_wkt()
    assert bowtie.repair().to_wkt() == expected
    assert bowtie.validate().repair().to_wkt() == expected
    array = gm.GeometryArray([bowtie, square])
    repaired = array.repair()
    assert repaired[0].to_wkt() == expected
    assert all(repaired.is_valid)
    assert array.repair().to_wkt() == repaired.to_wkt()
    assert square.repair() == square
    valid = gm.GeometryArray([square, gm.box(2, 2, 3, 3)], crs=4326)
    assert list(valid.repair()) == list(valid)


def test_repair_carries_z_and_m_through_the_rebuild() -> None:
    bowtie_z = gm.from_wkt('POLYGON Z ((0 0 1, 2 2 5, 2 0 3, 0 2 7, 0 0 1))')
    repaired = bowtie_z.repair()
    assert repaired.coordinate_axes == 'XYZ'
    coordinates = {
        tuple(point)
        for polygon in repaired.coords.to_nested()
        for ring in polygon
        for point in ring
    }
    assert (1.0, 1.0, 3.0) in coordinates


def test_polygonize_assembles_one_universe_across_array_rows() -> None:
    lines = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 0)]),
        gm.LineString([(1, 0), (1, 1)]),
        gm.LineString([(1, 1), (0, 0)]),
    ])
    # The free polygonize pools ALL rows' edges into one planar graph, so a ring
    # closes from edges spread across rows.
    polygons = gm.polygonize(lines)
    assert gm.polygonize(lines[0]) == lines[0].polygonize()
    assert (
        gm.polygonize_full(lines[0]).polygons == gm.polygonize_full([lines[0]]).polygons
    )
    assert len(polygons) == 1
    assert polygons[0].area == pytest.approx(0.5)
    # The per-row array method polygonizes each edge alone — a lone segment
    # closes nothing, so every group is empty (contrast with the aggregate).
    per_row = lines.polygonize()
    assert isinstance(per_row, gm.Groups)
    assert [len(per_row[i]) for i in range(len(per_row))] == [0, 0, 0]
    # The free full variant uses the same explicit pooled ownership.
    full_polygons, cuts, dangles, invalid = gm.polygonize_full(lines)
    assert len(full_polygons) == 1
    assert (len(cuts), len(dangles), len(invalid)) == (0, 0, 0)
    assert not hasattr(lines, 'polygonize_full')

    masked = gm.GeometryArray([lines[0], None, lines[1], lines[2]])
    assert gm.polygonize(masked) == polygons
    assert gm.polygonize_full(masked).polygons == full_polygons


def test_pickle_and_copy_round_trip_all_data_types() -> None:
    """Data types pickle (multiprocessing/caching just work); Z/M, leaf type,
    CRS, and epoch all survive. copy/deepcopy ride the same protocol.
    """
    import copy
    import pickle

    point = gm.Point(1, 2, crs=4326, epoch=2020.5)
    restored = pickle.loads(pickle.dumps(point))
    assert type(restored) is gm.Point
    assert restored == point and restored.epoch == 2020.5
    assert copy.copy(point) == point and copy.deepcopy(point) == point
    zm_line = gm.from_wkt('LINESTRING ZM (0 0 1 10, 1 1 2 20)', crs=3857)
    assert pickle.loads(pickle.dumps(zm_line)) == zm_line
    assert pickle.loads(pickle.dumps(gm.from_wkt('POLYGON EMPTY'))).is_empty
    packed = gm.points([1.0, 2.0], [3.0, 4.0], crs=4326, epoch=2020.5)
    restored_packed = pickle.loads(pickle.dumps(packed))
    assert list(restored_packed) == list(packed)
    assert (restored_packed.crs, restored_packed.epoch) == ('EPSG:4326', 2020.5)
    assert restored_packed.to_arrow().type.extension_name == 'geoarrow.point'
    lines = gm.GeometryArray(
        [
            gm.LineString([(0, 0, 5), (1, 1, 6), (2, 0, 7)]),
            gm.LineString([(3, 3, 8), (4, 4, 9)]),
        ],
        crs=4326,
    )
    restored_lines = pickle.loads(pickle.dumps(lines))
    assert list(restored_lines) == list(lines)
    assert restored_lines.crs == 'EPSG:4326'
    assert restored_lines.to_arrow().type.extension_name == 'geoarrow.linestring'
    polygons = gm.GeometryArray(
        [
            gm.from_wkt(
                'POLYGON Z ((0 0 1, 4 0 1, 4 4 1, 0 4 1, 0 0 1), (1 1 1, 2 1 1, 2 2 1, 1 2 1, 1 1 1))'
            ),
            gm.from_wkt('POLYGON Z ((10 10 2, 14 10 2, 14 14 2, 10 14 2, 10 10 2))'),
        ],
        crs=4326,
        epoch=2020.5,
    )
    restored_polygons = pickle.loads(pickle.dumps(polygons))
    assert [str(item) for item in restored_polygons] == [
        str(item) for item in polygons
    ]
    assert (restored_polygons.crs, restored_polygons.epoch) == ('EPSG:4326', 2020.5)
    assert restored_polygons.to_arrow().type.extension_name == 'geoarrow.polygon'
    mixed = gm.GeometryArray([gm.box(0, 0, 1, 1, crs=4326), gm.Point(2, 3, crs=4326)])
    assert list(pickle.loads(pickle.dumps(mixed))) == list(mixed)
    crs = gm.CRS(32633)
    assert pickle.loads(pickle.dumps(crs)) == crs
    h3_cell = gm.H3Cell(13.4, 52.5, resolution=9)
    assert pickle.loads(pickle.dumps(h3_cell)) == h3_cell
    s2_cell = gm.S2Cell(13.4, 52.5, level=12)
    assert pickle.loads(pickle.dumps(s2_cell)).token == s2_cell.token


def test_self_intersections_reports_every_nonsimple_node() -> None:
    bowtie = gm.LineString([(0, 0), (2, 0), (1, 1), (1, -1)])
    assert [(p.x, p.y) for p in bowtie.self_intersections()] == [(1.0, 0.0)]
    touch = gm.LineString([(0, 0), (2, 0), (2, 1), (1, 0)])
    assert [(p.x, p.y) for p in touch.self_intersections()] == [(1.0, 0.0)]
    spike = gm.LineString([(0, 0), (2, 0), (1, 0)])
    assert {(p.x, p.y) for p in spike.self_intersections()} == {(2.0, 0.0), (1.0, 0.0)}
    repeated = gm.LineString([(0, 0), (1, 1), (1, 1), (2, 0)])
    assert repeated.is_simple
    assert [(p.x, p.y) for p in repeated.self_intersections()] == []
    assert gm.from_wkt('POLYGON ((0 0, 0 0, 4 0, 4 4, 0 4, 0 0))').is_valid
    assert not gm.from_wkt('POLYGON ((0 0, 0 0, 0 0, 0 0))').is_valid
    assert not gm.from_wkt('POLYGON ((0 0, 4 0, 4 0, 0 0))').is_valid
    joined = gm.from_wkt('MULTILINESTRING ((0 0, 1 1), (1 1, 2 0))')
    assert joined.is_simple
    assert len(joined.self_intersections()) == 0
    tee = gm.from_wkt('MULTILINESTRING ((0 0, 2 0), (1 0, 1 5))')
    assert not tee.is_simple
    assert [(p.x, p.y) for p in tee.self_intersections()] == [(1.0, 0.0)]
    ringed = gm.from_wkt('MULTILINESTRING ((0 0, 1 0, 1 1, 0 0), (2 2, 0 0))')
    assert not ringed.is_simple
    assert {(p.x, p.y) for p in ringed.self_intersections()} == {(0.0, 0.0), (1.0, 1.0)}
    dupes = gm.from_wkt('MULTIPOINT ((1 1), (0 0), (1 1), (1 1))')
    assert [(p.x, p.y) for p in dupes.self_intersections()] == [(1.0, 1.0)]
    pinched = gm.from_wkt('POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))')
    assert [(p.x, p.y) for p in pinched.self_intersections()] == [(1.0, 1.0)]
    ring = gm.LineString([(0, 0), (1, 0), (1, 1), (0, 0)], crs=4326)
    assert len(ring.self_intersections()) == 0
    assert ring.self_intersections().crs == 'EPSG:4326'
    assert len(gm.Point(1, 1).self_intersections()) == 0
    assert len(gm.from_wkt('LINESTRING EMPTY').self_intersections()) == 0
    battery = [
        'POINT (0 0)',
        'MULTIPOINT ((0 0), (1 1))',
        'MULTIPOINT ((0 0), (0 0))',
        'LINESTRING (0 0, 1 0, 1 1)',
        'LINESTRING (0 0, 2 0, 1 1, 1 -1)',
        'LINESTRING (0 0, 1 1, 1 1, 2 0)',
        'MULTILINESTRING ((0 0, 1 0), (0 1, 1 1))',
        'MULTILINESTRING ((0 0, 1 1), (1 1, 2 0))',
        'GEOMETRYCOLLECTION (LINESTRING (0 0, 1 0), POINT (5 5))',
    ]
    for wkt in battery:
        geometry = gm.from_wkt(wkt)
        if geometry.geometry_type == 'GeometryCollection':
            continue
        assert bool(len(geometry.self_intersections())) == (not geometry.is_simple), wkt
    titan = gm.LineString([
        (1e308, 1e308),
        (-1e308, -1e308),
        (1e308, -1e308),
        (-1e308, 1e308),
    ])
    assert not titan.is_simple
    assert len(titan.self_intersections()) == 1
    nested = gm.from_wkt('GEOMETRYCOLLECTION (MULTIPOINT ((0 0), (0 0)))')
    assert [(p.x, p.y) for p in nested.self_intersections()] == [(0.0, 0.0)]
    twins = gm.from_wkt('GEOMETRYCOLLECTION (POINT (3 3), POINT (3 3))')
    assert [(p.x, p.y) for p in twins.self_intersections()] == [(3.0, 3.0)]
    rows = gm.GeometryArray([bowtie, gm.LineString([(0, 0), (1, 0), (1, 1), (0, 0)])])
    per_row = rows.self_intersections()
    assert [(p.x, p.y) for p in per_row[0]] == [(1.0, 0.0)]
    assert len(per_row[1]) == 0
    assert [(p.x, p.y) for p in bowtie.self_intersections()] == [(1.0, 0.0)]
    assert len(rows.self_intersections()[1]) == 0


def test_repeated_consecutive_vertices_are_removable_redundancy() -> None:
    """A duplicate consecutive vertex is stutter, not topology: valid/simple and
    behaviourally identical to the deduped geometry across every op (matching
    GEOS/Shapely), while a ring that stutters to nothing stays invalid.
    """
    shapely = pytest.importorskip('shapely')
    dup = gm.from_wkt('POLYGON ((0 0, 0 0, 4 0, 4 4, 0 4, 0 0))')
    clean = gm.from_wkt('POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))')
    other = gm.from_wkt('POLYGON ((2 2, 6 2, 6 6, 2 6, 2 2))')
    point_in = gm.from_wkt('POINT (1 1)')
    assert dup.is_valid and dup.is_simple
    assert shapely.from_wkt(dup.to_wkt()).is_valid
    assert gm.equals(dup, clean)
    assert dup.area == clean.area == 16
    for op in ('union', 'intersection', 'difference', 'symmetric_difference'):
        assert gm.equals(getattr(gm, op)(dup, other), getattr(gm, op)(clean, other))
    for pred in ('intersects', 'touches', 'covers', 'contains', 'disjoint'):
        assert getattr(gm, pred)(dup, point_in) == getattr(gm, pred)(clean, point_in)
        assert getattr(gm, pred)(dup, other) == getattr(gm, pred)(clean, other)
    assert gm.relate(dup, other) == gm.relate(clean, other)
    assert dup.self_intersections().__len__() == 0
    assert gm.coverage_is_valid([dup, other]) == gm.coverage_is_valid([clean, other])
    assert gm.coverage_invalid_edges([dup, other]) == gm.coverage_invalid_edges([
        clean,
        other,
    ])
    arr = gm.GeometryArray([dup, other])
    # These rows overlap, so the public check-and-do simplifier must reject
    # both presentations consistently rather than silently simplifying an
    # invalid coverage.
    with pytest.raises(gm.InvalidGeometryError, match='valid polygonal coverage'):
        arr.coverage_simplify(0.1)
    with pytest.raises(gm.InvalidGeometryError, match='valid polygonal coverage'):
        gm.coverage_simplify([clean, other], 0.1)
    assert gm.from_wkt('LINESTRING (0 0, 1 1, 1 1, 2 0)').is_simple
    assert not gm.from_wkt('POLYGON ((0 0, 0 0, 0 0, 0 0))').is_valid
    assert not gm.from_wkt('POLYGON ((0 0, 4 0, 4 0, 0 0))').is_valid


def test_validity_matches_geos_semantics_with_witnesses() -> None:
    """The indexed validity kernel: GEOS-agreeing verdicts (including the
    interior-disconnection class geo's validation missed entirely) and a
    witness location on every issue.
    """
    disconnected = gm.from_wkt(
        'POLYGON ((0 0, 8 0, 8 8, 0 8, 0 0), (0 4, 4 2, 8 4, 4 6, 0 4))'
    )
    report = disconnected.validate()
    assert not report.valid
    assert report.reason == 'interior is disconnected'
    assert report.location is not None
    crossing = gm.from_wkt(
        'POLYGON ((0 0, 8 0, 8 8, 0 8, 0 0), (4 4, 2 6, 2 2, 4 4), (4 4, 6 2, 6 6, 4 4))'
    )
    from shapely import from_wkt as swkt

    assert crossing.is_valid == bool(swkt(crossing.to_wkt()).is_valid)
    fan = gm.from_wkt(
        'POLYGON ((0 0, 9 0, 9 9, 0 9, 0 0), (4 4, 3 2, 5 2, 4 4), (4 4, 2 5, 2 3, 4 4), (4 4, 6 5, 5 6, 4 4))'
    )
    assert fan.is_valid
    for wkt in (
        'POLYGON ((0 0, 4 4, 4 0, 0 4, 0 0))',
        'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (5 5, 6 5, 6 6, 5 6, 5 5))',
        'MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 0 0)), ((2 2, 6 2, 6 6, 2 6, 2 2)))',
    ):
        issue = gm.from_wkt(wkt).validate()
        assert not issue.valid
        assert issue.location is not None, wkt
