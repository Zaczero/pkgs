"""Public-surface contracts — boundary validation of scalar/integer arguments,
Pythonic signature defaults, the curated `__all__`, and the README quickstart.
"""

import inspect
import math
import re
from importlib import metadata
from pathlib import Path
from typing import cast

import gometry as gm
from gometry import _lib
import numpy as np
import pytest

FLAT_FAMILY_EXPORTS = {
    'crs_apply',
    'crs_authorities',
    'crs_cache_info',
    'crs_catalog',
    'crs_celestial_bodies',
    'crs_clear_cache',
    'crs_codes',
    'crs_config',
    'crs_configure',
    'crs_ellipsoids',
    'crs_engine',
    'crs_grid',
    'crs_info',
    'crs_prime_meridians',
    'crs_proj_operations',
    'crs_reset',
    'crs_roundtrip',
    'crs_search',
    'crs_transform',
    'crs_transform_bounds',
    'crs_unit',
    'crs_units',
    'crs_utm_zones',
    'h3_base_cells',
    'h3_bounding_cell',
    'h3_cells',
    'h3_cover',
    'h3_difference',
    'h3_intersection',
    'h3_pentagons',
    'h3_union',
    's2_bounding_cell',
    's2_cells',
    's2_cover',
    's2_difference',
    's2_intersection',
    's2_union',
    'geohash_bounding_cell',
    'geohash_cells',
    'geohash_cover',
    'geohash_difference',
    'geohash_intersection',
    'geohash_union',
    'tile_bounding_cell',
    'tile_cells',
    'tile_cover',
    'tile_difference',
    'tile_intersection',
    'tile_union',
    'pluscode_encode',
    'pluscode_polygon',
    'pluscode_recover',
    'pluscode_shorten',
    'osm_shortlink_encode',
    'osm_shortlink_location',
}


def test_flat_native_family_exports_are_direct_aliases() -> None:
    for name in FLAT_FAMILY_EXPORTS:
        assert getattr(gm, name) is getattr(_lib, name)


def test_consolidated_algorithm_families_have_one_canonical_dispatch_surface() -> None:
    """Method selectors replace parallel public spellings without losing capability."""
    polygon = gm.Polygon([(0, 0), (3, 0), (3, 3), (0, 3), (0, 0)])
    sites = gm.MultiPoint([(0, 0), (3, 0), (0, 3), (3, 3)])
    for method, source in (
        ('earcut', polygon),
        ('delaunay', sites),
        ('constrained', polygon),
    ):
        triangles = source.triangulate(method=method)
        assert isinstance(triangles, gm.GeometryArray)
        assert len(triangles) > 0

    points = gm.points([3, 0, 2], [3, 0, 2])
    for curve in ('hilbert', 'morton'):
        keys = points.spatial_key(curve=curve)
        assert keys.shape == (3,)
        ordered = points.sort_by_spatial_key(curve=curve)
        assert isinstance(ordered, gm.GeometryArray)

    line = gm.LineString([(0, 0), (10, 0)])
    assert line.segmentize(2.5).num_coordinates == 5
    assert line.segmentize(fraction=0.5).num_coordinates == 3
    with pytest.raises(gm.GeometryError, match='exactly one'):
        line.segmentize()
    with pytest.raises(TypeError, match='positional-only'):
        line.segmentize(max_length=2.5, fraction=0.5)

    measured = gm.from_wkt('LINESTRING M (0 0 0, 10 0 100)')
    assert measured.line_interpolate(50, basis='m').x == pytest.approx(5)
    assert measured.line_locate(gm.Point(5, 0), basis='m') == pytest.approx(50)
    assert (
        measured.line_substring(20, 80, basis='m').to_wkt()
        == 'LINESTRING M (2 0 20, 8 0 80)'
    )
    with pytest.raises(gm.GeometryError, match="basis='distance'"):
        measured.line_interpolate(50, basis='m', normalized=True)
    with pytest.raises(gm.GeometryError, match="basis='distance'"):
        measured.line_interpolate(count=2, basis='m')


def test_bulk_refactor_entrypoints_are_single_native_calls() -> None:
    """Hot/bulk public entrypoints contain no Python row-dispatch bytecode."""
    for name in (
        'from_features',
        'nearest_points',
        'contains',
        'bearing',
        'point_between',
    ):
        function = getattr(gm, name)
        assert inspect.isbuiltin(function), name
        assert function.__module__ == 'gometry._lib', name


def test_geometry_array_vectorizes_homogeneous_shapely_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shapely = pytest.importorskip('shapely')
    original = shapely.to_wkb
    calls: list[object] = []

    def counted(values: object, *args: object, **kwargs: object) -> object:
        calls.append(values)
        return original(values, *args, **kwargs)

    monkeypatch.setattr(shapely, 'to_wkb', counted)
    ndarray = np.asarray([shapely.Point(1, 2), shapely.Point(3, 4)], dtype=object)
    array = gm.GeometryArray(ndarray)
    assert array.to_wkt() == ['POINT (1 2)', 'POINT (3 4)']
    assert calls == [ndarray]

    values = [shapely.Point(5, 6), None]
    nullable = gm.GeometryArray(values)
    assert nullable.to_wkt() == ['POINT (5 6)', None]
    assert calls == [ndarray, values]

    calls.clear()
    mixed = gm.GeometryArray([shapely.Point(7, 8), gm.Point(9, 10)])
    assert mixed.to_wkt() == ['POINT (7 8)', 'POINT (9 10)']
    assert calls == []


def _normalize_boundary_cases(
    cases: list[tuple], default_error: type[BaseException]
) -> list[tuple[object, type[BaseException], str | None]]:
    normalized: list[tuple[object, type[BaseException], str | None]] = []
    for entry in cases:
        if len(entry) == 2:
            call, msg = entry
            normalized.append((call, default_error, msg))
        else:
            call, error_type, msg = entry
            normalized.append((call, error_type, msg))
    return normalized


def test_public_integer_boundaries_raise_value_error_not_overflow() -> None:
    huge = 10**100
    point = gm.Point(0, 0, crs=4326)
    area = gm.box(-1, -1, 1, 1, crs=4326)
    h3_cell = gm.H3Cell(0, 0, resolution=5)
    s2_cell = gm.S2Cell(0, 0, level=5)
    idx = gm.SpatialIndex(gm.points([0, 1], [0, 1]))
    cases = [
        (lambda: gm.H3Cell(0, 0, resolution=-1), 'H3 resolution'),
        (lambda: gm.H3Cell(0, 0, resolution=huge), 'H3 resolution'),
        (lambda: gm.S2Cell(0, 0, level=-1), 'S2 level'),
        (lambda: gm.S2Cell(0, 0, level=huge), 'S2 level'),
        (lambda: gm.h3_cover(area, resolution=-1), 'H3 resolution'),
        (lambda: gm.s2_cover(area, level=-1), 'S2 level'),
        (lambda: gm.s2_cover(area, level=5, max_cells=-1), 'max_cells'),
        (lambda: h3_cell.parent(-1), 'H3 resolution'),
        (lambda: h3_cell.children(huge), 'H3 resolution'),
        (lambda: h3_cell.grid_disk(-1), 'H3 grid distance'),
        (lambda: h3_cell.grid_ring(huge), 'H3 grid distance'),
        (lambda: gm.CellArray([-1], type=gm.H3Cell).polygon, 'H3Cell id'),
        (lambda: gm.CellArray([huge], type=gm.H3Cell).polygon, 'H3Cell id'),
        (lambda: s2_cell.parent(-1), 'S2 level'),
        (lambda: gm.CellArray([-1], type=gm.S2Cell).polygon, 'S2Cell id'),
        (lambda: gm.CellArray([huge], type=gm.S2Cell).polygon, 'S2Cell id'),
        (lambda: point.to_wkt(output_dimension=-1), 'WKT output_dimension'),
        (lambda: point.quantize(huge), 'precision'),
        (lambda: gm.Point(0, 0).to_wkb(precision=16), 'precision'),
        (lambda: gm.Point(0, 0).buffer(1, quadrant_segments=-1), 'quadrant_segments'),
        (lambda: idx.nearest(gm.Point(0, 0), k=-1), 'nearest k'),
        (
            lambda: gm.crs_transform_bounds(4326, 3857, (-1, 0, 1, 1), densify=huge),
            OverflowError,
            None,
        ),
        (
            lambda: gm.crs_roundtrip(4326, 3857, 0, 0, iterations=huge),
            OverflowError,
            None,
        ),
        (lambda: gm.crs_info(-1), 'EPSG code'),
        (lambda: gm.crs_info(huge), 'EPSG code'),
        (lambda: gm.Point(0, 0, crs=-1), 'EPSG code'),
        (lambda: gm.Point(0, 0).set_crs(huge), 'EPSG code'),
        (lambda: gm.Point(0, 0, crs=4326).to_crs(-1), 'EPSG code'),
        (lambda: gm.crs_search('WGS 84', limit=0), 'CRS search limit'),
        (lambda: gm.crs_search('WGS 84', limit=1001), 'CRS search limit'),
    ]
    for call, error_type, message in _normalize_boundary_cases(cases, ValueError):
        kwargs = {'match': message} if message is not None else {}
        with pytest.raises(error_type, **kwargs):
            call()


def test_crs_scalar_boundaries_raise_geometry_errors_not_overflow() -> None:
    huge = 10**1000
    geom = gm.Point(0, 0, crs=4326)
    array = gm.points([0], [0], crs=4326)
    cases = [
        (
            lambda: gm.CRS(4326).operation(3857, source_epoch=huge),
            TypeError,
            'source_epoch',
        ),
        (lambda: gm.CRS(4326).operation(3857, accuracy=huge), TypeError, 'accuracy'),
        (
            lambda: gm.CRS(4326).operation(3857, area_of_interest=(huge, 0, 1, 1)),
            TypeError,
            'area',
        ),
        (
            lambda: gm.CRS(4326).operation(3857, source_epoch=math.inf),
            ValueError,
            'source_epoch',
        ),
        (
            lambda: gm.CRS(4326).operation(3857, accuracy=math.nan),
            ValueError,
            'accuracy',
        ),
        (lambda: gm.CRS(4326).operation(3857, at=(huge, 0)), TypeError, 'x'),
        (lambda: gm.CRS(4326).operation(3857, at=(0, 0, 0, huge)), TypeError, 't'),
        (lambda: gm.CRS(3857).factors(huge, 0), TypeError, 'lon'),
        (
            lambda: gm.crs_transform(4326, 3857, 0, 0, source_epoch=huge),
            TypeError,
            'source_epoch',
        ),
        (
            lambda: gm.crs_transform(
                4326, 3857, 0, 0, area_of_interest=(huge, 0, 1, 1)
            ),
            TypeError,
            'area',
        ),
        (
            lambda: gm.crs_transform(4326, 3857, [0], [0], accuracy=-1),
            ValueError,
            'accuracy',
        ),
        (
            lambda: gm.crs_transform_bounds(
                4326, 3857, (-1, -1, 1, 1), target_epoch=huge
            ),
            TypeError,
            'target_epoch',
        ),
        (
            lambda: gm.crs_transform_bounds(4326, 3857, (huge, -1, 1, 1)),
            TypeError,
            'bounds',
        ),
        (
            lambda: gm.crs_transform_bounds(4979, 4978, (huge, -1, 0, 1, 1, 1)),
            TypeError,
            'bounds',
        ),
        (lambda: gm.crs_roundtrip(4326, 3857, huge, 0), TypeError, 'x'),
        (
            lambda: gm.crs_roundtrip(4326, 3857, 0, 0, accuracy=math.nan),
            ValueError,
            'accuracy',
        ),
        (lambda: geom.to_crs(3857, epoch=huge), TypeError, 'epoch'),
        (lambda: geom.to_crs(3857, accuracy=huge), TypeError, 'accuracy'),
        (lambda: array.to_crs(3857, epoch=huge), TypeError, 'epoch'),
    ]
    for call, error_type, message in cases:
        with pytest.raises(error_type, match=message):
            call()


def test_public_scalar_boundaries_raise_geometry_errors_not_overflow() -> None:
    huge = 10**1000
    geom = gm.Point(0, 0, crs=4326)
    other = gm.Point(1, 1, crs=4326)
    array = gm.points([0], [0], crs=4326)
    line = gm.LineString([(0, 0), (1, 0)])
    polygon = gm.box(0, 0, 1, 1)
    sites = gm.MultiPoint([(0, 0), (1, 0), (0, 1)])
    idx = gm.SpatialIndex(array)
    cases = [
        (lambda: gm.Point(huge, 0), 'x'),
        (lambda: gm.Point(0, 0, epoch=huge), 'epoch'),
        (lambda: gm.Point(0, 0, z=huge), 'z'),
        (lambda: gm.Point(0, 0, z=1, epoch=huge), 'epoch'),
        (lambda: gm.Point(0, 0, m=huge), 'm'),
        (lambda: gm.Point(0, 0, m=1, epoch=huge), 'epoch'),
        (lambda: gm.Point(0, 0, z=1, m=huge), 'm'),
        (lambda: gm.Point(0, 0, z=1, m=2, epoch=huge), 'epoch'),
        (lambda: gm.Point(huge, 0, crs=4326), 'x must be a finite float'),
        (lambda: gm.Point(0, 0, epoch=huge, crs=4326), 'epoch'),
        (lambda: gm.box(huge, 0, 1, 1), 'minx'),
        (lambda: gm.box(0, 0, 1, 1, epoch=huge), 'epoch'),
        (lambda: gm.points([huge], [0]), 'x'),
        (lambda: gm.points([0], [huge]), 'y'),
        (lambda: gm.points([0], [0], z=[huge]), 'z'),
        (lambda: gm.points([0], [0], epoch=huge), 'epoch'),
        (lambda: gm.LineString([(huge, 0), (1, 1)]), 'coordinate'),
        (lambda: gm.LineString([(0, 0), (1, 1)], z=[0, huge]), 'z'),
        (lambda: gm.LineString([(0, 0), (1, 1)], epoch=huge), 'epoch'),
        (lambda: gm.Polygon([(huge, 0), (1, 0), (0, 1)]), 'coordinate'),
        (lambda: gm.Polygon([(0, 0), (1, 0), (0, 1)], epoch=huge), 'epoch'),
        (lambda: gm.MultiPoint([(0, 0)], epoch=huge), 'epoch'),
        (lambda: gm.MultiLineString([[(0, 0), (1, 1)]], epoch=huge), 'epoch'),
        (lambda: gm.MultiPolygon([[[(0, 0), (1, 0), (0, 1)]]], epoch=huge), 'epoch'),
        (lambda: gm.GeometryCollection([geom], epoch=huge), 'epoch'),
        (lambda: gm.GeometryArray([geom], epoch=huge), 'epoch'),
        (lambda: line.simplify(huge), 'tolerance'),
        (lambda: line.simplify(huge), 'tolerance'),
        (lambda: line.offset_curve(huge), 'distance'),
        (lambda: line.offset_curve(huge), 'distance'),
        (lambda: gm.Point(0, 0).buffer(huge), 'distance'),
        (lambda: gm.Point(0, 0).buffer(huge), 'distance'),
        (lambda: gm.GeometryArray([gm.Point(0, 0)]).buffer(huge), 'distance'),
        (lambda: gm.snap(line, gm.Point(0, 0), huge), 'tolerance'),
        (lambda: gm.snap(line, gm.Point(0, 0), huge), 'tolerance'),
        (lambda: line.remove_repeated_points(tolerance=huge), TypeError, 'tolerance'),
        (lambda: line.remove_repeated_points(tolerance=huge), TypeError, 'tolerance'),
        (lambda: line.segmentize(huge), 'max_length'),
        (lambda: line.segmentize(huge), 'max_length'),
        (lambda: line.clip_by_rect(huge, 0, 1, 1), 'minx'),
        (lambda: line.clip_by_rect(huge, 0, 1, 1), 'minx'),
        (lambda: line.line_interpolate(huge), 'at'),
        (lambda: line.line_interpolate(huge), 'at'),
        (lambda: line.line_substring(huge, 1), 'start'),
        (lambda: line.line_substring(0, huge), 'end'),
        (lambda: polygon.concave_hull(concavity=huge), TypeError, 'concavity'),
        (lambda: polygon.concave_hull(length_threshold=huge), 'length_threshold'),
        (lambda: polygon.polylabel(tolerance=huge), TypeError, 'tolerance'),
        (lambda: polygon.polylabel(tolerance=huge), 'tolerance'),
        (lambda: sites.voronoi_polygons(tolerance=huge), OverflowError, None),
        (lambda: sites.voronoi_edges(tolerance=huge), OverflowError, None),
        (lambda: gm.contains_xy(geom, huge, 0), 'x'),
        (lambda: gm.intersects_xy(geom, huge, 0), 'x'),
        (lambda: gm.contains_xy(geom.prepare(), huge, 0), 'x'),
        (lambda: gm.contains_xy(geom, huge, 0), 'x'),
        (lambda: gm.intersects_xy(geom, huge, 0), 'x'),
        (lambda: gm.dwithin(geom, other, huge), 'distance'),
        (lambda: gm.dwithin(array, other, huge), 'distance'),
        (lambda: gm.dwithin(geom, other, huge), 'distance'),
        (lambda: idx.query(geom, predicate='dwithin', distance=huge), 'distance'),
        (
            lambda: gm.join(array, array, predicate='dwithin', distance=huge),
            'distance',
        ),
        (lambda: geom.destination(huge, 1), 'bearing'),
        (lambda: geom.destination(0, huge), 'distance'),
        (lambda: gm.point_between(geom, other, huge), 'distance'),
        (lambda: geom.buffer(huge), 'distance'),
        (lambda: gm.H3Cell(huge, 0, resolution=5), 'longitude'),
        (lambda: gm.H3Cell(0, huge, resolution=5), 'latitude'),
        (lambda: gm.S2Cell(huge, 0, level=5), 'longitude'),
        (lambda: gm.S2Cell(0, huge, level=5), 'latitude'),
    ]
    for call, error_type, message in _normalize_boundary_cases(cases, TypeError):
        kwargs = {'match': message} if message is not None else {}
        with pytest.raises(error_type, **kwargs):
            call()


def test_public_signatures_keep_pythonic_defaults_after_native_validation() -> None:
    _ = gm.box(-1, -1, 1, 1, crs=4326)
    idx = gm.SpatialIndex(gm.points([0], [0]))
    geom = gm.Point(0, 0)
    arr = gm.GeometryArray([geom])
    assert 'buffer' not in gm.__all__
    assert not hasattr(gm, 'buffer')
    assert callable(geom.buffer)
    assert callable(arr.buffer)
    signatures = {
        'Geometry.buffer': str(inspect.signature(geom.buffer)),
        'GeometryArray.buffer': str(inspect.signature(arr.buffer)),
        'SpatialIndex.nearest': str(inspect.signature(idx.nearest)),
        'gm.nearest': str(inspect.signature(gm.nearest)),
        'gm.bearing': str(inspect.signature(gm.bearing)),
        'Geometry.destination': str(inspect.signature(geom.destination)),
        'gm.point_between': str(inspect.signature(gm.point_between)),
        'gm.h3_cover': str(inspect.signature(gm.h3_cover)),
        'gm.s2_cover': str(inspect.signature(gm.s2_cover)),
        'GeometryArray': str(inspect.signature(gm.GeometryArray)),
        'Point': str(inspect.signature(gm.Point)),
        'LineString': str(inspect.signature(gm.LineString)),
        'Polygon': str(inspect.signature(gm.Polygon)),
        'MultiPoint': str(inspect.signature(gm.MultiPoint)),
        'MultiLineString': str(inspect.signature(gm.MultiLineString)),
        'MultiPolygon': str(inspect.signature(gm.MultiPolygon)),
        'GeometryCollection': str(inspect.signature(gm.GeometryCollection)),
        'box': str(inspect.signature(gm.box)),
        'points': str(inspect.signature(gm.points)),
        'gm.crs_transform_bounds': str(inspect.signature(gm.crs_transform_bounds)),
        'gm.crs_roundtrip': str(inspect.signature(gm.crs_roundtrip)),
    }
    assert 'quadrant_segments=8' in signatures['Geometry.buffer']
    assert 'quadrant_segments=8' in signatures['GeometryArray.buffer']
    assert 'k=1' in signatures['SpatialIndex.nearest']
    assert 'k=1' in signatures['gm.nearest']
    assert signatures['gm.bearing'] == "(left, right, *, path='geodesic')"
    assert (
        signatures['Geometry.destination']
        == "(bearing, distance, *, path='geodesic', unit=None)"
    )
    assert (
        signatures['gm.point_between']
        == "(left, right, distance, *, normalized=False, path='geodesic', unit=None)"
    )
    assert "cell_rule='overlap'" in signatures['gm.h3_cover']
    assert 'max_cells=1000000' in signatures['gm.s2_cover']
    assert signatures['GeometryArray'] == '(values, *, crs=None, epoch=None)'
    assert (
        signatures['Point']
        == '(x=None, y=None, *, z=None, m=None, crs=None, epoch=None)'
    )
    assert (
        signatures['LineString']
        == '(coordinates=None, *, x=None, y=None, z=None, m=None, crs=None, epoch=None)'
    )
    assert (
        signatures['MultiPoint']
        == '(coordinates=None, *, x=None, y=None, z=None, m=None, crs=None, epoch=None)'
    )
    assert signatures['MultiLineString'] == '(lines=None, *, crs=None, epoch=None)'
    assert signatures['MultiPolygon'] == '(polygons=None, *, crs=None, epoch=None)'
    assert (
        signatures['GeometryCollection'] == '(geometries=None, *, crs=None, epoch=None)'
    )
    assert (
        signatures['box']
        == '(minx, miny, maxx, maxy, *, crs=None, wrap=None, ccw=True, epoch=None)'
    )
    assert signatures['points'] == '(x, y, *, z=None, m=None, crs=None, epoch=None)'
    assert (
        signatures['Polygon']
        == '(shell=None, holes=None, *, x=None, y=None, z=None, m=None, crs=None, epoch=None)'
    )
    assert 'densify=21' in signatures['gm.crs_transform_bounds']
    assert 'iterations=1' in signatures['gm.crs_roundtrip']


def test_version_matches_installed_metadata() -> None:
    assert gm.__version__ == metadata.version('gometry')


def test_changelog_not_included_claims_do_not_name_live_public_symbols() -> None:
    public = set(gm.__all__)
    changelog = Path('docs/about/changelog.md').read_text(encoding='utf-8')
    stale: list[str] = []
    for sentence in re.split(r'(?<=[.!?])\s+', changelog):
        if 'not included' not in sentence:
            continue
        stale.extend(
            name
            for name in re.findall(r'`([A-Za-z_][A-Za-z0-9_]*)`', sentence)
            if name in public
        )
    assert stale == []


def test_readme_quickstart_stays_executable() -> None:
    point = gm.Point(21.0, 52.0, crs=4326)
    area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
    wrapped_area = gm.box(170.0, -10.0, -170.0, 10.0, crs=4326, wrap='split')
    multi = gm.MultiPolygon([area], crs=4326)
    assert point.geometry_type == 'Point'
    assert area.geometry_type == 'Polygon'
    assert wrapped_area.geometry_type == 'MultiPolygon'
    assert multi.geometry_type == 'MultiPolygon'
    assert point.crs == 'EPSG:4326'
    assert point.coordinate_axes == 'XY'
    assert gm.contains(area, point)
    assert area.area > 0
    assert multi.bounds == area.bounds
    np.testing.assert_array_equal(area.coords.row_index, [0, 0, 0, 0, 0])
    np.testing.assert_array_equal(
        gm.intersects_xy(area, [21.0, 30.0], [52.0, 52.0]), [True, False]
    )
    distance_m = gm.distance(point, gm.Point(22.0, 52.0, crs=4326))
    bearing_deg = gm.bearing(point, gm.Point(22.0, 52.0, crs=4326))
    east_1km = point.destination(90.0, 1000.0)
    midpoint = gm.point_between(
        point, gm.Point(22.0, 52.0, crs=4326), 0.5, normalized=True
    )
    area_m2 = area.area
    perimeter_m = area.length
    assert distance_m > 0
    assert 80 < bearing_deg < 100
    assert east_1km.crs == 'EPSG:4326'
    assert midpoint.crs == 'EPSG:4326'
    assert area_m2 > 0
    assert perimeter_m > 0
    cells = gm.h3_cover(area, resolution=6, cell_rule='center')
    assert len(cells) > 0
    s2_cell = gm.S2Cell(point, level=12)
    s2_cells = gm.s2_cover(area, level=12)
    assert s2_cell.level == 12
    assert len(s2_cells) > 0
    idx = gm.SpatialIndex(gm.points([21.0, 30.0], [52.0, 52.0], crs=4326))
    np.testing.assert_allclose(idx.candidates(area), [0])
    np.testing.assert_allclose(idx.query(area, predicate='contains'), [0])
    np.testing.assert_allclose(idx.query(area, predicate='covers'), [0])
    np.testing.assert_allclose(
        idx.query(point, predicate='dwithin', distance=1.0, unit='planar'), [0]
    )
    assert (
        idx.explain(point, predicate='dwithin', distance=1.0, unit='planar')[-1]
        == 'exact planar distance refine within 1'
    )
    projected = area.to_crs(area.estimate_local_crs())
    assert projected.crs and str(projected.crs).startswith('EPSG:')
    wkb = gm.Point(21.012345, 52.987654, crs=4326).to_wkb(
        include_srid=True, precision=5
    )
    same = gm.from_wkb(wkb)
    same_ewkb = gm.from_wkb(point.to_wkb(include_srid=True))
    assert same.crs == 'EPSG:4326'
    assert same_ewkb.crs == point.crs
    roundtrip = cast(
        'gm.GeometryArray', gm.from_arrow(gm.GeometryArray([point, area]).to_arrow())
    )
    feature_geom = gm.from_geojson(
        {
            'type': 'Feature',
            'properties': {},
            'geometry': point.__geo_interface__,
        },
        crs=4326,
    )
    strict_geom = gm.require(feature_geom, crs=4326, axes='XY')
    assert len(roundtrip) == 2
    assert strict_geom.crs == 'EPSG:4326'


def test_public_all_exports_every_curated_facade_symbol() -> None:
    flat_gone = {
        'crs_best_local',
        'geodesic_area',
        'geodesic_distance',
        'h3_cell',
        'h3_cell_value',
        's2_cell_value',
        'wkb_point',
        'CrsInfo',
        'CrsAreaBounds',
        'Planar',
        'Geodesic',
        'geodesic',
        'crs_clear_grid_cache',
        'crs_download_grid',
    }
    expected = [
        'AccuracyWarning',
        'CRS',
        'CRSError',
        'CRSMismatchError',
        'CellArray',
        'Coordinates',
        'Extremes',
        'Features',
        'GeohashCell',
        'Geometry',
        'GeometryArray',
        'GeometryCollection',
        'GeometryError',
        'GeometryParts',
        'GeometryTypeError',
        'Groups',
        'H3Cell',
        'H3Edge',
        'H3EdgeArray',
        'H3Vertex',
        'H3VertexArray',
        'InvalidGeometryError',
        'LineString',
        'MultiLineString',
        'MultiPoint',
        'MultiPolygon',
        'ParseError',
        'Point',
        'Polygon',
        'PolygonizeResult',
        'PreparedGeometry',
        'S2Cell',
        'SpatialIndex',
        'Tile',
        'TransformError',
        'ValidationReport',
        'area',
        'bearing',
        'bounds',
        'box',
        'boxes',
        'contains',
        'contains_properly',
        'contains_xy',
        'coverage_clean',
        'coverage_invalid_edges',
        'coverage_is_valid',
        'coverage_simplify',
        'coverage_union',
        'covered_by',
        'covers',
        'cross_track_distance',
        'crosses',
        'difference',
        'disjoint',
        'distance',
        'distance_3d',
        'dwithin',
        'equals',
        'equals_exact',
        'equals_identical',
        'Cell',
        'frechet_distance',
        'from_arrow',
        'from_features',
        'from_geojson',
        'from_polyline',
        'from_wkb',
        'from_wkt',
        'get_coordinates',
        'hausdorff_distance',
        'intersection',
        'intersection_all',
        'intersects',
        'intersects_xy',
        'join',
        'length',
        'length_3d',
        'line_strings',
        'multi_line_strings',
        'multi_points',
        'multi_polygons',
        'nearest',
        'nearest_points',
        'overlaps',
        'parts',
        'point_between',
        'points',
        'polygonize',
        'polygonize_full',
        'polygons',
        'relate',
        'relate_pattern',
        'require',
        'rhumb_distance',
        'rings',
        'shared_paths',
        'shortest_line',
        'snap',
        'split',
        'symmetric_difference',
        'symmetric_difference_all',
        'to_feature',
        'to_feature_collection',
        'touches',
        'union',
        'union_all',
        'within',
    ]
    expected.extend(FLAT_FAMILY_EXPORTS)
    assert gm.__all__ == sorted(expected)
    assert flat_gone.isdisjoint(set(gm.__all__))
    assert all(hasattr(gm, name) for name in gm.__all__)
    assert not hasattr(gm, 'ExtremePoints')
    optional_exports = {
        'explore',
        'from_geopandas',
        'from_geoparquet',
        'from_pandas',
        'from_polars',
    }
    assert optional_exports.isdisjoint(gm.__all__)
    # The handwritten stub and API reference advertise optional exports.
    # Runtime dir() stays core-only because pydoc resolves every returned name
    # and would otherwise import optional frameworks as a side effect.
    assert optional_exports.isdisjoint(dir(gm))
    assert 'TYPE_CHECKING' not in dir(gm)
    assert all(
        callable(getattr(gm, name)) or isinstance(getattr(gm, name), type)
        for name in optional_exports
    )
    assert not hasattr(gm, 'GeometryDtype')
    assert callable(gm.crs_transform)
    assert callable(gm.crs_search)
    assert callable(gm.H3Cell)
    assert callable(gm.S2Cell)
    assert callable(gm.h3_cells)
    assert callable(gm.h3_cover)
    assert callable(gm.s2_cells)
    assert callable(gm.s2_cover)
    assert not hasattr(gm, 'geodesic')
    assert not hasattr(gm, 'Planar')
    assert not hasattr(gm, 'Geodesic')
    assert {'Any', 'TYPE_CHECKING', 'cast', 'overload'}.isdisjoint(dir(gm))


def test_cell_protocol_conformance() -> None:
    """Every cell class satisfies ``gometry.Cell`` structurally.

    The annotated assignments below are the pyright gate: a cell class that
    drifts from the uniform surface (a renamed member, a depth parameter
    that stops accepting ``None``) fails type checking here. The runtime
    asserts keep the same contract enforced when only pytest runs.
    """
    cells = [
        gm.H3Cell(13.4, 52.5, resolution=7),
        gm.S2Cell(13.4, 52.5, level=12),
        gm.GeohashCell(13.4, 52.5, precision=6),
        gm.Tile(lon=13.4, lat=52.5, zoom=12),
    ]
    assert gm.Cell.__module__ == 'gometry'
    for cell in cells:
        assert not hasattr(cell, 'geometry_type')
        assert not hasattr(gm.CellArray([cell], type=type(cell)), 'geometry_type')
        assert isinstance(cell.token, str)
        assert cell.center.geometry_type == 'Point'
        assert cell.polygon.geometry_type == 'Polygon'
        assert cell.area > 0.0
        assert cell.parent().children()
        assert cell.contains(cell.children()[0])
        assert cell.intersects(cell.children()[0].token)
        assert all(isinstance(neighbor.token, str) for neighbor in cell.neighbors)


def test_sequence_types_use_iteration_as_the_only_list_materialization() -> None:
    for cls in (gm.GeometryArray, gm.CellArray, gm.H3VertexArray, gm.H3EdgeArray):
        assert not hasattr(cls, 'to_list')
    assert not hasattr(gm.GeometryArray, 'to_pandas_frame')


def test_union_all_accepts_geometry_array_and_matches_array_method() -> None:
    arr = gm.points([0.0, 1.0], [0.0, 1.0])
    assert gm.union_all(arr) == arr.union_all()
    assert gm.union_all(arr).geometry_type == 'MultiPoint'
    with pytest.raises(TypeError, match='by'):
        arr.dissolve()


def test_nary_reductions_are_consistent_across_the_trio() -> None:
    """Free and array aggregate spellings accept the same column directly."""
    panes = gm.GeometryArray([
        gm.box(0, 0, 3, 3),
        gm.box(1, 1, 4, 4),
        gm.box(2, 2, 5, 5),
    ])
    core = panes.intersection_all()
    assert core == gm.intersection_all(panes)
    odd = panes.symmetric_difference_all()
    assert odd == gm.symmetric_difference_all(panes)
    # Aggregates skip missing rows.
    masked = gm.GeometryArray([gm.box(0, 0, 3, 3), None, gm.box(1, 1, 4, 4)])
    assert masked.intersection_all() == gm.intersection_all([
        gm.box(0, 0, 3, 3),
        gm.box(1, 1, 4, 4),
    ])


def test_geographic_array_nary_reductions_use_seam_topology() -> None:
    """R15 C5: array aggregates share the free n-ary topology dispatch."""
    left = gm.Polygon(
        [(170, 0), (-170, 0), (-170, 20), (170, 20), (170, 0)], crs='OGC:CRS84'
    )
    right = gm.Polygon(
        [(175, 10), (-175, 10), (-175, 30), (175, 30), (175, 10)], crs='OGC:CRS84'
    )
    values = gm.GeometryArray([left, right]).set_epoch(2020.5)

    for name in ('intersection_all', 'symmetric_difference_all'):
        from_array = getattr(values, name)()
        from_free = getattr(gm, name)(values)
        # Both source polygons cross the antimeridian. The free route's split
        # topology result is therefore a real independent expected path, not a
        # same-frame planar coincidence.
        assert gm.equals(from_array, from_free)
        assert from_array.area == pytest.approx(from_free.area)
        assert from_array.crs == from_free.crs == 'OGC:CRS84'
        assert from_array.epoch == from_free.epoch == 2020.5

        # Aggregate conventions must retain their existing missing and
        # one-row behaviour while using the same frame-aware dispatch.
        with_missing = gm.GeometryArray([left, None, right]).set_epoch(2020.5)
        assert gm.equals(getattr(with_missing, name)(), from_free)
        single = gm.GeometryArray([left]).set_epoch(2020.5)
        assert gm.equals(getattr(single, name)(), getattr(gm, name)(single))


def test_bulk_free_functions_accept_raw_iterables_uniformly() -> None:
    """Every bulk free function takes the same input vocabulary.

    Rule 4's justification for `gm.parts`/`gm.rings` existing beside `geom.parts`
    and `GeometryArray.parts()` is precisely "a free function exists only for raw
    iterables" — so refusing one was self-refuting. `gm.area`/`gm.bounds` always
    accepted them.
    """
    point = gm.Point(0, 0)
    line = gm.LineString([(0, 0), (1, 1)])
    donut = gm.Polygon(
        [(0, 0), (4, 0), (4, 4), (0, 4)], holes=[[(1, 1), (2, 1), (2, 2), (1, 2)]]
    )

    assert gm.area([point, line]).tolist() == [0.0, 0.0]
    assert len(gm.parts([point, line])) == 2
    assert len(gm.rings([donut])) == 2
    # generators and tuples, not just lists
    assert len(gm.parts(g for g in (point, line))) == 2
    assert len(gm.rings((donut, donut))) == 4
    # and the array/scalar forms keep working
    assert len(gm.parts(gm.GeometryArray([point, line]))) == 2
    assert len(gm.rings(donut)) == 2


def test_geometry_kind_rejections_are_catchable_as_geometry_error() -> None:
    """A wrong geometry KIND raises `GeometryTypeError`, not a bare `TypeError`.

    These escaped `except gm.GeometryError` entirely. `GeometryTypeError`
    dual-bases `(GeometryError, TypeError)`, so both spellings catch them and no
    existing handler breaks. A wrong *Python* type still raises plain
    `TypeError` — that distinction is the point.
    """
    point = gm.Point(0, 0)
    triangle = gm.MultiPoint([(0, 0), (1, 1), (0, 1)])
    for label, call in (
        ('rings on an array of points', lambda: gm.rings(gm.GeometryArray([point]))),
        ('rings on a raw iterable', lambda: gm.rings([point])),
        (
            'voronoi clip of the wrong kind',
            lambda: triangle.voronoi_polygons(clip=point),
        ),
        ('multi_polygons member kind', lambda: gm.multi_polygons([[point]])),
    ):
        with pytest.raises(gm.GeometryTypeError) as info:
            call()
        # dual base: anyone catching plain TypeError still catches it
        assert isinstance(info.value, TypeError), label
        assert isinstance(info.value, gm.GeometryError), label
