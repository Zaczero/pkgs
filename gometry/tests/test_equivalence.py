"""Cross-cutting equivalence engines — table-driven invariants guarding whole
bug classes rather than single bugs: the packed/mixed storage seam, the four
call-form spellings, frame (CRS/epoch) propagation, fixed Z/M behavior,
split, serialization round-trips, Python sequence semantics, and geodesic vs
planar mode switching. Each engine is one parametrized test over a declarative
case table; adding a row extends coverage with no new test code.
"""

import itertools
import math
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import gometry as gm
import numpy as np
import pytest
from conftest import (
    bools,
    canon,
    floats,
    line_storage_twins,
    polygon_storage_twins,
    polygon_z_storage_twins,
    storage_twins,
)


@dataclass(frozen=True)
class StorageCase:
    name: str
    axes: str
    fn: Callable[[gm.GeometryArray], object]


_PROBE = lambda: gm.Point(0.0, 10.0, crs=3857)
_BOX = lambda: gm.box(-1.0, 9.0, 2.0, 12.0, crs=3857)
STORAGE_CASES = [
    StorageCase('iter', 'XYZM', lambda a: [g.to_wkt() for g in a]),
    StorageCase('getitem', 'XYZM', lambda a: (a[1].to_wkt(), a[-1].to_wkt())),
    StorageCase(
        'slices', 'XYZM', lambda a: (canon(a[::2]), canon(a[::-1]), canon(a[1:]))
    ),
    StorageCase('to_list', 'XYZM', lambda a: canon(list(a))),
    StorageCase(
        'coords',
        'XYZM',
        lambda a: (a.coords.coordinate_axes, a.coords.row_index, list(a.coords)),
    ),
    StorageCase('bounds', 'XYZM', lambda a: (a.bounds, a.total_bounds, a.bounds_3d)),
    StorageCase('z_accessors', 'XYZM', lambda a: (a.min_z, a.max_z, a.z_range)),
    StorageCase('geometry_type', 'XYZM', lambda a: a.geometry_type),
    StorageCase('coordinate_axes', 'XYZM', lambda a: a.coordinate_axes),
    StorageCase(
        'wkt_wkb',
        'XYZM',
        lambda a: (a.to_wkt(), [gm.from_wkb(b).to_wkt() for b in a.to_wkb()]),
    ),
    StorageCase('geojson', 'XY', lambda a: a.set_crs(None).to_geojson()),
    StorageCase(
        'metrics',
        'XY',
        lambda a: (
            a.area,
            a.length,
            gm.distance(a, _PROBE()),
            gm.dwithin(a, _PROBE(), 2.0),
        ),
    ),
    StorageCase(
        'predicates',
        'XY',
        lambda a: (
            gm.equals(a, gm.Point(1.0, 11.0, crs=3857)),
            gm.intersects(a, _BOX()),
            gm.within(a, _BOX()),
            gm.relate(a, _PROBE()),
        ),
    ),
    StorageCase(
        'transforms',
        'XYZM',
        lambda a: canon(a.translate(1.0, 2.0).rotate(90.0, origin=(0.0, 0.0))),
    ),
    StorageCase(
        'unary_geometry',
        'XYZM',
        lambda a: (
            canon(a.reverse()),
            canon(a.normalize()),
            canon(a.set_z(None).set_m(None)),
        ),
    ),
    StorageCase(
        'selection', 'XYZM', lambda a: (canon(a[[2, 0]]), canon(a[[True, False, True]]))
    ),
    StorageCase(
        'frame_ops',
        'XYZM',
        lambda a: (
            a.set_crs(4326, overwrite=True).crs,
            a.set_crs(None).crs,
            gm.require(a, crs=3857).crs,
        ),
    ),
    StorageCase('validate', 'XYZM', lambda a: canon(a.validate())),
    StorageCase(
        'packed_lanes_identity',
        'XYZM',
        lambda a: (
            canon(a.convex_hull()),
            canon(a.simplify(0.5)),
            canon(a.remove_repeated_points()),
            canon(a.rotate(30.0)),
            canon(a.scale(2.0)),
            canon(a.skew(5.0)),
        ),
    ),
    StorageCase(
        'packed_lanes_columnar',
        'XY',
        lambda a: (
            canon(a.quantize(1)),
            canon(a.snap_to_grid(0.5)),
            canon(a.rotate(30.0, origin=(5.0, 5.0))),
            canon(a.scale(2.0, origin=(1.0, 1.0))),
            canon(a.affine_transform((2.0, 0.0, 0.0, 0.5, 1.0, -1.0))),
        ),
    ),
    StorageCase(
        'packed_lanes_computed',
        'XYZM',
        lambda a: (
            canon(a.centroid()),
            canon(a.envelope()),
            canon(a.point_on_surface()),
        ),
    ),
    StorageCase(
        'index',
        'XY',
        lambda a: (
            gm.SpatialIndex(a).query(_BOX()),
            gm.SpatialIndex(a).nearest(_PROBE(), k=2),
        ),
    ),
]


@pytest.mark.parametrize('case', STORAGE_CASES, ids=lambda case: case.name)
def test_packed_storage_matches_mixed_storage(case: StorageCase) -> None:
    packed, mixed = storage_twins(case.axes)
    assert canon(case.fn(packed)) == canon(case.fn(mixed))


@pytest.mark.parametrize(
    ('twins', 'arrow_kind', 'op'),
    [
        (line_storage_twins, 'geoarrow.linestring', lambda a: a.snap_to_grid(0.5)),
        (line_storage_twins, 'geoarrow.linestring', lambda a: a.set_z(5.0)),
        (line_storage_twins, 'geoarrow.linestring', lambda a: a.set_m(3.0)),
        (line_storage_twins, 'geoarrow.linestring', lambda a: a.line_merge()),
        (line_storage_twins, 'geoarrow.linestring', lambda a: a.rotate(90.0)),
        (line_storage_twins, 'geoarrow.linestring', lambda a: a.scale(2.0)),
        (
            line_storage_twins,
            'geoarrow.linestring',
            lambda a: a.scale(2.0, origin='center'),
        ),
        (polygon_storage_twins, 'geoarrow.polygon', lambda a: a.rotate(90.0)),
        (polygon_storage_twins, 'geoarrow.polygon', lambda a: a.scale(2.0)),
        (
            polygon_storage_twins,
            'geoarrow.polygon',
            lambda a: a.scale(2.0, origin='center'),
        ),
        (polygon_storage_twins, 'geoarrow.polygon', lambda a: a.snap_to_grid(0.5)),
        (polygon_storage_twins, 'geoarrow.polygon', lambda a: a.set_z(4.0)),
        (polygon_storage_twins, 'geoarrow.polygon', lambda a: a.set_m(9.0)),
        (polygon_z_storage_twins, 'geoarrow.polygon', lambda a: a.set_z(None)),
        (line_storage_twins, 'geoarrow.linestring', lambda a: a.segmentize(0.5)),
        (polygon_storage_twins, 'geoarrow.polygon', lambda a: a.segmentize(0.5)),
        (
            line_storage_twins,
            'geoarrow.linestring',
            lambda a: a.segmentize(fraction=0.5),
        ),
        (
            polygon_storage_twins,
            'geoarrow.polygon',
            lambda a: a.segmentize(fraction=0.5),
        ),
    ],
    ids=[
        'lines_snap',
        'lines_set_z',
        'lines_set_m',
        'lines_line_merge',
        'lines_rotate_centroid',
        'lines_scale_centroid',
        'lines_scale_center',
        'polygons_rotate_centroid',
        'polygons_scale_centroid',
        'polygons_scale_center',
        'polygons_snap',
        'polygons_set_z',
        'polygons_set_m',
        'polygons_clear_z',
        'lines_segmentize',
        'polygons_segmentize',
        'lines_densify',
        'polygons_densify',
    ],
)
def test_map_packed_storage_preserves_layout(
    twins: Callable[[], tuple[gm.GeometryArray, gm.GeometryArray]],
    arrow_kind: str,
    op: Callable[[gm.GeometryArray], gm.GeometryArray],
) -> None:
    """Columnar map_shapes lanes keep packed Lines/Polygons storage."""
    packed, mixed = twins()
    assert packed.to_arrow().type.extension_name == arrow_kind
    packed_out = op(packed)
    mixed_out = op(mixed)
    assert canon(packed_out) == canon(mixed_out)
    assert packed_out.to_arrow().type.extension_name == arrow_kind


@dataclass(frozen=True)
class SpellingCase:
    name: str
    make: Callable[[], gm.Geometry]
    args: tuple[Any, ...] | Any = ()
    kwargs: dict[str, Any] | None = None
    attr: str = ''

    @property
    def member(self) -> str:
        return self.attr or self.name


_Z_LINE = lambda: gm.LineString([(0.0, 0.0, 1.0), (3.0, 4.0, 5.0)], crs=3857)
_POLY = lambda: gm.box(0.0, 0.0, 2.0, 3.0, crs=3857)
SPELLING_CASES = [
    SpellingCase('to_wkt', _Z_LINE),
    SpellingCase('to_wkb', _Z_LINE),
    SpellingCase(
        'to_geojson',
        lambda: gm.LineString([(0.0, 0.0, 1.0), (3.0, 4.0, 5.0)], crs=4326),
    ),
    SpellingCase('validate', _POLY),
    SpellingCase('set_z', _Z_LINE, (None,)),
    SpellingCase('quantize', _Z_LINE, (1)),
    SpellingCase('length_3d', _Z_LINE),
    SpellingCase('area', _POLY),
    SpellingCase('length', _POLY),
    SpellingCase('bounds', _POLY),
    SpellingCase('is_valid', _POLY),
    SpellingCase('minimum_clearance', _POLY),
    SpellingCase('boundary', _POLY),
    SpellingCase('reverse', _Z_LINE),
    SpellingCase('normalize', _POLY),
    SpellingCase('envelope', _POLY),
    SpellingCase('centroid', _POLY),
    SpellingCase('convex_hull', _POLY),
    SpellingCase('buffer', _POLY, (1.0)),
    SpellingCase('simplify', _POLY, (0.5)),
    SpellingCase('translate', _Z_LINE, (2.0, 3.0)),
    SpellingCase('rotate', _POLY, (90.0), {'origin': (0.0, 0.0)}),
    SpellingCase('scale', _POLY, (2.0, 3.0), {'origin': (0.0, 0.0)}),
    SpellingCase('snap', _POLY, (gm.Point(0.1, 0.1, crs=3857), 0.5)),
    SpellingCase('affine_transform', _Z_LINE, ((1.0, 0.0, 0.0, 1.0, 5.0, 6.0),)),
    SpellingCase('intersection', _POLY, (gm.box(1.0, 1.0, 3.0, 4.0, crs=3857))),
    SpellingCase('union', _POLY, (gm.box(1.0, 1.0, 3.0, 4.0, crs=3857))),
    SpellingCase('distance', _POLY, (gm.Point(5.0, 0.0, crs=3857))),
    SpellingCase('intersects', _POLY, (gm.Point(1.0, 1.0, crs=3857))),
    SpellingCase(
        'area-geodesic', lambda: gm.box(20.0, 51.0, 21.0, 52.0, crs=4326), attr='area'
    ),
    SpellingCase(
        'length-geodesic',
        lambda: gm.box(20.0, 51.0, 21.0, 52.0, crs=4326),
        attr='length',
    ),
    SpellingCase(
        'distance-geodesic',
        lambda: gm.Point(21.0, 52.0, crs=4326),
        (gm.Point(21.1, 52.1, crs=4326)),
        attr='distance',
    ),
]


def _spell_raw(owner: object, case: SpellingCase) -> object:
    member = getattr(owner, case.member)
    if callable(member):
        args = case.args if isinstance(case.args, tuple) else (case.args,)
        return member(*args, **case.kwargs or {})
    return member


def _spell(owner: object, case: SpellingCase) -> object:
    return canon(_spell_raw(owner, case))


_PROPERTY_MEMBERS = frozenset({'area', 'bounds', 'is_valid', 'length', 'length_3d'})
_FREE_FN_MEMBERS = frozenset()
_BINARY_MEMBERS = frozenset({'intersection', 'union', 'distance', 'intersects', 'snap'})


@pytest.mark.parametrize('case', SPELLING_CASES, ids=lambda case: case.name)
def test_array_and_free_spellings_agree(case: SpellingCase) -> None:
    """Scalar method/property and array method/property lanes return the same values."""
    kwargs = case.kwargs or {}
    args = case.args if isinstance(case.args, tuple) else (case.args,)
    geom = case.make()
    array = gm.GeometryArray([case.make()])
    if case.member in _PROPERTY_MEMBERS:
        scalar_raw = getattr(geom, case.member)
        via_array_raw = getattr(array, case.member)
    elif case.member in _FREE_FN_MEMBERS or case.member in _BINARY_MEMBERS:
        scalar_raw = getattr(gm, case.member)(geom, *args, **kwargs)
        via_array_raw = getattr(gm, case.member)(array, *args, **kwargs)
    else:
        scalar_raw = getattr(geom, case.member)(*args, **kwargs)
        via_array_raw = getattr(array, case.member)(*args, **kwargs)
    float_lanes = {
        'area',
        'area-geodesic',
        'bounds',
        'distance',
        'distance-geodesic',
        'length',
        'length-geodesic',
        'length_3d',
        'minimum_clearance',
    }
    bool_lanes = {'intersects', 'is_valid'}
    if case.name in float_lanes:
        assert floats(via_array_raw)[0] == pytest.approx(scalar_raw)
    elif case.name in bool_lanes:
        assert bools(via_array_raw)[0] == scalar_raw
    else:
        assert canon(via_array_raw)[0] == canon(scalar_raw)


_FRAME_POLY = lambda: gm.box(0.0, 0.0, 2.0, 3.0, crs=3857, epoch=2020.0)
_FRAME_SITES = lambda: gm.MultiPoint(
    [(0.0, 0.0), (4.0, 0.0), (0.0, 4.0), (4.0, 4.0)], crs=3857, epoch=2020.0
)
FRAME_CASES: list[tuple[str, Callable[[], object]]] = [
    ('quantize', lambda: _FRAME_POLY().quantize(2)),
    ('translate', lambda: _FRAME_POLY().translate(1.0, 2.0)),
    ('reverse', lambda: _FRAME_POLY().reverse()),
    ('normalize', lambda: _FRAME_POLY().normalize()),
    ('set_z', lambda: _FRAME_POLY().set_z(None)),
    ('buffer', lambda: _FRAME_POLY().buffer(1.0)),
    ('centroid', lambda: _FRAME_POLY().centroid()),
    ('boundary', lambda: _FRAME_POLY().boundary()),
    ('envelope', lambda: _FRAME_POLY().envelope()),
    ('simplify', lambda: _FRAME_POLY().simplify(0.5)),
    (
        'snap',
        lambda: gm.snap(_FRAME_POLY(), gm.Point(0.1, 0.1, crs=3857, epoch=2020.0), 0.5),
    ),
    (
        'intersection',
        lambda: gm.intersection(
            _FRAME_POLY(), gm.box(1.0, 1.0, 3.0, 4.0, crs=3857, epoch=2020.0)
        ),
    ),
    (
        'union',
        lambda: gm.union(
            _FRAME_POLY(), gm.box(1.0, 1.0, 3.0, 4.0, crs=3857, epoch=2020.0)
        ),
    ),
    ('parts', lambda: gm.parts(_FRAME_SITES())),
    ('triangulate', lambda: _FRAME_SITES().triangulate(method='delaunay')),
    ('voronoi_polygons', lambda: _FRAME_SITES().voronoi_polygons()),
    (
        'triangulate_array',
        lambda: (
            gm.GeometryArray([_FRAME_SITES()]).triangulate(method='delaunay').values
        ),
    ),
    ('map_array', lambda: gm.GeometryArray([_FRAME_POLY()]).centroid()),
    ('array_overlay', lambda: gm.GeometryArray([_FRAME_POLY()]).buffer(1.0)),
]


def each_geometry(value: object) -> list[gm.Geometry]:
    if isinstance(value, gm.Geometry):
        return [value]
    assert isinstance(value, (list, tuple, gm.GeometryArray))
    out: list[gm.Geometry] = []
    for item in value:
        assert isinstance(item, gm.Geometry)
        out.append(item)
    return out


@pytest.mark.parametrize(
    ('name', 'fn'), FRAME_CASES, ids=[name for name, _ in FRAME_CASES]
)
def test_geometry_returning_ops_carry_the_input_frame(
    name: str, fn: Callable[[], object]
) -> None:
    result = fn()
    if isinstance(result, gm.GeometryArray):
        assert result.crs == 'EPSG:3857'
        assert result.epoch == 2020.0
    geoms = each_geometry(result)
    assert geoms, f'{name} returned no geometries to check'
    for geom in geoms:
        assert geom.crs == 'EPSG:3857'
        assert geom.epoch == 2020.0


_ZM_LINE = lambda: gm.from_wkt('LINESTRING ZM (0 0 1 10, 4 0 2 20, 4 4 3 30)')
_ZM_POLY = lambda: gm.from_wkt('POLYGON ZM ((0 0 1 10, 4 0 2 20, 4 4 3 30, 0 0 1 10))')
PRESERVE_CASES: list[tuple[str, Callable[[], gm.Geometry]]] = [
    ('translate', lambda: _ZM_LINE().translate(1.0, 2.0)),
    ('rotate', lambda: _ZM_LINE().rotate(90.0, origin=(0.0, 0.0))),
    ('quantize', lambda: _ZM_LINE().quantize(3)),
    ('reverse', lambda: _ZM_LINE().reverse()),
    ('normalize', lambda: _ZM_POLY().normalize()),
    ('segmentize', lambda: _ZM_LINE().segmentize(1.0)),
    ('snap', lambda: gm.snap(_ZM_LINE(), gm.Point(0.1, 0.1), 0.5)),
    ('remove_repeated_points', lambda: _ZM_LINE().remove_repeated_points()),
    ('simplify', lambda: _ZM_LINE().simplify(0.5)),
    ('convex_hull', lambda: _ZM_POLY().convex_hull()),
]
DERIVED_2D_CASES: list[tuple[str, Callable[[], gm.Geometry]]] = [
    ('buffer', lambda: _ZM_POLY().buffer(1.0)),
    ('centroid', lambda: _ZM_POLY().centroid()),
    ('point_on_surface', lambda: _ZM_POLY().point_on_surface()),
    ('envelope', lambda: _ZM_POLY().envelope()),
    ('minimum_rotated_rectangle', lambda: _ZM_POLY().minimum_rotated_rectangle()),
]


@pytest.mark.parametrize(
    ('name', 'fn'), PRESERVE_CASES, ids=[name for name, _ in PRESERVE_CASES]
)
def test_coordinate_preserving_ops_carry_zm(
    name: str, fn: Callable[[], gm.Geometry]
) -> None:
    assert fn().coordinate_axes == 'XYZM'


@pytest.mark.parametrize(
    ('name', 'fn'), DERIVED_2D_CASES, ids=[name for name, _ in DERIVED_2D_CASES]
)
def test_derived_ops_have_fixed_2d_results(
    name: str, fn: Callable[[], gm.Geometry]
) -> None:
    assert fn().coordinate_axes == 'XY', name


def test_overlay_restores_zm_by_default_and_force_2d_flattens() -> None:
    other = gm.from_wkt('POLYGON ZM ((0 0 1 10, 4 0 2 20, 0 4 3 30, 0 0 1 10))')
    assert gm.intersection(_ZM_POLY(), other).coordinate_axes == 'XYZM'
    assert gm.intersection(_ZM_POLY(), other).force_2d().coordinate_axes == 'XY'

    def staircase(x0: float, y0: float, steps: int) -> gm.Geometry:
        pts = [(x0, y0)]
        x, y = (x0, y0)
        for _ in range(steps):
            x += 1.0
            pts.append((x, y))
            y += 1.0
            pts.append((x, y))
        pts += [(x0, y), (x0, y0)]
        wkt = ', '.join((f'{px} {py} {i % 9}' for i, (px, py) in enumerate(pts)))
        return gm.from_wkt(f'POLYGON Z (({wkt}))')

    union = gm.union(staircase(0.0, 0.0, 8), staircase(0.6, -0.3, 8))
    assert union.coordinate_axes == 'XYZ'
    zs = [z for _, _, z in union.coords]
    assert all(z is not None and 0 <= z <= 8 for z in zs)


_KIND_WKTS = {
    'Point': 'POINT ({})',
    'MultiPoint': 'MULTIPOINT (({}), ({}))',
    'LineString': 'LINESTRING ({}, {})',
    'MultiLineString': 'MULTILINESTRING (({}, {}))',
    'Polygon': 'POLYGON (({}, {}, {}, {}))',
}
_AXES_COORDS = {
    'XY': ['0 0', '4 0', '4 4', '0 0'],
    'XYZ': ['0 0 1', '4 0 2', '4 4 3', '0 0 1'],
    'XYM': ['0 0 10', '4 0 20', '4 4 30', '0 0 10'],
    'XYZM': ['0 0 1 10', '4 0 2 20', '4 4 3 30', '0 0 1 10'],
}
_AXES_TAG = {'XY': '', 'XYZ': 'Z ', 'XYM': 'M ', 'XYZM': 'ZM '}


def _matrix_wkts() -> list[str]:
    out = []
    for kind, template in _KIND_WKTS.items():
        for axes, coords in _AXES_COORDS.items():
            body = template.format(*coords)
            head, _, tail = body.partition(' ')
            out.append(f'{head} {_AXES_TAG[axes]}{tail}')
        out.append(f'{kind.upper()} EMPTY')
    out.append('POLYGON ZM ((0 0 1 10, 4 0 2 20, 4 4 3 30, 0 0 1 10))')
    out.append('GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))')
    out.append('GEOMETRYCOLLECTION EMPTY')
    return out


@pytest.mark.parametrize('wkt', _matrix_wkts())
def test_wkt_wkb_round_trip_identity(wkt: str) -> None:
    geom = gm.from_wkt(wkt, crs=4326)
    assert isinstance(geom, gm.Geometry)
    assert geom.to_wkt() == wkt
    assert gm.from_wkt(geom.to_wkt()).to_wkt() == wkt
    assert gm.from_wkb(geom.to_wkb()).to_wkt() == wkt
    ewkb = gm.from_wkb(geom.to_wkb(include_srid=True))
    assert ewkb.to_wkt() == wkt
    assert ewkb.crs == 'EPSG:4326'
    if set(geom.coordinate_axes) <= {'X', 'Y', 'Z'}:
        assert gm.from_geojson(geom.to_geojson()).to_wkt() == wkt


_SLICES = [
    slice(None),
    slice(None, None, 2),
    slice(None, None, -1),
    slice(4, 1, -1),
    slice(-4, -1, 2),
    slice(10, -10, -2),
    slice(99, 100),
    slice(None, None, 99),
    slice(1, None),
    slice(None, -1),
]
SEQUENCE_CASES: list[tuple[str, Callable[[], Any]]] = [
    ('coords-line', lambda: gm.LineString([(i, i + 10.0) for i in range(5)]).coords),
    (
        'coords-collection',
        lambda: (
            gm.GeometryCollection([
                gm.Point(9.0, 9.0),
                gm.LineString([(0.0, 0.0), (1.0, 1.0)]),
            ]).coords
        ),
    ),
    (
        'coords-packed-array',
        lambda: gm.points([0.0, 1.0, 2.0], [10.0, 11.0, 12.0]).coords,
    ),
    ('column-x', lambda: gm.LineString([(i, i + 10.0) for i in range(5)]).coords.x),
    ('array-packed', lambda: gm.points([0.0, 1.0, 2.0], [10.0, 11.0, 12.0])),
    (
        'array-mixed',
        lambda: gm.from_wkt(['POINT (0 10)', 'LINESTRING (0 0, 1 1)', 'POINT (2 12)']),
    ),
]


@pytest.mark.parametrize(
    ('name', 'make'), SEQUENCE_CASES, ids=[name for name, _ in SEQUENCE_CASES]
)
def test_sequence_semantics_match_python_lists(
    name: str, make: Callable[[], Any]
) -> None:
    seq = make()
    reference = list(seq)
    assert len(seq) == len(reference)
    for index in (0, 1, -1, len(reference) - 1):
        assert canon(seq[index]) == canon(reference[index])
    for index in (len(reference), -len(reference) - 1):
        with pytest.raises(IndexError):
            seq[index]
    for slc in _SLICES:
        assert canon(list(seq[slc])) == canon(reference[slc])
    with pytest.raises(ValueError, match='step'):
        seq[::0]


def test_geodesic_mode_consistency_invariants() -> None:
    a_plain, b_plain = (gm.Point(0.0, 0.0), gm.Point(1.0, 0.0))
    a_geo, b_geo = (gm.Point(0.0, 0.0, crs=4326), gm.Point(1.0, 0.0, crs=4326))
    assert gm.distance(a_plain, b_plain) == 1.0
    one_degree = gm.distance(a_geo, b_geo)
    assert one_degree == pytest.approx(111319.49, rel=0.0001)
    assert gm.distance(a_geo, b_geo) == gm.distance(b_geo, a_geo)
    assert gm.distance(a_plain, b_plain) == gm.distance(b_plain, a_plain)
    line = gm.LineString([(0.0, 0.0), (1.0, 0.0)], crs=4326)
    assert line.length == pytest.approx(one_degree, rel=1e-09)
    assert gm.dwithin(a_geo, b_geo, one_degree * 1.001)
    assert not gm.dwithin(a_geo, b_geo, one_degree * 0.999)
    assert gm.dwithin(a_plain, b_plain, 1.0) and (
        not gm.dwithin(a_plain, b_plain, 0.999)
    )
    midpoint = line.line_interpolate(0.5, normalized=True)
    start, end = (gm.Point(0.0, 0.0, crs=4326), gm.Point(1.0, 0.0, crs=4326))
    assert gm.distance(midpoint, start) == pytest.approx(
        gm.distance(midpoint, end), rel=1e-09
    )
    c_geo = gm.Point(0.5, 0.5, crs=4326)
    assert (
        gm.distance(a_geo, b_geo)
        <= gm.distance(a_geo, c_geo) + gm.distance(c_geo, b_geo) + 1e-06
    )


def test_huge_coordinates_yield_finite_results_or_clean_errors() -> None:
    import math

    huge = gm.Point(1e308, 1e308)
    origin = gm.Point(0.0, 0.0)
    assert math.isfinite(gm.distance(huge, huge))
    assert gm.distance(huge, origin) > 0
    left, right = gm.nearest_points(origin, huge)
    assert left.to_wkt() == 'POINT (0 0)'
    assert right.coords.to_nested() == [1e308, 1e308]
    assert huge.quantize(0).coords.to_nested() == [1e308, 1e308]


def test_subnormal_separation_keeps_nonzero_distance() -> None:
    tiny = 5e-324
    a = gm.Point(0.0, 0.0)
    b = gm.Point(tiny, 0.0)
    assert gm.distance(a, b) == tiny
    assert not gm.equals(a, b)


def test_antimeridian_and_pole_geodesics_are_symmetric() -> None:
    west = gm.Point(179.5, 10.0, crs=4326)
    east = gm.Point(-179.5, 10.0, crs=4326)
    crossing = gm.distance(west, east)
    assert crossing == gm.distance(east, west)
    assert crossing < gm.distance(gm.Point(170.0, 10.0, crs=4326), west)
    pole = gm.Point(0.0, 90.0, crs=4326)
    for lon in (0.0, 90.0, 180.0):
        spoke = gm.Point(lon, 89.0, crs=4326)
        assert gm.distance(pole, spoke) == pytest.approx(
            gm.distance(pole, gm.Point(0.0, 89.0, crs=4326)), rel=1e-09
        )


def test_degenerate_rings_are_rejected_across_constructors() -> None:
    with pytest.raises(ValueError):
        gm.Polygon([(0.0, 0.0), (1.0, 1.0)])
    with pytest.raises(ValueError):
        gm.from_wkt('POLYGON ((0 0, 1 1))')
    with pytest.raises(gm.ParseError):
        gm.from_geojson('{"type": "Polygon", "coordinates": [[[0, 0], [1, 1]]]}')


def test_zero_length_lines_have_stable_lrs() -> None:
    line = gm.LineString([(1.0, 1.0), (1.0, 1.0)])
    assert line.length == 0.0
    assert line.line_interpolate(0.5, normalized=True).to_wkt() == 'POINT (1 1)'
    assert line.line_locate(gm.Point(5.0, 5.0)) == 0.0


def _wiggly_ring(n: int, cx: float, cy: float, r: float) -> gm.Polygon:
    pts = [
        (
            cx + r * (1 + 0.2 * math.sin(7 * a)) * math.cos(a),
            cy + r * (1 + 0.2 * math.sin(7 * a)) * math.sin(a),
        )
        for a in (2 * math.pi * i / n for i in range(n))
    ]
    return gm.Polygon(pts)


def _brute_boundary_distance(poly: gm.Polygon, x: float, y: float) -> float:
    best = math.inf
    coords = list(poly.exterior.coords)
    for (ax, ay), (bx, by) in itertools.pairwise(coords):
        dx, dy = (bx - ax, by - ay)
        t = 0.0
        if dx or dy:
            t = max(
                0.0, min(1.0, ((x - ax) * dx + (y - ay) * dy) / (dx * dx + dy * dy))
            )
        best = min(best, math.hypot(x - (ax + t * dx), y - (ay + t * dy)))
    return best


def test_bvh_accelerated_distance_family_matches_brute() -> None:
    """The facet-BVH engages at 64+ segments; sub-threshold inputs run the
    brute kernels. Results must be identical across the gate for the whole
    distance family — these accelerated descents otherwise only ever run in
    production.
    """
    probes = {
        24: [(2.625651554155823, 4.436471275686046), (-29.21314862466587, -16.99621197216911), (7.374101693382116, 14.507219355643763)],
        96: [(-2.6600939105601142, -4.69456308530464), (17.83360065547997, 21.581659151254804), (-27.80210520343035, 26.748011102531024)],
        200: [(1.088365826841386, 1.760404846163759), (-14.958462718469685, 10.298575434245961), (-29.3051605786515, 2.819453525714735)],
    }
    for segments, cases in probes.items():
        ring = _wiggly_ring(segments, 0.0, 0.0, 10.0)
        for x, y in cases:
            point = gm.Point(x, y)
            got = gm.distance(ring, point)
            if gm.covers(ring, point):
                assert got == 0.0
                continue
            assert got == pytest.approx(_brute_boundary_distance(ring, x, y), rel=1e-12)
            assert gm.dwithin(ring, point, math.nextafter(got, math.inf))
            assert not gm.dwithin(ring, point, got * (1 - 1e-09))
            _, witness = gm.nearest_points(point, ring)
            assert gm.distance(point, witness) == pytest.approx(got, rel=1e-12)
        outside = [
            gm.Point(x, y) for x, y in cases if not gm.covers(ring, gm.Point(x, y))
        ]
        array_distances = gm.distance(gm.GeometryArray(outside), ring)
        np.testing.assert_allclose(
            array_distances, [gm.distance(g, ring) for g in outside], rtol=1e-12
        )
    a = _wiggly_ring(128, 0.0, 0.0, 10.0)
    assert gm.intersects(a, _wiggly_ring(128, 9.0, 0.0, 10.0))
    assert not gm.intersects(a, _wiggly_ring(128, 40.0, 0.0, 10.0))
    assert gm.distance(a, _wiggly_ring(128, 40.0, 0.0, 10.0)) > 0.0


def test_bvh_overflow_lane_is_scale_invariant() -> None:
    """Coordinates too large for squared space take the hypot lane; the
    answer must scale exactly with the input (distance is homogeneous).
    """
    base = _wiggly_ring(96, 0.0, 0.0, 10.0)
    probe = gm.Point(25.0, 17.0)
    reference = gm.distance(base, probe)
    for scale in (1e150, 1e154):
        scaled_ring = gm.Polygon([
            (x * scale, y * scale) for x, y in list(base.exterior.coords)[:-1]
        ])
        scaled = gm.distance(scaled_ring, gm.Point(25.0 * scale, 17.0 * scale))
        assert scaled == pytest.approx(reference * scale, rel=1e-09)


def test_w4b_hausdorff_and_union_all_stable() -> None:
    """W4B-topology: Hausdorff heap-order + scratch and n-ary overlay bounds
    cache preserve numeric stability vs repeated calls.
    """
    left = gm.LineString([(float(i), math.sin(i * 0.1)) for i in range(200)])
    right = gm.LineString([(float(i) + 0.5, math.cos(i * 0.1)) for i in range(200)])
    h1 = gm.hausdorff_distance(left, right)
    h2 = gm.hausdorff_distance(left, right)
    assert h1 == h2
    assert h1 > 0.0
    polys = [
        gm.Polygon([
            (x, y),
            (x + 1.0, y),
            (x + 1.0, y + 1.0),
            (x, y + 1.0),
            (x, y),
        ])
        for x in range(8)
        for y in range(8)
    ]
    # Free union_all over a plain iterable exercises n-ary overlay bounds cache.
    u1 = gm.union_all(polys)
    u2 = gm.union_all(polys)
    assert gm.equals(u1, u2)
    assert u1.area == pytest.approx(64.0)
