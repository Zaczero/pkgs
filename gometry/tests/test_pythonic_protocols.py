"""CPython protocol surface — pattern matching, operators, formatting,
value semantics, weakrefs, pickling, and memory accounting.

Every dunder/protocol added for Pythonic ergonomics is pinned here so drift
fails loudly: structural pattern matching (``__match_args__`` +
``Py_TPFLAGS_SEQUENCE``), the ``& | - ^`` overlay operators, shapely-parity
``str``/``format``, truthiness, content equality/hash for arrays, the
sequence protocol (``in``/``index``/``count``/``reversed``), cell ordering,
``weakref``, ``pickle`` across protocols, and ``nbytes``/``__sizeof__``.

The ``pickle.loads`` calls only deserialize bytes produced in-process by this
test — no untrusted data is ever loaded.
"""

from __future__ import annotations

import collections.abc as cabc
import copy
import gc
import math
import pickle
import sys
import weakref

import gometry as gm
import numpy as np
import pytest
from gometry import _lib


def test_native_pyclass_type_objects_are_immutable() -> None:
    """Every native value type rejects class-level monkeypatching."""
    native_types = [
        value
        for value in vars(_lib).values()
        if isinstance(value, type)
        and value.__module__.startswith('gometry')
        and not issubclass(value, BaseException)
    ]
    assert native_types
    mutable = []
    unexpected_errors = []
    for native_type in native_types:
        try:
            type.__setattr__(native_type, '_w6_type_mutation_probe', True)
        except TypeError as error:
            if 'immutable' not in str(error):
                unexpected_errors.append((native_type.__name__, str(error)))
        else:
            mutable.append(native_type.__name__)
            type.__delattr__(native_type, '_w6_type_mutation_probe')
    assert not unexpected_errors, unexpected_errors
    assert not mutable, f'mutable native type objects: {mutable}'


def test_registered_native_sequences_declare_sequence_flag() -> None:
    """ABC registration and native pattern-matching flags stay in parity."""
    sequence_flag = 1 << 5  # CPython's Py_TPFLAGS_SEQUENCE.
    for native_type in gm._NATIVE_SEQUENCE_TYPES:
        assert native_type.__flags__ & sequence_flag, native_type.__name__


def _regular_polygon(
    center_x: float, center_y: float, vertices: int = 32
) -> gm.Polygon:
    ring = [
        (
            center_x + 0.45 * math.cos(math.tau * i / vertices),
            center_y + 0.45 * math.sin(math.tau * i / vertices),
        )
        for i in range(vertices)
    ]
    ring.append(ring[0])
    return gm.Polygon(ring)


def test_match_class_patterns_narrow_all_leaves() -> None:
    geometries = [
        gm.Point(1, 2),
        gm.MultiPoint([(0, 0), (1, 1)]),
        gm.LineString([(0, 0), (1, 1)]),
        gm.MultiLineString([[(0, 0), (1, 1)]]),
        gm.box(0, 0, 1, 1),
        gm.MultiPolygon([[[(0, 0), (1, 0), (1, 1)]]]),
        gm.GeometryCollection([gm.Point(0, 0)]),
    ]
    seen = []
    for geom in geometries:
        match geom:
            case gm.Point():
                seen.append('Point')
            case gm.MultiPoint():
                seen.append('MultiPoint')
            case gm.LineString():
                seen.append('LineString')
            case gm.MultiLineString():
                seen.append('MultiLineString')
            case gm.Polygon():
                seen.append('Polygon')
            case gm.MultiPolygon():
                seen.append('MultiPolygon')
            case gm.GeometryCollection():
                seen.append('GeometryCollection')
    assert seen == [g.geometry_type for g in geometries]


def test_copy_replace_point_and_geometry() -> None:
    pt = gm.Point(1, 2, z=3, crs=4326, epoch=2020.0)
    replaced = copy.replace(pt, x=9)
    assert replaced.x == 9 and replaced.y == 2 and replaced.z == 3
    assert replaced.crs == gm.CRS(4326) and replaced.epoch == 2020.0
    cleared = copy.replace(pt, crs=None, epoch=None)
    assert cleared.crs is None and cleared.epoch is None
    line = gm.LineString([(0, 0), (1, 1)], crs=4326)
    relabeled = copy.replace(line, crs=3857)
    assert relabeled.crs == gm.CRS(3857) and relabeled.to_wkt() == line.to_wkt()


def test_copy_replace_rejects_unknown_keyword_arguments() -> None:
    pt = gm.Point(1, 2)
    with pytest.raises(
        TypeError,
        match=r"Point\.__replace__\(\) got an unexpected keyword argument 'banana'",
    ):
        copy.replace(pt, banana=1)
    line = gm.LineString([(0, 0), (1, 1)])
    with pytest.raises(
        TypeError,
        match=r"Geometry\.__replace__\(\) got an unexpected keyword argument 'x'",
    ):
        copy.replace(line, x=5)


def test_copy_replace_named_result_containers() -> None:
    """The kept ``NamedTuple`` results support ``copy.replace``; plain-tuple
    results (e.g. ``nearest_points``, ``dissolve``) deliberately do not.
    """
    extremes = gm.LineString([(0, 0), (2, 1)]).extremes()
    moved = copy.replace(extremes, west=gm.Point(-1, 0))
    assert moved.west == gm.Point(-1, 0) and moved.east == extremes.east
    nearest = gm.nearest_points(gm.Point(0, 0), gm.Point(3, 4))
    with pytest.raises(TypeError, match='does not support tuple'):
        copy.replace(nearest, left=gm.Point(9, 9))
    dissolved = gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)]).dissolve(
        by=['a', 'a']
    )
    with pytest.raises(TypeError, match='does not support tuple'):
        copy.replace(dissolved, groups=['b'])


def test_match_positional_destructuring() -> None:
    match gm.Point(1.5, 2.5):
        case gm.Point(x, y):
            assert (x, y) == (1.5, 2.5)
        case _:
            pytest.fail('Point(x, y) did not match')
    match gm.Polygon(
        [(0, 0), (10, 0), (10, 10), (0, 10)], holes=[[(4, 4), (6, 4), (6, 6), (4, 6)]]
    ):
        case gm.Polygon(shell, [hole]):
            assert shell.is_ring and hole.is_ring
        case _:
            pytest.fail('Polygon(exterior, interiors) did not match')
    match gm.MultiPoint([(0, 0), (3, 3)]):
        case gm.MultiPoint([gm.Point(_, _), gm.Point(x2, _)]):
            assert x2 == 3.0
        case _:
            pytest.fail('MultiPoint([a, b]) did not match')
    empty = gm.from_wkt('POINT EMPTY')
    match empty:
        case gm.Point(_, _):
            pytest.fail('POINT EMPTY must not match Point(x, y)')
        case gm.Point():
            pass


def test_match_sequence_pattern_on_geometry_array() -> None:
    arr = gm.GeometryArray([gm.Point(0, 0), gm.LineString([(0, 0), (1, 1)])])
    assert isinstance(arr, cabc.Sequence)
    match arr:
        case [gm.Point(), gm.LineString() as tail]:
            assert tail.length > 0
        case _:
            pytest.fail('sequence pattern did not match GeometryArray')
    match arr:
        case [first, *rest]:
            assert first.geometry_type == 'Point' and len(rest) == 1


def test_match_cells_coverages_reports_and_features() -> None:
    coverage = gm.h3_cover(gm.box(0, 0, 1, 1, crs=4326), resolution=4)
    match coverage:
        case gm.H3Coverage(cells):
            assert cells == coverage.cells
    match coverage[0]:
        case gm.H3Cell(cell_id):
            assert cell_id == int(coverage[0])
    s2 = gm.s2_cover(gm.box(0, 0, 1, 1, crs=4326), target_cells=4)
    match s2[0]:
        case gm.S2Cell(cell_id):
            assert cell_id == int(s2[0])
    match gm.Point(0, 0).validate():
        case gm.ValidationReport(True, None):
            pass
        case _:
            pytest.fail('valid report did not match')
    bowtie = gm.Polygon([(0, 0), (2, 2), (0, 2), (2, 0), (0, 0)])
    match bowtie.validate():
        case gm.ValidationReport(False, reason):
            assert reason is not None and 'intersection' in reason
    features = gm.from_features({
        'type': 'Feature',
        'geometry': {'type': 'Point', 'coordinates': [1, 2]},
        'properties': {'name': 'A'},
    })
    match features:
        case gm.Features(geometries, properties, ids):
            assert len(geometries) == 1 and properties == ({'name': 'A'},)
            assert ids == (None,)


def test_overlay_operators_equal_named_methods() -> None:
    a, b = (gm.box(0, 0, 2, 2, crs=3857), gm.box(1, 1, 3, 3, crs=3857))
    assert a & b == gm.intersection(a, b)
    assert a | b == gm.union(a, b)
    assert a - b == gm.difference(a, b)
    assert a ^ b == gm.symmetric_difference(a, b)
    arr = gm.GeometryArray([a, b])
    assert arr & b == gm.intersection(arr, b)
    assert arr | b == gm.union(arr, b)
    assert arr - b == gm.difference(arr, b)
    assert arr ^ b == gm.symmetric_difference(arr, b)
    assert a & arr == gm.intersection(a, arr)
    assert arr - arr == gm.difference(arr, arr)
    c = a
    c |= b
    assert c == a | b and a.area == 4.0
    with pytest.raises(TypeError, match='unsupported operand'):
        _ = a & 5
    with pytest.raises(TypeError, match='unsupported operand'):
        _ = 5 | a
    with pytest.raises(ValueError, match='matching CRS'):
        _ = a & gm.box(0, 0, 1, 1, crs=4326)


def test_str_is_wkt_and_repr_keeps_the_frame() -> None:
    point = gm.Point(1.205, 5.0, crs=4326)
    assert str(point) == point.to_wkt() == 'POINT (1.205 5)'
    assert repr(point) == '<POINT (1.205 5) EPSG:4326>'
    assert f'{point}' == 'POINT (1.205 5)'


def test_format_specs_match_shapely_semantics() -> None:
    shapely = pytest.importorskip('shapely')
    ours = gm.Point(1.205, 5.0)
    theirs = shapely.Point(1.205, 5.0)
    for spec in ('', '.0f', '.2f', '.3f', '0.2f', '.2F', 'x', 'X'):
        assert format(ours, spec) == format(theirs, spec), spec
    assert format(ours, '.3g') == 'POINT (1.205 5)'
    assert format(ours, 'g') == str(ours)
    assert format(ours, '.2') == format(ours, '.2g') == 'POINT (1.21 5)'
    assert format(ours, '0.2') == format(ours, '.2')
    assert format(gm.Point(1.25, 5.0), '.1') == format(
        theirs.__class__(1.25, 5.0), '.1'
    )
    assert (
        format(gm.from_wkt('POINT Z (1.5 2.5 3.123)'), '.1f') == 'POINT Z (1.5 2.5 3.1)'
    )
    for bad in ('d', '>10', '.2,f', '.2x'):
        with pytest.raises(ValueError, match=r'precision|format specifier'):
            format(ours, bad)


def test_bool_is_not_empty() -> None:
    assert gm.Point(0, 0)
    assert not gm.from_wkt('POINT EMPTY')
    assert not gm.from_wkt('GEOMETRYCOLLECTION EMPTY')
    assert gm.Polygon([(0, 0), (2, 2), (0, 2), (2, 0), (0, 0)])


def test_geometry_array_value_equality_and_hash() -> None:
    pts = [gm.Point(0, 0), gm.Point(1, 1)]
    left, right = (gm.GeometryArray(pts, crs=4326), gm.GeometryArray(pts, crs=4326))
    assert left == right and left is not right
    assert hash(left) == hash(right)
    assert {left: 'cached'}[right] == 'cached'
    packed = gm.points([0.0, 1.0], [0.0, 1.0])
    mixed = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)])
    assert packed == mixed and hash(packed) == hash(mixed)
    assert left != gm.GeometryArray(pts)
    assert left != gm.GeometryArray(list(reversed(pts)), crs=4326)
    assert left != gm.GeometryArray(pts[:1], crs=4326)


def test_eq_defers_to_the_other_operand_with_notimplemented() -> None:

    class Greedy:
        def __eq__(self, other: object) -> str:
            return 'greedy'

        __hash__ = None

    greedy = Greedy()
    assert (gm.Point(0, 0) == greedy) == 'greedy'
    assert (gm.GeometryArray([gm.Point(0, 0)]) == greedy) == 'greedy'
    assert (gm.CRS(4326) == greedy) == 'greedy'
    assert gm.Point(0, 0) != 'wkt' and gm.Point(0, 0) != 'wkt'


def test_sequence_protocol_in_index_count_reversed() -> None:
    a, b = (gm.Point(0, 0, crs=4326), gm.Point(1, 1, crs=4326))
    arr = gm.GeometryArray([a, b, a], crs=4326)
    assert a in arr and gm.Point(9, 9, crs=4326) not in arr
    assert 'not a geometry' not in arr
    assert arr.index(b) == 1 and arr.count(a) == 2
    with pytest.raises(ValueError, match='not in array'):
        arr.index(gm.Point(9, 9, crs=4326))
    assert gm.Point(0, 0) not in arr and arr.count(gm.Point(0, 0)) == 0
    assert [g.to_wkt() for g in reversed(arr)] == [g.to_wkt() for g in list(arr)[::-1]]


def test_all_declared_native_sequences_are_registered_and_matchable() -> None:
    cell = gm.H3Cell(13.4, 52.5, resolution=7)
    coverages = (
        gm.h3_cover(gm.box(0, 0, 1, 1, crs=4326), resolution=3),
        gm.s2_cover(gm.box(0, 0, 1, 1, crs=4326), target_cells=8),
        gm.geohash_cover(gm.box(0, 0, 1, 1, crs=4326), precision=3),
        gm.tile_cover(gm.box(0, 0, 1, 1, crs=4326), zoom=5),
    )
    groups = _lib._unpickle_int64_groups([1, 2, 1, 2], [0, 2, 4])
    values = (
        gm.MultiPoint([(0, 0), (1, 1)]).parts,
        gm.MultiPoint([(0, 0), (1, 1)]).coords,
        groups,
        cell.vertices,
        cell.edges,
        *coverages,
    )
    for value in values:
        assert isinstance(value, cabc.Sequence), type(value).__name__
        match value:
            case [first, *rest]:
                # Rows may be ndarrays: compare with array-safe equality.
                assert np.array_equal(first, value[0])
                assert len(rest) == len(value) - 1
            case _:
                pytest.fail(f'{type(value).__name__} did not match as a sequence')


def test_sequence_index_count_windows_across_native_views() -> None:
    groups = _lib._unpickle_int64_groups([1, 2, 1, 2], [0, 2, 4])
    assert groups.index(groups[0]) == 0
    assert groups.index(groups[0], 1) == 1
    assert groups.count(groups[0]) == 2
    with pytest.raises(ValueError, match='not in Groups'):
        groups.index([9])

    coords = gm.MultiPoint([(0, 0), (1, 1), (0, 0)]).coords
    np.testing.assert_array_equal(coords.row_index, [0, 0, 0])
    assert coords.index((0, 0)) == 0
    assert coords.index((0, 0), 1) == 2
    assert coords.index((0, 0), -1) == 2
    assert coords.count((0, 0)) == 2 and coords.count(object()) == 0
    with pytest.raises(ValueError, match='not in Coordinates'):
        coords.index((9, 9))

    parts = gm.MultiPoint([(0, 0), (1, 1), (0, 0)]).parts
    assert parts.index(gm.Point(0, 0)) == 0
    assert parts.index(gm.Point(0, 0), 1) == 2
    assert parts.count(gm.Point(0, 0)) == 2 and parts.count(object()) == 0
    with pytest.raises(ValueError, match='not in GeometryParts'):
        parts.index(gm.Point(9, 9))

    for coverage in (
        gm.h3_cover(gm.box(0, 0, 1, 1, crs=4326), resolution=3),
        gm.s2_cover(gm.box(0, 0, 1, 1, crs=4326), target_cells=8),
        gm.geohash_cover(gm.box(0, 0, 1, 1, crs=4326), precision=3),
        gm.tile_cover(gm.box(0, 0, 1, 1, crs=4326), zoom=5),
    ):
        first = coverage[0]
        assert coverage.index(first) == 0 and coverage.count(first) == 1
        assert coverage.count(None) == 0
        with pytest.raises(ValueError, match=rf'not in {type(coverage).__name__}'):
            coverage.index(None)


def test_geometry_groups_range_and_strided_views_preserve_value_semantics() -> None:
    values = gm.GeometryArray([
        gm.LineString([(float(row), 0.0), (float(row), 1.0)]) for row in range(8)
    ])
    groups = _lib._unpickle_geometry_groups(values, [0, 2, 3, 6, 8])

    contiguous = groups[1:3]
    assert contiguous.values.to_wkt() == values[2:6].to_wkt()
    assert contiguous.nbytes == contiguous.values.nbytes + contiguous.offsets.nbytes

    strided = groups[::-2]
    assert strided == [groups[3], groups[1]]
    assert [row.to_wkt() for row in strided] == [
        groups[3].to_wkt(),
        groups[1].to_wkt(),
    ]
    assert strided.nbytes == strided.values.nbytes + strided.offsets.nbytes

    framed = _lib._unpickle_geometry_groups(values.set_crs(4326), [0, 2, 3, 6, 8])
    assert groups != framed
    assert groups[0] not in framed


def test_geometry_parts_has_immutable_view_value_semantics() -> None:
    geom = gm.MultiPoint([(0, 0), (1, 1)])
    parts = geom.parts
    assert parts == geom.parts == list(parts)
    assert parts != gm.MultiPoint([(1, 1), (0, 0)]).parts
    with pytest.raises(TypeError, match='unhashable'):
        hash(parts)
    assert copy.copy(parts) is parts and copy.deepcopy(parts) is parts
    restored = pickle.loads(pickle.dumps(parts))
    assert restored == parts and type(restored) is type(parts)


def test_reversed_edge_cases_for_geometry_cell_and_coverage_sequences() -> None:
    a, b = (gm.Point(0, 0), gm.Point(1, 1))
    masked = gm.GeometryArray([a, None, b])
    assert [None if item is None else item.to_wkt() for item in reversed(masked)] == [
        'POINT (1 1)',
        None,
        'POINT (0 0)',
    ]
    assert list(reversed(gm.GeometryArray([]))) == []
    assert list(reversed(gm.GeometryArray([None, None]))) == [None, None]

    cells = gm.h3_cells([0.0, 1.0], [0.0, 1.0], resolution=1)
    assert list(reversed(cells)) == list(cells)[::-1]
    assert list(reversed(gm.h3_cells([], [], resolution=1))) == []

    coverage = gm.h3_cover(gm.box(0, 0, 1, 1, crs=4326), resolution=3)
    assert list(reversed(coverage)) == list(coverage.cells)[::-1]
    empty_coverage = gm.h3_cover(
        gm.Point(0, 0, crs=4326), resolution=3, cell_rule='within'
    )
    assert list(reversed(empty_coverage)) == []


def test_multipart_and_coverage_support_slicing() -> None:
    mp = gm.MultiPoint([(0, 0), (1, 1), (2, 2), (3, 3)])
    assert mp[1].to_wkt() == 'POINT (1 1)'
    assert [p.to_wkt() for p in mp[1:3]] == ['POINT (1 1)', 'POINT (2 2)']
    assert [p.to_wkt() for p in mp[::-1]] == [p.to_wkt() for p in list(mp)[::-1]]
    assert mp[10:20] == []
    parts = mp.parts
    assert type(parts).__name__ == 'GeometryParts'
    assert parts[1].to_wkt() == 'POINT (1 1)'
    assert [p.to_wkt() for p in parts[1:3]] == ['POINT (1 1)', 'POINT (2 2)']
    assert [p.to_wkt() for p in parts] == [p.to_wkt() for p in mp]
    with pytest.raises(IndexError, match='GeometryParts index out of range'):
        parts[99]
    with pytest.raises(IndexError, match='GeometryParts index out of range'):
        mp[99]
    reverse_parts = reversed(parts)
    assert type(reverse_parts).__name__ == 'GeometryPartsIterator'
    assert [p.to_wkt() for p in reverse_parts] == [
        p.to_wkt() for p in list(parts)[::-1]
    ]
    box = gm.box(0, 0, 4, 4, crs=4326)
    for cov in (
        gm.h3_cover(box, resolution=3),
        gm.s2_cover(box, target_cells=8),
        gm.geohash_cover(box, precision=3),
        gm.tile_cover(box, zoom=5),
    ):
        head = cov[:2]
        assert isinstance(head, gm.CellArray) and list(head) == list(cov)[:2]
        assert cov[0] == cov.cells[0]


def test_cells_sort_by_id_and_reject_foreign_comparisons() -> None:
    h3_cells = gm.h3_cover(gm.box(0, 0, 2, 2, crs=4326), resolution=3).cells
    assert sorted(h3_cells) == sorted(h3_cells, key=int)
    assert h3_cells.count(gm.H3Cell(80.0, 80.0, resolution=3)) == 0
    assert h3_cells.count(None) == 0
    with pytest.raises(ValueError, match='None is not in array'):
        h3_cells.index(None)
    s2_cells = gm.s2_cover(gm.box(0, 0, 2, 2, crs=4326), target_cells=8).cells
    assert sorted(s2_cells) == sorted(s2_cells, key=int)
    assert min(s2_cells) == sorted(s2_cells)[0]
    with pytest.raises(TypeError):
        _ = h3_cells[0] < 5
    assert hex(h3_cells[0]).startswith('0x')
    assert int(s2_cells[0]) == s2_cells[0].id


def test_weakref_support_on_data_types() -> None:
    point, arr, crs = (
        gm.Point(1, 2),
        gm.GeometryArray([gm.Point(0, 0)]),
        gm.CRS(4326),
    )
    refs = [weakref.ref(point), weakref.ref(arr), weakref.ref(crs)]
    assert [ref() for ref in refs] == [point, arr, crs]
    del point, arr, crs
    gc.collect()
    assert [ref() for ref in refs] == [None, None, None]
    cell = gm.h3_cover(gm.box(0, 0, 1, 1, crs=4326), resolution=4)[0]
    with pytest.raises(TypeError, match='weak reference'):
        weakref.ref(cell)


@pytest.mark.parametrize('protocol', range(2, pickle.HIGHEST_PROTOCOL + 1))
def test_pickle_round_trips_across_protocols(protocol: int) -> None:
    coverage = gm.h3_cover(gm.box(0, 0, 1, 1, crs=4326), resolution=4)
    values = [
        gm.Point(1, 2, z=3, m=4, crs=4326),
        gm.from_wkt('SRID=3857;LINESTRING (0 0, 1 1)'),
        gm.points([0.0, 1.0], [0.0, 1.0]).set_crs(4326),
        gm.GeometryArray([gm.Point(0, 0), gm.box(0, 0, 1, 1)]),
        gm.CRS(4326),
        coverage[0],
        gm.s2_cover(gm.box(0, 0, 1, 1, crs=4326), target_cells=4)[0],
        gm.from_features({
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [1, 2]},
            'properties': {},
        }),
    ]
    for value in values:
        restored = pickle.loads(pickle.dumps(value, protocol))
        assert restored == value, type(value).__name__


def test_crs_equality_is_semantic_and_unhashable() -> None:
    crs = gm.CRS(4326)
    assert crs == 4326
    assert crs == 'EPSG:4326'
    assert crs == gm.CRS('EPSG:4326')
    assert crs == 4326
    assert crs == 'EPSG:4326'
    with pytest.raises(TypeError, match='unhashable'):
        hash(crs)
    with pytest.raises(TypeError, match='unhashable'):
        _ = {crs: 1}
    values = {crs.canonical: 1}
    assert values['EPSG:4326'] == 1


def test_validation_report_is_a_value() -> None:
    bowtie = gm.Polygon([(0, 0), (2, 2), (0, 2), (2, 0), (0, 0)])
    left, right = (bowtie.validate(), bowtie.validate())
    assert left == right and left is not right
    assert hash(left) == hash(right)
    assert left != gm.Point(0, 0).validate()
    assert copy.copy(left) is left and copy.deepcopy(left) is left


def test_operators_match_named_overlay_functions() -> None:
    a = gm.from_wkt('POLYGON Z ((0 0 0, 2 0 1, 2 2 2, 0 2 1, 0 0 0))')
    b = gm.from_wkt('POLYGON Z ((1 1 0, 3 1 1, 3 3 2, 1 3 1, 1 1 0))')
    assert a & b == gm.intersection(a, b)
    assert (a & b).has_z


def test_format_of_empty_geometries() -> None:
    shapely = pytest.importorskip('shapely')
    empty = gm.from_wkt('POINT EMPTY')
    assert format(empty, '.2f') == 'POINT EMPTY'
    assert format(empty, 'x') == format(shapely.from_wkt('POINT EMPTY'), 'x')


def test_coordinate_iteration_is_linear_and_hinted() -> None:
    import operator

    coords = gm.LineString([(float(i), 0.0) for i in range(100)]).coords
    it = iter(coords)
    assert operator.length_hint(it) == 100
    next(it)
    assert operator.length_hint(it) == 99
    assert operator.length_hint(iter(coords.x)) == 100
    reverse_coords = reversed(coords)
    assert type(reverse_coords).__name__ == 'CoordinatesIterator'
    assert operator.length_hint(reverse_coords) == 100
    assert next(reverse_coords) == (99.0, 0.0)
    assert operator.length_hint(reverse_coords) == 99
    cov = gm.h3_cover(gm.box(0, 0, 1, 1, crs=4326), resolution=4)
    assert list(cov[0:2]) == list(cov.cells[:2])
    assert list(reversed(cov)) == list(cov.cells[::-1])


def test_array_getitem_error_messages_share_one_template() -> None:
    geoms = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)])
    cells = gm.h3_cells([0.0, 1.0], [0.0, 1.0], resolution=1)

    def message(call) -> str:
        with pytest.raises((IndexError, TypeError)) as exc:
            call()
        return str(exc.value)

    def normalize(text: str) -> str:
        return text.replace('geometry array', '<container>').replace(
            'cell array', '<container>'
        )

    assert normalize(message(lambda: geoms[9])) == normalize(message(lambda: cells[9]))
    assert normalize(message(lambda: geoms[[1.5]])) == normalize(
        message(lambda: cells[[1.5]])
    )


def test_array_getitem_mask_and_bool_error_boundaries_are_frozen() -> None:
    geoms = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)])
    cells = gm.h3_cells([0.0, 1.0], [0.0, 1.0], resolution=1)

    with pytest.raises(TypeError, match='boolean scalar is not a GeometryArray index'):
        geoms[True]
    with pytest.raises(TypeError, match='boolean scalar is not a CellArray index'):
        cells[True]
    with pytest.raises(
        IndexError, match='boolean mask length 1 does not match GeometryArray length 2'
    ):
        geoms[np.array([True])]
    with pytest.raises(
        IndexError, match='boolean mask length 1 does not match CellArray length 2'
    ):
        cells[np.array([True])]
    with pytest.raises(ValueError, match='mask length 1 does not match array length 2'):
        geoms[[True]]
    with pytest.raises(
        IndexError, match='boolean mask length 1 does not match CellArray length 2'
    ):
        cells[[True]]


def test_copy_returns_self_for_immutable_values() -> None:
    coverage = gm.h3_cover(gm.box(0, 0, 1, 1, crs=4326), resolution=4)
    values = [
        gm.Point(1, 2, crs=4326),
        gm.points([0.0, 1.0], [0.0, 1.0]),
        gm.CRS(4326),
        coverage[0],
        coverage.cells,
    ]
    for value in values:
        assert copy.copy(value) is value, type(value).__name__
        assert copy.deepcopy(value) is value, type(value).__name__


def test_bool_of_array_is_container_emptiness() -> None:
    assert not gm.GeometryArray([])
    assert gm.GeometryArray([gm.from_wkt('POINT EMPTY')])


def test_iterator_length_hint() -> None:
    import operator

    arr = gm.GeometryArray([gm.Point(0, 0)] * 5)
    it = iter(arr)
    assert operator.length_hint(it) == 5
    next(it)
    assert operator.length_hint(it) == 4
    assert operator.length_hint(reversed(arr)) == 5


def test_engine_copy_and_pickle_contracts() -> None:
    geom = gm.box(0, 0, 1, 1, crs=4326)
    # Iterators are transient cursors: neither picklable nor copyable.
    iterator = iter(gm.GeometryArray([geom]))
    with pytest.raises(TypeError, match='cannot pickle'):
        pickle.dumps(iterator)
    with pytest.raises(TypeError, match='cannot pickle'):
        copy.copy(iterator)
    # SpatialIndex handles are public identities; copies and pickle preserve
    # tombstones instead of compactly renumbering surviving rows.
    idx = gm.SpatialIndex(
        gm.GeometryArray([geom, geom.centroid(), gm.Point(9, 9, crs=4326)])
    )
    assert idx.remove(0)
    expected_query = idx.query(geom).tolist()
    expected_nearest = idx.nearest(gm.Point(8.9, 9.1, crs=4326)).tolist()
    assert expected_query == [1]
    assert expected_nearest == [2]
    for clone in (copy.copy(idx), copy.deepcopy(idx), pickle.loads(pickle.dumps(idx))):
        assert len(clone) == 2
        assert clone.query(geom).tolist() == expected_query
        assert clone.nearest(gm.Point(8.9, 9.1, crs=4326)).tolist() == expected_nearest
        assert list(clone) == [1, 2]
        assert 0 not in clone and 1 in clone and '1' not in clone
        handle = clone.query(geom)[0]
        assert type(handle).__module__ == 'numpy'
        assert handle in clone
        assert clone[handle].to_wkt() == geom.centroid().to_wkt()
        assert clone[1].to_wkt() == geom.centroid().to_wkt()
        with pytest.raises(KeyError):
            clone[0]
    prepared = geom.prepare()
    assert copy.copy(prepared) is prepared
    assert copy.deepcopy(prepared) is prepared
    clone = pickle.loads(pickle.dumps(prepared))
    assert clone.contains(gm.Point(0.5, 0.5, crs=4326))
    assert clone.explain() == prepared.explain()


def test_coordinates_pickle_copy_and_deepcopy_are_type_stable() -> None:
    coords = gm.LineString([(0, 0), (1, 2)], z=[3, 4], m=[5, 6]).coords
    expected = np.asarray(coords)
    assert copy.copy(coords) is coords
    assert copy.deepcopy(coords) is coords
    with pytest.raises(TypeError, match='cannot pickle Coordinates'):
        pickle.dumps(coords)
    np.testing.assert_allclose(np.asarray(coords), expected)


def test_nbytes_and_sizeof_report_real_payload() -> None:
    packed = gm.points([float(i) for i in range(100)], [0.0] * 100)
    assert packed.nbytes == 100 * 2 * 8
    assert packed.coords.x.nbytes == 100 * 8
    assert packed.coords.nbytes == packed.nbytes
    assert sys.getsizeof(packed) > packed.nbytes
    assert sys.getsizeof(packed.coords) > packed.coords.nbytes
    assert gm.Point(1, 2).nbytes == 16
    assert gm.Point(1, 2, z=3).nbytes == 24
    assert gm.box(0, 0, 1, 1).nbytes == 5 * 16
    line_zm = gm.LineString([(0, 0), (1, 1)], z=[0, 1], m=[2, 3])
    assert line_zm.nbytes == 2 * 4 * 8
    assert sys.getsizeof(line_zm) >= line_zm.nbytes


def test_scalar_geometry_sizeof_counts_shapedata_without_forcing_caches() -> None:
    """Honest retained-cost model: ShapeData Arc + initialized caches only.

    Cold ``__sizeof__`` must not build lazy products (size stable across two
    cold reads). Warming bounds / prepare may grow the reported size; it must
    never shrink. Absolute sizes are not pinned — layout can evolve.
    """
    point = gm.Point(1.0, 2.0)
    line = gm.LineString([(0.0, 0.0), (1.0, 1.0), (2.0, 2.0)])
    poly = gm.box(0.0, 0.0, 1.0, 1.0)

    for geom in (point, line, poly):
        cold_a = geom.__sizeof__()
        cold_b = geom.__sizeof__()
        assert cold_a == cold_b
        # Must exceed the coordinate-only nbytes path (ShapeData Arc is real).
        assert cold_a > geom.nbytes
        _ = geom.bounds
        warm = geom.__sizeof__()
        assert warm >= cold_a

    # Prepared path may install additional products on the shared handle.
    prepared_poly = poly.prepare()
    assert prepared_poly.__sizeof__() >= poly.__sizeof__()
    # Second prepare-side sizeof must not force further growth by itself.
    again = prepared_poly.__sizeof__()
    assert again == prepared_poly.__sizeof__()

    # Leaf scalars: coordinate payload + ShapeData header under the unified
    # Arc retained-size policy (pointee layout + control block + nested heap).
    assert gm.Point(1.0, 2.0).__sizeof__() == 456
    assert gm.LineString([(0.0, 0.0), (1.0, 1.0)]).__sizeof__() == 472


def test_container_sizeof_scales_with_members_parts_and_holes() -> None:
    """``__sizeof__`` must count container allocations, not only ordinate bytes.

    A GeometryCollection of empty points has nbytes=0 but retains a Vec of
    Shape members; MultiPolygon parts and polygon holes likewise grow the
    retained native cost at every nesting level. Cold reads never force
    lazy caches.
    """
    empty = gm.Point()
    sizes = []
    for n in (0, 1, 10, 100, 1_000, 10_000):
        gc = gm.GeometryCollection([empty] * n) if n else gm.GeometryCollection([])
        cold_a = gc.__sizeof__()
        cold_b = gc.__sizeof__()
        assert cold_a == cold_b, 'sizeof must not force lazy caches'
        sizes.append(cold_a)
    # Strictly increasing with member count (Vec capacity / nested payload).
    for prev, cur, n in zip(
        sizes[:-1], sizes[1:], (1, 10, 100, 1_000, 10_000), strict=True
    ):
        assert cur > prev, f'GC sizeof failed to grow at n={n}: {sizes}'
    # 10k empty points retain ~hundreds of KB of Shape storage, not a flat 360.
    assert sizes[-1] > 100_000, sizes

    mp_sizes = []
    for n in (1, 10, 100):
        mp = gm.MultiPolygon([
            gm.box(float(i), 0.0, float(i) + 1.0, 1.0) for i in range(n)
        ])
        mp_sizes.append(mp.__sizeof__())
    assert mp_sizes[0] < mp_sizes[1] < mp_sizes[2], mp_sizes

    hole_sizes = []
    shell = [(0.0, 0.0), (100.0, 0.0), (100.0, 100.0), (0.0, 100.0)]
    for n in (0, 1, 10, 100):
        holes = [
            [(1.0 + i, 1.0), (2.0 + i, 1.0), (2.0 + i, 2.0), (1.0 + i, 2.0)]
            for i in range(n)
        ]
        poly = gm.Polygon(shell, holes=holes if n else None)
        hole_sizes.append(poly.__sizeof__())
    assert hole_sizes[0] < hole_sizes[1] < hole_sizes[2] < hole_sizes[3], hole_sizes

    # Nested containers accounted at every level.
    inner = gm.GeometryCollection([empty] * 50)
    outer = gm.GeometryCollection([inner] * 20)
    assert outer.__sizeof__() > inner.__sizeof__()
    assert outer.__sizeof__() > gm.GeometryCollection([empty] * 20).__sizeof__()


def test_packed_geometry_array_nbytes_is_logical_for_slices() -> None:
    lines = gm.GeometryArray([
        gm.LineString([(float(i), 0.0), (float(i), 1.0), (float(i), 2.0)])
        for i in range(1000)
    ])
    assert lines.nbytes == 1000 * 3 * 2 * 8
    assert lines[0:10].nbytes == 10 * 3 * 2 * 8
    assert lines[0:10].nbytes <= lines.nbytes
    assert lines[[9, 0, 4]].nbytes == 3 * 3 * 2 * 8
    assert sys.getsizeof(lines) > lines.nbytes
    assert sys.getsizeof(lines[0:10]) > lines[0:10].nbytes

    polys = gm.GeometryArray([
        gm.box(float(i), 0.0, float(i) + 1.0, 1.0) for i in range(1000)
    ])
    assert polys.nbytes == 1000 * 5 * 2 * 8
    assert polys[0:10].nbytes == 10 * 5 * 2 * 8
    assert polys[0:10].nbytes <= polys.nbytes
    assert polys[[9, 0, 4]].nbytes == 3 * 5 * 2 * 8
    assert sys.getsizeof(polys) > polys.nbytes
    assert sys.getsizeof(polys[0:10]) > polys[0:10].nbytes


def test_cellarray_coordinates_and_groups_memory_protocols() -> None:
    cells = gm.h3_cells([0.0, 1.0, 2.0], [0.0, 1.0, 2.0], resolution=1)
    assert cells.nbytes == 3 * 8
    assert cells[0:2].nbytes == 2 * 8
    assert sys.getsizeof(cells) > cells.nbytes
    assert sys.getsizeof(cells[[2, 0]]) > cells[[2, 0]].nbytes

    lines = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (1.0, 0.0)]),
        gm.LineString([(0.0, 1.0), (1.0, 1.0)]),
    ])
    coords = lines.coords
    assert coords.nbytes == lines.nbytes
    assert sys.getsizeof(coords) > coords.nbytes

    matches = gm.SpatialIndex(gm.points([0.0, 1.0, 2.0], [0.0, 1.0, 2.0])).query(
        gm.points([0.0, 1.0, 2.0], [0.0, 1.0, 2.0])
    )
    assert matches.nbytes == matches.values.nbytes + matches.offsets.nbytes
    assert sys.getsizeof(matches) > matches.nbytes

    intersections = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (1.0, 1.0), (0.0, 1.0), (1.0, 0.0)])
    ]).self_intersections()
    assert (
        intersections.nbytes
        == intersections.values.nbytes + intersections.offsets.nbytes
    )
    assert sys.getsizeof(intersections) > intersections.nbytes

    grouped_lines = _lib._unpickle_geometry_groups(
        gm.GeometryArray([
            gm.LineString([(float(i), 0.0), (float(i), 1.0), (float(i), 2.0)])
            for i in range(10)
        ]),
        [0, 10],
    )
    grouped_ids = _lib._unpickle_int64_groups(list(range(10)), [0, 10])
    assert (
        grouped_lines.nbytes
        == grouped_lines.values.nbytes + grouped_lines.offsets.nbytes
    )
    assert (grouped_lines.__sizeof__() - grouped_lines.nbytes) > (
        grouped_ids.__sizeof__() - grouped_ids.nbytes
    )


def test_remaining_heap_retaining_types_report_sizeof_and_nbytes() -> None:
    rows = 1000
    xy_index = gm.SpatialIndex(gm.points(range(rows), [0.0] * rows))
    xyz_index = gm.SpatialIndex(gm.points(range(rows), [0.0] * rows, z=[1.0] * rows))
    xyzm_index = gm.SpatialIndex(
        gm.points(range(rows), [0.0] * rows, z=[1.0] * rows, m=[2.0] * rows)
    )
    assert sys.getsizeof(xyz_index) - sys.getsizeof(xy_index) == rows * 8
    assert sys.getsizeof(xyzm_index) - sys.getsizeof(xy_index) == rows * 16

    small_index = gm.SpatialIndex(gm.GeometryArray([_regular_polygon(0.0, 0.0)]))
    polygons = gm.GeometryArray([
        _regular_polygon(float(i % 25) * 2.0, float(i // 25) * 2.0, vertices=40)
        for i in range(500)
    ])
    large_index = gm.SpatialIndex(polygons)
    assert polygons.nbytes > 0
    assert sys.getsizeof(large_index) > sys.getsizeof(small_index)
    assert sys.getsizeof(large_index) > polygons.nbytes

    prepared_poly = _regular_polygon(0.0, 0.0, vertices=256)
    prepared = prepared_poly.prepare()
    cold_size = sys.getsizeof(prepared)
    assert cold_size > prepared_poly.nbytes
    prepared.contains_xy([0.0, 2.0], [0.0, 2.0])
    assert sys.getsizeof(prepared) >= cold_size
    line = gm.LineString([(float(i), 0.0) for i in range(128)])
    prepared_line = line.prepare()
    line_cold_size = sys.getsizeof(prepared_line)
    line.line_interpolate(12.5)
    assert sys.getsizeof(prepared_line) > line_cold_size

    area = gm.box(13.2, 52.4, 13.6, 52.6, crs=4326)
    coverages = (
        gm.h3_cover(area, resolution=6),
        gm.s2_cover(area, level=10, max_cells=32),
        gm.geohash_cover(area, precision=5),
        gm.tile_cover(area, zoom=10),
    )
    for coverage in coverages:
        assert coverage.nbytes == len(coverage) * 8
        assert sys.getsizeof(coverage) > coverage.nbytes
    nearest_left, nearest_right = gm.nearest_points(
        gm.points([0.0, 3.0], [2.0, 0.0]),
        gm.LineString([(0.0, 0.0), (2.0, 0.0)]),
    )
    assert nearest_left.nbytes == nearest_right.nbytes > 0

    crs = gm.CRS(4326)
    crs_size = sys.getsizeof(crs)
    _ = crs.name
    assert sys.getsizeof(crs) >= crs_size > 0

    multipart = gm.MultiPolygon([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)])
    parts = multipart.parts
    assert parts.nbytes == multipart.nbytes
    assert sys.getsizeof(parts) > parts.nbytes

    h3_cell = gm.H3Cell(13.4, 52.5, resolution=7)
    h3_neighbor = h3_cell.neighbors[0]
    scalar_cells = (
        h3_cell,
        gm.S2Cell(13.4, 52.5, level=12),
        gm.GeohashCell(13.4, 52.5, precision=6),
        gm.Tile(lon=13.4, lat=52.5, zoom=12),
        h3_cell.edge(h3_neighbor),
        h3_cell.vertices[0],
    )
    for cell in scalar_cells:
        assert cell.nbytes == 8
        assert sys.getsizeof(cell) >= cell.nbytes


def test_runtime_generic_subscription() -> None:
    alias = gm.GeometryArray[gm.Point]
    assert 'GeometryArray' in str(alias)
    import typing as t

    assert t.get_origin(alias) is gm.GeometryArray
    assert t.get_args(alias) == (gm.Point,)


def test_every_geometry_leaf_and_result_container_supports_match() -> None:
    """Lock-in: each geometry leaf and each multi-field result container must
    declare ``__match_args__`` so structural matching is uniform (and recursion
    through ``Polygon(exterior=LineString, _)`` stays complete). A new leaf or
    result container cannot ship without it.
    """
    leaves = [
        gm.Point,
        gm.LineString,
        gm.Polygon,
        gm.MultiPoint,
        gm.MultiLineString,
        gm.MultiPolygon,
        gm.GeometryCollection,
    ]
    containers = [
        gm.Extremes,
        gm.PolygonizeResult,
        gm.Features,
        gm.ValidationReport,
    ]
    for cls in (*leaves, *containers):
        assert getattr(cls, '__match_args__', None), (
            f'{cls.__name__} lacks __match_args__'
        )
        assert cls.__module__ == 'gometry'


def test_python_result_types_pickle_through_top_level_identity() -> None:
    ring = gm.LineString([(0, 0), (1, 0), (1, 1), (0, 0)])
    values = (
        gm.Features(gm.GeometryArray([gm.Point(0, 0)])),
        gm.Point(0, 0).extremes(),
        gm.polygonize_full([ring]),
    )
    for value in values:
        restored = pickle.loads(pickle.dumps(value))
        assert type(restored) is type(value)
        assert type(value).__module__ == 'gometry'


def test_linestring_and_nearestpoints_structural_match() -> None:
    ls = gm.from_wkt('LINESTRING (0 0, 1 1, 2 2)')
    match ls:
        case gm.LineString(coords):
            assert type(coords).__name__ == 'Coordinates'
        case _:
            pytest.fail('LineString(coords) did not match')
    match gm.from_wkt('POLYGON ((0 0, 1 0, 1 1, 0 0))'):
        case gm.Polygon(gm.LineString(_), _):
            pass
        case _:
            pytest.fail('Polygon(LineString(_), _) recursion did not match')
    a = gm.from_wkt(['POINT (0 0)', 'POINT (10 10)'])
    b = gm.from_wkt(['POINT (1 1)', 'POINT (9 9)'])
    match gm.nearest_points(a, b):
        case (left, right):
            assert len(left) == len(right) == 2
        case _:
            pytest.fail('(left, right) tuple did not match')
