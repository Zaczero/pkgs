"""The top-level gometry exception hierarchy: bases, pickling, and the
class each error family raises (one pin per From<Error> mapping row).

Every class subclasses ValueError through GeometryError, so broad
``except ValueError`` handlers keep working; ``GeometryTypeError``
additionally subclasses TypeError (the numpy.exceptions.AxisError dual-base
pattern). The ``match=`` patterns double as message-grammar goldens.
"""

from __future__ import annotations

import base64
import math
import pickle

import gometry as gm
import pytest

# Keep the assertions below compact while exercising the canonical top-level
# exception classes; there is deliberately no public ``gometry.errors`` module.
errors = gm

HIERARCHY = {
    'GeometryError': ValueError,
    'InvalidGeometryError': errors.GeometryError,
    'GeometryTypeError': errors.GeometryError,
    'CRSError': errors.GeometryError,
    'CRSMismatchError': errors.CRSError,
    'TransformError': errors.CRSError,
    'ParseError': errors.GeometryError,
}


def test_hierarchy_bases_identity_and_module() -> None:
    for name, base in HIERARCHY.items():
        cls = getattr(errors, name)
        assert issubclass(cls, base)
        assert issubclass(cls, ValueError)
        assert getattr(gm, name) is cls
        assert cls.__module__ == 'gometry'
        assert cls.__doc__
    assert issubclass(errors.GeometryTypeError, TypeError)


def test_exceptions_pickle() -> None:
    for name in HIERARCHY:
        error = getattr(errors, name)('boom')
        clone = pickle.loads(pickle.dumps(error))
        assert type(clone) is type(error)
        assert str(clone) == 'boom'


def test_invalid_geometry_error_on_structural_rules() -> None:
    with pytest.raises(errors.InvalidGeometryError, match='x must be finite') as excinfo:
        gm.Point(float('nan'), 0)
    assert type(excinfo.value) is errors.InvalidGeometryError
    with pytest.raises(errors.InvalidGeometryError, match='at least three coordinates') as excinfo:
        gm.Polygon([(0, 0), (1, 1)])
    assert type(excinfo.value) is errors.InvalidGeometryError
    with pytest.raises(errors.InvalidGeometryError, match='same length') as excinfo:
        gm.LineString(x=[0.0, 1.0], y=[0.0])
    assert type(excinfo.value) is errors.InvalidGeometryError


def test_geometry_type_error_on_wrong_kind() -> None:
    line_only = gm.box(0, 0, 1, 1)
    with pytest.raises(errors.GeometryTypeError, match='LineString'):
        line_only.line_locate(gm.Point(0, 0))
    with pytest.raises(TypeError):
        line_only.line_interpolate(0.5)
    with pytest.raises(ValueError):
        line_only.line_interpolate(0.5)
    # Empty-point ordinate access is an AttributeError so `match Point(x, y)`
    # patterns FAIL (per the match protocol) instead of raising mid-match.
    with pytest.raises(AttributeError, match='empty point'):
        _ = gm.from_wkt('POINT EMPTY').x


def test_crs_error_on_invalid_crs() -> None:
    with pytest.raises(errors.CRSError, match='cannot create CRS'):
        gm.CRS('not-a-crs')
    with pytest.raises(errors.CRSError, match='re-tag'):
        gm.Point(0, 0, crs=4326).set_crs(3857)


def test_crs_mismatch_error_on_index_frame_guard() -> None:
    index = gm.SpatialIndex(gm.GeometryArray([gm.Point(0.5, 0.5)]))
    with pytest.raises(errors.CRSMismatchError, match='share the index CRS') as query:
        index.query(gm.box(0, 0, 1, 1, crs=4326))
    assert (query.value.field, query.value.left, query.value.right, query.value.index) == (
        'crs', None, 'EPSG:4326', None
    )
    with pytest.raises(errors.CRSMismatchError, match='share the index CRS'):
        index.insert(gm.Point(1, 1, crs=4326))


def test_direct_frame_mismatch_paths_keep_structured_attributes() -> None:
    bare = gm.GeometryArray([gm.Point(0, 0)])
    tagged = gm.GeometryArray([gm.Point(1, 1, crs=4326)])
    with pytest.raises(gm.CRSMismatchError) as concat:
        bare.concat(tagged)
    assert (concat.value.field, concat.value.left, concat.value.right, concat.value.index) == (
        'crs', None, 'EPSG:4326', 1
    )
    with pytest.raises(gm.CRSMismatchError) as required:
        gm.require(gm.Point(0, 0, crs=3857), crs=4326)
    assert (required.value.field, required.value.left, required.value.right) == (
        'crs', 'EPSG:4326', 'EPSG:3857'
    )


def test_crs_mismatch_error_on_frame_conflicts() -> None:
    tagged = gm.Point(1, 1, crs=4326)
    bare = gm.box(0, 0, 2, 2)
    with pytest.raises(errors.CRSMismatchError, match='matching CRS metadata'):
        gm.contains(bare, tagged)
    with pytest.raises(errors.CRSMismatchError, match='one shared CRS'):
        gm.GeometryArray([tagged, gm.Point(0, 0, crs=3857)])
    epoch = gm.Point(0, 0, crs=4326, epoch=2020.5)
    with pytest.raises(errors.CRSMismatchError, match='coordinate epoch'):
        gm.distance(tagged, epoch)


def test_epoch_without_crs_is_a_crs_error_not_a_mismatch() -> None:
    with pytest.raises(
        errors.CRSError,
        match=r'^a coordinate epoch requires a CRS; attach one with crs= \(or set_crs\(\.\.\.\)\) before tagging an epoch$',
    ) as raised:
        gm.Point(0, 0, epoch=2020.0)
    assert not isinstance(raised.value, errors.CRSMismatchError)


def test_transform_error_on_transform_failures() -> None:
    with pytest.raises(errors.TransformError, match='Web Mercator'):
        gm.Point(0, 89.9, crs=4326).to_crs(3857)


def test_parse_error_on_malformed_input() -> None:
    with pytest.raises(errors.ParseError, match='invalid WKT'):
        gm.from_wkt('POINT (oops)')
    with pytest.raises(errors.ParseError, match='invalid WKB'):
        gm.from_wkb(b'\x00\x01\x02')
    with pytest.raises(errors.ParseError, match='GeoJSON'):
        gm.from_geojson('{"type": "Nope"}')


def test_from_wkb_rejects_multipoint_members_with_mismatched_axes() -> None:
    payload = base64.b64decode(
        'AQQAAAACAAAAAQEAAEAA7e3t7e3t7e3t7e3t7e0BBAAAAAIAAAABAQAAAEDt7e3t7e3t7e3t7e3t7e3tAAEAAAAEAAAAAAAAAA=='
    )
    with pytest.raises(errors.ParseError, match='invalid WKB'):
        gm.from_wkb(payload)


def test_geometry_error_on_grid_parameters() -> None:
    square = gm.box(0, 0, 1, 1, crs=4326)
    with pytest.raises(errors.GeometryError, match='H3 resolution must be between'):
        gm.h3_cover(square, resolution=99)
    with pytest.raises(errors.GeometryError, match='S2 level'):
        gm.s2_cover(square, max_cells=4, min_level=99)
    with pytest.raises(errors.ParseError, match='invalid H3 cell token') as h3_info:
        gm.H3Cell('zzz')
    assert h3_info.value.format == 'h3'
    with pytest.raises(errors.ParseError, match='S2 cell id must be') as s2_info:
        gm.S2Cell(-1)
    assert s2_info.value.format == 's2'
    with pytest.raises(errors.ParseError, match='tile id must be') as tile_info:
        gm.Tile(2**64)
    assert tile_info.value.format == 'tile'


def test_geometry_error_on_cross_domain_parameters() -> None:
    with pytest.raises(errors.GeometryError, match='unknown buffer cap_style'):
        gm.Point(0, 0).buffer(1.0, cap_style='fancy')
    with pytest.raises(errors.GeometryError, match='quadrant_segments'):
        gm.Point(0, 0).buffer(1.0, quadrant_segments=0)


def test_non_finite_scalar_parameters_carry_attrs() -> None:
    cases = [
        (lambda: gm.Point(0, 0).buffer(float('inf')), 'distance', float('inf')),
        (
            lambda: gm.LineString([(0, 0), (1, 0)]).offset_curve(float('inf')),
            'distance',
            float('inf'),
        ),
        (
            lambda: gm.LineString([(0, 0), (1, 0)]).interpolate_m(float('nan'), 1.0),
            'start_m',
            float('nan'),
        ),
        (
            lambda: gm.GeometryArray([gm.Point(0, 0)]).buffer([float('inf')]),
            'distance',
            float('inf'),
        ),
    ]
    for trigger, param, value in cases:
        with pytest.raises(
            errors.GeometryError, match=f'{param} must be finite'
        ) as excinfo:
            trigger()
        assert type(excinfo.value) is errors.GeometryError
        assert excinfo.value.param == param
        if math.isnan(value):
            assert math.isnan(excinfo.value.value)
        else:
            assert excinfo.value.value == value


def test_protocol_errors_stay_builtin() -> None:
    arr = gm.GeometryArray([gm.Point(0, 0)])
    with pytest.raises(IndexError):
        arr[5]
    with pytest.raises(TypeError, match='expected Geometry'):
        gm.union_all([1, 2])
    with pytest.raises(
        TypeError, match='element 0; strings parse via from_wkt/from_wkb/from_geojson'
    ):
        gm.GeometryCollection(['POINT (0 0)'])
    with pytest.raises(ValueError, match='not in array') as excinfo:
        arr.index(gm.Point(9, 9))
    assert not isinstance(excinfo.value, errors.GeometryError)


def test_parse_error_covers_content_violations() -> None:
    with pytest.raises(
        errors.ParseError, match='invalid WKT: coordinates must be finite'
    ):
        gm.from_wkt('POINT (NaN 0)')


def test_export_srid_without_epsg_is_crs_error() -> None:
    with pytest.raises(
        errors.CRSError, match='EWKB SRID requires an EPSG-authority CRS'
    ):
        gm.Point(0, 0, crs='OGC:CRS84').to_wkb(include_srid=True)


def test_from_arrow_rejects_non_arrow_objects() -> None:
    with pytest.raises(TypeError, match='expected a GeoArrow-encoded Arrow'):
        gm.from_arrow(object())


def test_index_construction_frame_conflict() -> None:
    with pytest.raises(
        errors.CRSMismatchError, match='spatial index requires one shared CRS'
    ):
        gm.SpatialIndex([gm.Point(0, 0), gm.Point(1, 1, crs=4326)])


def test_frechet_separates_kind_from_content() -> None:
    line = gm.from_wkt('LINESTRING (0 0, 1 1)')
    with pytest.raises(errors.GeometryTypeError, match='Frechet distance requires'):
        gm.frechet_distance(gm.box(0, 0, 1, 1), line)
    with pytest.raises(errors.InvalidGeometryError, match='non-empty linework'):
        gm.frechet_distance(gm.from_wkt('LINESTRING EMPTY'), line)
    polygons = gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(1, 1, 2, 2)])
    with pytest.raises(errors.GeometryTypeError, match='Frechet distance requires'):
        gm.frechet_distance(polygons, polygons)
    empty_lines = gm.GeometryArray([gm.from_wkt('LINESTRING EMPTY'), line])
    with pytest.raises(errors.InvalidGeometryError, match='non-empty linework'):
        gm.frechet_distance(empty_lines, empty_lines)


def test_frechet_empty_geographic_raises_invalid_geometry_error() -> None:
    empty = gm.from_wkt('LINESTRING EMPTY', crs=4326)
    line = gm.LineString([(0, 0), (1, 1)], crs=4326)
    with pytest.raises(errors.InvalidGeometryError, match='non-empty linework'):
        gm.frechet_distance(empty, line)


def test_parameter_coherence_is_geometry_error() -> None:
    transformed = gm.crs_transform(4326, 3857, [1.0], [2.0], t=[2020.0])
    assert transformed.shape == (1, 2)
    with pytest.raises(errors.GeometryError, match='4-tuple or 6-tuple'):
        gm.crs_transform_bounds(4326, 3857, (0.0, 0.0, 1.0))


def test_unknown_tokens_suggest_the_closest_spelling() -> None:
    with pytest.raises(gm.GeometryError, match="did you mean 'flat'"):
        gm.box(0, 0, 1, 1).buffer(1, cap_style='flatt')
    with pytest.raises(gm.GeometryError, match="did you mean 'intersects'"):
        gm.SpatialIndex([gm.Point(0, 0)]).query(gm.Point(0, 0), predicate='intersect')
    with pytest.raises(gm.GeometryError, match="did you mean 'round'"):
        gm.box(0, 0, 1, 1).buffer(1, cap_style='rond')
    with pytest.raises(gm.GeometryError) as excinfo:
        gm.box(0, 0, 1, 1).buffer(1, cap_style='zzzzzz')
    assert 'did you mean' not in str(excinfo.value)
    # Short nonsense must not get a distance-2 suggestion (threshold is
    # (len/3).clamp(1, 2), not a floor of 2).
    with pytest.raises(gm.GeometryError) as short:
        gm.box(0, 0, 1, 1).simplify(1, method='xx')
    assert 'did you mean' not in str(short.value)
    assert str(short.value) == "unknown simplify method \"xx\"; expected 'vw' or 'dp'"
    with pytest.raises(gm.GeometryError) as short3:
        gm.box(0, 0, 1, 1).buffer(1, cap_style='zzz')
    assert 'did you mean' not in str(short3.value)


def test_web_mercator_latitude_message_avoids_long_default_float() -> None:
    with pytest.raises(errors.TransformError) as excinfo:
        gm.Point(1e30, 1e30, crs=4326).to_crs(3857)
    message = str(excinfo.value)
    assert 'Web Mercator' in message
    # Default `{}` dump of 1e30 is a 31-digit mantissa run; :e keeps it compact.
    assert '1000000000000000' not in message
    assert 'e30' in message.lower() or 'e+30' in message.lower()


def test_array_operations_note_the_failing_row() -> None:
    with pytest.raises(gm.ParseError) as excinfo:
        gm.from_wkt(['POINT (0 0)', 'POINT (oops)'])
    assert excinfo.value.__notes__ == ['while processing array element 1']
    with pytest.raises(gm.GeometryTypeError) as excinfo2:
        gm.GeometryArray([
            gm.LineString([(0, 0), (1, 1)]),
            gm.Point(0, 0),
        ]).line_interpolate(0.5)
    assert excinfo2.value.__notes__ == ['while processing array element 1']
    with pytest.raises(gm.GeometryTypeError) as excinfo3:
        gm.frechet_distance(
            gm.GeometryArray([
                gm.from_wkt('LINESTRING (0 0, 1 1)'),
                gm.box(0, 0, 1, 1),
            ]),
            gm.from_wkt('LINESTRING (0 0, 1 1)'),
        )
    assert excinfo3.value.__notes__ == ['while processing array element 1']


def test_masked_array_error_note_uses_original_logical_row() -> None:
    values = gm.GeometryArray([None, gm.LineString([(0, 0), (1, 1)]), gm.Point(0, 0)])
    with pytest.raises(gm.GeometryTypeError) as excinfo:
        values.line_interpolate(0.5)
    assert excinfo.value.__notes__ == ['while processing array element 2']


def test_errors_carry_structured_attributes() -> None:
    with pytest.raises(gm.CRSMismatchError) as excinfo:
        gm.contains(gm.Point(0, 0), gm.Point(1, 1, crs=4326))
    assert excinfo.value.field == 'crs'
    assert excinfo.value.right == 'EPSG:4326'
    assert excinfo.value.left is None
    with pytest.raises(gm.ParseError) as excinfo2:
        gm.from_wkt('POINT (bad)')
    assert excinfo2.value.format == 'wkt'
    codec_cases = [
        ('geohash', lambda: gm.GeohashCell('not-a-cell')),
        ('quadkey', lambda: gm.Tile('9')),
        ('polyline', lambda: gm.from_polyline('???')),
        ('pluscode', lambda: gm.pluscode_polygon('nope')),
        ('osm_shortlink', lambda: gm.osm_shortlink_location('ab!cd')),
    ]
    for expected, call in codec_cases:
        with pytest.raises(gm.ParseError) as info:
            call()
        assert info.value.format == expected
    with pytest.raises(gm.CRSMismatchError) as excinfo3:
        gm.union_all([gm.Point(0, 0), gm.Point(1, 1, crs=4326)])
    assert excinfo3.value.field == 'crs'
    assert excinfo3.value.right == 'EPSG:4326'
    assert gm.CRSMismatchError('boom').left is None
    assert gm.ParseError('boom').format is None
