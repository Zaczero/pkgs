"""Regression tests pinning the structured-error contract.

The Rust core uses ``thiserror`` enums (``GeometryErrorKind``/``CrsError``/``IoError``)
mapped to Python exceptions at the PyO3 boundary. Two guarantees must hold:

* wrong-geometry-kind operations raise ``TypeError`` (Pythonic), while invalid
  values / malformed input raise ``ValueError``;
* serialization errors carry a consistent ``invalid <FORMAT>: ...`` prefix.
"""

from __future__ import annotations

import gometry as gm
import pytest


@pytest.mark.parametrize(
    'op',
    [
        lambda: gm.Point(0, 0).line_interpolate(1),
        lambda: gm.split(gm.LineString([(0, 0), (1, 1)]), gm.box(0, 0, 1, 1)),
        lambda: gm.Point(0, 0).triangulate(method='earcut'),
        lambda: gm.frechet_distance(gm.box(0, 0, 1, 1), gm.box(0, 0, 1, 1)),
        lambda: gm.Point(0, 0).offset_curve(1),
    ],
)
def test_wrong_geometry_kind_raises_type_error(op) -> None:
    with pytest.raises(TypeError):
        op()


@pytest.mark.parametrize('value', [gm.Point(0, 0), gm.LineString([(0, 0), (1, 1)])])
@pytest.mark.parametrize('distance', [float('nan'), float('inf')])
def test_invalid_values_stay_value_error(value, distance) -> None:
    with pytest.raises(ValueError):
        value.buffer(distance)


@pytest.mark.parametrize(
    ('parse', 'bad', 'prefix'),
    [
        (gm.from_wkt, 'NOTAGEOMETRY (0 0)', 'invalid WKT:'),
        (gm.from_geojson, {'type': 'Nope', 'coordinates': []}, 'invalid GeoJSON:'),
        (gm.from_wkb, b'\x01\xff\xff\xff\xff', 'invalid WKB:'),
    ],
)
def test_io_errors_are_format_tagged(parse, bad, prefix) -> None:
    with pytest.raises(ValueError, match=prefix.replace('(', '\\(')):
        parse(bad)


def test_overlay_handles_every_dimension_combination() -> None:
    poly = gm.Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    assert gm.union(gm.Point(5, 5), poly).geometry_type == 'GeometryCollection'
    assert (
        gm.intersection(gm.LineString([(-1, 0.5), (2, 0.5)]), poly).geometry_type
        == 'LineString'
    )
    assert (
        gm.intersection(
            gm.LineString([(0, 0), (2, 2)]), gm.LineString([(0, 2), (2, 0)])
        ).geometry_type
        == 'Point'
    )
    mixed = gm.GeometryCollection([gm.LineString([(-1, 2), (5, 2)]), poly])
    result = gm.symmetric_difference(mixed, gm.box(2, -1, 6, 5))
    assert not result.is_empty


@pytest.mark.parametrize(
    ('trigger', 'message', 'param'),
    [
        pytest.param(
            lambda: gm.Point(0, 0).buffer(1, cap_style='butt'),
            "unknown buffer cap_style \"butt\"; expected 'round', 'flat', or 'square'",
            'cap_style',
            id='cap-style',
        ),
        pytest.param(
            lambda: gm.Point(0, 0).buffer(1, join_style='nope'),
            "unknown buffer join_style \"nope\"; expected 'round', 'miter', or 'bevel'",
            'join_style',
            id='join-style',
        ),
        pytest.param(
            lambda: gm.SpatialIndex([gm.Point(0, 0)]).query(
                gm.Point(0, 0), predicate='bad'
            ),
            'unknown predicate "bad"; expected',
            'predicate',
            id='predicate',
        ),
        pytest.param(
            lambda: gm.box(0, 0, 1, 1, wrap='bogus'),
            'unknown box wrap "bogus"; expected \'split\'',
            'wrap',
            id='box-wrap',
        ),
        pytest.param(
            lambda: gm.crs_apply('+proj=affine', 0, 0, direction='sideways'),
            "unknown direction \"sideways\"; expected 'forward' or 'inverse'",
            'direction',
            id='direction',
        ),
        pytest.param(
            lambda: gm.require(gm.Point(0, 0), axes='2D'),
            "unknown axes \"2D\"; expected 'XY', 'XYZ', 'XYM', or 'XYZM'",
            'axes',
            id='axes',
        ),
    ],
)
def test_token_rejections_follow_the_canonical_template(trigger, message, param) -> None:
    """Every unknown-token rejection reads ``unknown <concept> <value>;
    [did you mean '<closest>'? ]expected 'a', 'b', or 'c'`` — pinned so the
    template cannot drift (the did-you-mean clause appears when a valid
    token is within typo distance).
    """
    with pytest.raises(ValueError) as excinfo:
        trigger()
    prefix, expected = message.split('; ', 1)
    actual = str(excinfo.value)
    assert actual.startswith(prefix + '; ')
    rest = actual[len(prefix) + 2 :]
    if rest.startswith('did you mean '):
        rest = rest.split('? ', 1)[1]
    assert rest.startswith(expected)
    assert excinfo.value.param == param


@pytest.mark.parametrize(
    ('trigger', 'param', 'value'),
    [
        pytest.param(
            lambda: gm.h3_cover(gm.box(-1, -1, 1, 1, crs=4326), resolution=99),
            'resolution',
            99,
            id='h3-resolution-range',
        ),
        pytest.param(
            lambda: gm.Point(0, 0).buffer(1, quadrant_segments=0),
            'quadrant_segments',
            0,
            id='buffer-quadrant-segments',
        ),
        pytest.param(
            lambda: gm.h3_cover(gm.box(-1, -1, 1, 1, crs=4326), resolution=7, max_cells=1),
            'max_cells',
            1,
            id='h3-cover-budget',
        ),
    ],
)
def test_grid_and_numeric_parameter_errors_keep_structured_values(
    trigger, param, value
) -> None:
    with pytest.raises(gm.GeometryError) as excinfo:
        trigger()
    assert excinfo.value.param == param
    assert excinfo.value.value == value


def test_metric_token_error_keeps_its_parameter() -> None:
    with pytest.raises(gm.GeometryError) as excinfo:
        gm.area(gm.box(0, 0, 1, 1), unit='metres')
    assert excinfo.value.param == 'unit'
    assert excinfo.value.value is None


def test_voronoi_clip_typo_uses_canonical_suggestion() -> None:
    with pytest.raises(ValueError, match="did you mean 'envelope'\\?"):
        gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)]).voronoi_polygons(
            clip='envlope'
        )
