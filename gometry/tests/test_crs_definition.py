"""CRS definitions retain their parsed axis, unit, and datum meaning."""

from __future__ import annotations

import copy
import json
import math
import pickle
from typing import Any

import gometry as gm
import pytest

WGS84_A = 6_378_137.0
DEGREE = math.pi / 180.0


def _angular_unit(factor: float, name: str = 'authored angle') -> dict[str, Any]:
    return {'type': 'AngularUnit', 'name': name, 'conversion_factor': factor}


def _mixed_geographic(order: str, directions: str) -> gm.CRS:
    definition = copy.deepcopy(gm.CRS(4326).to_projjson_dict())
    axes = definition['coordinate_system']['axis']
    axes[0]['unit'] = _angular_unit(DEGREE, 'latitude unit')
    axes[1]['unit'] = _angular_unit(0.02, 'longitude unit')
    longitude, latitude = directions.split('-')
    axes[0]['direction'] = latitude
    axes[1]['direction'] = longitude
    if order == 'lon-lat':
        axes.reverse()
    return gm.CRS(definition)


def _assert_same_axis_meaning(left: gm.CRS, right: gm.CRS) -> None:
    left_axes = left.info['axes']
    right_axes = right.info['axes']
    assert [
        (axis['direction'], axis['unit_conversion_factor']) for axis in left_axes
    ] == [(axis['direction'], axis['unit_conversion_factor']) for axis in right_axes]


@pytest.mark.parametrize(
    ('order', 'directions'),
    [
        ('lat-lon', 'east-north'),
        ('lat-lon', 'west-north'),
        ('lat-lon', 'east-south'),
        ('lat-lon', 'west-south'),
        ('lon-lat', 'east-north'),
        ('lon-lat', 'west-north'),
        ('lon-lat', 'east-south'),
        ('lon-lat', 'west-south'),
    ],
    ids=lambda value: value,
)
def test_authored_axes_execute_independent_units_order_and_sign(
    order: str, directions: str
) -> None:
    crs = _mixed_geographic(order, directions)
    longitude, latitude = directions.split('-')
    expected = (
        (-1.0 if longitude == 'west' else 1.0) * WGS84_A * 100.0 * 0.02,
        (-1.0 if latitude == 'south' else 1.0)
        * WGS84_A
        * math.asinh(math.tan(0.5 * DEGREE)),
    )

    assert crs.to_epsg() is None
    assert crs != gm.CRS(4326)
    assert gm.crs_transform(crs, 3857, 100.0, 0.5) == expected
    inverse = gm.crs_transform(3857, crs, *expected)
    assert inverse == pytest.approx((100.0, 0.5), rel=0.0, abs=4.0 * math.ulp(100.0))
    assert gm.crs_cache_info()['last_transform_engine'] == 'proj'

    reentered = gm.CRS(crs.to_wkt())
    _assert_same_axis_meaning(crs, reentered)
    assert gm.crs_transform(reentered, 3857, 100.0, 0.5) == expected


@pytest.mark.parametrize(
    'factor',
    [
        math.nextafter(DEGREE, 0.0),
        math.nextafter(DEGREE, math.inf),
        DEGREE - 1.0e-12,
        DEGREE + 1.0e-12,
    ],
    ids=['degree-down', 'degree-up', 'near-minus', 'near-plus'],
)
def test_authored_near_degree_factor_survives_json_and_wkt_reentry(
    factor: float,
) -> None:
    definition = copy.deepcopy(gm.CRS(4326).to_projjson_dict())
    for axis in definition['coordinate_system']['axis']:
        axis['unit'] = _angular_unit(factor, 'authored degree')
    crs = gm.CRS(definition)
    expected = (
        WGS84_A * 100.0 * factor,
        WGS84_A * math.asinh(math.tan(0.5 * factor)),
    )

    assert gm.crs_transform(crs, 3857, 100.0, 0.5) == pytest.approx(
        expected, rel=0.0, abs=32.0 * max(math.ulp(value) for value in expected)
    )
    assert gm.crs_transform(gm.CRS(crs.to_wkt()), 3857, 100.0, 0.5) == pytest.approx(
        expected, rel=0.0, abs=32.0 * max(math.ulp(value) for value in expected)
    )
    _assert_same_axis_meaning(crs, gm.CRS(crs.to_wkt()))


def test_authored_projected_axes_use_independent_linear_factors_both_directions() -> (
    None
):
    definition = copy.deepcopy(gm.CRS(3857).to_projjson_dict())
    axes = definition['coordinate_system']['axis']
    axes[0]['unit'] = {'type': 'LinearUnit', 'name': 'x unit', 'conversion_factor': 2.0}
    axes[1]['unit'] = {'type': 'LinearUnit', 'name': 'y unit', 'conversion_factor': 3.0}
    crs = gm.CRS(definition)

    assert crs.to_epsg() is None
    assert gm.crs_transform(crs, 3857, 100.0, 0.5) == (200.0, 1.5)
    assert gm.crs_transform(3857, crs, 200.0, 1.5) == (100.0, 0.5)
    _assert_same_axis_meaning(crs, gm.CRS(crs.to_projjson()))


@pytest.mark.parametrize(
    ('parameter_code', 'value'),
    [
        (8801, 1.0e-11),
        (8802, 9.00000000001),
        (8805, 0.99960000001),
    ],
    ids=['near-natural-origin-latitude', 'near-central-meridian', 'near-scale'],
)
def test_near_utm_parameters_do_not_use_approximate_compact_recognition(
    parameter_code: int, value: float
) -> None:
    definition = copy.deepcopy(gm.CRS(32632).to_projjson_dict())
    parameter = next(
        item
        for item in definition['conversion']['parameters']
        if item['id']['code'] == parameter_code
    )
    parameter['value'] = value
    crs = gm.CRS(definition)

    gm.crs_clear_cache()
    operation = crs.operation(4326, at=(9.0, 50.0))
    assert operation['definition'] is None
    projection = next(
        step
        for step in operation['steps']
        if step['method'] is not None and step['method']['code'] == '9807'
    )
    changed_parameter = next(
        parameter
        for parameter in projection['parameters']
        if parameter['code'] == str(parameter_code)
    )
    assert changed_parameter['value'] == value
    gm.crs_transform(4326, crs, 9.0, 50.0)
    assert gm.crs_cache_info()['last_transform_engine'] == 'proj'


def test_authored_prime_meridian_offset_and_ellipsoid_are_not_repaired_by_name() -> (
    None
):
    geographic = copy.deepcopy(gm.CRS(4326).to_projjson_dict())
    ensemble = geographic.pop('datum_ensemble')
    ensemble['type'] = 'GeodeticReferenceFrame'
    geographic['datum'] = ensemble
    geographic['datum']['prime_meridian'] = {
        'name': 'Greenwich',
        'longitude': {'value': 1.0e-11, 'unit': _angular_unit(1.0, 'radian')},
        'id': {'authority': 'EPSG', 'code': 8901},
    }
    prime_meridian = gm.CRS(geographic)
    expected = (
        WGS84_A * (100.0 * DEGREE + 1.0e-11),
        WGS84_A * math.asinh(math.tan(0.5 * DEGREE)),
    )
    assert prime_meridian.info['prime_meridian']['longitude'] == 1.0e-11
    assert gm.crs_transform(prime_meridian, 3857, 100.0, 0.5) == expected

    projected = copy.deepcopy(gm.CRS(3857).to_projjson_dict())
    projected['base_crs']['datum_ensemble']['ellipsoid']['semi_major_axis'] = (
        7_000_000.0
    )
    custom_ellipsoid = gm.CRS(projected)
    source_ellipsoid = copy.deepcopy(projected['base_crs'])
    source_ellipsoid['type'] = 'GeographicCRS'
    source_ellipsoid = gm.CRS(source_ellipsoid)
    assert custom_ellipsoid.to_epsg() is None
    assert gm.crs_transform(source_ellipsoid, custom_ellipsoid, 100.0, 0.5) == (
        7_000_000.0 * 100.0 * DEGREE,
        7_000_000.0 * math.asinh(math.tan(0.5 * DEGREE)),
    )


@pytest.mark.parametrize(
    'offset',
    [0.1000000004, math.nextafter(0.1, math.inf), -1.0e-11],
    ids=['decimal-offset', 'adjacent-offset', 'negative-offset'],
)
def test_authored_projection_offset_keeps_exact_binary64_value(offset: float) -> None:
    definition = copy.deepcopy(gm.CRS(3857).to_projjson_dict())
    parameter = next(
        item
        for item in definition['conversion']['parameters']
        if item['id']['code'] == 8806
    )
    parameter['value'] = offset
    crs = gm.CRS(definition)

    assert gm.crs_transform(4326, crs, 0.0, 0.0)[0] == offset


def test_authored_near_pole_parameter_is_not_clamped() -> None:
    definition = copy.deepcopy(gm.CRS(32661).to_projjson_dict())
    parameter = next(
        item
        for item in definition['conversion']['parameters']
        if item['id']['code'] == 8801
    )
    parameter['unit'] = _angular_unit(
        math.nextafter(DEGREE, math.inf), 'authored degree'
    )
    crs = gm.CRS(definition)

    assert crs.to_epsg() is None
    with pytest.raises(gm.TransformError, match='No inverse operation'):
        gm.crs_transform(crs, 4326, 2_000_000.0, 2_000_000.0)


def test_authored_compound_child_definition_is_exposed_and_used_for_metrics() -> None:
    horizontal = _mixed_geographic('lat-lon', 'east-north').to_projjson_dict()
    vertical = gm.CRS(5703).to_projjson_dict()
    compound = gm.CRS({
        'type': 'CompoundCRS',
        'name': 'authored compound',
        'components': [horizontal, vertical],
        'id': {'authority': 'EPSG', 'code': 6349},
    })

    child = compound.info['sub_crs'][0]
    assert child['crs'] == 'EPSG:4326'
    assert 'definition' in child
    child_definition = json.loads(child['definition'])
    assert (
        child_definition['coordinate_system']['axis'][1]['unit']['conversion_factor']
        == 0.02
    )
    assert gm.crs_transform(compound, 3857, 100.0, 0.5, 0.0)[:2] == (
        WGS84_A * 100.0 * 0.02,
        WGS84_A * math.asinh(math.tan(0.5 * DEGREE)),
    )

    left = gm.Point(100.0, 0.5, crs=compound)
    right = gm.Point(101.0, 0.5, crs=compound)
    assert gm.distance(left, right) > 120_000.0


def test_authored_definition_is_the_only_stored_label_and_never_enters_in_core() -> (
    None
):
    authored = _mixed_geographic('lat-lon', 'east-north')
    assert authored.definition.startswith('{')
    assert not hasattr(authored, 'canonical')

    gm.crs_clear_cache()
    gm.crs_transform(4326, 3857, 100.0, 0.5)
    assert gm.crs_cache_info()['last_transform_engine'] == 'in_core'
    gm.crs_clear_cache()
    gm.crs_transform(authored, 3857, 100.0, 0.5)
    assert gm.crs_cache_info()['last_transform_engine'] == 'proj'


def test_authored_definition_crosses_pickle_arrow_geoparquet_and_pandas(
    tmp_path: Any,
) -> None:
    pytest.importorskip('pyarrow')
    pytest.importorskip('pandas')
    authored = _mixed_geographic('lat-lon', 'east-north')
    values = gm.GeometryArray([gm.Point(100.0, 0.5, crs=authored)], crs=authored)

    assert pickle.loads(pickle.dumps(authored)).definition == authored.definition
    arrow = gm.from_arrow(values.to_arrow())
    assert arrow.crs.definition == authored.definition
    _assert_same_axis_meaning(authored, arrow.crs)

    for encoding in ('wkb', 'native'):
        path = tmp_path / f'{encoding}.parquet'
        values.to_geoparquet(path, encoding=encoding)
        restored, _ = gm.from_geoparquet(path)
        _assert_same_axis_meaning(authored, restored.crs)
        assert gm.crs_transform(restored.crs, 3857, 100.0, 0.5) == (
            WGS84_A * 100.0 * 0.02,
            WGS84_A * math.asinh(math.tan(0.5 * DEGREE)),
        )

    restored = gm.from_pandas(values.to_pandas())
    assert restored.crs.definition == authored.definition
    _assert_same_axis_meaning(authored, restored.crs)
