"""Projected CF unit tokens: catalog identity, exact definitions, WKT-only fallback."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from typing import Any, cast

import gometry as gm
import pytest

_WKT1 = """PROJCS["probe",
 GEOGCS["WGS 84",
  DATUM["WGS_1984",
   SPHEROID["WGS 84",6378137,298.257223563]],
  PRIMEM["Greenwich",0],
  UNIT["degree",0.0174532925199433]],
 PROJECTION["Transverse_Mercator"],
 PARAMETER["latitude_of_origin",0],
 PARAMETER["central_meridian",0],
 PARAMETER["scale_factor",0.9996],
 PARAMETER["false_easting",500000],
 PARAMETER["false_northing",0],
 UNIT[{unit}],
 AXIS["Easting",EAST],
 AXIS["Northing",NORTH]]"""

_WKT2_ANISOTROPIC = """PROJCRS["probe",
 BASEGEOGCRS["WGS 84",
  DATUM["World Geodetic System 1984",
   ELLIPSOID["WGS 84",6378137,298.257223563,LENGTHUNIT["metre",1]]],
  PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]]],
 CONVERSION["Transverse Mercator",
  METHOD["Transverse Mercator",ID["EPSG",9807]],
  PARAMETER["Latitude of natural origin",0,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8801]],
  PARAMETER["Longitude of natural origin",0,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8802]],
  PARAMETER["Scale factor at natural origin",0.9996,SCALEUNIT["unity",1],ID["EPSG",8805]],
  PARAMETER["False easting",500000,LENGTHUNIT["metre",1],ID["EPSG",8806]],
  PARAMETER["False northing",0,LENGTHUNIT["metre",1],ID["EPSG",8807]]],
 CS[Cartesian,2],
  AXIS["easting (X)",east,ORDER[1],LENGTHUNIT["metre",1]],
  AXIS["northing (Y)",north,ORDER[2],LENGTHUNIT["foot",0.3048]]]"""


def _wkt(unit: str) -> str:
    return _WKT1.format(unit=unit)


def _cf(crs: object) -> dict[str, Any]:
    return dict(cast('dict[str, Any]', gm.CRS(crs).to_cf()))


def _pure(cf: dict[str, Any]) -> dict[str, Any]:
    out = dict(cf)
    out.pop('crs_wkt', None)
    out.pop('spatial_ref', None)
    return out


def _axis_keys(crs: object) -> set[str]:
    return set(gm.CRS(crs).axes[0])


def test_named_epsg_metre_foot_usft_admit_tokens() -> None:
    metre = _cf(32633)
    assert metre['units'] == 'm'
    assert metre['grid_mapping_name'] == 'transverse_mercator'
    foot = _cf(2222)
    assert foot['units'] == 'ft'
    assert foot['grid_mapping_name'] == 'transverse_mercator'
    usft = _cf(2227)
    assert usft['units'] == 'us-ft'
    assert usft['grid_mapping_name'] == 'lambert_conformal_conic'


def test_public_axis_dict_omits_unit_identity() -> None:
    keys = _axis_keys(2227)
    assert keys == {
        'name',
        'abbreviation',
        'direction',
        'unit_name',
        'unit_conversion_factor',
    }


def test_misleading_names_do_not_admit_tokens() -> None:
    for clause in (
        '"foot",2.5',
        '"metre",2.5',
        '"us-ft",2.5',
        '"US survey foot",2.5,AUTHORITY["EPSG","9003"]',
        '"banana",1.0000000000005',
        '"kilometre",1000',
        '"Clarke\'s foot",0.3047972654',
        '"German legal metre",1.0000135965',
        '"smoot",1.7018',
    ):
        cf = _cf(_wkt(clause))
        assert 'grid_mapping_name' not in cf, clause
        assert 'units' not in cf, clause
        assert 'crs_wkt' in cf
        assert 'semi_major_axis' in cf
        assert 'projected_crs_name' in cf
        with pytest.raises(gm.CRSError):
            gm.CRS(_pure(cf))
        kept = gm.CRS(cf)
        assert kept.axes[0]['unit_conversion_factor'] == pytest.approx(
            gm.CRS(_wkt(clause)).axes[0]['unit_conversion_factor']
        )


def test_exact_unidentified_definitions_admit_tokens() -> None:
    banana_metre = _cf(_wkt('"banana",1'))
    assert banana_metre['units'] == 'm'
    assert banana_metre['grid_mapping_name'] == 'transverse_mercator'
    banana_foot = _cf(_wkt('"banana",0.3048'))
    assert banana_foot['units'] == 'ft'
    assert banana_foot['grid_mapping_name'] == 'transverse_mercator'


def test_inconsistent_identified_unit_is_wkt_only() -> None:
    cf = _cf(_wkt('"US survey foot",2.5,AUTHORITY["EPSG","9003"]'))
    assert 'units' not in cf
    assert 'grid_mapping_name' not in cf
    assert 'crs_wkt' in cf


def test_same_unit_horizontal_axes_admit() -> None:
    cf = _cf(_wkt('"metre",1,AUTHORITY["EPSG","9001"]'))
    assert cf['units'] == 'm'
    axes = gm.CRS(_wkt('"metre",1,AUTHORITY["EPSG","9001"]')).axes
    assert len(axes) >= 2
    assert axes[0]['unit_conversion_factor'] == axes[1]['unit_conversion_factor'] == 1.0


def test_anisotropic_horizontal_axes_omit_mapping() -> None:
    crs = gm.CRS(_WKT2_ANISOTROPIC)
    factors = [axis['unit_conversion_factor'] for axis in crs.axes[:2]]
    assert factors == [1.0, 0.3048]
    cf = _cf(crs)
    assert 'units' not in cf
    assert 'grid_mapping_name' not in cf
    assert 'crs_wkt' in cf


def test_parse_aliases_and_precedence_unchanged() -> None:
    metre = 1.0
    foot = 0.3048
    us_survey_foot = 1200.0 / 3937.0
    aliases = (
        ('m', metre),
        ('metre', metre),
        ('meter', metre),
        ('metres', metre),
        ('meters', metre),
        ('ft', foot),
        ('foot', foot),
        ('feet', foot),
        ('us-ft', us_survey_foot),
        ('US survey foot', us_survey_foot),
        ('us_survey_foot', us_survey_foot),
        ('ftUS', us_survey_foot),
    )
    base = _pure(_cf(32633))
    for token, expected in aliases:
        cf = dict(base)
        cf['units'] = token
        rebuilt = gm.CRS(cf)
        assert rebuilt.axes[0]['unit_conversion_factor'] == pytest.approx(expected)
    omitted = dict(base)
    omitted.pop('units', None)
    omitted.pop('proj_units', None)
    assert gm.CRS(omitted).axes[0]['unit_conversion_factor'] == metre
    both = dict(base)
    both['units'] = 'us-ft'
    both['proj_units'] = 'm'
    assert gm.CRS(both).axes[0]['unit_conversion_factor'] == pytest.approx(us_survey_foot)
    proj_only = dict(base)
    proj_only.pop('units', None)
    proj_only['proj_units'] = 'ft'
    assert gm.CRS(proj_only).axes[0]['unit_conversion_factor'] == foot


def test_pure_cf_without_wkt_does_not_become_metre() -> None:
    cf = _pure(_cf(_wkt('"kilometre",1000')))
    with pytest.raises(gm.CRSError):
        gm.CRS(cf)
    foot = _pure(_cf(_wkt('"foot",2.5')))
    with pytest.raises(gm.CRSError):
        gm.CRS(foot)


def test_epsg_2227_pure_cf_roundtrip_native_offset() -> None:
    cf = _cf(2227)
    assert cf['units'] == 'us-ft'
    del cf['crs_wkt']
    rebuilt = gm.CRS(cf)
    origin_e, origin_n = cf['false_easting'], cf['false_northing']
    g_native = gm.Point(origin_e + 1000.0, origin_n, crs=2227).to_crs(4326)
    g_rebuilt = gm.Point(origin_e + 1000.0, origin_n, crs=rebuilt).to_crs(4326)
    assert abs(g_native.x - g_rebuilt.x) < 1e-8
    assert abs(g_native.y - g_rebuilt.y) < 1e-8


def test_near_unit_nextafter_metre_is_wkt_only() -> None:
    factor = math.nextafter(1.0, math.inf)
    cf = _cf(_wkt(f'"banana",{factor!r}'))
    assert 'units' not in cf
    assert 'grid_mapping_name' not in cf
    with pytest.raises(gm.CRSError):
        gm.CRS(_pure(cf))


_LCC1SP = """PROJCRS["probe",
 BASEGEOGCRS["WGS 84",
  DATUM["World Geodetic System 1984",
   ELLIPSOID["WGS 84",6378137,298.257223563,LENGTHUNIT["metre",1]]],
  PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]]],
 CONVERSION["LCC 1SP",
  METHOD["Lambert Conic Conformal (1SP)",ID["EPSG",9801]],
  PARAMETER["Latitude of natural origin",40,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8801]],
  PARAMETER["Longitude of natural origin",-100,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8802]],
  {scale}
  PARAMETER["False easting",0,LENGTHUNIT["metre",1],ID["EPSG",8806]],
  PARAMETER["False northing",0,LENGTHUNIT["metre",1],ID["EPSG",8807]]],
 CS[Cartesian,2],
  AXIS["easting (X)",east,ORDER[1],LENGTHUNIT["metre",1]],
  AXIS["northing (Y)",north,ORDER[2],LENGTHUNIT["metre",1]]]"""


def _lcc1sp_scale(value: str, unit: str) -> str:
    return (
        f'PARAMETER["Scale factor at natural origin",{value},'
        f'SCALEUNIT[{unit}],ID["EPSG",8805]],'
    )


def _lcc1sp(*, scale: str | None = None) -> str:
    clause = scale if scale is not None else _lcc1sp_scale('1', '"unity",1')
    return _LCC1SP.format(scale=clause)


def _scale_params(crs: object) -> list[Mapping[str, Any]]:
    operation = gm.CRS(crs).info['coordinate_operation']
    assert isinstance(operation, Mapping)
    parameters = operation['parameters']
    assert isinstance(parameters, Sequence)
    return [
        param
        for param in parameters
        if param.get('code') == '8805'
        or param.get('name') == 'Scale factor at natural origin'
    ]


def _assert_pure_cf_transform(source: object, easting: float, northing: float) -> None:
    cf = _pure(_cf(source))
    rebuilt = gm.CRS(cf)
    native = gm.crs_transform(source, 4326, easting, northing)
    roundtrip = gm.crs_transform(rebuilt, 4326, easting, northing)
    assert native == pytest.approx(roundtrip)


def test_epsg_3388_mercator_b_pure_cf() -> None:
    cf = _cf(3388)
    assert cf['grid_mapping_name'] == 'mercator'
    assert cf['standard_parallel'] == pytest.approx(42.0)
    assert 'scale_factor_at_projection_origin' not in cf
    del cf['crs_wkt']
    assert gm.CRS(cf).to_epsg() == 3388
    _assert_pure_cf_transform(3388, 165704.29, 5171848.07)


def test_epsg_24200_and_3448_lcc1sp_pure_cf() -> None:
    for code, easting, northing in (
        (24200, 250000.0, 150000.0),
        (3448, 750000.0, 650000.0),
    ):
        cf = _cf(code)
        assert cf['grid_mapping_name'] == 'lambert_conformal_conic'
        assert cf['standard_parallel'] == pytest.approx(18.0)
        assert cf['latitude_of_projection_origin'] == pytest.approx(18.0)
        assert 'scale_factor_at_projection_origin' not in cf
        del cf['crs_wkt']
        assert gm.CRS(cf).to_epsg() == code
        _assert_pure_cf_transform(code, easting, northing)


def test_epsg_27563_non_unity_scale_is_wkt_only() -> None:
    cf = _cf(27563)
    assert 'grid_mapping_name' not in cf
    assert 'crs_wkt' in cf
    scales = _scale_params(27563)
    assert len(scales) == 1
    assert scales[0]['value'] == pytest.approx(0.999877499)
    assert scales[0]['unit_conversion_factor'] == 1.0


def test_custom_southern_mercator_b_pure_cf() -> None:
    source = '+proj=merc +lat_ts=-30 +lon_0=20 +ellps=WGS84 +type=crs'
    cf = _cf(source)
    assert cf['grid_mapping_name'] == 'mercator'
    assert cf['standard_parallel'] == pytest.approx(-30.0)
    assert 'scale_factor_at_projection_origin' not in cf
    _assert_pure_cf_transform(source, 0.0, 0.0)
    _assert_pure_cf_transform(source, 1_000_000.0, 2_000_000.0)


def test_unidentified_unity_scale_admits_lcc1sp() -> None:
    wkt = _lcc1sp(scale=_lcc1sp_scale('1', '"banana",1'))
    scales = _scale_params(wkt)
    assert len(scales) == 1
    assert scales[0]['value'] == 1.0
    assert scales[0]['unit_conversion_factor'] == 1.0
    cf = _cf(wkt)
    assert cf['grid_mapping_name'] == 'lambert_conformal_conic'
    assert cf['standard_parallel'] == pytest.approx(40.0)
    assert 'scale_factor_at_projection_origin' not in cf
    _assert_pure_cf_transform(wkt, 0.0, 0.0)


def test_lcc1sp_product_identity_scale_is_wkt_only() -> None:
    wkt = _lcc1sp(scale=_lcc1sp_scale('0.5', '"unity",2'))
    scales = _scale_params(wkt)
    assert len(scales) == 1
    assert scales[0]['value'] == 0.5
    assert scales[0]['unit_conversion_factor'] == 2.0
    assert 'grid_mapping_name' not in _cf(wkt)


def test_lcc1sp_adjacent_float_scale_is_wkt_only() -> None:
    adjacent = math.nextafter(1.0, math.inf)
    wkt = _lcc1sp(scale=_lcc1sp_scale(repr(adjacent), '"unity",1'))
    scales = _scale_params(wkt)
    assert len(scales) == 1
    assert scales[0]['value'] == adjacent
    assert scales[0]['unit_conversion_factor'] == 1.0
    assert 'grid_mapping_name' not in _cf(wkt)


def test_lcc1sp_missing_scale_is_wkt_only() -> None:
    wkt = _lcc1sp(scale='')
    assert _scale_params(wkt) == []
    assert 'grid_mapping_name' not in _cf(wkt)


def test_lcc1sp_duplicate_scale_is_wkt_only() -> None:
    wkt = _lcc1sp(
        scale=_lcc1sp_scale('1', '"unity",1') + _lcc1sp_scale('1', '"unity",1')
    )
    assert len(_scale_params(wkt)) != 1
    assert 'grid_mapping_name' not in _cf(wkt)


def test_lcc2sp_and_mercator_a_and_unsupported_units_unchanged() -> None:
    lcc2 = _cf(3034)
    assert lcc2['grid_mapping_name'] == 'lambert_conformal_conic'
    assert lcc2['standard_parallel'] == pytest.approx([35.0, 65.0])
    merc_a = _cf(3395)
    assert merc_a['grid_mapping_name'] == 'mercator'
    assert merc_a['scale_factor_at_projection_origin'] == pytest.approx(1.0)
    assert 'standard_parallel' not in merc_a
    unsupported = _cf(_wkt('"kilometre",1000'))
    assert 'grid_mapping_name' not in unsupported
    assert 'units' not in unsupported
    assert 'crs_wkt' in unsupported


_DEG = 'ANGLEUNIT["degree",0.0174532925199433]'
_METRE = 'LENGTHUNIT["metre",1]'


def _merc_a_wkt(*, extra_8823: bool = False) -> str:
    extra = (
        f'  PARAMETER["Latitude of 1st standard parallel",30,{_DEG},ID["EPSG",8823]],\n'
        if extra_8823
        else ''
    )
    return f"""PROJCRS["probe A",
 BASEGEOGCRS["WGS 84",
  DATUM["World Geodetic System 1984",
   ELLIPSOID["WGS 84",6378137,298.257223563,{_METRE}]],
  PRIMEM["Greenwich",0,{_DEG}]],
 CONVERSION["Mercator A",
  METHOD["Mercator (variant A)",ID["EPSG",9804]],
  PARAMETER["Latitude of natural origin",0,{_DEG},ID["EPSG",8801]],
  PARAMETER["Longitude of natural origin",10,{_DEG},ID["EPSG",8802]],
  PARAMETER["Scale factor at natural origin",0.8,SCALEUNIT["unity",1],ID["EPSG",8805]],
{extra}  PARAMETER["False easting",1,{_METRE},ID["EPSG",8806]],
  PARAMETER["False northing",2,{_METRE},ID["EPSG",8807]]],
 CS[Cartesian,2],
  AXIS["easting (X)",east,ORDER[1],{_METRE}],
  AXIS["northing (Y)",north,ORDER[2],{_METRE}]]"""


def _merc_b_wkt(*, extra_8805: bool = False) -> str:
    extra = (
        '  PARAMETER["Scale factor at natural origin",0.8,SCALEUNIT["unity",1],ID["EPSG",8805]],\n'
        if extra_8805
        else ''
    )
    return f"""PROJCRS["probe B",
 BASEGEOGCRS["WGS 84",
  DATUM["World Geodetic System 1984",
   ELLIPSOID["WGS 84",6378137,298.257223563,{_METRE}]],
  PRIMEM["Greenwich",0,{_DEG}]],
 CONVERSION["Mercator B",
  METHOD["Mercator (variant B)",ID["EPSG",9805]],
  PARAMETER["Latitude of 1st standard parallel",42,{_DEG},ID["EPSG",8823]],
  PARAMETER["Longitude of natural origin",51,{_DEG},ID["EPSG",8802]],
{extra}  PARAMETER["False easting",0,{_METRE},ID["EPSG",8806]],
  PARAMETER["False northing",0,{_METRE},ID["EPSG",8807]]],
 CS[Cartesian,2],
  AXIS["easting (X)",east,ORDER[1],{_METRE}],
  AXIS["northing (Y)",north,ORDER[2],{_METRE}]]"""


def _lcc1sp_origin(*, latitudes: list[float], longitude: float | None = -100.0) -> str:
    lat_lines = ''.join(
        f'  PARAMETER["Latitude of natural origin",{lat},{_DEG},ID["EPSG",8801]],\n'
        for lat in latitudes
    )
    lon_line = (
        f'  PARAMETER["Longitude of natural origin",{longitude},{_DEG},ID["EPSG",8802]],\n'
        if longitude is not None
        else ''
    )
    return f"""PROJCRS["probe LCC origin",
 BASEGEOGCRS["WGS 84",
  DATUM["World Geodetic System 1984",
   ELLIPSOID["WGS 84",6378137,298.257223563,{_METRE}]],
  PRIMEM["Greenwich",0,{_DEG}]],
 CONVERSION["LCC 1SP",
  METHOD["Lambert Conic Conformal (1SP)",ID["EPSG",9801]],
{lat_lines}{lon_line}  PARAMETER["Scale factor at natural origin",1,SCALEUNIT["unity",1],ID["EPSG",8805]],
  PARAMETER["False easting",0,{_METRE},ID["EPSG",8806]],
  PARAMETER["False northing",0,{_METRE},ID["EPSG",8807]]],
 CS[Cartesian,2],
  AXIS["easting (X)",east,ORDER[1],{_METRE}],
  AXIS["northing (Y)",north,ORDER[2],{_METRE}]]"""


def test_mercator_a_stray_8823_exports_scale_only() -> None:
    extra = _merc_a_wkt(extra_8823=True)
    plain = _merc_a_wkt()
    cf = _cf(extra)
    assert cf['grid_mapping_name'] == 'mercator'
    assert cf['scale_factor_at_projection_origin'] == pytest.approx(0.8)
    assert 'standard_parallel' not in cf
    xy = (1_000_000.0, 2_000_000.0)
    assert gm.crs_transform(extra, 4326, *xy) == pytest.approx(
        gm.crs_transform(plain, 4326, *xy)
    )
    _assert_pure_cf_transform(extra, *xy)


def test_mercator_b_extra_8805_is_wkt_only() -> None:
    wkt = _merc_b_wkt(extra_8805=True)
    cf = _cf(wkt)
    assert 'grid_mapping_name' not in cf
    assert 'crs_wkt' in cf
    scales = _scale_params(wkt)
    assert any(param.get('code') == '8805' for param in scales)


def test_mercator_json_extras_match_wkt() -> None:
    a_json = json.loads(gm.CRS(_merc_a_wkt()).to_projjson())
    a_json['conversion']['parameters'].append({
        'name': 'Latitude of 1st standard parallel',
        'value': 30.0,
        'unit': 'degree',
        'id': {'authority': 'EPSG', 'code': 8823},
    })
    a_cf = _cf(a_json)
    assert a_cf['grid_mapping_name'] == 'mercator'
    assert a_cf['scale_factor_at_projection_origin'] == pytest.approx(0.8)
    assert 'standard_parallel' not in a_cf
    _assert_pure_cf_transform(a_json, 1_000_000.0, 2_000_000.0)

    b_json = json.loads(gm.CRS(_merc_b_wkt()).to_projjson())
    b_json['conversion']['parameters'].append({
        'name': 'Scale factor at natural origin',
        'value': 0.8,
        'unit': 'unity',
        'id': {'authority': 'EPSG', 'code': 8805},
    })
    b_cf = _cf(b_json)
    assert 'grid_mapping_name' not in b_cf
    assert 'crs_wkt' in b_cf


def test_lcc1sp_missing_and_duplicate_origin_are_wkt_only() -> None:
    missing_lat = _lcc1sp_origin(latitudes=[])
    missing_lon = _lcc1sp_origin(latitudes=[40.0], longitude=None)
    duplicate = _lcc1sp_origin(latitudes=[40.0, 50.0])
    for wkt in (missing_lat, missing_lon, duplicate):
        cf = _cf(wkt)
        assert 'grid_mapping_name' not in cf
        assert 'crs_wkt' in cf


def test_catalog_3388_3395_24200_3448_unchanged() -> None:
    merc_b = _cf(3388)
    assert merc_b['grid_mapping_name'] == 'mercator'
    assert merc_b['standard_parallel'] == pytest.approx(42.0)
    assert 'scale_factor_at_projection_origin' not in merc_b
    merc_a = _cf(3395)
    assert merc_a['grid_mapping_name'] == 'mercator'
    assert merc_a['scale_factor_at_projection_origin'] == pytest.approx(1.0)
    assert 'standard_parallel' not in merc_a
    for code in (24200, 3448):
        lcc = _cf(code)
        assert lcc['grid_mapping_name'] == 'lambert_conformal_conic'
        assert lcc['standard_parallel'] == pytest.approx(18.0)
        assert 'scale_factor_at_projection_origin' not in lcc
