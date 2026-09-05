"""Projected CF unit tokens: catalog identity, exact definitions, WKT-only fallback."""

from __future__ import annotations

import math
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
