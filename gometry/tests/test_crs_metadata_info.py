"""CRS behavior — geodesic measurement, transforms, best-UTM selection,
runtime config, cache info, and the PROJ authority metadata surface.
"""

import json
import math
from typing import Any, cast

import gometry as gm
import pytest

_WKT_4326 = gm.CRS(4326).to_wkt()


class WktCrsObject:
    def to_wkt(self) -> str:
        return _WKT_4326


class ProjjsonCrsObject:
    def to_json(self) -> str:
        return gm.CRS(4326).to_projjson()


class AuthorityCrsObject:
    def to_authority(self) -> tuple[str, str]:
        return ('EPSG', '4326')


class AuthorityPreferredCrsObject:
    calls: list[str]

    def __init__(self) -> None:
        self.calls = []

    def to_authority(self) -> tuple[str, int]:
        self.calls.append('authority')
        return ('EPSG', 4326)

    def to_wkt(self) -> str:
        self.calls.append('wkt')
        return _WKT_4326


class AuthorityFallbackWktCrsObject:
    def to_authority(self) -> None:
        return None

    def to_wkt(self) -> str:
        return _WKT_4326


class EpsgCrsObject:
    def to_epsg(self) -> int:
        return 4326


class InvalidEpsgCrsObject:
    def to_epsg(self) -> str:
        return 'EPSG:4326'


class EmptyEpsgCrsObject:
    def to_epsg(self) -> None:
        return None


class CrsHolderObject:
    crs = ('EPSG', 4326)


class SrsHolderObject:
    srs = EpsgCrsObject()


class NestedCrsHolderObject:
    crs = SrsHolderObject()


class EmptyAuthorityCrsObject:
    def to_authority(self) -> None:
        return None


def _assert_crs_namespace_database_catalog_and_crs_info() -> None:
    engine = gm.crs_engine()
    geographic = gm.crs_info(4326)
    projected = gm.crs_info('epsg:27700')
    projjson = json.loads(gm.CRS(4326).to_projjson())
    projjson_dict = gm.CRS(4326).to_projjson_dict()
    assert isinstance(engine['version'], str)
    assert isinstance(engine['major'], int)
    assert isinstance(engine['minor'], int)
    assert isinstance(engine['patch'], int)
    assert engine['backend'] == 'proj-sys/libPROJ'
    assert engine['bundled_proj'] is True
    assert isinstance(engine['database_metadata'], dict)
    assert 'DATABASE.LAYOUT.VERSION.MAJOR' in cast(
        'dict[str, str]', engine['database_metadata']
    )
    assert gm.CRS('epsg:4326').canonical == 'EPSG:4326'
    assert projjson_dict == projjson
    assert projjson_dict['type'] == 'GeographicCRS'
    assert projjson_dict['name'] == 'WGS 84'
    assert gm.CRS(projjson_dict).canonical == 'EPSG:4326'
    assert 'EPSG' in gm.crs_authorities()
    assert '4326' in gm.crs_codes('EPSG', kind='geographic_2d')
    assert '3857' in gm.crs_codes('EPSG', kind='projected')
    assert '4979' in gm.crs_codes('EPSG', kind='geographic_3d')
    assert '7030' in gm.crs_codes('EPSG', kind='ellipsoid')
    assert '8901' in gm.crs_codes('EPSG', kind='prime_meridian')
    assert '6326' in gm.crs_codes('EPSG', kind='datum_ensemble')
    assert '6326' in gm.crs_codes('EPSG', kind='geodetic_reference_frame')
    celestial_bodies = gm.crs_celestial_bodies()
    assert {'authority': 'PROJ', 'name': 'Earth'} in celestial_bodies
    proj_operations = gm.crs_proj_operations()
    mercator = next(
        operation for operation in proj_operations if operation['id'] == 'merc'
    )
    assert cast('str', mercator['description']).startswith('Mercator')
    ellipsoids = gm.crs_ellipsoids()
    wgs84_ellipsoid = next(
        ellipsoid for ellipsoid in ellipsoids if ellipsoid['id'] == 'WGS84'
    )
    assert wgs84_ellipsoid == {
        'id': 'WGS84',
        'semi_major': 'a=6378137.0',
        'definition': 'rf=298.257223563',
        'name': 'WGS 84',
    }
    assert {'id': 'greenwich', 'definition': '0dE'} in gm.crs_prime_meridians()
    utm_zones = gm.crs_utm_zones(
        datum_name='WGS 84', area_of_interest=(20.0, 51.0, 22.0, 53.0)
    )
    assert [item['crs'] for item in utm_zones] == ['EPSG:32634']
    assert utm_zones[0]['name'] == 'WGS 84 / UTM zone 34N'
    assert utm_zones[0]['projection_method_name'] == 'Transverse Mercator'
    crs_catalog = gm.crs_catalog(
        authority='EPSG', kind='projected', area_of_interest=(-1.0, 50.0, 1.0, 52.0)
    )
    catalog_27700 = next(item for item in crs_catalog if item['crs'] == 'EPSG:27700')
    assert catalog_27700['name'] == 'OSGB36 / British National Grid'
    assert catalog_27700['kind'] == 'projected'
    assert catalog_27700['deprecated'] is False
    assert catalog_27700['celestial_body'] == 'Earth'
    assert catalog_27700['projection_method_name'] == 'Transverse Mercator'
    catalog_area = cast('dict[str, object]', catalog_27700['area_of_use'])
    assert catalog_area['west'] == pytest.approx(-9.01)
    assert any(
        item['crs'] == 'EPSG:4326' for item in gm.crs_catalog(celestial_body='Earth')
    )
    assert any(
        item['crs'] == 'EPSG:4326'
        for item in gm.crs_catalog(authority='EPSG', celestial_body='Earth')
    )
    meter = gm.crs_unit('EPSG', '9001')
    assert meter == {
        'authority': 'EPSG',
        'code': '9001',
        'name': 'metre',
        'category': 'linear',
        'conversion_factor': 1.0,
        'proj_short_name': None,
    }
    linear_units = gm.crs_units('EPSG', category='linear')
    assert any(
        unit['code'] == '9001' and unit['proj_short_name'] == 'm'
        for unit in linear_units
    )
    geoid_models = gm.CRS(5703).geoid_models
    assert 'GEOID18' in geoid_models
    assert gm.CRS(4326).geoid_models == []
    with pytest.raises(ValueError, match='unknown PROJ database kind'):
        gm.crs_codes('EPSG', kind='postcode')
    with pytest.raises(ValueError, match='unknown PROJ database kind'):
        gm.crs_codes('EPSG', kind='projected_crs')
    with pytest.raises(ValueError, match='authority'):
        gm.crs_codes('')
    with pytest.raises(ValueError, match='CRS catalog kind must be a CRS type'):
        gm.crs_catalog(authority='EPSG', kind='ellipsoid')
    with pytest.raises(ValueError, match='unknown PROJ database kind'):
        gm.crs_catalog(authority='EPSG', kind='postcode')
    with pytest.raises(ValueError, match='CRS catalog authority'):
        gm.crs_catalog(authority='')
    with pytest.raises(ValueError, match='celestial_body must be'):
        gm.crs_catalog(authority='EPSG', celestial_body='')
    with pytest.raises(ValueError, match='celestial body authority must be'):
        gm.crs_celestial_bodies(authority='')
    with pytest.raises(ValueError, match='datum_name must be'):
        gm.crs_utm_zones(datum_name='')
    assert geographic['crs'] == 'EPSG:4326'
    assert geographic['name'] == 'WGS 84'
    assert geographic['authority'] == 'EPSG'
    assert geographic['code'] == '4326'
    assert geographic['kind'] == 'geographic_2d'
    assert geographic['is_derived'] is False
    assert geographic['deprecated'] is False
    assert geographic['sub_crs'] == []
    assert geographic['source_crs'] is None
    assert geographic['target_crs'] is None
    assert geographic['coordinate_operation'] is None
    assert cast('dict[str, object]', geographic['geodetic_crs'])['crs'] == 'EPSG:4326'
    assert cast('dict[str, object]', geographic['horizontal_datum'])['code'] == '6326'
    assert geographic['scope'] == 'Horizontal component of 3D system.'
    assert cast('list[object]', geographic['domains'])
    assert gm.crs_info(2037)['deprecated'] is True
    replacements = gm.CRS(2037).non_deprecated
    assert replacements == [
        {
            'crs': 'EPSG:2960',
            'authority': 'EPSG',
            'code': '2960',
            'name': 'NAD83(CSRS) / UTM zone 19N',
            'kind': 'projected',
            'deprecated': False,
            'area_of_use': {
                'west': -72.0,
                'south': 40.8,
                'east': -66.0,
                'north': 84.0,
                'name': 'Canada between 72°W and 66°W onshore and offshore - New Brunswick, Labrador, Nova Scotia, Nunavut, Quebec.',
            },
        }
    ]
    assert gm.CRS(4326).non_deprecated == []
    search = gm.crs_search(
        'British National Grid', authority='EPSG', kind='projected', limit=5
    )
    assert [item['crs'] for item in search] == ['EPSG:27700']
    assert (
        gm.crs_search(
            'OSGB36 / British National Grid',
            authority='EPSG',
            kind='projected',
            approximate=False,
        )[0]['crs']
        == 'EPSG:27700'
    )
    with pytest.raises(ValueError, match='CRS search name'):
        gm.crs_search('')
    with pytest.raises(ValueError, match='CRS search limit'):
        gm.crs_search('WGS', limit=0)
    with pytest.raises(ValueError, match='CRS search limit'):
        gm.crs_search('WGS', limit=-1)
    with pytest.raises(ValueError, match='CRS search limit'):
        gm.crs_search('WGS', limit=1001)
    datum = cast('dict[str, object]', geographic['datum'])
    assert datum['name'] == 'World Geodetic System 1984 ensemble'
    assert datum['authority'] == 'EPSG'
    assert datum['code'] == '6326'
    assert datum['kind'] == 'datum_ensemble'
    assert datum['ensemble_accuracy'] == pytest.approx(2.0)
    datum_members = cast('list[dict[str, object]]', datum['ensemble_members'])
    assert any(member['code'] == '1154' for member in datum_members)
    ellipsoid = cast('dict[str, object]', geographic['ellipsoid'])
    assert ellipsoid['name'] == 'WGS 84'
    assert ellipsoid['semi_major_metre'] == pytest.approx(6378137.0)
    assert ellipsoid['inverse_flattening'] == pytest.approx(298.257223563)
    prime_meridian = cast('dict[str, object]', geographic['prime_meridian'])
    assert prime_meridian['name'] == 'Greenwich'
    assert prime_meridian['longitude'] == 0.0
    assert geographic['celestial_body'] == 'Earth'
    assert geographic['has_point_motion_operation'] is False
    dynamic_info = gm.crs_info(7789)
    dynamic_datum = cast('dict[str, object]', dynamic_info['datum'])
    assert dynamic_datum['kind'] == 'dynamic_geodetic_reference_frame'
    assert dynamic_datum['frame_reference_epoch'] == pytest.approx(2010.0)
    assert geographic['coordinate_system'] == 'ellipsoidal'
    assert geographic['axis_order'] == ['lat', 'lon']
    assert gm.CRS(4326).kind == 'geographic_2d'
    assert gm.CRS(4979).kind == 'geographic_3d'
    assert gm.CRS(4326).is_geographic
    assert not gm.CRS(4326).is_projected
    assert gm.CRS(4326).axis_order == ['lat', 'lon']
    geographic_cf = cast('dict[str, object]', gm.CRS(4326).to_cf())
    assert geographic_cf['grid_mapping_name'] == 'latitude_longitude'
    assert geographic_cf['semi_major_axis'] == pytest.approx(6378137.0)
    assert geographic_cf['inverse_flattening'] == pytest.approx(298.257223563)
    assert geographic_cf['reference_ellipsoid_name'] == 'WGS 84'
    assert geographic_cf['prime_meridian_name'] == 'Greenwich'
    assert geographic_cf['geographic_crs_name'] == 'WGS 84'
    assert cast('str', geographic_cf['crs_wkt']).startswith('GEOGCRS["WGS 84"')
    assert gm.CRS(geographic_cf).canonical == 'EPSG:4326'
    assert gm.Point(21.0, 52.0, crs=geographic_cf).crs == 'EPSG:4326'
    geographic_cf_without_wkt = dict(geographic_cf)
    del geographic_cf_without_wkt['crs_wkt']
    assert gm.CRS(geographic_cf_without_wkt).to_authority() == ('OGC', 'CRS84')
    assert gm.Point(21.0, 52.0, crs=geographic_cf_without_wkt).crs == 'OGC:CRS84'
    assert geographic['axes'] == [
        {
            'name': 'Geodetic latitude',
            'abbreviation': 'Lat',
            'direction': 'north',
            'unit_name': 'degree',
            'unit_conversion_factor': pytest.approx(math.pi / 180.0),
        },
        {
            'name': 'Geodetic longitude',
            'abbreviation': 'Lon',
            'direction': 'east',
            'unit_name': 'degree',
            'unit_conversion_factor': pytest.approx(math.pi / 180.0),
        },
    ]
    assert geographic['is_geographic'] is True
    assert geographic['is_projected'] is False
    assert geographic['area_of_use'] == {
        'west': -180.0,
        'south': -90.0,
        'east': 180.0,
        'north': 90.0,
        'name': 'World.',
    }
    assert projected['crs'] == 'EPSG:27700'
    assert projected['name'] == 'OSGB36 / British National Grid'
    assert projected['kind'] == 'projected'
    assert projected['is_derived'] is True
    assert cast('dict[str, object]', projected['source_crs'])['crs'] == 'EPSG:4277'
    assert projected['target_crs'] is None
    assert cast('dict[str, object]', projected['geodetic_crs'])['crs'] == 'EPSG:4277'
    assert cast('dict[str, object]', projected['horizontal_datum'])['code'] == '6277'
    projected_operation = cast('dict[str, object]', projected['coordinate_operation'])
    assert projected_operation['name'] == 'British National Grid'
    projected_method = cast('dict[str, object]', projected_operation['method'])
    assert projected_method['name'] == 'Transverse Mercator'
    projected_parameters = cast(
        'list[dict[str, object]]', projected_operation['parameters']
    )
    assert [parameter['code'] for parameter in projected_parameters] == [
        '8801',
        '8802',
        '8805',
        '8806',
        '8807',
    ]
    false_easting = next(
        parameter
        for parameter in projected_parameters
        if parameter['name'] == 'False easting'
    )
    assert false_easting['value'] == pytest.approx(400000.0)
    assert false_easting['unit_name'] == 'metre'
    assert false_easting['unit_code'] == '9001'
    assert false_easting['unit_category'] == 'linear'
    scale = next(
        parameter
        for parameter in projected_parameters
        if parameter['name'] == 'Scale factor at natural origin'
    )
    assert scale['value'] == pytest.approx(0.9996012717)
    assert scale['unit_name'] == 'unity'
    assert scale['unit_category'] == 'scale'
    assert projected['coordinate_system'] == 'cartesian'
    assert projected['axis_order'] == ['x', 'y']
    assert gm.CRS('epsg:27700').kind == 'projected'
    assert gm.CRS('epsg:27700').is_projected
    assert not gm.CRS('epsg:27700').is_geographic
    assert gm.CRS('epsg:27700').axis_order == ['x', 'y']
    utm_cf = cast('dict[str, object]', gm.CRS(32634).to_cf())
    assert utm_cf['grid_mapping_name'] == 'transverse_mercator'
    assert utm_cf['projected_crs_name'] == 'WGS 84 / UTM zone 34N'
    assert utm_cf['geographic_crs_name'] == 'WGS 84'
    assert utm_cf['longitude_of_central_meridian'] == pytest.approx(21.0)
    assert utm_cf['scale_factor_at_central_meridian'] == pytest.approx(0.9996)
    assert utm_cf['false_easting'] == pytest.approx(500000.0)
    assert utm_cf['false_northing'] == pytest.approx(0.0)
    assert gm.CRS(utm_cf).to_epsg() == 32634
    utm_cf_without_wkt = dict(utm_cf)
    del utm_cf_without_wkt['crs_wkt']
    assert gm.CRS(utm_cf_without_wkt).to_epsg() == 32634
    assert gm.CRS(utm_cf_without_wkt).canonical == 'EPSG:32634'
    assert gm.Point(500000.0, 5750000.0, crs=utm_cf_without_wkt).crs == 'EPSG:32634'
    assert gm.Point(500000.0, 5750000.0).set_crs(utm_cf_without_wkt).crs == 'EPSG:32634'
    assert gm.crs_transform(
        utm_cf_without_wkt, 4326, 500000.0, 5750000.0
    ) == pytest.approx(gm.crs_transform(32634, 4326, 500000.0, 5750000.0))
    assert gm.Point(500000.0, 5750000.0, crs=utm_cf_without_wkt).to_crs(
        4326
    ).coords.to_nested() == pytest.approx(
        gm.Point(500000.0, 5750000.0, crs=32634).to_crs(4326).coords.to_nested()
    )
    laea_cf = cast('dict[str, object]', gm.CRS(3035).to_cf())
    assert laea_cf['grid_mapping_name'] == 'lambert_azimuthal_equal_area'
    assert laea_cf['latitude_of_projection_origin'] == pytest.approx(52.0)
    assert laea_cf['longitude_of_projection_origin'] == pytest.approx(10.0)
    laea_cf_without_wkt = dict(laea_cf)
    del laea_cf_without_wkt['crs_wkt']
    assert gm.CRS(laea_cf_without_wkt).canonical == 'IGNF:ETRS89LAEA'
    assert gm.crs_transform(4326, laea_cf_without_wkt, 10.0, 52.0) == pytest.approx(
        gm.crs_transform(4326, 3035, 10.0, 52.0)
    )
    lcc_cf = cast('dict[str, object]', gm.CRS(3034).to_cf())
    assert lcc_cf['grid_mapping_name'] == 'lambert_conformal_conic'
    assert lcc_cf['standard_parallel'] == pytest.approx([35.0, 65.0])
    lcc_cf_without_wkt = dict(lcc_cf)
    del lcc_cf_without_wkt['crs_wkt']
    assert gm.CRS(lcc_cf_without_wkt).canonical == 'IGNF:ETRS89LCC'
    assert gm.crs_transform(4326, lcc_cf_without_wkt, 10.0, 52.0) == pytest.approx(
        gm.crs_transform(4326, 3034, 10.0, 52.0)
    )
    mercator_cf = cast('dict[str, object]', gm.CRS(3395).to_cf())
    assert mercator_cf['grid_mapping_name'] == 'mercator'
    assert mercator_cf['scale_factor_at_projection_origin'] == pytest.approx(1.0)
    mercator_cf_without_wkt = dict(mercator_cf)
    del mercator_cf_without_wkt['crs_wkt']
    assert gm.CRS(mercator_cf_without_wkt).to_epsg() == 3395
    polar_cf = cast('dict[str, object]', gm.CRS(3413).to_cf())
    assert polar_cf['grid_mapping_name'] == 'polar_stereographic'
    assert polar_cf['standard_parallel'] == pytest.approx(70.0)
    assert polar_cf['straight_vertical_longitude_from_pole'] == pytest.approx(-45.0)
    polar_cf_without_wkt = dict(polar_cf)
    del polar_cf_without_wkt['crs_wkt']
    assert gm.CRS(polar_cf_without_wkt).to_epsg() == 3413
    cylindrical_equal_area_cf = cast('dict[str, object]', gm.CRS(6933).to_cf())
    assert (
        cylindrical_equal_area_cf['grid_mapping_name']
        == 'lambert_cylindrical_equal_area'
    )
    assert cylindrical_equal_area_cf['standard_parallel'] == pytest.approx(30.0)
    cylindrical_equal_area_cf_without_wkt = dict(cylindrical_equal_area_cf)
    del cylindrical_equal_area_cf_without_wkt['crs_wkt']
    assert gm.CRS(cylindrical_equal_area_cf_without_wkt).to_epsg() == 6933
    assert 'grid_mapping_name' not in gm.CRS(3857).to_cf()
    spatial_ref_cf = dict(utm_cf)
    spatial_ref_cf['spatial_ref'] = spatial_ref_cf.pop('crs_wkt')
    assert gm.CRS(spatial_ref_cf).to_epsg() == 32634
    with pytest.raises(
        ValueError, match='crs_wkt CRS dictionary value must be non-empty'
    ):
        gm.CRS({'crs_wkt': ''})
    with pytest.raises(ValueError, match='unsupported CF grid_mapping_name'):
        gm.CRS({'grid_mapping_name': 'rotated_latitude_longitude'})
    with pytest.raises(
        ValueError, match='CF CRS dictionary requires earth_radius or semi_major_axis'
    ):
        gm.CRS({'grid_mapping_name': 'latitude_longitude'})
    with pytest.raises(TypeError, match='semi_major_axis'):
        gm.CRS({
            'grid_mapping_name': 'latitude_longitude',
            'semi_major_axis': 10**1000,
            'inverse_flattening': 298.257223563,
        })
    lcc_bad_parallel = dict(lcc_cf_without_wkt)
    lcc_bad_parallel['standard_parallel'] = [10**1000]
    with pytest.raises(TypeError, match='standard_parallel'):
        gm.CRS(lcc_bad_parallel)
    projected_axes = cast('list[dict[str, object]]', projected['axes'])
    assert projected_axes[:2] == [
        {
            'name': 'Easting',
            'abbreviation': 'E',
            'direction': 'east',
            'unit_name': 'metre',
            'unit_conversion_factor': 1.0,
        },
        {
            'name': 'Northing',
            'abbreviation': 'N',
            'direction': 'north',
            'unit_name': 'metre',
            'unit_conversion_factor': 1.0,
        },
    ]

    assert projected['is_geographic'] is False
    assert projected['is_projected'] is True
    assert projected['is_vertical'] is False
    assert projected['is_geocentric'] is False
    assert projected['is_compound'] is False
    assert projected['is_engineering'] is False
    assert projected['is_bound'] is False
    assert projected['deprecated'] is False
    assert 'is_deprecated' not in projected
    assert gm.CRS('epsg:27700').is_projected
    assert not gm.CRS('epsg:27700').is_geographic
    projected_area = cast('dict[str, object]', projected['area_of_use'])
    assert projected_area['west'] == pytest.approx(-9.01)
    assert projected_area['east'] == pytest.approx(2.01)
    compound = gm.crs_info(7405)
    assert compound['kind'] == 'compound'
    assert compound['is_compound'] is True
    assert compound['is_geographic'] is False
    assert compound['is_projected'] is True
    assert compound['is_vertical'] is True
    assert gm.CRS(7405).is_compound
    assert gm.CRS(7405).is_projected
    assert gm.CRS(7405).is_vertical
    assert not gm.CRS(7405).is_geographic
    geographic_compound = gm.crs_info(5498)
    assert geographic_compound['is_compound'] is True
    assert geographic_compound['is_geographic'] is True
    assert geographic_compound['is_projected'] is False
    assert geographic_compound['is_vertical'] is True
    assert gm.CRS(5498).is_geographic
    assert not gm.CRS(5498).is_projected
    assert gm.CRS(5498).is_vertical
    assert gm.CRS(4978).is_geocentric
    assert not gm.CRS(4326).is_geocentric
    assert gm.CRS(3857).is_derived
    assert not gm.CRS(4326).is_derived
    assert gm.CRS(2037).is_deprecated
    assert not gm.CRS(4326).is_deprecated
    assert gm.CRS(5800).is_engineering
    assert not gm.CRS(4326).is_bound
    compound_parts = cast('list[dict[str, object]]', compound['sub_crs'])
    assert [part['crs'] for part in compound_parts] == ['EPSG:27700', 'EPSG:5701']
    assert cast('dict[str, object]', compound['geodetic_crs'])['crs'] == 'EPSG:4277'


def test_compound_crs_axes_match_pyproj_components() -> None:
    pyproj = pytest.importorskip('pyproj')
    for code in (7405, 7415, 5498):
        crs = gm.CRS(code)
        expected = [
            {
                'name': axis.name,
                'abbreviation': axis.abbrev,
                'direction': axis.direction,
                'unit_name': axis.unit_name,
                'unit_conversion_factor': pytest.approx(axis.unit_conversion_factor),
            }
            for axis in pyproj.CRS.from_epsg(code).axis_info
        ]
        assert crs.axes == expected
        assert crs.axis_order == [
            'lat'
            if axis.direction in {'north', 'south'} and axis.unit_name == 'degree'
            else 'lon'
            if axis.direction in {'east', 'west'} and axis.unit_name == 'degree'
            else 'height'
            if axis.direction in {'up', 'down'}
            else 'x'
            if axis.direction in {'east', 'west'}
            else 'y'
            if axis.direction in {'north', 'south'}
            else 'other'
            for axis in pyproj.CRS.from_epsg(code).axis_info
        ]


def test_cf_ellipsoid_descriptors_are_coherent() -> None:
    def ellipsoid_axes(mapping: dict[str, object]) -> tuple[float, float]:
        ellipsoid = cast('dict[str, object]', gm.CRS(mapping).info['ellipsoid'])
        return (
            cast('float', ellipsoid['semi_major_metre']),
            cast('float', ellipsoid['semi_minor_metre']),
        )

    assert ellipsoid_axes({
        'grid_mapping_name': 'latitude_longitude',
        'earth_radius': 6_371_000.0,
    }) == (6_371_000.0, 6_371_000.0)
    assert ellipsoid_axes({
        'grid_mapping_name': 'latitude_longitude',
        'semi_major_axis': 6_371_000.0,
        'inverse_flattening': 0.0,
    }) == (6_371_000.0, 6_371_000.0)
    assert (
        gm.CRS({
            'grid_mapping_name': 'latitude_longitude',
            'semi_major_axis': 6_378_137.0,
            'inverse_flattening': 298.257_223_563,
        }).canonical
        == 'OGC:CRS84'
    )
    with pytest.raises(ValueError, match='earth_radius cannot be combined'):
        gm.CRS({
            'grid_mapping_name': 'latitude_longitude',
            'earth_radius': 6_371_000.0,
            'semi_major_axis': 6_378_137.0,
            'inverse_flattening': 298.257_223_563,
        })
    assert ellipsoid_axes({
        'grid_mapping_name': 'latitude_longitude',
        'semi_major_axis': 6_378_137.0,
        'semi_minor_axis': 6_356_752.314_245_179,
        'inverse_flattening': 298.257_223_563,
    }) == pytest.approx((6_378_137.0, 6_356_752.314_245_179))
    with pytest.raises(ValueError, match='contradictory ellipsoid descriptors'):
        gm.CRS({
            'grid_mapping_name': 'latitude_longitude',
            'semi_major_axis': 6_378_137.0,
            'semi_minor_axis': 6_356_000.0,
            'inverse_flattening': 298.257_223_563,
        })


def _assert_crs_namespace_input_objects_and_serialization() -> None:
    projjson = json.loads(gm.CRS(4326).to_projjson())
    wkt = _WKT_4326
    assert wkt.startswith('GEOGCRS["WGS 84"')
    assert gm.CRS(4326) == wkt
    assert gm.CRS(('EPSG', '4326')).canonical == 'EPSG:4326'
    assert gm.CRS(('EPSG', 4326)).canonical == 'EPSG:4326'
    assert gm.CRS(['EPSG', '4326']).canonical == 'EPSG:4326'
    assert gm.CRS(WktCrsObject()).canonical == 'EPSG:4326'
    assert gm.CRS(AuthorityCrsObject()).canonical == 'EPSG:4326'
    authority_preferred = AuthorityPreferredCrsObject()
    assert gm.CRS(authority_preferred).canonical == 'EPSG:4326'
    assert authority_preferred.calls == ['authority']
    assert gm.CRS(AuthorityFallbackWktCrsObject()).canonical == 'EPSG:4326'
    assert gm.CRS(cast('Any', ProjjsonCrsObject())).to_epsg() == 4326
    assert gm.CRS(EpsgCrsObject()).canonical == 'EPSG:4326'
    assert gm.CRS(CrsHolderObject()).canonical == 'EPSG:4326'
    assert gm.CRS(SrsHolderObject()).canonical == 'EPSG:4326'
    assert gm.CRS(NestedCrsHolderObject()).canonical == 'EPSG:4326'
    assert gm.Point(-1.0, 50.0, crs=('EPSG', '4326')).crs == 'EPSG:4326'
    assert gm.Point(-1.0, 50.0, crs=WktCrsObject()).crs == 'EPSG:4326'
    assert gm.Point(-1.0, 50.0, crs=cast('Any', CrsHolderObject())).crs == 'EPSG:4326'
    assert gm.crs_transform(WktCrsObject(), 3857, -1.0, 50.0) == pytest.approx(
        tuple(gm.Point(-1.0, 50.0, crs=4326).to_crs(3857).coords.to_nested())
    )
    with pytest.raises(ValueError, match='CRS authority tuple'):
        gm.CRS(('EPSG',))
    with pytest.raises(ValueError, match='authority must be non-empty'):
        gm.CRS(('', '4326'))
    with pytest.raises(ValueError, match='code must be non-negative'):
        gm.CRS(('EPSG', -4326))

    class EmptyAuthorityCrsObject:
        def to_authority(self) -> None:
            return None

    with pytest.raises(ValueError, match='to_authority'):
        gm.CRS(EmptyAuthorityCrsObject())
    with pytest.raises(TypeError, match='to_epsg\\(\\) must return'):
        gm.CRS(InvalidEpsgCrsObject())
    with pytest.raises(ValueError, match='to_epsg\\(\\) returned None'):
        gm.CRS(EmptyEpsgCrsObject())
    assert gm.CRS(4326).same_as('OGC:CRS84', mode='ignore_axis_order')
    assert gm.CRS(4326) != 'OGC:CRS84'
    with pytest.raises(ValueError, match='comparison mode'):
        gm.CRS(4326).same_as(4326, mode=cast('Any', 'bogus'))
    assert (
        gm
        .CRS(4326)
        .to_wkt(version='WKT1_GDAL', output_axis='no')
        .startswith('GEOGCS["WGS 84"')
    )
    pretty_wkt = gm.CRS(4326).to_wkt(pretty=True, indentation_width=2)
    assert '\n  ENSEMBLE[' in pretty_wkt
    proj_string = gm.CRS(3857).to_proj()
    assert '+proj=' in proj_string
    assert '+datum=WGS84' in gm.CRS(4326).to_proj(version=4)
    with pytest.raises(ValueError, match='unknown WKT version'):
        gm.CRS(4326).to_wkt(version='wkt3')
    with pytest.raises(ValueError, match='WKT output_axis'):
        gm.CRS(4326).to_wkt(output_axis='maybe')
    with pytest.raises(ValueError, match='WKT indentation_width'):
        gm.CRS(4326).to_wkt(indentation_width=-1)
    with pytest.raises(ValueError, match='PROJ string version'):
        gm.CRS(4326).to_proj(version=6)
    with pytest.raises(ValueError, match='PROJ string version'):
        gm.CRS(4326).to_proj(version=-1)
    with pytest.raises(ValueError, match='PROJ indentation_width'):
        gm.CRS(4326).to_proj(indentation_width=-1)
    with pytest.raises(ValueError, match='PROJ max_line_length'):
        gm.CRS(4326).to_proj(max_line_length=-1)
    assert projjson['type'] == 'GeographicCRS'
    assert projjson['name'] == 'WGS 84'
    assert gm.CRS(projjson).canonical == 'EPSG:4326'
    assert gm.crs_info(projjson)['code'] == '4326'
    assert gm.CRS(projjson) == 4326
    assert gm.Point(-1.0, 50.0, crs=projjson).crs == 'EPSG:4326'
    assert json.loads(gm.CRS(projjson).to_projjson()) == projjson
    dict_transform = gm.crs_transform(projjson, 3857, -1.0, 50.0)
    assert dict_transform == pytest.approx(
        tuple(gm.Point(-1.0, 50.0, crs=4326).to_crs(3857).coords.to_nested())
    )
    pretty_projjson = gm.CRS(4326).to_projjson(pretty=True, indentation_width=4)
    assert pretty_projjson.startswith('{\n    "$schema"')
    assert json.loads(pretty_projjson) == projjson
    with pytest.raises(ValueError, match='PROJJSON indentation_width'):
        gm.CRS(4326).to_projjson(indentation_width=0)
    with pytest.raises(ValueError, match='PROJJSON indentation_width'):
        gm.CRS(4326).to_projjson(indentation_width=10**30)
    with pytest.raises(ValueError, match='CRS authority tuple'):
        gm.CRS(['EPSG:4326'])
    identified = gm.CRS(wkt).identify(authority='EPSG')
    assert identified[0] == {
        'crs': 'EPSG:4326',
        'name': 'WGS 84',
        'authority': 'EPSG',
        'code': '4326',
        'confidence': 100,
    }
    assert gm.CRS(wkt).to_authority(authority='EPSG') == ('EPSG', '4326')
    assert gm.CRS(wkt).identify(authority='EPSG') == [
        {
            'crs': 'EPSG:4326',
            'name': 'WGS 84',
            'authority': 'EPSG',
            'code': '4326',
            'confidence': 100,
        }
    ]
    assert gm.CRS('OGC:CRS84').identify() == [
        {
            'crs': 'OGC:CRS84',
            'name': 'WGS 84 (CRS84)',
            'authority': 'OGC',
            'code': 'CRS84',
            'confidence': 100,
        }
    ]
    assert gm.CRS(projjson).to_epsg() == 4326
    assert gm.CRS(4979).to_2d() == 'EPSG:4326'
    assert gm.CRS(4326).to_3d() == 'EPSG:4979'
    projected_3d = gm.CRS(3857).to_3d()
    assert gm.CRS(projected_3d).same_as(gm.CRS('EPSG:3857').to_3d(), mode='exact')
    assert gm.CRS(projected_3d).to_authority() is None
    assert all(
        candidate['confidence'] < 70 for candidate in gm.CRS(projected_3d).identify()
    )
    with pytest.raises(ValueError, match='min_confidence'):
        gm.CRS(4326).to_authority(min_confidence=101)
    with pytest.raises(ValueError, match='min_confidence must be between 0 and 100'):
        gm.CRS(4326).to_epsg(min_confidence=-1)
    for out_of_range in (2**200, -(2**200)):
        with pytest.raises(
            ValueError, match='min_confidence must be between 0 and 100'
        ):
            gm.CRS(4326).to_authority(min_confidence=out_of_range)
    operation = gm.CRS(4326).operation(3857)
    assert operation['source'] == 'EPSG:4326'
    assert operation['target'] == 'EPSG:3857'
    assert operation['source_epoch'] is None
    assert operation['target_epoch'] is None
    # Accuracy is whatever PROJ reports, and PROJ is free to change it: 9.6.2
    # gave 0.0 for this pipeline, 9.8.1 gives None (unknown). Pin our own
    # contract -- the key is present and typed ``float | None`` -- not PROJ's value.
    accuracy = operation['accuracy']
    assert accuracy is None or isinstance(accuracy, float)
    assert operation['has_inverse'] is True
    assert operation['has_ballpark_transformation'] is False
    assert operation['requires_coordinate_epoch'] is False
    assert 'Popular Visualisation Pseudo-Mercator' in cast(
        'str', operation['description']
    )
    assert operation['method'] is None
    assert operation['parameters'] == []
    assert 'proj=webmerc' in cast('str', operation['definition'])
    operation_area = cast('dict[str, object]', operation['area_of_use'])
    # The area name is EPSG database prose and changes between releases (EPSG
    # v12.029 renamed this one 'World' -> 'World.'). Pin our own contract --
    # the bounds we surface -- not the third-party string.
    assert str(operation_area['name']).startswith('World')
    assert (
        operation_area['west'],
        operation_area['south'],
        operation_area['east'],
        operation_area['north'],
    ) == (-180.0, -90.0, 180.0, 90.0)
    assert operation['grids'] == []
    assert [
        step['method']['name'] for step in operation['steps'] if step['method']
    ] == ['Axis Order Reversal (2D)', 'Popular Visualisation Pseudo Mercator']


def test_crs_namespace_exposes_proj_authority_metadata() -> None:
    from tests.test_crs_metadata_operations import (
        _assert_crs_namespace_operations_geodesic_and_transforms,
    )

    _assert_crs_namespace_database_catalog_and_crs_info()
    _assert_crs_namespace_input_objects_and_serialization()
    _assert_crs_namespace_operations_geodesic_and_transforms()


def test_crs_units_and_celestial_bodies_cache_identity() -> None:
    """Catalog list reads are process/thread-cached; clear resets, warm is identical."""
    gm.crs_clear_cache()
    cold_units = gm.crs_units('EPSG', category='linear')
    warm_units = gm.crs_units('EPSG', category='linear')
    assert cold_units == warm_units
    assert any(u.get('code') == '9001' for u in warm_units)
    # Returned list is a shallow copy: mutating it must not poison the cache.
    warm_units[0] = {'poison': True}
    assert gm.crs_units('EPSG', category='linear') == cold_units

    cold_bodies = gm.crs_celestial_bodies()
    warm_bodies = gm.crs_celestial_bodies()
    assert cold_bodies == warm_bodies
    assert {'authority': 'PROJ', 'name': 'Earth'} in warm_bodies

    # After clear, a fresh PROJ read still matches the prior snapshot.
    gm.crs_clear_cache()
    assert gm.crs_units('EPSG', category='linear') == cold_units
    assert gm.crs_celestial_bodies() == warm_bodies

    info = gm.crs_cache_info()
    names = {bucket['name'] for bucket in info['buckets']}
    assert 'crs_units' in names
    assert 'crs_celestial_bodies' in names


def test_crs_info_dict_cache_isolation() -> None:
    """crs_info returns isolation-safe dicts; nested mutation cannot poison the cache."""
    gm.crs_clear_cache()
    cold = gm.crs_info(4326)
    warm = gm.crs_info(4326)
    assert warm == cold
    assert warm['name'] == 'WGS 84'
    assert warm['axes'] == cold['axes']

    # Top-level key replacement must not poison subsequent calls.
    warm['name'] = 'POISON'
    assert gm.crs_info(4326)['name'] == cold['name']

    # Top-level list slot replacement must not poison.
    warm2 = gm.crs_info(4326)
    warm2['axes'] = [{'poison': True}]
    assert gm.crs_info(4326)['axes'] == cold['axes']

    # Nested dict leaves are frozen (MappingProxyType) — assignment raises and
    # cannot poison the cache even if a caller catches the error.
    warm3 = gm.crs_info(4326)
    nested_axis = warm3['axes'][0]
    try:
        nested_axis['name'] = 'POISON'  # type: ignore[index]
    except TypeError:
        pass
    else:
        # If nested were mutable, require isolation still holds.
        pass
    assert gm.crs_info(4326)['axes'] == cold['axes']

    # Nested list-like containers under a nested dict (ensemble members) must
    # not accept in-place append that would poison a shared list.
    warm4 = gm.crs_info(4326)
    datum = cast('dict[str, object]', warm4['datum'])
    members = datum['ensemble_members']
    before_len = len(cast('list[object]', members))
    try:
        cast('list[object]', members).append({'poison': True})
    except (TypeError, AttributeError):
        pass
    after = gm.crs_info(4326)
    after_datum = cast('dict[str, object]', after['datum'])
    assert len(cast('list[object]', after_datum['ensemble_members'])) == before_len
    assert after == cold

    # CRS.info shares the same cache semantics.
    assert gm.CRS(4326).info == cold

    # Generation bump / clear invalidates Python-side materialization.
    gm.crs_clear_cache()
    assert gm.crs_info(4326) == cold


def test_crs_operations_dict_cache_isolation() -> None:
    """CRS.operations / .operation return isolation-safe containers."""
    gm.crs_clear_cache()
    cold_ops = gm.CRS(4326).operations(3857)
    warm_ops = gm.CRS(4326).operations(3857)
    assert warm_ops == cold_ops
    assert len(warm_ops) >= 1

    # List-level mutation must not poison.
    warm_ops[0] = {'poison': True}
    assert gm.CRS(4326).operations(3857) == cold_ops

    # Nested dict freeze on an operation entry.
    warm2 = gm.CRS(4326).operations(3857)
    try:
        warm2[0]['name'] = 'POISON'
    except TypeError:
        pass
    assert gm.CRS(4326).operations(3857) == cold_ops

    cold_op = gm.CRS(4326).operation(3857)
    warm_op = gm.CRS(4326).operation(3857)
    assert warm_op == cold_op
    warm_op['name'] = 'POISON'
    assert gm.CRS(4326).operation(3857) == cold_op
    try:
        steps = warm_op['steps']
        if steps:
            steps[0]['name'] = 'POISON'  # type: ignore[index]
    except TypeError:
        pass
    assert gm.CRS(4326).operation(3857) == cold_op

    gm.crs_clear_cache()
    assert gm.CRS(4326).operations(3857) == cold_ops
    assert gm.CRS(4326).operation(3857) == cold_op


def _bundled_proj_db_src():
    """Locate the generated proj.db matching the bundled runtime."""
    import os
    import sqlite3
    from pathlib import Path

    target_dir = Path(
        os.environ.get(
            'CARGO_TARGET_DIR', Path(__file__).resolve().parents[2] / 'target'
        )
    ).resolve()
    engine = gm.crs_engine()
    expected = engine['database_metadata']
    runtime_paths = {
        Path(path).resolve() / 'proj.db'
        for path in engine['search_path'].split(os.pathsep)
    }
    candidates = [path for path in runtime_paths if path.is_file()]
    candidates.extend(
        path
        for path in target_dir.glob('**/proj-sys-*/out/**/proj.db')
        if path.resolve() not in runtime_paths
    )
    identity_keys = (
        'PROJ.VERSION',
        'DATABASE.LAYOUT.VERSION.MAJOR',
        'DATABASE.LAYOUT.VERSION.MINOR',
    )
    for candidate in candidates:
        con = sqlite3.connect(candidate)
        try:
            metadata = dict(con.execute('SELECT key, value FROM metadata'))
        finally:
            con.close()
        if all(metadata.get(key) == expected[key] for key in identity_keys):
            return candidate
    pytest.fail(
        f'no generated proj-sys proj.db under {target_dir} matches bundled '
        f'PROJ {engine["version"]} metadata; build gometry before this test'
    )


def _copy_proj_db_with_4326_name(src, dest_dir, name: str):
    import shutil
    import sqlite3
    from pathlib import Path

    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    db = dest_dir / 'proj.db'
    shutil.copy(src, db)
    con = sqlite3.connect(db)
    con.execute(
        "UPDATE geodetic_crs SET name=? WHERE code=4326 AND auth_name='EPSG'",
        (name,),
    )
    con.commit()
    con.close()
    return dest_dir


def test_crs_receiver_info_invalidates_on_configure_only() -> None:
    """``crs_configure`` alone (no ``crs_clear_cache``) must re-resolve receivers."""
    import shutil
    import tempfile

    src = _bundled_proj_db_src()
    with tempfile.TemporaryDirectory() as tmp:
        path = _copy_proj_db_with_4326_name(src, tmp, 'STALE_CONFIGURE_ONLY')
        crs = gm.CRS(4326)
        geom_crs = gm.Point(1.0, 2.0, crs=4326).crs
        assert crs.info['name'] == 'WGS 84'
        try:
            gm.crs_configure(search_paths=str(path))
            # Intentionally no crs_clear_cache — configure must bump generation.
            assert gm.crs_info(4326)['name'] == 'STALE_CONFIGURE_ONLY'
            assert crs.info['name'] == 'STALE_CONFIGURE_ONLY'
            assert (
                geom_crs is not None and geom_crs.info['name'] == 'STALE_CONFIGURE_ONLY'
            )
        finally:
            gm.crs_reset()
            gm.crs_clear_cache()
        # Windows refuses to remove an open database, unlike Unix. This proves
        # runtime reset releases PROJ's current-thread database handle.
        shutil.rmtree(path)
    assert gm.crs_info(4326)['name'] == 'WGS 84'


def test_crs_receiver_info_invalidates_on_clear_only() -> None:
    """``crs_clear_cache`` alone must re-resolve receivers after the db mutates."""
    import sqlite3
    import tempfile
    from pathlib import Path

    src = _bundled_proj_db_src()
    with tempfile.TemporaryDirectory() as tmp:
        path = _copy_proj_db_with_4326_name(src, tmp, 'STALE_CLEAR_A')
        try:
            gm.crs_configure(search_paths=str(path))
            crs = gm.CRS(4326)
            assert crs.info['name'] == 'STALE_CLEAR_A'
            # Mutate the live proj.db under the configured search path.
            con = sqlite3.connect(Path(path) / 'proj.db')
            con.execute(
                "UPDATE geodetic_crs SET name='STALE_CLEAR_B' "
                "WHERE code=4326 AND auth_name='EPSG'"
            )
            con.commit()
            con.close()
            # Clear only — no reconfigure. Generation bump must re-read db.
            gm.crs_clear_cache()
            assert gm.crs_info(4326)['name'] == 'STALE_CLEAR_B'
            assert crs.info['name'] == 'STALE_CLEAR_B'
        finally:
            gm.crs_reset()
            gm.crs_clear_cache()
    assert gm.crs_info(4326)['name'] == 'WGS 84'


def test_crs_receiver_info_invalidates_on_reconfigure() -> None:
    """A second ``crs_configure`` (path A → path B) must re-resolve receivers."""
    import tempfile
    from pathlib import Path

    src = _bundled_proj_db_src()
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        path_a = _copy_proj_db_with_4326_name(src, tmp_path / 'a', 'STALE_PATH_A')
        path_b = _copy_proj_db_with_4326_name(src, tmp_path / 'b', 'STALE_PATH_B')
        try:
            gm.crs_configure(search_paths=str(path_a))
            crs = gm.CRS(4326)
            assert crs.info['name'] == 'STALE_PATH_A'
            # Reconfigure only — no explicit clear between A and B.
            gm.crs_configure(search_paths=str(path_b))
            assert gm.crs_info(4326)['name'] == 'STALE_PATH_B'
            assert crs.info['name'] == 'STALE_PATH_B'
            assert crs.name == 'STALE_PATH_B'
        finally:
            gm.crs_reset()
            gm.crs_clear_cache()
    assert gm.crs_info(4326)['name'] == 'WGS 84'
