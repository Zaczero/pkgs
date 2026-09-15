"""Geometry epoch admission and public datum epoch metadata."""

from __future__ import annotations

import json
from typing import Any, cast

import gometry as gm
import pytest

_GEOMETRY_EPOCH = 2024.5

_GRS80 = {
    'name': 'GRS 1980',
    'semi_major_axis': 6378137,
    'inverse_flattening': 298.257222101,
}
_ELLIPSOIDAL_CS = {
    'subtype': 'ellipsoidal',
    'axis': [
        {
            'name': 'Geodetic latitude',
            'abbreviation': 'Lat',
            'direction': 'north',
            'unit': 'degree',
        },
        {
            'name': 'Geodetic longitude',
            'abbreviation': 'Lon',
            'direction': 'east',
            'unit': 'degree',
        },
    ],
}

_DYN_GEO = (
    'GEOGCRS["xyzzy-dyn-geo",'
    'DYNAMIC[FRAMEEPOCH[2010.0]],'
    'DATUM["Xyzzy Dynamic Geodetic",'
    'ELLIPSOID["GRS 1980",6378137,298.257222101,LENGTHUNIT["metre",1]]],'
    'PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]],'
    'CS[ellipsoidal,2],'
    'AXIS["geodetic latitude (Lat)",north,ORDER[1],ANGLEUNIT["degree",0.0174532925199433]],'
    'AXIS["geodetic longitude (Lon)",east,ORDER[2],ANGLEUNIT["degree",0.0174532925199433]]]'
)
_STAT_GEO = (
    'GEOGCRS["xyzzy-stat-geo",'
    'DATUM["Xyzzy Static Geodetic",'
    'ELLIPSOID["GRS 1980",6378137,298.257222101,LENGTHUNIT["metre",1]]],'
    'PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]],'
    'CS[ellipsoidal,2],'
    'AXIS["geodetic latitude (Lat)",north,ORDER[1],ANGLEUNIT["degree",0.0174532925199433]],'
    'AXIS["geodetic longitude (Lon)",east,ORDER[2],ANGLEUNIT["degree",0.0174532925199433]]]'
)
_DYN_VERT = (
    'VERTCRS["xyzzy-dyn-vert",'
    'DYNAMIC[FRAMEEPOCH[2005.0]],'
    'VDATUM["Xyzzy Dynamic Vertical"],'
    'CS[vertical,1],'
    'AXIS["gravity-related height (H)",up,LENGTHUNIT["metre",1]]]'
)
_STAT_VERT = (
    'VERTCRS["xyzzy-stat-vert",'
    'VDATUM["Xyzzy Static Vertical"],'
    'CS[vertical,1],'
    'AXIS["gravity-related height (H)",up,LENGTHUNIT["metre",1]]]'
)


def _dyn_geo_epoch(token: str) -> str:
    return (
        'GEOGCRS["xyzzy-neg-epoch",'
        f'DYNAMIC[FRAMEEPOCH[{token}]],'
        'DATUM["Xyzzy Neg Epoch",'
        'ELLIPSOID["GRS 1980",6378137,298.257222101,LENGTHUNIT["metre",1]]],'
        'PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]],'
        'CS[ellipsoidal,2],'
        'AXIS["geodetic latitude (Lat)",north,ORDER[1],ANGLEUNIT["degree",0.0174532925199433]],'
        'AXIS["geodetic longitude (Lon)",east,ORDER[2],ANGLEUNIT["degree",0.0174532925199433]]]'
    )


def _dyn_vert_epoch(token: str) -> str:
    return (
        'VERTCRS["xyzzy-neg-vert",'
        f'DYNAMIC[FRAMEEPOCH[{token}]],'
        'VDATUM["Xyzzy Neg Vertical"],'
        'CS[vertical,1],'
        'AXIS["gravity-related height (H)",up,LENGTHUNIT["metre",1]]]'
    )


def _geographic(name: str, datum: dict[str, Any]) -> dict[str, Any]:
    return {
        'type': 'GeographicCRS',
        'name': name,
        'datum': datum,
        'coordinate_system': _ELLIPSOIDAL_CS,
    }


def _static_geodetic(name: str) -> dict[str, Any]:
    return {
        'type': 'GeodeticReferenceFrame',
        'name': name,
        'ellipsoid': _GRS80,
    }


def _dynamic_geodetic(name: str, epoch: float) -> dict[str, Any]:
    return {
        'type': 'DynamicGeodeticReferenceFrame',
        'name': name,
        'frame_reference_epoch': epoch,
        'ellipsoid': _GRS80,
    }


def _bound_crs(source: dict[str, Any], target: dict[str, Any]) -> str:
    return json.dumps({
        'type': 'BoundCRS',
        'source_crs': source,
        'target_crs': target,
        'transformation': {
            'name': 'null',
            'method': {
                'name': 'Position Vector transformation (geog2D domain)',
                'id': {'authority': 'EPSG', 'code': 9606},
            },
            'parameters': [
                {
                    'name': 'X-axis translation',
                    'value': 0,
                    'unit': 'metre',
                    'id': {'authority': 'EPSG', 'code': 8605},
                },
                {
                    'name': 'Y-axis translation',
                    'value': 0,
                    'unit': 'metre',
                    'id': {'authority': 'EPSG', 'code': 8606},
                },
                {
                    'name': 'Z-axis translation',
                    'value': 0,
                    'unit': 'metre',
                    'id': {'authority': 'EPSG', 'code': 8607},
                },
                {
                    'name': 'X-axis rotation',
                    'value': 0,
                    'unit': {
                        'type': 'AngularUnit',
                        'name': 'arc-second',
                        'conversion_factor': 4.84813681109536e-06,
                    },
                    'id': {'authority': 'EPSG', 'code': 8608},
                },
                {
                    'name': 'Y-axis rotation',
                    'value': 0,
                    'unit': {
                        'type': 'AngularUnit',
                        'name': 'arc-second',
                        'conversion_factor': 4.84813681109536e-06,
                    },
                    'id': {'authority': 'EPSG', 'code': 8609},
                },
                {
                    'name': 'Z-axis rotation',
                    'value': 0,
                    'unit': {
                        'type': 'AngularUnit',
                        'name': 'arc-second',
                        'conversion_factor': 4.84813681109536e-06,
                    },
                    'id': {'authority': 'EPSG', 'code': 8610},
                },
                {
                    'name': 'Scale difference',
                    'value': 0,
                    'unit': {
                        'type': 'ScaleUnit',
                        'name': 'parts per million',
                        'conversion_factor': 1e-06,
                    },
                    'id': {'authority': 'EPSG', 'code': 8611},
                },
            ],
        },
    })


def _datum(crs: object) -> dict[str, Any]:
    info = gm.crs_info(crs)
    return cast('dict[str, Any]', info['datum'])


def _reject_epoch(crs: object) -> None:
    with pytest.raises(gm.CRSError) as caught:
        gm.Point(10.0, 50.0, crs=crs, epoch=_GEOMETRY_EPOCH)
    assert type(caught.value) is gm.CRSError


@pytest.mark.parametrize(
    ('builder', 'kind'),
    [
        (_dyn_geo_epoch, 'dynamic_geodetic_reference_frame'),
        (_dyn_vert_epoch, 'dynamic_vertical_reference_frame'),
    ],
)
@pytest.mark.parametrize(
    ('token', 'expected'),
    [
        ('-100', -100.0),
        ('-1', -1.0),
        ('-0.5', -0.5),
        ('0', 0.0),
        ('2010', 2010.0),
    ],
)
def test_public_frame_reference_epoch_retains_finite_years(
    builder: Any, kind: str, token: str, expected: float
) -> None:
    crs = builder(token)
    datum = _datum(crs)
    assert datum['kind'] == kind
    assert datum['frame_reference_epoch'] == expected
    assert 'admits_geometry_epoch' not in gm.crs_info(crs)
    point = gm.Point(10.0, 50.0, crs=crs, epoch=_GEOMETRY_EPOCH)
    assert point.epoch == _GEOMETRY_EPOCH
    assert point.epoch != datum['frame_reference_epoch']


def test_unnamed_dynamic_geographic_plus_static_vertical_compound_admits_epoch() -> (
    None
):
    crs = f'COMPOUNDCRS["xyzzy-comp-dyn-h",{_DYN_GEO},{_STAT_VERT}]'
    point = gm.Point(10.0, 50.0, crs=crs, epoch=_GEOMETRY_EPOCH)
    assert point.epoch == _GEOMETRY_EPOCH


def test_unnamed_static_geographic_plus_dynamic_vertical_compound_admits_epoch() -> (
    None
):
    crs = f'COMPOUNDCRS["xyzzy-comp-dyn-v",{_STAT_GEO},{_DYN_VERT}]'
    point = gm.Point(10.0, 50.0, crs=crs, epoch=_GEOMETRY_EPOCH)
    assert point.epoch == _GEOMETRY_EPOCH


def test_unnamed_static_compound_rejects_epoch() -> None:
    _reject_epoch(f'COMPOUNDCRS["xyzzy-comp-stat",{_STAT_GEO},{_STAT_VERT}]')


def test_bound_unnamed_dynamic_source_admits_epoch() -> None:
    crs = _bound_crs(
        _geographic(
            'xyzzy-dyn-geo', _dynamic_geodetic('Xyzzy Dynamic Geodetic', 2010.0)
        ),
        _geographic('xyzzy-stat-hub', _static_geodetic('Xyzzy Static Hub')),
    )
    point = gm.Point(10.0, 50.0, crs=crs, epoch=_GEOMETRY_EPOCH)
    assert point.epoch == _GEOMETRY_EPOCH


def test_bound_static_source_dynamic_hub_rejects_epoch() -> None:
    _reject_epoch(
        _bound_crs(
            _geographic('xyzzy-stat-geo', _static_geodetic('Xyzzy Static Geodetic')),
            _geographic(
                'xyzzy-dyn-hub', _dynamic_geodetic('Xyzzy Dynamic Hub', 2010.0)
            ),
        )
    )


def test_bound_wgs84_towgs84_admits_epoch() -> None:
    crs = '+proj=longlat +datum=WGS84 +towgs84=0,0,0 +type=crs'
    point = gm.Point(10.0, 50.0, crs=crs, epoch=_GEOMETRY_EPOCH)
    assert point.epoch == _GEOMETRY_EPOCH


def test_bound_grs80_towgs84_rejects_epoch() -> None:
    _reject_epoch('+proj=longlat +ellps=GRS80 +towgs84=0,0,0 +type=crs')


def test_etrs89_ensemble_rejects_epoch() -> None:
    _reject_epoch(4258)
