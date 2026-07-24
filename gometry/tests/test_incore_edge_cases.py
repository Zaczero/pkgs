"""Edge-case oracle suite for in-core CRS fast paths vs pyproj."""

from __future__ import annotations

import math

import gometry as gm
import numpy as np
import pytest

pyproj = pytest.importorskip('pyproj')
ABS_TOL_M = 1e-06
ABS_TOL_DEG = 1e-06
ROUND_TRIP_TOL_M = 1e-06
ROUND_TRIP_TOL_DEG = 1e-06
WGS84 = 'EPSG:4326'
WEB_MERCATOR = 'EPSG:3857'
WEB_MERCATOR_MAX_LAT = 85.0511287798066
UTM_LAT_MIN = -80.0
UTM_LAT_MAX = 84.0
GAUSS_KRUGER_GEO = 'EPSG:4314'
GAUSS_KRUGER_PROJ = 'EPSG:31467'
NAD83 = 'EPSG:4269'
NAD83_SPCS_TM = 'EPSG:32145'
RGF93_GEO = 'EPSG:4171'
LAMBERT_93 = 'EPSG:2154'
NAD83_2011 = 'EPSG:6318'
NAD83_2011_SPCS_LCC = 'EPSG:6577'
ANTARCTIC_STEREO = 'EPSG:3031'
NSIDC_NORTH = 'EPSG:3413'
ARCTIC_STEREO = 'EPSG:3995'
WGS84_ARCTIC_A = 'EPSG:5041'
WGS84_ANTARCTIC_A = 'EPSG:5042'


def _pyproj_fwd(src: str, tgt: str, x: float, y: float) -> tuple[float, float]:
    transformer = pyproj.Transformer.from_crs(src, tgt, always_xy=True)
    return transformer.transform(x, y)


def _pyproj_inv(tgt: str, src: str, x: float, y: float) -> tuple[float, float]:
    transformer = pyproj.Transformer.from_crs(tgt, src, always_xy=True)
    return transformer.transform(x, y)


def _assert_xy_parity(
    gometry_xy: tuple[float, float],
    ref_xy: tuple[float, float],
    *,
    tol: float,
    context: str,
) -> None:
    assert gometry_xy[0] == pytest.approx(ref_xy[0], abs=tol), context
    assert gometry_xy[1] == pytest.approx(ref_xy[1], abs=tol), context


def _longitude_equiv(a: float, b: float, tol: float = ABS_TOL_DEG) -> bool:
    delta = (a - b + 180.0) % 360.0 - 180.0
    return abs(delta) <= tol


def _round_trip(
    source: str,
    target: str,
    x: float,
    y: float,
    *,
    tol_m: float = ROUND_TRIP_TOL_M,
    tol_deg: float = ROUND_TRIP_TOL_DEG,
) -> None:
    out = gm.crs_transform(source, target, x, y)
    back = gm.crs_transform(target, source, out[0], out[1])
    if source == WGS84:
        assert _longitude_equiv(back[0], x, tol_deg), (source, target, (x, y), back)
        assert back[1] == pytest.approx(y, abs=tol_deg), (source, target, (x, y), back)
    else:
        assert back[0] == pytest.approx(x, abs=tol_m), (source, target, (x, y), back)
        assert back[1] == pytest.approx(y, abs=tol_m), (source, target, (x, y), back)


def _bucket_entries(name: str) -> int:
    return next(
        bucket['entries']
        for bucket in gm.crs_cache_info()['buckets']
        if bucket['name'] == name
    )


TM_UTM_PARITY_CASES: list[tuple[str, str, float, float, str]] = [
    ('EPSG:32633', WGS84, 500000.0, 0.0, 'utm33_cm_equator'),
    ('EPSG:32633', WGS84, 500000.0, 5000000.0, 'utm33_north_fn'),
    ('EPSG:32733', WGS84, 500000.0, 5000000.0, 'utm33_south_fn'),
    (WGS84, 'EPSG:32633', 15.0, 0.0, 'utm33_cm_exact'),
    (WGS84, 'EPSG:32633', 12.0, 45.0, 'utm33_cm_minus_3'),
    (WGS84, 'EPSG:32633', 18.0, 45.0, 'utm33_cm_plus_3'),
    (WGS84, 'EPSG:32633', 15.0, UTM_LAT_MAX, 'utm33_lat_84'),
    (WGS84, 'EPSG:32633', 15.0, 85.0, 'utm33_lat_85'),
    (WGS84, 'EPSG:32733', 15.0, UTM_LAT_MIN, 'utm33_lat_minus_80'),
    (WGS84, 'EPSG:32633', 15.0, 89.999, 'utm33_near_north_pole'),
    (WGS84, 'EPSG:32733', 15.0, -89.999, 'utm33_near_south_pole'),
    (WGS84, 'EPSG:32660', 177.0, 10.0, 'zone60_cm_exact'),
    (WGS84, 'EPSG:32660', -180.0, 10.0, 'zone60_lon_minus_180'),
    (WGS84, 'EPSG:32660', -179.0, 10.0, 'zone60_lon_minus_179'),
    (WGS84, 'EPSG:32601', -177.0, 10.0, 'zone1_cm_exact'),
    (WGS84, 'EPSG:32601', 180.0, 10.0, 'zone1_lon_180'),
    (WGS84, 'EPSG:32601', 179.0, 10.0, 'zone1_lon_179'),
    (GAUSS_KRUGER_GEO, GAUSS_KRUGER_PROJ, 10.5, 51.0, 'bessel_gk_fwd'),
    (GAUSS_KRUGER_PROJ, GAUSS_KRUGER_GEO, 3500000.0, 5500000.0, 'bessel_gk_inv'),
    (NAD83, NAD83_SPCS_TM, -72.5, 43.0, 'nad83_spcs_tm_fwd'),
    (NAD83_SPCS_TM, NAD83, 500000.0, 55541.95, 'nad83_spcs_tm_inv'),
]


@pytest.mark.parametrize(
    ('source', 'target', 'x', 'y', 'label'),
    TM_UTM_PARITY_CASES,
    ids=[case[4] for case in TM_UTM_PARITY_CASES],
)
def test_tm_utm_parity_vs_pyproj(
    source: str, target: str, x: float, y: float, label: str
) -> None:
    del label
    gometry_xy = gm.crs_transform(source, target, x, y)
    ref_xy = _pyproj_fwd(source, target, x, y)
    tol = ABS_TOL_DEG if target == WGS84 else ABS_TOL_M
    _assert_xy_parity(
        gometry_xy, ref_xy, tol=tol, context=f'{source}->{target} ({x}, {y})'
    )
    _round_trip(source, target, x, y)


@pytest.mark.parametrize(
    ('crs', 'lon', 'lat'), [('EPSG:32633', 15.0, 90.0), ('EPSG:32733', 15.0, -90.0)]
)
def test_tm_utm_exact_poles_match_pyproj_xy_and_latitude(
    crs: str, lon: float, lat: float
) -> None:
    gometry_xy = gm.crs_transform(WGS84, crs, lon, lat)
    ref_xy = _pyproj_fwd(WGS84, crs, lon, lat)
    _assert_xy_parity(gometry_xy, ref_xy, tol=ABS_TOL_M, context='pole forward')
    gometry_lonlat = gm.crs_transform(crs, WGS84, gometry_xy[0], gometry_xy[1])
    ref_lonlat = _pyproj_inv(crs, WGS84, gometry_xy[0], gometry_xy[1])
    assert gometry_lonlat[1] == pytest.approx(ref_lonlat[1], abs=ABS_TOL_DEG)
    assert abs(gometry_lonlat[1]) == pytest.approx(90.0, abs=ABS_TOL_DEG)


def test_tm_utm_domain_inside_150_deg_matches() -> None:
    base = gm.crs_transform(WGS84, 'EPSG:32633', 15.0, 0.0)
    x_inside = base[0] + 2.62 * 6371074.0
    gometry_lonlat = gm.crs_transform('EPSG:32633', WGS84, x_inside, base[1])
    ref_lonlat = _pyproj_inv('EPSG:32633', WGS84, x_inside, base[1])
    _assert_xy_parity(
        gometry_lonlat, ref_lonlat, tol=ABS_TOL_DEG, context='inside domain'
    )


def test_tm_utm_domain_outside_150_deg_errors() -> None:
    with pytest.raises(gm.TransformError, match='transverse Mercator domain'):
        gm.crs_transform(WGS84, 'EPSG:32660', -90.0, 0.0)
    base = gm.crs_transform(WGS84, 'EPSG:32633', 15.0, 0.0)
    x_outside = base[0] + 2.624 * 6371074.0
    with pytest.raises(gm.TransformError, match='transverse Mercator domain'):
        gm.crs_transform('EPSG:32633', WGS84, x_outside, base[1])


LCC_PARITY_CASES: list[tuple[str, str, float, float, str]] = [
    (RGF93_GEO, LAMBERT_93, 2.0, 46.5, 'rgf93_lambert93_interior'),
    (LAMBERT_93, RGF93_GEO, 700000.0, 6600000.0, 'lambert93_rgf93_interior'),
    (NAD83_2011, NAD83_2011_SPCS_LCC, -90.0, 38.0, 'nad83_lcc_interior'),
    (NAD83_2011_SPCS_LCC, NAD83_2011, 500000.0, 0.0, 'nad83_lcc_false_origin'),
    (RGF93_GEO, LAMBERT_93, 180.0, 46.0, 'lambert93_antimeridian_lon'),
    (RGF93_GEO, LAMBERT_93, 5.0, 49.0, 'lambert93_oblique_same_side'),
]


@pytest.mark.parametrize(
    ('source', 'target', 'x', 'y', 'label'),
    LCC_PARITY_CASES,
    ids=[case[4] for case in LCC_PARITY_CASES],
)
def test_lcc_parity_vs_pyproj(
    source: str, target: str, x: float, y: float, label: str
) -> None:
    del label
    gometry_xy = gm.crs_transform(source, target, x, y)
    ref_xy = _pyproj_fwd(source, target, x, y)
    tol = ABS_TOL_DEG if target in (RGF93_GEO, NAD83_2011) else ABS_TOL_M
    _assert_xy_parity(gometry_xy, ref_xy, tol=tol, context=f'{source}->{target}')
    _round_trip(source, target, x, y)


def test_lcc_pole_on_n_side_matches_pyproj() -> None:
    gometry_xy = gm.crs_transform(RGF93_GEO, LAMBERT_93, 3.0, 90.0)
    ref_xy = _pyproj_fwd(RGF93_GEO, LAMBERT_93, 3.0, 90.0)
    _assert_xy_parity(gometry_xy, ref_xy, tol=ABS_TOL_M, context='lcc north pole')
    assert gometry_xy[0] == pytest.approx(ref_xy[0], abs=ABS_TOL_M)
    assert gometry_xy[1] == pytest.approx(ref_xy[1], abs=ABS_TOL_M)


def test_lcc_wrong_side_pole_errors() -> None:
    with pytest.raises(gm.TransformError, match='Lambert Conformal Conic'):
        gm.crs_transform(RGF93_GEO, LAMBERT_93, 3.0, -90.0)


def test_lcc_southern_n_negative_inverse_branch() -> None:
    lon, lat = (-68.0, 45.0)
    gometry_xy = gm.crs_transform(NAD83_2011, NAD83_2011_SPCS_LCC, lon, lat)
    ref_xy = _pyproj_fwd(NAD83_2011, NAD83_2011_SPCS_LCC, lon, lat)
    _assert_xy_parity(gometry_xy, ref_xy, tol=ABS_TOL_M, context='southern lcc fwd')
    gometry_lonlat = gm.crs_transform(
        NAD83_2011_SPCS_LCC, NAD83_2011, gometry_xy[0], gometry_xy[1]
    )
    ref_lonlat = _pyproj_inv(
        NAD83_2011_SPCS_LCC, NAD83_2011, gometry_xy[0], gometry_xy[1]
    )
    _assert_xy_parity(
        gometry_lonlat, ref_lonlat, tol=ABS_TOL_DEG, context='southern lcc inv'
    )


POLAR_STEREO_FAMILIES: list[tuple[str, str]] = [
    (WGS84, ANTARCTIC_STEREO),
    (ANTARCTIC_STEREO, WGS84),
    (WGS84, NSIDC_NORTH),
    (NSIDC_NORTH, WGS84),
    (WGS84, ARCTIC_STEREO),
    (ARCTIC_STEREO, WGS84),
    (WGS84, WGS84_ARCTIC_A),
    (WGS84_ARCTIC_A, WGS84),
    (WGS84, WGS84_ANTARCTIC_A),
    (WGS84_ANTARCTIC_A, WGS84),
]
POLAR_STEREO_PARITY_CASES: list[tuple[str, str, float, float, str]] = [
    (WGS84, ANTARCTIC_STEREO, 0.0, -75.0, 'antarctic_interior'),
    (WGS84, ANTARCTIC_STEREO, 180.0, -70.0, 'antarctic_antimeridian'),
    (WGS84, NSIDC_NORTH, 45.0, 70.0, 'nsidc_north_interior'),
    (WGS84, ARCTIC_STEREO, -120.0, 80.0, 'arctic_stereo_b'),
    (WGS84, WGS84_ARCTIC_A, 0.0, 89.999, 'arctic_a_near_pole'),
    (WGS84, WGS84_ANTARCTIC_A, 90.0, -89.999, 'antarctic_a_near_pole'),
]


@pytest.mark.parametrize(
    ('source', 'target', 'x', 'y', 'label'),
    POLAR_STEREO_PARITY_CASES,
    ids=[case[4] for case in POLAR_STEREO_PARITY_CASES],
)
def test_polar_stereo_parity_vs_pyproj(
    source: str, target: str, x: float, y: float, label: str
) -> None:
    del label
    gometry_xy = gm.crs_transform(source, target, x, y)
    ref_xy = _pyproj_fwd(source, target, x, y)
    tol = ABS_TOL_DEG if target == WGS84 else ABS_TOL_M
    _assert_xy_parity(gometry_xy, ref_xy, tol=tol, context=f'{source}->{target}')
    _round_trip(source, target, x, y)


@pytest.mark.parametrize(
    ('target', 'pole_lat', 'lon_0'),
    [
        (ANTARCTIC_STEREO, -90.0, 0.0),
        (NSIDC_NORTH, 90.0, -45.0),
        (ARCTIC_STEREO, 90.0, -45.0),
        (WGS84_ARCTIC_A, 90.0, 0.0),
        (WGS84_ANTARCTIC_A, -90.0, 0.0),
    ],
)
def test_polar_stereo_exact_projection_pole_to_false_origin(
    target: str, pole_lat: float, lon_0: float
) -> None:
    gometry_xy = gm.crs_transform(WGS84, target, lon_0, pole_lat)
    ref_xy = _pyproj_fwd(WGS84, target, lon_0, pole_lat)
    _assert_xy_parity(gometry_xy, ref_xy, tol=ABS_TOL_M, context='exact pole forward')
    gometry_lonlat = gm.crs_transform(target, WGS84, gometry_xy[0], gometry_xy[1])
    ref_lonlat = _pyproj_inv(target, WGS84, gometry_xy[0], gometry_xy[1])
    assert gometry_lonlat[1] == pytest.approx(ref_lonlat[1], abs=ABS_TOL_DEG)
    assert abs(gometry_lonlat[1]) == pytest.approx(90.0, abs=ABS_TOL_DEG)


@pytest.mark.parametrize(
    ('target', 'wrong_pole_lat'),
    [
        (ANTARCTIC_STEREO, 90.0),
        (NSIDC_NORTH, -90.0),
        (WGS84_ARCTIC_A, -90.0),
        (WGS84_ANTARCTIC_A, 90.0),
    ],
)
def test_polar_stereo_opposite_pole_errors(target: str, wrong_pole_lat: float) -> None:
    with pytest.raises(gm.TransformError, match='polar stereographic'):
        gm.crs_transform(WGS84, target, 0.0, wrong_pole_lat)


WEB_MERC_PARITY_CASES: list[tuple[float, float, str]] = [
    (0.0, 0.0, 'equator_prime_meridian'),
    (180.0, 0.0, 'lon_180'),
    (-180.0, 0.0, 'lon_minus_180'),
    (45.0, WEB_MERCATOR_MAX_LAT, 'lat_exact_north_cap'),
    (-45.0, -WEB_MERCATOR_MAX_LAT, 'lat_exact_south_cap'),
]


@pytest.mark.parametrize(
    ('lon', 'lat', 'label'),
    WEB_MERC_PARITY_CASES,
    ids=[c[2] for c in WEB_MERC_PARITY_CASES],
)
def test_web_mercator_parity_vs_pyproj(lon: float, lat: float, label: str) -> None:
    del label
    gometry_xy = gm.crs_transform(WGS84, WEB_MERCATOR, lon, lat)
    ref_xy = _pyproj_fwd(WGS84, WEB_MERCATOR, lon, lat)
    _assert_xy_parity(gometry_xy, ref_xy, tol=ABS_TOL_M, context='webmerc fwd')
    _round_trip(WGS84, WEB_MERCATOR, lon, lat)


@pytest.mark.parametrize('sign', [1.0, -1.0])
def test_web_mercator_just_beyond_cap_raises_intentionally(sign: float) -> None:
    lat = math.nextafter(sign * WEB_MERCATOR_MAX_LAT, sign * 90.0)
    ref_xy = _pyproj_fwd(WGS84, WEB_MERCATOR, 0.0, lat)
    assert all(math.isfinite(v) for v in ref_xy)
    with pytest.raises(gm.TransformError, match='Web Mercator'):
        gm.crs_transform(WGS84, WEB_MERCATOR, 0.0, lat)


@pytest.mark.parametrize(
    ('source', 'target', 'x', 'y'),
    [
        (WGS84, 'EPSG:32610', -122.0, 37.5),
        (RGF93_GEO, LAMBERT_93, 2.5, 46.0),
        (WGS84, ANTARCTIC_STEREO, 0.0, -70.0),
        (WGS84, WEB_MERCATOR, 10.0, 45.0),
    ],
)
def test_z_m_preserved_scalar_geometry(
    source: str, target: str, x: float, y: float
) -> None:
    pt = gm.Point(x, y, z=123.0, m=456.0, crs=source)
    out = pt.to_crs(target)
    assert out.z == pytest.approx(123.0)
    assert out.m == pytest.approx(456.0)
    back = out.to_crs(source)
    assert back.z == pytest.approx(123.0)
    assert back.m == pytest.approx(456.0)


def test_z_m_preserved_geometry_array() -> None:
    arr = gm.points([0.0, 10.0], [0.0, 10.0], z=[1.0, 2.0], m=[3.0, 4.0], crs=WGS84)
    out = arr.to_crs(WEB_MERCATOR)
    coords = gm.get_coordinates(out, axes='XYZM')
    assert len(coords) == 2
    assert coords[0][2:] == pytest.approx([1.0, 3.0])
    assert coords[1][2:] == pytest.approx([2.0, 4.0])
    ref_xy = [
        _pyproj_fwd(WGS84, WEB_MERCATOR, lon, lat)
        for lon, lat in ((0.0, 0.0), (10.0, 10.0))
    ]
    assert coords[0][:2] == pytest.approx(ref_xy[0], abs=ABS_TOL_M)
    assert coords[1][:2] == pytest.approx(ref_xy[1], abs=ABS_TOL_M)


def test_z_preserved_raw_transform_coordinates() -> None:
    xs = np.array([0.0, 10.0])
    ys = np.array([0.0, 10.0])
    zs = np.array([5.0, 6.0])
    ts = np.array([7.0, 8.0])
    # Lane input returns an interleaved (N, 3) matrix; the input epoch `t`
    # steers the transform but is not a returned ordinate.
    out = gm.crs_transform(WGS84, WEB_MERCATOR, xs, ys, z=zs, t=ts)
    assert out.shape == (2, 3)
    assert out[:, 2] == pytest.approx(zs)  # Z passes through a 2D projection
    assert out[0, 0] == pytest.approx(0.0, abs=ABS_TOL_M)


@pytest.mark.parametrize('bad_value', [math.nan, math.inf, -math.inf])
def test_non_finite_scalar_input_errors(bad_value: float) -> None:
    with pytest.raises((gm.TransformError, gm.GeometryError, ValueError)):
        gm.crs_transform(WGS84, WEB_MERCATOR, bad_value, 0.0)


@pytest.mark.parametrize('bad_value', [math.nan, math.inf])
def test_non_finite_array_input_errors(bad_value: float) -> None:
    with pytest.raises((gm.TransformError, gm.GeometryError, ValueError)):
        gm.crs_transform(
            WGS84, WEB_MERCATOR, np.array([0.0, bad_value]), np.array([0.0, 1.0])
        )


def test_empty_geometry_to_crs_typed_empty_with_target_crs() -> None:
    empty = gm.from_wkt('POLYGON EMPTY', crs=WGS84)
    out = empty.to_crs(WEB_MERCATOR)
    assert out.is_empty
    assert out.crs == WEB_MERCATOR
    assert out.to_wkt() == 'POLYGON EMPTY'


@pytest.mark.parametrize(
    ('source', 'target'),
    [
        (WGS84, WEB_MERCATOR),
        (WEB_MERCATOR, 'EPSG:32632'),
        ('EPSG:32632', WEB_MERCATOR),
        (RGF93_GEO, LAMBERT_93),
    ],
)
def test_admitted_pipelines_keep_proj_cache_cold(source: str, target: str) -> None:
    gm.crs_clear_cache()
    assert _bucket_entries('proj_pipeline') == 0
    gm.crs_transform(source, target, 1.0, 2.0)
    assert _bucket_entries('proj_pipeline') == 0


def _webmerc_xy_at_lat(lat: float, lon: float = 0.0) -> tuple[float, float]:
    return _pyproj_fwd(WGS84, WEB_MERCATOR, lon, lat)


@pytest.mark.parametrize('lat', [UTM_LAT_MAX, UTM_LAT_MIN])
def test_webmerc_to_utm_snap_at_composition_boundary(lat: float) -> None:
    x, y = _webmerc_xy_at_lat(lat)
    gometry_xy = gm.crs_transform(WEB_MERCATOR, 'EPSG:32632', x, y)
    ref_lonlat = _pyproj_inv(WEB_MERCATOR, WGS84, x, y)
    ref_lonlat = (ref_lonlat[0], lat)
    ref_xy = _pyproj_fwd(WGS84, 'EPSG:32632', ref_lonlat[0], ref_lonlat[1])
    _assert_xy_parity(gometry_xy, ref_xy, tol=ABS_TOL_M, context=f'snap at lat={lat}')


def test_webmerc_to_utm_does_not_clamp_outside_snap_window() -> None:
    x, y_exact = _webmerc_xy_at_lat(84.0)
    y_outside = y_exact + (y_exact - _webmerc_xy_at_lat(84.0 - 0.0001)[1]) * 2e-05
    inv_lat = _pyproj_inv(WEB_MERCATOR, WGS84, x, y_outside)[1]
    assert abs(inv_lat - 84.0) > 1e-09
    assert inv_lat < 84.0 + 0.01
    gometry_xy = gm.crs_transform(WEB_MERCATOR, 'EPSG:32632', x, y_outside)
    ref_xy = _pyproj_fwd(
        WGS84, 'EPSG:32632', _pyproj_inv(WEB_MERCATOR, WGS84, x, y_outside)[0], inv_lat
    )
    _assert_xy_parity(gometry_xy, ref_xy, tol=ABS_TOL_M, context='outside snap window')


EDGE_CASE_COUNTS = {
    'tm_utm_parity': len(TM_UTM_PARITY_CASES) + 4,
    'lcc_parity': len(LCC_PARITY_CASES) + 3,
    'polar_stereo_parity': len(POLAR_STEREO_PARITY_CASES)
    + len(POLAR_STEREO_FAMILIES)
    + 4,
    'web_mercator_parity': len(WEB_MERC_PARITY_CASES) + 2,
    'cross_cutting': 12,
    'intentional_gometry_differs': 3,
}


def test_edge_case_inventory() -> None:
    """Sanity: edge-case parametrization counts stay stable."""
    assert sum(EDGE_CASE_COUNTS.values()) >= 50
