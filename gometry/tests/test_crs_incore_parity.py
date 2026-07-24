"""Differential oracle: in-core CRS fast paths vs pyproj."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

import gometry as gm
import pytest

pyproj = pytest.importorskip('pyproj')
if TYPE_CHECKING:
    from collections.abc import Iterator
ABS_TOL_M = 1e-06
ABS_TOL_DEG = 1e-06
OBSERVER_TOL_M = 0.001
OBSERVER_TOL_DEG = 1e-06
WGS84 = 'EPSG:4326'
WEB_MERCATOR = 'EPSG:3857'
WEB_MERCATOR_MAX_LAT = 85.0511287798066
UTM_LAT_MIN = -80.0
UTM_LAT_MAX = 84.0
NORTH_UTM = tuple(f'EPSG:{code}' for code in range(32601, 32661))
SOUTH_UTM = tuple(f'EPSG:{code}' for code in range(32701, 32761))
ALL_UTM = NORTH_UTM + SOUTH_UTM
UTM_CASES = [
    ('EPSG:32610', -122.0, 37.5),
    ('EPSG:32610', -121.5, 38.0),
    ('EPSG:32610', -122.5, 37.0),
    ('EPSG:32632', 12.0, 41.9),
    ('EPSG:32632', 11.5, 42.0),
    ('EPSG:32632', 12.5, 41.5),
    ('EPSG:32760', 174.0, -41.3),
    ('EPSG:32760', 173.5, -41.0),
    ('EPSG:32760', 174.5, -41.5),
]
WEB_MERCATOR_SPARSE = [
    (-120.0, 35.0),
    (0.0, 0.0),
    (45.0, 45.0),
    (-45.0, -45.0),
    (170.0, 60.0),
    (-10.0, 51.5),
]
ETRS89 = 'EPSG:4258'
ETRS89_UTM32 = 'EPSG:25832'
GAUSS_KRUGER_GEO = 'EPSG:4314'
GAUSS_KRUGER_PROJ = 'EPSG:31467'
NAD83 = 'EPSG:4269'
NAD83_SPCS_TM = 'EPSG:32145'
RGF93_GEO = 'EPSG:4171'
LAMBERT_93 = 'EPSG:2154'
NAD83_2011 = 'EPSG:6318'
NAD83_2011_SPCS_LCC = 'EPSG:6577'
NEW_LCC_FAMILIES: tuple[tuple[str, str], ...] = (
    (RGF93_GEO, LAMBERT_93),
    (LAMBERT_93, RGF93_GEO),
    (NAD83_2011, NAD83_2011_SPCS_LCC),
    (NAD83_2011_SPCS_LCC, NAD83_2011),
)
ANTARCTIC_STEREO = 'EPSG:3031'
NSIDC_NORTH = 'EPSG:3413'
ARCTIC_STEREO = 'EPSG:3995'
WGS84_ARCTIC_A = 'EPSG:5041'
WGS84_ANTARCTIC_A = 'EPSG:5042'
NEW_POLAR_STEREO_FAMILIES: tuple[tuple[str, str], ...] = (
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
)
NEW_TM_FAMILIES: tuple[tuple[str, str], ...] = (
    (ETRS89, ETRS89_UTM32),
    (ETRS89_UTM32, ETRS89),
    (GAUSS_KRUGER_GEO, GAUSS_KRUGER_PROJ),
    (GAUSS_KRUGER_PROJ, GAUSS_KRUGER_GEO),
    (NAD83, NAD83_SPCS_TM),
    (NAD83_SPCS_TM, NAD83),
)
ALIAS_WGS84 = ('CRS84', 'OGC:CRS84', 'urn:ogc:def:crs:OGC:1.3:CRS84')
ALIAS_WEB_MERCATOR = tuple(f'EPSG:{code}' for code in (900913, 3785, 102100))
WGS84_WKT = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]'
ALIAS_PAIRS: tuple[tuple[str, str], ...] = (
    *[(alias, WEB_MERCATOR) for alias in ALIAS_WGS84],
    *[(WEB_MERCATOR, alias) for alias in ALIAS_WGS84],
    *[(alias, WEB_MERCATOR) for alias in ALIAS_WEB_MERCATOR],
    *[(WEB_MERCATOR, alias) for alias in ALIAS_WEB_MERCATOR],
    (WGS84_WKT, WEB_MERCATOR),
)
ADMITTED_PAIRS: tuple[tuple[str, str], ...] = (
    (WGS84, WEB_MERCATOR),
    (WEB_MERCATOR, WGS84),
    *[(WGS84, crs) for crs in ALL_UTM],
    *[(crs, WGS84) for crs in ALL_UTM],
    *[(WEB_MERCATOR, crs) for crs in ALL_UTM],
    *[(crs, WEB_MERCATOR) for crs in ALL_UTM],
    *NEW_TM_FAMILIES,
    *NEW_LCC_FAMILIES,
    *NEW_POLAR_STEREO_FAMILIES,
    *ALIAS_PAIRS,
)
_max_projected_error = 0.0
_max_geographic_error = 0.0
_projected_compare_count = 0
_geographic_compare_count = 0
_observer_catalog_scanned = 0
_observer_catalog_admitted = 0
_observer_catalog_parity_passed = 0
FALLBACK_TO_PROJ: tuple[tuple[str, str], ...] = (
    ('EPSG:4289', 'EPSG:28992'),
    (ETRS89, 'EPSG:3035'),
    (NAD83, 'EPSG:2225'),
)


def _utm_zone_number(crs: str) -> int:
    code = int(crs.split(':')[1])
    if 32601 <= code <= 32660:
        return code - 32600
    if 32701 <= code <= 32760:
        return code - 32700
    raise ValueError(f'not a UTM CRS: {crs}')


def _utm_central_meridian(zone: int) -> float:
    return zone * 6.0 - 183.0


def _is_utm(crs: str) -> bool:
    return crs in ALL_UTM


def _is_wgs84_literal(crs: str) -> bool:
    return crs in (WGS84, *ALIAS_WGS84) or crs == WGS84_WKT


def _is_web_mercator_literal(crs: str) -> bool:
    return crs in (WEB_MERCATOR, *ALIAS_WEB_MERCATOR)


def _lonlat_valid_for_pair(lon: float, lat: float, source: str, target: str) -> bool:
    """Return whether (lon, lat) lies in the intersection of pair domains."""
    if (lat < UTM_LAT_MIN or lat > UTM_LAT_MAX) and (
        _is_utm(source) or _is_utm(target)
    ):
        return False
    if abs(lat) > WEB_MERCATOR_MAX_LAT and (
        _is_web_mercator_literal(source) or _is_web_mercator_literal(target)
    ):
        return False
    for crs in (source, target):
        if _is_utm(crs):
            zone = _utm_zone_number(crs)
            cm = _utm_central_meridian(zone)
            if not cm - 4.0 <= lon <= cm + 4.0:
                return False
    return True


def _area_of_use_grid(source: str, target: str) -> list[tuple[float, float]]:
    """Dense interior grid from pyproj area-of-use bounds."""
    ref_crs = source if not _is_wgs84_literal(source) else target
    if _is_wgs84_literal(ref_crs):
        ref_crs = target if not _is_wgs84_literal(target) else source
    try:
        area = pyproj.CRS(ref_crs).area_of_use
    except Exception:
        return []
    if area is None:
        return []
    west, south, east, north = (area.west, area.south, area.east, area.north)
    if not all(math.isfinite(v) for v in (west, south, east, north)):
        return []
    lon_span = east - west
    lat_span = north - south
    if lon_span <= 0.0 or lat_span <= 0.0:
        return []
    inset_lon = max(lon_span * 0.01, 0.01)
    inset_lat = max(lat_span * 0.01, 0.01)
    west += inset_lon
    east -= inset_lon
    south += inset_lat
    north -= inset_lat
    lon_steps = 5
    lat_steps = 5
    lon_vals = [west + (east - west) * i / (lon_steps - 1) for i in range(lon_steps)]
    lat_vals = [south + (north - south) * i / (lat_steps - 1) for i in range(lat_steps)]
    return [
        (lon, lat)
        for lon in lon_vals
        for lat in lat_vals
        if _lonlat_valid_for_pair(lon, lat, source, target)
    ]


def _sample_lonlat_grid(source: str, target: str) -> list[tuple[float, float]]:
    """Interior grid + boundary ring for the pair's valid lon/lat area."""
    area_points = _area_of_use_grid(source, target)
    if area_points:
        return area_points
    lats_interior = [-60.0, -20.0, 0.0, 20.0, 60.0]
    lons_interior = [-120.0, -60.0, 0.0, 60.0, 120.0]
    points: list[tuple[float, float]] = [
        (lon, lat)
        for lon in lons_interior
        for lat in lats_interior
        if _lonlat_valid_for_pair(lon, lat, source, target)
    ]
    boundary_lats = [
        UTM_LAT_MIN,
        UTM_LAT_MAX,
        -WEB_MERCATOR_MAX_LAT,
        WEB_MERCATOR_MAX_LAT,
    ]
    for crs in (source, target):
        if _is_utm(crs):
            cm = _utm_central_meridian(_utm_zone_number(crs))
            boundary_lons = [cm - 3.5, cm, cm + 3.5]
            points.extend(
                (lon, lat)
                for lon in boundary_lons
                for lat in (UTM_LAT_MIN, UTM_LAT_MAX)
                if _lonlat_valid_for_pair(lon, lat, source, target)
            )
            break
    else:
        points.extend(
            (lon, lat)
            for lon in (-170.0, 0.0, 170.0)
            for lat in boundary_lats
            if _lonlat_valid_for_pair(lon, lat, source, target)
        )
    seen: set[tuple[float, float]] = set()
    unique: list[tuple[float, float]] = []
    for point in points:
        if point not in seen:
            seen.add(point)
            unique.append(point)
    return unique


def _wgs84_for_pyproj(crs: str) -> str:
    if crs in ALIAS_WGS84 or crs in ('CRS84', WGS84_WKT):
        return WGS84
    if crs in ALIAS_WEB_MERCATOR:
        return WEB_MERCATOR
    return crs


def _sample_coords(source: str, target: str) -> Iterator[tuple[float, float, str]]:
    """Yield (x, y, coord_kind) samples for a source/target pair.

    coord_kind is ``'lonlat'`` or ``'projected'``.
    """
    if _is_wgs84_literal(source):
        for lon, lat in _sample_lonlat_grid(source, target):
            yield (lon, lat, 'lonlat')
        return
    from_wgs84 = pyproj.Transformer.from_crs(
        WGS84, _wgs84_for_pyproj(source), always_xy=True
    )
    for lon, lat in _sample_lonlat_grid(source, target):
        x, y = from_wgs84.transform(lon, lat)
        yield (x, y, 'projected')


def _longitude_equiv(a: float, b: float, tol: float = ABS_TOL_DEG) -> bool:
    delta = (a - b + 180.0) % 360.0 - 180.0
    return abs(delta) <= tol


def _track_errors(
    gometry_xy: tuple[float, float],
    ref_xy: tuple[float, float],
    coord_kind: str,
    pair: tuple[str, str],
    coord: tuple[float, float],
) -> None:
    global \
        _max_projected_error, \
        _max_geographic_error, \
        _projected_compare_count, \
        _geographic_compare_count
    dx = abs(gometry_xy[0] - ref_xy[0])
    dy = abs(gometry_xy[1] - ref_xy[1])
    err = max(dx, dy)
    if coord_kind == 'lonlat':
        _max_geographic_error = max(_max_geographic_error, err)
        _geographic_compare_count += 1
        tol = ABS_TOL_DEG
    else:
        _max_projected_error = max(_max_projected_error, err)
        _projected_compare_count += 1
        tol = ABS_TOL_M
    assert gometry_xy[0] == pytest.approx(ref_xy[0], abs=tol), (
        f'{pair} coord={coord}: x err {dx}'
    )
    assert gometry_xy[1] == pytest.approx(ref_xy[1], abs=tol), (
        f'{pair} coord={coord}: y err {dy}'
    )


def _compare_forward(
    source: str,
    target: str,
    x: float,
    y: float,
    coord_kind: str,
    *,
    transformer: object | None = None,
) -> tuple[float, float]:
    pair = (source, target)
    coord = (x, y)
    if transformer is None:
        src = _wgs84_for_pyproj(source)
        tgt = _wgs84_for_pyproj(target)
        transformer = pyproj.Transformer.from_crs(src, tgt, always_xy=True)
    ref = transformer.transform(x, y)  # type: ignore[union-attr]
    gometry_result = gm.crs_transform(source, target, x, y)
    _track_errors(gometry_result, ref, coord_kind, pair, coord)
    return gometry_result


def bucket_entries(name: str) -> int:
    return next(
        bucket['entries']
        for bucket in gm.crs_cache_info()['buckets']
        if bucket['name'] == name
    )


def _assert_xy_parity(
    gometry_xy: tuple[float, float],
    ref_xy: tuple[float, float],
    *,
    tol: float,
    context: str,
) -> None:
    dx = abs(gometry_xy[0] - ref_xy[0])
    dy = abs(gometry_xy[1] - ref_xy[1])
    assert dx <= tol and dy <= tol, f'{context}: dx={dx}, dy={dy} (tol={tol})'


def _assert_pair_parity_both_directions(
    source: str,
    target: str,
    lon: float,
    lat: float,
    *,
    tol_m: float = OBSERVER_TOL_M,
    tol_deg: float = OBSERVER_TOL_DEG,
) -> None:
    """Assert gometry matches pyproj for geo→target and target→geo."""
    pair = (source, target)
    src = _wgs84_for_pyproj(source)
    tgt = _wgs84_for_pyproj(target)
    fwd = pyproj.Transformer.from_crs(src, tgt, always_xy=True)
    inv = pyproj.Transformer.from_crs(tgt, src, always_xy=True)
    gometry_xy = gm.crs_transform(source, target, lon, lat)
    ref_xy = fwd.transform(lon, lat)
    _assert_xy_parity(
        gometry_xy, ref_xy, tol=tol_m, context=f'{pair} forward coord=({lon}, {lat})'
    )
    gometry_lonlat = gm.crs_transform(target, source, gometry_xy[0], gometry_xy[1])
    ref_lonlat = inv.transform(gometry_xy[0], gometry_xy[1])
    _assert_xy_parity(
        gometry_lonlat,
        ref_lonlat,
        tol=tol_deg,
        context=f'{(target, source)} inverse coord={gometry_xy}',
    )


def _enumerate_epsg_geo_projected_pairs() -> list[tuple[str, str]]:
    """Deterministic EPSG catalog: (base geographic CRS, projected CRS)."""
    from pyproj.enums import PJType

    infos = list(
        pyproj.database.query_crs_info(auth_name='EPSG', pj_types=PJType.PROJECTED_CRS)
    )
    infos.sort(key=lambda info: info.code)
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for info in infos:
        code = f'EPSG:{info.code}'
        try:
            crs = pyproj.CRS(code)
            geo = crs.source_crs
            if geo is None:
                continue
            geo_code = geo.to_epsg()
            if geo_code is None:
                continue
            geo_str = f'EPSG:{geo_code}'
        except Exception:  # noqa: S112 — unparseable catalog rows are skipped by design
            continue
        key = (geo_str, code)
        if key in seen:
            continue
        seen.add(key)
        pairs.append(key)
    return pairs


def _pick_test_lonlat(source: str, target: str) -> tuple[float, float] | None:
    """One deterministic in-area lon/lat for a geographic→projected pair."""
    ref_crs = target
    try:
        area = pyproj.CRS(ref_crs).area_of_use
    except Exception:
        area = None
    if area is not None:
        west, south, east, north = (area.west, area.south, area.east, area.north)
        if all(math.isfinite(v) for v in (west, south, east, north)) and (
            east > west and north > south
        ):
            lon = (west + east) / 2.0
            lat = (south + north) / 2.0
            if _lonlat_valid_for_pair(lon, lat, source, target):
                return (lon, lat)
    for lon, lat in _sample_lonlat_grid(source, target):
        return (lon, lat)
    return None


@pytest.mark.parametrize(('crs', 'test_lon', 'test_lat'), UTM_CASES)
def test_utm_forward_inverse_round_trip_sparse(
    crs: str, test_lon: float, test_lat: float
) -> None:
    if not UTM_LAT_MIN <= test_lat <= UTM_LAT_MAX:
        pytest.skip('outside UTM latitude domain')
    transformer_fwd = pyproj.Transformer.from_crs(WGS84, crs, always_xy=True)
    transformer_inv = pyproj.Transformer.from_crs(crs, WGS84, always_xy=True)
    gometry_xy = gm.crs_transform(4326, crs, test_lon, test_lat)
    proj_xy = transformer_fwd.transform(test_lon, test_lat)
    assert gometry_xy[0] == pytest.approx(proj_xy[0], abs=ABS_TOL_M)
    assert gometry_xy[1] == pytest.approx(proj_xy[1], abs=ABS_TOL_M)
    gometry_lonlat = gm.crs_transform(crs, 4326, gometry_xy[0], gometry_xy[1])
    proj_lonlat = transformer_inv.transform(gometry_xy[0], gometry_xy[1])
    assert gometry_lonlat[0] == pytest.approx(proj_lonlat[0], abs=ABS_TOL_DEG)
    assert gometry_lonlat[1] == pytest.approx(proj_lonlat[1], abs=ABS_TOL_DEG)
    assert gometry_lonlat[0] == pytest.approx(test_lon, abs=ABS_TOL_DEG)
    assert gometry_lonlat[1] == pytest.approx(test_lat, abs=ABS_TOL_DEG)


# Pair-local transformers for the fixed 4326↔3857 sparse matrix (built once).
_WEB_MERCATOR_FWD: object | None = None
_WEB_MERCATOR_INV: object | None = None


def _web_mercator_transformers() -> tuple[object, object]:
    global _WEB_MERCATOR_FWD, _WEB_MERCATOR_INV
    if _WEB_MERCATOR_FWD is None or _WEB_MERCATOR_INV is None:
        _WEB_MERCATOR_FWD = pyproj.Transformer.from_crs(
            WGS84, WEB_MERCATOR, always_xy=True
        )
        _WEB_MERCATOR_INV = pyproj.Transformer.from_crs(
            WEB_MERCATOR, WGS84, always_xy=True
        )
    return _WEB_MERCATOR_FWD, _WEB_MERCATOR_INV


@pytest.mark.parametrize(('lon', 'lat'), WEB_MERCATOR_SPARSE)
def test_web_mercator_forward_inverse_round_trip_sparse(lon: float, lat: float) -> None:
    if abs(lat) > WEB_MERCATOR_MAX_LAT:
        pytest.skip('outside Web Mercator latitude domain')
    transformer_fwd, transformer_inv = _web_mercator_transformers()
    gometry_xy = gm.crs_transform(4326, 3857, lon, lat)
    proj_xy = transformer_fwd.transform(lon, lat)  # type: ignore[union-attr]
    assert gometry_xy[0] == pytest.approx(proj_xy[0], abs=ABS_TOL_M)
    assert gometry_xy[1] == pytest.approx(proj_xy[1], abs=ABS_TOL_M)
    gometry_lonlat = gm.crs_transform(3857, 4326, gometry_xy[0], gometry_xy[1])
    proj_lonlat = transformer_inv.transform(gometry_xy[0], gometry_xy[1])  # type: ignore[union-attr]
    assert gometry_lonlat[0] == pytest.approx(proj_lonlat[0], abs=ABS_TOL_DEG)
    assert gometry_lonlat[1] == pytest.approx(proj_lonlat[1], abs=ABS_TOL_DEG)
    assert gometry_lonlat[0] == pytest.approx(lon, abs=ABS_TOL_DEG)
    assert gometry_lonlat[1] == pytest.approx(lat, abs=ABS_TOL_DEG)


@pytest.mark.parametrize('pair', ADMITTED_PAIRS)
def test_admitted_pair_forward_reverse_roundtrip(pair: tuple[str, str]) -> None:
    source, target = pair
    src = _wgs84_for_pyproj(source)
    tgt = _wgs84_for_pyproj(target)
    # One forward + one reverse transformer per CRS pair (not per coordinate).
    fwd = pyproj.Transformer.from_crs(src, tgt, always_xy=True)
    rev = pyproj.Transformer.from_crs(tgt, src, always_xy=True)
    for x, y, _coord_kind in _sample_coords(source, target):
        out_kind = 'lonlat' if _is_wgs84_literal(target) else 'projected'
        out = _compare_forward(source, target, x, y, out_kind, transformer=fwd)
        rev_kind = 'lonlat' if _is_wgs84_literal(source) else 'projected'
        # Reuse reverse comparison result for round-trip asserts (no third transform).
        back = _compare_forward(
            target, source, out[0], out[1], rev_kind, transformer=rev
        )
        if _is_wgs84_literal(source):
            rt_lon, rt_lat = back
            assert _longitude_equiv(rt_lon, x), (pair, (x, y), rt_lon)
            assert rt_lat == pytest.approx(y, abs=ABS_TOL_DEG), (pair, (x, y))
        elif _is_wgs84_literal(target):
            rt_x, rt_y = back
            assert rt_x == pytest.approx(x, abs=ABS_TOL_M), (pair, (x, y))
            assert rt_y == pytest.approx(y, abs=ABS_TOL_M), (pair, (x, y))


def test_in_core_pairs_do_not_warm_proj_pipeline() -> None:
    gm.crs_clear_cache()
    assert bucket_entries('proj_pipeline') == 0
    for source, target in ADMITTED_PAIRS:
        for x, y, _ in _sample_coords(source, target):
            gm.crs_transform(source, target, x, y)
    assert bucket_entries('proj_pipeline') == 0


@pytest.mark.exhaustive
def test_observer_admitted_pairs_match_pyproj() -> None:
    """Every in-core-admitted EPSG geo↔projected pair must match pyproj.

    Opt-in (full EPSG catalog scan): ``pytest -m exhaustive``. The sparse admitted-pair
    matrix elsewhere in this file keeps deterministic CRS parity in the default lane.
    """
    global \
        _observer_catalog_scanned, \
        _observer_catalog_admitted, \
        _observer_catalog_parity_passed
    pairs = _enumerate_epsg_geo_projected_pairs()
    scanned = len(pairs)
    admitted = 0
    parity_passed = 0
    for source, target in pairs:
        gm.crs_clear_cache()
        assert bucket_entries('proj_pipeline') == 0
        lonlat = _pick_test_lonlat(source, target)
        if lonlat is None:
            continue
        lon, lat = lonlat
        try:
            gometry_xy = gm.crs_transform(source, target, lon, lat)
            gm.crs_transform(target, source, gometry_xy[0], gometry_xy[1])
        except gm.TransformError:
            continue
        if bucket_entries('proj_pipeline') != 0:
            continue
        admitted += 1
        _assert_pair_parity_both_directions(source, target, lon, lat)
        parity_passed += 1
    _observer_catalog_scanned = scanned
    _observer_catalog_admitted = admitted
    _observer_catalog_parity_passed = parity_passed
    assert scanned > 4000, f'expected full EPSG projected catalog, got {scanned}'
    assert admitted > 0, 'observer found no in-core-admitted pairs'
    assert parity_passed == admitted, (
        f'catalog scan={scanned} admitted={admitted} parity_passed={parity_passed}'
    )


@pytest.mark.parametrize('pair', FALLBACK_TO_PROJ)
def test_non_incore_pairs_fall_back_to_proj(pair: tuple[str, str]) -> None:
    """CRS pairs that must use PROJ, not the in-core fast path."""
    geo, proj = pair
    lonlat = _pick_test_lonlat(geo, proj)
    if lonlat is None:
        lon, lat = (10.0, 50.0)
    else:
        lon, lat = lonlat
    gm.crs_clear_cache()
    assert bucket_entries('proj_pipeline') == 0
    gm.crs_transform(geo, proj, lon, lat)
    assert bucket_entries('proj_pipeline') > 0, (
        f'{proj} wrongly admitted to in_core (proj_pipeline stayed cold)'
    )
    _assert_pair_parity_both_directions(geo, proj, lon, lat)


def test_standard_literal_pairs_stay_in_core() -> None:
    gm.crs_clear_cache()
    assert bucket_entries('proj_pipeline') == 0
    gm.crs_transform(WGS84, WEB_MERCATOR, 10.0, 50.0)
    assert bucket_entries('proj_pipeline') == 0
    gm.crs_clear_cache()
    assert bucket_entries('proj_pipeline') == 0
    gm.crs_transform('CRS84', WEB_MERCATOR, 10.0, 50.0)
    assert bucket_entries('proj_pipeline') == 0


def test_oracle_reports_max_errors() -> None:
    """Session capstone: dense oracle trackers stay within tolerance.

    Drives a representative projected+geographic comparison batch itself so the
    capstone is non-vacuous and file-order independent — a bare tolerance check
    against zero-initialized globals would pass without any oracle case running.
    """
    for x, y, _coord_kind in _sample_coords(WGS84, WEB_MERCATOR):
        forward = _compare_forward(WGS84, WEB_MERCATOR, x, y, 'projected')
        _compare_forward(WEB_MERCATOR, WGS84, forward[0], forward[1], 'lonlat')
    assert _projected_compare_count > 0, 'no projected oracle comparison recorded'
    assert _geographic_compare_count > 0, 'no geographic oracle comparison recorded'
    assert _max_projected_error <= ABS_TOL_M, _max_projected_error
    assert _max_geographic_error <= ABS_TOL_DEG, _max_geographic_error
    assert math.isfinite(_max_projected_error)
    assert math.isfinite(_max_geographic_error)
