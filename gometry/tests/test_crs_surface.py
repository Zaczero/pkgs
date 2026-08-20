"""CRS behavior — geodesic measurement, transforms, best-UTM selection,
runtime config, cache info, and the PROJ authority metadata surface.
"""

import gometry as gm
import pytest


def test_crs_assignment_rejects_unknown_authority_metadata() -> None:
    with pytest.raises(ValueError, match='could not resolve CRS'):
        gm.Point(1, 2, crs=999999)
    with pytest.raises(ValueError, match='could not resolve CRS'):
        gm.Point(1, 2).set_crs('EPSG:999999')
    with pytest.raises(ValueError, match='Invalid PROJ string syntax'):
        gm.CRS('not-a-crs')


def test_crs_public_type_hints_resolve() -> None:
    from gometry import _lib, _types

    for name in (
        'apply',
        'transform',
        'info',
        'roundtrip',
        'cache_info',
        'transform_bounds',
    ):
        assert getattr(gm, f'crs_{name}') is getattr(_lib, f'crs_{name}')
    assert 'crs' in _types.CrsInfo.__annotations__
    assert 'distance' in _types.CrsGeodesicInfo.__annotations__
    assert issubclass(_types.CrsAreaOfUse, dict)
    assert gm.crs_info(4326)['kind'] == 'geographic_2d'
    assert gm.CRS(4326).kind == 'geographic_2d'
    assert gm.CRS(3857).is_projected is True
    assert isinstance(gm.CRS(5703).geoid_models, list)
    assert isinstance(gm.CRS(4326).to_projjson_dict(), dict)


def test_crs_public_surface_stays_documented_and_native_registered() -> None:
    """The ``crs_*`` family is exactly this inventory, and all of it is public.

    Checked against the imported module, not against source text: an
    unregistered native function cannot exist at runtime, and runtime/stub
    agreement is already stubtest's job (``pyo3stubs check-all``).
    """
    crs_ops = {
        'apply',
        'authorities',
        'cache_info',
        'catalog',
        'celestial_bodies',
        'clear_cache',
        'codes',
        'config',
        'configure',
        'ellipsoids',
        'engine',
        'grid',
        'info',
        'prime_meridians',
        'proj_operations',
        'reset',
        'roundtrip',
        'search',
        'transform',
        'transform_bounds',
        'unit',
        'units',
        'utm_zones',
    }
    native_exports = {f'crs_{name}' for name in crs_ops}
    # Exact equality both ways: a missing member and an unannounced new one are
    # both failures. This is stricter than the subset checks it replaces.
    assert {name for name in gm.__all__ if name.startswith('crs_')} == native_exports
    for name in sorted(native_exports):
        assert callable(getattr(gm, name)), f'{name} is not callable on gometry'
    assert 'CRS' in gm.__all__
    assert 'crs' not in gm.__all__


def test_to_crs_transforms_estimated_local_crs_roundtrip() -> None:
    geom = gm.Point(21.0, 52.0, z=9.0, m=10.0, crs=4326)
    projected = geom.to_crs(geom.estimate_local_crs())
    recovered = projected.to_crs(4326)
    assert projected.crs == 'EPSG:32634'
    assert projected.coords.to_nested() == pytest.approx(
        [499999.999, 5761038.2, 9.0, 10.0], rel=1e-06
    )
    assert recovered.coords.to_nested() == pytest.approx(
        [21.0, 52.0, 9.0, 10.0], abs=1e-06
    )
    assert projected.buffer(100).crs == 'EPSG:32634'


def test_estimate_local_crs_is_extent_aware_and_fail_closed() -> None:
    warsaw = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
    assert warsaw.estimate_local_crs() == 'EPSG:32634'
    assert warsaw.to_crs(3857).estimate_local_crs() == warsaw.estimate_local_crs()
    assert gm.Point(0.0, 89.0, crs=4326).estimate_local_crs().is_projected
    assert gm.Point(0.0, -89.0, crs=4326).estimate_local_crs().is_projected
    seam = gm.LineString([(179.8, 10.0), (-179.8, 10.0)], crs=4326)
    seam_crs = seam.estimate_local_crs()
    assert seam_crs.is_projected
    for lon in (179.8, -179.8, 180.0):
        factors = seam_crs.factors(lon, 10.0)
        assert abs(factors['meridional_scale'] - 1.0) <= 0.001
        assert abs(factors['parallel_scale'] - 1.0) <= 0.001

    with pytest.raises(gm.CRSError, match=r'0\.1%'):
        gm.box(-20.0, -20.0, 20.0, 20.0, crs=4326).estimate_local_crs()
    with pytest.raises(gm.CRSError, match='CRS-tagged'):
        gm.Point(0.0, 0.0).estimate_local_crs()
    with pytest.raises(gm.CRSError, match='empty'):
        gm.Point().set_crs(4326).estimate_local_crs()


def test_geometry_array_estimate_local_crs_skips_missing_rows() -> None:
    array = gm.GeometryArray([None, gm.Point(21.0, 52.0, crs=4326)])
    assert array.estimate_local_crs() == 'EPSG:32634'
    with pytest.raises(gm.CRSError, match='present, non-empty'):
        gm.GeometryArray([None], crs=4326).estimate_local_crs()


def test_utm_wraps_longitudes_across_the_antimeridian_like_proj() -> None:
    seam = gm.Point(-179.9, 0.0, crs=4326).to_crs(32660)
    assert seam.coords.to_nested() == pytest.approx([845121.946, 0.0], abs=0.01)
    assert seam.to_crs(4326).coords.to_nested() == pytest.approx(
        [-179.9, 0.0], abs=1e-09
    )
    east = gm.Point(180.0, 10.0, crs=4326).to_crs(32660)
    west = gm.Point(-180.0, 10.0, crs=4326).to_crs(32660)
    assert east.coords.to_nested() == pytest.approx(west.coords.to_nested())
    far = gm.Point(0.0, 0.0, crs=4326).to_crs(32660)
    assert far.coords.to_nested() == pytest.approx([166021.443, 19995929.886], abs=0.01)
    with pytest.raises(gm.TransformError, match='transverse Mercator domain'):
        gm.Point(-90.0, 0.0, crs=4326).to_crs(32660)
    strip = gm.LineString([(179.9, 0.0), (-179.9, 0.0)], crs=4326).buffer(
        100.0, side='left'
    )
    assert not strip.is_empty


def test_to_crs_uses_proj_for_authority_crs_outside_fast_path() -> None:
    point = gm.Point(530000, 180000, z=42, m=7, crs=27700)
    # PROJ lacks OSTN15, so British National Grid transforms degrade explicitly.
    with pytest.warns(gm.AccuracyWarning, match='uk_os_OSTN15_NTv2_OSGBtoETRS[.]tif'):
        lonlat = point.to_crs(4326)
    with pytest.warns(gm.AccuracyWarning, match='uk_os_OSTN15_NTv2_OSGBtoETRS[.]tif'):
        web = point.to_crs(3857)
    with pytest.warns(gm.AccuracyWarning, match='uk_os_OSTN15_NTv2_OSGBtoETRS[.]tif'):
        batch = gm.points([530000, 700000], [180000, 570000], crs=27700).to_crs(4326)
    assert lonlat.crs == 'EPSG:4326'
    assert lonlat.coordinate_axes == 'XYZM'
    assert lonlat.coords.to_nested() == pytest.approx([
        -0.12835394047946935,
        51.50399082763378,
        42,
        7,
    ])
    assert web.crs == 'EPSG:3857'
    assert web.coords.to_nested() == pytest.approx([
        -14288.295295484671,
        6710932.763391882,
        42,
        7,
    ])
    assert batch.crs == 'EPSG:4326'
    assert [
        coordinate for geometry in batch for coordinate in geometry.coords.to_nested()
    ] == pytest.approx([
        -0.12835394047946935,
        51.50399082763378,
        2.684371211084661,
        54.934171474754926,
    ])


def test_planar_metrics_reject_vertical_crs() -> None:
    geom = gm.box(0, 0, 1, 1, crs=5703)
    with pytest.raises(ValueError, match='horizontal'):
        _ = geom.area


def test_crs_input_forms_are_accepted_across_representative_params() -> None:
    """The public ``CrsInput`` contract: EPSG int, authority string,
    (authority, code) tuple, PROJJSON dict, and any object exposing
    ``to_wkt()`` (e.g. ``pyproj.CRS``) are interchangeable everywhere a
    ``crs=`` parameter appears.
    """
    import pyproj

    pyproj_crs = pyproj.CRS.from_epsg(4326)
    projjson = gm.CRS(4326).to_projjson_dict()
    forms = [4326, 'EPSG:4326', ('EPSG', 4326), projjson, pyproj_crs]
    for form in forms:
        assert gm.Point(0, 0, crs=form).crs == 'EPSG:4326'
        assert gm.Point(0, 0).set_crs(form).crs == 'EPSG:4326'
        assert gm.points([0.0], [0.0], crs=form).crs == 'EPSG:4326'
        assert gm.require(gm.Point(0, 0, crs=4326), crs=form).crs == 'EPSG:4326'
    planar = gm.crs_transform_bounds(pyproj_crs, gm.CRS(3857), (-1.0, 50.0, 1.0, 51.0))
    assert planar[0] == pytest.approx(-111319.4907932736)


def test_raw_transform_canonicalizes_equivalent_crs_spellings() -> None:
    wkt = gm.CRS(4326).to_wkt()
    int_matrix = gm.crs_transform(4326, 3857, [10.0], [50.0])
    str_matrix = gm.crs_transform('EPSG:4326', 'EPSG:3857', [10.0], [50.0])
    wkt_matrix = gm.crs_transform(wkt, 3857, [10.0], [50.0])
    by_int = [int_matrix[:, 0].tolist(), int_matrix[:, 1].tolist()]
    by_str = [str_matrix[:, 0].tolist(), str_matrix[:, 1].tolist()]
    by_wkt = [wkt_matrix[:, 0].tolist(), wkt_matrix[:, 1].tolist()]
    assert by_int == by_str == by_wkt
    assert gm.crs_transform_bounds(
        4326, 3857, (0.0, 0.0, 1.0, 1.0)
    ) == gm.crs_transform_bounds('EPSG:4326', 3857, (0.0, 0.0, 1.0, 1.0))
