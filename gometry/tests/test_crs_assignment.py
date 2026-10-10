"""CRS behavior — geodesic measurement, transforms, best-UTM selection,
runtime config, cache info, and the PROJ authority metadata surface.
"""

import json

import gometry as gm
import pytest


def test_with_crs_assigns_and_to_crs_transforms_web_mercator() -> None:
    raw_point = gm.Point(1, 2)
    point = raw_point.with_crs(4326)
    raw_points = gm.points([1.0, 2.0], [2.0, 3.0])
    points = raw_points.with_crs(4326)
    assert raw_point.crs is None
    assert raw_points.crs is None
    measured = gm.Point(1, 2, z=3, m=4, crs=4326)
    epoch_point = gm.Point(1, 2, z=3, crs=4979, epoch=2010.0)
    epoch_points = gm.points([1.0, 2.0], [2.0, 3.0], crs=4326, epoch=2010.0)
    dynamic_epoch_a = gm.Point(
        3657660.66, 255768.55, z=5201382.11, crs=7789, epoch=2010.0
    )
    dynamic_epoch_b = gm.Point(
        3657660.66, 255768.55, z=5201382.11, crs=7789, epoch=2020.0
    )
    assert point.crs == gm.CRS('EPSG:4326')
    assert points.crs == gm.CRS('EPSG:4326')
    assert point.epoch is None
    assert epoch_point.epoch == 2010.0
    assert epoch_points.epoch == 2010.0
    assert [item.epoch for item in epoch_points] == [2010.0, 2010.0]
    with pytest.raises(ValueError, match='one shared coordinate epoch'):
        gm.GeometryArray([point, gm.Point(1, 2, crs=4326, epoch=2010.0)])
    with pytest.raises(ValueError, match='one shared coordinate epoch'):
        gm.GeometryArray([dynamic_epoch_a, dynamic_epoch_b])
    assert gm.GeometryArray([point], epoch=2020.0)[0].epoch == 2020.0
    assert point.to_crs('EPSG:4326').crs == gm.CRS('EPSG:4326')
    assert (points).to_crs(3857).crs == gm.CRS('EPSG:3857')
    dynamic_epoch_array = gm.GeometryArray([dynamic_epoch_a, dynamic_epoch_a]).to_crs(
        7660
    )
    web_mercator = point.to_crs('epsg:3857')
    recovered = web_mercator.to_crs(4326)
    measured_web = measured.to_crs(3857)
    epoch_geocentric = epoch_point.to_crs(4978)
    epoch_target = epoch_point.to_crs(4978, epoch=2020.0)
    assert web_mercator.crs == gm.CRS('EPSG:3857')
    assert web_mercator.epoch is None
    expected_dynamic_epoch_coordinates = [
        dynamic_epoch_a.to_crs(7660).coords.to_nested(),
        dynamic_epoch_a.to_crs(7660).coords.to_nested(),
    ]
    for actual, expected in zip(
        dynamic_epoch_array, expected_dynamic_epoch_coordinates, strict=True
    ):
        assert actual.coords.to_nested() == pytest.approx(expected)
    assert (
        max(
            (
                abs(left - right)
                for left, right in zip(
                    dynamic_epoch_a.to_crs(7660).coords.to_nested(),
                    dynamic_epoch_b.to_crs(7660).coords.to_nested(),
                    strict=True,
                )
            )
        )
        > 0.001
    )
    # Both target frames admit the source epoch.
    assert dynamic_epoch_array.epoch == 2010.0
    assert [item.epoch for item in dynamic_epoch_array] == [2010.0, 2010.0]
    assert epoch_geocentric.epoch == 2010.0
    assert epoch_target.epoch == 2020.0
    assert web_mercator.coords.to_nested() == pytest.approx(
        [111319.49, 222684.21], rel=1e-06
    )
    assert recovered.coords.to_nested() == pytest.approx([1, 2])
    assert measured_web.coordinate_axes == 'XYZM'
    assert measured_web.z == 3
    assert measured_web.m == 4
    geographic_3d = gm.Point(21, 52, z=9, crs=4979)
    assert geographic_3d.to_crs(4326).coords.to_nested() == [21.0, 52.0, 9.0]
    assert geographic_3d.to_crs(3857).to_crs(4979).coords.to_nested() == pytest.approx([
        21.0,
        52.0,
        9.0,
    ])
    with pytest.raises(ValueError, match='coordinate epoch must be'):
        gm.Point(1, 2, epoch=float('nan'))
    with pytest.raises(ValueError):
        gm.Point(1, 90, crs=4326).to_crs(3857)
    with pytest.raises(ValueError):
        gm.Point(1, 2).to_crs(3857)


def test_with_epoch_assigns_clears_and_guards_retag() -> None:
    point = gm.Point(1, 2, crs=4326)
    stamped = point.with_epoch(2020.0)
    assert stamped.epoch == 2020.0
    assert point.epoch is None
    assert str(stamped) == 'POINT (1 2)'
    assert stamped.crs == gm.CRS('EPSG:4326')
    assert stamped.with_epoch(None).epoch is None
    with pytest.raises(
        ValueError, match='with_epoch would change the coordinate epoch'
    ):
        stamped.with_epoch(2024.0)
    assert stamped.with_epoch(2024.0, overwrite=True).epoch == 2024.0
    assert stamped.with_epoch(2020.0).epoch == 2020.0
    with pytest.raises(ValueError, match='finite decimal year'):
        point.with_epoch(float('inf'))
    assert (gm.Point(0, 0, crs=4326)).with_epoch(2021.5).epoch == 2021.5
    array = gm.GeometryArray([
        gm.Point(0, 0, crs=4326),
        gm.Point(1, 1, crs=4326),
    ]).with_epoch(2015.0)
    assert array.epoch == 2015.0
    assert [item.epoch for item in array] == [2015.0, 2015.0]
    assert array.with_epoch(None).epoch is None
    assert array.epoch == 2015.0


def test_epoch_requires_a_crs_and_with_crs_none_clears_it() -> None:
    with pytest.raises(
        ValueError,
        match=r'^a coordinate epoch requires a CRS; attach one with crs= \(or with_crs\(\.\.\.\)\) before tagging an epoch$',
    ):
        gm.Point(0, 0, epoch=2020.0)
    with pytest.raises(
        ValueError,
        match=r'^a coordinate epoch requires a CRS; attach one with crs= \(or with_crs\(\.\.\.\)\) before tagging an epoch$',
    ):
        gm.Point(0, 0).with_epoch(2020.0)
    with pytest.raises(
        ValueError,
        match=r'^a coordinate epoch requires a CRS; attach one with crs= \(or with_crs\(\.\.\.\)\) before tagging an epoch$',
    ):
        gm.GeometryArray([gm.Point(0, 0)], epoch=2020.0)
    with pytest.raises(
        ValueError,
        match=r'^a coordinate epoch requires a CRS; attach one with crs= \(or with_crs\(\.\.\.\)\) before tagging an epoch$',
    ):
        gm.points([0], [0]).with_epoch(2020.0)
    assert gm.Point(10, 20, epoch=2020.0, crs=4326).epoch == 2020.0
    stamped = gm.Point(0, 0, crs=4326, epoch=2020.0)
    cleared = stamped.with_crs(None)
    assert cleared.crs is None and cleared.epoch is None
    assert stamped.with_crs(3857, overwrite=True).epoch == 2020.0
    array = gm.GeometryArray([gm.Point(0, 0, crs=4326)]).with_epoch(2020.0)
    cleared_array = array.with_crs(None)
    assert cleared_array.crs is None and cleared_array.epoch is None
    assert [item.epoch for item in cleared_array] == [None]
    assert gm.Point(0, 0, crs=4326).with_epoch(-0.0).epoch == 0.0
    point = gm.Point(1, 2, crs=4326)
    with pytest.raises(TypeError):
        point.to_crs(3857, source_epoch=2020.0)
    with pytest.raises(TypeError):
        point.to_crs(3857, target_epoch=2020.0)
    assert point.with_epoch(2020.0).to_crs(4326).epoch == 2020.0
    assert point.with_epoch(2010.0).to_crs(4978, epoch=2020.0).epoch == 2020.0
    assert point.with_epoch(2020.0).to_crs(4326, only_best=True).epoch == 2020.0
    assert point.with_epoch(2020.0).to_crs(3857).epoch == 2020.0
    stamped_array = gm.GeometryArray([gm.Point(0, 0, crs=4326)]).with_epoch(2020.0)
    assert stamped_array.to_crs(4326, only_best=True).epoch == 2020.0
    assert stamped_array.to_crs(3857).epoch == 2020.0


def test_epoch_requires_an_admitted_crs_at_the_shared_frame_owner(
    capsys: pytest.CaptureFixture[str],
) -> None:
    message = r'^a coordinate epoch is not admitted for CRS EPSG:2180; remove epoch= or transform to a CRS that admits coordinate epochs first$'
    for make in (
        lambda: gm.Point(1.0, 2.0, crs=2180, epoch=2020.0),
        lambda: gm.GeometryArray([gm.Point(1.0, 2.0)], crs=2180, epoch=2020.0),
        lambda: gm.points([1.0], [2.0], crs=2180, epoch=2020.0),
        lambda: gm.box(0.0, 0.0, 1.0, 1.0, crs=2180, epoch=2020.0),
        lambda: gm.Point(1.0, 2.0, crs=2180).with_epoch(2020.0),
    ):
        with pytest.raises(ValueError, match=message):
            make()

    observed = gm.Point(21.0, 52.0, crs=4326, epoch=2020.0)
    with pytest.raises(ValueError, match=message):
        observed.to_crs(2180, epoch=2025.5)
    with pytest.raises(ValueError, match=message):
        observed.with_crs(2180, overwrite=True)
    assert capsys.readouterr().err == ''

    # The WGS84 ensemble and true dynamic datums remain usable end to end.
    assert observed.to_crs(9000).epoch == 2020.0
    assert observed.to_crs(2180).epoch is None


def test_estimate_local_crs_normalizes_frames_and_preserves_datums() -> None:
    geom = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
    assert geom.estimate_local_crs() == gm.CRS('EPSG:32634')
    assert geom.to_crs(3857).estimate_local_crs() == gm.CRS('EPSG:32634')
    assert geom.to_crs(32634).estimate_local_crs() == gm.CRS('EPSG:32634')
    assert gm.Point(21, 52, z=9, crs=4979).estimate_local_crs() == gm.CRS('EPSG:32634')
    osgb = gm.Point(530000, 180000, crs=27700)
    osgb_local = osgb.estimate_local_crs()
    assert osgb_local.info['name'] == 'Local conformal CRS'
    assert osgb_local.info['datum']['name'] == gm.CRS(27700).info['datum']['name']
    assert gm.Point(-74.0, 41.0, crs=4269).estimate_local_crs() == gm.CRS('EPSG:26918')
    assert gm.Point(500000, 4500000, crs=26918).estimate_local_crs() == gm.CRS(
        'EPSG:26918'
    )
    assert gm.Point(6.0, 60.0, crs=4326).estimate_local_crs() == gm.CRS('EPSG:32632')
    assert gm.Point(9.0, 78.0, crs=4326).estimate_local_crs() == gm.CRS('EPSG:32633')
    antimeridian = gm.LineString([(179.0, 0.0), (-179.0, 0.0)], crs=4326)
    assert antimeridian.estimate_local_crs().is_projected
    array = gm.GeometryArray([gm.Point(20.5, 51.5), gm.Point(21.5, 52.5)], crs=4326)
    assert array.estimate_local_crs() == gm.CRS('EPSG:32634')
    assert gm.Point(0.0, 89.0, crs=4326).estimate_local_crs().is_projected
    assert gm.Point(0.0, -89.0, crs=4326).estimate_local_crs().is_projected
    with pytest.raises(ValueError, match='present, non-empty'):
        gm.GeometryArray([], crs=4326).estimate_local_crs()


_LOCAL_SURVEY_CRS = json.dumps({
    '$schema': 'https://proj.org/schemas/v0.7/projjson.schema.json',
    'type': 'GeographicCRS',
    'name': 'Local survey geographic',
    'datum': {
        'type': 'GeodeticReferenceFrame',
        'name': 'Local survey datum',
        'ellipsoid': {
            'name': 'Local ellipsoid',
            'semi_major_axis': 6378137,
            'inverse_flattening': 298.257223563,
        },
    },
    'coordinate_system': {
        'subtype': 'ellipsoidal',
        'axis': [
            {
                'name': 'Longitude',
                'abbreviation': 'lon',
                'direction': 'east',
                'unit': 'degree',
            },
            {
                'name': 'Latitude',
                'abbreviation': 'lat',
                'direction': 'north',
                'unit': 'degree',
            },
        ],
    },
})


@pytest.mark.parametrize(
    ('x', 'y', 'z', 'crs', 'expected'),
    [
        (10.0, 51.0, None, 4258, 'EPSG:25832'),
        (10.0, 51.0, 0.0, 4937, 'EPSG:25832'),
        (-74.0, 46.0, None, 4617, 'EPSG:2959'),
        (-74.0, 46.0, 0.0, 4955, 'EPSG:2959'),
        (-74.0, 41.0, None, 4269, 'EPSG:26918'),
        (500000.0, 4500000.0, None, 26918, 'EPSG:26918'),
        (-74.0, 41.0, None, 6318, 'EPSG:6347'),
        (-74.0, 41.0, 0.0, 6319, 'EPSG:6347'),
        (151.2, -33.9, None, 4283, 'EPSG:28356'),
        (151.2, -33.9, 0.0, 4939, 'EPSG:28356'),
        (151.2, -33.9, None, 7844, 'EPSG:7856'),
        (151.2, -33.9, 0.0, 7843, 'EPSG:7856'),
        (151.2, -33.9, None, 4202, 'EPSG:20256'),
        (21.0, 52.0, None, _LOCAL_SURVEY_CRS, None),
        (4.0, 60.0, None, 4258, None),
        (9.0, 78.0, None, 4258, None),
        (6.0, 60.0, None, 4326, 'EPSG:32632'),
        (9.0, 78.0, None, 4326, 'EPSG:32633'),
        (21.0, 52.0, 9.0, 4979, 'EPSG:32634'),
    ],
    ids=[
        'etrs89_2d_germany',
        'etrs89_3d_germany',
        'csrs_2d_quebec',
        'csrs_3d_quebec',
        'nad83_2d_ny',
        'nad83_utm_ny',
        'nad83_2011_2d_ny',
        'nad83_2011_3d_ny',
        'gda94_2d_sydney',
        'gda94_3d_sydney',
        'gda2020_2d_sydney',
        'gda2020_3d_sydney',
        'agd66_sydney',
        'custom_no_id_warsaw',
        'etrs89_norway_source_datum_tm',
        'etrs89_svalbard_source_datum_tm',
        'wgs84_norway',
        'wgs84_svalbard',
        'wgs84_3d_warsaw',
    ],
)
def test_estimate_local_crs_prefers_datum_matched_utm_and_preserves_other_datums(
    x: float, y: float, z: float | None, crs: int | str, expected: str | None
) -> None:
    point = gm.Point(x, y, crs=crs) if z is None else gm.Point(x, y, z=z, crs=crs)
    local = point.estimate_local_crs()
    if expected is not None:
        assert local == gm.CRS(expected)
        return

    source_datum = gm.CRS(crs).info['datum']
    assert local.info['name'] == 'Local conformal CRS'
    assert local.info['datum']['name'] == source_datum['name']


def test_derived_geometries_inherit_the_full_frame() -> None:
    """The frame doctrine: every derived output carries CRS AND epoch, and
    binary ops require one frame (docs/guide/crs.md, 'frame doctrine').
    """
    geom = gm.Point(21.0, 52.0, crs=4326, epoch=2020.0)
    other = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326).with_epoch(2020.0)
    for derived in (geom.buffer(1.0), geom.centroid(), gm.intersection(geom, other)):
        assert str(derived.crs) == str(geom.crs)
        assert derived.epoch == 2020.0
    with pytest.raises(gm.CRSMismatchError):
        gm.distance(geom, other.with_epoch(2010.0, overwrite=True))


def test_to_crs_epoch_survives_exactly_while_the_target_admits_it() -> None:
    """Omitted ``epoch=`` keeps the source epoch when the target admits geometry
    epochs, clears it otherwise, and explicit ``epoch=`` labels the output.
    """
    observed = gm.Point(21.0, 52.0, crs=4326, epoch=2010.0)
    # The WGS 84 ensemble, its derived Web Mercator CRS, and ITRF admit epochs.
    assert observed.to_crs(9000).epoch == 2010.0
    assert observed.to_crs(3857).epoch == 2010.0
    # Static target clears it; the result composes with epoch-free data.
    projected = observed.to_crs(2180)
    assert projected.epoch is None
    plain = gm.Point(360000.0, 500000.0, crs=2180)
    assert gm.distance(projected, plain) >= 0.0
    # Dynamic round trip preserves it end to end.
    assert observed.to_crs(9000).to_crs(4326).epoch == 2010.0
    # Explicit epoch= overrides both ways; same-CRS behavior unchanged.
    assert observed.to_crs(9000, epoch=2025.5).epoch == 2025.5
    assert observed.to_crs(4326).epoch == 2010.0
    # Arrays ride the same frame machinery.
    arr = gm.points([21.0, 22.0], [52.0, 53.0], crs=4326, epoch=2010.0)
    assert arr.to_crs(9000).epoch == 2010.0
    assert arr.to_crs(2180).epoch is None
