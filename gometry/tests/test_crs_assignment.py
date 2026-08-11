"""CRS behavior — geodesic measurement, transforms, best-UTM selection,
runtime config, cache info, and the PROJ authority metadata surface.
"""

import gometry as gm
import pytest


def test_set_crs_assigns_and_to_crs_transforms_web_mercator() -> None:
    point = gm.Point(1, 2).set_crs(4326)
    points = (gm.points([1.0, 2.0], [2.0, 3.0])).set_crs(4326)
    measured = gm.Point(1, 2, z=3, m=4, crs=4326)
    epoch_point = gm.Point(1, 2, z=3, crs=4979, epoch=2010.0)
    epoch_points = gm.points([1.0, 2.0], [2.0, 3.0], crs=4326, epoch=2010.0)
    dynamic_epoch_a = gm.Point(
        3657660.66, 255768.55, z=5201382.11, crs=7789, epoch=2010.0
    )
    dynamic_epoch_b = gm.Point(
        3657660.66, 255768.55, z=5201382.11, crs=7789, epoch=2020.0
    )
    assert point.crs == 'EPSG:4326'
    assert points.crs == 'EPSG:4326'
    assert point.epoch is None
    assert epoch_point.epoch == 2010.0
    assert epoch_points.epoch == 2010.0
    assert [item.epoch for item in epoch_points] == [2010.0, 2010.0]
    with pytest.raises(ValueError, match='one shared coordinate epoch'):
        gm.GeometryArray([point, gm.Point(1, 2, crs=4326, epoch=2010.0)])
    with pytest.raises(ValueError, match='one shared coordinate epoch'):
        gm.GeometryArray([dynamic_epoch_a, dynamic_epoch_b])
    assert gm.GeometryArray([point], epoch=2020.0)[0].epoch == 2020.0
    assert point.to_crs('EPSG:4326').crs == 'EPSG:4326'
    assert (points).to_crs(3857).crs == 'EPSG:3857'
    dynamic_epoch_array = gm.GeometryArray([dynamic_epoch_a, dynamic_epoch_a]).to_crs(
        7660
    )
    web_mercator = point.to_crs('epsg:3857')
    recovered = web_mercator.to_crs(4326)
    measured_web = measured.to_crs(3857)
    epoch_geocentric = epoch_point.to_crs(4978)
    epoch_target = epoch_point.to_crs(4978, epoch=2020.0)
    assert web_mercator.crs == 'EPSG:3857'
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
    # Dynamic-aware epoch policy: both targets are dynamic frames, so the
    # source epoch survives the transform.
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


def test_set_epoch_assigns_clears_and_guards_retag() -> None:
    point = gm.Point(1, 2, crs=4326)
    stamped = point.set_epoch(2020.0)
    assert stamped.epoch == 2020.0
    assert str(stamped) == 'POINT (1 2)'
    assert stamped.crs == 'EPSG:4326'
    assert stamped.set_epoch(None).epoch is None
    with pytest.raises(ValueError, match='set_epoch would change the coordinate epoch'):
        stamped.set_epoch(2024.0)
    assert stamped.set_epoch(2024.0, overwrite=True).epoch == 2024.0
    assert stamped.set_epoch(2020.0).epoch == 2020.0
    with pytest.raises(ValueError, match='finite decimal year'):
        point.set_epoch(float('inf'))
    assert (gm.Point(0, 0, crs=4326)).set_epoch(2021.5).epoch == 2021.5
    array = gm.GeometryArray([
        gm.Point(0, 0, crs=4326),
        gm.Point(1, 1, crs=4326),
    ]).set_epoch(2015.0)
    assert array.epoch == 2015.0
    assert [item.epoch for item in array] == [2015.0, 2015.0]
    assert array.set_epoch(None).epoch is None


def test_epoch_requires_a_crs_and_set_crs_none_clears_it() -> None:
    with pytest.raises(
        ValueError,
        match=r'^a coordinate epoch requires a CRS; attach one with crs= \(or set_crs\(\.\.\.\)\) before tagging an epoch$',
    ):
        gm.Point(0, 0, epoch=2020.0)
    with pytest.raises(
        ValueError,
        match=r'^a coordinate epoch requires a CRS; attach one with crs= \(or set_crs\(\.\.\.\)\) before tagging an epoch$',
    ):
        gm.Point(0, 0).set_epoch(2020.0)
    with pytest.raises(
        ValueError,
        match=r'^a coordinate epoch requires a CRS; attach one with crs= \(or set_crs\(\.\.\.\)\) before tagging an epoch$',
    ):
        gm.GeometryArray([gm.Point(0, 0)], epoch=2020.0)
    with pytest.raises(
        ValueError,
        match=r'^a coordinate epoch requires a CRS; attach one with crs= \(or set_crs\(\.\.\.\)\) before tagging an epoch$',
    ):
        gm.points([0], [0]).set_epoch(2020.0)
    assert gm.Point(10, 20, epoch=2020.0, crs=4326).epoch == 2020.0
    stamped = gm.Point(0, 0, crs=4326, epoch=2020.0)
    cleared = stamped.set_crs(None)
    assert cleared.crs is None and cleared.epoch is None
    assert stamped.set_crs(3857, overwrite=True).epoch == 2020.0
    array = gm.GeometryArray([gm.Point(0, 0, crs=4326)]).set_epoch(2020.0)
    cleared_array = array.set_crs(None)
    assert cleared_array.crs is None and cleared_array.epoch is None
    assert [item.epoch for item in cleared_array] == [None]
    assert gm.Point(0, 0, crs=4326).set_epoch(-0.0).epoch == 0.0
    point = gm.Point(1, 2, crs=4326)
    with pytest.raises(TypeError):
        point.to_crs(3857, source_epoch=2020.0)
    with pytest.raises(TypeError):
        point.to_crs(3857, target_epoch=2020.0)
    assert point.set_epoch(2020.0).to_crs(4326).epoch == 2020.0
    assert point.set_epoch(2010.0).to_crs(4978, epoch=2020.0).epoch == 2020.0
    assert point.set_epoch(2020.0).to_crs(4326, only_best=True).epoch == 2020.0
    assert point.set_epoch(2020.0).to_crs(3857).epoch == 2020.0
    stamped_array = gm.GeometryArray([gm.Point(0, 0, crs=4326)]).set_epoch(2020.0)
    assert stamped_array.to_crs(4326, only_best=True).epoch == 2020.0
    assert stamped_array.to_crs(3857).epoch == 2020.0


def test_epoch_requires_a_dynamic_crs_at_the_shared_frame_owner(
    capsys: pytest.CaptureFixture[str],
) -> None:
    message = r'^a coordinate epoch requires a dynamic CRS; EPSG:2180 is static\. Remove epoch= or transform to a dynamic CRS first$'
    for make in (
        lambda: gm.Point(1.0, 2.0, crs=2180, epoch=2020.0),
        lambda: gm.GeometryArray([gm.Point(1.0, 2.0)], crs=2180, epoch=2020.0),
        lambda: gm.points([1.0], [2.0], crs=2180, epoch=2020.0),
        lambda: gm.box(0.0, 0.0, 1.0, 1.0, crs=2180, epoch=2020.0),
        lambda: gm.Point(1.0, 2.0, crs=2180).set_epoch(2020.0),
    ):
        with pytest.raises(ValueError, match=message):
            make()

    observed = gm.Point(21.0, 52.0, crs=4326, epoch=2020.0)
    with pytest.raises(ValueError, match=message):
        observed.to_crs(2180, epoch=2025.5)
    with pytest.raises(ValueError, match=message):
        observed.set_crs(2180, overwrite=True)
    assert capsys.readouterr().err == ''

    # The WGS84 ensemble and true dynamic frames remain usable end to end.
    assert observed.to_crs(9000).epoch == 2020.0
    assert observed.to_crs(2180).epoch is None


def test_estimate_local_crs_normalizes_frames_and_preserves_datums() -> None:
    geom = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
    assert geom.estimate_local_crs() == 'EPSG:32634'
    assert geom.to_crs(3857).estimate_local_crs() == 'EPSG:32634'
    assert geom.to_crs(32634).estimate_local_crs() == 'EPSG:32634'
    assert gm.Point(21, 52, z=9, crs=4979).estimate_local_crs() == 'EPSG:32634'
    assert gm.Point(530000, 180000, crs=27700).estimate_local_crs() == 'EPSG:32630'
    assert gm.Point(-74.0, 41.0, crs=4269).estimate_local_crs() == 'EPSG:26918'
    assert gm.Point(500000, 4500000, crs=26918).estimate_local_crs() == 'EPSG:26918'
    assert gm.Point(6.0, 60.0, crs=4326).estimate_local_crs() == 'EPSG:32632'
    assert gm.Point(9.0, 78.0, crs=4326).estimate_local_crs() == 'EPSG:32633'
    antimeridian = gm.LineString([(179.0, 0.0), (-179.0, 0.0)], crs=4326)
    assert antimeridian.estimate_local_crs().is_projected
    array = gm.GeometryArray([gm.Point(20.5, 51.5), gm.Point(21.5, 52.5)], crs=4326)
    assert array.estimate_local_crs() == 'EPSG:32634'
    assert gm.Point(0.0, 89.0, crs=4326).estimate_local_crs().is_projected
    assert gm.Point(0.0, -89.0, crs=4326).estimate_local_crs().is_projected
    with pytest.raises(ValueError, match='present, non-empty'):
        gm.GeometryArray([], crs=4326).estimate_local_crs()


def test_derived_geometries_inherit_the_full_frame() -> None:
    """The frame doctrine: every derived output carries CRS AND epoch, and
    binary ops require one frame (docs/guide/crs.md, 'frame doctrine').
    """
    geom = gm.Point(21.0, 52.0, crs=4326, epoch=2020.0)
    other = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326).set_epoch(2020.0)
    for derived in (geom.buffer(1.0), geom.centroid(), gm.intersection(geom, other)):
        assert str(derived.crs) == str(geom.crs)
        assert derived.epoch == 2020.0
    with pytest.raises(gm.CRSMismatchError):
        gm.distance(geom, other.set_epoch(2010.0, overwrite=True))


def test_to_crs_epoch_survives_exactly_while_the_frame_is_dynamic() -> None:
    """Dynamic-aware epoch policy: omitted ``epoch=`` keeps the source epoch
    when the target CRS is dynamic (WGS84 ensemble, ITRF), auto-clears it on a
    static target (plate-fixed national frames), and an explicit ``epoch=``
    always names the output epoch.
    """
    observed = gm.Point(21.0, 52.0, crs=4326, epoch=2010.0)
    # Dynamic targets keep the epoch (4326 ensemble -> ITRF2014 -> webmerc).
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
