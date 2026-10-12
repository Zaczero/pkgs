"""Public geographic mesh publication regressions."""

from __future__ import annotations

import math

import gometry as gm


def _wgs84_chart_fraction(
    start: tuple[float, float, float, float],
    end: tuple[float, float, float, float],
    parameter: float,
) -> float:
    """Independent ellipsoid-metric integral along the retained chart edge."""
    semimajor = 6_378_137.0
    flattening = 1 / 298.257_223_563
    eccentricity_squared = flattening * (2 - flattening)
    longitude_delta = math.radians(end[0] - start[0])
    latitude_delta = math.radians(end[1] - start[1])

    def speed(t: float) -> float:
        latitude = math.radians(start[1] + (end[1] - start[1]) * t)
        denominator = 1 - eccentricity_squared * math.sin(latitude) ** 2
        meridional_radius = semimajor * (1 - eccentricity_squared) / denominator**1.5
        prime_vertical_radius = semimajor / math.sqrt(denominator)
        return math.hypot(
            meridional_radius * latitude_delta,
            prime_vertical_radius * math.cos(latitude) * longitude_delta,
        )

    def length(end_parameter: float) -> float:
        intervals = 4096
        step = end_parameter / intervals
        weighted = speed(0.0) + speed(end_parameter)
        weighted += 4 * sum(speed(index * step) for index in range(1, intervals, 2))
        weighted += 2 * sum(speed(index * step) for index in range(2, intervals, 2))
        return weighted * step / 3

    return length(parameter) / length(1.0)


def test_geographic_earcut_receipt_paths_keep_selected_records() -> None:
    polygon = gm.from_wkt(
        "POLYGON ZM ((10 0 1 11,14 0 2 12,14 4 3 13,10 4 4 14,10 0 1 11),"
        "(11 1 5 15,12 1 6 16,12 2 7 17,11 2 8 18,11 1 5 15))",
        crs=4326,
    )
    source_records = {
        (10.0, 0.0, 1.0, 11.0),
        (14.0, 0.0, 2.0, 12.0),
        (14.0, 4.0, 3.0, 13.0),
        (10.0, 4.0, 4.0, 14.0),
        (11.0, 1.0, 5.0, 15.0),
        (12.0, 1.0, 6.0, 16.0),
        (12.0, 2.0, 7.0, 17.0),
        (11.0, 2.0, 8.0, 18.0),
    }
    scalar = polygon.triangulate(method='earcut')
    grouped = gm.GeometryArray([polygon, polygon]).triangulate(method='earcut')

    assert len(scalar) == 8
    for row in (scalar, grouped[0], grouped[1]):
        assert len(row) == 8
        for triangle in row:
            assert triangle.coordinate_axes == 'XYZM'
            ring = list(triangle.exterior.coords)
            assert ring[0] == ring[-1]
            assert all(tuple(coordinate) in source_records for coordinate in ring)


def test_mixed_axis_delaunay_keeps_retained_generated_roots() -> None:
    records = [
        (-170.0, -10.0, 1.0, 101.0),
        (-60.0, 30.0, 2.0, 102.0),
        (60.0, -20.0, 3.0, 103.0),
        (170.0, 40.0, 4.0, 104.0),
        (0.0, 5.0, 5.0, 105.0),
    ]
    source = gm.from_wkt(
        'MULTIPOINT ZM ((-170 -10 1 101),(-60 30 2 102),'
        '(60 -20 3 103),(170 40 4 104),(0 5 5 105))',
        crs=4326,
    )
    xy_row = gm.MultiPoint([(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)], crs=4326)
    grouped = gm.GeometryArray([source, xy_row]).triangulate(method='delaunay')
    triangles = grouped[0]

    source_xy = {(record[0], record[1]) for record in records}
    start, end = records[1], records[3]
    fraction = _wgs84_chart_fraction(start, end, 0.5)
    observed = set()
    assert len(triangles) == 3
    for triangle in triangles:
        for longitude, latitude, z, m in triangle.exterior.coords:
            if (longitude, latitude) in source_xy:
                continue
            assert (longitude, latitude) == (55.0, 35.0)
            assert math.isclose(
                z,
                start[2] + (end[2] - start[2]) * fraction,
                rel_tol=0.0,
                abs_tol=1e-10,
            )
            assert math.isclose(
                m,
                start[3] + (end[3] - start[3]) * fraction,
                rel_tol=0.0,
                abs_tol=1e-10,
            )
            observed.add((abs(longitude), latitude))
    assert observed == {(55.0, 35.0)}
