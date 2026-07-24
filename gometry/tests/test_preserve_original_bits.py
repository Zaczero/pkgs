import struct

import gometry as gm


def _bits(value: float) -> bytes:
    return struct.pack('>d', value)


def _point_xy_bits(point: gm.Point) -> tuple[bytes, bytes]:
    return (_bits(point.x), _bits(point.y))


def _coords_xy_bits(geom: gm.Geometry) -> list[tuple[bytes, bytes]]:
    return [(_bits(coord[0]), _bits(coord[1])) for coord in geom.coords]


def test_snap_to_grid_and_quantize_preserve_already_grid_aligned_bits() -> None:
    point = gm.Point(-0.0, 1.25)
    assert _point_xy_bits(point.snap_to_grid(0.25)) == _point_xy_bits(point)
    assert _point_xy_bits(point.quantize(2)) == _point_xy_bits(point)

    packed = gm.GeometryArray([gm.Point(-0.0, -0.0), gm.Point(1.25, -0.0)])
    snapped = packed.snap_to_grid(0.25)
    quantized = packed.quantize(2)
    for original, snap_row, quantized_row in zip(
        packed, snapped, quantized, strict=True
    ):
        assert _point_xy_bits(snap_row) == _point_xy_bits(original)
        assert _point_xy_bits(quantized_row) == _point_xy_bits(original)


def test_affine_identity_axes_and_self_origin_packed_identity_preserve_bits() -> None:
    line = gm.LineString([(0.0, -0.0), (1.0, -0.0)])
    translated = line.translate(5.0, 0.0)
    assert [_bits(coord[1]) for coord in translated.coords] == [
        _bits(coord[1]) for coord in line.coords
    ]

    packed = gm.GeometryArray([line])
    rotated = packed.rotate(0.0, origin='center')
    scaled = packed.scale(1.0, origin='center')
    assert rotated[0].to_wkb() == line.to_wkb()
    assert scaled[0].to_wkb() == line.to_wkb()


def test_point_navigation_and_lrs_boundaries_preserve_endpoint_bits() -> None:
    start = gm.Point(-0.0, -0.0, z=5.0, m=-0.0, crs=4326)
    end = gm.Point(1.0, -0.0, z=7.0, m=3.0, crs=4326)
    assert _point_xy_bits(gm.destination(start, 90.0, 0.0)) == _point_xy_bits(start)
    assert _point_xy_bits(
        gm.point_between(start, end, 0.0, normalized=True)
    ) == _point_xy_bits(start)
    assert _point_xy_bits(
        gm.point_between(start, end, 1.0, normalized=True)
    ) == _point_xy_bits(end)

    line = gm.LineString([(-0.0, -0.0, 1.0, -0.0), (1.0, -0.0, 2.0, 10.0)])
    assert _point_xy_bits(line.line_interpolate(0.0)) == _coords_xy_bits(line)[0]
    assert _point_xy_bits(line.line_interpolate(1.0)) == _coords_xy_bits(line)[1]
    assert (
        _point_xy_bits(line.line_interpolate(-0.0, basis='m'))
        == _coords_xy_bits(line)[0]
    )
    assert (
        _point_xy_bits(line.line_interpolate(10.0, basis='m'))
        == _coords_xy_bits(line)[1]
    )
    substring = line.line_substring(-0.0, 10.0, basis='m')
    assert _coords_xy_bits(substring) == _coords_xy_bits(line)


def test_raw_crs_geodesic_direct_and_transform_bounds_identity_preserve_bits() -> None:
    crs = gm.CRS(4326)
    direct = crs.geodesic_direct(-0.0, -0.0, 90.0, 0.0)
    assert _bits(direct['longitude']) == _bits(-0.0)
    assert _bits(direct['latitude']) == _bits(-0.0)

    batch = crs.geodesic_direct([-0.0, 1.0], [-0.0, -0.0], 90.0, [0.0, 0.0])
    assert [_bits(value) for value in batch['longitude']] == [_bits(-0.0), _bits(1.0)]
    assert [_bits(value) for value in batch['latitude']] == [_bits(-0.0), _bits(-0.0)]

    bounds = (-0.0, -0.0, 1.0, 1.0)
    same = gm.crs_transform_bounds(4326, 4326, bounds, densify=0)
    assert tuple(_bits(value) for value in same) == tuple(
        _bits(value) for value in bounds
    )
    many = gm.crs_transform_bounds(4326, 4326, [bounds], densify=0)
    assert tuple(_bits(value) for value in many[0]) == tuple(
        _bits(value) for value in bounds
    )

    polygon = gm.box(-0.0, -0.0, 1.0, 1.0, crs=4326)
    same_crs = polygon.to_crs(
        4326,
        area_of_interest=(-1.0, -1.0, 2.0, 2.0),
        only_best=True,
    )
    assert same_crs.to_wkb() == polygon.to_wkb()


def test_zero_distance_constructive_ops_preserve_original_coordinates() -> None:
    polygon = gm.Polygon(
        [(0.0, -0.0), (1.0, -0.0), (1.0, 1.0), (0.0, 1.0), (0.0, -0.0)], crs=4326
    )
    assert _coords_xy_bits(polygon.buffer(0.0)) == _coords_xy_bits(polygon)

    repeated = gm.LineString([(0.0, -0.0), (0.0, -0.0), (1.0, -0.0)])
    assert _coords_xy_bits(repeated.offset_curve(0.0)) == _coords_xy_bits(repeated)


def test_coverage_simplify_zero_preserves_ring_presentation() -> None:
    left = gm.Polygon([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)])
    right = gm.Polygon([(2.0, 0.0), (2.0, 1.0), (1.0, 1.0), (1.0, 0.0), (2.0, 0.0)])
    out = gm.GeometryArray([left, right]).coverage_simplify(0.0)
    assert [geom.to_wkb() for geom in out] == [left.to_wkb(), right.to_wkb()]
