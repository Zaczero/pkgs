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
    assert _point_xy_bits(start.destination(90.0, 0.0)) == _point_xy_bits(start)
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


def test_geographic_q_union_preserves_a_retained_source_point_latitude() -> None:
    seam_polygon = gm.Polygon(
        [
            (170.0, 2.0),
            (-170.0, 2.0),
            (-165.0, 12.0),
            (160.0, 10.0),
            (170.0, 2.0),
        ],
        crs=4326,
    )
    source_point = gm.Point(0.0, -0.0, crs=4326)

    union = gm.union_all([seam_polygon, source_point])

    assert union.geometry_type == 'GeometryCollection'
    point_parts = [part for part in union.parts if part.geometry_type == 'Point']
    assert len(point_parts) == 1
    assert _bits(point_parts[0].y) == _bits(source_point.y)


def test_line_interpolation_preserves_equal_subnormal_ordinates() -> None:
    vertical = gm.from_wkt('LINESTRING (5e-324 -1, 5e-324 2)')
    vertical_midpoint = vertical.line_interpolate(0.5, normalized=True)
    assert _bits(vertical_midpoint.x) == _bits(5e-324)
    assert _bits(vertical_midpoint.y) == _bits(0.5)

    horizontal = gm.from_wkt('LINESTRING (0 5e-324, 2 5e-324)')
    horizontal_midpoint = horizontal.line_interpolate(0.5, normalized=True)
    assert _bits(horizontal_midpoint.x) == _bits(1.0)
    assert _bits(horizontal_midpoint.y) == _bits(5e-324)


def test_line_interpolation_preserves_equal_ordinary_ordinates() -> None:
    line = gm.LineString([(0.0, 0.1), (1.0, 0.1)])
    point = line.line_interpolate(0.2, normalized=True)

    assert _bits(point.y) == _bits(0.1)


def test_zero_distance_destination_and_identity_bounds_use_frame_canonicalization() -> (
    None
):
    origin = gm.Point(-0.0, -0.0, crs=4326)
    direct = origin.destination(90.0, 0.0)
    assert _bits(direct.x) == _bits(-0.0)
    assert _bits(direct.y) == _bits(-0.0)

    batch = gm.points([-0.0, 1.0], [-0.0, -0.0], crs=4326).destination(90.0, [0.0, 0.0])
    assert [_bits(point.x) for point in batch] == [_bits(-0.0), _bits(1.0)]
    assert [_bits(point.y) for point in batch] == [_bits(-0.0), _bits(-0.0)]

    bounds = (-0.0, -0.0, 1.0, 1.0)
    same = gm.crs_transform_bounds(4326, 4326, bounds, densify=0)
    assert tuple(_bits(value) for value in same) == tuple(
        _bits(value) for value in (0.0, 0.0, 1.0, 1.0)
    )
    many = gm.crs_transform_bounds(4326, 4326, [bounds], densify=0)
    assert tuple(_bits(value) for value in many[0]) == tuple(
        _bits(value) for value in (0.0, 0.0, 1.0, 1.0)
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
    out = gm.coverage_simplify(gm.GeometryArray([left, right]), 0.0)
    assert [geom.to_wkb() for geom in out] == [left.to_wkb(), right.to_wkb()]
