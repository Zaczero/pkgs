"""Affine transforms — translate/rotate/scale/skew, explicit matrices,
Z/M preservation, and per-element array semantics.
"""

import math
import struct
import sys

import gometry as gm
import numpy as np
import pytest


@pytest.mark.parametrize(
    ('apply', 'make', 'expected'),
    [
        pytest.param(
            lambda g: (g).translate(3.0, -4.0),
            lambda: gm.Point(1.0, 2.0),
            [4.0, -2.0],
            id='translate',
        ),
        pytest.param(
            lambda g: (g).rotate(90.0, origin=(0.0, 0.0)),
            lambda: gm.Point(1.0, 0.0),
            [0.0, 1.0],
            id='rotate-degrees',
        ),
        pytest.param(
            lambda g: (g).rotate(math.pi / 2.0, origin=(0.0, 0.0), radians=True),
            lambda: gm.Point(1.0, 0.0),
            [0.0, 1.0],
            id='rotate-radians',
        ),
        pytest.param(
            lambda g: (g).scale(2.0, 3.0, origin=(0.0, 0.0)),
            lambda: gm.Point(1.0, 1.0),
            [2.0, 3.0],
            id='scale-xy',
        ),
        pytest.param(
            lambda g: (g).scale(2.0, origin=(0.0, 0.0)),
            lambda: gm.Point(2.0, 3.0),
            [4.0, 6.0],
            id='scale-uniform-yfact-defaults',
        ),
        pytest.param(
            lambda g: (g).skew(45.0, 0.0, origin=(0.0, 0.0)),
            lambda: gm.LineString([(0, 0), (1, 2)]),
            [(0.0, 0.0), (3.0, 2.0)],
            id='skew-x',
        ),
        pytest.param(
            lambda g: (g).affine_transform((0.0, -1.0, 1.0, 0.0, 0.0, 0.0)),
            lambda: gm.Point(2.0, 3.0),
            [-3.0, 2.0],
            id='affine-matrix',
        ),
    ],
)
def test_affine_basics_move_coordinates(apply, make, expected):
    np.testing.assert_allclose(
        apply(make()).coords.to_nested(), expected, rtol=1e-6, atol=1e-12
    )


def test_rotate_default_origin_is_centroid():
    square = gm.Polygon([(0, 0), (2, 0), (2, 2), (0, 2), (0, 0)])
    result = (square).rotate(90.0)
    assert result.bounds == pytest.approx((0.0, 0.0, 2.0, 2.0))


def test_rotate_preserves_z_and_m():
    point = gm.Point(1.0, 0.0, z=3.0, m=4.0)
    result = (point).rotate(90.0, origin=(0.0, 0.0))
    assert result.coordinate_axes == 'XYZM'
    assert result.z == pytest.approx(3.0)
    assert result.m == pytest.approx(4.0)
    assert result.coords.to_nested() == [
        pytest.approx(0.0),
        pytest.approx(1.0),
        pytest.approx(3.0),
        pytest.approx(4.0),
    ]


def test_translate_preserves_z_and_m():
    point = gm.Point(1.0, 2.0, z=3.0, m=4.0)
    result = (point).translate(10.0, 20.0)
    assert result.coordinate_axes == 'XYZM'
    assert result.z == pytest.approx(3.0)
    assert result.m == pytest.approx(4.0)
    assert result.coords.to_nested()[:2] == [pytest.approx(11.0), pytest.approx(22.0)]


def test_affine_parameter_names_are_descriptive() -> None:
    point = gm.Point(1, 2)
    assert point.translate(x_offset=3, y_offset=4).coords.to_nested() == [4.0, 6.0]
    assert point.scale(x_factor=2, y_factor=3, origin=(0, 0)).coords.to_nested() == [
        2.0,
        6.0,
    ]
    assert point.skew(x_angle=0, y_angle=0).coords.to_nested() == [1.0, 2.0]

    with pytest.raises(TypeError):
        point.translate(xoff=3, yoff=4)
    with pytest.raises(TypeError):
        point.scale(xfact=2)
    with pytest.raises(TypeError):
        point.skew(xs=1)


def test_affine_origins_accept_any_two_value_iterable() -> None:
    point = gm.Point(1, 2)
    assert point.scale(2, origin=[0, 0]).coords.to_nested() == [2.0, 4.0]
    assert point.rotate(0, origin=iter([0, 0])) == point
    assert point.skew(origin=(value for value in (0, 0))) == point


def test_affine_array_transforms_each_element():
    array = gm.GeometryArray([gm.Point(1.0, 0.0), gm.Point(0.0, 1.0)])
    result = (array).translate(1.0, 1.0)
    assert [geometry.coords.to_nested() for geometry in result] == [
        [pytest.approx(2.0), pytest.approx(1.0)],
        [pytest.approx(1.0), pytest.approx(2.0)],
    ]


def _wkb_rows(array):
    return [None if value is None else bytes(value) for value in array.to_wkb()]


def _scalar_array(array, transform):
    return gm.GeometryArray([
        None if geometry is None else transform(geometry) for geometry in array
    ])


def _packed_transform_cases():
    return [
        gm.points(
            [1.0, -2.0, 99.0],
            [3.0, -4.0, 99.0],
            z=[5.0, 6.0, 99.0],
            m=[7.0, 8.0, 99.0],
            crs=3857,
        )._with_missing([False, False, True]),
        gm.GeometryArray(
            [
                gm.LineString(
                    [(0.0, 0.0), (1.0, 2.0), (3.0, 5.0)],
                    z=[10.0, 11.0, 12.0],
                    m=[20.0, 21.0, 22.0],
                    crs=3857,
                ),
                gm.LineString(
                    [(-1.0, 4.0), (2.0, 8.0), (4.0, 9.0)],
                    z=[13.0, 14.0, 15.0],
                    m=[23.0, 24.0, 25.0],
                    crs=3857,
                ),
                None,
            ],
        ),
        gm.GeometryArray(
            [
                gm.Polygon(
                    [(0.0, 0.0), (2.0, 0.0), (2.0, 1.0), (0.0, 1.0), (0.0, 0.0)],
                    z=[1.0, 2.0, 3.0, 4.0, 1.0],
                    m=[5.0, 6.0, 7.0, 8.0, 5.0],
                    crs=3857,
                ),
                gm.Polygon(
                    [(4.0, 1.0), (7.0, 1.0), (7.0, 3.0), (4.0, 3.0), (4.0, 1.0)],
                    z=[9.0, 10.0, 11.0, 12.0, 9.0],
                    m=[13.0, 14.0, 15.0, 16.0, 13.0],
                    crs=3857,
                ),
                None,
            ],
        ),
    ]


@pytest.mark.parametrize('array', _packed_transform_cases())
def test_packed_translate_matches_scalar_rows_bitwise(array):
    result = array.translate(0.25, -1.5)
    expected = _scalar_array(array, lambda geometry: geometry.translate(0.25, -1.5))
    assert _wkb_rows(result) == _wkb_rows(expected)
    assert result.is_missing.tolist() == expected.is_missing.tolist()


@pytest.mark.parametrize(
    'transform',
    [
        lambda geometry, origin: geometry.rotate(30.0, origin=origin),
        lambda geometry, origin: geometry.scale(1.5, 0.75, origin=origin),
        lambda geometry, origin: geometry.skew(12.5, -3.0, origin=origin),
    ],
    ids=['rotate', 'scale', 'skew'],
)
@pytest.mark.parametrize('array', _packed_transform_cases())
@pytest.mark.parametrize('origin', ['centroid', (1.25, -0.5)])
def test_packed_origin_transforms_match_scalar_rows_bitwise(transform, array, origin):
    result = transform(array, origin)
    expected = _scalar_array(
        array,
        lambda geometry: transform(geometry, origin),
    )
    assert _wkb_rows(result) == _wkb_rows(expected)
    assert result.is_missing.tolist() == expected.is_missing.tolist()


@pytest.mark.parametrize('axes', ['XY', 'XYZ', 'XYM', 'XYZM'])
@pytest.mark.parametrize('selection', [slice(None), slice(1, 3), [2, 0, 2]])
@pytest.mark.parametrize(
    'transform',
    [
        lambda geometry: geometry.scale(1.5, 0.75, origin=(1.25, -0.5)),
        lambda geometry: geometry.rotate(30.0, origin=(1.25, -0.5)),
        lambda geometry: geometry.skew(12.5, -3.0, origin=(1.25, -0.5)),
        lambda geometry: geometry.scale(1.0, 2.0, origin=(0.0, 0.0)),
    ],
    ids=['scale', 'rotate', 'skew', 'identity-x'],
)
def test_fixed_origin_point_selection_matches_scalar_bits(axes, selection, transform):
    array = gm.points(
        [-0.0, 1.0, -2.0, 8.0],
        [3.0, -0.0, 7.0, -5.0],
        z=[4.0, 5.0, 6.0, 7.0] if 'Z' in axes else None,
        m=[8.0, 9.0, 10.0, 11.0] if 'M' in axes else None,
    )[selection]
    expected = _scalar_array(array, transform)
    result = transform(array)
    del array
    assert _wkb_rows(result) == _wkb_rows(expected)


@pytest.mark.parametrize('auxiliary_first', [False, True])
def test_fixed_origin_points_report_first_invalid_row(auxiliary_first):
    overflow = [sys.float_info.max, 0.0, 1.0]
    invalid_z = [1.0, 2.0, math.nan]
    errors = [invalid_z, overflow] if auxiliary_first else [overflow, invalid_z]
    array = gm.points([1.0, 2.0, 3.0], [3.0, 4.0, 5.0], z=[5.0, 6.0, 7.0])
    array = array.with_coordinates([[1.0, 2.0, 3.0], *errors])
    with pytest.raises(
        gm.InvalidGeometryError, match='coordinates must be finite'
    ) as error:
        array.scale(2.0, 2.0, origin=(0.0, 0.0))
    assert error.value.__notes__ == ['while processing array element 1']


def test_scale_array_uses_per_element_centroid():
    array = gm.GeometryArray([
        gm.Polygon([(0, 0), (2, 0), (2, 2), (0, 2), (0, 0)]),
        gm.Polygon([(10, 10), (12, 10), (12, 12), (10, 12), (10, 10)]),
    ])
    result = (array).scale(2.0)
    assert result[0].bounds == pytest.approx((-1.0, -1.0, 3.0, 3.0))
    assert result[1].bounds == pytest.approx((9.0, 9.0, 13.0, 13.0))


def test_affine_transform_rejects_bad_matrix():
    point = gm.Point(1.0, 1.0)
    with pytest.raises(gm.GeometryError):
        (point).affine_transform((1.0, 0.0, 0.0, 1.0, 0.0))
    with pytest.raises(gm.GeometryError):
        (point).affine_transform((1.0, 0.0, 0.0, 1.0, 0.0, float('nan')))


def test_reverse_does_not_reuse_direction_dependent_prepared_rows() -> None:
    line = gm.LineString([(float(i), float(i % 3)) for i in range(32)])
    array = gm.GeometryArray([line])

    # Prime the packed-row prepared cache before reversing its coordinates.
    assert array.line_interpolate(0)[0] == gm.Point(0, 0)
    reversed_array = array.reverse()

    assert reversed_array.line_interpolate(0)[0] == gm.Point(31, 1)


def _float_bits(value):
    return struct.pack('>d', value)


def test_origin_scale_rounds_finite_cancellation_exactly():
    result = gm.Point(1.0, 1.0).scale(2.0, 2.0, origin=(1e16, 1e16))
    assert result.coords.to_nested()[:2] == [-9999999999999998.0, -9999999999999998.0]


def test_origin_scale_preserves_identity_row_and_signed_zero():
    point = gm.Point(-0.0, 3.0).scale(1.0, 2.0, origin=(1e16, 0.0))
    assert point.x == 0.0
    assert _float_bits(point.x) == _float_bits(-0.0)

    point = gm.Point(1.0, -0.0).scale(2.0, 1.0, origin=(0.0, 0.0))
    packed = gm.GeometryArray([point]).scale(2.0, 1.0, origin=(0.0, 0.0))[0]
    assert _float_bits(point.y) == _float_bits(-0.0)
    assert _float_bits(packed.y) == _float_bits(-0.0)

    line = gm.LineString([(-0.0, -0.0), (1.0, -0.0)])
    result = line.scale(2.0, 1.0, origin=(0.0, 0.0))
    assert [_float_bits(xy[1]) for xy in result.coords] == [
        _float_bits(-0.0),
        _float_bits(-0.0),
    ]


def test_origin_scale_ties_to_even_at_binary64_midpoint():
    upper = math.nextafter(1.0, math.inf)
    result = gm.Point(1.0, 0.0).scale(0.5, 1.0, origin=(upper, 0.0))
    assert _float_bits(result.x) == _float_bits(1.0)


def test_origin_scale_rounds_just_above_midpoint_up():
    factor = 0.0009765624999999985
    origin = 0.0009765625000000015
    result = gm.Point(2.0, 0.0).scale(factor, 1.0, origin=(origin, 0.0))
    assert _float_bits(result.x) == bytes.fromhex('3f67fdfffffffffd')


def test_origin_scale_negative_subnormal_underflow_keeps_sign():
    minimum_subnormal = float.fromhex('0x0.0000000000001p-1022')
    result = gm.Point(-minimum_subnormal, 0.0).scale(0.5, 1.0, origin=(0.0, 0.0))
    assert _float_bits(result.x) == _float_bits(-0.0)


def test_origin_scale_overflow_midpoint_is_rejected():
    maximum = sys.float_info.max
    previous = math.nextafter(maximum, -math.inf)
    with pytest.raises(gm.InvalidGeometryError):
        gm.Point(maximum, 0.0).scale(1.5, 1.0, origin=(previous, 0.0))


def test_origin_scale_maximum_self_pivot_matches_packed_lane():
    maximum = sys.float_info.max
    point = gm.Point(maximum, maximum)
    scalar = point.scale(2.0, 2.0, origin=(maximum, maximum))
    packed = gm.GeometryArray([point]).scale(2.0, 2.0, origin=(maximum, maximum))[0]
    assert scalar.to_wkb() == packed.to_wkb()
    assert scalar.coords.to_nested()[:2] == [maximum, maximum]


@pytest.mark.parametrize(
    'transform',
    [
        lambda geometry, pivot: geometry.rotate(90.0, origin=pivot),
        lambda geometry, pivot: geometry.scale(2.0, 2.0, origin=pivot),
        lambda geometry, pivot: geometry.skew(45.0, 0.0, origin=pivot),
    ],
    ids=['rotate', 'scale', 'skew'],
)
def test_origin_transforms_keep_maximum_self_pivot_in_packed_lane(transform):
    maximum = sys.float_info.max
    point = gm.Point(maximum, maximum)
    scalar = transform(point, (maximum, maximum))
    packed = transform(gm.GeometryArray([point]), (maximum, maximum))[0]
    assert scalar.to_wkb() == packed.to_wkb()
    assert scalar.coords.to_nested()[:2] == [maximum, maximum]


@pytest.mark.parametrize(
    'transform',
    [
        lambda geometry: geometry.rotate(30.0, origin=(1.25, -0.5)),
        lambda geometry: geometry.scale(1.5, 0.75, origin=(1.25, -0.5)),
        lambda geometry: geometry.skew(12.5, -3.0, origin=(1.25, -0.5)),
    ],
    ids=['rotate', 'scale', 'skew'],
)
def test_origin_transforms_match_packed_and_mixed_rows(transform):
    line = gm.LineString([(0.1, 0.1), (0.9, 0.9), (1.1, -2.3)])
    packed = transform(gm.GeometryArray([line]))[0]
    mixed = transform(gm.GeometryArray([line, gm.Point(100.0, 100.0)]))[0]
    assert packed.to_wkb() == mixed.to_wkb()
