"""Affine transforms — translate/rotate/scale/skew, explicit matrices,
Z/M preservation, and per-element array semantics.
"""

import math

import gometry as gm
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
    assert apply(make()).coords.to_nested() == pytest.approx(expected)


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


@pytest.mark.parametrize('array', _packed_transform_cases())
@pytest.mark.parametrize('origin', ['centroid', (1.25, -0.5)])
def test_packed_skew_matches_scalar_rows_bitwise(array, origin):
    result = array.skew(12.5, -3.0, origin=origin)
    expected = _scalar_array(
        array,
        lambda geometry: geometry.skew(12.5, -3.0, origin=origin),
    )
    assert _wkb_rows(result) == _wkb_rows(expected)
    assert result.is_missing.tolist() == expected.is_missing.tolist()


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
    with pytest.raises(ValueError):
        (point).affine_transform((1.0, 0.0, 0.0, 1.0, 0.0))
    with pytest.raises(ValueError):
        (point).affine_transform((1.0, 0.0, 0.0, 1.0, 0.0, float('nan')))


def test_reverse_does_not_reuse_direction_dependent_prepared_rows() -> None:
    line = gm.LineString([(float(i), float(i % 3)) for i in range(32)])
    array = gm.GeometryArray([line])

    # Prime the packed-row prepared cache before reversing its coordinates.
    assert array.line_interpolate(0)[0] == gm.Point(0, 0)
    reversed_array = array.reverse()

    assert reversed_array.line_interpolate(0)[0] == gm.Point(31, 1)
