"""Full ``map_coordinates`` matrix: scalar/array x axes x errors x masking.

The single happy-path shapely-parity test in ``test_coordinates.py`` stays; this
module is the trust-boundary battery for the Python callback.
"""

from __future__ import annotations

import gometry as gm
import numpy as np
import pytest


def _shift(coords: np.ndarray) -> np.ndarray:
    out = np.array(coords, dtype=np.float64, copy=True)
    out[:, 0] += 10.0
    out[:, 1] += 20.0
    return out


@pytest.mark.parametrize(
    ('wkt', 'width'),
    [
        ('LINESTRING (0 0, 1 2, 3 4)', 2),
        ('LINESTRING Z (0 0 1, 1 2 3, 3 4 5)', 3),
        ('LINESTRING M (0 0 10, 1 2 20, 3 4 30)', 3),
        ('LINESTRING ZM (0 0 1 10, 1 2 3 20, 3 4 5 30)', 4),
    ],
    ids=['XY', 'XYZ', 'XYM', 'XYZM'],
)
def test_map_coordinates_scalar_preserves_axes(wkt: str, width: int) -> None:
    geom = gm.from_wkt(wkt)
    seen: list[tuple[int, ...]] = []

    def cb(coords: np.ndarray) -> np.ndarray:
        seen.append(coords.shape)
        assert coords.dtype == np.float64
        assert coords.ndim == 2
        assert coords.shape[1] == width
        # Callback input is a writable snapshot (caller may copy-edit).
        return coords + 1.0

    out = geom.map_coordinates(cb)
    assert out.coordinate_axes == geom.coordinate_axes
    assert seen == [(3, width)]
    assert out.to_wkt() != geom.to_wkt()
    # First vertex shifted by +1 on every ordinate
    assert next(iter(out.coords))[0] == pytest.approx(next(iter(geom.coords))[0] + 1.0)


@pytest.mark.parametrize(
    'wkt',
    [
        'LINESTRING (0 0, 1 1)',
        'LINESTRING Z (0 0 1, 1 1 2)',
        'LINESTRING M (0 0 10, 1 1 20)',
        'LINESTRING ZM (0 0 1 10, 1 1 2 20)',
    ],
    ids=['XY', 'XYZ', 'XYM', 'XYZM'],
)
def test_map_coordinates_array_uniform_axes(wkt: str) -> None:
    g0 = gm.from_wkt(wkt)
    g1 = gm.from_wkt(wkt).translate(5.0, 5.0)
    arr = gm.GeometryArray([g0, g1])
    shapes: list[tuple[int, ...]] = []

    def cb(coords: np.ndarray) -> np.ndarray:
        shapes.append(coords.shape)
        return coords * 2.0

    out = arr.map_coordinates(cb)
    assert len(out) == 2
    assert out.common_coordinate_axes == g0.coordinate_axes
    # Array path packs all vertices into one callback matrix.
    assert len(shapes) == 1
    assert shapes[0][0] == 4  # 2 geoms x 2 vertices
    assert shapes[0][1] == {'XY': 2, 'XYZ': 3, 'XYM': 3, 'XYZM': 4}[g0.coordinate_axes]


def test_map_coordinates_array_masked_rows() -> None:
    arr = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (1.0, 1.0)]),
        None,
        gm.Point(2.0, 3.0),
        None,
    ])
    out = arr.map_coordinates(_shift)
    assert out[1] is None and out[3] is None
    assert out[0].to_wkt() == 'LINESTRING (10 20, 11 21)'
    assert out[2].to_wkt() == 'POINT (12 23)'
    assert int(out.is_missing.sum()) == 2


def test_map_coordinates_empty_geometry() -> None:
    empty = gm.from_wkt('LINESTRING EMPTY')
    shapes: list[tuple[int, ...]] = []

    def cb(coords: np.ndarray) -> np.ndarray:
        shapes.append(coords.shape)
        return coords

    out = empty.map_coordinates(cb)
    assert out.is_empty
    assert out.geometry_type == 'LineString'
    assert shapes == [(0, 2)]


def test_map_coordinates_mixed_geometry_types_array() -> None:
    arr = gm.GeometryArray([
        gm.Point(0.0, 0.0),
        gm.LineString([(1.0, 1.0), (2.0, 2.0)]),
        gm.Polygon([(0, 0), (1, 0), (1, 1), (0, 0)]),
    ])
    out = arr.map_coordinates(_shift)
    assert out[0].to_wkt() == 'POINT (10 20)'
    assert out[1].to_wkt() == 'LINESTRING (11 21, 12 22)'
    assert out[2].geometry_type == 'Polygon'
    assert next(iter(out[2].exterior.coords))[:2] == pytest.approx((10.0, 20.0))


def test_map_coordinates_wrong_row_count_raises() -> None:
    line = gm.LineString([(0.0, 0.0), (1.0, 1.0)])
    with pytest.raises(gm.InvalidGeometryError, match='length 2'):
        line.map_coordinates(lambda c: c[:1])
    with pytest.raises(gm.InvalidGeometryError, match='length 2'):
        line.map_coordinates(lambda c: np.vstack([c, c[-1:]]))


def test_map_coordinates_wrong_width_raises() -> None:
    line = gm.LineString([(0.0, 0.0), (1.0, 1.0)])
    with pytest.raises(gm.InvalidGeometryError, match='width 2'):
        line.map_coordinates(lambda _c: np.zeros((2, 3)))
    zline = gm.from_wkt('LINESTRING Z (0 0 1, 1 1 2)')
    with pytest.raises(gm.InvalidGeometryError, match='width 3'):
        zline.map_coordinates(lambda _c: np.zeros((2, 2)))


def test_map_coordinates_non_finite_raises() -> None:
    line = gm.LineString([(0.0, 0.0), (1.0, 1.0)])
    with pytest.raises(gm.InvalidGeometryError, match='finite'):
        line.map_coordinates(lambda _c: np.array([[0.0, 0.0], [np.nan, 1.0]]))
    with pytest.raises(gm.InvalidGeometryError, match='finite'):
        line.map_coordinates(lambda _c: np.array([[0.0, 0.0], [np.inf, 1.0]]))


def test_map_coordinates_non_matrix_return_raises() -> None:
    line = gm.LineString([(0.0, 0.0), (1.0, 1.0)])
    with pytest.raises(gm.InvalidGeometryError, match='two-dimensional matrix'):
        line.map_coordinates(lambda _c: None)
    with pytest.raises(gm.InvalidGeometryError, match='two-dimensional matrix'):
        line.map_coordinates(lambda c: c.ravel())


def test_map_coordinates_python_exception_propagates() -> None:
    line = gm.LineString([(0.0, 0.0), (1.0, 1.0)])

    def boom(_coords: np.ndarray) -> np.ndarray:
        raise RuntimeError('callback exploded')

    with pytest.raises(RuntimeError, match='callback exploded'):
        line.map_coordinates(boom)

    arr = gm.GeometryArray([line, line])
    with pytest.raises(RuntimeError, match='callback exploded'):
        arr.map_coordinates(boom)


def test_map_coordinates_broken_polygon_closure_raises() -> None:
    poly = gm.Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])

    def open_ring(coords: np.ndarray) -> np.ndarray:
        out = np.array(coords, copy=True)
        out[-1] = [9.0, 9.0]
        return out

    with pytest.raises(gm.InvalidGeometryError, match='ring must be closed'):
        poly.map_coordinates(open_ring)


def test_map_coordinates_mixed_axes_array_identity() -> None:
    """Mixed-axes rows use the union layout; identity map is a no-op per row."""
    xy = gm.LineString([(0.0, 0.0), (1.0, 1.0)])
    xyz = gm.from_wkt('LINESTRING Z (0 0 1, 1 1 2)')
    mixed = gm.GeometryArray([xy, xyz])
    out = mixed.map_coordinates(lambda c: c)
    assert out[0] == xy
    assert out[1] == xyz
    assert out[0].coordinate_axes == 'XY'
    assert out[1].coordinate_axes == 'XYZ'
    # Non-trivial transform still applies on the union matrix.
    scaled = mixed.map_coordinates(lambda c: c * 2)
    assert scaled[0] == gm.LineString([(0.0, 0.0), (2.0, 2.0)])
    assert scaled[1] == gm.from_wkt('LINESTRING Z (0 0 2, 2 2 4)')


def test_map_coordinates_array_wrong_total_count_raises() -> None:
    arr = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (1.0, 1.0)]),
        gm.LineString([(2.0, 2.0), (3.0, 3.0)]),
    ])
    with pytest.raises(gm.InvalidGeometryError, match='length 4'):
        arr.map_coordinates(lambda c: c[:1])
    with pytest.raises(gm.InvalidGeometryError, match='finite'):
        arr.map_coordinates(lambda c: np.full_like(c, np.nan))


def test_map_coordinates_list_return_accepted_for_scalar() -> None:
    """A list-of-lists return is accepted (coerced) on the scalar path."""
    line = gm.LineString([(0.0, 0.0), (1.0, 1.0)])
    out = line.map_coordinates(lambda _c: [[10.0, 20.0], [11.0, 21.0]])
    assert out.to_wkt() == 'LINESTRING (10 20, 11 21)'
