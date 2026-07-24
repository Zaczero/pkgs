"""Tests for ``Geometry.smooth`` — Chaikin and centripetal Catmull-Rom smoothing."""

from __future__ import annotations

import math

import gometry as gm
import pytest
from gometry import GeometryError


def test_chaikin_square_one_iteration_coords() -> None:
    square = gm.box(0, 0, 1, 1)
    smoothed = square.smooth(iterations=1, method='chaikin')
    expected = [
        (0.25, 0.0),
        (0.75, 0.0),
        (1.0, 0.25),
        (1.0, 0.75),
        (0.75, 1.0),
        (0.25, 1.0),
        (0.0, 0.75),
        (0.0, 0.25),
        (0.25, 0.0),
    ]
    assert list(smoothed.exterior.coords) == pytest.approx(expected)


def test_chaikin_vertex_count_doubles_per_iteration() -> None:
    line = gm.LineString([(0, 0), (1, 0), (2, 0), (3, 0), (4, 0)])
    n0 = len(line.coords)
    once = line.smooth(iterations=1, method='chaikin')
    twice = line.smooth(iterations=2, method='chaikin')
    assert len(once.coords) == 2 * n0 - 2
    assert len(twice.coords) == 2 * len(once.coords) - 2


def test_chaikin_open_line_preserves_endpoints() -> None:
    line = gm.LineString([(0, 0), (1, 1), (2, 0), (3, 1), (4, 0)])
    smoothed = line.smooth(iterations=1, method='chaikin', keep_endpoints=True)
    assert smoothed.coords[0] == line.coords[0]
    assert smoothed.coords[-1] == line.coords[-1]


def test_chaikin_ring_stays_closed_and_valid() -> None:
    square = gm.box(0, 0, 2, 2)
    smoothed = square.smooth(iterations=2, method='chaikin')
    ring = list(smoothed.exterior.coords)
    assert ring[0] == ring[-1]
    assert smoothed.is_valid


def test_chaikin_area_shrinks_slightly() -> None:
    square = gm.box(0, 0, 10, 10)
    smoothed = square.smooth(iterations=1, method='chaikin')
    assert smoothed.area < square.area
    assert smoothed.is_valid


def test_iterations_zero_is_identity() -> None:
    line = gm.LineString([(0, 0), (1, 2), (3, 1), (5, 0)])
    assert line.smooth(iterations=0).to_wkt() == line.to_wkt()


def test_catmull_rom_passes_through_original_vertices() -> None:
    line = gm.LineString([(0, 0), (1, 1), (2, 0), (3, 1), (4, 0)])
    smoothed = line.smooth(iterations=2, method='catmull_rom')
    output = list(smoothed.coords)
    for vertex in line.coords:
        assert any(
            math.isclose(got[0], vertex[0]) and math.isclose(got[1], vertex[1])
            for got in output
        )


def test_catmull_rom_rejects_dropping_endpoints() -> None:
    line = gm.LineString([(0, 0), (1, 1), (2, 0)])
    with pytest.raises(GeometryError, match='Catmull-Rom always interpolates its endpoints'):
        line.smooth(method='catmull_rom', keep_endpoints=False)
    with pytest.raises(GeometryError, match='Catmull-Rom always interpolates its endpoints'):
        gm.GeometryArray([line]).smooth(method='catmull_rom', keep_endpoints=False)


def test_catmull_rom_gentle_path_has_no_self_intersection() -> None:
    line = gm.LineString([(0, 0), (1, 0.2), (2, 0), (3, 0.2), (4, 0)])
    smoothed = line.smooth(iterations=2, method='catmull_rom')
    assert smoothed.is_simple


def test_catmull_rom_ring() -> None:
    square = gm.box(0, 0, 1, 1)
    smoothed = square.smooth(iterations=1, method='catmull_rom')
    assert smoothed.is_valid
    assert len(smoothed.exterior.coords) > len(square.exterior.coords)


def test_zm_interpolated_and_endpoints_preserved() -> None:
    line = gm.from_wkt('LINESTRING ZM (0 0 0 10, 4 0 4 20)')
    smoothed = line.smooth(iterations=1, method='chaikin', keep_endpoints=True)
    assert smoothed.coords[0] == (0.0, 0.0, 0.0, 10.0)
    assert smoothed.coords[-1] == (4.0, 0.0, 4.0, 20.0)
    zs = [coord[2] for coord in smoothed.coords]
    ms = [coord[3] for coord in smoothed.coords]
    assert all(0.0 <= z <= 4.0 for z in zs)
    assert all(10.0 <= m <= 20.0 for m in ms)


def test_crs_epoch_carried() -> None:
    line = gm.LineString([(0, 0), (1, 1), (2, 0)], crs='EPSG:4326', epoch=2020.0)
    smoothed = line.smooth(iterations=1)
    assert smoothed.crs == 'EPSG:4326'
    assert smoothed.epoch == 2020.0


def test_array_lane_matches_scalar() -> None:
    lines = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 0.1), (2, 0)]),
        gm.LineString([(0, 0), (1, -0.1), (2, 0)]),
    ])
    scalar = [g.smooth(iterations=1) for g in lines]
    broadcast = lines.smooth(iterations=1)
    assert [g.to_wkt() for g in broadcast] == [g.to_wkt() for g in scalar]


def test_array_per_element_iterations_broadcast() -> None:
    lines = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 0), (2, 0)]),
        gm.LineString([(0, 0), (1, 0), (2, 0)]),
    ])
    out = lines.smooth(iterations=[0, 1])
    assert out[0].to_wkt() == lines[0].to_wkt()
    assert len(out[1].coords) > len(lines[1].coords)


def test_degenerate_inputs_unchanged() -> None:
    point = gm.Point(1, 2)
    empty = gm.from_wkt('LINESTRING EMPTY')
    two_pt = gm.LineString([(0, 0), (1, 1)])
    collinear = gm.LineString([(0, 0), (1, 1), (2, 2)])
    assert point.smooth().to_wkt() == point.to_wkt()
    assert empty.smooth().to_wkt() == empty.to_wkt()
    assert two_pt.smooth(iterations=1).to_wkt() == two_pt.to_wkt()
    assert collinear.smooth(iterations=1).is_valid


def test_negative_iterations_raises() -> None:
    line = gm.LineString([(0, 0), (1, 0)])
    with pytest.raises(GeometryError, match='iterations must be a non-negative integer'):
        line.smooth(iterations=-1)


@pytest.mark.parametrize('iterations', [30, 31, 2**31 - 1])
@pytest.mark.parametrize('method', ['chaikin', 'catmull_rom'])
def test_large_iterations_rejected_before_allocation(
    iterations: int, method: str
) -> None:
    # A tiny valid geometry paired with a valid-but-huge ``iterations`` would
    # otherwise double (Chaikin) or ``2**iterations``-sample (Catmull-Rom) into
    # an unbounded allocation. The projected coordinate count is checked with
    # checked arithmetic and rejected quickly, before any output is built.
    line = gm.LineString([(0, 0), (1, 0), (2, 0), (3, 0), (4, 0)])
    with pytest.raises(GeometryError, match='reduce iterations'):
        line.smooth(iterations=iterations, method=method)


def test_large_iterations_rejected_on_array_lane() -> None:
    lines = gm.GeometryArray([gm.LineString([(0, 0), (1, 0), (2, 0)])])
    with pytest.raises(GeometryError, match='reduce iterations'):
        lines.smooth(iterations=40)


def test_large_iterations_rejected_for_polygon_rings() -> None:
    square = gm.box(0, 0, 1, 1)
    with pytest.raises(GeometryError, match='reduce iterations'):
        square.smooth(iterations=40, method='chaikin')


def test_default_iterations_is_two() -> None:
    line = gm.LineString([(0, 0), (1, 0.2), (2, 0), (3, 0.2), (4, 0)])
    assert line.smooth().to_wkt() == line.smooth(iterations=2).to_wkt()
