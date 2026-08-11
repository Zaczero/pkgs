"""Projective-sign independence at exact Voronoi vertices."""

from __future__ import annotations

import struct
from typing import TYPE_CHECKING

import gometry as gm
import pytest

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence


SITES = (
    (0.0, 0.0),
    (4.0, 0.0),
    (0.0, 4.0),
    (4.0, 4.0),
    (1.0, 2.0),
    (3.0, 1.0),
    (2.0, 3.0),
    (5.0, 2.0),
    (-1.0, 1.0),
    (6.0, 4.0),
    (2.0, -1.0),
    (5.0, -2.0),
)


def _float_bits(value: float) -> bytes:
    return struct.pack('=d', value)


def _vertex_bits(
    geometries: Iterable[gm.Geometry | None],
) -> dict[tuple[float, float], set[tuple[bytes, bytes]]]:
    vertices: dict[tuple[float, float], set[tuple[bytes, bytes]]] = {}
    for geometry in geometries:
        assert geometry is not None
        for x, y in geometry.coords:
            assert x is not None and y is not None
            vertex = (x, y)
            vertices.setdefault(vertex, set()).add((
                _float_bits(vertex[0]),
                _float_bits(vertex[1]),
            ))
    return vertices


def _wkb_signature(geometries: Iterable[gm.Geometry | None]) -> tuple[bytes, ...]:
    signature = []
    for geometry in geometries:
        assert geometry is not None
        signature.append(geometry.to_wkb())
    return tuple(signature)


def _orders(
    sites: Sequence[tuple[float, float]],
) -> tuple[tuple[tuple[float, float], ...], ...]:
    sites = tuple(sites)
    return sites, tuple(reversed(sites)), sites[1:] + sites[:1]


def _swept_orders() -> Iterable[tuple[int, tuple[tuple[float, float], ...]]]:
    for count in range(5, 13):
        for order in _orders(SITES[:count]):
            yield count, order


def test_polygons_and_edges_share_identical_synthesized_vertex_bits() -> None:
    source = gm.MultiPoint(SITES[:6])
    polygons = source.voronoi_polygons(clip='envelope')
    edges = source.voronoi_edges(clip='envelope')
    polygon_vertices = _vertex_bits(polygons)
    edge_vertices = _vertex_bits(edges)
    shared = polygon_vertices.keys() & edge_vertices.keys()

    assert len(shared) == 10
    assert all(polygon_vertices[vertex] == edge_vertices[vertex] for vertex in shared)
    for vertex in ((0.0, 1.25), (0.0, 2.75), (5.0 / 3.0, 0.0), (3.0, 0.0)):
        assert polygon_vertices[vertex] == edge_vertices[vertex]
        assert all(
            bits == _float_bits(0.0)
            for coordinate, bits in zip(
                vertex, next(iter(polygon_vertices[vertex])), strict=True
            )
            if coordinate == 0.0
        )


@pytest.mark.parametrize(
    'sites',
    [
        SITES[:6],
        tuple((x * 0.5, y * 0.5) for x, y in SITES[:6]),
        SITES[:11],
    ],
    ids=['integer-six', 'fractional-six', 'integer-eleven'],
)
def test_named_and_explicit_equal_envelopes_are_byte_identical(
    sites: tuple[tuple[float, float], ...],
) -> None:
    source = gm.MultiPoint(sites)
    named = source.voronoi_polygons(clip='envelope')
    bounds = source.bounds
    assert bounds is not None
    min_x, min_y, max_x, max_y = bounds
    explicit = source.voronoi_polygons(clip=gm.box(min_x, min_y, max_x, max_y))

    assert _wkb_signature(named) == _wkb_signature(explicit)
    for left, right in zip(named, explicit, strict=True):
        assert left is not None and right is not None
        assert gm.equals_identical(left, right)


def test_integer_site_sweep_polygons_and_edges_share_vertex_bits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv('GOMETRY_VORO_REFERENCE', raising=False)
    for count, order in _swept_orders():
        source = gm.MultiPoint(order)
        polygon_vertices = _vertex_bits(source.voronoi_polygons(clip='envelope'))
        edge_vertices = _vertex_bits(source.voronoi_edges(clip='envelope'))
        shared = polygon_vertices.keys() & edge_vertices.keys()

        assert shared, (count, order)
        assert all(
            polygon_vertices[vertex] == edge_vertices[vertex] for vertex in shared
        ), (count, order)


def test_integer_site_sweep_emits_only_positive_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv('GOMETRY_VORO_REFERENCE', raising=False)
    for count, order in _swept_orders():
        source = gm.MultiPoint(order)
        for vertices in (
            _vertex_bits(source.voronoi_polygons(clip='envelope')),
            _vertex_bits(source.voronoi_edges(clip='envelope')),
        ):
            assert all(
                bits == _float_bits(0.0)
                for vertex, encodings in vertices.items()
                for encoding in encodings
                for coordinate, bits in zip(vertex, encoding, strict=True)
                if coordinate == 0.0
            ), (count, order)


def test_integer_site_sweep_default_and_arrangement_are_byte_identical(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for count, order in _swept_orders():
        source = gm.MultiPoint(order)
        monkeypatch.delenv('GOMETRY_VORO_REFERENCE', raising=False)
        default = _wkb_signature(source.voronoi_polygons(clip='envelope'))
        monkeypatch.setenv('GOMETRY_VORO_REFERENCE', '1')
        arrangement = _wkb_signature(source.voronoi_polygons(clip='envelope'))

        assert default == arrangement, (count, order)


@pytest.mark.parametrize('count', range(5, 13))
def test_integer_site_sweep_default_lane_is_permutation_deterministic(
    monkeypatch: pytest.MonkeyPatch,
    count: int,
) -> None:
    monkeypatch.delenv('GOMETRY_VORO_REFERENCE', raising=False)
    signatures = {
        _wkb_signature(gm.MultiPoint(order).voronoi_polygons(clip='envelope'))
        for order in _orders(SITES[:count])
    }

    assert len(signatures) == 1, (count, len(signatures))


@pytest.mark.parametrize('count', range(5, 13))
def test_integer_site_sweep_arrangement_lane_is_permutation_deterministic(
    monkeypatch: pytest.MonkeyPatch,
    count: int,
) -> None:
    monkeypatch.setenv('GOMETRY_VORO_REFERENCE', '1')
    signatures = {
        _wkb_signature(gm.MultiPoint(order).voronoi_polygons(clip='envelope'))
        for order in _orders(SITES[:count])
    }

    assert len(signatures) == 1, (count, len(signatures))
