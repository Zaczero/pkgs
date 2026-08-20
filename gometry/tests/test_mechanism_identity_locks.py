"""Deterministic identity locks for R4-L3 mechanism items (A/B/D/E/F).

Public-path checks only: simplify DP coords, index nearest ids+distances,
packed length allclose, tile/geohash cover token sets, mixed has_z exhaust.
"""

from __future__ import annotations

import gometry as gm
import numpy as np
import pytest


def test_simplify_dp_packed_is_deterministic():
    """Repeatability only; this is not an independent simplify oracle."""
    arr = gm.GeometryArray([
        gm.LineString([(0, 0), (0.1, 0.02), (0.2, -0.01), (0.3, 0.0)]),
        gm.LineString([(1, 1), (1.1, 1.03), (1.2, 0.97), (1.3, 1.0)]),
        gm.LineString([(-2, 3), (-1.9, 3.02), (-1.8, 2.98), (-1.7, 3.0)]),
    ])
    a = arr.simplify(0.001, method='dp', preserve_topology=False)
    b = arr.simplify(0.001, method='dp', preserve_topology=False)
    np.testing.assert_array_equal(gm.get_coordinates(a), gm.get_coordinates(b))
    assert a.to_wkb() == b.to_wkb()
    # VW path still works (unaffected)
    vw = arr.simplify(0.001, method='vw', preserve_topology=False)
    assert len(vw) == len(arr)
    topo = arr.simplify(0.001, method='dp', preserve_topology=True)
    assert len(topo) == len(arr)


def test_index_nearest_array_is_deterministic():
    """Repeatability only; fixed expected IDs keep the public path meaningful."""
    polys = gm.GeometryArray([
        gm.box(0.0, 0.0, 1.0, 1.0),
        gm.box(10.0, 10.0, 11.0, 11.0),
        gm.box(-5.0, -5.0, -4.0, -4.0),
    ])
    idx = gm.SpatialIndex(polys)
    queries = gm.GeometryArray([gm.Point(0.5, 0.5), gm.Point(10.5, 10.5)])
    g1, d1 = idx.nearest(queries, k=1, return_distance=True)
    g2, d2 = idx.nearest(queries, k=1, return_distance=True)
    np.testing.assert_array_equal(g1.values, [0, 1])
    np.testing.assert_array_equal(g1.offsets, [0, 1, 2])
    np.testing.assert_array_equal(d1, [0.0, 0.0])
    np.testing.assert_array_equal(g1.values, g2.values)
    np.testing.assert_array_equal(g1.offsets, g2.offsets)
    np.testing.assert_array_equal(d1, d2)


def test_packed_length_and_huge_hex_are_deterministic():
    """Repeatability only; ordinary values pin the exercised metric path."""
    arr = gm.GeometryArray([
        gm.LineString([(0, 0), (3, 4)]),
        gm.LineString([(0, 0), (1, 0), (1, 1)]),
        gm.LineString([(-2, 1), (-2, 4)]),
    ])
    a = arr.length
    b = arr.length
    np.testing.assert_array_equal(a, [5.0, 2.0, 3.0])
    np.testing.assert_allclose(a, b, rtol=0, atol=1e-12)
    huge = gm.GeometryArray([
        gm.from_wkt('LINESTRING (1e200 1e200, 1e200 1e200, 1e200 1e200)'),
        gm.from_wkt('LINESTRING (1e-200 1e-200, 2e-200 1e-200, 1e-200 2e-200)'),
    ])
    h1 = huge.length
    h2 = huge.length
    np.testing.assert_array_equal(h1.view(np.uint64), h2.view(np.uint64))


def _cover_tokens(cover):
    return tuple(sorted(c.token for c in cover))


@pytest.mark.parametrize('rule', ['center', 'overlap', 'within', 'bbox'])
@pytest.mark.parametrize('zoom', [6, 10, 15])
def test_tile_cover_token_set_is_deterministic(rule, zoom):
    # Compact SF bay footprint — non-empty under every rule at z6..15 without
    # tripping the default max_cells budget.
    band = gm.box(-122.5, 37.5, -122.0, 38.0, crs=4326)
    a = _cover_tokens(gm.tile_cover(band, zoom, cell_rule=rule))
    b = _cover_tokens(gm.tile_cover(band, zoom, cell_rule=rule))
    assert a == b
    # center/within may be empty at coarse depth for a small footprint —
    # identity is the contract, not non-emptiness.
    if rule in ('overlap', 'bbox') or zoom >= 15:
        assert len(a) > 0


@pytest.mark.parametrize('rule', ['center', 'overlap', 'within', 'bbox'])
@pytest.mark.parametrize('precision', [4, 5])
def test_geohash_cover_token_set_is_deterministic(rule, precision):
    band = gm.box(-122.5, 37.5, -122.0, 38.0, crs=4326)
    a = _cover_tokens(gm.geohash_cover(band, precision, cell_rule=rule))
    b = _cover_tokens(gm.geohash_cover(band, precision, cell_rule=rule))
    assert a == b
    if rule in ('overlap', 'bbox', 'center') or precision >= 5:
        assert len(a) > 0


def test_mixed_has_z_rows_length_and_values():
    rows = []
    for i in range(500):
        if i % 3 == 0:
            rows.append(gm.Point(float(i), 0.0, z=1.0 if i % 2 == 0 else None))
        elif i % 3 == 1:
            rows.append(gm.LineString([(0.0, 0.0), (1.0, 1.0)]))
        else:
            rows.append(gm.box(0, 0, 1, 1))
    arr = gm.GeometryArray(rows)
    mask = arr.has_z
    assert len(mask) == 500
    # Point with z=1.0 when i%2==0 and i%3==0
    assert bool(mask[0]) is True  # i=0
    assert bool(mask[3]) is False  # i=3, z=None (odd)
    assert bool(mask[1]) is False  # linestring XY
    # any_has_z
    assert arr.any_has_z is True


def test_prepared_tall_edge_masks_match_unprepared():
    n_edges = 64
    verts = [(i * 0.1, 0.0 if i % 2 == 0 else 1000.0) for i in range(n_edges)]
    verts.append(verts[0])
    poly = gm.Polygon(verts)
    prep = poly.prepare()
    xs = np.linspace(0, 6, 200)
    ys = np.linspace(0, 1000, 200)
    gx, gy = np.meshgrid(xs, ys, indexing='xy')
    prepared = gm.contains_xy(prep, gx.ravel(), gy.ravel())
    scalar = np.array(
        [
            gm.contains(poly, gm.Point(float(x), float(y)))
            for x, y in zip(gx.ravel(), gy.ravel(), strict=True)
        ]
    )
    np.testing.assert_array_equal(prepared, scalar)
    np.testing.assert_array_equal(
        prepared,
        gm.contains_xy(poly, gx.ravel(), gy.ravel()),
    )

    # The tall-edge mesh does not enter a certified interior cell at these
    # coordinates. Keep its identity lock, and also pin the shared path to an
    # independent scalar oracle on a certified interior cell.
    theta = np.arange(64) * (2.0 * np.pi / 64.0)
    oracle_poly = gm.Polygon([(10.0 * np.cos(t), 10.0 * np.sin(t)) for t in theta])
    oracle_prepared = oracle_poly.prepare()
    oracle_x = np.zeros(10_000)
    oracle_y = np.zeros(10_000)
    oracle_batch = gm.contains_xy(oracle_prepared, oracle_x, oracle_y)
    oracle_scalar = np.array(
        [gm.contains(oracle_poly, gm.Point(0.0, 0.0))] * len(oracle_x)
    )
    np.testing.assert_array_equal(oracle_batch, oracle_scalar)
