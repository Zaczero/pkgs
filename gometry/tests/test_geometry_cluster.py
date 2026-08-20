"""Lane R3-L3 geometry cluster: erosion certificate, ring-leaf perimeter,
packed-point touches, exact-list fancy index — identity + parity gates.

Baselines live under ``tests/data/r3_l3_geometry_cluster/``; a missing fixture is
a hard failure (never a silent skip on a dead scratch path).
"""

from __future__ import annotations

import json
from pathlib import Path

import gometry as gm
import numpy as np
import pytest

FIXTURE_DIR = Path(__file__).resolve().parent / 'data' / 'r3_l3_geometry_cluster'


def _require_fixture(name: str) -> Path:
    path = FIXTURE_DIR / name
    if not path.is_file():
        raise AssertionError(
            f'missing in-repo fixture {path}; regenerate under tests/data/'
            f'r3_l3_geometry_cluster/ rather than skipping'
        )
    return path


def test_convex_erosion_certificate_accept_reject():
    box = gm.box(0, 0, 10, 10)
    ok = box.buffer(-1.0)
    assert not ok.is_empty
    assert abs(ok.area - 64.0) < 1e-9
    gone = box.buffer(-6.0)
    assert gone.is_empty
    # Styles agree on convex erosion (monotone construction).
    sq = gm.box(0, 0, 4, 4)
    areas = [
        sq.buffer(-1.0, join_style=style).area for style in ('round', 'bevel', 'miter')
    ]
    assert all(abs(a - 4.0) < 1e-9 for a in areas)


def test_erosion_corpus_matches_baseline_geometry():
    """Geometric identity vs committed baseline (areas / emptiness / mutual cover)."""
    baseline_path = _require_fixture('erosion_baseline.json')
    baseline = json.loads(baseline_path.read_text(encoding='utf-8'))

    box = gm.box(0, 0, 10, 10)
    l = gm.Polygon([(0, 0), (5, 0), (5, 2), (2, 2), (2, 5), (0, 5), (0, 0)])
    sq = gm.box(0, 0, 4, 4)
    notched_coords = [
        (-0.5148763260552798, 48.44262919300455),
        (3.6615344632949895, 48.44262919300455),
        (3.8615344632949897, 50.9118437951912),
        (4.06153446329499, 48.44262919300455),
        (6.096036601959777, 48.44262919300455),
        (6.096036601959777, 54.61566569847118),
        (-0.5148763260552798, 54.61566569847118),
        (-0.5148763260552798, 48.44262919300455),
    ]
    notched = gm.Polygon(notched_coords)
    holed = gm.Polygon(
        [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)],
        [[(4, 4), (6, 4), (6, 6), (4, 6), (4, 4)]],
    )
    specs = {
        'box_d1': (box, -1.0, {}),
        'box_d6_empty': (box, -6.0, {}),
        'sq_arc_d1': (sq, -1.0, {'join_style': 'round'}),
        'sq_bevel_d1': (sq, -1.0, {'join_style': 'bevel'}),
        'sq_miter_d1': (sq, -1.0, {'join_style': 'miter'}),
        'l_bevel_1.2': (l, -1.2, {'join_style': 'bevel'}),
        'l_bevel_gone': (l, -(4 / 3 + 1e-9), {'join_style': 'bevel'}),
        'notched_annihilate': (notched, -3.9380952651070875, {'join_style': 'round'}),
        'box_d0': (box, 0.0, {}),
        'box_pos': (box, 1.0, {}),
        'holed': (holed, -0.5, {}),
    }
    assert set(baseline) == set(specs), 'baseline keys drifted from corpus'
    for name, (geom, d, kw) in specs.items():
        out = geom.buffer(d, **kw)
        base = baseline[name]
        assert bool(out.is_empty) == base['is_empty'], name
        if out.is_empty:
            continue
        assert abs(float(out.area) - base['area']) < 1e-9, (
            name,
            out.area,
            base['area'],
        )
        # Presentation may differ on the convex fast path; geometric equality
        # via mutual covers for non-empty results.
        ref = gm.from_wkb(bytes.fromhex(base['wkb_hex']))
        assert gm.covers(out, ref) and gm.covers(ref, out), name


def test_perimeter_holes_missing_byte_identical():
    b2 = _require_fixture('perimeter_baseline_2d.npy')
    b3 = _require_fixture('perimeter_baseline_3d.npy')
    polys = gm.GeometryArray([
        gm.box(0, 0, 1, 1),
        None,
        gm.Polygon(
            [(0, 0), (3, 0), (3, 2), (0, 2), (0, 0)],
            [[(1, 0.5), (2, 0.5), (2, 1.5), (1, 1.5), (1, 0.5)]],
        ),
        gm.box(10, 10, 12, 12),
    ])
    got2 = np.asarray(polys.length, dtype=np.float64)
    exp2 = np.load(b2)
    assert got2.tobytes() == exp2.tobytes()

    def zring(coords, z=1.0):
        return [(x, y, z) for x, y in coords]

    polys3d = gm.GeometryArray([
        gm.Polygon(zring([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])),
        None,
        gm.Polygon(
            zring([(0, 0), (3, 0), (3, 2), (0, 2), (0, 0)]),
            [zring([(1, 0.5), (2, 0.5), (2, 1.5), (1, 1.5), (1, 0.5)])],
        ),
    ])
    got3 = np.asarray(polys3d.length_3d, dtype=np.float64)
    exp3 = np.load(b3)
    assert got3.tobytes() == exp3.tobytes()


def test_touches_packed_points_match_scalar_corpus():
    poly = gm.box(0, 0, 10, 10)
    pts = gm.GeometryArray([
        gm.Point(0, 0),
        gm.Point(5, 0),
        gm.Point(10, 5),
        gm.Point(0, 10),
        gm.Point(5, 5),
        gm.Point(2, 3),
        gm.Point(11, 5),
        gm.Point(-1, -1),
        gm.Point(5, 10),
        gm.Point(10, 10),
    ])
    expected = np.array(
        [True, True, True, True, False, False, False, False, True, True],
        dtype=bool,
    )
    scalar = np.array([gm.touches(poly, p) for p in pts], dtype=bool)
    vec = np.asarray(gm.touches(poly, pts), dtype=bool)
    rev = np.asarray(gm.touches(pts, poly), dtype=bool)
    np.testing.assert_array_equal(scalar, expected)
    np.testing.assert_array_equal(vec, expected)
    np.testing.assert_array_equal(rev, expected)
    # Larger batch exercises the indexed point-membership path.
    big = gm.GeometryArray(
        [gm.Point(float(i % 11), float(i % 13)) for i in range(200)]
        + [gm.Point(0, 0), gm.Point(5, 0), gm.Point(5, 5), gm.Point(20, 20)]
    )
    big_scalar = np.array([gm.touches(poly, p) for p in big], dtype=bool)
    big_vec = np.asarray(gm.touches(poly, big), dtype=bool)
    np.testing.assert_array_equal(big_scalar, big_vec)


def test_exact_list_fancy_index_parity():
    arr = gm.GeometryArray([gm.box(i, i, i + 1, i + 1) for i in range(20)])
    mask = [True, False, True] + [False] * 17
    idx = [0, 2, 5, -1, 19]
    sel_mask = arr[mask]
    sel_idx = arr[idx]
    assert len(sel_mask) == 2
    assert len(sel_idx) == 5
    assert gm.equals_identical(sel_mask[0], arr[0])
    assert gm.equals_identical(sel_idx[-1], arr[19])
    with pytest.raises(gm.GeometryError, match='mask length'):
        _ = arr[[True, False]]
    with pytest.raises(IndexError, match='out of range'):
        _ = arr[[100]]
    empty = arr[[]]
    assert len(empty) == 0
