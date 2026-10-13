"""Coverage invalid-edge attribution and nullable scatter."""

from itertools import pairwise

import gometry as gm


def test_geographic_invalid_edge_rows_scatter_around_missing_rows() -> None:
    coverage = gm.GeometryArray([
        gm.Polygon([(-5, 2), (5, 2), (5, 8), (-5, 8)], crs='OGC:CRS84'),
        None,
        gm.Polygon([(170, 0), (-170, 0), (-170, 10), (170, 10)], crs='OGC:CRS84'),
        gm.Polygon([(175, 2), (-175, 2), (-175, 8), (175, 8)], crs='OGC:CRS84'),
    ])

    invalid_edges = gm.coverage_invalid_edges(coverage)

    assert invalid_edges.is_missing.tolist() == [False, True, False, False]
    assert invalid_edges[0].is_empty
    assert invalid_edges[1] is None
    assert invalid_edges[2].is_empty
    assert not invalid_edges[3].is_empty
    coordinates = list(invalid_edges[3].coords)
    assert len(coordinates) == 5
    assert {frozenset((start, end)) for start, end in pairwise(coordinates)} == {
        frozenset(((175, 2), (-175, 2))),
        frozenset(((-175, 2), (-175, 8))),
        frozenset(((-175, 8), (175, 8))),
        frozenset(((175, 8), (175, 2))),
    }


def test_geographic_invalid_edges_keep_empty_and_all_missing_rows() -> None:
    polygon = gm.Polygon(
        [(170, 0), (-170, 0), (-170, 10), (170, 10)],
        crs='OGC:CRS84',
    )
    scalar_edges = gm.coverage_invalid_edges(polygon)
    assert len(scalar_edges) == 1
    assert scalar_edges[0].is_empty
    assert scalar_edges.crs == polygon.crs

    empty = gm.GeometryArray([], crs='OGC:CRS84')
    empty_edges = gm.coverage_invalid_edges(empty)
    assert len(empty_edges) == 0
    assert empty_edges.crs == empty.crs

    missing = gm.GeometryArray([None, None], crs='OGC:CRS84')
    missing_edges = gm.coverage_invalid_edges(missing)
    assert missing_edges.is_missing.tolist() == [True, True]
    assert missing_edges.to_wkt() == [None, None]
    assert missing_edges.crs == missing.crs
