"""Coverage invalid-edge attribution and nullable scatter."""

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
    assert not invalid_edges[2].is_empty
    assert not invalid_edges[3].is_empty
