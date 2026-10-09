"""Exact source clearance keys and stable source ordinals."""

import gometry as gm
import pytest


@pytest.mark.parametrize(
    'spacing',
    [2.0**-700, 1.0, 2.0**600],
    ids=['tiny', 'normal', 'huge'],
)
def test_minimum_clearance_finishes_the_selected_source_pair(spacing: float) -> None:
    shape = gm.MultiPoint([(0.0, 0.0), (5.0 * spacing, 0.0), (spacing, 0.0)])

    assert shape.minimum_clearance() == spacing
    assert (
        shape.minimum_clearance_line().to_wkb()
        == gm.LineString([(0.0, 0.0), (spacing, 0.0)]).to_wkb()
    )


@pytest.mark.parametrize(
    'spacing',
    [2.0**-700, 2.0**600],
    ids=['indexed-tiny', 'indexed-huge'],
)
def test_indexed_minimum_clearance_keeps_tiny_and_huge_source_pairs(
    spacing: float,
) -> None:
    shape = gm.MultiPoint([
        (0.0, 0.0),
        (5.0 * spacing, 0.0),
        (spacing, 0.0),
        *[((8.0 + 2.0 * index) * spacing, 0.0) for index in range(64)],
    ])

    assert shape.minimum_clearance() == spacing
    assert (
        shape.minimum_clearance_line().to_wkb()
        == gm.LineString([(0.0, 0.0), (spacing, 0.0)]).to_wkb()
    )


@pytest.mark.parametrize('extra', [0, 58], ids=['small', 'indexed'])
def test_clearance_equal_probes_retain_first_improving_probe(extra: int) -> None:
    shape = gm.GeometryCollection([
        gm.MultiPoint([(0, 1), (10, 1)]),
        gm.LineString([(9, 0), (11, 0)]),
        gm.LineString([(-1, 0), (1, 0)]),
        *(gm.Point(1000 + 10 * i, 1000) for i in range(extra)),
    ])
    assert shape.minimum_clearance() == 1.0
    assert (
        shape.minimum_clearance_line().to_wkb()
        == gm.LineString([
            (0, 1),
            (0, 0),
        ]).to_wkb()
    )


def test_indexed_clearance_uses_exact_source_distance_for_rounded_near_tie() -> None:
    shape = gm.GeometryCollection([
        gm.MultiPoint([(0.0, 0.0), (1.0, 2.0**-28)]),
        gm.LineString([(-10.0, -1.0), (10.0, -1.0)]),
        *(gm.Point(100.0 + 10.0 * index, 100.0) for index in range(64)),
    ])

    assert shape.minimum_clearance() == 1.0
    assert (
        shape.minimum_clearance_line().to_wkb()
        == gm.LineString([(0.0, 0.0), (0.0, -1.0)]).to_wkb()
    )


def test_indexed_zero_segment_incidence_does_not_hide_positive_clearance() -> None:
    shape = gm.GeometryCollection([
        gm.Point(0.0, 0.0),
        gm.LineString([(-10.0, 0.0), (10.0, 0.0)]),
        gm.LineString([(-1.0, 1.0), (1.0, 1.0)]),
        *(gm.Point(100.0 + 10.0 * index, 100.0) for index in range(64)),
    ])

    assert shape.minimum_clearance() == 1.0
    assert (
        shape.minimum_clearance_line().to_wkb()
        == gm.LineString([(0.0, 0.0), (0.0, 1.0)]).to_wkb()
    )


def test_indexed_nearest_exact_tie_uses_lowest_source_segment() -> None:
    target = gm.MultiLineString([
        gm.LineString([(-1, 1), (1, 1)]),
        gm.LineString([(-1 + 3 * i, -3 + 4 * i) for i in range(81)]),
    ])
    _, foot = gm.nearest_points(gm.Point(0, 0), target)
    assert foot.to_wkb() == gm.Point(0, 1).to_wkb()


def test_indexed_clearance_vertex_witness_retains_first_source_ordinate() -> None:
    shape = gm.MultiPoint(
        [(1, 0), (0, 0), (0, 0), *((1000 + 10 * i, 1000) for i in range(61))],
        z=[20, 10, 100, *([1] * 61)],
    )
    assert (
        shape.minimum_clearance_line().to_wkb()
        == gm.LineString([(1, 0), (0, 0)], z=[20, 10]).to_wkb()
    )


@pytest.mark.parametrize('count', [31, 32, 1000])
@pytest.mark.parametrize('axes', ['XYZ', 'XYM', 'XYZM'])
def test_nearest_point_index_retains_source_ordinates(count: int, axes: str) -> None:
    query_kwargs = {}
    site_kwargs = {}
    expected_kwargs = {}
    if 'Z' in axes:
        query_kwargs['z'] = 2
        site_kwargs['z'] = [100 + i for i in range(count)]
        expected_kwargs['z'] = 100
    if 'M' in axes:
        query_kwargs['m'] = 3
        site_kwargs['m'] = [200 + i for i in range(count)]
        expected_kwargs['m'] = 200
    query = gm.Point(0.1, 1, **query_kwargs)
    sites = gm.MultiPoint([(i, 0) for i in range(count)], **site_kwargs)
    expected = gm.Point(0, 0, **expected_kwargs)
    left, right = gm.nearest_points(query, sites)
    assert left.to_wkb() == query.to_wkb()
    assert right.to_wkb() == expected.to_wkb()
    assert (
        gm.shortest_line(query, sites).to_wkb()
        == gm.LineString(
            [(0.1, 1), (0, 0)],
            **{
                name: [query_kwargs[name], expected_kwargs[name]]
                for name in query_kwargs
            },
        ).to_wkb()
    )
    rows = gm.GeometryArray([sites, None, sites])[[2, 0, 1, 2]]
    for operands, expected_first, expected_second in (
        ((query, rows), query, expected),
        ((rows, query), expected, query),
    ):
        first, second = gm.nearest_points(*operands)
        assert list(first.to_wkb()) == [
            expected_first.to_wkb(),
            expected_first.to_wkb(),
            None,
            expected_first.to_wkb(),
        ]
        assert list(second.to_wkb()) == [
            expected_second.to_wkb(),
            expected_second.to_wkb(),
            None,
            expected_second.to_wkb(),
        ]


@pytest.mark.parametrize('axes', ['XYZ', 'XYM', 'XYZM'])
def test_nearest_packed_point_rows_retain_each_source_ordinate(axes: str) -> None:
    query_kwargs = {}
    point_kwargs = {}
    if 'Z' in axes:
        query_kwargs['z'] = 2
        point_kwargs['z'] = 100
    if 'M' in axes:
        query_kwargs['m'] = 3
        point_kwargs['m'] = 200
    query = gm.Point(0.1, 1, **query_kwargs)
    source = [
        gm.Point(i, 0, **{k: v + i for k, v in point_kwargs.items()})
        for i in range(1000)
    ]
    rows = gm.GeometryArray([*source, None])[[999, 0, 1000, 32, 31]]
    left, right = gm.nearest_points(query, rows)
    assert list(left.to_wkb()) == [
        query.to_wkb(),
        query.to_wkb(),
        None,
        query.to_wkb(),
        query.to_wkb(),
    ]
    assert list(right.to_wkb()) == [
        source[999].to_wkb(),
        source[0].to_wkb(),
        None,
        source[32].to_wkb(),
        source[31].to_wkb(),
    ]
