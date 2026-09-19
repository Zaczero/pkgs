"""Degenerate input: empties of an accepted kind, rings that collapse under a
transform, zero plateaus, and amplification parameters past every budget.

Each case answers or raises the operation's typed error; none may panic,
hang, or surface an internal size overflow.
"""

from __future__ import annotations

import gometry as gm
import pytest

W = gm.from_wkt


@pytest.mark.parametrize('wkt', ['LINESTRING EMPTY', 'MULTILINESTRING EMPTY'])
def test_lineal_operations_answer_empty_linework(wkt: str) -> None:
    empty = W(wkt)
    assert empty.line_merge().to_wkt() == wkt
    assert empty.offset_curve(1.0).to_wkt() == wkt
    assert gm.split(empty, gm.Point(1, 0)).to_wkt() == []


def test_empty_collections_offset_to_themselves() -> None:
    assert W('GEOMETRYCOLLECTION EMPTY').offset_curve(1.0).to_wkt() == (
        'GEOMETRYCOLLECTION EMPTY'
    )


@pytest.mark.parametrize('splitter', ['POINT EMPTY', 'MULTIPOINT EMPTY'])
def test_an_empty_splitter_leaves_the_line_whole(splitter: str) -> None:
    line = gm.LineString([(0, 0), (2, 0)])
    assert gm.split(line, W(splitter)).to_wkt() == ['LINESTRING (0 0, 2 0)']
    rows = gm.split(gm.GeometryArray([line]), gm.GeometryArray([W(splitter)]))
    assert rows.to_wkt() == ['LINESTRING (0 0, 2 0)']


def test_an_empty_point_encodes_to_an_empty_polyline() -> None:
    assert W('POINT EMPTY', crs=4326).to_polyline() == ''


def test_affine_transforms_keep_mixed_axes_rows() -> None:
    rows = gm.GeometryArray([W('POINT (1 2)'), W('POINT Z (1 2 3)')])
    assert rows.translate(1, 1).to_wkt() == ['POINT (2 3)', 'POINT Z (2 3 3)']
    assert rows.affine_transform([2, 0, 0, 2, 0, 0]).to_wkt() == [
        'POINT (2 4)',
        'POINT Z (2 4 3)',
    ]


@pytest.mark.parametrize(
    ('call', 'parameter'),
    [
        (lambda: gm.LineString([(0, 0), (1, 1)]).segmentize(fraction=5e-324), 'fraction'),
        (lambda: gm.LineString([(0, 0), (1, 1)]).segmentize(5e-324), 'max_length'),
        (
            lambda: gm.frechet_distance(
                gm.LineString([(0, 0), (1, 1)]),
                gm.LineString([(0, 1), (1, 0)]),
                densify=5e-324,
            ),
            'densify',
        ),
    ],
    ids=['segmentize-fraction', 'segmentize-max-length', 'frechet-densify'],
)
def test_amplification_past_every_count_names_its_parameter(
    call: object, parameter: str
) -> None:
    with pytest.raises(gm.GeometryError, match=f'reduce {parameter}$') as info:
        call()  # type: ignore[operator]
    assert info.value.parameter == parameter
    assert info.value.produced > info.value.limit > 0
