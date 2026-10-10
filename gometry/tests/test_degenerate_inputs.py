"""Degenerate input: empties of an accepted kind, rings that collapse under a
transform, zero plateaus, and amplification parameters past every budget.

Each case answers or raises the operation's typed error; none may panic,
hang, or surface an internal size overflow.
"""

from __future__ import annotations

import math

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
    assert rows.offsets.tolist() == [0, 1]
    assert rows[0].to_wkt() == ['LINESTRING (0 0, 2 0)']


def test_an_empty_point_encodes_to_an_empty_polyline() -> None:
    assert W('POINT EMPTY', crs=4326).to_polyline() == ''


def test_repeated_point_removal_drops_collapsed_holes_and_keeps_rings_closed() -> None:
    # Both holes collapse below four coordinates at this tolerance.
    polygon = W(
        'POLYGON ((0 0, 10 0, 10 10, 0 0), (1 0.5, 2 0.5, 2 1, 1 0.5),'
        ' (1 0.5, 2 1, 1.5 1.2, 1 0.5))'
    )
    assert polygon.remove_repeated_points(tolerance=0.5).to_wkt() == (
        'POLYGON ((0 0, 10 0, 10 10, 0 0), (1 0.5, 2 1, 1.5 1.2, 1 0.5))'
    )
    # The vertex beside the closure gives way, so the ring stays closed.
    near_closure = W('POLYGON ((0 0, 10 0, 10 10, 0.1 0.1, 0 0))')
    cleaned = near_closure.remove_repeated_points(tolerance=0.5)
    assert cleaned.to_wkt() == 'POLYGON ((0 0, 10 0, 10 10, 0 0))'
    assert cleaned.is_valid
    tiny = W('POLYGON ((0 0, 0.1 0, 0.1 0.1, 0 0))')
    assert tiny.remove_repeated_points(tolerance=0.5).to_wkt() == 'POLYGON EMPTY'


def test_snapping_collapses_rings_instead_of_failing() -> None:
    reference = W('POINT (4 4)')
    holed = W('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (4 4, 4.1 4, 4.1 4.1, 4 4))')
    snapped_holed = gm.snap(holed, reference, 0.5)
    assert snapped_holed.to_wkt() == ('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))')
    assert snapped_holed.is_valid
    collapsed = W('POLYGON ((0 0, 0 0, 0 0, 0 0))')
    snapped_collapsed = gm.snap(collapsed, W('POINT (0 0)'), 0.5)
    assert snapped_collapsed.to_wkt() == 'POLYGON EMPTY'
    assert snapped_collapsed.is_valid


def test_snap_does_not_repair_an_invalid_polygon_result() -> None:
    polygon = W('POLYGON ((0 0, 4 0, 4 4, 2 1, 0 4, 0 0))')
    assert polygon.is_valid

    snapped = gm.snap(polygon, W('POINT (2 -0.1)'), 1.2)
    assert snapped.to_wkt() == ('POLYGON ((0 0, 2 -0.1, 4 0, 4 4, 2 -0.1, 0 4, 0 0))')
    assert not snapped.is_valid


def test_affine_transforms_keep_mixed_axes_rows() -> None:
    rows = gm.GeometryArray([W('POINT (1 2)'), W('POINT Z (1 2 3)')])
    assert rows.translate(1, 1).to_wkt() == ['POINT (2 3)', 'POINT Z (2 3 3)']
    assert rows.affine_transform([2, 0, 0, 2, 0, 0]).to_wkt() == [
        'POINT (2 4)',
        'POINT Z (2 4 3)',
    ]


def test_a_polygon_without_interior_labels_at_once() -> None:
    collinear = W('POLYGON ((0 0, 1 1, 2 2, 0 0))')
    assert collinear.polylabel(tolerance=5e-324).to_wkt() == 'POINT (1 1)'


def test_hausdorff_of_a_covered_filled_operand_terminates() -> None:
    # Every vertex of each operand lies on the other, and the pinched polygon
    # lies inside the square: the covered direction is a zero plateau.
    pinched = W('POLYGON ((0 0, 2 0, 1 1, 2 2, 0 2, 1 1, 0 0))')
    square = gm.box(0, 0, 2, 2)
    expected = math.sqrt(0.5)
    assert gm.hausdorff_distance(pinched, square) == pytest.approx(expected)
    assert gm.hausdorff_distance(square, pinched) == pytest.approx(expected)


@pytest.mark.parametrize(
    ('call', 'parameter'),
    [
        (
            lambda: gm.LineString([(0, 0), (1, 1)]).segmentize(fraction=5e-324),
            'fraction',
        ),
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
