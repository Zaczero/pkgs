import math

import gometry as gm
import pytest


def _coords(geom: gm.Geometry) -> list[tuple[float, ...]]:
    return [tuple(coord) for coord in geom.coords]


@pytest.mark.parametrize(
    'length',
    [1.0e6, 2.0**60, 2.0**200, 2.0**500, 2.0**1000],
)
@pytest.mark.parametrize('ratio_exponent', [-54, -52])
@pytest.mark.parametrize('reverse', [False, True])
def test_split_keeps_cut_on_both_sides_of_accumulated_offset_ulp_boundary(
    length: float,
    ratio_exponent: int,
    *,
    reverse: bool,
) -> None:
    offset = math.ldexp(length, ratio_exponent)
    coords = [(0.0, 0.0), (length, 0.0), (length, length)]
    if reverse:
        coords.reverse()
    line = gm.LineString(coords)
    cut = gm.Point(length, offset)

    pieces = gm.split(line, cut)

    assert len(pieces) == 2
    assert sum((length, offset) in _coords(piece) for piece in pieces) == 2


def test_split_exact_projection_survives_every_line_point_and_array_carrier() -> None:
    length = 2.0**1022
    offset = 2.0**-54
    forward = gm.LineString([(0.0, 0.0), (length, 0.0)])
    reverse = gm.LineString([(length, 0.0), (0.0, 0.0)])
    point = gm.Point(offset, 0.0)
    multipoint = gm.MultiPoint([(offset, 0.0)])

    for line in (forward, reverse):
        for receiver in (line, gm.MultiLineString([_coords(line)])):
            for splitter in (point, multipoint):
                pieces = gm.split(receiver, splitter)
                assert len(pieces) == 2
                assert sum((offset, 0.0) in _coords(piece) for piece in pieces) == 2

    array_pieces = gm.split(gm.GeometryArray([forward, reverse]), point)
    assert len(array_pieces) == 4
    assert sum((offset, 0.0) in _coords(piece) for piece in array_pieces) == 4


def _clearance_fixture(*, near_end: bool, indexed: bool) -> gm.MultiLineString:
    length = 2.0**1023
    height = 2.0**-53
    x = -(2.0**-52) if near_end else 2.0**-52
    segment = [(-length, 0.0), (0.0, 0.0)] if near_end else [(0.0, 0.0), (length, 0.0)]
    parts = [segment, [(x, height), (x, 1.0)]]
    if indexed:
        parts.extend([
            [(-i - 1.0, 2.0**1000), (-i - 1.0, 2.0**1000 + 1.0)] for i in range(70)
        ])
    return gm.MultiLineString(parts)


@pytest.mark.parametrize('near_end', [False, True])
def test_minimum_clearance_witness_realizes_scalar_at_both_projection_clamps(
    *,
    near_end: bool,
) -> None:
    carriers = [
        _clearance_fixture(near_end=near_end, indexed=False),
        _clearance_fixture(near_end=near_end, indexed=True),
    ]
    for carrier in carriers:
        witness = carrier.minimum_clearance_line()
        assert witness.length == carrier.minimum_clearance()

    batch = gm.GeometryArray(carriers)
    for witness, clearance in zip(
        batch.minimum_clearance_line(), batch.minimum_clearance(), strict=True
    ):
        assert witness.length == clearance


def test_minimum_clearance_brute_and_indexed_carriers_select_same_witness() -> None:
    length = 2.0**1023
    x = 2.0**-52
    height = 2.0**-53
    core = [
        [(0.0, 0.0), (length, 0.0)],
        [(length, 4.0 * height), (0.0, 4.0 * height)],
        [(x, 2.0 * height), (x + 2.0**-40, 2.0 * height)],
    ]
    padding = [[(-i - 1.0, 2.0**1000), (-i - 1.0, 2.0**1000 + 1.0)] for i in range(70)]
    brute = gm.MultiLineString(core)
    indexed = gm.MultiLineString([*core, *padding])

    brute_witness = brute.minimum_clearance_line()
    indexed_witness = indexed.minimum_clearance_line()
    assert brute_witness.length == brute.minimum_clearance()
    assert indexed_witness.length == indexed.minimum_clearance()
    assert brute_witness.to_wkb() == indexed_witness.to_wkb()


def _transpose(matrix: str) -> str:
    return ''.join(matrix[index] for index in (0, 3, 6, 1, 4, 7, 2, 5, 8))


def test_relate_retains_lineal_interior_residue_on_scalar_and_array() -> None:
    length = 2.0**1022
    offset = 2.0**-53
    line = gm.LineString([(0.0, 0.0), (length, 0.0)])
    tail = gm.LineString([(offset, 0.0), (length, 0.0)])
    lineal = '101F00FF2'

    assert gm.relate(line, tail) == lineal
    assert gm.relate(tail, line) == _transpose(lineal)
    assert list(gm.relate(gm.GeometryArray([line]), tail)) == [lineal]
    assert list(gm.relate(tail, gm.GeometryArray([line]))) == [_transpose(lineal)]


def test_relate_retains_mixed_interior_residue_on_scalar_and_array() -> None:
    length = 2.0**1022
    offset = 2.0**-53
    line = gm.LineString([(0.0, 0.0), (length, 0.0)])
    area = gm.box(offset, -1.0, length, 1.0)
    mixed = '101F00212'

    assert gm.relate(line, area) == mixed
    assert gm.relate(area, line) == _transpose(mixed)
    assert list(gm.relate(gm.GeometryArray([line]), area)) == [mixed]
    assert list(gm.relate(area, gm.GeometryArray([line]))) == [_transpose(mixed)]


def test_predicates_cannot_claim_equality_when_difference_has_a_lineal_gap() -> None:
    length = 2.0**1022
    first = 2.0**-81
    second = 2.0**-61
    line = gm.LineString([(0.0, 0.0), (length, 0.0)])
    gapped = gm.MultiLineString([
        [(0.0, 0.0), (first, 0.0)],
        [(second, 0.0), (length, 0.0)],
    ])

    assert not gm.difference(line, gapped).is_empty
    assert gm.equals(line, gapped) is False
    assert gm.equals(gapped, line) is False
    assert gm.contains(gapped, line) is False
    assert gm.within(line, gapped) is False
    assert gm.covers(gapped, line) is False
    assert gm.covered_by(line, gapped) is False
    assert list(gm.equals(gm.GeometryArray([line]), gapped)) == [False]
    assert list(gm.equals(line, gm.GeometryArray([gapped]))) == [False]
    assert line.prepare().equals(gapped) is False
    assert gapped.prepare().equals(line) is False
    assert list(gm.SpatialIndex([line]).query(gapped, predicate='equals')) == []
    assert list(gm.SpatialIndex([gapped]).query(line, predicate='equals')) == []


def test_overlap_residue_reaches_scalar_array_prepared_and_index_predicates() -> None:
    left = gm.LineString([(0.0, 0.0), (2.0**999, 0.0)])
    right = gm.LineString([(2.0**-77, 0.0), (2.0**1009, 0.0)])

    assert gm.overlaps(left, right) is True
    assert gm.overlaps(right, left) is True
    assert list(gm.overlaps(gm.GeometryArray([left]), right)) == [True]
    assert list(gm.overlaps(left, gm.GeometryArray([right]))) == [True]
    assert left.prepare().overlaps(right) is True
    assert right.prepare().overlaps(left) is True
    assert list(gm.SpatialIndex([left]).query(right, predicate='overlaps')) == [0]
    assert list(gm.SpatialIndex([right]).query(left, predicate='overlaps')) == [0]


def test_mixed_residue_reaches_scalar_array_prepared_and_index_predicates() -> None:
    length = 2.0**1022
    offset = 2.0**-53
    line = gm.LineString([(0.0, 0.0), (length, 0.0)])
    area = gm.box(offset, -1.0, length, 1.0)

    assert gm.covers(area, line) is False
    assert gm.covered_by(line, area) is False
    assert list(gm.covers(gm.GeometryArray([area]), line)) == [False]
    assert list(gm.covered_by(gm.GeometryArray([line]), area)) == [False]
    assert area.prepare().covers(line) is False
    assert list(gm.SpatialIndex([line]).query(area, predicate='covers')) == []
    assert list(gm.SpatialIndex([area]).query(line, predicate='covered_by')) == []
