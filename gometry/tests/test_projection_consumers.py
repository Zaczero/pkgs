import math

import gometry as gm
import pytest

from tests._support import WEST_RADIAN_SOUTH_GRAD_CRS


def _coords(geom: gm.Geometry) -> list[tuple[float, ...]]:
    return [tuple(coord) for coord in geom.coords]


def _root_ordinates(
    crs: int | str,
    start: tuple[float, float],
    end: tuple[float, float],
    sample: tuple[float, float],
) -> tuple[float, float]:
    root = gm.LineString([start, end], crs=crs)
    prefix = gm.LineString([start, sample], crs=crs)
    fraction = prefix.length / root.length
    return 10.0 + 200.0 * fraction, 100.0 + 400.0 * fraction


def _assert_split_sample(
    pieces: gm.GeometryArray,
    xy: tuple[float, float],
    ordinates: tuple[float, float],
) -> None:
    assert len(pieces) == 2
    before = tuple(pieces[0].coords[-1])
    after = tuple(pieces[1].coords[0])
    assert before == after
    assert before[:2] == pytest.approx(xy, abs=1e-12)
    assert before[2:] == pytest.approx(ordinates, rel=1e-12, abs=1e-12)


def _assert_snap_sample(
    point: tuple[float, ...],
    xy: tuple[float, float],
    ordinates: tuple[float, float],
) -> None:
    assert point[:2] == xy
    assert point[2:] == pytest.approx(ordinates, rel=1e-12, abs=1e-12)


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
    assert array_pieces.offsets.tolist() == [0, 2, 4]
    assert (
        sum(
            (offset, 0.0) in _coords(piece) for group in array_pieces for piece in group
        )
        == 4
    )


@pytest.mark.parametrize(
    ('crs', 'end', 'cut'),
    [
        pytest.param(4326, (120.0, 60.0), (60.0, 30.0), id='degrees'),
        pytest.param(4807, (120.0, 60.0), (60.0, 30.0), id='grads-clarke-1880'),
        pytest.param(
            WEST_RADIAN_SOUTH_GRAD_CRS,
            (2.0, 60.0),
            (1.0, 30.0),
            id='west-radians-south-grads',
        ),
    ],
)
def test_scalar_split_uses_original_root_arclength(
    crs: int | str,
    end: tuple[float, float],
    cut: tuple[float, float],
) -> None:
    source = gm.LineString(
        [(0.0, 0.0), end],
        z=[10.0, 210.0],
        m=[100.0, 500.0],
        crs=crs,
    )
    splitter = gm.Point(*cut, crs=crs)
    expected = _root_ordinates(crs, (0.0, 0.0), end, cut)

    _assert_split_sample(gm.split(source, splitter), cut, expected)


def test_toleranced_split_uses_projection_parameter_for_ordinates() -> None:
    source = gm.LineString(
        [(0.0, 0.0), (120.0, 60.0)],
        z=[10.0, 210.0],
        m=[100.0, 500.0],
        crs=4326,
    )
    reference = gm.Point(60.0, 30.01, crs=4326)
    parameter = (60.0 * 120.0 + 30.01 * 60.0) / (120.0**2 + 60.0**2)
    projection = (120.0 * parameter, 60.0 * parameter)
    expected = _root_ordinates(4326, (0.0, 0.0), (120.0, 60.0), projection)

    _assert_split_sample(
        gm.split(source, reference, tolerance=0.02),
        projection,
        expected,
    )


@pytest.mark.parametrize(
    ('crs', 'end', 'cut'),
    [
        pytest.param(4326, (120.0, 60.0), (60.0, 30.0), id='degrees'),
        pytest.param(4807, (120.0, 60.0), (60.0, 30.0), id='grads-clarke-1880'),
        pytest.param(
            WEST_RADIAN_SOUTH_GRAD_CRS,
            (2.0, 60.0),
            (1.0, 30.0),
            id='west-radians-south-grads',
        ),
    ],
)
def test_scalar_snap_uses_original_root_arclength(
    crs: int | str,
    end: tuple[float, float],
    cut: tuple[float, float],
) -> None:
    source = gm.LineString(
        [(0.0, 0.0), end],
        z=[10.0, 210.0],
        m=[100.0, 500.0],
        crs=crs,
    )
    reference = gm.Point(*cut, crs=crs)
    expected = _root_ordinates(crs, (0.0, 0.0), end, cut)

    snapped = gm.snap(source, reference, tolerance=0.0)

    _assert_snap_sample(tuple(snapped.coords[1]), cut, expected)


def test_toleranced_snap_keeps_reference_xy_and_projection_parameter() -> None:
    source = gm.LineString(
        [(0.0, 0.0), (120.0, 60.0)],
        z=[10.0, 210.0],
        m=[100.0, 500.0],
        crs=4326,
    )
    xy = (60.0, 30.01)
    parameter = (xy[0] * 120.0 + xy[1] * 60.0) / (120.0**2 + 60.0**2)
    projection = (120.0 * parameter, 60.0 * parameter)
    expected = _root_ordinates(4326, (0.0, 0.0), (120.0, 60.0), projection)
    references = (
        gm.MultiPoint([xy], crs=4326),
        gm.MultiPoint(
            [xy, *((0.1 * index, 10.0) for index in range(1, 32))],
            crs=4326,
        ),
    )

    for reference in references:
        snapped = gm.snap(source, reference, tolerance=0.02)
        _assert_snap_sample(tuple(snapped.coords[1]), xy, expected)


def test_snap_array_source_uses_root_arclength_for_row_tolerances() -> None:
    source = gm.LineString(
        [(0.0, 0.0), (120.0, 60.0)],
        z=[10.0, 210.0],
        m=[100.0, 500.0],
        crs=4326,
    )
    xy = (60.0, 30.01)
    parameter = (xy[0] * 120.0 + xy[1] * 60.0) / (120.0**2 + 60.0**2)
    projection = (120.0 * parameter, 60.0 * parameter)
    expected = _root_ordinates(4326, (0.0, 0.0), (120.0, 60.0), projection)
    sources = gm.GeometryArray([source, None, source])[[2, 0, 1]]
    reference = gm.MultiPoint([xy], crs=4326)

    snapped = gm.snap(sources, reference, [0.02, 0.02, 0.02])

    assert snapped.is_missing.tolist() == [False, False, True]
    _assert_snap_sample(tuple(snapped[0].coords[1]), xy, expected)
    _assert_snap_sample(tuple(snapped[1].coords[1]), xy, expected)


def test_snap_fixed_source_uses_root_arclength_for_reference_array_tolerances() -> None:
    source = gm.LineString(
        [(0.0, 0.0), (120.0, 60.0)],
        z=[10.0, 210.0],
        m=[100.0, 500.0],
        crs=4326,
    )
    xy = (60.0, 30.01)
    parameter = (xy[0] * 120.0 + xy[1] * 60.0) / (120.0**2 + 60.0**2)
    projection = (120.0 * parameter, 60.0 * parameter)
    expected = _root_ordinates(4326, (0.0, 0.0), (120.0, 60.0), projection)
    small = gm.MultiPoint([xy], crs=4326)
    indexed = gm.MultiPoint(
        [xy, *((0.1 * index, 10.0) for index in range(1, 32))],
        crs=4326,
    )
    references = gm.GeometryArray([small, indexed])

    snapped = gm.snap(source, references, [0.02, 0.02])

    _assert_snap_sample(tuple(snapped[0].coords[1]), xy, expected)
    _assert_snap_sample(tuple(snapped[1].coords[1]), xy, expected)


def test_snap_paired_arrays_use_root_arclength_for_scalar_tolerance() -> None:
    source = gm.LineString(
        [(0.0, 0.0), (120.0, 60.0)],
        z=[10.0, 210.0],
        m=[100.0, 500.0],
        crs=4326,
    )
    xy = (60.0, 30.01)
    parameter = (xy[0] * 120.0 + xy[1] * 60.0) / (120.0**2 + 60.0**2)
    projection = (120.0 * parameter, 60.0 * parameter)
    expected = _root_ordinates(4326, (0.0, 0.0), (120.0, 60.0), projection)
    sources = gm.GeometryArray([source, source])[[1, 0]]
    references = gm.GeometryArray([
        gm.MultiPoint([xy], crs=4326),
        gm.MultiPoint(
            [xy, *((0.1 * index, 10.0) for index in range(1, 32))],
            crs=4326,
        ),
    ])

    snapped = gm.snap(sources, references, 0.02)

    _assert_snap_sample(tuple(snapped[0].coords[1]), xy, expected)
    _assert_snap_sample(tuple(snapped[1].coords[1]), xy, expected)


def test_split_array_and_group_adapters_resolve_original_roots() -> None:
    forward = gm.LineString(
        [(0.0, 0.0), (120.0, 60.0)],
        z=[10.0, 210.0],
        m=[100.0, 500.0],
        crs=4326,
    )
    reverse = gm.LineString(
        [(120.0, 60.0), (0.0, 0.0)],
        z=[210.0, 10.0],
        m=[500.0, 100.0],
        crs=4326,
    )
    point = gm.Point(60.0, 30.0, crs=4326)
    expected = _root_ordinates(4326, (0.0, 0.0), (120.0, 60.0), (60.0, 30.0))

    scalar_splitter_array = gm.split(forward, gm.GeometryArray([point]))
    _assert_split_sample(scalar_splitter_array, (60.0, 30.0), expected)

    sources = gm.GeometryArray([forward, reverse])
    fixed_splitter = gm.split(sources, point)
    assert fixed_splitter.offsets.tolist() == [0, 2, 4]
    for row in range(2):
        _assert_split_sample(fixed_splitter[row], (60.0, 30.0), expected)

    gathered = sources[[1, 0, 1]]
    paired = gm.split(gathered, gm.GeometryArray([point, point, point]))
    assert paired.offsets.tolist() == [0, 2, 4, 6]
    for row in range(3):
        _assert_split_sample(paired[row], (60.0, 30.0), expected)

    nullable = gm.split(gm.GeometryArray([forward, None, reverse]), point)
    assert nullable.offsets.tolist() == [0, 2, 2, 4]
    assert nullable[1] is None
    _assert_split_sample(nullable[0], (60.0, 30.0), expected)
    _assert_split_sample(nullable[2], (60.0, 30.0), expected)


def test_split_indexed_projection_uses_original_root_arclength() -> None:
    coordinates = [
        (0.0, 0.0),
        (60.0, 60.0),
        *((60.0 + index, 60.0 + 0.1 * index) for index in range(1, 85)),
    ]
    source = gm.LineString(
        coordinates,
        z=[10.0, 210.0, *(211.0 + index for index in range(84))],
        m=[100.0, 500.0, *(501.0 + index for index in range(84))],
        crs=4326,
    )
    projection = (30.005, 30.005)
    cutters = gm.MultiPoint(
        [(30.0, 30.01), (61.5, 60.15), (63.5, 60.35)],
        crs=4326,
    )
    expected = _root_ordinates(4326, (0.0, 0.0), (60.0, 60.0), projection)

    pieces = gm.split(source, cutters, tolerance=0.02)

    assert len(pieces) == 4
    sample = tuple(pieces[0].coords[-1])
    assert sample[:2] == pytest.approx(projection, abs=1e-12)
    assert sample[2:] == pytest.approx(expected, rel=1e-12, abs=1e-12)


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
    assert gm.equals(line, gapped) is False
    assert gm.equals(gapped, line) is False
    assert list(gm.SpatialIndex([line]).query(gapped, predicate='equals')) == []
    assert list(gm.SpatialIndex([gapped]).query(line, predicate='equals')) == []


def test_overlap_residue_reaches_scalar_array_prepared_and_index_predicates() -> None:
    left = gm.LineString([(0.0, 0.0), (2.0**999, 0.0)])
    right = gm.LineString([(2.0**-77, 0.0), (2.0**1009, 0.0)])

    assert gm.overlaps(left, right) is True
    assert gm.overlaps(right, left) is True
    assert list(gm.overlaps(gm.GeometryArray([left]), right)) == [True]
    assert list(gm.overlaps(left, gm.GeometryArray([right]))) == [True]
    assert gm.overlaps(left, right) is True
    assert gm.overlaps(right, left) is True
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
    assert gm.covers(area, line) is False
    assert list(gm.SpatialIndex([line]).query(area, predicate='covers')) == []
    assert list(gm.SpatialIndex([area]).query(line, predicate='covered_by')) == []
