"""GeometryArray CRS failures retain the first displayed logical row."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import gometry as gm
import pytest

if TYPE_CHECKING:
    from collections.abc import Callable


def test_sibling_exact_length_checks_coordinate_layout_before_next() -> None:
    class LengthlessCoordinates:
        def __init__(self) -> None:
            self.remaining = sys.maxsize
            self.iter_calls = 0
            self.next_calls = 0

        def __iter__(self):
            self.iter_calls += 1
            return self

        def __next__(self) -> float:
            self.next_calls += 1
            if self.remaining == 0:
                raise StopIteration
            self.remaining -= 1
            return 1.0

    class ExactLengthCoordinates(LengthlessCoordinates):
        def __len__(self) -> int:
            return self.remaining

    x = LengthlessCoordinates()
    y = ExactLengthCoordinates()
    with pytest.raises(OverflowError, match='resource size arithmetic overflowed'):
        gm.crs_transform(4326, 4326, x, y)

    assert x.iter_calls == 1
    assert x.next_calls == 0
    assert y.iter_calls == 0
    assert y.next_calls == 0


def _assert_row_error(operation: Callable[[], object], row: int) -> BaseException:
    with pytest.raises(gm.TransformError) as excinfo:
        operation()
    assert excinfo.value.__notes__ == [f'while processing array element {row}']
    return excinfo.value


@pytest.mark.parametrize('force_over', [False, True], ids=['in-core', 'proj'])
def test_mixed_to_crs_arbitrates_xy_and_xyz_failures_by_logical_row(
    force_over: bool,
) -> None:
    values = gm.GeometryArray([
        gm.Point(-90.0, 0.0, z=1.0, crs=4326),
        gm.Point(-90.0, 0.0, crs=4326),
    ])

    error = _assert_row_error(
        lambda: values.to_crs(32660, force_over=force_over),
        0,
    )
    if force_over:
        assert error.source == 'EPSG:4326'
        assert error.target == 'EPSG:32660'


@pytest.mark.parametrize(
    ('selected', 'row'),
    [
        ('slice', 1),
        ('fancy', 0),
        ('reverse', 0),
        ('duplicate', 0),
    ],
)
@pytest.mark.parametrize('force_over', [False, True], ids=['in-core', 'proj'])
def test_packed_point_to_crs_reports_displayed_selection_row(
    selected: str,
    row: int,
    force_over: bool,
) -> None:
    source = gm.points(
        [0.0, 1.0, -90.0, -90.0],
        [0.0, 0.0, 0.0, 0.0],
        crs=4326,
    )
    values = {
        'slice': source[1:4],
        'fancy': source[[3, 2, 0]],
        'reverse': source[::-1],
        'duplicate': source[[2, 2, 0]],
    }[selected]

    _assert_row_error(lambda: values.to_crs(32660, force_over=force_over), row)


@pytest.mark.parametrize('force_over', [False, True], ids=['in-core', 'proj'])
@pytest.mark.parametrize('kind', ['lines', 'polygons'])
def test_packed_line_and_polygon_to_crs_reports_displayed_row(
    force_over: bool,
    kind: str,
) -> None:
    if kind == 'lines':
        source = gm.GeometryArray([
            gm.LineString([(0.0, 0.0), (1.0, 1.0)], crs=4326),
            gm.LineString([(0.0, 0.0), (-90.0, 0.0)], crs=4326),
            gm.LineString([(2.0, 2.0), (3.0, 3.0)], crs=4326),
        ])
    else:
        source = gm.GeometryArray([
            gm.box(0.0, 0.0, 1.0, 1.0, crs=4326),
            gm.Polygon(
                [(-90.0, 0.0), (-89.0, 0.0), (-89.0, 1.0), (-90.0, 0.0)],
                crs=4326,
            ),
            gm.box(2.0, 2.0, 3.0, 3.0, crs=4326),
        ])

    selected = source[[2, 1, 0]]
    _assert_row_error(lambda: selected.to_crs(32660, force_over=force_over), 1)


@pytest.mark.parametrize('force_over', [False, True], ids=['in-core', 'proj'])
def test_compact_nullable_packed_point_to_crs_reports_gathered_logical_row(
    force_over: bool,
) -> None:
    source = gm.GeometryArray([
        gm.Point(0.0, 0.0, crs=4326),
        None,
        gm.Point(-90.0, 0.0, crs=4326),
    ])
    selected = source[[1, 2, 0]]

    # The point extension and present-only coordinate child are public
    # evidence for the packed nullable path; the mask stays logical.
    assert selected.to_arrow().type.extension_name == 'geoarrow.point'
    assert len(selected.coords.x) == 2

    _assert_row_error(lambda: selected.to_crs(32660, force_over=force_over), 1)


@pytest.mark.parametrize('force_over', [False, True], ids=['in-core', 'proj'])
def test_masked_mixed_to_crs_reports_gathered_logical_row(force_over: bool) -> None:
    """Mixed WKB storage keeps row-aligned missingness outside packed lanes."""
    source = gm.GeometryArray([
        None,
        gm.Point(-90.0, 0.0, crs=4326),
        gm.LineString([(0.0, 0.0), (1.0, 1.0)], crs=4326),
    ])
    selected = source[[2, 0, 1]]

    # A generic WKB extension is the public consequence of heterogeneous
    # storage; the missing row is still before the bad present row after gather.
    assert selected.to_arrow().type.extension_name == 'geoarrow.wkb'
    assert selected.geometry_type == ['LineString', None, 'Point']

    _assert_row_error(lambda: selected.to_crs(32660, force_over=force_over), 2)


@pytest.mark.parametrize('force_over', [False, True], ids=['in-core', 'proj'])
def test_multiple_bad_rows_report_first_logical_row(
    force_over: bool,
) -> None:
    values = gm.points(
        [-90.0, -90.0, 0.0],
        [0.0, 0.0, 0.0],
        crs=4326,
    )

    _assert_row_error(lambda: values.to_crs(32660, force_over=force_over), 0)


def test_array_setup_error_and_scalar_transform_error_have_no_row_note() -> None:
    values = gm.points([0.0], [0.0], crs=4326)
    with pytest.raises(gm.CRSError) as setup:
        values.to_crs('EPSG:999999')
    assert not hasattr(setup.value, '__notes__')

    with pytest.raises(gm.TransformError) as scalar:
        gm.Point(-90.0, 0.0, crs=4326).to_crs(32660)
    assert not hasattr(scalar.value, '__notes__')


def test_successful_homogeneous_packed_transform_stays_one_batch_invocation() -> None:
    gm.crs_clear_cache()
    values = gm.points([0.0, 1.0], [0.0, 1.0], crs=4326)

    transformed = values.to_crs(3857)

    assert transformed.geometry_type == ['Point', 'Point']
    assert gm.crs_cache_info()['transform_invocations'] == 1


def test_successful_mixed_xyz_and_xy_transform_preserves_known_values_and_engine() -> (
    None
):
    gm.crs_clear_cache()
    values = gm.GeometryArray([
        gm.Point(1.0, 2.0, z=7.0, crs=4326),
        gm.Point(-3.0, 4.0, crs=4326),
    ])

    transformed = values.to_crs(3857)

    assert transformed[0].x == pytest.approx(111319.49079327357)
    assert transformed[0].y == pytest.approx(222684.20850554405)
    assert transformed[0].z == pytest.approx(7.0)
    assert transformed[1].x == pytest.approx(-333958.4723798207)
    assert transformed[1].y == pytest.approx(445640.10965602624)
    assert gm.crs_cache_info()['last_transform_engine'] == 'in_core'
