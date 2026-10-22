"""Directed joins use geographic site identity and retain source chart edges."""

from __future__ import annotations

import gometry as gm
import numpy as np
import pytest


@pytest.mark.parametrize(('crs', 'seam'), [(4326, 180.0), (4807, 200.0)])
def test_directed_merge_joins_forward_seam_aliases(crs: int, seam: float) -> None:
    source = gm.MultiLineString(
        [
            [(seam - 10, 0), (seam, 0)],
            [(-seam, 0), (-seam + 10, 0)],
        ],
        crs=crs,
    )
    result = source.line_merge(directed=True)
    assert isinstance(result, gm.LineString)
    assert result.crs == source.crs
    assert result.equals(source)
    assert result.length == pytest.approx(source.length, rel=1e-10)


def test_directed_merge_does_not_reverse_opposed_seam_lines() -> None:
    source = gm.MultiLineString(
        [
            [(170, 0), (180, 0)],
            [(-170, 0), (-180, 0)],
        ],
        crs=4326,
    )
    assert source.line_merge(directed=True) == source


def test_directed_seam_join_keeps_distinct_ordinate_occurrences_and_nullable_rows() -> (
    None
):
    source = gm.from_wkt(
        'MULTILINESTRING ZM ((170 0 1 10, 180 0 3 30), (-180 0 4 40, -170 0 5 50))',
        crs=4326,
        epoch=2025,
    )
    result = source.line_merge(directed=True)
    assert isinstance(result, gm.LineString)
    assert result.epoch == source.epoch
    assert result.coordinate_axes == 'XYZM'
    np.testing.assert_array_equal(
        np.asarray(result.coords)[:, 2:],
        [
            [1, 10],
            [3, 30],
            [4, 40],
            [5, 50],
        ],
    )
    assert result.length == pytest.approx(source.length, rel=1e-10)
    rows = gm.GeometryArray([None, source, source])[::-1].line_merge(directed=True)
    assert rows.to_wkt() == [result.to_wkt(), result.to_wkt(), None]


@pytest.mark.parametrize('crs', [None, 4326])
def test_directed_join_keeps_distinct_signed_zero_ordinates(crs) -> None:
    source = gm.from_wkt(
        'MULTILINESTRING ZM ((0 0 1 -1, 1 0 -0 -0), (1 0 0 0, 2 0 2 1))',
        crs=crs,
    )
    result = source.line_merge(directed=True)
    assert len(result.coords) == 4
    np.testing.assert_array_equal(
        np.asarray(result.coords)[1:3, 2:].view(np.uint64),
        [[1 << 63, 1 << 63], [0, 0]],
    )


@pytest.mark.parametrize('sign', [1, -1], ids=['eastward', 'westward'])
def test_directed_join_preserves_authored_full_period_edges(sign: int) -> None:
    source = gm.MultiLineString(
        [
            [(sign * 170, -10), (sign * 180, -10)],
            [(-sign * 180, -10), (sign * 180, 10)],
            [(-sign * 180, 10), (-sign * 170, 10)],
        ],
        crs=4326,
    )
    result = source.line_merge(directed=True)
    assert isinstance(result, gm.LineString)
    assert result.equals(source)
    assert result.length == pytest.approx(source.length, rel=1e-10)
    assert result.length > 35_000_000


def test_full_period_seam_loop_remains_a_branch_barrier() -> None:
    source = gm.MultiLineString(
        [
            [(170, 0), (180, 0)],
            [(-180, 0), (180, 0)],
            [(-180, 0), (-170, 0)],
        ],
        crs=4326,
    )
    result = source.line_merge(directed=True)
    assert isinstance(result, gm.MultiLineString)
    assert len(result.parts) == 3
    assert result.length == pytest.approx(source.length, rel=1e-10)


def test_full_period_join_samples_inserted_zm_from_its_own_source_arc() -> None:
    source = gm.from_wkt(
        'MULTILINESTRING ZM ('
        '(170 -10 1 10, 180 -10 2 20), '
        '(-180 -10 3 30, 180 10 5 50), '
        '(-180 10 6 60, -170 10 7 70))',
        crs=4326,
    )
    result = source.line_merge(directed=True)
    assert isinstance(result, gm.LineString)
    coords = np.asarray(result.coords)
    # The symmetric -10..10 degree full-period source arc has its metric
    # midpoint at latitude zero; its endpoint ordinates are (3, 30) and (5, 50).
    midpoint = coords[coords[:, 1] == 0]
    assert len(midpoint) == 1
    np.testing.assert_allclose(midpoint[:, 2:], [[4, 40]], rtol=1e-12)
    assert result.length == pytest.approx(source.length, rel=1e-10)


@pytest.mark.parametrize(('crs', 'pole'), [(4326, 90.0), (4807, 100.0)])
def test_directed_merge_joins_pole_aliases_without_changing_source_meridians(
    crs: int, pole: float
) -> None:
    source = gm.MultiLineString(
        [
            [(0, pole - 10), (0, pole)],
            [(120, pole), (120, pole - 10)],
        ],
        crs=crs,
    )
    result = source.line_merge(directed=True)
    assert isinstance(result, gm.LineString)
    assert result.equals(source)
    assert result.length == pytest.approx(source.length, rel=1e-10)
    np.testing.assert_array_equal(
        np.asarray(result.coords),
        [
            [0, pole - 10],
            [0, pole],
            [120, pole],
            [120, pole - 10],
        ],
    )


@pytest.mark.parametrize(
    'longitude', [180, 360], ids=['pole_half_turn', 'pole_full_turn']
)
def test_alias_join_does_not_sample_ambiguous_ordinates_on_a_pole_only_edge(
    longitude: int,
) -> None:
    alias = longitude - 360
    source = gm.from_wkt(
        f'MULTILINESTRING ZM ((0 80 1 10, 0 90 2 20, {longitude} 90 3 30, '
        f'{longitude} 70 4 40), ({alias} 70 5 50, {alias} 60 6 60))',
        crs=4326,
    )
    result = source.line_merge(directed=True)
    assert isinstance(result, gm.LineString)
    assert result.equals(source)
    np.testing.assert_array_equal(
        np.asarray(result.coords)[:, 2:],
        [
            [1, 10],
            [2, 20],
            [3, 30],
            [4, 40],
            [5, 50],
            [6, 60],
        ],
    )
    assert result.length == pytest.approx(source.length, rel=1e-10)
