"""Polars binary-Series storage and explicit conversion interop."""

from __future__ import annotations

import gometry as gm
import polars as pl
import pytest
from gometry._polars import from_polars, to_polars

from tests._support import canon


@pytest.fixture
def points() -> gm.GeometryArray:
    return gm.GeometryArray([
        gm.Point(0, 0, crs=4326),
        gm.box(0, 0, 1, 1, crs=4326),
        gm.Point(2, 3, crs=4326),
    ])


def test_to_polars_wkb(points: gm.GeometryArray) -> None:
    series = points.to_polars()
    assert series.dtype == pl.Binary
    assert len(series) == 3
    restored = from_polars(series)
    assert canon(restored) == canon(points)
    assert restored.crs == points.crs


def test_to_polars_rejects_epoch_loss_without_flag(
    points: gm.GeometryArray,
) -> None:
    epoch_points = points.set_epoch(2020.0)
    with pytest.raises(ValueError, match='drop_epoch=True'):
        epoch_points.to_polars()
    restored = from_polars(epoch_points.to_polars(drop_epoch=True))
    assert canon(restored) == canon(points)
    assert restored.crs == points.crs
    assert restored.epoch is None


def test_non_epsg_crs_requires_acknowledgement_and_can_be_restored() -> None:
    points = gm.GeometryArray([gm.Point(1, 2)], crs='OGC:CRS84').set_epoch(2020.0)
    with pytest.raises(ValueError, match='drop_crs=True'):
        to_polars(points, drop_epoch=True)

    series = to_polars(points, drop_crs=True, drop_epoch=True)
    unframed = from_polars(series)
    assert unframed.crs is None
    assert unframed.epoch is None

    restored = from_polars(series, crs='OGC:CRS84', epoch=2020.0)
    assert canon(restored) == canon(points)
    assert restored.crs == points.crs
    assert restored.epoch == 2020.0


def test_missing_rows_round_trip_as_polars_nulls() -> None:
    values = gm.GeometryArray([gm.Point(0, 0), None, gm.Point(2, 3)])
    series = values.to_polars()
    assert series.null_count() == 1

    restored = gm.from_polars(series)
    assert restored.to_wkt() == values.to_wkt()
    assert restored.is_missing.tolist() == [False, True, False]


def test_from_polars_rejects_non_binary_series() -> None:
    with pytest.raises((TypeError, ValueError), match=r'WKB|bytes|binary'):
        gm.from_polars(pl.Series('geometry', [1, 2, 3]))
