"""Pyright-primary assertions for precision Mypy cannot currently retain.

The public stub remains checker-neutral. This fixture isolates exact subtype
relationships whose safe Mypy approximation would otherwise weaken the
contract shared by Pyright users and generated reference documentation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, assert_type

if TYPE_CHECKING:
    from collections.abc import Mapping

    import gometry as gm
    import numpy as np

    def feature_snapshots(
        points: gm.GeometryArray[gm.Point],
        polygons: gm.GeometryArray[gm.Polygon],
        scalar_index: np.ndarray[tuple[()], np.dtype[np.int64]],
        gather: np.ndarray[tuple[int], np.dtype[np.int64]],
        unknown_rank: np.ndarray[tuple[int, ...], np.dtype[np.int64]],
        numpy_ids: np.ndarray[tuple[int], np.dtype[np.int64]],
        arrow_value: gm.ArrowValue,
    ) -> None:
        stations = gm.Features(points, properties={'name': 'Central'}, ids=[7])
        assert_type(stations, gm.Features[gm.Point, str, int])
        assert_type(
            gm.Features(points, properties={'observed': arrow_value}, ids=[7]),
            gm.Features[gm.Point, gm.ArrowValue, int],
        )
        assert_type(
            gm.Features(points, properties={'name': 'Central'}),
            gm.Features[gm.Point, str, str | int | float],
        )
        assert_type(stations[0], gm.Feature[gm.Point, str, int])
        assert_type(stations[scalar_index], gm.Feature[gm.Point, str, int])
        assert_type(stations[gather], gm.Features[gm.Point, str, int])
        assert_type(
            stations[unknown_rank],
            gm.Feature[gm.Point, str, int] | gm.Features[gm.Point, str, int],
        )
        assert_type(stations[[True, False]], gm.Features[gm.Point, str, int])
        assert_type(stations[0].properties, Mapping[str, str] | None)
        assert_type(
            stations.with_geometries(polygons), gm.Features[gm.Polygon, str, int]
        )
        assert_type(stations.__replace__(), gm.Features[gm.Point, str, int])
        assert_type(
            stations.__replace__(geometries=polygons), gm.Features[gm.Polygon, str, int]
        )
        assert_type(
            stations[0].__replace__(geometry=polygons[0]),
            gm.Feature[gm.Polygon, str, int],
        )
        assert_type(
            stations.with_columns({'rank': [1]}), gm.Features[gm.Point, str | int, int]
        )
        assert_type(
            gm.Features.from_columns(points, columns={'rank': [1]}, ids=[7]),
            gm.Features[gm.Point, int, int],
        )
        normalized_ids = gm.Features(points, properties={'name': 'a'}, ids=numpy_ids)
        assert_type(normalized_ids, gm.Features[gm.Point, str, str | int | float])
        inferred_ids = gm.Features(
            points, properties={'name': 'a'}, ids=np.array([1], dtype=np.int64)
        )
        assert_type(inferred_ids, gm.Features[gm.Point, str, str | int | float])
        assert_type(
            gm.Features.from_columns(
                points, columns={'rank': [1]}, ids=np.array([1], dtype=np.int64)
            ),
            gm.Features[gm.Point, int, str | int | float],
        )
        assert_type(
            stations.with_ids(np.array([1], dtype=np.int64)),
            gm.Features[gm.Point, str, str | int | float],
        )
        assert_type(
            gm.Feature(gm.Point(1, 2), properties={'name': 'a'}, id=np.int64(1)),
            gm.Feature[gm.Point, str, str | int | float],
        )
        assert_type(
            gm.Features(points, properties={'name': 'a'}, ids=[np.int64(1)]),
            gm.Features[gm.Point, str, str | int | float],
        )
        assert_type(stations.with_ids(['station']), gm.Features[gm.Point, str, str])

    def mixed_missing_removal(
        values: gm.GeometryArray[gm.Point | gm.LineString | None],
    ) -> None:
        assert_type(
            values.drop_missing(),
            gm.GeometryArray[gm.Point | gm.LineString],
        )

    def nullable_cell_groups(
        cells: gm.CellArray[gm.H3Cell | None],
    ) -> None:
        assert_type(
            cells.children(),
            gm.Groups[gm.CellArray[gm.H3Cell] | None],
        )
