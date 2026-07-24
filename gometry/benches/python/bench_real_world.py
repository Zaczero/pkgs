from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar, cast

import shapely
import gometry as gm
from _bench_config import queue_selected_benchmarks
from _bench_config import runner as bench_runner
from _bench_real_world_layers import (
    country_boundary_lines,
    country_boundary_vertices,
)
from shapely import point_on_surface

if TYPE_CHECKING:
    from collections.abc import Callable

_T = TypeVar('_T')
FIXTURE = Path(__file__).resolve().parents[2] / 'fixtures' / 'osm_countries_0_1.geojson'
GEOJSON = FIXTURE.read_text(encoding='utf-8')
GOMETRY_PARTS = cast('gm.GeometryArray[gm.Geometry]', gm.from_geojson(GEOJSON))
GOMETRY_GEOMETRY = gm.GeometryCollection(list(GOMETRY_PARTS), crs=4326)
COUNTRY_COUNT = len(GOMETRY_PARTS)
COUNTRY_LABEL = f'{COUNTRY_COUNT}_countries'
SHAPELY_GEOMETRY = shapely.from_geojson(GEOJSON)
SHAPELY_PARTS = tuple(SHAPELY_GEOMETRY.geoms)
REAL_WORLD_POINTS = country_boundary_vertices(GOMETRY_PARTS)
REAL_WORLD_LINES = country_boundary_lines(GOMETRY_PARTS)
REAL_WORLD_POLYGON = gm.box(*REAL_WORLD_POINTS.total_bounds, crs=4326)
SHAPELY_REAL_WORLD_POINTS = shapely.points(
    REAL_WORLD_POINTS.coords.x, REAL_WORLD_POINTS.coords.y
)
SHAPELY_REAL_WORLD_LINES = tuple(
    shapely.LineString(zip(line.coords.x, line.coords.y, strict=True))
    for line in REAL_WORLD_LINES
)
SHAPELY_REAL_WORLD_POLYGON = shapely.box(*REAL_WORLD_POINTS.total_bounds)


def _validate_checked_result(name: str, result: Any) -> None:
    if name.endswith('from_geojson'):
        if isinstance(result, gm.GeometryArray):
            assert len(result) == COUNTRY_COUNT
        else:
            assert result.geom_type == 'GeometryCollection'
    elif name.endswith(('bounds_cold', 'bounds_warm')):
        assert len(result) == 4
    elif name.endswith(('area_cold', 'area_warm')):
        assert abs(result) > 0
    elif name.endswith('point_on_surface'):
        assert len(result) == COUNTRY_COUNT
    elif name.endswith('buffer_points'):
        assert result.area > 0
    elif name.endswith('contains_points'):
        assert len(result) == len(REAL_WORLD_POINTS)
    elif name.endswith('length_lines'):
        assert result > 0


def _checked(name: str, func: Callable[[], _T]) -> Callable[[], _T]:
    checked = False

    def wrapper() -> _T:
        nonlocal checked
        result = func()
        if not checked:
            _validate_checked_result(name, result)
            checked = True
        return result

    return wrapper


def gometry_from_geojson() -> gm.GeometryArray:
    return cast('gm.GeometryArray', gm.from_geojson(GEOJSON))


def gometry_bounds_cold() -> tuple[float, float, float, float] | None:
    return cast('gm.GeometryArray', gm.from_geojson(GEOJSON)).total_bounds


def gometry_bounds_warm() -> tuple[float, float, float, float] | None:
    return GOMETRY_GEOMETRY.bounds


def gometry_area_cold() -> float:
    return sum(cast('gm.GeometryArray', gm.from_geojson(GEOJSON)).area)


def gometry_area_warm() -> float:
    return GOMETRY_GEOMETRY.area


def gometry_point_on_surface() -> gm.GeometryArray:
    return GOMETRY_PARTS.point_on_surface()


def gometry_buffer_points() -> gm.Geometry:
    return REAL_WORLD_POINTS.buffer(0.05, quadrant_segments=8).union_all()


def gometry_contains_points() -> object:
    return gm.contains(REAL_WORLD_POLYGON, REAL_WORLD_POINTS)


def gometry_length_lines() -> float:
    return float(REAL_WORLD_LINES.length.sum())


def shapely_from_geojson() -> object:
    return shapely.from_geojson(GEOJSON)


def shapely_bounds_cold() -> tuple[float, float, float, float]:
    return shapely.from_geojson(GEOJSON).bounds


def shapely_bounds_warm() -> tuple[float, float, float, float]:
    return SHAPELY_GEOMETRY.bounds


def shapely_area_cold() -> float:
    return float(shapely.area(shapely.from_geojson(GEOJSON)))


def shapely_area_warm() -> float:
    return float(shapely.area(SHAPELY_GEOMETRY))


def shapely_point_on_surface() -> object:
    return point_on_surface(SHAPELY_PARTS)


def shapely_buffer_points() -> object:
    from shapely import buffer, union_all

    return union_all(buffer(SHAPELY_REAL_WORLD_POINTS, 0.05, 8))


def shapely_contains_points() -> object:
    from shapely import contains

    return contains(SHAPELY_REAL_WORLD_POLYGON, SHAPELY_REAL_WORLD_POINTS)


def shapely_length_lines() -> float:
    from shapely import length

    return float(length(SHAPELY_REAL_WORLD_LINES).sum())


def main() -> None:
    runner = bench_runner()
    runner.metadata['project'] = 'gometry'
    runner.metadata['fixture'] = 'osm-countries-0-1'
    flush_benchmarks = queue_selected_benchmarks(runner, 'real_world')
    runner.bench_func(
        f'gometry.real_world.from_geojson/{COUNTRY_LABEL}',
        _checked('gometry.from_geojson', gometry_from_geojson),
    )
    runner.bench_func(
        f'gometry.real_world.bounds_cold/{COUNTRY_LABEL}',
        _checked('gometry.bounds_cold', gometry_bounds_cold),
    )
    runner.bench_func(
        f'gometry.real_world.bounds_warm/{COUNTRY_LABEL}',
        _checked('gometry.bounds_warm', gometry_bounds_warm),
    )
    runner.bench_func(
        f'gometry.real_world.area_cold/{COUNTRY_LABEL}',
        _checked('gometry.area_cold', gometry_area_cold),
    )
    runner.bench_func(
        f'gometry.real_world.area_warm/{COUNTRY_LABEL}',
        _checked('gometry.area_warm', gometry_area_warm),
    )
    runner.bench_func(
        f'gometry.real_world.point_on_surface/{COUNTRY_LABEL}',
        _checked('gometry.point_on_surface', gometry_point_on_surface),
    )
    runner.bench_func(
        f'shapely.real_world.from_geojson/{COUNTRY_LABEL}',
        _checked('shapely.from_geojson', shapely_from_geojson),
    )
    runner.bench_func(
        f'shapely.real_world.bounds_cold/{COUNTRY_LABEL}',
        _checked('shapely.bounds_cold', shapely_bounds_cold),
    )
    runner.bench_func(
        f'shapely.real_world.bounds_warm/{COUNTRY_LABEL}',
        _checked('shapely.bounds_warm', shapely_bounds_warm),
    )
    runner.bench_func(
        f'shapely.real_world.area_cold/{COUNTRY_LABEL}',
        _checked('shapely.area_cold', shapely_area_cold),
    )
    runner.bench_func(
        f'shapely.real_world.area_warm/{COUNTRY_LABEL}',
        _checked('shapely.area_warm', shapely_area_warm),
    )
    runner.bench_func(
        f'shapely.real_world.point_on_surface/{COUNTRY_LABEL}',
        _checked('shapely.point_on_surface', shapely_point_on_surface),
    )
    runner.bench_func(
        f'gometry.real_world.buffer_points/{COUNTRY_LABEL}',
        _checked('gometry.buffer_points', gometry_buffer_points),
    )
    runner.bench_func(
        f'gometry.real_world.contains_points/{COUNTRY_LABEL}',
        _checked('gometry.contains_points', gometry_contains_points),
    )
    runner.bench_func(
        f'gometry.real_world.length_lines/{COUNTRY_LABEL}',
        _checked('gometry.length_lines', gometry_length_lines),
    )
    runner.bench_func(
        f'shapely.real_world.buffer_points/{COUNTRY_LABEL}',
        _checked('shapely.buffer_points', shapely_buffer_points),
    )
    runner.bench_func(
        f'shapely.real_world.contains_points/{COUNTRY_LABEL}',
        _checked('shapely.contains_points', shapely_contains_points),
    )
    runner.bench_func(
        f'shapely.real_world.length_lines/{COUNTRY_LABEL}',
        _checked('shapely.length_lines', shapely_length_lines),
    )
    flush_benchmarks()


if __name__ == '__main__':
    main()
