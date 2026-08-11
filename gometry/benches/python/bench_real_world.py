from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import shapely
import gometry as gm
from _bench_config import queue_selected_benchmarks
from _bench_config import runner as bench_runner
from _bench_real_world_layers import (
    country_boundary_lines,
    country_boundary_vertices,
)
from shapely import point_on_surface

FIXTURE = Path(__file__).resolve().parents[2] / 'fixtures' / 'osm_countries_0_1.geojson'
GEOJSON = FIXTURE.read_text(encoding='utf-8')
GOMETRY_PARTS = cast(
    'gm.GeometryArray[gm.Geometry]', gm.from_geojson(GEOJSON, crs=4326)
)
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


def gometry_from_geojson() -> gm.GeometryArray:
    return cast('gm.GeometryArray', gm.from_geojson(GEOJSON, crs=4326))


def gometry_bounds_cold() -> tuple[float, float, float, float] | None:
    return cast('gm.GeometryArray', gm.from_geojson(GEOJSON, crs=4326)).total_bounds


def gometry_bounds_warm() -> tuple[float, float, float, float] | None:
    return GOMETRY_GEOMETRY.bounds


def gometry_area_cold() -> float:
    return sum(cast('gm.GeometryArray', gm.from_geojson(GEOJSON, crs=4326)).area)


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
        gometry_from_geojson,
    )
    runner.bench_func(
        f'gometry.real_world.bounds_cold/{COUNTRY_LABEL}',
        gometry_bounds_cold,
    )
    runner.bench_func(
        f'gometry.real_world.bounds_warm/{COUNTRY_LABEL}',
        gometry_bounds_warm,
    )
    runner.bench_func(
        f'gometry.real_world.area_cold/{COUNTRY_LABEL}',
        gometry_area_cold,
    )
    # Planar area_warm deleted; public geodesic_area is the RELEASE metric.
    runner.bench_func(
        f'gometry.real_world.point_on_surface/{COUNTRY_LABEL}',
        gometry_point_on_surface,
    )
    runner.bench_func(
        f'shapely.real_world.from_geojson/{COUNTRY_LABEL}',
        shapely_from_geojson,
    )
    runner.bench_func(
        f'shapely.real_world.bounds_cold/{COUNTRY_LABEL}',
        shapely_bounds_cold,
    )
    runner.bench_func(
        f'shapely.real_world.bounds_warm/{COUNTRY_LABEL}',
        shapely_bounds_warm,
    )
    runner.bench_func(
        f'shapely.real_world.area_cold/{COUNTRY_LABEL}',
        shapely_area_cold,
    )
    # Planar shapely area_warm deleted; public geodesic_area uses pyproj.
    runner.bench_func(
        f'shapely.real_world.point_on_surface/{COUNTRY_LABEL}',
        shapely_point_on_surface,
    )
    runner.bench_func(
        f'gometry.real_world.buffer_points/{COUNTRY_LABEL}',
        gometry_buffer_points,
    )
    runner.bench_func(
        f'gometry.real_world.contains_points/{COUNTRY_LABEL}',
        gometry_contains_points,
    )
    runner.bench_func(
        f'gometry.real_world.length_lines/{COUNTRY_LABEL}',
        gometry_length_lines,
    )
    runner.bench_func(
        f'shapely.real_world.buffer_points/{COUNTRY_LABEL}',
        shapely_buffer_points,
    )
    runner.bench_func(
        f'shapely.real_world.contains_points/{COUNTRY_LABEL}',
        shapely_contains_points,
    )
    runner.bench_func(
        f'shapely.real_world.length_lines/{COUNTRY_LABEL}',
        shapely_length_lines,
    )
    _register_public_release_ops(runner, 'real_world')
    flush_benchmarks()


def _register_public_release_ops(runner: Any, suite: str) -> None:
    """Register Lane-2 public timed callables (selected rows only; lazy fixtures)."""
    from _bench_config import register_selected_public_release_ops

    register_selected_public_release_ops(runner, suite)


if __name__ == '__main__':
    main()
