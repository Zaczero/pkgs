"""The complete bounded benchmark manifest.

There are deliberately two profiles: a single-value smoke run that proves the
selected rows execute, and a statistically sampled release run.  Ad-hoc probes
belong in ``benches/cases`` and use ``bench_ab.py``; they do not need another
orchestrator profile.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Profile:
    """One bounded benchmark run."""

    sampling_args: tuple[str, ...]
    paired_sampling_args: tuple[str, ...]
    row_timeout: int
    command_timeout: int
    total_timeout: int
    gometry: tuple[str, ...]
    competitors: tuple[str, ...]
    real_world: tuple[str, ...]

    def rows(self, suite: str) -> tuple[str, ...]:
        return getattr(self, suite)


SMOKE = Profile(
    sampling_args=('--debug-single-value',),
    paired_sampling_args=('--debug-single-value',),
    row_timeout=30,
    command_timeout=180,
    total_timeout=420,
    gometry=(
        'gometry.points/10k',
        'gometry.contains/polygon_points_10k',
        'gometry.from_wkb/1k',
        'gometry.crs_operation_warm/1k',
        'gometry.crs_transform_bounds/1k',
    ),
    competitors=(
        'gometry.points/10k',
        'shapely.points/10k',
        'gometry.contains/polygon_points_10k',
        'shapely.contains/polygon_points_10k',
        'gometry.from_wkb.batch/1k',
        'shapely.from_wkb.batch/1k',
        'gometry.crs_transform/10k',
        'pyproj.Transformer.transform_numpy/10k',
    ),
    real_world=(
        'gometry.real_world.from_geojson/217_countries',
        'shapely.real_world.from_geojson/217_countries',
    ),
)

RELEASE = Profile(
    sampling_args=(
        '--processes',
        '6',
        '--values',
        '5',
        '--warmups',
        '2',
        '--min-time',
        '0.1',
    ),
    # Competitive pairs run once in each lead order. Three processes per pass
    # preserves the six-process release sample budget while cancelling drift.
    paired_sampling_args=(
        '--processes',
        '3',
        '--values',
        '5',
        '--warmups',
        '2',
        '--min-time',
        '0.1',
    ),
    row_timeout=60,
    command_timeout=900,
    total_timeout=2400,
    gometry=(
        'gometry.points/10k',
        'gometry.contains/polygon_points_10k',
        'gometry.crs_transform_bounds/1k',
        'gometry.crs_factors/1k',
        'gometry.to_crs.masked/200k_10pct_missing',
        'gometry.h3_cover.contains_xy/jagged_5k_50k',
        'gometry.distance_3d/128x1024_segments',
        'gometry.index.query_pairs/dense_2k',
        'gometry.geodesic.destination_batch/1k',
        'gometry.smooth/polygon_200',
    ),
    competitors=(
        'gometry.from_wkb.batch/1k',
        'shapely.from_wkb.batch/1k',
        'gometry.split/1k',
        'shapely.split/1k',
        'gometry.line_substring/1k',
        'shapely.line_substring/1k',
        'gometry.h3_cells/10k',
        'h3.latlng_to_cell/10k',
        'gometry.s2_cells/10k',
        's2sphere.cell/10k',
        'gometry.distance/10k',
        'pyproj.Geod.inv/10k',
        'gometry.destination/10k',
        'pyproj.Geod.fwd/10k',
        'gometry.crs_transform/10k',
        'pyproj.Transformer.transform_numpy/10k',
        'gometry.index.build/10k',
        'shapely.index.build/10k',
        'gometry.index.query/boxes_1k',
        'shapely.index.query/boxes_1k',
        'gometry.index.nearest/k10_planar_10k',
        'shapely.index.nearest/k10_planar_10k',
        'gometry.dwithin/pairwise_10k',
        'shapely.dwithin/pairwise_10k',
        'gometry.prepared.contains/polygon_points_10k',
        'shapely.prepared.contains/polygon_points_10k',
        'gometry.intersects/polygon_points_10k',
        'shapely.intersects/polygon_points_10k',
        'gometry.within/polygon_points_10k',
        'shapely.within/polygon_points_10k',
        'gometry.buffer/points_1k',
        'shapely.buffer/points_1k',
        'gometry.union_all/overlap_1k',
        'shapely.union_all/overlap_1k',
        'gometry.union/pairwise_1k',
        'shapely.union/pairwise_1k',
        'gometry.polylabel/1k',
        'shapely.polylabel/1k',
        'gometry.maximum_inscribed_circle/1k',
        'shapely.maximum_inscribed_circle/1k',
        'gometry.crs_geodesic_direct_batch/1k',
        'pyproj.Geod.crs_geodesic_direct_batch/1k',
        'gometry.crs_transform_bounds_3d_corners/1k',
        'pyproj.Transformer.transform_bounds_3d_corners/1k',
    ),
    real_world=(
        'gometry.real_world.from_geojson/217_countries',
        'shapely.real_world.from_geojson/217_countries',
        'gometry.real_world.bounds_warm/217_countries',
        'shapely.real_world.bounds_warm/217_countries',
        'gometry.real_world.area_warm/217_countries',
        'shapely.real_world.area_warm/217_countries',
        'gometry.real_world.point_on_surface/217_countries',
        'shapely.real_world.point_on_surface/217_countries',
    ),
)

PROFILES = {'smoke': SMOKE, 'release': RELEASE}
SUITES = ('gometry', 'competitors', 'real_world')
SCRIPTS = {
    'gometry': 'bench_gometry.py',
    'competitors': 'bench_competitors.py',
    'real_world': 'bench_real_world.py',
}
CHUNK_SIZE = 40
