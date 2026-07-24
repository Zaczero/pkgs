"""Shared gometry↔competitor pairing for benchmark orchestration and summaries."""

from __future__ import annotations

PAIR_OVERRIDES: dict[str, str] = {
    'gometry.polylabel/1k': 'shapely.polylabel/1k',
    'gometry.h3_cells/10k': 'h3.latlng_to_cell/10k',
    'gometry.h3_polygon/10k': 'h3.cell_to_boundary/10k',
    'gometry.h3_compact/10k': 'h3.compact_cells/10k',
    'gometry.h3_to_polygon/10k': 'h3.cells_to_geo/10k',
    'gometry.s2_cells/10k': 's2sphere.cell/10k',
    'gometry.distance/10k': 'pyproj.Geod.inv/10k',
    'gometry.bearing/10k': 'pyproj.Geod.bearing/10k',
    'gometry.destination/10k': 'pyproj.Geod.fwd/10k',
    'gometry.point_between/10k': 'pyproj.Geod.interpolate/10k',
    'gometry.geodesic.destination_batch/1k': 'pyproj.Geod.crs_geodesic_direct_batch/1k',
    'gometry.nearest_m/10k': 'pyproj.Geod.nearest_m/10k',
    'gometry.to_crs_aoi_options/10k': 'pyproj.Transformer.to_crs_aoi_options/10k',
    'gometry.crs_transform/10k': 'pyproj.Transformer.transform_numpy/10k',
    'gometry.crs_transform_buffer/10k': 'pyproj.Transformer.transform_numpy/10k',
    'gometry.crs_transform_aoi/10k': 'pyproj.Transformer.transform_aoi/10k',
    'gometry.crs_transform_3d/10k': 'pyproj.Transformer.transform_3d/10k',
    'gometry.crs_transform_4d/10k': 'pyproj.Transformer.transform_4d/10k',
    'gometry.crs_apply/10k': 'pyproj.Transformer.from_pipeline/10k',
    'gometry.crs_apply_buffer/10k': 'pyproj.Transformer.from_pipeline/10k',
    'gometry.crs_apply_inverse/10k': 'pyproj.Transformer.from_pipeline_inverse/10k',
    'gometry.crs_geodesic_batch/1k': 'pyproj.Geod.crs_geodesic_batch/1k',
    'gometry.crs_geodesic_direct_batch/1k': 'pyproj.Geod.crs_geodesic_direct_batch/1k',
    'gometry.crs_geodesic_interpolate_batch/1k': 'pyproj.Geod.crs_geodesic_interpolate_batch/1k',
    'gometry.crs_geodesic_geometry_batch/1k': 'pyproj.Geod.crs_geodesic_geometry_batch/1k',
    'gometry.crs_roundtrip/1k': 'pyproj.Transformer.roundtrip_reused/1k',
    'gometry.crs_factors/1k': 'pyproj.Proj.factors/1k',
    'gometry.crs_factors_batch/1k': 'pyproj.Proj.factors_batch/1k',
    'gometry.crs_info_churn/120': 'pyproj.CRS.info_churn/120',
    'gometry.crs_operation_churn/120': 'pyproj.Transformer.operation_churn/120',
    'gometry.crs_authority_conversion/120': 'pyproj.CRS.authority_conversion/120',
    'gometry.crs_cf/120': 'pyproj.CRS.cf/120',
    'gometry.crs_list/120': 'pyproj.database.query_crs_info/120',
    'gometry.crs_utm_zones/120': 'pyproj.database.query_utm_crs_info/120',
    'gometry.crs_units/120': 'pyproj.database.get_units_map/120',
    'gometry.crs_non_deprecated/120': 'pyproj.CRS.get_non_deprecated/120',
    'gometry.crs_search/120': 'pyproj.database.query_crs_info_search/120',
    'gometry.crs_exports/120': 'pyproj.CRS.exports/120',
    'gometry.crs_same/120': 'pyproj.CRS.equals/120',
    'gometry.crs_transform_bounds/1k': 'pyproj.Transformer.transform_bounds/1k',
    'gometry.crs_transform_bounds_3d_corners/1k': 'pyproj.Transformer.transform_bounds_3d_corners/1k',
    'gometry.to_wkb.batch/1k': 'shapely.to_wkb.batch/1k',
    'gometry.repair/1k': 'shapely.make_valid/1k',
    'gometry.scale.packed_lines/20k': 'shapely.affine.scale.packed_lines/20k',
    'gometry.skew.packed_lines/20k': 'shapely.affine.skew.packed_lines/20k',
    'gometry.translate.packed_lines/20k': 'shapely.affine.translate.packed_lines/20k',
    'gometry.affine_transform.packed_lines/20k': 'shapely.affine.affine_transform.packed_lines/20k',
}


def _suffix(name: str) -> str:
    return name.split('.', 1)[1]


def find_competitor(gometry_name: str, available: set[str]) -> str | None:
    if gometry_name in PAIR_OVERRIDES:
        override = PAIR_OVERRIDES[gometry_name]
        if override in available:
            return override
    gometry_suffix = _suffix(gometry_name)
    for competitor in sorted(available):
        if competitor.startswith('gometry.'):
            continue
        if _suffix(competitor) == gometry_suffix:
            return competitor
    return None


def find_real_world_competitor(gometry_name: str, available: set[str]) -> str | None:
    if not gometry_name.startswith('gometry.real_world.'):
        return None
    candidate = f'shapely.{_suffix(gometry_name)}'
    return candidate if candidate in available else None


def pair_units(rows: tuple[str, ...], *, suite: str) -> tuple[tuple[str, ...], ...]:
    """Group rows into atomic pair units for pair-aware chunking."""
    if suite == 'real_world':
        matcher = find_real_world_competitor
        driver_prefix = 'gometry.'
    elif suite == 'competitors':
        matcher = find_competitor
        driver_prefix = 'gometry.'
    else:
        return tuple((row,) for row in rows)
    available = set(rows)
    used: set[str] = set()
    units: list[tuple[str, ...]] = []
    for row in rows:
        if row in used:
            continue
        if row.startswith(driver_prefix):
            competitor = matcher(row, available - used)
            if competitor is not None and competitor not in used:
                units.append((row, competitor))
                used.update((row, competitor))
                continue
        units.append((row,))
        used.add(row)
    for row in rows:
        if row not in used:
            units.append((row,))
            used.add(row)
    return tuple(units)


def pair_orderings(
    rows: tuple[str, ...], *, suite: str
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Return balanced A/B and B/A row orderings without splitting pairs."""
    units = pair_units(rows, suite=suite)
    ab = tuple(row for unit in units for row in unit)
    ba = tuple(
        row for unit in units for row in (unit[::-1] if len(unit) == 2 else unit)
    )
    return ab, ba
