//! Planar and geodesic distance, `dwithin`, nearest-points, Hausdorff and
//! Fréchet distance on `Shape`, plus the geodesic linear-referencing methods.

use crate::error::Result;
mod chart_parts;
mod distance_3d;
mod frechet;
mod geodesic_hausdorff_helpers;
mod geodesic_parts;
mod geodesic_scratch;
mod geodesic_sweep;
mod hausdorff;
mod intersects;
mod misc;
mod nearest;
mod parts_sweep;
mod row_pair;
pub(crate) use row_pair::RowMetricPair;
mod shape_data_distance;
mod shape_data_nearest;
mod shape_impl;

pub(crate) use chart_parts::{
    ChartEdgePart, ChartParts, cached_chart_distance, cached_chart_dwithin,
    cached_chart_nearest_points, point_chart_distance, point_chart_dwithin, point_chart_locate,
};
#[cfg(test)]
pub(crate) use distance_3d::distance_3d_brute_parts;
pub(crate) use distance_3d::{Distance3dParts, distance_3d_with_parts};
pub(crate) use frechet::{
    FrechetDpScratch, discrete_frechet_distance_with_scratch, frechet_distance_line_columns,
    frechet_distance_line_columns_batch,
};
pub(crate) use geodesic_hausdorff_helpers::geodesic_directed_hausdorff_distance;
#[cfg(test)]
pub(crate) use geodesic_hausdorff_helpers::{
    GEODESIC_HAUSDORFF_TOLERANCE, GeodesicHausdorffWitness, geodesic_hausdorff_counters,
    geodesic_max_min_on_source_segment,
};
pub(crate) use geodesic_parts::ensure_geodesic_linework;
#[cfg(test)]
pub(crate) use geodesic_scratch::GeodesicScratch;
pub(crate) use geodesic_scratch::{collect_point_only_into, geodesic_scratch};
pub(crate) use hausdorff::{
    DirectedOptimizationResult, PreparedSupport, directed_inradius_scaled, hausdorff_distance,
    hausdorff_distance_line_columns, hausdorff_distance_line_columns_batch,
};
pub(super) use intersects::{
    area_overlap_probe, bounds_squared_safe, coordinate_squared_safe, parts_boundary_contact,
    squared_space_safe,
};
pub(crate) use misc::{bounds_distance_squared, line_length_3d};
pub(super) use nearest::{nearest_probe_to_parts, parts_boundary_witness};
pub(crate) use parts_sweep::{
    any_parts_within, min_parts_to_parts, parts_covers_point, parts_segments_cross,
    puntal_brute_distance, puntal_brute_distance_squared, quick_area_overlap,
};
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_geodesic;
