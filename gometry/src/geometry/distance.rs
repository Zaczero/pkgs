//! Planar and geodesic distance, `dwithin`, nearest-points, Hausdorff and
//! Fréchet distance on `Shape`, plus the geodesic linear-referencing methods.

use crate::error::Result;
mod chart_hausdorff;
mod chart_parts;
mod chart_scratch;
mod distance_3d;
mod exact_segment;
mod frechet;
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

#[cfg(test)]
pub(crate) use chart_hausdorff::{ChartHausdorffWitness, counters as chart_hausdorff_counters};
pub(crate) use chart_hausdorff::{
    chart_directed_hausdorff_distance, ensure_geographic_hausdorff_linework,
};
pub(crate) use chart_parts::{
    ChartEdgePart, ChartParts, cached_chart_distance, cached_chart_dwithin,
    cached_chart_nearest_points, cached_chart_nearest_points_with_intent, point_chart_distance,
    point_chart_dwithin, point_chart_locate,
};
pub(crate) use chart_scratch::chart_scratch;
#[cfg(test)]
pub(crate) use distance_3d::distance_3d_brute_parts;
pub(crate) use distance_3d::{Distance3dParts, distance_3d_with_parts};
pub(in crate::geometry) use exact_segment::{ExactPoint, point_segment_distance_squared_exact};
pub(crate) use frechet::{
    FrechetDpScratch, discrete_frechet_distance_with_scratch, frechet_distance_line_columns,
    frechet_distance_line_columns_batch,
};
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
