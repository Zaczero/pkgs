//! Planar and geodesic distance, `dwithin`, nearest-points, Hausdorff and
//! Fréchet distance on `Shape`, plus the geodesic linear-referencing methods.

use crate::error::Result;
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
pub(super) struct RequiredPointTester(());
mod shape_data_distance;
mod shape_data_nearest;
mod shape_impl;

#[cfg(test)]
pub(crate) use distance_3d::distance_3d_brute_parts;
pub(crate) use distance_3d::{Distance3dParts, distance_3d_with_parts};
pub(crate) use frechet::{
    FrechetDpScratch, discrete_frechet_distance, discrete_frechet_distance_with_scratch,
    frechet_distance_line_columns, frechet_distance_line_columns_batch,
};
pub(crate) use geodesic_hausdorff_helpers::geodesic_directed_hausdorff_distance;
#[cfg(test)]
pub(crate) use geodesic_hausdorff_helpers::{
    GEODESIC_HAUSDORFF_TOLERANCE, GeodesicHausdorffWitness, geodesic_hausdorff_counters,
    geodesic_max_min_on_source_segment,
};
pub(crate) use geodesic_parts::{
    ensure_geodesic_linework, geodesic_cap_streaming, geodesic_distance_with_parts,
    geodesic_dwithin_with_parts, geodesic_nearest_points_with_parts,
    geodesic_point_distance_with_parts_and_scratch, geodesic_point_dwithin_with_parts_and_scratch,
    geodesic_segments_cross_streaming,
};
pub(crate) use geodesic_scratch::{
    GeodesicScratch, collect_geodesic_segments_into, collect_point_only_into, geodesic_scratch,
};
pub(in crate::geometry) use geodesic_sweep::GeodesicSweepCaps;
pub(crate) use geodesic_sweep::{
    CapGroup, GeodesicSweepCapsAccum, RowProbe, geodesic_capped_dwithin_with_parts,
    geodesic_capped_sweep, geodesic_capped_sweep_with_parts, geodesic_capped_witness_sweep,
    geodesic_witness_sweep_with_parts,
};
pub(crate) use hausdorff::{
    HausdorffFeature, HausdorffTarget, compact_hausdorff_params,
    directed_hausdorff_distance_squared_shapes, filled_hausdorff_distance,
    hausdorff_distance_line_columns, hausdorff_distance_line_columns_batch,
    push_equidistant_roots_on_interval, push_point_on_line_breakpoint,
    push_segment_projection_breakpoints,
};
#[cfg(test)]
pub(crate) use hausdorff::{
    directed_hausdorff_distance_squared_with_target_columns, hausdorff_distance_4v_exact,
    hausdorff_distance_4v_fused, hausdorff_distance_squared_line_columns,
    max_point_to_target_squared_on_segment_culled_legacy_for_test, sample_hausdorff_on_segment,
    should_build_index,
};
pub(super) use intersects::{
    SQUARED_SPACE_MAX_MAGNITUDE, area_overlap_probe, bounds_squared_safe, coordinate_squared_safe,
    parts_boundary_contact, squared_space_safe,
};
pub(crate) use misc::{bounds_distance_squared, line_length_3d};
pub(super) use nearest::{nearest_probe_to_parts, parts_boundary_witness};
pub(crate) use parts_sweep::{
    any_parts_within, min_parts_to_parts, parts_covers_point, parts_segments_cross,
    puntal_brute_distance, puntal_brute_distance_squared, quick_area_overlap,
};
#[cfg(test)]
mod hausdorff_stats_test;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_geodesic;
