//! Planar Hausdorff distance: directed targets and the kernel core.

mod algorithm;
mod breakpoints;
mod coverage;
mod kernel;
mod quadratic;
mod query;
mod target;

#[cfg(test)]
pub(crate) use algorithm::directed_hausdorff_distance_squared_with_target_columns;
pub(crate) use algorithm::{
    directed_hausdorff_distance_squared_with_target_pruned,
    directed_hausdorff_distance_squared_with_target_pruned_initial,
    directed_hausdorff_vertex_lower_bound_squared, hausdorff_segment_upper_bound_squared,
    sqrt_distance_squared,
};
pub(crate) use breakpoints::{
    EquidistantRootSink, HausdorffFeature, HausdorffParamSink, SmallEquidistantRoots,
    SmallHausdorffParams, compact_hausdorff_params,
    hausdorff_feature_bbox_disjoint_from_expanded_source, hausdorff_feature_distance_squared_at,
    max_point_to_target_squared_on_segment_culled, push_point_on_line_breakpoint,
    push_segment_projection_breakpoints,
};
#[cfg(test)]
pub(crate) use breakpoints::{
    max_point_to_target_squared_on_segment_culled_legacy_for_test,
    point_envelope_dual_hull_len_for_test, sample_hausdorff_on_segment,
};
pub(crate) use coverage::segment_radius_coverage_certified;
pub(crate) use kernel::{
    HausdorffTargetLike, directed_hausdorff_distance_squared_shapes,
    hausdorff_distance_line_columns, hausdorff_distance_line_columns_batch,
};
#[cfg(test)]
pub(crate) use kernel::{
    hausdorff_distance_4v_exact, hausdorff_distance_4v_fused,
    hausdorff_distance_squared_line_columns,
};
pub(crate) use quadratic::{HausdorffQuadratic, push_equidistant_roots_on_interval};
pub(crate) use query::{
    HausdorffLineworkQuery, HausdorffProbeResult, hausdorff_segment_tight_upper_bound_squared,
    stats,
};
pub(crate) use target::{
    HAUSDORFF_SMALL_TARGET_MAX_VERTICES, HausdorffTarget, SmallLineTarget,
    max_point_to_target_squared_on_segment_small_culled, small_line_target_distance_squared,
    small_line_target_features_slice,
};
#[cfg(test)]
pub(crate) use target::{
    collect_hausdorff_segment_params_small,
    evaluate_max_point_to_target_squared_on_segment_bisect_small, should_build_index,
};
