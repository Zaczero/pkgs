//! `split_antimeridian`: split geometries that cross the ±180 meridian into
//! multipart geometries with seam-following edges.
//!
//! The stitching phase derives from the JOSS-reviewed `antimeridian` algorithm
//! (gadomski/antimeridian): walk canonical chart segments, split on jumps over
//! 180 degrees at the great-circle crossing latitude, stitch polygon
//! fragments back together along the seam (closest-start search toward the
//! pole), close over a pole when a seam search runs off the end, and keep
//! ring winding canonical. Differences from upstream are deliberate:
//! winding always fixes silently (one way), the pole `force_*` flags do not
//! exist (the both-poles winding reversal covers their automatic cases),
//! seam identity is exact rather than tolerance-snapped, and seam vertices
//! interpolate Z/M (upstream is XY-only).

// Exact float comparisons are the algorithm's contract: seam longitudes are
// ASSIGNED constants (±180.0) and exact-360 jumps must not split, exactly
// like the upstream reference implementation.
#![allow(
    clippy::float_cmp,
    reason = "seam topology is defined by exact assigned longitudes and exact 360-degree jumps"
)]

mod crs;
mod linestring;
mod normalize;
mod polygon;

pub(crate) use crs::{
    crosses_antimeridian_coords_in_frame, crosses_antimeridian_in_frame, geographic_crossing,
    topology_domain,
};
pub(crate) use polygon::split_geographic_view;
use linestring::{collect_points, dedup_consecutive, resolve_seam_aliases};
pub(crate) use linestring::{crossing_latitude_in_frame, segment_coords};
pub(crate) use normalize::{
    DerivedPointStrategy, geographic_crossing_bounds,
    geographic_crossing_bounds_for_shapes_in_frame, geographic_crossing_bounds_in_frame,
    is_ring_data_in_frame, is_simple_data_in_frame, repair_data_in_frame, repair_shape_in_frame,
    self_intersections_in_frame, unary_antimeridian_derived, validate_data_in_frame,
    validate_shape_in_frame,
};
