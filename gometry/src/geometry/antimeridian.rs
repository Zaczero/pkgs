//! `split_antimeridian`: split geometries that cross the ±180 meridian into
//! multipart geometries with seam-following edges.
//!
//! The stitching phase walks canonical unwrapped chart segments, splits on
//! jumps over 180 degrees at the affine chart seam crossing, stitches polygon
//! fragments back together along the seam (closest-start search toward the
//! pole), closes over a pole when a seam search runs off the end, and keeps
//! ring winding canonical. The public path fixes winding silently, selects
//! pole closure automatically or through its explicit pole operation, uses
//! exact seam identity, and interpolates Z/M at seam vertices.

// Exact float comparisons are the algorithm's contract: seam longitudes are
// assigned constants (±180.0) and exact-360 jumps must not split.
#![expect(
    clippy::float_cmp,
    reason = "seam topology is defined by exact assigned longitudes and exact 360-degree jumps"
)]

mod crs;
mod linestring;
mod normalize;
mod polygon;

pub(crate) use crs::{
    GeographicDerivedPointScan, GeographicTopologyRoute, antimeridian_frame,
    chain_crosses_antimeridian, classify_geographic_chart, classify_geographic_validity_chart,
    crosses_antimeridian_in_frame, faithful_geographic_chain, geographic_crossing,
    geographic_topology_route, topology_domain,
};
#[cfg(test)]
pub(crate) use linestring::crossing_latitude_in_frame;
pub(crate) use linestring::segment_coords;
use linestring::{SeamRing, collect_points, ring_follows_seam, split_ring_at_seam};
#[cfg(test)]
pub(crate) use normalize::self_intersections_in_frame;
pub(super) use normalize::validate_structure;
pub(crate) use normalize::{
    DerivedPointStrategy, geographic_crossing_bounds,
    geographic_crossing_bounds_for_shapes_in_frame, geographic_crossing_bounds_in_frame,
    is_ring_data_in_frame, is_simple_data_in_frame, repair_data_in_frame, repair_shape_in_frame,
    self_intersections_data_in_frame, unary_antimeridian_centroid, unary_antimeridian_derived,
    validate_data_in_frame, validate_shape_in_frame,
};
