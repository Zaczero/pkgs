//! `split_antimeridian`: split geometries that cross the ±180 meridian into
//! multipart geometries with seam-following edges.
//!
//! A faithful port of the JOSS-reviewed `antimeridian` algorithm
//! (gadomski/antimeridian): walk segments, split on longitude jumps over
//! 180 degrees at the great-circle crossing latitude, stitch polygon
//! fragments back together along the seam (closest-start search toward the
//! pole), close over a pole when a seam search runs off the end, and keep
//! ring winding canonical. Differences from upstream are deliberate:
//! winding always fixes silently (one way), the pole `force_*` flags do not
//! exist (the both-poles winding reversal covers their automatic cases),
//! and seam vertices interpolate Z/M (upstream is XY-only).

// Exact float comparisons are the algorithm's contract: seam longitudes are
// ASSIGNED constants (±180.0) and exact-360 jumps must not split, exactly
// like the upstream reference implementation.
#![allow(clippy::float_cmp)]

use super::*;

mod crs;
mod linestring;
mod normalize;
mod polygon;

pub(crate) use crs::*;
use linestring::*;
pub(crate) use normalize::*;
