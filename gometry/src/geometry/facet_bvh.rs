//! Packed facet BVH — the vertex-sweep distance engine.
//!
//! Linework segments are grouped into *facets* of up to [`FACET_SEGMENTS`]
//! consecutive segments (the GEOS `IndexedFacetDistance` grouping, sized to the
//! SIMD lane count), and a flat binary AABB tree over the facets drives
//! branch-and-bound point probes. The exact point-vs-facet kernel evaluates
//! all eight segment distances in one vector op.
//!
//! There is deliberately NO segment×segment machinery: distance callers
//! guarantee disjoint operands (the `intersects` short-circuit), and for
//! non-crossing segments the pair distance is the min of the four
//! vertex-onto-segment projections — so two vertex-vs-linework sweeps cover
//! every segment pair. See `distance_disjoint_with_parts` for the convexity
//! argument.
//!
//! Exactness: traversal bounds only *prune* (envelope distance is a true
//! lower bound), so results equal the brute-force folds; `min` over segment
//! distances is evaluation-order independent. The SIMD kernel computes the
//! same clamped-projection distance as the scalar kernel (lane math may
//! reassociate — measurement arithmetic per the project float policy).

mod aabb;
mod engine;
mod simd;

pub(crate) use aabb::{
    aabb_distance, aabb_distance_squared, aabb_max_distance_squared, aabbs_overlap, point_aabb,
    segment_aabb, union_aabb,
};
pub(crate) use engine::{Facet, FacetBvh, NearestCandidate, PreparedLinework};
pub(crate) use simd::simd_point_facet_distance_squared;

/// Segments per facet — one SIMD register of point-to-segment distances.
pub(crate) const FACET_SEGMENTS: usize = 8;
// FACET_SEGMENTS is a geometry-topology constant (9 facet vertices -> 8
// segments), independent of SIMD width — but it currently matches the
// reduction lane count, which the facet kernels rely on. Assert the
// coincidence so a SIMD_VECTOR_BITS retarget fails the build here instead of
// silently leaving this at 8.
const _: () = assert!(FACET_SEGMENTS == crate::geometry::REDUCE_LANES);

/// Per-side segment count below which the BVH build does not pay for itself
/// and the flat facet scan (point probes) or scalar pairs (segment phase)
/// win.
pub(crate) const BVH_MIN_INDEXED_SEGMENTS: usize = 64;
