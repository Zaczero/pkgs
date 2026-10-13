//! Polygonal-coverage kernels: validation, boundary simplification, and
//! cleaning over a `GeometryArray` of polygons that are supposed to tile a
//! region without gaps or overlaps (parcels, admin boundaries — the
//! GEOS/PostGIS "coverage" model; distinct from the DGGS cell coverages).
//!
//! Validation stages an edge map: every boundary segment of every row, keyed
//! by its undirected endpoints. A segment that appears in two rows is a
//! *matched* interface (exactly shared linework — the coverage contract);
//! everything else is candidate exterior boundary, and the validator checks
//! that it genuinely stays exterior to every other row. Topology consumers
//! then use the exact partition as their sole incidence owner.

use std::cmp::Reverse;

pub(crate) use rstar::AABB;

pub(crate) use crate::collections::{HashMap, HashMapExt};
use crate::error::Result;

mod clean;
mod rank;
mod simplify;
mod validate;

#[cfg(test)]
pub(crate) use clean::coverage_clean;
pub(crate) use clean::{
    CoverageCleanOutput, CoverageOverlapRule, coverage_clean_output, coverage_union_topo,
};
pub(crate) use rank::{
    BorderTieBreak, ScaledLength, ScaledLengthSum, longest_border_owner,
    longest_border_ranked_owner, scaled_point_distance,
};
pub(in crate::geometry) use simplify::{TopologyGuard, simplify_chain_guarded, triangle_area};
pub(crate) use simplify::{vw_area_tolerance, *};
#[cfg(test)]
pub(crate) use validate::coverage_invalid_edges;
#[cfg(test)]
pub(crate) use validate::coverage_invalid_edges_geographic_analysis;
pub(crate) use validate::{
    coverage_invalid_edges_cartesian, coverage_invalid_edges_geographic_finished,
    coverage_is_valid, coverage_is_valid_with_domain, coverage_rows, valid_coverage_rows,
    validate_polygonal_rows, visit_invalid_segments,
};
