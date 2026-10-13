//! Delaunay and constrained Delaunay triangulation over the exact engine.
//!
//! [`exact_delaunay`] decides every step of the triangulation with exact
//! predicates, so a mesh is Delaunay as built and carries its cocircular
//! flags out of one exact pass; [`cdt`] forces constraint edges into that
//! mesh and refines the faces inside them. This module owns the site and
//! mesh types the Voronoi dual reads, the hull and collinear orders the
//! engine seeds from, and the public entry points.

use std::cmp::Ordering;

use crate::geographic::retained_exact::sites::{
    retained_site_longitude, retained_site_orientation,
};
use crate::geographic::{
    ChartPointSite, ChartPole, GeographicFrame, GeographicRing, PointSelectionChart,
    RetainedLongitudeSite, RingDomain, RingSummary, SeamIdentity,
};
use crate::geometry::publication::{
    ChartCutSide, ChartRestriction, DirectedChartRestrictionUse, SyntheticChartVertex,
};
use crate::geometry::tessellation::{
    EMPTY, MeshEdgeRole, MeshProducer, MeshSiteLocation, MeshUseId, PreparedMeshBoundaryUse,
    PreparedMeshFace, PreparedMeshPublication, PreparedMeshSite, RingOwner, Triangulation, exact,
    next_halfedge, overlapping_rings_error, prev_halfedge,
};
use crate::geometry::{
    CoordinateAxes, Coordinates as _, ExpansionBudget, GENERATED_ITEM_LIMIT, GeometryErrorKind,
    HasM, HasZ, MOrdinate, Point, PointKey, Result, Ring, XY, ZOrdinate, same_point,
};

#[cfg(test)]
thread_local! {
    static DELAUNAY_OUTPUT_RESERVES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn note_delaunay_output_reserve() {
    DELAUNAY_OUTPUT_RESERVES.with(|count| {
        let reserves = count
            .get()
            .checked_add(1)
            .expect("Delaunay output reserve test counter overflow");
        count.set(reserves);
    });
}

#[cfg(test)]
pub(super) fn take_delaunay_output_reserves() -> usize {
    DELAUNAY_OUTPUT_RESERVES.with(|count| count.replace(0))
}

#[path = "cdt.rs"]
mod cdt;
pub(crate) use cdt::{CdtSideBoundaryUse, ConstrainedMeshPlan};
mod certified;
mod hull;
mod publication;
mod refinement;
mod retained;
mod source;

pub(super) use certified::{
    CertifiedDelaunay, CertifiedPrimalEdge, Site, certified_delaunay,
    certified_delaunay_with_coordinates,
};
use hull::delaunay_halfedges;
pub(crate) use hull::delaunay_triangulation;
pub(super) use hull::{exact_corner_hull_cycle_with, exact_hull_cycle_with};
pub(crate) use publication::publish_constrained_plan;
pub(super) use publication::{
    prepare_constrained_triangle_plan, prepare_raw_delaunay_publication,
    prepare_retained_delaunay_publication,
};
pub(crate) use refinement::CdtRefinement;
use refinement::{effective_refinement, validate_refinement_request};
pub(super) use retained::{RetainedPointCoordinates, certified_delaunay_for_retained_points};
use retained::{
    RetainedPolarClosure, compare_retained_x, retained_chart_area,
    retained_point_in_coordinate_cycle, retained_point_in_cycle, retained_site_key,
};
use source::{ConstrainedSourcePlan, constrained_source_plan};
#[cfg(test)]
#[path = "delaunay/chart_restriction_tests.rs"]
mod chart_restriction_tests;
#[cfg(test)]
#[path = "delaunay/tests.rs"]
mod tests;
#[cfg(test)]
pub(super) use hull::{exact_corner_hull_cycle, exact_hull_cycle};
#[cfg(test)]
pub(super) use source::cleanup_constraint_lines;
#[path = "exact_delaunay.rs"]
pub(super) mod exact_delaunay;
#[cfg(test)]
#[path = "delaunay/world_band_tests.rs"]
mod world_band_tests;

// Test-only companion to the output budget: constrained output admission
// must happen before the result vector is reserved.
#[cfg(test)]
thread_local! {
    static CONSTRAINED_OUTPUT_RESERVES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn note_constrained_output_reserve() {
    CONSTRAINED_OUTPUT_RESERVES.with(|cell| cell.set(cell.get().saturating_add(1)));
}

#[cfg(test)]
pub(super) fn take_constrained_output_reserves() -> usize {
    CONSTRAINED_OUTPUT_RESERVES.with(|cell| cell.replace(0))
}
