//! Exact Cartesian source support for point and contact-witness queries.
//!
//! This module owns the source-side fold used by scalar, packed, prepared,
//! and indexed point membership, plus the stored, rational, and infinitesimal
//! probes used by DE-9IM evaluation. It deliberately does not build an
//! arrangement: away from source contact, directed winding is locally
//! constant; at contact, the relevant topology is the exact fan of source
//! rays incident to the query.

#![expect(
    clippy::float_cmp,
    reason = "coordinate identity and formal fan ordering use exact represented values"
)]

use std::ops::Range;

use crate::error::Result;
#[cfg(test)]
use crate::geometry::Ring;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::segments::{stored_orientation_sign, try_sort_by};
use crate::geometry::tessellation::exact::{
    ExactDyadic, compare_ratios, outward_ratio_bounds, round_ratio,
};
use crate::geometry::{
    Bounds, Loc, Point, Polygon, Shape, SharedShapeOwners, XY, point_on_segment, same_point,
    same_topological_coordinate, storage,
};

mod query;
mod rays;
mod streaming;
#[cfg(test)]
mod tests;

pub(in crate::geometry) use query::{
    ExactQuery, InfinitesimalQuery, PointQuery, QueryAxis, StoredQuery,
    exact_offset_point_in_bounds, exact_point_in_bounds, exact_query_xy, exact_query_y,
    exact_stored_point,
};
use rays::{
    Ray, compare_query_rays, line_location, query_ray_groups, symbolic_orientation_from_sign,
};
#[cfg(test)]
use rays::{RayGroup, has_nonzero_segment};
pub(in crate::geometry) use rays::{compare_rays, exact_edge_winding, exact_offset_edge_winding};
pub(crate) use rays::{edge_winding, ring_kind_columns};
use streaming::LocationMode;
pub(in crate::geometry) use streaming::StreamingLocation;

#[derive(Clone, Copy)]
pub(crate) enum PointMembership {
    Strict,
    BoundaryInclusive,
}

impl PointMembership {
    pub(crate) const fn accepts(self, location: Loc) -> bool {
        match self {
            Self::Strict => matches!(location, Loc::Interior),
            Self::BoundaryInclusive => !matches!(location, Loc::Exterior),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::geometry) struct SourcePointLocation {
    pub(in crate::geometry) location: Loc,
    pub(in crate::geometry) area: bool,
}

impl SourcePointLocation {
    /// Project a full node query onto the areal label of a one-dimensional
    /// ring atom. Point and line support cannot label that atom.
    pub(in crate::geometry) const fn edge_atom_location(self) -> Loc {
        if self.area {
            self.location
        } else {
            Loc::Exterior
        }
    }
}

pub(in crate::geometry) trait RingEdgeCandidates {
    fn ring_view(&self) -> RingView<'_>;
    fn visit_stabbed_edges(
        &self,
        y: f64,
        visit: &mut dyn FnMut(XY, XY) -> Result<()>,
    ) -> Result<()>;
}

/// Whether a stored ring supplies areal winding or collapsed line support.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RingKind {
    Winding,
    Collapsed,
}

const fn ring_ray_delta(kind: RingKind, reverse: bool) -> i64 {
    match (kind, reverse) {
        (RingKind::Winding, false) => 1,
        (RingKind::Winding, true) => -1,
        (RingKind::Collapsed, _) => 0,
    }
}

#[derive(Clone, Copy)]
pub(crate) struct RingView<'a> {
    pub(crate) xs: &'a [f64],
    pub(crate) ys: &'a [f64],
    pub(crate) kind: RingKind,
}

/// Cartesian location for any source shape.
pub(crate) fn cartesian_point_location(shape: &Shape, point: Point) -> Loc {
    let mut location = StreamingLocation::for_stored_point(point);
    let mut owners = None;
    location
        .visit_shape(shape, &mut owners)
        .expect("stored point location does not construct exact values");
    location
        .finish_location()
        .expect("stored point location does not construct exact values")
}

pub(in crate::geometry) fn cartesian_point_location_and_area(
    shape: &Shape,
    point: Point,
) -> SourcePointLocation {
    let mut location = StreamingLocation::for_stored_point(point);
    let mut owners = None;
    location
        .visit_shape(shape, &mut owners)
        .expect("stored point location does not construct exact values");
    location
        .finish()
        .expect("stored point location does not construct exact values")
}

pub(crate) fn cartesian_row_point_location(
    row: storage::ShapeRow<'_>,
    point: Point,
    coverage: bool,
) -> Loc {
    let mut location = StreamingLocation::for_stored_point(point);
    let mut shared = SharedShapeOwners::for_collections();
    let mut owners = coverage.then_some(&mut shared);
    location
        .visit_row(row, &mut owners)
        .expect("stored point location does not construct exact values");
    location
        .finish_location()
        .expect("stored point location does not construct exact values")
}

pub(in crate::geometry) fn cartesian_row_point_location_and_area(
    row: storage::ShapeRow<'_>,
    point: Point,
) -> SourcePointLocation {
    let mut location = StreamingLocation::for_stored_point(point);
    let mut owners = None;
    location
        .visit_row(row, &mut owners)
        .expect("stored point location does not construct exact values");
    location
        .finish()
        .expect("stored point location does not construct exact values")
}

/// Boolean point coverage is a set-membership query: duplicate components can
/// change a line point's boundary/interior label, but not whether it is present.
pub(crate) fn cartesian_point_covers(shape: &Shape, point: Point) -> bool {
    cartesian_row_point_location(shape.into(), point, true) != Loc::Exterior
}

pub(crate) fn cartesian_area_covers_point(shape: &Shape, point: Point) -> bool {
    cartesian_row_area_covers_point(shape.into(), point)
}

pub(crate) fn cartesian_row_area_covers_point(row: storage::ShapeRow<'_>, point: Point) -> bool {
    let mut location = StreamingLocation::new(StoredQuery(point.xy()), LocationMode::Area);
    let mut shared_owners = SharedShapeOwners::for_collections();
    let mut owners = Some(&mut shared_owners);
    location
        .visit_row(row, &mut owners)
        .expect("stored point location does not construct exact values");
    location
        .finish()
        .expect("stored point location does not construct exact values")
        .area
}

pub(in crate::geometry) fn cartesian_exact_point_location_and_area(
    shape: &Shape,
    point: &[ExactDyadic; 3],
) -> Result<SourcePointLocation> {
    if let Some(point) = exact_stored_point(point) {
        return Ok(cartesian_point_location_and_area(shape, point));
    }
    let mut location = StreamingLocation::for_exact_point(point)?;
    let mut owners = None;
    location.visit_shape(shape, &mut owners)?;
    location.finish()
}

#[cfg(test)]
pub(in crate::geometry) fn cartesian_exact_point_location(
    shape: &Shape,
    point: &[ExactDyadic; 3],
) -> Result<Loc> {
    Ok(cartesian_exact_point_location_and_area(shape, point)?.location)
}

pub(in crate::geometry) fn cartesian_exact_offset_location_and_area(
    shape: &Shape,
    point: &[ExactDyadic; 3],
    direction: &[ExactDyadic; 2],
) -> Result<SourcePointLocation> {
    let mut location = StreamingLocation::for_exact_offset(point, direction)?;
    let mut owners = None;
    location.visit_shape(shape, &mut owners)?;
    location.finish()
}

#[cfg(test)]
pub(in crate::geometry) fn cartesian_exact_offset_location(
    shape: &Shape,
    point: &[ExactDyadic; 3],
    direction: &[ExactDyadic; 2],
) -> Result<Loc> {
    Ok(cartesian_exact_offset_location_and_area(shape, point, direction)?.location)
}

pub(in crate::geometry) fn cartesian_row_exact_point_location_and_area(
    row: storage::ShapeRow<'_>,
    point: &[ExactDyadic; 3],
) -> Result<SourcePointLocation> {
    if let Some(point) = exact_stored_point(point) {
        return Ok(cartesian_row_point_location_and_area(row, point));
    }
    let mut location = StreamingLocation::for_exact_point(point)?;
    let mut owners = None;
    location.visit_row(row, &mut owners)?;
    location.finish()
}

pub(in crate::geometry) fn cartesian_row_exact_offset_location_and_area(
    row: storage::ShapeRow<'_>,
    point: &[ExactDyadic; 3],
    direction: &[ExactDyadic; 2],
) -> Result<SourcePointLocation> {
    let mut location = StreamingLocation::for_exact_offset(point, direction)?;
    let mut owners = None;
    location.visit_row(row, &mut owners)?;
    location.finish()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::geometry) enum OccupiedToward {
    Yes,
    No,
    Tangent,
}

pub(in crate::geometry) const fn occupied_toward(location: SourcePointLocation) -> OccupiedToward {
    match (location.location, location.area) {
        (Loc::Interior, true) => OccupiedToward::Yes,
        (Loc::Exterior, _) => OccupiedToward::No,
        _ => OccupiedToward::Tangent,
    }
}
