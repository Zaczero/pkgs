//! Shared segment types and kernels for projection, distance, contact,
//! clipping, interpolation, and ordering.

use crate::geometry::tessellation::exact::ExactDyadic;
use crate::geometry::{Segment, XY};

pub(in crate::geometry) mod clip;
pub(in crate::geometry) mod contact;
pub(in crate::geometry) mod distance;
mod exact;
pub(in crate::geometry) mod interpolation;
pub(in crate::geometry) mod ordering;
pub(crate) mod projection;
#[cfg(test)]
#[path = "segments/tests/support.rs"]
mod test_support;

pub(crate) use exact::orientation;
pub(super) use exact::{CCW_ERRBOUND_A, orientation_xy, ray_crossing_is_right};
pub(in crate::geometry) use exact::{
    DdInterval, crossing_homogeneous, crossing_homogeneous_stored, crossing_homogeneous_with_work,
    crossing_point, crossing_point_with, dd_strict_cmp, stored_cross_sign, stored_orientation_sign,
};
use exact::{certified_projection, dd_interpolate};

/// Rounded source expression with an outward error radius. The radius covers
/// the source-coordinate differences, products, and final two-term sum.
#[derive(Clone, Copy)]
struct SourcePair {
    value: f64,
    radius: f64,
    left: f64,
    right: f64,
    subtract: bool,
}

#[derive(Clone, Copy)]
struct FilteredInteriorProjection {
    fraction: f64,
    terms_do_not_cancel: bool,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SourceInteriorProjection {
    point: XY,
    segment: Segment,
    fraction: f64,
}

#[derive(Clone, Copy)]
enum ClassifiedSegmentProjection {
    Start,
    FilteredInterior(FilteredInteriorProjection),
    End,
    Declined,
}

enum ExactClassifiedSegmentProjection {
    Start,
    Interior(ExactProjectionTerms),
    End,
}

#[derive(Clone, Debug)]
pub(crate) struct ExactSegmentProjection {
    numerator: ExactDyadic,
    denominator: ExactDyadic,
    fraction: f64,
    // This is an emitted-witness override only; source calculations use N/D.
    emitted_foot: Option<XY>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CertifiedSegmentProjection {
    point: XY,
    segment: Segment,
    fraction: f64,
    foot: XY,
}

/// The source primitive governing a witness, independent of rounded emission.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum WitnessRole {
    Interior,
    Point,
}

/// One certified clamped projection. The provenance-carrying variants retain
/// their source ratio until coordinate, distance, and along-distance
/// reconstruction; a rounded `t == 0` or `t == 1` therefore cannot turn a
/// genuine interior foot into an endpoint.
#[derive(Clone, Debug)]
pub(crate) enum SegmentProjection {
    Start,
    Interior(SourceInteriorProjection),
    CertifiedInterior(CertifiedSegmentProjection),
    ExactInterior(Box<ExactSegmentProjection>),
    End,
}

struct ExactProjectionTerms {
    numerator: ExactDyadic,
    denominator: ExactDyadic,
    determinant: ExactDyadic,
}

struct ExactSquaredDistance {
    numerator: ExactDyadic,
    denominator: ExactDyadic,
}

enum PointSegmentMetric {
    Squared {
        measurement_squared: f64,
        filter: SquaredDistanceFilter,
        witness_kind: PointSegmentWitnessKind,
        projection: SelectionProjectionSource,
    },
    Projection(SegmentProjection),
}

#[derive(Clone)]
enum SelectionProjectionSource {
    Classified(ClassifiedSegmentProjection),
    Ready(SegmentProjection),
}

pub(crate) struct PointSegmentSelection {
    distance: f64,
    filter: SquaredDistanceFilter,
    witness_kind: PointSegmentWitnessKind,
    projection: SelectionProjectionSource,
}

#[derive(Clone, Copy)]
enum PointSegmentWitnessKind {
    Start,
    Interior,
    End,
}

#[derive(Clone, Copy)]
pub(super) enum ExactDistanceWitness {
    Points(XY, XY),
    Projected(XY, Segment),
    Clamped(XY, Segment),
}

/// Order-faithful key for one nearest-witness candidate.
///
/// Distance measures the source continuum, while emitted nearest/shortest-line
/// points are faithful stored witnesses. A projected key retains the original
/// point and segment, so an emitted foot can never change its ordering.
/// Conservative source-distance intervals decide disjoint candidates cheaply;
/// every overlap goes to the stored-dyadic owner, never to a bare float `<`.
#[derive(Clone, Copy)]
pub(crate) struct SquaredDistanceKey {
    filter: SquaredDistanceFilter,
    witness: ExactDistanceWitness,
}

#[derive(Clone, Copy)]
pub(in crate::geometry) struct SquaredDistanceFilter {
    lo: f64,
    hi: f64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Orientation {
    Collinear,
    Clockwise,
    CounterClockwise,
}
