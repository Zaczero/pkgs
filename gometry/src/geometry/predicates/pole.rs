use crate::geographic::{
    GeographicFrame, GeographicRing, RingSummary, RingTurn, ScalarGeographicView, SeamIdentity,
};
use crate::geometry::{Polygon, Ring, Shape};

fn ring_summary_in_frame(ring: &Ring, frame: GeographicFrame) -> Option<RingSummary> {
    RingSummary::try_new(frame, &GeographicRing::from_ring(ring)).ok()
}

fn ring_has_full_turn_in_frame(ring: &Ring, frame: GeographicFrame) -> bool {
    ring_summary_in_frame(ring, frame)
        .is_some_and(|summary| matches!(summary.turn, RingTurn::Full(_)))
}

/// Whether a ring encircles the north (``north == true``) or south pole via
/// full-longitude winding at high latitude.
pub(crate) fn ring_encloses_pole(ring: &Ring, north: bool) -> bool {
    ring_encloses_pole_in_frame(ring, GeographicFrame::crs84(), north)
}

fn ring_encloses_pole_in_frame(ring: &Ring, frame: GeographicFrame, north: bool) -> bool {
    let Some(summary) = ring_summary_in_frame(ring, frame) else {
        return false;
    };
    if !matches!(summary.turn, RingTurn::Full(_)) {
        return false;
    }
    if north {
        summary.south.get() > 0.0
    } else {
        summary.north.get() < 0.0
    }
}

fn polygon_encloses_pole_in_frame(polygon: &Polygon, frame: GeographicFrame, north: bool) -> bool {
    if !ring_encloses_pole_in_frame(&polygon.shell, frame, north) {
        return false;
    }
    !polygon
        .holes
        .iter()
        .any(|hole| ring_encloses_pole_in_frame(hole, frame, north))
}

pub(crate) fn shape_encloses_pole_in_frame(
    shape: &Shape,
    frame: GeographicFrame,
    north: bool,
) -> bool {
    match shape {
        Shape::Polygon(polygon) => polygon_encloses_pole_in_frame(polygon, frame, north),
        Shape::MultiPolygon(polygons) => polygons
            .iter()
            .any(|polygon| polygon_encloses_pole_in_frame(polygon, frame, north)),
        Shape::GeometryCollection(parts) => parts
            .iter()
            .any(|part| shape_encloses_pole_in_frame(part, frame, north)),
        _ => false,
    }
}

pub(crate) fn shape_has_polar_ring_in_frame(shape: &Shape, frame: GeographicFrame) -> bool {
    let polygon_has_polar_ring = |polygon: &Polygon| {
        ring_has_full_turn_in_frame(&polygon.shell, frame)
            || polygon
                .holes
                .iter()
                .any(|hole| ring_has_full_turn_in_frame(hole, frame))
    };
    match shape {
        Shape::Polygon(polygon) => polygon_has_polar_ring(polygon),
        Shape::MultiPolygon(polygons) => polygons.iter().any(polygon_has_polar_ring),
        Shape::GeometryCollection(parts) => parts
            .iter()
            .any(|part| shape_has_polar_ring_in_frame(part, frame)),
        _ => false,
    }
}

/// Frame-aware pole detection for geographic callers whose native latitude
/// unit is not degrees. Invalid coordinates fail open so a bounds shortcut
/// cannot hide the admission error owned by the exact predicate kernel.
pub(crate) fn shape_reaches_geographic_pole_in_frame(
    shape: &Shape,
    frame: GeographicFrame,
) -> bool {
    let Ok(view) = ScalarGeographicView::try_new(frame, shape) else {
        return true;
    };
    let mut reaches = false;
    if view
        .try_for_each_coordinate(|coordinate| {
            reaches |= coordinate.identity(frame) == SeamIdentity::Pole;
            Ok(())
        })
        .is_err()
    {
        true
    } else {
        reaches
    }
}
