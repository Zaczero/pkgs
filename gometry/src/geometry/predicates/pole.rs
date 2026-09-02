use crate::geographic::{
    GeographicFrame, GeographicRing, RingSummary, RingTurn, ScalarGeographicView, SeamIdentity,
};
use crate::geometry::{Polygon, Ring, Shape};

fn ring_summary(ring: &Ring) -> Option<RingSummary> {
    RingSummary::try_new(GeographicFrame::crs84(), &GeographicRing::from_ring(ring)).ok()
}

fn ring_has_full_turn(ring: &Ring) -> bool {
    ring_summary(ring).is_some_and(|summary| matches!(summary.turn, RingTurn::Full(_)))
}

/// Whether a ring encircles the north (``north == true``) or south pole via
/// full-longitude winding at high latitude.
pub(crate) fn ring_encloses_pole(ring: &Ring, north: bool) -> bool {
    let Some(summary) = ring_summary(ring) else {
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

fn polygon_encloses_pole(polygon: &Polygon, north: bool) -> bool {
    if !ring_encloses_pole(&polygon.shell, north) {
        return false;
    }
    !polygon
        .holes
        .iter()
        .any(|hole| ring_encloses_pole(hole, north))
}

/// Whether an areal ``shape`` encloses ``north``/south pole (shell/hole parity).
pub(crate) fn shape_encloses_pole(shape: &Shape, north: bool) -> bool {
    match shape {
        Shape::Polygon(polygon) => polygon_encloses_pole(polygon, north),
        Shape::MultiPolygon(polygons) => polygons
            .iter()
            .any(|polygon| polygon_encloses_pole(polygon, north)),
        Shape::GeometryCollection(parts) => {
            parts.iter().any(|part| shape_encloses_pole(part, north))
        },
        _ => false,
    }
}

/// Whether any areal ring winds around a pole, including a hole that removes
/// the pole from the final surface.  This differs deliberately from
/// [`shape_encloses_pole`]: a polar annulus contains neither pole, yet still
/// spans every longitude between its shell and polar hole and therefore needs
/// a full-longitude conservative envelope.
pub(crate) fn shape_has_polar_ring(shape: &Shape) -> bool {
    let polygon_has_polar_ring = |polygon: &Polygon| {
        ring_has_full_turn(&polygon.shell) || polygon.holes.iter().any(ring_has_full_turn)
    };
    match shape {
        Shape::Polygon(polygon) => polygon_has_polar_ring(polygon),
        Shape::MultiPolygon(polygons) => polygons.iter().any(polygon_has_polar_ring),
        Shape::GeometryCollection(parts) => parts.iter().any(shape_has_polar_ring),
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
