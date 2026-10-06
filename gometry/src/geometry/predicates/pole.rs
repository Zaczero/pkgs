use std::collections::HashMap;

use crate::geographic::{
    ChartPole, GeographicFrame, GeographicRing, RingSummary, RingTurn, ScalarGeographicView,
    SeamIdentity,
};
use crate::geometry::{Polygon, Ring, Shape, ShapeOwnerKey, VisibleXYKey};

fn ring_summary_in_frame(ring: &Ring, frame: GeographicFrame) -> Option<RingSummary> {
    RingSummary::try_new(frame, &GeographicRing::from_ring(ring)).ok()
}

fn summary_encloses_pole(summary: &RingSummary, pole: ChartPole) -> bool {
    summary.bounded_pole() == Some(pole)
}

#[derive(Default)]
struct PoleMemo {
    rings: HashMap<VisibleXYKey, Option<RingSummary>>,
    north_enclosure: HashMap<ShapeOwnerKey, bool>,
    south_enclosure: HashMap<ShapeOwnerKey, bool>,
    polar_ring: HashMap<ShapeOwnerKey, bool>,
}

impl PoleMemo {
    fn ring_summary(&mut self, ring: &Ring, frame: GeographicFrame) -> Option<RingSummary> {
        if let Some(key) = ring.coords().shared_xy_key() {
            if let Some(&summary) = self.rings.get(&key) {
                return summary;
            }
            let summary = ring_summary_in_frame(ring, frame);
            self.rings.insert(key, summary);
            summary
        } else {
            ring_summary_in_frame(ring, frame)
        }
    }

    fn ring_encloses_pole(&mut self, ring: &Ring, frame: GeographicFrame, pole: ChartPole) -> bool {
        let Some(summary) = self.ring_summary(ring, frame) else {
            return false;
        };
        summary_encloses_pole(&summary, pole)
    }

    fn polygon_encloses_pole(
        &mut self,
        polygon: &Polygon,
        frame: GeographicFrame,
        pole: ChartPole,
    ) -> bool {
        self.ring_encloses_pole(&polygon.shell, frame, pole)
            && !polygon
                .holes
                .iter()
                .any(|hole| self.ring_encloses_pole(hole, frame, pole))
    }

    fn shape_encloses_pole(
        &mut self,
        shape: &Shape,
        frame: GeographicFrame,
        pole: ChartPole,
    ) -> bool {
        let north = pole == ChartPole::North;
        if let Shape::GeometryCollection(parts) = shape {
            let owner = parts
                .members_are_shared()
                .then(|| ShapeOwnerKey::new(parts.as_slice()));
            let cached = owner.as_ref().and_then(|owner| {
                if north {
                    self.north_enclosure.get(owner)
                } else {
                    self.south_enclosure.get(owner)
                }
            });
            if let Some(&enclosed) = cached {
                return enclosed;
            }
            let enclosed = parts
                .iter()
                .any(|part| self.shape_encloses_pole(part, frame, pole));
            if let Some(owner) = owner {
                if north {
                    self.north_enclosure.insert(owner, enclosed);
                } else {
                    self.south_enclosure.insert(owner, enclosed);
                }
            }
            return enclosed;
        }
        match shape {
            Shape::Polygon(polygon) => self.polygon_encloses_pole(polygon, frame, pole),
            Shape::MultiPolygon(polygons) => polygons
                .iter()
                .any(|polygon| self.polygon_encloses_pole(polygon, frame, pole)),
            Shape::GeometryCollection(_) => unreachable!("handled above"),
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::Empty(..) => false,
        }
    }

    fn ring_has_full_turn(&mut self, ring: &Ring, frame: GeographicFrame) -> bool {
        self.ring_summary(ring, frame)
            .is_some_and(|summary| matches!(summary.turn(), RingTurn::Full(_)))
    }

    fn shape_has_polar_ring(&mut self, shape: &Shape, frame: GeographicFrame) -> bool {
        if let Shape::GeometryCollection(parts) = shape {
            let owner = parts
                .members_are_shared()
                .then(|| ShapeOwnerKey::new(parts.as_slice()));
            if let Some(&has_polar_ring) =
                owner.as_ref().and_then(|owner| self.polar_ring.get(owner))
            {
                return has_polar_ring;
            }
            let has_polar_ring = parts
                .iter()
                .any(|part| self.shape_has_polar_ring(part, frame));
            if let Some(owner) = owner {
                self.polar_ring.insert(owner, has_polar_ring);
            }
            return has_polar_ring;
        }
        match shape {
            Shape::Polygon(polygon) => {
                self.ring_has_full_turn(&polygon.shell, frame)
                    || polygon
                        .holes
                        .iter()
                        .any(|hole| self.ring_has_full_turn(hole, frame))
            },
            Shape::MultiPolygon(polygons) => polygons.iter().any(|polygon| {
                self.ring_has_full_turn(&polygon.shell, frame)
                    || polygon
                        .holes
                        .iter()
                        .any(|hole| self.ring_has_full_turn(hole, frame))
            }),
            Shape::GeometryCollection(_) => unreachable!("handled above"),
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::Empty(..) => false,
        }
    }
}

/// Whether a ring encloses the selected native-chart pole through full-longitude winding.
pub(crate) fn ring_encloses_pole(ring: &Ring, pole: ChartPole) -> bool {
    ring_encloses_pole_in_frame(ring, GeographicFrame::crs84(), pole)
}

#[cfg(test)]
pub(crate) fn ring_bounded_pole(ring: &Ring, frame: GeographicFrame) -> Option<ChartPole> {
    ring_summary_in_frame(ring, frame).and_then(|summary| summary.bounded_pole())
}

fn ring_encloses_pole_in_frame(ring: &Ring, frame: GeographicFrame, pole: ChartPole) -> bool {
    PoleMemo::default().ring_encloses_pole(ring, frame, pole)
}

pub(crate) fn shape_encloses_pole_in_frame(
    shape: &Shape,
    frame: GeographicFrame,
    pole: ChartPole,
) -> bool {
    PoleMemo::default().shape_encloses_pole(shape, frame, pole)
}

pub(crate) fn shape_has_polar_ring_in_frame(shape: &Shape, frame: GeographicFrame) -> bool {
    PoleMemo::default().shape_has_polar_ring(shape, frame)
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
    view.any_first_xy_coordinate(|coordinate| coordinate.identity(frame) == SeamIdentity::Pole)
}
