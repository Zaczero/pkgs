#![cfg_attr(
    test,
    allow(
        clippy::similar_names,
        reason = "S2 seam tests compare paired latitude and longitude bounds"
    )
)]
//! The coverer's bounds gate: per-part source windows tested against each
//! cell's exact [`LatLngRect`] bound.
//!
//! Soundness comes from the rect itself — `Cell::rect_bound` provably
//! contains the lon/lat of every point of the true spherical cell — so a
//! disjoint verdict here is a certificate, antimeridian and poles included
//! (wrapped rects test as two intervals; polar rects span all longitudes).

use crate::geometry::{Shape, ShapePart};
use crate::grid::s2::cell::LatLngRect;

/// The per-part lon/lat windows of the source geometry. Parts are planar
/// `[-180, 180]` boxes (gometry validates the lon/lat domain; seam-crossing
/// sources arrive split into parts hugging each side).
pub(crate) struct SourceWindows {
    parts: Vec<(f64, f64, f64, f64)>, // (lon_lo, lat_lo, lon_hi, lat_hi)
}

impl SourceWindows {
    pub(crate) fn new(source: &Shape) -> Self {
        let mut parts = Vec::new();
        collect_windows(source, &mut parts);
        Self { parts }
    }

    /// Whether any part's window may overlap the cell's exact bound.
    /// `false` certifies the cell disjoint from the source.
    pub(crate) fn may_overlap(&self, rect: LatLngRect) -> bool {
        let (first, second) = rect.lng_windows();
        self.parts.iter().any(|&(lon_lo, lat_lo, lon_hi, lat_hi)| {
            if rect.lat_hi < lat_lo || rect.lat_lo > lat_hi {
                return false;
            }
            lng_windows_overlap_part(first, second, lon_lo, lon_hi)
        })
    }
}

/// Retain each primitive normalized part as its own window.  Antimeridian
/// splitting can leave a `Multi*` inside a `GeometryCollection`; treating
/// that nested collection's aggregate as one window recreates a full-world
/// seam band and defeats the sound bounds gate.
fn collect_windows(source: &Shape, windows: &mut Vec<(f64, f64, f64, f64)>) {
    // Borrowed traversal avoids cloning the normalized coordinate storage.
    // Returning false makes `any_part` a plain for-each.
    source.any_part(|part: ShapePart<'_>| match part {
        ShapePart::Nested(shape) => {
            collect_windows(shape, windows);
            false
        },
        primitive => {
            if let Some(bounds) = primitive.bounds() {
                windows.push((bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy()));
            }
            false
        },
    });
}

/// Closed lon-interval overlap, antimeridian-aware (±180 is one point).
fn lng_windows_overlap_part(
    first: (f64, f64),
    second: Option<(f64, f64)>,
    lon_lo: f64,
    lon_hi: f64,
) -> bool {
    let overlaps = |window: (f64, f64)| lng_interval_overlap(window.0, window.1, lon_lo, lon_hi);
    overlaps(first) || second.is_some_and(overlaps)
}

/// Two closed lon intervals on `[-180, 180]` overlap, counting the shared
/// ±180 meridian as contact (same geographic point, two spellings).
fn lng_interval_overlap(a_lo: f64, a_hi: f64, b_lo: f64, b_hi: f64) -> bool {
    // Standard closed interval overlap on the unwrapped line.
    if a_hi >= b_lo && a_lo <= b_hi {
        return true;
    }
    // Shared antimeridian: both intervals touch ±180 → they meet at that point.
    touches_antimeridian(a_lo, a_hi) && touches_antimeridian(b_lo, b_hi)
}

/// Whether a closed lon interval includes the antimeridian (±180).
fn touches_antimeridian(lo: f64, hi: f64) -> bool {
    // Endpoints exactly at the seam, or a full/near-full band that covers it.
    lo <= -180.0 + 1e-12 || hi >= 180.0 - 1e-12 || lo >= 180.0 - 1e-12 || hi <= -180.0 + 1e-12
}

#[cfg(test)]
mod tests {
    use super::super::cell::Cell;
    use super::super::cellid::CellId;
    use super::*;
    use crate::geometry::{Point, Polygon, Ring};

    fn rect(lon: f64, lat: f64, level: u8) -> LatLngRect {
        let cell = Cell::from_id(
            CellId::from_lonlat(lon, lat)
                .parent(level)
                .expect("coarser"),
        );
        cell.rect_bound(&cell.vertices_lonlat())
    }

    fn box_shape(west: f64, south: f64, east: f64, north: f64) -> Shape {
        let shell = vec![
            Point::new_unchecked_xy(west, south),
            Point::new_unchecked_xy(east, south),
            Point::new_unchecked_xy(east, north),
            Point::new_unchecked_xy(west, north),
            Point::new_unchecked_xy(west, south),
        ];
        Shape::Polygon(Polygon::new(Ring::from_trusted_closed(shell), Vec::new()))
    }

    /// The gate rejects far-away cells and keeps overlapping ones, including
    /// across the seam and at the poles.
    #[test]
    fn overlap_gate_is_sound_and_useful() {
        let windows = SourceWindows::new(&box_shape(13.0, 52.0, 14.0, 53.0));
        assert!(windows.may_overlap(rect(13.4, 52.5, 10)));
        assert!(!windows.may_overlap(rect(-100.0, 52.5, 10)));
        assert!(!windows.may_overlap(rect(13.4, -20.0, 10)));

        // A seam-adjacent source meets wrapped cell rects from both sides.
        let seam = SourceWindows::new(&box_shape(179.5, -1.0, 180.0, 1.0));
        assert!(seam.may_overlap(rect(179.9, 0.0, 10)));
        assert!(seam.may_overlap(rect(180.0, 0.5, 12)));
        assert!(!seam.may_overlap(rect(170.0, 0.0, 10)));

        // Polar cells (full-longitude rects) pass the gate for any source
        // in their latitude band, and never for distant bands.
        let arctic = SourceWindows::new(&box_shape(-10.0, 88.0, 10.0, 89.0));
        let pole_rect = rect(0.0, 89.9999, 3);
        assert!(pole_rect.is_full_lng());
        assert!(arctic.may_overlap(pole_rect));
        assert!(!SourceWindows::new(&box_shape(-10.0, -5.0, 10.0, 5.0)).may_overlap(pole_rect));

        // ±180 is one meridian: east-spelling and west-spelling touch.
        assert!(lng_interval_overlap(170.0, 180.0, -180.0, -90.0));
        assert!(lng_interval_overlap(-180.0, -170.0, 90.0, 180.0));
        // True opposite (no shared seam) stay disjoint.
        assert!(!lng_interval_overlap(0.0, 10.0, -180.0, -90.0));
        assert!(!lng_interval_overlap(170.0, 175.0, -170.0, -160.0));
    }

    #[test]
    fn normalized_seam_components_do_not_reunite_into_a_world_window() {
        let raw = box_shape(179.0, -1.0, -179.0, 1.0);
        let split = raw
            .split_antimeridian()
            .expect("the exact seam rectangle splits into normalized components");
        // Antimeridian normalization can place the split MultiPolygon below
        // a collection member; windows must retain its primitive parts rather
        // than use that nested member's aggregate full-world bounds.
        let nested = Shape::GeometryCollection(vec![Shape::GeometryCollection(vec![split])]);
        let windows = SourceWindows::new(&nested);
        assert!(windows.may_overlap(rect(179.5, 0.0, 8)));
        assert!(!windows.may_overlap(rect(179.5, 45.0, 8)));
        assert!(!windows.may_overlap(rect(0.0, 0.0, 8)));
    }
}
