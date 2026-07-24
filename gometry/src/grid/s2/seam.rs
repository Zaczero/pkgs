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

use super::cell::LatLngRect;
use crate::geometry::{Shape, ShapePart};

/// The per-part lon/lat windows of the source geometry. Parts are planar
/// `[-180, 180]` boxes (gometry validates the lon/lat domain; seam-crossing
/// sources arrive split into parts hugging each side).
pub(crate) struct SourceWindows {
    parts: Vec<(f64, f64, f64, f64)>, // (lon_lo, lat_lo, lon_hi, lat_hi)
    /// True when some part (or the collective cover) spans all longitudes
    /// — a genuine full-longitude polar cap / world-wide band. Polar cell
    /// rects expand to full-lng via pole closure; fail-open Boundary for
    /// those is only sound when the source itself is full-longitude.
    full_longitude: bool,
}

impl SourceWindows {
    pub(crate) fn new(source: &Shape) -> Self {
        let mut parts = Vec::new();
        // Borrowed part visitor (no coordinate cloning); the always-false
        // predicate makes `any_part` a plain for-each.
        source.any_part(|part: ShapePart<'_>| {
            if let Some(bounds) = part.bounds() {
                parts.push((bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy()));
            }
            false
        });
        let full_longitude = parts_span_full_longitude(&parts);
        Self {
            parts,
            full_longitude,
        }
    }

    /// Whether the source spans all longitudes (full polar cap / world band).
    #[inline]
    pub(crate) const fn is_full_longitude(&self) -> bool {
        self.full_longitude
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

    /// Lon-only overlap of `lng_windows` against source parts (lat already ok).
    /// Used to classify polar-expanded full-lng cell rects against a
    /// partial-longitude source via the cell's true vertex lon span.
    ///
    /// Lon intervals are closed on the circle where ±180 is one meridian: a
    /// cell that only spells the boundary as −180 still meets a source that
    /// only spells it as +180 (and vice versa). True opposite wedges with no
    /// shared interior or shared seam stay Outside.
    pub(crate) fn may_overlap_lng(
        &self,
        first: (f64, f64),
        second: Option<(f64, f64)>,
        lat_lo: f64,
        lat_hi: f64,
    ) -> bool {
        self.parts
            .iter()
            .any(|&(lon_lo, lat_lo_s, lon_hi, lat_hi_s)| {
                if lat_hi < lat_lo_s || lat_lo > lat_hi_s {
                    return false;
                }
                lng_windows_overlap_part(first, second, lon_lo, lon_hi)
            })
    }
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
#[inline]
fn lng_interval_overlap(a_lo: f64, a_hi: f64, b_lo: f64, b_hi: f64) -> bool {
    // Standard closed interval overlap on the unwrapped line.
    if a_hi >= b_lo && a_lo <= b_hi {
        return true;
    }
    // Shared antimeridian: both intervals touch ±180 → they meet at that point.
    touches_antimeridian(a_lo, a_hi) && touches_antimeridian(b_lo, b_hi)
}

/// Whether a closed lon interval includes the antimeridian (±180).
#[inline]
fn touches_antimeridian(lo: f64, hi: f64) -> bool {
    // Endpoints exactly at the seam, or a full/near-full band that covers it.
    lo <= -180.0 + 1e-12 || hi >= 180.0 - 1e-12 || lo >= 180.0 - 1e-12 || hi <= -180.0 + 1e-12
}

/// A part spans full longitude when its lon interval is the closed world
/// band `[-180, 180]` (canonical box spelling of a full-longitude cap).
fn parts_span_full_longitude(parts: &[(f64, f64, f64, f64)]) -> bool {
    const FULL_SPAN: f64 = 360.0 - 1e-9;
    parts.iter().any(|&(lon_lo, _, lon_hi, _)| {
        // Non-wrapped full band.
        (lon_lo <= -180.0 + 1e-9 && lon_hi >= 180.0 - 1e-9)
            // Or a part whose width is essentially 360°.
            || (lon_hi - lon_lo) >= FULL_SPAN
    })
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
}
