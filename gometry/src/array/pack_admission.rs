//! Shared structural pack-admission for untrusted polygon ingress.
//!
//! Every path that may place polygons into trusted packed storage
//! (`uniform_polygon_column`, streaming bulk `from_wkb`, packed GeoArrow
//! import) must route through these predicates. Malformed rings fall back
//! to Mixed (or clean reject) — never packed storage.

use crate::geometry::{CoordSeq, CoordinateAxes, Polygon, Ring, same_active_position};

/// Minimum stored vertices for a closed ring eligible for packed polygon storage.
/// Alias of [`Ring::MIN_VERTICES_CLOSED`] so every pack site shares one name.
pub(crate) const MIN_PACK_RING_VERTICES: usize = Ring::MIN_VERTICES_CLOSED;

/// Whether a closed-ring vertex count meets the pack floor (≥4).
pub(crate) const fn packable_closed_ring_len(len: usize) -> bool {
    len >= MIN_PACK_RING_VERTICES
}

/// Whether a coordinate sequence is a packable closed ring: ≥4 vertices and
/// first == last on every active ordinate (X/Y/Z/M), matching GeoJSON RFC
/// ring-closure admission.
pub(crate) fn ring_seq_is_packable(seq: &CoordSeq) -> bool {
    if !packable_closed_ring_len(seq.len()) {
        return false;
    }
    let first = seq.point_at(0);
    let last = seq.point_at(seq.len() - 1);
    same_active_position(first, last)
}

/// Pack-admission axes for one polygon: nonempty ring set (shell present),
/// every ring packable, uniform axes across rings. `None` means Mixed /
/// do-not-pack.
pub(crate) fn polygon_pack_axes(polygon: &Polygon) -> Option<CoordinateAxes> {
    let mut axes: Option<CoordinateAxes> = None;
    let mut n_rings = 0_usize;
    for seq in polygon.rings() {
        if !ring_seq_is_packable(seq) {
            return None;
        }
        let row_axes = seq.axes();
        match axes {
            None => axes = Some(row_axes),
            Some(existing) if existing != row_axes => return None,
            Some(_) => {},
        }
        n_rings += 1;
    }
    if n_rings == 0 {
        return None;
    }
    axes
}
