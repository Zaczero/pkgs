#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! CSR-native packed-line similarity metrics — shared column views, identity
//! shortcuts, and batch execution for Hausdorff and Fréchet.
//!
//! # Architecture
//!
//! ```text
//! PyO3 array method (broadcast/metrics.rs)
//!   └─ PackedLineColumnView (logical row → CSR window via row_map)
//!        └─ hausdorff_distance_line_columns_batch / frechet_distance_line_columns_batch
//!             ├─ identity shortcut (same coord slice → 0 or ∞)
//!             └─ per-pair column kernel (`distance.rs`)
//! ```
//!
//! Kernels stay in [`crate::geometry::distance`]; this module owns **pairing
//! and batch plumbing only** so Hausdorff/Fréchet never duplicate `row_map`
//! resolution or identity semantics.

use crate::array::RowSelectionRef;
use crate::geometry::CoordSeq;

/// Borrowed packed `Lines` CSR columns for batch similarity kernels.
#[derive(Clone, Copy)]
pub struct PackedLineColumnView<'a> {
    pub coords: &'a CoordSeq,
    pub offsets: &'a [i32],
    pub row_map: RowSelectionRef<'a>,
}

impl PackedLineColumnView<'_> {
    pub const fn logical_len(&self) -> usize {
        self.row_map.len(self.offsets.len().saturating_sub(1))
    }

    pub const fn physical_row(&self, logical: usize) -> usize {
        self.row_map.physical(logical)
    }

    pub fn row_xy(&self, logical: usize) -> (&[f64], &[f64]) {
        // Window endpoints from CSR; slice once so the coordinate reborrows
        // share one bounds-check pair (elision of the dual xs/ys path).
        let std::ops::Range { start, end } = self.row_map.csr_window(self.offsets, logical);
        let xs = self.coords.xs();
        let ys = self.coords.ys();
        // Length-exact y reborrow after x window sets the live width.
        let xs = &xs[start..end];
        let ys = &ys[start..start + xs.len()];
        (xs, ys)
    }
}

/// Identity distance for two CSR windows that share the same coordinate slice.
///
/// `empty_value` is the metric value when both windows are empty (Hausdorff →
/// `∞`; Fréchet batch uses `∞` on the identity fast lane, not `EmptyLinework`).
pub(crate) fn packed_line_same_window_value(
    left_xs: &[f64],
    left_ys: &[f64],
    right_xs: &[f64],
    right_ys: &[f64],
    empty_value: f64,
) -> Option<f64> {
    if std::ptr::eq(left_xs.as_ptr(), right_xs.as_ptr())
        && std::ptr::eq(left_ys.as_ptr(), right_ys.as_ptr())
        && left_xs.len() == right_xs.len()
    {
        return Some(if left_xs.is_empty() { empty_value } else { 0.0 });
    }
    None
}

/// Scale batch metric outputs in-place when the CRS linear axis is not meters.
pub(crate) fn scale_metric_values(values: &mut [f64], to_metre: f64) {
    if to_metre.to_bits() == 1.0_f64.to_bits() {
        return;
    }
    for value in values {
        *value *= to_metre;
    }
}
