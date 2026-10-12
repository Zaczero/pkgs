use std::ops::ControlFlow;
use std::sync::Arc;

use pyo3::{PyResult, Python};

use super::point_access::visible_coordinate_values;
use super::{
    CoordSeq, CoordinateAxis, CoordinateColumnRef, CoordinateOwner, CoordinateView, Shape,
    ShapeData, checked_vector_len,
};
use crate::geometry::storage::{self, GeometryArrayStorage, PackedStorage, RowSelectionRef};
use crate::geometry::{CoordWindow, CoordinateAxes};

fn owners_identical(left: &CoordinateOwner, right: &CoordinateOwner) -> bool {
    match (left, right) {
        (CoordinateOwner::Shape(a), CoordinateOwner::Shape(b)) => Arc::ptr_eq(a, b),
        (CoordinateOwner::Array(a), CoordinateOwner::Array(b)) => {
            Arc::ptr_eq(a.storage_arc(), b.storage_arc()) && a.missing() == b.missing()
        },
        _ => false,
    }
}

/// Numeric column equality, matching Python tuple equality for visible ordinates.
#[expect(
    clippy::float_cmp,
    reason = "Coordinates equality follows Python's exact numeric equality semantics"
)]
fn f64_columns_equal(left: &[f64], right: &[f64]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter().zip(right.iter()).all(|(a, b)| a == b)
}

fn coordseq_columns_equal(left: &CoordSeq, right: &CoordSeq) -> bool {
    left.axes() == right.axes()
        && f64_columns_equal(left.xs(), right.xs())
        && f64_columns_equal(left.ys(), right.ys())
        && match (left.zs(), right.zs()) {
            (None, None) => true,
            (Some(a), Some(b)) => f64_columns_equal(a, b),
            _ => false,
        }
        && match (left.ms(), right.ms()) {
            (None, None) => true,
            (Some(a), Some(b)) => f64_columns_equal(a, b),
            _ => false,
        }
}

#[expect(
    clippy::indexing_slicing,
    reason = "the packed span is checked against the shared CoordSeq column length before it is borrowed"
)]
pub(super) fn borrowed_axis(
    coords: &CoordSeq,
    span: std::ops::Range<usize>,
    axis: CoordinateAxis,
) -> CoordinateColumnRef<'_> {
    match axis {
        CoordinateAxis::X => CoordinateColumnRef::Borrowed(&coords.xs()[span]),
        CoordinateAxis::Y => CoordinateColumnRef::Borrowed(&coords.ys()[span]),
        CoordinateAxis::Z => coords.zs().map_or(CoordinateColumnRef::Missing, |zs| {
            CoordinateColumnRef::Borrowed(&zs[span])
        }),
        CoordinateAxis::M => coords.ms().map_or(CoordinateColumnRef::Missing, |ms| {
            CoordinateColumnRef::Borrowed(&ms[span])
        }),
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "the checked end bounds both endpoints of this admitted CSR row window"
)]
fn line_window_coord_span(
    offsets: &[i32],
    coords_len: usize,
    start: usize,
    len: usize,
) -> Option<std::ops::Range<usize>> {
    let end = start.checked_add(len)?;
    if offsets.len() <= end {
        return None;
    }
    let start = offsets[start] as usize;
    let end = offsets[end] as usize;
    (end <= coords_len && start <= end).then_some(start..end)
}

#[expect(
    clippy::indexing_slicing,
    reason = "the checked polygon and ring endpoints bound the admitted nested CSR window"
)]
fn polygon_window_coord_span(
    ring_offsets: &[i32],
    polygon_offsets: &[i32],
    coords_len: usize,
    start: usize,
    len: usize,
) -> Option<std::ops::Range<usize>> {
    let end = start.checked_add(len)?;
    if polygon_offsets.len() <= end {
        return None;
    }
    let ring_start = polygon_offsets[start] as usize;
    let ring_end = polygon_offsets[end] as usize;
    if ring_offsets.len() <= ring_end || ring_start > ring_end {
        return None;
    }
    let start = ring_offsets[ring_start] as usize;
    let end = ring_offsets[ring_end] as usize;
    (end <= coords_len && start <= end).then_some(start..end)
}

/// Visible contiguous coordinate span for unmasked identity or windowed packed
/// storage. Gathered maps are not one span.
pub(super) fn packed_contiguous_coord_span(
    packed: &PackedStorage,
) -> Option<(&CoordSeq, std::ops::Range<usize>)> {
    match packed {
        PackedStorage::Points { coords, row_map } => match row_map.as_deref() {
            RowSelectionRef::Identity => Some((coords.as_ref(), 0..coords.len())),
            RowSelectionRef::Window { start, len } => {
                let end = start.checked_add(len)?;
                (end <= coords.len()).then_some((coords.as_ref(), start..end))
            },
            RowSelectionRef::Gather(_) => None,
        },
        PackedStorage::Lines {
            coords,
            offsets,
            row_map,
        } => match row_map.as_deref() {
            RowSelectionRef::Identity => Some((coords.as_ref(), 0..coords.len())),
            RowSelectionRef::Window { start, len } => {
                let span = line_window_coord_span(offsets.as_slice(), coords.len(), start, len)?;
                Some((coords.as_ref(), span))
            },
            RowSelectionRef::Gather(_) => None,
        },
        PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        } => match row_map.as_deref() {
            RowSelectionRef::Identity => Some((coords.as_ref(), 0..coords.len())),
            RowSelectionRef::Window { start, len } => {
                let span = polygon_window_coord_span(
                    ring_offsets.as_slice(),
                    polygon_offsets.as_slice(),
                    coords.len(),
                    start,
                    len,
                )?;
                Some((coords.as_ref(), span))
            },
            RowSelectionRef::Gather(_) => None,
        },
    }
}

fn coord_views_numeric_eq(
    left: &CoordSeq,
    left_span: std::ops::Range<usize>,
    right: &CoordSeq,
    right_span: std::ops::Range<usize>,
) -> bool {
    let left = left.view(CoordWindow::from_range(left_span, left.len()));
    let right = right.view(CoordWindow::from_range(right_span, right.len()));
    coordseq_columns_equal(&left, &right)
}

fn row_selection_eq(left: &storage::RowSelection, right: &storage::RowSelection) -> bool {
    match (left.as_deref(), right.as_deref()) {
        (storage::RowSelectionRef::Identity, storage::RowSelectionRef::Identity) => true,
        (
            crate::geometry::storage::RowSelectionRef::Window { start: sa, len: la },
            crate::geometry::storage::RowSelectionRef::Window { start: sb, len: lb },
        ) => sa == sb && la == lb,
        (storage::RowSelectionRef::Gather(a), storage::RowSelectionRef::Gather(b)) => a == b,
        _ => false,
    }
}

/// When both owners are packed arrays with comparable structure, compare
/// columns (and CSR maps) without Point materialization. `None` means fall
/// back to the dual stream (mixed / masked / cross-kind).
fn try_columnar_storage_equal(left: &CoordinateOwner, right: &CoordinateOwner) -> Option<bool> {
    match (left, right) {
        (CoordinateOwner::Shape(a), CoordinateOwner::Shape(b)) => shape_columnar_equal(a, b),
        (CoordinateOwner::Array(a), CoordinateOwner::Array(b))
            if a.missing().is_none() && b.missing().is_none() =>
        {
            packed_columnar_equal(a.storage(), b.storage())
        },
        _ => None,
    }
}

fn shape_columnar_equal(left: &ShapeData, right: &ShapeData) -> Option<bool> {
    match (left.shape(), right.shape()) {
        (Shape::LineString(la), Shape::LineString(lb)) => {
            Some(coordseq_columns_equal(la.as_coords(), lb.as_coords()))
        },
        (Shape::MultiPoint(la), Shape::MultiPoint(lb)) => Some(coordseq_columns_equal(la, lb)),
        (Shape::Point(pa), Shape::Point(pb)) => Some(pa == pb),
        (Shape::Empty(ka, aa), Shape::Empty(kb, ab)) => Some(ka == kb && aa == ab),
        // Multipart / polygon: fall back to dual stream.
        _ => None,
    }
}

fn packed_columnar_equal(
    left: &GeometryArrayStorage,
    right: &GeometryArrayStorage,
) -> Option<bool> {
    match (left, right) {
        (
            GeometryArrayStorage::Packed(PackedStorage::Points {
                coords: ca,
                row_map: ma,
            }),
            GeometryArrayStorage::Packed(PackedStorage::Points {
                coords: cb,
                row_map: mb,
            }),
        ) => packed_points_columnar_eq(ca, ma, cb, mb),
        (
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords: ca,
                offsets: oa,
                row_map: ma,
            }),
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords: cb,
                offsets: ob,
                row_map: mb,
            }),
        ) => packed_lines_columnar_eq(ca, oa.as_slice(), ma, cb, ob.as_slice(), mb),
        (
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords: ca,
                ring_offsets: ra,
                polygon_offsets: pa,
                row_map: ma,
            }),
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords: cb,
                ring_offsets: rb,
                polygon_offsets: pb,
                row_map: mb,
            }),
        ) => packed_polygons_columnar_eq(
            ca,
            ra.as_slice(),
            pa.as_slice(),
            ma,
            cb,
            rb.as_slice(),
            pb.as_slice(),
            mb,
        ),
        // Mixed or cross-kind: stream.
        _ => None,
    }
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "admitted row windows make start plus length representable before constructing their coordinate spans"
)]
fn packed_points_columnar_eq(
    ca: &CoordSeq,
    ma: &storage::RowSelection,
    cb: &CoordSeq,
    mb: &storage::RowSelection,
) -> Option<bool> {
    // Cross row-map pairs (identity vs window/gather, gather vs rebuild) can
    // still be value-equal — never answer False solely from map shape.
    if !row_selection_eq(ma, mb) {
        return None;
    }
    match ma.as_deref() {
        storage::RowSelectionRef::Identity => Some(coordseq_columns_equal(ca, cb)),
        crate::geometry::storage::RowSelectionRef::Window { start, len } => {
            let la = ca.view(CoordWindow::from_range(start..start + len, ca.len()));
            let lb = cb.view(CoordWindow::from_range(start..start + len, cb.len()));
            Some(coordseq_columns_equal(&la, &lb))
        },
        // Gathered points: physical rows may differ — stream is the safe path.
        storage::RowSelectionRef::Gather(_) => None,
    }
}

/// Flattened `Coordinates` equality cares about the visible vertex stream only.
/// CSR row/ring breaks do not change flatten order when both sides walk the
/// same packed column in row order — never require absolute offsets to match.
fn packed_lines_columnar_eq(
    ca: &CoordSeq,
    oa: &[i32],
    ma: &storage::RowSelection,
    cb: &CoordSeq,
    ob: &[i32],
    mb: &storage::RowSelection,
) -> Option<bool> {
    if !row_selection_eq(ma, mb) {
        return None;
    }
    match ma.as_deref() {
        // Identity: flattened stream IS the coord columns (offsets only
        // partition rows; different breaks with the same verts stay equal).
        storage::RowSelectionRef::Identity => Some(coordseq_columns_equal(ca, cb)),
        crate::geometry::storage::RowSelectionRef::Window { start, len } => {
            // Per-side absolute spans (prefixes may differ); compare only the
            // visible coordinate slices, never the absolute offset numbers.
            let Some(left) = line_window_coord_span(oa, ca.len(), start, len) else {
                return Some(false);
            };
            let Some(right) = line_window_coord_span(ob, cb.len(), start, len) else {
                return Some(false);
            };
            Some(coord_views_numeric_eq(ca, left, cb, right))
        },
        storage::RowSelectionRef::Gather(_) => None,
    }
}

fn packed_polygons_columnar_eq(
    ca: &CoordSeq,
    ra: &[i32],
    pa: &[i32],
    ma: &storage::RowSelection,
    cb: &CoordSeq,
    rb: &[i32],
    pb: &[i32],
    mb: &storage::RowSelection,
) -> Option<bool> {
    if !row_selection_eq(ma, mb) {
        return None;
    }
    match ma.as_deref() {
        // Same as lines: flatten order is the packed coord column; CSR only
        // partitions rings/polygons.
        storage::RowSelectionRef::Identity => Some(coordseq_columns_equal(ca, cb)),
        crate::geometry::storage::RowSelectionRef::Window { start, len } => {
            // Visible polygons start..end → ring range via polygon_offsets →
            // coord span via ring_offsets. Different absolute prefixes are fine.
            let Some(left) = polygon_window_coord_span(ra, pa, ca.len(), start, len) else {
                return Some(false);
            };
            let Some(right) = polygon_window_coord_span(rb, pb, cb.len(), start, len) else {
                return Some(false);
            };
            Some(coord_views_numeric_eq(ca, left, cb, right))
        },
        storage::RowSelectionRef::Gather(_) => None,
    }
}

impl CoordinateView {
    /// Visible-value equality of two flattened views. Uses owner-identity and
    /// packed columnar bulk paths for full equal scans, and a dual sequential
    /// stream for the general / early-mismatch case. Never indexes the right
    /// side with per-coordinate CSR `point_at` (that is O(n log rows)).
    pub(crate) fn try_equal_visible(
        &self,
        py: Python<'_>,
        other: &Self,
        self_layout: Option<CoordinateAxes>,
        other_layout: Option<CoordinateAxes>,
    ) -> PyResult<bool> {
        // Same owner Arc (+ missing mask): identical flattened content.
        if owners_identical(&self.owner, &other.owner) && self_layout == other_layout {
            return Ok(true);
        }
        let left = self.exact_measure(py)?;
        let right = other.exact_measure(py)?;
        if !left.has_same_count(py, &right)? {
            return Ok(false);
        }
        // Same packed structure, no layout override: compare coordinate
        // columns run-wise without materializing Points.
        if self_layout.is_none()
            && other_layout.is_none()
            && let Some(equal) = try_columnar_storage_equal(&self.owner, &other.owner)
        {
            return Ok(equal);
        }
        // Dual sequential stream: walk left, materialize right once into a
        // visible-value buffer. Full equal is O(n); first mismatch early-exits
        // the left walk (right is already buffered — still O(n) prep, never
        // O(n log rows)).
        let count = right.len_for_python()?;
        let capacity =
            checked_vector_len::<([Option<f64>; 4], usize)>(count, "coordinate equality")?;
        let mut right_visible = Vec::with_capacity(capacity);
        other.for_each_point(|coord| {
            right_visible.push(visible_coordinate_values(coord, other_layout));
        });
        let mut idx = 0_usize;
        let equal = self.try_for_each_point(&mut |coord| {
            let left = visible_coordinate_values(coord, self_layout);
            if right_visible.get(idx).copied() != Some(left) {
                return ControlFlow::Break(false);
            }
            idx = idx.wrapping_add(1);
            ControlFlow::Continue(())
        });
        Ok(!matches!(equal, ControlFlow::Break(false)))
    }

    #[cfg(test)]
    pub fn equal_visible(
        &self,
        other: &Self,
        self_layout: Option<CoordinateAxes>,
        other_layout: Option<CoordinateAxes>,
    ) -> bool {
        Self::attach_test_python(|py| {
            self.try_equal_visible(py, other, self_layout, other_layout)
                .expect("test coordinate equality fits native storage")
        })
    }
}
