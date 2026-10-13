use pyo3::{PyResult, Python};

use super::{CoordSeq, CoordinateColumnRef, CoordinateView, Point, checked_vector_len};
use crate::geometry::{CoordinateAxes, storage};

/// Visible ordinates used by layout-aware equality.
#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "the closed XY/Z/M layout has at most four slots, and n advances only for present axes"
)]
pub(super) fn visible_coordinate_values(
    point: Point,
    layout: Option<CoordinateAxes>,
) -> ([Option<f64>; 4], usize) {
    let axes = layout.unwrap_or_else(|| CoordinateAxes::from_point(point));
    let mut values = [None; 4];
    values[0] = Some(point.x);
    values[1] = Some(point.y);
    let mut n = 2;
    if axes.has_z() {
        values[n] = point.z();
        n += 1;
    }
    if axes.has_m() {
        values[n] = point.m();
        n += 1;
    }
    (values, n)
}

/// O(1) flat-index lookup into packed line CSR under identity or a contiguous
/// row window, whose coordinates are one contiguous span. Gather falls
/// through (non-contiguous coord spans).
#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "identity and window rows are admitted by their CSR column; the flat index is checked against its resolved span"
)]
pub(super) fn packed_line_point_at(
    coords: &CoordSeq,
    offsets: &[i32],
    map: storage::RowSelectionRef<'_>,
    index: usize,
) -> Option<Point> {
    if offsets.len() < 2 {
        return None;
    }
    let span = match map {
        storage::RowSelectionRef::Identity => 0..offsets[offsets.len() - 1] as usize,
        crate::geometry::storage::RowSelectionRef::Window { start, len } => {
            offsets[start] as usize..offsets[start + len] as usize
        },
        storage::RowSelectionRef::Gather(_) => return None,
    };
    (index < span.len()).then(|| coords.point_at(span.start + index))
}

/// O(1) flat-index lookup into packed polygon CSR under identity or a
/// contiguous row window, whose coordinates are one contiguous span.
#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "identity and window rows are admitted by nested CSR columns; the flat index is checked against its resolved span"
)]
pub(super) fn packed_polygon_point_at(
    coords: &CoordSeq,
    rings: &[i32],
    polys: &[i32],
    map: storage::RowSelectionRef<'_>,
    index: usize,
) -> Option<Point> {
    if rings.len() < 2 || polys.len() < 2 {
        return None;
    }
    let span = match map {
        storage::RowSelectionRef::Identity => 0..rings[rings.len() - 1] as usize,
        crate::geometry::storage::RowSelectionRef::Window { start, len } => {
            rings[polys[start] as usize] as usize..rings[polys[start + len] as usize] as usize
        },
        storage::RowSelectionRef::Gather(_) => return None,
    };
    (index < span.len()).then(|| coords.point_at(span.start + index))
}

impl CoordinateView {
    pub(super) fn gather_optional(
        &self,
        py: Python<'_>,
        project: impl Fn(Point) -> Option<f64>,
    ) -> PyResult<CoordinateColumnRef<'_>> {
        // One pass into a dense f64 lane + 1-byte presence mask (≈9 B/coord).
        // Homogeneous present → Dense (no Option). Fully absent → Missing.
        // Mixed → Nullable once at the end so list paths keep None for absent
        // ordinates while still preserving Some(NaN) as a real measured value
        // (a NaN-only lane would collapse the two). Peak is the dense+mask
        // working set, not a 16 B Option lane overlapping a dense prefix.
        let count = self.len_for_python(py)?;
        let capacity = checked_vector_len::<f64>(count, "coordinate column")?;
        checked_vector_len::<bool>(count, "coordinate presence mask")?;
        checked_vector_len::<Option<f64>>(count, "nullable coordinate column")?;
        let mut values = Vec::with_capacity(capacity);
        let mut present = Vec::with_capacity(capacity);
        let mut any = false;
        let mut all_present = true;
        self.for_each_point(|coord| {
            let (value, is_present) = project(coord).map_or_else(
                || {
                    all_present = false;
                    (0.0, false)
                },
                |value| {
                    any = true;
                    (value, true)
                },
            );
            values.push(value);
            present.push(is_present);
        });
        if !any {
            return Ok(CoordinateColumnRef::Missing);
        }
        if all_present {
            return Ok(CoordinateColumnRef::Dense(values));
        }
        let nullable = values
            .into_iter()
            .zip(present)
            .map(|(value, is_present)| is_present.then_some(value))
            .collect();
        Ok(CoordinateColumnRef::Nullable(nullable))
    }
}
