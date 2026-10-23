mod affine;
mod bounds;
mod construct;
#[cfg(test)]
mod tests;
mod triangles;
use std::sync::Arc;

use crate::array::{
    ArrayRows, Bounds, CoordSeq, CoordinateAxes, Crs, CsrOffsetBuilder, CsrOffsetColumn,
    ElementBounds, Frame, LogicalRow, LogicalRows, OriginSpec, Point, PolygonLevel, Py, PyAny,
    PyGeometry, PyGeometryArray, PyResult, Python, RingLevel, Shape, StorageLayout,
    TotalBoundsCache, prepared,
};
use crate::geometry::publication::FinishedGeometry;
use crate::geometry::storage::kernels::packed_per_row_self_origin_affine_columns;
use crate::geometry::storage::packed_columns::PackedColumns;
use crate::geometry::storage::{
    GeometryArrayStorage, PackedStorage, RowSelection, ValidityMask, row_map_is_identity,
    row_ord_extremes,
};
#[cfg(test)]
use crate::geometry::types::ShapeHeapMemo;
use crate::geometry::{CoordSeqBuilder, storage};
use crate::py::numpy::float64_array;

/// Geometry-derived cache owners supplied to the computed-data constructor.
/// Prepared/frame products deliberately do not live here: every computed
/// result receives a fresh product owner.
pub(crate) struct ComputedCaches {
    bounds_cache: Arc<std::sync::OnceLock<ElementBounds>>,
    total_bounds_cache: TotalBoundsCache,
}

impl ComputedCaches {
    pub(crate) fn fresh() -> Self {
        Self {
            bounds_cache: Arc::new(std::sync::OnceLock::new()),
            total_bounds_cache: Arc::new(std::sync::OnceLock::new()),
        }
    }

    pub(crate) const fn carried(
        bounds_cache: Arc<std::sync::OnceLock<ElementBounds>>,
        total_bounds_cache: TotalBoundsCache,
    ) -> Self {
        Self {
            bounds_cache,
            total_bounds_cache,
        }
    }
}

impl PyGeometryArray {
    pub(crate) const fn row_owner(&self) -> &ArrayRows {
        &self.rows
    }

    pub(crate) fn storage(&self) -> &GeometryArrayStorage {
        self.rows.storage()
    }

    pub(crate) const fn storage_arc(&self) -> &Arc<GeometryArrayStorage> {
        self.rows.storage_arc()
    }

    pub(crate) const fn missing(&self) -> Option<&ValidityMask> {
        self.rows.missing()
    }

    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn logical_row(&self, logical: usize) -> Option<LogicalRow<'_>> {
        self.rows.logical_row(logical)
    }

    pub(crate) fn logical_rows(&self) -> LogicalRows<'_> {
        self.rows.logical_rows()
    }

    pub(crate) fn packed_columns(&self) -> Option<PackedColumns<'_>> {
        self.rows.packed_columns()
    }

    pub(crate) fn is_compact_nullable(&self) -> bool {
        self.rows.is_compact_nullable()
    }

    /// Whether a scalar geometry shares this array's frame (CRS + epoch) —
    /// the precondition for element-level `__eq__` comparisons.
    pub(crate) fn frame_matches(&self, geom: &PyGeometry) -> bool {
        self.crs_ref() == geom.crs_ref() && self.epoch() == geom.epoch()
    }

    /// Whether any present row carries a Z ordinate.
    pub(crate) fn has_z(&self) -> bool {
        self.any_present_axes(CoordinateAxes::has_z)
    }

    /// Whether any present row carries an M ordinate.
    pub(crate) fn has_m(&self) -> bool {
        self.any_present_axes(CoordinateAxes::has_m)
    }

    /// Whether any present row's ordinate layout satisfies `carries`. Packed
    /// storage shares one coordinate column, so its answer is a single axes
    /// read.
    fn any_present_axes(&self, carries: fn(CoordinateAxes) -> bool) -> bool {
        if self
            .missing()
            .as_ref()
            .is_some_and(|mask| mask.present_count() == 0)
        {
            return false;
        }
        match self.storage() {
            GeometryArrayStorage::Packed(
                PackedStorage::Points { coords, .. }
                | PackedStorage::Lines { coords, .. }
                | PackedStorage::Polygons { coords, .. },
            ) => carries(coords.axes()),
            GeometryArrayStorage::Mixed(shapes) => shapes
                .iter()
                .enumerate()
                .any(|(row, shape)| !self.is_row_missing(row) && carries(shape.axes())),
        }
    }

    /// The single ordinate layout shared by every PRESENT row, or `None` when
    /// present rows disagree. Packed storage owns one layout even through row
    /// selections and missing placeholders, so its answer is O(1). Mixed
    /// missing rows are skipped (their XY placeholder must not vote). Packed
    /// all-missing arrays retain their declared layout; heterogeneous
    /// all-missing arrays are `XY`.
    pub fn uniform_axes(&self) -> Option<CoordinateAxes> {
        let GeometryArrayStorage::Mixed(shapes) = self.storage() else {
            return self.storage().packed_axes();
        };
        if self.missing().is_some_and(|mask| mask.present_count() == 0) {
            return Some(CoordinateAxes::XY);
        }
        let mut axes = None;
        for (row, shape) in shapes.iter().enumerate() {
            if self.is_row_missing(row) {
                continue;
            }
            let row_axes = shape.axes();
            match axes {
                None => axes = Some(row_axes),
                Some(seen) if seen != row_axes => return None,
                Some(_) => {},
            }
        }
        Some(axes.unwrap_or(CoordinateAxes::XY))
    }

    // --- Frame accessors (#37 migration; see PyGeometry::crs_ref) -----------
    /// The array's CRS as a borrow — internal counterpart to the `crs` getter
    /// (which clones into `PyCrs`). Replaces `self.crs_ref()`.
    pub(crate) const fn crs_ref(&self) -> Option<&Crs> {
        self.frame.crs_ref()
    }

    /// The array's CRS code as `&str` — replaces `self.crs_str()`.
    pub(crate) fn crs_str(&self) -> Option<&str> {
        self.frame.crs_str()
    }

    /// Whether any row is missing (`None` mask = dense fast path).
    pub(crate) const fn has_missing(&self) -> bool {
        self.missing().is_some()
    }

    pub(crate) fn is_row_missing(&self, row: usize) -> bool {
        self.missing()
            .as_ref()
            .is_some_and(|mask| mask.is_missing(row))
    }
}

#[cfg(test)]
mod packed_simplify_keep_tests {
    use super::*;

    #[test]
    fn packed_dp_keep_all_row_and_collinear_drop() {
        let coords = CoordSeq::try_from_vecs(
            vec![0.0, 1.0, 2.0, 0.0, 1.0, 2.0],
            vec![0.0, 1.0, 0.0, 0.0, 0.0, 0.0],
            None,
            None,
        )
        .unwrap();
        let offsets = CsrOffsetColumn::try_new(vec![0, 3, 6], 6).unwrap();
        let array = PyGeometryArray::packed_lines(coords, offsets, Frame::None).unwrap();
        let GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords, offsets, ..
        }) = array.storage()
        else {
            panic!("packed lines");
        };
        let out = array
            .simplify_dp_packed_lines(coords, offsets, 1e-9)
            .unwrap();
        let GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords: out_coords,
            offsets: out_offsets,
            ..
        }) = out.storage()
        else {
            panic!("packed lines remain packed");
        };
        assert_eq!(out_offsets.as_slice(), &[0, 3, 5]);
        assert_eq!(out_coords.xs(), &[0.0, 1.0, 2.0, 0.0, 2.0]);
        assert_eq!(out_coords.ys(), &[0.0, 1.0, 0.0, 0.0, 0.0]);
    }

    #[test]
    fn packed_vw_zero_keeps_every_vertex_including_zm_bits() {
        let nan = f64::NAN;
        let coords = CoordSeq::try_from_vecs(
            vec![-0.0, 1.0, 2.0],
            vec![0.0, -0.0, 0.0],
            Some(vec![nan, 1.0, nan]),
            Some(vec![0.0, nan, -0.0]),
        )
        .unwrap();
        let offsets = CsrOffsetColumn::try_new(vec![0, 3], 3).unwrap();
        let array = PyGeometryArray::packed_lines(coords, offsets, Frame::None).unwrap();
        let GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords, offsets, ..
        }) = array.storage()
        else {
            panic!("packed lines");
        };
        let out = array
            .simplify_vw_packed_lines(coords, offsets, 0.0)
            .unwrap();
        let GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords: out_coords, ..
        }) = out.storage()
        else {
            panic!("packed lines remain packed");
        };
        assert_eq!(out_coords.xs()[0].to_bits(), (-0.0_f64).to_bits());
        assert_eq!(out_coords.zs().unwrap()[0].to_bits(), nan.to_bits());
        assert_eq!(out_coords.ms().unwrap()[2].to_bits(), (-0.0_f64).to_bits());
        assert_eq!(out_coords.len(), 3);
    }

    #[test]
    fn packed_dp_zero_keeps_l_bend_and_drops_collinear() {
        let coords = CoordSeq::try_from_vecs(
            vec![0.0, 1.0, 2.0, 0.0, 1.0, 2.0],
            vec![0.0, 1.0, 0.0, 0.0, 0.0, 0.0],
            None,
            None,
        )
        .unwrap();
        let offsets = CsrOffsetColumn::try_new(vec![0, 3, 6], 6).unwrap();
        let array = PyGeometryArray::packed_lines(coords, offsets, Frame::None).unwrap();
        let GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords, offsets, ..
        }) = array.storage()
        else {
            panic!("packed lines");
        };
        let out = array
            .simplify_dp_packed_lines(coords, offsets, 0.0)
            .unwrap();
        let GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords: out_coords,
            offsets: out_offsets,
            ..
        }) = out.storage()
        else {
            panic!("packed lines remain packed");
        };
        assert_eq!(out_offsets.as_slice(), &[0, 3, 5]);
        assert_eq!(out_coords.xs(), &[0.0, 1.0, 2.0, 0.0, 2.0]);
        assert_eq!(out_coords.ys(), &[0.0, 1.0, 0.0, 0.0, 0.0]);
    }
}
