#[cfg(test)]
use std::cell::Cell;
mod iter;
mod rows;
mod selection;
mod sizes;
pub(crate) use iter::{RowsIter, ShapesIter};
#[cfg(test)]
pub(crate) use iter::{
    csr_size_row_visits, record_csr_size_row_visit, record_shape_iter_materialization,
    reset_csr_size_row_visits, reset_shape_iter_materializations, shape_iter_materializations,
};
pub(crate) use rows::{
    LineRows, PackedRow, PointRows, packed_line_row, packed_polygon_bounds,
    packed_polygon_vertex_window, packed_polygon_view,
};
pub(crate) use selection::{
    RowSelection, RowSelectionRef, admitted_csr_window, contiguous_physical_range, csr_value_range,
    csr_value_window, line_logical_len, physical_row, point_logical_len, polygon_logical_len,
    row_map_is_identity, row_selection_from_logical_rows,
};
pub(crate) use sizes::{
    admit_packed_coord_count, admit_packed_total, materialized_packed_lines_len,
    materialized_packed_polygons_len, packed_lines_coord_len, packed_polygons_coord_len,
    packed_polygons_ring_len,
};
#[cfg(test)]
mod tests;
use std::sync::Arc;

use super::{ShapeRow, column_window, row_bounds};
use crate::HeapSize;
use crate::geometry::{
    Bounds, CoordSeq, CoordWindow, CsrOffsetColumn, GeometryErrorKind, GeometryKind, LineSeq,
    Point, Polygon, PolygonLevel, RingLevel, Shape, SharedSlice,
};

#[derive(Clone, Debug)]
pub(crate) enum GeometryArrayStorage {
    /// Heterogeneous (or non-packable) rows owned as plain shapes.
    ///
    /// The array frame, missing mask, prepared-row cache, and frame-cache
    /// sidecars live on `PyGeometryArray` — not per-row scalar wrappers.
    /// Scalar extract (`arr[i]`) and prepared kernels materialize
    /// [`crate::geometry::PreparedPlan`] once into the array-side prepared
    /// cache.
    Mixed(Vec<Shape>),
    /// Homogeneous rows in one columnar layout. Packed-only code takes the
    /// inner type directly, so it can never be handed `Mixed`.
    Packed(PackedStorage),
}

/// The three columnar layouts. A value of this type is packed by construction:
/// every operation that only makes sense on columns takes it, and the `Mixed`
/// arms those operations used to bail on are gone with the case.
#[derive(Clone, Debug)]
pub(crate) enum PackedStorage {
    Points {
        coords: Arc<CoordSeq>,
        row_map: RowSelection,
    },
    Lines {
        coords: Arc<CoordSeq>,
        offsets: CsrOffsetColumn,
        row_map: RowSelection,
    },
    Polygons {
        coords: Arc<CoordSeq>,
        ring_offsets: CsrOffsetColumn<RingLevel>,
        polygon_offsets: CsrOffsetColumn<PolygonLevel>,
        row_map: RowSelection,
    },
}

// SAFETY: `ShapesIter` always yields exactly `size_hint().0` items — never
// fewer. The lower/upper bounds come from `PhysicalRowsIter`, which tracks a
// fixed logical length set once at construction and advances a monotoic
// `index` by exactly one on every successful `next`:
//
// - Mixed: `len = items.len()`; yields `items[index]` while `index < len`.
//   Missing array rows are still present as placeholder `Shape` entries in
//   the mixed column — they count toward len and are yielded (the array
//   missing-mask is orthogonal and not consulted here).
// - Points / Lines / Polygons: `len = {point,line,polygon}_logical_len(...)`
//   which is `RowSelectionRef::len(physical_len)`:
//     Identity → physical_len
//     Window { len } → len
//     Gather(map) → map.len()
//   Each `next` returns one present `PackedRow` through the shared selector/CSR
//   resolver while `index < len`.
//
// There is no filtering, short-circuit, or fallible yield path that can
// return `None` before exhaustion. A bad gather index is a construction-
// time/`debug_assert` defect, not a shorter iterator. Do not spread this
// impl to other iterators without an independent exactness proof.
unsafe impl std::iter::TrustedLen for ShapesIter<'_> {}
