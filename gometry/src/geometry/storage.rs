//! Native geometry rows, packed coordinate storage, selectors, and validity.

pub(crate) mod admission;
pub(crate) mod builder;
pub(crate) mod columns;
pub(crate) mod gather;
pub(crate) mod kernels;
mod ops;
pub(crate) mod packed_columns;
pub(crate) mod replacement;
pub(crate) mod row;
mod rows;
mod types;
pub(crate) mod validity;

pub(crate) use columns::{
    column_window, packed_centroid_xy, packed_surface_point, row_bounds, row_bounds_values,
    row_ord_extremes,
};
pub(crate) use ops::reverse_coord_windows;
pub(crate) use row::ShapeRow;
pub(crate) use rows::{ArrayRows, LogicalRow, LogicalRows, StorageLayout};
pub(crate) use types::{
    GeometryArrayStorage, LineRows, PackedRow, PackedStorage, PointRows, RowSelection,
    RowSelectionRef, RowsIter, ShapesIter, admit_packed_coord_count, admit_packed_total,
    admitted_csr_window, contiguous_physical_range, csr_value_window, line_logical_len,
    materialized_packed_lines_len, materialized_packed_polygons_len, packed_line_row,
    packed_lines_coord_len, packed_polygon_bounds, packed_polygon_vertex_window,
    packed_polygon_view, packed_polygons_coord_len, packed_polygons_ring_len, physical_row,
    point_logical_len, polygon_logical_len, row_map_is_identity, row_selection_from_logical_rows,
};
#[cfg(test)]
pub(crate) use types::{reset_shape_iter_materializations, shape_iter_materializations};
pub(crate) use validity::{ValidityBuilder, ValidityMask, ValidityRank};
