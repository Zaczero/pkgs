//! The `GeometryArray` Python method surface (`#[pymethods] impl
//! PyGeometryArray`).
//!
//! Cohesive `#[pymethods]` blocks are split across files beside the packed
//! array storage they operate on. Broadcast dispatch and sibling crate types
//! enter through `use super::*`; methods auto-register with the
//! `GeometryArray` class wherever the impl is compiled.

pub(crate) use methods_indexing::{
    CollectedFancyIndex, NumpyFancyIndex, classify_and_collect_fancy_index, is_bool_scalar,
    numpy_fancy_index,
};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyTuple};

// Child `#[pymethods]` modules glob `use super::*`; re-export imports here so
// they are parent-defined names (ancestor `use` alone is not globbed).
pub(crate) use crate::broadcast::{CollectRows, rows_err};
use crate::error::{Error, Result};
use crate::geometry::CsrOffsetColumn;
pub(crate) use crate::geometry::{
    CsrOffsetBuilder, PolygonLevel, RingLevel, line_is_ccw, line_is_closed, line_is_simple,
    line_is_valid, polygon_is_valid,
};
pub(crate) use crate::py::errors::{CRSError, GeometryError, InvalidGeometryError};
use crate::py::numpy::bool_array;
use crate::{
    Arc, ArrowEncoding, Bounds, CoordSeq, CoordinateAxes, Crs, DefaultedF64Input, DistanceUnit,
    F64Param, Frame, FrameAdoption, FrameEdit, GeometryKind, GeometryTransformFrame, HeapSize,
    I64Param, InterpolatePlan, OriginSpec, OverlayOp, Point, Polygon, PyCoordinates, PyCrs,
    PyGeometry, PyTypeError, PyValidationReport, PyValueError, RepairMethod, Ring,
    SVG_ARRAY_PREVIEW, Shape, SimplifyMethod, SpatialCurve, TransformOptionArgs, Typed,
    VoronoiClipInput, cdt_refinement_values, coerce_present_geometry_items,
    coordinate_epoch_option, coordinates, crs, crs_arc, crs_label, curves, exact_geometry,
    exact_geometry_array, expected_geometry_or_array, geometry,
    geometry_array_svg_grid_html_masked, io, line_interpolate_points_coordseq,
    line_interpolate_points_shape, line_locate_coordseq, map_coordinates_callback,
    non_negative_int, note_array_row, overlay_operator, owned_voronoi_boundary,
    parse_coordinate_replacement, parse_crs, parse_curve_bounds, parse_interpolate_plan,
    parse_precision, positive_int, pyclass, replace_shape_coordinates, require_geojson_crs,
    resolve_line_metric, resolve_metric, row_sample_seed, slice_replacement_for_shape,
    validate_densify_fraction, validate_equals_exact_tolerance, validate_max_segment_length,
    validate_subdivide_max_vertices,
};

/// An immutable array of geometries sharing one CRS/epoch frame.
///
/// The vectorized counterpart of ``Geometry``: methods mirror the scalar
/// names element-wise (``arr.area``, ``(arr).buffer(10.0)``,
/// ``contains(arr, geom)``) and run as batched Rust kernels with no
/// per-element Python. Homogeneous point, line, and polygon arrays pack into
/// shared coordinate columns (with CSR offsets where needed) for zero-copy
/// Arrow and columnar kernels. Build with ``GeometryArray(...)`` /
/// ``points(...)`` or the WKT/WKB/GeoJSON/Arrow importers; index and
/// slice it like a sequence.
#[derive(Clone, Debug)]
enum ArrayRows {
    /// Every logical row is present. This is the allocation-free hot path.
    Dense(Arc<GeometryArrayStorage>),
    /// Storage and its row-aligned missingness are one indivisible state.
    Masked {
        storage: Arc<GeometryArrayStorage>,
        missing: ValidityMask,
    },
    /// Homogeneous nullable rows whose packed payload contains present rows
    /// only. Lines and polygons retain logical outer offsets; point rank comes
    /// from validity.
    CompactNullable {
        storage: Arc<GeometryArrayStorage>,
        missing: ValidityMask,
    },
}

/// Physical layout of packed point rows relative to a validity mask.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PointRowLayout {
    Dense,
    RowAligned,
    Compact,
}

/// Physical layout presented to a nullable-array constructor.
///
/// The layout is an admission fact supplied by the producer, not something a
/// constructor derives from a payload length (which is ambiguous when a mask
/// happens to have the same number of present rows as logical rows).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StorageLayout {
    /// The payload has one physical row/window for every logical mask row.
    LogicalRows,
    /// The payload contains only the rows selected by present mask bits.
    PresentOnly,
}

impl PointRowLayout {
    pub(crate) const fn is_compact(self) -> bool {
        matches!(self, Self::Compact)
    }
}

#[derive(Clone, Copy)]
pub(crate) enum LogicalRow<'a> {
    Missing { logical: usize },
    Present { logical: usize, row: ShapeRow<'a> },
}

enum LogicalRowsState<'a> {
    Dense(RowsIter<'a>),
    RowAligned {
        rows: RowsIter<'a>,
        validity: missing::ValidityCursor<'a>,
    },
    CompactPoints {
        rows: RowsIter<'a>,
        validity: missing::ValidityCursor<'a>,
    },
}

pub(crate) struct LogicalRows<'a> {
    state: LogicalRowsState<'a>,
    logical: usize,
    remaining: usize,
}

impl<'a> Iterator for LogicalRows<'a> {
    type Item = LogicalRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let logical = self.logical;
        self.logical = self.logical.wrapping_add(1);
        self.remaining = self.remaining.wrapping_sub(1);
        Some(match &mut self.state {
            LogicalRowsState::Dense(rows) => LogicalRow::Present {
                logical,
                row: rows.next().expect("dense storage length is exact"),
            },
            LogicalRowsState::RowAligned { rows, validity } => {
                let (_, present) = validity.next().expect("validity length is exact");
                let row = rows.next().expect("row-aligned storage length is exact");
                present.map_or(LogicalRow::Missing { logical }, |_| LogicalRow::Present {
                    logical,
                    row,
                })
            },
            LogicalRowsState::CompactPoints { rows, validity } => {
                let (_, present) = validity.next().expect("validity length is exact");
                present.map_or(LogicalRow::Missing { logical }, |_| LogicalRow::Present {
                    logical,
                    row: rows.next().expect("present point payload count is exact"),
                })
            },
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for LogicalRows<'_> {}

impl ArrayRows {
    fn new(storage: Arc<GeometryArrayStorage>, missing: Option<ValidityMask>) -> Self {
        match missing {
            Some(missing) if missing.any() => {
                assert_eq!(
                    storage.len(),
                    missing.len(),
                    "missing mask length must match array length"
                );
                Self::Masked { storage, missing }
            },
            Some(missing) => {
                assert_eq!(
                    storage.len(),
                    missing.len(),
                    "missing mask length must match array length"
                );
                Self::Dense(storage)
            },
            None => Self::Dense(storage),
        }
    }

    const fn storage(&self) -> &Arc<GeometryArrayStorage> {
        match self {
            Self::Dense(storage)
            | Self::Masked { storage, .. }
            | Self::CompactNullable { storage, .. } => storage,
        }
    }

    const fn missing(&self) -> Option<&ValidityMask> {
        match self {
            Self::Dense(_) => None,
            Self::Masked { missing, .. } | Self::CompactNullable { missing, .. } => Some(missing),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Dense(storage) | Self::Masked { storage, .. } => storage.len(),
            Self::CompactNullable { missing, .. } => missing.len(),
        }
    }

    const fn is_compact_nullable(&self) -> bool {
        matches!(self, Self::CompactNullable { .. })
    }

    const fn point_row_layout(&self) -> PointRowLayout {
        match self {
            Self::Dense(_) => PointRowLayout::Dense,
            Self::Masked { .. } => PointRowLayout::RowAligned,
            Self::CompactNullable { .. } => PointRowLayout::Compact,
        }
    }

    fn logical_row(&self, logical: usize) -> Option<LogicalRow<'_>> {
        if logical >= self.len() {
            return None;
        }
        Some(match self {
            Self::Dense(storage) => LogicalRow::Present {
                logical,
                row: storage.row(logical),
            },
            Self::Masked { storage, missing } => {
                if missing.is_missing(logical) {
                    LogicalRow::Missing { logical }
                } else {
                    LogicalRow::Present {
                        logical,
                        row: storage.row(logical),
                    }
                }
            },
            Self::CompactNullable { storage, missing } => {
                if missing.is_missing(logical) {
                    LogicalRow::Missing { logical }
                } else {
                    let payload = if matches!(
                        storage.as_ref(),
                        GeometryArrayStorage::Packed(PackedStorage::Points { .. })
                    ) {
                        missing.present_before(logical)
                    } else {
                        logical
                    };
                    LogicalRow::Present {
                        logical,
                        row: storage.row(payload),
                    }
                }
            },
        })
    }

    fn logical_rows(&self) -> LogicalRows<'_> {
        let remaining = self.len();
        let state = match self {
            Self::Dense(storage) => LogicalRowsState::Dense(storage.iter_rows()),
            Self::CompactNullable { storage, missing }
                if matches!(
                    storage.as_ref(),
                    GeometryArrayStorage::Packed(PackedStorage::Points { .. })
                ) =>
            {
                LogicalRowsState::CompactPoints {
                    rows: storage.iter_rows(),
                    validity: missing.cursor(),
                }
            },
            Self::Masked { storage, missing } | Self::CompactNullable { storage, missing } => {
                LogicalRowsState::RowAligned {
                    rows: storage.iter_rows(),
                    validity: missing.cursor(),
                }
            },
        };
        LogicalRows {
            state,
            logical: 0,
            remaining,
        }
    }
}

#[pyclass(
    name = "GeometryArray",
    module = "gometry",
    frozen,
    immutable_type,
    sequence,
    generic,
    weakref,
    skip_from_py_object
)]
#[derive(Clone, Debug)]
/// An immutable, vectorized geometry column with one shared CRS/epoch frame.
///
/// Homogeneous arrays use packed coordinate storage and batched Rust kernels;
/// indexing and slicing preserve zero-copy views where possible.
pub struct PyGeometryArray {
    /// The single source of truth for row storage and row-aligned missingness.
    /// Masked slots contain rectangular packed placeholders, but callers can
    /// never replace storage and its mask independently.
    rows: ArrayRows,
    /// The coordinate reference frame shared by every row (CRS + optional
    /// epoch). One `Frame` makes epoch-without-CRS unrepresentable (#37); the
    /// `crs`/`epoch` Python getters and the `crs_ref`/`crs_str` internal
    /// accessors derive from it.
    pub(crate) frame: Frame,
    /// Lazy per-element bounding boxes, shared across clones — built once from
    /// the packed columns when explicitly required and reused by every
    /// predicate refutation. The array is frozen, so the cache cannot become
    /// stale. `Arc<OnceLock>` makes cloning the handle share the cache.
    pub(crate) bounds_cache: Arc<std::sync::OnceLock<ElementBounds>>,
    /// Lazy array-wide aggregate bounds (including the geographic antimeridian
    /// fold when the frame is geographic). Same lifecycle as `bounds_cache`:
    /// fresh on `from_storage_arc` / frame retag (geographic fold depends on
    /// frame); shared only where geometry-derived bounds are proven unchanged
    /// and the frame is the same.
    pub(crate) total_bounds_cache: TotalBoundsCache,
    /// Lazy products keyed by stable physical source row. The owner allocates
    /// fixed-size chunks only when a row first needs a product; the selection
    /// maps this array's logical rows back to that source.
    pub(crate) row_products: prepared::RowProducts,
}

/// Cached per-element bounding boxes (one entry per row, `None` for empty
/// rows). The vector is kept behind an `Arc` so required construction can
/// publish one fallibly allocated owner and cache-only callers can reuse it.
#[expect(
    clippy::redundant_pub_crate,
    reason = "crate-visible for the predicate broadcast dispatch"
)]
pub(crate) type ElementBounds = Arc<Vec<Option<Bounds>>>;

/// Lazy array-wide aggregate bounds cache handle (`None` when every row is empty).
type TotalBoundsCache = Arc<std::sync::OnceLock<Option<(f64, f64, f64, f64)>>>;
mod linref_kernels;
mod masked;
mod methods_buffer;
mod methods_convert;
mod methods_core;
mod methods_distance;
mod methods_indexing;
mod methods_interop;
mod methods_measures;
mod methods_predicates;
mod methods_topology;
pub(crate) mod missing;
pub(crate) use missing::{ValidityBuilder, ValidityMask, ValidityRank};

mod methods_unary_io;
mod ops;
pub(crate) use linref_kernels::{line_interpolate_points_rows, line_locate_point_on_lines};
mod iter;
pub(crate) use iter::PyGeometryArrayIter;
mod prepared;
pub(crate) use prepared::{
    ArrayRow, ArrayRowOperation, BoundsSeed, PreparedRow, RowProducts, packed_row_persistent,
};
mod storage_helpers;
mod storage_types;
pub(crate) use packed_column_builder::{
    PackedColumnBuilder, PointColumnBuilder, concat_packed_line_columns,
};
pub(crate) use storage_helpers::{
    column_window, packed_centroid_xy, packed_surface_point, row_bounds, row_bounds_values,
    row_ord_extremes,
};
pub(crate) use storage_impl::reverse_coord_windows;
mod coordinate_replacement;
pub(crate) use coordinate_replacement::{CoordinateReplacement, ReplacementAxis};
mod packed;
mod packed_column_kernels;
mod packed_columns;
mod packed_gather;
pub(crate) use packed_column_kernels::{
    bounds_values_from_columns_masked, buffer_point_columns, clip_lines_packed_xy,
    geodesic_line_measure, geodesic_polygon_measure, geographic_bounds_values_from_columns,
    map_masked_point_columns_to_crs, packed_per_row_self_origin_affine_columns,
    reduce_lines_or_polygons, segmented_planar_lengths, segmented_planar_lengths_3d,
    subdivide_line_columns, subdivide_polygon_columns, total_bounds_from_columns,
};
pub(crate) use packed_columns::{PackedColumnError, PackedColumnOutput, PackedColumns};
pub(crate) use packed_gather::materialized_gather;
mod pack_admission;
mod packed_builders;
mod packed_column_builder;
mod packed_equal;
mod packed_ops;
mod streaming;
mod uniform_columns;
pub(crate) use pack_admission::{CheckedRingWindow, RingWindowError, polygon_pack_axes};
pub(crate) use packed::ComputedCaches;
pub(crate) use streaming::StreamingShapes;
pub(crate) mod shape_row;
mod storage_impl;
pub(crate) use packed_equal::pair_packed_equals_exact;
mod tests;

pub(crate) use shape_row::ShapeRow;
pub(crate) use storage_types::{
    GeometryArrayStorage, LineRows, PackedRow, PackedStorage, PointRows, RowSelection,
    RowSelectionRef, RowsIter, ShapesIter, admit_packed_coord_count, admit_packed_total,
    admitted_csr_window, contiguous_physical_range, csr_value_window, line_logical_len,
    materialized_packed_lines_len, materialized_packed_polygons_len, packed_line_row,
    packed_lines_coord_len, packed_polygon_bounds, packed_polygon_vertex_window,
    packed_polygon_view, packed_polygons_coord_len, packed_polygons_ring_len, physical_row,
    point_logical_len, polygon_logical_len, row_map_is_identity, row_selection_from_logical_rows,
};
#[cfg(test)]
pub(crate) use storage_types::{reset_shape_iter_materializations, shape_iter_materializations};
