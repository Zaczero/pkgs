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
    CsrOffsetBuilder, PolygonLevel, RingLevel, affine_about, concat_coord_columns,
    line_crosses_antimeridian, line_is_ccw, line_is_closed, line_is_simple, line_is_valid,
    polygon_is_valid, polygon_row_point_membership,
};
pub(crate) use crate::py::errors::{CRSError, GeometryError, InvalidGeometryError};
use crate::py::numpy::bool_array;
use crate::{
    Arc, ArrowEncoding, Bounds, CoercedCollectedGeometryItems, CoordSeq, CoordinateAxes, Crs,
    DefaultedF64Input, DistanceUnit, EmptyKind, F64Param, Frame, FrameAdoption, FrameEdit,
    GeometryTransformFrame, HeapSize, I64Param, InterpolatePlan, OriginSpec, OverlayOp, Point,
    Polygon, PyBytes, PyCoordinates, PyCrs, PyGeometry, PyTypeError, PyValidationReport,
    PyValueError, RepairMethod, Ring, SVG_ARRAY_PREVIEW, Shape, ShapeData, SimplifyMethod,
    SpatialCurve, Typed, VoronoiClipInput, array_binary_geometry, cdt_refinement_values,
    coerce_collected_geometry_items, coordinate_epoch_option, coordinates, crs, crs_arc, crs_label,
    curves, exact_geometry, exact_geometry_array, expected_geometry_or_array, f64_column_le_bytes,
    fixed_geometry_array_nearest_points, geometry, geometry_array_svg_grid_html_masked, io,
    line_interpolate_points_coordseq, line_interpolate_points_shape, line_locate_coordseq,
    map_coordinates_callback, metric_nearest_points, metric_shortest_line, non_negative_int,
    note_array_row, overlay_operator, owned_voronoi_boundary, packed_points_to_arrow,
    paired_arrays, parse_coordinate_replacement, parse_crs, parse_curve_bounds,
    parse_geometry_transform_options, parse_interpolate_plan, parse_precision,
    parse_wkt_output_dimension, positive_int, pyclass, replace_shape_coordinates,
    require_geojson_crs, resolve_line_metric, resolve_metric, row_sample_seed,
    slice_replacement_for_shape, usize_row_map_le_bytes, validate_densify_fraction,
    validate_equals_exact_tolerance, validate_max_segment_length, validate_subdivide_max_vertices,
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
        missing: MissingMask,
    },
}

impl ArrayRows {
    fn new(storage: Arc<GeometryArrayStorage>, missing: Option<MissingMask>) -> Self {
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
            Self::Dense(storage) | Self::Masked { storage, .. } => storage,
        }
    }

    const fn missing(&self) -> Option<&MissingMask> {
        match self {
            Self::Dense(_) => None,
            Self::Masked { missing, .. } => Some(missing),
        }
    }

    fn with_missing(&self, missing: Option<MissingMask>) -> Self {
        Self::new(Arc::clone(self.storage()), missing)
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
    /// Lazy per-element bounding boxes, shared across clones — built ONCE from
    /// the packed columns and reused by every predicate refutation, mirroring
    /// shapely's per-geometry envelope cache (the array is frozen, never
    /// stale). `Arc<OnceLock>` so cloning the handle shares the cache.
    pub(crate) bounds_cache: Arc<std::sync::OnceLock<Option<ElementBounds>>>,
    /// Lazy array-wide aggregate bounds (including the geographic antimeridian
    /// fold when the frame is geographic). Same lifecycle as `bounds_cache`:
    /// fresh on `from_storage_arc` / frame retag (geographic fold depends on
    /// frame); shared only where geometry-derived bounds are proven unchanged
    /// and the frame is the same.
    pub(crate) total_bounds_cache: TotalBoundsCache,
    /// Lazy per-row prepared handles for packed line/polygon rows — shared
    /// across clones so repeated predicates reuse facet trees and point
    /// testers.
    pub(crate) prepared_cache: prepared::PreparedRowCache,
    /// Frame-owned per-row geodesic/LRS products. Row slots are reference
    /// counted so clones, slices, gathers, and scalar extraction share the
    /// same lazy cache without attaching CRS-derived state to coordinates.
    pub(crate) frame_caches: prepared::FrameCacheRows,
    /// Lazy materialization of a fancy-selected (`RowSelection::Gather`)
    /// storage, shared across clones like `bounds_cache`. Filter-then-compute
    /// workflows (`arr[mask].area` + `.length` + ...) resolve the gather ONCE;
    /// every later packed-column op reuses the contiguous columns zero-copy.
    /// `None`-typed until the first packed op on a gathered array; Identity/
    /// Window storages never populate it (they are already zero-copy views).
    pub(crate) gathered_memo: Arc<std::sync::OnceLock<Arc<GeometryArrayStorage>>>,
}

/// Cached per-element bounding boxes (one entry per row, `None` for empty
/// rows).
#[expect(
    clippy::redundant_pub_crate,
    reason = "crate-visible for the predicate broadcast dispatch"
)]
pub(crate) type ElementBounds = Arc<[Option<Bounds>]>;

/// Lazy array-wide aggregate bounds cache handle (`None` when every row is empty).
type TotalBoundsCache = Arc<std::sync::OnceLock<Option<(f64, f64, f64, f64)>>>;
mod linref_kernels;
mod masked;
mod methods_convert;
mod methods_core;
mod methods_coverage;
mod methods_distance;
mod methods_indexing;
mod methods_interop;
mod methods_measures;
mod methods_predicates;
mod methods_topology;
pub(crate) mod missing;
pub(crate) use missing::MissingMask;

mod methods_unary_io;
mod ops;
pub(crate) use linref_kernels::{line_interpolate_points_rows, line_locate_point_on_lines};
mod iter;
pub(crate) use iter::PyGeometryArrayIter;
mod prepared;
pub(crate) use prepared::{FrameCacheRows, fresh_prepared_cache};
mod storage_helpers;
mod storage_types;
pub(crate) use packed_column_builder::{PackedColumnBuilder, PointColumnBuilder};
pub(crate) use storage_helpers::{
    column_window, packed_centroid_xy, packed_surface_point, row_bounds, row_bounds_3d,
    row_bounds_values, row_ord_extremes,
};
pub(crate) use storage_impl::reverse_coord_windows;
mod coordinate_replacement;
pub(crate) use coordinate_replacement::{CoordinateReplacement, ReplacementAxis};
mod packed;
mod packed_column_kernels;
mod packed_columns;
mod packed_gather;
pub(crate) use packed_column_kernels::{
    bounds_3d_values_from_columns, bounds_values_from_columns_masked,
    ensure_geographic_columns_present, geographic_bounds_values_from_columns, line_measure_masked,
    map_coordseq_to_crs, packed_per_row_self_origin_affine_columns, polygon_measure_masked,
    reduce_lines_or_polygons, segmented_planar_lengths, segmented_planar_lengths_3d,
    subdivide_line_columns, subdivide_polygon_columns, total_bounds_from_columns,
};
pub(crate) use packed_columns::{
    PackedColumnError, PackedColumnOutput, PackedColumns, packed_columns_err,
};
pub(crate) use packed_gather::normalized_gather_storage;
mod pack_admission;
mod packed_builders;
mod packed_column_builder;
mod packed_equal;
mod packed_ops;
mod streaming;
mod uniform_columns;
pub(crate) use pack_admission::{
    packable_closed_ring_len, polygon_pack_axes, ring_seq_is_packable,
};
pub(crate) use packed::sparse_missing_mask;
pub(crate) use streaming::StreamingShapes;
mod shape_row;
mod storage_impl;
pub(crate) use packed_equal::pair_packed_equals_exact;
mod tests;

pub(crate) use shape_row::ShapeRow;
pub(crate) use storage_types::{
    GeometryArrayStorage, LineRows, PointRows, RowSelection, RowSelectionRef, RowsIter, ShapesIter,
    contiguous_physical_range, line_logical_len, packed_lines_coord_len, packed_polygons_coord_len,
    packed_polygons_ring_len, physical_row, point_logical_len, polygon_logical_len,
    polygon_rings_range, row_map_is_identity, row_selection_from_logical_rows,
};
