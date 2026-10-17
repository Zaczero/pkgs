//! The `GeometryArray` Python method surface (`#[pymethods] impl
//! PyGeometryArray`).
//!
//! Cohesive `#[pymethods]` blocks are split across files over native
//! storage in `geometry::storage`. Broadcast dispatch and sibling crate types
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
pub(crate) use crate::geometry::storage::{ArrayRows, LogicalRow, LogicalRows, StorageLayout};
pub(crate) use crate::geometry::{
    CsrOffsetBuilder, PolygonLevel, RingLevel, line_is_ccw, line_is_simple, line_is_valid,
    polygon_is_valid,
};
pub(crate) use crate::py::errors::{CRSError, GeometryError, InvalidGeometryError};
use crate::py::numpy::bool_array;
use crate::{
    Arc, ArrowEncoding, Bounds, CoordSeq, CoordinateAxes, Crs, DefaultedF64Input, DistanceUnit,
    F64Param, Frame, FrameAdoption, FrameEdit, GeometryKind, GeometryTransformFrame, HeapSize,
    I64Param, InterpolatePlan, OriginSpec, OverlayOp, Point, PyCoordinates, PyCrs, PyGeometry,
    PyTypeError, PyValidationReport, PyValueError, RepairMethod, SVG_ARRAY_PREVIEW, Shape,
    SimplifyMethod, SpatialCurve, TransformOptionArgs, Typed, VoronoiClipInput, VoronoiOutput,
    cdt_refinement_values, coerce_present_geometry_items, coordinate_epoch_option, coordinates,
    crs, crs_arc, crs_label, curves, exact_geometry, exact_geometry_array,
    expected_geometry_or_array, geometry, geometry_array_svg_grid_html_masked, io,
    line_interpolate_points_coordseq, line_interpolate_points_shape, line_locate_coordseq,
    map_coordinates_callback, non_negative_int, note_array_row, overlay_operator,
    owned_voronoi_boundary, parse_coordinate_replacement, parse_crs, parse_curve_bounds,
    parse_interpolate_plan, parse_precision, positive_int, pyclass, replace_shape_coordinates,
    require_geojson_crs, resolve_metric, row_sample_seed, slice_replacement_for_shape,
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
#[pyclass(
    name = "GeometryArray",
    module = "gometry",
    frozen,
    sequence,
    generic,
    weakref
)]
#[cfg_attr(not(Py_3_14), pyo3(immutable_type))]
#[derive(Clone, Debug)]
/// An immutable, vectorized geometry column with one shared CRS/epoch frame.
///
/// Homogeneous arrays use packed coordinate storage and batched Rust kernels;
/// indexing and slicing preserve zero-copy views where possible.
pub struct PyGeometryArray {
    /// Native owner for row storage, validity, and family-specific addressing.
    /// Packed points omit missing payload; CSR rows retain empty windows and
    /// Mixed storage retains a hidden placeholder at each missing position.
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

mod methods_unary_io;
mod ops;
pub(crate) use linref_kernels::{line_interpolate_points_rows, line_locate_point_on_lines};
mod iter;
pub(crate) use iter::PyGeometryArrayIter;
mod prepared;
pub(crate) use prepared::{
    ArrayRow, ArrayRowOperation, BoundsSeed, PreparedRow, RowProducts, packed_row_persistent,
};

mod packed;
mod packed_builders;
mod packed_columns;
mod packed_equal;
mod packed_ops;
mod streaming;
mod uniform_columns;
pub(crate) use packed::ComputedCaches;
pub(crate) use packed_equal::pair_packed_equals_exact;
pub(crate) use streaming::StreamingShapes;
mod tests;
