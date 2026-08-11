#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::num::NonZeroU32;

use smol_str::SmolStr;

use crate::{
    AABB, Bound, Bounds, BufferCapStyle, BufferJoinStyle, BufferSide, CRSError, CollectRows,
    CoordSeq, CoordinateAxes, DistanceUnit, Frame, FromPyObject, GeometryError, GeometryErrorKind,
    GeometryInput, InvalidGeometryError, LineIndex, NonNegative, PlanarMetric, Point, PointRows,
    Polygon, PolygonizeFull, Positive, Py, PyAny, PyAnyMethods, PyBool, PyBoolMethods,
    PyDictMethods, PyErr, PyFloatMethods, PyGeometry, PyGeometryArray, PyInt, PyListMethods,
    PyOnceLock, PyResult, PyStringMethods, PyTuple, PyTupleMethods, PyTypeError, PyTypeMethods,
    Python, RepairMethod, Result, Ring, Shape, ShapeData, SimplifyMethod, SmoothMethod, Typed,
    Value, VoronoiBoundary, classify_input, coerce_geometry, coordinate_epoch_option,
    coordinate_values, exact_geometry, finite_f64_required, geometry_type_err,
    metric_nearest_points, non_negative_int, push_ring_shapes, py_i64_bounded, py_i64_required,
    pyfunction, resolve_metric, rows_err, wgs84_crs, with_one_byte_buffer,
};

pub(crate) type Crs = SmolStr;

pub(crate) const QUADRANT_SEGMENTS_DEFAULT: NonZeroU32 = NonZeroU32::new(8).unwrap();
pub(crate) const QUADRANT_SEGMENTS_DEFAULT_I64: i64 = 8;
const _: () = assert!(QUADRANT_SEGMENTS_DEFAULT.get() == QUADRANT_SEGMENTS_DEFAULT_I64 as u32);

/// Cached `gometry._types.Extremes` constructor — `extremes()` runs in
/// user loops, so the per-call module import + attribute lookup is hoisted
/// into a process-wide cell.
pub(crate) fn extreme_points_type(py: Python<'_>) -> PyResult<&Bound<'_, pyo3::types::PyType>> {
    static CELL: pyo3::sync::PyOnceLock<Py<pyo3::types::PyType>> = pyo3::sync::PyOnceLock::new();
    CELL.import(py, "gometry._types", "Extremes")
}

/// Cached `gometry._types.PolygonizeResult` constructor (see
/// [`extreme_points_type`]).
pub(crate) fn polygonize_result_type(py: Python<'_>) -> PyResult<&Bound<'_, pyo3::types::PyType>> {
    static CELL: pyo3::sync::PyOnceLock<Py<pyo3::types::PyType>> = pyo3::sync::PyOnceLock::new();
    CELL.import(py, "gometry._types", "PolygonizeResult")
}

/// Cached `gometry._types.Features` constructor returned by the native
/// `from_features` boundary.
pub(crate) fn features_type(py: Python<'_>) -> PyResult<&Bound<'_, pyo3::types::PyType>> {
    static CELL: pyo3::sync::PyOnceLock<Py<pyo3::types::PyType>> = pyo3::sync::PyOnceLock::new();
    CELL.import(py, "gometry._types", "Features")
}

pub(crate) fn gometry_lib_module(py: Python<'_>) -> PyResult<&Bound<'_, pyo3::types::PyModule>> {
    static CELL: pyo3::sync::PyOnceLock<Py<pyo3::types::PyModule>> = pyo3::sync::PyOnceLock::new();
    CELL.get_or_try_init(py, || py.import("gometry._lib").map(Bound::unbind))
        .map(|module| module.bind(py))
}

/// Maximum number of geometries rendered in a `GeometryArray` HTML preview.
pub(crate) const SVG_ARRAY_PREVIEW: usize = 12;

/// Finest H3 resolution (cells span ``0..=H3_MAX_RESOLUTION``).
pub(crate) const H3_MAX_RESOLUTION: u8 = 15;

mod bounds_index;
mod constructive;
mod extract;
mod frame_args;
mod geojson;
mod io_encoders;
mod linref;
mod nearest;
mod tokens;
mod validate;

pub(crate) use bounds_index::{
    bounds_envelope, global_geographic_candidate_envelope, index_bounds, index_envelope,
    point_from_bounds, point_index_envelope,
};
pub(crate) use constructive::{
    VoronoiClipInput, geodesic_by_identity, metric_concave_hull, metric_constructive_shape,
    metric_constructive_shape_budgeted, metric_interpolate_m, metric_minimum_bounding_circle,
    metric_minimum_bounding_radius, metric_optional_constructive_shape, owned_voronoi_boundary,
    py_polygonize_full, voronoi_flatten,
};
pub(crate) use extract::{
    CoercedCollectedGeometryItems, GeometryValues, coerce_collected_geometry_items,
    ensure_homogeneous_axes, extract_coordinate, extract_lines, extract_points, extract_polygons,
    geometry_items, missing_geometry_items_error, optional_coordinates, parse_wkb_payload,
    parse_wkb_payload_batch, parse_wkb_payload_bytes, parts, rings,
};
pub(crate) use frame_args::{
    Wgs84DefaultCrs, common_crs_required, crs_label, deserialized_epoch, guard_epoch_frame,
    parse_crs, parse_crs_epoch, py_text_borrow, require_antimeridian_crs,
};
pub(crate) use geojson::{
    GeoJsonDecodeContext, ScalarOrIterator, classify_scalar_or_iterator,
    coerce_geojson_geometry_value, is_mapping_like, is_py_bytes_or_bytearray, is_wgs84_family_crs,
    mapping_as_dict, parse_geojson_geometry_value, parse_geojson_slice, parse_geojson_value,
    py_to_json_value, require_geojson_crs,
};
pub(crate) use io_encoders::{box_polygon, wrapped_box};
pub(crate) use linref::{
    InterpolatePlan, crs_line_locate_point, line_interpolate_coordseq,
    line_interpolate_points_coordseq, line_interpolate_points_shape, line_interpolate_shape,
    line_locate_coordseq, line_locate_shape, line_substring_coordseq, line_substring_shape,
    parse_interpolate_plan, resolve_line_metric,
};
pub(crate) use nearest::{
    fixed_geometry_array_nearest_points, geometry_array_line_locate_point_geometry,
    nearest_point_columns_masked, py_geometry_array, py_nearest_points, require_locate_point,
    require_locate_points,
};
pub(crate) use tokens::{
    _token_vocabulary, LineReferenceBasis, NavigationPath, SpatialCurve, TriangulationMethod,
};
pub(crate) use validate::{
    check_i32_min, iterable_lane, note_array_row, parse_curve_bounds, parse_precision,
    parse_spatial_index_handle, parse_wkt_output_dimension, spatial_key_for_shape_opt,
    validate_buffer_miter_limit, validate_buffer_quadrant_segments, validate_densify_fraction,
    validate_distance, validate_distance_arg, validate_equals_exact_tolerance,
    validate_max_segment_length, validate_nearest_k, validate_smooth_iterations,
};

/// Restore CPython's sequence-pattern flag for immutable PyO3 classes.
/// PyO3's `sequence` option installs sequence slots but does not include
/// `Py_TPFLAGS_SEQUENCE` in heap type flags; ABC registration cannot add it
/// after `immutable_type` is set.
pub(crate) fn mark_sequence_flag<T: pyo3::PyClass>(
    module: &pyo3::Bound<'_, pyo3::types::PyModule>,
) {
    if !T::IS_SEQUENCE {
        return;
    }
    let ty = module.py().get_type::<T>();
    // SAFETY: the type was created by PyO3 and this one-time write runs during
    // module assembly, before user code or a second thread can observe the
    // supposedly-immutable type. Only the sequence flag is added. On a
    // free-threaded build `tp_flags` is atomic; relaxed ordering is sufficient
    // because module publication provides the synchronization boundary.
    unsafe {
        TypeFlags::insert(
            &mut (*ty.as_type_ptr()).tp_flags,
            pyo3::ffi::Py_TPFLAGS_SEQUENCE,
        );
    }
}

trait TypeFlags {
    fn insert(&mut self, flags: core::ffi::c_ulong);
}

impl TypeFlags for core::ffi::c_ulong {
    fn insert(&mut self, flags: core::ffi::c_ulong) {
        *self |= flags;
    }
}

macro_rules! impl_atomic_type_flags {
    ($atomic:ty, $integer:ty) => {
        impl TypeFlags for $atomic {
            fn insert(&mut self, flags: core::ffi::c_ulong) {
                self.fetch_or(flags as $integer, core::sync::atomic::Ordering::Relaxed);
            }
        }
    };
}

impl_atomic_type_flags!(core::sync::atomic::AtomicU32, u32);
impl_atomic_type_flags!(core::sync::atomic::AtomicU64, u64);

/// Register a list of `#[pyclass]` types on the module in one statement.
macro_rules! add_classes {
    ($m:ident; $($class:ty),+ $(,)?) => {
        $($m.add_class::<$class>()?;)+
    };
}
pub(crate) use add_classes;

/// Register a list of `#[pyfunction]`s on the module in one statement.
macro_rules! add_functions {
    ($m:ident; $($function:path),+ $(,)?) => {
        $($m.add_function(pyo3::wrap_pyfunction!($function, $m)?)?;)+
    };
}
pub(crate) use add_functions;
