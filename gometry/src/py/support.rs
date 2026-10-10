use crate::crs::frame::crs_arc;
use crate::{
    AABB, Bound, Bounds, BufferCapStyle, BufferJoinStyle, BufferSide, CRSError, CollectRows,
    CoordSeq, CoordinateAxes, Crs, DistanceUnit, DwithinDistance, Frame, FromPyObject,
    GeometryError, GeometryErrorKind, GeometryInput, InvalidGeometryError, LineIndex, NonNegative,
    PlanarMetric, Point, Polygon, PolygonizeFull, Positive, Py, PyAny, PyAnyMethods, PyBool,
    PyBoolMethods, PyDictMethods, PyErr, PyFloatMethods, PyGeometry, PyGeometryArray, PyInt,
    PyListMethods, PyOnceLock, PyResult, PyStringMethods, PyTuple, PyTupleMethods, PyTypeError,
    PyTypeMethods, Python, RepairMethod, Result, Ring, Shape, SimplifyMethod, SmoothMethod, Value,
    VoronoiBoundary, classify_input, coerce_geometry, coordinate_epoch_option, exact_geometry,
    float_values, float_values_at_most, float_values_exact, metric_nearest_points_resolved,
    non_negative_int, py_i64_bounded, pyfunction, rows_err, wgs84_crs,
};

pub(crate) const QUADRANT_SEGMENTS_DEFAULT_I64: i64 = 8;

/// Resolve the chart metric used by a GeoJSON writer's temporary antimeridian
/// split. CRS-free coordinates use the CRS84 frame explicitly assigned by
/// RFC 7946; a named source frame retains its ellipsoid and angular units.
pub(crate) fn geojson_domain_metric<'a>(
    crs: Option<&str>,
    scratch: &'a mut crate::dispatch::MetricScratch,
) -> PyResult<(
    crate::geometry::TopologyDomain,
    crate::dispatch::MetricCtx<'a>,
)> {
    let source_crs = crs.map_or_else(wgs84_crs, crs_arc);
    let frame = Frame::new(Some(source_crs), None)?;
    crate::dispatch::MetricResolver::GeographicCarry.resolve_topology_ctx(&frame, scratch)
}

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
    bounds_envelope, global_geographic_candidate_envelope, index_envelopes, point_from_bounds,
};
pub(crate) use constructive::{
    VoronoiClipInput, owned_voronoi_boundary, py_polygonize_full, voronoi_edge_lines_flatten,
    voronoi_flatten,
};
pub(crate) use extract::{
    GeometryValues, OwnedBytes, coerce_present_geometry_items, copy_wkb_payload,
    ensure_homogeneous_axes, exact_backed_bytes, extract_coordinate, extract_coordinate_or_empty,
    extract_lines, extract_points, extract_polygons, geometry_items, missing_geometry_items_error,
    optional_coordinates, own_geometry_item, parse_owned_geometry_item, point_xy,
};
pub(crate) use frame_args::{
    Wgs84DefaultCrs, deserialized_epoch, guard_epoch_frame, parse_crs, parse_crs_epoch,
    py_text_borrow,
};
pub(crate) use geojson::{
    GeoJsonDecodeContext, JsonValueError, MAX_JSON_NESTING, ScalarOrIterator,
    classify_scalar_or_iterator, coerce_geojson_geometry_value, coerce_geojson_geometry_value_at,
    deserialize_json, is_concrete_batch, is_mapping_like, is_py_bytes_or_bytearray,
    is_wgs84_family_crs, is_wkt_string, mapping_as_dict, parse_geojson_geometry_value,
    parse_geojson_slice, parse_geojson_text, py_to_geojson_data_value, py_to_geojson_json_value,
    py_to_json_value, py_to_json_value_classified, require_geojson_crs,
    validate_geojson_feature_side_data, validate_geojson_json_value,
};
pub(crate) use io_encoders::box_polygon;
pub(crate) use linref::{
    InterpolatePlan, PreparedLineLocate, line_interpolate_coordseq,
    line_interpolate_points_coordseq, line_interpolate_points_shape, line_interpolate_shape,
    line_interpolate_shape_m, line_locate_coordseq, line_substring_coordseq, line_substring_shape,
    line_substring_shape_m, parse_interpolate_plan, prepare_line_locate,
};
pub(crate) use nearest::{
    fixed_geometry_array_nearest_points_admitted, fixed_geometry_array_shortest_line_geodesic,
    locate_query, nearest_point_columns_masked,
};
pub(crate) use tokens::{
    _token_vocabulary, LineReferenceBasis, NavigationPath, SpatialCurve, TriangulationMethod,
    VoronoiOutput,
};
pub(crate) use validate::{
    check_i32_min, note_array_row, parse_curve_bounds, parse_precision, parse_spatial_index_handle,
    validate_buffer_miter_limit, validate_buffer_quadrant_segments, validate_densify_fraction,
    validate_distance, validate_dwithin_distance, validate_dwithin_distance_arg,
    validate_equals_exact_tolerance, validate_max_segment_length, validate_nearest_k,
    validate_smooth_iterations,
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
