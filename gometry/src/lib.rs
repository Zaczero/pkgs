// Crate-wide lint policy lives here and nowhere else. `similar_names` is
// deliberately not hoisted: it stays an `expect` on the files whose axis and
// formula vocabulary trips it, so it still flags confusable names elsewhere.
#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "cohesive item layout is clearer than a mandated declaration order here"
)]
#![expect(
    clippy::redundant_closure_for_method_calls,
    reason = "the suggested method paths are private module paths that do not resolve at the call site; the closure is the readable form"
)]
#![expect(
    clippy::too_many_lines,
    reason = "a flow reads best in one place; the long functions here are single pipelines whose phases share state, and splitting them would smear one operation across helpers"
)]
#![deny(clippy::unreadable_literal, clippy::redundant_clone)]
#![feature(
    allocator_api,
    once_cell_try,
    portable_simd,
    slice_partition_dedup,
    trusted_len,
    vec_fallible_shrink,
    vec_push_within_capacity
)]

mod boundary;
use boundary::{
    CoordinateInput, CrsCoordinateArgs, CsrBuilder, DefaultedF64Input, DefaultedI64Input, F64Param,
    Frame, FrameAdoption, FrameEdit, FrameError, GeometryTransformFrame, GridOrigin, I64Param,
    OriginSpec, TransformOptionArgs, Zt, ZtLaneRefs, ZtLanes, ZtValues, advisory_len,
    angle_radians, broadcast_coordinate_group, broadcast_coordinate_input,
    broadcast_crs_coordinate_inputs, buffer_copy_to_slice_u64, buffer_to_vec_u64,
    cdt_refinement_values, check_python_signals, collect_bool_mask, collect_bytes_rows,
    collect_i64_sequence, collect_py_iter, collect_py_iter_at_most_from_iter,
    collect_py_iter_exact, collect_py_iter_exact_from_iter, collect_py_iter_from_iter,
    collect_py_strings, collect_sequence_items, collect_u64_sequence, coordinate_column_values,
    coordinate_column_values_exact, coordinate_column_values_from_buffer, coordinate_epoch_option,
    coordinate_input_with_expected, coordinate_inputs_are_scalar, coordinate_sequence_len_hint,
    coordinates, coordinates_object, copy_one_byte_buffer, crs_arc, crs_operationally_equal,
    epoch_label, f64_required, f64_scalar_named, finite_coordinate_required, finite_f64_required,
    float_values, float_values_at_most, float_values_exact, is_one_byte_buffer, json_to_py,
    non_negative_int, optional_coordinate_column_values,
    optional_coordinate_column_values_from_buffer, optional_coordinate_input_with_expected,
    parameter_input_with_expected, parse_affine_matrix, parse_area, parse_cdt_refinement,
    parse_grid_size, parse_sample_count, parse_sample_seed, point_tuple, point_xy, positive_int,
    py_i64_bounded, py_i64_required, py_index_check, py_index_object, py_isize_required,
    py_iter_or_error, py_u64_if_index, py_u64_required, reject_multidimensional_coordinate_array,
    strict_i32, strict_i32_overflow, strict_i64, strict_i64_overflow, strict_i128,
    strict_optional_i64, strict_u64, try_collect_results, try_collect_vec,
    try_coordseq_from_nd_buffer, try_push, try_push_heap, try_push_pair, try_push_vec,
    try_reserve_heap, try_reserve_hint, try_reserve_pair, try_reserve_vec, try_string_from_str,
    try_vec_with_capacity, try_vec_with_capacity_hint, validate_lonlat_shape,
    validate_subdivide_max_vertices, wgs84_crs, with_one_byte_buffer,
};
mod collections;
#[cfg(test)]
mod compile_fail_gate;
mod crs;
mod curves;
mod error;
mod geographic;
mod geometry;
#[cfg(test)]
mod test_support;
#[cfg(test)]
#[global_allocator]
static TEST_ALLOCATOR: test_support::CountingAllocator = test_support::CountingAllocator;
#[cfg(not(test))]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;
#[macro_use]
mod heap_size;
pub(crate) use heap_size::HeapSize;
#[macro_use]
mod pymethod_macros;
mod io;
mod measures;
mod numeric;
use numeric::{DwithinDistance, Finite, NonNegative, Positive};
mod text;
mod tokens;
use measures::{
    area, area_natural_array, area_natural_scalar, bearing, bounds_array, cross_track_distance,
    length, length_3d, length_natural_array, length_natural_scalar, point_between, rhumb_distance,
    shared_paths, snap, split,
};
mod array;
pub(crate) use array::{
    GeometryArrayStorage, PackedColumnError, PointRows, PyGeometryArray, ShapeRow,
};
mod broadcast;
use broadcast::{
    CollectRows, DistanceUnit, GeometryInput, LonLatPointsError, broadcast2, broadcast2_geometry,
    classify_input, classify_required, coerce_geometry, ensure_same_len, exact_geometry,
    exact_geometry_array, expected_geometry_or_array, lonlat_points, lonlat_shape,
    metric_maximum_inscribed_radius, metric_minimum_clearance, metric_minimum_clearance_line,
    metric_nearest_points_resolved, metric_shortest_line_resolved, multipoint_splitter_from_array,
    pair_distance_geodesic, pair_distance_planar, pair_dwithin_geodesic, pair_dwithin_planar,
    parse_wkb_geometry, py_bool, relate_pattern_broadcast, relate_string_broadcast, require_point,
    resolve_metric, rows_err, xy_predicate,
};
mod dispatch;
pub(crate) use py::classes::{
    PyCoordinates, PyCoordinatesIter, PyGeometry, PyGeometryCollection, PyGeometryParts,
    PyGeometryPartsIter, PyLineString, PyMultiLineString, PyMultiPoint, PyMultiPolygon, PyPoint,
    PyPolygon, Typed, get_coordinates, map_coordinates_callback, parse_coordinate_replacement,
    replace_shape_coordinates, slice_replacement_for_shape,
};
use py::functions::polyline::from_polyline;
use py::functions::{
    _unpickle_geometry, _unpickle_geometry_array, _unpickle_packed_array,
    _unpickle_validation_report, OverlayOp, PyValidationReport, coverage_clean,
    coverage_invalid_edges, coverage_is_valid, coverage_simplify, coverage_union, difference,
    from_features, from_geojson, from_wkb, from_wkt, intersection, intersection_all,
    osm_shortlink_encode, osm_shortlink_location, overlay_operator, pluscode_encode,
    pluscode_polygon, pluscode_recover, pluscode_shorten, polygonize, polygonize_full, require,
    symmetric_difference, symmetric_difference_all, to_feature, to_feature_collection, union,
    union_all,
};
mod grid;
mod predicates;
pub(crate) use predicates::{
    IndexEnvelope, Predicate, contains, contains_properly, contains_xy, covered_by, covers,
    crosses, disjoint, distance, distance_3d, dwithin, equals, equals_exact, equals_identical,
    frechet_distance, hausdorff_distance, intersects, intersects_xy, nearest_points, overlaps,
    relate, relate_pattern, scalar_vs_shapes, shortest_line, topology_pair_after_gates,
    topology_scalar_pair, touches, within,
};
mod py;
mod render;
mod scratch_pool;
use std::sync::Arc;

use boundary::coordinates::CoordinateAxis;
use error::Result;
use geometry::{
    Bounds, BufferCapStyle, BufferJoinStyle, BufferSide, CoordSeq, CoordinateAxes, Coordinates,
    GeometryErrorKind, GeometryKind, LineIndex, PlanarMetric, Point, Polygon, PolygonizeFull,
    PreparedPlan, RepairMethod, Ring, Shape, ShapeData, SimplifyMethod, SmoothMethod,
    ValidationIssue, VoronoiBoundary, point_distance, row_sample_seed,
};
use py::arrow::ArrowEncoding;
use py::crs::PyCrs;
use py::errors::{CRSError, GeometryError, InvalidGeometryError, geometry_type_err};
use py::support::{
    _token_vocabulary, CoercedCollectedGeometryItems, Crs, GeoJsonDecodeContext, GeometryValues,
    H3_MAX_RESOLUTION, InterpolatePlan, NavigationPath, OwnedBytes, QUADRANT_SEGMENTS_DEFAULT_I64,
    SVG_ARRAY_PREVIEW, ScalarOrIterator, SpatialCurve, VoronoiClipInput, Wgs84DefaultCrs,
    add_classes, add_functions, bounds_envelope, box_polygon, check_i32_min,
    classify_scalar_or_iterator, coerce_collected_geometry_items, coerce_geojson_geometry_value,
    coerce_geojson_geometry_value_at, copy_wkb_payload, crs_label, crs_line_locate_point,
    deserialized_epoch, ensure_homogeneous_axes, exact_backed_bytes, extract_coordinate,
    extract_coordinate_or_empty, extract_lines, extract_points, extract_polygons, features_type,
    fixed_geometry_array_nearest_points_admitted, fixed_geometry_array_shortest_line_geodesic,
    geometry_array_line_locate_point_geometry, geometry_items,
    global_geographic_candidate_envelope, gometry_lib_module, guard_epoch_frame, index_envelopes,
    is_concrete_batch, is_mapping_like, is_py_bytes_or_bytearray, is_wgs84_family_crs,
    is_wkt_string, line_interpolate_coordseq, line_interpolate_points_coordseq,
    line_interpolate_points_shape, line_interpolate_shape, line_interpolate_shape_m,
    line_locate_coordseq, line_locate_shape, line_locate_shape_m, line_substring_coordseq,
    line_substring_shape, line_substring_shape_m, mapping_as_dict, metric_concave_hull,
    metric_constructive_shape, metric_constructive_shape_budgeted, metric_interpolate_m,
    metric_maximum_inscribed_circle, metric_minimum_bounding_circle,
    metric_minimum_bounding_radius, metric_optional_constructive_shape,
    missing_geometry_items_error, nearest_point_columns_masked, note_array_row,
    optional_coordinates, owned_voronoi_boundary, parse_crs, parse_crs_epoch, parse_curve_bounds,
    parse_geojson_geometry_value, parse_geojson_slice, parse_interpolate_plan, parse_precision,
    parse_spatial_index_handle, parse_wkt_output_dimension, point_from_bounds, py_polygonize_full,
    py_text_borrow, py_to_json_value, require_geojson_crs, require_locate_point,
    require_locate_points, resolve_line_metric, validate_buffer_miter_limit,
    validate_buffer_quadrant_segments, validate_densify_fraction, validate_distance,
    validate_distance_arg, validate_dwithin_distance, validate_dwithin_distance_arg,
    validate_equals_exact_tolerance, validate_max_segment_length, validate_nearest_k,
    validate_smooth_iterations, voronoi_edges_flatten, voronoi_flatten, wrapped_box,
};
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyAny, PyBool, PyDict, PyDictMethods, PyInt, PyList, PyModule, PyTuple};
pub(crate) use render::{
    geometry_array_svg_grid_html_masked, html_text_escape, render_shape_html, render_shape_svg,
};
use rstar::AABB;
use serde_json::Value;

use crate::py::numpy::float64_array;

// `gil_used = false`: free-threaded CPython is supported — every `#[pyclass]`
// is Send+Sync (lazy caches use OnceLock/Mutex/atomic; scratch pools are
// thread_local).
#[pymodule(gil_used = false)]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Cargo is the package version authority; exporting its compile-time
    // value keeps Python import off the importlib.metadata filesystem path.
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    let build_info = PyDict::new(m.py());
    build_info.set_item("schema_version", 3)?;
    build_info.set_item(
        "source_inputs_format",
        env!("GOMETRY_BUILD_SOURCE_INPUTS_FORMAT"),
    )?;
    build_info.set_item(
        "source_inputs_sha256",
        env!("GOMETRY_BUILD_SOURCE_INPUTS_SHA256"),
    )?;
    build_info.set_item(
        "native_source_inputs_format",
        env!("GOMETRY_BUILD_NATIVE_SOURCE_INPUTS_FORMAT"),
    )?;
    build_info.set_item(
        "native_source_inputs_sha256",
        env!("GOMETRY_BUILD_NATIVE_SOURCE_INPUTS_SHA256"),
    )?;
    build_info.set_item("profile_family", env!("GOMETRY_BUILD_PROFILE_FAMILY"))?;
    build_info.set_item("opt_level", env!("GOMETRY_BUILD_OPT_LEVEL"))?;
    build_info.set_item(
        "debug_assertions",
        env!("GOMETRY_BUILD_DEBUG_ASSERTIONS") == "true",
    )?;
    build_info.set_item("target", env!("GOMETRY_BUILD_TARGET"))?;
    build_info.set_item("target_arch", env!("GOMETRY_BUILD_TARGET_ARCH"))?;
    build_info.set_item("target_features", env!("GOMETRY_BUILD_TARGET_FEATURES"))?;
    m.add("__build_info__", build_info)?;
    add_classes!(m;
        PyGeometry,
        // Typed leaf subclasses of Geometry (one per Shape variant).
        PyPoint,
        PyMultiPoint,
        PyLineString,
        PyMultiLineString,
        PyPolygon,
        PyMultiPolygon,
        PyGeometryCollection,
        PyGeometryParts,
        PyGeometryPartsIter,
        PyGeometryArray,
        array::PyGeometryArrayIter,
        PyCoordinates,
        PyCoordinatesIter,
        PyValidationReport,
    );
    py::functions::constructors::register(m)?;
    add_functions!(m; area, length, length_3d);
    add_functions!(m;
        snap, shared_paths,
        // Spatial predicates and pairwise measures.
        contains, contains_properly, within, covers, covered_by, contains_xy,
        intersects_xy, intersects, disjoint, touches, crosses, overlaps,
        relate, relate_pattern, equals, equals_exact, equals_identical,
        distance, distance_3d, hausdorff_distance,
        frechet_distance, nearest_points, shortest_line, split, dwithin,
        bearing, cross_track_distance, point_between, rhumb_distance,
    );
    py::index::register(m)?;
    add_functions!(m;
        intersection, union, difference, symmetric_difference, union_all,
        intersection_all, symmetric_difference_all,
        coverage_is_valid, coverage_invalid_edges, coverage_simplify, coverage_clean,
        coverage_union, polygonize, polygonize_full,
        // IO, validation, and coordinate access.
        get_coordinates,
        from_wkt, from_wkb, from_geojson, from_features, to_feature, to_feature_collection,
        from_polyline, require,
        // Pickle payload rebuilders (private; see the `__reduce__` impls).
        _unpickle_geometry, _unpickle_geometry_array, _unpickle_packed_array,
        _unpickle_validation_report,
        // Token vocabularies for the stub-parity gate (private).
        _token_vocabulary,
    );
    py::arrow::register(m)?;
    py::crs::register(m)?;
    py::vectors::register(m)?;
    py::errors::register(m)?;
    py::cells::register(m)?;
    // Private test-owned indirect PEP-3118 buffers (suboffset soundness).
    py::test_buffers::register(m)?;
    add_functions!(m;
        pluscode_encode, pluscode_polygon, pluscode_shorten, pluscode_recover,
        osm_shortlink_encode, osm_shortlink_location,
    );
    // PyO3 auto-collects every registered name into `__all__`; drop the
    // private plumbing (pickle rebuilders, gate registries) so
    // `from gometry._lib import *` and the stub's mirrored `__all__` stay
    // public-only.
    let public: Vec<String> = m
        .getattr("__all__")?
        .extract::<Vec<String>>()?
        .into_iter()
        .filter(|name| !name.starts_with('_'))
        .collect();
    m.setattr("__all__", public)?;
    Ok(())
}

#[cfg(test)]
mod above_gate_parity_tests {
    use std::fmt::Write as _;

    use pyo3::types::{PyList, PyModule};

    use super::*;

    #[derive(Clone, Copy)]
    struct NamedProbe {
        name: &'static str,
        x: f64,
        y: f64,
        expected: bool,
    }

    const NAMED_PROBES: [NamedProbe; 9] = [
        NamedProbe {
            name: "inside-center",
            x: 0.0,
            y: 0.0,
            expected: true,
        },
        NamedProbe {
            name: "inside-east",
            x: 9.0,
            y: 0.0,
            expected: true,
        },
        NamedProbe {
            name: "outside-east",
            x: 11.0,
            y: 0.0,
            expected: false,
        },
        NamedProbe {
            name: "outside-corner",
            x: 7.5,
            y: 7.5,
            expected: false,
        },
        NamedProbe {
            name: "vertex-east",
            x: 10.0,
            y: 0.0,
            expected: false,
        },
        NamedProbe {
            name: "vertex-north",
            x: 0.0,
            y: 10.0,
            expected: false,
        },
        NamedProbe {
            name: "edge-east-north",
            x: 9.5,
            y: 0.5,
            expected: false,
        },
        NamedProbe {
            name: "near-edge-inside",
            x: 9.499_023_437_5,
            y: 0.5,
            expected: true,
        },
        NamedProbe {
            name: "near-edge-outside",
            x: 9.500_976_562_5,
            y: 0.5,
            expected: false,
        },
    ];

    fn repeated_probe_batch(repetitions: usize) -> (Vec<f64>, Vec<f64>, Vec<bool>) {
        let count = NAMED_PROBES.len() * repetitions;
        let mut xs = Vec::with_capacity(count);
        let mut ys = Vec::with_capacity(count);
        let mut expected = Vec::with_capacity(count);
        for _ in 0..repetitions {
            for probe in NAMED_PROBES {
                xs.push(probe.x);
                ys.push(probe.y);
                expected.push(probe.expected);
            }
        }
        (xs, ys, expected)
    }

    fn indexed_polygon_wkt() -> String {
        let mut wkt = String::from("POLYGON ((");
        // Keep the non-rectangular ring on the indexed boundary-classification
        // path without pinning that implementation's crossover in assertions.
        for index in 0..256 {
            let (x, y) = match index {
                0 => (10.0, 0.0),
                1 => (9.0, 1.0),
                64 => (0.0, 10.0),
                128 => (-10.0, 0.0),
                192 => (0.0, -10.0),
                _ => {
                    let angle = std::f64::consts::TAU * f64::from(index) / 256.0;
                    (10.0 * angle.cos(), 10.0 * angle.sin())
                },
            };
            if index != 0 {
                wkt.push_str(", ");
            }
            let _ = write!(wkt, "{x} {y}");
        }
        wkt.push_str(", 10 0))");
        wkt
    }

    fn bools(value: &Bound<'_, PyAny>) -> PyResult<Vec<bool>> {
        value.call_method0("tolist")?.extract()
    }

    fn xy_values(
        py: Python<'_>,
        contains_xy: &Bound<'_, PyAny>,
        geometry: &Bound<'_, PyAny>,
        xs: &[f64],
        ys: &[f64],
    ) -> PyResult<Vec<bool>> {
        let x = PyList::new(py, xs)?;
        let y = PyList::new(py, ys)?;
        bools(&contains_xy.call1((geometry, &x, &y))?)
    }

    fn packed_values(
        contains: &Bound<'_, PyAny>,
        geometry: &Bound<'_, PyAny>,
        from_wkt: &Bound<'_, PyAny>,
        xs: &[f64],
        ys: &[f64],
    ) -> PyResult<Vec<bool>> {
        let point_wkts: Vec<String> = xs
            .iter()
            .zip(ys)
            .map(|(x, y)| format!("POINT ({x} {y})"))
            .collect();
        let points = from_wkt.call1((point_wkts,))?;
        bools(&contains.call1((geometry, &points))?)
    }

    fn assert_batch(label: &str, actual: &[bool], expected: &[bool]) {
        assert_eq!(
            actual.len(),
            expected.len(),
            "{label} returned a different number of results"
        );
        assert_eq!(actual, expected, "{label} results differed");
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "assertions include the first mismatching coordinate in the failure"
    )]
    #[expect(
        clippy::similar_names,
        reason = "xs/ys are the two coordinate lanes of one probe batch"
    )]
    fn contains_xy_and_packed_points_match_named_indexed_cases() -> PyResult<()> {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let module = PyModule::new(py, "gometry._lib")?;
            lib(&module)?;
            let from_wkt = module.getattr("from_wkt")?;
            let geometry = from_wkt.call1((indexed_polygon_wkt(),))?;
            let contains_xy = module.getattr("contains_xy")?;
            let contains = module.getattr("contains")?;

            for probe in NAMED_PROBES {
                let actual = contains_xy
                    .call1((&geometry, probe.x, probe.y))?
                    .extract::<bool>()?;
                assert_eq!(
                    actual, probe.expected,
                    "scalar {} classified ({}, {}) incorrectly",
                    probe.name, probe.x, probe.y
                );
            }

            let (small_xs, small_ys, small_expected) = repeated_probe_batch(1);
            assert_batch(
                "small contains_xy batch",
                &xy_values(py, &contains_xy, &geometry, &small_xs, &small_ys)?,
                &small_expected,
            );
            assert_batch(
                "small packed batch",
                &packed_values(&contains, &geometry, &from_wkt, &small_xs, &small_ys)?,
                &small_expected,
            );

            // The indexed admission belongs to the 256-edge polygon, not to a
            // probe-count threshold; repetition only exercises a larger batch.
            let (large_xs, large_ys, large_expected) = repeated_probe_batch(64);
            assert_batch(
                "large contains_xy batch",
                &xy_values(py, &contains_xy, &geometry, &large_xs, &large_ys)?,
                &large_expected,
            );
            assert_batch(
                "large packed batch",
                &packed_values(&contains, &geometry, &from_wkt, &large_xs, &large_ys)?,
                &large_expected,
            );
            Ok(())
        })
    }
}
