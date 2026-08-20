// `arbitrary_source_item_ordering` is suppressed HERE and only here.  It was
// additionally repeated verbatim in 187 files, and because lint levels cascade
// to child modules every one of those copies was dead weight — deleting them
// provably cannot change a lint outcome, which one `cargo clippy` run confirms.
//
// `clippy::absolute_paths` and `clippy::similar_names` deliberately stay
// FILE-SCOPED (~149 files) and are NOT hoisted here.  Hoisting them looks like
// the same cleanup and is not: those file-local blocks were the sole
// suppression, so a crate-root allow would silently extend it to ~390 files
// that never had one, hiding 543 + 285 real violations from any code written
// later.  Measured, not assumed.  Keep the blast radius per-file so the lints
// stay a usable signal.
#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "cohesive item layout is clearer than a mandated declaration order here"
)]
#![feature(portable_simd, trusted_len)]

mod boundary;
use boundary::{
    CoordinateInput, CrsCoordinateArgs, DefaultedF64Input, DefaultedI64Input, F64Param, Frame,
    FrameAdoption, FrameEdit, FrameError, GeometryTransformFrame, GridOrigin, I64Param, OriginSpec,
    Zt, ZtLaneRefs, ZtLanes, ZtValues, accuracy_option, angle_radians, broadcast_coordinate_group,
    broadcast_coordinate_input, broadcast_crs_coordinate_inputs, buffer_copy_to_slice_u64,
    buffer_to_vec_u64, cdt_refinement_values, checked_length_sum, collect_bool_mask,
    collect_bytes_rows, collect_i64_sequence, collect_py_iter, collect_py_iter_exact,
    collect_sequence_items, collect_u64_sequence, collect_usize_sequence, coordinate_arc_values,
    coordinate_arc_values_exact, coordinate_epoch_option, coordinate_input,
    coordinate_input_with_error, coordinate_input_with_expected, coordinate_inputs_are_scalar,
    coordinate_sequence_len_hint, coordinate_values, coordinates, coordinates_object, crs_arc,
    crs_arc_str, crs_operationally_equal, epoch_label, epochs_equal, finite_coordinate_required,
    finite_f64_required, geojson_dict, grow_sequence_error, is_one_byte_buffer, json_to_py,
    non_negative_int, optional_coordinate_arc_values, optional_coordinate_input_with_expected,
    parse_affine_matrix, parse_area, parse_cdt_refinement, parse_geometry_transform_options,
    parse_grid_size, parse_sample_count, parse_sample_seed, point_tuple, point_xy, positive_int,
    py_i64_bounded, py_i64_required, string_alloc_error, try_coordseq_from_nd_buffer, try_push,
    try_reserve_hint, try_string_from_str, try_vec_with_capacity, try_vec_with_capacity_hint,
    validate_lonlat_shape, validate_subdivide_max_vertices, wgs84_crs, with_one_byte_buffer,
};
mod collections;
#[cfg(test)]
mod compile_fail_gate;
mod crs;
mod curves;
mod error;
mod geometry;
#[cfg(test)]
mod test_support;
#[macro_use]
mod heap_size;
pub(crate) use heap_size::HeapSize;
#[macro_use]
mod pymethod_macros;
mod io;
mod measures;
mod numeric;
use numeric::{Finite, NonNegative, Positive};
mod text;
mod tokens;
use measures::{
    area, area_natural_array, area_natural_scalar, bearing, bounds, bounds_array,
    cross_track_distance, length, length_3d, length_natural_array, length_natural_scalar,
    point_between, rhumb_distance, shared_paths, snap, split,
};
mod array;
pub(crate) use array::{
    GeometryArrayStorage, PackedColumnError, PointRows, PyGeometryArray, ShapeRow,
};
mod broadcast;
use broadcast::{
    CollectRows, DistanceUnit, GeometryInput, OptionalDensifyParam, array_binary_geometry,
    broadcast2, broadcast2_geometry, classify_input, classify_required, coerce_geometry,
    crs_metric_binary_geometry_broadcast, ensure_same_len, exact_geometry, exact_geometry_array,
    expected_geometry_or_array, lonlat_shape, lonlat_shape_under, metric_frechet_densified,
    metric_hausdorff_densified, metric_maximum_inscribed_radius, metric_minimum_clearance,
    metric_minimum_clearance_line, metric_nearest_points, metric_shortest_line,
    multipoint_splitter_from_array, pair_distance_resolved, pair_distance_resolved_result,
    pair_dwithin_resolved, pair_dwithin_resolved_result, paired_arrays, parse_wkb_geometry,
    predicate_broadcast, push_ring_shapes, py_bool, relate_pattern_broadcast,
    relate_string_broadcast, require_point, resolve_metric, rows_err, validate_densify,
    xy_predicate,
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
    _unpickle_geometry, _unpickle_geometry_array, _unpickle_line_array, _unpickle_point_array,
    _unpickle_polygon_array, _unpickle_validation_report, OverlayOp, PyValidationReport,
    coverage_clean, coverage_invalid_edges, coverage_is_valid, coverage_simplify, coverage_union,
    difference, f64_column_le_bytes, from_features, from_geojson, from_wkb, from_wkt, intersection,
    intersection_all, osm_shortlink_encode, osm_shortlink_location, overlay_operator,
    pluscode_encode, pluscode_polygon, pluscode_recover, pluscode_shorten, polygonize,
    polygonize_full, require, symmetric_difference, symmetric_difference_all, to_feature,
    to_feature_collection, union, union_all, usize_row_map_le_bytes,
};
mod grid;
mod predicates;
pub(crate) use predicates::{
    _unpickle_prepared, IndexEnvelope, Predicate, PyPreparedGeometry, contains, contains_properly,
    contains_xy, covered_by, covers, crosses, disjoint, distance, distance_3d, dwithin, equals,
    equals_exact, equals_identical, frechet_distance, hausdorff_distance, intersects,
    intersects_xy, nearest_points, overlaps, relate, relate_pattern, scalar_vs_shapes,
    shortest_line, topology_scalar_pair, touches, within,
};
mod py;
mod render;
use std::sync::Arc;

use boundary::coordinates::CoordinateAxis;
use error::Result;
use geometry::{
    Bounds, BufferCapStyle, BufferJoinStyle, BufferSide, CoordSeq, CoordinateAxes, Coordinates,
    EmptyKind, GeometryErrorKind, GeometryKind, LineIndex, PlanarMetric, Point, Polygon,
    PolygonizeFull, RepairMethod, Ring, Shape, ShapeData, SimplifyMethod, SmoothMethod,
    ValidationIssue, VoronoiBoundary, point_distance, row_sample_seed,
};
use py::arrow::{ArrowEncoding, packed_points_to_arrow};
use py::crs::PyCrs;
use py::errors::{CRSError, GeometryError, InvalidGeometryError, geometry_type_err};
use py::support::{
    _token_vocabulary, CoercedCollectedGeometryItems, Crs, GeoJsonDecodeContext, GeometryValues,
    H3_MAX_RESOLUTION, InterpolatePlan, NavigationPath, QUADRANT_SEGMENTS_DEFAULT_I64,
    SVG_ARRAY_PREVIEW, ScalarOrIterator, SpatialCurve, VoronoiClipInput, Wgs84DefaultCrs,
    add_classes, add_functions, bounds_envelope, box_polygon, check_i32_min,
    classify_scalar_or_iterator, coerce_collected_geometry_items, coerce_geojson_geometry_value,
    common_crs_required, crs_label, crs_line_locate_point, deserialized_epoch,
    ensure_homogeneous_axes, extract_coordinate, extract_lines, extract_points, extract_polygons,
    features_type, fixed_geometry_array_nearest_points, geometry_array_line_locate_point_geometry,
    geometry_items, global_geographic_candidate_envelope, gometry_lib_module, guard_epoch_frame,
    index_bounds, index_envelope, is_mapping_like, is_py_bytes_or_bytearray, is_wgs84_family_crs,
    iterable_lane, line_interpolate_coordseq, line_interpolate_points_coordseq,
    line_interpolate_points_shape, line_interpolate_shape, line_locate_coordseq, line_locate_shape,
    line_substring_coordseq, line_substring_shape, mapping_as_dict, metric_concave_hull,
    metric_constructive_shape, metric_constructive_shape_budgeted, metric_interpolate_m,
    metric_minimum_bounding_circle, metric_minimum_bounding_radius,
    metric_optional_constructive_shape, missing_geometry_items_error, note_array_row,
    optional_coordinates, owned_voronoi_boundary, parse_crs, parse_crs_epoch, parse_curve_bounds,
    parse_geojson_geometry_value, parse_geojson_slice, parse_geojson_value, parse_interpolate_plan,
    parse_precision, parse_spatial_index_handle, parse_wkb_payload, parse_wkb_payload_batch,
    parse_wkb_payload_bytes, parse_wkt_output_dimension, parts, point_from_bounds,
    point_index_envelope, py_nearest_points, py_polygonize_full, py_text_borrow, py_to_json_value,
    require_geojson_crs, require_locate_point, require_locate_points, resolve_line_metric, rings,
    validate_buffer_miter_limit, validate_buffer_quadrant_segments, validate_densify_fraction,
    validate_distance, validate_distance_arg, validate_equals_exact_tolerance,
    validate_max_segment_length, validate_nearest_k, validate_smooth_iterations, voronoi_flatten,
    wrapped_box,
};
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyAny, PyBool, PyBytes, PyInt, PyList, PyModule, PyTuple};
use render::{geometry_array_svg_grid_html_masked, render_shape_svg};
use rstar::AABB;
use serde_json::Value;

use crate::py::numpy::float64_array;

// `gil_used = false`: free-threaded CPython is supported — every `#[pyclass]`
// is Send+Sync (lazy caches use OnceLock/Mutex/atomic; scratch pools are
// thread_local). PreparedGeometry no longer opts out of cross-thread sharing.
#[pymodule(gil_used = false)]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Cargo is the package version authority; exporting its compile-time
    // value keeps Python import off the importlib.metadata filesystem path.
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
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
        PyPreparedGeometry,
        PyCoordinates,
        PyCoordinatesIter,
        PyValidationReport,
    );
    py::functions::constructors::register(m)?;
    add_functions!(m; area, length, length_3d, bounds);
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
        parts, rings, get_coordinates,
        from_wkt, from_wkb, from_geojson, from_features, to_feature, to_feature_collection,
        from_polyline, require,
        // Pickle payload rebuilders (private; see the `__reduce__` impls).
        _unpickle_geometry, _unpickle_geometry_array, _unpickle_point_array, _unpickle_line_array,
        _unpickle_polygon_array,
        _unpickle_prepared, _unpickle_validation_report,
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

    const PROBE_COUNT_BELOW_GATE: usize = 9_999;
    const PROBE_COUNT_ABOVE_GATE: usize = 10_000;

    fn probes(count: usize) -> (Vec<f64>, Vec<f64>) {
        let mut state = 0x4D59_5DF4_u64;
        let mut next = || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            (state >> 11) as f64 / ((1_u64 << 53) as f64)
        };
        (
            std::iter::repeat_with(&mut next)
                .take(count)
                .map(|value| value * 24.0 - 12.0)
                .collect(),
            std::iter::repeat_with(next)
                .take(count)
                .map(|value| value * 24.0 - 12.0)
                .collect(),
        )
    }

    fn regular_polygon_wkt() -> String {
        let mut wkt = String::from("POLYGON ((");
        for index in 0..64 {
            let angle = std::f64::consts::TAU * f64::from(index) / 64.0;
            if index != 0 {
                wkt.push_str(", ");
            }
            let _ = write!(wkt, "{} {}", 10.0 * angle.cos(), 10.0 * angle.sin());
        }
        wkt.push_str(", 10 0))");
        wkt
    }

    fn bools(value: &Bound<'_, PyAny>) -> PyResult<Vec<bool>> {
        value.call_method0("tolist")?.extract()
    }

    fn xy_mismatch(
        py: Python<'_>,
        module: &Bound<'_, PyModule>,
        geometry: &Bound<'_, PyAny>,
        xs: &[f64],
        ys: &[f64],
        label: &str,
    ) -> PyResult<Option<String>> {
        let contains_xy = module.getattr("contains_xy")?;
        let x = PyList::new(py, xs)?;
        let y = PyList::new(py, ys)?;
        let fast = bools(&contains_xy.call1((geometry, &x, &y))?)?;
        for (index, ((&x, &y), &fast_verdict)) in xs.iter().zip(ys).zip(&fast).enumerate() {
            let exact = contains_xy.call1((geometry, x, y))?.extract::<bool>()?;
            if fast_verdict != exact {
                return Ok(Some(format!(
                    "{label} mismatch at probe {index} coordinate ({x}, {y}): fast={fast_verdict}, exact={exact}"
                )));
            }
        }
        Ok(None)
    }

    fn packed_mismatch(
        module: &Bound<'_, PyModule>,
        geometry: &Bound<'_, PyAny>,
        from_wkt: &Bound<'_, PyAny>,
        xs: &[f64],
        ys: &[f64],
    ) -> PyResult<Option<String>> {
        let point_wkts: Vec<String> = xs
            .iter()
            .zip(ys)
            .map(|(x, y)| format!("POINT ({x} {y})"))
            .collect();
        let points = from_wkt.call1((point_wkts,))?;
        let fast = bools(&module.getattr("contains")?.call1((geometry, &points))?)?;
        let contains_xy = module.getattr("contains_xy")?;
        for (index, ((&x, &y), &fast_verdict)) in xs.iter().zip(ys).zip(&fast).enumerate() {
            let exact = contains_xy.call1((geometry, x, y))?.extract::<bool>()?;
            if fast_verdict != exact {
                return Ok(Some(format!(
                    "packed mismatch at probe {index} coordinate ({x}, {y}): fast={fast_verdict}, exact={exact}"
                )));
            }
        }
        Ok(None)
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "assertions include the first mismatching coordinate in the failure"
    )]
    fn contains_xy_and_packed_points_match_exact_across_grid_gate() -> PyResult<()> {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let module = PyModule::new(py, "gometry._lib")?;
            lib(&module)?;
            let from_wkt = module.getattr("from_wkt")?;
            let geometry = from_wkt.call1((regular_polygon_wkt(),))?;

            let (xs, ys) = probes(PROBE_COUNT_BELOW_GATE);
            let below_mismatch = xy_mismatch(py, &module, &geometry, &xs, &ys, "XY below gate")?;
            assert!(
                below_mismatch.is_none(),
                "below-gate parity unexpectedly failed: {}",
                below_mismatch.unwrap_or_default()
            );
            let packed_below = packed_mismatch(&module, &geometry, &from_wkt, &xs, &ys)?;
            assert!(
                packed_below.is_none(),
                "packed below-gate parity unexpectedly failed: {}",
                packed_below.unwrap_or_default()
            );

            let (xs_above, ys_above) = probes(PROBE_COUNT_ABOVE_GATE);
            let mut failures = Vec::new();
            if let Some(failure) = xy_mismatch(
                py,
                &module,
                &geometry,
                &xs_above,
                &ys_above,
                "XY above gate",
            )? {
                failures.push(failure);
            }

            if let Some(failure) =
                packed_mismatch(&module, &geometry, &from_wkt, &xs_above, &ys_above)?
            {
                failures.push(failure);
            }
            assert!(failures.is_empty(), "{}", failures.join("\n"));
            Ok(())
        })
    }
}
