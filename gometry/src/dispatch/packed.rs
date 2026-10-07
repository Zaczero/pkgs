use std::sync::Arc;

use pyo3::prelude::*;

use crate::broadcast::{
    CollectRows as _, degrade_linref_linestring, degrade_linref_point, rows_err,
};
use crate::crs::MetricModel;
use crate::dispatch::metric::MetricScratch;
use crate::dispatch::operation::{MetricOp, Operation};
use crate::dispatch::{Lane, kernels};
use crate::geometry::storage::packed_columns::resolve_packed_columns;
use crate::geometry::storage::{
    GeometryArrayStorage, PackedStorage, RowSelection, RowSelectionRef,
};
use crate::geometry::{
    Bounds, CoordSeq, CsrOffsetColumn, MeasureRange, OriginSpec, Shape, SimplifyMethod,
    TopologyDomain,
};
use crate::py::numpy::{bool_array, bounds_array, float64_array};
use crate::{
    DistanceUnit, F64Param, NavigationPath, PyGeometryArray, line_interpolate_coordseq,
    line_substring_coordseq,
};

/// Per-op parameters for the parameterized unary packed-column fast paths.
///
/// One variant per op family, carrying exactly that op's fields — a producer
/// that misses a parameter is a COMPILE error. (The predecessor was a
/// 25-field all-`Option` bag consumed with `?`, where one forgotten field
/// silently dropped the fast path to the per-row lane.) Param-less fast
/// paths (convex hull, boundary, reverse, …) stay keyed by [`Operation`]
/// alone and take no variant.
pub(crate) enum PackedUnary {
    /// Synthesizing ops whose packed lane needs no runtime parameter.
    Synthesized,
    Segmentize {
        max_segment_length: F64Param,
        path: Option<NavigationPath>,
    },
    Densify {
        fraction: F64Param,
        path: Option<NavigationPath>,
    },
    Simplify {
        tolerance: F64Param,
        method: SimplifyMethod,
        preserve_topology: bool,
    },
    Affine {
        matrix: [f64; 6],
    },
    Rotate {
        origin: OriginSpec,
        angle_radians: f64,
    },
    Scale {
        origin: OriginSpec,
        xfact: f64,
        yfact: f64,
    },
    Skew {
        origin: OriginSpec,
        tan_x: f64,
        tan_y: f64,
    },
    OrientPolygons {
        ccw: bool,
    },
    RemoveRepeatedPoints {
        tolerance: F64Param,
    },
    SnapToGrid {
        size: (f64, f64),
        origin: (f64, f64),
        repair: bool,
    },
    Force3d {
        z_fill: f64,
    },
    /// `set_z` / `set_m` (which ordinate comes from the [`Operation`]).
    SetOrdinate {
        value: Option<f64>,
        overwrite: bool,
    },
    Quantize {
        precision: i32,
    },
    ClipByRect {
        minx: f64,
        miny: f64,
        maxx: f64,
        maxy: f64,
    },
}

/// Distance-basis array linref parameters. Owns the admitted [`MetricModel`]
/// once for packed hit and packed-miss.
pub(crate) enum PackedLinearReference {
    InterpolatePoint {
        distance: F64Param,
        normalized: bool,
        metric: MetricModel,
    },
    Substring {
        start_distance: F64Param,
        end_distance: F64Param,
        normalized: bool,
        metric: MetricModel,
    },
}

fn packed_f64_lane(
    py: Python<'_>,
    array: &PyGeometryArray,
    values: PyResult<Option<Vec<f64>>>,
) -> Option<PyResult<Py<PyAny>>> {
    match values {
        Ok(Some(mut values)) => {
            if let Some(mask) = array.missing() {
                for (value, missing) in values.iter_mut().zip(mask.iter()) {
                    if missing {
                        *value = f64::NAN;
                    }
                }
            }
            Some(float64_array(py, values))
        },
        Ok(None) => None,
        Err(error) => Some(Err(error)),
    }
}

/// Packed-column fast paths for unary measure ops returning ``float64`` ndarrays.
#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "the packed f64 fast path handles only three measures; other operations continue to later dispatch"
)]
pub(crate) fn try_unary_packed_f64(
    py: Python<'_>,
    array: &PyGeometryArray,
    op: Operation,
    unit: Option<DistanceUnit>,
) -> Option<PyResult<Py<PyAny>>> {
    match op {
        Operation::Metric(MetricOp::Area) => {
            packed_f64_lane(py, array, array.area_unary_packed(py, unit))
        },
        Operation::Metric(MetricOp::Length) => packed_f64_lane(
            py,
            array,
            array.length_unary_packed(py, unit, MetricOp::Length),
        ),
        Operation::Metric(MetricOp::Length3d) => packed_f64_lane(
            py,
            array,
            array.length_3d_unary_packed(py, unit, MetricOp::Length3d),
        ),
        _ => None,
    }
}

fn packed_bool_lane(
    array: &PyGeometryArray,
    values: Option<PyResult<Vec<bool>>>,
) -> Option<PyResult<Vec<bool>>> {
    let mut values = match values? {
        Ok(values) => values,
        Err(error) => return Some(Err(error)),
    };
    // Belt-and-braces: kernels that already skip missing rows still get the
    // shape-lane missing sentinel (`false`) applied here. Cheap when the
    // kernel already wrote false for masked slots.
    if let Some(mask) = array.missing() {
        for (value, missing) in values.iter_mut().zip(mask.iter()) {
            if missing {
                *value = false;
            }
        }
    }
    Some(Ok(values))
}

/// Packed-column fast paths for unary validation predicates returning ``bool`` ndarrays.
pub(crate) fn try_unary_packed_bool(
    py: Python<'_>,
    array: &PyGeometryArray,
    op: Operation,
) -> Option<PyResult<Py<PyAny>>> {
    if array.is_compact_nullable() {
        return None;
    }
    // Every row of real topology work runs detached, like the f64 lanes; only
    // the ndarray build needs the interpreter back.
    let values = py.detach(|| packed_bool_values(array, op))?;
    Some(values.and_then(|values| bool_array(py, values)))
}

/// The packed boolean kernels, with the missing-row sentinel applied.
#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "only packed boolean predicates are handled here; other operations continue to later dispatch"
)]
fn packed_bool_values(array: &PyGeometryArray, op: Operation) -> Option<PyResult<Vec<bool>>> {
    // Packed bool kernels run over physical rows (including NaN placeholders
    // on missing slots). [`packed_bool_lane`] then writes the missing-row
    // sentinel (`false`) from the mask — identical to the shape-lane answer
    // for masked rows. No blanket `has_missing()` bail: that forced a 10–500×
    // shape-lane cliff on nullable is_valid/is_simple.
    let domain = match crate::geometry::topology_domain(&array.frame) {
        Ok(domain) => domain,
        Err(error) => return Some(Err(error.into())),
    };
    if matches!(
        op,
        Operation::IsRing | Operation::IsSimple | Operation::IsValid
    ) && let TopologyDomain::Geographic(frame) = domain
    {
        let columns =
            resolve_packed_columns(array.storage(), array.missing(), array.point_row_layout())?;
        if !columns.faithful_geographic_predicates(frame) {
            for row in array.row_operation().present_rows() {
                let route = match row.row().with_shape(|shape| {
                    crate::geographic::ScalarGeographicView::try_new(frame, shape)
                        .ok()
                        .map(crate::geometry::classify_geographic_validity_chart)
                }) {
                    Ok(Some(route)) => route,
                    Ok(None) => return None,
                    Err(error) => return Some(Err(error.into())),
                };
                if route == crate::geometry::GeographicTopologyRoute::Quotient {
                    // Packed validity/simplicity kernels intentionally know nothing
                    // about frames. Inputs that need the quotient chart take the
                    // shared per-row lane; exact-partition charts stay packed.
                    return None;
                }
            }
        }
    }
    match op {
        Operation::IsEmpty => packed_bool_lane(array, Some(array.is_empty_unary_packed())),
        Operation::IsClosed => packed_bool_lane(array, Some(array.is_closed_unary_packed(domain))),
        Operation::IsRing => packed_bool_lane(array, Some(array.is_ring_unary_packed(domain))),
        Operation::IsCcw => packed_bool_lane(array, Some(array.is_ccw_unary_packed())),
        // is_simple / is_valid kernels are mask-aware (skip placeholders, write
        // false for missing); skip the second mask pass in packed_bool_lane.
        Operation::IsSimple => array.is_simple_unary_packed(),
        Operation::IsValid => array.is_valid_unary_packed(),
        Operation::IsConvex => packed_bool_lane(array, Some(array.is_convex_unary_packed())),
        Operation::CrossesAntimeridian => {
            match crate::geometry::antimeridian_frame(&array.frame, domain, "crosses_antimeridian")
            {
                Ok(frame) => {
                    packed_bool_lane(array, array.crosses_antimeridian_unary_packed(frame))
                },
                Err(error) => Some(Err(error.into())),
            }
        },
        _ => None,
    }
}

/// Packed-column fast path for unary ``bounds``.
pub(crate) fn try_unary_packed_bounds(
    py: Python<'_>,
    array: &PyGeometryArray,
) -> Option<PyResult<Py<PyAny>>> {
    match array.bounds_unary_packed(py) {
        Ok(Some(values)) => Some(bounds_array(py, values)),
        Ok(None) => None,
        Err(error) => Some(Err(error)),
    }
}

/// Try the packed-column array kernel for ``op`` before the generic per-row lane.
///
/// Param-less fast paths key on ``op``; parameterized ones key on the
/// [`PackedUnary`] variant (whose construction is compile-checked).
#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "unmatched operations and parameter pairs decline the packed lane and continue to row dispatch"
)]
pub(crate) fn try_unary_packed_array(
    py: Python<'_>,
    array: &PyGeometryArray,
    op: Operation,
    resolver: super::MetricResolver,
    packed: Option<&PackedUnary>,
) -> Option<PyResult<PyGeometryArray>> {
    // Missing rows carry coordinate placeholders, so kernels that inspect
    // packed storage directly stay on the present-row/scatter lane. `Boundary`
    // preserves packed structure without inspecting missing coordinates;
    // `Segmentize` owns a mask-aware fallback so one generated-work budget
    // spans every present row. `ClipByRect` validates its bounds before its
    // mask-aware detached map, so missing rows do not suppress argument checks.
    // Other constructive ops would need different skip-or-scatter logic and
    // stay bailed here.
    if array.has_missing()
        && !matches!(
            op,
            Operation::Boundary | Operation::ClipByRect | Operation::Metric(MetricOp::Segmentize)
        )
    {
        return None;
    }
    // param-less lanes: the operation alone says everything
    match op {
        Operation::ConvexHull => return Some(array.convex_hull_unary_packed(py)),
        Operation::Boundary => return array.boundary_unary_packed(),
        Operation::Reverse => return Some(array.reverse_unary_packed()),
        Operation::Normalize => return Some(array.normalize_unary_packed(py)),
        Operation::SwapXy => return Some(array.swap_xy_unary_packed()),
        Operation::UniquePoints => return Some(array.unique_points_unary_packed(py)),
        Operation::Force2d => return Some(array.force_2d_unary_packed(py)),
        _ => {},
    }
    match (op, packed?) {
        (Operation::Centroid, PackedUnary::Synthesized) => Some(array.centroid_unary_packed()),
        (Operation::Envelope, PackedUnary::Synthesized) => Some(array.envelope_unary_packed()),
        (Operation::PointOnSurface, PackedUnary::Synthesized) => {
            Some(array.point_on_surface_unary_packed())
        },
        (
            Operation::Metric(MetricOp::Segmentize),
            PackedUnary::Segmentize {
                max_segment_length,
                path,
            },
        ) => {
            // Resolve the ellipsoid and chart-edge placement once, then carry
            // it through the detached packed kernels.
            match segmentize_metric(array, resolver, *path) {
                Ok((placement, to_metre)) => {
                    Some(array.segmentize_unary_packed(max_segment_length, placement, to_metre))
                },
                Err(error) => Some(Err(error)),
            }
        },
        (Operation::Metric(MetricOp::Segmentize), PackedUnary::Densify { fraction, path }) => {
            match segmentize_metric(array, resolver, *path) {
                Ok((placement, _)) => Some(array.densify_unary_packed(fraction, placement)),
                Err(error) => Some(Err(error)),
            }
        },
        (
            Operation::Simplify,
            PackedUnary::Simplify {
                tolerance,
                method,
                preserve_topology,
            },
        ) => array.simplify_unary_packed(tolerance, *method, *preserve_topology),
        (Operation::AffineTransform | Operation::Translate, PackedUnary::Affine { matrix }) => {
            array.affine_transform_unary_packed(*matrix)
        },
        (
            Operation::Rotate,
            PackedUnary::Rotate {
                origin,
                angle_radians,
            },
        ) => array.rotate_unary_packed(*origin, *angle_radians),
        (
            Operation::Scale,
            PackedUnary::Scale {
                origin,
                xfact,
                yfact,
            },
        ) => array.scale_unary_packed(*origin, *xfact, *yfact),
        (
            Operation::Skew,
            PackedUnary::Skew {
                origin,
                tan_x,
                tan_y,
            },
        ) => array.skew_unary_packed(*origin, *tan_x, *tan_y),
        (Operation::OrientPolygons, PackedUnary::OrientPolygons { ccw }) => {
            Some(array.orient_polygons_unary_packed(*ccw))
        },
        (Operation::RemoveRepeatedPoints, PackedUnary::RemoveRepeatedPoints { tolerance }) => {
            Some(array.remove_repeated_points_unary_packed(py, tolerance))
        },
        (
            Operation::SnapToGrid,
            PackedUnary::SnapToGrid {
                size,
                origin,
                repair,
            },
        ) => array.snap_to_grid_unary_packed(py, *size, *origin, *repair),
        (Operation::Force3d, PackedUnary::Force3d { z_fill }) => {
            Some(array.force_3d_unary_packed(py, *z_fill))
        },
        (Operation::SetZ, PackedUnary::SetOrdinate { value, overwrite }) => {
            Some(array.set_z_unary_packed(py, *value, *overwrite))
        },
        (Operation::SetM, PackedUnary::SetOrdinate { value, overwrite }) => {
            Some(array.set_m_unary_packed(py, *value, *overwrite))
        },
        (Operation::Quantize, PackedUnary::Quantize { precision }) => {
            Some(array.quantize_unary_packed(py, *precision))
        },
        (
            Operation::ClipByRect,
            PackedUnary::ClipByRect {
                minx,
                miny,
                maxx,
                maxy,
            },
        ) => {
            let mut scratch = MetricScratch::default();
            let result = match resolver.resolve_topology_ctx(&array.frame, &mut scratch) {
                Ok((domain, metric)) => {
                    let rect = match domain {
                        TopologyDomain::Geographic(_) => {
                            Bounds::new_geographic(*minx, *miny, *maxx, *maxy)
                        },
                        TopologyDomain::Cartesian => Bounds::new(*minx, *miny, *maxx, *maxy),
                    };
                    match rect {
                        Ok(rect) => {
                            let metric_access = || {
                                metric.geographic_chart_metric()?.ok_or_else(|| {
                                    crate::crs::CrsError::invalid(
                                        "geographic rectangle clipping requires the operation's CRS metric",
                                    )
                                    .into()
                                })
                            };
                            array.clip_by_rect_unary_packed(py, rect, domain, &metric_access)
                        },
                        Err(error) => Err(error.into()),
                    }
                },
                Err(error) => Err(error),
            };
            drop(scratch);
            Some(result)
        },
        _ => None,
    }
}

pub(crate) fn try_packed_linear_reference(
    py: Python<'_>,
    array: &PyGeometryArray,
    packed: &PackedLinearReference,
) -> Option<PyResult<PyGeometryArray>> {
    if array.has_missing() {
        return None;
    }
    let GeometryArrayStorage::Packed(PackedStorage::Lines {
        coords,
        offsets,
        row_map,
    }) = array.storage()
    else {
        return None;
    };
    match packed {
        PackedLinearReference::InterpolatePoint {
            distance,
            normalized,
            metric,
        } => Some(
            linref_point_between_packed(
                py,
                Arc::clone(coords),
                offsets.clone(),
                row_map.clone(),
                array.len(),
                metric.clone(),
                distance.clone(),
                *normalized,
            )
            .and_then(|shapes| {
                PyGeometryArray::from_shapes(shapes, array.frame.clone()).map_err(Into::into)
            }),
        ),
        PackedLinearReference::Substring {
            start_distance,
            end_distance,
            normalized,
            metric,
        } => Some(
            linref_substring_packed(
                py,
                Arc::clone(coords),
                offsets.clone(),
                row_map.clone(),
                array.len(),
                metric.clone(),
                start_distance.clone(),
                end_distance.clone(),
                *normalized,
            )
            .and_then(|shapes| {
                PyGeometryArray::from_shapes(shapes, array.frame.clone()).map_err(Into::into)
            }),
        ),
    }
}

fn linref_point_between_packed(
    py: Python<'_>,
    coords: Arc<CoordSeq>,
    offsets: CsrOffsetColumn,
    row_map: RowSelection,
    len: usize,
    model: MetricModel,
    distance: F64Param,
    normalized: bool,
) -> PyResult<Vec<Shape>> {
    // This boundary validation may construct a Python exception; keep it out
    // of the detached kernel below.
    distance.ensure_finite("distance")?;
    py.detach(move || {
        linref_point_between_packed_inner(
            &coords,
            &offsets,
            row_map.as_deref(),
            len,
            &model,
            &distance,
            normalized,
        )
    })
    .map_err(rows_err)
}

fn linref_point_between_packed_inner(
    coords: &CoordSeq,
    offsets: &CsrOffsetColumn,
    row_map: RowSelectionRef<'_>,
    len: usize,
    model: &MetricModel,
    distance: &F64Param,
    normalized: bool,
) -> Result<Vec<Shape>, (usize, crate::error::Error)> {
    // Parameter errors raise on BOTH surfaces: the coordseq row loop below
    // clamps out-of-range stationing by design, so a non-finite distance
    // must be rejected here (the scalar path's kernel owns its own check).
    if let MetricModel::Geodesic { crs, state, .. } = model {
        crate::crs::with_geographic_coordseq_collect_rows_at(crs, state, |metric| {
            (0..len)
                .map(|row| {
                    let line = GeometryArrayStorage::line_view(coords, offsets, row_map, row);
                    let point = crate::crs::chart_line_interpolate_coordseq(
                        &line,
                        distance.get(row),
                        normalized,
                        metric,
                    );
                    degrade_linref_point(point.map(Shape::Point), Lane::Array(row))
                })
                .collect_rows()
        })
    } else {
        (0..len)
            .map(|row| {
                let line = GeometryArrayStorage::line_view(coords, offsets, row_map, row);
                degrade_linref_point(
                    line_interpolate_coordseq(model, &line, distance.get(row), normalized)
                        .map(Shape::Point),
                    Lane::Array(row),
                )
            })
            .collect_rows()
    }
}

fn linref_substring_packed(
    py: Python<'_>,
    coords: Arc<CoordSeq>,
    offsets: CsrOffsetColumn,
    row_map: RowSelection,
    len: usize,
    model: MetricModel,
    start_distance: F64Param,
    end_distance: F64Param,
    normalized: bool,
) -> PyResult<Vec<Shape>> {
    py.detach(move || {
        linref_substring_packed_inner(
            &coords,
            &offsets,
            row_map.as_deref(),
            len,
            &model,
            &start_distance,
            &end_distance,
            normalized,
        )
    })
    .map_err(rows_err)
}

fn linref_substring_packed_inner(
    coords: &CoordSeq,
    offsets: &CsrOffsetColumn,
    row_map: RowSelectionRef<'_>,
    len: usize,
    model: &MetricModel,
    start_distance: &F64Param,
    end_distance: &F64Param,
    normalized: bool,
) -> Result<Vec<Shape>, (usize, crate::error::Error)> {
    if let MetricModel::Geodesic { crs, state, .. } = model {
        crate::crs::with_geographic_coordseq_collect_rows_at(crs, state, |metric| {
            (0..len)
                .map(|row| {
                    let line = GeometryArrayStorage::line_view(coords, offsets, row_map, row);
                    let range = MeasureRange::substring_distance(
                        start_distance.get(row),
                        end_distance.get(row),
                    )?;
                    let substring =
                        crate::crs::chart_line_substring_coordseq(&line, range, normalized, metric);
                    degrade_linref_linestring(substring, Lane::Array(row))
                })
                .collect_rows()
        })
    } else {
        (0..len)
            .map(|row| {
                let line = GeometryArrayStorage::line_view(coords, offsets, row_map, row);
                let range = MeasureRange::substring_distance(
                    start_distance.get(row),
                    end_distance.get(row),
                )?;
                degrade_linref_linestring(
                    line_substring_coordseq(model, &line, range, normalized),
                    Lane::Array(row),
                )
            })
            .collect_rows()
    }
}

/// `segmentize`'s resolved placement and coordinate-unit divisor for the
/// packed lane.
///
/// Must stay the exact counterpart of `kernels::unary_segmentize` — a packed
/// array and a scalar geometry in the same CRS have to subdivide identically.
fn segmentize_metric(
    array: &PyGeometryArray,
    resolver: super::MetricResolver,
    path: Option<NavigationPath>,
) -> PyResult<(crate::geometry::SegmentPlacementOwned, f64)> {
    let super::MetricResolver::Metric { op, unit } = resolver else {
        return kernels::segmentize_placement(None, path).map_err(Into::into);
    };
    let model = crate::broadcast::resolve_metric(array.crs_str(), unit, op)?;
    let metric = crate::crs::ResolvedMetric::from_model(&model)?;
    kernels::segmentize_placement(Some(&metric), path).map_err(Into::into)
}

#[cfg(test)]
mod geographic_mixed_storage_tests {
    use super::*;
    use crate::crs::frame::{Frame, crs_arc_static};
    use crate::geometry::storage::ValidityMask;
    use crate::geometry::{CoordinateAxes, MultiMember, MultiPolygon, Point, Polygon, Ring};
    use crate::predicates::unary::{is_ring_array, is_simple_array, is_valid_array};
    use crate::py::functions::validation::require;

    fn ring(points: &[(f64, f64)]) -> Ring {
        Ring::closed(
            points
                .iter()
                .map(|&(x, y)| Point::new_raw_xy(x, y))
                .collect(),
        )
        .expect("test ring is closed")
    }

    fn polygon(points: &[(f64, f64)]) -> Polygon {
        Polygon::new(ring(points), Vec::new())
    }

    fn seam_polygon() -> Shape {
        Shape::Polygon(polygon(&[
            (170.0, -10.0),
            (-170.0, -10.0),
            (-170.0, 10.0),
            (170.0, 10.0),
            (170.0, -10.0),
        ]))
    }

    fn multi_polygon(north: f64) -> Shape {
        Shape::MultiPolygon(
            MultiPolygon::try_new(CoordinateAxes::XY, vec![MultiMember::Value(polygon(&[
                (0.0, 0.0),
                (1.0, 0.0),
                (1.0, north),
                (0.0, north),
                (0.0, 0.0),
            ]))])
            .expect("one XY polygon forms a MultiPolygon"),
        )
    }

    fn array(shapes: Vec<Shape>) -> PyGeometryArray {
        let array = array_from_shapes(shapes);
        assert!(
            matches!(array.storage(), GeometryArrayStorage::Mixed(_)),
            "Polygon and MultiPolygon rows require mixed storage",
        );
        array
    }

    fn packed_array(shapes: Vec<Shape>) -> PyGeometryArray {
        let array = array_from_shapes(shapes);
        assert!(
            matches!(array.storage(), GeometryArrayStorage::Packed(_)),
            "homogeneous polygon rows require packed storage",
        );
        array
    }

    fn array_from_shapes(shapes: Vec<Shape>) -> PyGeometryArray {
        let frame = Frame::new(Some(crs_arc_static("EPSG:4326")), None)
            .expect("EPSG:4326 is a valid geographic frame");
        PyGeometryArray::from_shapes(shapes, frame).unwrap()
    }

    fn valid_polygon(west: f64, north: f64) -> Shape {
        Shape::Polygon(polygon(&[
            (west, 30.0),
            (west + 1.0, 30.0),
            (west + 1.0, north),
            (west, north),
            (west, 30.0),
        ]))
    }

    fn ambiguous_polygon() -> Shape {
        Shape::Polygon(polygon(&[
            (0.0, -10.0),
            (360.0, -10.0),
            (300.0, 10.0),
            (0.0, -10.0),
        ]))
    }

    fn bool_values(py: Python<'_>, result: PyResult<Py<PyAny>>) -> Vec<bool> {
        result
            .expect("boolean array operation succeeds")
            .bind(py)
            .extract::<Vec<bool>>()
            .expect("boolean array result extracts")
    }

    fn require_error_row(py: Python<'_>, array: &PyGeometryArray, expected_row: usize) {
        let argument = Bound::new(py, array.clone())
            .expect("mixed array binds for the public require boundary")
            .into_any();
        let error =
            require(py, &argument, None, None).expect_err("require returns the first invalid row");
        let value = error.value(py);
        assert_eq!(
            value
                .getattr("row")
                .expect("invalid-geometry error has a row")
                .extract::<usize>()
                .expect("row is an integer"),
            expected_row,
        );
        assert_eq!(
            value
                .getattr("code")
                .expect("invalid-geometry error has a code")
                .extract::<String>()
                .expect("code is text"),
            "latitude_out_of_range",
        );
    }

    fn assert_error_note_row(py: Python<'_>, error: &PyErr, expected_row: usize) {
        let notes = error
            .value(py)
            .getattr("__notes__")
            .expect("row fallback annotates its first error")
            .extract::<Vec<String>>()
            .expect("array error notes are strings");
        assert_eq!(notes, [format!(
            "while processing array element {expected_row}"
        )]);
    }

    #[test]
    fn mixed_geographic_predicates_keep_values_with_seam_row_at_either_end() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            for shapes in [vec![seam_polygon(), multi_polygon(1.0)], vec![
                multi_polygon(1.0),
                seam_polygon(),
            ]] {
                let array = array(shapes);
                assert!(
                    packed_bool_values(&array, Operation::IsValid).is_none(),
                    "mixed storage cannot use the packed validity kernel",
                );
                assert!(
                    packed_bool_values(&array, Operation::IsSimple).is_none(),
                    "mixed storage cannot use the packed simplicity kernel",
                );
                assert!(
                    packed_bool_values(&array, Operation::IsRing).is_none(),
                    "mixed storage uses the ordered row lane for ring status",
                );
                assert_eq!(bool_values(py, is_valid_array(py, &array)), [true, true],);
                assert_eq!(bool_values(py, is_simple_array(py, &array)), [true, true],);
                assert_eq!(bool_values(py, is_ring_array(py, &array)), [false, false],);
            }
        });
    }

    #[test]
    fn mixed_geographic_preflight_keeps_first_error_before_or_after_seam_row() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            for (shapes, expected_row, expected_validity) in [
                (vec![seam_polygon(), multi_polygon(91.0)], 1, [true, false]),
                (vec![multi_polygon(91.0), seam_polygon()], 0, [false, true]),
            ] {
                let array = array(shapes);
                assert!(
                    packed_bool_values(&array, Operation::IsValid).is_none(),
                    "mixed storage goes directly to the ordered row lane",
                );
                assert_eq!(
                    bool_values(py, is_valid_array(py, &array)),
                    expected_validity,
                );

                let reports = array
                    .validate_impl(py)
                    .expect("validation reports both rows");
                let issue_codes = reports
                    .iter()
                    .map(|report| {
                        report
                            .as_ref()
                            .and_then(|report| report.issue.as_ref())
                            .map(|issue| issue.code)
                    })
                    .collect::<Vec<_>>();
                let expected_codes = if expected_row == 0 {
                    [Some("latitude_out_of_range"), None]
                } else {
                    [None, Some("latitude_out_of_range")]
                };
                assert_eq!(issue_codes, expected_codes);

                require_error_row(py, &array, expected_row);
                let error =
                    is_simple_array(py, &array).expect_err("is_simple reports the invalid row");
                assert_error_note_row(py, &error, expected_row);
                let error = is_ring_array(py, &array).expect_err("is_ring reports the invalid row");
                assert_error_note_row(py, &error, expected_row);
            }
        });
    }

    #[test]
    fn packed_ambiguous_ring_declines_the_column_and_reaches_row_validity() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let array = packed_array(vec![
                valid_polygon(0.0, 31.0),
                ambiguous_polygon(),
                valid_polygon(4.0, 31.0),
            ]);
            assert!(
                packed_bool_values(&array, Operation::IsValid).is_none(),
                "the explicit-period row declines the ordinary column certificate",
            );
            assert_eq!(bool_values(py, is_valid_array(py, &array)), [
                true, false, true
            ],);
        });
    }

    #[test]
    fn packed_geographic_fallback_preserves_nullable_gathered_error_order() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let source = packed_array(vec![
                valid_polygon(0.0, 31.0),
                valid_polygon(2.0, 91.0),
                valid_polygon(4.0, 31.0),
                ambiguous_polygon(),
            ]);
            let gathered = source
                .gather_logical_rows(&[2, 0, 2])
                .expect("ordinary gathered rows preserve their selection");
            assert!(
                packed_bool_values(&gathered, Operation::IsValid).is_some(),
                "the screen admits only selected present rows",
            );
            assert_eq!(bool_values(py, is_valid_array(py, &gathered)), [
                true, true, true
            ],);

            let window = source
                .gather_logical_row_range(2..3)
                .expect("ordinary windows preserve their source range");
            assert!(
                packed_bool_values(&window, Operation::IsValid).is_some(),
                "the window certificate ignores unselected rows",
            );
            let masked = source
                .clone()
                .with_missing_mask(Some(
                    ValidityMask::from_vec(4, vec![false, true, false, true])
                        .expect("row-aligned mask"),
                ))
                .expect("invalid hidden rows can be masked");
            assert!(
                packed_bool_values(&masked, Operation::IsValid).is_some(),
                "missing payload rows are never admitted",
            );
            assert_eq!(bool_values(py, is_valid_array(py, &masked)), [
                true, false, true, false
            ],);

            let reordered = source
                .gather_logical_rows(&[3, 1, 0])
                .expect("gather retains selected logical order");
            assert!(
                packed_bool_values(&reordered, Operation::IsValid).is_none(),
                "a failed column certificate returns to ordered row preflight",
            );
            assert_eq!(bool_values(py, is_valid_array(py, &reordered)), [
                false, false, true
            ],);
            for operation in [is_simple_array, is_ring_array] {
                let error = operation(py, &reordered).expect_err("invalid row is reported");
                assert_error_note_row(py, &error, 0);
            }
            let argument = Bound::new(py, reordered)
                .expect("gathered packed array binds")
                .into_any();
            let error =
                require(py, &argument, None, None).expect_err("require returns first error");
            assert_eq!(
                error
                    .value(py)
                    .getattr("row")
                    .expect("validation error includes logical row")
                    .extract::<usize>()
                    .expect("row is integer"),
                0,
            );

            let latitude_first = source
                .gather_logical_rows(&[1, 3, 0])
                .expect("gather keeps selected order");
            let argument = Bound::new(py, latitude_first)
                .expect("latitude-first array binds")
                .into_any();
            let error = require(py, &argument, None, None)
                .expect_err("the earliest latitude error wins over a later ambiguous ring");
            assert_eq!(
                error
                    .value(py)
                    .getattr("code")
                    .expect("validation error includes issue code")
                    .extract::<String>()
                    .expect("issue code is text"),
                "latitude_out_of_range",
            );
        });
    }
}
