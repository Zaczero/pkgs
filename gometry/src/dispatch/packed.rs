use std::sync::Arc;

use pyo3::prelude::*;

use crate::array::{GeometryArrayStorage, RowSelection, RowSelectionRef};
use crate::boundary::input::OriginSpec;
use crate::broadcast::{
    CollectRows as _, degrade_linref_linestring, degrade_linref_point_between, rows_err,
};
use crate::crs::MetricModel;
use crate::dispatch::operation::Operation;
use crate::geometry::{
    Bounds, CoordSeq, CsrOffsetColumn, MeasureRange, Shape, SimplifyMethod, is_geographic_frame,
};
use crate::py::numpy::{bool_array, bounds_array, float64_array};
use crate::py::support::require_antimeridian_crs;
use crate::{
    DistanceUnit, F64Param, PyGeometryArray, line_interpolate_coordseq, line_substring_coordseq,
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
    },
    Densify {
        fraction: F64Param,
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
        rect: Bounds,
    },
    LineInterpolatePoint {
        distance: F64Param,
        normalized: bool,
        metric: MetricModel,
    },
    LineSubstring {
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
                    if *missing {
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
pub(crate) fn try_unary_packed_f64(
    py: Python<'_>,
    array: &PyGeometryArray,
    op: Operation,
    unit: Option<DistanceUnit>,
    op_name: &str,
) -> Option<PyResult<Py<PyAny>>> {
    match op {
        Operation::Area => packed_f64_lane(py, array, array.area_unary_packed(py, unit, op_name)),
        Operation::Length => {
            packed_f64_lane(py, array, array.length_unary_packed(py, unit, op_name))
        },
        Operation::Length3d => {
            packed_f64_lane(py, array, array.length_3d_unary_packed(py, unit, op_name))
        },
        _ => None,
    }
}

fn packed_bool_lane(
    py: Python<'_>,
    array: &PyGeometryArray,
    values: Option<Vec<bool>>,
) -> Option<PyResult<Py<PyAny>>> {
    let mut values = values?;
    // Belt-and-braces: kernels that already skip missing rows still get the
    // shape-lane missing sentinel (`false`) applied here. Cheap when the
    // kernel already wrote false for masked slots.
    if let Some(mask) = array.missing() {
        for (value, missing) in values.iter_mut().zip(mask.iter()) {
            if *missing {
                *value = false;
            }
        }
    }
    Some(bool_array(py, values))
}

/// Packed-column fast paths for unary validation predicates returning ``bool`` ndarrays.
pub(crate) fn try_unary_packed_bool(
    py: Python<'_>,
    array: &PyGeometryArray,
    op: Operation,
) -> Option<PyResult<Py<PyAny>>> {
    // Packed bool kernels run over physical rows (including NaN placeholders
    // on missing slots). [`packed_bool_lane`] then writes the missing-row
    // sentinel (`false`) from the mask — identical to the shape-lane answer
    // for masked rows. No blanket `has_missing()` bail: that forced a 10–500×
    // shape-lane cliff on nullable is_valid/is_simple.
    if matches!(
        op,
        Operation::IsRing | Operation::IsSimple | Operation::IsValid
    ) && is_geographic_frame(&array.frame)
        && array
            .storage()
            .iter_shapes()
            .any(|shape| shape.crosses_antimeridian())
    {
        // Packed validity/simplicity kernels intentionally know nothing about
        // frames. A crossing geographic column takes the shared per-row lane;
        // ordinary geographic columns keep the packed fast path.
        return None;
    }
    match op {
        Operation::IsEmpty => packed_bool_lane(py, array, Some(array.is_empty_unary_packed())),
        Operation::IsClosed => packed_bool_lane(py, array, Some(array.is_closed_unary_packed())),
        Operation::IsRing => packed_bool_lane(py, array, Some(array.is_ring_unary_packed())),
        Operation::IsCcw => packed_bool_lane(py, array, Some(array.is_ccw_unary_packed())),
        // is_simple / is_valid kernels are mask-aware (skip placeholders, write
        // false for missing); skip the second mask pass in packed_bool_lane.
        Operation::IsSimple => array
            .is_simple_unary_packed()
            .map(|values| bool_array(py, values)),
        Operation::IsValid => array
            .is_valid_unary_packed()
            .map(|values| bool_array(py, values)),
        Operation::IsConvex => packed_bool_lane(py, array, Some(array.is_convex_unary_packed())),
        Operation::CrossesAntimeridian => match require_antimeridian_crs(array.crs_str()) {
            Ok(()) => packed_bool_lane(py, array, array.crosses_antimeridian_unary_packed()),
            Err(error) => Some(Err(error)),
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
        Ok(Some(mut values)) => {
            if let Some(mask) = array.missing() {
                for (row, missing) in mask.iter().enumerate() {
                    if *missing {
                        values[row * 4..row * 4 + 4].fill(f64::NAN);
                    }
                }
            }
            Some(bounds_array(py, values))
        },
        Ok(None) => None,
        Err(error) => Some(Err(error)),
    }
}

/// Try the packed-column array kernel for ``op`` before the generic per-row lane.
///
/// Param-less fast paths key on ``op``; parameterized ones key on the
/// [`PackedUnary`] variant (whose construction is compile-checked).
#[expect(
    clippy::too_many_lines,
    reason = "one match arm per unary packed fast path"
)]
pub(crate) fn try_unary_packed_array(
    py: Python<'_>,
    array: &PyGeometryArray,
    op: Operation,
    resolver: super::MetricResolver,
    packed: Option<&PackedUnary>,
) -> Option<PyResult<PyGeometryArray>> {
    // Most constructive packed kernels consume every physical coordinate.
    // Missing rows deliberately carry NaN placeholders; ops that would feed
    // those NaNs into geometry construction stay on the present-row/scatter
    // lane. `Boundary` preserves packed structure without inspecting missing
    // coordinates; `Segmentize` owns its own mask-aware fallback so one
    // generated-work budget spans every present row. Other
    // constructive ops (affine, simplify, centroid, …) would need per-op
    // skip-or-scatter logic — not the same shape as bool mask overlay — and
    // stay bailed here.
    if array.has_missing() && !matches!(op, Operation::Boundary | Operation::Segmentize) {
        return None;
    }
    // param-less lanes: the operation alone says everything
    match op {
        Operation::ConvexHull => return Some(array.convex_hull_unary_packed(py)),
        Operation::Boundary => return Some(Ok(array.boundary_unary_packed())),
        Operation::Reverse => return Some(Ok(array.reverse_unary_packed())),
        Operation::Normalize => return Some(Ok(array.normalize_unary_packed(py))),
        Operation::SwapXy => return Some(Ok(array.swap_xy_unary_packed())),
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
        (Operation::Segmentize, PackedUnary::Segmentize { max_segment_length }) => {
            // `segmentize` measures and places under the array's metric, so
            // the ellipsoid is resolved ONCE here and cloned into the kernels
            // rather than re-derived per row.
            match segmentize_metric(array, resolver) {
                Ok((geodesic, to_metre)) => {
                    Some(array.segmentize_unary_packed(max_segment_length, geodesic, to_metre))
                },
                Err(error) => Some(Err(error)),
            }
        },
        (Operation::Segmentize, PackedUnary::Densify { fraction }) => {
            Some(array.densify_unary_packed(fraction))
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
            Some(Ok(array.orient_polygons_unary_packed(*ccw)))
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
        (Operation::ClipByRect, PackedUnary::ClipByRect { rect }) => {
            Some(array.clip_by_rect_unary_packed(*rect, false))
        },
        (
            Operation::LineInterpolate,
            PackedUnary::LineInterpolatePoint {
                distance,
                normalized,
                metric,
            },
        ) => {
            let GeometryArrayStorage::Lines {
                coords,
                offsets,
                row_map,
            } = array.storage()
            else {
                return None;
            };
            Some(
                linref_point_between_packed(
                    py,
                    Arc::clone(coords),
                    offsets.clone(),
                    row_map.clone(),
                    array.storage().len(),
                    metric.clone(),
                    distance.clone(),
                    *normalized,
                )
                .map(|shapes| PyGeometryArray::from_shapes(shapes, array.frame.clone())),
            )
        },
        (
            Operation::LineSubstring,
            PackedUnary::LineSubstring {
                start_distance,
                end_distance,
                normalized,
                metric,
            },
        ) => {
            let GeometryArrayStorage::Lines {
                coords,
                offsets,
                row_map,
            } = array.storage()
            else {
                return None;
            };
            Some(
                linref_substring_packed(
                    py,
                    Arc::clone(coords),
                    offsets.clone(),
                    row_map.clone(),
                    array.storage().len(),
                    metric.clone(),
                    start_distance.clone(),
                    end_distance.clone(),
                    *normalized,
                )
                .map(|shapes| PyGeometryArray::from_shapes(shapes, array.frame.clone())),
            )
        },
        _ => None,
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
    if let MetricModel::Geodesic(crs) = model {
        crate::crs::with_geodesic_coordseq_collect_rows(crs, |metric| {
            (0..len)
                .map(|row| {
                    let line = GeometryArrayStorage::line_view(coords, offsets, row_map, row);
                    let point = crate::crs::geodesic_line_interpolate_coordseq(
                        &line,
                        distance.get(row),
                        normalized,
                        metric,
                    );
                    degrade_linref_point_between(point)
                })
                .collect_rows()
        })
    } else {
        (0..len)
            .map(|row| {
                let line = GeometryArrayStorage::line_view(coords, offsets, row_map, row);
                degrade_linref_point_between(line_interpolate_coordseq(
                    model,
                    &line,
                    distance.get(row),
                    normalized,
                ))
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
    if let MetricModel::Geodesic(crs) = model {
        crate::crs::with_geodesic_coordseq_collect_rows(crs, |metric| {
            (0..len)
                .map(|row| {
                    let line = GeometryArrayStorage::line_view(coords, offsets, row_map, row);
                    let range = MeasureRange::substring_distance(
                        start_distance.get(row),
                        end_distance.get(row),
                    )?;
                    let substring = crate::crs::geodesic_line_substring_coordseq(
                        &line, range, normalized, metric,
                    );
                    degrade_linref_linestring(substring)
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
                degrade_linref_linestring(line_substring_coordseq(model, &line, range, normalized))
            })
            .collect_rows()
    }
}

/// `segmentize`'s resolved metric for the packed lane: the ellipsoid to place
/// along (`None` = planar), and the coordinate-units-per-input-unit divisor.
///
/// Must stay the exact counterpart of `kernels::unary_segmentize` — a packed
/// array and a scalar geometry in the same CRS have to subdivide identically.
fn segmentize_metric(
    array: &PyGeometryArray,
    resolver: super::MetricResolver,
) -> PyResult<(Option<geographiclib_rs::Geodesic>, f64)> {
    let super::MetricResolver::Metric { unit } = resolver else {
        return Ok((None, 1.0));
    };
    let model = crate::broadcast::resolve_metric(array.crs_str(), unit, "segmentize")?;
    match crate::crs::ResolvedMetric::from_model(&model)? {
        crate::crs::ResolvedMetric::Geodesic { geodesic, .. } => Ok((Some(*geodesic), 1.0)),
        crate::crs::ResolvedMetric::Planar { to_metre } => Ok((None, to_metre.get())),
    }
}
