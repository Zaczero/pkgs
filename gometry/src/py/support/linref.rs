#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::py::support::{
    Bound, CoordSeq, DistanceUnit, GeometryError, LineIndex, PlanarMetric, Point, PyAny,
    PyGeometry, PyResult, Result, Shape, ShapeData, coordinate_values, require_locate_point,
    resolve_metric,
};

/// Resolve the metric for a line-LRS op. `normalized` distances are fractions
/// of total length, so they are unit-independent — pairing `unit=` with
/// `normalized=True` is a contradiction and is rejected.
pub(crate) fn resolve_line_metric(
    crs: Option<&str>,
    unit: Option<DistanceUnit>,
    normalized: bool,
    op: &str,
) -> PyResult<crate::crs::MetricModel> {
    if normalized && unit.is_some() {
        return Err(GeometryError::new_err(format!(
            "{op}: unit does not apply to normalized distances (they are fractions of \
             total length); drop unit= or set normalized=False"
        )));
    }
    resolve_metric(crs, unit, op)
}

/// A parsed `line_interpolate_points` sampling plan: `count` linspace
/// fractions (endpoints included from two samples up), or explicit
/// CRS-aware distances.
pub(crate) enum InterpolatePlan {
    Count(usize),
    Distances(Vec<f64>, crate::crs::DistanceMode),
}

impl InterpolatePlan {
    /// Whether the plan's distances are fractions of total length (a `count`
    /// linspace, or explicit `normalized` distances) — so a `unit=` override
    /// is meaningless and rejected.
    pub(crate) fn is_fractional(&self) -> bool {
        match self {
            Self::Count(_) => true,
            Self::Distances(_, mode) => mode.is_normalized(),
        }
    }
}

pub(crate) fn parse_interpolate_plan(
    count: Option<&Bound<'_, PyAny>>,
    distances: Option<&Bound<'_, PyAny>>,
    normalized: bool,
) -> PyResult<InterpolatePlan> {
    match (count, distances) {
        (Some(count), None) => {
            if normalized {
                return Err(GeometryError::new_err(
                    "normalized applies to at; count samples fractions already",
                ));
            }
            let count = crate::boundary::input::positive_int(
                "line_interpolate_points",
                "count",
                crate::boundary::input::py_i64_required("count", count)?,
            )?;
            let count = usize::try_from(count).expect("count >= 1 fits usize");
            crate::geometry::ExpansionBudget::check("line_interpolate_points", "count", count)?;
            Ok(InterpolatePlan::Count(count))
        },
        (None, Some(distances)) => {
            let values = coordinate_values(distances.py(), distances, "at")?;
            if !crate::geometry::column_all_finite(&values) {
                return Err(GeometryError::new_err("at must be finite"));
            }
            Ok(InterpolatePlan::Distances(
                values,
                crate::crs::DistanceMode::of(normalized),
            ))
        },
        _ => Err(GeometryError::new_err(
            "line_interpolate requires exactly one of at or count",
        )),
    }
}

/// Packed-line column kernel for `line_interpolate_points`: one
/// [`LineIndex::build_coordseq`] per row, many samples — no `ShapeData`
/// wrapper.
pub(crate) fn line_interpolate_points_coordseq(
    model: &crate::crs::MetricModel,
    line: &CoordSeq,
    plan: &InterpolatePlan,
) -> Result<Vec<Point>, crate::error::Error> {
    let crate::crs::MetricModel::Planar { to_metre } = model else {
        unreachable!("packed column path is planar-only");
    };
    let index = LineIndex::build_coordseq(line, &PlanarMetric)?;
    match plan {
        InterpolatePlan::Count(count) => (0..*count)
            .map(|sample| {
                let fraction = if *count > 1 {
                    sample as f64 / (*count - 1) as f64
                } else {
                    0.0
                };
                Ok(index.interpolate(fraction, true, &PlanarMetric))
            })
            .collect(),
        InterpolatePlan::Distances(distances, mode) => distances
            .iter()
            .map(|&distance| {
                Ok(index.interpolate(
                    mode.planar_along(distance, to_metre.get()),
                    mode.is_normalized(),
                    &PlanarMetric,
                ))
            })
            .collect(),
    }
}

/// The `line_interpolate_points` row kernel: one cached-index line handle,
/// many samples.
pub(crate) fn line_interpolate_points_shape(
    model: &crate::crs::MetricModel,
    line: &ShapeData,
    frame_cache: &crate::geometry::FrameDependentCaches,
    plan: &InterpolatePlan,
) -> Result<Vec<Shape>, crate::error::Error> {
    match plan {
        InterpolatePlan::Count(count) => (0..*count)
            .map(|index| {
                let fraction = if *count > 1 {
                    index as f64 / (*count - 1) as f64
                } else {
                    0.0
                };
                line_interpolate_shape(model, line, frame_cache, fraction, true)
            })
            .collect(),
        InterpolatePlan::Distances(distances, mode) => distances
            .iter()
            .map(|&distance| {
                line_interpolate_shape_with_mode(model, line, frame_cache, distance, *mode)
            })
            .collect(),
    }
}

/// The `line_interpolate_point` row kernel (model resolved once per call by
/// the surfaces; a persistent `line` handle keeps its cached `LineIndex`).
pub(crate) fn line_interpolate_shape(
    model: &crate::crs::MetricModel,
    line: &ShapeData,
    frame_cache: &crate::geometry::FrameDependentCaches,
    distance: f64,
    normalized: bool,
) -> Result<Shape> {
    line_interpolate_shape_with_mode(
        model,
        line,
        frame_cache,
        distance,
        crate::crs::DistanceMode::of(normalized),
    )
}

fn line_interpolate_shape_with_mode(
    model: &crate::crs::MetricModel,
    line: &ShapeData,
    frame_cache: &crate::geometry::FrameDependentCaches,
    distance: f64,
    distance_mode: crate::crs::DistanceMode,
) -> Result<Shape> {
    Ok(match model {
        crate::crs::MetricModel::Planar { to_metre } => line.line_interpolate_point(
            distance_mode.planar_along(distance, to_metre.get()),
            distance_mode.is_normalized(),
        )?,
        crate::crs::MetricModel::Geodesic(crs) => {
            crate::crs::geodesic_line_interpolate(crs, line, frame_cache, distance, distance_mode)?
        },
    })
}

/// The `line_substring` row kernel (see [`line_interpolate_shape`]).
pub(crate) fn line_substring_shape(
    model: &crate::crs::MetricModel,
    line: &ShapeData,
    frame_cache: &crate::geometry::FrameDependentCaches,
    range: crate::geometry::MeasureRange,
    normalized: bool,
) -> Result<Shape> {
    let distance_mode = crate::crs::DistanceMode::of(normalized);
    Ok(match model {
        crate::crs::MetricModel::Planar { to_metre } => line.line_substring(
            crate::geometry::MeasureRange::substring_distance(
                distance_mode.planar_along(range.start(), to_metre.get()),
                distance_mode.planar_along(range.end(), to_metre.get()),
            )?,
            distance_mode.is_normalized(),
        )?,
        crate::crs::MetricModel::Geodesic(crs) => {
            crate::crs::geodesic_line_substring(crs, line, frame_cache, range, distance_mode)?
        },
    })
}

/// CRS-aware `line_locate_point`: returns meters (or a `[0, 1]` fraction) along
/// the line for the projection of `point`, consistent with `length()`.
pub(crate) fn crs_line_locate_point(
    line: &PyGeometry,
    point: &PyGeometry,
    normalized: bool,
    unit: Option<DistanceUnit>,
) -> PyResult<f64> {
    let query = require_locate_point(point, "line_locate_point")?;
    let model = resolve_line_metric(line.crs_str(), unit, normalized, "line_locate_point")?;
    Ok(line_locate_shape(
        &model,
        &line.shape,
        &line.frame_cache,
        query,
        normalized,
    )?)
}

/// Packed-line column kernel for `line_interpolate_point`: one
/// [`LineIndex::build_coordseq`] per row — no `ShapeData` wrapper.
pub(crate) fn line_interpolate_coordseq(
    model: &crate::crs::MetricModel,
    line: &CoordSeq,
    distance: f64,
    normalized: bool,
) -> Result<Point, crate::error::Error> {
    let distance_mode = crate::crs::DistanceMode::of(normalized);
    match model {
        crate::crs::MetricModel::Planar { to_metre } => {
            let index = LineIndex::build_coordseq(line, &PlanarMetric)?;
            Ok(index.interpolate(
                distance_mode.planar_along(distance, to_metre.get()),
                distance_mode.is_normalized(),
                &PlanarMetric,
            ))
        },
        crate::crs::MetricModel::Geodesic(crs) => {
            crate::crs::with_geodesic_coordseq_metric(crs, |metric| {
                crate::crs::geodesic_line_interpolate_coordseq(
                    line,
                    distance,
                    distance_mode,
                    metric,
                )
            })
        },
    }
}

/// Packed-line column kernel for `line_substring`: one
/// [`LineIndex::build_coordseq`] per row — no `ShapeData` wrapper.
pub(crate) fn line_substring_coordseq(
    model: &crate::crs::MetricModel,
    line: &CoordSeq,
    range: crate::geometry::MeasureRange,
    normalized: bool,
) -> Result<Shape, crate::error::Error> {
    let distance_mode = crate::crs::DistanceMode::of(normalized);
    match model {
        crate::crs::MetricModel::Planar { to_metre } => {
            let index = LineIndex::build_coordseq(line, &PlanarMetric)?;
            index.substring(
                crate::geometry::MeasureRange::substring_distance(
                    distance_mode.planar_along(range.start(), to_metre.get()),
                    distance_mode.planar_along(range.end(), to_metre.get()),
                )?,
                distance_mode.is_normalized(),
                &PlanarMetric,
            )
        },
        crate::crs::MetricModel::Geodesic(crs) => {
            crate::crs::with_geodesic_coordseq_metric(crs, |metric| {
                crate::crs::geodesic_line_substring_coordseq(line, range, distance_mode, metric)
            })
        },
    }
}

/// Packed-line column kernel for `line_locate_point`: one
/// [`LineIndex::build_coordseq`] per row — no `ShapeData` wrapper.
pub(crate) fn line_locate_coordseq(
    model: &crate::crs::MetricModel,
    line: &CoordSeq,
    query: Point,
    normalized: bool,
) -> Result<f64, crate::error::Error> {
    let distance_mode = crate::crs::DistanceMode::of(normalized);
    match model {
        crate::crs::MetricModel::Planar { to_metre } => {
            let index = LineIndex::build_coordseq(line, &PlanarMetric)?;
            let located = index.locate_point(query, distance_mode.is_normalized(), &PlanarMetric);
            Ok(if distance_mode.is_normalized() {
                located
            } else {
                located * to_metre.get()
            })
        },
        crate::crs::MetricModel::Geodesic(crs) => {
            crate::crs::with_geodesic_coordseq_metric(crs, |metric| {
                crate::crs::geodesic_line_locate_coordseq(line, query, distance_mode, metric)
            })
        },
    }
}

/// The `line_locate_point` row kernel: the metric model is resolved once per
/// call by the surfaces, and a persistent `line` handle keeps its cached
/// `LineIndex` across rows and calls.
pub(crate) fn line_locate_shape(
    model: &crate::crs::MetricModel,
    line: &ShapeData,
    frame_cache: &crate::geometry::FrameDependentCaches,
    query: Point,
    normalized: bool,
) -> Result<f64> {
    let distance_mode = crate::crs::DistanceMode::of(normalized);
    Ok(match model {
        crate::crs::MetricModel::Planar { to_metre } => {
            let located = line.line_locate_point(query, distance_mode.is_normalized())?;
            if distance_mode.is_normalized() {
                located
            } else {
                located * to_metre.get()
            }
        },
        crate::crs::MetricModel::Geodesic(crs) => {
            crate::crs::geodesic_line_locate(crs, line, frame_cache, query, distance_mode)?
        },
    })
}
