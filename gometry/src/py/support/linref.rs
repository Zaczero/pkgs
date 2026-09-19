use crate::boundary::{FloatInput, input};
use crate::dispatch::MetricOp;
use crate::geometry::PreparedPlan;
use crate::py::support::{
    Bound, CoordSeq, DistanceUnit, GeometryError, LineIndex, PlanarMetric, Point, PyAny, PyResult,
    Result, Shape, float_values, resolve_metric,
};

pub(crate) fn line_interpolate_shape_m(
    model: &crate::crs::MetricModel,
    line: &PreparedPlan,
    m: f64,
) -> Result<Shape> {
    match model {
        crate::crs::MetricModel::Planar { .. } => {
            line.line_interpolate_point_m_with(m, &PlanarMetric)
        },
        crate::crs::MetricModel::Geodesic {
            crs, frame, state, ..
        } => {
            crate::crs::ensure_geographic_domain_in_frame(*frame, line.shape())?;
            crate::crs::with_ellipsoid_metric_at(crs, &[line.shape()], state, |metric| {
                line.line_interpolate_point_m_with(m, metric)
            })
        },
    }
}

pub(crate) fn line_locate_shape_m(
    model: &crate::crs::MetricModel,
    line: &PreparedPlan,
    query: Point,
) -> Result<f64> {
    match model {
        crate::crs::MetricModel::Planar { .. } => {
            line.line_locate_point_m_with(query, &PlanarMetric)
        },
        crate::crs::MetricModel::Geodesic { crs, frame, state } => {
            crate::crs::ensure_geographic_domain_in_frame(*frame, line.shape())?;
            if !query.x.is_nan() {
                crate::crs::ensure_geographic_lonlat_in_frame(*frame, query.x, query.y)?;
            }
            crate::crs::with_ellipsoid_metric_at(crs, &[line.shape()], state, |metric| {
                line.line_locate_point_m_with(query, metric)
            })
        },
    }
}

pub(crate) fn line_substring_shape_m(
    model: &crate::crs::MetricModel,
    line: &PreparedPlan,
    range: crate::geometry::MeasureRange,
) -> Result<Shape> {
    match model {
        crate::crs::MetricModel::Planar { .. } => line.line_substring_m_with(range, &PlanarMetric),
        crate::crs::MetricModel::Geodesic {
            crs, frame, state, ..
        } => {
            crate::crs::ensure_geographic_domain_in_frame(*frame, line.shape())?;
            crate::crs::with_ellipsoid_metric_at(crs, &[line.shape()], state, |metric| {
                line.line_substring_m_with(range, metric)
            })
        },
    }
}

/// Resolve the metric for a line-LRS op. `normalized` distances are fractions
/// of total length, so they are unit-independent — pairing `unit=` with
/// `normalized=True` is a contradiction and is rejected.
pub(crate) fn resolve_line_metric(
    crs: Option<&str>,
    unit: Option<DistanceUnit>,
    normalized: bool,
    op: MetricOp,
) -> PyResult<crate::crs::MetricModel> {
    let op_name = op.name();
    if normalized && unit.is_some() {
        return Err(GeometryError::new_err(format!(
            "{op_name}: unit does not apply to normalized distances (they are fractions of \
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

    const fn sample_count(&self) -> usize {
        match self {
            Self::Count(count) => *count,
            Self::Distances(distances, _) => distances.len(),
        }
    }

    /// The fraction of sample `index` in a `count`-sample linspace: both
    /// endpoints from two samples up, the start alone for one.
    fn count_fraction(index: usize, count: usize) -> f64 {
        if count > 1 {
            index as f64 / (count - 1) as f64
        } else {
            0.0
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
            let count = input::positive_int(
                "line_interpolate",
                "count",
                input::py_i64_required("count", count)?,
            )?;
            let count = usize::try_from(count).expect("count >= 1 fits usize");
            crate::geometry::ExpansionBudget::check("line_interpolate", "count", count)?;
            Ok(InterpolatePlan::Count(count))
        },
        (None, Some(distances)) => {
            let values = float_values(distances.py(), distances, "at", FloatInput::Parameter)?;
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

/// Packed-line column kernel for many `line_interpolate` samples: one
/// [`LineIndex::build_coordseq`] per row, no `ShapeData` wrapper. Empty
/// linework answers every sample with an empty point.
pub(crate) fn line_interpolate_points_coordseq(
    model: &crate::crs::MetricModel,
    line: &CoordSeq,
    plan: &InterpolatePlan,
) -> Result<Vec<Shape>, crate::error::Error> {
    let crate::crs::MetricModel::Planar { to_metre } = model else {
        unreachable!("packed column path is planar-only");
    };
    let mut points = Vec::new();
    crate::boundary::try_reserve_vec(&mut points, plan.sample_count())?;
    if line.is_empty() {
        for _ in 0..plan.sample_count() {
            crate::boundary::try_push_vec(&mut points, Shape::empty_point())?;
        }
        return Ok(points);
    }
    let index = LineIndex::build_coordseq(line, &PlanarMetric)?;
    match plan {
        InterpolatePlan::Count(count) => {
            for sample in 0..*count {
                crate::boundary::try_push_vec(
                    &mut points,
                    Shape::Point(index.interpolate(
                        InterpolatePlan::count_fraction(sample, *count),
                        true,
                        &PlanarMetric,
                    )),
                )?;
            }
        },
        InterpolatePlan::Distances(distances, mode) => {
            for &distance in distances {
                crate::boundary::try_push_vec(
                    &mut points,
                    Shape::Point(index.interpolate(
                        mode.planar_along(distance, to_metre.get()),
                        mode.is_normalized(),
                        &PlanarMetric,
                    )),
                )?;
            }
        },
    }
    Ok(points)
}

/// The row kernel for many `line_interpolate` samples: one cached-index line
/// handle, many samples.
pub(crate) fn line_interpolate_points_shape(
    model: &crate::crs::MetricModel,
    shape: &PreparedPlan,
    frame_cache: &crate::geometry::FrameDependentCaches,
    plan: &InterpolatePlan,
    lane: crate::dispatch::Lane,
) -> Result<Vec<Shape>, crate::error::Error> {
    let mut shapes = Vec::new();
    crate::boundary::try_reserve_vec(&mut shapes, plan.sample_count())?;
    let sample = |distance, mode| {
        crate::broadcast::degrade_linref_point(
            line_interpolate_shape_with_mode(model, shape, frame_cache, distance, mode),
            lane,
        )
    };
    match plan {
        InterpolatePlan::Count(count) => {
            for index in 0..*count {
                crate::boundary::try_push_vec(
                    &mut shapes,
                    sample(
                        InterpolatePlan::count_fraction(index, *count),
                        crate::crs::DistanceMode::of(true),
                    )?,
                )?;
            }
        },
        InterpolatePlan::Distances(distances, mode) => {
            for &distance in distances {
                crate::boundary::try_push_vec(&mut shapes, sample(distance, *mode)?)?;
            }
        },
    }
    Ok(shapes)
}

/// The `line_interpolate_point` row kernel (model resolved once per call by
/// the surfaces; a persistent `line` handle keeps its cached `LineIndex`).
pub(crate) fn line_interpolate_shape(
    model: &crate::crs::MetricModel,
    line: &PreparedPlan,
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
    line: &PreparedPlan,
    frame_cache: &crate::geometry::FrameDependentCaches,
    distance: f64,
    distance_mode: crate::crs::DistanceMode,
) -> Result<Shape> {
    Ok(match model {
        crate::crs::MetricModel::Planar { to_metre } => line.line_interpolate_point(
            distance_mode.planar_along(distance, to_metre.get()),
            distance_mode.is_normalized(),
        )?,
        crate::crs::MetricModel::Geodesic { crs, state, .. } => {
            crate::crs::geodesic_line_interpolate(
                crs,
                line,
                frame_cache,
                distance,
                distance_mode,
                state,
            )?
        },
    })
}

/// The `line_substring` row kernel (see [`line_interpolate_shape`]).
pub(crate) fn line_substring_shape(
    model: &crate::crs::MetricModel,
    line: &PreparedPlan,
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
        crate::crs::MetricModel::Geodesic { crs, state, .. } => {
            crate::crs::geodesic_line_substring(
                crs,
                line,
                frame_cache,
                range,
                distance_mode,
                state,
            )?
        },
    })
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
        crate::crs::MetricModel::Geodesic { crs, state, .. } => {
            crate::crs::with_geodesic_coordseq_metric_at(crs, state, |metric| {
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
        crate::crs::MetricModel::Geodesic { crs, state, .. } => {
            crate::crs::with_geodesic_coordseq_metric_at(crs, state, |metric| {
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
            let located =
                index.locate_point(query, distance_mode.is_normalized(), &PlanarMetric)?;
            Ok(if distance_mode.is_normalized() {
                located
            } else {
                located * to_metre.get()
            })
        },
        crate::crs::MetricModel::Geodesic { crs, state, .. } => {
            crate::crs::with_geodesic_coordseq_metric_at(crs, state, |metric| {
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
    line: &PreparedPlan,
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
        crate::crs::MetricModel::Geodesic { crs, state, .. } => {
            crate::crs::geodesic_line_locate(crs, line, frame_cache, query, distance_mode, state)?
        },
    })
}
