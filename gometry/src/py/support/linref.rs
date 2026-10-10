use std::cell::Cell;

use crate::boundary::{FloatInput, input};
use crate::crs::{GeodesicMetric, ResolvedMetric};
use crate::geometry::{
    ChartLineIndex, ChartParts, DistanceParts, FrameDependentCaches, GeographicPrepared,
    MeasuredLineIndex, PreparedPlan,
};
use crate::py::support::{
    Bound, CoordSeq, GeometryError, LineIndex, PlanarMetric, Point, PyAny, PyResult, Result, Shape,
    float_values,
};
#[cfg(test)]
use crate::test_support::linear_reference_counters;

fn chart_line_index(
    line: &PreparedPlan,
    cache: &FrameDependentCaches,
    metric: &GeodesicMetric,
) -> Result<std::sync::Arc<ChartLineIndex>> {
    #[cfg(test)]
    linear_reference_counters::prepared_chart_index_resolution();
    GeographicPrepared::try_new(line, cache, metric.frame)?;
    line.chart_line_index(
        cache,
        &metric.crs,
        metric.geodesic.a,
        metric.geodesic.f,
        metric.state.generation,
        metric,
    )
}

fn measured_line_context<'a>(
    line: &'a PreparedPlan,
    cache: &FrameDependentCaches,
    metric: &GeodesicMetric,
) -> Result<(std::sync::Arc<ChartParts>, &'a MeasuredLineIndex)> {
    GeographicPrepared::try_new(line, cache, metric.frame)?;
    let parts = line.measured_chart_parts(
        cache,
        &metric.crs,
        metric.geodesic.a,
        metric.geodesic.f,
        metric.state.generation,
        metric,
    )?;
    Ok((parts, line.measured_line_index()?))
}

pub(crate) enum PreparedLineLocate<'a> {
    PlanarDistance {
        line: &'a PreparedPlan,
        index: &'a LineIndex,
        parts: Cell<Option<&'a DistanceParts>>,
        normalized: bool,
        to_metre: f64,
    },
    PlanarMeasure(&'a MeasuredLineIndex),
    GeographicDistance {
        index: std::sync::Arc<ChartLineIndex>,
        metric: &'a GeodesicMetric,
        normalized: bool,
    },
    GeographicMeasure {
        index: &'a MeasuredLineIndex,
        parts: std::sync::Arc<ChartParts>,
        metric: &'a GeodesicMetric,
    },
}

impl PreparedLineLocate<'_> {
    pub(crate) fn locate(&self, query: Point) -> Result<f64> {
        match self {
            Self::GeographicDistance { metric, .. } | Self::GeographicMeasure { metric, .. }
                if !query.x.is_nan() =>
            {
                crate::crs::ensure_geographic_lonlat_in_frame(metric.frame, query.x, query.y)?;
            },
            Self::PlanarDistance { .. }
            | Self::PlanarMeasure(_)
            | Self::GeographicDistance { .. }
            | Self::GeographicMeasure { .. } => {},
        }
        self.locate_admitted(query)
    }

    fn locate_admitted(&self, query: Point) -> Result<f64> {
        match self {
            Self::PlanarDistance {
                line,
                index,
                parts,
                normalized,
                to_metre,
            } => {
                let station = index.locate_point_prepared(query, *normalized, || {
                    if let Some(parts) = parts.get() {
                        return Ok(parts);
                    }
                    let resolved = line.distance_parts()?;
                    parts.set(Some(resolved));
                    Ok(resolved)
                })?;
                Ok(if *normalized {
                    station
                } else {
                    station * to_metre
                })
            },
            Self::PlanarMeasure(index) => index.locate(query, &PlanarMetric),
            Self::GeographicDistance {
                index,
                metric,
                normalized,
            } => index.locate_chart_point(query, *normalized, *metric),
            Self::GeographicMeasure {
                index,
                parts,
                metric,
            } => index.chart_locate(query, parts.as_ref(), *metric),
        }
    }
}

pub(crate) fn prepare_line_locate<'a>(
    metric: &'a ResolvedMetric,
    line: &'a PreparedPlan,
    cache: &FrameDependentCaches,
    query: Point,
    basis_m: bool,
    normalized: bool,
) -> Result<(PreparedLineLocate<'a>, f64)> {
    let prepared = match metric {
        ResolvedMetric::Planar { .. } if basis_m => {
            PreparedLineLocate::PlanarMeasure(line.measured_line_index()?)
        },
        ResolvedMetric::Planar { to_metre } => PreparedLineLocate::PlanarDistance {
            line,
            index: line.line_index()?,
            parts: Cell::new(None),
            normalized,
            to_metre: to_metre.get(),
        },
        ResolvedMetric::Geodesic(metric) => {
            GeographicPrepared::try_new(line, cache, metric.frame)?;
            if !query.x.is_nan() {
                crate::crs::ensure_geographic_lonlat_in_frame(metric.frame, query.x, query.y)?;
            }
            if basis_m {
                let (parts, index) = measured_line_context(line, cache, metric)?;
                PreparedLineLocate::GeographicMeasure {
                    index,
                    parts,
                    metric,
                }
            } else {
                PreparedLineLocate::GeographicDistance {
                    index: chart_line_index(line, cache, metric)?,
                    metric,
                    normalized,
                }
            }
        },
    };
    let located = prepared.locate_admitted(query)?;
    Ok((prepared, located))
}

pub(crate) fn line_interpolate_shape_m(
    metric: &ResolvedMetric,
    line: &PreparedPlan,
    cache: &crate::geometry::FrameDependentCaches,
    m: f64,
) -> Result<Shape> {
    match metric {
        ResolvedMetric::Planar { .. } => line.line_interpolate_point_m_with(m, &PlanarMetric),
        ResolvedMetric::Geodesic(metric) => {
            let (parts, index) = measured_line_context(line, cache, metric)?;
            Ok(Shape::Point(index.chart_interpolate(m, &parts, metric)?))
        },
    }
}

pub(crate) fn line_substring_shape_m(
    metric: &ResolvedMetric,
    line: &PreparedPlan,
    cache: &crate::geometry::FrameDependentCaches,
    range: crate::geometry::MeasureRange,
) -> Result<Shape> {
    match metric {
        ResolvedMetric::Planar { .. } => line.line_substring_m_with(range, &PlanarMetric),
        ResolvedMetric::Geodesic(metric) => {
            let (parts, index) = measured_line_context(line, cache, metric)?;
            index.chart_substring(range, &parts, metric)
        },
    }
}

/// A parsed `line_interpolate_points` sampling plan: `count` linspace
/// fractions (endpoints included from two samples up), or explicit
/// CRS-aware distances.
pub(crate) enum InterpolatePlan {
    Count(usize),
    Distances(Vec<f64>, crate::crs::DistanceMode),
}

impl InterpolatePlan {
    pub(crate) const fn sample_count(&self) -> usize {
        match self {
            Self::Count(count) => *count,
            Self::Distances(distances, _) => distances.len(),
        }
    }

    /// The fraction of sample `index` in a `count`-sample linspace: both
    /// endpoints from two samples up, the start alone for one.
    fn count_fraction(index: usize, count: usize) -> f64 {
        if count > 1 {
            index as f64 / count.wrapping_sub(1) as f64
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
    let mut points = Vec::with_capacity(plan.sample_count());
    if line.is_empty() {
        for _ in 0..plan.sample_count() {
            points.push(Shape::empty_point());
        }
        return Ok(points);
    }
    let index = LineIndex::build_coordseq(line, &PlanarMetric)?;
    match plan {
        InterpolatePlan::Count(count) => {
            for sample in 0..*count {
                points.push(Shape::Point(index.interpolate_route_position(
                    InterpolatePlan::count_fraction(sample, *count),
                    true,
                    &PlanarMetric,
                )?));
            }
        },
        InterpolatePlan::Distances(distances, mode) => {
            for &distance in distances {
                points.push(Shape::Point(index.interpolate_route_position(
                    mode.planar_along(distance, to_metre.get()),
                    mode.is_normalized(),
                    &PlanarMetric,
                )?));
            }
        },
    }
    Ok(points)
}

/// The row kernel for many `line_interpolate` samples: one cached-index line
/// handle, many samples.
pub(crate) fn line_interpolate_points_shape(
    metric: &ResolvedMetric,
    shape: &PreparedPlan,
    frame_cache: &crate::geometry::FrameDependentCaches,
    plan: &InterpolatePlan,
    lane: crate::dispatch::Lane,
) -> Result<Vec<Shape>, crate::error::Error> {
    let mut shapes = Vec::with_capacity(plan.sample_count());
    if plan.sample_count() == 0 {
        return Ok(shapes);
    }
    match metric {
        ResolvedMetric::Planar { to_metre } => {
            let index = match shape.line_index() {
                Ok(index) => index,
                Err(error) => {
                    shapes.push(crate::broadcast::degrade_linref_point(Err(error), lane)?);
                    shapes.resize(plan.sample_count(), Shape::empty_point());
                    return Ok(shapes);
                },
            };
            match plan {
                InterpolatePlan::Count(count) => {
                    for sample in 0..*count {
                        shapes.push(crate::broadcast::degrade_linref_point(
                            index
                                .interpolate_route_position(
                                    InterpolatePlan::count_fraction(sample, *count),
                                    true,
                                    &PlanarMetric,
                                )
                                .map(Shape::Point),
                            lane,
                        )?);
                    }
                },
                InterpolatePlan::Distances(distances, mode) => {
                    for &distance in distances {
                        shapes.push(crate::broadcast::degrade_linref_point(
                            index
                                .interpolate_route_position(
                                    mode.planar_along(distance, to_metre.get()),
                                    mode.is_normalized(),
                                    &PlanarMetric,
                                )
                                .map(Shape::Point),
                            lane,
                        )?);
                    }
                },
            }
        },
        ResolvedMetric::Geodesic(metric) => {
            let index = match chart_line_index(shape, frame_cache, metric) {
                Ok(index) => index,
                Err(error) => {
                    shapes.push(crate::broadcast::degrade_linref_point(Err(error), lane)?);
                    shapes.resize(plan.sample_count(), Shape::empty_point());
                    return Ok(shapes);
                },
            };
            match plan {
                InterpolatePlan::Count(count) => {
                    for sample in 0..*count {
                        shapes.push(crate::broadcast::degrade_linref_point(
                            index
                                .interpolate_route_position(
                                    InterpolatePlan::count_fraction(sample, *count),
                                    true,
                                    metric,
                                )
                                .map(Shape::Point),
                            lane,
                        )?);
                    }
                },
                InterpolatePlan::Distances(distances, mode) => {
                    for &distance in distances {
                        shapes.push(crate::broadcast::degrade_linref_point(
                            index
                                .interpolate_route_position(distance, mode.is_normalized(), metric)
                                .map(Shape::Point),
                            lane,
                        )?);
                    }
                },
            }
        },
    }
    Ok(shapes)
}

/// The `line_interpolate_point` row kernel using the operation's resolved metric.
pub(crate) fn line_interpolate_shape(
    metric: &ResolvedMetric,
    line: &PreparedPlan,
    frame_cache: &crate::geometry::FrameDependentCaches,
    distance: f64,
    normalized: bool,
) -> Result<Shape> {
    Ok(match metric {
        ResolvedMetric::Planar { to_metre } => line.line_interpolate_point(
            crate::crs::DistanceMode::of(normalized).planar_along(distance, to_metre.get()),
            normalized,
        )?,
        ResolvedMetric::Geodesic(metric) => {
            GeographicPrepared::try_new(line, frame_cache, metric.frame)?;
            let distance = crate::Finite::try_new("distance", distance)?.get();
            Shape::Point(
                chart_line_index(line, frame_cache, metric)?
                    .interpolate_route_position(distance, normalized, metric)?,
            )
        },
    })
}

/// The `line_substring` row kernel (see [`line_interpolate_shape`]).
pub(crate) fn line_substring_shape(
    metric: &ResolvedMetric,
    line: &PreparedPlan,
    frame_cache: &crate::geometry::FrameDependentCaches,
    range: crate::geometry::MeasureRange,
    normalized: bool,
) -> Result<Shape> {
    Ok(match metric {
        ResolvedMetric::Planar { to_metre } => line.line_substring(
            crate::geometry::MeasureRange::substring_distance(
                crate::crs::DistanceMode::of(normalized)
                    .planar_along(range.start(), to_metre.get()),
                crate::crs::DistanceMode::of(normalized).planar_along(range.end(), to_metre.get()),
            )?,
            normalized,
        )?,
        ResolvedMetric::Geodesic(metric) => {
            chart_line_index(line, frame_cache, metric)?.substring(range, normalized, metric)?
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
            Ok(index.interpolate_route_position(
                distance_mode.planar_along(distance, to_metre.get()),
                distance_mode.is_normalized(),
                &PlanarMetric,
            )?)
        },
        crate::crs::MetricModel::Geodesic {
            crs, frame, state, ..
        } => {
            crate::crs::ensure_geographic_coordseq_in_frame(*frame, line)?;
            crate::crs::with_geographic_coordseq_metric_at(crs, state, |metric| {
                crate::crs::chart_line_interpolate_coordseq_admitted(
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
        crate::crs::MetricModel::Geodesic {
            crs, frame, state, ..
        } => {
            crate::crs::ensure_geographic_coordseq_in_frame(*frame, line)?;
            crate::crs::with_geographic_coordseq_metric_at(crs, state, |metric| {
                crate::crs::chart_line_substring_coordseq_admitted(
                    line,
                    range,
                    distance_mode,
                    metric,
                )
            })
        },
    }
}

/// Packed-line column kernel for `line_locate_point`, streaming planar stationing.
pub(crate) fn line_locate_coordseq(
    model: &crate::crs::MetricModel,
    line: &CoordSeq,
    query: Point,
    normalized: bool,
) -> Result<f64, crate::error::Error> {
    let distance_mode = crate::crs::DistanceMode::of(normalized);
    match model {
        crate::crs::MetricModel::Planar { to_metre } => {
            let located = crate::geometry::locate_planar_route(
                std::iter::once(line),
                query,
                distance_mode.is_normalized(),
            )?;
            Ok(if distance_mode.is_normalized() {
                located
            } else {
                located * to_metre.get()
            })
        },
        crate::crs::MetricModel::Geodesic {
            crs, frame, state, ..
        } => {
            crate::crs::ensure_geographic_coordseq_in_frame(*frame, line)?;
            if !query.x.is_nan() {
                crate::crs::ensure_geographic_lonlat_in_frame(*frame, query.x, query.y)?;
            }
            crate::crs::with_geographic_coordseq_metric_at(crs, state, |metric| {
                crate::crs::chart_line_locate_coordseq_admitted(line, query, distance_mode, metric)
            })
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{
        InterpolatePlan, PreparedPlan, line_interpolate_points_shape, linear_reference_counters,
    };
    use crate::crs::{MetricModel, ResolvedMetric};
    use crate::dispatch::Lane;
    use crate::geometry::{ChartLineIndex, CoordSeq, FrameDependentCaches, LineSeq, Point, Shape};

    fn line(z: bool, m: bool) -> PreparedPlan {
        PreparedPlan::new(Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(
                    vec![0.0, 4.0],
                    vec![10.0, 20.0],
                    z.then_some(vec![2.0, 8.0]),
                    m.then_some(vec![12.0, 18.0]),
                )
                .unwrap(),
            )
            .unwrap(),
        ))
    }

    fn point_bits(shape: &Shape) -> [Option<u64>; 4] {
        let Shape::Point(point) = shape else {
            panic!("interpolation produces points");
        };
        [
            Some(point.x.to_bits()),
            Some(point.y.to_bits()),
            point.z().map(f64::to_bits),
            point.m().map(f64::to_bits),
        ]
    }

    fn assert_many_samples_match_single_samples(model: &MetricModel) {
        for (has_z, has_m) in [(false, false), (true, false), (false, true), (true, true)] {
            let line = line(has_z, has_m);
            let cache = FrameDependentCaches::default();
            let plan = InterpolatePlan::Count(5);
            linear_reference_counters::reset();
            let metric = ResolvedMetric::from_model(model).unwrap();
            let many =
                line_interpolate_points_shape(&metric, &line, &cache, &plan, Lane::Scalar).unwrap();
            let counts = linear_reference_counters::snapshot();
            assert_eq!(counts.resolved_metrics, 1);
            if matches!(&metric, ResolvedMetric::Geodesic(_)) {
                assert_eq!(counts.prepared_chart_index_resolutions, 1);
            } else {
                assert_eq!(counts.prepared_chart_index_resolutions, 0);
            }
            let singles = match model {
                MetricModel::Planar { .. } => (0..5)
                    .map(|sample| {
                        line.line_interpolate_point(f64::from(sample) / 4.0, true)
                            .unwrap()
                    })
                    .collect::<Vec<_>>(),
                MetricModel::Geodesic { crs, state, .. } => {
                    crate::crs::with_ellipsoid_metric_at(crs, &[], state, |metric| {
                        let index = ChartLineIndex::build_chart(line.shape(), metric)?;
                        (0..5)
                            .map(|sample| {
                                index
                                    .interpolate_route_position(
                                        f64::from(sample) / 4.0,
                                        true,
                                        metric,
                                    )
                                    .map(Shape::Point)
                            })
                            .collect::<crate::error::Result<Vec<_>>>()
                    })
                    .unwrap()
                },
            };
            assert_eq!(
                many.iter().map(point_bits).collect::<Vec<_>>(),
                singles.iter().map(point_bits).collect::<Vec<_>>(),
                "axes Z={has_z}, M={has_m}",
            );
        }
    }

    #[test]
    fn many_samples_preserve_planar_xy_z_m_and_zm_bits() {
        assert_many_samples_match_single_samples(&MetricModel::COORDINATE);
    }

    #[test]
    fn many_samples_resolve_one_chart_index_and_preserve_geographic_bits() {
        let model = crate::crs::metric_model(Some("OGC:CRS84")).unwrap();
        assert_many_samples_match_single_samples(&model);
    }

    #[test]
    fn coordinate_sequence_locate_builds_one_chart_index_per_line() {
        let model = crate::crs::metric_model(Some("OGC:CRS84")).unwrap();
        let crate::crs::MetricModel::Geodesic {
            crs, frame, state, ..
        } = model
        else {
            panic!("CRS84 selects a geodesic metric");
        };
        let lines = [
            CoordSeq::try_from_vecs(vec![0.0, 2.0], vec![0.0, 0.0], None, None).unwrap(),
            CoordSeq::try_from_vecs(vec![10.0, 12.0], vec![1.0, 1.0], None, None).unwrap(),
        ];
        let queries = [Point::new_raw_xy(1.0, 0.0), Point::new_raw_xy(11.0, 1.0)];
        for (line, query) in lines.iter().zip(queries) {
            crate::crs::ensure_geographic_coordseq_in_frame(frame, line).unwrap();
            crate::crs::ensure_geographic_lonlat_in_frame(frame, query.x, query.y).unwrap();
        }
        linear_reference_counters::reset();
        let located = crate::crs::with_geographic_coordseq_metric_at(&crs, &state, |metric| {
            lines
                .iter()
                .enumerate()
                .map(|(row, line)| {
                    crate::crs::chart_line_locate_coordseq_admitted(
                        line,
                        *queries.get(row).expect("line and query rows are paired"),
                        true,
                        metric,
                    )
                })
                .collect::<crate::error::Result<Vec<_>>>()
        })
        .unwrap();
        assert_eq!(
            located
                .iter()
                .map(|value| value.to_bits())
                .collect::<Vec<_>>(),
            [0.5_f64.to_bits(), 0.5_f64.to_bits()],
        );
        let counts = linear_reference_counters::snapshot();
        assert_eq!(counts.geographic_metric_scopes, 1);
        assert_eq!(counts.coordseq_chart_index_builds, lines.len());
    }
}
