//! Source-frame vertex provenance for operations whose output vertices are
//! selected from, or interpolated on, the original input geometry.

use crate::error::Result;
use crate::geographic::GeographicFrame;
use crate::geometry::ordinates::{
    NodeOrdinates, RequestRowInput, RootCatalog, RootId, RootRequest, resolve_request_rows,
    resolve_source_root,
};
use crate::geometry::{
    ChartMetric, CoordinateAxes, ExactRational, MOrdinate, Point, RootMetricState,
    SegmentProjection, SourceArcMetricAccess, ZOrdinate,
};
use crate::work::Work;

/// How one output vertex derives from the operation's source-frame input.
#[derive(Clone, Debug)]
pub(crate) enum VertexProvenance {
    Input(usize),
    /// Planar interpolation; geographic source interpolation uses `OnSource`.
    OnSegment {
        i: usize,
        j: usize,
        projection: SegmentProjection,
    },
    /// Geographic interpolation bound to its original source edge.
    OnSource {
        i: usize,
        j: usize,
        projection: SegmentProjection,
        root_request: RootRequest,
    },
}

/// A checked geographic plan. A segment sample carrying source Z/M cannot be
/// admitted without a request for its original edge.
pub(crate) struct GeographicProvenancePlan<'a> {
    source_points: &'a [Point],
    vertices: &'a [VertexProvenance],
    axes: CoordinateAxes,
}

impl<'a> GeographicProvenancePlan<'a> {
    pub(crate) fn new(
        source_points: &'a [Point],
        vertices: &'a [VertexProvenance],
        axes: CoordinateAxes,
    ) -> Result<Self> {
        for vertex in vertices {
            let VertexProvenance::OnSegment {
                i, j, projection, ..
            } = vertex
            else {
                continue;
            };
            let start = *source_points.get(*i).ok_or_else(|| {
                crate::geometry::GeometryErrorKind::Invalid(
                    "geographic provenance segment start is outside its source points".into(),
                )
            })?;
            let end = *source_points.get(*j).ok_or_else(|| {
                crate::geometry::GeometryErrorKind::Invalid(
                    "geographic provenance segment end is outside its source points".into(),
                )
            })?;
            let at_start = projection.is_start();
            let at_end = projection.is_end();
            let has_z = if at_start {
                start.z().is_some()
            } else if at_end {
                end.z().is_some()
            } else {
                start.z().is_some() && end.z().is_some()
            };
            let has_m = if at_start {
                start.m().is_some()
            } else if at_end {
                end.m().is_some()
            } else {
                start.m().is_some() && end.m().is_some()
            };
            if (axes.has_z() && has_z) || (axes.has_m() && has_m) {
                return Err(crate::crs::CrsError::invalid(
                    "geographic on-segment provenance is missing its original source root",
                ));
            }
        }
        Ok(Self {
            source_points,
            vertices,
            axes,
        })
    }
}

/// Per-edge subdivision counts for one original line or ring chain.
///
/// The chain ordinal is in the geometry's logical line/ring traversal order.
/// Counts preserve which generated support segments belong to each original
/// edge without treating inserted coordinates as new vertex sites.
#[derive(Clone, Debug)]
pub(crate) struct RefinedChainProvenance {
    pub(crate) chain_index: usize,
    /// End offsets of original edges in the refined chain's segment sequence.
    pub(crate) piece_ends: Vec<usize>,
    /// Exact binary64 chart parameters at emitted support vertices.
    ///
    /// Each original edge owns a sequence starting at zero and ending at one.
    /// Values are captured from `ChartEdge` before point materialization; the
    /// adjacent pair is converted to exact rationals when a witness selects it.
    pub(crate) source_parameters: Option<Vec<f64>>,
}

impl RefinedChainProvenance {
    pub(crate) fn from_chart_parameters(
        chain_index: usize,
        parameter_ends: &[usize],
        source_parameters: Vec<f64>,
    ) -> Result<Self> {
        if parameter_ends.last().copied().unwrap_or(0) != source_parameters.len() {
            return Err(crate::geometry::GeometryErrorKind::Invalid(
                "refined source parameters do not match their support edges".into(),
            )
            .into());
        }
        let mut start = 0_usize;
        let mut segment_end = 0_usize;
        let mut piece_ends = Vec::with_capacity(parameter_ends.len());
        for &end in parameter_ends {
            let parameters = source_parameters.get(start..end).ok_or_else(|| {
                crate::geometry::GeometryErrorKind::Invalid(
                    "refined source parameter edge range is unknown".into(),
                )
            })?;
            let starts_at_zero = parameters
                .first()
                .is_some_and(|parameter| parameter.to_bits() == 0.0_f64.to_bits());
            let ends_at_one = parameters
                .last()
                .is_some_and(|parameter| parameter.to_bits() == 1.0_f64.to_bits());
            let strictly_increasing = parameters
                .iter()
                .zip(parameters.iter().skip(1))
                .all(|(left, right)| left < right);
            if parameters.len() < 2
                || !starts_at_zero
                || !ends_at_one
                || parameters
                    .iter()
                    .any(|parameter| !parameter.is_finite() || *parameter < 0.0 || *parameter > 1.0)
                || !strictly_increasing
            {
                return Err(crate::geometry::GeometryErrorKind::Invalid(
                    "refined source parameters are not a directed partition of their root edge"
                        .into(),
                )
                .into());
            }
            let segment_count = parameters
                .len()
                .checked_sub(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            segment_end = segment_end
                .checked_add(segment_count)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            piece_ends.push(segment_end);
            start = end;
        }
        Ok(Self {
            chain_index,
            piece_ends,
            source_parameters: Some(source_parameters),
        })
    }

    pub(crate) fn source_interval(
        &self,
        original_edge: usize,
        local_segment: usize,
    ) -> Option<(f64, f64)> {
        let edge_segment_start = if original_edge == 0 {
            0
        } else {
            *self.piece_ends.get(original_edge.checked_sub(1)?)?
        };
        let edge_segment_end = *self.piece_ends.get(original_edge)?;
        if !(edge_segment_start..edge_segment_end).contains(&local_segment) {
            return None;
        }
        let parameter_start = local_segment.checked_add(original_edge)?;
        let parameters = self.source_parameters.as_ref()?;
        Some((
            *parameters.get(parameter_start)?,
            *parameters.get(parameter_start.checked_add(1)?)?,
        ))
    }
}

/// Materialize planar vertex provenance in the original source frame.
///
/// `Input(i)` is a bit-exact copy of `original[i]`; `OnSegment` interpolates
/// directly on the original segment.
pub(crate) fn emit_from_original(
    original: &[Point],
    plan: &[VertexProvenance],
) -> Result<Vec<Point>> {
    let mut output = Vec::with_capacity(plan.len());
    for vertex in plan {
        let point = match vertex {
            VertexProvenance::Input(index) => *original
                .get(*index)
                .expect("input provenance index is within the source geometry"),
            VertexProvenance::OnSegment {
                i, j, projection, ..
            } => projection.interpolate_point(
                *original
                    .get(*i)
                    .expect("segment provenance start is within the source geometry"),
                *original
                    .get(*j)
                    .expect("segment provenance end is within the source geometry"),
            )?,
            VertexProvenance::OnSource { .. } => {
                return Err(crate::crs::CrsError::invalid(
                    "geographic source provenance requires root-based materialization",
                ));
            },
        };
        output.push(point);
    }
    Ok(output)
}

/// Materialize only the XY projection used by a local-frame geometry.
pub(crate) fn emit_from_original_xy(
    original: &[Point],
    plan: &[VertexProvenance],
) -> Result<Vec<Point>> {
    let mut output = Vec::with_capacity(plan.len());
    for vertex in plan {
        let point = match vertex {
            VertexProvenance::Input(index) => {
                let source = original
                    .get(*index)
                    .expect("input provenance index is within the source geometry");
                Point::new_raw_xy(source.x, source.y)
            },
            VertexProvenance::OnSegment {
                i, j, projection, ..
            }
            | VertexProvenance::OnSource {
                i, j, projection, ..
            } => {
                let start = *original
                    .get(*i)
                    .expect("segment provenance start is within the source geometry");
                let end = *original
                    .get(*j)
                    .expect("segment provenance end is within the source geometry");
                let projected = projection.interpolate_point(start, end)?;
                Point::new_raw_xy(projected.x, projected.y)
            },
        };
        output.push(point);
    }
    Ok(output)
}

struct OriginalPointRoots<'a> {
    frame: GeographicFrame,
    /// Root IDs are flat point offsets; each source line/ring edge is adjacent.
    points: &'a [Point],
}

impl RootCatalog for OriginalPointRoots<'_> {
    fn resolve<M: ChartMetric, W: Work>(
        &self,
        root: RootId,
        parameter: &ExactRational,
        axes: CoordinateAxes,
        metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
        state: &mut RootMetricState,
        fraction: &mut Option<f64>,
        work: &mut W,
    ) -> Result<NodeOrdinates> {
        let start_index = root.index();
        let end_index = start_index
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let source = [
            *self.points.get(start_index).ok_or_else(|| {
                crate::geometry::GeometryErrorKind::Invalid(
                    "source-root request start is outside the original points".into(),
                )
            })?,
            *self.points.get(end_index).ok_or_else(|| {
                crate::geometry::GeometryErrorKind::Invalid(
                    "source-root request end is outside the original points".into(),
                )
            })?,
        ];
        let longitude = self
            .frame
            .longitude()
            .chart_connection(source[0].x, source[1].x)
            .map(crate::geographic::ChartConnection::retained)
            .map_err(|error| {
                crate::geometry::GeometryErrorKind::Invalid(error.to_string().into())
            })?;
        resolve_source_root(
            self.frame,
            &source,
            longitude,
            parameter,
            axes,
            metric_access,
            state,
            fraction,
            work,
        )
    }
}

pub(crate) fn emit_from_original_with_source_roots<M: ChartMetric>(
    plan: &GeographicProvenancePlan<'_>,
    original_roots: &[Point],
    frame: GeographicFrame,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    work: &mut impl Work,
) -> Result<Vec<Point>> {
    let mut output = Vec::with_capacity(plan.vertices.len());
    for vertex in plan.vertices {
        let point = match vertex {
            VertexProvenance::Input(index) => *plan
                .source_points
                .get(*index)
                .expect("input provenance index is within the source geometry"),
            VertexProvenance::OnSegment {
                i, j, projection, ..
            }
            | VertexProvenance::OnSource {
                i, j, projection, ..
            } => {
                let start = *plan
                    .source_points
                    .get(*i)
                    .expect("segment provenance start is within the source geometry");
                let end = *plan
                    .source_points
                    .get(*j)
                    .expect("segment provenance end is within the source geometry");
                let projected = projection.interpolate_point(start, end)?;
                Point::new_raw_xy(projected.x, projected.y)
            },
        };
        output.push(point);
    }
    if !(plan.axes.has_z() || plan.axes.has_m())
        || !plan
            .vertices
            .iter()
            .any(|vertex| matches!(vertex, VertexProvenance::OnSource { .. }))
    {
        return Ok(output);
    }

    let mut rows = Vec::with_capacity(plan.vertices.len());
    for vertex in plan.vertices {
        let requests = match vertex {
            VertexProvenance::OnSource { root_request, .. } => vec![root_request.clone()],
            VertexProvenance::Input(_) | VertexProvenance::OnSegment { .. } => Vec::new(),
        };
        rows.push(RequestRowInput {
            axes: plan.axes,
            requests,
        });
    }
    let roots = OriginalPointRoots {
        frame,
        points: original_roots,
    };
    let resolved = resolve_request_rows(&rows, &roots, metric_access, work)?;
    for (point, (vertex, ordinates)) in output.iter_mut().zip(plan.vertices.iter().zip(resolved)) {
        if !matches!(vertex, VertexProvenance::OnSource { .. }) {
            continue;
        }
        *point = Point::new_raw_axes(
            point.x,
            point.y,
            ZOrdinate(ordinates.z.value()),
            MOrdinate(ordinates.m.value()),
        );
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::segments::projection::segment_projection;
    use crate::geometry::{CoordSeq, LineSeq, Segment, Shape, XY};

    #[test]
    fn geographic_provenance_rejects_an_on_segment_sample_without_a_root() {
        let source = [
            Point::new_axes(10.0, 5.0, ZOrdinate(Some(0.0)), MOrdinate(None)).unwrap(),
            Point::new_axes(40.0, 70.0, ZOrdinate(Some(100.0)), MOrdinate(None)).unwrap(),
        ];
        let projection = segment_projection(XY::new(25.0, 37.5), Segment {
            start: source[0].xy(),
            end: source[1].xy(),
        })
        .unwrap();
        assert_eq!(projection.fraction().to_bits(), 0.5_f64.to_bits());
        let plan = [VertexProvenance::OnSegment {
            i: 0,
            j: 1,
            projection,
        }];
        let axes = source[0].axes().union(source[1].axes());
        let result = GeographicProvenancePlan::new(&source, &plan, axes);

        assert!(
            result.is_err(),
            "a rootless WGS84 midpoint must not publish affine Z=50; its source-edge arclength Z is approximately 51.056",
        );
    }

    #[test]
    fn geographic_on_source_uses_the_original_wgs84_root_arclength() -> Result<()> {
        let source = [
            Point::new_axes(10.0, 5.0, ZOrdinate(Some(0.0)), MOrdinate(None))?,
            Point::new_axes(40.0, 70.0, ZOrdinate(Some(100.0)), MOrdinate(None))?,
        ];
        let line = LineSeq::try_new(CoordSeq::try_from_points(&source)?)?;
        let shape = Shape::LineString(line);
        let state = crate::crs::runtime_state_snapshot()?;

        let output =
            crate::crs::with_ellipsoid_metric_at("EPSG:4326", &[&shape], &state, |metric| {
                let metric_access = || Ok(metric);
                let mut work = crate::work::Unmetered;
                let projection = segment_projection(XY::new(25.0, 37.5), Segment {
                    start: source[0].xy(),
                    end: source[1].xy(),
                })?;
                let root_request = RootRequest::new_with_work(
                    RootId::from_index(0),
                    ExactRational::from_f64(0.5),
                    &mut work,
                )?;
                let plan = [VertexProvenance::OnSource {
                    i: 0,
                    j: 1,
                    projection,
                    root_request,
                }];
                let axes = source[0].axes().union(source[1].axes());
                let plan = GeographicProvenancePlan::new(&source, &plan, axes)?;
                emit_from_original_with_source_roots(
                    &plan,
                    &source,
                    GeographicFrame::crs84(),
                    Some(&metric_access),
                    &mut work,
                )
            })?;

        let point = output.first().ok_or_else(|| {
            crate::geometry::GeometryErrorKind::Invalid(
                "geographic provenance emitted no midpoint".into(),
            )
        })?;
        let z = point.z().ok_or_else(|| {
            crate::geometry::GeometryErrorKind::Invalid(
                "geographic provenance midpoint has no source Z".into(),
            )
        })?;
        if (z - 51.056_024_441_584_35).abs() > 1.0e-7 {
            return Err(crate::geometry::GeometryErrorKind::Invalid(
                format!("geographic midpoint Z {z} differs from its WGS84 source arclength").into(),
            )
            .into());
        }
        Ok(())
    }
}
