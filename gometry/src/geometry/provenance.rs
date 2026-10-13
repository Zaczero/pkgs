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
///
/// This type deliberately cannot hold a coordinate computed in another frame:
/// the only representable outputs are an exact source vertex copy or an
/// interpolation along a source segment. Local-projection kernels may use a
/// projected frame to decide this plan, but final coordinates are emitted by
/// [`emit_from_original`] from the original input bits.
#[derive(Clone, Debug)]
pub(crate) enum VertexProvenance {
    Input(usize),
    OnSegment {
        i: usize,
        j: usize,
        projection: SegmentProjection,
        root_request: Option<RootRequest>,
    },
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

/// Materialize a vertex-provenance plan in the original source frame.
///
/// `Input(i)` is a bit-exact copy of `original[i]`. `OnSegment` interpolates
/// directly on the original segment, so projected coordinates can never leak
/// into a copy/partial-output operation.
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
    source_points: &[Point],
    original_roots: &[Point],
    plan: &[VertexProvenance],
    axes: CoordinateAxes,
    frame: GeographicFrame,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    work: &mut impl Work,
) -> Result<Vec<Point>> {
    let mut output = emit_from_original(source_points, plan)?;
    if !axes.has_z() && !axes.has_m() {
        return Ok(output);
    }
    if !plan.iter().any(|vertex| {
        matches!(vertex, VertexProvenance::OnSegment {
            root_request: Some(_),
            ..
        })
    }) {
        return Ok(output);
    }

    let mut rows = Vec::with_capacity(plan.len());
    for vertex in plan {
        let requests = match vertex {
            VertexProvenance::OnSegment {
                root_request: Some(request),
                ..
            } => vec![request.clone()],
            VertexProvenance::Input(_) | VertexProvenance::OnSegment { .. } => Vec::new(),
        };
        rows.push(RequestRowInput { axes, requests });
    }
    let roots = OriginalPointRoots {
        frame,
        points: original_roots,
    };
    let resolved = resolve_request_rows(&rows, &roots, metric_access, work)?;
    for (point, (vertex, ordinates)) in output.iter_mut().zip(plan.iter().zip(resolved)) {
        if !matches!(vertex, VertexProvenance::OnSegment {
            root_request: Some(_),
            ..
        }) {
            continue;
        }
        *point = Point::new_raw_axes(
            point.x,
            point.y,
            ZOrdinate(ordinates.z.value().or_else(|| point.z())),
            MOrdinate(ordinates.m.value().or_else(|| point.m())),
        );
    }
    Ok(output)
}
