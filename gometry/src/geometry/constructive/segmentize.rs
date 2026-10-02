//! Route-aware subdivision of coordinate sequences.

use std::sync::Arc;

use crate::geographic::GeographicFrame;
use crate::geometry::constructive::Result;
use crate::geometry::types::ensure_coordseq_vertex_capacity;
use crate::geometry::{
    CoordSeq, ExpansionBudget, GENERATED_ITEM_LIMIT, GeometryErrorKind, Point, Segment, XY,
    axis_pow2_scale, interpolate_subdivision_ordinate, line_segments, point_distance, same_point,
    scaled_residual,
};

pub(crate) fn segmentize_points_budgeted(
    points: &CoordSeq,
    max_segment_length: f64,
    placement: SegmentPlacement<'_>,
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    if points.len() < 2 {
        return Ok(points.clone());
    }
    let mut plans = plan_segments(
        points,
        placement,
        SubdivisionTarget::MaxLength(max_segment_length),
    )?;
    // One extra piece supplies slack for a count that landed exactly on the
    // requested limit. The same selected-route verifier owns every placement.
    const MAX_REPRESENTABILITY_RETRIES: usize = 1;
    for retry in 0..=MAX_REPRESENTABILITY_RETRIES {
        let (output, generated) = subdivide_columns_from_plans_uncommitted(points, &plans, budget)?;
        match verify_segmentize_output(&output, max_segment_length, placement) {
            Ok(()) => {
                budget.add(generated)?;
                return Ok(output);
            },
            Err(error)
                if matches!(
                    error.kind(),
                    crate::error::ErrorKind::Geometry(GeometryErrorKind::UnrepresentableOutput {
                        operation: "segmentize"
                    })
                ) && retry < MAX_REPRESENTABILITY_RETRIES =>
            {
                for (plan, segment) in plans.iter_mut().zip(line_segments(points)) {
                    let nonzero = match plan.route {
                        SegmentRoutePlan::Planar => !same_point(segment.start, segment.end),
                        _ => plan.length > 0.0,
                    };
                    if nonzero {
                        plan.steps = plan
                            .steps
                            .checked_add(1)
                            .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    }
                }
            },
            Err(error) => return Err(error),
        }
    }
    unreachable!("the final representability retry returns its verification error")
}

fn segmentize_steps(length: f64, max_segment_length: f64) -> usize {
    if length == 0.0 || !length.is_finite() || max_segment_length <= 0.0 {
        return 1;
    }
    // A huge quotient must not wrap to a small step count, which would admit
    // output beyond the shared 16M generated-work budget.
    let pieces = (length / max_segment_length).ceil();
    if !pieces.is_finite() || pieces > (GENERATED_ITEM_LIMIT + 1) as f64 {
        // The sink charges inserted vertices (`pieces - 1`), so its sentinel
        // must remain one past that limit rather than merely one past pieces.
        GENERATED_ITEM_LIMIT + 2
    } else {
        pieces as usize
    }
}

/// Plan the ideal planar piece count without allowing an overflowing endpoint
/// difference to masquerade as an already-complete segment. The common path
/// keeps the established distance arithmetic; only a non-finite distance uses
/// a common power-of-two frame, so the endpoint residual and the limit are
/// compared in the same scaled units without forming an overflowing length.
fn planar_segmentize_steps(segment: Segment, max_segment_length: f64) -> usize {
    if !max_segment_length.is_finite() || max_segment_length <= 0.0 {
        return 1;
    }
    let length = point_distance(segment.start, segment.end);
    if length.is_finite() {
        return segmentize_steps(length, max_segment_length);
    }

    let maximum = segment
        .start
        .x
        .abs()
        .max(segment.start.y.abs())
        .max(segment.end.x.abs())
        .max(segment.end.y.abs());
    let scale = axis_pow2_scale(maximum);
    let dx = scaled_residual(segment.end.x, segment.start.x, scale);
    let dy = scaled_residual(segment.end.y, segment.start.y, scale);
    let scaled_limit = max_segment_length * scale;
    let ratio = if scaled_limit > 0.0 {
        dx.hypot(dy) / scaled_limit
    } else {
        f64::INFINITY
    };
    let pieces = ratio.ceil();
    // A huge quotient must not wrap to a small step count, which would admit
    // output beyond the shared 16M generated-work budget.
    if !pieces.is_finite() || pieces > (GENERATED_ITEM_LIMIT + 1) as f64 {
        // The sink charges inserted vertices (`pieces - 1`), so its sentinel
        // must remain one past that limit rather than merely one past pieces.
        GENERATED_ITEM_LIMIT + 2
    } else {
        pieces.max(1.0) as usize
    }
}

/// The ideal count describes the continuous subdivision, not the binary64
/// coordinates that the emitter actually stores. Audit the resulting chain so
/// the public maximum-length promise is about every emitted chord, including
/// source-to-source chords that can survive as repeated interior points.
fn verify_planar_segmentize_output(points: &CoordSeq, max_segment_length: f64) -> Result<()> {
    if !max_segment_length.is_finite() || max_segment_length <= 0.0 {
        return Ok(());
    }
    for segment in line_segments(points) {
        if point_distance(segment.start, segment.end)
            .partial_cmp(&max_segment_length)
            .is_none_or(std::cmp::Ordering::is_gt)
        {
            return Err(GeometryErrorKind::unrepresentable_output("segmentize"));
        }
    }
    Ok(())
}

/// Insert `ceil(1 / fraction) - 1` evenly spaced vertices into every segment
/// (the GEOS densify semantics): each segment is split into equal pieces no
/// longer than `fraction` of its own length, re-segmenting continuous
/// Hausdorff and refining the vertex sequence for discrete Fréchet. `fraction` is in
/// `(0, 1]`; `1` keeps the vertices unchanged.
#[cfg(test)]
pub(crate) fn densify_points_budgeted(
    points: &CoordSeq,
    fraction: f64,
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    densify_points_budgeted_with_placement(points, fraction, SegmentPlacement::Planar, budget)
}

pub(crate) fn densify_points_budgeted_with_placement(
    points: &CoordSeq,
    fraction: f64,
    placement: SegmentPlacement<'_>,
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    // One segment's steps already past the budget stop here, before a
    // saturated count (a subnormal `fraction`) reaches the step arithmetic.
    let steps = (1.0 / fraction).ceil() as usize;
    // The returned value is cumulative usage, not this edge's step count; the
    // budget may already include vertices from another part of this shape.
    budget.check_additional(steps)?;
    let plans = plan_segments(points, placement, SubdivisionTarget::Fraction(steps))?;
    subdivide_columns_from_plans(points, &plans, budget)
}

#[derive(Clone, Copy)]
struct GeodesicSegmentPlan {
    solution: (f64, f64, f64, f64),
    canonical_end: XY,
}

#[derive(Clone, Copy)]
struct RhumbSegmentPlan {
    start: Point,
    bearing: f64,
    length: f64,
}

#[derive(Clone, Copy)]
enum SegmentRoutePlan<'a> {
    Planar,
    Chart {
        edge: crate::crs::ChartEdge,
        geodesic: &'a geographiclib_rs::Geodesic,
    },
    Geodesic {
        plan: GeodesicSegmentPlan,
        geodesic: &'a geographiclib_rs::Geodesic,
        frame: GeographicFrame,
    },
    Rhumb {
        plan: RhumbSegmentPlan,
        crs: &'a str,
        state: &'a crate::crs::RuntimeState,
        frame: GeographicFrame,
    },
}

#[derive(Clone, Copy)]
struct SegmentPlan<'a> {
    route: SegmentRoutePlan<'a>,
    length: f64,
    steps: usize,
}

#[derive(Clone, Copy)]
enum SubdivisionTarget {
    MaxLength(f64),
    Fraction(usize),
}

fn point_from_xy(point: XY) -> Point {
    Point::new_raw_xy(point.x, point.y)
}

fn geodesic_segment_plan(
    geodesic: &geographiclib_rs::Geodesic,
    frame: GeographicFrame,
    start: Point,
    end: Point,
) -> Result<GeodesicSegmentPlan> {
    let start = frame
        .to_geodesic_point(start)
        .expect("geographic subdivision received admitted coordinates");
    let end = frame
        .to_geodesic_point(end)
        .expect("geographic subdivision received admitted coordinates");
    let segment = crate::crs::checked_geodesic_segment(geodesic, start, end)?;
    Ok(GeodesicSegmentPlan {
        solution: (
            segment.start.x,
            segment.start.y,
            segment.azimuth0,
            segment.length,
        ),
        canonical_end: segment.end.xy(),
    })
}

fn plan_segment<'a>(
    placement: SegmentPlacement<'a>,
    segment: Segment,
) -> Result<(SegmentRoutePlan<'a>, f64)> {
    Ok(match placement {
        SegmentPlacement::Planar => (SegmentRoutePlan::Planar, 0.0),
        SegmentPlacement::Chart(geodesic, frame) => {
            let edge = crate::crs::ChartEdge::new(
                point_from_xy(segment.start),
                point_from_xy(segment.end),
                frame,
            )?;
            let length = edge.length(geodesic)?;
            (SegmentRoutePlan::Chart { edge, geodesic }, length)
        },
        SegmentPlacement::Geodesic(geodesic, frame) => {
            let plan = geodesic_segment_plan(
                geodesic,
                frame,
                point_from_xy(segment.start),
                point_from_xy(segment.end),
            )?;
            (
                SegmentRoutePlan::Geodesic {
                    plan,
                    geodesic,
                    frame,
                },
                plan.solution.3,
            )
        },
        SegmentPlacement::Rhumb { crs, state, frame } => {
            let start = frame
                .to_geodesic_point(point_from_xy(segment.start))
                .expect("rhumb subdivision received admitted coordinates");
            let end = frame
                .to_geodesic_point(point_from_xy(segment.end))
                .expect("rhumb subdivision received admitted coordinates");
            let length =
                crate::crs::rhumb_distance_crs_at(crs, start.x, start.y, end.x, end.y, state)?;
            let bearing =
                crate::crs::rhumb_bearing_crs_at(crs, start.x, start.y, end.x, end.y, state)?;
            (
                SegmentRoutePlan::Rhumb {
                    plan: RhumbSegmentPlan {
                        start,
                        bearing,
                        length,
                    },
                    crs,
                    state,
                    frame,
                },
                length,
            )
        },
    })
}

fn plan_segments<'a>(
    points: &CoordSeq,
    placement: SegmentPlacement<'a>,
    target: SubdivisionTarget,
) -> Result<Vec<SegmentPlan<'a>>> {
    let mut plans = Vec::with_capacity(points.len().saturating_sub(1));
    for segment in line_segments(points) {
        let (route, length) = plan_segment(placement, segment)?;
        let steps = match target {
            SubdivisionTarget::MaxLength(max_length) => match route {
                SegmentRoutePlan::Planar => planar_segmentize_steps(segment, max_length),
                _ => segmentize_steps(length, max_length),
            },
            SubdivisionTarget::Fraction(steps) => match route {
                SegmentRoutePlan::Planar if same_point(segment.start, segment.end) => 1,
                SegmentRoutePlan::Planar => steps,
                _ if length > 0.0 => steps,
                _ => 1,
            },
        };
        plans.push(SegmentPlan {
            route,
            length,
            steps,
        });
    }
    Ok(plans)
}

fn measure_segment(placement: SegmentPlacement<'_>, segment: Segment) -> Result<f64> {
    Ok(match placement {
        SegmentPlacement::Planar => point_distance(segment.start, segment.end),
        SegmentPlacement::Chart(geodesic, frame) => crate::crs::ChartEdge::new(
            point_from_xy(segment.start),
            point_from_xy(segment.end),
            frame,
        )?
        .length(geodesic)?,
        SegmentPlacement::Geodesic(geodesic, frame) => {
            geodesic_segment_plan(
                geodesic,
                frame,
                point_from_xy(segment.start),
                point_from_xy(segment.end),
            )?
            .solution
            .3
        },
        SegmentPlacement::Rhumb { crs, state, frame } => {
            let start = frame
                .to_geodesic_point(point_from_xy(segment.start))
                .expect("rhumb verification received admitted coordinates");
            let end = frame
                .to_geodesic_point(point_from_xy(segment.end))
                .expect("rhumb verification received admitted coordinates");
            crate::crs::rhumb_distance_crs_at(crs, start.x, start.y, end.x, end.y, state)?
        },
    })
}

fn verify_segmentize_output(
    points: &CoordSeq,
    max_segment_length: f64,
    placement: SegmentPlacement<'_>,
) -> Result<()> {
    if !max_segment_length.is_finite() || max_segment_length <= 0.0 {
        return Ok(());
    }
    if matches!(placement, SegmentPlacement::Planar) {
        return verify_planar_segmentize_output(points, max_segment_length);
    }
    for segment in line_segments(points) {
        if measure_segment(placement, segment)?
            .partial_cmp(&max_segment_length)
            .is_none_or(std::cmp::Ordering::is_gt)
        {
            return Err(GeometryErrorKind::unrepresentable_output("segmentize"));
        }
    }
    Ok(())
}

trait SubdivisionSink {
    fn source(&mut self, source_index: usize) -> Result<()>;
    fn interior(&mut self, segment_index: usize, steps: usize) -> Result<()>;
}

/// Run the exact same segment plan through a counting sink and a materializing
/// sink.  Keeping this as one traversal shape makes the budget admission
/// mechanically match the coordinates subsequently emitted.
fn emit_subdivision(plans: &[SegmentPlan<'_>], sink: &mut impl SubdivisionSink) -> Result<()> {
    sink.source(0)?;
    for (segment_index, plan) in plans.iter().enumerate() {
        sink.interior(segment_index, plan.steps.max(1))?;
        sink.source(segment_index.wrapping_add(1))?;
    }
    Ok(())
}

struct SubdivisionCount {
    emitted: usize,
}

impl SubdivisionCount {
    #[expect(
        clippy::missing_const_for_fn,
        reason = "generated output is runtime subdivision accounting with no compile-time caller"
    )]
    fn generated(&self, input_len: usize) -> usize {
        self.emitted.wrapping_sub(input_len)
    }
}

impl SubdivisionSink for SubdivisionCount {
    fn source(&mut self, _: usize) -> Result<()> {
        self.emitted = self
            .emitted
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        Ok(())
    }

    fn interior(&mut self, _: usize, steps: usize) -> Result<()> {
        self.emitted = self
            .emitted
            .checked_add(steps.wrapping_sub(1))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        Ok(())
    }
}

/// How interior vertices are placed along a segment.
///
/// Z and M stay per-column under every placement — they are attribute ramps,
/// not positions.
#[derive(Clone, Copy)]
pub(crate) enum SegmentPlacement<'a> {
    /// Straight line in coordinate space (projected or CRS-free input).
    Planar,
    /// Along the straight coordinate-chart edge, at equal ellipsoidal
    /// arclength, for geographic coordinates in the typed native frame.
    Chart(&'a geographiclib_rs::Geodesic, GeographicFrame),
    /// Along the shortest ellipsoidal path between geographic endpoints.
    Geodesic(&'a geographiclib_rs::Geodesic, GeographicFrame),
    /// Along the constant-bearing route between geographic endpoints.
    Rhumb {
        crs: &'a str,
        state: &'a crate::crs::RuntimeState,
        frame: GeographicFrame,
    },
}

#[derive(Clone)]
pub(crate) enum SegmentPlacementOwned {
    Planar,
    Chart(geographiclib_rs::Geodesic, GeographicFrame),
    Geodesic(geographiclib_rs::Geodesic, GeographicFrame),
    Rhumb {
        crs: Arc<String>,
        state: Arc<crate::crs::RuntimeState>,
        frame: GeographicFrame,
    },
}

impl SegmentPlacementOwned {
    pub(crate) fn as_borrowed(&self) -> SegmentPlacement<'_> {
        match self {
            Self::Planar => SegmentPlacement::Planar,
            Self::Chart(geodesic, frame) => SegmentPlacement::Chart(geodesic, *frame),
            Self::Geodesic(geodesic, frame) => SegmentPlacement::Geodesic(geodesic, *frame),
            Self::Rhumb { crs, state, frame } => SegmentPlacement::Rhumb {
                crs,
                state,
                frame: *frame,
            },
        }
    }

    pub(crate) const fn frame(&self) -> Option<GeographicFrame> {
        match self {
            Self::Planar => None,
            Self::Chart(_, frame) | Self::Geodesic(_, frame) | Self::Rhumb { frame, .. } => {
                Some(*frame)
            },
        }
    }
}

struct SubdivisionColumns<'points, 'plans, 'metric> {
    points: &'points CoordSeq,
    plans: &'plans [SegmentPlan<'metric>],
    xs: Vec<f64>,
    ys: Vec<f64>,
    zs: Option<Vec<f64>>,
    ms: Option<Vec<f64>>,
}

impl<'points, 'plans, 'metric> SubdivisionColumns<'points, 'plans, 'metric> {
    fn new(
        points: &'points CoordSeq,
        plans: &'plans [SegmentPlan<'metric>],
        total: usize,
    ) -> Result<Self> {
        ensure_coordseq_vertex_capacity(total)?;
        let z_column = points.zs();
        let m_column = points.ms();
        Ok(Self {
            points,
            plans,
            xs: Vec::with_capacity(total),
            ys: Vec::with_capacity(total),
            zs: z_column.map(|_| Vec::with_capacity(total)),
            ms: m_column.map(|_| Vec::with_capacity(total)),
        })
    }

    fn finish(self) -> Result<CoordSeq> {
        CoordSeq::try_from_vecs(self.xs, self.ys, self.zs, self.ms)
    }

    fn push_interpolated(
        output: &mut Vec<f64>,
        column: &[f64],
        segment_index: usize,
        steps: usize,
    ) {
        let start = *column
            .get(segment_index)
            .expect("subdivision segment start belongs to its coordinate column");
        let end = *column
            .get(segment_index.wrapping_add(1))
            .expect("subdivision segment end belongs to its coordinate column");
        for step in 1..steps {
            let value = interpolate_subdivision_ordinate(start, end, step as f64 / steps as f64);
            output.push(if value.is_finite() {
                value
            } else if step <= steps / 2 {
                start
            } else {
                end
            });
        }
    }
}

fn emit_geodesic_plan(
    geodesic: &geographiclib_rs::Geodesic,
    frame: GeographicFrame,
    plan: GeodesicSegmentPlan,
    steps: usize,
    xs: &mut Vec<f64>,
    ys: &mut Vec<f64>,
) -> Result<()> {
    // `plan` contains canonical degrees; restore the caller's angular frame
    // only for inserted coordinates. Source vertices and Z/M stay in their
    // original columns in `SubdivisionColumns`.
    let (x0, y0, ..) = plan.solution;
    let (x1, y1) = (plan.canonical_end.x, plan.canonical_end.y);
    let equatorial = y0 == 0.0 && y1 == 0.0 && (x1 - x0).abs() < 180.0;
    for step in 1..steps {
        let fraction = step as f64 / steps as f64;
        let (mut longitude, mut latitude) = if equatorial {
            (interpolate_subdivision_ordinate(x0, x1, fraction), 0.0)
        } else {
            let info = crate::crs::geodesic_interpolate_on_line(geodesic, plan.solution, fraction)?;
            (info.longitude, info.latitude)
        };
        let point = frame
            .point_from_geodesic(Point::new_raw_xy(longitude, latitude))
            .expect("geographic subdivision produced a finite coordinate");
        (longitude, latitude) = (point.x, point.y);
        if !(longitude.is_finite() && latitude.is_finite()) {
            return Err(GeometryErrorKind::message(
                "geodesic subdivision calculation produced a non-finite coordinate",
            ));
        }
        xs.push(longitude);
        ys.push(latitude);
    }
    Ok(())
}

fn emit_chart_plan(
    edge: crate::crs::ChartEdge,
    geodesic: &geographiclib_rs::Geodesic,
    length: f64,
    steps: usize,
    xs: &mut Vec<f64>,
    ys: &mut Vec<f64>,
) -> Result<()> {
    for step in 1..steps {
        let fraction = step as f64 / steps as f64;
        let parameter = if edge.is_parallel() {
            fraction
        } else {
            edge.locate_t(length * fraction, geodesic)?
        };
        let point = edge.position(parameter);
        if !(point.x.is_finite() && point.y.is_finite()) {
            return Err(GeometryErrorKind::message(
                "chart-edge subdivision calculation produced a non-finite coordinate",
            ));
        }
        xs.push(point.x);
        ys.push(point.y);
    }
    Ok(())
}

fn emit_rhumb_plan(
    plan: RhumbSegmentPlan,
    crs: &str,
    state: &crate::crs::RuntimeState,
    frame: GeographicFrame,
    steps: usize,
    xs: &mut Vec<f64>,
    ys: &mut Vec<f64>,
) -> Result<()> {
    for step in 1..steps {
        let fraction = step as f64 / steps as f64;
        let Some((longitude, latitude)) = crate::crs::rhumb_destination_crs_at(
            crs,
            plan.start.x,
            plan.start.y,
            plan.bearing,
            plan.length * fraction,
            state,
        )?
        else {
            return Err(GeometryErrorKind::message(
                "rhumb subdivision calculation crosses a pole",
            ));
        };
        let point = frame
            .point_from_geodesic(Point::new_raw_xy(longitude, latitude))
            .expect("rhumb subdivision produced a finite coordinate");
        if !(point.x.is_finite() && point.y.is_finite()) {
            return Err(GeometryErrorKind::message(
                "rhumb subdivision calculation produced a non-finite coordinate",
            ));
        }
        xs.push(point.x);
        ys.push(point.y);
    }
    Ok(())
}

impl SubdivisionSink for SubdivisionColumns<'_, '_, '_> {
    fn source(&mut self, source_index: usize) -> Result<()> {
        self.xs.push(
            *self
                .points
                .xs()
                .get(source_index)
                .expect("subdivision source vertex belongs to the X column"),
        );
        self.ys.push(
            *self
                .points
                .ys()
                .get(source_index)
                .expect("subdivision source vertex belongs to the Y column"),
        );
        if let Some(output) = &mut self.zs {
            output.push(
                *self
                    .points
                    .zs()
                    .expect("Z output has an admitted source column")
                    .get(source_index)
                    .expect("subdivision source vertex belongs to the Z column"),
            );
        }
        if let Some(output) = &mut self.ms {
            output.push(
                *self
                    .points
                    .ms()
                    .expect("M output has an admitted source column")
                    .get(source_index)
                    .expect("subdivision source vertex belongs to the M column"),
            );
        }
        Ok(())
    }

    fn interior(&mut self, segment_index: usize, steps: usize) -> Result<()> {
        let plan = self
            .plans
            .get(segment_index)
            .expect("subdivision plan matches source segments");
        match plan.route {
            SegmentRoutePlan::Planar => {
                Self::push_interpolated(&mut self.xs, self.points.xs(), segment_index, steps);
                Self::push_interpolated(&mut self.ys, self.points.ys(), segment_index, steps);
            },
            SegmentRoutePlan::Chart { edge, geodesic } => {
                emit_chart_plan(
                    edge,
                    geodesic,
                    plan.length,
                    steps,
                    &mut self.xs,
                    &mut self.ys,
                )?;
            },
            SegmentRoutePlan::Geodesic {
                plan,
                geodesic,
                frame,
            } => emit_geodesic_plan(geodesic, frame, plan, steps, &mut self.xs, &mut self.ys)?,
            SegmentRoutePlan::Rhumb {
                plan,
                crs,
                state,
                frame,
            } => emit_rhumb_plan(plan, crs, state, frame, steps, &mut self.xs, &mut self.ys)?,
        }
        if let (Some(output), Some(column)) = (&mut self.zs, self.points.zs()) {
            Self::push_interpolated(output, column, segment_index, steps);
        }
        if let (Some(output), Some(column)) = (&mut self.ms, self.points.ms()) {
            Self::push_interpolated(output, column, segment_index, steps);
        }
        Ok(())
    }
}

fn subdivide_columns_from_plans(
    points: &CoordSeq,
    plans: &[SegmentPlan<'_>],
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    let (output, generated) = subdivide_columns_from_plans_uncommitted(points, plans, budget)?;
    budget.add(generated)?;
    Ok(output)
}

fn subdivide_columns_from_plans_uncommitted(
    points: &CoordSeq,
    plans: &[SegmentPlan<'_>],
    budget: &ExpansionBudget,
) -> Result<(CoordSeq, usize)> {
    let count = points.len();
    let mut count_sink = SubdivisionCount { emitted: 0 };
    emit_subdivision(plans, &mut count_sink)?;
    let total = count_sink.emitted;
    // Existing source vertices are input-sized work. Charge only the vertices
    // this operation creates, once for the whole caller-owned budget.
    let generated = count_sink.generated(count);
    budget.check_additional(generated)?;
    if generated == 0 {
        // Nothing subdivides (every segment already satisfies the bound —
        // the common case once data is denser than the requested length):
        // the input IS the output, share it.
        return Ok((points.clone(), 0));
    }
    let mut output = SubdivisionColumns::new(points, plans, total)?;
    emit_subdivision(plans, &mut output)?;
    let output = output.finish()?;
    Ok((output, generated))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subdivision_admits_exact_generated_limit_without_charging_input_vertices() {
        let plans = [SegmentPlan {
            route: SegmentRoutePlan::Planar,
            length: 0.0,
            steps: GENERATED_ITEM_LIMIT + 1,
        }];
        let mut count = SubdivisionCount { emitted: 0 };
        emit_subdivision(&plans, &mut count).unwrap();
        let generated = count.generated(2);
        assert_eq!(generated, GENERATED_ITEM_LIMIT);

        let mut budget = ExpansionBudget::new("segmentize", "max_length");
        budget.check_additional(generated).unwrap();
        budget.add(generated).unwrap();
        assert_eq!(budget.used(), GENERATED_ITEM_LIMIT);

        let plans = [SegmentPlan {
            route: SegmentRoutePlan::Planar,
            length: 0.0,
            steps: GENERATED_ITEM_LIMIT + 2,
        }];
        let mut count = SubdivisionCount { emitted: 0 };
        emit_subdivision(&plans, &mut count).unwrap();
        let generated = count.generated(2);
        let over_budget = ExpansionBudget::new("segmentize", "max_length");
        let error = over_budget.check_additional(generated).unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(
                crate::geometry::GeometryErrorKind::GeneratedOutputTooLarge {
                    operation: "segmentize",
                    parameter: "max_length",
                    ..
                }
            )
        ));
    }

    #[test]
    fn subdivision_rejects_coordinate_width_before_reserving_columns() {
        let points = CoordSeq::try_from_xy(&[XY::new(0.0, 0.0), XY::new(1.0, 0.0)]).unwrap();
        let error = SubdivisionColumns::new(&points, &[], usize::MAX)
            .err()
            .expect("unrepresentable coordinate width is rejected");
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(
                crate::geometry::GeometryErrorKind::OffsetCapacityExceeded
            )
        ));
    }

    #[test]
    fn segmentize_planning_sentinel_is_reserved_for_more_than_the_limit() {
        assert_eq!(
            segmentize_steps((GENERATED_ITEM_LIMIT + 1) as f64, 1.0),
            GENERATED_ITEM_LIMIT + 1
        );
        assert_eq!(
            segmentize_steps((GENERATED_ITEM_LIMIT + 2) as f64, 1.0),
            GENERATED_ITEM_LIMIT + 2
        );
    }

    #[test]
    fn fractional_step_count_does_not_include_prior_budget_usage() {
        let line = CoordSeq::try_from_xy(&[XY::new(0.0, 0.0), XY::new(10.0, 10.0)]).unwrap();
        let mut budget = ExpansionBudget::new("segmentize", "fraction");
        let first = densify_points_budgeted_with_placement(
            &line,
            0.5,
            SegmentPlacement::Planar,
            &mut budget,
        )
        .unwrap();
        let second = densify_points_budgeted_with_placement(
            &line,
            0.5,
            SegmentPlacement::Planar,
            &mut budget,
        )
        .unwrap();

        assert_eq!(first.len(), 3);
        assert_eq!(second.len(), 3);
        assert_eq!(budget.used(), 2);
    }

    #[test]
    fn planar_segmentize_planner_keeps_the_exact_generated_boundary() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new((GENERATED_ITEM_LIMIT + 1) as f64, 0.0),
        };
        assert_eq!(
            planar_segmentize_steps(segment, 1.0),
            GENERATED_ITEM_LIMIT + 1
        );
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new((GENERATED_ITEM_LIMIT + 2) as f64, 0.0),
        };
        assert_eq!(
            planar_segmentize_steps(segment, 1.0),
            GENERATED_ITEM_LIMIT + 2
        );
    }
}
