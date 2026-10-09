//! Route-aware subdivision of coordinate sequences.

use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::Arc;

use crate::geographic::GeographicFrame;
use crate::geometry::constructive::Result;
use crate::geometry::types::ensure_coordseq_vertex_capacity;
use crate::geometry::{
    CoordSeq, Coordinates as _, ExpansionBudget, GENERATED_ITEM_LIMIT, GeometryErrorKind, Point,
    Segment, XY, axis_pow2_scale, interpolate_subdivision_ordinate, line_segments, point_distance,
    same_point, scaled_residual,
};

pub(crate) fn segmentize_points_budgeted(
    points: &CoordSeq,
    max_segment_length: f64,
    placement: &SegmentPlacement<'_>,
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    subdivide_placed(
        points,
        placement,
        Target::MaxLength(max_segment_length),
        budget,
    )
}

/// Place chart-edge vertices using counts certified by the geographic local
/// projection certificate. The ordinary subdivision path provides route
/// evaluation, ordinate interpolation, output columns, and the caller-owned
/// generated-work budget.
pub(crate) fn segmentize_points_with_piece_counts_budgeted(
    points: &CoordSeq,
    piece_counts: &[NonZeroUsize],
    placement: &SegmentPlacement<'_>,
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    if piece_counts.len() != points.len().saturating_sub(1) {
        return Err(GeometryErrorKind::message(
            "chart-edge piece counts do not match the coordinate sequence",
        ));
    }
    if piece_counts.iter().all(|pieces| pieces.get() == 1) {
        return Ok(points.clone());
    }
    subdivide_placed(points, placement, Target::Pieces(piece_counts), budget)
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
#[cfg(test)]
fn planar_segmentize_steps(segment: Segment, max_segment_length: f64) -> usize {
    if !max_segment_length.is_finite() || max_segment_length <= 0.0 {
        return 1;
    }
    let length = point_distance(segment.start, segment.end);
    planar_segmentize_steps_from_length(segment, length, max_segment_length)
}

fn planar_segmentize_steps_from_length(
    segment: Segment,
    length: f64,
    max_segment_length: f64,
) -> usize {
    if !max_segment_length.is_finite() || max_segment_length <= 0.0 {
        return 1;
    }
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
    densify_points_budgeted_with_placement(points, fraction, &SegmentPlacement::Planar, budget)
}

pub(crate) fn densify_points_budgeted_with_placement(
    points: &CoordSeq,
    fraction: f64,
    placement: &SegmentPlacement<'_>,
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    let steps = (1.0 / fraction).ceil() as usize;
    subdivide_placed(points, placement, Target::Fraction(steps), budget)
}

#[derive(Clone, Copy)]
struct DirectedLength {
    bearing: f64,
    length: f64,
    longitude_delta_native: f64,
}

trait Route {
    type Solution: Copy;

    /// Returns a reusable solution and the same route length as `measure`.
    fn solve(&self, segment: Segment, target: Target<'_>) -> Result<(f64, Self::Solution)>;

    fn splittable(&self, _segment: Segment, length: f64) -> bool {
        length > 0.0
    }

    fn pieces(&self, _segment: Segment, length: f64, max_length: f64) -> usize {
        segmentize_steps(length, max_length)
    }

    fn interior(
        &self,
        segment: Segment,
        solution: Self::Solution,
        pieces: usize,
        source: &CoordSeq,
        source_segment: usize,
        output: &mut OutputColumns,
        budget: &ExpansionBudget,
        planned_generated: usize,
        refined_generated: &mut usize,
    ) -> Result<()>;

    fn measure(&self, segment: Segment) -> Result<f64>;
}

struct Planar;

struct Chart<'a> {
    geodesic: &'a geographiclib_rs::Geodesic,
    frame: GeographicFrame,
}

struct GeodesicRoute<'a> {
    geodesic: &'a geographiclib_rs::Geodesic,
    frame: GeographicFrame,
}

struct Rhumb<'a> {
    crs: &'a str,
    state: &'a crate::crs::RuntimeState,
    frame: GeographicFrame,
}

#[derive(Clone, Copy)]
enum Target<'a> {
    MaxLength(f64),
    Fraction(usize),
    Pieces(&'a [NonZeroUsize]),
}

struct Plan<S> {
    steps: Vec<usize>,
    solutions: Vec<S>,
    generated: usize,
    /// An unsplit source chord may still violate MaxLength.
    source_exceeds_max_length: bool,
}

const fn point_from_xy(point: XY) -> Point {
    Point::new_raw_xy(point.x, point.y)
}

/// Geodesic and rhumb paths are defined by their endpoints, so an explicit
/// full-turn chart edge spans no longitude here; only the chart route reads it
/// as a circle.
fn lifted_longitude_delta(frame: GeographicFrame, segment: Segment) -> f64 {
    let axis = frame.longitude();
    axis.shortest_alias_step(segment.start.x, segment.end.x)
        .expect("geographic subdivision received admitted coordinates")
        .delta(axis)
}

impl Route for Planar {
    type Solution = ();

    fn solve(&self, segment: Segment, target: Target<'_>) -> Result<(f64, Self::Solution)> {
        let length = match target {
            Target::MaxLength(max_length) if max_length.is_finite() && max_length > 0.0 => {
                point_distance(segment.start, segment.end)
            },
            Target::MaxLength(_) | Target::Fraction(_) | Target::Pieces(_) => 0.0,
        };
        Ok((length, ()))
    }

    fn splittable(&self, segment: Segment, _length: f64) -> bool {
        !same_point(segment.start, segment.end)
    }

    fn pieces(&self, segment: Segment, length: f64, max_length: f64) -> usize {
        planar_segmentize_steps_from_length(segment, length, max_length)
    }

    fn interior(
        &self,
        segment: Segment,
        (): Self::Solution,
        pieces: usize,
        source: &CoordSeq,
        source_segment: usize,
        output: &mut OutputColumns,
        _budget: &ExpansionBudget,
        _planned_generated: usize,
        _refined_generated: &mut usize,
    ) -> Result<()> {
        push_interpolated(&mut output.xs, segment.start.x, segment.end.x, pieces);
        push_interpolated(&mut output.ys, segment.start.y, segment.end.y, pieces);
        output.push_ordinates(source, source_segment, pieces);
        Ok(())
    }

    fn measure(&self, segment: Segment) -> Result<f64> {
        Ok(point_distance(segment.start, segment.end))
    }
}

impl Route for Chart<'_> {
    type Solution = f64;

    fn solve(&self, segment: Segment, _target: Target<'_>) -> Result<(f64, Self::Solution)> {
        let edge = crate::crs::ChartEdge::new(
            point_from_xy(segment.start),
            point_from_xy(segment.end),
            self.frame,
        )?;
        let length = edge.length(self.geodesic)?;
        Ok((length, length))
    }

    fn interior(
        &self,
        segment: Segment,
        length: Self::Solution,
        pieces: usize,
        source: &CoordSeq,
        source_segment: usize,
        output: &mut OutputColumns,
        budget: &ExpansionBudget,
        planned_generated: usize,
        refined_generated: &mut usize,
    ) -> Result<()> {
        let edge = crate::crs::ChartEdge::new(
            point_from_xy(segment.start),
            point_from_xy(segment.end),
            self.frame,
        )?;
        emit_chart_plan(
            source,
            source_segment,
            &edge,
            self.geodesic,
            length,
            pieces,
            output,
            budget,
            planned_generated,
            refined_generated,
        )
    }

    fn measure(&self, segment: Segment) -> Result<f64> {
        crate::crs::ChartEdge::new(
            point_from_xy(segment.start),
            point_from_xy(segment.end),
            self.frame,
        )?
        .length(self.geodesic)
    }
}

impl Route for GeodesicRoute<'_> {
    type Solution = DirectedLength;

    fn solve(&self, segment: Segment, _target: Target<'_>) -> Result<(f64, Self::Solution)> {
        let start = self
            .frame
            .to_geodesic_point(point_from_xy(segment.start))
            .expect("geographic subdivision received admitted coordinates");
        let end = self
            .frame
            .to_geodesic_point(point_from_xy(segment.end))
            .expect("geographic subdivision received admitted coordinates");
        let longitude_delta_native = lifted_longitude_delta(self.frame, segment);
        let route = crate::crs::checked_geodesic_route(self.geodesic, start, end)?;
        Ok((route.length, DirectedLength {
            bearing: route.azimuth,
            length: route.length,
            longitude_delta_native,
        }))
    }

    fn interior(
        &self,
        segment: Segment,
        solution: Self::Solution,
        pieces: usize,
        source: &CoordSeq,
        source_segment: usize,
        output: &mut OutputColumns,
        _budget: &ExpansionBudget,
        _planned_generated: usize,
        _refined_generated: &mut usize,
    ) -> Result<()> {
        let start = self
            .frame
            .to_geodesic_point(point_from_xy(segment.start))
            .expect("geographic subdivision received admitted coordinates");
        let end = self
            .frame
            .to_geodesic_point(point_from_xy(segment.end))
            .expect("geographic subdivision received admitted coordinates");
        emit_geodesic_plan(
            self.geodesic,
            self.frame,
            segment,
            start,
            end,
            solution,
            pieces,
            &mut output.xs,
            &mut output.ys,
        )
        .map(|()| output.push_ordinates(source, source_segment, pieces))
    }

    fn measure(&self, segment: Segment) -> Result<f64> {
        let start = self
            .frame
            .to_geodesic_point(point_from_xy(segment.start))
            .expect("geographic verification received admitted coordinates");
        let end = self
            .frame
            .to_geodesic_point(point_from_xy(segment.end))
            .expect("geographic verification received admitted coordinates");
        Ok(crate::crs::checked_geodesic_route(self.geodesic, start, end)?.length)
    }
}

impl Route for Rhumb<'_> {
    type Solution = DirectedLength;

    fn solve(&self, segment: Segment, _target: Target<'_>) -> Result<(f64, Self::Solution)> {
        let start = self
            .frame
            .to_geodesic_point(point_from_xy(segment.start))
            .expect("rhumb subdivision received admitted coordinates");
        let end = self
            .frame
            .to_geodesic_point(point_from_xy(segment.end))
            .expect("rhumb subdivision received admitted coordinates");
        let length = crate::crs::rhumb_distance_crs_at(
            self.crs, start.x, start.y, end.x, end.y, self.state,
        )?;
        let bearing =
            crate::crs::rhumb_bearing_crs_at(self.crs, start.x, start.y, end.x, end.y, self.state)?;
        Ok((length, DirectedLength {
            bearing,
            length,
            longitude_delta_native: lifted_longitude_delta(self.frame, segment),
        }))
    }

    fn interior(
        &self,
        segment: Segment,
        solution: Self::Solution,
        pieces: usize,
        source: &CoordSeq,
        source_segment: usize,
        output: &mut OutputColumns,
        _budget: &ExpansionBudget,
        _planned_generated: usize,
        _refined_generated: &mut usize,
    ) -> Result<()> {
        let start = self
            .frame
            .to_geodesic_point(point_from_xy(segment.start))
            .expect("rhumb subdivision received admitted coordinates");
        let lifted_end = segment.start.x + solution.longitude_delta_native;
        for step in 1..pieces {
            let fraction = step as f64 / pieces as f64;
            let Some((longitude, latitude)) = crate::crs::rhumb_destination_crs_at(
                self.crs,
                start.x,
                start.y,
                solution.bearing,
                solution.length * fraction,
                self.state,
            )?
            else {
                return Err(GeometryErrorKind::message(
                    "rhumb subdivision calculation crosses a pole",
                ));
            };
            let lifted_reference =
                interpolate_subdivision_ordinate(segment.start.x, lifted_end, fraction);
            let point = self
                .frame
                .point_from_geodesic_in_lifted_chart(
                    Point::new_raw_xy(longitude, latitude),
                    lifted_reference,
                )
                .expect("rhumb subdivision produced a finite coordinate");
            if !(point.x.is_finite() && point.y.is_finite()) {
                return Err(GeometryErrorKind::message(
                    "rhumb subdivision calculation produced a non-finite coordinate",
                ));
            }
            output.xs.push(point.x);
            output.ys.push(point.y);
        }
        output.push_ordinates(source, source_segment, pieces);
        Ok(())
    }

    fn measure(&self, segment: Segment) -> Result<f64> {
        let start = self
            .frame
            .to_geodesic_point(point_from_xy(segment.start))
            .expect("rhumb verification received admitted coordinates");
        let end = self
            .frame
            .to_geodesic_point(point_from_xy(segment.end))
            .expect("rhumb verification received admitted coordinates");
        crate::crs::rhumb_distance_crs_at(self.crs, start.x, start.y, end.x, end.y, self.state)
    }
}

fn plan<R: Route>(
    route: &R,
    points: &CoordSeq,
    target: Target<'_>,
    retry: usize,
) -> Result<Plan<R::Solution>> {
    if let Target::Pieces(requested) = target {
        let edge_count = points.len().saturating_sub(1);
        if requested.len() != edge_count {
            return Err(GeometryErrorKind::message(
                "chart-edge piece counts do not match the coordinate sequence",
            ));
        }
        let mut steps = Vec::with_capacity(edge_count);
        let mut solutions = Vec::new();
        let mut generated = 0_usize;
        for (segment, &requested_pieces) in line_segments(points).zip(requested) {
            let pieces = requested_pieces.get();
            if pieces > 1 {
                // Only failed source edges need a ChartEdge solution. Safe
                // edges carry their certified count of one through the plan.
                let (_, solution) = route.solve(segment, target)?;
                solutions.push(solution);
                generated = generated.saturating_add(pieces.wrapping_sub(1));
            }
            steps.push(pieces);
        }
        return Ok(Plan {
            steps,
            solutions,
            generated,
            source_exceeds_max_length: false,
        });
    }

    let mut steps = Vec::with_capacity(points.len().saturating_sub(1));
    let mut solutions = Vec::new();
    let mut generated = 0_usize;
    let mut source_exceeds_max_length = false;
    for segment in line_segments(points) {
        let (length, solution) = route.solve(segment, target)?;
        let needs_splittable = matches!(target, Target::Fraction(_)) || retry > 0;
        let splittable = needs_splittable && route.splittable(segment, length);
        let mut pieces = match target {
            Target::MaxLength(max_length) => route.pieces(segment, length, max_length),
            Target::Fraction(fraction_pieces) if splittable => fraction_pieces,
            Target::Fraction(_) => 1,
            Target::Pieces(_) => unreachable!("piece-count targets were planned above"),
        };
        if retry > 0 && splittable {
            pieces = pieces
                .checked_add(retry)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        }
        if pieces == 1
            && let Target::MaxLength(max_length) = target
            && max_length.is_finite()
            && max_length > 0.0
        {
            source_exceeds_max_length |= length
                .partial_cmp(&max_length)
                .is_none_or(std::cmp::Ordering::is_gt);
        }
        if pieces > 1 {
            solutions.push(solution);
            generated = generated.saturating_add(pieces.wrapping_sub(1));
        }
        steps.push(pieces);
    }
    Ok(Plan {
        steps,
        solutions,
        generated,
        source_exceeds_max_length,
    })
}

fn subdivide_placed(
    points: &CoordSeq,
    placement: &SegmentPlacement<'_>,
    target: Target<'_>,
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    match placement {
        SegmentPlacement::Planar => subdivide(&Planar, points, target, budget),
        SegmentPlacement::Chart(geodesic, frame) => subdivide(
            &Chart {
                geodesic,
                frame: *frame,
            },
            points,
            target,
            budget,
        ),
        SegmentPlacement::Geodesic(geodesic, frame) => subdivide(
            &GeodesicRoute {
                geodesic,
                frame: *frame,
            },
            points,
            target,
            budget,
        ),
        SegmentPlacement::Rhumb { crs, state, frame } => subdivide(
            &Rhumb {
                crs,
                state,
                frame: *frame,
            },
            points,
            target,
            budget,
        ),
    }
}

const MAX_REPRESENTABILITY_RETRIES: usize = 1;

fn subdivide<R: Route>(
    route: &R,
    points: &CoordSeq,
    target: Target<'_>,
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    match target {
        Target::Fraction(_) | Target::Pieces(_) => {
            let plan = plan(route, points, target, 0)?;
            budget.check_additional(plan.generated)?;
            let (output, generated) = materialize(route, points, &plan, budget)?;
            budget.add(generated)?;
            Ok(output)
        },
        Target::MaxLength(max_length) => {
            let mut retry = 0;
            loop {
                let plan = plan(route, points, target, retry)?;
                budget.check_additional(plan.generated)?;
                let (output, generated) = materialize(route, points, &plan, budget)?;
                match verify_segmentize_output(route, &output, &plan, generated, max_length) {
                    Ok(()) => {
                        budget.add(generated)?;
                        return Ok(output);
                    },
                    Err(error)
                        if matches!(
                            error.kind(),
                            crate::error::ErrorKind::Geometry(
                                GeometryErrorKind::UnrepresentableOutput {
                                    operation: "segmentize",
                                    ..
                                }
                            )
                        ) && retry < MAX_REPRESENTABILITY_RETRIES =>
                    {
                        retry = MAX_REPRESENTABILITY_RETRIES;
                    },
                    Err(error) => return Err(error),
                }
            }
        },
    }
}

#[expect(
    clippy::debug_assert_with_mut_call,
    reason = "the final debug-only next checks that all planned chords were consumed"
)]
fn verify_segmentize_output<R: Route>(
    route: &R,
    points: &CoordSeq,
    plan: &Plan<R::Solution>,
    generated: usize,
    max_segment_length: f64,
) -> Result<()> {
    if !max_segment_length.is_finite() || max_segment_length <= 0.0 {
        return Ok(());
    }

    // Refined publication can add chords outside the planned piece counts.
    // Recheck the full chain in either case instead of trusting plan.steps.
    if plan.source_exceeds_max_length || generated > plan.generated {
        return verify_all_segmentize_output(route, points, max_segment_length);
    }

    // Every unsplit output chord is a source chord whose solved route length
    // is within the limit; only newly constructed chords need remeasurement.
    let mut output_segments = line_segments(points);
    for &pieces in &plan.steps {
        if pieces == 1 {
            output_segments
                .next()
                .expect("every source edge has one output chord");
            continue;
        }
        for _ in 0..pieces {
            let segment = output_segments
                .next()
                .expect("each planned piece has one output chord");
            if route
                .measure(segment)?
                .partial_cmp(&max_segment_length)
                .is_none_or(std::cmp::Ordering::is_gt)
            {
                return Err(GeometryErrorKind::unrepresentable_output("segmentize"));
            }
        }
    }
    debug_assert!(output_segments.next().is_none());
    Ok(())
}

fn verify_all_segmentize_output<R: Route>(
    route: &R,
    points: &CoordSeq,
    max_segment_length: f64,
) -> Result<()> {
    for segment in line_segments(points) {
        if route
            .measure(segment)?
            .partial_cmp(&max_segment_length)
            .is_none_or(std::cmp::Ordering::is_gt)
        {
            return Err(GeometryErrorKind::unrepresentable_output("segmentize"));
        }
    }
    Ok(())
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
        #[expect(
            clippy::rc_buffer,
            reason = "reuse the operation metric's canonical CRS allocation without copying the label"
        )]
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

struct OutputColumns {
    xs: Vec<f64>,
    ys: Vec<f64>,
    zs: Option<Vec<f64>>,
    ms: Option<Vec<f64>>,
}

impl OutputColumns {
    fn with_capacity(points: &CoordSeq, total: usize) -> Result<Self> {
        ensure_coordseq_vertex_capacity(total)?;
        Ok(Self {
            xs: Vec::with_capacity(total),
            ys: Vec::with_capacity(total),
            zs: points.zs().map(|_| Vec::with_capacity(total)),
            ms: points.ms().map(|_| Vec::with_capacity(total)),
        })
    }

    fn extend_sources(&mut self, points: &CoordSeq, range: Range<usize>) {
        self.xs.extend_from_slice(
            points
                .xs()
                .get(range.clone())
                .expect("subdivision source run belongs to the X column"),
        );
        self.ys.extend_from_slice(
            points
                .ys()
                .get(range.clone())
                .expect("subdivision source run belongs to the Y column"),
        );
        if let (Some(output), Some(column)) = (&mut self.zs, points.zs()) {
            output.extend_from_slice(
                column
                    .get(range.clone())
                    .expect("subdivision source run belongs to the Z column"),
            );
        }
        if let (Some(output), Some(column)) = (&mut self.ms, points.ms()) {
            output.extend_from_slice(
                column
                    .get(range)
                    .expect("subdivision source run belongs to the M column"),
            );
        }
    }

    fn push_ordinates(&mut self, points: &CoordSeq, segment: usize, pieces: usize) {
        if let (Some(output), Some(column)) = (&mut self.zs, points.zs()) {
            let start = *column
                .get(segment)
                .expect("subdivision Z segment start belongs to its column");
            let end = *column
                .get(segment.wrapping_add(1))
                .expect("subdivision Z segment end belongs to its column");
            push_interpolated(output, start, end, pieces);
        }
        if let (Some(output), Some(column)) = (&mut self.ms, points.ms()) {
            let start = *column
                .get(segment)
                .expect("subdivision M segment start belongs to its column");
            let end = *column
                .get(segment.wrapping_add(1))
                .expect("subdivision M segment end belongs to its column");
            push_interpolated(output, start, end, pieces);
        }
    }

    fn push_chart_point(
        &mut self,
        points: &CoordSeq,
        segment: usize,
        point: Point,
        fraction: f64,
    ) -> Result<()> {
        let length = self
            .xs
            .len()
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        ensure_coordseq_vertex_capacity(length)?;
        self.xs.push(point.x);
        self.ys.push(point.y);
        if let (Some(output), Some(column)) = (&mut self.zs, points.zs()) {
            let start = *column
                .get(segment)
                .expect("chart subdivision Z start belongs to its column");
            let end = *column
                .get(segment.wrapping_add(1))
                .expect("chart subdivision Z end belongs to its column");
            let value = interpolate_subdivision_ordinate(start, end, fraction);
            output.push(if value.is_finite() {
                value
            } else if fraction <= 0.5 {
                start
            } else {
                end
            });
        }
        if let (Some(output), Some(column)) = (&mut self.ms, points.ms()) {
            let start = *column
                .get(segment)
                .expect("chart subdivision M start belongs to its column");
            let end = *column
                .get(segment.wrapping_add(1))
                .expect("chart subdivision M end belongs to its column");
            let value = interpolate_subdivision_ordinate(start, end, fraction);
            output.push(if value.is_finite() {
                value
            } else if fraction <= 0.5 {
                start
            } else {
                end
            });
        }
        Ok(())
    }

    fn finish(self) -> Result<CoordSeq> {
        CoordSeq::try_from_vecs(self.xs, self.ys, self.zs, self.ms)
    }
}

fn push_interpolated(output: &mut Vec<f64>, start: f64, end: f64, pieces: usize) {
    for step in 1..pieces {
        let value = interpolate_subdivision_ordinate(start, end, step as f64 / pieces as f64);
        output.push(if value.is_finite() {
            value
        } else if step <= pieces / 2 {
            start
        } else {
            end
        });
    }
}

fn emit_geodesic_plan(
    geodesic: &geographiclib_rs::Geodesic,
    frame: GeographicFrame,
    segment: Segment,
    start: Point,
    end: Point,
    solution: DirectedLength,
    pieces: usize,
    xs: &mut Vec<f64>,
    ys: &mut Vec<f64>,
) -> Result<()> {
    let line = (start.x, start.y, solution.bearing, solution.length);
    let equatorial = start.y == 0.0 && end.y == 0.0 && (end.x - start.x).abs() < 180.0;
    let lifted_end = segment.start.x + solution.longitude_delta_native;
    for step in 1..pieces {
        let fraction = step as f64 / pieces as f64;
        let (mut longitude, mut latitude) = if equatorial {
            (
                interpolate_subdivision_ordinate(start.x, end.x, fraction),
                0.0,
            )
        } else {
            let info = crate::crs::geodesic_interpolate_on_line(geodesic, line, fraction)?;
            (info.longitude, info.latitude)
        };
        let lifted_reference =
            interpolate_subdivision_ordinate(segment.start.x, lifted_end, fraction);
        let point = frame
            .point_from_geodesic_in_lifted_chart(
                Point::new_raw_xy(longitude, latitude),
                lifted_reference,
            )
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

#[expect(
    clippy::float_cmp,
    reason = "source parameters are exact binary64 vertices of the emitted chain"
)]
fn emit_chart_plan(
    points: &CoordSeq,
    source_segment: usize,
    edge: &crate::crs::ChartEdge,
    geodesic: &geographiclib_rs::Geodesic,
    length: f64,
    pieces: usize,
    output: &mut OutputColumns,
    budget: &ExpansionBudget,
    planned_generated: usize,
    refined_generated: &mut usize,
) -> Result<()> {
    let source_start = points.nth_coord(source_segment);
    let source_end = points.nth_coord(source_segment.wrapping_add(1));
    let mut previous_parameter = 0.0;
    let mut last_emitted_parameter = None;
    for step in 1..=pieces {
        let fraction = step as f64 / pieces as f64;
        let parameter = if step == pieces {
            1.0
        } else if edge.is_parallel() {
            fraction
        } else {
            edge.locate_t(length * fraction, length, geodesic)?
        };
        if parameter <= previous_parameter || (step < pieces && parameter >= 1.0) {
            return Err(GeometryErrorKind::unrepresentable_output("segmentize"));
        }
        edge.publish_source_interval(
            source_start,
            source_end,
            previous_parameter,
            parameter,
            |parameter| edge.chart_position(parameter),
            |parameter, point, refined| {
                if parameter == 0.0 || parameter == 1.0 || last_emitted_parameter == Some(parameter)
                {
                    return Ok(());
                }
                if !(point.x.is_finite() && point.y.is_finite()) {
                    return Err(GeometryErrorKind::message(
                        "chart-edge subdivision calculation produced a non-finite coordinate",
                    ));
                }
                let ordinate_fraction = if refined {
                    edge.arclength_fraction(parameter, Some(length), geodesic)?
                } else {
                    fraction
                };
                if refined {
                    let additional = refined_generated
                        .checked_add(1)
                        .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    let requested = planned_generated
                        .checked_add(additional)
                        .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    budget.check_additional(requested)?;
                    *refined_generated = additional;
                }
                output.push_chart_point(points, source_segment, point, ordinate_fraction)?;
                last_emitted_parameter = Some(parameter);
                Ok(())
            },
            "segmentize",
        )
        .map_err(|error| {
            if matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(GeometryErrorKind::UnrepresentableOutput {
                    operation: "computed edge",
                    ..
                })
            ) {
                GeometryErrorKind::unrepresentable_output("segmentize")
            } else {
                error
            }
        })?;
        previous_parameter = parameter;
    }
    Ok(())
}

fn materialize<R: Route>(
    route: &R,
    points: &CoordSeq,
    plan: &Plan<R::Solution>,
    budget: &ExpansionBudget,
) -> Result<(CoordSeq, usize)> {
    let total = points
        .len()
        .checked_add(plan.generated)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    if plan.generated == 0 {
        return Ok((points.clone(), 0));
    }

    let mut output = OutputColumns::with_capacity(points, total)?;
    let mut copied = 0;
    let mut refined_generated = 0;
    let mut solutions = plan.solutions.iter();
    for (edge, (segment, &pieces)) in line_segments(points).zip(plan.steps.iter()).enumerate() {
        if pieces < 2 {
            continue;
        }
        output.extend_sources(points, copied..edge.wrapping_add(1));
        copied = edge.wrapping_add(1);
        let solution = *solutions
            .next()
            .expect("every split edge has one route solution");
        route.interior(
            segment,
            solution,
            pieces,
            points,
            edge,
            &mut output,
            budget,
            plan.generated,
            &mut refined_generated,
        )?;
    }
    output.extend_sources(points, copied..points.len());
    debug_assert_eq!(solutions.len(), 0);
    let generated = plan
        .generated
        .checked_add(refined_generated)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let expected_total = points
        .len()
        .checked_add(generated)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    debug_assert_eq!(output.xs.len(), expected_total);
    debug_assert_eq!(output.ys.len(), expected_total);
    Ok((output.finish()?, generated))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn retained_bytes<T>(values: &Vec<T>) -> usize {
        values.capacity().saturating_mul(std::mem::size_of::<T>())
    }

    #[test]
    fn subdivision_admits_exact_generated_limit_without_charging_input_vertices() {
        let at_limit = CoordSeq::try_from_xy(&[
            XY::new(0.0, 0.0),
            XY::new((GENERATED_ITEM_LIMIT + 1) as f64, 0.0),
        ])
        .unwrap();
        let at_limit = plan(&Planar, &at_limit, Target::MaxLength(1.0), 0).unwrap();
        assert_eq!(at_limit.steps, [GENERATED_ITEM_LIMIT + 1]);
        assert_eq!(at_limit.generated, GENERATED_ITEM_LIMIT);

        let mut budget = ExpansionBudget::new("segmentize", "max_length");
        budget.check_additional(at_limit.generated).unwrap();
        budget.add(at_limit.generated).unwrap();
        assert_eq!(budget.used(), GENERATED_ITEM_LIMIT);

        let over_limit = CoordSeq::try_from_xy(&[
            XY::new(0.0, 0.0),
            XY::new((GENERATED_ITEM_LIMIT + 2) as f64, 0.0),
        ])
        .unwrap();
        let over_limit = plan(&Planar, &over_limit, Target::MaxLength(1.0), 0).unwrap();
        assert_eq!(over_limit.generated, GENERATED_ITEM_LIMIT + 1);
        let over_budget = ExpansionBudget::new("segmentize", "max_length");
        let error = over_budget
            .check_additional(over_limit.generated)
            .unwrap_err();
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
        let plan = Plan {
            steps: vec![1],
            solutions: Vec::<()>::new(),
            generated: i32::MAX as usize,
            source_exceeds_max_length: false,
        };
        let budget = ExpansionBudget::new("segmentize", "max_length");
        let error = materialize(&Planar, &points, &plan, &budget)
            .expect_err("unrepresentable coordinate width is rejected");
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
            &SegmentPlacement::Planar,
            &mut budget,
        )
        .unwrap();
        let second = densify_points_budgeted_with_placement(
            &line,
            0.5,
            &SegmentPlacement::Planar,
            &mut budget,
        )
        .unwrap();

        assert_eq!(first.len(), 3);
        assert_eq!(second.len(), 3);
        assert_eq!(budget.used(), 2);
    }

    #[test]
    fn chart_piece_counts_refine_only_requested_edges_and_preserve_ordinates() {
        let line = CoordSeq::try_from_vecs(
            vec![0.0, 1.0, 1.0, 2.0],
            vec![0.0, 0.0, 1.0, 1.0],
            Some(vec![1.0, 2.0, 32.0, 33.0]),
            Some(vec![10.0, 20.0, 80.0, 90.0]),
        )
        .unwrap();
        let geodesic = geographiclib_rs::Geodesic::new(6_371_000.0, 0.0);
        let counts = [
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(3).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ];
        let mut budget = ExpansionBudget::new("buffer", "quadrant_segments");
        budget.add(7).unwrap();
        let refined = segmentize_points_with_piece_counts_budgeted(
            &line,
            &counts,
            &SegmentPlacement::Chart(&geodesic, GeographicFrame::crs84()),
            &mut budget,
        )
        .unwrap();

        assert_eq!(refined.xs(), &[0.0, 1.0, 1.0, 1.0, 1.0, 2.0]);
        assert_eq!(refined.ys(), &[0.0, 0.0, 1.0 / 3.0, 2.0 / 3.0, 1.0, 1.0]);
        assert_eq!(refined.zs().unwrap(), &[1.0, 2.0, 12.0, 22.0, 32.0, 33.0]);
        assert_eq!(refined.ms().unwrap(), &[10.0, 20.0, 40.0, 60.0, 80.0, 90.0]);
        assert_eq!(budget.used(), 9);
    }

    #[test]
    fn one_piece_counts_return_the_shared_source_without_allocating() {
        use crate::test_support::allocation_count;

        let points =
            CoordSeq::try_from_xy(&[XY::new(0.0, 0.0), XY::new(0.1, 0.0), XY::new(0.2, 0.0)])
                .unwrap();
        let counts = [NonZeroUsize::MIN, NonZeroUsize::MIN];
        let geodesic = geographiclib_rs::Geodesic::new(6_378_137.0, 1.0 / 298.257_223_563);
        let mut budget = ExpansionBudget::new("buffer", "quadrant_segments");

        let (refined, allocations) = allocation_count(|| {
            segmentize_points_with_piece_counts_budgeted(
                &points,
                &counts,
                &SegmentPlacement::Chart(&geodesic, GeographicFrame::crs84()),
                &mut budget,
            )
            .unwrap()
        });

        assert_eq!(allocations, 0);
        assert_eq!(refined.xs(), points.xs());
        assert_eq!(refined.ys(), points.ys());
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn no_op_plans_allocate_only_dense_steps_and_keep_no_route_solutions() {
        use crate::test_support::allocation_count;

        let coordinates: Vec<_> = (0..100_000)
            .map(|index| XY::new(f64::from(index) / 4.0, 0.0))
            .collect();
        let points = CoordSeq::try_from_xy(&coordinates).unwrap();
        let (planar_plan, allocations) =
            allocation_count(|| plan(&Planar, &points, Target::MaxLength(1.0), 0).unwrap());
        assert_eq!(allocations, 1);
        assert_eq!(planar_plan.steps.len(), points.len() - 1);
        assert_eq!(planar_plan.generated, 0);
        assert!(planar_plan.solutions.is_empty());
        assert_eq!(retained_bytes(&planar_plan.solutions), 0);

        let geographic = CoordSeq::try_from_xy(&[
            XY::new(0.0, 0.0),
            XY::new(0.000_001, 0.0),
            XY::new(0.000_002, 0.0),
        ])
        .unwrap();
        let geodesic = geographiclib_rs::Geodesic::new(6_378_137.0, 1.0 / 298.257_223_563);
        let frame = GeographicFrame::crs84();
        let chart_plan = plan(
            &Chart {
                geodesic: &geodesic,
                frame,
            },
            &geographic,
            Target::MaxLength(1.0),
            0,
        )
        .unwrap();
        let geodesic_plan = plan(
            &GeodesicRoute {
                geodesic: &geodesic,
                frame,
            },
            &geographic,
            Target::MaxLength(1.0),
            0,
        )
        .unwrap();
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let rhumb_plan = plan(
            &Rhumb {
                crs: "EPSG:4326",
                state: &state,
                frame,
            },
            &geographic,
            Target::MaxLength(1.0),
            0,
        )
        .unwrap();
        assert_eq!(chart_plan.generated, 0);
        assert_eq!(geodesic_plan.generated, 0);
        assert_eq!(rhumb_plan.generated, 0);
        assert_eq!(retained_bytes(&chart_plan.solutions), 0);
        assert_eq!(retained_bytes(&geodesic_plan.solutions), 0);
        assert_eq!(retained_bytes(&rhumb_plan.solutions), 0);
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

    #[test]
    fn planar_representability_retry_replans_and_charges_the_accepted_output_once() {
        let points = CoordSeq::try_from_xy(&[XY::new(0.0, 0.0), XY::new(1.0, 0.0)]).unwrap();
        let max_length = 1.0 / 3.0;

        let first = plan(&Planar, &points, Target::MaxLength(max_length), 0).unwrap();
        assert_eq!(first.steps, [3]);
        let budget = ExpansionBudget::new("segmentize", "max_length");
        let (first_output, first_generated) =
            materialize(&Planar, &points, &first, &budget).unwrap();
        let first_error =
            verify_segmentize_output(&Planar, &first_output, &first, first_generated, max_length)
                .unwrap_err();
        assert!(matches!(
            first_error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::UnrepresentableOutput {
                operation: "segmentize",
                ..
            })
        ));

        let mut budget = ExpansionBudget::new("segmentize", "max_length");
        let output =
            segmentize_points_budgeted(&points, max_length, &SegmentPlacement::Planar, &mut budget)
                .unwrap();
        assert_eq!(output.len(), 5);
        assert_eq!(output.xs(), &[0.0, 0.25, 0.5, 0.75, 1.0]);
        assert_eq!(budget.used(), output.len() - points.len());
        let accepted = plan(&Planar, &points, Target::MaxLength(max_length), 1).unwrap();
        verify_segmentize_output(&Planar, &output, &accepted, accepted.generated, max_length)
            .unwrap();
    }

    #[test]
    fn unsplit_source_length_keeps_the_full_verification_error_path() {
        struct Underplanned;

        impl Route for Underplanned {
            type Solution = ();

            fn solve(
                &self,
                _segment: Segment,
                _target: Target<'_>,
            ) -> Result<(f64, Self::Solution)> {
                Ok((2.0, ()))
            }

            fn pieces(&self, _segment: Segment, _length: f64, _max_length: f64) -> usize {
                1
            }

            fn interior(
                &self,
                _segment: Segment,
                (): Self::Solution,
                _pieces: usize,
                _source: &CoordSeq,
                _source_segment: usize,
                _output: &mut OutputColumns,
                _budget: &ExpansionBudget,
                _planned_generated: usize,
                _refined_generated: &mut usize,
            ) -> Result<()> {
                Ok(())
            }

            fn measure(&self, _segment: Segment) -> Result<f64> {
                Ok(2.0)
            }
        }

        let points = CoordSeq::try_from_xy(&[XY::new(0.0, 0.0), XY::new(2.0, 0.0)]).unwrap();
        let plan = plan(&Underplanned, &points, Target::MaxLength(1.0), 0).unwrap();
        assert!(plan.source_exceeds_max_length);
        let budget = ExpansionBudget::new("segmentize", "max_length");
        let (output, generated) = materialize(&Underplanned, &points, &plan, &budget).unwrap();
        let error =
            verify_segmentize_output(&Underplanned, &output, &plan, generated, 1.0).unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::UnrepresentableOutput {
                operation: "segmentize",
                ..
            })
        ));
    }

    #[test]
    fn zero_and_one_vertex_plans_share_input_and_generate_nothing() {
        let inputs = [
            CoordSeq::empty(crate::geometry::CoordinateAxes::XY),
            CoordSeq::try_from_xy(&[XY::new(1.0, 2.0)]).unwrap(),
        ];
        for points in &inputs {
            let mut segmentize_budget = ExpansionBudget::new("segmentize", "max_length");
            let segmented = segmentize_points_budgeted(
                points,
                1.0,
                &SegmentPlacement::Planar,
                &mut segmentize_budget,
            )
            .unwrap();
            assert_eq!(&segmented, points);
            assert_eq!(segmentize_budget.used(), 0);

            let mut densify_budget = ExpansionBudget::new("segmentize", "fraction");
            let densified = densify_points_budgeted_with_placement(
                points,
                f64::MIN_POSITIVE,
                &SegmentPlacement::Planar,
                &mut densify_budget,
            )
            .unwrap();
            assert_eq!(&densified, points);
            assert_eq!(densify_budget.used(), 0);
        }
    }
}
