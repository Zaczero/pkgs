use super::output::{OutputColumns, materialize};
use super::*;

pub(super) fn segmentize_steps(length: f64, max_segment_length: f64) -> usize {
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

pub(super) trait Route {
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
        &mut self,
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

#[derive(Clone, Copy)]
pub(super) enum Target<'a> {
    MaxLength(f64),
    Fraction(usize),
    Pieces(&'a [NonZeroUsize]),
}

pub(super) struct Plan<S> {
    pub(super) steps: Vec<usize>,
    pub(super) solutions: Vec<S>,
    pub(super) generated: usize,
    /// An unsplit source chord may still violate MaxLength.
    pub(super) source_exceeds_max_length: bool,
}

pub(super) fn plan<R: Route>(
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

const MAX_REPRESENTABILITY_RETRIES: usize = 1;

pub(super) fn subdivide<R: Route>(
    route: &mut R,
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
                match verify_segmentize_output(&*route, &output, &plan, generated, max_length) {
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
pub(super) fn verify_segmentize_output<R: Route>(
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
