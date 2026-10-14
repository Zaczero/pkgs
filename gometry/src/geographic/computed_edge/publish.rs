use super::aliases::{ZeroLongitudeAlias, zero_longitude_aliases};
use super::intents::{collect_edge_intents, sample_xy_ordinates};
use super::vertices::{SampledVertex, observe_sampled_vertex, publish_vertex_at};
use super::{
    ChartStep, ComputedEdgeIntent, ComputedLongitudeRelation, ComputedSampleOrdinates,
    ComputedVertexIntent, CoordSeq, Coordinates, EdgeDirection, ExpansionBudget, GeographicError,
    GeographicFrame, GeometryErrorKind, Point, Result, RetainedComputedSample,
    RetainedLongitudeClass,
};

/// Canonicalize a computed chain and split only edges whose canonical writing
/// no longer reads the producer-retained displacement.
///
/// `None` means every record was already canonical and every edge was
/// representable without inserting a vertex. A closed ring retains its first
/// vertex and uses that complete canonical record at closure.
pub(crate) fn publish_computed_chain<'intent, P: Coordinates + ?Sized>(
    frame: GeographicFrame,
    points: &P,
    closed_ring: bool,
    budget: &mut ExpansionBudget,
    intent: impl Fn(usize, Point, Point) -> Result<ComputedEdgeIntent<'intent>>,
) -> Result<Option<Vec<Point>>> {
    if points.coord_count() == 0 {
        return Ok(None);
    }
    let edge_intents = collect_edge_intents(points, intent)?;
    publish_chain_with_aliases(
        frame,
        points,
        closed_ring,
        budget,
        &edge_intents,
        |_, _| ComputedVertexIntent::Canonical,
        |edge_index, endpoints, relation, parameter, retained| {
            sample_xy_ordinates(edge_index, endpoints, relation, parameter, retained)
        },
        |_, _, _, _, _, _| Ok(()),
        &[],
        None,
    )
}

pub(crate) fn publish_certified_minor_chain(
    frame: GeographicFrame,
    points: &CoordSeq,
    closed_ring: bool,
    budget: &mut ExpansionBudget,
) -> Result<Option<Vec<Point>>> {
    publish_certified_minor_chain_with_vertex_intents(frame, points, closed_ring, budget, |_, _| {
        ComputedVertexIntent::Canonical
    })
}

pub(crate) fn publish_preserved_minor_chain(
    frame: GeographicFrame,
    points: &CoordSeq,
    closed_ring: bool,
    budget: &mut ExpansionBudget,
) -> Result<Option<Vec<Point>>> {
    publish_certified_minor_chain_with_vertex_intents(frame, points, closed_ring, budget, |_, _| {
        ComputedVertexIntent::Preserved
    })
}

fn publish_certified_minor_chain_with_vertex_intents(
    frame: GeographicFrame,
    points: &CoordSeq,
    closed_ring: bool,
    budget: &mut ExpansionBudget,
    vertex_intent: impl Fn(usize, Point) -> ComputedVertexIntent,
) -> Result<Option<Vec<Point>>> {
    let bound = frame.longitude().ordinary_longitude_bound();
    let canonical =
        points.xs().iter().zip(points.ys()).all(|(&x, &y)| {
            x.abs() < bound && x.to_bits() != (-0.0_f64).to_bits() && y.is_finite()
        });
    if canonical && (!closed_ring || points.first_coord() == points.last_coord()) {
        debug_assert!(
            points
                .segment_pairs()
                .all(|[start, end]| certified_minor_reads(frame, start, end)),
            "local projected-edge certificate must exclude half-period and major readings"
        );
        return Ok(None);
    }
    publish_computed_chain_with_vertex_intents(
        frame,
        points,
        closed_ring,
        budget,
        |_, _, _| Ok(ComputedEdgeIntent::CertifiedMinor),
        vertex_intent,
    )
}

pub(crate) fn publish_computed_chain_with_vertex_intents<'intent, P: Coordinates + ?Sized>(
    frame: GeographicFrame,
    points: &P,
    closed_ring: bool,
    budget: &mut ExpansionBudget,
    intent: impl Fn(usize, Point, Point) -> Result<ComputedEdgeIntent<'intent>>,
    vertex_intent: impl Fn(usize, Point) -> ComputedVertexIntent,
) -> Result<Option<Vec<Point>>> {
    publish_computed_chain_with_vertex_intents_and_samples(
        frame,
        points,
        closed_ring,
        budget,
        intent,
        vertex_intent,
        |edge_index, endpoints, relation, parameter, retained| {
            sample_xy_ordinates(edge_index, endpoints, relation, parameter, retained)
        },
        |_, _, _, _, _, _| Ok(()),
    )
}

pub(crate) fn publish_computed_chain_with_vertex_intents_and_samples<
    'intent,
    P: Coordinates + ?Sized,
>(
    frame: GeographicFrame,
    points: &P,
    closed_ring: bool,
    budget: &mut ExpansionBudget,
    intent: impl Fn(usize, Point, Point) -> Result<ComputedEdgeIntent<'intent>>,
    vertex_intent: impl Fn(usize, Point) -> ComputedVertexIntent,
    samples: impl FnMut(
        usize,
        &[Point; 2],
        &ComputedLongitudeRelation<'intent>,
        f64,
        &RetainedComputedSample,
    ) -> Result<ComputedSampleOrdinates>,
    on_published: impl FnMut(
        usize,
        &[Point; 2],
        &ComputedLongitudeRelation<'intent>,
        f64,
        &RetainedComputedSample,
        Point,
    ) -> Result<()>,
) -> Result<Option<Vec<Point>>> {
    publish_computed_chain_with_vertex_intents_and_samples_with_edge_ranges(
        frame,
        points,
        closed_ring,
        budget,
        intent,
        vertex_intent,
        samples,
        on_published,
        None,
    )
}

pub(crate) fn publish_computed_chain_with_vertex_intents_and_samples_with_edge_ranges<
    'intent,
    P: Coordinates + ?Sized,
>(
    frame: GeographicFrame,
    points: &P,
    closed_ring: bool,
    budget: &mut ExpansionBudget,
    intent: impl Fn(usize, Point, Point) -> Result<ComputedEdgeIntent<'intent>>,
    vertex_intent: impl Fn(usize, Point) -> ComputedVertexIntent,
    samples: impl FnMut(
        usize,
        &[Point; 2],
        &ComputedLongitudeRelation<'intent>,
        f64,
        &RetainedComputedSample,
    ) -> Result<ComputedSampleOrdinates>,
    on_published: impl FnMut(
        usize,
        &[Point; 2],
        &ComputedLongitudeRelation<'intent>,
        f64,
        &RetainedComputedSample,
        Point,
    ) -> Result<()>,
    mut edge_ranges: Option<&mut Vec<(usize, usize)>>,
) -> Result<Option<Vec<Point>>> {
    let point_count = points.coord_count();
    if point_count == 0 {
        return Ok(None);
    }
    if let Some(edge_ranges) = edge_ranges.as_deref_mut() {
        edge_ranges.clear();
        edge_ranges.reserve(point_count.saturating_sub(1));
    }
    let edge_intents = collect_edge_intents(points, intent)?;
    let zero_aliases = zero_longitude_aliases(
        frame,
        points,
        closed_ring,
        &edge_intents,
        &vertex_intent,
        budget.operation(),
    )?;
    publish_chain_with_aliases(
        frame,
        points,
        closed_ring,
        budget,
        &edge_intents,
        vertex_intent,
        samples,
        on_published,
        &zero_aliases,
        edge_ranges,
    )
}

fn publish_chain_with_aliases<'intent, P: Coordinates + ?Sized>(
    frame: GeographicFrame,
    points: &P,
    closed_ring: bool,
    budget: &mut ExpansionBudget,
    edge_intents: &[ComputedEdgeIntent<'intent>],
    vertex_intent: impl Fn(usize, Point) -> ComputedVertexIntent,
    mut samples: impl FnMut(
        usize,
        &[Point; 2],
        &ComputedLongitudeRelation<'intent>,
        f64,
        &RetainedComputedSample,
    ) -> Result<ComputedSampleOrdinates>,
    mut on_published: impl FnMut(
        usize,
        &[Point; 2],
        &ComputedLongitudeRelation<'intent>,
        f64,
        &RetainedComputedSample,
        Point,
    ) -> Result<()>,
    zero_aliases: &[ZeroLongitudeAlias],
    mut edge_ranges: Option<&mut Vec<(usize, usize)>>,
) -> Result<Option<Vec<Point>>> {
    let point_count = points.coord_count();
    if point_count == 0 {
        return Ok(None);
    }
    let edge_count = point_count.saturating_sub(1);
    let first_source = points.nth_coord(0);
    let (first, first_sample) = publish_vertex_at(
        frame,
        points,
        first_source,
        0,
        vertex_intent(0, first_source),
        edge_intents,
        point_count,
        closed_ring,
        Some((0, 0.0)),
        &mut samples,
        zero_aliases,
    )?;
    let mut output = (first != first_source || first_sample.is_some()).then(|| vec![first]);
    observe_sampled_vertex(first_sample.as_ref(), first, &mut on_published)?;
    let mut canonical_start = first;

    for index in 0..edge_count {
        let edge_start_index = match output.as_ref() {
            Some(written) => written
                .len()
                .checked_sub(1)
                .ok_or_else(|| GeometryErrorKind::message("computed output has no edge start"))?,
            None => index,
        };
        let source_start = points.nth_coord(index);
        let source_end = points.nth_coord(index.wrapping_add(1));
        let edge_intent = *edge_intents
            .get(index)
            .expect("publication edge belongs to the retained intent inventory");
        let (published_end, end_sample) = publish_vertex_at(
            frame,
            points,
            source_end,
            index.wrapping_add(1),
            vertex_intent(index.wrapping_add(1), source_end),
            edge_intents,
            point_count,
            closed_ring,
            Some((index, 1.0)),
            &mut samples,
            zero_aliases,
        )?;
        let canonical_end = if closed_ring && index.wrapping_add(1) == edge_count {
            first
        } else {
            published_end
        };
        match edge_intent {
            ComputedEdgeIntent::Preserved { .. } => {
                if let Some(emitted) = output.as_mut() {
                    emitted.push(canonical_end);
                } else if canonical_end != source_end {
                    output = Some(canonical_prefix(
                        frame,
                        points,
                        index.wrapping_add(1),
                        closed_ring,
                        edge_intents,
                        &vertex_intent,
                        &mut samples,
                        zero_aliases,
                    )?);
                    output
                        .as_mut()
                        .expect("canonicalizing an endpoint allocates the output chain")
                        .push(canonical_end);
                }
                observe_sampled_vertex(end_sample.as_ref(), canonical_end, &mut on_published)?;
                canonical_start = canonical_end;
            },
            ComputedEdgeIntent::CertifiedMinor => {
                debug_assert!(
                    certified_minor_reads(frame, canonical_start, canonical_end),
                    "local projected-edge certificate must exclude half-period and major readings"
                );
                if let Some(emitted) = output.as_mut() {
                    emitted.push(canonical_end);
                } else if canonical_end != source_end {
                    output = Some(canonical_prefix(
                        frame,
                        points,
                        index.wrapping_add(1),
                        closed_ring,
                        edge_intents,
                        &vertex_intent,
                        &mut samples,
                        zero_aliases,
                    )?);
                    output
                        .as_mut()
                        .expect("canonicalizing an endpoint allocates the output chain")
                        .push(canonical_end);
                }
                observe_sampled_vertex(end_sample.as_ref(), canonical_end, &mut on_published)?;
                canonical_start = canonical_end;
            },
            ComputedEdgeIntent::Retained(relation) => {
                publish_retained_edge(
                    frame,
                    &[source_start, source_end],
                    canonical_start,
                    canonical_end,
                    relation,
                    end_sample,
                    points,
                    index,
                    budget,
                    closed_ring,
                    edge_intents,
                    &vertex_intent,
                    &mut samples,
                    &mut on_published,
                    zero_aliases,
                    &mut output,
                )?;
                canonical_start = canonical_end;
            },
        }
        if let Some(edge_ranges) = edge_ranges.as_deref_mut() {
            let edge_end_index = match output.as_ref() {
                Some(written) => written
                    .len()
                    .checked_sub(1)
                    .ok_or_else(|| GeometryErrorKind::message("computed output has no edge end"))?,
                None => index
                    .checked_add(1)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?,
            };
            edge_ranges.push((edge_start_index, edge_end_index));
        }
    }

    if closed_ring {
        debug_assert!(
            point_count >= 2,
            "a closed computed chain repeats its endpoint record"
        );
    }

    Ok(output)
}

#[expect(
    clippy::too_many_arguments,
    reason = "one publisher owns exact relation, original ordinal/parameters, carry callback and expansion budget"
)]
#[expect(
    clippy::float_cmp,
    reason = "interval endpoints are exact publisher parameters and a repeated midpoint means subdivision cannot make representable progress"
)]
fn publish_retained_edge<'intent, P: Coordinates + ?Sized>(
    frame: GeographicFrame,
    source_endpoints: &[Point; 2],
    canonical_start: Point,
    canonical_end: Point,
    relation: ComputedLongitudeRelation<'intent>,
    mut end_sample: Option<SampledVertex<'intent>>,
    source_points: &P,
    edge_index: usize,
    budget: &mut ExpansionBudget,
    closed_ring: bool,
    edge_intents: &[ComputedEdgeIntent<'intent>],
    vertex_intent: &impl Fn(usize, Point) -> ComputedVertexIntent,
    samples: &mut impl FnMut(
        usize,
        &[Point; 2],
        &ComputedLongitudeRelation<'intent>,
        f64,
        &RetainedComputedSample,
    ) -> Result<ComputedSampleOrdinates>,
    on_published: &mut impl FnMut(
        usize,
        &[Point; 2],
        &ComputedLongitudeRelation<'intent>,
        f64,
        &RetainedComputedSample,
        Point,
    ) -> Result<()>,
    zero_aliases: &[ZeroLongitudeAlias],
    output: &mut Option<Vec<Point>>,
) -> Result<()> {
    if relation.reads_as(frame, canonical_start.x, canonical_end.x, 0.0, 1.0)? {
        if let Some(emitted) = output.as_mut() {
            emitted.push(canonical_end);
        } else if canonical_end != source_endpoints[1] || end_sample.is_some() {
            *output = Some(canonical_prefix(
                frame,
                source_points,
                edge_index.wrapping_add(1),
                closed_ring,
                edge_intents,
                vertex_intent,
                samples,
                zero_aliases,
            )?);
            output
                .as_mut()
                .expect("canonicalizing an endpoint allocates the output chain")
                .push(canonical_end);
        }
        observe_sampled_vertex(end_sample.as_ref(), canonical_end, on_published)?;
        return Ok(());
    }

    let piece_count = relation.split_piece_count(frame.longitude())?;
    // `strict_subhalf_piece_count` returns at least two pieces.
    budget.check_additional(piece_count.wrapping_sub(1))?;
    if output.is_none() {
        *output = Some(canonical_prefix(
            frame,
            source_points,
            edge_index.wrapping_add(1),
            closed_ring,
            edge_intents,
            vertex_intent,
            samples,
            zero_aliases,
        )?);
    }

    let mut pending = vec![(0.0, 1.0)];
    while let Some((start_t, end_t)) = pending.pop() {
        let start_x = if start_t == 0.0 {
            canonical_start.x
        } else {
            output
                .as_ref()
                .and_then(|points| points.last())
                .expect("a left subdivision piece emits its shared endpoint")
                .x
        };
        let mut generated_retained = None;
        let end_x = if end_t == 1.0 {
            canonical_end.x
        } else {
            let retained = relation.sample(frame.longitude(), end_t)?;
            let longitude = retained.longitude.get();
            generated_retained = Some(retained);
            longitude
        };
        let class = relation.class(frame.longitude(), start_t, end_t)?;
        let subhalf = matches!(
            class,
            RetainedLongitudeClass::Zero | RetainedLongitudeClass::Minor(_)
        );
        if subhalf && relation.reads_as(frame, start_x, end_x, start_t, end_t)? {
            let (end, sampled_end) = if end_t == 1.0 {
                (canonical_end, end_sample.take())
            } else {
                let retained = generated_retained
                    .take()
                    .expect("an interior subdivision endpoint has a retained sample");
                let point = publish_sampled_point(
                    edge_index,
                    source_endpoints,
                    relation,
                    end_t,
                    &retained,
                    samples,
                )?;
                (
                    point,
                    Some(SampledVertex {
                        edge_index,
                        endpoints: *source_endpoints,
                        relation,
                        parameter: end_t,
                        retained,
                    }),
                )
            };
            output
                .as_mut()
                .expect("subdivision allocates the output chain before visiting pieces")
                .push(end);
            observe_sampled_vertex(sampled_end.as_ref(), end, on_published)?;
            continue;
        }

        let middle_t = f64::midpoint(start_t, end_t);
        if middle_t == start_t || middle_t == end_t {
            return Err(GeometryErrorKind::unrepresentable_output("computed edge"));
        }
        budget.add(1)?;
        pending.push((middle_t, end_t));
        pending.push((start_t, middle_t));
    }
    Ok(())
}

fn publish_sampled_point<'intent>(
    edge_index: usize,
    endpoints: &[Point; 2],
    relation: ComputedLongitudeRelation<'intent>,
    parameter: f64,
    retained: &RetainedComputedSample,
    samples: &mut impl FnMut(
        usize,
        &[Point; 2],
        &ComputedLongitudeRelation<'intent>,
        f64,
        &RetainedComputedSample,
    ) -> Result<ComputedSampleOrdinates>,
) -> Result<Point> {
    let ordinates = samples(edge_index, endpoints, &relation, parameter, retained)?;
    let point = Point::new_axes(
        retained.longitude.get(),
        ordinates.y,
        ordinates.z,
        ordinates.m,
    )?;
    Ok(point)
}

fn certified_minor_reads(frame: GeographicFrame, start: Point, end: Point) -> bool {
    matches!(
        frame.longitude().chart_step(start.x, end.x),
        Ok(ChartStep::Shortest(EdgeDirection::Minor(delta)))
            if delta.abs() < frame.longitude().half_period()
    )
}

fn canonical_prefix<'intent, P: Coordinates + ?Sized>(
    frame: GeographicFrame,
    points: &P,
    end: usize,
    closed_ring: bool,
    edge_intents: &[ComputedEdgeIntent<'intent>],
    vertex_intent: &impl Fn(usize, Point) -> ComputedVertexIntent,
    samples: &mut impl FnMut(
        usize,
        &[Point; 2],
        &ComputedLongitudeRelation<'intent>,
        f64,
        &RetainedComputedSample,
    ) -> Result<ComputedSampleOrdinates>,
    zero_aliases: &[ZeroLongitudeAlias],
) -> Result<Vec<Point>> {
    // Every caller passes edge_index+1 inside the immutable edge loop:
    // edge_count is coord_count-1, so this prefix end is always in range.
    let mut output = Vec::with_capacity(points.coord_count());
    for index in 0..end {
        let point = points.nth_coord(index);
        let preferred = if index == 0 {
            Some((0, 0.0))
        } else {
            Some((index.wrapping_sub(1), 1.0))
        };
        let (point, _) = publish_vertex_at(
            frame,
            points,
            point,
            index,
            vertex_intent(index, point),
            edge_intents,
            points.coord_count(),
            closed_ring,
            preferred,
            samples,
            zero_aliases,
        )?;
        output.push(point);
    }
    Ok(output)
}

pub(super) fn map_geographic_error(error: GeographicError) -> crate::error::Error {
    match error {
        GeographicError::NonFinite { .. } => GeometryErrorKind::NonFiniteCoordinate.into(),
        GeographicError::LatitudeOutOfRange { value } => crate::crs::CrsError::invalid(format!(
            "latitude {value:e} is outside the geographic coordinate domain"
        )),
        other @ (GeographicError::InvalidAngularFactor { .. }
        | GeographicError::ChartTurnOverflow
        | GeographicError::AmbiguousFullTurnRing { .. }) => {
            crate::crs::CrsError::invalid(other.to_string())
        },
    }
}
