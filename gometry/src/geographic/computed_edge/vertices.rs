use super::aliases::{ZeroLongitudeAlias, zero_longitude_alias_at};
use super::publish::map_geographic_error;
use super::*;

pub(super) struct SampledVertex<'intent> {
    pub(super) edge_index: usize,
    pub(super) endpoints: [Point; 2],
    pub(super) relation: ComputedLongitudeRelation<'intent>,
    pub(super) parameter: f64,
    pub(super) retained: RetainedComputedSample,
}

pub(super) fn retained_relation_at_vertex<'intent>(
    edge_intents: &[ComputedEdgeIntent<'intent>],
    point_count: usize,
    closed_ring: bool,
    index: usize,
    preferred: Option<(usize, f64)>,
) -> Result<(usize, ComputedLongitudeRelation<'intent>, f64)> {
    let edge_count = point_count.saturating_sub(1);
    let closure = (closed_ring && index == 0).then_some((edge_count.saturating_sub(1), 1.0));
    let candidates = [
        preferred,
        (index < edge_count).then_some((index, 0.0)),
        (index > 0).then_some((index.wrapping_sub(1), 1.0)),
        closure,
    ];
    for (edge, parameter) in candidates.into_iter().flatten() {
        if edge >= edge_count {
            continue;
        }
        if let Some(ComputedEdgeIntent::Retained(relation)) = edge_intents.get(edge) {
            return Ok((edge, *relation, parameter));
        }
    }
    Err(
        GeometryErrorKind::Invalid("sampled computed vertex has no retained edge endpoint".into())
            .into(),
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "one routine binds the actual source root, endpoints, aliases, sample callbacks and publication policy"
)]
#[expect(
    clippy::float_cmp,
    reason = "sample parameters equal to zero or one identify exact source-record endpoints"
)]
pub(super) fn publish_vertex_at<'intent, P: Coordinates + ?Sized>(
    frame: GeographicFrame,
    points: &P,
    point: Point,
    index: usize,
    intent: ComputedVertexIntent,
    edge_intents: &[ComputedEdgeIntent<'intent>],
    point_count: usize,
    closed_ring: bool,
    preferred: Option<(usize, f64)>,
    samples: &mut impl FnMut(
        usize,
        &[Point; 2],
        &ComputedLongitudeRelation<'intent>,
        f64,
        &RetainedComputedSample,
    ) -> Result<ComputedSampleOrdinates>,
    aliases: &[ZeroLongitudeAlias],
) -> Result<(Point, Option<SampledVertex<'intent>>)> {
    let (point, source_sample) = match intent {
        ComputedVertexIntent::Canonical => (
            frame
                .canonical_computed_point(point)
                .map_err(map_geographic_error)?,
            None,
        ),
        ComputedVertexIntent::Preserved => (point, None),
        ComputedVertexIntent::Sampled => {
            let (edge_index, relation, parameter) = retained_relation_at_vertex(
                edge_intents,
                point_count,
                closed_ring,
                index,
                preferred,
            )?;
            let endpoints = [
                points.nth_coord(edge_index),
                points.nth_coord(edge_index.wrapping_add(1)),
            ];
            let retained = relation.sample(frame.longitude(), parameter)?;
            let ordinates = if parameter == 0.0 || parameter == 1.0 {
                ComputedSampleOrdinates {
                    y: point.y,
                    z: ZOrdinate(point.z()),
                    m: MOrdinate(point.m()),
                }
            } else {
                samples(edge_index, &endpoints, &relation, parameter, &retained)?
            };
            (
                Point::new_axes(
                    retained.longitude.get(),
                    ordinates.y,
                    ordinates.z,
                    ordinates.m,
                )?,
                Some(SampledVertex {
                    edge_index,
                    endpoints,
                    relation,
                    parameter,
                    retained,
                }),
            )
        },
    };
    let point = zero_longitude_alias_at(aliases, index)
        .map_or(point, |longitude| point.with_xy_raw(longitude, point.y));
    Ok((point, source_sample))
}

pub(super) fn observe_sampled_vertex<'intent>(
    sampled: Option<&SampledVertex<'intent>>,
    point: Point,
    on_published: &mut impl FnMut(
        usize,
        &[Point; 2],
        &ComputedLongitudeRelation<'intent>,
        f64,
        &RetainedComputedSample,
        Point,
    ) -> Result<()>,
) -> Result<()> {
    if let Some(sampled) = sampled {
        on_published(
            sampled.edge_index,
            &sampled.endpoints,
            &sampled.relation,
            sampled.parameter,
            &sampled.retained,
            point,
        )?;
    }
    Ok(())
}
