use super::{
    ChartStep, EdgeDirection, GeographicError, GeographicFrame, RetainedChartDisplacement,
    RetainedLongitudeClass,
};
use crate::error::Result;
use crate::geometry::exact_arith::{
    RetainedComputedSample, retained_computed_samples_connection, retained_edge_from_chart_witness,
    retained_edge_from_lifted_binary64_pair, retained_exact_interval_class,
    retained_exact_publisher_sample, retained_exact_split_piece_count,
    retained_native_interval_class, retained_native_publisher_sample,
    retained_native_split_piece_count,
};
use crate::geometry::{
    CoordSeq, Coordinates, ExpansionBudget, GeometryErrorKind, MOrdinate, Point, ZOrdinate,
    interpolate_optional_ordinate, interpolate_subdivision_ordinate,
};

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum ComputedLongitudeRelation<'a> {
    Native(RetainedChartDisplacement),
    ExactLifted {
        endpoints: [&'a crate::geometry::ExactRational; 2],
    },
}

impl ComputedLongitudeRelation<'_> {
    pub(crate) fn from_chart_connection(
        frame: GeographicFrame,
        endpoints: [f64; 2],
    ) -> Result<Self> {
        let connection = frame
            .longitude()
            .chart_connection(endpoints[0], endpoints[1])
            .map_err(map_geographic_error)?;
        Ok(Self::Native(connection.retained()))
    }

    pub(crate) fn from_chart_witness(
        frame: GeographicFrame,
        endpoints: [f64; 2],
        witness: ChartStep,
    ) -> Result<Self> {
        Ok(Self::Native(retained_edge_from_chart_witness(
            frame.longitude(),
            endpoints,
            witness,
        )?))
    }

    fn class(
        self,
        axis: super::PeriodicAxis,
        from: f64,
        to: f64,
    ) -> Result<RetainedLongitudeClass> {
        match self {
            Self::Native(edge) => retained_native_interval_class(edge, axis, from, to),
            Self::ExactLifted { endpoints } => {
                retained_exact_interval_class(endpoints, axis, from, to)
            },
        }
    }

    fn sample(self, axis: super::PeriodicAxis, parameter: f64) -> Result<RetainedComputedSample> {
        match self {
            Self::Native(edge) => retained_native_publisher_sample(axis, edge, parameter),
            Self::ExactLifted { endpoints } => {
                retained_exact_publisher_sample(axis, endpoints, parameter)
            },
        }
    }

    fn split_piece_count(self, axis: super::PeriodicAxis) -> Result<usize> {
        match self {
            Self::Native(edge) => retained_native_split_piece_count(edge, axis),
            Self::ExactLifted { endpoints } => retained_exact_split_piece_count(endpoints, axis),
        }
    }

    #[expect(
        clippy::float_cmp,
        reason = "from and to use the exact binary64 endpoint parameters supplied by the publisher"
    )]
    fn reads_as(
        self,
        frame: GeographicFrame,
        start: f64,
        end: f64,
        from: f64,
        to: f64,
    ) -> Result<bool> {
        let actual = frame
            .longitude()
            .chart_connection(start, end)
            .map_err(map_geographic_error)?
            .retained();
        if from == 0.0
            && to == 1.0
            && let Self::Native(expected) = self
        {
            return Ok(expected == actual);
        }
        let start_sample = self.sample(frame.longitude(), from)?;
        let end_sample = self.sample(frame.longitude(), to)?;
        let intended =
            retained_computed_samples_connection(frame.longitude(), [&start_sample, &end_sample])?;
        Ok(intended == actual)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum ComputedEdgeIntent<'a> {
    /// A source-provenance edge whose written coordinates must be replayed.
    Preserved { zero_longitude: bool },
    /// An exact producer relation retained through canonical publication.
    Retained(ComputedLongitudeRelation<'a>),
    /// A local projected segment certified to sweep less than half a period.
    CertifiedMinor,
}

impl<'a> ComputedEdgeIntent<'a> {
    pub(crate) const fn preserved(zero_longitude: bool) -> Self {
        Self::Preserved { zero_longitude }
    }

    pub(crate) const fn retained_native(edge: RetainedChartDisplacement) -> Self {
        Self::Retained(ComputedLongitudeRelation::Native(edge))
    }

    pub(crate) const fn exact_lifted(endpoints: [&'a crate::geometry::ExactRational; 2]) -> Self {
        Self::Retained(ComputedLongitudeRelation::ExactLifted { endpoints })
    }

    pub(crate) fn retained_binary64_pair(
        axis: super::PeriodicAxis,
        endpoints: [f64; 2],
    ) -> Result<Self> {
        Ok(Self::retained_native(
            retained_edge_from_lifted_binary64_pair(axis, endpoints)?,
        ))
    }

    pub(crate) fn retained_chart_connection(
        frame: GeographicFrame,
        endpoints: [f64; 2],
    ) -> Result<Self> {
        Ok(Self::Retained(
            ComputedLongitudeRelation::from_chart_connection(frame, endpoints)?,
        ))
    }

    pub(crate) fn retained_chart_witness(
        frame: GeographicFrame,
        endpoints: [f64; 2],
        witness: ChartStep,
    ) -> Result<Self> {
        Ok(Self::Retained(
            ComputedLongitudeRelation::from_chart_witness(frame, endpoints, witness)?,
        ))
    }

    fn certifies_zero_longitude_change(self, axis: super::PeriodicAxis) -> Result<bool> {
        match self {
            Self::Preserved { zero_longitude } => Ok(zero_longitude),
            Self::Retained(relation) => {
                Ok(relation.class(axis, 0.0, 1.0)? == RetainedLongitudeClass::Zero)
            },
            // A minor certificate supplies no exact displacement. Mixed-source
            // chains retain explicit or stored displacement for their zero classes.
            Self::CertifiedMinor => Ok(false),
        }
    }

    const fn preserves_source_vertices(self) -> bool {
        matches!(self, Self::Preserved { .. })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ComputedVertexIntent {
    Canonical,
    Preserved,
    /// This vertex's XY and ordinates are sampled from an adjacent retained
    /// relation, rather than read from the layout Point.
    Sampled,
}

pub(crate) struct ComputedSampleOrdinates {
    pub(crate) y: f64,
    pub(crate) z: ZOrdinate,
    pub(crate) m: MOrdinate,
}

fn collect_edge_intents<'intent, P: Coordinates + ?Sized>(
    points: &P,
    intent: impl Fn(usize, Point, Point) -> Result<ComputedEdgeIntent<'intent>>,
) -> Result<Vec<ComputedEdgeIntent<'intent>>> {
    let edge_count = points.coord_count().saturating_sub(1);
    let mut intents = Vec::with_capacity(edge_count);
    for index in 0..edge_count {
        intents.push(intent(
            index,
            points.nth_coord(index),
            points.nth_coord(index.wrapping_add(1)),
        )?);
    }
    Ok(intents)
}

fn interpolate_sample_ordinates(
    _edge_index: usize,
    endpoints: &[Point; 2],
    _relation: &ComputedLongitudeRelation<'_>,
    parameter: f64,
    _retained: &RetainedComputedSample,
) -> ComputedSampleOrdinates {
    let [start, end] = *endpoints;
    ComputedSampleOrdinates {
        y: interpolate_subdivision_ordinate(start.y, end.y, parameter),
        z: ZOrdinate(interpolate_optional_ordinate(start.z(), end.z(), parameter)),
        m: MOrdinate(interpolate_optional_ordinate(start.m(), end.m(), parameter)),
    }
}

#[derive(Clone, Copy)]
struct ZeroLongitudeAlias {
    start: usize,
    end: usize,
    longitude: f64,
}

fn merge_preserved_spelling(
    operation: &'static str,
    current: &mut Option<f64>,
    spelling: f64,
) -> Result<()> {
    match current {
        Some(previous) if previous.to_bits() != spelling.to_bits() => Err(
            GeometryErrorKind::conflicting_preserved_longitudes(operation, *previous, spelling),
        ),
        Some(_) => Ok(()),
        None => {
            *current = Some(spelling);
            Ok(())
        },
    }
}

fn vertex_has_preserved_record(
    index: usize,
    point_count: usize,
    closed_ring: bool,
    point: Point,
    edge_intents: &[ComputedEdgeIntent<'_>],
    vertex_intent: &impl Fn(usize, Point) -> ComputedVertexIntent,
) -> bool {
    if vertex_intent(index, point) == ComputedVertexIntent::Preserved {
        return true;
    }
    let edge_count = point_count.saturating_sub(1);
    let incoming = if index == 0 {
        closed_ring.then(|| edge_count.saturating_sub(1))
    } else {
        Some(index.wrapping_sub(1))
    };
    let outgoing = if index == edge_count {
        closed_ring.then_some(0)
    } else {
        Some(index)
    };
    incoming
        .and_then(|edge| edge_intents.get(edge))
        .is_some_and(|edge| edge.preserves_source_vertices())
        || outgoing
            .and_then(|edge| edge_intents.get(edge))
            .is_some_and(|edge| edge.preserves_source_vertices())
}

fn preserved_longitude_in_range<P: Coordinates + ?Sized>(
    points: &P,
    start: usize,
    end: usize,
    closed_ring: bool,
    edge_intents: &[ComputedEdgeIntent<'_>],
    vertex_intent: &impl Fn(usize, Point) -> ComputedVertexIntent,
    operation: &'static str,
    preserved_longitude: &mut Option<f64>,
) -> Result<()> {
    let point_count = points.coord_count();
    for index in start..end {
        let point = points.nth_coord(index);
        if vertex_has_preserved_record(
            index,
            point_count,
            closed_ring,
            point,
            edge_intents,
            vertex_intent,
        ) {
            merge_preserved_spelling(operation, preserved_longitude, point.x)?;
        }
    }
    Ok(())
}

fn add_zero_class_alias(
    frame: GeographicFrame,
    longitude: Option<f64>,
    start: usize,
    end: usize,
    aliases: &mut Vec<ZeroLongitudeAlias>,
) -> Result<()> {
    let Some(longitude) = longitude else {
        return Ok(());
    };
    let canonical = frame
        .longitude()
        .canonicalize(longitude)
        .map_err(map_geographic_error)?
        .get();
    if canonical.to_bits() != longitude.to_bits() {
        aliases.push(ZeroLongitudeAlias {
            start,
            end,
            longitude,
        });
    }
    Ok(())
}

fn zero_longitude_aliases<P: Coordinates + ?Sized>(
    frame: GeographicFrame,
    points: &P,
    closed_ring: bool,
    edge_intents: &[ComputedEdgeIntent],
    vertex_intent: &impl Fn(usize, Point) -> ComputedVertexIntent,
    operation: &'static str,
) -> Result<Vec<ZeroLongitudeAlias>> {
    let edge_count = edge_intents.len();
    let point_count = points.coord_count();
    let mut runs = Vec::new();
    let mut edge = 0;
    while edge < edge_count {
        if !edge_intents
            .get(edge)
            .expect("zero-class cursor belongs to the edge inventory")
            .certifies_zero_longitude_change(frame.longitude())?
        {
            edge = edge.wrapping_add(1);
            continue;
        }
        let start = edge;
        while edge < edge_count
            && edge_intents
                .get(edge)
                .expect("zero-class cursor belongs to the edge inventory")
                .certifies_zero_longitude_change(frame.longitude())?
        {
            edge = edge.wrapping_add(1);
        }
        let mut preserved_longitude = None;
        preserved_longitude_in_range(
            points,
            start,
            edge.wrapping_add(1),
            closed_ring,
            edge_intents,
            vertex_intent,
            operation,
            &mut preserved_longitude,
        )?;
        runs.push((start, edge, preserved_longitude));
    }

    let prefix = runs.first().filter(|&&(start, ..)| start == 0);
    let suffix = runs.last().filter(|&&(_, end, _)| end == edge_count);
    let mut closure_longitude = None;
    if closed_ring && (prefix.is_some() || suffix.is_some()) {
        for &(_, _, longitude) in prefix.into_iter().chain(suffix) {
            if let Some(longitude) = longitude {
                merge_preserved_spelling(operation, &mut closure_longitude, longitude)?;
            }
        }
        for index in [0, edge_count] {
            let point = points.nth_coord(index);
            if vertex_has_preserved_record(
                index,
                point_count,
                closed_ring,
                point,
                edge_intents,
                vertex_intent,
            ) {
                merge_preserved_spelling(operation, &mut closure_longitude, point.x)?;
            }
        }
    }
    let mut aliases = Vec::new();
    if closed_ring && prefix.is_none() && suffix.is_some() {
        add_zero_class_alias(frame, closure_longitude, 0, 1, &mut aliases)?;
    }
    for &(start, end, longitude) in &runs {
        let longitude = if closed_ring && (start == 0 || end == edge_count) {
            closure_longitude
        } else {
            longitude
        };
        add_zero_class_alias(frame, longitude, start, end.wrapping_add(1), &mut aliases)?;
    }
    if closed_ring && prefix.is_some() && suffix.is_none() {
        add_zero_class_alias(
            frame,
            closure_longitude,
            edge_count,
            edge_count.wrapping_add(1),
            &mut aliases,
        )?;
    }
    Ok(aliases)
}

fn zero_longitude_alias_at(aliases: &[ZeroLongitudeAlias], index: usize) -> Option<f64> {
    let range = aliases.partition_point(|alias| alias.end <= index);
    aliases
        .get(range)
        .filter(|alias| index >= alias.start)
        .map(|alias| alias.longitude)
}

struct SampledVertex<'intent> {
    edge_index: usize,
    endpoints: [Point; 2],
    relation: ComputedLongitudeRelation<'intent>,
    parameter: f64,
    retained: RetainedComputedSample,
}

fn retained_relation_at_vertex<'intent>(
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
fn publish_vertex_at<'intent, P: Coordinates + ?Sized>(
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

fn observe_sampled_vertex<'intent>(
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
            Ok(interpolate_sample_ordinates(
                edge_index, endpoints, relation, parameter, retained,
            ))
        },
        |_, _, _, _, _, _| Ok(()),
        &[],
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
            Ok(interpolate_sample_ordinates(
                edge_index, endpoints, relation, parameter, retained,
            ))
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
    let point_count = points.coord_count();
    if point_count == 0 {
        return Ok(None);
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

fn map_geographic_error(error: GeographicError) -> crate::error::Error {
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

#[cfg(test)]
mod tests {
    use crate::geographic::{
        AngularFactor, ChartStep, ComputedEdgeIntent, ComputedSampleOrdinates,
        ComputedVertexIntent, EdgeDirection, GeographicFrame, RetainedChartDisplacement,
        TurnDirection, publish_computed_chain, publish_computed_chain_with_vertex_intents,
        publish_computed_chain_with_vertex_intents_and_samples,
    };
    use crate::geometry::{
        ExpansionBudget, GeometryErrorKind, MOrdinate, Point, ZOrdinate,
        interpolate_optional_ordinate, interpolate_subdivision_ordinate,
    };

    fn degrees() -> GeographicFrame {
        GeographicFrame::crs84()
    }

    fn angular_frame(radians_per_unit: f64) -> GeographicFrame {
        let unit = AngularFactor::try_new("test angle", radians_per_unit).unwrap();
        GeographicFrame::try_new(unit, unit, 0.0).unwrap()
    }

    fn publish(points: &[Point], closed_ring: bool, steps: &[f64]) -> Vec<Point> {
        publish_in(degrees(), points, closed_ring, steps)
    }

    fn publish_in(
        frame: GeographicFrame,
        points: &[Point],
        closed_ring: bool,
        steps: &[f64],
    ) -> Vec<Point> {
        let mut budget = ExpansionBudget::new("test", "computed edges");
        publish_computed_chain(
            frame,
            points,
            closed_ring,
            &mut budget,
            |index, start, end| {
                let step = *steps
                    .get(index)
                    .expect("each source edge has one retained test relation");
                assert_eq!(step.to_bits(), (end.x - start.x).to_bits());
                ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [start.x, end.x])
            },
        )
        .unwrap()
        .unwrap_or_else(|| points.to_vec())
    }

    #[test]
    fn major_and_period_edges_keep_native_steps_in_degrees_grads_and_radians() {
        let frames = [
            degrees(),
            angular_frame(std::f64::consts::PI / 200.0),
            angular_frame(1.0),
        ];
        for frame in frames {
            let axis = frame.longitude();
            let half = axis.half_period();
            let small = axis.period() / 1000.0;
            for delta in [half + small, -half - small, axis.period() - small] {
                let result = publish_in(
                    frame,
                    &[Point::new_raw_xy(0.0, 10.0), Point::new_raw_xy(delta, 30.0)],
                    false,
                    &[delta],
                );
                assert!(result.len() >= 3, "frame={frame:?}, delta={delta}");
                assert_eq!(result.first().unwrap().x, 0.0);
                assert_eq!(
                    result.last().unwrap().x,
                    axis.canonicalize(delta).unwrap().get(),
                    "frame={frame:?}, delta={delta}"
                );
                let mut recovered = 0.0;
                for pair in result.array_windows::<2>() {
                    let step = axis.chart_step(pair[0].x, pair[1].x).unwrap();
                    assert!(step.delta(axis).abs() <= half);
                    recovered += step.delta(axis);
                }
                assert_eq!(recovered, delta, "frame={frame:?}, delta={delta}");
            }

            let full_turn = axis.period();
            let ring = publish_in(
                frame,
                &[Point::new_raw_xy(-half, 0.0), Point::new_raw_xy(half, 0.0)],
                false,
                &[full_turn],
            );
            let mut recovered = 0.0;
            for pair in ring.array_windows::<2>() {
                recovered += axis.chart_step(pair[0].x, pair[1].x).unwrap().delta(axis);
            }
            assert_eq!(recovered, full_turn, "frame={frame:?}");

            let correctly_spelled_half = publish_in(
                frame,
                &[Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(-half, 1.0)],
                false,
                &[-half],
            );
            assert_eq!(correctly_spelled_half.len(), 2, "frame={frame:?}");
        }
    }

    #[test]
    fn computed_major_period_wrong_half_and_correct_half_edges_keep_their_steps() {
        let positive_major = publish(
            &[
                Point::new_raw_xy(10.0, 20.0),
                Point::new_raw_xy(260.0, 40.0),
            ],
            false,
            &[250.0],
        );
        assert_eq!(positive_major, [
            Point::new_raw_xy(10.0, 20.0),
            Point::new_raw_xy(135.0, 30.0),
            Point::new_raw_xy(-100.0, 40.0),
        ]);

        let wrong_half = publish(
            &[Point::new_raw_xy(0.0, 30.0), Point::new_raw_xy(180.0, 40.0)],
            false,
            &[180.0],
        );
        assert_eq!(wrong_half, [
            Point::new_raw_xy(0.0, 30.0),
            Point::new_raw_xy(90.0, 35.0),
            Point::new_raw_xy(-180.0, 40.0),
        ]);

        let correct_half = publish(
            &[
                Point::new_raw_xy(0.0, 30.0),
                Point::new_raw_xy(-180.0, 40.0),
            ],
            false,
            &[-180.0],
        );
        assert_eq!(correct_half, [
            Point::new_raw_xy(0.0, 30.0),
            Point::new_raw_xy(-180.0, 40.0),
        ]);
    }

    #[test]
    fn major_half_and_near_period_edges_use_their_full_intended_steps() {
        for (end, delta, midpoint) in [
            (187.0, 187.0, 93.5),
            (-187.0, -187.0, -93.5),
            (353.0, 353.0, 176.5),
            (-353.0, -353.0, -176.5),
        ] {
            let result = publish(
                &[Point::new_raw_xy(0.0, 4.0), Point::new_raw_xy(end, 8.0)],
                false,
                &[delta],
            );
            assert_eq!(result, [
                Point::new_raw_xy(0.0, 4.0),
                Point::new_raw_xy(midpoint, 6.0),
                Point::new_raw_xy(degrees().longitude().canonicalize(end).unwrap().get(), 8.0,),
            ]);
        }
    }

    #[test]
    fn computed_period_edges_include_a_seam_anchored_turn() {
        for (start, end, delta) in [
            (10.0, 370.0, 360.0),
            (370.0, 10.0, -360.0),
            (10.0, 730.0, 720.0),
            (10.0, -710.0, -720.0),
            (-180.0, 180.0, 360.0),
        ] {
            let result = publish(
                &[Point::new_raw_xy(start, 0.0), Point::new_raw_xy(end, 0.0)],
                false,
                &[delta],
            );
            let mut turn = 0_i64;
            let mut recovered_delta = 0.0;
            for pair in result.array_windows::<2>() {
                let step = degrees()
                    .longitude()
                    .chart_step(pair[0].x, pair[1].x)
                    .unwrap();
                let step_delta = step.delta(degrees().longitude());
                assert!(step_delta.abs() <= degrees().longitude().half_period());
                recovered_delta += step_delta;
                turn = degrees()
                    .longitude()
                    .add_chart_transit(turn, pair[0].x, pair[1].x)
                    .unwrap();
            }
            assert_eq!(recovered_delta, delta);
            assert_eq!(turn, (delta / 360.0) as i64);
        }
        assert_eq!(
            publish(
                &[
                    Point::new_raw_xy(-180.0, 0.0),
                    Point::new_raw_xy(180.0, 0.0)
                ],
                false,
                &[360.0],
            ),
            [
                Point::new_raw_xy(-180.0, 0.0),
                Point::new_raw_xy(-90.0, 0.0),
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(90.0, 0.0),
                Point::new_raw_xy(-180.0, 0.0),
            ]
        );
    }

    #[test]
    fn stored_chart_circle_step_subdivides_before_canonical_publication() {
        let points = [Point::new_raw_xy(10.0, 0.0), Point::new_raw_xy(370.0, 0.0)];
        let mut budget = ExpansionBudget::new("test", "stored computed edge");
        let output = publish_computed_chain(
            degrees(),
            points.as_slice(),
            false,
            &mut budget,
            |_, _, _| {
                ComputedEdgeIntent::retained_chart_witness(
                    degrees(),
                    [points[0].x, points[1].x],
                    ChartStep::Circle(TurnDirection::Positive),
                )
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(output, [
            Point::new_raw_xy(10.0, 0.0),
            Point::new_raw_xy(100.0, 0.0),
            Point::new_raw_xy(-170.0, 0.0),
            Point::new_raw_xy(-80.0, 0.0),
            Point::new_raw_xy(10.0, 0.0),
        ]);
    }

    #[test]
    fn subdivided_full_turn_edges_publish_only_minor_pieces() {
        for latitude in [-90.0, 10.0, 90.0] {
            for (start, end, delta) in [
                (-180.0, 180.0, 360.0),
                (180.0, -180.0, -360.0),
                (-180.0, 540.0, 720.0),
                (180.0, -540.0, -720.0),
            ] {
                let points = [
                    Point::new_raw_xy(start, latitude),
                    Point::new_raw_xy(end, latitude),
                ];
                let output = publish(&points, false, &[delta]);
                let mut recovered = 0.0;
                for [start, end] in output.array_windows::<2>() {
                    let axis = degrees().longitude();
                    let delta = axis.chart_step(start.x, end.x).unwrap().delta(axis);
                    assert!(delta.abs() < 180.0);
                    recovered += delta;
                }
                assert_eq!(recovered, delta);
            }
        }
    }

    #[test]
    fn computed_ring_keeps_its_start_and_copies_full_closure() {
        let start = Point::new_raw_axes(10.0, 0.0, ZOrdinate(Some(3.0)), MOrdinate(Some(11.0)));
        let east = Point::new_raw_axes(260.0, 0.0, ZOrdinate(Some(19.0)), MOrdinate(Some(43.0)));
        let north_east =
            Point::new_raw_axes(260.0, 10.0, ZOrdinate(Some(23.0)), MOrdinate(Some(47.0)));
        let north_west =
            Point::new_raw_axes(10.0, 10.0, ZOrdinate(Some(7.0)), MOrdinate(Some(13.0)));
        let ring_points = [start, east, north_east, north_west, start];
        let result = publish(&ring_points, true, &[250.0, 0.0, -250.0, 0.0]);

        assert_eq!(result[0], start);
        assert_eq!(result, [
            start,
            Point::new_raw_axes(135.0, 0.0, ZOrdinate(Some(11.0)), MOrdinate(Some(27.0)),),
            Point::new_raw_axes(-100.0, 0.0, ZOrdinate(Some(19.0)), MOrdinate(Some(43.0)),),
            Point::new_raw_axes(-100.0, 10.0, ZOrdinate(Some(23.0)), MOrdinate(Some(47.0)),),
            Point::new_raw_axes(135.0, 10.0, ZOrdinate(Some(15.0)), MOrdinate(Some(30.0)),),
            north_west,
            start,
        ]);
        assert_eq!(result.first(), result.last());
    }

    #[test]
    fn sampled_closed_chain_observes_the_preserved_first_full_record() {
        let first = Point::new_raw_axes(-0.0, 0.0, ZOrdinate(Some(3.0)), MOrdinate(Some(11.0)));
        let middle = Point::new_raw_axes(10.0, 0.0, ZOrdinate(Some(7.0)), MOrdinate(Some(19.0)));
        let top = Point::new_raw_axes(10.0, 10.0, ZOrdinate(Some(13.0)), MOrdinate(Some(23.0)));
        let ring = [first, middle, top, first];
        let mut budget = ExpansionBudget::new("sampled closure", "computed edges");
        let mut interior_samples = 0_usize;
        let mut observed = Vec::new();
        let output = publish_computed_chain_with_vertex_intents_and_samples(
            degrees(),
            ring.as_slice(),
            true,
            &mut budget,
            |_, start, end| {
                ComputedEdgeIntent::retained_binary64_pair(degrees().longitude(), [start.x, end.x])
            },
            |index, _| {
                if index == ring.len().wrapping_sub(1) {
                    ComputedVertexIntent::Sampled
                } else if index == 0 {
                    ComputedVertexIntent::Preserved
                } else {
                    ComputedVertexIntent::Sampled
                }
            },
            |_, endpoints, _, parameter, _| {
                let &[start, end] = endpoints;
                interior_samples = interior_samples.wrapping_add(1);
                Ok(ComputedSampleOrdinates {
                    y: interpolate_subdivision_ordinate(start.y, end.y, parameter),
                    z: ZOrdinate(interpolate_optional_ordinate(start.z(), end.z(), parameter)),
                    m: MOrdinate(interpolate_optional_ordinate(start.m(), end.m(), parameter)),
                })
            },
            |edge, _, _, parameter, _, point| {
                observed.push((edge, parameter, point));
                Ok(())
            },
        )
        .unwrap()
        .unwrap();

        assert_eq!(interior_samples, 0);
        assert_eq!(output.first(), output.last());
        assert_eq!(output[0].x.to_bits(), (-0.0_f64).to_bits());
        assert_eq!(output[3].x.to_bits(), output[0].x.to_bits());
        assert_eq!(output[3].z(), output[0].z());
        assert_eq!(output[3].m(), output[0].m());
        let (edge, parameter, observed_closure) = observed
            .last()
            .expect("publisher observes the closing sample");
        assert_eq!((*edge, *parameter), (2, 1.0));
        assert_eq!(observed_closure.x.to_bits(), output[0].x.to_bits());
        assert_eq!(observed_closure.z(), output[0].z());
        assert_eq!(observed_closure.m(), output[0].m());
    }

    #[test]
    fn split_computed_edges_keep_preserved_endpoint_records() {
        let points = [
            Point::new_raw_xy(370.0, 20.0),
            Point::new_raw_xy(260.0, 40.0),
        ];
        let mut budget = ExpansionBudget::new("test", "computed edges");
        let output = publish_computed_chain_with_vertex_intents(
            degrees(),
            points.as_slice(),
            false,
            &mut budget,
            |_, _, _| {
                let axis = degrees().longitude();
                let edge = RetainedChartDisplacement::from_native_endpoints(
                    [
                        axis.canonicalize(points[0].x).unwrap(),
                        axis.canonicalize(points[1].x).unwrap(),
                    ],
                    1,
                );
                Ok(ComputedEdgeIntent::retained_native(edge))
            },
            |_, _| ComputedVertexIntent::Preserved,
        )
        .unwrap()
        .unwrap();
        assert_eq!(output, [
            points[0],
            Point::new_raw_xy(135.0, 30.0),
            points[1],
        ]);
        assert_eq!(
            degrees()
                .longitude()
                .chart_step(output[0].x, output[1].x)
                .unwrap()
                .delta(degrees().longitude()),
            125.0
        );
        assert_eq!(
            degrees()
                .longitude()
                .chart_step(output[1].x, output[2].x)
                .unwrap()
                .delta(degrees().longitude()),
            125.0
        );
    }

    #[test]
    fn mixed_chains_replay_source_edges_and_split_only_computed_edges() {
        let points = [
            Point::new_raw_xy(370.0, 0.0),
            Point::new_raw_xy(0.0, 5.0),
            Point::new_raw_xy(250.0, 10.0),
        ];
        let mut budget = ExpansionBudget::new("test", "computed edges");
        let output = publish_computed_chain_with_vertex_intents(
            degrees(),
            points.as_slice(),
            false,
            &mut budget,
            |index, start, end| {
                Ok(if index == 0 {
                    ComputedEdgeIntent::preserved(end.x - start.x == 0.0)
                } else {
                    ComputedEdgeIntent::retained_binary64_pair(degrees().longitude(), [
                        start.x, end.x,
                    ])?
                })
            },
            |index, _| {
                if index < 2 {
                    ComputedVertexIntent::Preserved
                } else {
                    ComputedVertexIntent::Canonical
                }
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(output, [
            points[0],
            points[1],
            Point::new_raw_xy(125.0, 7.5),
            Point::new_raw_xy(-110.0, 10.0),
        ]);
    }

    #[test]
    fn computed_only_zero_longitude_class_uses_canonical_spelling() {
        let points = [Point::new_raw_xy(370.0, 0.0), Point::new_raw_xy(370.0, 1.0)];
        let mut budget = ExpansionBudget::new("computed meridian", "computed edges");
        let output = publish_computed_chain(
            degrees(),
            points.as_slice(),
            false,
            &mut budget,
            |_, start, end| {
                ComputedEdgeIntent::retained_binary64_pair(degrees().longitude(), [start.x, end.x])
            },
        )
        .unwrap()
        .unwrap();

        assert_eq!(output, [
            Point::new_raw_xy(10.0, 0.0),
            Point::new_raw_xy(10.0, 1.0),
        ]);
        assert_eq!(
            degrees()
                .longitude()
                .chart_step(output[0].x, output[1].x)
                .unwrap()
                .delta(degrees().longitude()),
            0.0
        );

        let mut stored_budget = ExpansionBudget::new("stored meridian", "computed edges");
        let stored = publish_computed_chain(
            degrees(),
            points.as_slice(),
            false,
            &mut stored_budget,
            |_, _, _| {
                ComputedEdgeIntent::retained_chart_witness(
                    degrees(),
                    [points[0].x, points[1].x],
                    ChartStep::Shortest(EdgeDirection::Minor(0.0)),
                )
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(stored, output);
    }

    #[test]
    fn published_samples_keep_the_original_root_parameter_and_lift() {
        let points = [
            Point::new_raw_xy(10.0, 20.0),
            Point::new_raw_xy(260.0, 40.0),
        ];
        let frame = degrees();
        let mut budget = ExpansionBudget::new("exact interval", "computed edges");
        let mut observed = Vec::new();
        let output = publish_computed_chain_with_vertex_intents_and_samples(
            frame,
            points.as_slice(),
            false,
            &mut budget,
            |_, start, end| {
                ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [start.x, end.x])
            },
            |_, _| ComputedVertexIntent::Sampled,
            |_, endpoints, _, parameter, _| {
                let &[start, end] = endpoints;
                Ok(ComputedSampleOrdinates {
                    y: interpolate_subdivision_ordinate(start.y, end.y, parameter),
                    z: ZOrdinate(None),
                    m: MOrdinate(None),
                })
            },
            |edge, _, _, parameter, retained, point| {
                observed.push((edge, parameter, retained.longitude.get(), point));
                Ok(())
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(output, [
            Point::new_raw_xy(10.0, 20.0),
            Point::new_raw_xy(135.0, 30.0),
            Point::new_raw_xy(-100.0, 40.0),
        ]);
        assert_eq!(observed, [
            (0, 0.0, 10.0, Point::new_raw_xy(10.0, 20.0)),
            (0, 0.5, 135.0, Point::new_raw_xy(135.0, 30.0)),
            (0, 1.0, -100.0, Point::new_raw_xy(-100.0, 40.0)),
        ]);
    }

    #[test]
    fn zero_longitude_class_crosses_ring_closure_without_rewriting_nonzero_neighbors() {
        let points = [
            Point::new_raw_xy(250.0, 0.0),
            Point::new_raw_xy(250.0, 1.0),
            Point::new_raw_xy(260.0, 2.0),
            Point::new_raw_xy(250.0, -1.0),
            Point::new_raw_xy(250.0, 0.0),
        ];
        let mut budget = ExpansionBudget::new("ring meridian", "computed edges");
        let output = publish_computed_chain_with_vertex_intents(
            degrees(),
            points.as_slice(),
            true,
            &mut budget,
            |_, start, end| {
                ComputedEdgeIntent::retained_binary64_pair(degrees().longitude(), [start.x, end.x])
            },
            |index, _| {
                if index == 1 {
                    ComputedVertexIntent::Preserved
                } else {
                    ComputedVertexIntent::Canonical
                }
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(output, [
            points[0],
            points[1],
            Point::new_raw_xy(-100.0, 2.0),
            points[3],
            points[0],
        ]);
    }

    #[test]
    fn tiny_nonzero_edge_next_to_preserved_meridian_stays_canonical() {
        let tiny = 2.0_f64.powi(-20);
        let points = [
            Point::new_raw_xy(250.0, 0.0),
            Point::new_raw_xy(250.0, 1.0),
            Point::new_raw_xy(250.0 + tiny, 2.0),
        ];
        let mut budget = ExpansionBudget::new("tiny edge", "computed edges");
        let output = publish_computed_chain_with_vertex_intents(
            degrees(),
            points.as_slice(),
            false,
            &mut budget,
            |index, start, end| {
                Ok(if index == 0 {
                    ComputedEdgeIntent::preserved(true)
                } else {
                    ComputedEdgeIntent::retained_binary64_pair(degrees().longitude(), [
                        start.x, end.x,
                    ])?
                })
            },
            |index, _| {
                if index < 2 {
                    ComputedVertexIntent::Preserved
                } else {
                    ComputedVertexIntent::Canonical
                }
            },
        )
        .unwrap()
        .unwrap();
        let delta = points[2].x - points[1].x;

        assert_eq!(output.len(), 3);
        assert_eq!(output[0], points[0]);
        assert_eq!(output[1], points[1]);
        assert_eq!(
            output[2].x,
            degrees()
                .longitude()
                .canonicalize(points[2].x)
                .unwrap()
                .get()
        );
        assert_eq!(
            degrees()
                .longitude()
                .chart_step(output[1].x, output[2].x)
                .unwrap()
                .delta(degrees().longitude()),
            delta
        );
    }

    #[test]
    fn zero_longitude_class_rejects_conflicting_preserved_spellings() {
        let points = [
            Point::new_raw_xy(250.0, 0.0),
            Point::new_raw_xy(-110.0, 1.0),
        ];
        let mut budget = ExpansionBudget::new("zero class conflict", "computed edges");
        let error = publish_computed_chain_with_vertex_intents(
            degrees(),
            points.as_slice(),
            false,
            &mut budget,
            |_, _, _| {
                let axis = degrees().longitude();
                Ok(ComputedEdgeIntent::retained_native(
                    RetainedChartDisplacement::from_native_endpoints(
                        [
                            axis.canonicalize(points[0].x).unwrap(),
                            axis.canonicalize(points[1].x).unwrap(),
                        ],
                        0,
                    ),
                ))
            },
            |_, _| ComputedVertexIntent::Preserved,
        )
        .unwrap_err();

        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::UnrepresentableOutput {
                operation: "zero class conflict",
                ..
            })
        ));
        let message = error.to_string();
        assert!(message.contains("250.0"), "{message}");
        assert!(message.contains("-110.0"), "{message}");
    }

    #[test]
    fn canonical_period_line_carries_zm_at_the_original_chart_parameter() {
        let start = Point::new_raw_axes(10.0, 20.0, ZOrdinate(Some(3.0)), MOrdinate(Some(11.0)));
        let end = Point::new_raw_axes(370.0, 40.0, ZOrdinate(Some(19.0)), MOrdinate(Some(43.0)));
        assert_eq!(publish(&[start, end], false, &[360.0]), [
            start,
            Point::new_raw_axes(100.0, 25.0, ZOrdinate(Some(7.0)), MOrdinate(Some(19.0)),),
            Point::new_raw_axes(-170.0, 30.0, ZOrdinate(Some(11.0)), MOrdinate(Some(27.0)),),
            Point::new_raw_axes(-80.0, 35.0, ZOrdinate(Some(15.0)), MOrdinate(Some(35.0)),),
            Point::new_raw_axes(10.0, 40.0, ZOrdinate(Some(19.0)), MOrdinate(Some(43.0)),),
        ]);
    }

    #[test]
    fn positive_half_period_closing_cap_is_not_rotated_or_split() {
        let ring = [
            Point::new_raw_xy(0.0, 70.0),
            Point::new_raw_xy(90.0, 65.0),
            Point::new_raw_xy(180.0, 70.0),
            Point::new_raw_xy(0.0, 70.0),
        ];
        let frame = degrees();
        let mut budget = ExpansionBudget::new("positive half cap", "computed edges");
        let output = publish_computed_chain(
            frame,
            ring.as_slice(),
            true,
            &mut budget,
            |index, start, end| {
                if index == 2 {
                    ComputedEdgeIntent::retained_chart_witness(
                        frame,
                        [start.x, end.x],
                        ChartStep::Shortest(EdgeDirection::HalfTurn(TurnDirection::Positive)),
                    )
                } else {
                    ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [start.x, end.x])
                }
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(output, [
            ring[0],
            ring[1],
            Point::new_raw_xy(-180.0, 70.0),
            ring[0],
        ]);
    }

    #[test]
    fn certified_minor_edges_canonicalize_without_subdivision() {
        let points = [
            Point::new_raw_xy(170.0, 0.0),
            Point::new_raw_xy(180.0, 1.0),
            Point::new_raw_xy(190.0, 0.0),
        ];
        let mut budget = ExpansionBudget::new("test", "certified minor");
        let output = publish_computed_chain(
            degrees(),
            points.as_slice(),
            false,
            &mut budget,
            |_, _, _| Ok(ComputedEdgeIntent::CertifiedMinor),
        )
        .unwrap()
        .unwrap();
        assert_eq!(output, [
            Point::new_raw_xy(170.0, 0.0),
            Point::new_raw_xy(-180.0, 1.0),
            Point::new_raw_xy(-170.0, 0.0),
        ]);
    }

    #[test]
    fn minor_chain_certificate_preserves_spelling_and_complete_closure() {
        let first = Point::new_raw_axes(10.0, 20.0, ZOrdinate(Some(3.0)), MOrdinate(Some(11.0)));
        let middle = Point::new_raw_axes(20.0, 30.0, ZOrdinate(Some(7.0)), MOrdinate(Some(19.0)));
        let last = Point::new_raw_axes(10.0, 20.0, ZOrdinate(Some(13.0)), MOrdinate(Some(23.0)));
        let coordinates =
            crate::geometry::CoordSeq::try_from_points(&[first, middle, last]).unwrap();
        let mut budget = ExpansionBudget::new("test", "computed edges");
        let output =
            super::publish_certified_minor_chain(degrees(), &coordinates, true, &mut budget)
                .unwrap()
                .unwrap();
        assert_eq!(output, [first, middle, first]);

        for (start, expected) in [(-0.0, 0.0), (180.0, -180.0)] {
            let coordinates = crate::geometry::CoordSeq::try_from_points(&[
                Point::new_raw_xy(start, 20.0),
                Point::new_raw_xy(90.0, 30.0),
            ])
            .unwrap();
            let output =
                super::publish_certified_minor_chain(degrees(), &coordinates, false, &mut budget)
                    .unwrap()
                    .unwrap();
            assert_eq!(output, [
                Point::new_raw_xy(expected, 20.0),
                Point::new_raw_xy(90.0, 30.0),
            ]);
        }
    }
}
