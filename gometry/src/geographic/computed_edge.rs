use super::{ChartStep, EdgeDirection, GeographicError, GeographicFrame};
use crate::error::Result;
use crate::geometry::{
    CoordSeq, Coordinates, ExpansionBudget, GeometryErrorKind, MOrdinate, Point, ZOrdinate,
    interpolate_optional_ordinate, interpolate_subdivision_ordinate,
};

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum ComputedLongitudeIntent {
    Zero,
    NonZero(f64),
}

impl ComputedLongitudeIntent {
    pub(crate) fn from_f64(delta: f64) -> Self {
        if delta == 0.0 {
            Self::Zero
        } else {
            Self::NonZero(delta)
        }
    }

    pub(crate) const fn from_rounded_exact(delta: f64, is_zero: bool) -> Self {
        if is_zero {
            Self::Zero
        } else {
            Self::NonZero(delta)
        }
    }

    const fn delta(self) -> f64 {
        match self {
            Self::Zero => 0.0,
            Self::NonZero(delta) => delta,
        }
    }

    const fn is_zero(self) -> bool {
        matches!(self, Self::Zero)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum ComputedEdgeIntent {
    /// A source-provenance edge whose written coordinates must be replayed.
    Preserved { zero_longitude: bool },
    /// A producer-retained displacement in its continuous longitude chart.
    Displacement(ComputedLongitudeIntent),
    /// A stored chart interpretation, including an explicit full-period turn.
    Stored(ChartStep),
    /// A local projected segment certified to sweep less than half a period.
    CertifiedMinor,
}

impl ComputedEdgeIntent {
    pub(crate) const fn preserved(zero_longitude: bool) -> Self {
        Self::Preserved { zero_longitude }
    }

    pub(crate) fn displacement(delta: f64) -> Self {
        Self::Displacement(ComputedLongitudeIntent::from_f64(delta))
    }

    pub(crate) const fn displacement_from_exact(delta: f64, is_zero: bool) -> Self {
        Self::Displacement(ComputedLongitudeIntent::from_rounded_exact(delta, is_zero))
    }

    fn certifies_zero_longitude_change(self, axis: super::PeriodicAxis) -> bool {
        match self {
            Self::Preserved { zero_longitude } => zero_longitude,
            Self::Displacement(delta) => delta.is_zero(),
            Self::Stored(step) => step.delta(axis) == 0.0,
            // A minor certificate supplies no exact displacement. Mixed-source
            // chains retain explicit or stored displacement for their zero classes.
            Self::CertifiedMinor => false,
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
}

pub(crate) struct ComputedEdgeInterval {
    pub(crate) start: Point,
    pub(crate) end: Point,
    pub(crate) delta: f64,
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
    edge_intents: &[ComputedEdgeIntent],
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
    edge_intents: &[ComputedEdgeIntent],
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
            .certifies_zero_longitude_change(frame.longitude())
        {
            edge = edge.wrapping_add(1);
            continue;
        }
        let start = edge;
        while edge_intents
            .get(edge)
            .is_some_and(|intent| intent.certifies_zero_longitude_change(frame.longitude()))
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

fn publish_vertex_at(
    frame: GeographicFrame,
    point: Point,
    index: usize,
    intent: ComputedVertexIntent,
    aliases: &[ZeroLongitudeAlias],
) -> Result<Point> {
    let point = publish_vertex(frame, point, intent)?;
    Ok(zero_longitude_alias_at(aliases, index)
        .map_or(point, |longitude| point.with_xy_raw(longitude, point.y)))
}

/// Canonicalize a computed chain and split only edges whose canonical writing
/// no longer reads the producer-retained displacement.
///
/// `None` means every record was already canonical and every edge was
/// representable without inserting a vertex. A closed ring retains its first
/// vertex and uses that complete canonical record at closure.
pub(crate) fn publish_computed_chain<P: Coordinates + ?Sized>(
    frame: GeographicFrame,
    points: &P,
    closed_ring: bool,
    budget: &mut ExpansionBudget,
    intent: impl Fn(usize, Point, Point) -> Result<ComputedEdgeIntent>,
) -> Result<Option<Vec<Point>>> {
    publish_chain_with_aliases(
        frame,
        points,
        closed_ring,
        budget,
        intent,
        |_, _| ComputedVertexIntent::Canonical,
        |_, start, end, intended_delta, start_t, end_t| {
            interpolate_chart_interval(start, end, intended_delta, start_t, end_t)
        },
        &[],
    )
}

pub(crate) fn publish_certified_minor_chain(
    frame: GeographicFrame,
    points: &CoordSeq,
    closed_ring: bool,
    budget: &mut ExpansionBudget,
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
    publish_computed_chain(frame, points, closed_ring, budget, |_, _, _| {
        Ok(ComputedEdgeIntent::CertifiedMinor)
    })
}

pub(crate) fn publish_computed_chain_with_vertex_intents<P: Coordinates + ?Sized>(
    frame: GeographicFrame,
    points: &P,
    closed_ring: bool,
    budget: &mut ExpansionBudget,
    intent: impl Fn(usize, Point, Point) -> Result<ComputedEdgeIntent>,
    vertex_intent: impl Fn(usize, Point) -> ComputedVertexIntent,
) -> Result<Option<Vec<Point>>> {
    publish_computed_chain_with_vertex_intents_and_intervals(
        frame,
        points,
        closed_ring,
        budget,
        intent,
        vertex_intent,
        |_, start, end, intended_delta, start_t, end_t| {
            interpolate_chart_interval(start, end, intended_delta, start_t, end_t)
        },
    )
}

pub(crate) fn publish_computed_chain_with_vertex_intents_and_intervals<P: Coordinates + ?Sized>(
    frame: GeographicFrame,
    points: &P,
    closed_ring: bool,
    budget: &mut ExpansionBudget,
    intent: impl Fn(usize, Point, Point) -> Result<ComputedEdgeIntent>,
    vertex_intent: impl Fn(usize, Point) -> ComputedVertexIntent,
    interval: impl FnMut(usize, Point, Point, f64, f64, f64) -> Result<ComputedEdgeInterval>,
) -> Result<Option<Vec<Point>>> {
    let point_count = points.coord_count();
    if point_count == 0 {
        return Ok(None);
    }
    let edge_count = point_count.saturating_sub(1);
    let mut edge_intents = Vec::with_capacity(edge_count);
    for index in 0..edge_count {
        edge_intents.push(intent(
            index,
            points.nth_coord(index),
            points.nth_coord(index.wrapping_add(1)),
        )?);
    }
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
        |index, _, _| {
            Ok(*edge_intents
                .get(index)
                .expect("publication edge belongs to the retained intent inventory"))
        },
        vertex_intent,
        interval,
        &zero_aliases,
    )
}

fn publish_chain_with_aliases<P: Coordinates + ?Sized>(
    frame: GeographicFrame,
    points: &P,
    closed_ring: bool,
    budget: &mut ExpansionBudget,
    intent: impl Fn(usize, Point, Point) -> Result<ComputedEdgeIntent>,
    vertex_intent: impl Fn(usize, Point) -> ComputedVertexIntent,
    mut interval: impl FnMut(usize, Point, Point, f64, f64, f64) -> Result<ComputedEdgeInterval>,
    zero_aliases: &[ZeroLongitudeAlias],
) -> Result<Option<Vec<Point>>> {
    let point_count = points.coord_count();
    if point_count == 0 {
        return Ok(None);
    }
    let edge_count = point_count.saturating_sub(1);
    let first_source = points.nth_coord(0);
    let first = publish_vertex_at(
        frame,
        first_source,
        0,
        vertex_intent(0, first_source),
        zero_aliases,
    )?;
    let mut output = if first == first_source {
        None
    } else {
        Some(vec![first])
    };
    let mut canonical_start = first;

    for index in 0..edge_count {
        let source_start = points.nth_coord(index);
        let source_end = points.nth_coord(index.wrapping_add(1));
        let canonical_end = publish_vertex_at(
            frame,
            source_end,
            index.wrapping_add(1),
            vertex_intent(index.wrapping_add(1), source_end),
            zero_aliases,
        )?;
        match intent(index, source_start, source_end)? {
            ComputedEdgeIntent::Preserved { .. } => {
                if let Some(emitted) = output.as_mut() {
                    emitted.push(canonical_end);
                } else if canonical_end != source_end {
                    output = Some(canonical_prefix(
                        frame,
                        points,
                        index.wrapping_add(1),
                        &vertex_intent,
                        zero_aliases,
                    )?);
                    output
                        .as_mut()
                        .expect("canonicalizing an endpoint allocates the output chain")
                        .push(canonical_end);
                }
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
                        &vertex_intent,
                        zero_aliases,
                    )?);
                    output
                        .as_mut()
                        .expect("canonicalizing an endpoint allocates the output chain")
                        .push(canonical_end);
                }
                canonical_start = canonical_end;
            },
            ComputedEdgeIntent::Displacement(change) => {
                publish_stored_edge(
                    frame,
                    source_start,
                    source_end,
                    canonical_start,
                    canonical_end,
                    change.delta(),
                    points,
                    index,
                    budget,
                    &mut interval,
                    &vertex_intent,
                    zero_aliases,
                    &mut output,
                )?;
                canonical_start = canonical_end;
            },
            ComputedEdgeIntent::Stored(step) => {
                publish_stored_edge(
                    frame,
                    source_start,
                    source_end,
                    canonical_start,
                    canonical_end,
                    step.delta(frame.longitude()),
                    points,
                    index,
                    budget,
                    &mut interval,
                    &vertex_intent,
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
        if let Some(emitted) = output.as_mut() {
            if let Some(last) = emitted.last_mut() {
                *last = first;
            }
        } else if points.nth_coord(point_count.wrapping_sub(1)) != first_source {
            output = Some(canonical_prefix(
                frame,
                points,
                point_count,
                &vertex_intent,
                zero_aliases,
            )?);
            if let Some(last) = output.as_mut().and_then(|emitted| emitted.last_mut()) {
                *last = first;
            }
        }
    }

    Ok(output)
}

#[expect(
    clippy::too_many_arguments,
    clippy::float_cmp,
    reason = "subdivision combines retained source and published endpoints with caller-owned provenance, interpolation and budget; dyadic endpoint identity is exact"
)]
fn publish_stored_edge<P: Coordinates + ?Sized>(
    frame: GeographicFrame,
    source_start: Point,
    source_end: Point,
    canonical_start: Point,
    canonical_end: Point,
    intended_delta: f64,
    source_points: &P,
    edge_index: usize,
    budget: &mut ExpansionBudget,
    interval: &mut impl FnMut(usize, Point, Point, f64, f64, f64) -> Result<ComputedEdgeInterval>,
    vertex_intent: &impl Fn(usize, Point) -> ComputedVertexIntent,
    zero_aliases: &[ZeroLongitudeAlias],
    output: &mut Option<Vec<Point>>,
) -> Result<()> {
    if !intended_delta.is_finite() {
        return Err(GeometryErrorKind::unrepresentable_output("computed edge"));
    }
    let intended_delta = if intended_delta == 0.0 {
        0.0
    } else {
        intended_delta
    };
    if edge_reads_correctly(frame, canonical_start, canonical_end, intended_delta)? {
        match output {
            Some(emitted) => emitted.push(canonical_end),
            None if canonical_end != source_end => {
                *output = Some(canonical_prefix(
                    frame,
                    source_points,
                    edge_index.wrapping_add(1),
                    vertex_intent,
                    zero_aliases,
                )?);
                output
                    .as_mut()
                    .expect("canonicalizing an endpoint allocates the output chain")
                    .push(canonical_end);
            },
            None => {},
        }
        return Ok(());
    }

    preflight_edge_expansion(intended_delta, frame, budget)?;
    if output.is_none() {
        *output = Some(canonical_prefix(
            frame,
            source_points,
            edge_index.wrapping_add(1),
            vertex_intent,
            zero_aliases,
        )?);
    }

    let mut pending = vec![(0.0, 1.0)];
    while let Some((start_t, end_t)) = pending.pop() {
        let piece = interval(
            edge_index,
            source_start,
            source_end,
            intended_delta,
            start_t,
            end_t,
        )?;
        let start = interval_endpoint(
            frame,
            piece.start,
            (start_t == 0.0).then_some(canonical_start),
        )?;
        let end = interval_endpoint(frame, piece.end, (end_t == 1.0).then_some(canonical_end))?;
        let piece_delta = piece.delta;
        // The original edge already failed its reading probe. Its replacement
        // pieces must not introduce half-turn records rejected by chart measures.
        if piece_delta.abs() < frame.longitude().half_period()
            && edge_reads_correctly(frame, start, end, piece_delta)?
        {
            output
                .as_mut()
                .expect("subdivision allocates the output chain before visiting pieces")
                .push(end);
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

fn preflight_edge_expansion(
    intended_delta: f64,
    frame: GeographicFrame,
    budget: &ExpansionBudget,
) -> Result<()> {
    let minimum_pieces = (intended_delta.abs() / frame.longitude().half_period()).ceil();
    if minimum_pieces > 1.0 {
        budget.check_additional((minimum_pieces - 1.0) as usize)?;
    }
    Ok(())
}

#[expect(
    clippy::float_cmp,
    reason = "the stored chart step must equal the retained producer displacement, including signed zero equivalence"
)]
fn edge_reads_correctly(
    frame: GeographicFrame,
    start: Point,
    end: Point,
    intended_delta: f64,
) -> Result<bool> {
    let axis = frame.longitude();
    if axis.ordinary_chart_edge(start.x, end.x) && end.x - start.x == intended_delta {
        debug_assert_eq!(
            axis.chart_step(start.x, end.x)
                .expect("canonical ordinary endpoints are valid")
                .delta(axis),
            intended_delta
        );
        return Ok(true);
    }
    let reading = axis
        .chart_step(start.x, end.x)
        .map_err(map_geographic_error)?;
    Ok(reading.delta(axis) == intended_delta)
}

fn certified_minor_reads(frame: GeographicFrame, start: Point, end: Point) -> bool {
    matches!(
        frame.longitude().chart_step(start.x, end.x),
        Ok(ChartStep::Shortest(EdgeDirection::Minor(delta)))
            if delta.abs() < frame.longitude().half_period()
    )
}

fn interpolate_chart_position(
    start: Point,
    end: Point,
    intended_delta: f64,
    parameter: f64,
) -> Result<Point> {
    let chart_end = start.x + intended_delta;
    Point::new_axes(
        interpolate_subdivision_ordinate(start.x, chart_end, parameter),
        interpolate_subdivision_ordinate(start.y, end.y, parameter),
        ZOrdinate(interpolate_optional_ordinate(start.z(), end.z(), parameter)),
        MOrdinate(interpolate_optional_ordinate(start.m(), end.m(), parameter)),
    )
}

fn interpolate_chart_interval(
    start: Point,
    end: Point,
    intended_delta: f64,
    start_t: f64,
    end_t: f64,
) -> Result<ComputedEdgeInterval> {
    let interval_start = interpolate_chart_position(start, end, intended_delta, start_t)?;
    let interval_end = interpolate_chart_position(start, end, intended_delta, end_t)?;
    Ok(ComputedEdgeInterval {
        start: interval_start,
        end: interval_end,
        delta: interval_end.x - interval_start.x,
    })
}

fn interval_endpoint(
    frame: GeographicFrame,
    point: Point,
    endpoint: Option<Point>,
) -> Result<Point> {
    endpoint.map_or_else(
        || {
            frame
                .canonical_computed_point(point)
                .map_err(map_geographic_error)
        },
        Ok,
    )
}

fn canonical_prefix<P: Coordinates + ?Sized>(
    frame: GeographicFrame,
    points: &P,
    end: usize,
    vertex_intent: &impl Fn(usize, Point) -> ComputedVertexIntent,
    zero_aliases: &[ZeroLongitudeAlias],
) -> Result<Vec<Point>> {
    if end > points.coord_count() {
        return Err(GeometryErrorKind::unrepresentable_output("computed chain"));
    }
    let mut output = Vec::with_capacity(points.coord_count());
    for index in 0..end {
        let point = points.nth_coord(index);
        output.push(publish_vertex_at(
            frame,
            point,
            index,
            vertex_intent(index, point),
            zero_aliases,
        )?);
    }
    Ok(output)
}

fn publish_vertex(
    frame: GeographicFrame,
    point: Point,
    intent: ComputedVertexIntent,
) -> Result<Point> {
    match intent {
        ComputedVertexIntent::Canonical => frame
            .canonical_computed_point(point)
            .map_err(map_geographic_error),
        ComputedVertexIntent::Preserved => Ok(point),
    }
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
        AngularFactor, ChartStep, ComputedEdgeIntent, ComputedEdgeInterval, ComputedVertexIntent,
        EdgeDirection, GeographicFrame, TurnDirection, publish_computed_chain,
        publish_computed_chain_with_vertex_intents,
        publish_computed_chain_with_vertex_intents_and_intervals,
    };
    use crate::geometry::{ExpansionBudget, GeometryErrorKind, MOrdinate, Point, ZOrdinate};

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
        publish_computed_chain(frame, points, closed_ring, &mut budget, |index, _, _| {
            Ok(ComputedEdgeIntent::displacement(steps[index]))
        })
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
                Ok(ComputedEdgeIntent::Stored(ChartStep::Circle(
                    TurnDirection::Positive,
                )))
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
            |_, _, _| Ok(ComputedEdgeIntent::displacement(250.0)),
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
                    ComputedEdgeIntent::displacement(end.x - start.x)
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
            |_, _, _| Ok(ComputedEdgeIntent::displacement(0.0)),
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
                Ok(ComputedEdgeIntent::Stored(ChartStep::Shortest(
                    EdgeDirection::Minor(0.0),
                )))
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(stored, output);
    }

    #[test]
    fn canonical_interval_positions_do_not_replace_retained_chart_displacement() {
        let points = [
            Point::new_raw_xy(10.0, 20.0),
            Point::new_raw_xy(260.0, 40.0),
        ];
        let frame = degrees();
        let mut budget = ExpansionBudget::new("exact interval", "computed edges");
        let output = publish_computed_chain_with_vertex_intents_and_intervals(
            frame,
            points.as_slice(),
            false,
            &mut budget,
            |_, _, _| Ok(ComputedEdgeIntent::displacement(250.0)),
            |_, _| ComputedVertexIntent::Canonical,
            |_, _, _, delta, start_t, end_t| {
                let point_at = |parameter| {
                    frame
                        .canonical_computed_point(Point::new_raw_xy(
                            10.0 + 250.0 * parameter,
                            20.0 + 20.0 * parameter,
                        ))
                        .unwrap()
                };
                Ok(ComputedEdgeInterval {
                    start: point_at(start_t),
                    end: point_at(end_t),
                    delta: delta * (end_t - start_t),
                })
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(output, [
            Point::new_raw_xy(10.0, 20.0),
            Point::new_raw_xy(135.0, 30.0),
            Point::new_raw_xy(-100.0, 40.0),
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
            |_, start, end| Ok(ComputedEdgeIntent::displacement(end.x - start.x)),
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
                    ComputedEdgeIntent::displacement(end.x - start.x)
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
            |_, _, _| Ok(ComputedEdgeIntent::displacement(0.0)),
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
        let output = publish(&ring, true, &[90.0, 90.0, 180.0]);
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
