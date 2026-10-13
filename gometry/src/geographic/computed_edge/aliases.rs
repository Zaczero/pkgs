use super::publish::map_geographic_error;
use super::*;

#[derive(Clone, Copy)]
pub(super) struct ZeroLongitudeAlias {
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

pub(super) fn zero_longitude_aliases<P: Coordinates + ?Sized>(
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

pub(super) fn zero_longitude_alias_at(aliases: &[ZeroLongitudeAlias], index: usize) -> Option<f64> {
    let range = aliases.partition_point(|alias| alias.end <= index);
    aliases
        .get(range)
        .filter(|alias| index >= alias.start)
        .map(|alias| alias.longitude)
}
