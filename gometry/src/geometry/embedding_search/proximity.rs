use std::cmp::Ordering;

use crate::collections::{HashMap, UnionFind, try_hash_map_with_capacity};
use crate::error::{ResourceError, Result};
use crate::geometry::segments::ordering::try_sort_by_with_work;
use crate::geometry::{Bounds, Segment, XY, for_each_overlapping_bounds_cross_pair_with_work};
use crate::work::{Unmetered, Work};

const PROXIMITY_RELATIVE_SLACK: f64 = 9.094_947_017_729_282e-13;

/// An emitted segment and the conservative mobility of each endpoint.
#[derive(Clone, Copy)]
pub(in crate::geometry) struct ProximityChord {
    pub nodes: [u32; 2],
    pub nearest: Segment,
    pub can_move: [bool; 2],
}

/// Independent classes reached from witnesses in the nearest assignment.
pub(in crate::geometry) struct ProximityClasses {
    pub classes: Vec<Vec<u32>>,
    pub node_location: Vec<Option<(usize, usize)>>,
    pub radii: Vec<Option<f64>>,
    pub initial_evaluations: u64,
}

pub(in crate::geometry) fn single_step_radius(value: f64) -> f64 {
    if !value.is_finite() {
        return f64::INFINITY;
    }
    (value.next_up() - value).max(value - value.next_down())
}

pub(in crate::geometry) fn swept_bounds(segment: Segment, radius: f64) -> Bounds {
    let bounds = Bounds::from_xy_iter([segment.start, segment.end]);
    if radius == 0.0 {
        return bounds;
    }
    let radius = if radius.is_finite() {
        (radius + radius * PROXIMITY_RELATIVE_SLACK).next_up()
    } else {
        f64::INFINITY
    };
    Bounds::new_raw(
        (bounds.minx() - radius).next_down(),
        (bounds.miny() - radius).next_down(),
        (bounds.maxx() + radius).next_up(),
        (bounds.maxy() + radius).next_up(),
    )
}

pub(in crate::geometry) fn possible_swept_bounds(chord: ProximityChord) -> Bounds {
    let radius = chord
        .can_move
        .into_iter()
        .zip([chord.nearest.start, chord.nearest.end])
        .filter_map(|(can_move, point)| can_move.then_some(point))
        .map(|point| single_step_radius(point.x).max(single_step_radius(point.y)))
        .fold(0.0_f64, f64::max);
    swept_bounds(chord.nearest, radius)
}

pub(in crate::geometry) fn proximity_limit(left_radius: f64, right_radius: f64) -> f64 {
    let radius = left_radius + right_radius;
    if radius == 0.0 || !radius.is_finite() {
        return radius;
    }
    (radius + radius * PROXIMITY_RELATIVE_SLACK).next_up()
}

fn cached_movable(
    node: u32,
    movable_cache: &mut [Option<bool>],
    is_movable: &mut impl FnMut(u32) -> Result<bool>,
) -> Result<bool> {
    let cached = movable_cache
        .get(node as usize)
        .copied()
        .expect("proximity node belongs to the admitted node domain");
    if let Some(movable) = cached {
        return Ok(movable);
    }
    let movable = is_movable(node)?;
    *movable_cache
        .get_mut(node as usize)
        .expect("proximity node belongs to the admitted node domain") = Some(movable);
    Ok(movable)
}

pub(in crate::geometry) fn exact_chord_radius(
    chord: ProximityChord,
    radius_cache: &mut [Option<f64>],
    chord_index: usize,
    movable_cache: &mut [Option<bool>],
    is_movable: &mut impl FnMut(u32) -> Result<bool>,
) -> Result<f64> {
    if let Some(radius) = radius_cache
        .get(chord_index)
        .copied()
        .expect("chord radius cache aligns with the emitted chord table")
    {
        return Ok(radius);
    }
    let mut radius = 0.0_f64;
    let endpoints = [
        Some(chord.nodes[0]),
        (chord.nodes[1] != chord.nodes[0]).then_some(chord.nodes[1]),
    ];
    for node in endpoints.into_iter().flatten() {
        if cached_movable(node, movable_cache, is_movable)? {
            let point = if node == chord.nodes[0] {
                chord.nearest.start
            } else {
                chord.nearest.end
            };
            radius = radius
                .max(single_step_radius(point.x))
                .max(single_step_radius(point.y));
        }
    }
    *radius_cache
        .get_mut(chord_index)
        .expect("chord radius cache aligns with the emitted chord table") = Some(radius);
    Ok(radius)
}

fn chord_movable_endpoints(
    chord: ProximityChord,
    movable_cache: &mut [Option<bool>],
    is_movable: &mut impl FnMut(u32) -> Result<bool>,
) -> Result<[Option<u32>; 2]> {
    let first =
        cached_movable(chord.nodes[0], movable_cache, is_movable)?.then_some(chord.nodes[0]);
    let second = if chord.nodes[0] == chord.nodes[1] {
        None
    } else {
        cached_movable(chord.nodes[1], movable_cache, is_movable)?.then_some(chord.nodes[1])
    };
    Ok([first, second])
}

fn one_step_endpoint_bounds(point: XY) -> Bounds {
    Bounds::new_raw(
        point.x.next_down().max(-f64::MAX),
        point.y.next_down().max(-f64::MAX),
        point.x.next_up().min(f64::MAX),
        point.y.next_up().min(f64::MAX),
    )
}

/// Build ordinary proximity classes with unmetered primitive work while
/// retaining the legacy evaluation tally for the owning Search wrapper.
pub(in crate::geometry) fn build_proximity_classes(
    node_count: usize,
    chords: &[ProximityChord],
    incident_chords: &[Vec<usize>],
    seeds: &[u32],
    movable_cache: &mut [Option<bool>],
    mut is_movable: impl FnMut(u32) -> Result<bool>,
    mut compare_nodes: impl FnMut(u32, u32) -> Result<Ordering>,
    mut segments_within: impl FnMut(Segment, Segment, f64) -> bool,
) -> Result<ProximityClasses> {
    build_proximity_classes_with_work(
        node_count,
        chords,
        incident_chords,
        seeds,
        movable_cache,
        &mut is_movable,
        &mut compare_nodes,
        &mut segments_within,
        &mut Unmetered,
        |_, initial_evaluations, units| {
            *initial_evaluations = (*initial_evaluations)
                .checked_add(units)
                .ok_or(ResourceError::SizeOverflow)?;
            Ok(())
        },
    )
}

/// Expand witness nodes to independent classes whose candidate drawings can
/// change a shared chord predicate.
///
/// Primitive work and evaluation-event accounting use statically selected
/// caller policies, so ordinary preparation can retain its unpaid tally while
/// a borrowed account pays each event before it occurs. Raw charges cover
/// explicit vector, UnionFind, and sorting progress; overlap-sweep, cache-helper,
/// and variable-width arithmetic internals remain with their owning helpers.
#[expect(
    clippy::indexing_slicing,
    reason = "chord endpoint IDs, incidence rows, sweep ordinals, and class ranks are parallel columns built from this admitted graph"
)]
pub(in crate::geometry) fn build_proximity_classes_with_work<W, E, M, N, S>(
    node_count: usize,
    chords: &[ProximityChord],
    incident_chords: &[Vec<usize>],
    seeds: &[u32],
    movable_cache: &mut [Option<bool>],
    mut is_movable: M,
    mut compare_nodes: N,
    mut segments_within: S,
    work: &mut W,
    mut evaluation_event: E,
) -> Result<ProximityClasses>
where
    W: Work,
    E: FnMut(&mut W, &mut u64, u64) -> Result<()>,
    M: FnMut(u32) -> Result<bool>,
    N: FnMut(u32, u32) -> Result<Ordering>,
    S: FnMut(Segment, Segment, f64) -> bool,
{
    if seeds.is_empty() {
        work.charge_product_into_error(node_count, 1)?;
        work.charge_product_into_error(chords.len(), 1)?;
        return Ok(ProximityClasses {
            classes: Vec::new(),
            node_location: vec![None; node_count],
            radii: vec![None; chords.len()],
            initial_evaluations: 0,
        });
    }
    work.charge_product_into_error(node_count, 1)?;
    let mut closure = vec![false; node_count];
    let mut closure_nodes = Vec::new();
    for &node in seeds {
        work.charge_into_error(1)?;
        if cached_movable(node, movable_cache, &mut is_movable)? && !closure[node as usize] {
            work.charge_into_error(2)?;
            closure[node as usize] = true;
            closure_nodes.push(node);
        }
    }
    work.charge_product_into_error(closure_nodes.len(), 1)?;
    let mut frontier_nodes = closure_nodes.clone();
    let mut union = UnionFind::new_with_work(node_count, work)?;
    work.charge_product_into_error(chords.len(), 1)?;
    let all_bounds = chords
        .iter()
        .copied()
        .map(possible_swept_bounds)
        .collect::<Vec<_>>();
    work.charge_product_into_error(chords.len(), 2)?;
    let mut radius_cache = vec![None; chords.len()];
    let mut frontier_seen = vec![false; chords.len()];
    let mut initial_evaluations = 0_u64;
    while !frontier_nodes.is_empty() {
        work.charge_product_into_error(frontier_nodes.len(), 1)?;
        let mut frontier_chords = Vec::new();
        for node in std::mem::take(&mut frontier_nodes) {
            for &chord in incident_chords
                .get(node as usize)
                .expect("frontier node belongs to the incident-chord domain")
            {
                work.charge_into_error(1)?;
                let seen = frontier_seen
                    .get_mut(chord)
                    .expect("incident chord belongs to the emitted chord table");
                if !*seen {
                    work.charge_into_error(2)?;
                    *seen = true;
                    frontier_chords.push(chord);
                }
            }
        }
        for &chord in &frontier_chords {
            work.charge_into_error(1)?;
            *frontier_seen
                .get_mut(chord)
                .expect("frontier chord belongs to the emitted chord table") = false;
        }
        try_sort_by_with_work(&mut frontier_chords, work, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        if frontier_chords.is_empty() {
            break;
        }
        work.charge_product_into_error(frontier_chords.len(), 1)?;
        let left_bounds: Vec<_> = frontier_chords
            .iter()
            .map(|&index| {
                all_bounds
                    .get(index)
                    .copied()
                    .expect("frontier chord belongs to the emitted chord table")
            })
            .collect();
        let sweep_cost =
            u64::try_from(chords.len().div_ceil(16)).map_err(|_| ResourceError::SizeOverflow)?;
        evaluation_event(work, &mut initial_evaluations, sweep_cost)?;
        let mut newly_reached = Vec::new();
        for &chord_index in &frontier_chords {
            work.charge_into_error(1)?;
            let chord = *chords
                .get(chord_index)
                .expect("frontier chord belongs to the emitted chord table");
            let [Some(first), Some(second)] =
                chord_movable_endpoints(chord, movable_cache, &mut is_movable)?
            else {
                continue;
            };
            let start = chord.nearest.start;
            let end = chord.nearest.end;
            evaluation_event(work, &mut initial_evaluations, 1)?;
            if !one_step_endpoint_bounds(start).intersects(one_step_endpoint_bounds(end)) {
                continue;
            }
            union.union_with_work(first as usize, second as usize, work)?;
            for node in [first, second] {
                work.charge_into_error(1)?;
                let is_reached = *closure
                    .get(node as usize)
                    .expect("chord endpoint belongs to the node domain");
                if !is_reached {
                    work.charge_into_error(3)?;
                    *closure
                        .get_mut(node as usize)
                        .expect("chord endpoint belongs to the node domain") = true;
                    closure_nodes.push(node);
                    newly_reached.push(node);
                }
            }
        }
        let flow = for_each_overlapping_bounds_cross_pair_with_work(
            &left_bounds,
            &all_bounds,
            work,
            |left, right, work| {
                work.charge_into_error(1)?;
                let left_index = frontier_chords[left];
                if left_index == right {
                    return Ok(std::ops::ControlFlow::<()>::Continue(()));
                }
                let left_chord = *chords
                    .get(left_index)
                    .expect("frontier chord belongs to the emitted chord table");
                let right_chord = *chords
                    .get(right)
                    .expect("overlap sweep ordinal belongs to the emitted chord table");
                let left_radius = exact_chord_radius(
                    left_chord,
                    &mut radius_cache,
                    left_index,
                    movable_cache,
                    &mut is_movable,
                )?;
                let right_radius = exact_chord_radius(
                    right_chord,
                    &mut radius_cache,
                    right,
                    movable_cache,
                    &mut is_movable,
                )?;
                if !segments_within(
                    left_chord.nearest,
                    right_chord.nearest,
                    proximity_limit(left_radius, right_radius),
                ) {
                    return Ok(std::ops::ControlFlow::<()>::Continue(()));
                }
                let left_movable =
                    chord_movable_endpoints(left_chord, movable_cache, &mut is_movable)?;
                let right_movable =
                    chord_movable_endpoints(right_chord, movable_cache, &mut is_movable)?;
                let movable_nodes = [
                    left_movable[0],
                    left_movable[1],
                    right_movable[0],
                    right_movable[1],
                ];
                for endpoints in [left_movable, right_movable] {
                    if let [Some(first), Some(second)] = endpoints {
                        evaluation_event(work, &mut initial_evaluations, 1)?;
                        union.union_with_work(first as usize, second as usize, work)?;
                    }
                }
                for left_node in left_movable.into_iter().flatten() {
                    for right_node in right_movable.into_iter().flatten() {
                        union.union_with_work(left_node as usize, right_node as usize, work)?;
                    }
                }
                for node in movable_nodes.into_iter().flatten() {
                    work.charge_into_error(1)?;
                    let is_reached = *closure
                        .get(node as usize)
                        .expect("chord endpoint belongs to the node domain");
                    if !is_reached {
                        work.charge_into_error(3)?;
                        *closure
                            .get_mut(node as usize)
                            .expect("chord endpoint belongs to the node domain") = true;
                        closure_nodes.push(node);
                        newly_reached.push(node);
                    }
                }
                Ok(std::ops::ControlFlow::<()>::Continue(()))
            },
        )?;
        debug_assert!(flow.is_continue());
        frontier_nodes = newly_reached;
    }

    work.charge_product_into_error(closure_nodes.len(), 1)?;
    let mut components: HashMap<usize, Vec<u32>> = try_hash_map_with_capacity(closure_nodes.len())?;
    for node in closure_nodes {
        let root = union.find_with_work(node as usize, work)?;
        work.charge_into_error(2)?;
        components.entry(root).or_default().push(node);
    }
    work.charge_product_into_error(components.len(), 1)?;
    let mut classes: Vec<_> = components.into_values().collect();
    for class in &mut classes {
        try_sort_by_with_work(class, work, |left, right, _work| {
            compare_nodes(*left, *right)
        })?;
    }
    try_sort_by_with_work(&mut classes, work, |left, right, _work| {
        compare_nodes(
            *left.first().expect("proximity class is nonempty"),
            *right.first().expect("proximity class is nonempty"),
        )
    })?;
    work.charge_product_into_error(node_count, 1)?;
    let mut node_location = vec![None; node_count];
    for (class, nodes) in classes.iter().enumerate() {
        for (rank, &node) in nodes.iter().enumerate() {
            work.charge_into_error(1)?;
            *node_location
                .get_mut(node as usize)
                .expect("class member belongs to the node domain") = Some((class, rank));
        }
    }
    Ok(ProximityClasses {
        classes,
        node_location,
        radii: radius_cache,
        initial_evaluations,
    })
}
