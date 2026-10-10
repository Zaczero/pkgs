//! Deterministic search over finite embedding domains.

use std::cmp::Ordering;
use std::hash::Hash;

use crate::collections::{
    HashMap, HashSet, UnionFind, try_hash_map_with_capacity, try_hash_set_with_capacity,
};
use crate::error::{ResourceError, Result};
use crate::geometry::{
    Bounds, GeometryErrorKind, Segment, XY, for_each_overlapping_bounds_cross_pair_with_work,
    try_sort_by_with_work,
};

const BASE_EVALUATION_LIMIT: u64 = 1 << 16;
const EVALUATIONS_PER_CONSTRUCTED_NODE: u64 = 1 << 12;
const WORK_TICKS_PER_EVALUATION: u128 = 16;
const PROXIMITY_RELATIVE_SLACK: f64 = 9.094_947_017_729_282e-13;

/// Raw primitive work. Evaluation events remain a separate unit on the same
/// account; one evaluation consumes sixteen raw work ticks.
pub(crate) trait Work {
    fn charge(&mut self, ticks: u64) -> Result<()>;

    fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
        let ticks = count
            .checked_mul(ticks_per_item)
            .ok_or(ResourceError::SizeOverflow)?;
        self.charge(u64::try_from(ticks).map_err(|_| ResourceError::SizeOverflow)?)
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct Unmetered;

impl Work for Unmetered {
    fn charge(&mut self, _ticks: u64) -> Result<()> {
        Ok(())
    }

    fn charge_product(&mut self, _count: usize, _ticks_per_item: usize) -> Result<()> {
        Ok(())
    }
}

/// One mutable owner for raw primitive work and evaluation events.
pub(in crate::geometry) trait SearchAccount: Work {
    /// Reborrow the same allowance at an explicit nested accounting boundary.
    fn budget_mut(&mut self) -> &mut EvaluationBudget;

    fn charge_evaluations(&mut self, evaluations: u64) -> Result<()> {
        self.budget_mut().debit_evaluations(evaluations)
    }
}

/// Borrowed ordinary policy: preserve evaluation events and erase primitive
/// Work until an owning Native-demand branch explicitly reborrows the budget.
pub(in crate::geometry) struct EvaluationOnly<'a> {
    budget: &'a mut EvaluationBudget,
}

impl<'a> EvaluationOnly<'a> {
    pub(in crate::geometry) const fn new(budget: &'a mut EvaluationBudget) -> Self {
        Self { budget }
    }
}

impl Work for EvaluationOnly<'_> {
    fn charge(&mut self, _ticks: u64) -> Result<()> {
        Ok(())
    }

    fn charge_product(&mut self, _count: usize, _ticks_per_item: usize) -> Result<()> {
        Ok(())
    }
}

impl SearchAccount for EvaluationOnly<'_> {
    fn budget_mut(&mut self) -> &mut EvaluationBudget {
        self.budget
    }
}

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
        work.charge_product(node_count, 1)?;
        work.charge_product(chords.len(), 1)?;
        return Ok(ProximityClasses {
            classes: Vec::new(),
            node_location: vec![None; node_count],
            radii: vec![None; chords.len()],
            initial_evaluations: 0,
        });
    }
    work.charge_product(node_count, 1)?;
    let mut closure = vec![false; node_count];
    let mut closure_nodes = Vec::new();
    for &node in seeds {
        work.charge(1)?;
        if cached_movable(node, movable_cache, &mut is_movable)? && !closure[node as usize] {
            work.charge(2)?;
            closure[node as usize] = true;
            closure_nodes.push(node);
        }
    }
    work.charge_product(closure_nodes.len(), 1)?;
    let mut frontier_nodes = closure_nodes.clone();
    let mut union = UnionFind::new_with_work(node_count, work)?;
    work.charge_product(chords.len(), 1)?;
    let all_bounds = chords
        .iter()
        .copied()
        .map(possible_swept_bounds)
        .collect::<Vec<_>>();
    work.charge_product(chords.len(), 2)?;
    let mut radius_cache = vec![None; chords.len()];
    let mut frontier_seen = vec![false; chords.len()];
    let mut initial_evaluations = 0_u64;
    while !frontier_nodes.is_empty() {
        work.charge_product(frontier_nodes.len(), 1)?;
        let mut frontier_chords = Vec::new();
        for node in std::mem::take(&mut frontier_nodes) {
            for &chord in incident_chords
                .get(node as usize)
                .expect("frontier node belongs to the incident-chord domain")
            {
                work.charge(1)?;
                let seen = frontier_seen
                    .get_mut(chord)
                    .expect("incident chord belongs to the emitted chord table");
                if !*seen {
                    work.charge(2)?;
                    *seen = true;
                    frontier_chords.push(chord);
                }
            }
        }
        for &chord in &frontier_chords {
            work.charge(1)?;
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
        work.charge_product(frontier_chords.len(), 1)?;
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
            work.charge(1)?;
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
                work.charge(1)?;
                let is_reached = *closure
                    .get(node as usize)
                    .expect("chord endpoint belongs to the node domain");
                if !is_reached {
                    work.charge(3)?;
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
                work.charge(1)?;
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
                    work.charge(1)?;
                    let is_reached = *closure
                        .get(node as usize)
                        .expect("chord endpoint belongs to the node domain");
                    if !is_reached {
                        work.charge(3)?;
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

    work.charge_product(closure_nodes.len(), 1)?;
    let mut components: HashMap<usize, Vec<u32>> = try_hash_map_with_capacity(closure_nodes.len())?;
    for node in closure_nodes {
        let root = union.find_with_work(node as usize, work)?;
        work.charge(2)?;
        components.entry(root).or_default().push(node);
    }
    work.charge_product(components.len(), 1)?;
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
    work.charge_product(node_count, 1)?;
    let mut node_location = vec![None; node_count];
    for (class, nodes) in classes.iter().enumerate() {
        for (rank, &node) in nodes.iter().enumerate() {
            work.charge(1)?;
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

/// A candidate assignment for one independent class of movable nodes.
#[derive(Clone, Copy)]
pub(in crate::geometry) struct ClassAssignment<'a, N, C> {
    pub variables: &'a [N],
    pub values: &'a [C],
}

#[derive(Debug)]
pub(in crate::geometry) struct ClassSolution<N, C> {
    pub variables: Vec<N>,
    pub values: Vec<C>,
}

#[derive(Debug)]
pub(in crate::geometry) struct SearchAssignment<N, C, O = ()> {
    pub classes: Vec<ClassSolution<N, C>>,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "production problems return unit; generic search tests exercise moving non-Clone global outputs"
        )
    )]
    pub output: O,
    #[cfg(test)]
    pub evaluations: u64,
}

/// A global G result; rejection names implicated indices from `Problem::classes()`.
///
/// An empty rejection asserts that no prepared variable assignment can repair
/// this rejection.
/// The first nonempty rejection uses its blame to choose a fixed traversal
/// order, but that order must not prune the Cartesian product of prepared local
/// solution streams.
pub(in crate::geometry) enum GlobalCheck<O = ()> {
    Pass(O),
    Reject(Vec<usize>),
}

#[derive(Debug)]
pub(in crate::geometry) enum SearchOutcome<T> {
    Pass(T),
    Exhausted,
}

type SearchResult<N, C, O> = Result<SearchOutcome<SearchAssignment<N, C, O>>>;

/// Domain-specific predicates around the common matching and search algorithm.
///
/// Classes and variables may arrive in any order. Candidate comparison orders
/// values by exact displacement, then by the domain's deterministic tie-break.
/// `fixed_keys` contains the nearest cells occupied by nodes outside the
/// current class, including members of other classes. `check_local` evaluates
/// ready pair and fan predicates for the assigned prefix; each predicate
/// charges the shared budget before running. Each local solution stream is
/// finite, deterministic, and independent of other classes' current assignments.
/// The search enumerates their complete Cartesian product. Nonempty
/// `check_global` blame chooses the product's digit order once; later blame
/// never changes or prunes that order. Empty blame asserts that no prepared
/// assignment can repair the rejection.
pub(in crate::geometry) trait Problem {
    type Node: Copy;
    type Candidate: Copy;
    type Key: Copy + Eq + Hash;
    type ClassState;
    type Output;

    /// Each movable node appears in exactly one nonempty class.
    fn classes(&self) -> &[Vec<Self::Node>];

    /// Nearest-coordinate keys for all live nodes outside this class.
    fn fixed_keys<W: Work>(
        &self,
        class: usize,
        variables: &[Self::Node],
        work: &mut W,
    ) -> Result<Vec<Self::Key>>;

    fn compare_nodes(&self, left: Self::Node, right: Self::Node) -> Result<Ordering>;

    /// Append domain-admitted candidates for `node`; their input order is ignored.
    fn fill_candidates<W: Work>(
        &self,
        node: Self::Node,
        candidates: &mut Vec<Self::Candidate>,
        work: &mut W,
    ) -> Result<()>;

    /// Exact displacement order, followed by the domain's coordinate tie-break.
    fn compare_candidates(
        &self,
        node: Self::Node,
        left: Self::Candidate,
        right: Self::Candidate,
    ) -> Result<Ordering>;

    fn candidate_key(&self, candidate: Self::Candidate) -> Self::Key;

    /// Prepare proximity-partner and ready-predicate data for one ordered class.
    ///
    /// Charge any closure sweep before performing it.
    fn prepare_class<A: SearchAccount>(
        &self,
        class: usize,
        variables: &[Self::Node],
        account: &mut A,
    ) -> Result<Self::ClassState>;

    /// Run ready P/F predicates for `assigned`, which contains the ordered prefix through `rank`.
    fn check_local<A: SearchAccount>(
        &self,
        class_state: &Self::ClassState,
        class: usize,
        variables: &[Self::Node],
        rank: usize,
        assigned: &[Option<Self::Candidate>],
        account: &mut A,
    ) -> Result<bool>;

    /// Run G on the composed assignment and name classes implicated by a rejection.
    ///
    /// The first nonempty blame prioritizes digit order, not products to skip.
    /// Later blame is validated but cannot change product order. Return empty
    /// blame only when no prepared variable assignment can repair the rejection;
    /// otherwise name at least one influential prepared class.
    /// Charge `items.len().div_ceil(16)` for each G sweep before running it.
    ///
    /// A global callback may update its own history and output, but must not
    /// change prepared classes, candidate/key domains or ordering, class state,
    /// or local-predicate behavior between calls.
    fn check_global<A: SearchAccount>(
        &mut self,
        assignments: &[ClassAssignment<'_, Self::Node, Self::Candidate>],
        account: &mut A,
    ) -> Result<GlobalCheck<Self::Output>>;

    /// Work units for closure sweeps already performed while building seeds and classes.
    fn initial_evaluations(&self) -> u64 {
        0
    }
}

pub(in crate::geometry) struct EvaluationBudget {
    operation: &'static str,
    constructed: usize,
    limit: u64,
    spent_ticks: u128,
}

impl EvaluationBudget {
    pub(in crate::geometry) fn new(operation: &'static str, constructed: usize) -> Result<Self> {
        let constructed = u64::try_from(constructed).map_err(|_| ResourceError::SizeOverflow)?;
        let per_node = EVALUATIONS_PER_CONSTRUCTED_NODE
            .checked_mul(constructed)
            .ok_or(ResourceError::SizeOverflow)?;
        Ok(Self {
            operation,
            constructed: usize::try_from(constructed).map_err(|_| ResourceError::SizeOverflow)?,
            limit: BASE_EVALUATION_LIMIT.max(per_node),
            spent_ticks: 0,
        })
    }

    #[cfg(test)]
    pub(in crate::geometry) fn with_remaining_ticks_for_test(
        operation: &'static str,
        constructed: usize,
        remaining_ticks: u128,
    ) -> Result<Self> {
        let mut budget = Self::new(operation, constructed)?;
        let limit_ticks = budget.limit_ticks();
        budget.spent_ticks = limit_ticks
            .checked_sub(remaining_ticks)
            .ok_or(ResourceError::SizeOverflow)?;
        Ok(budget)
    }

    pub(in crate::geometry) fn set_constructed(&mut self, constructed: usize) -> Result<()> {
        let constructed = u64::try_from(constructed).map_err(|_| ResourceError::SizeOverflow)?;
        let per_node = EVALUATIONS_PER_CONSTRUCTED_NODE
            .checked_mul(constructed)
            .ok_or(ResourceError::SizeOverflow)?;
        self.constructed = usize::try_from(constructed).map_err(|_| ResourceError::SizeOverflow)?;
        self.limit = BASE_EVALUATION_LIMIT.max(per_node);
        if self.spent_ticks > self.limit_ticks() {
            return Err(self.limit_error());
        }
        Ok(())
    }

    /// Charge an evaluation event before running the corresponding predicate.
    fn debit_evaluations(&mut self, units: u64) -> Result<()> {
        let ticks = u128::from(units) * WORK_TICKS_PER_EVALUATION;
        self.debit_ticks(ticks)
    }

    fn debit_ticks(&mut self, ticks: u128) -> Result<()> {
        let Some(next) = self.spent_ticks.checked_add(ticks) else {
            return Err(self.limit_error());
        };
        if next > self.limit_ticks() {
            return Err(self.limit_error());
        }
        self.spent_ticks = next;
        Ok(())
    }

    /// Record work that the problem had to complete before search began.
    pub(in crate::geometry) fn record_completed_evaluations(&mut self, units: u64) -> Result<()> {
        let completed = self
            .evaluation_count()
            .checked_add(units)
            .ok_or_else(|| self.limit_error())?;
        let ticks = u128::from(units) * WORK_TICKS_PER_EVALUATION;
        self.spent_ticks = self
            .spent_ticks
            .checked_add(ticks)
            .ok_or_else(|| self.limit_error())?;
        if completed > self.limit {
            return Err(self.limit_error());
        }
        Ok(())
    }

    fn limit_ticks(&self) -> u128 {
        u128::from(self.limit) * WORK_TICKS_PER_EVALUATION
    }

    fn evaluation_count(&self) -> u64 {
        u64::try_from(self.spent_ticks.div_ceil(WORK_TICKS_PER_EVALUATION))
            .expect("the single raw-work balance fits the u64 evaluation error field")
    }

    fn limit_error(&self) -> crate::error::Error {
        GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: self.operation,
            constructed: self.constructed,
            evaluations: self.evaluation_count(),
        }
        .into()
    }

    fn unrepresentable(&self) -> crate::error::Error {
        GeometryErrorKind::ExactEmbeddingUnrepresentable {
            operation: self.operation,
            constructed: self.constructed,
        }
        .into()
    }
}

impl Work for EvaluationBudget {
    fn charge(&mut self, ticks: u64) -> Result<()> {
        self.debit_ticks(u128::from(ticks))
    }

    fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
        let ticks = (count as u128).wrapping_mul(ticks_per_item as u128);
        self.debit_ticks(ticks)
    }
}

impl SearchAccount for EvaluationBudget {
    fn budget_mut(&mut self) -> &mut EvaluationBudget {
        self
    }
}

struct ClassSpec<N> {
    class: usize,
    variables: Vec<N>,
}

struct PreparedClass<N, C, K, S> {
    class: usize,
    variables: Vec<N>,
    options: Vec<Vec<(C, K)>>,
    local: S,
}

type PreparedClassSet<P> = (
    Vec<
        PreparedClass<
            <P as Problem>::Node,
            <P as Problem>::Candidate,
            <P as Problem>::Key,
            <P as Problem>::ClassState,
        >,
    >,
    Vec<usize>,
);

struct PreparedOptions<N, C, K> {
    class: usize,
    variables: Vec<N>,
    options: Vec<Vec<(C, K)>>,
}

struct ClassSearchState<C, K> {
    next_candidate: Vec<usize>,
    selected: Vec<Option<C>>,
    selected_keys: Vec<Option<K>>,
    used_keys: HashSet<K>,
    yielded: bool,
    exhausted: bool,
}

impl<C: Copy, K: Copy + Eq + Hash> ClassSearchState<C, K> {
    fn new<N, S, W: Work>(class: &PreparedClass<N, C, K, S>, work: &mut W) -> Result<Self> {
        let candidate_count = class.options.iter().try_fold(0_usize, |count, options| {
            count
                .checked_add(options.len())
                .ok_or(ResourceError::SizeOverflow)
        })?;
        work.charge_product(class.variables.len(), 3)?;
        work.charge_product(candidate_count, 1)?;
        Ok(Self {
            next_candidate: vec![0; class.variables.len()],
            selected: vec![None; class.variables.len()],
            selected_keys: vec![None; class.variables.len()],
            used_keys: try_hash_set_with_capacity(candidate_count)?,
            yielded: false,
            exhausted: false,
        })
    }

    fn reset<W: Work>(&mut self, work: &mut W) -> Result<()> {
        work.charge_product(self.next_candidate.len(), 3)?;
        work.charge_product(self.used_keys.capacity(), 1)?;
        work.charge_product(2, 1)?;
        self.next_candidate.fill(0);
        self.selected.fill(None);
        self.selected_keys.fill(None);
        self.used_keys.clear();
        self.yielded = false;
        self.exhausted = false;
        Ok(())
    }

    fn unassign<W: Work>(&mut self, rank: usize, work: &mut W) -> Result<()> {
        let key = self
            .selected_keys
            .get(rank)
            .copied()
            .expect("rank belongs to the class search state");
        work.charge(2)?;
        if let Some(key) = key {
            self.used_keys.remove(&key);
        }
        *self
            .selected
            .get_mut(rank)
            .expect("rank belongs to the class search state") = None;
        *self
            .selected_keys
            .get_mut(rank)
            .expect("rank belongs to the class search state") = None;
        Ok(())
    }

    fn next_solution<P, A: SearchAccount>(
        &mut self,
        class: &PreparedClass<P::Node, P::Candidate, P::Key, P::ClassState>,
        problem: &P,
        account: &mut A,
    ) -> Result<Option<Vec<P::Candidate>>>
    where
        P: Problem<Key = K, Candidate = C>,
    {
        if self.exhausted {
            return Ok(None);
        }
        let variable_count = class.variables.len();

        let mut rank = if self.yielded {
            let last = variable_count.wrapping_sub(1);
            self.unassign(last, account)?;
            account.charge(1)?;
            self.yielded = false;
            last
        } else {
            0
        };

        loop {
            let options = class
                .options
                .get(rank)
                .expect("rank is within the prepared class");
            let candidate_index = *self
                .next_candidate
                .get(rank)
                .expect("rank is within the class search state");
            if candidate_index >= options.len() {
                account.charge(1)?;
                *self
                    .next_candidate
                    .get_mut(rank)
                    .expect("rank is within the class search state") = 0;
                if rank == 0 {
                    account.charge(1)?;
                    self.exhausted = true;
                    return Ok(None);
                }
                rank = rank.wrapping_sub(1);
                self.unassign(rank, account)?;
                continue;
            }

            account.charge_evaluations(1)?;
            account.charge(1)?;
            *self
                .next_candidate
                .get_mut(rank)
                .expect("rank is within the class search state") = candidate_index.wrapping_add(1);
            let (candidate, key) = *options
                .get(candidate_index)
                .expect("candidate ordinal is bounded by its option row");
            account.charge(3)?;
            if !self.used_keys.insert(key) {
                continue;
            }
            *self
                .selected
                .get_mut(rank)
                .expect("rank is within the class search state") = Some(candidate);
            *self
                .selected_keys
                .get_mut(rank)
                .expect("rank is within the class search state") = Some(key);

            let assigned = self
                .selected
                .get(..rank.wrapping_add(1))
                .expect("assigned values are the class prefix");
            if !problem.check_local(
                &class.local,
                class.class,
                &class.variables,
                rank,
                assigned,
                account,
            )? {
                self.unassign(rank, account)?;
                continue;
            }
            if rank.wrapping_add(1) == variable_count {
                account.charge_product(variable_count, 2)?;
                self.yielded = true;
                let mut solution = Vec::with_capacity(variable_count);
                for value in &self.selected {
                    solution
                        .push(value.expect("a complete local solution assigns every class member"));
                }
                return Ok(Some(solution));
            }
            rank = rank.wrapping_add(1);
            account.charge(1)?;
            *self
                .next_candidate
                .get_mut(rank)
                .expect("the next rank belongs to the class search state") = 0;
        }
    }
}

pub(in crate::geometry) struct Search<P: Problem> {
    problem: P,
    budget: EvaluationBudget,
}

impl<P: Problem> Search<P> {
    /// `constructed` is the number of conflicting constructed vertices; it sizes
    /// the predicate budget and is retained in any search-limit error.
    pub(in crate::geometry) fn new(
        problem: P,
        operation: &'static str,
        constructed: usize,
    ) -> Result<Self> {
        Ok(Self::with_budget(
            problem,
            EvaluationBudget::new(operation, constructed)?,
        ))
    }

    pub(in crate::geometry) const fn with_budget(problem: P, budget: EvaluationBudget) -> Self {
        Self { problem, budget }
    }

    #[cfg(test)]
    pub(in crate::geometry) fn with_evaluation_limit(
        problem: P,
        operation: &'static str,
        constructed: usize,
        limit: u64,
    ) -> Result<Self> {
        let mut search = Self::new(problem, operation, constructed)?;
        search.budget.limit = limit;
        Ok(search)
    }

    pub(in crate::geometry) fn run(
        mut self,
    ) -> Result<SearchAssignment<P::Node, P::Candidate, P::Output>> {
        self.budget
            .record_completed_evaluations(self.problem.initial_evaluations())?;
        let outcome = {
            let mut account = EvaluationOnly::new(&mut self.budget);
            run_with_budget(&mut self.problem, &mut account)?
        };
        match outcome {
            SearchOutcome::Pass(assignment) => Ok(assignment),
            SearchOutcome::Exhausted => Err(self.budget.unrepresentable()),
        }
    }
}

pub(in crate::geometry) fn run_with_budget<P: Problem, A: SearchAccount>(
    problem: &mut P,
    account: &mut A,
) -> SearchResult<P::Node, P::Candidate, P::Output> {
    let Some((prepared, class_positions)) = prepare_classes(problem, account)? else {
        return Ok(SearchOutcome::Exhausted);
    };
    account.charge_product(prepared.len(), 2)?;
    let mut states = Vec::with_capacity(prepared.len());
    let mut current_solutions = Vec::with_capacity(prepared.len());
    for class in &prepared {
        let mut state = ClassSearchState::new(class, account)?;
        let Some(solution) = state.next_solution(class, problem, account)? else {
            return Ok(SearchOutcome::Exhausted);
        };
        account.charge(2)?;
        states.push(state);
        current_solutions.push(solution);
    }
    let mut product_order = None;
    loop {
        let global = {
            account.charge_product(prepared.len(), 1)?;
            let mut assignments = Vec::with_capacity(prepared.len());
            for (class, values) in prepared.iter().zip(&current_solutions) {
                account.charge(1)?;
                assignments.push(ClassAssignment {
                    variables: &class.variables,
                    values,
                });
            }
            problem.check_global(&assignments, account)?
        };
        match global {
            GlobalCheck::Pass(output) => {
                account.charge_product(prepared.len(), 1)?;
                let mut classes = Vec::with_capacity(prepared.len());
                for (class, values) in prepared.into_iter().zip(current_solutions) {
                    account.charge(1)?;
                    classes.push(ClassSolution {
                        variables: class.variables,
                        values,
                    });
                }
                return Ok(SearchOutcome::Pass(SearchAssignment {
                    classes,
                    output,
                    #[cfg(test)]
                    evaluations: account.budget_mut().evaluation_count(),
                }));
            },
            GlobalCheck::Reject(blamed_classes) => {
                if blamed_classes.is_empty() {
                    return Ok(SearchOutcome::Exhausted);
                }
                if product_order.is_none() {
                    account.charge_product(prepared.len(), 1)?;
                    let mut order = Vec::with_capacity(prepared.len());
                    for class in blamed_classes {
                        account.charge(1)?;
                        let position = prepared_position_for_blame(&class_positions, class)?;
                        account.charge(1)?;
                        order.push(position);
                    }
                    try_sort_by_with_work(&mut order, account, |left, right, _work| {
                        Ok(left.cmp(right))
                    })?;
                    let blamed_count = order.len();
                    account.charge_product(blamed_count, 1)?;
                    order.dedup();
                    let blamed_count = order.len();
                    for position in 0..prepared.len() {
                        let is_blamed = {
                            let blamed_prefix = order
                                .get(..blamed_count)
                                .expect("blamed prefix belongs to the product order");
                            contains_ordered_position(blamed_prefix, position, account)?
                        };
                        if !is_blamed {
                            account.charge(1)?;
                            order.push(position);
                        }
                    }
                    product_order = Some(order);
                } else {
                    for class in blamed_classes {
                        account.charge(1)?;
                        prepared_position_for_blame(&class_positions, class)?;
                    }
                }

                let order = product_order
                    .as_ref()
                    .expect("a nonempty rejection establishes the product order");
                let mut advanced = false;
                for &position in order {
                    account.charge(1)?;
                    let class = prepared
                        .get(position)
                        .expect("class rank is within prepared classes");
                    let state = states
                        .get_mut(position)
                        .expect("class rank is within class search states");
                    if let Some(solution) = state.next_solution(class, problem, account)? {
                        replace_class_solution(
                            current_solutions.as_mut_slice(),
                            position,
                            solution,
                            account,
                        )?;
                        advanced = true;
                        break;
                    }

                    state.reset(account)?;
                    let Some(solution) = state.next_solution(class, problem, account)? else {
                        return Err(GeometryErrorKind::Invalid(
                            "embedding search could not restart a prepared local solution stream"
                                .into(),
                        )
                        .into());
                    };
                    replace_class_solution(
                        current_solutions.as_mut_slice(),
                        position,
                        solution,
                        account,
                    )?;
                }
                if !advanced {
                    return Ok(SearchOutcome::Exhausted);
                }
            },
        }
    }
}

fn prepared_position_for_blame(class_positions: &[usize], class: usize) -> Result<usize> {
    class_positions.get(class).copied().ok_or_else(|| {
        GeometryErrorKind::Invalid("embedding search blamed an unknown variable class".into())
            .into()
    })
}

fn contains_ordered_position<W: Work>(
    values: &[usize],
    target: usize,
    work: &mut W,
) -> Result<bool> {
    let mut left = 0_usize;
    let mut right = values.len();
    while left < right {
        work.charge(1)?;
        let middle = left.wrapping_add(right.wrapping_sub(left) / 2);
        let value = *values
            .get(middle)
            .expect("binary search middle belongs to the admitted prefix");
        match value.cmp(&target) {
            Ordering::Less => left = middle.wrapping_add(1),
            Ordering::Greater => right = middle,
            Ordering::Equal => return Ok(true),
        }
    }
    Ok(false)
}

fn replace_class_solution<C, W: Work>(
    current_solutions: &mut [Vec<C>],
    position: usize,
    solution: Vec<C>,
    work: &mut W,
) -> Result<()> {
    work.charge(1)?;
    *current_solutions
        .get_mut(position)
        .expect("class rank is within current solutions") = solution;
    Ok(())
}

fn prepare_classes<P: Problem, A: SearchAccount>(
    problem: &P,
    account: &mut A,
) -> Result<Option<PreparedClassSet<P>>> {
    let classes = problem.classes();

    account.charge_product(classes.len(), 1)?;
    let mut specs = Vec::with_capacity(classes.len());
    for (class, variables) in classes.iter().enumerate() {
        if variables.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "embedding search received an empty variable class".into(),
            )
            .into());
        }
        account.charge_product(variables.len(), 1)?;
        let mut variables = variables.clone();
        try_sort_by_with_work(&mut variables, account, |left, right, _work| {
            problem.compare_nodes(*left, *right)
        })?;
        account.charge(1)?;
        specs.push(ClassSpec { class, variables });
    }
    try_sort_by_with_work(&mut specs, account, |left, right, _work| {
        let left_class = left.class;
        let right_class = right.class;
        let left_node = *left
            .variables
            .first()
            .expect("variable classes are nonempty");
        let right_node = *right
            .variables
            .first()
            .expect("variable classes are nonempty");
        let order = problem.compare_nodes(left_node, right_node)?;
        Ok(if order == Ordering::Equal {
            left_class.cmp(&right_class)
        } else {
            order
        })
    })?;

    let mut candidate_scratch = Vec::new();
    let mut seen_keys = Vec::new();
    account.charge_product(specs.len(), 1)?;
    let mut admitted = Vec::with_capacity(specs.len());
    for spec in specs {
        let fixed_candidates = problem.fixed_keys(spec.class, &spec.variables, account)?;
        account.charge_product(fixed_candidates.len(), 1)?;
        let mut fixed_keys = try_hash_set_with_capacity(fixed_candidates.len())?;
        for key in fixed_candidates {
            account.charge(1)?;
            fixed_keys.insert(key);
        }
        account.charge_product(spec.variables.len(), 1)?;
        let mut options = Vec::with_capacity(spec.variables.len());
        for &node in &spec.variables {
            account.charge_product(candidate_scratch.len(), 1)?;
            candidate_scratch.clear();
            problem.fill_candidates(node, &mut candidate_scratch, account)?;
            sort_candidates(problem, node, &mut candidate_scratch, account)?;
            account.charge_product(seen_keys.len(), 1)?;
            seen_keys.clear();
            let additional_keys = candidate_scratch.len().saturating_sub(seen_keys.capacity());
            account.charge_product(additional_keys, 1)?;
            seen_keys.reserve(candidate_scratch.len());
            account.charge_product(candidate_scratch.len(), 1)?;
            let mut node_options = Vec::with_capacity(candidate_scratch.len());
            for candidate in candidate_scratch.iter().copied() {
                account.charge(1)?;
                let key = problem.candidate_key(candidate);
                account.charge_evaluations(1)?;
                if fixed_keys.contains(&key) || seen_keys.contains(&key) {
                    continue;
                }
                account.charge(2)?;
                seen_keys.push(key);
                node_options.push((candidate, key));
            }
            if node_options.is_empty() {
                return Ok(None);
            }
            account.charge(1)?;
            options.push(node_options);
        }

        let class = PreparedOptions {
            class: spec.class,
            variables: spec.variables,
            options,
        };
        if !has_perfect_matching(&class, account)? {
            return Ok(None);
        }
        account.charge(1)?;
        admitted.push(class);
    }

    account.charge_product(admitted.len(), 1)?;
    account.charge_product(classes.len(), 1)?;
    let mut prepared = Vec::with_capacity(admitted.len());
    let mut class_positions = vec![0; classes.len()];
    // Hall failures should reject the whole problem before domain-specific
    // preparation materializes potentially dense proximity partners.
    for class in admitted {
        let local = problem.prepare_class(class.class, &class.variables, account)?;
        let class = PreparedClass {
            class: class.class,
            variables: class.variables,
            options: class.options,
            local,
        };
        let position = prepared.len();
        account.charge(2)?;
        *class_positions
            .get_mut(class.class)
            .expect("class IDs are the indexes in Problem::classes") = position;
        prepared.push(class);
    }
    Ok(Some((prepared, class_positions)))
}

fn sort_candidates<P: Problem, W: Work>(
    problem: &P,
    node: P::Node,
    candidates: &mut [P::Candidate],
    work: &mut W,
) -> Result<()> {
    for index in 1..candidates.len() {
        work.charge(1)?;
        let candidate = *candidates
            .get(index)
            .expect("candidate index is within the vector");
        let mut position = index;
        while position > 0 {
            let previous = position.wrapping_sub(1);
            work.charge(1)?;
            let previous_candidate = *candidates
                .get(previous)
                .expect("previous candidate is within the sorted prefix");
            if problem.compare_candidates(node, candidate, previous_candidate)? != Ordering::Less {
                break;
            }
            work.charge(2)?;
            candidates.swap(position, previous);
            position = previous;
        }
    }
    Ok(())
}

fn has_perfect_matching<N, C, K, W: Work>(
    class: &PreparedOptions<N, C, K>,
    work: &mut W,
) -> Result<bool>
where
    K: Copy + Eq + Hash,
{
    let variable_count = class.variables.len();
    let candidate_count = class.options.iter().try_fold(0_usize, |count, options| {
        count
            .checked_add(options.len())
            .ok_or(ResourceError::SizeOverflow)
    })?;
    work.charge_product(candidate_count, 2)?;
    work.charge_product(variable_count, 3)?;
    let mut owners = try_hash_map_with_capacity(candidate_count)?;
    let mut seen_keys = try_hash_set_with_capacity(candidate_count)?;
    let mut seen_variables = Vec::with_capacity(variable_count);
    seen_variables.resize(variable_count, usize::MAX);
    let mut parents = Vec::with_capacity(variable_count);
    parents.resize(variable_count, None::<(usize, K)>);
    let mut stack = Vec::with_capacity(variable_count);

    for root in 0..variable_count {
        work.charge_product(seen_keys.capacity(), 1)?;
        seen_keys.clear();
        work.charge_product(stack.len(), 1)?;
        stack.clear();
        work.charge(2)?;
        *seen_variables
            .get_mut(root)
            .expect("root variable is within the class") = root;
        *parents
            .get_mut(root)
            .expect("root variable is within the class") = None;
        work.charge(1)?;
        stack.push(MatchFrame {
            variable: root,
            next_candidate: 0,
        });

        let mut free_cell = None;
        loop {
            let (variable, candidate_index) = {
                let Some(frame) = stack.last_mut() else {
                    break;
                };
                let options = class
                    .options
                    .get(frame.variable)
                    .expect("matching variable is within the class");
                if frame.next_candidate == options.len() {
                    work.charge(1)?;
                    stack.pop();
                    continue;
                }
                let index = frame.next_candidate;
                work.charge(1)?;
                frame.next_candidate = frame.next_candidate.wrapping_add(1);
                (frame.variable, index)
            };
            let (_, key) = *class
                .options
                .get(variable)
                .and_then(|options| options.get(candidate_index))
                .expect("matching candidate is within its row");
            work.charge(1)?;
            if !seen_keys.insert(key) {
                continue;
            }
            work.charge(1)?;
            match owners.get(&key).copied() {
                None => {
                    free_cell = Some((variable, key));
                    break;
                },
                Some(owner) => {
                    work.charge(1)?;
                    if *seen_variables
                        .get(owner)
                        .expect("matched variable is within the class")
                        == root
                    {
                        continue;
                    }
                    work.charge(2)?;
                    *seen_variables
                        .get_mut(owner)
                        .expect("matched variable is within the class") = root;
                    *parents
                        .get_mut(owner)
                        .expect("matched variable is within the class") = Some((variable, key));
                    work.charge(1)?;
                    stack.push(MatchFrame {
                        variable: owner,
                        next_candidate: 0,
                    });
                },
            }
        }

        let Some((mut variable, mut key)) = free_cell else {
            return Ok(false);
        };
        loop {
            work.charge(1)?;
            owners.insert(key, variable);
            work.charge(1)?;
            let Some((parent, parent_key)) = parents
                .get(variable)
                .copied()
                .expect("matched variable is within the class")
            else {
                break;
            };
            variable = parent;
            key = parent_key;
        }
    }
    Ok(true)
}

struct MatchFrame {
    variable: usize,
    next_candidate: usize,
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::ops::ControlFlow;
    use std::rc::Rc;

    use super::*;
    use crate::error::ErrorKind;
    use crate::geometry::segment_index::sweep::{
        for_each_candidate_pair_with_work as sweep_for_each_candidate_pair_with_work,
        for_each_overlapping_bounds_cross_pair_with_work as sweep_for_each_overlapping_bounds_cross_pair_with_work,
        for_each_overlapping_bounds_pair_with_work as sweep_for_each_overlapping_bounds_pair_with_work,
    };
    use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational, round_ratio_with_work};

    type Assignment = Vec<(usize, u8)>;
    type AssignmentBlame = Vec<(Assignment, Vec<usize>)>;
    type AssignmentVisitLog = Rc<RefCell<Vec<Assignment>>>;

    #[derive(Clone, Copy)]
    enum NestedSearch {
        None,
        HallOn(u8),
        HallAlways,
    }

    #[test]
    fn ordinary_and_paid_proximity_builders_keep_their_evaluation_ownership() {
        let chords = [ProximityChord {
            nodes: [0, 1],
            nearest: Segment {
                start: XY::new(0.0, 0.0),
                end: XY::new(0.0, 0.0),
            },
            can_move: [true, true],
        }];
        let incident_chords = [vec![0], vec![0]];
        let seeds = [0];
        let mut ordinary_movable = vec![None; 2];
        let ordinary = build_proximity_classes(
            2,
            &chords,
            &incident_chords,
            &seeds,
            &mut ordinary_movable,
            |_| Ok(true),
            |left, right| Ok(left.cmp(&right)),
            |_, _, _| true,
        )
        .unwrap();

        let mut paid_movable = vec![None; 2];
        let mut budget = EvaluationBudget::new("search_test", 2).unwrap();
        let mut paid_evaluations = 0_u64;
        let paid = build_proximity_classes_with_work(
            2,
            &chords,
            &incident_chords,
            &seeds,
            &mut paid_movable,
            |_| Ok(true),
            |left, right| Ok(left.cmp(&right)),
            |_, _, _| true,
            &mut budget,
            |account, _initial_evaluations, units| {
                account.charge_evaluations(units)?;
                paid_evaluations = paid_evaluations
                    .checked_add(units)
                    .ok_or(ResourceError::SizeOverflow)?;
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(ordinary.classes, [vec![0, 1]]);
        assert_eq!(ordinary.initial_evaluations, 4);
        assert_eq!(paid.classes, ordinary.classes);
        assert_eq!(paid.initial_evaluations, 0);
        assert_eq!(paid_evaluations, 4);
    }

    struct FixtureProblem {
        classes: Vec<Vec<usize>>,
        candidates: Vec<Vec<u8>>,
        fixed_keys_by_class: Vec<Vec<u8>>,
        rejected: Vec<(usize, usize, u8)>,
        global_requirements: Option<Vec<Assignment>>,
        blamed_classes: Vec<usize>,
        blame_by_assignment: AssignmentBlame,
        nested_search: NestedSearch,
        nested_accounted_ticks: u128,
        global_error: bool,
        initial_evaluations: u64,
        prepare_calls: Rc<Cell<usize>>,
        local_calls: Rc<Cell<usize>>,
        global_calls: Rc<Cell<usize>>,
        global_visits: Option<AssignmentVisitLog>,
    }

    #[derive(Debug, Eq, PartialEq)]
    struct FixtureOutput(Assignment);

    impl FixtureProblem {
        fn new(classes: Vec<Vec<usize>>, candidates: Vec<Vec<u8>>) -> Self {
            let fixed_keys_by_class = vec![Vec::new(); classes.len()];
            Self {
                classes,
                candidates,
                fixed_keys_by_class,
                rejected: Vec::new(),
                global_requirements: None,
                blamed_classes: Vec::new(),
                blame_by_assignment: Vec::new(),
                nested_search: NestedSearch::None,
                nested_accounted_ticks: 0,
                global_error: false,
                initial_evaluations: 0,
                prepare_calls: Rc::new(Cell::new(0)),
                local_calls: Rc::new(Cell::new(0)),
                global_calls: Rc::new(Cell::new(0)),
                global_visits: None,
            }
        }
    }

    impl Problem for FixtureProblem {
        type Node = usize;
        type Candidate = u8;
        type Key = u8;
        type ClassState = ();
        type Output = FixtureOutput;

        fn classes(&self) -> &[Vec<Self::Node>] {
            &self.classes
        }

        fn fixed_keys<W: Work>(
            &self,
            class: usize,
            _variables: &[Self::Node],
            _work: &mut W,
        ) -> Result<Vec<Self::Key>> {
            self.fixed_keys_by_class
                .get(class)
                .cloned()
                .ok_or_else(|| GeometryErrorKind::Invalid("fixture class is unknown".into()).into())
        }

        fn compare_nodes(&self, left: Self::Node, right: Self::Node) -> Result<Ordering> {
            Ok(left.cmp(&right))
        }

        fn fill_candidates<W: Work>(
            &self,
            node: Self::Node,
            candidates: &mut Vec<Self::Candidate>,
            work: &mut W,
        ) -> Result<()> {
            let options = self.candidates.get(node).ok_or_else(|| {
                GeometryErrorKind::Invalid("fixture node has no candidate row".into())
            })?;
            work.charge_product(options.len(), 1)?;
            candidates.reserve(options.len());
            candidates.extend(options.iter().copied());
            Ok(())
        }

        fn compare_candidates(
            &self,
            _node: Self::Node,
            left: Self::Candidate,
            right: Self::Candidate,
        ) -> Result<Ordering> {
            Ok(left.cmp(&right))
        }

        fn candidate_key(&self, candidate: Self::Candidate) -> Self::Key {
            candidate
        }

        fn prepare_class<A: SearchAccount>(
            &self,
            _class: usize,
            _variables: &[Self::Node],
            _account: &mut A,
        ) -> Result<Self::ClassState> {
            self.prepare_calls
                .set(self.prepare_calls.get().wrapping_add(1));
            Ok(())
        }

        fn check_local<A: SearchAccount>(
            &self,
            class_state: &Self::ClassState,
            class: usize,
            _variables: &[Self::Node],
            rank: usize,
            assigned: &[Option<Self::Candidate>],
            account: &mut A,
        ) -> Result<bool> {
            let () = class_state;
            self.local_calls.set(self.local_calls.get().wrapping_add(1));
            account.charge_evaluations(1)?;
            let candidate = assigned
                .get(rank)
                .copied()
                .flatten()
                .expect("search checks only a completed candidate prefix");
            Ok(!self.rejected.contains(&(class, rank, candidate)))
        }

        fn check_global<A: SearchAccount>(
            &mut self,
            assignments: &[ClassAssignment<'_, Self::Node, Self::Candidate>],
            account: &mut A,
        ) -> Result<GlobalCheck<Self::Output>> {
            debug_assert!(assignments.iter().all(|assignment| {
                self.classes.iter().any(|variables| {
                    variables.len() == assignment.variables.len()
                        && assignment
                            .variables
                            .iter()
                            .all(|variable| variables.contains(variable))
                })
            }));
            account.charge_evaluations(1)?;
            self.global_calls
                .set(self.global_calls.get().wrapping_add(1));
            let actual_count = assignments.iter().try_fold(0_usize, |count, assignment| {
                count
                    .checked_add(assignment.variables.len().min(assignment.values.len()))
                    .ok_or(ResourceError::SizeOverflow)
            })?;
            account.charge_product(actual_count, 1)?;
            let mut actual = Vec::with_capacity(actual_count);
            for assignment in assignments {
                for (&node, &value) in assignment.variables.iter().zip(assignment.values) {
                    account.charge(1)?;
                    actual.push((node, value));
                }
            }
            actual.sort_unstable_by_key(|&(node, _)| node);
            if let Some(visits) = &self.global_visits {
                visits.borrow_mut().push(actual.clone());
            }
            if self.global_error {
                return Err(GeometryErrorKind::Invalid("fixture global error".into()).into());
            }
            let run_nested = match self.nested_search {
                NestedSearch::None => false,
                NestedSearch::HallOn(candidate) => {
                    actual.iter().any(|&(_, value)| value == candidate)
                },
                NestedSearch::HallAlways => true,
            };
            if run_nested {
                let before = account.budget_mut().spent_ticks;
                let mut nested = Self::new(vec![vec![0, 1]], vec![vec![7], vec![7]]);
                let outcome = {
                    let native_budget = account.budget_mut();
                    run_with_budget(&mut nested, native_budget)?
                };
                let after = account.budget_mut().spent_ticks;
                self.nested_accounted_ticks = after
                    .checked_sub(before)
                    .expect("the shared search account only accumulates work");
                if matches!(outcome, SearchOutcome::Exhausted) {
                    return Ok(GlobalCheck::Reject(vec![0]));
                }
            }
            let Some(requirements) = &self.global_requirements else {
                return Ok(GlobalCheck::Pass(FixtureOutput(actual)));
            };
            Ok(
                if requirements.iter().any(|required| {
                    required
                        .iter()
                        .all(|assignment| actual.contains(assignment))
                }) {
                    GlobalCheck::Pass(FixtureOutput(actual))
                } else {
                    let blamed = self
                        .blame_by_assignment
                        .iter()
                        .find_map(|(assignment, classes)| {
                            (assignment == &actual).then_some(classes)
                        })
                        .unwrap_or(&self.blamed_classes);
                    GlobalCheck::Reject(blamed.clone())
                },
            )
        }

        fn initial_evaluations(&self) -> u64 {
            self.initial_evaluations
        }
    }

    fn search_fixture(
        problem: FixtureProblem,
        constructed: usize,
    ) -> Result<SearchAssignment<usize, u8, FixtureOutput>> {
        Search::new(problem, "search_test", constructed)?.run()
    }

    #[test]
    fn search_sorts_variables_and_candidates_before_returning_the_first_solution() {
        let problem = FixtureProblem::new(vec![vec![3, 1]], vec![
            Vec::new(),
            vec![3, 2],
            Vec::new(),
            vec![8, 6],
        ]);
        let assignment = search_fixture(problem, 2).unwrap();
        let class = assignment.classes.first().unwrap();
        assert_eq!(class.variables, [1, 3]);
        assert_eq!(class.values, [2, 6]);
    }

    #[test]
    fn matching_rearranges_an_earlier_cell_to_find_a_perfect_assignment() {
        let problem = FixtureProblem::new(vec![vec![0, 1]], vec![vec![0, 1], vec![0]]);
        let assignment = search_fixture(problem, 2).unwrap();
        let class = assignment.classes.first().unwrap();
        assert_eq!(class.variables, [0, 1]);
        assert_eq!(class.values, [1, 0]);
    }

    #[test]
    fn matching_parent_rewrite_refusal_preserves_the_shared_raw_balance() {
        let class = PreparedOptions {
            class: 0,
            variables: vec![0, 1],
            options: vec![vec![(0_u8, 0_u8), (1, 1)], vec![(0, 0)]],
        };
        let seen_key_capacity = try_hash_set_with_capacity::<u8>(3).unwrap().capacity();
        // The two reservations cost 12 ticks; each root's admitted prefix
        // costs 8 plus the actual seen-key capacity. The remaining tick is
        // insufficient for the second root's two-tick parent/seen rewrite.
        let prefix_ticks = 28 + 2 * u128::try_from(seen_key_capacity).unwrap();
        let available_ticks = prefix_ticks + 1;
        let limit = u64::try_from(available_ticks.div_ceil(WORK_TICKS_PER_EVALUATION)).unwrap();
        let mut budget = EvaluationBudget::new("search_test", 2).unwrap();
        budget.limit = limit;
        let limit_ticks = budget.limit_ticks();
        budget.spent_ticks = limit_ticks - available_ticks;

        let error = has_perfect_matching(&class, &mut budget).unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "search_test",
                constructed: 2,
                evaluations
            }) if *evaluations == limit
        ));
        assert_eq!(budget.spent_ticks, limit_ticks - 1);
    }

    #[test]
    fn fixed_nearest_cells_are_removed_before_search() {
        let mut problem = FixtureProblem::new(vec![vec![0]], vec![vec![0, 1]]);
        problem.fixed_keys_by_class[0].push(0);
        let assignment = search_fixture(problem, 1).unwrap();
        assert_eq!(assignment.classes.first().unwrap().values, [1]);
    }

    #[test]
    fn each_class_reserves_nearest_cells_of_other_classes() {
        let mut problem = FixtureProblem::new(vec![vec![0], vec![1]], vec![vec![0, 1], vec![0, 1]]);
        problem.fixed_keys_by_class[0].push(0);
        problem.fixed_keys_by_class[1].push(1);
        let assignment = search_fixture(problem, 2).unwrap();

        let first = assignment.classes.first().unwrap();
        let second = &assignment.classes[1];
        assert_eq!(
            (
                *first.variables.first().unwrap(),
                *first.values.first().unwrap()
            ),
            (0, 1)
        );
        assert_eq!(
            (
                *second.variables.first().unwrap(),
                *second.values.first().unwrap()
            ),
            (1, 0)
        );
    }

    #[test]
    fn hall_failure_is_unrepresentable_before_depth_first_search() {
        let problem = FixtureProblem::new(vec![vec![0, 1, 2]], vec![vec![0, 1], vec![0, 1], vec![
            0, 1,
        ]]);
        let local_calls = Rc::clone(&problem.local_calls);
        let error = search_fixture(problem, 3).unwrap_err();

        assert_eq!(local_calls.get(), 0);
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingUnrepresentable {
                operation: "search_test",
                constructed: 3
            })
        ));
    }

    #[test]
    fn hall_failure_skips_class_preparation() {
        let problem = FixtureProblem::new(vec![vec![0, 1]], vec![vec![7], vec![7]]);
        let prepare_calls = Rc::clone(&problem.prepare_calls);
        let error = search_fixture(problem, 2).unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingUnrepresentable {
                operation: "search_test",
                constructed: 2
            })
        ));
        assert_eq!(prepare_calls.get(), 0);
    }

    #[test]
    fn local_failure_backtracks_to_the_next_lexicographic_candidate() {
        let mut problem = FixtureProblem::new(vec![vec![1, 0]], vec![vec![1, 0], vec![1, 0]]);
        problem.rejected.push((0, 0, 0));
        let assignment = search_fixture(problem, 2).unwrap();
        let class = assignment.classes.first().unwrap();
        assert_eq!(class.variables, [0, 1]);
        assert_eq!(class.values, [1, 0]);
    }

    #[test]
    fn global_rejections_visit_products_in_deterministic_variable_order() {
        let mut problem = FixtureProblem::new(
            vec![vec![10], vec![0]],
            std::iter::repeat_n(Vec::new(), 11).collect(),
        );
        problem.candidates[0] = vec![1, 0];
        problem.candidates[10] = vec![3, 2];
        problem.global_requirements = Some(vec![vec![(0, 1), (10, 3)]]);
        problem.blamed_classes = vec![0, 1];
        let global_calls = Rc::clone(&problem.global_calls);
        let assignment = search_fixture(problem, 2).unwrap();

        assert_eq!(global_calls.get(), 4);
        let first = assignment.classes.first().unwrap();
        assert_eq!(first.variables, [0]);
        assert_eq!(first.values, [1]);
        let second = &assignment.classes[1];
        assert_eq!(second.variables, [10]);
        assert_eq!(second.values, [3]);
    }

    #[test]
    fn empty_blame_is_unrepresentable_when_no_prepared_assignment_can_repair() {
        let mut problem = FixtureProblem::new(vec![vec![0]], vec![vec![0]]);
        problem.global_requirements = Some(vec![vec![(0, 1)]]);
        let error = search_fixture(problem, 1).unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingUnrepresentable {
                operation: "search_test",
                constructed: 1
            })
        ));
    }

    #[test]
    fn exhausting_every_blamed_class_is_unrepresentable() {
        let mut problem = FixtureProblem::new(vec![vec![0]], vec![vec![0]]);
        problem.global_requirements = Some(vec![vec![(0, 1)]]);
        problem.blamed_classes = vec![0];
        let error = search_fixture(problem, 1).unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingUnrepresentable {
                operation: "search_test",
                constructed: 1
            })
        ));
    }

    #[test]
    fn global_rejection_blame_cannot_skip_a_prepared_product() {
        let mut problem = FixtureProblem::new(vec![vec![0], vec![1]], vec![vec![0, 1], vec![2, 3]]);
        problem.global_requirements = Some(vec![vec![(0, 0), (1, 3)]]);
        problem.blame_by_assignment = vec![
            (vec![(0, 0), (1, 2)], vec![0, 1]),
            (vec![(0, 1), (1, 2)], vec![1]),
            (vec![(0, 1), (1, 3)], vec![0]),
        ];
        let visits = Rc::new(RefCell::new(Vec::new()));
        problem.global_visits = Some(Rc::clone(&visits));
        let assignment = search_fixture(problem, 2);

        assert!(
            assignment.is_ok(),
            "search skipped the passing prepared product (class 0 -> 0, class 1 -> 3): {assignment:?}"
        );
        let assignment = assignment.unwrap();
        assert_eq!(assignment.classes[0].values, [0]);
        assert_eq!(assignment.classes[1].values, [3]);
        assert_eq!(*visits.borrow(), [
            vec![(0, 0), (1, 2)],
            vec![(0, 1), (1, 2)],
            vec![(0, 0), (1, 3)],
        ]);
        assert_eq!(assignment.output.0, [(0, 0), (1, 3)]);
    }

    #[test]
    fn borrowed_and_owned_search_preserve_first_blame_and_move_the_global_output() {
        let mut borrowed_problem =
            FixtureProblem::new(vec![vec![0], vec![1]], vec![vec![0, 1], vec![2, 3]]);
        borrowed_problem.global_requirements = Some(vec![vec![(0, 0), (1, 3)]]);
        borrowed_problem.blamed_classes = vec![0, 1];
        borrowed_problem.blame_by_assignment = vec![
            (vec![(0, 0), (1, 2)], vec![0, 1]),
            (vec![(0, 1), (1, 2)], vec![1]),
            (vec![(0, 1), (1, 3)], vec![0]),
        ];
        let borrowed_visits = Rc::new(RefCell::new(Vec::new()));
        borrowed_problem.global_visits = Some(Rc::clone(&borrowed_visits));
        let mut budget = EvaluationBudget::new("search_test", 2).unwrap();
        let SearchOutcome::Pass(borrowed) =
            run_with_budget(&mut borrowed_problem, &mut budget).unwrap()
        else {
            panic!("the final prepared product satisfies the global certificate");
        };

        let mut owned_problem =
            FixtureProblem::new(vec![vec![0], vec![1]], vec![vec![0, 1], vec![2, 3]]);
        owned_problem.global_requirements = Some(vec![vec![(0, 0), (1, 3)]]);
        owned_problem.blamed_classes = vec![0, 1];
        owned_problem.blame_by_assignment = vec![
            (vec![(0, 0), (1, 2)], vec![0, 1]),
            (vec![(0, 1), (1, 2)], vec![1]),
            (vec![(0, 1), (1, 3)], vec![0]),
        ];
        let owned_visits = Rc::new(RefCell::new(Vec::new()));
        owned_problem.global_visits = Some(Rc::clone(&owned_visits));
        let owned = Search::new(owned_problem, "search_test", 2)
            .unwrap()
            .run()
            .unwrap();

        let expected_visits = [vec![(0, 0), (1, 2)], vec![(0, 1), (1, 2)], vec![
            (0, 0),
            (1, 3),
        ]];
        assert_eq!(*borrowed_visits.borrow(), expected_visits);
        assert_eq!(*owned_visits.borrow(), expected_visits);
        assert_eq!(borrowed.output.0, [(0, 0), (1, 3)]);
        assert_eq!(owned.output.0, borrowed.output.0);
        assert_eq!(owned.evaluations, 17);
        assert!(
            budget.spent_ticks > u128::from(owned.evaluations) * WORK_TICKS_PER_EVALUATION,
            "the borrowed account also pays raw engine work"
        );
    }

    #[test]
    fn borrowed_search_does_not_import_the_problem_legacy_evaluation_tally() {
        let mut problem = FixtureProblem::new(vec![vec![0]], vec![vec![7]]);
        problem.initial_evaluations = BASE_EVALUATION_LIMIT + 7;
        let mut budget = EvaluationBudget::new("search_test", 1).unwrap();

        let SearchOutcome::Pass(assignment) = run_with_budget(&mut problem, &mut budget).unwrap()
        else {
            panic!("the fixture has one finite candidate assignment");
        };

        assert_eq!(assignment.output.0, [(0, 7)]);
        assert!(budget.spent_ticks < budget.limit_ticks());
    }

    #[test]
    fn borrowed_search_checks_a_zero_class_problem_and_moves_its_payload() {
        let mut problem = FixtureProblem::new(Vec::new(), Vec::new());
        let global_calls = Rc::clone(&problem.global_calls);
        let mut budget = EvaluationBudget::new("search_test", 0).unwrap();

        let (outcome, allocations) = crate::test_support::allocation_count(|| {
            run_with_budget(&mut problem, &mut budget).unwrap()
        });
        let SearchOutcome::Pass(assignment) = outcome else {
            panic!("an empty assignment passes the fixture's global certificate");
        };

        assert!(assignment.classes.is_empty());
        assert!(assignment.output.0.is_empty());
        assert_eq!(global_calls.get(), 1);
        assert_eq!(
            allocations, 0,
            "empty classes do not allocate candidate scratch"
        );
    }

    #[test]
    fn borrowed_zero_class_empty_blame_is_finite_exhaustion() {
        let mut problem = FixtureProblem::new(Vec::new(), Vec::new());
        problem.global_requirements = Some(Vec::new());
        let global_calls = Rc::clone(&problem.global_calls);
        let mut budget = EvaluationBudget::new("search_test", 0).unwrap();

        let outcome = run_with_budget(&mut problem, &mut budget).unwrap();

        assert!(matches!(outcome, SearchOutcome::Exhausted));
        assert_eq!(global_calls.get(), 1);
    }

    #[test]
    fn borrowed_zero_class_global_error_is_propagated() {
        let mut problem = FixtureProblem::new(Vec::new(), Vec::new());
        problem.global_error = true;
        let global_calls = Rc::clone(&problem.global_calls);
        let mut budget = EvaluationBudget::new("search_test", 0).unwrap();

        let error = run_with_budget(&mut problem, &mut budget).unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        ));
        assert_eq!(global_calls.get(), 1);
    }

    #[test]
    fn borrowed_zero_class_nonempty_blame_is_invalid() {
        let mut problem = FixtureProblem::new(Vec::new(), Vec::new());
        problem.global_requirements = Some(Vec::new());
        problem.blamed_classes = vec![0];
        let mut budget = EvaluationBudget::new("search_test", 0).unwrap();

        let error = run_with_budget(&mut problem, &mut budget).unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        ));
    }

    #[test]
    fn global_output_reservation_refusal_precedes_materialization() {
        let variables = [0_usize];
        let values = [7_u8];
        let assignments = [ClassAssignment {
            variables: &variables,
            values: &values,
        }];
        let mut problem = FixtureProblem::new(vec![vec![0]], vec![vec![7]]);
        let global_calls = Rc::clone(&problem.global_calls);
        let mut budget = EvaluationBudget::new("search_test", 1).unwrap();
        budget.limit = 1;

        let Err(error) = Problem::check_global(&mut problem, &assignments, &mut budget) else {
            panic!("the output buffer debit must refuse before materialization");
        };

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "search_test",
                constructed: 1,
                evaluations: 1
            })
        ));
        assert_eq!(global_calls.get(), 1);
        assert_eq!(budget.spent_ticks, 16);
    }

    #[test]
    fn evaluation_only_outer_pays_raw_work_only_in_the_native_inner_search() {
        let mut ordinary_problem = FixtureProblem::new(vec![vec![0]], vec![vec![0, 1]]);
        ordinary_problem.nested_search = NestedSearch::HallOn(0);
        let ordinary_visits = Rc::new(RefCell::new(Vec::new()));
        ordinary_problem.global_visits = Some(Rc::clone(&ordinary_visits));
        let mut ordinary_budget = EvaluationBudget::new("search_test", 1).unwrap();
        let ordinary_outcome = {
            let mut outer = EvaluationOnly::new(&mut ordinary_budget);
            run_with_budget(&mut ordinary_problem, &mut outer).unwrap()
        };
        let SearchOutcome::Pass(ordinary_assignment) = ordinary_outcome else {
            panic!("the second outer candidate passes after the nested Hall rejection");
        };
        let ordinary_inner_ticks = ordinary_problem.nested_accounted_ticks;

        let mut native_problem = FixtureProblem::new(vec![vec![0]], vec![vec![0, 1]]);
        native_problem.nested_search = NestedSearch::HallOn(0);
        let native_visits = Rc::new(RefCell::new(Vec::new()));
        native_problem.global_visits = Some(Rc::clone(&native_visits));
        let mut native_budget = EvaluationBudget::new("search_test", 1).unwrap();
        let SearchOutcome::Pass(native_assignment) =
            run_with_budget(&mut native_problem, &mut native_budget).unwrap()
        else {
            panic!("the native outer account also reaches the passing candidate");
        };
        let native_inner_ticks = native_problem.nested_accounted_ticks;

        assert_eq!(*ordinary_visits.borrow(), [vec![(0, 0)], vec![(0, 1)]]);
        assert_eq!(*native_visits.borrow(), *ordinary_visits.borrow());
        assert_eq!(ordinary_assignment.classes[0].values, [1]);
        assert_eq!(native_assignment.classes[0].values, [1]);
        // The Hall fixture admits two candidate keys before matching rejects it.
        let inner_raw_ticks = ordinary_inner_ticks.checked_sub(32).unwrap();
        assert!(inner_raw_ticks > 0);
        assert_eq!(native_inner_ticks, ordinary_inner_ticks);
        assert!(native_budget.spent_ticks > ordinary_budget.spent_ticks);
    }

    #[test]
    fn nested_limit_keeps_the_enclosing_operation_and_constructed_count() {
        let mut problem = FixtureProblem::new(Vec::new(), Vec::new());
        problem.nested_search = NestedSearch::HallAlways;
        let mut budget = EvaluationBudget::new("outer_operation", 42).unwrap();
        budget.limit = 1;

        let error = run_with_budget(&mut problem, &mut budget).unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "outer_operation",
                constructed: 42,
                evaluations: 1
            })
        ));
        assert_eq!(budget.spent_ticks, 16);
    }

    #[test]
    fn evaluation_only_outer_native_inner_limit_keeps_the_outer_budget() {
        let mut problem = FixtureProblem::new(vec![vec![0]], vec![vec![7]]);
        problem.nested_search = NestedSearch::HallAlways;
        let global_calls = Rc::clone(&problem.global_calls);
        let mut budget = EvaluationBudget::new("mixed_outer", 42).unwrap();
        budget.limit = 4;

        let error = {
            let mut outer = EvaluationOnly::new(&mut budget);
            run_with_budget(&mut problem, &mut outer).unwrap_err()
        };

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "mixed_outer",
                constructed: 42,
                evaluations: 4
            })
        ));
        assert_eq!(global_calls.get(), 1);
        assert_eq!(budget.spent_ticks, 64);
    }

    #[test]
    fn candidate_cursor_refusal_precedes_dfs_state_mutation() {
        let problem = FixtureProblem::new(vec![vec![0]], vec![vec![7]]);
        let class = PreparedClass {
            class: 0,
            variables: vec![0],
            options: vec![vec![(7, 7)]],
            local: (),
        };
        let mut state = ClassSearchState::new(&class, &mut Unmetered).unwrap();
        let mut account = EvaluationBudget::new("search_test", 1).unwrap();
        account.limit = 1;

        let error = state
            .next_solution(&class, &problem, &mut account)
            .unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "search_test",
                constructed: 1,
                evaluations: 1
            })
        ));
        assert_eq!(account.spent_ticks, 16);
        assert_eq!(state.next_candidate, [0]);
        assert_eq!(state.selected, [None]);
        assert!(state.used_keys.is_empty());
        assert_eq!(problem.local_calls.get(), 0);
    }

    #[test]
    fn cell_assignment_refusal_precedes_key_and_value_mutation() {
        let problem = FixtureProblem::new(vec![vec![0]], vec![vec![7]]);
        let class = PreparedClass {
            class: 0,
            variables: vec![0],
            options: vec![vec![(7, 7)]],
            local: (),
        };
        let mut state = ClassSearchState::new(&class, &mut Unmetered).unwrap();
        let mut account = EvaluationBudget::new("search_test", 1).unwrap();
        account.limit = 2;
        account.spent_ticks = 14;

        let error = state
            .next_solution(&class, &problem, &mut account)
            .unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "search_test",
                constructed: 1,
                evaluations: 2
            })
        ));
        assert_eq!(account.spent_ticks, 31);
        assert_eq!(state.next_candidate, [1]);
        assert_eq!(state.selected, [None]);
        assert_eq!(state.selected_keys, [None]);
        assert!(state.used_keys.is_empty());
        assert_eq!(problem.local_calls.get(), 0);
    }

    #[test]
    fn option_exhaustion_refusal_precedes_digit_reset() {
        let problem = FixtureProblem::new(vec![vec![0]], vec![vec![7]]);
        let class = PreparedClass {
            class: 0,
            variables: vec![0],
            options: vec![vec![(7, 7)]],
            local: (),
        };
        let mut state = ClassSearchState::new(&class, &mut Unmetered).unwrap();
        state.next_candidate[0] = 1;
        let mut account = EvaluationBudget::new("search_test", 1).unwrap();
        account.limit = 1;
        account.spent_ticks = 16;

        let error = state
            .next_solution(&class, &problem, &mut account)
            .unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "search_test",
                constructed: 1,
                evaluations: 1
            })
        ));
        assert_eq!(account.spent_ticks, 16);
        assert_eq!(state.next_candidate, [1]);
        assert!(!state.exhausted);
        assert_eq!(problem.local_calls.get(), 0);
    }

    #[test]
    fn reset_refusal_leaves_the_current_local_solution_intact() {
        let class = PreparedClass {
            class: 0,
            variables: vec![0],
            options: vec![vec![(7, 7)]],
            local: (),
        };
        let mut state = ClassSearchState::new(&class, &mut Unmetered).unwrap();
        state.next_candidate[0] = 1;
        state.selected[0] = Some(7);
        state.selected_keys[0] = Some(7);
        state.used_keys.insert(7);
        state.yielded = true;
        let mut account = EvaluationBudget::new("search_test", 1).unwrap();
        account.limit = 1;
        account.spent_ticks = 14;

        let error = state.reset(&mut account).unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "search_test",
                constructed: 1,
                evaluations: 1
            })
        ));
        assert_eq!(account.spent_ticks, 14);
        assert_eq!(state.next_candidate, [1]);
        assert_eq!(state.selected, [Some(7)]);
        assert_eq!(state.selected_keys, [Some(7)]);
        assert!(state.used_keys.contains(&7));
        assert!(state.yielded);
        assert!(!state.exhausted);
    }

    #[test]
    fn solution_copy_refusal_does_not_mark_the_stream_as_yielded() {
        let problem = FixtureProblem::new(vec![vec![0]], vec![vec![7]]);
        let class = PreparedClass {
            class: 0,
            variables: vec![0],
            options: vec![vec![(7, 7)]],
            local: (),
        };
        let mut state = ClassSearchState::new(&class, &mut Unmetered).unwrap();
        let mut account = EvaluationBudget::new("search_test", 1).unwrap();
        account.limit = 3;
        account.spent_ticks = 11;

        let error = state
            .next_solution(&class, &problem, &mut account)
            .unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "search_test",
                constructed: 1,
                evaluations: 3
            })
        ));
        assert_eq!(account.spent_ticks, 47);
        assert_eq!(state.next_candidate, [1]);
        assert_eq!(state.selected, [Some(7)]);
        assert_eq!(state.selected_keys, [Some(7)]);
        assert!(state.used_keys.contains(&7));
        assert!(!state.yielded);
        assert_eq!(problem.local_calls.get(), 1);
    }

    fn cartesian_products_with_first_stream_fastest(streams: &[Vec<u8>]) -> Vec<Vec<u8>> {
        fn visit(
            streams: &[Vec<u8>],
            stream: usize,
            reversed: &mut Vec<u8>,
            products: &mut Vec<Vec<u8>>,
        ) {
            let options = streams
                .get(stream)
                .expect("the product enumerator visits an existing stream");
            for &value in options {
                reversed.push(value);
                if stream == 0 {
                    products.push(reversed.iter().rev().copied().collect());
                } else {
                    visit(streams, stream.wrapping_sub(1), reversed, products);
                }
                reversed
                    .pop()
                    .expect("the recursive product walk just pushed one value");
            }
        }

        assert!(!streams.is_empty());
        let mut products = Vec::new();
        visit(
            streams,
            streams.len().wrapping_sub(1),
            &mut Vec::with_capacity(streams.len()),
            &mut products,
        );
        products
    }

    fn cartesian_products_with_ordered_fastest_stream(
        streams: &[Vec<u8>],
        order: &[usize],
    ) -> Vec<Vec<u8>> {
        let ordered_streams = order
            .iter()
            .map(|&node| {
                streams
                    .get(node)
                    .expect("the product order names a prepared stream")
                    .clone()
            })
            .collect::<Vec<_>>();
        cartesian_products_with_first_stream_fastest(&ordered_streams)
            .into_iter()
            .map(|ordered| {
                let mut product = Vec::with_capacity(streams.len());
                product.resize(streams.len(), None::<u8>);
                for (position, &node) in order.iter().enumerate() {
                    *product
                        .get_mut(node)
                        .expect("the product order names an output position") = Some(
                        *ordered
                            .get(position)
                            .expect("the product contains every prepared stream"),
                    );
                }
                product
                    .into_iter()
                    .map(|value| value.expect("the product order visits every stream"))
                    .collect()
            })
            .collect()
    }

    #[test]
    fn every_two_and_three_class_acceptance_table_follows_first_blame_order() {
        for (streams, class_orders) in [
            (vec![vec![0, 1], vec![2, 3]], vec![vec![0, 1], vec![1, 0]]),
            (vec![vec![0, 1], vec![2, 3], vec![4, 5]], vec![
                vec![0, 1, 2],
                vec![0, 2, 1],
                vec![1, 0, 2],
                vec![1, 2, 0],
                vec![2, 0, 1],
                vec![2, 1, 0],
            ]),
        ] {
            let first_blame = 1;
            let mut digit_order = Vec::with_capacity(streams.len());
            digit_order.push(first_blame);
            digit_order.extend((0..streams.len()).filter(|&node| node != first_blame));
            let products = cartesian_products_with_ordered_fastest_stream(&streams, &digit_order);
            let acceptance_count = 1_usize
                .checked_shl(u32::try_from(products.len()).unwrap())
                .unwrap();
            let candidates = (0..streams.len())
                .map(|node| {
                    let first = u8::try_from(node).unwrap().wrapping_mul(2);
                    vec![first, first.wrapping_add(1)]
                })
                .collect::<Vec<_>>();

            for class_order in class_orders {
                for accepted in 0..acceptance_count {
                    let global_requirements = products
                        .iter()
                        .enumerate()
                        .filter_map(|(index, product)| {
                            let bit = 1_usize.checked_shl(u32::try_from(index).unwrap()).unwrap();
                            (accepted & bit != 0)
                                .then(|| product.iter().copied().enumerate().collect::<Vec<_>>())
                        })
                        .collect();
                    let class_for_node = |node| {
                        class_order
                            .iter()
                            .position(|&class_node| class_node == node)
                            .expect("each singleton node has one original class ID")
                    };
                    let blame_by_assignment = products
                        .iter()
                        .enumerate()
                        .map(|(index, product)| {
                            let assignment = product.iter().copied().enumerate().collect();
                            let blamed_node = match index {
                                1 => 0,
                                2 => streams.len().wrapping_sub(1),
                                _ => first_blame,
                            };
                            (assignment, vec![class_for_node(blamed_node)])
                        })
                        .collect();
                    let classes = class_order.iter().copied().map(|node| vec![node]).collect();
                    let mut problem = FixtureProblem::new(classes, candidates.clone());
                    problem.global_requirements = Some(global_requirements);
                    problem.blamed_classes = vec![class_for_node(first_blame)];
                    problem.blame_by_assignment = blame_by_assignment;
                    let visits = Rc::new(RefCell::new(Vec::new()));
                    problem.global_visits = Some(Rc::clone(&visits));
                    let result = search_fixture(problem, streams.len());

                    let first = products.iter().enumerate().find(|(index, _)| {
                        let bit = 1_usize.checked_shl(u32::try_from(*index).unwrap()).unwrap();
                        accepted & bit != 0
                    });
                    let expected_visit_count =
                        first.map_or(products.len(), |(index, _)| index.wrapping_add(1));
                    let expected_visits = products
                        .iter()
                        .take(expected_visit_count)
                        .map(|product| product.iter().copied().enumerate().collect::<Vec<_>>())
                        .collect::<Vec<_>>();
                    assert_eq!(
                        *visits.borrow(),
                        expected_visits,
                        "acceptance mask {accepted:#x}, class order {class_order:?}"
                    );

                    if let Some((_, expected)) = first {
                        assert!(
                            result.is_ok(),
                            "acceptance mask {accepted:#x}, class order {class_order:?} must return the first passing product: {result:?}"
                        );
                        let assignment = result.unwrap();
                        let actual = assignment
                            .classes
                            .iter()
                            .flat_map(|class| {
                                class
                                    .variables
                                    .iter()
                                    .copied()
                                    .zip(class.values.iter().copied())
                            })
                            .collect::<Vec<_>>();
                        assert_eq!(
                            actual,
                            expected.iter().copied().enumerate().collect::<Vec<_>>()
                        );
                    } else {
                        let error = result.unwrap_err();
                        assert!(matches!(
                            error.kind(),
                            ErrorKind::Geometry(
                                GeometryErrorKind::ExactEmbeddingUnrepresentable { .. }
                            )
                        ));
                    }
                }
            }
        }
    }

    #[test]
    fn product_carry_restarts_unequal_local_streams_and_keeps_fixed_classes() {
        let mut problem = FixtureProblem::new(vec![vec![0, 1], vec![2], vec![3]], vec![
            vec![0, 1, 2],
            vec![0, 1, 2],
            vec![3, 4],
            vec![5],
        ]);
        problem.rejected.push((0, 0, 0));
        problem.global_requirements = Some(vec![vec![(0, 2), (1, 1), (2, 4), (3, 5)]]);
        problem.blamed_classes = vec![0, 1, 2];
        let visits = Rc::new(RefCell::new(Vec::new()));
        problem.global_visits = Some(Rc::clone(&visits));
        let assignment = search_fixture(problem, 3).unwrap();

        let visits_expected = [
            [1, 0, 3, 5],
            [1, 2, 3, 5],
            [2, 0, 3, 5],
            [2, 1, 3, 5],
            [1, 0, 4, 5],
            [1, 2, 4, 5],
            [2, 0, 4, 5],
            [2, 1, 4, 5],
        ]
        .map(|values| values.into_iter().enumerate().collect::<Vec<_>>());
        assert_eq!(*visits.borrow(), visits_expected);
        assert_eq!(
            assignment
                .classes
                .iter()
                .flat_map(|class| {
                    class
                        .variables
                        .iter()
                        .copied()
                        .zip(class.values.iter().copied())
                })
                .collect::<Vec<_>>(),
            [(0, 2), (1, 1), (2, 4), (3, 5)]
        );
    }

    #[test]
    fn finite_product_exhaustion_and_search_budget_remain_distinct() {
        let mut no_passing_product =
            FixtureProblem::new(vec![vec![0], vec![1]], vec![vec![0, 1], vec![2, 3]]);
        no_passing_product.global_requirements = Some(Vec::new());
        no_passing_product.blamed_classes = vec![0, 1];
        let error = search_fixture(no_passing_product, 2).unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingUnrepresentable {
                operation: "search_test",
                constructed: 2
            })
        ));

        let mut budgeted = FixtureProblem::new(vec![vec![0], vec![1]], vec![vec![0, 1], vec![2]]);
        budgeted.global_requirements = Some(vec![vec![(0, 1), (1, 2)]]);
        budgeted.blamed_classes = vec![0, 1];
        let error = Search::with_evaluation_limit(budgeted, "search_test", 2, 9)
            .unwrap()
            .run()
            .unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "search_test",
                constructed: 2,
                evaluations: 9
            })
        ));
    }

    #[test]
    fn a_single_last_class_repair_fits_the_default_budget() {
        const CLASS_COUNT: usize = 20;

        let classes = (0..CLASS_COUNT).map(|node| vec![node]).collect();
        let candidates = (0..CLASS_COUNT)
            .map(|node| {
                let first = u8::try_from(node).unwrap().wrapping_mul(2);
                vec![first, first.wrapping_add(1)]
            })
            .collect();
        let target = (0..CLASS_COUNT)
            .map(|node| {
                let first = u8::try_from(node).unwrap().wrapping_mul(2);
                (
                    node,
                    if node == CLASS_COUNT.wrapping_sub(1) {
                        first.wrapping_add(1)
                    } else {
                        first
                    },
                )
            })
            .collect::<Vec<_>>();
        let mut problem = FixtureProblem::new(classes, candidates);
        problem.global_requirements = Some(vec![vec![(19, 39)]]);
        problem.blamed_classes = vec![CLASS_COUNT.wrapping_sub(1)];
        let global_calls = Rc::clone(&problem.global_calls);
        let assignment = search_fixture(problem, CLASS_COUNT);

        assert!(
            assignment.is_ok(),
            "the single candidate in the only blamed class must be tried within the default budget; completed global checks: {}; result: {assignment:?}",
            global_calls.get()
        );
        let assignment = assignment.unwrap();
        let actual = assignment
            .classes
            .iter()
            .flat_map(|class| {
                class
                    .variables
                    .iter()
                    .copied()
                    .zip(class.values.iter().copied())
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, target);
        assert_eq!(
            global_calls.get(),
            2,
            "the nearest product and its one-class repair each receive one global check"
        );
    }

    #[test]
    fn unknown_blame_is_invalid_before_or_after_product_order_initialization() {
        let mut problem = FixtureProblem::new(vec![vec![0]], vec![vec![0, 1]]);
        problem.global_requirements = Some(Vec::new());
        problem.blamed_classes = vec![1];
        let error = search_fixture(problem, 1).unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        ));

        let mut problem = FixtureProblem::new(vec![vec![0], vec![1]], vec![vec![0, 1], vec![2, 3]]);
        problem.global_requirements = Some(vec![vec![(0, 1), (1, 3)]]);
        problem.blamed_classes = vec![0];
        problem.blame_by_assignment = vec![(vec![(0, 1), (1, 2)], vec![2])];
        let visits = Rc::new(RefCell::new(Vec::new()));
        problem.global_visits = Some(Rc::clone(&visits));
        let error = search_fixture(problem, 2).unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        ));
        assert_eq!(
            *visits.borrow(),
            [vec![(0, 0), (1, 2)], vec![(0, 1), (1, 2)]],
            "the second rejection's unknown class is checked after traversal order is fixed"
        );
    }

    #[test]
    fn empty_variable_class_is_rejected_during_preparation() {
        let problem = FixtureProblem::new(vec![vec![0], Vec::new()], vec![vec![0]]);
        let error = search_fixture(problem, 1).unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        ));
    }

    #[test]
    fn one_budget_covers_candidate_admission_and_all_classes() {
        let mut problem = FixtureProblem::new(vec![vec![0], vec![1]], vec![vec![7], vec![8]]);
        problem.initial_evaluations = BASE_EVALUATION_LIMIT - 2;
        let error = search_fixture(problem, 1).unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "search_test",
                constructed: 1,
                evaluations
            }) if *evaluations == BASE_EVALUATION_LIMIT
        ));
    }

    #[test]
    fn tiny_budget_is_a_search_limit_and_default_budget_accepts_the_same_problem() {
        let error = Search::with_evaluation_limit(
            FixtureProblem::new(vec![vec![0]], vec![vec![7]]),
            "search_test",
            1,
            0,
        )
        .unwrap()
        .run()
        .unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "search_test",
                constructed: 1,
                evaluations: 0
            })
        ));

        let assignment =
            search_fixture(FixtureProblem::new(vec![vec![0]], vec![vec![7]]), 1).unwrap();
        assert_eq!(assignment.classes.first().unwrap().values, [7]);
    }

    #[test]
    fn over_budget_closure_reports_its_completed_work() {
        let mut problem = FixtureProblem::new(vec![vec![0]], vec![vec![7]]);
        problem.initial_evaluations = BASE_EVALUATION_LIMIT + 7;
        let error = search_fixture(problem, 1).unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "search_test",
                constructed: 1,
                evaluations
            }) if *evaluations == BASE_EVALUATION_LIMIT + 7
        ));
    }

    #[test]
    fn budget_grows_with_the_number_of_constructed_nodes() {
        let mut budget = EvaluationBudget::new("search_test", 17).unwrap();
        let limit = 17 * EVALUATIONS_PER_CONSTRUCTED_NODE;
        budget.charge_evaluations(limit).unwrap();
        let error = budget.charge_evaluations(1).unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "search_test",
                constructed: 17,
                evaluations
            }) if *evaluations == limit
        ));
    }

    #[test]
    fn one_evaluation_and_sixteen_raw_ticks_share_the_same_balance() {
        let mut raw = EvaluationBudget::new("search_test", 1).unwrap();
        for _ in 0..16 {
            <EvaluationBudget as Work>::charge(&mut raw, 1).unwrap();
        }

        let mut evaluation = EvaluationBudget::new("search_test", 1).unwrap();
        evaluation.charge_evaluations(1).unwrap();
        assert_eq!(raw.spent_ticks, evaluation.spent_ticks);
        assert_eq!(raw.spent_ticks, 16);

        let mut raw_batch = EvaluationBudget::new("search_test", 1).unwrap();
        <EvaluationBudget as Work>::charge(&mut raw_batch, 256).unwrap();
        let mut evaluation_batch = EvaluationBudget::new("search_test", 1).unwrap();
        for _ in 0..16 {
            evaluation_batch.charge_evaluations(1).unwrap();
        }
        assert_eq!(raw_batch.spent_ticks, evaluation_batch.spent_ticks);
        assert_eq!(raw_batch.spent_ticks, 256);
    }

    #[test]
    fn evaluation_only_account_ignores_raw_ticks_and_billing_products() {
        let mut budget = EvaluationBudget::new("search_test", 1).unwrap();
        {
            let mut account = EvaluationOnly::new(&mut budget);
            account.charge(u64::MAX).unwrap();
            account.charge_product(usize::MAX, usize::MAX).unwrap();
            account.charge_evaluations(1).unwrap();
        }

        assert_eq!(budget.spent_ticks, 16);
        let mut unmetered = Unmetered;
        unmetered.charge_product(usize::MAX, usize::MAX).unwrap();
    }

    fn equal_primary_sweep_bounds() -> Vec<Bounds> {
        (0..40)
            .map(|ordinal| match ordinal {
                0 | 20 | 39 => Bounds::new_raw(100.0, 0.0, 101.0, 1.0),
                _ => {
                    let cross = f64::from(ordinal) + 10.0;
                    Bounds::new_raw(0.0, cross, 0.0, cross)
                },
            })
            .collect()
    }

    fn stable_sweep_pairs<W: Work>(bounds: &[Bounds], work: &mut W) -> Vec<(usize, usize)> {
        let mut pairs = Vec::new();
        let flow =
            sweep_for_each_overlapping_bounds_pair_with_work(bounds, work, |left, right, _work| {
                pairs.push((left, right));
                Ok(ControlFlow::<std::convert::Infallible>::Continue(()))
            })
            .expect("the test bounds fit their structural width");
        assert_eq!(flow, ControlFlow::Continue(()));
        pairs
    }

    fn stable_cross_sweep_pairs<W: Work>(
        left: &[Bounds],
        right: &[Bounds],
        work: &mut W,
    ) -> Vec<(usize, usize)> {
        let mut pairs = Vec::new();
        let flow = sweep_for_each_overlapping_bounds_cross_pair_with_work(
            left,
            right,
            work,
            |left, right, _work| {
                pairs.push((left, right));
                Ok(ControlFlow::<std::convert::Infallible>::Continue(()))
            },
        )
        .expect("the test bounds fit their structural width");
        assert_eq!(flow, ControlFlow::Continue(()));
        pairs
    }

    fn assert_stable_break<W: Work>(bounds: &[Bounds], work: &mut W) {
        let mut pairs = Vec::new();
        let flow =
            sweep_for_each_overlapping_bounds_pair_with_work(bounds, work, |left, right, _work| {
                pairs.push((left, right));
                Ok(if pairs.len() == 2 {
                    ControlFlow::Break("second stable pair".to_owned())
                } else {
                    ControlFlow::Continue(())
                })
            })
            .expect("the test bounds fit their structural width");
        assert_eq!(flow, ControlFlow::Break("second stable pair".to_owned()));
        assert_eq!(pairs, [(0, 20), (0, 39)]);
    }

    fn assert_sweep_error_stops_after_second_pair<W: Work>(bounds: &[Bounds], work: &mut W) {
        let mut pairs = Vec::new();
        let error =
            sweep_for_each_overlapping_bounds_pair_with_work(bounds, work, |left, right, _work| {
                pairs.push((left, right));
                if pairs.len() == 2 {
                    Err(GeometryErrorKind::Invalid("stable sweep callback".into()).into())
                } else {
                    Ok(ControlFlow::<()>::Continue(()))
                }
            })
            .unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        ));
        assert_eq!(pairs, [(0, 20), (0, 39)]);
    }

    #[test]
    fn equal_primary_bounds_keep_staging_order_for_every_work_policy() {
        let bounds = equal_primary_sweep_bounds();
        let expected = [(0, 20), (0, 39), (20, 39)];

        let mut ordinary = Vec::new();
        let flow = crate::geometry::for_each_overlapping_bounds_pair(&bounds, |left, right| {
            ordinary.push((left, right));
            ControlFlow::<()>::Continue(())
        })
        .unwrap();
        assert_eq!(flow, ControlFlow::Continue(()));
        assert_eq!(ordinary, expected);

        let mut unmetered = Unmetered;
        assert_eq!(stable_sweep_pairs(&bounds, &mut unmetered), expected);

        let mut erased_budget = EvaluationBudget::new("sweep_test", 1).unwrap();
        let erased = {
            let mut account = EvaluationOnly::new(&mut erased_budget);
            stable_sweep_pairs(&bounds, &mut account)
        };
        assert_eq!(erased, expected);
        assert_eq!(erased_budget.spent_ticks, 0);

        let mut budget = EvaluationBudget::new("sweep_test", 1).unwrap();
        assert_eq!(stable_sweep_pairs(&bounds, &mut budget), expected);
        assert!(budget.spent_ticks > 0);
    }

    #[test]
    fn equal_primary_cross_bounds_keep_left_then_right_staging_order() {
        let left = [Bounds::new_raw(0.0, 0.0, 1.0, 1.0); 3];
        let right = [Bounds::new_raw(0.0, 0.0, 1.0, 1.0); 3];
        let expected = [
            (0, 0),
            (0, 1),
            (0, 2),
            (1, 0),
            (1, 1),
            (1, 2),
            (2, 0),
            (2, 1),
            (2, 2),
        ];

        let mut ordinary = Vec::new();
        let flow =
            crate::geometry::for_each_overlapping_bounds_cross_pair(&left, &right, |a, b| {
                ordinary.push((a, b));
                ControlFlow::<()>::Continue(())
            })
            .unwrap();
        assert_eq!(flow, ControlFlow::Continue(()));
        assert_eq!(ordinary, expected);

        let mut unmetered = Unmetered;
        assert_eq!(
            stable_cross_sweep_pairs(&left, &right, &mut unmetered),
            expected
        );
        let mut erased_budget = EvaluationBudget::new("sweep_test", 1).unwrap();
        {
            let mut account = EvaluationOnly::new(&mut erased_budget);
            assert_eq!(
                stable_cross_sweep_pairs(&left, &right, &mut account),
                expected
            );
        }
        let mut budget = EvaluationBudget::new("sweep_test", 1).unwrap();
        assert_eq!(
            stable_cross_sweep_pairs(&left, &right, &mut budget),
            expected
        );
    }

    #[test]
    fn stable_sweep_break_and_callback_error_stop_at_the_same_pair_prefix() {
        let bounds = equal_primary_sweep_bounds();

        let mut ordinary_pairs = Vec::new();
        let ordinary_flow =
            crate::geometry::for_each_overlapping_bounds_pair(&bounds, |left, right| {
                ordinary_pairs.push((left, right));
                if ordinary_pairs.len() == 2 {
                    ControlFlow::Break("second stable pair".to_owned())
                } else {
                    ControlFlow::Continue(())
                }
            })
            .unwrap();
        assert_eq!(
            ordinary_flow,
            ControlFlow::Break("second stable pair".to_owned())
        );
        assert_eq!(ordinary_pairs, [(0, 20), (0, 39)]);

        let mut unmetered = Unmetered;
        assert_stable_break(&bounds, &mut unmetered);
        assert_sweep_error_stops_after_second_pair(&bounds, &mut unmetered);

        let mut erased_budget = EvaluationBudget::new("sweep_test", 1).unwrap();
        {
            let mut account = EvaluationOnly::new(&mut erased_budget);
            assert_stable_break(&bounds, &mut account);
            assert_sweep_error_stops_after_second_pair(&bounds, &mut account);
        }
        assert_eq!(erased_budget.spent_ticks, 0);

        let mut budget = EvaluationBudget::new("sweep_test", 1).unwrap();
        assert_stable_break(&bounds, &mut budget);
        assert_sweep_error_stops_after_second_pair(&bounds, &mut budget);
    }

    #[test]
    fn sweep_callback_reborrow_uses_the_same_exhausted_account() {
        let bounds = equal_primary_sweep_bounds();
        let mut baseline = EvaluationBudget::new("sweep_test", 1).unwrap();
        let mut before_callback = None;
        let flow = sweep_for_each_overlapping_bounds_pair_with_work(
            &bounds,
            &mut baseline,
            |left, right, work| {
                assert_eq!((left, right), (0, 20));
                before_callback = Some(work.spent_ticks);
                Ok(ControlFlow::Break(()))
            },
        )
        .unwrap();
        assert_eq!(flow, ControlFlow::Break(()));
        let before_callback = before_callback.unwrap();

        let allowance_after_sweep = before_callback.wrapping_add(1);
        let mut budget = EvaluationBudget::new("sweep_test", 1).unwrap();
        budget.limit =
            u64::try_from(allowance_after_sweep.div_ceil(WORK_TICKS_PER_EVALUATION)).unwrap();
        budget.spent_ticks = budget.limit_ticks().wrapping_sub(allowance_after_sweep);
        let mut callback_count = 0;
        let mut callback_progress = 0;

        let error =
            sweep_for_each_overlapping_bounds_pair_with_work(&bounds, &mut budget, |_, _, work| {
                callback_count += 1;
                work.charge(1)?;
                callback_progress += 1;
                work.charge(1)?;
                callback_progress += 1;
                Ok(ControlFlow::<()>::Continue(()))
            })
            .unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "sweep_test",
                constructed: 1,
                ..
            })
        ));
        assert_eq!(callback_count, 1);
        assert_eq!(callback_progress, 1);
        assert_eq!(budget.spent_ticks, budget.limit_ticks());
    }

    #[test]
    fn sweep_refuses_before_the_next_monotone_source_probe() {
        let pool = [
            Segment {
                start: XY::new(0.0, 0.0),
                end: XY::new(1.0, 1.0),
            },
            Segment {
                start: XY::new(3.0, 0.0),
                end: XY::new(4.0, 1.0),
            },
        ];
        let chain_calls = Cell::new(0);
        let candidate_calls = Cell::new(0);
        let mut budget = EvaluationBudget::new("sweep_test", 1).unwrap();
        budget.limit = 0;

        let error = sweep_for_each_candidate_pair_with_work(
            &pool,
            |_| {
                chain_calls.set(chain_calls.get() + 1);
                0
            },
            &mut budget,
            |_, _, _work| {
                candidate_calls.set(candidate_calls.get() + 1);
                Ok(ControlFlow::<()>::Continue(()))
            },
        )
        .unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "sweep_test",
                constructed: 1,
                evaluations: 0
            })
        ));
        assert_eq!(chain_calls.get(), 0);
        assert_eq!(candidate_calls.get(), 0);
        assert_eq!(budget.spent_ticks, 0);
    }

    #[test]
    fn bounds_brute_refusal_precedes_filter_and_callback() {
        let left = [Bounds::new_raw(0.0, 0.0, 1.0, 1.0)];
        let right = [Bounds::new_raw(0.0, 0.0, 1.0, 1.0)];
        let mut budget = EvaluationBudget::new("sweep_test", 1).unwrap();
        budget.limit = 1;
        budget.spent_ticks = 16;
        let mut callbacks = 0;

        let error = sweep_for_each_overlapping_bounds_cross_pair_with_work(
            &left,
            &right,
            &mut budget,
            |_, _, _work| {
                callbacks += 1;
                Ok(ControlFlow::<()>::Continue(()))
            },
        )
        .unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "sweep_test",
                constructed: 1,
                evaluations: 1
            })
        ));
        assert_eq!(callbacks, 0);
        assert_eq!(budget.spent_ticks, 16);
    }

    #[test]
    fn banded_window_refusal_precedes_the_first_4096_entry_callback() {
        let bounds = vec![Bounds::new_raw(0.0, 0.0, 1.0, 1.0); 4096];
        let mut budget = EvaluationBudget::new("sweep_test", 1).unwrap();
        // The funded entry, stable-sort, two extent-fold, CSR, band-row and
        // left-row phases total 126,736 ticks before the first right window.
        let remaining_ticks = 126_736_u128;
        budget.limit = u64::try_from(remaining_ticks.div_ceil(WORK_TICKS_PER_EVALUATION)).unwrap();
        budget.spent_ticks = budget.limit_ticks().wrapping_sub(remaining_ticks);
        let mut callbacks = 0;

        let error = sweep_for_each_overlapping_bounds_pair_with_work(
            &bounds,
            &mut budget,
            |_, _, _work| {
                callbacks += 1;
                Ok(ControlFlow::<()>::Break(()))
            },
        )
        .unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "sweep_test",
                constructed: 1,
                evaluations: 7_921
            })
        ));
        assert_eq!(callbacks, 0);
        assert_eq!(budget.spent_ticks, 126_736);
    }

    #[test]
    fn unaffordable_billing_product_is_a_native_search_limit() {
        let mut budget = EvaluationBudget::new("search_test", 1).unwrap();

        let error = <EvaluationBudget as Work>::charge_product(&mut budget, usize::MAX, usize::MAX)
            .unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "search_test",
                constructed: 1,
                evaluations: 0
            })
        ));
        assert_eq!(budget.spent_ticks, 0);
    }

    #[test]
    fn exact_product_refuses_before_multilimb_cells_after_reservation() {
        let left = ExactDyadic::from_parts(false, vec![u64::MAX; 9], 0);
        let right = ExactDyadic::from_parts(false, vec![u64::MAX; 10], 0);
        let mut budget = EvaluationBudget::new("exact_product", 1).unwrap();
        budget.limit = 6;

        let error = left.try_product_with_work(&right, &mut budget).unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "exact_product",
                constructed: 1,
                evaluations: 2
            })
        ));
        assert_eq!(budget.spent_ticks, 19);
    }

    #[test]
    fn exact_division_refuses_before_the_first_algorithm_d_digit_cells() {
        const TOP: u64 = 1 << 63;
        let dividend = [0, 0, TOP, TOP - 1];
        let divisor = [1, 0, TOP];
        let mut budget = EvaluationBudget::new("exact_division", 1).unwrap();
        budget.limit = 2;
        budget.spent_ticks = 8;

        let error =
            ExactDyadic::div_rem_unsigned_with_work(&dividend, &divisor, &mut budget).unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "exact_division",
                constructed: 1,
                evaluations: 2
            })
        ));
        assert_eq!(budget.spent_ticks, 30);
    }

    #[test]
    fn exact_inline_gcd_funds_each_binary_iteration() {
        const PRIME: u64 = 0xFFFF_FFFF_FFFF_FFC5;
        let left = ExactDyadic::try_from_u128(false, u128::from(PRIME).wrapping_mul(3), 5).unwrap();
        let right =
            ExactDyadic::try_from_u128(false, u128::from(PRIME).wrapping_mul(5), -3).unwrap();
        let mut budget = EvaluationBudget::new("exact_gcd", 1).unwrap();
        budget.limit = 1;
        budget.spent_ticks = 15;

        let error = left.gcd_with_work(&right, &mut budget).unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "exact_gcd",
                constructed: 1,
                evaluations: 1
            })
        ));
        assert_eq!(budget.spent_ticks, 16);
    }

    #[test]
    fn exact_ratio_rounding_refuses_before_multilimb_product_cells() {
        let numerator = ExactDyadic::one();
        let denominator = ExactDyadic::from_parts(false, vec![1, 0, 1], 0);
        let mut budget = EvaluationBudget::new("exact_round", 1).unwrap();
        budget.limit = 1;
        budget.spent_ticks = 6;

        let error = round_ratio_with_work(&numerator, &denominator, &mut budget).unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "exact_round",
                constructed: 1,
                evaluations: 1
            })
        ));
        assert_eq!(budget.spent_ticks, 14);
    }

    #[test]
    fn exact_primitives_share_native_fuel_and_evaluation_only_erases_work() {
        let left = ExactDyadic::from_parts(false, vec![u64::MAX; 9], 0);
        let right = ExactDyadic::from_parts(false, vec![u64::MAX; 10], 0);
        let expected_product = [
            1,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            u64::MAX,
            u64::MAX - 1,
            u64::MAX,
            u64::MAX,
            u64::MAX,
            u64::MAX,
            u64::MAX,
            u64::MAX,
            u64::MAX,
            u64::MAX,
        ];
        let numerator = ExactDyadic::from_f64(615.0);
        let denominator = ExactDyadic::from_f64(9.0);
        let one = ExactDyadic::one();
        let three = ExactDyadic::from_f64(3.0);

        let mut native = EvaluationBudget::new("exact_primitives", 1).unwrap();
        let native_product = left.try_product_with_work(&right, &mut native).unwrap();
        let native_ratio =
            ExactRational::new_with_work(numerator.clone(), denominator.clone(), &mut native)
                .unwrap();
        let native_round = round_ratio_with_work(&one, &three, &mut native).unwrap();

        assert_eq!(native_product.limbs.as_slice(), expected_product);
        let (normalized_numerator, normalized_denominator) = native_ratio.parts();
        assert_eq!(normalized_numerator.limbs.as_slice(), [205]);
        assert_eq!(normalized_denominator.limbs.as_slice(), [3]);
        assert_eq!(normalized_numerator.exponent, 0);
        assert_eq!(normalized_denominator.exponent, 0);
        assert_eq!(native_round.to_bits(), 0x3FD5_5555_5555_5555);
        assert!(native.spent_ticks > 0);
        assert!(native.spent_ticks <= native.limit_ticks());

        let mut depleted = EvaluationBudget::new("exact_erasure", 1).unwrap();
        depleted.limit = 1;
        depleted.spent_ticks = depleted.limit_ticks();
        let spent_before_erasure = depleted.spent_ticks;
        let (erased_product, erased_ratio, erased_round) = {
            let mut erased = EvaluationOnly::new(&mut depleted);
            let product = left.try_product_with_work(&right, &mut erased).unwrap();
            let ratio = ExactRational::new_with_work(numerator, denominator, &mut erased).unwrap();
            let rounded = round_ratio_with_work(&one, &three, &mut erased).unwrap();
            (product, ratio, rounded)
        };

        assert_eq!(erased_product.limbs.as_slice(), expected_product);
        let (normalized_numerator, normalized_denominator) = erased_ratio.parts();
        assert_eq!(normalized_numerator.limbs.as_slice(), [205]);
        assert_eq!(normalized_denominator.limbs.as_slice(), [3]);
        assert_eq!(erased_round.to_bits(), 0x3FD5_5555_5555_5555);
        assert_eq!(depleted.spent_ticks, spent_before_erasure);
    }

    #[test]
    fn native_budget_refuses_the_first_stable_merge_comparison() {
        let mut items: Vec<_> = (16_u8..32).chain(0..16).map(|key| (key, key)).collect();
        let original = items.clone();
        let mut comparisons = 0;
        let mut budget = EvaluationBudget::new("search_test", 1).unwrap();
        budget.limit = 15;
        budget.spent_ticks = 146;

        let error = try_sort_by_with_work(&mut items, &mut budget, |left, right, _work| {
            comparisons += 1;
            Ok(left.0.cmp(&right.0))
        })
        .unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "search_test",
                constructed: 1,
                evaluations: 15
            })
        ));
        assert_eq!(comparisons, 30);
        assert_eq!(items, original);
        assert_eq!(budget.spent_ticks, 240);
    }

    #[test]
    fn stable_sort_comparator_uses_the_same_native_budget_before_its_progress() {
        let mut items = [(2_u8, 'A'), (1, 'B')];
        let original = items;
        let mut comparator_progress = 0;
        let mut budget = EvaluationBudget::new("search_test", 1).unwrap();
        budget.limit = 1;
        budget.spent_ticks = 14;

        let error = try_sort_by_with_work(&mut items, &mut budget, |left, right, work| {
            work.charge(2)?;
            comparator_progress += 1;
            Ok(left.0.cmp(&right.0))
        })
        .unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "search_test",
                constructed: 1,
                evaluations: 1
            })
        ));
        assert_eq!(comparator_progress, 0);
        assert_eq!(items, original);
        assert_eq!(budget.spent_ticks, 15);
    }

    #[test]
    fn merge_comparator_uses_the_same_native_budget_before_its_progress() {
        let mut items: Vec<_> = (16_u8..32).chain(0..16).map(|key| (key, key)).collect();
        let original = items.clone();
        let mut merge_callbacks = 0;
        let mut comparator_progress = 0;
        let mut budget = EvaluationBudget::new("search_test", 1).unwrap();
        budget.limit = 15;
        budget.spent_ticks = 144;

        let error = try_sort_by_with_work(&mut items, &mut budget, |left, right, work| {
            if (*left == (16, 16)) && (*right == (0, 0)) {
                merge_callbacks += 1;
                work.charge(2)?;
                comparator_progress += 1;
            }
            Ok(left.0.cmp(&right.0))
        })
        .unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "search_test",
                constructed: 1,
                evaluations: 15
            })
        ));
        assert_eq!(merge_callbacks, 1);
        assert_eq!(comparator_progress, 0);
        assert_eq!(items, original);
        assert_eq!(budget.spent_ticks, 239);
    }

    #[test]
    fn native_budget_refuses_the_merge_output_slot_after_its_comparison() {
        let mut items: Vec<_> = (16_u8..32).chain(0..16).map(|key| (key, key)).collect();
        let original = items.clone();
        let mut comparisons = 0;
        let mut budget = EvaluationBudget::new("search_test", 1).unwrap();
        budget.limit = 15;
        budget.spent_ticks = 145;

        let error = try_sort_by_with_work(&mut items, &mut budget, |left, right, _work| {
            comparisons += 1;
            Ok(left.0.cmp(&right.0))
        })
        .unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "search_test",
                constructed: 1,
                evaluations: 15
            })
        ));
        assert_eq!(comparisons, 31);
        assert_eq!(items, original);
        assert_eq!(budget.spent_ticks, 240);
    }

    #[test]
    fn native_budget_refuses_a_permutation_swap_before_touching_items() {
        let mut items: Vec<_> = (16_u8..32).chain(0..16).map(|key| (key, key)).collect();
        let original = items.clone();
        let mut comparisons = 0;
        let mut budget = EvaluationBudget::new("search_test", 1).unwrap();
        budget.limit = 11;
        budget.spent_ticks = 33;

        let error = try_sort_by_with_work(&mut items, &mut budget, |left, right, _work| {
            comparisons += 1;
            Ok(left.0.cmp(&right.0))
        })
        .unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "search_test",
                constructed: 1,
                evaluations: 11
            })
        ));
        assert_eq!(comparisons, 46);
        assert_eq!(items, original);
        assert_eq!(budget.spent_ticks, 176);
    }

    #[test]
    fn native_budget_refusal_precedes_product_carry_solution_replacement() {
        let mut current_solutions = vec![vec![9_u8]];
        let mut budget = EvaluationBudget::new("search_test", 1).unwrap();
        budget.limit = 1;
        budget.spent_ticks = 16;

        let error =
            replace_class_solution(&mut current_solutions, 0, vec![7], &mut budget).unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "search_test",
                constructed: 1,
                evaluations: 1
            })
        ));
        assert_eq!(current_solutions, [vec![9]]);
        assert_eq!(budget.spent_ticks, 16);
    }

    #[test]
    fn raw_tick_refusal_leaves_the_shared_balance_unchanged() {
        let mut budget = EvaluationBudget::new("search_test", 1).unwrap();
        budget.limit = 1;
        for _ in 0..15 {
            <EvaluationBudget as Work>::charge(&mut budget, 1).unwrap();
        }
        assert_eq!(budget.spent_ticks, 15);
        <EvaluationBudget as Work>::charge(&mut budget, 1).unwrap();

        let error = <EvaluationBudget as Work>::charge(&mut budget, 1).unwrap_err();

        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "search_test",
                constructed: 1,
                evaluations: 1
            })
        ));
        assert_eq!(budget.spent_ticks, 16);
    }

    fn native_source_sample<M: crate::geometry::ChartMetric>(
        metric: &M,
        frame: crate::geographic::GeographicFrame,
        source: &[crate::geometry::Point; 2],
        longitude: crate::geographic::RetainedChartDisplacement,
        parameter: f64,
        axes: crate::geometry::CoordinateAxes,
        work: &mut impl Work,
    ) -> Result<crate::geographic::ComputedSampleOrdinates> {
        let mut root = crate::geometry::RootMetricState::default();
        crate::geometry::computed_native_sample_ordinates(
            frame,
            source,
            longitude,
            parameter,
            axes,
            || Ok(metric),
            &mut root,
            work,
        )
    }

    #[derive(Clone, Copy)]
    struct SuccessfulRawCharge {
        ticks: u128,
        spent_before: u128,
        progress: crate::crs::QuadratureTestProgress,
    }

    struct RecordingWork<'a> {
        budget: &'a mut EvaluationBudget,
        successful: Vec<SuccessfulRawCharge>,
    }

    impl RecordingWork<'_> {
        fn record_success(
            &mut self,
            spent_before: u128,
            progress: crate::crs::QuadratureTestProgress,
        ) {
            let ticks = self.budget.spent_ticks.saturating_sub(spent_before);
            self.successful.push(SuccessfulRawCharge {
                ticks,
                spent_before,
                progress,
            });
        }
    }

    impl Work for RecordingWork<'_> {
        fn charge(&mut self, ticks: u64) -> Result<()> {
            let spent_before = self.budget.spent_ticks;
            let progress = crate::crs::quadrature_test_progress();
            <EvaluationBudget as Work>::charge(self.budget, ticks)?;
            self.record_success(spent_before, progress);
            Ok(())
        }

        fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
            let spent_before = self.budget.spent_ticks;
            let progress = crate::crs::quadrature_test_progress();
            <EvaluationBudget as Work>::charge_product(self.budget, count, ticks_per_item)?;
            self.record_success(spent_before, progress);
            Ok(())
        }
    }

    fn budget_refusing_before(
        operation: &'static str,
        event: &SuccessfulRawCharge,
    ) -> EvaluationBudget {
        assert!(event.ticks > 0);
        let mut budget =
            EvaluationBudget::new(operation, 1).expect("the test budget is representable");
        let earlier_work = event
            .spent_before
            .checked_add(event.ticks.checked_sub(1).expect("charge is positive"))
            .expect("observed work fits the raw account");
        let prior_spend = budget
            .limit_ticks()
            .checked_sub(earlier_work)
            .expect("the event fits within the real budget");
        <EvaluationBudget as Work>::charge(
            &mut budget,
            u64::try_from(prior_spend).expect("the account limit fits u64 ticks"),
        )
        .expect("the calibrated prior spend fits the real account");
        budget
    }

    fn assert_raw_budget_refuses_before_progress(
        operation: &'static str,
        event: &SuccessfulRawCharge,
        action: &'static str,
        run: impl FnOnce(&mut EvaluationBudget) -> Result<crate::geographic::ComputedSampleOrdinates>,
    ) {
        crate::crs::reset_quadrature_test_progress();
        let mut budget = budget_refusing_before(operation, event);
        let Err(error) = run(&mut budget) else {
            panic!("{action}: operation unexpectedly succeeded")
        };
        let ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: actual_operation,
            constructed: 1,
            ..
        }) = error.kind()
        else {
            panic!(
                "{action}: expected a raw budget refusal, got {:?}",
                error.kind()
            )
        };
        assert_eq!(*actual_operation, operation, "{action}");
        assert_eq!(
            budget.spent_ticks,
            budget
                .limit_ticks()
                .checked_sub(event.ticks)
                .and_then(|ticks| ticks.checked_add(1))
                .expect("the refused charge remains unpaid"),
            "{action}"
        );
        assert_eq!(
            crate::crs::quadrature_test_progress(),
            event.progress,
            "{action}"
        );
    }

    #[test]
    fn native_source_arc_quadrature_funds_refinement_before_progress() {
        const OPERATION: &str = "quadrature_source_arc";
        let runtime = crate::crs::runtime_state_snapshot()
            .expect("the test runtime configuration is available");
        crate::crs::with_ellipsoid_metric_at("EPSG:4326", &[], &runtime, |metric| {
            let frame = crate::geometry::ChartMetric::chart_frame(metric);
            let source = [
                crate::geometry::Point::new_axes(
                    0.0,
                    0.0,
                    crate::geometry::ZOrdinate(Some(7.0)),
                    crate::geometry::MOrdinate(Some(101.0)),
                )?,
                crate::geometry::Point::new_axes(
                    179.0,
                    89.9,
                    crate::geometry::ZOrdinate(Some(47.0)),
                    crate::geometry::MOrdinate(Some(701.0)),
                )?,
            ];
            let longitude = frame
                .longitude()
                .chart_connection(source[0].x, source[1].x)
                .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))?
                .retained();
            let parameter = 0.5;
            let axes = crate::geometry::CoordinateAxes::XYZM;

            crate::crs::reset_quadrature_test_progress();
            let mut metered_budget = EvaluationBudget::new(OPERATION, 1)?;
            let (metered, successful_charges) = {
                let mut work = RecordingWork {
                    budget: &mut metered_budget,
                    successful: Vec::new(),
                };
                let sample = native_source_sample(
                    metric, frame, &source, longitude, parameter, axes, &mut work,
                )?;
                (sample, std::mem::take(&mut work.successful))
            };
            let metered_progress = crate::crs::quadrature_test_progress();
            assert!(
                metered_progress.max_refinements >= 2,
                "the real source-arc path refined only {} times in one integration",
                metered_progress.max_refinements
            );
            assert!(metered_budget.spent_ticks > 0);

            let actual_z = metered.z.0.expect("XYZM sampling carries Z");
            let expected_z = 31.528_758_192_036_157;
            assert!(
                (actual_z - expected_z).abs() <= 1.0e-12,
                "Z oracle: actual={actual_z:.17}, expected={expected_z:.17}"
            );
            let actual_m = metered.m.0.expect("XYZM sampling carries M");
            let expected_m = 468.931_372_880_542_37;
            assert!(
                (actual_m - expected_m).abs() <= 1.0e-11,
                "M oracle: actual={actual_m:.17}, expected={expected_m:.17}"
            );

            crate::crs::reset_quadrature_test_progress();
            let mut ordinary_work = Unmetered;
            let ordinary = native_source_sample(
                metric,
                frame,
                &source,
                longitude,
                parameter,
                axes,
                &mut ordinary_work,
            )?;
            assert_eq!(metered.y.to_bits(), ordinary.y.to_bits());
            assert_eq!(
                metered.z.0.map(f64::to_bits),
                ordinary.z.0.map(f64::to_bits)
            );
            assert_eq!(
                metered.m.0.map(f64::to_bits),
                ordinary.m.0.map(f64::to_bits)
            );

            let funded_growths = successful_charges
                .iter()
                .filter(|charge| charge.progress.growth_requests > charge.progress.growths)
                .count();
            assert_eq!(
                u32::try_from(funded_growths).expect("growth count fits the interval cap"),
                metered_progress.growths,
                "each interval growth must follow a successful raw commit debit"
            );
            let scan_charge = successful_charges
                .iter()
                .copied()
                .find(|charge| charge.progress.seeds == 1 && charge.progress.scans == 0)
                .expect("the real source path charges the largest-error scan");
            let commit_charge = successful_charges
                .iter()
                .copied()
                .find(|charge| {
                    charge.progress.child_pairs > charge.progress.refinements
                        && charge.progress.growth_requests > charge.progress.growths
                })
                .expect("the real source path charges a refinement that requires growth");

            let removal_charge = successful_charges
                .iter()
                .copied()
                .find(|charge| charge.progress.scans == 1 && charge.progress.removals == 0)
                .expect("the real source path charges before removing the largest interval");
            for (charge, action) in [
                (&scan_charge, "before the interval scan"),
                (&removal_charge, "before interval removal"),
                (&commit_charge, "before the refinement commit"),
            ] {
                assert_raw_budget_refuses_before_progress(OPERATION, charge, action, |budget| {
                    let mut work = RecordingWork {
                        budget,
                        successful: Vec::new(),
                    };
                    native_source_sample(
                        metric, frame, &source, longitude, parameter, axes, &mut work,
                    )
                });
            }
            Ok(())
        })
        .expect("EPSG:4326 resolves the source-arc test metric");
    }
}
