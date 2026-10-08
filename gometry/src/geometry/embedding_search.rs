//! Deterministic search over finite embedding domains.

use std::cmp::Ordering;
use std::hash::Hash;

use crate::collections::{
    HashMap, HashSet, UnionFind, try_hash_map_with_capacity, try_hash_set_with_capacity,
};
use crate::error::{ResourceError, Result};
use crate::geometry::{
    Bounds, GeometryErrorKind, Segment, XY, for_each_overlapping_bounds_cross_pair,
};

const BASE_EVALUATION_LIMIT: u64 = 1 << 16;
const EVALUATIONS_PER_CONSTRUCTED_NODE: u64 = 1 << 12;
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

/// Expand witness nodes to independent classes whose candidate drawings can
/// change a shared chord predicate.
#[expect(
    clippy::indexing_slicing,
    reason = "chord endpoint IDs, incidence rows, sweep ordinals, and class ranks are parallel columns built from this admitted graph"
)]
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
    if seeds.is_empty() {
        return Ok(ProximityClasses {
            classes: Vec::new(),
            node_location: vec![None; node_count],
            radii: vec![None; chords.len()],
            initial_evaluations: 0,
        });
    }
    let mut closure = vec![false; node_count];
    let mut closure_nodes = Vec::new();
    for &node in seeds {
        if cached_movable(node, movable_cache, &mut is_movable)? && !closure[node as usize] {
            closure[node as usize] = true;
            closure_nodes.push(node);
        }
    }
    let mut frontier_nodes = closure_nodes.clone();
    let mut union = UnionFind::new(node_count);
    let all_bounds = chords
        .iter()
        .copied()
        .map(possible_swept_bounds)
        .collect::<Vec<_>>();
    let mut radius_cache = vec![None; chords.len()];
    let mut frontier_seen = vec![false; chords.len()];
    let mut initial_evaluations = 0_u64;
    while !frontier_nodes.is_empty() {
        let mut frontier_chords = Vec::new();
        for node in std::mem::take(&mut frontier_nodes) {
            for &chord in incident_chords
                .get(node as usize)
                .expect("frontier node belongs to the incident-chord domain")
            {
                let seen = frontier_seen
                    .get_mut(chord)
                    .expect("incident chord belongs to the emitted chord table");
                if !std::mem::replace(seen, true) {
                    frontier_chords.push(chord);
                }
            }
        }
        for &chord in &frontier_chords {
            *frontier_seen
                .get_mut(chord)
                .expect("frontier chord belongs to the emitted chord table") = false;
        }
        frontier_chords.sort_unstable();
        if frontier_chords.is_empty() {
            break;
        }
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
        initial_evaluations = initial_evaluations
            .checked_add(sweep_cost)
            .ok_or(ResourceError::SizeOverflow)?;
        let mut newly_reached = Vec::new();
        for &chord_index in &frontier_chords {
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
            initial_evaluations = initial_evaluations
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            if !one_step_endpoint_bounds(start).intersects(one_step_endpoint_bounds(end)) {
                continue;
            }
            union.union(first as usize, second as usize);
            for node in [first, second] {
                let reached = closure
                    .get_mut(node as usize)
                    .expect("chord endpoint belongs to the node domain");
                if !std::mem::replace(reached, true) {
                    closure_nodes.push(node);
                    newly_reached.push(node);
                }
            }
        }
        let mut callback_error = None;
        let flow =
            for_each_overlapping_bounds_cross_pair(&left_bounds, &all_bounds, |left, right| {
                if callback_error.is_some() {
                    return std::ops::ControlFlow::Break(());
                }
                let left_index = frontier_chords[left];
                if left_index == right {
                    return std::ops::ControlFlow::Continue(());
                }
                let left_chord = *chords
                    .get(left_index)
                    .expect("frontier chord belongs to the emitted chord table");
                let right_chord = *chords
                    .get(right)
                    .expect("overlap sweep ordinal belongs to the emitted chord table");
                let exact_radii = (|| {
                    Ok((
                        exact_chord_radius(
                            left_chord,
                            &mut radius_cache,
                            left_index,
                            movable_cache,
                            &mut is_movable,
                        )?,
                        exact_chord_radius(
                            right_chord,
                            &mut radius_cache,
                            right,
                            movable_cache,
                            &mut is_movable,
                        )?,
                    ))
                })();
                let (left_radius, right_radius) = match exact_radii {
                    Ok(radii) => radii,
                    Err(error) => {
                        callback_error = Some(error);
                        return std::ops::ControlFlow::Break(());
                    },
                };
                if !segments_within(
                    left_chord.nearest,
                    right_chord.nearest,
                    proximity_limit(left_radius, right_radius),
                ) {
                    return std::ops::ControlFlow::Continue(());
                }
                let endpoints = (|| {
                    Ok((
                        chord_movable_endpoints(left_chord, movable_cache, &mut is_movable)?,
                        chord_movable_endpoints(right_chord, movable_cache, &mut is_movable)?,
                    ))
                })();
                let (left_movable, right_movable) = match endpoints {
                    Ok(endpoints) => endpoints,
                    Err(error) => {
                        callback_error = Some(error);
                        return std::ops::ControlFlow::Break(());
                    },
                };
                let movable_nodes = [
                    left_movable[0],
                    left_movable[1],
                    right_movable[0],
                    right_movable[1],
                ];
                for node in movable_nodes.into_iter().flatten() {
                    let reached = closure
                        .get_mut(node as usize)
                        .expect("chord endpoint belongs to the node domain");
                    if !std::mem::replace(reached, true) {
                        closure_nodes.push(node);
                        newly_reached.push(node);
                    }
                }
                for endpoints in [left_movable, right_movable] {
                    if let [Some(first), Some(second)] = endpoints {
                        let Some(evaluations) = initial_evaluations.checked_add(1) else {
                            callback_error = Some(ResourceError::SizeOverflow.into());
                            return std::ops::ControlFlow::Break(());
                        };
                        initial_evaluations = evaluations;
                        union.union(first as usize, second as usize);
                    }
                }
                for left_node in left_movable.into_iter().flatten() {
                    for right_node in right_movable.into_iter().flatten() {
                        union.union(left_node as usize, right_node as usize);
                    }
                }
                std::ops::ControlFlow::Continue(())
            })?;
        if let Some(error) = callback_error {
            return Err(error);
        }
        debug_assert!(flow.is_continue());
        frontier_nodes = newly_reached;
    }

    let mut components: HashMap<usize, Vec<u32>> = try_hash_map_with_capacity(closure_nodes.len())?;
    for node in closure_nodes {
        let root = union.find(node as usize);
        components.entry(root).or_default().push(node);
    }
    let mut classes: Vec<_> = components.into_values().collect();
    for class in &mut classes {
        sort_fallible(class, |left, right| compare_nodes(*left, *right))?;
    }
    sort_fallible(&mut classes, |left, right| {
        compare_nodes(
            *left.first().expect("proximity class is nonempty"),
            *right.first().expect("proximity class is nonempty"),
        )
    })?;
    let mut node_location = vec![None; node_count];
    for (class, nodes) in classes.iter().enumerate() {
        for (rank, &node) in nodes.iter().enumerate() {
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
pub(in crate::geometry) struct SearchAssignment<N, C> {
    pub classes: Vec<ClassSolution<N, C>>,
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
pub(in crate::geometry) enum GlobalCheck {
    Pass,
    Reject(Vec<usize>),
}

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

    /// Each movable node appears in exactly one nonempty class.
    fn classes(&self) -> &[Vec<Self::Node>];

    /// Nearest-coordinate keys for all live nodes outside this class.
    fn fixed_keys(&self, class: usize, variables: &[Self::Node]) -> Result<Vec<Self::Key>>;

    fn compare_nodes(&self, left: Self::Node, right: Self::Node) -> Result<Ordering>;

    /// Append domain-admitted candidates for `node`; their input order is ignored.
    fn fill_candidates(
        &self,
        node: Self::Node,
        candidates: &mut Vec<Self::Candidate>,
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
    fn prepare_class(
        &self,
        class: usize,
        variables: &[Self::Node],
        budget: &mut EvaluationBudget,
    ) -> Result<Self::ClassState>;

    /// Run ready P/F predicates for `assigned`, which contains the ordered prefix through `rank`.
    fn check_local(
        &self,
        class_state: &Self::ClassState,
        class: usize,
        variables: &[Self::Node],
        rank: usize,
        assigned: &[Option<Self::Candidate>],
        budget: &mut EvaluationBudget,
    ) -> Result<bool>;

    /// Run G on the composed assignment and name classes implicated by a rejection.
    ///
    /// The first nonempty blame prioritizes digit order, not products to skip.
    /// Later blame is validated but cannot change product order. Return empty
    /// blame only when no prepared variable assignment can repair the rejection;
    /// otherwise name at least one influential prepared class.
    /// Charge `items.len().div_ceil(16)` for each G sweep before running it.
    fn check_global(
        &self,
        assignments: &[ClassAssignment<'_, Self::Node, Self::Candidate>],
        budget: &mut EvaluationBudget,
    ) -> Result<GlobalCheck>;

    /// Work units for closure sweeps already performed while building seeds and classes.
    fn initial_evaluations(&self) -> u64 {
        0
    }
}

pub(in crate::geometry) struct EvaluationBudget {
    operation: &'static str,
    constructed: usize,
    limit: u64,
    evaluations: u64,
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
            evaluations: 0,
        })
    }

    pub(in crate::geometry) fn set_constructed(&mut self, constructed: usize) -> Result<()> {
        let constructed = u64::try_from(constructed).map_err(|_| ResourceError::SizeOverflow)?;
        let per_node = EVALUATIONS_PER_CONSTRUCTED_NODE
            .checked_mul(constructed)
            .ok_or(ResourceError::SizeOverflow)?;
        self.constructed = usize::try_from(constructed).map_err(|_| ResourceError::SizeOverflow)?;
        self.limit = BASE_EVALUATION_LIMIT.max(per_node);
        if self.evaluations > self.limit {
            return Err(self.limit_error());
        }
        Ok(())
    }

    /// Charge a completed-work unit before running the corresponding predicate.
    pub(in crate::geometry) fn charge(&mut self, units: u64) -> Result<()> {
        let Some(next) = self.evaluations.checked_add(units) else {
            return Err(self.limit_error());
        };
        if next > self.limit {
            return Err(self.limit_error());
        }
        self.evaluations = next;
        Ok(())
    }

    /// Record work that the problem had to complete before search began.
    pub(in crate::geometry) fn charge_completed(&mut self, units: u64) -> Result<()> {
        self.evaluations = self
            .evaluations
            .checked_add(units)
            .ok_or_else(|| self.limit_error())?;
        if self.evaluations > self.limit {
            return Err(self.limit_error());
        }
        Ok(())
    }

    fn limit_error(&self) -> crate::error::Error {
        GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: self.operation,
            constructed: self.constructed,
            evaluations: self.evaluations,
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
    fn new<N, S>(class: &PreparedClass<N, C, K, S>) -> Result<Self> {
        let candidate_count = class.options.iter().try_fold(0_usize, |count, options| {
            count
                .checked_add(options.len())
                .ok_or(ResourceError::SizeOverflow)
        })?;
        Ok(Self {
            next_candidate: vec![0; class.variables.len()],
            selected: vec![None; class.variables.len()],
            selected_keys: vec![None; class.variables.len()],
            used_keys: try_hash_set_with_capacity(candidate_count)?,
            yielded: false,
            exhausted: false,
        })
    }

    fn reset(&mut self) {
        self.next_candidate.fill(0);
        self.selected.fill(None);
        self.selected_keys.fill(None);
        self.used_keys.clear();
        self.yielded = false;
        self.exhausted = false;
    }

    fn unassign(&mut self, rank: usize) {
        if let Some(key) = self
            .selected_keys
            .get_mut(rank)
            .expect("rank belongs to the class search state")
            .take()
        {
            self.used_keys.remove(&key);
        }
        *self
            .selected
            .get_mut(rank)
            .expect("rank belongs to the class search state") = None;
    }

    fn next_solution<P>(
        &mut self,
        class: &PreparedClass<P::Node, P::Candidate, P::Key, P::ClassState>,
        problem: &P,
        budget: &mut EvaluationBudget,
    ) -> Result<Option<Vec<P::Candidate>>>
    where
        P: Problem<Key = K, Candidate = C>,
    {
        if self.exhausted {
            return Ok(None);
        }
        let variable_count = class.variables.len();

        let mut rank = if self.yielded {
            self.yielded = false;
            let last = variable_count.wrapping_sub(1);
            self.unassign(last);
            last
        } else {
            0
        };

        loop {
            let options = class
                .options
                .get(rank)
                .expect("rank is within the prepared class");
            let next = self
                .next_candidate
                .get_mut(rank)
                .expect("rank is within the class search state");
            if *next >= options.len() {
                *next = 0;
                if rank == 0 {
                    self.exhausted = true;
                    return Ok(None);
                }
                rank = rank.wrapping_sub(1);
                self.unassign(rank);
                continue;
            }

            let candidate_index = *next;
            *next = next.wrapping_add(1);
            let (candidate, key) = *options
                .get(candidate_index)
                .expect("candidate ordinal is bounded by its option row");
            budget.charge(1)?;
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
                budget,
            )? {
                self.unassign(rank);
                continue;
            }
            if rank.wrapping_add(1) == variable_count {
                self.yielded = true;
                let mut solution = Vec::with_capacity(variable_count);
                solution.extend(self.selected.iter().map(|value| {
                    value.expect("a complete local solution assigns every class member")
                }));
                return Ok(Some(solution));
            }
            rank = rank.wrapping_add(1);
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

    pub(in crate::geometry) fn run(mut self) -> Result<SearchAssignment<P::Node, P::Candidate>> {
        self.budget
            .charge_completed(self.problem.initial_evaluations())?;
        let (prepared, class_positions) = prepare_classes(&self.problem, &mut self.budget)?;
        let mut states = Vec::with_capacity(prepared.len());
        let mut current_solutions = Vec::with_capacity(prepared.len());
        for class in &prepared {
            let mut state = ClassSearchState::new(class)?;
            let Some(solution) = state.next_solution(class, &self.problem, &mut self.budget)?
            else {
                return Err(self.budget.unrepresentable());
            };
            states.push(state);
            current_solutions.push(solution);
        }
        let mut product_order = None;
        loop {
            let global = {
                let mut assignments = Vec::with_capacity(prepared.len());
                for (class, values) in prepared.iter().zip(&current_solutions) {
                    assignments.push(ClassAssignment {
                        variables: &class.variables,
                        values,
                    });
                }
                self.problem.check_global(&assignments, &mut self.budget)?
            };
            match global {
                GlobalCheck::Pass => {
                    let mut classes = Vec::with_capacity(prepared.len());
                    for (class, values) in prepared.into_iter().zip(current_solutions) {
                        classes.push(ClassSolution {
                            variables: class.variables,
                            values,
                        });
                    }
                    return Ok(SearchAssignment {
                        classes,
                        #[cfg(test)]
                        evaluations: self.budget.evaluations,
                    });
                },
                GlobalCheck::Reject(blamed_classes) => {
                    if blamed_classes.is_empty() {
                        return Err(self.budget.unrepresentable());
                    }
                    if product_order.is_none() {
                        let mut order = Vec::with_capacity(prepared.len());
                        for class in blamed_classes {
                            order.push(prepared_position_for_blame(&class_positions, class)?);
                        }
                        order.sort_unstable();
                        order.dedup();
                        let blamed_count = order.len();
                        for position in 0..prepared.len() {
                            if order
                                .get(..blamed_count)
                                .expect("blamed prefix belongs to the product order")
                                .binary_search(&position)
                                .is_err()
                            {
                                order.push(position);
                            }
                        }
                        product_order = Some(order);
                    } else {
                        for class in blamed_classes {
                            prepared_position_for_blame(&class_positions, class)?;
                        }
                    }

                    let order = product_order
                        .as_ref()
                        .expect("a nonempty rejection establishes the product order");
                    let mut advanced = false;
                    for &position in order {
                        let class = prepared
                            .get(position)
                            .expect("class rank is within prepared classes");
                        let state = states
                            .get_mut(position)
                            .expect("class rank is within class search states");
                        if let Some(solution) =
                            state.next_solution(class, &self.problem, &mut self.budget)?
                        {
                            *current_solutions
                                .get_mut(position)
                                .expect("class rank is within current solutions") = solution;
                            advanced = true;
                            break;
                        }

                        state.reset();
                        let Some(solution) =
                            state.next_solution(class, &self.problem, &mut self.budget)?
                        else {
                            return Err(GeometryErrorKind::Invalid(
                                "embedding search could not restart a prepared local solution stream"
                                    .into(),
                            )
                            .into());
                        };
                        *current_solutions
                            .get_mut(position)
                            .expect("class rank is within current solutions") = solution;
                    }
                    if !advanced {
                        return Err(self.budget.unrepresentable());
                    }
                },
            }
        }
    }
}

fn prepared_position_for_blame(class_positions: &[usize], class: usize) -> Result<usize> {
    class_positions.get(class).copied().ok_or_else(|| {
        GeometryErrorKind::Invalid("embedding search blamed an unknown variable class".into())
            .into()
    })
}

fn prepare_classes<P: Problem>(
    problem: &P,
    budget: &mut EvaluationBudget,
) -> Result<PreparedClassSet<P>> {
    let classes = problem.classes();

    let mut specs = Vec::with_capacity(classes.len());
    for (class, variables) in classes.iter().enumerate() {
        if variables.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "embedding search received an empty variable class".into(),
            )
            .into());
        }
        let mut variables = variables.clone();
        sort_fallible(&mut variables, |left, right| {
            problem.compare_nodes(*left, *right)
        })?;
        specs.push(ClassSpec { class, variables });
    }
    sort_fallible(&mut specs, |left, right| {
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
    let _candidate_hint = candidate_scratch.try_reserve(9);
    let mut seen_keys = Vec::new();
    let _key_hint = seen_keys.try_reserve(9);
    let mut admitted = Vec::with_capacity(specs.len());
    for spec in specs {
        let fixed_candidates = problem.fixed_keys(spec.class, &spec.variables)?;
        let mut fixed_keys = try_hash_set_with_capacity(fixed_candidates.len())?;
        for key in fixed_candidates {
            fixed_keys.insert(key);
        }
        let mut options = Vec::with_capacity(spec.variables.len());
        for &node in &spec.variables {
            candidate_scratch.clear();
            problem.fill_candidates(node, &mut candidate_scratch)?;
            sort_candidates(problem, node, &mut candidate_scratch)?;
            seen_keys.clear();
            let mut node_options = Vec::with_capacity(candidate_scratch.len());
            for candidate in candidate_scratch.iter().copied() {
                let key = problem.candidate_key(candidate);
                budget.charge(1)?;
                if fixed_keys.contains(&key) || seen_keys.contains(&key) {
                    continue;
                }
                seen_keys.push(key);
                node_options.push((candidate, key));
            }
            if node_options.is_empty() {
                return Err(budget.unrepresentable());
            }
            options.push(node_options);
        }

        let class = PreparedOptions {
            class: spec.class,
            variables: spec.variables,
            options,
        };
        if !has_perfect_matching(&class)? {
            return Err(budget.unrepresentable());
        }
        admitted.push(class);
    }

    let mut prepared = Vec::with_capacity(admitted.len());
    let mut class_positions = vec![0; classes.len()];
    // Hall failures should reject the whole problem before domain-specific
    // preparation materializes potentially dense proximity partners.
    for class in admitted {
        let local = problem.prepare_class(class.class, &class.variables, budget)?;
        let class = PreparedClass {
            class: class.class,
            variables: class.variables,
            options: class.options,
            local,
        };
        let position = prepared.len();
        *class_positions
            .get_mut(class.class)
            .expect("class IDs are the indexes in Problem::classes") = position;
        prepared.push(class);
    }
    Ok((prepared, class_positions))
}

fn sort_fallible<T>(
    values: &mut [T],
    mut compare: impl FnMut(&T, &T) -> Result<Ordering>,
) -> Result<()> {
    let mut failure = None;
    values.sort_unstable_by(|left, right| {
        if failure.is_some() {
            return Ordering::Equal;
        }
        match compare(left, right) {
            Ok(ordering) => ordering,
            Err(error) => {
                failure = Some(error);
                Ordering::Equal
            },
        }
    });
    failure.map_or(Ok(()), Err)
}

fn sort_candidates<P: Problem>(
    problem: &P,
    node: P::Node,
    candidates: &mut [P::Candidate],
) -> Result<()> {
    for index in 1..candidates.len() {
        let candidate = *candidates
            .get(index)
            .expect("candidate index is within the vector");
        let mut position = index;
        while position > 0 {
            let previous = *candidates
                .get(position.wrapping_sub(1))
                .expect("previous candidate is within the sorted prefix");
            if problem.compare_candidates(node, candidate, previous)? != Ordering::Less {
                break;
            }
            candidates.swap(position, position.wrapping_sub(1));
            position = position.wrapping_sub(1);
        }
    }
    Ok(())
}

fn has_perfect_matching<N, C, K>(class: &PreparedOptions<N, C, K>) -> Result<bool>
where
    K: Copy + Eq + Hash,
{
    let variable_count = class.variables.len();
    let candidate_count = class.options.iter().try_fold(0_usize, |count, options| {
        count
            .checked_add(options.len())
            .ok_or(ResourceError::SizeOverflow)
    })?;
    let mut owners = try_hash_map_with_capacity(candidate_count)?;
    let mut seen_keys = try_hash_set_with_capacity(candidate_count)?;
    let mut seen_variables = Vec::with_capacity(variable_count);
    seen_variables.resize(variable_count, usize::MAX);
    let mut parents = Vec::with_capacity(variable_count);
    parents.resize(variable_count, None::<(usize, K)>);
    let mut stack = Vec::with_capacity(variable_count);

    for root in 0..variable_count {
        seen_keys.clear();
        stack.clear();
        *seen_variables
            .get_mut(root)
            .expect("root variable is within the class") = root;
        *parents
            .get_mut(root)
            .expect("root variable is within the class") = None;
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
                    stack.pop();
                    continue;
                }
                let index = frame.next_candidate;
                frame.next_candidate = frame.next_candidate.wrapping_add(1);
                (frame.variable, index)
            };
            let (_, key) = *class
                .options
                .get(variable)
                .and_then(|options| options.get(candidate_index))
                .expect("matching candidate is within its row");
            if !seen_keys.insert(key) {
                continue;
            }
            match owners.get(&key).copied() {
                None => {
                    free_cell = Some((variable, key));
                    break;
                },
                Some(owner) => {
                    if *seen_variables
                        .get(owner)
                        .expect("matched variable is within the class")
                        == root
                    {
                        continue;
                    }
                    *seen_variables
                        .get_mut(owner)
                        .expect("matched variable is within the class") = root;
                    *parents
                        .get_mut(owner)
                        .expect("matched variable is within the class") = Some((variable, key));
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
            owners.insert(key, variable);
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
    use std::rc::Rc;

    use super::*;
    use crate::error::ErrorKind;

    type Assignment = Vec<(usize, u8)>;
    type AssignmentBlame = Vec<(Assignment, Vec<usize>)>;
    type AssignmentVisitLog = Rc<RefCell<Vec<Assignment>>>;

    struct FixtureProblem {
        classes: Vec<Vec<usize>>,
        candidates: Vec<Vec<u8>>,
        fixed_keys_by_class: Vec<Vec<u8>>,
        rejected: Vec<(usize, usize, u8)>,
        global_requirements: Option<Vec<Assignment>>,
        blamed_classes: Vec<usize>,
        blame_by_assignment: AssignmentBlame,
        initial_evaluations: u64,
        prepare_calls: Rc<Cell<usize>>,
        local_calls: Rc<Cell<usize>>,
        global_calls: Rc<Cell<usize>>,
        global_visits: Option<AssignmentVisitLog>,
    }

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

        fn classes(&self) -> &[Vec<Self::Node>] {
            &self.classes
        }

        fn fixed_keys(&self, class: usize, _variables: &[Self::Node]) -> Result<Vec<Self::Key>> {
            self.fixed_keys_by_class
                .get(class)
                .cloned()
                .ok_or_else(|| GeometryErrorKind::Invalid("fixture class is unknown".into()).into())
        }

        fn compare_nodes(&self, left: Self::Node, right: Self::Node) -> Result<Ordering> {
            Ok(left.cmp(&right))
        }

        fn fill_candidates(
            &self,
            node: Self::Node,
            candidates: &mut Vec<Self::Candidate>,
        ) -> Result<()> {
            let options = self.candidates.get(node).ok_or_else(|| {
                GeometryErrorKind::Invalid("fixture node has no candidate row".into())
            })?;
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

        fn prepare_class(
            &self,
            _class: usize,
            _variables: &[Self::Node],
            _budget: &mut EvaluationBudget,
        ) -> Result<Self::ClassState> {
            self.prepare_calls
                .set(self.prepare_calls.get().wrapping_add(1));
            Ok(())
        }

        fn check_local(
            &self,
            class_state: &Self::ClassState,
            class: usize,
            _variables: &[Self::Node],
            rank: usize,
            assigned: &[Option<Self::Candidate>],
            budget: &mut EvaluationBudget,
        ) -> Result<bool> {
            let () = class_state;
            self.local_calls.set(self.local_calls.get().wrapping_add(1));
            budget.charge(1)?;
            let candidate = assigned
                .get(rank)
                .copied()
                .flatten()
                .expect("search checks only a completed candidate prefix");
            Ok(!self.rejected.contains(&(class, rank, candidate)))
        }

        fn check_global(
            &self,
            assignments: &[ClassAssignment<'_, Self::Node, Self::Candidate>],
            budget: &mut EvaluationBudget,
        ) -> Result<GlobalCheck> {
            debug_assert!(assignments.iter().all(|assignment| {
                self.classes.iter().any(|variables| {
                    variables.len() == assignment.variables.len()
                        && assignment
                            .variables
                            .iter()
                            .all(|variable| variables.contains(variable))
                })
            }));
            budget.charge(1)?;
            self.global_calls
                .set(self.global_calls.get().wrapping_add(1));
            let mut actual = assignments
                .iter()
                .flat_map(|class| {
                    class
                        .variables
                        .iter()
                        .copied()
                        .zip(class.values.iter().copied())
                })
                .collect::<Vec<_>>();
            actual.sort_unstable_by_key(|&(node, _)| node);
            if let Some(visits) = &self.global_visits {
                visits.borrow_mut().push(actual.clone());
            }
            let Some(requirements) = &self.global_requirements else {
                return Ok(GlobalCheck::Pass);
            };
            Ok(
                if requirements.iter().any(|required| {
                    required
                        .iter()
                        .all(|assignment| actual.contains(assignment))
                }) {
                    GlobalCheck::Pass
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
    ) -> Result<SearchAssignment<usize, u8>> {
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
        budget.charge(limit).unwrap();
        let error = budget.charge(1).unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "search_test",
                constructed: 17,
                evaluations
            }) if *evaluations == limit
        ));
    }
}
