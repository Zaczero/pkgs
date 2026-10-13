//! Deterministic search over finite embedding domains.

use std::cmp::Ordering;
use std::convert::Infallible;
use std::hash::Hash;

use self::account::SearchAccount;
#[cfg(test)]
use crate::error::ResourceError;
use crate::error::Result;

pub(in crate::geometry) mod account;
pub(in crate::geometry) mod matching;
pub(in crate::geometry) mod preparation;
pub(in crate::geometry) mod proximity;
pub(in crate::geometry) mod search;
/// Raw primitive work. Evaluation events remain a separate unit on the same
/// account; one evaluation consumes sixteen raw work ticks.
pub(crate) trait Work {
    /// The policy's failure type; ordinary policies use `Infallible`.
    type Error: Into<crate::error::Error>;

    fn charge(&mut self, ticks: u64) -> std::result::Result<(), Self::Error>;

    fn charge_product(
        &mut self,
        count: usize,
        ticks_per_item: usize,
    ) -> std::result::Result<(), Self::Error>;

    fn charge_into_error(&mut self, ticks: u64) -> Result<()> {
        self.charge(ticks).map_err(Into::into)
    }

    fn charge_product_into_error(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
        self.charge_product(count, ticks_per_item)
            .map_err(Into::into)
    }
}

pub(crate) type WorkResult<W, T> = std::result::Result<T, <W as Work>::Error>;

#[cfg(test)]
pub(crate) fn work_product_ticks(count: usize, ticks_per_item: usize) -> Result<u64> {
    let ticks = count
        .checked_mul(ticks_per_item)
        .ok_or(ResourceError::SizeOverflow)?;
    u64::try_from(ticks).map_err(|_| ResourceError::SizeOverflow.into())
}

#[derive(Clone, Copy, Default)]
pub(crate) struct Unmetered;

impl Work for Unmetered {
    type Error = Infallible;

    fn charge(&mut self, _ticks: u64) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    fn charge_product(
        &mut self,
        _count: usize,
        _ticks_per_item: usize,
    ) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
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

    fn compare_nodes<W: Work>(
        &self,
        left: Self::Node,
        right: Self::Node,
        work: &mut W,
    ) -> Result<Ordering>;

    /// Append domain-admitted candidates for `node`; their input order is ignored.
    fn fill_candidates<W: Work>(
        &self,
        node: Self::Node,
        candidates: &mut Vec<Self::Candidate>,
        work: &mut W,
    ) -> Result<()>;

    /// Exact displacement order, followed by the domain's coordinate tie-break.
    fn compare_candidates<W: Work>(
        &self,
        node: Self::Node,
        left: Self::Candidate,
        right: Self::Candidate,
        work: &mut W,
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

#[cfg(test)]
mod tests;
