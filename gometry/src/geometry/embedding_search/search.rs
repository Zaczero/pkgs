use std::hash::Hash;

use super::account::{EvaluationBudget, EvaluationOnly, SearchAccount};
use super::preparation::{PreparedClass, PreparedClassSet, prepare_classes};
use super::{
    ClassAssignment, GlobalCheck, Problem, SearchAssignment, SearchOutcome, SearchResult, Work,
};
use crate::collections::{HashSet, try_hash_set_with_capacity};
use crate::error::{GeometryErrorKind, ResourceError, Result};
use crate::geometry::segments::ordering::try_sort_by_with_work;

pub(super) struct ClassSearchState<C, K> {
    pub(super) next_candidate: Vec<usize>,
    pub(super) selected: Vec<Option<C>>,
    pub(super) selected_keys: Vec<Option<K>>,
    pub(super) used_keys: HashSet<K>,
    pub(super) yielded: bool,
    pub(super) exhausted: bool,
}

impl<C: Copy, K: Copy + Eq + Hash> ClassSearchState<C, K> {
    pub(super) fn new<N, S, W: Work>(
        class: &PreparedClass<N, C, K, S>,
        work: &mut W,
    ) -> Result<Self> {
        let candidate_count = class.options.iter().try_fold(0_usize, |count, options| {
            count
                .checked_add(options.len())
                .ok_or(ResourceError::SizeOverflow)
        })?;
        work.charge_product_into_error(class.variables.len(), 3)?;
        work.charge_product_into_error(candidate_count, 1)?;
        Ok(Self {
            next_candidate: vec![0; class.variables.len()],
            selected: vec![None; class.variables.len()],
            selected_keys: vec![None; class.variables.len()],
            used_keys: try_hash_set_with_capacity(candidate_count)?,
            yielded: false,
            exhausted: false,
        })
    }

    pub(super) fn reset<W: Work>(&mut self, work: &mut W) -> Result<()> {
        work.charge_product_into_error(self.next_candidate.len(), 3)?;
        work.charge_product_into_error(self.used_keys.capacity(), 1)?;
        work.charge_product_into_error(2, 1)?;
        self.next_candidate.fill(0);
        self.selected.fill(None);
        self.selected_keys.fill(None);
        self.used_keys.clear();
        self.yielded = false;
        self.exhausted = false;
        Ok(())
    }

    pub(super) fn unassign<W: Work>(&mut self, rank: usize, work: &mut W) -> Result<()> {
        let key = self
            .selected_keys
            .get(rank)
            .copied()
            .expect("rank belongs to the class search state");
        work.charge_into_error(2)?;
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

    pub(super) fn next_solution<P, A: SearchAccount>(
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
            account.charge_into_error(1)?;
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
                account.charge_into_error(1)?;
                *self
                    .next_candidate
                    .get_mut(rank)
                    .expect("rank is within the class search state") = 0;
                if rank == 0 {
                    account.charge_into_error(1)?;
                    self.exhausted = true;
                    return Ok(None);
                }
                rank = rank.wrapping_sub(1);
                self.unassign(rank, account)?;
                continue;
            }

            account.charge_evaluations(1)?;
            account.charge_into_error(1)?;
            *self
                .next_candidate
                .get_mut(rank)
                .expect("rank is within the class search state") = candidate_index.wrapping_add(1);
            let (candidate, key) = *options
                .get(candidate_index)
                .expect("candidate ordinal is bounded by its option row");
            account.charge_into_error(3)?;
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
                account.charge_product_into_error(variable_count, 2)?;
                self.yielded = true;
                let mut solution = Vec::with_capacity(variable_count);
                for value in &self.selected {
                    solution
                        .push(value.expect("a complete local solution assigns every class member"));
                }
                return Ok(Some(solution));
            }
            rank = rank.wrapping_add(1);
            account.charge_into_error(1)?;
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
    account.charge_product_into_error(prepared.len(), 2)?;
    let mut states = Vec::with_capacity(prepared.len());
    let mut current_solutions = Vec::with_capacity(prepared.len());
    for class in &prepared {
        let mut state = ClassSearchState::new(class, account)?;
        let Some(solution) = state.next_solution(class, problem, account)? else {
            return Ok(SearchOutcome::Exhausted);
        };
        account.charge_into_error(2)?;
        states.push(state);
        current_solutions.push(solution);
    }
    let mut product_order = None;
    loop {
        let global = {
            account.charge_product_into_error(prepared.len(), 1)?;
            let mut assignments = Vec::with_capacity(prepared.len());
            for (class, values) in prepared.iter().zip(&current_solutions) {
                account.charge_into_error(1)?;
                assignments.push(ClassAssignment {
                    variables: &class.variables,
                    values,
                });
            }
            problem.check_global(&assignments, account)?
        };
        match global {
            GlobalCheck::Pass(output) => {
                account.charge_product_into_error(prepared.len(), 1)?;
                let mut classes = Vec::with_capacity(prepared.len());
                for (class, values) in prepared.into_iter().zip(current_solutions) {
                    account.charge_into_error(1)?;
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
                    account.charge_product_into_error(prepared.len(), 1)?;
                    let mut order = Vec::with_capacity(prepared.len());
                    for class in blamed_classes {
                        account.charge_into_error(1)?;
                        let position = prepared_position_for_blame(&class_positions, class)?;
                        account.charge_into_error(1)?;
                        order.push(position);
                    }
                    try_sort_by_with_work(&mut order, account, |left, right, _work| {
                        Ok(left.cmp(right))
                    })?;
                    let blamed_count = order.len();
                    account.charge_product_into_error(blamed_count, 1)?;
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
                            account.charge_into_error(1)?;
                            order.push(position);
                        }
                    }
                    product_order = Some(order);
                } else {
                    for class in blamed_classes {
                        account.charge_into_error(1)?;
                        prepared_position_for_blame(&class_positions, class)?;
                    }
                }

                let order = product_order
                    .as_ref()
                    .expect("a nonempty rejection establishes the product order");
                let mut advanced = false;
                for &position in order {
                    account.charge_into_error(1)?;
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
        work.charge_into_error(1)?;
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
    work.charge_into_error(1)?;
    *current_solutions
        .get_mut(position)
        .expect("class rank is within current solutions") = solution;
    Ok(())
}
