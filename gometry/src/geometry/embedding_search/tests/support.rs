use std::cell::{Cell, RefCell};
use std::convert::Infallible;
use std::ops::ControlFlow;
use std::rc::Rc;

use super::account::{
    BASE_EVALUATION_LIMIT, EVALUATIONS_PER_CONSTRUCTED_NODE, EvaluationBudget, EvaluationOnly,
    WORK_TICKS_PER_EVALUATION,
};
use super::matching::has_perfect_matching;
use super::preparation::{PreparedClass, PreparedOptions};
use super::proximity::{
    ProximityChord, build_proximity_classes, build_proximity_classes_with_work,
};
use super::search::{ClassSearchState, Search, replace_class_solution, run_with_budget};
use super::*;
use crate::collections::try_hash_set_with_capacity;
use crate::error::{ErrorKind, ResourceError};
use crate::geometry::segment_index::sweep::{
    for_each_candidate_pair_with_work as sweep_for_each_candidate_pair_with_work,
    for_each_overlapping_bounds_cross_pair_with_work as sweep_for_each_overlapping_bounds_cross_pair_with_work,
    for_each_overlapping_bounds_pair_with_work as sweep_for_each_overlapping_bounds_pair_with_work,
};
use crate::geometry::segments::ordering::try_sort_by_with_work;
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational, round_ratio_with_work};
use crate::geometry::{Bounds, GeometryErrorKind, Segment, XY};
use crate::work::{Unmetered, Work};

type Assignment = Vec<(usize, u8)>;
type AssignmentBlame = Vec<(Assignment, Vec<usize>)>;
type AssignmentVisitLog = Rc<RefCell<Vec<Assignment>>>;

#[derive(Clone, Copy)]
enum NestedSearch {
    None,
    HallOn(u8),
    HallAlways,
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
    comparison_phase: Option<Rc<Cell<u8>>>,
    nested_completion: bool,
    nested_hall_calls: Rc<Cell<usize>>,
    nested_pass_calls: Rc<Cell<usize>>,
    nested_before_ticks: Option<u128>,
    nested_completion_ticks: u128,
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
            comparison_phase: None,
            nested_completion: false,
            nested_hall_calls: Rc::new(Cell::new(0)),
            nested_pass_calls: Rc::new(Cell::new(0)),
            nested_before_ticks: None,
            nested_completion_ticks: 0,
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

    fn compare_nodes<W: Work>(
        &self,
        left: Self::Node,
        right: Self::Node,
        work: &mut W,
    ) -> Result<Ordering> {
        if let Some(phase) = &self.comparison_phase {
            phase.set(1);
            let result = work.charge_into_error(37);
            phase.set(0);
            result?;
        }
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
        work.charge_product_into_error(options.len(), 1)?;
        candidates.reserve(options.len());
        candidates.extend(options.iter().copied());
        Ok(())
    }

    fn compare_candidates<W: Work>(
        &self,
        _node: Self::Node,
        left: Self::Candidate,
        right: Self::Candidate,
        work: &mut W,
    ) -> Result<Ordering> {
        if let Some(phase) = &self.comparison_phase {
            phase.set(2);
            let result = work.charge_into_error(37);
            phase.set(0);
            result?;
        }
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
        account.charge_product_into_error(actual_count, 1)?;
        let mut actual = Vec::with_capacity(actual_count);
        for assignment in assignments {
            for (&node, &value) in assignment.variables.iter().zip(assignment.values) {
                account.charge_into_error(1)?;
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
        if self.nested_completion {
            let (node, candidate) = assignments
                .iter()
                .find_map(|assignment| {
                    assignment
                        .variables
                        .first()
                        .zip(assignment.values.first())
                        .map(|(&node, &candidate)| (node, candidate))
                })
                .expect("nested fixture has one assigned outer variable");
            let before = account.budget_mut().spent_ticks;
            if candidate == 0 {
                self.nested_hall_calls
                    .set(self.nested_hall_calls.get().wrapping_add(1));
                let mut completion = hall_completion_problem();
                let outcome = run_with_budget(&mut completion, account.budget_mut())?;
                let after = account.budget_mut().spent_ticks;
                if self.nested_before_ticks.is_none() {
                    self.nested_before_ticks = Some(before);
                    self.nested_completion_ticks = after.saturating_sub(before);
                }
                return match outcome {
                    SearchOutcome::Exhausted => Ok(GlobalCheck::Reject(vec![0])),
                    SearchOutcome::Pass(_) => Err(GeometryErrorKind::Invalid(
                        "nested Hall completion unexpectedly passed".into(),
                    )
                    .into()),
                };
            }
            if candidate == 1 {
                self.nested_pass_calls
                    .set(self.nested_pass_calls.get().wrapping_add(1));
                let mut completion = passing_completion_problem();
                let outcome = run_with_budget(&mut completion, account.budget_mut())?;
                let after = account.budget_mut().spent_ticks;
                if self.nested_before_ticks.is_none() {
                    self.nested_before_ticks = Some(before);
                    self.nested_completion_ticks = after.saturating_sub(before);
                }
                return match outcome {
                    SearchOutcome::Pass(assignment) => {
                        if assignment.output.0 != [(0, 7)] {
                            return Err(GeometryErrorKind::Invalid(
                                "nested completion returned a different output".into(),
                            )
                            .into());
                        }
                        Ok(GlobalCheck::Pass(FixtureOutput(actual)))
                    },
                    SearchOutcome::Exhausted => Ok(GlobalCheck::Reject(vec![0])),
                };
            }
            return Err(GeometryErrorKind::Invalid(
                format!("nested fixture has no completion for node {node} candidate {candidate}")
                    .into(),
            )
            .into());
        }
        let run_nested = match self.nested_search {
            NestedSearch::None => false,
            NestedSearch::HallOn(candidate) => actual.iter().any(|&(_, value)| value == candidate),
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
                    .find_map(|(assignment, classes)| (assignment == &actual).then_some(classes))
                    .unwrap_or(&self.blamed_classes);
                GlobalCheck::Reject(blamed.clone())
            },
        )
    }

    fn initial_evaluations(&self) -> u64 {
        self.initial_evaluations
    }
}

struct ComparisonWorkProbe {
    budget: EvaluationBudget,
    phase: Rc<Cell<u8>>,
    comparator_ticks: Cell<[u64; 3]>,
}

impl Work for ComparisonWorkProbe {
    type Error = crate::error::Error;

    fn charge(&mut self, ticks: u64) -> Result<()> {
        self.budget.charge_into_error(ticks)?;
        let phase = usize::from(self.phase.get());
        if phase != 0 {
            let mut comparator_ticks = self.comparator_ticks.get();
            comparator_ticks[phase] = comparator_ticks[phase]
                .checked_add(ticks)
                .ok_or(ResourceError::SizeOverflow)?;
            self.comparator_ticks.set(comparator_ticks);
        }
        Ok(())
    }

    fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
        self.budget
            .charge_product_into_error(count, ticks_per_item)?;
        let phase = usize::from(self.phase.get());
        if phase != 0 {
            let ticks = u64::try_from(count)
                .map_err(|_| ResourceError::SizeOverflow)?
                .checked_mul(
                    u64::try_from(ticks_per_item).map_err(|_| ResourceError::SizeOverflow)?,
                )
                .ok_or(ResourceError::SizeOverflow)?;
            let mut comparator_ticks = self.comparator_ticks.get();
            comparator_ticks[phase] = comparator_ticks[phase]
                .checked_add(ticks)
                .ok_or(ResourceError::SizeOverflow)?;
            self.comparator_ticks.set(comparator_ticks);
        }
        Ok(())
    }
}

impl SearchAccount for ComparisonWorkProbe {
    fn budget_mut(&mut self) -> &mut EvaluationBudget {
        &mut self.budget
    }
}

fn search_fixture(
    problem: FixtureProblem,
    constructed: usize,
) -> Result<SearchAssignment<usize, u8, FixtureOutput>> {
    Search::new(problem, "search_test", constructed)?.run()
}

fn hall_completion_problem() -> FixtureProblem {
    FixtureProblem::new(vec![vec![0, 1, 2]], vec![vec![7], vec![7], vec![7]])
}

fn passing_completion_problem() -> FixtureProblem {
    FixtureProblem::new(vec![vec![0]], vec![vec![7]])
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
    let metric_access = || Ok(metric);
    crate::geometry::computed_native_sample_ordinates(
        frame,
        source,
        longitude,
        parameter,
        axes,
        Some(&metric_access),
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
    fn record_success(&mut self, spent_before: u128, progress: crate::crs::QuadratureTestProgress) {
        let ticks = self.budget.spent_ticks.saturating_sub(spent_before);
        self.successful.push(SuccessfulRawCharge {
            ticks,
            spent_before,
            progress,
        });
    }
}

impl Work for RecordingWork<'_> {
    type Error = crate::error::Error;

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
    let mut budget = EvaluationBudget::new(operation, 1).expect("the test budget is representable");
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
