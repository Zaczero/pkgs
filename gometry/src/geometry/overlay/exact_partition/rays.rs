//! Leftward ray contacts over exact points.
//!
//! A cyclic walk needs to know which span lies immediately left of a
//! component's lexicographic minimum. [`RayPoints`] abstracts over the two
//! point sources that question is asked of -- the partition's exact arena
//! during construction, and the rounded items at emission -- so the sweep
//! itself is written once.

use std::cmp::Ordering;
#[cfg(test)]
use std::{cell::Cell, thread_local};

use super::construction::PointArena;
use crate::geometry::graph::{RayAxis as Axis, RayPoints, RayPointsWithWork};
use crate::geometry::numerics::product::ExactSign;
use crate::geometry::segments::stored_orientation_sign;
use crate::geometry::tessellation::exact::{Interval, compare_ratios_with_work};
use crate::geometry::{Result, XY};
use crate::work::{Unmetered, Work};

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct RayWorkProgress {
    axis_attempted: usize,
    axis_completed: usize,
    orientation_attempted: usize,
    orientation_completed: usize,
}

#[cfg(test)]
thread_local! {
    static RAY_WORK_PROGRESS: Cell<RayWorkProgress> =
        const { Cell::new(RayWorkProgress {
            axis_attempted: 0,
            axis_completed: 0,
            orientation_attempted: 0,
            orientation_completed: 0,
        }) };
}

#[cfg(test)]
fn reset_ray_work_progress() {
    RAY_WORK_PROGRESS.with(|progress| progress.set(RayWorkProgress::default()));
}

#[cfg(test)]
fn ray_work_progress() -> RayWorkProgress {
    RAY_WORK_PROGRESS.with(Cell::get)
}

#[cfg(test)]
fn record_axis_attempt() {
    RAY_WORK_PROGRESS.with(|progress| {
        let mut value = progress.get();
        value.axis_attempted = value.axis_attempted.wrapping_add(1);
        progress.set(value);
    });
}

#[cfg(test)]
fn record_axis_complete() {
    RAY_WORK_PROGRESS.with(|progress| {
        let mut value = progress.get();
        value.axis_completed = value.axis_completed.wrapping_add(1);
        progress.set(value);
    });
}

#[cfg(test)]
fn record_orientation_attempt() {
    RAY_WORK_PROGRESS.with(|progress| {
        let mut value = progress.get();
        value.orientation_attempted = value.orientation_attempted.wrapping_add(1);
        progress.set(value);
    });
}

#[cfg(test)]
fn record_orientation_complete() {
    RAY_WORK_PROGRESS.with(|progress| {
        let mut value = progress.get();
        value.orientation_completed = value.orientation_completed.wrapping_add(1);
        progress.set(value);
    });
}

impl RayPoints for PointArena<'_> {
    #[expect(
        clippy::indexing_slicing,
        reason = "the point ID was admitted by PointArena before it entered the sweep"
    )]
    fn rounded(&self, index: u32) -> XY {
        self.rounded[index as usize]
    }

    fn compare_axis(&self, left: u32, right: u32, axis: Axis) -> Result<Ordering> {
        self.compare_axis_with_work(left, right, axis, &mut Unmetered)
    }

    fn orientation(&self, a: u32, b: u32, c: u32) -> Result<ExactSign> {
        self.orientation_with_work(a, b, c, &mut Unmetered)
    }
}

impl RayPointsWithWork for PointArena<'_> {
    /// Order two points along one axis. Nearest-even rounding is monotone, so
    /// rounded coordinates that differ order the exact points the same way;
    /// only a tie that is not between two stored values reads exact points.
    #[expect(
        clippy::indexing_slicing,
        reason = "sweep endpoints and queries are IDs from this PointArena"
    )]
    fn compare_axis_with_work<W: Work>(
        &self,
        left: u32,
        right: u32,
        axis: Axis,
        work: &mut W,
    ) -> Result<Ordering> {
        #[cfg(test)]
        record_axis_attempt();
        let (l, r) = (
            axis.of(self.rounded[left as usize]),
            axis.of(self.rounded[right as usize]),
        );
        if l < r {
            #[cfg(test)]
            record_axis_complete();
            return Ok(Ordering::Less);
        }
        if l > r {
            #[cfg(test)]
            record_axis_complete();
            return Ok(Ordering::Greater);
        }
        if left == right || (self.is_stored(left) && self.is_stored(right)) {
            #[cfg(test)]
            record_axis_complete();
            return Ok(Ordering::Equal);
        }
        let (left, right) = (
            self.exact_with_work(left, work)?,
            self.exact_with_work(right, work)?,
        );
        let index = axis.index();
        let order =
            compare_ratios_with_work(&left[index], &left[2], &right[index], &right[2], work)?;
        #[cfg(test)]
        record_axis_complete();
        Ok(order)
    }

    /// The orientation of `c` against the directed line through `a` and `b`:
    /// the stored predicate when all three are stored, otherwise an interval
    /// filter over the certified bounds and the exact homogeneous determinant.
    #[expect(
        clippy::indexing_slicing,
        reason = "sweep endpoints and queries are IDs from this PointArena"
    )]
    fn orientation_with_work<W: Work>(
        &self,
        a: u32,
        b: u32,
        c: u32,
        work: &mut W,
    ) -> Result<ExactSign> {
        #[cfg(test)]
        record_orientation_attempt();
        if self.is_stored(a) && self.is_stored(b) && self.is_stored(c) {
            let [a, b, c] = [a, b, c].map(|index| self.rounded[index as usize]);
            #[cfg(test)]
            record_orientation_complete();
            return Ok(stored_orientation_sign(a.x, a.y, b.x, b.y, c.x, c.y));
        }
        let interval = |index: u32, axis: Axis| {
            let (lo, hi) = self.axis_bounds(index, axis);
            Interval { lo, hi }
        };
        let (ax, ay) = (interval(a, Axis::X), interval(a, Axis::Y));
        let determinant = interval(b, Axis::X)
            .sub(ax)
            .mul(interval(c, Axis::Y).sub(ay))
            .sub(
                interval(b, Axis::Y)
                    .sub(ay)
                    .mul(interval(c, Axis::X).sub(ax)),
            );
        if determinant.is_finite()
            && let Some(sign) = determinant.sign()
        {
            #[cfg(test)]
            record_orientation_complete();
            return Ok(sign);
        }
        let a = self.exact_with_work(a, work)?;
        let b = self.exact_with_work(b, work)?;
        let c = self.exact_with_work(c, work)?;
        let [ax, ay, aw] = &*a;
        let [bx, by, bw] = &*b;
        let [cx, cy, cw] = &*c;
        let dx_left = bx.try_product_with_work(aw, work)?;
        let dx_right = ax.try_product_with_work(bw, work)?;
        let dx = dx_left.try_subtract_with_work(dx_right, work)?;
        let dy_left = by.try_product_with_work(aw, work)?;
        let dy_right = ay.try_product_with_work(bw, work)?;
        let dy = dy_left.try_subtract_with_work(dy_right, work)?;
        let cx_left = cx.try_product_with_work(aw, work)?;
        let cx_right = ax.try_product_with_work(cw, work)?;
        let cx_rel = cx_left.try_subtract_with_work(cx_right, work)?;
        let cy_left = cy.try_product_with_work(aw, work)?;
        let cy_right = ay.try_product_with_work(cw, work)?;
        let cy_rel = cy_left.try_subtract_with_work(cy_right, work)?;
        let determinant_left = dx.try_product_with_work(&cy_rel, work)?;
        let determinant_right = dy.try_product_with_work(&cx_rel, work)?;
        let sign = determinant_left
            .try_subtract_with_work(determinant_right, work)?
            .sign();
        #[cfg(test)]
        record_orientation_complete();
        // The determinant carries aw^2 bw cw, and aw^2 is positive.
        let flipped = (bw.sign() == ExactSign::Negative) != (cw.sign() == ExactSign::Negative);
        Ok(match (flipped, sign) {
            (true, ExactSign::Negative) => ExactSign::Positive,
            (true, ExactSign::Positive) => ExactSign::Negative,
            (_, sign) => sign,
        })
    }
}

/// Emitted node coordinates, exact as the stored doubles they are.
pub(super) struct EmittedPoints<'p>(pub(super) &'p [XY]);

impl RayPoints for EmittedPoints<'_> {
    #[expect(
        clippy::indexing_slicing,
        reason = "emitted node IDs index the coordinate slice produced for the same partition"
    )]
    fn rounded(&self, index: u32) -> XY {
        self.0[index as usize]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "emitted node IDs index the coordinate slice produced for the same partition"
    )]
    fn compare_axis(&self, left: u32, right: u32, axis: Axis) -> Result<Ordering> {
        let (left, right) = (
            axis.of(self.0[left as usize]),
            axis.of(self.0[right as usize]),
        );
        Ok(if left < right {
            Ordering::Less
        } else if left > right {
            Ordering::Greater
        } else {
            Ordering::Equal
        })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "emitted node IDs index the coordinate slice produced for the same partition"
    )]
    fn orientation(&self, a: u32, b: u32, c: u32) -> Result<ExactSign> {
        let [a, b, c] = [a, b, c].map(|index| self.0[index as usize]);
        Ok(stored_orientation_sign(a.x, a.y, b.x, b.y, c.x, c.y))
    }
}

impl RayPointsWithWork for EmittedPoints<'_> {
    fn compare_axis_with_work<W: Work>(
        &self,
        left: u32,
        right: u32,
        axis: Axis,
        _work: &mut W,
    ) -> Result<Ordering> {
        self.compare_axis(left, right, axis)
    }

    fn orientation_with_work<W: Work>(
        &self,
        a: u32,
        b: u32,
        c: u32,
        _work: &mut W,
    ) -> Result<ExactSign> {
        self.orientation(a, b, c)
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::super::NodeRecipe;
    use super::{PointArena, RayWorkProgress, ray_work_progress, reset_ray_work_progress};
    use crate::error::{ErrorKind, Result};
    use crate::geometry::XY;
    use crate::geometry::embedding_search::account::{EvaluationBudget, EvaluationOnly};
    use crate::geometry::graph::{
        RayAxis as Axis, RayPointsWithWork, compare_departures_with_work,
    };
    use crate::geometry::tessellation::exact::ExactDyadic;
    use crate::work::{Unmetered, Work};

    #[derive(Clone, Copy)]
    struct ChargeEvent {
        spent_before: u128,
        ticks: u128,
        progress: RayWorkProgress,
    }

    struct RecordingWork<'a> {
        budget: &'a mut EvaluationBudget,
        spent: u128,
        events: Vec<ChargeEvent>,
        refused: Option<ChargeEvent>,
    }

    impl<'a> RecordingWork<'a> {
        fn new(budget: &'a mut EvaluationBudget) -> Self {
            Self {
                budget,
                spent: 0,
                events: Vec::new(),
                refused: None,
            }
        }

        fn record(&mut self, ticks: u128, result: &Result<()>) {
            let event = ChargeEvent {
                spent_before: self.spent,
                ticks,
                progress: ray_work_progress(),
            };
            if result.is_ok() {
                self.events.push(event);
                self.spent = self.spent.wrapping_add(ticks);
            } else {
                self.refused = Some(event);
            }
        }
    }

    impl Work for RecordingWork<'_> {
        type Error = crate::error::Error;

        fn charge(&mut self, ticks: u64) -> Result<()> {
            let result = <EvaluationBudget as Work>::charge(self.budget, ticks);
            self.record(u128::from(ticks), &result);
            result
        }

        fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
            let ticks = (count as u128).wrapping_mul(ticks_per_item as u128);
            let result =
                <EvaluationBudget as Work>::charge_product(self.budget, count, ticks_per_item);
            self.record(ticks, &result);
            result
        }
    }

    fn budget_refusing_before(operation: &'static str, event: ChargeEvent) -> EvaluationBudget {
        let remaining = event
            .spent_before
            .checked_add(event.ticks)
            .and_then(|ticks| ticks.checked_sub(1))
            .expect("the target ray charge is positive and fits the budget");
        EvaluationBudget::with_remaining_ticks_for_test(operation, 1, remaining)
            .expect("calibrated ray budget is representable")
    }

    fn assert_limit_error(error: &crate::error::Error, operation: &'static str) {
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(crate::geometry::GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: actual,
                ..
            }) if *actual == operation
        ));
    }

    fn add_f64(left: f64, right: f64) -> ExactDyadic {
        ExactDyadic::from_f64(left)
            .try_add_with_work(ExactDyadic::from_f64(right), &mut Unmetered)
            .expect("test dyadics are representable")
    }

    fn push_point(points: &mut PointArena<'_>, rounded: XY, x: ExactDyadic, y: ExactDyadic) -> u32 {
        points
            .push_constructed(rounded, [x, y, ExactDyadic::one()])
            .expect("test point ID is representable")
    }

    fn crossing_is_materialized(points: &PointArena<'_>, node: u32) -> bool {
        matches!(
            points.recipes.get(node as usize),
            Some(NodeRecipe::Crossing { exact, .. }) if exact.get().is_some()
        )
    }

    fn tied_axis_points() -> PointArena<'static> {
        let segments: &'static [_] = &[];
        let mut points = PointArena::with_capacity(segments, 2);
        let one = ExactDyadic::one();
        let x_next = add_f64(1.0, 2.0_f64.powi(-256));
        push_point(&mut points, XY::new(1.0, 0.0), one, ExactDyadic::zero());
        push_point(&mut points, XY::new(1.0, 0.0), x_next, ExactDyadic::zero());
        points
    }

    fn tied_orientation_points() -> PointArena<'static> {
        let segments: &'static [_] = &[];
        let mut points = PointArena::with_capacity(segments, 3);
        let rounded = XY::new(1.0, 1.0);
        let y = add_f64(1.0, 2.0_f64.powi(-256));
        push_point(&mut points, rounded, ExactDyadic::one(), ExactDyadic::one());
        push_point(
            &mut points,
            rounded,
            add_f64(1.0, 2.0_f64.powi(-256)),
            y.clone(),
        );
        push_point(&mut points, rounded, add_f64(1.0, 2.0_f64.powi(-255)), y);
        points
    }

    fn sort_tied_axis<W: Work>(
        points: &PointArena<'_>,
        items: &mut [u32],
        work: &mut W,
    ) -> Result<()> {
        RayPointsWithWork::sort_by_axis_with_work(
            points,
            items,
            Axis::X,
            |index| index,
            |_, _| Ok(()),
            work,
        )
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "the recorder assertions pin refusal before exact tied-axis ordering"
    )]
    fn native_ray_tied_axis_refuses_before_exact_ordering() -> Result<()> {
        const OPERATION: &str = "native_ray_axis";
        let points = tied_axis_points();
        let mut items = [1_u32, 0_u32];
        reset_ray_work_progress();
        let (ordered, charges) = {
            let mut budget = EvaluationBudget::new(OPERATION, 1)?;
            let mut work = RecordingWork::new(&mut budget);
            sort_tied_axis(&points, &mut items, &mut work)?;
            (items, work.events)
        };
        assert_eq!(ordered, [0, 1]);
        let target = charges
            .into_iter()
            .find(|event| {
                event.progress.axis_attempted == 1
                    && event.progress.axis_completed == 0
                    && event.ticks > 0
            })
            .expect("tied-axis ordering performs metered exact comparison work");

        reset_ray_work_progress();
        let mut items = [1_u32, 0_u32];
        let mut budget = budget_refusing_before(OPERATION, target);
        let error = {
            let mut work = RecordingWork::new(&mut budget);
            let error = sort_tied_axis(&points, &mut items, &mut work)
                .expect_err("budget refusal precedes exact tied-axis comparison");
            assert_eq!(
                work.refused.map(|event| event.progress),
                Some(target.progress)
            );
            error
        };
        assert_limit_error(&error, OPERATION);
        assert_eq!(
            items,
            [1, 0],
            "failed exact sort did not publish a permutation"
        );
        assert_eq!(ray_work_progress(), target.progress);

        reset_ray_work_progress();
        let mut unmetered_items = [1_u32, 0_u32];
        sort_tied_axis(&points, &mut unmetered_items, &mut Unmetered)?;
        assert_eq!(unmetered_items, ordered);

        let mut ordinary_budget = EvaluationBudget::new(OPERATION, 1)?;
        let ordinary_spend = ordinary_budget.spent_ticks_for_test();
        let mut evaluation_items = [1_u32, 0_u32];
        {
            let mut work = EvaluationOnly::new(&mut ordinary_budget);
            sort_tied_axis(&points, &mut evaluation_items, &mut work)?;
        }
        assert_eq!(evaluation_items, ordered);
        assert_eq!(
            ordinary_budget.spent_ticks_for_test(),
            ordinary_spend,
            "EvaluationOnly erases ray arithmetic and sorter hooks"
        );
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "the recorder assertions pin refusal before lazy crossing cache publication"
    )]
    fn native_point_arena_lazy_crossing_refuses_before_cache_publication() -> Result<()> {
        const OPERATION: &str = "native_ray_lazy_crossing";
        let segments = [
            crate::geometry::Segment {
                start: XY::new(0.0, 0.0),
                end: XY::new(2.0, 2.0),
            },
            crate::geometry::Segment {
                start: XY::new(0.0, 2.0),
                end: XY::new(2.0, 0.0),
            },
        ];
        let make_points = || {
            let mut points = PointArena::with_capacity(&segments, 2);
            let crossing = points
                .push_crossing([0, 1])
                .expect("crossing node ID is representable");
            let query = push_point(
                &mut points,
                XY::new(1.0, 1.0),
                add_f64(1.0, 2.0_f64.powi(-256)),
                ExactDyadic::one(),
            );
            (points, crossing, query)
        };
        let (points, crossing, query) = make_points();
        assert!(!crossing_is_materialized(&points, crossing));
        reset_ray_work_progress();
        let (order, charges) = {
            let mut budget = EvaluationBudget::new(OPERATION, 1)?;
            let mut work = RecordingWork::new(&mut budget);
            let order = points.compare_axis_with_work(crossing, query, Axis::X, &mut work)?;
            (order, work.events)
        };
        assert_eq!(order, Ordering::Less);
        assert!(crossing_is_materialized(&points, crossing));
        let crossing_charges: Vec<_> = charges
            .into_iter()
            .filter(|event| {
                event.progress.axis_attempted == 1 && event.progress.axis_completed == 0
            })
            .collect();
        let target = *crossing_charges
            .get(1)
            .expect("lazy exact crossing construction has its own Work charge");

        let (points, crossing, query) = make_points();
        assert!(!crossing_is_materialized(&points, crossing));
        reset_ray_work_progress();
        let mut budget = budget_refusing_before(OPERATION, target);
        let error = {
            let mut work = RecordingWork::new(&mut budget);
            let error = points
                .compare_axis_with_work(crossing, query, Axis::X, &mut work)
                .expect_err("crossing construction refuses before publishing the cache");
            assert_eq!(
                work.refused.map(|event| event.progress),
                Some(target.progress)
            );
            error
        };
        assert_limit_error(&error, OPERATION);
        assert_eq!(ray_work_progress(), target.progress);
        assert!(
            !crossing_is_materialized(&points, crossing),
            "failed lazy exact construction leaves the OnceCell empty"
        );

        let (points, crossing, query) = make_points();
        let mut unmetered = Unmetered;
        assert_eq!(
            points.compare_axis_with_work(crossing, query, Axis::X, &mut unmetered)?,
            order
        );
        let (points, crossing, query) = make_points();
        let mut ordinary_budget = EvaluationBudget::new(OPERATION, 1)?;
        let ordinary_spend = ordinary_budget.spent_ticks_for_test();
        {
            let mut work = EvaluationOnly::new(&mut ordinary_budget);
            assert_eq!(
                points.compare_axis_with_work(crossing, query, Axis::X, &mut work)?,
                order
            );
        }
        assert_eq!(ordinary_budget.spent_ticks_for_test(), ordinary_spend);
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "the recorder assertions pin refusal before exact tied-orientation evaluation"
    )]
    fn native_ray_tied_nonzero_orientation_refuses_before_determinant() -> Result<()> {
        const OPERATION: &str = "native_ray_orientation";
        let points = tied_orientation_points();
        reset_ray_work_progress();
        let (order, charges) = {
            let mut budget = EvaluationBudget::new(OPERATION, 1)?;
            let mut work = RecordingWork::new(&mut budget);
            let order = compare_departures_with_work(&points, 0, 1, 2, &mut work)?;
            (order, work.events)
        };
        assert_eq!(order, Ordering::Greater);
        let target = charges
            .into_iter()
            .find(|event| {
                event.progress.orientation_attempted == 1
                    && event.progress.orientation_completed == 0
                    && event.ticks > 0
            })
            .expect("same-rounded nonzero rays reach metered exact orientation");

        reset_ray_work_progress();
        let mut budget = budget_refusing_before(OPERATION, target);
        let error = {
            let mut work = RecordingWork::new(&mut budget);
            let error = compare_departures_with_work(&points, 0, 1, 2, &mut work)
                .expect_err("budget refusal precedes exact determinant completion");
            assert_eq!(
                work.refused.map(|event| event.progress),
                Some(target.progress)
            );
            error
        };
        assert_limit_error(&error, OPERATION);
        let progress = ray_work_progress();
        assert_eq!(progress.orientation_attempted, 1);
        assert_eq!(progress.orientation_completed, 0);
        assert_eq!(progress.axis_attempted, 4);
        assert_eq!(progress.axis_completed, 4);

        let mut unmetered = Unmetered;
        assert_eq!(
            compare_departures_with_work(&points, 0, 1, 2, &mut unmetered)?,
            order
        );
        let mut ordinary_budget = EvaluationBudget::new(OPERATION, 1)?;
        let ordinary_spend = ordinary_budget.spent_ticks_for_test();
        {
            let mut evaluation_only = EvaluationOnly::new(&mut ordinary_budget);
            assert_eq!(
                compare_departures_with_work(&points, 0, 1, 2, &mut evaluation_only)?,
                order
            );
        }
        assert_eq!(ordinary_budget.spent_ticks_for_test(), ordinary_spend);
        Ok(())
    }
}
