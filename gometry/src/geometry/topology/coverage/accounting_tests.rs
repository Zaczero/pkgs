use std::convert::Infallible;

use super::analyze_geographic_coverage_with_work;
use crate::error::Result;
use crate::geographic::GeographicFrame;
use crate::geometry::coverage::CoverageOverlapRule;
use crate::geometry::embedding_search::account::{EvaluationBudget, EvaluationOnly, SearchAccount};
use crate::geometry::publication::StorageTarget;
use crate::geometry::{Shape, SourceArcContext, TopologyDomain};
use crate::work::Work;

struct RecordingPreparation<'a> {
    inner: EvaluationOnly<'a>,
    primitive_ticks: u128,
    evaluations: u64,
}

impl<'a> RecordingPreparation<'a> {
    fn new(budget: &'a mut EvaluationBudget) -> Self {
        Self {
            inner: EvaluationOnly::new(budget),
            primitive_ticks: 0,
            evaluations: 0,
        }
    }
}

impl Work for RecordingPreparation<'_> {
    type Error = Infallible;

    fn charge(&mut self, ticks: u64) -> std::result::Result<(), Self::Error> {
        self.primitive_ticks = self
            .primitive_ticks
            .checked_add(u128::from(ticks))
            .expect("fixture primitive work fits");
        self.inner.charge(ticks)
    }

    fn charge_product(
        &mut self,
        count: usize,
        ticks_per_item: usize,
    ) -> std::result::Result<(), Self::Error> {
        self.primitive_ticks = self
            .primitive_ticks
            .checked_add((count as u128).wrapping_mul(ticks_per_item as u128))
            .expect("fixture primitive work fits");
        self.inner.charge_product(count, ticks_per_item)
    }
}

impl SearchAccount for RecordingPreparation<'_> {
    fn budget_mut(&mut self) -> &mut EvaluationBudget {
        self.inner.budget_mut()
    }

    fn charge_evaluations(&mut self, evaluations: u64) -> Result<()> {
        self.evaluations = self
            .evaluations
            .checked_add(evaluations)
            .expect("fixture evaluations fit");
        self.inner.charge_evaluations(evaluations)
    }
}

fn overlapping_rows() -> [Shape; 2] {
    [
        crate::io::parse_wkt("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))")
            .expect("first coverage polygon"),
        crate::io::parse_wkt("POLYGON ((1 0, 3 0, 3 2, 1 2, 1 0))")
            .expect("overlapping coverage polygon"),
    ]
}

#[test]
fn ordinary_coverage_preparation_preserves_the_evaluation_balance() {
    let rows = overlapping_rows();
    let mut budget = EvaluationBudget::new("coverage_clean", 0).expect("coverage budget");
    let mut work = RecordingPreparation::new(&mut budget);
    work.charge_evaluations(1).expect("first evaluation");
    let before = work.budget_mut().spent_ticks_for_test();
    assert!(before > 0);
    let analysis = analyze_geographic_coverage_with_work(
        &rows,
        GeographicFrame::crs84(),
        0.0,
        "coverage_clean",
        &mut work,
    )
    .expect("ordinary source analysis");
    assert!(!analysis.is_valid());
    analysis
        .assign_cleaning_faces(
            Some(&rows.each_ref().map(Shape::area)),
            0.0,
            CoverageOverlapRule::MinIndex,
            None::<&crate::crs::GeodesicMetric>,
            &mut work,
        )
        .expect("ordinary face selection");
    assert!(work.primitive_ticks > 0);
    assert_eq!(work.budget_mut().spent_ticks_for_test(), before);
    work.charge_evaluations(1).expect("second evaluation");
    assert_eq!(
        work.budget_mut().spent_ticks_for_test(),
        before.checked_mul(2).expect("two evaluation events fit"),
    );
}

#[test]
fn coverage_native_admission_debits_the_ordinary_accounts_budget() {
    let rows = overlapping_rows();
    let frame = GeographicFrame::crs84();
    let mut budget = EvaluationBudget::new("coverage_clean", 0).expect("coverage budget");
    let mut work = RecordingPreparation::new(&mut budget);
    work.charge_evaluations(1).expect("prepaid evaluation");
    let evaluation_ticks = work.budget_mut().spent_ticks_for_test();
    assert!(evaluation_ticks > 0);
    let analysis =
        analyze_geographic_coverage_with_work(&rows, frame, 0.0, "coverage_clean", &mut work)
            .expect("ordinary source analysis");
    let assignment = analysis
        .assign_cleaning_faces(
            Some(&rows.each_ref().map(Shape::area)),
            0.0,
            CoverageOverlapRule::MinIndex,
            None::<&crate::crs::GeodesicMetric>,
            &mut work,
        )
        .expect("ordinary face selection");
    let context: SourceArcContext<'_, '_> =
        SourceArcContext::new(TopologyDomain::Geographic(frame), None);
    let _receipt = analysis
        .coverage_drawing_receipt(
            &assignment,
            &rows.each_ref().map(Shape::axes),
            &context,
            StorageTarget::Array,
            &mut work,
        )
        .expect("genuine Native coverage receipt");
    let evaluations = work.evaluations;
    let spent = work.budget_mut().spent_ticks_for_test();
    assert!(evaluations > 1);
    assert!(
        spent
            > u128::from(evaluations)
                .checked_mul(evaluation_ticks)
                .expect("fixture evaluation work fits"),
        "Native admission must debit the same budget beyond outer evaluation events",
    );
}
