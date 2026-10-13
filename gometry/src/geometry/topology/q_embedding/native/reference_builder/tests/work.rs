    #![expect(
        clippy::panic_in_result_fn,
        reason = "ray Work controls assert observed refusal and exact predicate results"
    )]

    use std::cmp::Ordering;

    use super::{NativeRayTransform, NativeReferencePoints, NativeReferenceRayPoints, RayAxis};
    use crate::error::{ErrorKind, Result};
    use crate::geometry::embedding_search::{EvaluationBudget, EvaluationOnly};
    use crate::geometry::exact_arith::ExactSign;
    use crate::geometry::overlay::RayPointsWithWork;
    use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
    use crate::geometry::topology::ExactChartPoint;
    use crate::geometry::{GeometryErrorKind, Unmetered, Work, XY};





    fn budget_refusing_before(
        operation: &'static str,
        event: ChargeEvent,
    ) -> Result<EvaluationBudget> {
        let remaining = event
            .spent_before
            .checked_add(event.ticks)
            .and_then(|ticks| ticks.checked_sub(1))
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("invalid native-ray charge calibration".into())
            })?;
        EvaluationBudget::with_remaining_ticks_for_test(operation, 1, remaining)
    }

    fn assert_limit_error(error: &crate::error::Error, operation: &'static str) {
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: actual,
                ..
            }) if *actual == operation
        ));
    }

    fn nudge_one(offset: f64) -> Result<ExactRational> {
        let numerator = ExactDyadic::from_f64(1.0)
            .try_add_with_work(ExactDyadic::from_f64(offset), &mut Unmetered)?;
        Ok(ExactRational::from_dyadic(numerator))
    }

    fn chart_point(x: ExactRational, y: ExactRational) -> ExactChartPoint {
        ExactChartPoint::from_rationals(x, y)
    }

    fn native_reference_points(points: Vec<ExactChartPoint>) -> Result<NativeReferencePoints> {
        NativeReferencePoints::from_exact(points, &mut Unmetered)
    }

    fn compare_axis<W: Work>(
        points: &NativeReferenceRayPoints<'_>,
        work: &mut W,
    ) -> Result<Ordering> {
        work.charge_into_error(1)?;
        RayPointsWithWork::compare_axis_with_work(points, 0, 1, RayAxis::X, work)
    }

    fn orientation<W: Work>(
        points: &NativeReferenceRayPoints<'_>,
        work: &mut W,
    ) -> Result<ExactSign> {
        work.charge_into_error(1)?;
        RayPointsWithWork::orientation_with_work(points, 0, 1, 2, work)
    }

    #[test]
    fn native_reference_ray_axis_refuses_inside_exact_comparison() -> Result<()> {
        const OPERATION: &str = "native_reference_ray_axis";
        let base = native_reference_points(vec![chart_point(
            ExactRational::from_f64(1.0),
            ExactRational::from_f64(1.0),
        )])?;
        let exact_queries = [chart_point(
            nudge_one(2.0_f64.powi(-256))?,
            ExactRational::from_f64(1.0),
        )];
        let rounded_queries = [XY::new(1.0, 1.0)];
        let points = NativeReferenceRayPoints {
            base: &base,
            exact_queries: &exact_queries,
            rounded_queries: &rounded_queries,
            transform: NativeRayTransform::Identity,
        };
        let (order, charges) = {
            let mut budget = EvaluationBudget::new(OPERATION, 1)?;
            let mut work = RecordingWork::new(&mut budget);
            let order = compare_axis(&points, &mut work)?;
            (order, work.charges)
        };
        assert_eq!(order, Ordering::Less);
        let target = charges
            .iter()
            .copied()
            .find(|event| event.spent_before > 0 && event.ticks > 0)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Native ray axis comparison did not charge Work".into())
            })?;

        let mut budget = budget_refusing_before(OPERATION, target)?;
        let error = {
            let mut work = RecordingWork::new(&mut budget);
            let error = compare_axis(&points, &mut work)
                .expect_err("native ray refuses before returning exact axis order");
            assert_eq!(
                work.refused.map(|event| event.spent_before),
                Some(target.spent_before)
            );
            error
        };
        assert_limit_error(&error, OPERATION);

        let mut unmetered = Unmetered;
        assert_eq!(
            RayPointsWithWork::compare_axis_with_work(&points, 0, 1, RayAxis::X, &mut unmetered,)?,
            order
        );
        let mut ordinary_budget = EvaluationBudget::new(OPERATION, 1)?;
        let ordinary_spend = ordinary_budget.spent_ticks_for_test();
        {
            let mut work = EvaluationOnly::new(&mut ordinary_budget);
            assert_eq!(
                RayPointsWithWork::compare_axis_with_work(&points, 0, 1, RayAxis::X, &mut work,)?,
                order
            );
        }
        assert_eq!(ordinary_budget.spent_ticks_for_test(), ordinary_spend);
        Ok(())
    }

    #[test]
    fn native_reference_ray_orientation_refuses_inside_exact_determinant() -> Result<()> {
        const OPERATION: &str = "native_reference_ray_orientation";
        let base = native_reference_points(vec![
            chart_point(ExactRational::from_f64(1.0), ExactRational::from_f64(1.0)),
            chart_point(
                nudge_one(2.0_f64.powi(-256))?,
                nudge_one(2.0_f64.powi(-256))?,
            ),
            chart_point(
                nudge_one(2.0_f64.powi(-255))?,
                nudge_one(2.0_f64.powi(-256))?,
            ),
        ])?;
        let points = NativeReferenceRayPoints {
            base: &base,
            exact_queries: &[],
            rounded_queries: &[],
            transform: NativeRayTransform::Identity,
        };
        let (sign, charges) = {
            let mut budget = EvaluationBudget::new(OPERATION, 1)?;
            let mut work = RecordingWork::new(&mut budget);
            let sign = orientation(&points, &mut work)?;
            (sign, work.charges)
        };
        assert_eq!(sign, ExactSign::Negative);
        let target = charges
            .iter()
            .copied()
            .find(|event| event.spent_before > 0 && event.ticks > 0)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Native ray orientation did not charge Work".into())
            })?;

        let mut budget = budget_refusing_before(OPERATION, target)?;
        let error = {
            let mut work = RecordingWork::new(&mut budget);
            let error = orientation(&points, &mut work)
                .expect_err("native ray refuses before returning determinant sign");
            assert_eq!(
                work.refused.map(|event| event.spent_before),
                Some(target.spent_before)
            );
            error
        };
        assert_limit_error(&error, OPERATION);

        let mut unmetered = Unmetered;
        assert_eq!(
            RayPointsWithWork::orientation_with_work(&points, 0, 1, 2, &mut unmetered)?,
            sign
        );
        let mut ordinary_budget = EvaluationBudget::new(OPERATION, 1)?;
        let ordinary_spend = ordinary_budget.spent_ticks_for_test();
        {
            let mut work = EvaluationOnly::new(&mut ordinary_budget);
            assert_eq!(
                RayPointsWithWork::orientation_with_work(&points, 0, 1, 2, &mut work)?,
                sign
            );
        }
        assert_eq!(ordinary_budget.spent_ticks_for_test(), ordinary_spend);
        Ok(())
    }
