use super::*;

#[cfg(test)]
mod work_funded_constructor_tests {
    use super::*;
    use crate::geometry::embedding_search::{EvaluationBudget, EvaluationOnly};

    struct RecordingWork {
        budget: EvaluationBudget,
        attempted: Vec<u64>,
        accepted: Vec<u64>,
    }

    impl RecordingWork {
        fn with_remaining(remaining_ticks: u128) -> crate::error::Result<Self> {
            Ok(Self {
                budget: EvaluationBudget::with_remaining_ticks_for_test(
                    "coordinate columns",
                    1,
                    remaining_ticks,
                )?,
                attempted: Vec::new(),
                accepted: Vec::new(),
            })
        }
    }

    impl Work for RecordingWork {
        type Error = crate::error::Error;

        fn charge(&mut self, ticks: u64) -> crate::error::Result<()> {
            self.attempted.push(ticks);
            self.budget.charge(ticks)?;
            self.accepted.push(ticks);
            Ok(())
        }

        fn charge_product(
            &mut self,
            count: usize,
            ticks_per_item: usize,
        ) -> crate::error::Result<()> {
            self.charge(crate::work::work_product_ticks(count, ticks_per_item)?)
        }
    }

    fn search_limit(error: &crate::error::Error) -> bool {
        matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "coordinate columns",
                constructed: 1,
                ..
            })
        )
    }

    fn record_bits(coords: &CoordSeq) -> Vec<[u64; 4]> {
        coords
            .iter()
            .map(|point| {
                [
                    point.x.to_bits(),
                    point.y.to_bits(),
                    point.z.to_bits(),
                    point.m.to_bits(),
                ]
            })
            .collect()
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "assertions pin refusal before coordinate-column allocation"
    )]
    fn column_reservation_refuses_before_allocating() -> crate::error::Result<()> {
        let mut work = RecordingWork::with_remaining(0)?;

        let Err(error) = reserve_point_column(2, &mut work) else {
            return Err(GeometryErrorKind::Invalid(
                "coordinate column reserved without work allowance".into(),
            )
            .into());
        };

        assert!(search_limit(&error));
        assert_eq!(work.attempted, [2]);
        assert!(work.accepted.is_empty());
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "assertions pin refusal before point reads or output writes"
    )]
    fn column_copy_refuses_before_reading_or_writing_points() -> crate::error::Result<()> {
        let points = [Point::new_raw_xy(1.0, 2.0), Point::new_raw_xy(3.0, 4.0)];
        let mut output = vec![99.0];
        let reads = std::cell::Cell::new(0);
        let mut work = RecordingWork::with_remaining(0)?;

        let error = match copy_point_column(
            &points,
            &mut output,
            |point| {
                reads.set(reads.get() + 1);
                point.x
            },
            &mut work,
        ) {
            Ok(()) => {
                return Err(GeometryErrorKind::Invalid(
                    "coordinate column copied without work allowance".into(),
                )
                .into());
            },
            Err(error) => error,
        };

        assert!(search_limit(&error));
        assert_eq!(work.attempted, [2]);
        assert!(work.accepted.is_empty());
        assert_eq!(reads.get(), 0);
        assert_eq!(output, [99.0]);
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "assertions pin refusal before coordinate-value inspection"
    )]
    fn column_finiteness_pass_refuses_before_inspecting_values() -> crate::error::Result<()> {
        let values = [1.0, f64::NAN];
        let mut work = RecordingWork::with_remaining(0)?;

        let Err(error) = point_column_all_finite(&values, &mut work) else {
            return Err(GeometryErrorKind::Invalid(
                "coordinate column was scanned without work allowance".into(),
            )
            .into());
        };

        assert!(search_limit(&error));
        assert_eq!(work.attempted, [2]);
        assert!(work.accepted.is_empty());
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "assertions pin coordinate bits and funded column-phase counts"
    )]
    fn xyzm_column_phases_are_billed_and_ordinary_policies_erase_them() -> crate::error::Result<()>
    {
        let points = [
            Point::new_raw_axes(-0.0, 1.25, ZOrdinate(Some(2.5)), MOrdinate(Some(3.75))),
            Point::new_raw_axes(4.5, -0.0, ZOrdinate(Some(5.25)), MOrdinate(Some(6.75))),
        ];
        let expected = vec![
            [
                (-0.0_f64).to_bits(),
                1.25_f64.to_bits(),
                2.5_f64.to_bits(),
                3.75_f64.to_bits(),
            ],
            [
                4.5_f64.to_bits(),
                (-0.0_f64).to_bits(),
                5.25_f64.to_bits(),
                6.75_f64.to_bits(),
            ],
        ];
        let ordinary = CoordSeq::try_from_points(&points)?;
        let mut unmetered = Unmetered;
        let native_unmetered = CoordSeq::try_from_points_with_work(&points, &mut unmetered)?;
        let mut budget =
            EvaluationBudget::with_remaining_ticks_for_test("coordinate columns", 1, 1)?;
        let evaluation_only = {
            let mut work = EvaluationOnly::new(&mut budget);
            CoordSeq::try_from_points_with_work(&points, &mut work)?
        };
        <EvaluationBudget as Work>::charge(&mut budget, 1)?;

        let mut work = RecordingWork::with_remaining(100)?;
        let funded = CoordSeq::try_from_points_with_work(&points, &mut work)?;

        assert_eq!(record_bits(&ordinary), expected);
        assert_eq!(record_bits(&native_unmetered), expected);
        assert_eq!(record_bits(&evaluation_only), expected);
        assert_eq!(record_bits(&funded), expected);
        assert_eq!(work.attempted, [2; 12]);
        assert_eq!(work.accepted, [2; 12]);
        Ok(())
    }
}

#[cfg(test)]
mod coerce_tests {
    use super::*;

    /// The witness pair drops to the axes BOTH endpoints carry: a Z/M endpoint
    /// vs a bare XY endpoint becomes XY (no fabricated Z/M), and the result is
    /// axis-homogeneous so `LineSeq::segment` and `nearest_points` cannot diverge.
    #[test]
    fn coerce_to_common_axes_drops_to_intersection() {
        let zm = Point::new_raw_axes(1.0, 0.0, ZOrdinate(Some(2.0)), MOrdinate(Some(12.0)));
        let xy = Point::new_raw_axes(1.0, 3.0, ZOrdinate(None), MOrdinate(None));
        let (a, b) = coerce_to_common_axes(zm, xy);
        assert!(!a.axes.has_z() && !a.axes.has_m());
        assert!(!b.axes.has_z() && !b.axes.has_m());
        assert_eq!(
            a,
            Point::new_raw_axes(1.0, 0.0, ZOrdinate(None), MOrdinate(None))
        );
        assert_eq!(b, xy);
    }

    /// Shared Z (no M on either) is preserved on both endpoints.
    #[test]
    fn coerce_to_common_axes_keeps_shared_z() {
        let a = Point::new_raw_axes(1.0, 0.0, ZOrdinate(Some(2.0)), MOrdinate(None));
        let b = Point::new_raw_axes(1.0, 3.0, ZOrdinate(Some(99.0)), MOrdinate(None));
        let (a2, b2) = coerce_to_common_axes(a, b);
        assert!(a2.axes.has_z() && !a2.axes.has_m());
        assert_eq!(a2, a);
        assert_eq!(b2, b);
    }
}
