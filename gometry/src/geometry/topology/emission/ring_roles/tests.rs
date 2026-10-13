    #![expect(
        clippy::panic_in_result_fn,
        reason = "numerical filter controls assert independently known exact windings"
    )]

    use super::*;

    fn exact_points(values: &[(f64, f64)]) -> Vec<ExactChartPoint> {
        values
            .iter()
            .map(|&(x, y)| {
                ExactChartPoint::from_rationals(
                    ExactRational::from_f64(x),
                    ExactRational::from_f64(y),
                )
            })
            .collect()
    }

    #[test]
    fn quotient_circle_roles_keep_stored_period_edges_and_reject_retraces() -> Result<()> {
        use crate::geographic::RingDomain;

        let frame = GeographicFrame::crs84();
        for coordinates in [[(0.0, 60.0), (360.0, 60.0), (315.0, 65.0), (0.0, 60.0)], [
            (0.0, -60.0),
            (-360.0, -60.0),
            (-315.0, -65.0),
            (0.0, -60.0),
        ]] {
            let ring = rounded_exact_ring(&exact_points(&coordinates))?;
            let summary = RingSummary::try_new(frame, &GeographicRing::from_ring(&ring))
                .expect("stored full-period circle is admitted");
            assert!(matches!(summary.domain, RingDomain::FullLongitude(_)));
            assert_eq!(quotient_ring_role(frame, &ring)?, Some(false));
            let reversed = coordinates.into_iter().rev().collect::<Vec<_>>();
            let reversed_ring = rounded_exact_ring(&exact_points(&reversed))?;
            assert_eq!(quotient_ring_role(frame, &reversed_ring)?, Some(true));
        }
        let retrace = rounded_exact_ring(&exact_points(&[
            (0.0, 60.0),
            (360.0, 60.0),
            (0.0, 60.0),
            (0.0, 60.0),
        ]))?;
        let summary = RingSummary::try_new(frame, &GeographicRing::from_ring(&retrace))
            .expect("zero-turn retrace has a summary");
        assert_eq!(summary.chart_turn, 0);
        assert_eq!(summary.domain, RingDomain::Ordinary);
        assert_eq!(quotient_ring_role(frame, &retrace)?, None);
        Ok(())
    }

    #[test]
    fn quotient_closure_roles_use_chart_turn_instead_of_folded_winding() -> Result<()> {
        let frame = GeographicFrame::crs84();
        for (name, coordinates) in [
            ("north", [
                (0.0, 80.0),
                (90.0, 90.0),
                (180.0, 80.0),
                (-90.0, 80.0),
                (0.0, 80.0),
            ]),
            ("south", [
                (0.0, -80.0),
                (-90.0, -90.0),
                (-180.0, -80.0),
                (90.0, -80.0),
                (0.0, -80.0),
            ]),
            ("equator-touching north", [
                (0.0, 0.0),
                (90.0, 10.0),
                (180.0, 0.0),
                (-90.0, 0.0),
                (0.0, 0.0),
            ]),
        ] {
            let ports = exact_points(&coordinates);
            let published = rounded_exact_ring(&ports)?;
            let role = compare_q_ring_role(frame, &published, &published, &ports, true)?
                .expect("bounded quotient closure has a role");
            assert!(!role.exact_is_hole, "{name}");
            assert!(!role.emitted_is_hole, "{name}");
            assert!(role.matches(), "{name}");
            let reversed = ports.iter().rev().cloned().collect::<Vec<_>>();
            let reversed_ring = rounded_exact_ring(&reversed)?;
            let complement =
                compare_q_ring_role(frame, &reversed_ring, &reversed_ring, &ports, true)?
                    .expect("reversed bounded closure has a role");
            assert!(complement.emitted_is_hole, "{name}");
            assert!(!complement.matches(), "{name}");
        }
        Ok(())
    }

    #[test]
    fn interval_ring_filter_certifies_fixed_known_windings() {
        let frame = GeographicFrame::crs84();
        let unit = f64::from_bits(0x3D10_0000_0000_0000);
        let mut rational_box =
            exact_points(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]);
        for point in &mut rational_box {
            point.x = point
                .x
                .try_quotient(&ExactRational::from_f64(3.0))
                .expect("divide rational longitude");
            point.y = point
                .y
                .try_quotient(&ExactRational::from_f64(7.0))
                .expect("divide rational latitude");
        }
        let mut translated_box = exact_points(&[
            (122.427_194, 10.0),
            (123.427_194, 10.0),
            (123.427_194, 11.0),
            (122.427_194, 11.0),
            (122.427_194, 10.0),
        ]);
        for point in &mut translated_box {
            point.x = point
                .x
                .try_add(&ExactRational::from_f64(360.0))
                .expect("translate exact longitude by a period");
        }

        let cases = [
            (
                "ordinary counterclockwise ring",
                exact_points(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]),
                RingWinding::CounterClockwise,
            ),
            (
                "rational thirds and sevenths",
                rational_box,
                RingWinding::CounterClockwise,
            ),
            (
                "one-ulp narrow ring",
                exact_points(&[
                    (100.0, 80.0),
                    (100.0 + unit, 80.0),
                    (100.0, 80.0 + unit),
                    (100.0, 80.0),
                ]),
                RingWinding::CounterClockwise,
            ),
            (
                "seam-crossing box",
                exact_points(&[
                    (179.0, 0.0),
                    (-179.0, 0.0),
                    (-179.0, 1.0),
                    (179.0, 1.0),
                    (179.0, 0.0),
                ]),
                RingWinding::CounterClockwise,
            ),
            (
                "pole ring",
                exact_points(&[
                    (0.0, 80.0),
                    (90.0, 90.0),
                    (180.0, 80.0),
                    (-90.0, 80.0),
                    (0.0, 80.0),
                ]),
                RingWinding::Clockwise,
            ),
            (
                "low-bit longitude plus a full period",
                translated_box,
                RingWinding::CounterClockwise,
            ),
        ];
        for (name, ports, expected) in cases {
            assert_eq!(
                exact_q_ring_winding_filter(frame, &ports).map(|proof| proof.winding),
                Some(expected),
                "{name}"
            );
        }
    }

    #[test]
    fn interval_ring_filter_declines_uncertain_signs_and_exact_fallback_preserves_errors()
    -> Result<()> {
        let frame = GeographicFrame::crs84();
        let epsilon = ExactRational::from_f64(2.0_f64.powi(-54));
        let mut counterclockwise = exact_points(&[(0.0, 0.0), (1.0, 1.0), (2.0, 2.0), (0.0, 0.0)]);
        counterclockwise[2].y = ExactRational::from_f64(2.0).try_add(&epsilon)?;
        let mut clockwise = counterclockwise.clone();
        clockwise[2].y = ExactRational::from_f64(2.0).try_subtract(&epsilon)?;
        for (ports, expected) in [
            (&counterclockwise, RingWinding::CounterClockwise),
            (&clockwise, RingWinding::Clockwise),
        ] {
            assert_eq!(exact_q_ring_winding_filter(frame, ports), None);
            let lifted = continuously_lift_exact_ring(frame, ports)?;
            assert_eq!(exact_ring_winding(&lifted)?, expected);
        }

        let flat_full_turn = exact_points(&[
            (0.0, 10.0),
            (120.0, 10.0),
            (240.0, 10.0),
            (360.0, 10.0),
            (0.0, 10.0),
        ]);
        assert_eq!(exact_q_ring_winding_filter(frame, &flat_full_turn), None);

        let half_turn_tie = exact_points(&[
            (0.0, 0.0),
            (180.0, 0.0),
            (180.0, 1.0),
            (0.0, 1.0),
            (0.0, 0.0),
        ]);
        assert_eq!(exact_q_ring_winding_filter(frame, &half_turn_tie), None);

        let subnormal = f64::from_bits(1);
        let subnormal_triangle =
            exact_points(&[(0.0, 0.0), (subnormal, 0.0), (0.0, subnormal), (0.0, 0.0)]);
        assert_eq!(
            exact_q_ring_winding_filter(frame, &subnormal_triangle),
            None
        );

        let twice_max =
            ExactRational::from_f64(f64::MAX).try_product(&ExactRational::from_f64(2.0))?;
        let outside_binary64 = vec![
            ExactChartPoint::from_rationals(ExactRational::zero(), ExactRational::zero()),
            ExactChartPoint::from_rationals(twice_max.clone(), ExactRational::zero()),
            ExactChartPoint::from_rationals(twice_max, ExactRational::one()),
            ExactChartPoint::from_rationals(ExactRational::zero(), ExactRational::zero()),
        ];
        assert_eq!(exact_q_ring_winding_filter(frame, &outside_binary64), None);
        let error = exact_q_ring_role(frame, &outside_binary64)
            .expect_err("exact fallback retains binary64 conversion failure");
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactCoordinateOutOfRange)
        ));
        Ok(())
    }

    #[test]
    fn interval_ring_filter_rejects_uncertain_clockwise_cancellation() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let epsilon = ExactRational::from_f64(2.0_f64.powi(-54));
        let mut clockwise = exact_points(&[(0.0, 0.0), (1.0, 1.0), (2.0, 2.0), (0.0, 0.0)]);
        clockwise[2].y = ExactRational::from_f64(2.0).try_subtract(&epsilon)?;

        assert_eq!(exact_q_ring_winding_filter(frame, &clockwise), None);
        let lifted = continuously_lift_exact_ring(frame, &clockwise)?;
        assert_eq!(exact_ring_winding(&lifted)?, RingWinding::Clockwise);
        Ok(())
    }
