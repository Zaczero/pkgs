    #[test]
    fn finite_wedge_does_not_reject_ideal_overflow() {
        let previous = f64::MAX.next_down();
        let ulp = f64::MAX - previous;
        let mut x_numerator = ExactSum::zero();
        x_numerator.add_product(f64::MAX, 1.0);
        x_numerator.add_product(f64::MAX, 1.0);
        let preferred = PreferredCorner {
            point: point(f64::INFINITY, 0.0),
            numerator: [x_numerator, ExactSum::zero()],
            denominator: [1.0; 2],
            anisotropic_frame: false,
        };
        let candidate = finite_support_wedge_point(
            SupportHalfplane {
                gradient: (1.0, 2.0),
                support: point(previous, ulp),
            },
            SupportHalfplane {
                gradient: (-2.0, 1.0),
                support: point(f64::MAX, -2.0 * ulp),
            },
            &preferred,
        )
        .expect("finite outward wedge");
        assert_eq!(candidate.x.to_bits(), f64::MAX.to_bits());
        assert_eq!(candidate.y.to_bits(), (ulp / 2.0).to_bits());
    }

    #[test]
    fn overflowing_preferred_keeps_its_exact_magnitude_for_displacement() {
        let mut x_numerator = ExactSum::zero();
        x_numerator.add_product(f64::MAX, 1.0);
        x_numerator.add_product(f64::MAX, 1.0);
        let preferred = PreferredCorner {
            point: point(f64::INFINITY, 0.0),
            numerator: [x_numerator, ExactSum::zero()],
            denominator: [1.0; 2],
            anisotropic_frame: false,
        };
        // Saturating the ideal x to MAX would choose `far`; the exact ideal is
        // near 2*MAX, for which the square-boundary point is strictly nearer.
        let boundary = point(f64::MAX, f64::MAX);
        let far = point(-f64::MAX, 0.0);
        assert_eq!(
            displacement_order(boundary, far, &preferred),
            Ordering::Less
        );
    }

    #[test]
    fn anisotropic_displacement_uses_world_coordinate_denominators() {
        let preferred = PreferredCorner {
            point: point(0.0, 0.0),
            numerator: [ExactSum::zero(), ExactSum::zero()],
            denominator: [1.0, 2.0],
            anisotropic_frame: true,
        };
        // These candidates have world squared distances 1 and 2.25. Their raw
        // residual-numerator scores are 4 and 2.25, the inverse ordering.
        assert_eq!(
            displacement_order(point(0.0, 1.0), point(1.5, 0.0), &preferred),
            Ordering::Less
        );
    }

    #[test]
    fn equal_denominator_displacement_keeps_the_raw_residual_order() {
        let preferred = PreferredCorner {
            point: point(0.0, 0.0),
            numerator: [ExactSum::zero(), ExactSum::zero()],
            denominator: [2.0; 2],
            anisotropic_frame: false,
        };
        assert_eq!(
            displacement_order(point(0.0, 1.0), point(1.5, 0.0), &preferred),
            Ordering::Less
        );
    }

    #[test]
    fn anisotropic_displacement_weights_negative_axis_residuals() {
        let preferred = PreferredCorner {
            point: point(0.0, 0.0),
            numerator: [ExactSum::zero(), ExactSum::zero()],
            denominator: [2.0, 1.0],
            anisotropic_frame: true,
        };
        // The x candidate is 1 away in world coordinates, while the negative
        // y candidate is 2.25 away. Their raw residual scores are 4 and 2.25,
        // so numerator-only ranking incorrectly chooses the y candidate.
        assert_eq!(
            displacement_order(point(1.0, 0.0), point(0.0, -1.5), &preferred),
            Ordering::Less
        );
    }

    #[test]
    fn actual_edge_certificate_accepts_a_nonzero_origin_rectangle() {
        let rectangle = [
            point(1.0, 2.0),
            point(5.0, 2.0),
            point(5.0, 7.0),
            point(1.0, 7.0),
        ];
        let rectangle = hull_vertices(&rectangle);
        let candidate = select_minimum_candidate(&rectangle);
        assert!(legacy_rectangle_is_certified(
            &rectangle,
            &candidate.legacy_corners(&rectangle).unwrap(),
            candidate,
        ));
    }

    #[test]
    fn directed_quotient_neighbourhood_matches_the_format_wide_oracle() {
        for (numerator, denominator) in [
            (1.0, 3.0),
            (f64::from_bits(1), f64::MIN_POSITIVE),
            (f64::MIN_POSITIVE, f64::from_bits(1)),
            (f64::MAX.next_down(), 1.5),
            (
                f64::from_bits((1_523_u64 << 52) | 1),
                f64::from_bits((523_u64 << 52) | 1),
            ),
            (
                f64::from_bits((523_u64 << 52) | 1),
                f64::from_bits((1_523_u64 << 52) | 1),
            ),
        ] {
            let mut exact = ExactSum::zero();
            exact.add_product(numerator, 1.0);
            let (_, magnitude) = exact.signed_magnitude();
            let fast = positive_quotient_interval(&magnitude, denominator);
            let oracle = positive_quotient_interval_binary(&magnitude, denominator);
            assert_eq!(
                (fast.lo.to_bits(), fast.hi.to_bits()),
                (oracle.lo.to_bits(), oracle.hi.to_bits())
            );
        }
    }

    #[test]
    fn scaled_triangle_has_no_finite_minimum_rectangle() {
        let scale = 1.01e308;
        let points = [
            point(-1.75 * scale, -1.75 * scale),
            point(-1.5 * scale, -0.25 * scale),
            point(-1.75 * scale, -1.5 * scale),
        ];
        let hull = hull_vertices(&points);
        let error = minimum_area_rectangle(&hull).expect_err("unrepresentable support wedge");
        assert_eq!(
            error.to_string(),
            "minimum_rotated_rectangle result is not representable with finite coordinates",
        );
    }
