    #[test]
    fn chart_support_intersection_keeps_1798_fifths_before_native_rounding() {
        use crate::geometry::tessellation::exact::ExactDyadic;

        let (frame, chart) =
            point_selection_chart(&[point(170.0, 0.0), point(180.0, 5.0), point(190.0, 10.0)]);
        let along_min = chart
            .site(point(360.0, 0.0), 0)
            .expect("retained along support");
        let outward_max = chart
            .site(point(360.0, 1.0), 1)
            .expect("retained outward support");
        assert_eq!(along_min.longitude().periods(), 1);
        assert_eq!(outward_max.longitude().periods(), 1);
        let first = ChartSupportHalfplane {
            gradient: (2.0, 1.0),
            support: along_min,
        };
        let second = ChartSupportHalfplane {
            gradient: (-1.0, 2.0),
            support: outward_max,
        };

        let corner = retained_chart_corner(first, second, frame).unwrap();
        assert_eq!(
            corner.x,
            ExactRational::new(ExactDyadic::from_i64(1798), ExactDyadic::from_i64(5)).unwrap()
        );
        assert_eq!(
            corner.y,
            ExactRational::new(ExactDyadic::from_i64(4), ExactDyadic::from_i64(5)).unwrap()
        );
        let rendered = render_chart_corner(&corner, frame).unwrap();
        assert_ne!(corner.x, rendered.exact.x);
        assert_ne!(corner.y, rendered.exact.y);
    }

    #[test]
    fn chart_corner_repair_moves_the_exact_endpoint_outward_after_rn() {
        use crate::geometry::tessellation::exact::ExactDyadic;

        let (frame, chart) = point_selection_chart(&[point(0.0, 0.0), point(1.0, 0.0)]);
        let first = ChartSupportHalfplane {
            gradient: (1.0, 1.0),
            support: chart.site(point(1.0, 0.0), 0).expect("first support site"),
        };
        let second = ChartSupportHalfplane {
            gradient: (-1.0, 2.0),
            support: chart.site(point(0.0, 0.0), 1).expect("second support site"),
        };
        let ideal = retained_chart_corner(first, second, frame).unwrap();
        assert_eq!(
            ideal.x,
            ExactRational::new(ExactDyadic::from_i64(2), ExactDyadic::from_i64(3)).unwrap()
        );
        assert_eq!(
            ideal.y,
            ExactRational::new(ExactDyadic::from_i64(1), ExactDyadic::from_i64(3)).unwrap()
        );
        let trial = render_chart_corner(&ideal, frame).unwrap();
        assert_eq!(
            chart_support_margin(first, &trial.exact, frame)
                .unwrap()
                .sign(),
            ExactSign::Negative
        );

        let repaired = repair_chart_corner(first, second, ideal.clone(), frame).unwrap();
        assert_ne!(repaired.exact, ideal);
        assert_ne!(repaired.point.y.to_bits(), trial.point.y.to_bits());
        assert!(chart_corner_encloses_supports(first, second, &repaired.exact, frame,).unwrap());
        let actual = render_chart_corner(&repaired.exact, frame).unwrap();
        assert!(chart_corner_encloses_supports(first, second, &actual.exact, frame).unwrap());
    }

    #[test]
    fn oblique_chart_rectangle_publishes_a_non_dyadic_support_corner() {
        use crate::geometry::tessellation::exact::ExactDyadic;

        let (frame, hull) = chart_hull(&[
            point(0.0, 0.0),
            point(0.0, 1.0),
            point(2.0, 1.0),
            point(1.0, 1.5),
        ]);
        let candidate = select_minimum_candidate_chart(&hull, frame).unwrap();
        let corners = emit_chart_candidate(&hull, candidate, frame).unwrap();
        let expected = ExactChartPoint {
            x: ExactRational::new(ExactDyadic::from_i64(-2), ExactDyadic::from_i64(5)).unwrap(),
            y: ExactRational::new(ExactDyadic::from_i64(4), ExactDyadic::from_i64(5)).unwrap(),
        };
        let corner = corners
            .iter()
            .find(|corner| corner.exact == expected)
            .expect("caliper support intersection retains its exact quotient");
        let rendered = render_chart_corner(&corner.exact, frame).unwrap();
        assert_ne!(corner.exact.x, rendered.exact.x);
        assert_ne!(corner.exact.y, rendered.exact.y);

        let published = publish_and_certify_chart_rectangle(&hull, &corners, frame).unwrap();
        assert_eq!(published.coord_count(), 5);
    }

    #[test]
    fn chart_rectangle_certificate_includes_generated_side_samples() {
        let source = [
            point(-170.0, 0.0),
            point(0.0, 0.0),
            point(170.0, 0.0),
            point(-170.0, 10.0),
            point(0.0, 10.0),
            point(170.0, 10.0),
        ];
        let (frame, hull) = chart_hull(&source);
        let mut ordered = hull.clone();
        ordered.sort_unstable_by(|left, right| left.compare_chart_xy(*right));
        let axis = frame.longitude();
        let west = axis
            .lifted_site_longitude(
                ordered
                    .first()
                    .expect("chart hull has a west site")
                    .longitude(),
            )
            .unwrap();
        let east = axis
            .lifted_site_longitude(
                ordered
                    .last()
                    .expect("chart hull has an east site")
                    .longitude(),
            )
            .unwrap();
        assert_eq!(east - west, 190.0);

        let rectangle = minimum_area_rectangle_chart(&hull, frame).unwrap();
        let vertices = (0..rectangle.coord_count())
            .map(|index| rectangle.nth_coord(index))
            .collect::<Vec<_>>();
        assert!(
            vertices.len() > 5,
            "both long sides require publication samples"
        );
        assert_eq!(vertices.first(), vertices.last());
    }
