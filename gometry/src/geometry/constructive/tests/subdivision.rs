use super::*;

#[cfg(test)]
mod subdivide_columns_tests {

    use geographiclib_rs::Geodesic;

    use super::*;
    use crate::geographic::GeographicFrame;
    use crate::geometry::constructive::{densify_points_budgeted, segmentize_points_budgeted};

    #[test]
    fn densify_xyzm_interpolates_every_ordinate_lane() {
        let line = CoordSeq::try_from_vecs(
            vec![0.0, 4.0],
            vec![0.0, 0.0],
            Some(vec![10.0, 30.0]),
            Some(vec![100.0, 300.0]),
        )
        .unwrap();
        let mut budget = ExpansionBudget::new("segmentize", "fraction");
        let densified = densify_points_budgeted(&line, 0.25, &mut budget).unwrap();
        assert_eq!(densified.len(), 5);
        assert_eq!(densified.xs(), &[0.0, 1.0, 2.0, 3.0, 4.0]);
        assert_eq!(densified.ys(), &[0.0, 0.0, 0.0, 0.0, 0.0]);
        assert_eq!(densified.zs().unwrap(), &[10.0, 15.0, 20.0, 25.0, 30.0]);
        assert_eq!(densified.ms().unwrap(), &[
            100.0, 150.0, 200.0, 250.0, 300.0
        ]);
    }

    #[test]
    fn segmentize_matches_fraction_dedup_reference() {
        let line = CoordSeq::try_from_points(&[
            Point::new(0.0, 0.0).unwrap(),
            Point::new(3.0, 0.0).unwrap(),
        ])
        .unwrap();
        let mut budget = ExpansionBudget::new("segmentize", "max_length");
        let segmented = segmentize_points_budgeted(
            &line,
            1.0,
            &crate::geometry::SegmentPlacement::Planar,
            &mut budget,
        )
        .unwrap();
        assert_eq!(segmented.len(), 4);
        assert_eq!(segmented.xs(), &[0.0, 1.0, 2.0, 3.0]);
    }

    #[test]
    fn planar_segmentize_scales_opposing_endpoint_distance_before_planning() {
        let line = CoordSeq::try_from_points(&[
            Point::new(-1e308, 0.0).unwrap(),
            Point::new(1e308, 0.0).unwrap(),
        ])
        .unwrap();
        let mut budget = ExpansionBudget::new("segmentize", "max_length");
        let segmented = segmentize_points_budgeted(
            &line,
            1e308,
            &crate::geometry::SegmentPlacement::Planar,
            &mut budget,
        )
        .unwrap();

        assert_eq!(segmented.xs(), &[-1e308, 0.0, 1e308]);
        assert_eq!(segmented.ys(), &[0.0, 0.0, 0.0]);
        assert_eq!(budget.used(), 1);
    }

    #[test]
    fn planar_segmentize_accepts_exact_representable_length_boundaries() {
        let line = CoordSeq::try_from_points(&[
            Point::new(0.0, 0.0).unwrap(),
            Point::new(4.0, 0.0).unwrap(),
        ])
        .unwrap();

        let mut unchanged_budget = ExpansionBudget::new("segmentize", "max_length");
        let unchanged = segmentize_points_budgeted(
            &line,
            4.0,
            &crate::geometry::SegmentPlacement::Planar,
            &mut unchanged_budget,
        )
        .unwrap();
        assert_eq!(unchanged.xs(), &[0.0, 4.0]);
        assert_eq!(unchanged_budget.used(), 0);

        let mut split_budget = ExpansionBudget::new("segmentize", "max_length");
        let split = segmentize_points_budgeted(
            &line,
            2.0,
            &crate::geometry::SegmentPlacement::Planar,
            &mut split_budget,
        )
        .unwrap();
        assert_eq!(split.xs(), &[0.0, 2.0, 4.0]);
        assert_eq!(split_budget.used(), 1);

        let adjacent = f64::from_bits(1.0_f64.to_bits() + 1);
        let adjacent_line = CoordSeq::try_from_points(&[
            Point::new(1.0, 0.0).unwrap(),
            Point::new(adjacent, 0.0).unwrap(),
        ])
        .unwrap();
        let mut adjacent_budget = ExpansionBudget::new("segmentize", "max_length");
        let exact = segmentize_points_budgeted(
            &adjacent_line,
            2.0_f64.powi(-52),
            &crate::geometry::SegmentPlacement::Planar,
            &mut adjacent_budget,
        )
        .unwrap();
        assert_eq!(exact.xs(), &[1.0, adjacent]);
        assert_eq!(adjacent_budget.used(), 0);
    }

    #[test]
    fn planar_segmentize_rejects_an_unrepresentable_binary64_chord() {
        let adjacent = f64::from_bits(1.0_f64.to_bits() + 1);
        let line = CoordSeq::try_from_points(&[
            Point::new(1.0, 0.0).unwrap(),
            Point::new(adjacent, 0.0).unwrap(),
        ])
        .unwrap();
        let error = Shape::LineString(LineSeq::from_proven(line))
            .segmentize(2.0_f64.powi(-54))
            .unwrap_err();

        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::UnrepresentableOutput {
                operation: "segmentize",
                ..
            })
        ));
    }

    #[test]
    fn geographic_segmentize_preserves_curved_xy_and_interpolated_zm() {
        let line = CoordSeq::try_from_vecs(
            [0.0, 10.0].into(),
            [0.0, 10.0].into(),
            Some([10.0, 30.0].into()),
            Some([100.0, 300.0].into()),
        )
        .unwrap();
        let geodesic = Geodesic::wgs84();
        let mut budget = ExpansionBudget::new("segmentize", "max_length");
        let segmented = segmentize_points_budgeted(
            &line,
            1_000_000.0,
            &crate::geometry::SegmentPlacement::Geodesic(&geodesic, GeographicFrame::crs84()),
            &mut budget,
        )
        .unwrap();

        assert_eq!(segmented.len(), 3);
        assert_eq!(segmented.xs()[0].to_bits(), 0.0_f64.to_bits());
        assert_eq!(segmented.ys()[0].to_bits(), 0.0_f64.to_bits());
        assert_eq!(segmented.xs()[2].to_bits(), 10.0_f64.to_bits());
        assert_eq!(segmented.ys()[2].to_bits(), 10.0_f64.to_bits());
        assert!((segmented.xs()[1] - 4.961_882_922_527_124).abs() <= 1e-12);
        assert!((segmented.ys()[1] - 5.019_508_800_894_45).abs() <= 1e-12);
        assert_eq!(segmented.zs().unwrap(), &[10.0, 20.0, 30.0]);
        assert_eq!(segmented.ms().unwrap(), &[100.0, 200.0, 300.0]);
    }

    #[test]
    fn geographic_segmentize_selects_equatorial_interpolation_per_segment() {
        let line = CoordSeq::try_from_points(&[
            Point::new(-10.0, 0.0).unwrap(),
            Point::new(0.0, 0.0).unwrap(),
            Point::new(10.0, 10.0).unwrap(),
        ])
        .unwrap();
        let geodesic = Geodesic::wgs84();
        let mut budget = ExpansionBudget::new("segmentize", "max_length");
        let segmented = segmentize_points_budgeted(
            &line,
            1_000_000.0,
            &crate::geometry::SegmentPlacement::Geodesic(&geodesic, GeographicFrame::crs84()),
            &mut budget,
        )
        .unwrap();

        assert_eq!(segmented.len(), 5);
        assert_eq!(segmented.xs()[1], -5.0);
        assert_eq!(segmented.ys()[1].to_bits(), 0.0_f64.to_bits());
        assert!((segmented.xs()[3] - 4.961_882_922_527_124).abs() <= 1e-12);
        assert!((segmented.ys()[3] - 5.019_508_800_894_45).abs() <= 1e-12);
    }

    #[test]
    fn geographic_segmentize_consumes_sparse_plans_in_source_order() {
        let line = CoordSeq::try_from_vecs(
            [-0.0001, 0.0, 10.0, 10.0001, 20.0001, 20.0002].into(),
            [-0.0001, 0.0, 10.0, 10.0001, 20.0001, 20.0002].into(),
            Some([10.0, 20.0, 30.0, 40.0, 50.0, 60.0].into()),
            Some([100.0, 200.0, 300.0, 400.0, 500.0, 600.0].into()),
        )
        .unwrap();
        let geodesic = Geodesic::wgs84();
        let mut budget = ExpansionBudget::new("segmentize", "max_length");
        let segmented = segmentize_points_budgeted(
            &line,
            1_000_000.0,
            &crate::geometry::SegmentPlacement::Geodesic(&geodesic, GeographicFrame::crs84()),
            &mut budget,
        )
        .unwrap();

        // The inserted coordinates are the stored binary64 midpoints on the
        // WGS84 geodesics; source vertices remain in their original order.
        assert_eq!(segmented.len(), 8);
        assert_eq!(segmented.xs()[0].to_bits(), (-0.0001_f64).to_bits());
        assert_eq!(segmented.ys()[0].to_bits(), (-0.0001_f64).to_bits());
        assert_eq!(segmented.xs()[1].to_bits(), 0.0_f64.to_bits());
        assert_eq!(segmented.ys()[1].to_bits(), 0.0_f64.to_bits());
        assert!((segmented.xs()[2] - 4.961_882_922_527_124).abs() <= 1e-12);
        assert!((segmented.ys()[2] - 5.019_508_800_894_45).abs() <= 1e-12);
        assert_eq!(segmented.xs()[3].to_bits(), 10.0_f64.to_bits());
        assert_eq!(segmented.ys()[3].to_bits(), 10.0_f64.to_bits());
        assert_eq!(segmented.xs()[4].to_bits(), 10.0001_f64.to_bits());
        assert_eq!(segmented.ys()[4].to_bits(), 10.0001_f64.to_bits());
        assert!((segmented.xs()[5] - 14.883_313_082_425_628).abs() <= 1e-12);
        assert!((segmented.ys()[5] - 15.056_212_173_359_553).abs() <= 1e-12);
        assert_eq!(segmented.xs()[6].to_bits(), 20.0001_f64.to_bits());
        assert_eq!(segmented.ys()[6].to_bits(), 20.0001_f64.to_bits());
        assert_eq!(segmented.xs()[7].to_bits(), 20.0002_f64.to_bits());
        assert_eq!(segmented.ys()[7].to_bits(), 20.0002_f64.to_bits());
        assert_eq!(segmented.zs().unwrap(), &[
            10.0, 20.0, 25.0, 30.0, 40.0, 45.0, 50.0, 60.0
        ]);
        assert_eq!(segmented.ms().unwrap(), &[
            100.0, 200.0, 250.0, 300.0, 400.0, 450.0, 500.0, 600.0
        ]);
    }

    #[test]
    fn geodesic_segmentize_rejects_an_ambiguous_antipodal_route() {
        let line = CoordSeq::try_from_points(&[
            Point::new(0.0, 0.0).unwrap(),
            Point::new(180.0, 0.0).unwrap(),
        ])
        .unwrap();
        let geodesic = Geodesic::wgs84();
        let mut budget = ExpansionBudget::new("segmentize", "max_length");
        let error = segmentize_points_budgeted(
            &line,
            11_000_000.0,
            &crate::geometry::SegmentPlacement::Geodesic(&geodesic, GeographicFrame::crs84()),
            &mut budget,
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("geodesic segment endpoints have multiple shortest geodesics")
        );
    }

    #[test]
    fn geographic_segmentize_preserves_coordinates_without_charging() {
        let line = CoordSeq::try_from_points(&[
            Point::new(0.0, 0.0).unwrap(),
            Point::new(1.0, 0.0).unwrap(),
        ])
        .unwrap();
        let geodesic = Geodesic::wgs84();
        let mut budget = ExpansionBudget::new("segmentize", "max_length");
        let segmented = segmentize_points_budgeted(
            &line,
            2_000_000.0,
            &crate::geometry::SegmentPlacement::Geodesic(&geodesic, GeographicFrame::crs84()),
            &mut budget,
        )
        .unwrap();

        assert_eq!(budget.used(), 0);
        assert_eq!(segmented.xs(), line.xs());
        assert_eq!(segmented.ys(), line.ys());
    }

    #[test]
    fn pathological_subdivision_counts_fail_before_allocation() {
        let line = CoordSeq::try_from_points(&[
            Point::new(0.0, 0.0).unwrap(),
            Point::new(1.0, 0.0).unwrap(),
        ])
        .unwrap();
        let mut segmentize_budget = ExpansionBudget::new("segmentize", "max_length");
        let segmentize = segmentize_points_budgeted(
            &line,
            f64::MIN_POSITIVE,
            &crate::geometry::SegmentPlacement::Planar,
            &mut segmentize_budget,
        )
        .unwrap_err();
        assert!(matches!(
            segmentize.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::GeneratedOutputTooLarge {
                operation: "segmentize",
                parameter: "max_length",
                ..
            })
        ));
        let mut densify_budget = ExpansionBudget::new("segmentize", "fraction");
        let densify =
            densify_points_budgeted(&line, f64::MIN_POSITIVE, &mut densify_budget).unwrap_err();
        assert!(matches!(
            densify.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::GeneratedOutputTooLarge {
                operation: "segmentize",
                parameter: "fraction",
                ..
            })
        ));
    }
}
