use super::analyze_geographic_coverage;
use crate::error::{ErrorKind, Result};
use crate::geographic::GeographicFrame;
use crate::geometry::coverage::{CoverageOverlapRule, coverage_clean};
use crate::geometry::embedding_search::account::EvaluationBudget;
use crate::geometry::publication::StorageTarget;
use crate::geometry::{CoordinateAxes, GeometryErrorKind, Shape, SourceArcContext, TopologyDomain};

#[test]
fn geographic_clean_never_collapses_a_rational_sliver() {
    let rows = [
        crate::io::parse_wkt("POLYGON ((1 0, 2 0, 2 1, 1.0000000000000002 1, 1 0))")
            .expect("oblique boundary"),
        crate::io::parse_wkt("POLYGON ((1 0.25, 1.5 0.25, 1.5 0.5, 1 0.5, 1 0.25))")
            .expect("overlapping rectangle"),
    ];
    let context = SourceArcContext::new(TopologyDomain::Geographic(GeographicFrame::crs84()), None);
    match coverage_clean(&rows, 0.0, 0.0, CoverageOverlapRule::MinIndex, &context) {
        Ok(output) => {
            let sliver = output.get(1).expect("second coverage row");
            assert!(!sliver.is_empty());
            assert!(sliver.area() > 0.0, "nonempty exact face cannot collapse");
            assert_eq!(sliver.validate().expect("sliver validates"), None);
        },
        Err(error) => assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::UnrepresentableOutput { .. })
        )),
    }
}

#[rstest::rstest]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
#[case::disabled(false)]
#[case::enabled(true)]
fn geographic_clean_attributes_belong_to_the_output_occurrence(#[case] reverse: bool) {
    let rows = [
        crate::io::parse_wkt("POLYGON ZM ((0 0 10 7, 1 0 10 7, 1 1 10 7, 0 1 10 7, 0 0 10 7))")
            .expect("first adjacent row"),
        crate::io::parse_wkt("POLYGON ZM ((1 0 20 7, 2 0 20 7, 2 1 20 7, 1 1 20 7, 1 0 20 7))")
            .expect("second adjacent row"),
        crate::io::parse_wkt(
            "POLYGON ZM ((10 0 30 7, 12 0 30 7, 12 1 30 7, 10 1 30 7, 10 0 30 7))",
        )
        .expect("first distant row"),
        crate::io::parse_wkt(
            "POLYGON ZM ((11 0 30 7, 13 0 30 7, 13 1 30 7, 11 1 30 7, 11 0 30 7))",
        )
        .expect("second distant row"),
    ];
    let model = crate::crs::metric_model(Some("OGC:CRS84")).expect("CRS84 metric model");
    let metric = crate::crs::GeodesicMetric::from_model(&model).expect("ellipsoidal metric");
    let metric_access = || Ok(&metric);
    let context = SourceArcContext::new(
        TopologyDomain::Geographic(GeographicFrame::crs84()),
        Some(&metric_access),
    );

    for permute in [false, true] {
        let mut input = rows.to_vec();
        if reverse {
            input = input
                .into_iter()
                .map(|row| row.reverse())
                .collect::<Result<Vec<_>>>()
                .expect("reversed rows");
        }
        if permute {
            input.swap(2, 3);
        }
        for selected in [&input[..2], &input[..]] {
            let output =
                coverage_clean(selected, 0.0, 0.0, CoverageOverlapRule::MinIndex, &context)
                    .expect("coverage drawing");
            for (row, z) in output.iter().take(2).zip([10.0_f64, 20.0]) {
                assert_eq!(row.axes(), CoordinateAxes::XYZM);
                for point in row.points_vec().expect("output coordinates") {
                    assert_eq!(point.z().map(f64::to_bits), Some(z.to_bits()));
                    assert_eq!(point.m().map(f64::to_bits), Some(7.0_f64.to_bits()));
                }
            }
        }
    }
}

#[test]
fn geographic_clean_empty_row_preserves_source_axes() {
    let row =
        crate::io::parse_wkt("POLYGON ZM ((0 0 10 7, 1 0 10 7, 1 1 10 7, 0 1 10 7, 0 0 10 7))")
            .expect("duplicate measured row");
    let context = SourceArcContext::new(TopologyDomain::Geographic(GeographicFrame::crs84()), None);
    let output = coverage_clean(
        &[row.clone(), row],
        0.0,
        0.0,
        CoverageOverlapRule::MinIndex,
        &context,
    )
    .expect("duplicate coverage drawing");
    let empty = output.get(1).expect("second coverage row");
    assert!(empty.is_empty());
    assert_eq!(empty.axes(), CoordinateAxes::XYZM);
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn geographic_clean_reuses_polar_source_rings_per_selected_region() {
    let outer = crate::io::parse_wkt(
        "POLYGON ZM ((0 80 1 101, 90 80 11 201, 180 80 21 301, 270 80 31 401, 0 80 1 101))",
    )
    .expect("outer polar cap");
    let inner = crate::io::parse_wkt(
        "POLYGON ZM ((0 85 10 110, 90 85 20 210, 180 85 30 310, 270 85 40 410, 0 85 10 110))",
    )
    .expect("inner polar cap");
    let model = crate::crs::metric_model(Some("+proj=longlat +a=7000000 +rf=150 +type=crs"))
        .expect("custom ellipsoid");
    let metric = crate::crs::GeodesicMetric::from_model(&model).expect("ellipsoidal metric");
    let access = || Ok(&metric);
    let domain = TopologyDomain::Geographic(GeographicFrame::crs84());
    let context = SourceArcContext::new(domain, Some(&access));
    for (order, rows) in [
        ("outer first", [outer.clone(), inner.clone()]),
        ("inner first", [inner, outer]),
    ] {
        let output = coverage_clean(&rows, 0.0, 0.0, CoverageOverlapRule::MinIndex, &context)
            .unwrap_or_else(|error| {
                panic!("{order}: selected polar regions have no drawing: {error:?}")
            });
        assert!(
            crate::geometry::coverage_is_valid_with_domain(&output, 0.0, domain)
                .expect("polar coverage validates"),
        );
        assert!(!output.first().expect("first selected row").is_empty());
    }
}

#[rstest::rstest]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
#[case::coverageoverlaprule_maxarea(CoverageOverlapRule::MaxArea)]
#[case::coverageoverlaprule_minarea(CoverageOverlapRule::MinArea)]
fn geographic_clean_area_rankings_retain_source_ring_anchors(#[case] rule: CoverageOverlapRule) {
    let rows = [
        crate::io::parse_wkt("POLYGON ((10 60, 10 85, 0 85, 0 60, 10 60))")
            .expect("northern rectangle"),
        crate::io::parse_wkt("POLYGON ((10 50, 10 70, 0 70, 0 50, 10 50))")
            .expect("southern rectangle"),
    ];
    let model = crate::crs::metric_model(Some("OGC:CRS84")).expect("CRS84 metric");
    let metric = crate::crs::GeodesicMetric::from_model(&model).expect("ellipsoidal metric");
    let access = || Ok(&metric);
    let domain = TopologyDomain::Geographic(GeographicFrame::crs84());
    let context = SourceArcContext::new(domain, Some(&access));

    let output = coverage_clean(&rows, 0.0, 0.0, rule, &context).expect("ranked coverage drawing");
    assert!(
        crate::geometry::coverage_is_valid_with_domain(&output, 0.0, domain)
            .expect("ranked coverage validates"),
    );
}

#[rstest::rstest]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
#[case::north(
    "north",
    "POLYGON ((10 60, 10 80, 0 80, 0 60, 10 60))",
    "POLYGON ((10 70, 10 90, 0 90, 0 70, 10 70))"
)]
#[case::south(
    "south",
    "POLYGON ((0 -60, 0 -80, 10 -80, 10 -60, 0 -60))",
    "POLYGON ((0 -70, 0 -90, 10 -90, 10 -70, 0 -70))"
)]
#[case::north_seam(
    "north seam",
    "POLYGON ((-170 60, -170 80, 170 80, 170 60, -170 60))",
    "POLYGON ((-170 70, -170 90, 170 90, 170 70, -170 70))"
)]
fn geographic_clean_longest_border_retains_polar_boundary(
    #[case] case: &'static str,
    #[case] first: &'static str,
    #[case] second: &'static str,
) {
    let model = crate::crs::metric_model(Some("OGC:CRS84")).expect("CRS84 metric");
    let metric = crate::crs::GeodesicMetric::from_model(&model).expect("ellipsoidal metric");
    let access = || Ok(&metric);
    let domain = TopologyDomain::Geographic(GeographicFrame::crs84());
    let context = SourceArcContext::new(domain, Some(&access));

    let rows = [
        crate::io::parse_wkt(first).expect("overlapping rectangle"),
        crate::io::parse_wkt(second).expect("polar rectangle"),
    ];
    let output = coverage_clean(
        &rows,
        0.0,
        0.0,
        CoverageOverlapRule::LongestBorder,
        &context,
    )
    .unwrap_or_else(|error| panic!("{case}: polar ranked coverage has no drawing: {error:?}"));
    assert!(
        crate::geometry::coverage_is_valid_with_domain(&output, 0.0, domain)
            .expect("polar ranked coverage validates"),
        "{case}",
    );
}

#[test]
fn geographic_scalar_coverage_receipt_rejects_two_live_rows() {
    let rows = [
        crate::io::parse_wkt("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))").expect("first live row"),
        crate::io::parse_wkt("POLYGON ((3 0, 4 0, 4 1, 3 1, 3 0))").expect("second live row"),
    ];
    let frame = GeographicFrame::crs84();
    let analysis = analyze_geographic_coverage(&rows, frame, 0.0, "coverage_clean")
        .expect("coverage analysis");
    let mut work = EvaluationBudget::new("coverage_clean", 0).expect("coverage work");
    let assignment = analysis
        .assign_cleaning_faces(
            &rows.iter().map(Shape::area).collect::<Vec<_>>(),
            0.0,
            CoverageOverlapRule::MinIndex,
            None::<&crate::crs::GeodesicMetric>,
            &mut work,
        )
        .expect("both rows keep their faces");
    let context: SourceArcContext<'_, '_> =
        SourceArcContext::new(TopologyDomain::Geographic(frame), None);
    match analysis.coverage_drawing_receipt(
        &assignment,
        &rows.iter().map(Shape::axes).collect::<Vec<_>>(),
        &context,
        StorageTarget::Scalar,
        &mut work,
    ) {
        Err(error) => assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        )),
        Ok(_) => panic!("scalar coverage silently discarded a live output row"),
    }
}
