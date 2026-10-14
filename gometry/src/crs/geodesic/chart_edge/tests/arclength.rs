
#[derive(Default)]
struct CountingWork(u64);

impl Work for CountingWork {
    type Error = crate::error::Error;

    fn charge(&mut self, ticks: u64) -> crate::error::Result<()> {
        self.0 = self.0.wrapping_add(ticks);
        Ok(())
    }

    fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> crate::error::Result<()> {
        self.charge(crate::work::work_product_ticks(count, ticks_per_item)?)
    }
}

#[test]
fn xy_witness_skips_arclength_but_ordinate_witnesses_compute_it() {
    let edge = ChartEdge::new(
        Point::new_raw_xy(0.0, 10.0),
        Point::new_raw_xy(20.0, 30.0),
        degrees(),
    )
    .unwrap();
    let geodesic = wgs84();
    let metric = crate::crs::EllipsoidMetric::for_geodesic_at_frame(&geodesic, 0, degrees());

    let mut work = CountingWork::default();
    edge.witness_with_work(
        Point::new_raw_xy(0.0, 10.0),
        Point::new_raw_xy(20.0, 30.0),
        0.4,
        None,
        &metric,
        &mut work,
    )
    .unwrap();
    assert_eq!(work.0, 0);

    for (start, end) in [
        (
            Point::new_axes(0.0, 10.0, ZOrdinate(Some(2.0)), MOrdinate(None)).unwrap(),
            Point::new_axes(20.0, 30.0, ZOrdinate(Some(7.0)), MOrdinate(None)).unwrap(),
        ),
        (
            Point::new_axes(0.0, 10.0, ZOrdinate(None), MOrdinate(Some(2.0))).unwrap(),
            Point::new_axes(20.0, 30.0, ZOrdinate(None), MOrdinate(Some(7.0))).unwrap(),
        ),
        (
            Point::new_axes(0.0, 10.0, ZOrdinate(Some(2.0)), MOrdinate(Some(12.0))).unwrap(),
            Point::new_axes(20.0, 30.0, ZOrdinate(Some(7.0)), MOrdinate(Some(17.0))).unwrap(),
        ),
    ] {
        let mut work = CountingWork::default();
        let witness = edge
            .witness_with_work(start, end, 0.4, None, &metric, &mut work)
            .unwrap();
        assert!(work.0 > 0);
        assert_eq!(witness.z().is_some(), start.z().is_some());
        assert_eq!(witness.m().is_some(), start.m().is_some());
    }
}

#[test]
fn chart_edge_matches_mpmath_length_literals() {
    let cases = [
        (
            "general ellipsoidal edge",
            edge_length(degrees(), (-10.0, 35.0), (10.0, 48.0)).unwrap(),
            2_205_762.711_444_021_7,
        ),
        (
            "near-pole chart edge",
            edge_length(degrees(), (0.0, 89.9999), (100.0, 89.99995)).unwrap(),
            15.684_865_577_474_26,
        ),
        (
            "parallel closed form",
            edge_length(degrees(), (0.0, 60.0), (90.0, 60.0)).unwrap(),
            5_022_000.141_519_251,
        ),
        (
            "meridian closed form",
            edge_length(degrees(), (15.0, -20.0), (15.0, 70.0)).unwrap(),
            9_981_346.981_941_827,
        ),
    ];
    for (label, actual, expected) in cases {
        let relative_error = (actual - expected).abs() / expected;
        assert!(
            relative_error <= 1.0e-15,
            "{label}: actual={actual:.17}, expected={expected:.17}, relative_error={relative_error:.3e}"
        );
    }
}

#[test]
fn chart_edge_partial_arclength_and_inverse_match_mpmath() {
    let geodesic = wgs84();
    let edge = ChartEdge::new(
        Point::new_raw_xy(0.0, 10.0),
        Point::new_raw_xy(120.0, 60.0),
        degrees(),
    )
    .unwrap();
    let expected_partial = 6_723_143.426_641_406_5;
    let expected_half_length = 6_004_860.750_429_569;
    let partial = edge.arclength(0.5, &geodesic).unwrap();
    assert!(
        (partial - expected_partial).abs() <= 1.0e-8,
        "partial={partial:.17}, expected={expected_partial:.17}"
    );

    let length = edge.length(&geodesic).unwrap();
    let parameter = edge
        .locate_t(expected_half_length, length, &geodesic)
        .unwrap();
    assert!(
        (parameter - 0.442_288_586_250_222_77).abs() <= 2.0e-15,
        "parameter={parameter:.17}"
    );
    assert!((edge.arclength(parameter, &geodesic).unwrap() - expected_half_length).abs() <= 1.0e-8);
    let point = edge.chart_position(parameter).unwrap();
    assert!((point.x - 53.074_630_350_026_73).abs() <= 3.0e-14);
    assert!((point.y - 32.114_429_312_511_14).abs() <= 3.0e-14);
}

#[test]
fn chart_edge_parallel_inversion_is_exact_and_seam_positions_follow_the_lifted_chart() {
    let geodesic = wgs84();
    let parallel = ChartEdge::new(
        Point::new_raw_xy(0.0, 60.0),
        Point::new_raw_xy(90.0, 60.0),
        degrees(),
    )
    .unwrap();
    assert!(parallel.is_parallel());
    let length = parallel.length(&geodesic).unwrap();
    assert_eq!(
        parallel.locate_t(length / 2.0, length, &geodesic).unwrap(),
        0.5
    );
    assert_eq!(parallel.chart_position(0.5).unwrap().x, 45.0);
    assert_eq!(parallel.chart_position(0.5).unwrap().y, 60.0);

    let seam = ChartEdge::new(
        Point::new_raw_xy(170.0, 10.0),
        Point::new_raw_xy(-170.0, 10.0),
        degrees(),
    )
    .unwrap();
    // The lifted edge runs 170°→190°; retain +180° rather than the
    // equivalent canonical spelling -180° so inserted vertices stay on
    // the source edge's chart.
    assert_eq!(seam.chart_position(0.5).unwrap().x, 180.0);
    assert_eq!(seam.chart_position(0.5).unwrap().y, 10.0);

    let aliased_seam = ChartEdge::new(
        Point::new_raw_xy(530.0, 10.0),
        Point::new_raw_xy(190.0, 10.0),
        degrees(),
    )
    .unwrap();
    let midpoint = aliased_seam.chart_position(0.5).unwrap().x;
    assert_eq!(midpoint, 180.0);
    let axis = degrees().longitude();
    for (start, middle, end) in [(170.0, 180.0, -170.0), (530.0, 540.0, 190.0)] {
        for (from, to) in [(start, middle), (middle, end)] {
            assert!(axis.full_turn_delta(from, to).is_none());
            assert_eq!(
                axis.shortest_alias_step(from, to).unwrap().delta(axis),
                10.0
            );
        }
    }
}

#[test]
fn chart_edge_fraction_uses_the_actual_non_wgs84_ellipsoid() {
    const CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";
    let state = crate::crs::runtime_state_snapshot().unwrap();
    crate::crs::with_ellipsoid_metric_at(CRS, &[], &state, |metric| {
        assert_eq!(metric.ellipsoid_parameters(), (7_000_000.0, 1.0 / 150.0));
        let frame = metric.frame();
        let start = Point::new_axes(0.0, -30.0, ZOrdinate(Some(7.0)), MOrdinate(Some(101.0)))?;
        let end = Point::new_axes(0.0, 70.0, ZOrdinate(Some(47.0)), MOrdinate(Some(701.0)))?;
        let longitude = frame
            .longitude()
            .chart_connection(start.x, end.x)
            .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))?
            .retained();
        let edge = ChartEdge::from_retained(start, end, frame, longitude)?;
        let total = metric.chart_edge_length(&edge)?;
        let fraction = metric.chart_edge_arclength_fraction(&edge, 0.25, Some(total))?;
        assert!((fraction - 0.249_093_482_943_736_33).abs() <= 2.0e-15);
        let z = crate::geometry::interpolate_f64(7.0, 47.0, fraction);
        let m = crate::geometry::interpolate_f64(101.0, 701.0, fraction);
        assert!((z - 16.963_739_317_749_453).abs() <= 2.0e-13);
        assert!((m - 250.456_089_766_241_8).abs() <= 2.0e-12);
        Ok(())
    })
    .unwrap();
}

#[test]
fn chart_edge_length_is_invariant_between_degree_grad_and_radian_frames() {
    let degree_length = edge_length(degrees(), (20.0, 35.0), (40.0, 48.0)).unwrap();
    let grad_length = edge_length(
        grads(),
        (20.0 * 10.0 / 9.0, 35.0 * 10.0 / 9.0),
        (40.0 * 10.0 / 9.0, 48.0 * 10.0 / 9.0),
    )
    .unwrap();
    let radian_length = edge_length(
        radians(),
        (20.0_f64.to_radians(), 35.0_f64.to_radians()),
        (40.0_f64.to_radians(), 48.0_f64.to_radians()),
    )
    .unwrap();

    for actual in [grad_length, radian_length] {
        assert!(
            (actual - degree_length).abs() <= 2.0e-9,
            "frame mismatch: degree={degree_length:.17}, actual={actual:.17}"
        );
    }
}

#[test]
fn chart_edge_witness_uses_canonical_point_emission_and_arclength_axes() {
    let start = Point::new_axes(170.0, 10.0, ZOrdinate(Some(2.0)), MOrdinate(Some(-0.0))).unwrap();
    let end = Point::new_axes(-170.0, 10.0, ZOrdinate(Some(6.0)), MOrdinate(Some(8.0))).unwrap();
    let edge = ChartEdge::new(start, end, degrees()).unwrap();
    let geodesic = wgs84();
    let metric = crate::crs::EllipsoidMetric::for_geodesic_at_frame(&geodesic, 0, degrees());
    let witness = edge.witness(start, end, 0.5, None, &metric).unwrap();

    assert_eq!(witness.x, -180.0);
    assert_eq!(witness.y, 10.0);
    assert_eq!(witness.z(), Some(4.0));
    assert_eq!(witness.m(), Some(4.0));
    assert_eq!(edge.chart_position(0.5).unwrap().x, 180.0);
}

#[test]
fn gauss_legendre_admission_is_bounded_to_one_degree_and_near_pole_ratio() {
    let edge = ChartEdge::new(
        Point::new_raw_xy(0.0, 45.0),
        Point::new_raw_xy(0.01, 45.01),
        degrees(),
    )
    .unwrap();
    assert!(edge.admits_gl4(edge.latitude_delta * edge.latitude_unit));
    assert!(ONE_DEGREE > edge.latitude_delta * edge.latitude_unit);
    let polar = ChartEdge::new(
        Point::new_raw_xy(0.0, 89.99),
        Point::new_raw_xy(0.9, 89.991),
        degrees(),
    )
    .unwrap();
    assert!(!polar.admits_gl4(polar.latitude_delta * polar.latitude_unit));
}
