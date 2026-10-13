use super::*;

#[test]
fn closures_measure_the_same_parallel_in_supported_angular_frames() {
    let degree = GeographicFrame::crs84();
    let grad_unit = AngularFactor::try_new("grad", std::f64::consts::PI / 200.0).unwrap();
    let grad = GeographicFrame::try_new(grad_unit, grad_unit, 0.0).unwrap();
    let radian_unit = AngularFactor::try_new("radian", 1.0).unwrap();
    let radians = GeographicFrame::try_new(radian_unit, radian_unit, 0.0).unwrap();
    let degree_unit = AngularFactor::try_new("degree", std::f64::consts::PI / 180.0).unwrap();
    let paris = GeographicFrame::try_new(
        degree_unit,
        degree_unit,
        2.596_921_3 * std::f64::consts::PI / 180.0,
    )
    .unwrap();

    for (name, frame, scale, longitude) in [
        ("degrees", degree, 1.0, 40.0),
        (
            "radians at zero",
            radians,
            std::f64::consts::PI / 180.0,
            0.0,
        ),
        (
            "radians at the seam",
            radians,
            std::f64::consts::PI / 180.0,
            -180.0,
        ),
        (
            "radians at 90 degrees",
            radians,
            std::f64::consts::PI / 180.0,
            90.0,
        ),
        ("grads at zero", grad, 10.0 / 9.0, 0.0),
        ("grads at the seam", grad, 10.0 / 9.0, -180.0),
        ("Paris prime meridian", paris, 1.0, 40.0),
    ] {
        let metric = metric_with_flattening_and_frame(1.0 / 298.257_223_563, frame);
        let points = north_closure(longitude, 60.0, false)
            .into_iter()
            .map(|(longitude, latitude)| (longitude * scale, latitude * scale))
            .collect::<Vec<_>>();
        let shape = Shape::Polygon(Polygon::new(ring(&points), Vec::new()));
        let actual = perimeter(&metric, &shape, frame);
        let expected = analytic_parallel_circumference(&metric, 60.0);
        assert!(
            (actual - expected).abs() <= 1.0e-8 * expected,
            "{name}: {actual:.12} != {expected:.12}"
        );
    }
}
#[test]
fn nonidentical_radian_and_grad_aliases_remain_stored_without_a_tolerance() {
    for (name, frame, scale) in [
        (
            "radians",
            GeographicFrame::try_new(
                AngularFactor::try_new("radian", 1.0).unwrap(),
                AngularFactor::try_new("radian", 1.0).unwrap(),
                0.0,
            )
            .unwrap(),
            std::f64::consts::PI / 180.0,
        ),
        (
            "grads",
            GeographicFrame::try_new(
                AngularFactor::try_new("grad", std::f64::consts::PI / 200.0).unwrap(),
                AngularFactor::try_new("grad", std::f64::consts::PI / 200.0).unwrap(),
                0.0,
            )
            .unwrap(),
            10.0 / 9.0,
        ),
    ] {
        let metric = metric_with_flattening_and_frame(1.0 / 298.257_223_563, frame);
        let points = north_closure(40.0, 60.0, false)
            .into_iter()
            .map(|(longitude, latitude)| (longitude * scale, latitude * scale))
            .collect::<Vec<_>>();
        let shape = Shape::Polygon(Polygon::new(ring(&points), Vec::new()));
        let stored = raw_stored_perimeter(&metric, &shape);
        let actual = perimeter(&metric, &shape, frame);
        assert_eq!(
            actual.to_bits(),
            stored.to_bits(),
            "{name} alias spellings are distinct exact positions"
        );
    }
}
#[test]
fn chart_perimeter_cancels_partial_periodic_seam_overlaps() {
    let metric = metric();
    let west = polygon(
        &[(170.0, -10.0), (180.0, -10.0), (180.0, 10.0), (170.0, 10.0)],
        &[],
    );
    let east = polygon(
        &[(-180.0, 0.0), (-170.0, 0.0), (-170.0, 20.0), (-180.0, 20.0)],
        &[],
    );
    let west_perimeter = perimeter(&metric, &Shape::Polygon(west.clone()), metric.frame);
    let east_perimeter = perimeter(&metric, &Shape::Polygon(east.clone()), metric.frame);
    let combined = perimeter(
        &metric,
        &Shape::MultiPolygon(MultiPolygon::from_dense(vec![west, east]).unwrap()),
        metric.frame,
    );
    let shared_edge = metric
        .chart_edge(
            Point::new_raw_xy(-180.0, 0.0),
            Point::new_raw_xy(-180.0, 10.0),
        )
        .unwrap();
    let shared = metric.chart_edge_length(&shared_edge).unwrap();
    let expected = west_perimeter + east_perimeter - 2.0 * shared;
    assert!(
        (combined - expected).abs() <= 1.0e-8,
        "combined={combined:.17}, expected={expected:.17}"
    );
}
#[test]
fn stepped_and_hop_polar_closures_measure_one_parallel() {
    let metric = metric();
    let expected = analytic_parallel_circumference(&metric, 60.0);
    for longitude in [0.0, 40.0, -179.0, 180.0, -180.0] {
        for hop in [false, true] {
            let shape = Shape::Polygon(Polygon::new(
                ring(&north_closure(longitude, 60.0, hop)),
                Vec::new(),
            ));
            let actual = perimeter(&metric, &shape, metric.frame);
            assert!(
                (actual - expected).abs() <= 1.0e-8 * expected,
                "longitude={longitude}, hop={hop}: {actual:.12} != {expected:.12}"
            );
        }
    }
}
#[test]
fn south_polar_closure_measures_one_parallel() {
    let metric = metric();
    let shape = Shape::Polygon(Polygon::new(
        ring(&south_closure(40.0, 60.0, false)),
        Vec::new(),
    ));
    let expected = analytic_parallel_circumference(&metric, 60.0);
    let actual = perimeter(&metric, &shape, metric.frame);
    assert!(
        (actual - expected).abs() <= 1.0e-8 * expected,
        "south closure={actual:.12}, expected={expected:.12}"
    );
}
#[test]
fn clockwise_polar_closure_measures_one_parallel() {
    let metric = metric();
    let mut points = north_closure(40.0, 60.0, false);
    points.reverse();
    let shape = Shape::Polygon(Polygon::new(ring(&points), Vec::new()));
    let expected = analytic_parallel_circumference(&metric, 60.0);
    let actual = perimeter(&metric, &shape, metric.frame);
    assert!(
        (actual - expected).abs() <= 1.0e-8 * expected,
        "clockwise closure={actual:.12}, expected={expected:.12}"
    );
}
#[test]
fn oblique_and_noded_polar_closures_measure_one_parallel() {
    let metric = metric();
    let expected = analytic_parallel_circumference(&metric, 60.0);
    for noded in [false, true] {
        let shape = Shape::Polygon(Polygon::new(
            ring(&oblique_north_closure(0.0, 60.0, noded)),
            Vec::new(),
        ));
        let actual = perimeter(&metric, &shape, metric.frame);
        assert!(
            (actual - expected).abs() <= 1.0e-8 * expected,
            "noded={noded}: {actual:.12} != {expected:.12}"
        );
    }
}
#[test]
fn arbitrary_slope_lifted_copies_cancel_with_or_without_a_collinear_node() {
    let metric = metric();
    let expected = analytic_parallel_circumference(&metric, 60.0);
    for noded in [false, true] {
        let shape = Shape::Polygon(Polygon::new(
            ring(&arbitrary_cut_closure(noded)),
            Vec::new(),
        ));
        let actual = perimeter(&metric, &shape, metric.frame);
        assert!(
            (actual - expected).abs() <= 1.0e-8 * expected,
            "noded={noded}: {actual:.12} != {expected:.12}"
        );
    }
}
#[test]
fn fractional_collinear_cut_node_keeps_polar_closure_perimeter() {
    let metric = metric();
    let points = [
        (-248.0, -60.0),
        (-158.0, -60.0),
        (-68.0, -60.0),
        (22.0, -60.0),
        (112.0, -60.0),
        (114.0, -63.0),
        (118.0, -76.0),
        (135.0, -82.0),
        (128.0, -87.0),
        (-196.0, -90.0),
        (-106.0, -90.0),
        (-16.0, -90.0),
        (74.0, -90.0),
        (164.0, -90.0),
        (128.0, -87.0),
        (135.0, -82.0),
        (118.0, -76.0),
        (116.0, -69.5),
        (114.0, -63.0),
    ];
    let shape = Shape::Polygon(Polygon::new(ring(&points), Vec::new()));
    let expected = analytic_parallel_circumference(&metric, 60.0);
    let actual = perimeter(&metric, &shape, metric.frame);
    assert!(
        (actual - expected).abs() <= 1.0e-8 * expected,
        "noded polar closure={actual:.12}, expected={expected:.12}"
    );
}
#[test]
fn world_bands_and_explicit_period_cap_keep_their_parallel_boundaries() {
    let metric = metric();
    let one_parallel = analytic_parallel_circumference(&metric, 60.0);
    for longitude in [0.0, 40.0, -180.0] {
        let shape = Shape::Polygon(Polygon::new(
            ring(&full_longitude_band(longitude, -60.0, 60.0)),
            Vec::new(),
        ));
        let actual = perimeter(&metric, &shape, metric.frame);
        assert!(
            (actual - 2.0 * one_parallel).abs() <= 1.0e-8 * one_parallel,
            "band longitude={longitude}: {actual:.12} != {:.12}",
            2.0 * one_parallel
        );
    }

    let cap = Shape::Polygon(Polygon::new(
        ring(&[(0.0, 60.0), (360.0, 60.0), (360.0, 90.0), (0.0, 90.0)]),
        Vec::new(),
    ));
    let cap_length = perimeter(&metric, &cap, metric.frame);
    assert!(
        (cap_length - one_parallel).abs() <= 1.0e-8 * one_parallel,
        "explicit-period cap={cap_length:.12}, expected={one_parallel:.12}"
    );

    let shifted_band = Shape::Polygon(Polygon::new(
        ring(&[(40.0, -60.0), (400.0, -60.0), (400.0, 60.0), (40.0, 60.0)]),
        Vec::new(),
    ));
    let shifted_length = perimeter(&metric, &shifted_band, metric.frame);
    assert!(
        (shifted_length - 2.0 * one_parallel).abs() <= 1.0e-8 * one_parallel,
        "explicit-period band={shifted_length:.12}, expected={:.12}",
        2.0 * one_parallel
    );
}
#[test]
fn split_north_cap_at_eighty_degrees_has_the_analytic_parallel_perimeter() {
    let metric = metric();
    let cap = Shape::Polygon(Polygon::new(
        ring(&[
            (-180.0, 80.0),
            (-90.0, 80.0),
            (0.0, 80.0),
            (90.0, 80.0),
            (180.0, 80.0),
            (180.0, 90.0),
            (-180.0, 90.0),
        ]),
        Vec::new(),
    ));
    let actual = perimeter(&metric, &cap, metric.frame);
    let expected = analytic_parallel_circumference(&metric, 80.0);
    assert!(
        (actual - expected).abs() < 1.0,
        "N cap 80 perimeter={actual:.6} m, analytic={expected:.6} m"
    );
    assert!(
        (actual - 6_981_655.0).abs() < 1.0,
        "N cap 80 perimeter={actual:.6} m, expected about 6,981,655 m"
    );
}
#[test]
fn public_scalar_length_uses_the_polar_chart_cut_perimeter() {
    crate::test_support::initialize_python();
    let metric = metric();
    let cap = Polygon::new(
        ring(&[
            (-180.0, 80.0),
            (-90.0, 80.0),
            (0.0, 80.0),
            (90.0, 80.0),
            (180.0, 80.0),
            (180.0, 90.0),
            (-180.0, 90.0),
        ]),
        Vec::new(),
    );
    let geometry = crate::PyGeometry::wgs84(Shape::Polygon(cap));
    let actual = pyo3::Python::attach(|py| crate::length_natural_scalar(py, &geometry)).unwrap();
    let expected = analytic_parallel_circumference(&metric, 80.0);
    assert!(
        (actual - expected).abs() < 1.0,
        "public length={actual:.6} m, expected={expected:.6} m"
    );
}
#[test]
fn seam_split_and_mixed_orientation_keep_only_the_exterior_boundary() {
    let metric = metric();
    let shared = metric
        .chart_edge(
            Point::new_raw_xy(-180.0, -10.0),
            Point::new_raw_xy(-180.0, 10.0),
        )
        .unwrap();
    let shared_length = metric.chart_edge_length(&shared).unwrap();
    let stored_sum = raw_stored_perimeter(
        &metric,
        &multi_polygon(vec![
            box_polygon(170.0, -10.0, 180.0, 10.0),
            box_polygon(-180.0, -10.0, -170.0, 10.0),
        ]),
    );
    let expected = stored_sum - 2.0 * shared_length;
    for reverse_east in [false, true] {
        let orientation_case = if reverse_east {
            "E CW + W CCW"
        } else {
            "E CCW + W CCW"
        };
        let east = box_polygon(170.0, -10.0, 180.0, 10.0);
        let east_ring = if reverse_east {
            east.shell.reversed().unwrap()
        } else {
            east.shell
        };
        let east_member = Polygon::new(east_ring, Vec::new());
        let shape = multi_polygon(vec![east_member, box_polygon(-180.0, -10.0, -170.0, 10.0)]);
        let actual = perimeter(&metric, &shape, metric.frame);
        assert!(
            (actual - expected).abs() < 1.0e-7,
            "{orientation_case}: mixed seam perimeter={actual:.12}, expected={expected:.12}"
        );
    }
}
#[test]
fn partial_seam_overlap_cancels_only_its_shared_latitude_interval() {
    let metric = metric();
    let shape = multi_polygon(vec![
        box_polygon(170.0, -10.0, 180.0, 10.0),
        box_polygon(-180.0, 0.0, -170.0, 20.0),
    ]);
    let shared = metric
        .chart_edge(
            Point::new_raw_xy(-180.0, 0.0),
            Point::new_raw_xy(-180.0, 10.0),
        )
        .unwrap();
    let expected =
        raw_stored_perimeter(&metric, &shape) - 2.0 * metric.chart_edge_length(&shared).unwrap();
    let actual = perimeter(&metric, &shape, metric.frame);
    assert!(
        (actual - expected).abs() < 1.0e-7,
        "partial seam perimeter={actual:.12}, expected={expected:.12}"
    );
}
#[test]
fn seam_duplicates_retain_same_side_copies_and_reduce_east_east_west_to_two_sides() {
    let metric = metric();
    let east_twice = multi_polygon(vec![
        box_polygon(170.0, -10.0, 180.0, 10.0),
        box_polygon(170.0, -10.0, 180.0, 10.0),
    ]);
    let twice_east = perimeter(&metric, &east_twice, metric.frame);
    assert_eq!(
        twice_east.to_bits(),
        raw_stored_perimeter(&metric, &east_twice).to_bits()
    );

    let triple = multi_polygon(vec![
        box_polygon(170.0, -10.0, 180.0, 10.0),
        box_polygon(170.0, -10.0, 180.0, 10.0),
        box_polygon(-180.0, -10.0, -170.0, 10.0),
    ]);
    let shared = metric
        .chart_edge(
            Point::new_raw_xy(-180.0, -10.0),
            Point::new_raw_xy(-180.0, 10.0),
        )
        .unwrap();
    let expected =
        raw_stored_perimeter(&metric, &triple) - 2.0 * metric.chart_edge_length(&shared).unwrap();
    let actual = perimeter(&metric, &triple, metric.frame);
    assert!(
        (actual - expected).abs() < 1.0e-7,
        "E,E,W perimeter={actual:.12}, expected={expected:.12}"
    );
}
#[test]
fn hole_and_multipolygon_perimeters_keep_unshared_boundaries() {
    let metric = metric();
    let hole_coordinates = [(50.0, 70.0), (60.0, 70.0), (60.0, 80.0), (50.0, 80.0)];
    let donut = Shape::Polygon(Polygon::new(ring(&north_closure(40.0, 60.0, false)), vec![
        ring(&hole_coordinates),
    ]));
    let hole_shape = Shape::Polygon(Polygon::new(ring(&hole_coordinates), Vec::new()));
    let expected =
        analytic_parallel_circumference(&metric, 60.0) + raw_stored_perimeter(&metric, &hole_shape);
    let actual = perimeter(&metric, &donut, metric.frame);
    assert!(
        (actual - expected).abs() < 1.0e-7,
        "cap with hole={actual:.12}, expected={expected:.12}"
    );

    let cap = Polygon::new(ring(&north_closure(40.0, 60.0, false)), Vec::new());
    let multi = multi_polygon(vec![cap, box_polygon(100.0, 0.0, 110.0, 10.0)]);
    let expected = analytic_parallel_circumference(&metric, 60.0)
        + raw_stored_perimeter(
            &metric,
            &Shape::Polygon(box_polygon(100.0, 0.0, 110.0, 10.0)),
        );
    let actual = perimeter(&metric, &multi, metric.frame);
    assert!(
        (actual - expected).abs() < 1.0e-7,
        "multipolygon perimeter={actual:.12}, expected={expected:.12}"
    );
}
#[test]
fn ring_rotation_reversal_and_period_respelling_preserve_exact_perimeter() {
    let metric = metric();
    let original = north_closure(40.0, 60.0, false);
    let baseline_shape = Shape::Polygon(Polygon::new(ring(&original), Vec::new()));
    let baseline = perimeter(&metric, &baseline_shape, metric.frame);

    let rotated = original
        .iter()
        .cycle()
        .skip(3)
        .take(original.len())
        .copied()
        .collect::<Vec<_>>();
    let reversed = original.iter().copied().rev().collect::<Vec<_>>();
    let shifted = original
        .iter()
        .map(|&(x, y)| (x + 360.0, y))
        .collect::<Vec<_>>();
    let per_vertex = original
        .iter()
        .enumerate()
        .map(|(index, &(x, y))| {
            let turns = [0.0, 1.0, 0.0, -1.0, 1.0, 1.0, -1.0, 0.0, -1.0, 0.0];
            (x + turns[index] * 360.0, y)
        })
        .collect::<Vec<_>>();
    for (name, points) in [
        ("rotation", rotated),
        ("reversal", reversed),
        ("whole-period lift", shifted),
        ("per-vertex lift", per_vertex),
    ] {
        let shape = Shape::Polygon(Polygon::new(ring(&points), Vec::new()));
        let actual = perimeter(&metric, &shape, metric.frame);
        assert_eq!(
            actual.to_bits(),
            baseline.to_bits(),
            "{name} changed perimeter from {baseline:.17} to {actual:.17}"
        );
    }
}
#[test]
fn selected_grad_period_cancels_retraced_edges_within_a_ring() {
    let factor = AngularFactor::try_new("grad", std::f64::consts::PI / 200.0).unwrap();
    let frame = GeographicFrame::try_new(factor, factor, 0.0).unwrap();
    let metric = metric_with_flattening_and_frame(1.0 / 298.257_223_563, frame);
    let shape = Shape::Polygon(Polygon::new(
        ring(&[(40.0, 10.0), (50.0, 20.0), (450.0, 20.0), (440.0, 10.0)]),
        Vec::new(),
    ));
    let actual = perimeter(&metric, &shape, frame);
    let expected = analytic_parallel_circumference(&metric, 9.0)
        + analytic_parallel_circumference(&metric, 18.0);
    assert!(
        (actual - expected).abs() <= 1.0e-14 * expected,
        "actual={actual:.17}, expected={expected:.17}"
    );
}
#[test]
fn seam_spelling_at_five_hundred_forty_still_cancels_the_same_ring_pair() {
    let metric = metric();
    let shape = Shape::Polygon(Polygon::new(
        ring(&[
            (530.0, 0.0),
            (540.0, 0.0),
            (540.0, 20.0),
            (530.0, 20.0),
            (530.0, 15.0),
            (540.0, 15.0),
            (540.0, 5.0),
            (530.0, 5.0),
        ]),
        Vec::new(),
    ));
    let shared = metric
        .chart_edge(
            Point::new_raw_xy(-180.0, 5.0),
            Point::new_raw_xy(-180.0, 15.0),
        )
        .unwrap();
    let expected =
        raw_stored_perimeter(&metric, &shape) - 2.0 * metric.chart_edge_length(&shared).unwrap();
    let actual = perimeter(&metric, &shape, metric.frame);
    assert!(
        (actual - expected).abs() < 1.0e-7,
        "seam-spelled perimeter={actual:.12}, expected={expected:.12}"
    );
}
#[test]
fn a_later_multipolygon_ring_with_a_lifted_pair_is_not_screened_away() {
    let metric = metric();
    let first = box_polygon(10.0, -10.0, 20.0, 10.0);
    let closure = Polygon::new(ring(&north_closure(0.0, 50.0, false)), Vec::new());
    let shape = multi_polygon(vec![first.clone(), closure]);
    let expected = analytic_parallel_circumference(&metric, 50.0)
        + raw_stored_perimeter(&metric, &Shape::Polygon(first));
    let actual = perimeter(&metric, &shape, metric.frame);
    assert!(
        (actual - expected).abs() < 1.0e-7,
        "multipolygon perimeter={actual:.12}, expected={expected:.12}"
    );
}
#[test]
fn cached_bounds_and_longitude_fold_take_the_same_slow_perimeter_path() {
    let metric = metric();
    let cases = [
        Shape::Polygon(box_polygon(-10.0, -10.0, 20.0, 30.0)),
        Shape::Polygon(Polygon::new(
            ring(&oblique_north_closure(0.0, 60.0, true)),
            Vec::new(),
        )),
        Shape::Polygon(Polygon::new(
            ring(&full_longitude_band(40.0, -60.0, 60.0)),
            Vec::new(),
        )),
        multi_polygon(vec![
            box_polygon(170.0, -10.0, 180.0, 10.0),
            box_polygon(-180.0, -10.0, -170.0, 10.0),
        ]),
    ];
    let expanded_bounds = Bounds::new_raw(-1000.0, -90.0, 1000.0, 90.0);
    for shape in cases {
        let folded = perimeter(&metric, &shape, metric.frame);
        let cached_bounds = crate::geometry::geographic_polygonal_perimeter(
            &metric,
            &shape,
            metric.frame,
            shape.bounds(),
        )
        .unwrap();
        let forced_slow = crate::geometry::geographic_polygonal_perimeter(
            &metric,
            &shape,
            metric.frame,
            Some(expanded_bounds),
        )
        .unwrap();
        for actual in [cached_bounds, forced_slow] {
            assert!(
                (actual - folded).abs() <= 1.0e-12 * folded.abs().max(1.0),
                "screened={folded:.17}, cached/slow={actual:.17}"
            );
        }
    }
}
