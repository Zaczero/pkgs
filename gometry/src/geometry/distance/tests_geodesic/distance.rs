use super::support::assert_chart_pair_contract;
use super::*;

#[test]
fn point_to_line_distance_and_nearest_witness_use_the_same_chart_edge() -> Result<()> {
    let point_shape = Shape::Point(p(0.0, 82.9));
    let line_shape = line(vec![p(-45.0, 80.0), p(45.0, 80.0)]);
    let point = PreparedPlan::new(point_shape.clone());
    let line_plan = PreparedPlan::new(line_shape.clone());
    let point_cache = FrameDependentCaches::default();
    let line_cache = FrameDependentCaches::default();
    with_ellipsoid_metric("EPSG:4326", &[&point_shape, &line_shape], |metric| {
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        let distance = point.geodesic_distance_cached(
            &point_cache,
            &line_plan,
            &line_cache,
            "EPSG:4326",
            semi_major,
            flattening,
            metric,
        )?;
        let known_distance = 323_839.508_746_922_83;
        let scale = distance.abs().max(known_distance).max(1.0);
        let rounding_floor = 32.0 * (scale.next_up() - scale);
        assert!(
            (distance - known_distance).abs() <= MINIMUM_GAP_METERS + rounding_floor,
            "point-to-chart-edge distance was {distance}"
        );

        let nearest = point
            .geodesic_nearest_points_cached(
                &point_cache,
                &line_plan,
                &line_cache,
                "EPSG:4326",
                semi_major,
                flattening,
                metric,
            )?
            .expect("nonempty point and line have nearest witnesses");
        let attained = metric.segment_length(nearest.0, nearest.1);
        assert_eq!(distance, attained);
        assert!((-45.0..=45.0).contains(&nearest.1.x));
        assert_eq!(nearest.1.y, 80.0);
        let reference = metric.segment_length(p(0.0, 82.9), p(0.0, 80.0));
        assert!(
            attained <= reference + MINIMUM_GAP_METERS,
            "attained {attained} exceeds the reference {reference} by more than \
             the Q8 gap {MINIMUM_GAP_METERS}"
        );

        assert!(!point.geodesic_dwithin_cached(
            &point_cache,
            &line_plan,
            &line_cache,
            "EPSG:4326",
            semi_major,
            flattening,
            metric,
            distance - 1.0,
        )?);
        assert!(point.geodesic_dwithin_cached(
            &point_cache,
            &line_plan,
            &line_cache,
            "EPSG:4326",
            semi_major,
            flattening,
            metric,
            distance + 1.0,
        )?);

        let touching_shape = Shape::Point(p(0.0, 80.0));
        let touching = PreparedPlan::new(touching_shape);
        let touching_cache = FrameDependentCaches::default();
        assert_eq!(
            touching.geodesic_distance_cached(
                &touching_cache,
                &line_plan,
                &line_cache,
                "EPSG:4326",
                semi_major,
                flattening,
                metric,
            )?,
            0.0
        );
        assert!(touching.geodesic_dwithin_cached(
            &touching_cache,
            &line_plan,
            &line_cache,
            "EPSG:4326",
            semi_major,
            flattening,
            metric,
            0.0,
        )?);
        Ok(())
    })?;
    Ok(())
}
#[test]
fn public_distance_measures_a_full_turn_line_as_its_parallel() {
    crate::test_support::initialize_python();
    Python::attach(|py| {
        let frame = Frame::new(Some(crs_arc_static("EPSG:4326")), None)
            .expect("CRS84 is a valid geographic frame");
        let point = crate::Typed(crate::PyGeometry::with_frame(
            Shape::Point(p(200.0, 50.0)),
            frame.clone(),
        ))
        .into_pyobject(py)
        .expect("point binds as its public typed geometry");
        let line = crate::Typed(crate::PyGeometry::with_frame(
            line(vec![p(10.0, 60.0), p(370.0, 60.0)]),
            frame,
        ))
        .into_pyobject(py)
        .expect("linestring binds as its public typed geometry");

        let distance = crate::distance(py, &point, &line, crate::NavigationPath::Geodesic, None)
            .expect("public distance succeeds")
            .bind(py)
            .extract::<f64>()
            .expect("distance is a scalar");
        let expected: f64 = Geodesic::wgs84().inverse(50.0, 200.0, 60.0, 200.0);
        let rounding_floor = 32.0 * (expected.next_up() - expected);
        assert!(
            (distance - expected).abs() <= MINIMUM_GAP_METERS + rounding_floor,
            "full-turn distance {distance} differs from the 60-degree parallel \
             meridian distance {expected}"
        );
    });
}
#[test]
fn point_to_chart_edge_nearest_witness_interpolates_ordinates_by_arclength() -> Result<()> {
    let point_shape = Shape::Point(p(0.0, 82.9));
    let line_shape = line(vec![
        Point::new_axes(-45.0, 80.0, ZOrdinate(Some(10.0)), MOrdinate(Some(100.0)))
            .expect("finite start ordinates"),
        Point::new_axes(45.0, 80.0, ZOrdinate(Some(30.0)), MOrdinate(Some(300.0)))
            .expect("finite end ordinates"),
    ]);
    with_ellipsoid_metric("EPSG:4326", &[&point_shape, &line_shape], |metric| {
        let point = PreparedPlan::new(point_shape.clone());
        let line_plan = PreparedPlan::new(line_shape.clone());
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        let nearest = point
            .geodesic_nearest_points_cached(
                &FrameDependentCaches::default(),
                &line_plan,
                &FrameDependentCaches::default(),
                "EPSG:4326",
                semi_major,
                flattening,
                metric,
            )?
            .expect("nonempty point and line have nearest witnesses");
        assert!((-45.0..=45.0).contains(&nearest.1.x));
        assert_eq!(nearest.1.y, 80.0);
        let reference = metric.segment_length(p(0.0, 82.9), p(0.0, 80.0));
        let attained = metric.segment_length(nearest.0, nearest.1);
        assert!(
            attained <= reference + MINIMUM_GAP_METERS,
            "attained {attained} exceeds the reference {reference} by more than \
             the Q8 gap {MINIMUM_GAP_METERS}"
        );
        let parameter = (nearest.1.x + 45.0) / 90.0;
        let expected_z = 10.0 + 20.0 * parameter;
        let expected_m = 100.0 + 200.0 * parameter;
        assert!((nearest.1.z().expect("target carries Z") - expected_z).abs() <= 1.0e-12);
        assert!((nearest.1.m().expect("target carries M") - expected_m).abs() <= 1.0e-10);

        let touching = PreparedPlan::new(Shape::Point(p(0.0, 80.0)));
        let touching_witness = touching
            .geodesic_nearest_points_cached(
                &FrameDependentCaches::default(),
                &line_plan,
                &FrameDependentCaches::default(),
                "EPSG:4326",
                semi_major,
                flattening,
                metric,
            )?
            .expect("the point lies on the chart edge");
        assert_eq!((touching_witness.0.x, touching_witness.0.y), (0.0, 80.0));
        assert_eq!((touching_witness.1.x, touching_witness.1.y), (0.0, 80.0));
        assert!((touching_witness.1.z().expect("target carries Z") - 20.0).abs() <= 1.0e-12);
        assert!((touching_witness.1.m().expect("target carries M") - 200.0).abs() <= 1.0e-10);
        Ok(())
    })?;
    Ok(())
}
#[test]
fn cached_point_pair_nearest_validates_both_geographic_operands() -> Result<()> {
    let valid_shape = Shape::Point(p(0.0, 0.0));
    let invalid_shape = Shape::Point(p(1.0, 95.0));
    let valid = PreparedPlan::new(valid_shape);
    let invalid = PreparedPlan::new(invalid_shape);
    with_ellipsoid_metric("EPSG:4326", &[], |metric| {
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        for (left, right) in [(&valid, &invalid), (&invalid, &valid)] {
            let error = left
                .geodesic_nearest_points_cached(
                    &FrameDependentCaches::default(),
                    right,
                    &FrameDependentCaches::default(),
                    "EPSG:4326",
                    semi_major,
                    flattening,
                    metric,
                )
                .expect_err("both point operands require geographic-domain admission");
            assert!(error.to_string().contains("invalid longitude/latitude"));
        }
        Ok(())
    })?;
    Ok(())
}
#[test]
fn chart_distance_property_six_covers_crossings_and_filled_polygons() -> Result<()> {
    let crossing = line(vec![p(-20.0, -10.0), p(30.0, 20.0)]);
    let crossing_other = line(vec![p(-15.0, 20.0), p(25.0, -10.0)]);
    let area = polygon(vec![
        p(-1.0, -1.0),
        p(1.0, -1.0),
        p(1.0, 1.0),
        p(-1.0, 1.0),
        p(-1.0, -1.0),
    ]);
    let inner = Shape::Point(p(0.0, 0.0));
    let outside = Shape::Point(p(2.0, 0.0));
    let overlap = polygon(vec![
        p(0.5, -0.5),
        p(1.5, -0.5),
        p(1.5, 0.5),
        p(0.5, 0.5),
        p(0.5, -0.5),
    ]);
    let separated = polygon(vec![
        p(2.0, -0.5),
        p(3.0, -0.5),
        p(3.0, 0.5),
        p(2.0, 0.5),
        p(2.0, -0.5),
    ]);

    with_ellipsoid_metric("EPSG:4326", &[], |metric| {
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        assert_chart_pair_contract(
            &crossing,
            &crossing_other,
            true,
            metric,
            semi_major,
            flattening,
        )?;
        assert_chart_pair_contract(&inner, &area, true, metric, semi_major, flattening)?;
        assert_chart_pair_contract(&outside, &area, false, metric, semi_major, flattening)?;
        assert_chart_pair_contract(&area, &overlap, true, metric, semi_major, flattening)?;
        assert_chart_pair_contract(&area, &separated, false, metric, semi_major, flattening)?;
        Ok(())
    })?;
    Ok(())
}
#[test]
fn chart_edge_contacts_at_high_latitudes_are_zero_distance() -> Result<()> {
    let cases = [
        (
            "tiny northern contact",
            [p(-0.0001, 89.5), p(0.0001, 89.5)],
            [p(0.0, 79.5), p(0.0, 89.5)],
        ),
        (
            "near north pole contact",
            [p(-0.001, 89.94), p(0.001, 89.94)],
            [p(0.0, 79.94), p(0.0, 89.94)],
        ),
        (
            "near south pole contact",
            [p(-0.001, -89.94), p(0.001, -89.94)],
            [p(0.0, -79.94), p(0.0, -89.94)],
        ),
    ];
    with_ellipsoid_metric("EPSG:4326", &[], |metric| {
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        for (name, parallel_points, meridian_points) in cases {
            let parallel = line(parallel_points.to_vec());
            let meridian = line(meridian_points.to_vec());
            assert_chart_pair_contract(&parallel, &meridian, true, metric, semi_major, flattening)
                .unwrap_or_else(|error| panic!("{name}: {error}"));
        }
        Ok(())
    })?;
    Ok(())
}
#[test]
fn fixed_short_parallel_pairs_have_the_meridional_minimum() -> Result<()> {
    let cases = [
        (
            "equatorial overlapping longitude intervals",
            [p(-0.2, 0.0), p(0.2, 0.0)],
            [p(-0.1, 0.02), p(0.3, 0.02)],
            [p(0.0, 0.0), p(0.0, 0.02)],
        ),
        (
            "high latitude overlapping longitude intervals",
            [p(-120.0, 80.0), p(-119.55, 80.0)],
            [p(-119.9, 80.19), p(-119.45, 80.19)],
            [p(-119.8, 80.0), p(-119.8, 80.19)],
        ),
        (
            "seam crossing overlapping longitude intervals",
            [p(179.8, 45.0), p(-179.8, 45.0)],
            [p(179.9, 45.02), p(-179.7, 45.02)],
            [p(180.0, 45.0), p(180.0, 45.02)],
        ),
    ];
    let geodesic = Geodesic::wgs84();
    with_ellipsoid_metric("EPSG:4326", &[], |metric| {
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        for (name, first_points, second_points, reference) in cases {
            let first_shape = line(first_points.to_vec());
            let second_shape = line(second_points.to_vec());
            let first = PreparedPlan::new(first_shape);
            let second = PreparedPlan::new(second_shape);
            let actual = first.geodesic_distance_cached(
                &FrameDependentCaches::default(),
                &second,
                &FrameDependentCaches::default(),
                "EPSG:4326",
                semi_major,
                flattening,
                metric,
            )?;
            let [a, b] = reference;
            let expected: f64 = geodesic.inverse(a.y, a.x, b.y, b.x);
            assert!(
                (actual - expected).abs() <= MINIMUM_GAP_METERS,
                "{name}: distance {actual} differs from meridional minimum {expected}"
            );
        }
        Ok(())
    })?;
    Ok(())
}
