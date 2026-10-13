use super::support::{
    assert_chart_edge_point_hausdorff, assert_public_hausdorff_work_scaling,
    directed_chart_hausdorff, parallel_point_maximum_oracle, public_hausdorff_work,
};
use super::*;

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
fn geographic_hausdorff_parallel_pole_plateaus_close_with_second_order_bounds() -> Result<()> {
    let source = line(vec![p(0.0, 85.0), p(179.0, 85.0)]);
    let geodesic = Geodesic::wgs84();
    let cases = [
        ("colatitude_0_01_degree", 0.01),
        ("colatitude_0_1_degree", 0.1),
        ("colatitude_1_degree", 1.0),
        ("colatitude_3_degrees", 3.0),
    ];
    with_ellipsoid_metric("EPSG:4326", &[&source], |metric| {
        for (name, colatitude) in cases {
            let target = Shape::Point(p(10.0, 90.0 - colatitude));
            chart_hausdorff_counters::reset();
            let witness = directed_chart_hausdorff(&source, &target, metric);
            let evaluations = chart_hausdorff_counters::source_evaluations();
            let expected = parallel_point_maximum_oracle(&geodesic, colatitude);
            let attained = metric.segment_length(witness.source, witness.target);
            let scale = expected.max(witness.distance).max(1.0);
            let rounding = 32.0 * (scale.next_up() - scale);

            assert!(
                witness.distance <= expected + rounding,
                "{name}: reported {} above the independent maximum {expected}",
                witness.distance,
            );
            assert!(
                expected - witness.distance <= 0.01 + rounding,
                "{name}: reported {} below the independent maximum {expected}",
                witness.distance,
            );
            assert!(
                (attained - witness.distance).abs() <= rounding,
                "{name}: source/target witness distance {attained} does not attain {}",
                witness.distance,
            );
            assert!(
                evaluations <= 100,
                "{name}: source frontier used {evaluations} evaluations",
            );
        }
        Ok(())
    })?;
    Ok(())
}
#[test]
fn geographic_hausdorff_endpoint_maximum_is_attained() -> Result<()> {
    assert_chart_edge_point_hausdorff(
        "maximum at the source endpoint",
        p(0.0, 0.0),
        p(1.0, 0.0),
        p(10.0, 5.0),
    )
}
#[test]
fn geographic_hausdorff_short_way_seam_edge_uses_chart_interpolation() -> Result<()> {
    assert_chart_edge_point_hausdorff(
        "short-way seam crossing",
        p(179.5, 45.0),
        p(-179.5, 45.0),
        p(170.0, 43.0),
    )
}
#[test]
fn geographic_hausdorff_near_antipodal_point_target_is_attained() -> Result<()> {
    assert_chart_edge_point_hausdorff(
        "near-antipodal target",
        p(-1.0, 0.0),
        p(1.0, 0.0),
        p(178.0, 5.0),
    )
}
#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
fn geographic_hausdorff_closes_a_nearest_point_feature_switch() -> Result<()> {
    let source = line(vec![p(0.0, 0.0), p(10.0, 0.0)]);
    let target =
        Shape::geometry_collection(vec![Shape::Point(p(-1.0, 0.0)), Shape::Point(p(11.0, 0.0))])
            .expect("two-point target is valid");
    with_ellipsoid_metric("EPSG:4326", &[&source, &target], |metric| {
        let witness = directed_chart_hausdorff(&source, &target, metric);
        let expected = metric.segment_length(p(5.0, 0.0), p(-1.0, 0.0));
        assert!((witness.distance - expected).abs() <= 0.01);
        assert!((witness.source.x - 5.0).abs() <= 1.0e-8);
        Ok(())
    })?;
    Ok(())
}
#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
fn geographic_hausdorff_keeps_a_farther_reverse_point_minimum() -> Result<()> {
    let source = line(vec![p(0.0, 0.0), p(10.0, 0.0)]);
    let target = Shape::geometry_collection(vec![
        Shape::Point(p(-1.0, 0.0)),
        Shape::Point(p(170.0, 0.0)),
    ])
    .expect("two-point target is valid");
    with_ellipsoid_metric("EPSG:4326", &[&source, &target], |metric| {
        // The 160-degree equatorial arc to the eastern source endpoint
        // dominates every source-to-target minimum.
        let distance = source.chart_hausdorff_distance(&target, metric)?;
        assert!(
            (distance - 17_811_118.526_923_772).abs() <= 0.01,
            "reverse point maximum was {distance}; expected the 160-degree equatorial arc"
        );
        Ok(())
    })?;
    Ok(())
}
#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
fn geographic_hausdorff_closes_an_exact_polar_parallel_family() -> Result<()> {
    let source = line(vec![p(-45.0, 85.0), p(45.0, 85.0)]);
    let pole = Shape::Point(p(10.0, 90.0));
    with_ellipsoid_metric("EPSG:4326", &[&source, &pole], |metric| {
        chart_hausdorff_counters::reset();
        let witness = directed_chart_hausdorff(&source, &pole, metric);
        let evaluations = chart_hausdorff_counters::source_evaluations();
        let expected = metric.segment_length(p(0.0, 85.0), p(10.0, 90.0));
        assert!((witness.distance - expected).abs() <= 0.01);
        assert!(
            evaluations <= 3,
            "polar family used {evaluations} evaluations"
        );
        Ok(())
    })?;
    Ok(())
}
#[test]
fn geographic_hausdorff_plateau_uses_native_grad_and_radian_frames() {
    let geodesic = Geodesic::wgs84();
    let expected = 569_423.751_409_724;
    for (name, unit_name, radians_per_unit) in [
        ("grad", "grad", std::f64::consts::PI / 200.0),
        ("radian", "radian", 1.0),
    ] {
        let unit = AngularFactor::try_new(unit_name, radians_per_unit)
            .expect("angular frame unit is valid");
        let frame = GeographicFrame::try_new(unit, unit, 0.0).expect("native frame is valid");
        let to_native = (std::f64::consts::PI / 180.0) / radians_per_unit;
        let source = line(vec![
            p(0.0, 85.0 * to_native),
            p(179.0 * to_native, 85.0 * to_native),
        ]);
        let target = Shape::Point(p(10.0 * to_native, 89.9 * to_native));
        let metric = crate::crs::EllipsoidMetric::for_geodesic_at_frame(&geodesic, 0, frame);

        chart_hausdorff_counters::reset();
        let witness = directed_chart_hausdorff(&source, &target, &metric);
        let evaluations = chart_hausdorff_counters::source_evaluations();
        assert!(
            (witness.distance - expected).abs() <= 0.01,
            "{name}: chart-edge plateau was {}; expected {expected}",
            witness.distance,
        );
        assert!(
            (metric.segment_length(witness.source, witness.target) - witness.distance).abs()
                <= 1.0e-7,
            "{name}: chart-edge witness does not attain the reported distance",
        );
        assert!(
            evaluations <= 100,
            "{name}: {evaluations} source evaluations"
        );
    }
}
#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
fn geographic_hausdorff_returns_exact_zero_for_reversed_equal_chart_line() -> Result<()> {
    let original = line(vec![p(-10.0, 30.0), p(15.0, 40.0)]);
    let reversed = line(vec![p(15.0, 40.0), p(-10.0, 30.0)]);
    with_ellipsoid_metric("EPSG:4326", &[&original, &reversed], |metric| {
        assert_eq!(original.chart_hausdorff_distance(&reversed, metric)?, 0.0);
        Ok(())
    })?;
    Ok(())
}
#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions define the test oracle while Result propagates setup errors"
)]
fn geographic_hausdorff_pole_equality_folds_exterior_latitude_before_bounds() -> Result<()> {
    let source = line(vec![p(0.0, 90.0), p(180.0, 90.0)]);
    let target = Shape::Point(p(0.0, 90.000_000_000_000_01));
    with_ellipsoid_metric("EPSG:4326", &[&source, &target], |metric| {
        assert_eq!(
            source.chart_hausdorff_distance(&target, metric)?.to_bits(),
            0.0_f64.to_bits(),
        );
        Ok(())
    })?;
    Ok(())
}
#[test]
fn public_mixed_unit_hausdorff_has_bounded_edge_search_work() {
    const MIXED_UNIT_CRS: &str = r#"GEOGCRS["West-radian south-grad WGS84",
 DATUM["World Geodetic System 1984",ELLIPSOID["WGS 84",6378137,298.257223563]],
 PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.017453292519943295]],
 CS[ellipsoidal,2],
 AXIS["Longitude",west,ORDER[1],ANGLEUNIT["radian",1]],
 AXIS["Latitude",south,ORDER[2],ANGLEUNIT["grad",0.015707963267948967]]]"#;
    let expected: f64 = Geodesic::wgs84().inverse(60.0, 180.0, 60.01, 180.0);

    for (name, perturb_longitude) in [("matching", false), ("one-ULP", true)] {
        let longitudes = (0..=8).map(|index| 170.0 + 2.5 * f64::from(index));
        let source_points = longitudes
            .clone()
            .map(|longitude| p(-longitude.to_radians(), -60.0 * 10.0 / 9.0))
            .collect();
        let target_points = longitudes
            .map(|longitude| {
                let native_longitude = -longitude.to_radians();
                p(
                    if perturb_longitude {
                        native_longitude.next_up()
                    } else {
                        native_longitude
                    },
                    -60.01 * 10.0 / 9.0,
                )
            })
            .collect();
        let source = line(source_points);
        let target = line(target_points);
        let work = public_hausdorff_work(MIXED_UNIT_CRS, source, target, 64, 10_000);

        assert!(
            work.source_evaluations <= 64,
            "{name}: public Hausdorff visited {} source parameters",
            work.source_evaluations
        );
        assert!(
            work.moving_evaluations <= 256,
            "{name}: public Hausdorff evaluated {} moving correspondences",
            work.moving_evaluations
        );
        assert!(
            work.target_candidates <= 10_000,
            "{name}: public Hausdorff visited {} BVH edge candidates",
            work.target_candidates
        );
        assert!(
            work.point_edge_samples <= 10_000,
            "{name}: public Hausdorff evaluated {} point-edge samples",
            work.point_edge_samples
        );
        assert!(
            (work.distance - expected).abs() <= 0.01,
            "{name}: {} vs {expected}",
            work.distance
        );
    }
}
#[test]
fn public_parallel_hausdorff_perturbations_have_bounded_work() {
    let cases = [
        ("exact", [10.0, 12.5]),
        ("one ULP", [10.0_f64.next_up(), 12.5_f64.next_up()]),
        ("1e-9 degree", [10.0 + 1.0e-9, 12.5 + 1.0e-9]),
        ("1e-6 degree", [10.0 + 1.0e-6, 12.5 + 1.0e-6]),
        ("different endpoint offsets", [
            10.0_f64.next_up(),
            12.5 + 1.0e-9,
        ]),
        ("reversed target", [12.5 + 1.0e-9, 10.0 + 1.0e-9]),
    ];
    let expected: f64 = Geodesic::wgs84().inverse(60.0, 0.0, 60.01, 0.0);

    for (name, target_longitudes) in cases {
        let source = line(vec![p(10.0, 60.0), p(12.5, 60.0)]);
        let target = line(vec![
            p(target_longitudes[0], 60.01),
            p(target_longitudes[1], 60.01),
        ]);
        let work = public_hausdorff_work("EPSG:4326", source, target, 64, 2_048);
        assert!(
            work.source_evaluations <= 64,
            "{name}: {} source parameters",
            work.source_evaluations
        );
        assert!(
            work.moving_evaluations <= 256,
            "{name}: {} moving correspondences",
            work.moving_evaluations
        );
        assert!(
            work.point_edge_samples <= 2_048,
            "{name}: {} point-edge samples",
            work.point_edge_samples
        );
        assert!(
            (work.distance - expected).abs() <= 0.01,
            "{name}: {} vs {expected}",
            work.distance
        );
    }
}
#[test]
fn public_hausdorff_refreshes_a_clipped_target_edge_correspondence() {
    let source = line(vec![p(10.0, 60.0), p(12.5, 60.0)]);
    let target = line(vec![p(10.8, 60.01), p(11.7, 60.01)]);
    let endpoint_distances: [f64; 2] = [
        Geodesic::wgs84().inverse(60.0, 10.0, 60.01, 10.8),
        Geodesic::wgs84().inverse(60.0, 12.5, 60.01, 11.7),
    ];
    let expected = endpoint_distances[0].max(endpoint_distances[1]);
    let work = public_hausdorff_work("EPSG:4326", source, target, 256, 20_000);

    assert!(work.source_evaluations <= 256, "{work:?}");
    assert!(work.moving_endpoint_minima <= 64, "{work:?}");
    assert!(work.point_edge_samples <= 20_000, "{work:?}");
    assert!(
        (work.distance - expected).abs() <= 0.01,
        "{work:?} vs {expected}"
    );
}
#[test]
fn public_hausdorff_maps_a_short_source_onto_the_target_edge_interior() {
    let source_edge = line(vec![p(10.5, 60.0), p(11.5, 60.0)]);
    let target = line(vec![p(10.0, 60.01), p(12.0, 60.01)]);
    let source = Shape::geometry_collection(vec![source_edge, target.clone()])
        .expect("source contains the short edge and the longer target support");
    let expected: f64 = Geodesic::wgs84().inverse(60.0, 10.5, 60.01, 10.5);
    let work = public_hausdorff_work("EPSG:4326", source, target, 64, 10_000);

    assert!(work.source_evaluations <= 64, "{work:?}");
    assert!(work.point_edge_samples <= 10_000, "{work:?}");
    assert!(
        (work.distance - expected).abs() <= 0.01,
        "{work:?} vs {expected}"
    );
}
#[test]
fn public_near_pole_hausdorff_has_bounded_symmetric_work() {
    let source = line(vec![p(170.0, 89.9), p(190.0, 89.91)]);
    let target = line(vec![p(170.000_001, 89.900_01), p(190.000_001, 89.910_01)]);
    let work = public_hausdorff_work("EPSG:4326", source, target, 64, 10_000);
    // Independent pyproj Geod oracle: target minima bracketed by scans of 257
    // and 1,025 points; source maxima by 129 and 513 samples plus local solves.
    let expected = 1.116_939_781_867_64;

    assert!(work.source_evaluations <= 64, "{work:?}");
    assert!(work.moving_evaluations <= 128, "{work:?}");
    assert!(work.moving_endpoint_minima <= 64, "{work:?}");
    assert!(work.target_candidates <= 64, "{work:?}");
    assert!(work.point_edge_samples <= 10_000, "{work:?}");
    assert!(
        (work.distance - expected).abs() <= 0.01,
        "{work:?} vs independent oracle {expected}"
    );
}
#[test]
fn public_hausdorff_refreshes_resegmented_target_parameters() {
    let source = line(vec![p(10.0, 60.0), p(12.5, 60.0)]);
    let target = Shape::geometry_collection(vec![
        line(vec![p(11.25, 60.01), p(12.5, 60.01)]),
        line(vec![p(10.0, 60.01), p(11.25, 60.01)]),
    ])
    .expect("reordered target segments form a valid linework collection");
    let expected: f64 = Geodesic::wgs84().inverse(60.0, 10.0, 60.01, 10.0);
    let work = public_hausdorff_work("EPSG:4326", source, target, 128, 20_000);

    assert!(work.source_evaluations <= 128, "{work:?}");
    assert!(work.moving_endpoint_minima <= 64, "{work:?}");
    assert!(work.point_edge_samples <= 20_000, "{work:?}");
    assert!(
        (work.distance - expected).abs() <= 0.01,
        "{work:?} vs {expected}"
    );
}
#[test]
fn public_hausdorff_keeps_candidate_edge_witnesses_separate_from_set_minima() {
    let source_edge = line(vec![p(10.5, 60.0), p(11.5, 60.0)]);
    let target_edge = line(vec![p(10.0, 60.01), p(12.0, 60.01)]);
    let endpoints = [Shape::Point(p(10.5, 60.0)), Shape::Point(p(11.5, 60.0))];
    let source = Shape::geometry_collection(vec![
        source_edge,
        target_edge.clone(),
        endpoints[0].clone(),
        endpoints[1].clone(),
    ])
    .expect("source collection retains both linework supports and endpoint points");
    let target = Shape::geometry_collection(vec![
        target_edge,
        endpoints[0].clone(),
        endpoints[1].clone(),
    ])
    .expect("target collection retains its line and the closer endpoint features");
    let expected: f64 = Geodesic::wgs84().inverse(60.0, 10.5, 60.01, 10.5);
    let work = public_hausdorff_work("EPSG:4326", source, target, 64, 10_000);

    assert!(work.source_evaluations <= 64, "{work:?}");
    assert!(work.point_edge_samples <= 10_000, "{work:?}");
    assert!(
        (work.distance - expected).abs() <= 0.01,
        "{work:?} vs {expected}"
    );
}
#[test]
fn public_hausdorff_does_not_use_another_feature_as_candidate_endpoint_witness() {
    let source = line(vec![p(10.0, 60.0), p(12.5, 60.0)]);
    let target = Shape::geometry_collection(vec![
        Shape::Point(p(10.0, 60.0)),
        line(vec![p(10.0, 60.01), p(12.5, 60.01)]),
    ])
    .expect("endpoint point and offset line form a valid target collection");
    let expected: f64 = Geodesic::wgs84().inverse(60.0, 10.0, 60.01, 10.0);
    let work = public_hausdorff_work("EPSG:4326", source, target, 64, 4_096);

    assert!(work.source_evaluations <= 64, "{work:?}");
    assert!(work.point_edge_samples <= 4_096, "{work:?}");
    assert!(
        (work.distance - expected).abs() <= 0.01,
        "{work:?} vs {expected}"
    );
}
#[test]
fn public_hausdorff_keeps_the_fixed_witness_fallback_without_target_edges() {
    const SPHERE_CRS: &str = "+proj=longlat +a=6378137 +b=6378137 +type=crs";
    let source = line(vec![p(-40.0, 0.0), p(40.0, 0.0)]);
    let target = Shape::Point(p(180.0, 0.0));
    let work = public_hausdorff_work(SPHERE_CRS, source, target, 4_096, 10_000);
    let expected = std::f64::consts::PI * 6_378_137.0;

    assert!(
        work.source_evaluations > 2,
        "the equatorial interior maximum must be sampled"
    );
    assert_eq!(work.moving_evaluations, 0);
    assert!((work.distance - expected).abs() <= 0.01);
}
#[test]
fn public_fixed_span_flat_hausdorff_work_scales_by_vertex_count() {
    assert_public_hausdorff_work_scaling("fixed-span flat", false, 60.0, 0.0);
}
#[test]
fn public_fixed_span_oblique_hausdorff_work_scales_by_vertex_count() {
    // Densify one oblique support against a fixed two-vertex support.
    assert_public_hausdorff_work_scaling("fixed-span oblique", false, 10.0, 0.02);
}
#[test]
fn public_growing_span_flat_hausdorff_work_scales_by_vertex_count() {
    assert_public_hausdorff_work_scaling("growing-span flat", true, 60.0, 0.0);
}
#[test]
fn public_growing_span_oblique_hausdorff_work_scales_by_vertex_count() {
    // This series uses locally ordered edges whose total support grows with n.
    assert_public_hausdorff_work_scaling("growing-span oblique", true, 10.0, 0.02);
}
