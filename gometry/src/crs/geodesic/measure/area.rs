use super::*;

#[test]
fn chart_and_rectangular_cell_areas_match_high_precision_literals() {
    let metric = metric();
    let frame = metric.frame;
    let zone = polygon(
        &[(-45.0, 70.0), (45.0, 70.0), (45.0, 80.0), (-45.0, 80.0)],
        &[],
    );
    let zone_area = chart_polygon_area(&metric, &zone, frame).unwrap();
    let zone_expected = 2_899_441_975_904.581;
    assert!(
        (zone_area - zone_expected).abs() <= 1.0e-14 * zone_expected,
        "zone area={zone_area:.17}, expected={zone_expected:.17}"
    );
    let rectangular_cell_area = lonlat_rect_area_m2(Bounds::new_raw(-45.0, 70.0, 45.0, 80.0));
    assert!(
        (rectangular_cell_area - zone_expected).abs() <= 1.0e-14 * zone_expected,
        "rectangular cell area={rectangular_cell_area:.17}, expected={zone_expected:.17}"
    );

    let triangle = polygon(&[(0.0, 10.0), (30.0, 10.0), (15.0, 50.0)], &[]);
    let triangle_area = chart_polygon_area(&metric, &triangle, frame).unwrap();
    let triangle_expected = 6_706_052_330_164.59;
    assert!(
        (triangle_area - triangle_expected).abs() <= 1.0e-14 * triangle_expected,
        "triangle area={triangle_area:.17}, expected={triangle_expected:.17}"
    );
}
#[test]
#[expect(
    clippy::excessive_precision,
    reason = "40-digit mpmath oracle literals preserve the independent reference value"
)]
fn chart_area_series_matches_spherical_oblate_and_prolate_literals() {
    let frame = GeographicFrame::crs84();
    let zone = polygon(
        &[(-45.0, 70.0), (45.0, 70.0), (45.0, 80.0), (-45.0, 80.0)],
        &[],
    );
    let cases = [
            (
                0.0,
                2_882_901_463_210.015_326_402_293_007_066_545_690_845_072_588_580_157_504_759_345_263_7,
            ),
            (
                0.02,
                2_983_569_749_854.379_597_021_215_698_894_220_798_358_603_884_467_051_635_905_974_172_6,
            ),
            (
                -0.01,
                2_834_347_845_268.142_710_504_611_349_147_843_595_380_138_617_606_293_374_597_410_335_3,
            ),
            (
                -0.02,
                2_786_935_860_224.112_177_661_439_970_501_767_621_835_507_624_194_058_932_566_591_678_653_706_110_492_542_005_216_399_049_841_502_098_670_087_493_2,
            ),
        ];
    for (flattening, expected) in cases {
        let metric = metric_with_flattening(flattening);
        let actual = chart_polygon_area(&metric, &zone, frame).unwrap();
        assert!(
            (actual - expected).abs() <= 1.0e-14 * expected,
            "flattening={flattening}: actual={actual:.17}, expected={expected:.17}"
        );
    }
}
#[test]
fn chart_area_and_perimeter_are_invariant_between_degree_grad_and_radian_frames() {
    let degree_polygon: [(f64, f64); 4] =
        [(-45.0, 70.0), (45.0, 70.0), (45.0, 80.0), (-45.0, 80.0)];
    let grad_factor = AngularFactor::try_new("grad", std::f64::consts::PI / 200.0).unwrap();
    let grad_frame = GeographicFrame::try_new(grad_factor, grad_factor, 0.0).unwrap();
    let radian_factor = AngularFactor::try_new("radian", 1.0).unwrap();
    let radian_frame = GeographicFrame::try_new(radian_factor, radian_factor, 0.0).unwrap();
    let grad_polygon =
        degree_polygon.map(|(longitude, latitude)| (longitude * 10.0 / 9.0, latitude * 10.0 / 9.0));
    let radian_polygon =
        degree_polygon.map(|(longitude, latitude)| (longitude.to_radians(), latitude.to_radians()));

    let degree_metric = metric_with_flattening(1.0 / 298.257_223_563);
    let grad_metric = metric_with_flattening_and_frame(1.0 / 298.257_223_563, grad_frame);
    let radian_metric = metric_with_flattening_and_frame(1.0 / 298.257_223_563, radian_frame);
    let degree_shape = polygon(&degree_polygon, &[]);
    let grad_shape = polygon(&grad_polygon, &[]);
    let radian_shape = polygon(&radian_polygon, &[]);
    let degree_area =
        chart_polygon_area(&degree_metric, &degree_shape, GeographicFrame::crs84()).unwrap();
    let grad_area = chart_polygon_area(&grad_metric, &grad_shape, grad_frame).unwrap();
    let radian_area = chart_polygon_area(&radian_metric, &radian_shape, radian_frame).unwrap();
    let degree_length = perimeter(
        &degree_metric,
        &Shape::Polygon(degree_shape),
        GeographicFrame::crs84(),
    );
    let grad_length = perimeter(&grad_metric, &Shape::Polygon(grad_shape), grad_frame);
    let radian_length = perimeter(&radian_metric, &Shape::Polygon(radian_shape), radian_frame);

    for actual in [grad_area, radian_area] {
        assert!((actual - degree_area).abs() <= 1.0e-14 * degree_area);
    }
    for actual in [grad_length, radian_length] {
        assert!((actual - degree_length).abs() <= 1.0e-14 * degree_length);
    }
}
#[test]
fn chart_polar_area_maps_native_poles_to_physical_hemispheres() {
    let expected = 3_908_572_761_836.564_5;
    let frame = GeographicFrame::try_new_with_orientation(
        AngularFactor::try_new("west longitude grad", std::f64::consts::TAU / 400.0).unwrap(),
        AngularFactor::try_new("south latitude radian", 1.0).unwrap(),
        0.0,
        GeographicAxisOrientation::from_axis_directions(false, false),
    )
    .unwrap();
    let metric = metric_with_flattening_and_frame(1.0 / 298.257_223_563, frame);
    let longitudes = [0.0, 60.0, 120.0, 180.0, -120.0, -60.0];

    for (name, physical_latitude) in [("north", 80.0_f64), ("south", -80.0)] {
        let native_latitude = -physical_latitude.to_radians();
        let points = longitudes.map(|longitude| (-longitude * 10.0 / 9.0, native_latitude));
        let polygon = polygon(&points, &[]);
        let area = chart_polygon_area(&metric, &polygon, frame).unwrap();
        assert!(
            (area - expected).abs() <= 3.0e-15 * expected,
            "{name} cap area={area:.17}, expected={expected:.17}"
        );
    }
}
#[test]
#[expect(
    clippy::excessive_precision,
    reason = "40-digit mpmath oracle literal preserves the independent reference value"
)]
fn chart_area_subtracts_holes_against_high_precision_literals() {
    let metric = metric();
    let frame = metric.frame;
    let donut = polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0)], &[&[
        (1.0, 1.0),
        (3.0, 1.0),
        (3.0, 3.0),
        (1.0, 3.0),
    ]]);
    let area = chart_polygon_area(&metric, &donut, frame).unwrap();
    let expected =
        147_584_819_008.449_501_378_451_988_766_978_276_231_871_241_968_190_528_449_191_370_78;
    assert!(
        (area - expected).abs() <= 1.0e-13 * expected,
        "donut area={area:.17}, expected={expected:.17}"
    );
}
#[test]
#[expect(
    clippy::excessive_precision,
    reason = "Independent Decimal integral of the exact stored binary64 endpoints"
)]
fn chart_area_integrates_slanted_thin_rings_from_original_endpoints() {
    let metric = metric();
    let frame = metric.frame;
    let points = [
        (13.0, 52.5),
        (13.001, 52.501),
        (13.001, 52.501_000_000_1),
        (13.0, 52.500_000_000_1),
    ];
    let expected =
            0.000_755_688_547_419_930_258_991_880_480_619_221_115_460_111_464_140_957_137_933_622_464_683_107_255_206_770_415_967_871_802_785_631_464_917_801_071_65;
    let cases = [
        points.to_vec(),
        points.iter().copied().rev().collect(),
        points[1..]
            .iter()
            .chain(points[..1].iter())
            .copied()
            .collect(),
    ];
    for vertices in cases {
        let polygon = Polygon::new(ring(&vertices), Vec::new());
        let actual = chart_polygon_area(&metric, &polygon, frame).unwrap();
        assert!(
            (actual - expected).abs() <= 1.0e-14 * expected,
            "slanted thin-ring area={actual:.17}, expected={expected:.17}"
        );
    }
}
#[test]
#[expect(
    clippy::excessive_precision,
    reason = "Independent Decimal integral uses exact original endpoint differences"
)]
fn chart_area_combines_shell_and_hole_moments_before_rounding() {
    let metric = metric();
    let frame = metric.frame;
    let shell = [(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0)];
    let hole = [
        (1.0e-12, 1.0e-12),
        (4.0 - 1.0e-12, 1.0e-12),
        (4.0 - 1.0e-12, 4.0 - 1.0e-12),
        (1.0e-12, 4.0 - 1.0e-12),
    ];
    let expected =
            0.196_759_322_348_571_707_015_499_580_822_847_489_025_311_059_800_594_392_006_782_042_778_536_997_321_310_519_898_656_288_031_956_50;
    for vertices in [hole.to_vec(), hole.iter().copied().rev().collect()] {
        let polygon = Polygon::new(ring(&shell), vec![ring(&vertices)]);
        let actual = chart_polygon_area(&metric, &polygon, frame).unwrap();
        assert!(
            (actual - expected).abs() <= 1.0e-14 * expected,
            "thin donut area={actual:.17}, expected={expected:.17}"
        );
    }
}
#[test]
#[expect(
    clippy::excessive_precision,
    reason = "Independent 150-digit spherical integral for the original binary64 lobe endpoints"
)]
fn chart_area_does_not_infer_zero_from_an_initial_zero_moment_prefix() {
    let metric = metric_with_flattening(0.0);
    let frame = metric.frame;
    let mut points = vec![(0.0, 0.0)];
    for (index, weight) in [1.0, 4.0, 6.0, 4.0, 1.0].into_iter().enumerate() {
        let low = index as f64 / 8.0;
        let high = low + 1.0 / 256.0;
        let width = weight / 256.0;
        let mut rectangle = vec![
            (0.0, low),
            (width, low),
            (width, high),
            (0.0, high),
            (0.0, low),
        ];
        if index % 2 != 0 {
            rectangle.reverse();
        }
        points.extend(rectangle);
        points.push((0.0, 0.0));
    }
    let lobe_walk = Polygon::new(ring(&points), Vec::new());
    let expected = 4.283_585_800_751_005_345_637_248_483_996_184_8e-6;
    let actual = chart_polygon_area(&metric, &lobe_walk, frame).unwrap();
    assert!(
        actual > 0.0 && (actual - expected).abs() <= 1.0e-12 * expected,
        "lobe-walk area={actual:.17e}, expected={expected:.17e}"
    );
}
#[test]
#[expect(
    clippy::excessive_precision,
    reason = "Independent Decimal integral preserves stored invalid-ring winding"
)]
fn chart_area_uses_weighted_metric_sign_for_invalid_stored_walks() {
    let metric = metric();
    let frame = metric.frame;
    let planar_zero = polygon(&[(0.0, 0.0), (4.0, 4.0), (0.0, 4.0), (4.0, 0.0)], &[]);
    let planar_zero_expected =
            77_829_497.353_282_223_887_503_117_299_501_910_907_592_474_278_130_301_146_780_577_797_862_575_809_591_551_530_425_262_458_539_208_912_271_014_29;
    let area = chart_polygon_area(&metric, &planar_zero, frame).unwrap();
    assert!(
        (area - planar_zero_expected).abs() <= 1.0e-14 * planar_zero_expected,
        "planar-zero bowtie area={area:.17}, expected={planar_zero_expected:.17}"
    );

    let opposite_sign = polygon(&[(0.0, 0.0), (4.0, 4.0), (0.0, 4.0), (3.9999, 0.0)], &[]);
    let opposite_sign_expected = 75_368_655.925_641_673;
    let area = chart_polygon_area(&metric, &opposite_sign, frame).unwrap();
    assert!(
        (area - opposite_sign_expected).abs() <= 1.0e-14 * opposite_sign_expected,
        "opposite-sign bowtie area={area:.17}, expected={opposite_sign_expected:.17}"
    );

    let cancelled = polygon(
        &[
            (0.0, 0.0),
            (4.0, 0.0),
            (4.0, 4.0),
            (0.0, 4.0),
            (0.0, 0.0),
            (0.0, 4.0),
            (4.0, 4.0),
            (4.0, 0.0),
        ],
        &[],
    );
    assert_eq!(chart_polygon_area(&metric, &cancelled, frame).unwrap(), 0.0);
}
#[test]
fn chart_area_retraced_signed_zero_publishes_positive_zero() {
    let metric = metric();
    let retraced = polygon(
        &[
            (-0.0, 0.0),
            (4.0, 0.0),
            (4.0, 4.0),
            (0.0, 4.0),
            (0.0, 0.0),
            (0.0, 4.0),
            (4.0, 4.0),
            (4.0, 0.0),
            (0.0, 0.0),
        ],
        &[],
    );

    assert_eq!(
        chart_polygon_area(&metric, &retraced, metric.frame)
            .unwrap()
            .to_bits(),
        0.0_f64.to_bits()
    );
}
#[test]
#[expect(
    clippy::excessive_precision,
    reason = "Independent Decimal integral retains an outlying invalid hole"
)]
fn chart_area_subtracts_outlying_holes_as_stored() {
    let metric = metric();
    let frame = metric.frame;
    let invalid = polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0)], &[&[
        (20.0, -20.0),
        (30.0, -20.0),
        (30.0, -10.0),
        (20.0, -10.0),
    ]]);
    let expected = -991_762_400_434.761_110_524_391_469_4;
    let actual = chart_polygon_area(&metric, &invalid, frame).unwrap();
    assert!(
        (actual - expected).abs() <= 1.0e-14 * expected.abs(),
        "outlying-hole area={actual:.17}, expected={expected:.17}"
    );
}
#[test]
#[expect(
    clippy::excessive_precision,
    reason = "40-digit mpmath oracle literal preserves the independent reference value"
)]
fn chart_area_uses_typed_topology_poles_and_rejects_ambiguous_turns() {
    let metric = metric();
    let frame = metric.frame;
    let north_cap = polygon(
        &[
            (-180.0, 80.0),
            (-90.0, 80.0),
            (0.0, 80.0),
            (90.0, 80.0),
            (180.0, 80.0),
        ],
        &[],
    );
    assert_eq!(
        ring_bounded_pole(&north_cap.shell, frame),
        Some(crate::geographic::ChartPole::North)
    );
    let north_cap_area = chart_polygon_area(&metric, &north_cap, frame).unwrap();
    let north_cap_expected =
        3_908_572_761_836.572_209_792_944_327_893_186_980_909_264_547_994_116_673_544_607_356_9;
    assert!(
        (north_cap_area - north_cap_expected).abs() <= 1.0e-14 * north_cap_expected,
        "north cap area={north_cap_area:.17}, expected={north_cap_expected:.17}"
    );

    let straddling = polygon(
        &[
            (-180.0, 20.0),
            (-90.0, -20.0),
            (0.0, 20.0),
            (90.0, -20.0),
            (180.0, 20.0),
        ],
        &[],
    );
    let shape = Shape::Polygon(straddling);
    assert!(matches!(
        crate::geographic::ScalarGeographicView::try_new(frame, &shape),
        Err(diagnostic)
            if matches!(
                diagnostic.source,
                crate::geographic::GeographicError::AmbiguousFullTurnRing { .. }
            )
    ));
}
#[test]
#[expect(
    clippy::excessive_precision,
    reason = "Independent Decimal integrals pin periodic bands and multi-turn source walks"
)]
fn chart_area_preserves_world_circle_and_multi_turn_walks() {
    let metric = metric();
    let frame = metric.frame;
    let world_band = polygon(&full_longitude_band(40.0, -60.0, 60.0), &[]);
    let expected_world_band =
            441_233_920_694_280.737_992_025_912_931_493_155_169_195_458_212_945_862_801_508_295_145_604_032_216_568_603_916_565_391_645_360_359_680_669_911_06;
    let actual = chart_polygon_area(&metric, &world_band, frame).unwrap();
    assert!(
        (actual - expected_world_band).abs() <= 1.0e-14 * expected_world_band,
        "world-band area={actual:.17}, expected={expected_world_band:.17}"
    );

    let circle_cap = polygon(
        &[(40.0, 60.0), (400.0, 60.0), (400.0, 90.0), (40.0, 90.0)],
        &[],
    );
    let expected_circle_cap = 34_415_850_514_903.877_978_912_786_498_372;
    let actual = chart_polygon_area(&metric, &circle_cap, frame).unwrap();
    assert!(
        (actual - expected_circle_cap).abs() <= 1.0e-14 * expected_circle_cap,
        "Circle cap area={actual:.17}, expected={expected_circle_cap:.17}"
    );

    let south_cap = polygon(
        &[(0.0, -80.0), (90.0, -80.0), (180.0, -80.0), (270.0, -80.0)],
        &[],
    );
    let expected_south_cap = 3_908_572_761_836.572;
    let actual = chart_polygon_area(&metric, &south_cap, frame).unwrap();
    assert!(
        (actual - expected_south_cap).abs() <= 1.0e-14 * expected_south_cap,
        "south cap area={actual:.17}, expected={expected_south_cap:.17}"
    );

    let multi_turn = polygon(
        &[
            (0.0, 1.0),
            (120.0, 1.0),
            (240.0, 1.0),
            (360.0, 1.0),
            (480.0, 1.0),
            (600.0, 1.0),
            (0.0, 1.0),
        ],
        &[],
    );
    let expected_multi_turn = 501_203_527_720_426.234_056_243_624_791_2;
    let actual = chart_polygon_area(&metric, &multi_turn, frame).unwrap();
    assert!(
        (actual - expected_multi_turn).abs() <= 1.0e-14 * expected_multi_turn,
        "multi-turn area={actual:.17}, expected={expected_multi_turn:.17}"
    );
}
#[test]
fn chart_area_uses_the_summary_pole_when_first_vertex_is_on_the_equator() {
    let metric = metric();
    let frame = metric.frame;
    let ring = ring(&[(0.0, 0.0), (90.0, -80.0), (180.0, -80.0), (270.0, -80.0)]);
    let summary = crate::geographic::RingSummary::try_new(
        frame,
        &crate::geographic::GeographicRing::from_ring(&ring),
    )
    .unwrap();
    assert_eq!(
        summary.bounded_pole(),
        Some(crate::geographic::ChartPole::South)
    );

    let polygon = Polygon::new(ring, Vec::new());
    let actual = chart_polygon_area(&metric, &polygon, frame).unwrap();
    let expected = 54_138_230_010_739.305;
    assert!(
        (actual - expected).abs() <= 1.0e-14 * expected,
        "south-bounded turn area={actual:.17}, expected={expected:.17}"
    );
}
#[test]
fn chart_cap_closure_uses_the_native_summary_pole_under_axis_reversal() {
    for (longitude_east, latitude_north) in
        [(true, true), (false, true), (true, false), (false, false)]
    {
        let frame = oriented_degree_frame(longitude_east, latitude_north);
        let metric = metric_with_flattening_and_frame(1.0 / 298.257_223_563, frame);
        for latitude in [80.0, -80.0] {
            let cap = polygon(
                &[(0.0, latitude), (120.0, latitude), (-120.0, latitude)],
                &[],
            );
            let summary = crate::geographic::RingSummary::try_new(
                frame,
                &crate::geographic::GeographicRing::from_ring(&cap.shell),
            )
            .unwrap();
            assert_eq!(
                summary.bounded_pole(),
                Some(if latitude > 0.0 {
                    crate::geographic::ChartPole::North
                } else {
                    crate::geographic::ChartPole::South
                })
            );
            let actual = chart_polygon_area(&metric, &cap, frame).unwrap();
            let expected = 3_908_572_761_836.572;
            assert!(
                (actual - expected).abs() <= 1.0e-14 * expected,
                "cap area={actual:.17}, expected={expected:.17}, axes=({longitude_east}, {latitude_north}), chart latitude={latitude}"
            );
        }
    }

    let frame = oriented_degree_frame(true, false);
    let metric = metric_with_flattening_and_frame(1.0 / 298.257_223_563, frame);
    let equatorial_start = polygon(
        &[(0.0, 0.0), (90.0, 80.0), (180.0, 80.0), (270.0, 80.0)],
        &[],
    );
    let actual = chart_polygon_area(&metric, &equatorial_start, frame).unwrap();
    let expected = 54_138_230_010_739.305;
    assert!(
        (actual - expected).abs() <= 1.0e-14 * expected,
        "southward-axis cap area={actual:.17}, expected={expected:.17}"
    );
}
#[test]
fn chart_area_reference_phase_uses_physical_poles_for_axis_orientations() {
    const EXPECTED: f64 = 2.198_605_450_772_230_784_075_320_676_741_271_294_1e-22;

    for (longitude_east, latitude_north) in
        [(true, true), (false, true), (true, false), (false, false)]
    {
        let frame = oriented_degree_frame(longitude_east, latitude_north);
        let metric = metric_with_flattening_and_frame(1.0 / 298.257_223_563, frame);
        for pole in [GeodeticPole::North, GeodeticPole::South] {
            let native_pole = frame.native_latitude_pole(pole);
            let equatorward = if native_pole.is_sign_positive() {
                native_pole.next_down()
            } else {
                native_pole.next_up()
            };
            let rectangle = polygon(
                &[
                    (13.0, native_pole),
                    (13.01, native_pole),
                    (13.01, equatorward),
                    (13.0, equatorward),
                ],
                &[],
            );
            let actual = chart_polygon_area(&metric, &rectangle, frame).unwrap();
            assert!(
                actual.is_finite()
                    && actual > 0.0
                    && (actual - EXPECTED).abs() <= 1.0e-14 * EXPECTED,
                "one-ULP {pole:?} area={actual:.17e}, expected={EXPECTED:.17e}, axes=({longitude_east}, {latitude_north})"
            );
        }
    }
}
#[test]
#[expect(
    clippy::excessive_precision,
    reason = "Independent Decimal area uses custom native angular units"
)]
fn chart_area_preserves_custom_angular_units() {
    let unit = AngularFactor::try_new("custom angular unit", 0.003).unwrap();
    let frame = GeographicFrame::try_new(unit, unit, 0.0).unwrap();
    let metric = metric_with_flattening_and_frame(1.0 / 298.257_223_563, frame);
    let scale = (std::f64::consts::PI / 180.0) / 0.003;
    let shell =
        [(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0)].map(|(x, y)| (x * scale, y * scale));
    let hole = [
        (1.0e-12, 1.0e-12),
        (4.0 - 1.0e-12, 1.0e-12),
        (4.0 - 1.0e-12, 4.0 - 1.0e-12),
        (1.0e-12, 4.0 - 1.0e-12),
    ]
    .map(|(x, y)| (x * scale, y * scale));
    let donut = Polygon::new(ring(&shell), vec![ring(&hole)]);
    let expected = 0.196_777_297_092_643_510_663_874_846_791_097_26;
    let actual = chart_polygon_area(&metric, &donut, frame).unwrap();
    assert!(
        (actual - expected).abs() <= 1.0e-14 * expected,
        "custom-unit donut area={actual:.17}, expected={expected:.17}"
    );
}
#[test]
fn chart_area_scales_underflowing_source_products_before_final_rounding() {
    let large_metric =
        metric_with_semimajor_and_frame(1.0e154, 1.0 / 298.257_223_563, GeographicFrame::crs84());
    let width = 1.0e-160;
    let tiny = box_polygon(0.0, 0.0, width, width);
    let actual = chart_polygon_area(&large_metric, &tiny, large_metric.frame).unwrap();
    let expected = 3.025_781_950_270_4e-16;
    assert!(actual.is_finite() && actual > 0.0);
    assert!(
        (actual - expected).abs() <= 1.0e-14 * expected,
        "scaled tiny area={actual:.17e}, expected={expected:.17e}"
    );

    let subnormal_metric = metric();
    let subnormal = box_polygon(0.0, 0.0, width, width);
    let actual = chart_polygon_area(&subnormal_metric, &subnormal, subnormal_metric.frame).unwrap();
    let expected = 1.230_907_207_929_5e-310;
    assert!(actual.is_finite() && actual > 0.0);
    assert!(
        (actual - expected).abs() <= 8.0 * f64::from_bits(1),
        "subnormal area={actual:.17e}, expected={expected:.17e}"
    );
}
#[test]
fn chart_area_preserves_a_finite_result_after_the_axis_square_overflows() {
    let metric = metric_with_semimajor_and_frame(1.0e155, 0.0, GeographicFrame::crs84());
    let square = box_polygon(
        0.0,
        0.0,
        1.296_455_495_266_232_8e-151,
        1.296_455_495_266_232_8e-151,
    );
    assert!(!(metric.geodesic.a * metric.geodesic.a).is_finite());
    let actual = chart_polygon_area(&metric, &square, metric.frame).unwrap();
    let expected = 51_200.000_000_000_01;
    assert!(
        actual.is_finite() && (actual / expected - 1.0).abs() <= 1.0e-14,
        "finite area after overflowing a²={actual:.17}, expected={expected:.17}"
    );
}
