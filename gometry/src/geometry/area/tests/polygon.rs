#[test]
fn packed_polygon_measure_preserves_extreme_semantic_values() {
    let mixed_xs = [0.0, f64::from_bits(1), 2.0_f64.powi(-20), 0.0];
    let mixed_ys = [0.0, f64::from_bits(2), 2.0_f64.powi(-20), 0.0];
    assert_eq!(
        polygon_area_measure_columns(&mixed_xs, &mixed_ys, &[0, 4], 0..1),
        0.0
    );

    let huge_xs = [0.0, 1e300, 1e300, 0.0, 0.0];
    let huge_ys = [0.0, 0.0, 1e300, 1e300, 0.0];
    let huge = polygon_area_measure_columns(&huge_xs, &huge_ys, &[0, 5], 0..1);
    assert!(huge.is_infinite() && huge.is_sign_positive());
}

#[test]
fn shell_hole_cancellation_rounds_the_exact_area_once() {
    type Columns<'a> = (&'a [f64], &'a [f64]);
    let (lo, hi) = (2.0_f64.powi(-60), 1.0 - 2.0_f64.powi(-53));
    let cases: [(Columns<'_>, Columns<'_>, f64); 2] = [
        // The shell bumps 2^-52 above its equal hole: both rings round to
        // one binary64 area, and their exact difference is 2^-53.
        (
            (&[0.0, 1.0, 1.0, 0.5, 0.0, 0.0], &[
                0.0,
                0.0,
                1.0,
                1.0 + 2.0_f64.powi(-52),
                1.0,
                0.0,
            ]),
            (&[0.0, 0.0, 1.0, 1.0, 0.0], &[0.0, 1.0, 1.0, 0.0, 0.0]),
            2.0_f64.powi(-53),
        ),
        // A valid thin annulus whose rounded rings leave a finite
        // difference with no reliable bits.
        (
            (&[0.0, 1.0, 1.0, 0.0, 0.0], &[0.0, 0.0, 1.0, 1.0, 0.0]),
            (&[lo, lo, hi, hi, lo], &[lo, hi, hi, lo, lo]),
            2.0_f64.powi(-52) + 2.0_f64.powi(-59),
        ),
    ];
    for ((shell_xs, shell_ys), (hole_xs, hole_ys), expected) in cases {
        let scalar = polygon_area_measure_with(|visit| {
            visit(shell_xs, shell_ys, false);
            visit(hole_xs, hole_ys, true);
        });
        assert_eq!(scalar.to_bits(), expected.to_bits(), "{scalar:e}");
        let xs = [shell_xs, hole_xs].concat();
        let ys = [shell_ys, hole_ys].concat();
        let offsets = [0, shell_xs.len() as i32, xs.len() as i32];
        let packed = polygon_area_measure_columns(&xs, &ys, &offsets, 0..2);
        assert_eq!(packed.to_bits(), expected.to_bits(), "{packed:e}");
    }
}

#[test]
fn packed_polygon_area_fuses_short_rings_without_losing_holes() {
    let xs = [
        500_000.0, 500_010.0, 500_010.0, 500_000.0, 500_000.0, 500_002.0, 500_002.0, 500_008.0,
        500_008.0, 500_002.0,
    ];
    let ys = [
        5_200_000.0,
        5_200_000.0,
        5_200_010.0,
        5_200_010.0,
        5_200_000.0,
        5_200_002.0,
        5_200_008.0,
        5_200_008.0,
        5_200_002.0,
        5_200_002.0,
    ];
    assert_eq!(
        polygon_area_measure_columns(&xs, &ys, &[0, 5, 10], 0..2),
        64.0
    );
}

#[test]
fn packed_polygon_area_falls_back_on_shell_hole_cancellation() {
    let xs = [0.0, 10.0, 10.0, 0.0, 0.0, 0.0, 10.0, 10.0, 0.0, 0.0];
    let ys = [0.0, 0.0, 10.0, 10.0, 0.0, 0.0, 0.0, 10.0, 10.0, 0.0];
    // The second ring has the same magnitude as the shell. An unsigned
    // ring fold would return 200 rather than the contract's zero.
    assert_eq!(
        polygon_area_measure_columns(&xs, &ys, &[0, 5, 10], 0..2),
        0.0
    );
}

#[test]
fn thin_open_cycle_and_closed_polygon_round_to_known_area() {
    let tiny = 2.0_f64.powi(-54);
    let open = [XY::new(1.0, 0.0), XY::new(0.0, 1.0), XY::new(tiny, 1.0)];
    assert_eq!(
        open_xy_cycle_magnitude(&open).get().to_bits(),
        2.0_f64.powi(-55).to_bits()
    );
    let open_points = [
        Point::new_raw_xy(1.0, 0.0),
        Point::new_raw_xy(0.0, 1.0),
        Point::new_raw_xy(tiny, 1.0),
    ];
    assert_eq!(
        open_point_cycle_magnitude(&open_points).get().to_bits(),
        2.0_f64.powi(-55).to_bits()
    );

    // The middle rounded cross product is zero, while the exact doubled
    // area is 2^-54. The packed owner must take its exact fallback.
    let closed_xs = [0.0, 1.0, 0.999_999_992_549_419_4, 0.0];
    let closed_ys = [0.0, 1.000_000_007_450_580_6, 1.0, 0.0];
    assert_eq!(
        polygon_area_measure_columns(&closed_xs, &closed_ys, &[0, 4], 0..1).to_bits(),
        2.0_f64.powi(-55).to_bits()
    );
}

#[test]
fn least_subnormal_rectangle_survives_scalar_polygon_area() {
    let tiny = f64::from_bits(1);
    let xs = [0.0, 1.0, 1.0, 0.0, 0.0];
    let ys = [0.0, 0.0, tiny, tiny, 0.0];
    let area = polygon_area_measure_with(|visit| visit(&xs, &ys, false));
    assert_eq!(area.to_bits(), tiny.to_bits());
}

#[test]
fn utm_scale_irregular_polygon_area_matches_shifted_reference() {
    // Translating before the independent binary64 fold avoids catastrophic
    // cancellation at UTM coordinates; the raw-coordinate fold below must
    // visibly lose precision on this four-edge ring.
    let xs = [500_000.1_f64, 500_020.3, 500_019.7, 500_000.5, 500_000.1];
    let ys = [
        5_200_000.2_f64,
        5_200_000.1,
        5_200_010.4,
        5_200_010.3,
        5_200_000.2,
    ];
    let area = polygon_area_measure_with(|visit| visit(&xs, &ys, false));
    let reference = shifted_scalar_area_reference(&xs, &ys).abs();
    let unshifted = xs
        .array_windows::<2>()
        .zip(ys.array_windows::<2>())
        .fold(0.0_f64, |area, ([x0, x1], [y0, y1])| {
            area + (x0 * y1 - x1 * y0)
        })
        .abs()
        / 2.0;
    assert!(
        (area - reference).abs() <= 1e-12 * reference.max(1.0),
        "area={area} reference={reference}"
    );
    assert!(
        (unshifted - reference).abs() > 1e-5,
        "unshifted={unshifted} reference={reference}"
    );
    assert!(
        (unshifted - reference).abs() > (area - reference).abs() * 1e6,
        "unshifted={unshifted} area={area} reference={reference}"
    );
}

#[test]
fn long_utm_circle_area_matches_shifted_reference() {
    // The 128-edge ring declines the packed short-ring fold and exercises
    // the shared-frame fallback at UTM coordinate magnitudes.
    let k = 128;
    let ox = 500_000.0_f64;
    let oy = 5_200_000.0_f64;
    let radius = 10.0_f64;
    let mut xs: Vec<f64> = (0..k)
        .map(|i| ox + (f64::from(i) * std::f64::consts::TAU / f64::from(k)).cos() * radius)
        .collect();
    let mut ys: Vec<f64> = (0..k)
        .map(|i| oy + (f64::from(i) * std::f64::consts::TAU / f64::from(k)).sin() * radius)
        .collect();
    xs.push(xs[0]);
    ys.push(ys[0]);
    let offsets = [0, xs.len() as i32];
    let area = polygon_area_measure_columns(&xs, &ys, &offsets, 0..1);
    let reference = shifted_scalar_area_reference(&xs, &ys).abs();
    let unshifted = xs
        .array_windows::<2>()
        .zip(ys.array_windows::<2>())
        .fold(0.0_f64, |area, ([x0, x1], [y0, y1])| {
            area + (x0 * y1 - x1 * y0)
        })
        .abs()
        / 2.0;
    assert!(
        (area - reference).abs() <= 1e-12 * reference.max(1.0),
        "area={area} reference={reference}"
    );
    assert!(
        (unshifted - reference).abs() > (area - reference).abs(),
        "unshifted={unshifted} area={area} reference={reference}"
    );
}
