#[test]
fn fused_long_area_keeps_reference_bits_for_random_and_degenerate_rings() {
    let radial = |count: usize, radius: f64, phase: f64| {
        let mut xs = Vec::with_capacity(count + 1);
        let mut ys = Vec::with_capacity(count + 1);
        let count_f64 = f64::from(u32::try_from(count).expect("ring size fits u32"));
        for index in 0..count {
            let index_f64 = f64::from(u32::try_from(index).expect("index fits u32"));
            let angle = phase + 2.0 * std::f64::consts::PI * index_f64 / count_f64;
            let radius = radius
                * (1.0 + 0.07 * (5.0 * angle + 0.2).sin() + 0.025 * (13.0 * angle).cos());
            xs.push(radius * angle.cos());
            ys.push(radius * angle.sin());
        }
        xs.push(*xs.first().expect("radial ring has vertices"));
        ys.push(*ys.first().expect("radial ring has vertices"));
        (xs, ys)
    };
    let (shell_xs, shell_ys) = radial(4096, 100.0, 0.0);
    let (hole_xs, hole_ys) = radial(257, 15.0, 0.17);
    assert_area_bits_match_reference(&[
        (&shell_xs, &shell_ys, false),
        (&hole_xs, &hole_ys, true),
    ]);

    let mut seed = 0x1B74_6C2D_950F_A813_u64;
    let mut coordinate = || {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        f64::from(u32::try_from(seed >> 32).expect("upper word fits u32") % 257) - 128.0
    };
    for ring_index in 0..16 {
        let mut xs = Vec::with_capacity(65);
        let mut ys = Vec::with_capacity(65);
        for vertex in 0..64 {
            xs.push(500_000.0 + coordinate());
            ys.push(if ring_index % 3 == 0 {
                5_200_000.0 + f64::from(u32::try_from(vertex).expect("64 vertices fit u32"))
            } else {
                5_200_000.0 + coordinate()
            });
        }
        if ring_index % 4 == 0 {
            let duplicate_x = *xs.get(7).expect("64 generated vertices");
            let duplicate_y = *ys.get(7).expect("64 generated vertices");
            *xs.get_mut(8).expect("64 generated vertices") = duplicate_x;
            *ys.get_mut(8).expect("64 generated vertices") = duplicate_y;
        }
        xs.push(*xs.first().expect("64 generated vertices"));
        ys.push(*ys.first().expect("64 generated vertices"));
        assert_area_bits_match_reference(&[(&xs, &ys, false)]);
    }

    let sliver_xs = [
        19.343_75,
        -30.765_625,
        -18.265_625,
        0.140_625,
        -3.109_375,
        30.359_375,
        19.343_75,
    ];
    let sliver_ys = [
        59.031_25,
        -91.296_875,
        -53.796_875,
        1.421_875_000_000_000_4,
        -8.328_125,
        92.078_125,
        59.031_25,
    ];
    assert_area_bits_match_reference(&[(&sliver_xs, &sliver_ys, false)]);

    let (lo, hi) = (2.0_f64.powi(-60), 1.0 - 2.0_f64.powi(-53));
    let shell_xs = [0.0, 1.0, 1.0, 0.5, 0.0, 0.0];
    let shell_ys = [0.0, 0.0, 1.0, 1.0 + 2.0_f64.powi(-52), 1.0, 0.0];
    let hole_xs = [0.0, 0.0, 1.0, 1.0, 0.0];
    let hole_ys = [0.0, 1.0, 1.0, 0.0, 0.0];
    let thin_hole_xs = [lo, lo, hi, hi, lo];
    let thin_hole_ys = [lo, hi, hi, lo, lo];
    assert_area_bits_match_reference(&[
        (&shell_xs, &shell_ys, false),
        (&hole_xs, &hole_ys, true),
    ]);
    assert_area_bits_match_reference(&[
        (
            &[0.0, 1.0, 1.0, 0.0, 0.0],
            &[0.0, 0.0, 1.0, 1.0, 0.0],
            false,
        ),
        (&thin_hole_xs, &thin_hole_ys, true),
    ]);
}
