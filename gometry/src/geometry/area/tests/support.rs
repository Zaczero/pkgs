/// Independent binary64 shoelace reference after translating to one vertex.
fn shifted_scalar_area_reference(xs: &[f64], ys: &[f64]) -> f64 {
    let pairs = xs.len().saturating_sub(1);
    if pairs == 0 {
        return 0.0;
    }
    let (ox, oy) = (xs[0], ys[0]);
    xs.array_windows::<2>()
        .zip(ys.array_windows::<2>())
        .fold(0.0_f64, |area, ([x0, x1], [y0, y1])| {
            area + ((x0 - ox) * (y1 - oy) - (x1 - ox) * (y0 - oy))
        })
        / 2.0
}

fn assert_ring_sign_matches_reference(xs: &[f64], ys: &[f64]) {
    let pairs = xs.len().saturating_sub(1);
    assert_eq!(
        exact_ring_area_sign_columns(xs, ys, pairs),
        reference_exact_ring_area_sign_columns(xs, ys, pairs),
        "ring columns differ: {xs:?} {ys:?}"
    );
}

fn assert_area_bits_match_reference(rings: &[(&[f64], &[f64], bool)]) {
    let actual = polygon_area_measure_with(|visit| {
        for &(xs, ys, hole) in rings {
            visit(xs, ys, hole);
        }
    });
    let expected = reference_polygon_area_measure_with(|visit| {
        for &(xs, ys, hole) in rings {
            visit(xs, ys, hole);
        }
    });
    assert_eq!(
        actual.to_bits(),
        expected.to_bits(),
        "{actual:e} != {expected:e}"
    );
}
