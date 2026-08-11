use crate::geometry::access::*;
use crate::geometry::{RingWinding, closed_columns_winding, ring_area_measure_columns};

#[test]
fn column_all_finite_rejects_non_finite() {
    let column = vec![0.0, 1.0, f64::NAN, 3.0];
    assert!(!column_all_finite(&column));
}

#[test]
fn column_all_finite_accepts_finite() {
    let column = vec![0.0, 1.0, 2.0, 3.0];
    assert!(column_all_finite(&column));
}

#[test]
fn column_minmax_empty_is_none() {
    assert_eq!(column_minmax(&[]), None);
}

#[test]
fn column_minmax_single_element() {
    assert_eq!(column_minmax(&[5.0]), Some((5.0, 5.0)));
}

#[test]
fn column_minmax_finds_extremes() {
    assert_eq!(column_minmax(&[3.0, -1.0, 7.0, 2.0]), Some((-1.0, 7.0)));
}

#[test]
fn column_mean2_matches_scalar_mean() {
    let xs: Vec<f64> = (0..64).map(f64::from).collect();
    let ys: Vec<f64> = (0..64).map(|index| f64::from(index) * 2.0).collect();
    let count = xs.len() as f64;
    let expected = (
        xs.iter().sum::<f64>() / count,
        ys.iter().sum::<f64>() / count,
    );
    let (mx, my) = column_mean2(&xs, &ys).expect("non-empty");
    let tol = 1e-9 * expected.0.abs().max(expected.1.abs()).max(1.0);
    assert!((mx - expected.0).abs() <= tol && (my - expected.1).abs() <= tol);
    assert_eq!(column_mean2(&[], &[]), None);
}

#[test]
fn column_mean_rescues_overflowing_sum_of_finite_values() {
    // The flat `Σ` overflows f64 range (`2e308 == inf`), but the true mean is
    // finite and exactly representable; the online-mean rescue must recover it
    // instead of committing a non-finite value a `Point::new` would reject.
    let values = [1e308, 1e308];
    assert!((column_sum(&values) / values.len() as f64).is_infinite());
    assert_eq!(column_mean(&values), Some(1e308));
    // A genuinely non-finite input still propagates (its mean IS non-finite).
    assert!(
        column_mean(&[f64::INFINITY, 1.0])
            .expect("non-empty")
            .is_infinite()
    );
    assert!(column_mean(&[f64::NAN, 1.0]).expect("non-empty").is_nan());
    // Ordinary magnitudes stay bit-identical to the flat `Σ / n`.
    let ordinary = [1.0, 2.0, 3.0, 4.5];
    assert_eq!(
        column_mean(&ordinary),
        Some(column_sum(&ordinary) / ordinary.len() as f64)
    );
}

#[test]
fn ring_winding_stable_near_cancellation_when_measurement_may_differ() {
    // CCW unit square duplicated with tiny perturbations — 16 cross-product
    // terms whose signed sum is ~0; SIMD reassociation on the measurement path
    // could differ, but the decision winding must stay CCW.
    let xs: Vec<f64> = vec![
        0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0,
    ];
    let ys: Vec<f64> = vec![
        0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
    ];
    let pairs = xs.len() - 1;
    assert!(pairs >= REDUCE_SIMD_MIN);
    assert_eq!(
        closed_columns_winding(&xs, &ys, pairs),
        RingWinding::CounterClockwise
    );
    let measure = ring_area_measure_columns(&xs, &ys, pairs).get();
    // First vertex is the origin — the measurement origin-shift is a no-op.
    let (ox, oy) = (xs[0], ys[0]);
    let scalar_reference = xs
        .array_windows::<2>()
        .zip(ys.array_windows::<2>())
        .fold(0.0_f64, |area, ([x0, x1], [y0, y1])| {
            area + ((x0 - ox) * (y1 - oy) - (x1 - ox) * (y0 - oy))
        })
        / 2.0;
    assert!(scalar_reference > 0.0);
    assert_eq!(measure.to_bits(), scalar_reference.to_bits());
}

#[test]
fn line_length_columns_falls_back_for_tiny_nonzero_segments() {
    let xs: Vec<f64> = (0..=8).map(|i| f64::from(i) * 1e-200).collect();
    let ys = vec![0.0; xs.len()];
    let expected: f64 = xs
        .array_windows::<2>()
        .map(|[x0, x1]| (x1 - x0).hypot(0.0))
        .sum();
    let length = line_length_columns(&xs, &ys);

    assert_eq!(expected.to_bits(), 8e-200_f64.to_bits());
    assert_eq!(length.to_bits(), expected.to_bits());
}
