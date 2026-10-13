use super::*;

/// The textbook FULL `O(n·m)` discrete-Fréchet DP — the oracle the banded
/// [`frechet_dp_columns`] must reproduce bit-for-bit.
fn frechet_full_reference(sx: &[f64], sy: &[f64], lx: &[f64], ly: &[f64]) -> f64 {
    let width = sx.len();
    let mut previous = vec![0.0_f64; width];
    let mut current = vec![0.0_f64; width];
    let mut running = 0.0_f64;
    for index in 0..width {
        let (dx, dy) = (sx[index] - lx[0], sy[index] - ly[0]);
        running = running.max(dx * dx + dy * dy);
        previous[index] = running;
    }
    for row in 1..lx.len() {
        let (dx, dy) = (sx[0] - lx[row], sy[0] - ly[row]);
        let mut left = (dx * dx + dy * dy).max(previous[0]);
        current[0] = left;
        for index in 1..width {
            let (dx, dy) = (sx[index] - lx[row], sy[index] - ly[row]);
            let reach = previous[index]
                .min(previous[index.wrapping_sub(1)])
                .min(left);
            left = (dx * dx + dy * dy).max(reach);
            current[index] = left;
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[width.wrapping_sub(1)].sqrt()
}

#[test]
fn banded_frechet_matches_full_dp_bit_for_bit() {
    let base = |n| {
        (0..n)
            .map(|i| {
                let i = f64::from(i);
                (i, 5.0 * i.sin())
            })
            .unzip::<_, _, Vec<_>, Vec<_>>()
    };
    let perturb = |n, magnitude| {
        let (xs, ys) = base(n);
        xs.into_iter()
            .zip(ys)
            .enumerate()
            .map(|(i, (x, y))| {
                let sign = if i % 2 == 0 { 1.0 } else { -1.0 };
                (x + sign * magnitude, y - sign * magnitude)
            })
            .unzip::<_, _, Vec<_>, Vec<_>>()
    };
    let check = |name, (sx, sy): (Vec<f64>, Vec<f64>), (lx, ly): (Vec<f64>, Vec<f64>)| {
        let banded = frechet_dp_columns(&sx, &sy, &lx, &ly);
        let reference = frechet_full_reference(&sx, &sy, &lx, &ly);
        assert_eq!(
            banded.to_bits(),
            reference.to_bits(),
            "{name}: banded {banded} != full {reference}"
        );
    };
    check("1x1_identical", base(1), base(1));
    check("1x5_base", base(1), base(5));
    check("5x1_base", base(5), base(1));
    check("16x16_near", base(16), perturb(16, 1e-9));
    check("7x13_moderate", base(7), perturb(13, 0.5));
    check("13x7_moderate", perturb(13, 0.5), base(7));
    check("40x39_far", base(40), perturb(39, 50.0));
    check("39x40_far", perturb(39, 50.0), base(40));
    let (mut reversed_x, mut reversed_y) = base(5);
    reversed_x.reverse();
    reversed_y.reverse();
    check("5x5_reversed", base(5), (reversed_x, reversed_y));
}
