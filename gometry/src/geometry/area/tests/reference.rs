#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "This test oracle preserves adjacent closed-ring indexing for the original-coordinate exact fallback"
)]
pub(super) fn reference_exact_ring_area_sign_columns(xs: &[f64], ys: &[f64], pairs: usize) -> AreaSign {
    reference_cycle_sign(pairs, |index| {
        (
            XY::new(xs[index], ys[index]),
            XY::new(xs[index + 1], ys[index + 1]),
        )
    })
}

#[cfg(test)]
pub(super) fn reference_cycle_sign(edge_count: usize, mut edge_at: impl FnMut(usize) -> (XY, XY)) -> AreaSign {
    if edge_count == 0 {
        return AreaSign::Zero;
    }
    if let Some(sign) = reference_rounded_translated_cycle_sign(edge_count, &mut edge_at) {
        return sign;
    }
    let mut sum = ExactSum::zero();
    for index in 0..edge_count {
        let (a, b) = edge_at(index);
        sum.add_product(a.x, b.y);
        sum.subtract_product(b.x, a.y);
    }
    area_sign_from_ordering(sum.ordering())
}

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "The test oracle reproduces the former translated shoelace filter evaluation"
)]
pub(super) fn reference_rounded_translated_cycle_sign(
    edge_count: usize,
    mut edge_at: impl FnMut(usize) -> (XY, XY),
) -> Option<AreaSign> {
    if edge_count > 1 << 26 {
        return None;
    }
    let (origin, _) = edge_at(0);
    if !origin.x.is_finite() || !origin.y.is_finite() {
        return None;
    }
    let mut previous = XY::new(0.0, 0.0);
    let mut maximum_x_residual = 0.0_f64;
    let mut maximum_y_residual = 0.0_f64;
    let mut maximum_product = 0.0_f64;
    let mut sum = 0.0_f64;
    for index in 0..edge_count {
        let (_, destination) = edge_at(index);
        let next = if index + 1 == edge_count {
            XY::new(0.0, 0.0)
        } else {
            XY::new(destination.x - origin.x, destination.y - origin.y)
        };
        maximum_x_residual = maximum_x_residual.max(next.x.abs());
        maximum_y_residual = maximum_y_residual.max(next.y.abs());
        let left = previous.x * next.y;
        let right = next.x * previous.y;
        maximum_product = maximum_product.max(left.abs()).max(right.abs());
        let difference = left - right;
        sum += difference;
        previous = next;
    }
    if !sum.is_finite() {
        return None;
    }
    let error_x = rounded_residual_error_bound(maximum_x_residual);
    let error_y = rounded_residual_error_bound(maximum_y_residual);
    let translation_term = outward_add(
        outward_add(
            outward_mul(error_x, maximum_y_residual),
            outward_mul(error_y, maximum_x_residual),
        ),
        outward_mul(error_x, error_y),
    );
    let translation_error = outward_mul(2.0 * edge_count as f64, translation_term);
    let edge_count_squared = (edge_count as f64) * (edge_count as f64);
    let arithmetic_error = outward_add(
        outward_mul(8.0 * f64::EPSILON * edge_count_squared, maximum_product),
        outward_mul(8.0 * f64::MIN_POSITIVE, edge_count_squared),
    );
    let bound = outward_add(arithmetic_error, translation_error);
    if !bound.is_finite() {
        return None;
    }
    if sum > bound {
        Some(AreaSign::Positive)
    } else if sum < -bound {
        Some(AreaSign::Negative)
    } else {
        None
    }
}

#[cfg(test)]
/// A conservative absolute error bound for a finite rounded subtraction.
pub(super) fn rounded_residual_error_bound(maximum_residual: f64) -> f64 {
    outward_add(
        outward_mul(f64::EPSILON, maximum_residual),
        f64::from_bits(1),
    )
}

#[cfg(test)]
pub(super) fn outward_mul(left: f64, right: f64) -> f64 {
    (left * right).next_up()
}

#[cfg(test)]
pub(super) fn outward_add(left: f64, right: f64) -> f64 {
    (left + right).next_up()
}

#[cfg(test)]
pub(super) fn reference_shoelace_shifted_roundoff(
    xs: &[f64],
    ys: &[f64],
    pairs: usize,
    ox: f64,
    oy: f64,
    sx: f64,
    sy: f64,
) -> f64 {
    let identity = sx.to_bits() == 1.0_f64.to_bits() && sy.to_bits() == 1.0_f64.to_bits();
    let mut product_scale = 0.0_f64;
    for ((&x0, &x1), (&y0, &y1)) in xs[..pairs]
        .iter()
        .zip(&xs[1..=pairs])
        .zip(ys[..pairs].iter().zip(&ys[1..=pairs]))
    {
        let (x0, y0) = if identity {
            (x0 - ox, y0 - oy)
        } else {
            (scaled_residual(x0, ox, sx), scaled_residual(y0, oy, sy))
        };
        let (x1, y1) = if identity {
            (x1 - ox, y1 - oy)
        } else {
            (scaled_residual(x1, ox, sx), scaled_residual(y1, oy, sy))
        };
        product_scale += x0.abs() * y1.abs() + x1.abs() * y0.abs();
    }
    let operations = pairs.saturating_mul(4).saturating_add(1) as f64;
    product_scale * EPSILON * operations
}

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "The test oracle preserves the original binary64 area/error accumulation order"
)]
pub(super) fn reference_polygon_area_measure_with(
    visit: impl Fn(&mut dyn FnMut(&[f64], &[f64], bool)),
) -> f64 {
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;
    let mut any = false;
    visit(&mut |xs, ys, _hole| {
        for (&x, &y) in xs.iter().zip(ys.iter()) {
            any = true;
            min_x = min_x.min(x);
            max_x = max_x.max(x);
            min_y = min_y.min(y);
            max_y = max_y.max(y);
        }
    });
    if !(any && min_x.is_finite() && min_y.is_finite()) {
        return f64::NAN;
    }
    let ox = f64::midpoint(min_x, max_x);
    let oy = f64::midpoint(min_y, max_y);
    let sx = axis_pow2_scale(max_x.abs().max(min_x.abs()).max(ox.abs()));
    let sy = axis_pow2_scale(max_y.abs().max(min_y.abs()).max(oy.abs()));
    let mut area = 0.0_f64;
    let mut magnitude = 0.0_f64;
    let mut error = 0.0_f64;
    let mut rings = 0_usize;
    visit(&mut |xs, ys, hole| {
        let pairs = xs.len().saturating_sub(1);
        if pairs == 0 {
            return;
        }
        let ring = reference::shoelace_shifted(xs, ys, pairs, ox, oy, sx, sy).abs();
        rings += 1;
        magnitude += ring;
        error += reference_shoelace_shifted_roundoff(xs, ys, pairs, ox, oy, sx, sy);
        if hole {
            area -= ring;
        } else {
            area += ring;
        }
    });
    error += magnitude * EPSILON * rings.saturating_add(1) as f64;
    if !(area.is_finite() && error.is_finite() && area.abs() > error) {
        return exact_polygon_area_measure_with(visit);
    }
    unscale_area(area, sx, sy)
}
