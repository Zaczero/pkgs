use super::*;

/// Polygon area in one common, power-of-two-normalized frame for the shell
/// and every hole. Per-ring rescues are insufficient at extreme magnitudes:
/// a shell and a hole can each legitimately measure as infinity even though
/// their mathematical difference has one unambiguous sign. The shared frame
/// performs `shell - holes` before one final unscale.
pub(crate) fn polygon_area_measure_with(
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
        let (signed_ring, ring_error) =
            shoelace_shifted_with_roundoff(xs, ys, pairs, ox, oy, sx, sy);
        let ring = signed_ring.abs();
        rings = rings.saturating_add(1);
        magnitude += ring;
        error += ring_error;
        if hole {
            area -= ring;
        } else {
            area += ring;
        }
    });
    // The bound covers every ring's roundoff and the shell-minus-holes fold.
    // Ring overflow is an arithmetic-loss signal rather than proof that the
    // area is unrepresentable, and it makes the bound nonfinite, so the exact
    // sum can still cancel it to a finite answer.
    error += magnitude * EPSILON * rings.saturating_add(1) as f64;
    if !(area.is_finite() && error.is_finite() && area.abs() > error) {
        return exact_polygon_area_measure_with(visit);
    }
    unscale_area(area, sx, sy)
}

/// Exact shell-minus-holes area, rounded once. The product-sum owner works in
/// the original coordinate frame, so no subnormal intermediate is needed and
/// shell/hole cancellation stays exact.
pub(super) fn exact_polygon_area_measure_with(
    visit: impl Fn(&mut dyn FnMut(&[f64], &[f64], bool)),
) -> f64 {
    let mut total = ExactSum::zero();
    visit(&mut |xs, ys, hole| {
        let pairs = xs.len().saturating_sub(1);
        if pairs == 0 {
            return;
        }
        let mut ring = ExactSum::zero();
        for (&[x0, x1], &[y0, y1]) in xs
            .array_windows::<2>()
            .zip(ys.array_windows::<2>())
            .take(pairs)
        {
            ring.add_cross(x0, y0, x1, y1);
        }
        let ordering = ring.ordering();
        if ordering == std::cmp::Ordering::Equal {
            return;
        }
        let negate = (ordering == std::cmp::Ordering::Less) ^ hole;
        for (&[x0, x1], &[y0, y1]) in xs
            .array_windows::<2>()
            .zip(ys.array_windows::<2>())
            .take(pairs)
        {
            if negate {
                total.subtract_product(x0, y1);
                total.add_product(x1, y0);
            } else {
                total.add_product(x0, y1);
                total.subtract_product(x1, y0);
            }
        }
    });
    total.round_to_area()
}

pub(super) fn selected_ring_columns<'a>(
    ring_offsets: &[i32],
    xs: &'a [f64],
    ys: &'a [f64],
    ring_index: usize,
) -> (&'a [f64], &'a [f64], usize) {
    let start = *ring_offsets
        .get(ring_index)
        .expect("selected ring ID fits the offset column") as usize;
    let end = *ring_offsets
        .get(ring_index.wrapping_add(1))
        .expect("selected ring successor fits the offset column") as usize;
    let pairs = end.saturating_sub(start.wrapping_add(1));
    (
        xs.get(start..end).expect("ring offsets fit the x column"),
        ys.get(start..end).expect("ring offsets fit the y column"),
        pairs,
    )
}

/// Packed polygon area with a fused ordinary path.
///
/// Most packed polygons have short, finite rings whose per-ring origin shift
/// is enough to condition the shoelace products.  Fuse the cross-term and
/// roundoff-bound folds so those rows do not first build a polygon-wide frame
/// and then replay every vertex.  A shell/hole cancellation, overflow, long
/// ring, or uncertain bound returns to [`polygon_area_measure_with`], whose
/// shared frame and exact product sum preserve the extreme-coordinate contract.
pub(crate) fn polygon_area_measure_columns(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
) -> f64 {
    let mut area = 0.0_f64;
    let mut magnitude = 0.0_f64;
    let mut error = 0.0_f64;
    let mut fast = !rings.is_empty();

    for ring_index in rings.clone() {
        let (ring_xs, ring_ys, pairs) = selected_ring_columns(ring_offsets, xs, ys, ring_index);
        let Some((signed, bound)) = fast_ring_area_columns(ring_xs, ring_ys, pairs) else {
            fast = false;
            break;
        };
        let ring = signed.abs();
        magnitude += ring;
        error += bound;
        if ring_index == rings.start {
            area += ring;
        } else {
            area -= ring;
        }
    }

    // Include the final shell-minus-holes fold in the bound.  If that bound
    // cannot distinguish the result from zero, use the shared-frame exact
    // owner rather than letting independent ring frames cancel poorly.
    let fold_error = magnitude * EPSILON * rings.len().saturating_add(1) as f64;
    error += fold_error;
    if fast && area.is_finite() && error.is_finite() && area.abs() > error {
        return area;
    }

    polygon_area_measure_with(|visit| {
        for ring_index in rings.clone() {
            let (ring_xs, ring_ys, _) = selected_ring_columns(ring_offsets, xs, ys, ring_index);
            visit(ring_xs, ring_ys, ring_index != rings.start);
        }
    })
}

/// One-pass short-ring reducer. `None` is the cold-path signal, not a result
/// sentinel: it covers arithmetic that needs the shared-frame exact owner.
pub(super) fn fast_ring_area_columns(xs: &[f64], ys: &[f64], pairs: usize) -> Option<(f64, f64)> {
    if pairs >= SHOELACE_SIMD_MIN {
        return None;
    }
    if pairs == 0 {
        return Some((0.0, 0.0));
    }
    let (&ox, &oy) = xs.first().zip(ys.first())?;
    let x0s = xs.get(..pairs).expect("pairs fit the closed x column");
    let x1s = xs.get(1..=pairs).expect("pairs fit the closed x column");
    let y0s = ys.get(..pairs).expect("pairs fit the closed y column");
    let y1s = ys.get(1..=pairs).expect("pairs fit the closed y column");
    let mut cross = 0.0_f64;
    let mut product_scale = 0.0_f64;
    for ((&x0, &x1), (&y0, &y1)) in x0s.iter().zip(x1s).zip(y0s.iter().zip(y1s)) {
        let x0 = x0 - ox;
        let y0 = y0 - oy;
        let x1 = x1 - ox;
        let y1 = y1 - oy;
        let term = x0 * y1 - x1 * y0;
        let products = x0.abs() * y1.abs() + x1.abs() * y0.abs();
        cross += term;
        product_scale += products;
    }
    // A non-finite term keeps the non-negative `product_scale` non-finite, so
    // one check after the loop leaves the loop free of branches.
    if !(cross.is_finite() && product_scale.is_finite()) {
        return None;
    }
    let measured = cross / 2.0;
    if measured == 0.0 || product_scale == 0.0 {
        return None;
    }
    let operations = pairs.saturating_mul(4).saturating_add(1) as f64;
    let error = product_scale * EPSILON * operations;
    if !error.is_finite() || measured.abs() <= error {
        return None;
    }
    Some((measured, error))
}

#[cfg(test)]
pub(crate) fn open_point_cycle_magnitude(points: &[Point]) -> AreaMeasure {
    unsigned_area(&open_cycle_sum(points))
}

pub(crate) fn open_xy_cycle_magnitude(points: &[XY]) -> AreaMeasure {
    unsigned_area(&open_cycle_sum(points))
}
