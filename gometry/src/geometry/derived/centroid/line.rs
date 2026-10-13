use super::*;

pub(super) fn exact_coordinate_difference(left: f64, right: f64) -> CoreResult<ExactDyadic> {
    ExactDyadic::from_f64(left).try_subtract(ExactDyadic::from_f64(right))
}

#[expect(
    clippy::float_cmp,
    reason = "exact stored XY equality defines whether a segment has zero length"
)]
pub(super) fn exact_segment_length(
    x0: f64,
    y0: f64,
    x1: f64,
    y1: f64,
) -> CoreResult<Option<ExactDyadic>> {
    if x0 == x1 && y0 == y1 {
        return Ok(None);
    }
    let distance = point_distance(Point::new_raw_xy(x0, y0), Point::new_raw_xy(x1, y1));
    if distance > 0.0 && distance.is_finite() {
        return Ok(Some(ExactDyadic::from_f64(distance)));
    }

    // Keep an unrepresentable world length as a rounded significand plus its
    // exact exponent. Distinct finite binary64 endpoints have positive exact
    // distance even when subtraction or world-scale rounding overflows.
    let dx = exact_coordinate_difference(x1, x0)?;
    let dy = exact_coordinate_difference(y1, y0)?;
    let squared = dx.try_product(&dx)?.try_add(dy.try_product(&dy)?)?;
    debug_assert!(!squared.is_zero());
    let high = *squared.limbs.last().expect("positive squared length");
    let preceding_limbs = squared
        .limbs
        .len()
        .checked_sub(1)
        .expect("positive squared length has at least one limb");
    let limb_bits = usize::try_from(u64::BITS).expect("u64 width fits usize");
    let high_bits = usize::try_from(high.bit_width()).expect("u64 bit width fits usize");
    let bits = preceding_limbs
        .checked_mul(limb_bits)
        .and_then(|bits| bits.checked_add(high_bits))
        .expect("binary64 squared-length width fits usize");
    let highest_bit = bits
        .checked_sub(1)
        .expect("positive squared length has at least one bit");
    let top = squared
        .exponent
        .checked_add(i64::try_from(highest_bit).expect("binary64 squared-length width fits i64"))
        .ok_or(ResourceError::SizeOverflow)?;
    let root_exponent = top.div_euclid(2);
    let mut normalized_squared = squared;
    normalized_squared.exponent = normalized_squared
        .exponent
        .checked_sub(
            root_exponent
                .checked_mul(2)
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .ok_or(ResourceError::SizeOverflow)?;
    let significand = round_sqrt_ratio(&normalized_squared, &ExactDyadic::one())?;
    debug_assert!(significand.is_finite() && (1.0..=2.0).contains(&significand));
    let mut length = ExactDyadic::from_f64(significand);
    length.exponent = length
        .exponent
        .checked_add(root_exponent)
        .ok_or(ResourceError::SizeOverflow)?;
    length.try_trim()?;
    Ok(Some(length))
}

#[expect(
    clippy::indexing_slicing,
    reason = "segment and shifted coordinates stay inside admitted equal-length coordinate columns"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the incremented segment index is bounded by the admitted coordinate columns"
)]
pub(super) fn lineal_centroid_segment_contrib(
    xs: &[f64],
    ys: &[f64],
    segment: usize,
) -> (f64, f64, f64) {
    let length = point_distance(
        Point::new_raw_xy(xs[segment], ys[segment]),
        Point::new_raw_xy(xs[segment + 1], ys[segment + 1]),
    );
    (
        length,
        length.algebraic_mul(f64::midpoint(xs[segment], xs[segment + 1])),
        length.algebraic_mul(f64::midpoint(ys[segment], ys[segment + 1])),
    )
}

pub(super) fn lineal_centroid_scalar_plain(
    xs: &[f64],
    ys: &[f64],
    range: std::ops::Range<usize>,
) -> (f64, f64, f64) {
    // Use the shared point-distance (hypot rescue on untrusted squares) so
    // length matches `.length` / `point_distance` at subnormal scales. The
    // length·midpoint product may still underfloor — finish rescues via online.
    if range.len() < 2 {
        return range.fold((0.0_f64, 0.0_f64, 0.0_f64), |(tl, lx, ly), segment| {
            let (length, cx, cy) = lineal_centroid_segment_contrib(xs, ys, segment);
            (tl + length, lx + cx, ly + cy)
        });
    }

    let mut total_length = 0.0_f64;
    let mut moment_x = 0.0_f64;
    let mut moment_y = 0.0_f64;
    let mut moment_x_signs = 0_u8;
    let mut moment_y_signs = 0_u8;
    for segment in range.clone() {
        let (length, cx, cy) = lineal_centroid_segment_contrib(xs, ys, segment);
        total_length += length;
        moment_x += cx;
        moment_y += cy;
        moment_x_signs |= if cx > 0.0 {
            0b01
        } else if cx < 0.0 {
            0b10
        } else {
            0
        };
        moment_y_signs |= if cy > 0.0 {
            0b01
        } else if cy < 0.0 {
            0b10
        } else {
            0
        };
    }

    if moment_x_signs == 0b11 || moment_y_signs == 0b11 {
        let mut compensated_x = NeumaierSum::default();
        let mut compensated_y = NeumaierSum::default();
        for segment in range {
            let (_, cx, cy) = lineal_centroid_segment_contrib(xs, ys, segment);
            if moment_x_signs == 0b11 {
                compensated_x.add(cx);
            }
            if moment_y_signs == 0b11 {
                compensated_y.add(cy);
            }
        }
        if moment_x_signs == 0b11 {
            moment_x = compensated_x.total();
        }
        if moment_y_signs == 0b11 {
            moment_y = compensated_y.total();
        }
    }
    (total_length, moment_x, moment_y)
}

pub(super) fn lineal_centroid_scalar_algebraic(
    xs: &[f64],
    ys: &[f64],
    range: std::ops::Range<usize>,
) -> (f64, f64, f64) {
    range.fold((0.0_f64, 0.0_f64, 0.0_f64), |(tl, lx, ly), segment| {
        let (length, cx, cy) = lineal_centroid_segment_contrib(xs, ys, segment);
        (
            tl.algebraic_add(length),
            lx.algebraic_add(cx),
            ly.algebraic_add(cy),
        )
    })
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "the SIMD floating-point expressions and reducer-bounded chunk arithmetic preserve the centroid's established operation order"
)]
#[expect(
    clippy::indexing_slicing,
    reason = "simd_reduce_f64 supplies lane-aligned starts within the full shifted as_chunks slices derived from equal-length columns"
)]
pub(super) fn lineal_centroid_column_sums(xs: &[f64], ys: &[f64]) -> (f64, f64, f64) {
    let segments = xs.len().saturating_sub(1);
    if segments < REDUCE_SIMD_MIN {
        return lineal_centroid_scalar_plain(xs, ys, 0..segments);
    }
    let simd_moment_sum = |accumulated: ReduceSimd, scalar: f64| {
        let zero = ReduceSimd::splat(0.0);
        let has_positive = accumulated.simd_gt(zero).any() || scalar > 0.0;
        let has_negative = accumulated.simd_lt(zero).any() || scalar < 0.0;
        if has_positive && has_negative {
            let mut sum = NeumaierSum::default();
            for lane in accumulated.to_array() {
                sum.add(lane);
            }
            sum.add(scalar);
            sum.total()
        } else {
            accumulated.reduce_sum().algebraic_add(scalar)
        }
    };
    let (x0, _) = xs[..segments].as_chunks::<REDUCE_LANES>();
    let (x1, _) = xs[1..=segments].as_chunks::<REDUCE_LANES>();
    let (y0, _) = ys[..segments].as_chunks::<REDUCE_LANES>();
    let (y1, _) = ys[1..=segments].as_chunks::<REDUCE_LANES>();
    let simd_fold = |mut acc: (ReduceSimd, ReduceSimd, ReduceSimd),
                     mut scalar: (f64, f64, f64),
                     start: usize| {
        let index = start / REDUCE_LANES;
        let dx = ReduceSimd::from_array(x1[index]) - ReduceSimd::from_array(x0[index]);
        let dy = ReduceSimd::from_array(y1[index]) - ReduceSimd::from_array(y0[index]);
        let squared = dx * dx + dy * dy;
        let length = squared.sqrt();
        let zero_delta = dx.simd_eq(ReduceSimd::splat(0.0)) & dy.simd_eq(ReduceSimd::splat(0.0));
        let underflow = squared.simd_eq(ReduceSimd::splat(0.0)) & !zero_delta;
        if length.is_finite().all() && !underflow.any() {
            let half = ReduceSimd::splat(0.5);
            let mid_x =
                (ReduceSimd::from_array(x0[index]) + ReduceSimd::from_array(x1[index])) * half;
            let mid_y =
                (ReduceSimd::from_array(y0[index]) + ReduceSimd::from_array(y1[index])) * half;
            acc.0 += length;
            acc.1 += length * mid_x;
            acc.2 += length * mid_y;
        } else {
            for segment in start..start + REDUCE_LANES {
                if segment < segments {
                    let (length, cx, cy) = lineal_centroid_segment_contrib(xs, ys, segment);
                    scalar.0 = scalar.0.algebraic_add(length);
                    scalar.1 = scalar.1.algebraic_add(cx);
                    scalar.2 = scalar.2.algebraic_add(cy);
                }
            }
        }
        (acc, scalar)
    };
    let lanes = x0.len() * REDUCE_LANES;
    simd_reduce_f64(
        segments,
        (
            ReduceSimd::splat(0.0),
            ReduceSimd::splat(0.0),
            ReduceSimd::splat(0.0),
        ),
        (0.0_f64, 0.0_f64, 0.0_f64),
        simd_fold,
        |scalar, range| {
            if range.start == 0 {
                return lineal_centroid_scalar_algebraic(xs, ys, range);
            }
            let start = segments - REDUCE_LANES;
            let dx =
                ReduceSimd::from_slice(&xs[start + 1..]) - ReduceSimd::from_slice(&xs[start..]);
            let dy =
                ReduceSimd::from_slice(&ys[start + 1..]) - ReduceSimd::from_slice(&ys[start..]);
            let lane_index = ReduceSimd::from_array(std::array::from_fn(|lane| lane as f64));
            let fresh = lane_index.simd_ge(ReduceSimd::splat(f64::from((lanes - start) as u32)));
            let squared = dx * dx + dy * dy;
            let length = fresh.select(squared.sqrt(), ReduceSimd::splat(0.0));
            let zero_delta =
                dx.simd_eq(ReduceSimd::splat(0.0)) & dy.simd_eq(ReduceSimd::splat(0.0));
            let underflow = fresh & squared.simd_eq(ReduceSimd::splat(0.0)) & !zero_delta;
            if length.is_finite().all() && !underflow.any() {
                let half = ReduceSimd::splat(0.5);
                let mid_x = (ReduceSimd::from_slice(&xs[start..])
                    + ReduceSimd::from_slice(&xs[start + 1..]))
                    * half;
                let mid_y = (ReduceSimd::from_slice(&ys[start..])
                    + ReduceSimd::from_slice(&ys[start + 1..]))
                    * half;
                let contrib_x = fresh.select(length * mid_x, ReduceSimd::splat(0.0));
                let contrib_y = fresh.select(length * mid_y, ReduceSimd::splat(0.0));
                let contrib_len = fresh.select(length, ReduceSimd::splat(0.0));
                (
                    scalar.0.algebraic_add(contrib_len.reduce_sum()),
                    scalar.1.algebraic_add(simd_moment_sum(contrib_x, 0.0)),
                    scalar.2.algebraic_add(simd_moment_sum(contrib_y, 0.0)),
                )
            } else {
                let (added_len, added_x, added_y) =
                    lineal_centroid_scalar_algebraic(xs, ys, lanes..segments);
                (
                    scalar.0.algebraic_add(added_len),
                    scalar.1.algebraic_add(added_x),
                    scalar.2.algebraic_add(added_y),
                )
            }
        },
        |(len_acc, x_acc, y_acc), scalar| {
            (
                len_acc.reduce_sum().algebraic_add(scalar.0),
                simd_moment_sum(x_acc, scalar.1),
                simd_moment_sum(y_acc, scalar.2),
            )
        },
    )
}

pub(super) fn accumulate_lineal_centroid(
    xs: &[f64],
    ys: &[f64],
    total_length: &mut f64,
    line_sum: &mut (f64, f64),
) {
    if xs.len() < 2 {
        return;
    }
    let (len, sum_x, sum_y) = lineal_centroid_column_sums(xs, ys);
    *total_length += len;
    line_sum.0 += sum_x;
    line_sum.1 += sum_y;
}

pub(super) fn finish_lineal_centroid(
    total_length: f64,
    line_sum: (f64, f64),
) -> Option<(f64, f64)> {
    (total_length > 0.0).then(|| (line_sum.0 / total_length, line_sum.1 / total_length))
}

/// Exact fallback over these physical coordinate runs. Zero-length segments
/// contribute nothing; positive lengths retain their exponent, so no
/// world-scale frame can be moved by a zero-weight run or erase a tiny weight.
pub(super) fn exact_lineal_centroid_runs(
    revisit: impl Fn(&mut dyn FnMut(&[f64], &[f64])),
) -> Option<(f64, f64)> {
    let mut sums = GrowableCentroidSums::zero();
    revisit(&mut |xs, ys| {
        debug_assert_eq!(xs.len(), ys.len());
        for ((&x0, &x1), (&y0, &y1)) in xs
            .iter()
            .zip(xs.iter().skip(1))
            .zip(ys.iter().zip(ys.iter().skip(1)))
        {
            let Some(length) = exact_segment_length(x0, y0, x1, y1)
                .expect("finite binary64 line terms fit the growable centroid bound")
            else {
                continue;
            };
            let midpoint_x = ExactDyadic::from_f64(f64::midpoint(x0, x1));
            let midpoint_y = ExactDyadic::from_f64(f64::midpoint(y0, y1));
            let term = GrowableCentroidSums {
                measure: length.clone(),
                moment_x: length
                    .try_product(&midpoint_x)
                    .expect("finite binary64 line moment fits the growable bound"),
                moment_y: length
                    .try_product(&midpoint_y)
                    .expect("finite binary64 line moment fits the growable bound"),
            };
            let previous = std::mem::replace(&mut sums, GrowableCentroidSums::zero());
            sums = previous
                .add(term)
                .expect("finite binary64 line sum fits the growable centroid bound");
        }
    });
    exact_summary_centroid(sums, CentroidDimension::Lineal)
        .expect("finite binary64 line quotient fits the growable centroid bound")
}

/// [`finish_lineal_centroid`] with a growable exact rescue. The
/// `Σ length·midpoint` fast-path can overflow, underflow, or lose cancellation
/// tails even when the finite source has a finite centroid. Replay this one
/// physical owner through exact dyadic weights and moments; zero-length runs
/// cannot affect a frame because the rescue uses no world-coordinate frame.
pub(super) fn finish_lineal_centroid_rescued(
    total_length: f64,
    line_sum: (f64, f64),
    revisit: impl Fn(&mut dyn FnMut(&[f64], &[f64])),
) -> Option<(f64, f64)> {
    match finish_lineal_centroid(total_length, line_sum) {
        Some((x, y)) if x.is_finite() && y.is_finite() => {
            // A subnormal total or weighted sum may have rounded away centroid
            // bits even when the quotient is finite and nonzero. Exact
            // cancellation to the origin is also worth resolving.
            let underresolved = !total_length.is_normal()
                || (line_sum.0 != 0.0 && !line_sum.0.is_normal())
                || (line_sum.1 != 0.0 && !line_sum.1.is_normal());
            if underresolved || (total_length > 0.0 && x == 0.0 && y == 0.0) {
                return exact_lineal_centroid_runs(revisit).or(Some((x, y)));
            }
            Some((x, y))
        },
        Some(_) => exact_lineal_centroid_runs(revisit),
        None => None,
    }
}

/// Column kernel for lineal centroid (dimension-1 pass) over one coordinate
/// run.
pub(in crate::geometry::derived) fn centroid_line_columns(
    xs: &[f64],
    ys: &[f64],
) -> Option<(f64, f64)> {
    let mut total_length = 0.0_f64;
    let mut line_sum = (0.0_f64, 0.0_f64);
    accumulate_lineal_centroid(xs, ys, &mut total_length, &mut line_sum);
    finish_lineal_centroid_rescued(total_length, line_sum, |visit| visit(xs, ys))
}

/// Zero-area polygon fallback: length-weighted edge midpoints over all rings.
pub(super) fn centroid_polygon_lineal_columns(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
) -> Option<(f64, f64)> {
    let mut total_length = 0.0_f64;
    let mut line_sum = (0.0_f64, 0.0_f64);
    for (_, ring_xs, ring_ys) in polygon_ring_columns(xs, ys, ring_offsets, rings.clone()) {
        accumulate_lineal_centroid(ring_xs, ring_ys, &mut total_length, &mut line_sum);
    }
    finish_lineal_centroid_rescued(total_length, line_sum, |visit| {
        for (_, ring_xs, ring_ys) in polygon_ring_columns(xs, ys, ring_offsets, rings.clone()) {
            visit(ring_xs, ring_ys);
        }
    })
}

/// Packed-line row kernel: lineal centroid, then the first-vertex puntal
/// anchor.
pub(crate) fn centroid_line_row_columns(xs: &[f64], ys: &[f64]) -> Option<(f64, f64)> {
    centroid_line_columns(xs, ys).or_else(|| xs.first().zip(ys.first()).map(|(&x, &y)| (x, y)))
}

/// Per-component lineal centroid for a single coordinate run, returned with
/// its length weight for the caller's merge or whole-shape replay decision.
pub(super) fn lineal_component_centroid(xs: &[f64], ys: &[f64]) -> Option<(f64, f64, f64)> {
    let mut total_length = 0.0_f64;
    let mut line_sum = (0.0_f64, 0.0_f64);
    accumulate_lineal_centroid(xs, ys, &mut total_length, &mut line_sum);
    let (cx, cy) = finish_lineal_centroid_rescued(total_length, line_sum, |visit| visit(xs, ys))?;
    (total_length > 0.0).then_some((total_length, cx, cy))
}

pub(super) const fn lineal_part_requires_exact_replay(weight: f64, cx: f64, cy: f64) -> bool {
    // An infinite length remains a replay sentinel even if its rescued center failed too.
    weight.is_infinite() || !cx.is_finite() || !cy.is_finite()
}

pub(super) fn lineal_fast_result_needs_exact(total_length: f64, line_sum: (f64, f64)) -> bool {
    match finish_lineal_centroid(total_length, line_sum) {
        Some((x, y)) if x.is_finite() && y.is_finite() => {
            !total_length.is_normal()
                || (line_sum.0 != 0.0 && !line_sum.0.is_normal())
                || (line_sum.1 != 0.0 && !line_sum.1.is_normal())
                || (total_length > 0.0 && x == 0.0 && y == 0.0)
        },
        Some(_) => true,
        None => false,
    }
}

pub(super) fn lineal_single_run_centroid(
    shape: &Shape,
    xs: &[f64],
    ys: &[f64],
) -> CoreResult<Option<(f64, f64)>> {
    let mut total_length = 0.0_f64;
    let mut line_sum = (0.0_f64, 0.0_f64);
    accumulate_lineal_centroid(xs, ys, &mut total_length, &mut line_sum);
    if lineal_fast_result_needs_exact(total_length, line_sum) {
        exact_owner_centroid(shape, CentroidDimension::Lineal)
    } else {
        Ok(finish_lineal_centroid(total_length, line_sum))
    }
}

pub(super) fn try_for_each_lineal(
    shape: &Shape,
    visit: &mut impl FnMut(&[f64], &[f64]) -> bool,
) -> bool {
    shape
        .try_for_each_component(&mut |component| {
            let stopped = match component {
                Component::Point(_) => false,
                Component::LineString(line) => visit(line.xs(), line.ys()),
                Component::Polygon(polygon) => polygon_rings(polygon)
                    .any(|(ring, _)| ring.xy_columns().is_some_and(|(xs, ys)| visit(xs, ys))),
            };
            if stopped { Err(()) } else { Ok(()) }
        })
        .is_err()
}

pub(super) fn for_each_lineal(shape: &Shape, visit: &mut impl FnMut(&[f64], &[f64])) {
    let stopped = try_for_each_lineal(shape, &mut |xs, ys| {
        visit(xs, ys);
        false
    });
    debug_assert!(!stopped);
}

/// Traverse collections with owner admission; a non-collection subtree uses
/// its direct component kernel without checking its leaf storage owners.
pub(super) fn visit_lineal_owner_runs<'owner>(
    shape: &'owner Shape,
    admission: &mut Option<OwnerAdmission<'owner>>,
    visit: &mut impl FnMut(&[f64], &[f64]) -> bool,
) -> CentroidAttempt<()> {
    let repeated_work = if let Shape::GeometryCollection(collection) = shape {
        collection.depth() > 1
            || admission
                .as_ref()
                .is_some_and(|admission| admission.repeated_owner)
    } else {
        false
    };
    if repeated_work {
        visit_lineal_owner_runs_impl::<false, true>(shape, admission, visit)
    } else {
        visit_lineal_owner_runs_impl::<false, false>(shape, admission, visit)
    }
}

pub(super) fn visit_lineal_owner_runs_impl<
    'owner,
    const TRACK_OWNERS: bool,
    const TRACK_REPEAT_WORK: bool,
>(
    shape: &'owner Shape,
    admission: &mut Option<OwnerAdmission<'owner>>,
    visit: &mut impl FnMut(&[f64], &[f64]) -> bool,
) -> CentroidAttempt<()> {
    let Shape::GeometryCollection(collection) = shape else {
        let repeated = TRACK_REPEAT_WORK
            && admission
                .as_ref()
                .is_some_and(|admission| admission.repeated_owner);
        let stopped = try_for_each_lineal(shape, &mut |xs, ys| {
            (repeated && charge_repeated_work(admission, xs.len())) || visit(xs, ys)
        });
        return if stopped {
            CentroidAttempt::ExactAtRoot(ExactAtRoot)
        } else {
            CentroidAttempt::Ready(())
        };
    };
    if TRACK_OWNERS && observe_collection_owner(shape, admission) {
        return CentroidAttempt::ExactAtRoot(ExactAtRoot);
    }
    for child in collection {
        let repeated = TRACK_REPEAT_WORK
            && admission
                .as_ref()
                .is_some_and(|admission| admission.repeated_owner);
        if repeated && charge_repeated_work(admission, 1) {
            return CentroidAttempt::ExactAtRoot(ExactAtRoot);
        }
        let outcome = if let Shape::GeometryCollection(child_collection) = child {
            let track_owners = child_collection.members_are_shared()
                || (TRACK_OWNERS
                    && admission
                        .as_ref()
                        .is_some_and(|admission| admission.repeated_owner));
            if track_owners {
                visit_lineal_owner_runs_impl::<true, TRACK_REPEAT_WORK>(child, admission, visit)
            } else {
                visit_lineal_owner_runs_impl::<false, TRACK_REPEAT_WORK>(child, admission, visit)
            }
        } else {
            visit_lineal_owner_runs_impl::<TRACK_OWNERS, TRACK_REPEAT_WORK>(child, admission, visit)
        };
        if let CentroidAttempt::ExactAtRoot(reason) = outcome {
            return CentroidAttempt::ExactAtRoot(reason);
        }
    }
    CentroidAttempt::Ready(())
}

/// Dimension-1 centroid: length-weighted edge-midpoint sums over every lineal
/// run — standalone lines and polygon-ring boundaries (the zero-area fallback).
/// `None` when the total length is zero. Multi-part lines use component-local
/// centroids then a length-weighted stable merge so order and cancellation
/// cannot move the mean.
pub(crate) fn lineal_centroid(shape: &Shape) -> CoreResult<Option<(f64, f64)>> {
    match shape {
        Shape::Point(_) | Shape::MultiPoint(_) | Shape::Empty(..) => Ok(None),
        Shape::LineString(line) => lineal_single_run_centroid(shape, line.xs(), line.ys()),
        Shape::MultiLineString(lines) if lines.len() <= 1 => {
            lines.first().map_or(Ok(None), |line| {
                lineal_single_run_centroid(shape, line.xs(), line.ys())
            })
        },
        Shape::MultiLineString(lines) => {
            let mut parts = Vec::with_capacity(lines.len());
            for line in lines {
                if let Some((w, cx, cy)) = lineal_component_centroid(line.xs(), line.ys()) {
                    if lineal_part_requires_exact_replay(w, cx, cy) {
                        drop(parts);
                        return exact_owner_centroid(shape, CentroidDimension::Lineal);
                    }
                    parts.push((w, cx, cy));
                }
            }
            let merged = weighted_centroid_merge::<true, true>(&parts);
            let fallback = merged.fallback;
            let centroid = merged.centroid;
            drop(parts);
            match fallback {
                WeightedCentroidMergeFallback::ReplayWholeSource
                | WeightedCentroidMergeFallback::PositiveWeightNormalizationUnderflow => {
                    exact_owner_centroid(shape, CentroidDimension::Lineal)
                },
                WeightedCentroidMergeFallback::None => Ok(centroid),
            }
        },
        Shape::GeometryCollection(collection)
            if collection.topological_dimension() == Dimension::Point =>
        {
            Ok(None)
        },
        Shape::Polygon(_) | Shape::MultiPolygon(_) => {
            let mut total_length = 0.0_f64;
            let mut line_sum = (0.0_f64, 0.0_f64);
            for_each_lineal(shape, &mut |xs, ys| {
                accumulate_lineal_centroid(xs, ys, &mut total_length, &mut line_sum);
            });
            if lineal_fast_result_needs_exact(total_length, line_sum) {
                exact_owner_centroid(shape, CentroidDimension::Lineal)
            } else {
                Ok(finish_lineal_centroid(total_length, line_sum))
            }
        },
        Shape::GeometryCollection(_) => {
            let mut admission = None;
            // Mixed collections: accumulate per lineal leaf, then merge.
            let mut parts = Vec::new();
            let outcome = visit_lineal_owner_runs(shape, &mut admission, &mut |xs, ys| {
                if let Some((w, cx, cy)) = lineal_component_centroid(xs, ys) {
                    if lineal_part_requires_exact_replay(w, cx, cy) {
                        return true;
                    }
                    parts.push((w, cx, cy));
                }
                false
            });
            if let CentroidAttempt::ExactAtRoot(_) = outcome {
                drop(parts);
                return exact_owner_centroid(shape, CentroidDimension::Lineal);
            }
            if parts.len() > 1 {
                let merged = weighted_centroid_merge::<true, true>(&parts);
                let fallback = merged.fallback;
                let centroid = merged.centroid;
                drop(parts);
                match fallback {
                    WeightedCentroidMergeFallback::ReplayWholeSource
                    | WeightedCentroidMergeFallback::PositiveWeightNormalizationUnderflow => {
                        exact_owner_centroid(shape, CentroidDimension::Lineal)
                    },
                    WeightedCentroidMergeFallback::None => Ok(centroid),
                }
            } else if let Some(&(weight, cx, cy)) = parts.first() {
                if lineal_part_requires_exact_replay(weight, cx, cy) {
                    drop(parts);
                    exact_owner_centroid(shape, CentroidDimension::Lineal)
                } else {
                    Ok(Some((cx, cy)))
                }
            } else {
                Ok(None)
            }
        },
    }
}
