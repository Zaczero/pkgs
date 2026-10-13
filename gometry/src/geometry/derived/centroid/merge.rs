use super::*;

/// Streaming Neumaier accumulator for scalar cancellation and component merges,
/// without staging contributions.
#[derive(Default)]
pub(super) struct NeumaierSum {
    sum: f64,
    correction: f64,
}

impl NeumaierSum {
    pub(super) fn add(&mut self, value: f64) {
        let next = self.sum + value;
        if self.sum.abs() >= value.abs() {
            self.correction += (self.sum - next) + value;
        } else {
            self.correction += (value - next) + self.sum;
        }
        self.sum = next;
    }

    pub(super) fn total(self) -> f64 {
        self.sum + self.correction
    }
}

pub(super) fn neumaier_sum(values: impl IntoIterator<Item = f64>) -> f64 {
    let mut sum = NeumaierSum::default();
    for value in values {
        sum.add(value);
    }
    sum.total()
}

pub(super) fn positive_weight_normalizes_to_zero(weight: f64, normalized: f64) -> bool {
    weight > 0.0 && normalized == 0.0
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WeightedCentroidMergeFallback {
    None,
    ReplayWholeSource,
    PositiveWeightNormalizationUnderflow,
}

pub(super) struct WeightedCentroidMergeResult {
    pub(super) centroid: Option<(f64, f64)>,
    pub(super) fallback: WeightedCentroidMergeFallback,
}

/// Area/length-weighted merge of component centroids. Ordinary mid-range
/// inputs use a flat weighted sum (bit-stable). Large absolute coordinates or
/// detected cancellation scale by a power-of-two first and Neumaier-sum the
/// scaled weighted coordinates so order cannot move the result (N6).
///
/// Finite weights are normalized by their finite maximum before *both* the
/// denominator and coordinate moments. Infinite weights and nonfinite lineal
/// centers require whole-source replay before any moment is accumulated.
///
/// With `LINEAL_PARTS_ADMITTED`, the first scan owns replay admission; the fast
/// and stable moment scans can then skip their repeated validity checks.
/// Lineal parts come from positive sums of nonnegative segment lengths, so
/// after replay sentinels are removed their weights are positive and finite.
pub(super) fn weighted_centroid_merge<
    const DETECT_POSITIVE_WEIGHT_UNDERFLOW: bool,
    const LINEAL_PARTS_ADMITTED: bool,
>(
    parts: &[(f64, f64, f64)],
) -> WeightedCentroidMergeResult {
    if parts.is_empty() {
        return WeightedCentroidMergeResult {
            centroid: None,
            fallback: WeightedCentroidMergeFallback::None,
        };
    }
    let mut max_weight = 0.0_f64;
    let mut max_abs_x = 0.0_f64;
    let mut max_abs_y = 0.0_f64;
    for &(w, cx, cy) in parts {
        if LINEAL_PARTS_ADMITTED {
            if lineal_part_requires_exact_replay(w, cx, cy) {
                return WeightedCentroidMergeResult {
                    centroid: None,
                    fallback: WeightedCentroidMergeFallback::ReplayWholeSource,
                };
            }
        } else {
            if !(w > 0.0 && cx.is_finite() && cy.is_finite()) {
                continue;
            }
            if !w.is_finite() {
                return WeightedCentroidMergeResult {
                    centroid: None,
                    fallback: WeightedCentroidMergeFallback::None,
                };
            }
        }
        max_weight = max_weight.max(w);
        max_abs_x = max_abs_x.max(cx.abs());
        max_abs_y = max_abs_y.max(cy.abs());
    }
    if !(max_weight > 0.0 && max_weight.is_finite()) {
        return WeightedCentroidMergeResult {
            centroid: None,
            fallback: WeightedCentroidMergeFallback::None,
        };
    }
    // Fast path: unscaled weighted sum when coordinates are mid-range.
    if max_abs_x.max(max_abs_y) <= AREAL_EXACT_MAX_COORD {
        let mut sx = 0.0_f64;
        let mut sy = 0.0_f64;
        let mut w_sum = 0.0_f64;
        for &(w, cx, cy) in parts {
            if !(LINEAL_PARTS_ADMITTED
                || (w > 0.0 && w.is_finite() && cx.is_finite() && cy.is_finite()))
            {
                continue;
            }
            let normalized = w / max_weight;
            if DETECT_POSITIVE_WEIGHT_UNDERFLOW && positive_weight_normalizes_to_zero(w, normalized)
            {
                return WeightedCentroidMergeResult {
                    centroid: None,
                    fallback: WeightedCentroidMergeFallback::PositiveWeightNormalizationUnderflow,
                };
            }
            w_sum += normalized;
            sx += normalized * cx;
            sy += normalized * cy;
        }
        let (mx, my) = (sx / w_sum, sy / w_sum);
        if mx.is_finite() && my.is_finite() {
            // Cancellation guard: if the mean is tiny vs max |c|, recompute.
            if max_abs_x.max(max_abs_y) == 0.0
                || mx.abs().max(my.abs()) >= max_abs_x.max(max_abs_y) * f64::EPSILON * 32.0
            {
                return WeightedCentroidMergeResult {
                    centroid: Some((canonicalize_zero(mx), canonicalize_zero(my))),
                    fallback: WeightedCentroidMergeFallback::None,
                };
            }
        }
    }
    // Stable path: scale each coordinate axis independently, then
    // Neumaier-sum the normalized-weight moments. A single scale chosen from
    // huge X makes a finite subnormal Y round to zero before it is merged.
    // The normalization is load-bearing: doing it only in the coordinate sums
    // still lets the denominator overflow.
    let scale_x = axis_pow2_scale(max_abs_x);
    let scale_y = axis_pow2_scale(max_abs_y);
    let inv_scale_x = 1.0 / scale_x;
    let inv_scale_y = 1.0 / scale_y;
    let mut w_sum = 0.0_f64;
    let mut x_sum = NeumaierSum::default();
    let mut y_sum = NeumaierSum::default();
    for &(w, cx, cy) in parts {
        if !(LINEAL_PARTS_ADMITTED
            || (w > 0.0 && w.is_finite() && cx.is_finite() && cy.is_finite()))
        {
            continue;
        }
        let normalized = w / max_weight;
        if DETECT_POSITIVE_WEIGHT_UNDERFLOW && positive_weight_normalizes_to_zero(w, normalized) {
            return WeightedCentroidMergeResult {
                centroid: None,
                fallback: WeightedCentroidMergeFallback::PositiveWeightNormalizationUnderflow,
            };
        }
        w_sum += normalized;
        x_sum.add(normalized * (cx * scale_x));
        y_sum.add(normalized * (cy * scale_y));
    }
    if w_sum <= 0.0 {
        return WeightedCentroidMergeResult {
            centroid: None,
            fallback: WeightedCentroidMergeFallback::None,
        };
    }
    WeightedCentroidMergeResult {
        centroid: Some((
            canonicalize_zero((x_sum.total() / w_sum) * inv_scale_x),
            canonicalize_zero((y_sum.total() / w_sum) * inv_scale_y),
        )),
        fallback: WeightedCentroidMergeFallback::None,
    }
}
