//! Shewchuk-style floating-point expansion primitives.
//!
//! Storage and admissibility belong to callers: dynamic users may retain every
//! finite or infinite IEEE result, while bounded exact predicates can decline a
//! result that exceeds their capacity or finite domain.

use std::cmp::Ordering;

pub(crate) use crate::geometry::exact_arith::{
    fast_two_sum, two_product_exact as two_product, two_sum,
};

/// Sum two increasing-magnitude expansions into caller-owned storage.
///
/// `NEGATE_RIGHT` subtracts by reading `-right[i]` without cloning. `None` if
/// `output` cannot hold `left.len() + right.len()`, or if `require_finite` is
/// set and a two-sum head is non-finite. Callers that admit infinities pass
/// `require_finite = false`.
#[expect(
    clippy::indexing_slicing,
    reason = "the initial reads follow nonempty checks, merge indices stay below their source lengths, and the checked input-size sum admits every output slot"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "each merge cursor and output length advances only within its checked source-length or output-capacity bound"
)]
pub(crate) fn expansion_sum_zeroelim<const NEGATE_RIGHT: bool>(
    left: &[f64],
    right: &[f64],
    output: &mut [f64],
    require_finite: bool,
) -> Option<usize> {
    if left.len().checked_add(right.len())? > output.len() {
        return None;
    }
    let sign_right = |value: f64| if NEGATE_RIGHT { -value } else { value };
    if right.is_empty() {
        output[..left.len()].copy_from_slice(left);
        return Some(left.len());
    }
    if left.is_empty() {
        if NEGATE_RIGHT {
            for (slot, &value) in output[..right.len()].iter_mut().zip(right) {
                *slot = -value;
            }
        } else {
            output[..right.len()].copy_from_slice(right);
        }
        return Some(right.len());
    }

    let mut left_now = left[0];
    let mut right_now = sign_right(right[0]);
    let mut left_index = 0;
    let mut right_index = 0;
    let mut accumulator;
    if (right_now > left_now) == (right_now > -left_now) {
        accumulator = left_now;
        left_index += 1;
    } else {
        accumulator = right_now;
        right_index += 1;
    }

    let mut output_len = 0;
    if left_index < left.len() && right_index < right.len() {
        left_now = left[left_index];
        right_now = sign_right(right[right_index]);
        let (sum, tail) = if (right_now > left_now) == (right_now > -left_now) {
            left_index += 1;
            fast_two_sum(left_now, accumulator)
        } else {
            right_index += 1;
            fast_two_sum(right_now, accumulator)
        };
        if require_finite && !sum.is_finite() {
            return None;
        }
        accumulator = sum;
        if tail != 0.0 {
            output[output_len] = tail;
            output_len += 1;
        }
        while left_index < left.len() && right_index < right.len() {
            left_now = left[left_index];
            right_now = sign_right(right[right_index]);
            let (sum, tail) = if (right_now > left_now) == (right_now > -left_now) {
                left_index += 1;
                two_sum(accumulator, left_now)
            } else {
                right_index += 1;
                two_sum(accumulator, right_now)
            };
            if require_finite && !sum.is_finite() {
                return None;
            }
            accumulator = sum;
            if tail != 0.0 {
                output[output_len] = tail;
                output_len += 1;
            }
        }
    }
    while left_index < left.len() {
        left_now = left[left_index];
        let (sum, tail) = two_sum(accumulator, left_now);
        if require_finite && !sum.is_finite() {
            return None;
        }
        accumulator = sum;
        left_index += 1;
        if tail != 0.0 {
            output[output_len] = tail;
            output_len += 1;
        }
    }
    while right_index < right.len() {
        right_now = sign_right(right[right_index]);
        let (sum, tail) = two_sum(accumulator, right_now);
        if require_finite && !sum.is_finite() {
            return None;
        }
        accumulator = sum;
        right_index += 1;
        if tail != 0.0 {
            output[output_len] = tail;
            output_len += 1;
        }
    }
    if accumulator != 0.0 || output_len == 0 {
        output[output_len] = accumulator;
        output_len += 1;
    }
    Some(output_len)
}

pub(crate) fn expansion_sign(expansion: &[f64]) -> Ordering {
    expansion
        .iter()
        .rev()
        .find(|&&limb| limb != 0.0)
        .map_or(Ordering::Equal, |limb| limb.total_cmp(&0.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::tessellation::exact::ExactDyadic;

    // The old implementation remains a characterization oracle. Expansion
    // length and limb spelling are not part of its semantic contract.
    fn exact_expansion_value(expansion: &[f64]) -> ExactDyadic {
        expansion
            .iter()
            .copied()
            .fold(ExactDyadic::from_f64(0.0), |sum, limb| {
                sum.try_add(ExactDyadic::from_f64(limb)).unwrap()
            })
    }

    fn assert_semantically_equal(left: &[f64], right: &[f64]) {
        let left_finite = left.iter().all(|value| value.is_finite());
        let right_finite = right.iter().all(|value| value.is_finite());
        assert_eq!(left_finite, right_finite, "finite admission changed");
        if left_finite {
            assert_eq!(
                expansion_sign(left),
                expansion_sign(right),
                "expansion sign changed"
            );
            assert_eq!(
                exact_expansion_value(left),
                exact_expansion_value(right),
                "exact expansion value changed"
            );
        }
    }

    fn old_area_sum(left: &[f64], right: &[f64], output: &mut [f64]) -> usize {
        if right.is_empty() {
            output[..left.len()].copy_from_slice(left);
            return left.len();
        }
        if left.is_empty() {
            output[..right.len()].copy_from_slice(right);
            return right.len();
        }
        let (mut li, mut ri) = (0_usize, 0_usize);
        let (mut l, mut r) = (left[0], right[0]);
        let mut q;
        if (r > l) == (r > -l) {
            q = l;
            li = li.wrapping_add(1);
        } else {
            q = r;
            ri = ri.wrapping_add(1);
        }
        let mut oi = 0;
        if li < left.len() && ri < right.len() {
            l = left[li];
            r = right[ri];
            let (next, tail) = if (r > l) == (r > -l) {
                li = li.wrapping_add(1);
                (l + q, q - ((l + q) - l))
            } else {
                ri = ri.wrapping_add(1);
                (r + q, q - ((r + q) - r))
            };
            q = next;
            if tail != 0.0 {
                output[oi] = tail;
                oi = oi.wrapping_add(1);
            }
            while li < left.len() && ri < right.len() {
                l = left[li];
                r = right[ri];
                let (next, tail) = if (r > l) == (r > -l) {
                    li = li.wrapping_add(1);
                    old_two_sum(q, l)
                } else {
                    ri = ri.wrapping_add(1);
                    old_two_sum(q, r)
                };
                q = next;
                if tail != 0.0 {
                    output[oi] = tail;
                    oi = oi.wrapping_add(1);
                }
            }
        }
        while li < left.len() {
            l = left[li];
            let (next, tail) = old_two_sum(q, l);
            q = next;
            li = li.wrapping_add(1);
            if tail != 0.0 {
                output[oi] = tail;
                oi = oi.wrapping_add(1);
            }
        }
        while ri < right.len() {
            r = right[ri];
            let (next, tail) = old_two_sum(q, r);
            q = next;
            ri = ri.wrapping_add(1);
            if tail != 0.0 {
                output[oi] = tail;
                oi = oi.wrapping_add(1);
            }
        }
        if q != 0.0 || oi == 0 {
            output[oi] = q;
            oi = oi.wrapping_add(1);
        }
        oi
    }

    fn old_two_sum(left: f64, right: f64) -> (f64, f64) {
        let sum = left + right;
        let right_virtual = sum - left;
        (
            sum,
            (left - (sum - right_virtual)) + (right - right_virtual),
        )
    }

    fn product(value: f64) -> [f64; 2] {
        product_by(value, 1.5)
    }

    fn product_by(value: f64, scale: f64) -> [f64; 2] {
        let head = value * scale;
        let tail = if head.is_finite() {
            value.mul_add(scale, -head)
        } else {
            0.0
        };
        if tail == 0.0 {
            [head, 0.0]
        } else {
            [tail, head]
        }
    }

    #[test]
    fn shared_kernel_preserves_legacy_sum_semantics() {
        let mut values = vec![
            0.0,
            -0.0,
            f64::from_bits(1),
            -f64::from_bits(1),
            f64::MIN_POSITIVE,
            -f64::MIN_POSITIVE,
            f64::MAX,
            -f64::MAX,
            2.0_f64.powi(-1022),
            2.0_f64.powi(1023),
            1.0,
            -1.0,
            1.0 + f64::EPSILON,
            -(1.0 + f64::EPSILON),
        ];
        let mut state = 0x9E37_79B9_7F4A_7C15_u64;
        for _ in 0..512 {
            state ^= state << 7;
            state ^= state >> 9;
            state ^= state << 8;
            values.push(f64::from_bits(state & 0x7FEF_FFFF_FFFF_FFFF));
            values.push(-f64::from_bits(
                state.rotate_left(29) & 0x7FEF_FFFF_FFFF_FFFF,
            ));
        }
        for pair in values.as_chunks::<2>().0 {
            let left = product(pair[0]);
            let right = product(pair[1]);
            let left = &left[..=usize::from(left[1] != 0.0)];
            let right = &right[..=usize::from(right[1] != 0.0)];
            let mut legacy = [0.0; 4];
            let legacy_len = old_area_sum(left, right, &mut legacy);
            let mut shared = [0.0; 4];
            let shared_len =
                expansion_sum_zeroelim::<false>(left, right, &mut shared, false).unwrap();
            assert_semantically_equal(&legacy[..legacy_len], &shared[..shared_len]);
        }

        // The finite policy declines overflow before a partial expansion
        // escapes to a caller.
        let mut output = [0.0; 2];
        assert!(
            expansion_sum_zeroelim::<false>(&[f64::MAX], &[f64::MAX], &mut output, true).is_none()
        );
    }

    #[test]
    fn negate_right_cancels_two_limb_unit_plus_half_ulp() {
        // 1 - 2^-53 is a single binary64. 1 + 2^-53 is not: the sum of those
        // two one-limb expansions needs two limbs.
        let unit = [1.0];
        let half_ulp = [2.0_f64.powi(-53)];
        let mut sum = [0.0; 2];
        let sum_len = expansion_sum_zeroelim::<false>(&unit, &half_ulp, &mut sum, true).unwrap();
        assert!(sum_len > 1);

        let mut cancelled = [0.0; 3];
        let cancelled_len =
            expansion_sum_zeroelim::<true>(&sum[..sum_len], &unit, &mut cancelled, true).unwrap();
        assert_semantically_equal(&cancelled[..cancelled_len], &half_ulp);

        let mut empty_right = [0.0; 2];
        let empty_len =
            expansion_sum_zeroelim::<true>(&sum[..sum_len], &[], &mut empty_right, true).unwrap();
        assert_semantically_equal(&empty_right[..empty_len], &sum[..sum_len]);

        let mut empty_left = [0.0; 1];
        let empty_left_len =
            expansion_sum_zeroelim::<true>(&[], &half_ulp, &mut empty_left, true).unwrap();
        assert_eq!(&empty_left[..empty_left_len], &[-half_ulp[0]]);

        let mut too_small = [0.0; 1];
        assert!(expansion_sum_zeroelim::<false>(&unit, &half_ulp, &mut too_small, true).is_none());

        let mut overflow = [0.0; 2];
        assert!(
            expansion_sum_zeroelim::<true>(&[f64::MAX], &[-f64::MAX], &mut overflow, true)
                .is_none()
        );
    }
}
