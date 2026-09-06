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
/// `None` means only that the supplied storage is too small. Finite-domain
/// admission remains a caller policy.
pub(crate) fn expansion_sum_zeroelim(
    left: &[f64],
    right: &[f64],
    output: &mut [f64],
    require_finite: bool,
) -> Option<usize> {
    if left.len().checked_add(right.len())? > output.len() {
        return None;
    }
    if right.is_empty() {
        output[..left.len()].copy_from_slice(left);
        return Some(left.len());
    }
    if left.is_empty() {
        output[..right.len()].copy_from_slice(right);
        return Some(right.len());
    }

    let mut left_now = left[0];
    let mut right_now = right[0];
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
        right_now = right[right_index];
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
            right_now = right[right_index];
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
        right_now = right[right_index];
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

/// Multiply an expansion by one scalar using caller-owned ping-pong storage.
/// The result is retained in `output`; both buffers need room for twice the
/// input length plus the canonical zero limb.
pub(crate) fn scale_expansion_zeroelim(
    input: &[f64],
    scale: f64,
    output: &mut [f64],
    scratch: &mut [f64],
    require_finite: bool,
) -> Option<usize> {
    let required = input.len().checked_mul(2)?.checked_add(1)?;
    if required > output.len() || required > scratch.len() {
        return None;
    }
    if let [value] = input {
        let (head, tail) = two_product(*value, scale);
        if !head.is_finite() || !tail.is_finite() {
            if require_finite {
                return None;
            }
        } else {
            if tail != 0.0 {
                output[0] = tail;
                if head != 0.0 {
                    output[1] = head;
                    return Some(2);
                }
                return Some(1);
            }
            output[0] = if head == 0.0 { 0.0 } else { head };
            return Some(1);
        }
    }
    output[0] = 0.0;
    let mut output_len = 1;
    for &value in input {
        let (head, tail) = two_product(value, scale);
        let term = if tail == 0.0 {
            [head, 0.0]
        } else {
            [tail, head]
        };
        let term_len = usize::from(tail != 0.0) + 1;
        if require_finite && (!head.is_finite() || !tail.is_finite()) {
            return None;
        }
        let next_len = expansion_sum_zeroelim(
            &output[..output_len],
            &term[..term_len],
            scratch,
            require_finite,
        )?;
        output[..next_len].copy_from_slice(&scratch[..next_len]);
        output_len = next_len;
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
                sum.add(ExactDyadic::from_f64(limb))
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
        let (mut li, mut ri) = (0, 0);
        let (mut l, mut r) = (left[0], right[0]);
        let mut q;
        if (r > l) == (r > -l) {
            q = l;
            li += 1;
        } else {
            q = r;
            ri += 1;
        }
        let mut oi = 0;
        if li < left.len() && ri < right.len() {
            l = left[li];
            r = right[ri];
            let (next, tail) = if (r > l) == (r > -l) {
                li += 1;
                (l + q, q - ((l + q) - l))
            } else {
                ri += 1;
                (r + q, q - ((r + q) - r))
            };
            q = next;
            if tail != 0.0 {
                output[oi] = tail;
                oi += 1;
            }
            while li < left.len() && ri < right.len() {
                l = left[li];
                r = right[ri];
                let (next, tail) = if (r > l) == (r > -l) {
                    li += 1;
                    old_two_sum(q, l)
                } else {
                    ri += 1;
                    old_two_sum(q, r)
                };
                q = next;
                if tail != 0.0 {
                    output[oi] = tail;
                    oi += 1;
                }
            }
        }
        while li < left.len() {
            l = left[li];
            let (next, tail) = old_two_sum(q, l);
            q = next;
            li += 1;
            if tail != 0.0 {
                output[oi] = tail;
                oi += 1;
            }
        }
        while ri < right.len() {
            r = right[ri];
            let (next, tail) = old_two_sum(q, r);
            q = next;
            ri += 1;
            if tail != 0.0 {
                output[oi] = tail;
                oi += 1;
            }
        }
        if q != 0.0 || oi == 0 {
            output[oi] = q;
            oi += 1;
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

    fn old_scale(input: &[f64], scale: f64, output: &mut [f64], scratch: &mut [f64]) -> usize {
        output[0] = 0.0;
        let mut output_len = 1;
        for &value in input {
            let head = value * scale;
            let tail = if head.is_finite() {
                value.mul_add(scale, -head)
            } else {
                0.0
            };
            let term = if tail == 0.0 {
                [head, 0.0]
            } else {
                [tail, head]
            };
            let term_len = 1 + usize::from(tail != 0.0);
            let next_len = old_area_sum(&output[..output_len], &term[..term_len], scratch);
            output[..next_len].copy_from_slice(&scratch[..next_len]);
            output_len = next_len;
        }
        output_len
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
            let shared_len = expansion_sum_zeroelim(left, right, &mut shared, false).unwrap();
            assert_semantically_equal(&legacy[..legacy_len], &shared[..shared_len]);

            let mut legacy_scale = [0.0; 5];
            let mut legacy_scratch = [0.0; 5];
            let legacy_scale_len = old_scale(left, pair[1], &mut legacy_scale, &mut legacy_scratch);
            let mut shared_scale = [0.0; 5];
            let mut shared_scratch = [0.0; 5];
            let shared_scale_len = scale_expansion_zeroelim(
                left,
                pair[1],
                &mut shared_scale,
                &mut shared_scratch,
                false,
            )
            .unwrap();
            assert_semantically_equal(
                &legacy_scale[..legacy_scale_len],
                &shared_scale[..shared_scale_len],
            );
        }

        // The finite policy declines overflow before a partial expansion
        // escapes to a caller.
        let mut output = [0.0; 2];
        assert!(expansion_sum_zeroelim(&[f64::MAX], &[f64::MAX], &mut output, true).is_none());
        let mut scaled = [0.0; 3];
        let mut scratch = [0.0; 3];
        assert!(
            scale_expansion_zeroelim(&[f64::MAX], 2.0, &mut scaled, &mut scratch, true).is_none()
        );
    }

    #[test]
    fn scale_specialization_preserves_legacy_semantics_through_64_limbs() {
        let scales = [1.5, -1.5, f64::from_bits(1), -f64::from_bits(1), f64::MAX];
        for length in 0..=64 {
            let input = (0..length)
                .map(|index| {
                    let exponent = -900 + index as i32 * 28;
                    let mantissa = 1.0 + (index as f64 + 1.0) * f64::EPSILON;
                    mantissa * 2.0_f64.powi(exponent)
                })
                .collect::<Vec<_>>();
            for &scale in &scales {
                let mut legacy = vec![0.0; length * 2 + 1];
                let mut legacy_scratch = vec![0.0; length * 2 + 1];
                let legacy_len = old_scale(&input, scale, &mut legacy, &mut legacy_scratch);
                let mut actual = vec![0.0; length * 2 + 1];
                let mut actual_scratch = vec![0.0; length * 2 + 1];
                let actual_len = scale_expansion_zeroelim(
                    &input,
                    scale,
                    &mut actual,
                    &mut actual_scratch,
                    false,
                )
                .unwrap();
                assert_semantically_equal(&legacy[..legacy_len], &actual[..actual_len]);
            }
        }

        for &(value, scale) in &[
            (f64::NAN, 1.0),
            (f64::INFINITY, 1.0),
            (f64::NEG_INFINITY, 1.0),
            (1.0, f64::NAN),
            (1.0, f64::INFINITY),
        ] {
            let mut actual = [0.0; 3];
            let mut actual_scratch = [0.0; 3];
            assert!(
                scale_expansion_zeroelim(&[value], scale, &mut actual, &mut actual_scratch, true,)
                    .is_none(),
                "value={value}, scale={scale}"
            );
        }
    }
}
