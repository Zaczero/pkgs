use super::row::AffineExpansion;
#[cfg(test)]
use super::row::AffineOriginRow;
use super::{
    DekkerParts, ExactSum, GeometryErrorKind, Result, binary_zero, checked_normal_sum,
    normal_or_zero, same_topological_coordinate, two_product_dekker_checked_parts, two_sum,
};

pub(super) fn exact_affine_origin_ordinate(
    coefficient_x: f64,
    coefficient_y: f64,
    output_origin: f64,
    origin_x: f64,
    origin_y: f64,
    value_x: f64,
    value_y: f64,
) -> Result<f64> {
    let mut sum = ExactSum::zero();
    sum.add_product(coefficient_x, value_x);
    sum.add_product(coefficient_y, value_y);
    sum.add_product(output_origin, 1.0);
    sum.subtract_product(coefficient_x, origin_x);
    sum.subtract_product(coefficient_y, origin_y);
    let value = sum.round_to_binary64();
    value
        .is_finite()
        .then_some(value)
        .ok_or_else(|| GeometryErrorKind::NonFiniteCoordinate.into())
}

#[expect(
    clippy::float_cmp,
    reason = "affine identity rows are exact coefficients"
)]
pub(crate) fn affine_x_row_identity(a: f64, b: f64, xoff: f64) -> bool {
    a == 1.0 && same_topological_coordinate(b, 0.0) && same_topological_coordinate(xoff, 0.0)
}

#[expect(
    clippy::float_cmp,
    reason = "affine identity rows are exact coefficients"
)]
pub(crate) fn affine_y_row_identity(d: f64, e: f64, yoff: f64) -> bool {
    same_topological_coordinate(d, 0.0) && e == 1.0 && same_topological_coordinate(yoff, 0.0)
}

/// Round one non-identity origin-relative affine row from the exact stored
/// binary64 expression. Identity rows are handled by callers so signed zero
/// and unchanged coordinates remain bit-identical.
#[cfg(test)]
pub(super) fn affine_about_origin_ordinate(
    coefficient_x: f64,
    coefficient_y: f64,
    output_origin: f64,
    origin_x: f64,
    origin_y: f64,
    value_x: f64,
    value_y: f64,
) -> Result<f64> {
    if !coefficient_x.is_finite()
        || !coefficient_y.is_finite()
        || !output_origin.is_finite()
        || !origin_x.is_finite()
        || !origin_y.is_finite()
        || !value_x.is_finite()
        || !value_y.is_finite()
    {
        return Err(GeometryErrorKind::NonFiniteCoordinate.into());
    }
    AffineOriginRow::new(
        coefficient_x,
        coefficient_y,
        output_origin,
        origin_x,
        origin_y,
        DekkerParts::checked(coefficient_x),
        DekkerParts::checked(coefficient_y),
        DekkerParts::checked(origin_x),
        DekkerParts::checked(origin_y),
        DekkerParts::checked(output_origin),
    )
    .map_finite(value_x, value_y)
}

/// Prepare the three invariant terms once per affine row. The accumulated
/// error bounds the exact bias outside the retained double-double.
pub(super) fn affine_origin_bias(
    output_origin_parts: Option<DekkerParts>,
    coefficient_x_parts: Option<DekkerParts>,
    coefficient_y_parts: Option<DekkerParts>,
    origin_x_parts: Option<DekkerParts>,
    origin_y_parts: Option<DekkerParts>,
) -> Option<AffineExpansion> {
    let (mut high, mut low, mut error) = (0.0, 0.0, 0.0);
    let one = DekkerParts::checked(1.0)?;
    add_affine_product_parts!(high, low, error, output_origin_parts?, one);
    add_affine_product_parts!(
        high,
        low,
        error,
        coefficient_x_parts?.negated(),
        origin_x_parts?
    );
    add_affine_product_parts!(
        high,
        low,
        error,
        coefficient_y_parts?.negated(),
        origin_y_parts?
    );
    Some(AffineExpansion { high, low, error })
}

/// Correctly rounded fast path for the exact five-product origin-relative row.
/// Its invariant bias is prepared once; each coordinate adds only the two
/// variable products. Upward-rounded magnitudes bound all omitted limbs.
#[cfg(test)]
pub(super) fn fast_affine_about_origin_ordinate(
    coefficient_x: f64,
    coefficient_y: f64,
    output_origin: f64,
    origin_x: f64,
    origin_y: f64,
    value_x: f64,
    value_y: f64,
) -> Option<f64> {
    [
        coefficient_x,
        coefficient_y,
        output_origin,
        origin_x,
        origin_y,
        value_x,
        value_y,
    ]
    .into_iter()
    .all(f64::is_finite)
    .then(|| {
        AffineOriginRow::new(
            coefficient_x,
            coefficient_y,
            output_origin,
            origin_x,
            origin_y,
            DekkerParts::checked(coefficient_x),
            DekkerParts::checked(coefficient_y),
            DekkerParts::checked(origin_x),
            DekkerParts::checked(origin_y),
            DekkerParts::checked(output_origin),
        )
        .fast_map_finite(value_x, value_y)
    })
    .flatten()
}

pub(super) fn checked_two_sum(lhs: f64, rhs: f64) -> Option<(f64, f64)> {
    if !normal_or_zero(lhs) || !normal_or_zero(rhs) {
        return None;
    }
    let (sum, error) = two_sum(lhs, rhs);
    (normal_or_zero(sum) && normal_or_zero(error)).then_some((sum, error))
}

/// Add nonnegative error magnitudes with an outward-rounded upper bound.
pub(super) fn upper_sum(lhs: f64, rhs: f64) -> Option<f64> {
    if !normal_or_zero(lhs) || !normal_or_zero(rhs) {
        return None;
    }
    let sum = checked_normal_sum(lhs, rhs)?;
    if binary_zero(sum) {
        return Some(0.0);
    }
    let upper = sum.next_up();
    upper.is_normal().then_some(upper)
}

const fn next_down_finite(value: f64) -> f64 {
    const SIGN: u64 = 1 << 63;
    let bits = value.to_bits();
    let magnitude = bits & !SIGN;
    if magnitude == 0 {
        f64::from_bits(SIGN | 1)
    } else if bits & SIGN == 0 {
        f64::from_bits(bits.wrapping_sub(1))
    } else {
        f64::from_bits(bits.wrapping_add(1))
    }
}

const fn adjacent_finite_values(value: f64) -> (f64, f64) {
    const SIGN: u64 = 1 << 63;
    let bits = value.to_bits();
    let magnitude = bits & !SIGN;
    if magnitude == 0 {
        (f64::from_bits(SIGN | 1), f64::from_bits(1))
    } else if bits & SIGN == 0 {
        (
            f64::from_bits(bits.wrapping_sub(1)),
            f64::from_bits(bits.wrapping_add(1)),
        )
    } else {
        (
            f64::from_bits(bits.wrapping_add(1)),
            f64::from_bits(bits.wrapping_sub(1)),
        )
    }
}

fn lower_bound_certified<const ADMITTED: bool>(value: f64) -> (f64, bool) {
    if ADMITTED {
        (next_down_finite(value), true)
    } else if binary_zero(value) {
        (0.0, true)
    } else {
        let lower = value.next_down();
        (lower, value.is_normal() && normal_or_zero(lower))
    }
}

fn lower_sum_certified<const ADMITTED: bool>(lhs: f64, rhs: f64) -> (f64, bool) {
    let sum = lhs + rhs;
    let (lower, bound_is_valid) = lower_bound_certified::<ADMITTED>(sum);
    let valid = if ADMITTED {
        true
    } else {
        normal_or_zero(sum) && bound_is_valid
    };
    (lower, valid)
}

fn lower_difference_certified<const ADMITTED: bool>(lhs: f64, rhs: f64) -> (f64, bool) {
    let difference = lhs - rhs;
    let (lower, bound_is_valid) = lower_bound_certified::<ADMITTED>(difference);
    let valid = if ADMITTED {
        true
    } else {
        normal_or_zero(difference) && difference >= 0.0 && bound_is_valid
    };
    (lower, valid)
}

/// Accept only when `candidate + tail ± error` stays strictly inside its
/// asymmetric binary64 rounding cell. `ADMITTED` removes range checks already
/// proved by the row bounds; both lanes use the same strict cell inequalities.
pub(super) fn round_certified_double_double<const ADMITTED: bool>(
    high: f64,
    low: f64,
    error: f64,
) -> (f64, bool) {
    let (candidate, tail) = two_sum(high, low);
    let (previous, next) = if ADMITTED {
        adjacent_finite_values(candidate)
    } else {
        (candidate.next_down(), candidate.next_up())
    };
    let lower_gap = candidate - previous;
    let upper_gap = next - candidate;
    let lower_half_gap = lower_gap * 0.5;
    let upper_half_gap = upper_gap * 0.5;
    let tail_magnitude = tail.abs();
    let (lower_margin, lower_is_valid, upper_margin, upper_is_valid) = if tail < 0.0 {
        let (lower, lower_valid) =
            lower_difference_certified::<ADMITTED>(lower_half_gap, tail_magnitude);
        let (upper, upper_valid) = lower_sum_certified::<ADMITTED>(upper_half_gap, tail_magnitude);
        (lower, lower_valid, upper, upper_valid)
    } else {
        let (lower, lower_valid) = lower_sum_certified::<ADMITTED>(lower_half_gap, tail_magnitude);
        let (upper, upper_valid) =
            lower_difference_certified::<ADMITTED>(upper_half_gap, tail_magnitude);
        (lower, lower_valid, upper, upper_valid)
    };
    let range_is_valid = if ADMITTED {
        true
    } else {
        normal_or_zero(high)
            && normal_or_zero(low)
            && normal_or_zero(tail)
            && previous.is_normal()
            && next.is_normal()
            && normal_or_zero(lower_gap)
            && normal_or_zero(upper_gap)
            && lower_half_gap.is_normal()
            && upper_half_gap.is_normal()
            && lower_is_valid
            && upper_is_valid
            && error >= 0.0
            && error.is_finite()
    };
    let certified =
        candidate.is_normal() && range_is_valid && error < lower_margin && error < upper_margin;
    (candidate, certified)
}

#[cfg(test)]
mod affine_rounding_tests {
    use super::exact_affine_origin_ordinate;
    use super::row::{
        AdmittedAffineMode, AdmittedAffineOriginRow, AffineExpansion, AffineLinear,
        AffineOriginRow, AffineRowExtents,
    };
    use crate::geometry::numerics::product::DekkerParts;
    use crate::geometry::numerics::product_sum::ExactSum;

    #[test]
    fn residual_bound_rejects_an_expansion_whose_exact_value_crosses_halfway() {
        // The retained sum is 2^-106 below halfway; the residual moves it
        // 2^-106 above halfway and therefore changes the rounded result.
        let low = 2.0_f64.powi(-53) - 2.0_f64.powi(-106);
        let residual = 2.0_f64.powi(-105);
        let row = AffineOriginRow {
            coefficient_x: 0.0,
            coefficient_y: 0.0,
            coefficient_x_parts: DekkerParts::checked(0.0),
            coefficient_y_parts: DekkerParts::checked(0.0),
            output_origin: 0.0,
            origin_x: 0.0,
            origin_y: 0.0,
            bias: Some(AffineExpansion {
                high: 1.0,
                low,
                error: residual,
            }),
        };
        let mut exact = ExactSum::zero();
        exact.add_product(1.0, 1.0);
        exact.add_product(low, 1.0);
        exact.add_product(residual, 1.0);
        assert_eq!(
            exact.round_to_binary64().to_bits(),
            1.0_f64.next_up().to_bits()
        );
        assert_eq!(row.fast_map_finite(0.0, 0.0), None);
    }

    #[test]
    fn row_admission_rejects_a_product_with_a_half_subnormal_tail() {
        let one_plus_ulp = 1.0 + 2.0_f64.powi(-52);
        let coefficient_x = one_plus_ulp * 2.0_f64.powi(-400);
        let value_x = one_plus_ulp * 2.0_f64.powi(-571);
        let coefficient_y = -0.5;
        let value_y = 2.0_f64.powi(-970) + 2.0_f64.powi(-1021);
        let origin_y = 2.0_f64.powi(-52);
        let row = AffineOriginRow::new(
            coefficient_x,
            coefficient_y,
            1.0,
            0.0,
            origin_y,
            DekkerParts::checked(coefficient_x),
            DekkerParts::checked(coefficient_y),
            DekkerParts::checked(0.0),
            DekkerParts::checked(origin_y),
            DekkerParts::checked(1.0),
        );
        let extents = AffineRowExtents::from_columns(&[value_x], &[value_y])
            .expect("the witness coordinates are finite");
        let expected = exact_affine_origin_ordinate(
            coefficient_x,
            coefficient_y,
            1.0,
            0.0,
            origin_y,
            value_x,
            value_y,
        )
        .unwrap();
        let (unchecked_candidate, unchecked_certified) = AdmittedAffineOriginRow {
            mode: AdmittedAffineMode::General(
                row.coefficient_x_parts.unwrap(),
                row.coefficient_y_parts.unwrap(),
                row.bias.unwrap(),
            ),
        }
        .map(
            value_x,
            value_y,
            DekkerParts::admitted(value_x),
            DekkerParts::admitted(value_y),
        );

        assert_eq!(expected.to_bits(), 1.0_f64.next_up().to_bits());
        assert_eq!(unchecked_candidate.to_bits(), 1.0_f64.to_bits());
        assert!(!unchecked_certified);
        assert_ne!(unchecked_candidate.to_bits(), expected.to_bits());
        assert!(
            row.admitted(extents.x, extents.y).is_none(),
            "admission would allow unchecked result {:#018x}, but ExactSum gives {:#018x}",
            unchecked_candidate.to_bits(),
            expected.to_bits(),
        );
    }

    #[test]
    fn admitted_coordinate_loop_matches_exact_rows_and_fixes_uncertain_vertices() {
        let xs = [
            2.0_f64.powi(-100),
            1.0,
            2.0_f64.powi(100),
            -(2.0_f64.powi(100)),
        ];
        let ys = [
            -(2.0_f64.powi(-80)),
            0.25,
            -(2.0_f64.powi(100)),
            2.0_f64.powi(99),
        ];
        let transform = AffineLinear::new([1.25, -0.75, 0.5, 2.0])
            .unwrap()
            .at((0.0, 0.0))
            .unwrap();
        let [Some(x_row), Some(y_row)] = &transform.rows else {
            panic!("both affine rows are nonidentity")
        };
        let extents = AffineRowExtents::from_columns(&xs, &ys).unwrap();
        assert!(x_row.admitted(extents.x, extents.y).is_some());
        assert!(y_row.admitted(extents.x, extents.y).is_some());

        let mut actual_x = vec![0.0; xs.len()];
        let mut actual_y = vec![0.0; ys.len()];
        transform
            .map_xy_columns(&xs, &ys, Some(&mut actual_x), Some(&mut actual_y))
            .unwrap();
        for (((&x, &y), &mapped_x), &mapped_y) in xs.iter().zip(&ys).zip(&actual_x).zip(&actual_y) {
            assert_eq!(
                mapped_x.to_bits(),
                exact_affine_origin_ordinate(1.25, -0.75, 0.0, 0.0, 0.0, x, y)
                    .unwrap()
                    .to_bits()
            );
            assert_eq!(
                mapped_y.to_bits(),
                exact_affine_origin_ordinate(0.5, 2.0, 0.0, 0.0, 0.0, x, y)
                    .unwrap()
                    .to_bits()
            );
        }

        let upper = 1.0_f64.next_up();
        let midpoint = AffineLinear::new([0.5, 0.0, 0.0, 1.0])
            .unwrap()
            .at((upper, 0.0))
            .unwrap();
        let [Some(x_row), None] = &midpoint.rows else {
            panic!("the X affine row is nonidentity and Y is preserved")
        };
        let (xs, ys) = ([1.0], [0.0]);
        let extents = AffineRowExtents::from_columns(&xs, &ys).unwrap();
        assert!(x_row.admitted(extents.x, extents.y).is_some());
        let mut actual_x = [0.0];
        midpoint
            .map_xy_columns(&xs, &ys, Some(&mut actual_x), None)
            .unwrap();
        let mapped_x = actual_x
            .first()
            .copied()
            .expect("the midpoint row has one coordinate");
        assert_eq!(
            mapped_x.to_bits(),
            exact_affine_origin_ordinate(0.5, 0.0, upper, upper, 0.0, 1.0, 0.0)
                .unwrap()
                .to_bits()
        );
        assert_eq!(mapped_x.to_bits(), 1.0_f64.to_bits());
    }

    #[test]
    fn origin_zero_single_product_rows_match_exact_product_rounding() {
        let xs = [-0.0, 2.0_f64.powi(-500), 1.0, -(2.0_f64.powi(100))];
        let ys = [0.0, -(2.0_f64.powi(-200)), -3.0, 2.0_f64.powi(100)];
        let transform = AffineLinear::new([1.25, 0.0, 0.0, 0.75])
            .unwrap()
            .at((0.0, 0.0))
            .unwrap();
        let [Some(x_row), Some(y_row)] = &transform.rows else {
            panic!("both nonidentity scale rows are present")
        };
        let extents = AffineRowExtents::from_columns(&xs, &ys).unwrap();
        assert!(x_row.admitted(extents.x, extents.y).is_some());
        assert!(y_row.admitted(extents.x, extents.y).is_some());

        let mut actual_x = vec![0.0; xs.len()];
        let mut actual_y = vec![0.0; ys.len()];
        transform
            .map_xy_columns(&xs, &ys, Some(&mut actual_x), Some(&mut actual_y))
            .unwrap();
        for (((&x, &y), &mapped_x), &mapped_y) in xs.iter().zip(&ys).zip(&actual_x).zip(&actual_y) {
            assert_eq!(
                mapped_x.to_bits(),
                exact_affine_origin_ordinate(1.25, 0.0, 0.0, 0.0, 0.0, x, y)
                    .unwrap()
                    .to_bits()
            );
            assert_eq!(
                mapped_y.to_bits(),
                exact_affine_origin_ordinate(0.0, 0.75, 0.0, 0.0, 0.0, x, y)
                    .unwrap()
                    .to_bits()
            );
        }
        assert_eq!(
            actual_x
                .first()
                .copied()
                .expect("the row has coordinates")
                .to_bits(),
            0.0_f64.to_bits()
        );
    }

    #[test]
    fn coordinate_row_with_subnormal_values_uses_the_checked_path() {
        let minimum_subnormal = f64::from_bits(1);
        let transform = AffineLinear::new([2.0, 0.0, 0.0, 1.0])
            .unwrap()
            .at((0.0, 0.0))
            .unwrap();
        let xs = [minimum_subnormal, -minimum_subnormal];
        let ys = [0.0, 0.0];
        let [Some(x_row), None] = &transform.rows else {
            panic!("only the X scale row is nonidentity")
        };
        let extents = AffineRowExtents::from_columns(&xs, &ys).unwrap();
        assert!(x_row.admitted(extents.x, extents.y).is_none());
        let mut actual_x = [0.0; 2];
        transform
            .map_xy_columns(&xs, &ys, Some(&mut actual_x), None)
            .unwrap();
        assert_eq!(actual_x.map(f64::to_bits), [
            f64::from_bits(2).to_bits(),
            f64::from_bits((1 << 63) | 2).to_bits()
        ]);
    }
}
