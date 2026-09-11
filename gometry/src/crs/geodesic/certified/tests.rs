use std::cmp::Ordering;

use super::taylor::{
    TaylorOptions, TaylorPhaseInterval, TaylorSource, TaylorSurface, evaluate as evaluate_taylor,
    evaluate_enclosed as evaluate_taylor_enclosed,
};
use super::{
    Degree, Dyadic, Enclosure, ExactBinary64, PrecisionContext, PrecisionPolicy, RoundedBinary64,
    RoundingCell, Sign,
};
use crate::error::{Error, ErrorKind, ResourceError};
use crate::test_support::fail_allocations_after;

fn context() -> PrecisionContext {
    PrecisionContext::default_context()
}

fn wide_context() -> PrecisionContext {
    PrecisionContext::new(PrecisionPolicy::new(
        4_096,
        4_096,
        1_000_000,
        16 * 1024 * 1024,
    ))
    .expect("wide test policy")
}

fn exact(value: f64, context: &mut PrecisionContext) -> Dyadic {
    ExactBinary64::from_f64(value)
        .expect("finite test value")
        .exact(context)
        .expect("test precision policy")
}

fn integer(value: i128, context: &mut PrecisionContext) -> Dyadic {
    Dyadic::from_i128(value, context).expect("test integer")
}

fn assert_numerical(error: Error) {
    assert!(matches!(error.kind(), ErrorKind::NumericalIndeterminacy));
}

#[test]
fn finite_binary64_provenance_round_trips_representative_boundaries() {
    let fractions = [0, 1, 1_u64 << 51, (1_u64 << 52) - 1];
    let exponents = [0, 1, 2, 1022, 1023, 2046];
    for exponent in exponents {
        for fraction in fractions {
            for sign in [0_u64, 1_u64 << 63] {
                let bits = sign | (exponent << 52) | fraction;
                let value = ExactBinary64::from_bits(bits).expect("finite representative");
                assert_eq!(value.bits(), bits);
                assert_eq!(value.to_f64().to_bits(), bits);
            }
        }
    }
    for bits in [
        0_u64,
        1_u64 << 63,
        1,
        (1_u64 << 52) - 1,
        1_u64 << 52,
        0x000F_FFFF_FFFF_FFFF,
        0x7FE0_0000_0000_0000,
        0x7FEF_FFFF_FFFF_FFFF,
        1_u64 << 63 | 1,
        1_u64 << 63 | 0x7FE0_0000_0000_0000,
        0xFFEF_FFFF_FFFF_FFFF,
    ] {
        let value = ExactBinary64::from_bits(bits).expect("finite boundary");
        assert_eq!(value.to_f64().to_bits(), bits);
    }
    let mut context = context();
    for bits in [
        1_u64,
        (1_u64 << 52) - 1,
        1_u64 << 52,
        0x3FF0_0000_0000_0001,
        0x7FEF_FFFF_FFFF_FFFF,
        1_u64 << 63 | 1,
        1_u64 << 63 | 0x3FF0_0000_0000_0001,
        0xFFEF_FFFF_FFFF_FFFF,
    ] {
        let value = ExactBinary64::from_bits(bits).expect("finite rounding representative");
        assert_eq!(
            value
                .exact(&mut context)
                .expect("exact rounding representative")
                .round_binary64(&mut context)
                .expect("binary64 rounding"),
            RoundedBinary64::Finite(bits)
        );
    }
    assert!(ExactBinary64::from_bits(0x7FF0_0000_0000_0000).is_err());
    assert!(ExactBinary64::from_bits(0x7FFF_FFFF_FFFF_FFFF).is_err());
}

#[test]
fn exact_one_minus_flattening_and_e_retain_subnormal_information() {
    let mut context = wide_context();
    let one = exact(1.0, &mut context);
    let two = exact(2.0, &mut context);
    let minimum_flattening = exact(f64::from_bits(1), &mut context);
    let one_minus = one
        .sub(&minimum_flattening, &mut context)
        .expect("exact 1-f");
    assert_eq!(one_minus.sign(), Sign::Positive);
    assert_eq!(one_minus.exponent(), -1074);
    assert_eq!(one_minus.significand_bits(), 1074);
    assert!(one_minus.significand_is_all_ones());
    assert!(one_minus.significand_bit(1073));

    let complement = two
        .sub(&minimum_flattening, &mut context)
        .expect("exact 2-f");
    let eccentricity = minimum_flattening
        .mul(&complement, &mut context)
        .expect("exact f*(2-f)");
    assert_eq!(eccentricity.sign(), Sign::Positive);
    assert_eq!(eccentricity.exponent(), -2148);
    assert_eq!(eccentricity.significand_bits(), 1075);
    assert!(eccentricity.significand_is_all_ones());

    let next_down_one = exact(f64::from_bits(0x3FEF_FFFF_FFFF_FFFF), &mut context);
    let one_minus_next = one
        .sub(&next_down_one, &mut context)
        .expect("exact next-down difference");
    assert_eq!(one_minus_next.significand_u128(), Some(1));
    assert_eq!(one_minus_next.exponent(), -53);
    let next_complement = two
        .sub(&next_down_one, &mut context)
        .expect("exact next-down complement");
    let next_eccentricity = next_down_one
        .mul(&next_complement, &mut context)
        .expect("exact next-down E");
    assert_eq!(next_eccentricity.exponent(), -106);
    assert_eq!(next_eccentricity.significand_bits(), 106);
    assert!(next_eccentricity.significand_is_all_ones());
}

#[test]
fn degree_ingress_keeps_minimum_subnormal_and_replays_source_bits() {
    let degree = Degree::from_f64(f64::from_bits(1)).expect("finite degree");
    assert_eq!(degree.bits(), 1);
    let mut context = context();
    let exact_degree = degree.exact(&mut context).expect("exact degree");
    assert_eq!(exact_degree.sign(), Sign::Positive);
    assert_eq!(exact_degree.exponent(), -1074);
    assert_eq!(exact_degree.significand_u128(), Some(1));
}

#[test]
fn endpoint_longitude_reduction_uses_exact_subtraction_at_the_180_seam() {
    let lambda1 = Degree::from_f64(f64::from_bits(0xBC30_0000_0000_0000)).expect("-2^-60");
    let lambda2 = Degree::from_f64(180.0).expect("180 degrees");
    let mut context = context();
    let reduced = super::principal_longitude_difference(lambda1, lambda2, &mut context)
        .expect("principal longitude");
    let expected = Dyadic::from_parts(
        Sign::Negative,
        super::natural::Magnitude::from_u128((180_i128 << 60) as u128 - 1),
        -60,
        &mut context,
    )
    .expect("exact principal representative");
    assert_eq!(reduced, expected);
    assert_eq!(reduced.sign(), Sign::Negative);
    assert_eq!(reduced.exponent(), -60);

    let zero = Degree::from_f64(0.0).expect("zero degrees");
    let five_hundred = Degree::from_f64(500.0).expect("500 degrees");
    assert_eq!(
        super::principal_longitude_difference(zero, five_hundred, &mut context)
            .expect("positive principal longitude"),
        integer(140, &mut context),
    );
    assert_eq!(
        super::principal_longitude_difference(five_hundred, zero, &mut context)
            .expect("negative principal longitude"),
        integer(-140, &mut context),
    );
}

#[test]
fn cancellation_heavy_addition_and_subtraction_remain_exact() {
    let mut context = context();
    let tiny = Dyadic::from_parts(
        Sign::Positive,
        super::natural::Magnitude::from_u128(1),
        -100,
        &mut context,
    )
    .expect("tiny dyadic");
    let one = integer(1, &mut context);
    let near_one = one.sub(&tiny, &mut context).expect("near one");
    let recovered = near_one.add(&tiny, &mut context).expect("cancellation");
    assert_eq!(recovered, one);
    let cancellation = one.sub(&one, &mut context).expect("zero cancellation");
    assert_eq!(cancellation.sign(), Sign::Zero);
    assert!(cancellation.is_zero());
}

#[test]
fn division_and_square_root_publish_residual_verified_endpoints() {
    let mut context = wide_context();
    let one = integer(1, &mut context);
    let three = integer(3, &mut context);
    let quotient = one.divide(&three, &mut context).expect("1/3 enclosure");
    assert!(quotient.lo() < &one);
    assert!(quotient.hi() > &one.scale_pow2(-2, &mut context).expect("quarter"));
    let lower_product = quotient
        .lo()
        .mul(&three, &mut context)
        .expect("lower residual product");
    let upper_product = quotient
        .hi()
        .mul(&three, &mut context)
        .expect("upper residual product");
    assert!(lower_product <= one);
    assert!(upper_product >= one);
    assert!(quotient.lo() < quotient.hi());

    let two = integer(2, &mut context);
    let root = two.sqrt(&mut context).expect("sqrt enclosure");
    let mut residual_context = PrecisionContext::new(PrecisionPolicy::new(
        16_384,
        16_384,
        1_000_000,
        16 * 1024 * 1024,
    ))
    .expect("residual verification policy");
    let lower_square = root
        .lo()
        .square(&mut residual_context)
        .expect("lower square");
    let upper_square = root
        .hi()
        .square(&mut residual_context)
        .expect("upper square");
    assert!(lower_square <= two);
    assert!(upper_square >= two);
    // Negative control: an inward candidate cannot satisfy the lower-square
    // inequality merely because the interval overlaps the true root.
    let inward_lower = root
        .hi()
        .square(&mut residual_context)
        .expect("inward square");
    assert!(inward_lower > two);

    let mut perfect_context = wide_context();
    let nine = integer(9, &mut perfect_context);
    let perfect = nine
        .sqrt(&mut perfect_context)
        .expect("perfect square root");
    let three = integer(3, &mut perfect_context);
    assert_eq!(perfect.lo(), &three);
    assert_eq!(perfect.hi(), &three);
}

#[test]
fn enclosure_composition_narrows_wider_scratch_at_active_precision() {
    let mut context = context();
    let one = integer(1, &mut context);
    let three = integer(3, &mut context);
    let third = one.divide(&three, &mut context).expect("1/3 enclosure");
    let three_interval = Enclosure::exact(
        three.try_clone(&mut context).expect("three singleton"),
        &mut context,
    )
    .expect("three enclosure");
    let product = third
        .mul(&three_interval, &mut context)
        .expect("composed 1/3 product");
    assert!(product.lo() <= &one);
    assert!(product.hi() >= &one);
    assert!(product.lo().significand_bits() <= 128);
    assert!(product.hi().significand_bits() <= 128);

    let two = integer(2, &mut context);
    let root = two.sqrt(&mut context).expect("sqrt enclosure");
    let squared = root.mul(&root, &mut context).expect("composed sqrt square");
    assert!(squared.lo() <= &two);
    assert!(squared.hi() >= &two);
    assert!(squared.lo().significand_bits() <= 128);
    assert!(squared.hi().significand_bits() <= 128);

    let half = one
        .scale_pow2(-1, &mut context)
        .expect("half ingress scale");
    let half_interval = Enclosure::exact(half, &mut context).expect("half enclosure");
    let mut chain = third.try_clone(&mut context).expect("chain seed");
    for _ in 0..8 {
        chain = chain
            .mul(&three_interval, &mut context)
            .expect("bounded chain product");
        chain = chain.add(&third, &mut context).expect("bounded chain sum");
        chain = chain
            .mul(&half_interval, &mut context)
            .expect("bounded chain scale");
        assert!(chain.lo().significand_bits() <= 128);
        assert!(chain.hi().significand_bits() <= 128);
    }
}

#[test]
fn division_reduces_a_wide_significand_with_an_integer_scaled_denominator() {
    let mut source_context = wide_context();
    let one = integer(1, &mut source_context);
    let minimum = ExactBinary64::from_bits(1)
        .expect("minimum subnormal")
        .exact(&mut source_context)
        .expect("minimum ingress");
    let wide = one
        .sub(&minimum, &mut source_context)
        .expect("wide exact numerator");
    let three = integer(3, &mut source_context);

    // The active precision is deliberately smaller than the numerator's
    // stored significand.  Division therefore shifts the integer denominator
    // rather than rounding or first normalising the numerator through f64.
    let mut quotient_context = context();
    let quotient = wide
        .divide(&three, &mut quotient_context)
        .expect("scaled integer quotient");
    let mut verify_context = wide_context();
    let lower = quotient
        .lo()
        .mul(&three, &mut verify_context)
        .expect("lower wide residual");
    let upper = quotient
        .hi()
        .mul(&three, &mut verify_context)
        .expect("upper wide residual");
    assert!(lower <= wide);
    assert!(upper >= wide);
}

#[test]
fn enclosure_intersection_hull_and_sign_are_exact() {
    let mut context = context();
    let one = integer(1, &mut context);
    let two = integer(2, &mut context);
    let three = integer(3, &mut context);
    let left = Enclosure::new(
        one.try_clone(&mut context).expect("left lower"),
        two.try_clone(&mut context).expect("left upper"),
    )
    .expect("ordered left enclosure");
    let right = Enclosure::new(
        one.try_clone(&mut context).expect("right lower"),
        three.try_clone(&mut context).expect("right upper"),
    )
    .expect("ordered right enclosure");
    assert_eq!(left.sign(), Some(Sign::Positive));
    let intersection = left
        .intersection(&right, &mut context)
        .expect("intersection")
        .expect("overlap");
    assert_eq!(intersection.lo(), &one);
    assert_eq!(intersection.hi(), &two);
    let hull = left.hull(&right, &mut context).expect("hull");
    assert_eq!(hull.lo(), &one);
    assert_eq!(hull.hi(), &three);

    let same_left = Enclosure::new(
        one.try_clone(&mut context).expect("same lower"),
        two.try_clone(&mut context).expect("same upper"),
    )
    .expect("same left enclosure");
    let same_right = Enclosure::new(
        one.try_clone(&mut context).expect("same lower copy"),
        two.try_clone(&mut context).expect("same upper copy"),
    )
    .expect("same right enclosure");
    // Negative control: equal endpoints describe overlapping unknown values,
    // not equality of the values hidden inside those intervals.
    assert_eq!(same_left.ordering(&same_right), None);
    let singleton_left = Enclosure::exact(
        one.try_clone(&mut context).expect("singleton"),
        &mut context,
    )
    .expect("singleton left");
    let singleton_right = Enclosure::exact(one, &mut context).expect("singleton right");
    assert_eq!(
        singleton_left.ordering(&singleton_right),
        Some(Ordering::Equal)
    );
}

#[test]
fn exponent_overflow_is_not_resource_or_numerical_exhaustion() {
    let policy = PrecisionPolicy::new(128, 128, 10_000, 1024).with_exponent_range(-10, 10);
    let mut context = PrecisionContext::new(policy).expect("bounded exponent policy");
    let one = integer(1, &mut context);
    let error = one
        .scale_pow2(11, &mut context)
        .expect_err("overflowing scale");
    assert!(matches!(
        error.kind(),
        ErrorKind::Crs(crate::crs::CrsError::GeodesicExponentOverflow)
    ));
    assert!(!matches!(
        error.kind(),
        ErrorKind::Resource(ResourceError::SizeOverflow)
    ));
}

#[test]
fn precision_fuel_and_workspace_exhaustion_are_retryable_numerical_results() {
    let low_precision = PrecisionPolicy::new(32, 32, 10_000, 1024);
    let mut context = PrecisionContext::new(low_precision).expect("low precision policy");
    let value =
        ExactBinary64::from_f64(f64::from_bits(0x3FEF_FFFF_FFFF_FFFF)).expect("finite value");
    let error = value.exact(&mut context).expect_err("precision exhaustion");
    assert_numerical(error);

    let mut fuel_context =
        PrecisionContext::new(PrecisionPolicy::new(128, 128, 0, 1024)).expect("zero-fuel policy");
    let one = integer(1, &mut fuel_context);
    let error = one
        .add(&one, &mut fuel_context)
        .expect_err("fuel exhaustion");
    assert_numerical(error);

    let mut limited_fuel_context =
        PrecisionContext::new(PrecisionPolicy::new(4_096, 4_096, 1, 16 * 1024 * 1024))
            .expect("limited fuel policy");
    let one = integer(1, &mut limited_fuel_context);
    let minimum = ExactBinary64::from_bits(1)
        .expect("minimum subnormal")
        .exact(&mut limited_fuel_context)
        .expect("minimum subnormal ingress");
    assert_numerical(
        one.sub(&minimum, &mut limited_fuel_context)
            .expect_err("wide arithmetic fuel exhaustion"),
    );

    let mut workspace_context = PrecisionContext::new(PrecisionPolicy::new(4096, 4096, 10_000, 0))
        .expect("zero-workspace policy");
    let one = integer(1, &mut workspace_context);
    let minimum = ExactBinary64::from_bits(1)
        .expect("minimum subnormal")
        .exact(&mut workspace_context)
        .expect("minimum subnormal ingress");
    let error = one
        .sub(&minimum, &mut workspace_context)
        .expect_err("workspace exhaustion");
    assert_numerical(error);

    let retry_policy = PrecisionPolicy::new(4096, 4096, 100_000, 16 * 1024 * 1024);
    let mut retry_context = PrecisionContext::new(retry_policy).expect("larger retry policy");
    let one = integer(1, &mut retry_context);
    let minimum = ExactBinary64::from_bits(1)
        .expect("minimum subnormal")
        .exact(&mut retry_context)
        .expect("minimum subnormal ingress");
    assert!(one.sub(&minimum, &mut retry_context).is_ok());
}

#[test]
fn escalation_publication_boundaries_preserve_allocation_errors() {
    let mut succeeded = false;
    for allowed in 0..=4 {
        let result = fail_allocations_after(allowed, || {
            let mut context = wide_context();
            let one = integer(1, &mut context);
            let minimum = ExactBinary64::from_bits(1)
                .expect("minimum subnormal")
                .exact(&mut context)
                .expect("minimum subnormal ingress");
            one.sub(&minimum, &mut context)
        });
        match result {
            Ok(value) => {
                assert_eq!(value.sign(), Sign::Positive);
                succeeded = true;
                break;
            },
            Err(error) => assert!(matches!(
                error.kind(),
                ErrorKind::Resource(ResourceError::Allocation)
            )),
        }
    }
    assert!(
        succeeded,
        "all escalation allocations should eventually publish"
    );
}

#[test]
fn final_rounding_cells_cover_zero_subnormal_powers_ties_and_overflow() {
    let mut context = context();
    let zero = Enclosure::exact(Dyadic::zero(), &mut context).expect("zero cell");
    assert_eq!(
        zero.rounding_cell(&mut context).expect("zero rounding"),
        RoundingCell::Unique(0)
    );

    let minimum = Dyadic::from_parts(
        Sign::Positive,
        super::natural::Magnitude::from_u128(1),
        -1074,
        &mut context,
    )
    .expect("minimum subnormal");
    let minimum_cell = Enclosure::exact(minimum, &mut context).expect("minimum cell");
    assert_eq!(
        minimum_cell
            .rounding_cell(&mut context)
            .expect("minimum rounding"),
        RoundingCell::Unique(1)
    );
    let negative_minimum = Dyadic::from_parts(
        Sign::Negative,
        super::natural::Magnitude::from_u128(1),
        -1074,
        &mut context,
    )
    .expect("negative minimum subnormal");
    assert_eq!(
        negative_minimum
            .round_binary64(&mut context)
            .expect("negative minimum rounding"),
        RoundedBinary64::Finite((1_u64 << 63) | 1)
    );

    let half_minimum = Dyadic::from_parts(
        Sign::Positive,
        super::natural::Magnitude::from_u128(1),
        -1075,
        &mut context,
    )
    .expect("minimum midpoint");
    assert_eq!(
        half_minimum
            .round_binary64(&mut context)
            .expect("minimum midpoint rounding"),
        RoundedBinary64::Finite(0)
    );
    let negative_half_minimum = Dyadic::from_parts(
        Sign::Negative,
        super::natural::Magnitude::from_u128(1),
        -1075,
        &mut context,
    )
    .expect("negative minimum midpoint");
    assert_eq!(
        negative_half_minimum
            .round_binary64(&mut context)
            .expect("negative midpoint rounding"),
        RoundedBinary64::Finite(1_u64 << 63)
    );
    let three_half_minimum = Dyadic::from_parts(
        Sign::Positive,
        super::natural::Magnitude::from_u128(3),
        -1075,
        &mut context,
    )
    .expect("second subnormal midpoint");
    assert_eq!(
        three_half_minimum
            .round_binary64(&mut context)
            .expect("second subnormal rounding"),
        RoundedBinary64::Finite(2)
    );

    let one = integer(1, &mut context);
    assert_eq!(
        Enclosure::exact(one, &mut context)
            .expect("one cell")
            .rounding_cell(&mut context)
            .expect("one rounding"),
        RoundingCell::Unique(0x3FF0_0000_0000_0000)
    );

    let lower_midpoint = Dyadic::from_parts(
        Sign::Positive,
        super::natural::Magnitude::from_u128((1_u128 << 53) + 1),
        -53,
        &mut context,
    )
    .expect("one-up midpoint");
    assert_eq!(
        lower_midpoint
            .round_binary64(&mut context)
            .expect("lower tie rounding"),
        RoundedBinary64::Finite(0x3FF0_0000_0000_0000)
    );
    let upper_midpoint = Dyadic::from_parts(
        Sign::Positive,
        super::natural::Magnitude::from_u128((1_u128 << 53) + 3),
        -53,
        &mut context,
    )
    .expect("up-up midpoint");
    assert_eq!(
        upper_midpoint
            .round_binary64(&mut context)
            .expect("upper tie rounding"),
        RoundedBinary64::Finite(0x3FF0_0000_0000_0002)
    );

    let maximum = ExactBinary64::from_bits(0x7FEF_FFFF_FFFF_FFFF)
        .expect("maximum finite")
        .exact(&mut context)
        .expect("maximum ingress");
    assert_eq!(
        Enclosure::exact(maximum, &mut context)
            .expect("maximum cell")
            .rounding_cell(&mut context)
            .expect("maximum rounding"),
        RoundingCell::Unique(0x7FEF_FFFF_FFFF_FFFF)
    );
    let overflow_midpoint = Dyadic::from_parts(
        Sign::Positive,
        super::natural::Magnitude::from_u128((1_u128 << 54) - 1),
        970,
        &mut context,
    )
    .expect("finite overflow midpoint");
    assert_eq!(
        overflow_midpoint
            .round_binary64(&mut context)
            .expect("overflow midpoint rounding"),
        RoundedBinary64::Overflow(Sign::Positive)
    );

    let maximum_for_cells = ExactBinary64::from_bits(0x7FEF_FFFF_FFFF_FFFF)
        .expect("maximum finite cell source")
        .exact(&mut context)
        .expect("maximum finite cell ingress");
    let twice_maximum = maximum_for_cells
        .scale_pow2(1, &mut context)
        .expect("twice maximum");
    let finite_to_overflow = Enclosure::new(
        maximum_for_cells
            .try_clone(&mut context)
            .expect("maximum lower endpoint"),
        twice_maximum
            .try_clone(&mut context)
            .expect("overflow upper endpoint"),
    )
    .expect("finite-to-overflow interval");
    assert_eq!(
        finite_to_overflow
            .rounding_cell(&mut context)
            .expect("finite-to-overflow classification"),
        RoundingCell::Indeterminate
    );
    let opposite_sign_overflow = Enclosure::new(
        twice_maximum
            .try_clone(&mut context)
            .expect("opposite lower magnitude")
            .negate(),
        twice_maximum
            .try_clone(&mut context)
            .expect("opposite upper magnitude"),
    )
    .expect("opposite-sign overflow interval");
    assert_eq!(
        opposite_sign_overflow
            .rounding_cell(&mut context)
            .expect("opposite-sign classification"),
        RoundingCell::Indeterminate
    );
    let common_sign_overflow = Enclosure::new(
        twice_maximum
            .try_clone(&mut context)
            .expect("common lower magnitude"),
        maximum_for_cells
            .scale_pow2(2, &mut context)
            .expect("four times maximum"),
    )
    .expect("common-sign overflow interval");
    assert_eq!(
        common_sign_overflow
            .rounding_cell(&mut context)
            .expect("common-sign classification"),
        RoundingCell::Overflow
    );

    let next = ExactBinary64::from_bits(0x3FF0_0000_0000_0001)
        .expect("next after one")
        .exact(&mut context)
        .expect("next ingress");
    let crossing = Enclosure::new(
        ExactBinary64::from_f64(1.0)
            .expect("one source")
            .exact(&mut context)
            .expect("one ingress"),
        next,
    )
    .expect("ordered crossing");
    assert_eq!(
        crossing
            .rounding_cell(&mut context)
            .expect("crossing classification"),
        RoundingCell::Indeterminate
    );
}

#[test]
fn refinement_replays_original_inputs_instead_of_promoting_a_rounded_value() {
    let policy = PrecisionPolicy::new(64, 2_048, 100_000, 16 * 1024 * 1024);
    let mut context = PrecisionContext::new(policy).expect("adaptive policy");
    let one = integer(1, &mut context);
    let minimum = ExactBinary64::from_bits(1)
        .expect("minimum subnormal")
        .exact(&mut context)
        .expect("minimum ingress");
    assert_numerical(
        one.sub(&minimum, &mut context)
            .expect_err("first precision attempt"),
    );
    context.refine().expect("refine to 128");
    assert_numerical(
        one.sub(&minimum, &mut context)
            .expect_err("second precision attempt"),
    );
    context.refine().expect("refine to 256");
    assert_numerical(
        one.sub(&minimum, &mut context)
            .expect_err("third precision attempt"),
    );
    context.refine().expect("refine to 512");
    assert_numerical(
        one.sub(&minimum, &mut context)
            .expect_err("fourth precision attempt"),
    );
    context.refine().expect("refine to 1024");
    assert_numerical(
        one.sub(&minimum, &mut context)
            .expect_err("fifth precision attempt"),
    );
    context.refine().expect("refine to 2048");
    let replayed = one
        .sub(&minimum, &mut context)
        .expect("replayed exact inputs");
    assert_eq!(replayed.exponent(), -1074);
    assert_eq!(replayed.significand_bits(), 1074);
}

fn taylor_context_at(bits: u32) -> PrecisionContext {
    PrecisionContext::new(PrecisionPolicy::new(
        bits,
        bits,
        50_000_000,
        256 * 1024 * 1024,
    ))
    .expect("Taylor test policy")
}

fn taylor_source(
    surface: TaylorSurface,
    flattening: f64,
    amplitude: f64,
    phase: f64,
    lower: f64,
    upper: f64,
) -> TaylorSource {
    TaylorSource::from_f64(
        surface,
        flattening,
        amplitude,
        phase,
        TaylorPhaseInterval::from_f64(lower, upper).expect("Taylor phase"),
    )
    .expect("Taylor source")
}

fn enclosure_width(value: &Enclosure, context: &mut PrecisionContext) -> Dyadic {
    value
        .hi()
        .sub_scratch(value.lo(), context)
        .expect("Taylor enclosure width")
}

#[test]
fn graded_taylor_oblate_and_prolate_panels_cover_independent_bounds() {
    let oblate = taylor_source(
        TaylorSurface::Oblate,
        0.999_999,
        0.999_999,
        -0.4,
        0.75,
        0.75,
    );
    let options = TaylorOptions::new(10.0, 32, 2_048, 58, true).expect("Taylor options");
    let mut oblate_context = taylor_context_at(256);
    assert_numerical(
        evaluate_taylor(&oblate, options, &mut oblate_context)
            .expect_err("availability budget for extreme oblate"),
    );

    for (surface, flattening, amplitude) in [(TaylorSurface::Prolate, -1.0e3, 0.8)] {
        let source = taylor_source(surface, flattening, amplitude, -0.4, 0.75, 0.75);
        let mut context = taylor_context_at(256);
        let result = evaluate_taylor(&source, options, &mut context).expect("extreme Taylor case");
        assert!(result.panels() > 0);
        assert!(result.depth() <= 58);
        assert!(result.q().lo() <= result.q().hi());
        assert!(result.one_plus_reciprocal().lo() <= result.one_plus_reciprocal().hi());
        let terminal = exact(0.75, &mut context);
        let q_lower = if matches!(surface, TaylorSurface::Oblate) {
            exact(1.0 - flattening, &mut context)
        } else {
            exact(1.0, &mut context)
        };
        let lower_integral = q_lower
            .mul_scratch(&terminal, &mut context)
            .expect("independent lower product");
        let q_upper = if matches!(surface, TaylorSurface::Oblate) {
            terminal
        } else {
            exact(1.0 - flattening, &mut context)
                .mul_scratch(&terminal, &mut context)
                .expect("independent upper product")
        };
        assert!(result.q().lo() <= &q_upper);
        assert!(result.q().hi() >= &lower_integral);
    }
}

#[test]
fn graded_taylor_sphere_matches_exact_constant_integrals() {
    let source = taylor_source(TaylorSurface::Oblate, 0.0, 0.0, -1.2, 0.75, 0.75);
    let options = TaylorOptions::new(1.0e-2, 32, 2_048, 58, true).expect("sphere Taylor options");
    let mut context = taylor_context_at(128);
    let result = evaluate_taylor(&source, options, &mut context).expect("sphere Taylor case");
    let length = exact(0.75, &mut context);
    let half_length = length
        .scale_pow2(-1, &mut context)
        .expect("sphere reciprocal oracle");
    assert!(result.q().lo() <= &length);
    assert!(result.q().hi() >= &length);
    assert!(result.one_plus_reciprocal().lo() <= &half_length);
    assert!(result.one_plus_reciprocal().hi() >= &half_length);
    let zero = Dyadic::zero();
    let weighted = result
        .weighted_reciprocal()
        .expect("sphere weighted integral");
    assert!(weighted.lo() <= &zero);
    assert!(weighted.hi() >= &zero);
}

#[test]
fn graded_taylor_handles_subnormal_flattening_and_pole_equator_limits() {
    let cases = [
        (TaylorSurface::Oblate, f64::from_bits(1), 0.5, -1.4),
        (
            TaylorSurface::Oblate,
            0.2,
            f64::from_bits(0x3FEF_FFFF_FFFF_FFFF),
            0.0,
        ),
        (TaylorSurface::Prolate, -0.2, f64::from_bits(1), -1.5),
        (TaylorSurface::Prolate, -0.2, 1.0, 1.5),
    ];
    for (surface, flattening, amplitude, phase) in cases {
        let source = taylor_source(surface, flattening, amplitude, phase, 0.2, 0.2);
        let options = TaylorOptions::new(1.0e-6, 32, 2_048, 58, true).expect("Taylor options");
        let mut context = taylor_context_at(256);
        let result =
            evaluate_taylor(&source, options, &mut context).expect("pole/equator Taylor case");
        assert!(result.q().lo() <= result.q().hi());
        assert!(result.weighted_reciprocal().is_some());
    }
}

#[test]
fn graded_taylor_tail_is_present_in_the_published_enclosure() {
    let source = taylor_source(TaylorSurface::Oblate, 0.2, 0.9, -0.3, 0.7, 0.7);
    let options = TaylorOptions::new(1.0e-3, 10, 2_048, 58, true).expect("Taylor options");
    let mut context = taylor_context_at(128);
    let result =
        evaluate_taylor(&source, options, &mut context).expect("tail-controlled Taylor case");
    let width = enclosure_width(result.q(), &mut context);
    assert!(result.tails().q() > &Dyadic::zero());
    assert!(width >= *result.tails().q());
    assert!(result.tails().weighted_reciprocal().is_some());
}

#[test]
fn graded_taylor_replays_sources_and_strictly_narrows_with_precision() {
    let source = taylor_source(TaylorSurface::Prolate, -0.15, 0.7, -0.6, 0.9, 0.9);
    let options = TaylorOptions::new(1.0e-4, 16, 2_048, 58, true).expect("Taylor options");
    let mut widths = Vec::new();
    for bits in [64, 128, 256] {
        let mut context = taylor_context_at(bits);
        let result =
            evaluate_taylor(&source, options, &mut context).expect("precision Taylor case");
        widths.push(enclosure_width(result.q(), &mut context));
    }
    assert!(widths[0] > widths[1]);
    assert!(widths[1] > widths[2]);
}

#[test]
fn graded_taylor_terminal_strip_is_bounded_without_boundary_subdivision() {
    let source = taylor_source(TaylorSurface::Oblate, 0.1, 0.8, -0.4, 0.0, 0.0);
    let options = TaylorOptions::new(1.0e-5, 32, 2_048, 58, false).expect("Taylor options");
    let mut context = taylor_context_at(128);
    let lower = exact(0.6, &mut context);
    let upper = exact(0.600_000_000_000_001, &mut context);
    let terminal = Enclosure::new(lower, upper).expect("terminal phase enclosure");
    let result = evaluate_taylor_enclosed(&source, &terminal, options, &mut context)
        .expect("uncertain terminal Taylor case");
    let strip = exact(0.000_000_000_000_001, &mut context);
    assert!(enclosure_width(result.q(), &mut context) >= strip);
}

#[test]
fn graded_taylor_budget_and_allocator_failures_remain_distinct() {
    let source = taylor_source(TaylorSurface::Oblate, 0.2, 0.8, -0.2, 1.0, 1.0);
    let budget_options = TaylorOptions::new(1.0e-4, 16, 1, 1, false).expect("budget options");
    let mut budget_context = taylor_context_at(128);
    let budget_error = evaluate_taylor(&source, budget_options, &mut budget_context)
        .expect_err("panel budget exhaustion");
    assert_numerical(budget_error);

    let options = TaylorOptions::new(1.0e-4, 16, 2_048, 58, false).expect("allocation options");
    let allocation_error = fail_allocations_after(0, || {
        let mut context = taylor_context_at(128);
        evaluate_taylor(&source, options, &mut context)
    })
    .expect_err("first Taylor scratch allocation should fail");
    assert!(matches!(
        allocation_error.kind(),
        ErrorKind::Resource(ResourceError::Allocation)
    ));
    let mut context = taylor_context_at(128);
    evaluate_taylor(&source, options, &mut context)
        .expect("Taylor output should publish without allocator failure");
}
