use super::columns::upper_sum_admitted;
use super::rounding::{
    affine_origin_bias, checked_two_sum, exact_affine_origin_ordinate,
    round_certified_double_double, upper_sum,
};
use super::{
    DekkerParts, GeometryErrorKind, OriginAffine, ReduceSimd, Result, binary_zero,
    floor_binary_exponent, normal_or_zero, same_topological_coordinate,
    two_product_dekker_admitted, two_product_dekker_checked_parts, two_sum,
};

/// Inline the admitted product and residual merge into the coordinate loop.
macro_rules! add_affine_product_admitted {
    ($high:ident, $low:ident, $error:ident, $lhs:expr, $rhs:expr) => {{
        let (product, product_error) = two_product_dekker_admitted($lhs, $rhs);
        let (sum, sum_error) = two_sum($high, product);
        let (low_sum, low_error) = two_sum($low, product_error);
        let (merged_error, discarded_high) = two_sum(sum_error, low_sum);
        let (next_high, next_residual) = two_sum(sum, merged_error);
        $error = upper_sum_admitted($error, discarded_high.abs());
        $error = upper_sum_admitted($error, low_error.abs());
        $high = next_high;
        $low = next_residual;
    }};
}

#[derive(Clone, Copy)]
pub(super) struct AffineExpansion {
    pub(super) high: f64,
    pub(super) low: f64,
    pub(super) error: f64,
}

#[derive(Clone, Copy)]
pub(super) struct ExponentExtent {
    min: Option<i32>,
    max: Option<i32>,
}

impl ExponentExtent {
    fn split_safe(self) -> bool {
        self.min.is_none_or(|exponent| exponent >= -970)
            && self.max.is_none_or(|exponent| exponent <= 995)
    }
}

#[derive(Clone, Copy)]
struct MagnitudeExtent {
    min_bits: u64,
    max_bits: u64,
}

impl MagnitudeExtent {
    const EMPTY: Self = Self {
        min_bits: 0x7FF0_0000_0000_0000,
        max_bits: 0,
    };

    fn include(&mut self, value: f64) -> bool {
        let magnitude_bits = value.to_bits() & 0x7FFF_FFFF_FFFF_FFFF;
        if magnitude_bits >= 0x7FF0_0000_0000_0000 {
            return false;
        }
        if magnitude_bits != 0 {
            self.min_bits = self.min_bits.min(magnitude_bits);
            self.max_bits = self.max_bits.max(magnitude_bits);
        }
        true
    }

    fn exponents(self) -> ExponentExtent {
        if self.min_bits == 0x7FF0_0000_0000_0000 {
            return ExponentExtent {
                min: None,
                max: None,
            };
        }
        ExponentExtent {
            min: Some(floor_binary_exponent(f64::from_bits(self.min_bits))),
            max: Some(floor_binary_exponent(f64::from_bits(self.max_bits))),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct AffineRowExtents {
    pub(super) x: ExponentExtent,
    pub(super) y: ExponentExtent,
}

impl AffineRowExtents {
    pub(super) fn from_columns(xs: &[f64], ys: &[f64]) -> Option<Self> {
        debug_assert_eq!(xs.len(), ys.len());
        let mut x = MagnitudeExtent::EMPTY;
        let mut y = MagnitudeExtent::EMPTY;
        for (&x_value, &y_value) in xs.iter().zip(ys) {
            if !x.include(x_value) || !y.include(y_value) {
                return None;
            }
        }
        Some(Self {
            x: x.exponents(),
            y: y.exponents(),
        })
    }
}

#[derive(Clone, Copy)]
pub(super) struct AffineOriginRow {
    pub(super) coefficient_x: f64,
    pub(super) coefficient_y: f64,
    pub(super) coefficient_x_parts: Option<DekkerParts>,
    pub(super) coefficient_y_parts: Option<DekkerParts>,
    pub(super) output_origin: f64,
    pub(super) origin_x: f64,
    pub(super) origin_y: f64,
    pub(super) bias: Option<AffineExpansion>,
}

impl AffineOriginRow {
    pub(super) fn new(
        coefficient_x: f64,
        coefficient_y: f64,
        output_origin: f64,
        origin_x: f64,
        origin_y: f64,
        coefficient_x_parts: Option<DekkerParts>,
        coefficient_y_parts: Option<DekkerParts>,
        origin_x_parts: Option<DekkerParts>,
        origin_y_parts: Option<DekkerParts>,
        output_origin_parts: Option<DekkerParts>,
    ) -> Self {
        Self {
            coefficient_x,
            coefficient_y,
            coefficient_x_parts,
            coefficient_y_parts,
            output_origin,
            origin_x,
            origin_y,
            bias: affine_origin_bias(
                output_origin_parts,
                coefficient_x_parts,
                coefficient_y_parts,
                origin_x_parts,
                origin_y_parts,
            ),
        }
    }

    pub(super) fn fast_map_finite(&self, value_x: f64, value_y: f64) -> Option<f64> {
        let AffineExpansion {
            mut high,
            mut low,
            mut error,
        } = self.bias?;
        let value_x_parts = DekkerParts::checked(value_x)?;
        let value_y_parts = DekkerParts::checked(value_y)?;
        add_affine_product_parts!(high, low, error, self.coefficient_x_parts?, value_x_parts);
        add_affine_product_parts!(high, low, error, self.coefficient_y_parts?, value_y_parts);
        let (candidate, certified) = round_certified_double_double::<false>(high, low, error);
        certified.then_some(candidate)
    }

    pub(super) fn map_finite(&self, value_x: f64, value_y: f64) -> Result<f64> {
        if let Some(value) = self.fast_map_finite(value_x, value_y) {
            return Ok(value);
        }
        self.exact_map_finite(value_x, value_y)
    }

    pub(super) fn exact_map_finite(&self, value_x: f64, value_y: f64) -> Result<f64> {
        exact_affine_origin_ordinate(
            self.coefficient_x,
            self.coefficient_y,
            self.output_origin,
            self.origin_x,
            self.origin_y,
            value_x,
            value_y,
        )
    }

    pub(super) fn admitted(
        &self,
        x_extent: ExponentExtent,
        y_extent: ExponentExtent,
    ) -> Option<AdmittedAffineOriginRow> {
        let coefficient_x_parts = self.coefficient_x_parts?;
        let coefficient_y_parts = self.coefficient_y_parts?;
        let bias = self.bias?;
        if !x_extent.split_safe() || !y_extent.split_safe() {
            return None;
        }
        if !affine_product_exponents_admitted(coefficient_x_parts, x_extent)
            || !affine_product_exponents_admitted(coefficient_y_parts, y_extent)
        {
            return None;
        }
        if !affine_expansion_admitted(bias) {
            return None;
        }
        let x_is_zero = coefficient_x_parts.is_zero();
        let y_is_zero = coefficient_y_parts.is_zero();
        let bias_is_zero =
            binary_zero(bias.high) && binary_zero(bias.low) && binary_zero(bias.error);
        let mode = match (bias_is_zero, x_is_zero, y_is_zero) {
            (true, false, true) => AdmittedAffineMode::ProductX(coefficient_x_parts),
            (true, true, false) => AdmittedAffineMode::ProductY(coefficient_y_parts),
            (true, false, false) => {
                AdmittedAffineMode::Dot(coefficient_x_parts, coefficient_y_parts)
            },
            (true, true, true) => AdmittedAffineMode::Zero,
            (false, true, true) => AdmittedAffineMode::BiasOnly(bias),
            (false, false, true) => AdmittedAffineMode::BiasedProductX(coefficient_x_parts, bias),
            (false, true, false) => AdmittedAffineMode::BiasedProductY(coefficient_y_parts, bias),
            (false, false, false) => {
                AdmittedAffineMode::General(coefficient_x_parts, coefficient_y_parts, bias)
            },
        };
        Some(AdmittedAffineOriginRow { mode })
    }
}

#[derive(Clone, Copy)]
pub(super) enum AdmittedAffineMode {
    Zero,
    ProductX(DekkerParts),
    ProductY(DekkerParts),
    BiasOnly(AffineExpansion),
    BiasedProductX(DekkerParts, AffineExpansion),
    BiasedProductY(DekkerParts, AffineExpansion),
    Dot(DekkerParts, DekkerParts),
    General(DekkerParts, DekkerParts, AffineExpansion),
}

#[derive(Clone, Copy)]
pub(super) struct AdmittedAffineOriginRow {
    pub(super) mode: AdmittedAffineMode,
}

impl AdmittedAffineOriginRow {
    pub(super) const fn requires_coordinate_split(&self) -> bool {
        matches!(
            &self.mode,
            AdmittedAffineMode::BiasedProductX(..)
                | AdmittedAffineMode::BiasedProductY(..)
                | AdmittedAffineMode::Dot(..)
                | AdmittedAffineMode::General(..)
        )
    }

    pub(super) fn map(
        self,
        value_x: f64,
        value_y: f64,
        parts_x: DekkerParts,
        parts_y: DekkerParts,
    ) -> (f64, bool) {
        match self.mode {
            AdmittedAffineMode::Zero => (0.0, true),
            AdmittedAffineMode::ProductX(coefficient) => {
                (single_product(coefficient.value(), value_x), true)
            },
            AdmittedAffineMode::ProductY(coefficient) => {
                (single_product(coefficient.value(), value_y), true)
            },
            AdmittedAffineMode::BiasOnly(bias) => {
                round_certified_double_double::<true>(bias.high, bias.low, bias.error)
            },
            AdmittedAffineMode::BiasedProductX(coefficient, bias) => {
                map_biased_product(bias, coefficient, parts_x)
            },
            AdmittedAffineMode::BiasedProductY(coefficient, bias) => {
                map_biased_product(bias, coefficient, parts_y)
            },
            AdmittedAffineMode::Dot(coefficient_x, coefficient_y) => {
                map_exact_dot(coefficient_x, coefficient_y, parts_x, parts_y)
            },
            AdmittedAffineMode::General(coefficient_x, coefficient_y, bias) => {
                map_general_affine_row(bias, coefficient_x, coefficient_y, parts_x, parts_y)
            },
        }
    }
}

/// A sole exact product rounds correctly with one binary64 multiply. ExactSum
/// canonicalizes a mathematically zero row to +0, unlike signed-zero multiply.
fn single_product(coefficient: f64, value: f64) -> f64 {
    let product = coefficient * value;
    if binary_zero(value) { 0.0 } else { product }
}

fn map_biased_product(
    bias: AffineExpansion,
    coefficient: DekkerParts,
    value: DekkerParts,
) -> (f64, bool) {
    let AffineExpansion {
        mut high,
        mut low,
        mut error,
    } = bias;
    add_affine_product_admitted!(high, low, error, coefficient, value);
    round_certified_double_double::<true>(high, low, error)
}

/// Compact two exact products; the residuals discarded from the low-limb
/// merge are added to the outward error bound.
fn map_exact_dot(
    coefficient_x: DekkerParts,
    coefficient_y: DekkerParts,
    value_x: DekkerParts,
    value_y: DekkerParts,
) -> (f64, bool) {
    let (product_x, error_x) = two_product_dekker_admitted(coefficient_x, value_x);
    let (product_y, error_y) = two_product_dekker_admitted(coefficient_y, value_y);
    let (head, head_error) = two_sum(product_x, product_y);
    let (product_tail, discarded_products) = two_sum(error_x, error_y);
    let (merged_tail, discarded_merge) = two_sum(head_error, product_tail);
    let (high, low) = two_sum(head, merged_tail);
    let error = upper_sum_admitted(0.0, discarded_products.abs());
    let error = upper_sum_admitted(error, discarded_merge.abs());
    round_certified_double_double::<true>(high, low, error)
}

fn map_general_affine_row(
    bias: AffineExpansion,
    coefficient_x: DekkerParts,
    coefficient_y: DekkerParts,
    value_x: DekkerParts,
    value_y: DekkerParts,
) -> (f64, bool) {
    let AffineExpansion {
        mut high,
        mut low,
        mut error,
    } = bias;
    add_affine_product_admitted!(high, low, error, coefficient_x, value_x);
    add_affine_product_admitted!(high, low, error, coefficient_y, value_y);
    round_certified_double_double::<true>(high, low, error)
}

const MIN_ADMITTED_PRODUCT_EXPONENT: i32 = -970;
const MAX_ADMITTED_PRODUCT_EXPONENT: i32 = 850;
const MAX_ADMITTED_ACCUMULATOR_EXPONENT: i32 = 860;
// A nonidentity affine output is finite or the operation errors. This NaN
// payload is therefore an in-buffer fixup tag, avoiding per-row scratch.
pub(super) const AFFINE_FIXUP_MARKER: f64 = f64::from_bits(0x7FF8_0000_0000_0001);

// A binary64 with floor exponent e is a multiple of 2^(e-52). Dekker's split
// keeps both parts on that lattice, so every partial product in a two-product
// transform is a multiple of 2^(e_lhs+e_rhs-104). Requiring a product exponent
// sum of at least -970 keeps that lattice on or above 2^-1074, the minimum
// subnormal quantum; at -971 a half-subnormal residual can round away (the
// boundary witness below). Coordinate exponents [-970, 995] also keep the
// splitter product finite and every nonzero split limb normal. At the upper
// product sum of 850, each product stays below 2^852; the admitted bias limbs
// stay below 2^861, so their fixed-size accumulation remains far from overflow.
fn affine_product_exponents_admitted(coefficient: DekkerParts, coordinate: ExponentExtent) -> bool {
    if coefficient.is_zero() {
        return true;
    }
    let (Some(min_coordinate), Some(max_coordinate)) = (coordinate.min, coordinate.max) else {
        return true;
    };
    let Some(min_product_exponent) = coefficient.exponent().checked_add(min_coordinate) else {
        return false;
    };
    let Some(max_product_exponent) = coefficient.exponent().checked_add(max_coordinate) else {
        return false;
    };
    (MIN_ADMITTED_PRODUCT_EXPONENT..=MAX_ADMITTED_PRODUCT_EXPONENT).contains(&min_product_exponent)
        && max_product_exponent <= MAX_ADMITTED_PRODUCT_EXPONENT
}

fn affine_expansion_admitted(expansion: AffineExpansion) -> bool {
    [expansion.high, expansion.low, expansion.error]
        .into_iter()
        .all(|value| {
            normal_or_zero(value)
                && (binary_zero(value)
                    || floor_binary_exponent(value.abs()) <= MAX_ADMITTED_ACCUMULATOR_EXPONENT)
        })
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "SIMD affine lanes preserve the declared multiply-add expression order"
)]
pub(in crate::geometry::constructive::edit) fn affine_ordinate_simd(
    first: ReduceSimd,
    second: ReduceSimd,
    offset: ReduceSimd,
    x: ReduceSimd,
    y: ReduceSimd,
) -> ReduceSimd {
    first * x + second * y + offset
}

pub(crate) struct AffineLinear {
    matrix: [f64; 4],
    coefficient_parts: [Option<DekkerParts>; 4],
    preserved: [bool; 2],
}

impl AffineLinear {
    pub(crate) fn new(matrix: [f64; 4]) -> Result<Self> {
        if !matrix.iter().all(|value| value.is_finite()) {
            return Err(GeometryErrorKind::NonFiniteCoordinate.into());
        }
        let [m00, m01, m10, m11] = matrix;
        Ok(Self {
            matrix,
            coefficient_parts: matrix.map(DekkerParts::checked),
            preserved: [
                same_topological_coordinate(m00, 1.0) && same_topological_coordinate(m01, 0.0),
                same_topological_coordinate(m10, 0.0) && same_topological_coordinate(m11, 1.0),
            ],
        })
    }

    pub(crate) const fn is_identity(&self) -> bool {
        let [preserve_x, preserve_y] = self.preserved;
        preserve_x && preserve_y
    }

    pub(crate) fn at(&self, origin: (f64, f64)) -> Result<OriginAffine> {
        if !origin.0.is_finite() || !origin.1.is_finite() {
            return Err(GeometryErrorKind::NonFiniteCoordinate.into());
        }
        let origin_x_parts = DekkerParts::checked(origin.0);
        let origin_y_parts = DekkerParts::checked(origin.1);
        Ok(OriginAffine {
            preserved: self.preserved,
            rows: [
                (!self.preserved[0]).then(|| {
                    AffineOriginRow::new(
                        self.matrix[0],
                        self.matrix[1],
                        origin.0,
                        origin.0,
                        origin.1,
                        self.coefficient_parts[0],
                        self.coefficient_parts[1],
                        origin_x_parts,
                        origin_y_parts,
                        origin_x_parts,
                    )
                }),
                (!self.preserved[1]).then(|| {
                    AffineOriginRow::new(
                        self.matrix[2],
                        self.matrix[3],
                        origin.1,
                        origin.0,
                        origin.1,
                        self.coefficient_parts[2],
                        self.coefficient_parts[3],
                        origin_x_parts,
                        origin_y_parts,
                        origin_y_parts,
                    )
                }),
            ],
        })
    }
}
