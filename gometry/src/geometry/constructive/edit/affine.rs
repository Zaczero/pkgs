use super::{
    DekkerParts, ExactSum, GeometryErrorKind, ReduceSimd, Result, axis_pow2_scale, binary_zero,
    checked_normal_sum, floor_binary_exponent, normal_or_zero, same_topological_coordinate,
    two_product_dekker_admitted, two_product_dekker_checked_parts, two_sum,
};

/// Expand each checked product into the retained double-double and its error
/// bound for affine setup and coordinate evaluation.
macro_rules! add_affine_expansion_product {
    ($high:ident, $low:ident, $error:ident, $product:ident, $product_error:ident) => {{
        let (sum, sum_error) = checked_two_sum($high, $product)?;
        let (low_sum, low_error) = checked_two_sum($low, $product_error)?;
        let (merged_error, discarded_high) = checked_two_sum(sum_error, low_sum)?;
        let (next_high, next_residual) = checked_two_sum(sum, merged_error)?;
        $error = upper_sum($error, discarded_high.abs())?;
        $error = upper_sum($error, low_error.abs())?;
        $high = next_high;
        $low = next_residual;
    }};
}

/// Use prepared matrix and checked pivot splits when setting up an affine row.
macro_rules! add_affine_product_parts {
    ($high:ident, $low:ident, $error:ident, $lhs:expr, $rhs:expr) => {{
        let (product, product_error) = two_product_dekker_checked_parts($lhs, $rhs)?;
        add_affine_expansion_product!($high, $low, $error, product, product_error);
    }};
}

mod columns;
mod rounding;
mod row;

pub(crate) use columns::OriginAffine;
pub(crate) use rounding::{affine_x_row_identity, affine_y_row_identity};
pub(crate) use row::AffineLinear;
pub(super) use row::affine_ordinate_simd;

/// Affine map with power-of-two pre-scale of the input coordinates so
/// cancelling huge products (`2*1e308 + (-2)*1e308`) stay finite. Ordinary
/// mid-range inputs take the classic multiply-add (bit-identical).
#[expect(
    clippy::many_single_char_names,
    reason = "a,b,d,e,x,y are the canonical affine matrix and point coordinates"
)]
pub(crate) fn stable_affine_xy(
    a: f64,
    b: f64,
    d: f64,
    e: f64,
    xoff: f64,
    yoff: f64,
    x: f64,
    y: f64,
) -> (f64, f64) {
    let classic_x = a * x + b * y + xoff;
    let classic_y = d * x + e * y + yoff;
    if classic_x.is_finite() && classic_y.is_finite() {
        return (classic_x, classic_y);
    }
    let max_abs = x.abs().max(y.abs());
    if max_abs == 0.0 || !max_abs.is_finite() {
        return (classic_x, classic_y);
    }
    let scale = axis_pow2_scale(max_abs);
    let (xs, ys) = (x * scale, y * scale);
    let rx = (a * xs + b * ys) / scale + xoff;
    let ry = (d * xs + e * ys) / scale + yoff;
    (rx, ry)
}

#[cfg(test)]
mod tests;
