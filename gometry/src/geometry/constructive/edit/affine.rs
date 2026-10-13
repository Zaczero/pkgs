use super::*;

mod columns;
mod rounding;
mod row;

pub(crate) use columns::OriginAffine;
#[cfg(test)]
pub(crate) use rounding::affine_about_origin_ordinate;
#[cfg(test)]
pub(super) use rounding::fast_affine_about_origin_ordinate;
pub(crate) use rounding::{affine_x_row_identity, affine_y_row_identity};
pub(crate) use row::AffineLinear;
pub(super) use row::{
    AdmittedAffineMode, AdmittedAffineOriginRow, AffineExpansion, AffineOriginRow,
    AffineRowExtents, affine_ordinate_simd,
};

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
