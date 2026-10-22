use std::simd::cmp::{SimdPartialEq as _, SimdPartialOrd as _};
use std::simd::num::SimdFloat as _;
use std::simd::{Select as _, Simd, StdFloat as _};

use crate::error::Result;
use crate::geometry::{GeometryErrorKind, REDUCE_LANES, ReduceSimd, same_topological_coordinate};

// 10^p is exact in binary64 through p=22. Larger precisions remain supported
// by the decimal fallback; this table is a filter's domain, not an API limit.
const EXACT_SCALES: [f64; 23] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
];
const INTEGER_FILTER_LIMIT: f64 = 2_251_799_813_685_248.0;

/// Shared scalar/SIMD certificate for a finite scaled value below 2^51.
/// A rounded product/quotient's error is at most eps*abs(product)+min_subnormal.
/// Incrementing positive encodings rounds that enclosure and its sum outward.
/// A margin below 1/2 proves the exact scaled value rounds to this exact integer.
/// Multiplying/dividing that integer by the exact scale rounds the result once.
macro_rules! decimal_integer_certificate {
    ($product:expr, $epsilon:expr, $tiny:expr, $one:expr, $float:ty) => {{
        let product = $product;
        let integer = product.round_ties_even();
        let error = <$float>::from_bits((product.abs() * $epsilon + $tiny).to_bits() + $one);
        let margin = <$float>::from_bits(((product - integer).abs() + error).to_bits() + $one);
        (integer, margin)
    }};
}

fn exact_scale(precision: i32) -> Option<f64> {
    usize::try_from(precision.unsigned_abs())
        .ok()
        .and_then(|index| EXACT_SCALES.get(index))
        .copied()
}

/// Round the represented binary64 value to a decimal quantum, ties to even,
/// then round that decimal result once to binary64.
pub(crate) fn quantize_decimal(value: f64, precision: i32) -> Result<f64> {
    if precision >= 324 || (precision >= 0 && same_topological_coordinate(value, value.trunc())) {
        return Ok(value);
    }
    if precision <= -309 {
        return Ok(0.0_f64.copysign(value));
    }
    if let Some(scale) = exact_scale(precision) {
        let product = if precision >= 0 {
            value * scale
        } else {
            value / scale
        };
        if product.abs() < INTEGER_FILTER_LIMIT {
            let (integer, margin) =
                decimal_integer_certificate!(product, f64::EPSILON, f64::from_bits(1), 1, f64);
            if margin < 0.5 {
                return Ok(if precision >= 0 {
                    integer / scale
                } else {
                    integer * scale
                });
            }
        }
    }
    quantize_decimal_fallback(value, precision)
}

fn quantize_decimal_fallback(value: f64, precision: i32) -> Result<f64> {
    let rounded = if precision >= 0 {
        format!("{value:.places$}", places = precision as usize)
    } else {
        let scientific = format!("{value:e}");
        let (_, exponent) = scientific.split_once('e').expect("scientific exponent");
        let exponent: i32 = exponent.parse().expect("binary64 decimal exponent");
        let places = exponent + precision;
        if places >= 0 {
            format!("{value:.places$e}", places = places as usize)
        } else if places < -1 {
            return Ok(0.0_f64.copysign(value));
        } else {
            // No significant digit survives. The halfway value is the integer
            // 5*10^exponent. Below 10^16 it is exact in binary64; above that,
            // every binary64 input is integral and fixed formatting is exact.
            let above_half = if exponent <= 15 {
                value.abs() > 5.0 * 10.0_f64.powi(exponent)
            } else {
                let integer = format!("{:.0}", value.abs());
                let half = format!("5{}", "0".repeat(exponent as usize));
                integer > half
            };
            if !above_half {
                return Ok(0.0_f64.copysign(value));
            }
            format!(
                "{}1e{}",
                if value.is_sign_negative() { "-" } else { "" },
                -precision
            )
        }
    };
    let result: f64 = rounded.parse().expect("formatted decimal number");
    if !result.is_finite() {
        return Err(GeometryErrorKind::Invalid(
            "quantize result is outside the finite coordinate range".into(),
        )
        .into());
    }
    Ok(result)
}

pub(crate) fn quantize_column(column: &[f64], precision: i32) -> Result<Vec<f64>> {
    let Some(scale) = exact_scale(precision) else {
        return column
            .iter()
            .map(|&value| quantize_decimal(value, precision))
            .collect();
    };
    let mut output = Vec::with_capacity(column.len());
    let scale = ReduceSimd::splat(scale);
    let epsilon = ReduceSimd::splat(f64::EPSILON);
    let tiny = ReduceSimd::splat(f64::from_bits(1));
    let one = Simd::<u64, REDUCE_LANES>::splat(1);
    let half = ReduceSimd::splat(0.5);
    let mut chunks = column.chunks_exact(REDUCE_LANES);
    for chunk in &mut chunks {
        let values = ReduceSimd::from_slice(chunk);
        let product = if precision >= 0 {
            values * scale
        } else {
            values / scale
        };
        let admitted = product
            .abs()
            .simd_lt(ReduceSimd::splat(INTEGER_FILTER_LIMIT));
        let product = admitted.select(product, ReduceSimd::splat(0.0));
        let (integer, margin) =
            decimal_integer_certificate!(product, epsilon, tiny, one, ReduceSimd);
        let integral = values.simd_eq(values.trunc()) & std::simd::Mask::splat(precision >= 0);
        let certified = integral | (admitted & margin.simd_lt(half));
        let rounded = integral.select(
            values,
            if precision >= 0 {
                integer / scale
            } else {
                integer * scale
            },
        );
        if certified.all() {
            output.extend(rounded.to_array());
        } else {
            for ((&value, rounded), certified) in chunk
                .iter()
                .zip(rounded.to_array())
                .zip(certified.to_array())
            {
                output.push(if certified {
                    rounded
                } else {
                    quantize_decimal_fallback(value, precision)?
                });
            }
        }
    }
    for &value in chunks.remainder() {
        output.push(quantize_decimal(value, precision)?);
    }
    Ok(output)
}
