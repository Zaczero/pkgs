// Encoded polylines use printable ASCII by adding '?' (63) to each chunk.
pub(crate) const ASCII_OFFSET: u8 = b'?';

// A chunk contains 5 bits of payload and may set a continuation bit.
pub(crate) const CHUNK_BITS: u32 = 5;
pub(crate) const CHUNK_MASK: u8 = 0b1_1111;

pub(crate) const CONTINUATION_BIT: u8 = 0x20;

pub(crate) struct PrecisionScale {
    pub(crate) scale: f64,
    pub(crate) inverse_scale: f64,
}

const SCALE_TABLE_LEN: usize = 16;
const SCALES: [f64; SCALE_TABLE_LEN] = [
    1.0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15,
];
const INV_SCALES: [f64; SCALE_TABLE_LEN] = [
    1.0, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9, 1e-10, 1e-11, 1e-12, 1e-13, 1e-14,
    1e-15,
];

fn cached_precision_value(table: &[f64; SCALE_TABLE_LEN], precision: i32) -> Option<f64> {
    let index = usize::try_from(precision).ok()?;
    table.get(index).copied()
}

fn scale_for_precision(precision: i32) -> f64 {
    cached_precision_value(&SCALES, precision).unwrap_or_else(|| 10_f64.powi(precision))
}

fn inverse_scale_for_precision(precision: i32) -> f64 {
    cached_precision_value(&INV_SCALES, precision)
        .unwrap_or_else(|| 10_f64.powi(precision.saturating_neg()))
}

pub(crate) fn precision_scale(precision: i32) -> Option<PrecisionScale> {
    let scale = scale_for_precision(precision);
    let inverse_scale = inverse_scale_for_precision(precision);
    // i32::MIN has the greatest absolute wire coordinate, so this also proves
    // every other accumulated i32 coordinate decodes to a finite f64.
    let largest_decoded_coordinate = f64::from(i32::MIN) * inverse_scale;

    (scale.is_finite()
        && scale > 0.0
        && inverse_scale.is_finite()
        && inverse_scale > 0.0
        && largest_decoded_coordinate.is_finite())
    .then_some(PrecisionScale {
        scale,
        inverse_scale,
    })
}
