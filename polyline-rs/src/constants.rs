// Encoded polylines use printable ASCII by adding '?' (63) to each chunk.
pub const ASCII_OFFSET: u8 = b'?';

// A chunk contains 5 bits of payload and may set a continuation bit.
pub const CHUNK_BITS: u32 = 5;
pub const CHUNK_MASK: u8 = 0b1_1111;

pub const CONTINUATION_BIT: u8 = 0x20;

const SCALE_TABLE_LEN: usize = 16;
const SCALES: [f64; SCALE_TABLE_LEN] = {
    let mut table = [1.0; SCALE_TABLE_LEN];
    let mut index = 1;
    while index < table.len() {
        table[index] = table[index - 1] * 10.0;
        index += 1;
    }
    table
};
const INV_SCALES: [f64; SCALE_TABLE_LEN] = {
    let mut table = [1.0; SCALE_TABLE_LEN];
    let mut index = 1;
    while index < table.len() {
        table[index] = 1.0 / SCALES[index];
        index += 1;
    }
    table
};

fn cached_precision_value(table: &[f64; SCALE_TABLE_LEN], precision: i32) -> Option<f64> {
    let index = usize::try_from(precision).ok()?;
    table.get(index).copied()
}

pub fn scale_for_precision(precision: i32) -> f64 {
    cached_precision_value(&SCALES, precision).unwrap_or_else(|| 10_f64.powi(precision))
}

pub fn inv_scale_for_precision(precision: i32) -> f64 {
    cached_precision_value(&INV_SCALES, precision).unwrap_or_else(|| 10_f64.powi(-precision))
}
