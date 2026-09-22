use std::hint::unlikely;

use crate::errors::TotpError;

const INVALID: u8 = 0xFF;
const IGNORE: u8 = 0xFE;
const DECODE_LUT: [u8; 256] = {
    let mut lut = [INVALID; 256];
    let mut index = 0;
    while let Some(slot) = lut.get_mut(index) {
        *slot = match index as u8 {
            byte @ b'A'..=b'Z' => byte - b'A',
            byte @ b'a'..=b'z' => byte - b'a',
            byte @ b'2'..=b'7' => byte - b'2' + 26,
            b'\t'..=b'\r' | b' ' | b'=' => IGNORE,
            _ => INVALID,
        };
        index += 1;
    }

    lut
};

pub(crate) fn decode_base32_secret(encoded: &str) -> Result<Vec<u8>, TotpError> {
    let bytes = encoded.as_bytes();

    let capacity = (bytes.len() / 8)
        .wrapping_mul(5)
        .wrapping_add((bytes.len() % 8).wrapping_mul(5) / 8);
    let mut out = Vec::with_capacity(capacity);
    let mut acc = 0;
    let mut bits = 0_u8;

    for (index, &b) in bytes.iter().enumerate() {
        let v = DECODE_LUT
            .get(b as usize)
            .copied()
            .expect("every byte has a lookup entry");
        if unlikely(v == INVALID) {
            return Err(TotpError::InvalidSecretChar { index });
        }
        if v == IGNORE {
            continue;
        }

        acc = (acc << 5) | u32::from(v);
        bits = bits.wrapping_add(5);

        while bits >= 8 {
            bits = bits.wrapping_sub(8);
            out.push((acc >> bits) as u8);
            acc &= (1_u32 << bits).wrapping_sub(1);
        }
    }

    Ok(out)
}
