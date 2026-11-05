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

    let tail = (bytes.len() % 8)
        .checked_mul(5)
        .expect("seven trailing digits fit usize")
        / 8;
    let capacity = (bytes.len() / 8)
        .checked_mul(5)
        .and_then(|whole| whole.checked_add(tail))
        .expect("base32 output is no longer than its input");
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
        bits = bits
            .checked_add(5)
            .expect("a base32 remainder has fewer than eight bits");

        // Each digit adds five bits to a remainder below eight.
        if bits >= 8 {
            bits = bits.checked_sub(8).expect("a complete byte is available");
            out.push((acc >> bits) as u8);
            acc &= (1_u32 << bits)
                .checked_sub(1)
                .expect("a bit mask starts at one");
        }
    }

    Ok(out)
}
