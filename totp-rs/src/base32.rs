use std::hint::unlikely;

use crate::errors::Error;

const INVALID: u8 = 0xFF;
const IGNORE: u8 = 0xFE;

const fn build_decode_lut() -> [u8; 256] {
    let mut lut = [INVALID; 256];

    // A-Z / a-z
    let mut i = 0;
    while i < 26 {
        lut[(b'A' + i) as usize] = i;
        lut[(b'a' + i) as usize] = i;
        i += 1;
    }

    // 2-7
    i = 0;
    while i < 6 {
        lut[(b'2' + i) as usize] = 26 + i;
        i += 1;
    }

    // Whitespace and padding
    i = b'\t';
    while i <= b'\r' {
        lut[i as usize] = IGNORE;
        i += 1;
    }
    lut[b' ' as usize] = IGNORE;
    lut[b'=' as usize] = IGNORE;

    lut
}

const DECODE_LUT: [u8; 256] = build_decode_lut();

pub(crate) fn decode_base32_secret(encoded: &str) -> Result<Vec<u8>, Error> {
    let bytes = encoded.as_bytes();

    let mut out = Vec::with_capacity(bytes.len() * 5 / 8);
    let mut acc = 0;
    let mut bits = 0u8;

    for (index, &b) in bytes.iter().enumerate() {
        let v = DECODE_LUT[b as usize];
        if unlikely(v == INVALID) {
            return Err(Error::InvalidSecretChar { index });
        }
        if v == IGNORE {
            continue;
        }

        acc = (acc << 5) | (v as u32);
        bits += 5;

        while bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }

    Ok(out)
}
