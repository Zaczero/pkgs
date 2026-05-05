use std::hint::unlikely;

const CHARSET: &[u8; 83] =
    b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz#$%*+,-.:;=?@[]^_{|}~";

const INVALID: u8 = 0xFF;
const DECODE: [u8; 256] = {
    let mut table = [INVALID; 256];
    let mut i = 0;
    while i < 83 {
        table[CHARSET[i] as usize] = i as u8;
        i += 1;
    }
    table
};
const POW83: [u32; 5] = {
    let mut out = [0_u32; 5];
    out[0] = 1;
    let mut i = 1;
    while i < out.len() {
        out[i] = out[i - 1] * 83;
        i += 1;
    }
    out
};

pub const fn decode_byte(b: u8) -> Option<u8> {
    let v = DECODE[b as usize];
    if unlikely(v == INVALID) {
        None
    } else {
        Some(v)
    }
}

pub fn decode_u32(bytes: &[u8]) -> Option<u32> {
    let mut value = 0;
    for &b in bytes {
        let digit = u32::from(decode_byte(b)?);
        value = value * 83 + digit;
    }
    Some(value)
}

pub fn push_base83(out: &mut Vec<u8>, value: u32, length: usize) {
    let mut i = length;
    while i > 0 {
        i -= 1;
        let divisor = POW83[i];
        let digit = (value / divisor) % 83;
        out.push(CHARSET[digit as usize]);
    }
}
