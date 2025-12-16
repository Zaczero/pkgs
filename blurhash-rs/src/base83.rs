const CHARSET: &[u8; 83] =
    b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz#$%*+,-.:;=?@[]^_{|}~";

const INVALID: u8 = 0xFF;

const fn build_decode_table() -> [u8; 256] {
    let mut table = [INVALID; 256];
    let mut i = 0usize;
    while i < 83 {
        table[CHARSET[i] as usize] = i as u8;
        i += 1;
    }
    table
}

const DECODE: [u8; 256] = build_decode_table();

const fn build_pow83_table() -> [u32; 5] {
    let mut out = [0u32; 5];
    out[0] = 1;
    let mut i = 1usize;
    while i < out.len() {
        out[i] = out[i - 1] * 83;
        i += 1;
    }
    out
}

const POW83: [u32; 5] = build_pow83_table();

pub fn decode_byte(b: u8) -> Option<u8> {
    let v = DECODE[b as usize];
    if v == INVALID { None } else { Some(v) }
}

pub fn decode_u32(bytes: &[u8]) -> Option<u32> {
    let mut value = 0u32;
    for &b in bytes {
        let digit = decode_byte(b)? as u32;
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
