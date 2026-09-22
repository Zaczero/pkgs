pub(crate) const INVALID_VALUE: u8 = u8::MAX;

pub(crate) const HEADER_NAME_VALID: u8 = 1;
pub(crate) const HEADER_NAME_UPPER: u8 = 2;

pub(crate) const HEX_VALUE: [u8; 256] = {
    let table = [INVALID_VALUE; 256];
    let table = fill_linear_range(table, b'0', b'9', 0);
    let table = fill_linear_range(table, b'a', b'f', 10);
    fill_linear_range(table, b'A', b'F', 10)
};
pub(crate) const BASE64_VALUE: [u8; 256] = base64_value_table(b'+', b'/');
pub(crate) const BASE64URL_VALUE: [u8; 256] = base64_value_table(b'-', b'_');
pub(crate) const HEADER_NAME_FLAGS: [u8; 256] = {
    let table = [0; 256];
    let table = fill_constant_range(table, b'0', b'9', HEADER_NAME_VALID);
    let table = fill_constant_range(table, b'a', b'z', HEADER_NAME_VALID);
    let table = fill_constant_range(table, b'A', b'Z', HEADER_NAME_VALID | HEADER_NAME_UPPER);
    fill_bytes(table, b"!#$%&'*+-.^_`|~", HEADER_NAME_VALID)
};

/// `memchr` reads its setup cost back over a long haystack and not over a short
/// one, where a plain scan is already at the byte. Header field lists and
/// request paths are usually short, so both callers pick per input.
pub(crate) fn find_byte(needle: u8, haystack: &[u8]) -> Option<usize> {
    const SCAN_LIMIT: usize = 16;

    if haystack.len() <= SCAN_LIMIT {
        haystack.iter().position(|&byte| byte == needle)
    } else {
        memchr::memchr(needle, haystack)
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "const evaluation bounds-checks these writes when the table is built"
)]
const fn fill_linear_range(
    mut table: [u8; 256],
    first_byte: u8,
    last_byte: u8,
    first_value: u8,
) -> [u8; 256] {
    let mut byte = first_byte as usize;
    while byte <= last_byte as usize {
        table[byte] = first_value
            .checked_add((byte as u8).wrapping_sub(first_byte))
            .expect("linear lookup values fit one byte");
        byte = byte.wrapping_add(1);
    }
    table
}

#[expect(
    clippy::indexing_slicing,
    reason = "const evaluation bounds-checks these writes when the table is built"
)]
const fn fill_constant_range(
    mut table: [u8; 256],
    first_byte: u8,
    last_byte: u8,
    value: u8,
) -> [u8; 256] {
    let mut byte = first_byte as usize;
    while byte <= last_byte as usize {
        table[byte] = value;
        byte = byte.wrapping_add(1);
    }
    table
}

#[expect(
    clippy::indexing_slicing,
    reason = "const evaluation bounds-checks these writes when the table is built"
)]
const fn fill_bytes<const N: usize>(mut table: [u8; 256], bytes: &[u8; N], value: u8) -> [u8; 256] {
    let mut index = 0_usize;
    while index < bytes.len() {
        table[bytes[index] as usize] = value;
        index = index.wrapping_add(1);
    }
    table
}

#[expect(
    clippy::indexing_slicing,
    reason = "const evaluation bounds-checks these writes when the table is built"
)]
const fn base64_value_table(char_62: u8, char_63: u8) -> [u8; 256] {
    let table = [INVALID_VALUE; 256];
    let table = fill_linear_range(table, b'A', b'Z', 0);
    let table = fill_linear_range(table, b'a', b'z', 26);
    let mut table = fill_linear_range(table, b'0', b'9', 52);
    table[char_62 as usize] = 62;
    table[char_63 as usize] = 63;
    table
}
