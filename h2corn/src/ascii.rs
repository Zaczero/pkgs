pub const INVALID_VALUE: u8 = u8::MAX;

pub const HEADER_NAME_VALID: u8 = 1;
pub const HEADER_NAME_UPPER: u8 = 2;

pub const HEX_VALUE: [u8; 256] = {
    let table = [INVALID_VALUE; 256];
    let table = fill_linear_range(table, b'0', b'9', 0);
    let table = fill_linear_range(table, b'a', b'f', 10);
    fill_linear_range(table, b'A', b'F', 10)
};
pub const BASE64_VALUE: [u8; 256] = base64_value_table(b'+', b'/');
pub const BASE64URL_VALUE: [u8; 256] = base64_value_table(b'-', b'_');
pub const HEADER_NAME_FLAGS: [u8; 256] = {
    let table = [0; 256];
    let table = fill_constant_range(table, b'0', b'9', HEADER_NAME_VALID);
    let table = fill_constant_range(table, b'a', b'z', HEADER_NAME_VALID);
    let table = fill_constant_range(table, b'A', b'Z', HEADER_NAME_VALID | HEADER_NAME_UPPER);
    fill_bytes(table, b"!#$%&'*+-.^_`|~", HEADER_NAME_VALID)
};

pub const fn is_base64(byte: u8) -> bool {
    BASE64_VALUE[byte as usize] != INVALID_VALUE
}

const fn fill_linear_range(
    mut table: [u8; 256],
    first_byte: u8,
    last_byte: u8,
    first_value: u8,
) -> [u8; 256] {
    let mut byte = first_byte;
    while byte <= last_byte {
        table[byte as usize] = first_value + (byte - first_byte);
        byte += 1;
    }
    table
}

const fn fill_constant_range(
    mut table: [u8; 256],
    first_byte: u8,
    last_byte: u8,
    value: u8,
) -> [u8; 256] {
    let mut byte = first_byte;
    while byte <= last_byte {
        table[byte as usize] = value;
        byte += 1;
    }
    table
}

const fn fill_bytes<const N: usize>(mut table: [u8; 256], bytes: &[u8; N], value: u8) -> [u8; 256] {
    let mut index = 0;
    while index < bytes.len() {
        table[bytes[index] as usize] = value;
        index += 1;
    }
    table
}

const fn base64_value_table(char_62: u8, char_63: u8) -> [u8; 256] {
    let table = [INVALID_VALUE; 256];
    let table = fill_linear_range(table, b'A', b'Z', 0);
    let table = fill_linear_range(table, b'a', b'z', 26);
    let mut table = fill_linear_range(table, b'0', b'9', 52);
    table[char_62 as usize] = 62;
    table[char_63 as usize] = 63;
    table
}
