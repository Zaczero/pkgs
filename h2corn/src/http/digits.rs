const DIGIT_PAIRS: [u8; 200] = {
    let mut out = [0; 200];
    let mut value = 0;
    while value < 100 {
        out[value * 2] = b'0' + (value / 10) as u8;
        out[value * 2 + 1] = b'0' + (value % 10) as u8;
        value += 1;
    }
    out
};

pub(crate) fn two_digit_bytes(value: usize) -> [u8; 2] {
    debug_assert!(value < 100);
    let offset = value * 2;
    [DIGIT_PAIRS[offset], DIGIT_PAIRS[offset + 1]]
}

pub(crate) fn three_digit_bytes(value: u16) -> [u8; 3] {
    debug_assert!((100..=999).contains(&value));
    let suffix = two_digit_bytes(usize::from(value % 100));
    [b'0' + (value / 100) as u8, suffix[0], suffix[1]]
}

#[cfg(test)]
mod tests {
    use super::{three_digit_bytes, two_digit_bytes};

    #[test]
    fn two_digit_bytes_formats_valid_values() {
        assert_eq!(two_digit_bytes(0), *b"00");
        assert_eq!(two_digit_bytes(7), *b"07");
        assert_eq!(two_digit_bytes(99), *b"99");
    }

    #[test]
    fn three_digit_bytes_formats_valid_values() {
        assert_eq!(three_digit_bytes(100), *b"100");
        assert_eq!(three_digit_bytes(200), *b"200");
        assert_eq!(three_digit_bytes(999), *b"999");
    }
}
