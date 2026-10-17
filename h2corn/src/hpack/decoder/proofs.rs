use super::{DecoderError, NeedMore, decode_int};

#[kani::proof]
#[kani::unwind(8)]
fn integer_fields_match_base128_and_consume_only_the_field() {
    let bytes: [u8; 6] = kani::any();
    let len: usize = kani::any();
    kani::assume(len <= bytes.len());
    let input = &bytes[..len];
    let mut remaining = input;
    let choice: u8 = kani::any();
    let (actual, width) = match choice & 3 {
        0 => (decode_int::<4, _>(&mut remaining), 4),
        1 => (decode_int::<5, _>(&mut remaining), 5),
        2 => (decode_int::<6, _>(&mut remaining), 6),
        _ => (decode_int::<7, _>(&mut remaining), 7),
    };

    let underflow = Err(DecoderError::NeedMore(NeedMore::IntegerUnderflow));
    let (expected, consumed) = if let Some(first) = input.first() {
        let mask = (1_u8 << width) - 1;
        let prefix = usize::from(first & mask);
        if prefix < usize::from(mask) {
            (Ok(prefix), 1)
        } else {
            let digits = &input[1..len.min(5)];
            if let Some(last) = digits.iter().position(|byte| byte & 128 == 0) {
                let suffix = digits[..=last]
                    .iter()
                    .rev()
                    .fold(0_usize, |value, byte| value * 128 + usize::from(byte & 127));
                (Ok(prefix + suffix), last + 2)
            } else if len < 5 {
                (underflow, len)
            } else {
                (Err(DecoderError::IntegerOverflow), 5)
            }
        }
    } else {
        (underflow, 0)
    };

    assert_eq!(actual, expected);
    assert_eq!(remaining, &input[consumed..]);
}
