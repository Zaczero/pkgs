use std::ascii::Char;

use super::{decode_array, write_base83};

fn digit(byte: u8) -> Option<u32> {
    match byte {
        b'0'..=b'9' => Some(u32::from(byte - b'0')),
        b'A'..=b'Z' => Some(10 + u32::from(byte - b'A')),
        b'a'..=b'z' => Some(36 + u32::from(byte - b'a')),
        b'#' => Some(62),
        b'$' => Some(63),
        b'%' => Some(64),
        b'*' => Some(65),
        b'+' => Some(66),
        b',' => Some(67),
        b'-' => Some(68),
        b'.' => Some(69),
        b':' => Some(70),
        b';' => Some(71),
        b'=' => Some(72),
        b'?' => Some(73),
        b'@' => Some(74),
        b'[' => Some(75),
        b']' => Some(76),
        b'^' => Some(77),
        b'_' => Some(78),
        b'{' => Some(79),
        b'|' => Some(80),
        b'}' => Some(81),
        b'~' => Some(82),
        _ => None,
    }
}

#[kani::proof]
#[kani::unwind(8)]
fn single_digit_fields_match_the_base83_alphabet() {
    let value: u32 = kani::any();
    let prefix_bytes: [u8; 2] = kani::any();
    let prefix = prefix_bytes.map(|byte| Char::from_u8(byte % 128).unwrap());
    let mut output = [prefix[0], prefix[1], Char::MIN];
    let remaining = write_base83::<1>(&mut output[prefix.len()..], value);
    assert!(remaining.is_empty());
    assert_eq!(&output[..prefix.len()], &prefix);
    assert_eq!(output.len(), prefix.len() + 1);
    assert_eq!(digit(output[prefix.len()].to_u8()), Some(value % 83));

    let byte: u8 = kani::any();
    assert_eq!(decode_array(&[byte]), digit(byte));
}
