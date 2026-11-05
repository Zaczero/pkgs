//! Typed base83 output and a byte-admission lookup table generated from one alphabet.

use std::ascii::Char;
use std::hint::unlikely;
use std::num::NonZeroU32;

const CHARSET: &[Char; 83] =
    b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz#$%*+,-.:;=?@[]^_{|}~"
        .as_ascii()
        .unwrap();

const INVALID: u8 = 0xFF;
const DECODE: [u8; 256] = {
    let mut table = [INVALID; 256];
    let mut i = 0_usize;
    while let Some(&byte) = CHARSET.get(i) {
        let Some(slot) = table.get_mut(byte.to_u8() as usize) else {
            unreachable!()
        };
        *slot = i as u8;
        i = i.wrapping_add(1);
    }
    table
};
const POW83: [NonZeroU32; 5] = [
    NonZeroU32::new(1).unwrap(),
    NonZeroU32::new(83).unwrap(),
    NonZeroU32::new(6_889).unwrap(),
    NonZeroU32::new(571_787).unwrap(),
    NonZeroU32::new(47_458_321).unwrap(),
];

pub(crate) fn decode_byte(b: u8) -> Option<u8> {
    let v = *DECODE
        .get(usize::from(b))
        .expect("every byte has a lookup entry");
    if unlikely(v == INVALID) {
        None
    } else {
        Some(v)
    }
}

pub(crate) fn decode_array<const LENGTH: usize>(bytes: &[u8; LENGTH]) -> Option<u32> {
    const { assert!(LENGTH <= POW83.len(), "base83 field exceeds u32 range") };
    let mut value = 0_u32;
    for &b in bytes {
        let digit = u32::from(decode_byte(b)?);
        value = value.wrapping_mul(83).wrapping_add(digit);
    }
    Some(value)
}

/// Writes the low fixed-width base83 digits and returns the unfilled ASCII suffix.
///
/// Panics if the remaining view is too short. Admitted BlurHash shapes supply
/// exactly the sum of their field widths.
pub(crate) fn write_base83<const LENGTH: usize>(out: &mut [Char], value: u32) -> &mut [Char] {
    const { assert!(LENGTH <= POW83.len(), "base83 field exceeds u32 range") };
    let (field, remaining) = out.split_at_mut(LENGTH);
    for (character, &divisor) in field.iter_mut().zip(POW83.iter().take(LENGTH).rev()) {
        let digit = (value / divisor) % 83;
        let Some(&byte) = CHARSET.get(digit as usize) else {
            unreachable!("base83 remainder indexes the alphabet")
        };
        *character = byte;
    }
    remaining
}

#[cfg(kani)]
mod proofs;
