//! Coordinate admission, Morton encoding and direct ASCII output.

use std::ascii::Char;
use std::hint::unlikely;
use std::simd::u64x2;

use crate::constants::{
    CHARSET, DECODE_INVALID, DECODE_LUT, DECODE_OFFSET, X_SCALE, X_SCALE_INV, Y_SCALE, Y_SCALE_INV,
};
use crate::errors::{DecodeError, EncodeError};

const COORD_BITS: u8 = 32;
const MIN_ZOOM_BIT_COUNT: u8 = 8;
const BITS_PER_DIGIT: u8 = 3;
pub(crate) const MAX_ZOOM: u8 = 22;
const MAX_ZOOM_BIT_COUNT: u8 = MAX_ZOOM + MIN_ZOOM_BIT_COUNT;
const _: () = assert!(MAX_ZOOM_BIT_COUNT <= COORD_BITS);
const _: () = assert!(MAX_ZOOM_BIT_COUNT.is_multiple_of(BITS_PER_DIGIT));

#[expect(
    clippy::arithmetic_side_effects,
    reason = "the fixed shifts are all below the u64 lane width and implement Morton bit spreading"
)]
fn interleave_bits(x: u32, y: u32) -> u64 {
    let mut v = u64x2::from_array([u64::from(x), u64::from(y)]);
    v = (v | (v << 16)) & u64x2::splat(0x0000_FFFF_0000_FFFF);
    v = (v | (v << 8)) & u64x2::splat(0x00FF_00FF_00FF_00FF);
    v = (v | (v << 4)) & u64x2::splat(0x0F0F_0F0F_0F0F_0F0F);
    v = (v | (v << 2)) & u64x2::splat(0x3333_3333_3333_3333);
    v = (v | (v << 1)) & u64x2::splat(0x5555_5555_5555_5555);
    let [sx, sy] = v.to_array();
    (sx << 1) | sy
}

pub(crate) fn encode(lon: f64, lat: f64, zoom: u8) -> Result<EncodedShortlink, EncodeError> {
    if unlikely(zoom > MAX_ZOOM) {
        return Err(EncodeError::ZoomOutOfRange { zoom });
    }
    if unlikely(!lon.is_finite()) {
        return Err(EncodeError::NonFiniteCoordinate {
            coordinate: "longitude",
        });
    }
    if unlikely(!lat.is_finite()) {
        return Err(EncodeError::NonFiniteCoordinate {
            coordinate: "latitude",
        });
    }

    let normalized_lat = lat + 90.0;

    // In-range inputs (virtually every call) need no modular wraparound:
    // a remainder calls `fmod` on the x86-64-v2 baseline. The fast arm
    // keeps the identical remaining operations, so it is bit-exact.
    let (wrapped_x, wrapped_y) =
        if (0.0..180.0).contains(&normalized_lat) && (-180.0..180.0).contains(&lon) {
            (lon + 180.0, 180.0 - (normalized_lat - 180.0).abs())
        } else {
            // Reduce before adding offsets: large finite inputs can absorb
            // those offsets, and their quotient no longer retains parity.
            let lat = lat % 360.0;
            let lat = if lat > 180.0 {
                lat - 360.0
            } else if lat < -180.0 {
                lat + 360.0
            } else {
                lat
            };
            // Classify the pole before adding 90, which can round a nearby
            // latitude onto it. The north pole includes the longitude flip.
            let reflected = !(-90.0..90.0).contains(&lat);
            let lat = if lat >= 90.0 {
                180.0 - lat
            } else if lat < -90.0 {
                -180.0 - lat
            } else {
                lat
            };
            let lon = lon % 360.0;
            // A final modulo would wrap a value rounded up to 360 onto the
            // wrong seam; the u32 projection keeps it in the final column.
            let wrapped_x = if reflected {
                if lon < 0.0 { lon + 360.0 } else { lon }
            } else if lon < -180.0 {
                lon + 540.0
            } else if lon >= 180.0 {
                lon - 180.0
            } else {
                lon + 180.0
            };
            (wrapped_x, 180.0 - ((lat + 90.0) - 180.0).abs())
        };
    let encoded_x = (wrapped_x * X_SCALE) as u32;
    let encoded_y = (wrapped_y * Y_SCALE) as u32;
    let cell = interleave_bits(encoded_x, encoded_y);

    let bit_count = zoom
        .checked_add(MIN_ZOOM_BIT_COUNT)
        .expect("admitted zoom fits coordinate bits");
    Ok(EncodedShortlink {
        cell,
        digits: bit_count.div_ceil(BITS_PER_DIGIT),
        padding: bit_count % BITS_PER_DIGIT,
    })
}

/// Admitted coordinates with a fixed output shape, ready for final-storage filling.
pub(crate) struct EncodedShortlink {
    /// Morton-interleaved longitude and latitude bits.
    cell: u64,
    /// Six-bit alphabet characters preceding the zoom suffix.
    digits: u8,
    /// Hyphens representing the remaining zoom bits.
    padding: u8,
}

impl EncodedShortlink {
    /// Number of one-byte ASCII characters in the encoded link.
    pub(crate) const fn len(&self) -> usize {
        (self.digits as usize)
            .checked_add(self.padding as usize)
            .expect("two widened u8 counts fit usize")
    }

    /// Fills an exactly sized output view; panics if its length differs from len().
    pub(crate) fn write(&self, characters: &mut [Char]) {
        assert_eq!(characters.len(), self.len(), "shortlink output length");
        let (digits, padding) = characters.split_at_mut(self.digits as usize);
        let mut cell = self.cell;
        for character in digits {
            let digit = (cell >> 58) as usize;
            *character = *CHARSET.get(digit).expect("six-bit Morton digit");
            cell <<= 6;
        }
        padding.fill(Char::HyphenMinus);
    }
}

pub(crate) fn decode(s: &str) -> Result<(f64, f64, u8), DecodeError> {
    let mut x = 0;
    let mut y = 0;
    let mut z = 0_u8;
    let mut offset_bits = 0_u8;

    for c in s.bytes() {
        let packed = DECODE_LUT
            .get(c as usize)
            .copied()
            .expect("every byte has a lookup entry");
        if unlikely(packed == DECODE_INVALID) {
            continue;
        }
        if packed == DECODE_OFFSET {
            offset_bits = offset_bits.checked_sub(1).unwrap_or(BITS_PER_DIGIT - 1);
            continue;
        }

        if unlikely(z == MAX_ZOOM_BIT_COUNT) {
            return Err(DecodeError::TooLong);
        }

        x = (x << BITS_PER_DIGIT) | u32::from(packed & 0b111);
        y = (y << BITS_PER_DIGIT) | u32::from(packed >> 3);
        z = z
            .checked_add(BITS_PER_DIGIT)
            .expect("admitted digit count fits coordinate bits");
    }

    let minimum_bits = MIN_ZOOM_BIT_COUNT
        .checked_add(offset_bits)
        .expect("zoom suffix is at most two bits");
    let zoom = z.checked_sub(minimum_bits).ok_or(DecodeError::TooShort)?;

    let shift = COORD_BITS
        .checked_sub(z)
        .expect("coordinate precision was admitted");
    x <<= shift;
    y <<= shift;

    Ok((
        (f64::from(x) * X_SCALE_INV) - 180.0,
        (f64::from(y) * Y_SCALE_INV) - 90.0,
        zoom,
    ))
}

#[cfg(kani)]
mod proofs {
    use super::interleave_bits;

    #[kani::proof]
    fn morton_bits_preserve_both_coordinate_lanes() {
        let x: u32 = kani::any();
        let y: u32 = kani::any();
        let bit: u32 = kani::any();
        kani::assume(bit < 32);
        let packed = interleave_bits(x, y);
        assert_eq!((packed >> (2 * bit)) & 1, u64::from((y >> bit) & 1));
        assert_eq!((packed >> (2 * bit + 1)) & 1, u64::from((x >> bit) & 1));
    }
}
