use crate::geographic::lonlat::{MAX_LATITUDE, MIN_LATITUDE};
use crate::py::errors;
use crate::py::functions::geocode::PyResult;

const SHORTLINK_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_~";
const SHORTLINK_ALPHABET_VALUE: [i8; 256] = build_shortlink_alphabet_lut();
pub(super) const SHORTLINK_MAX_ZOOM: u8 = 22;
const SHORTLINK_ZOOM_BIAS: u8 = 8;
const SHORTLINK_MAX_DATA_BITS: u8 = SHORTLINK_MAX_ZOOM + SHORTLINK_ZOOM_BIAS;

#[expect(
    clippy::indexing_slicing,
    reason = "the const loop bounds the alphabet index and a u8 bounds the table index; slice::get is not const on the pinned toolchain"
)]
const fn build_shortlink_alphabet_lut() -> [i8; 256] {
    let mut values = [-1_i8; 256];
    let mut index = 0;
    while index < SHORTLINK_ALPHABET.len() {
        let byte = SHORTLINK_ALPHABET[index];
        values[byte as usize] = index as i8;
        index = index.wrapping_add(1);
    }
    values
}

/// OSM `short_link.rb` `encode`: 32-bit scaled coordinates interleaved
/// x-first into a Morton code, six bits per character, `-` padding for
/// partial zoom levels. `zoom` is boundary-validated to `0..=22`.
///
/// Latitude is clamped after boundary admission, then scaled directly into
/// the unsigned lattice. A direct float-to-``u32`` cast saturates the north
/// endpoint (and its rounded predecessor) at ``u32::MAX`` instead of wrapping
/// through a signed intermediate to the south pole.
pub(super) fn shortlink_encode(lon: f64, lat: f64, zoom: u8) -> String {
    let x = ((lon + 180.0) * (2_f64.powi(32)) / 360.0) as i64 as u32;
    let lat = lat.clamp(MIN_LATITUDE, MAX_LATITUDE);
    let scale = 2_f64.powi(32) / 180.0;
    let y = (lat * scale - MIN_LATITUDE * scale) as u32;
    // Ruby interleaves x into the higher bit of each pair.
    let code = crate::curves::morton_interleave(y, x);
    let mut out = String::new();
    let bits = usize::from(zoom).wrapping_add(usize::from(SHORTLINK_ZOOM_BIAS));
    for index in 0..bits.div_ceil(3) {
        let shift = 58_usize.wrapping_sub(6_usize.wrapping_mul(index));
        let digit = (code >> shift) & 0x3F;
        let digit = usize::try_from(digit).expect("shortlink digit fits usize");
        out.push(char::from(
            *SHORTLINK_ALPHABET
                .get(digit)
                .expect("Morton digit is bounded by the shortlink alphabet"),
        ));
    }
    for _ in 0..bits % 3 {
        out.push('-');
    }
    out
}

/// OSM `short_link.rb` `decode`, with the legacy ``@`` spelling of ``~``.
pub(super) fn shortlink_decode(code: &str) -> PyResult<(f64, f64, u8)> {
    let mut x = 0_u64;
    let mut y = 0_u64;
    let mut data_characters = 0_usize;
    let mut padding_mod_3 = 0_u8;
    for ch in code.chars() {
        let byte = if ch == '@' {
            b'~'
        } else if ch.is_ascii() {
            ch as u8
        } else {
            return Err(errors::parse_error(
                format!("invalid OSM shortlink character {ch:?}"),
                crate::error::ParseFormat::OsmShortlink,
            ));
        };
        let position = *SHORTLINK_ALPHABET_VALUE
            .get(usize::from(byte))
            .expect("u8 bytes fit the 256-entry shortlink table");
        match position {
            value if value >= 0 => {
                let mut value = value as u64;
                for _ in 0..3 {
                    x = (x << 1) | u64::from(value & 0x20 != 0);
                    value = (value << 1) & 0x3F;
                    y = (y << 1) | u64::from(value & 0x20 != 0);
                    value = (value << 1) & 0x3F;
                }
                // A valid string has at most isize::MAX bytes, so its scalar
                // character count cannot overflow usize.
                data_characters = data_characters.wrapping_add(1);
            },
            _ if ch == '-' => {
                padding_mod_3 = padding_mod_3.wrapping_add(2) % 3;
            },
            _ => {
                return Err(errors::parse_error(
                    format!("invalid OSM shortlink character {ch:?}"),
                    crate::error::ParseFormat::OsmShortlink,
                ));
            },
        }
    }
    if data_characters == 0 {
        return Err(errors::parse_error(
            "empty OSM shortlink",
            crate::error::ParseFormat::OsmShortlink,
        ));
    }
    if data_characters > usize::from(SHORTLINK_MAX_DATA_BITS / 3) {
        // 10 data characters max: zoom tops out at 22 (z = chars * 3).
        return Err(errors::parse_error(
            "OSM shortlink is too long",
            crate::error::ParseFormat::OsmShortlink,
        ));
    }
    let z = u8::try_from(data_characters)
        .expect("admitted shortlink length fits u8")
        .wrapping_mul(3);
    // Zoom is implied by the code length: 3 bits per character minus the
    // `-` padding. Encode always emits at least 3 characters (zoom 0), so a
    // shorter code cannot name a real zoom level — reject it rather than
    // underflowing the decoded zoom.
    let zoom = i16::from(z)
        .wrapping_sub(i16::from(SHORTLINK_ZOOM_BIAS))
        .wrapping_sub(i16::from(padding_mod_3));
    let Ok(zoom) = u8::try_from(zoom) else {
        return Err(errors::parse_error(
            "OSM shortlink is too short",
            crate::error::ParseFormat::OsmShortlink,
        ));
    };
    let shift = u32::from(32_u8.wrapping_sub(z));
    let x = x.wrapping_shl(shift);
    let y = y.wrapping_shl(shift);
    Ok((
        (x as f64) * 360.0 / 2_f64.powi(32) - 180.0,
        (y as f64) * 180.0 / 2_f64.powi(32) - 90.0,
        zoom,
    ))
}
