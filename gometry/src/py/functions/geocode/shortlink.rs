#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::py::functions::geocode::PyResult;

const SHORTLINK_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_~";

const SHORTLINK_ALPHABET_VALUE: [i8; 256] = build_shortlink_alphabet_lut();

const fn build_shortlink_alphabet_lut() -> [i8; 256] {
    let mut lut = [-1_i8; 256];
    let mut index = 0;
    while index < 64 {
        lut[SHORTLINK_ALPHABET[index] as usize] = index as i8;
        index += 1;
    }
    lut
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
    let lat = lat.clamp(
        crate::boundary::geographic::MIN_LATITUDE,
        crate::boundary::geographic::MAX_LATITUDE,
    );
    let scale = 2_f64.powi(32) / 180.0;
    let y = (lat * scale - crate::boundary::geographic::MIN_LATITUDE * scale) as u32;
    // Ruby interleaves x into the higher bit of each pair.
    let code = crate::curves::morton_interleave(y, x);
    let mut out = String::new();
    let bits = usize::from(zoom) + 8;
    for index in 0..bits.div_ceil(3) {
        let digit = (code >> (58 - 6 * index)) & 0x3F;
        out.push(char::from(SHORTLINK_ALPHABET[digit as usize]));
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
    let mut z = 0_i64;
    let mut z_offset = 0_i64;
    for ch in code.chars() {
        let byte = if ch == '@' {
            b'~'
        } else if ch.is_ascii() {
            ch as u8
        } else {
            return Err(crate::py::errors::parse_error(
                format!("invalid OSM shortlink character {ch:?}"),
                crate::error::ParseFormat::OsmShortlink,
            ));
        };
        let position = SHORTLINK_ALPHABET_VALUE[byte as usize];
        match position {
            value if value >= 0 => {
                let mut value = value as u64;
                for _ in 0..3 {
                    x = (x << 1) | u64::from(value & 0x20 != 0);
                    value = (value << 1) & 0x3F;
                    y = (y << 1) | u64::from(value & 0x20 != 0);
                    value = (value << 1) & 0x3F;
                }
                z += 3;
            },
            _ if ch == '-' => z_offset -= 1,
            _ => {
                return Err(crate::py::errors::parse_error(
                    format!("invalid OSM shortlink character {ch:?}"),
                    crate::error::ParseFormat::OsmShortlink,
                ));
            },
        }
    }
    if z == 0 {
        return Err(crate::py::errors::parse_error(
            "empty OSM shortlink",
            crate::error::ParseFormat::OsmShortlink,
        ));
    }
    if z > 30 {
        // 10 data characters max: zoom tops out at 22 (z = chars * 3).
        return Err(crate::py::errors::parse_error(
            "OSM shortlink is too long",
            crate::error::ParseFormat::OsmShortlink,
        ));
    }
    // Zoom is implied by the code length: 3 bits per character minus the
    // `-` padding. Encode always emits at least 3 characters (zoom 0), so a
    // shorter code cannot name a real zoom level — reject it rather than
    // report upstream Ruby's nonsense negative zoom.
    let zoom = z - 8 - z_offset.rem_euclid(3);
    let Ok(zoom) = u8::try_from(zoom) else {
        return Err(crate::py::errors::parse_error(
            "OSM shortlink is too short",
            crate::error::ParseFormat::OsmShortlink,
        ));
    };
    let x = x << (32 - z);
    let y = y << (32 - z);
    Ok((
        (x as f64) * 360.0 / 2_f64.powi(32) - 180.0,
        (y as f64) * 180.0 / 2_f64.powi(32) - 90.0,
        zoom,
    ))
}
