#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::GeometryError;
use crate::py::functions::geocode::PyResult;

const OLC_ALPHABET: &[u8; 20] = b"23456789CFGHJMPQRVWX";

const OLC_ALPHABET_VALUE: [i8; 256] = build_olc_alphabet_lut();

const fn build_olc_alphabet_lut() -> [i8; 256] {
    let mut lut = [-1_i8; 256];
    let mut index = 0;
    while index < 20 {
        let byte = OLC_ALPHABET[index];
        lut[byte as usize] = index as i8;
        lut[byte.to_ascii_lowercase() as usize] = index as i8;
        lut[byte.to_ascii_uppercase() as usize] = index as i8;
        index += 1;
    }
    lut
}
const OLC_SEPARATOR: u8 = b'+';
const OLC_SEPARATOR_POSITION: usize = 8;
const OLC_PADDING: u8 = b'0';
const OLC_PAIR_CODE_LENGTH: usize = 10;
const OLC_MAX_DIGITS: usize = 15;
/// `20^3`: integer place value of one full pair block.
const OLC_PAIR_PRECISION: i64 = 8000;
/// `20^4`: place value of the most significant pair digit.
const OLC_PAIR_FIRST_PLACE: i64 = 160_000;
const OLC_GRID_ROWS: i64 = 5;
const OLC_GRID_COLUMNS: i64 = 4;
/// `20^3 * 5^5` / `20^3 * 4^5`: integer precision of the finest grid digit.
const OLC_FINAL_LAT_PRECISION: i64 = OLC_PAIR_PRECISION * 3125;
const OLC_FINAL_LNG_PRECISION: i64 = OLC_PAIR_PRECISION * 1024;
/// Reference `PAIR_RESOLUTIONS_`, degrees per pair level.
const OLC_PAIR_RESOLUTIONS: [f64; 5] = [20.0, 1.0, 0.05, 0.0025, 0.000_125];

fn olc_value(byte: u8) -> Option<i64> {
    let value = OLC_ALPHABET_VALUE[byte as usize];
    (value >= 0).then_some(i64::from(value))
}

/// Reference `locationToIntegers`: clip latitude, wrap longitude, floor to
/// the finest integer grid.
fn olc_integers(latitude: f64, longitude: f64) -> (i64, i64) {
    let mut lat = (latitude * OLC_FINAL_LAT_PRECISION as f64).floor() as i64;
    lat += 90 * OLC_FINAL_LAT_PRECISION;
    lat = lat.clamp(0, 180 * OLC_FINAL_LAT_PRECISION - 1);
    let mut lng = (longitude * OLC_FINAL_LNG_PRECISION as f64).floor() as i64;
    lng += 180 * OLC_FINAL_LNG_PRECISION;
    lng = lng.rem_euclid(360 * OLC_FINAL_LNG_PRECISION);
    (lat, lng)
}

/// Reference `encodeIntegers` (the caller validates `length`).
pub(super) fn olc_encode(latitude: f64, longitude: f64, length: usize) -> String {
    let (mut lat, mut lng) = olc_integers(latitude, longitude);
    let mut digits = [0_u8; OLC_MAX_DIGITS];
    if length > OLC_PAIR_CODE_LENGTH {
        for slot in (OLC_PAIR_CODE_LENGTH..OLC_MAX_DIGITS).rev() {
            let index = (lat % OLC_GRID_ROWS) * OLC_GRID_COLUMNS + (lng % OLC_GRID_COLUMNS);
            digits[slot] = OLC_ALPHABET[index as usize];
            lat /= OLC_GRID_ROWS;
            lng /= OLC_GRID_COLUMNS;
        }
    } else {
        lat /= OLC_GRID_ROWS.pow(5);
        lng /= OLC_GRID_COLUMNS.pow(5);
    }
    for pair in (0..OLC_PAIR_CODE_LENGTH / 2).rev() {
        digits[pair * 2] = OLC_ALPHABET[(lat % 20) as usize];
        digits[pair * 2 + 1] = OLC_ALPHABET[(lng % 20) as usize];
        lat /= 20;
        lng /= 20;
    }
    let mut code = String::with_capacity(OLC_MAX_DIGITS + 1);
    let take = length.min(OLC_MAX_DIGITS);
    for (slot, &digit) in digits
        .iter()
        .enumerate()
        .take(take.max(OLC_SEPARATOR_POSITION))
    {
        if slot == OLC_SEPARATOR_POSITION {
            code.push(char::from(OLC_SEPARATOR));
        }
        code.push(if slot < take {
            char::from(digit)
        } else {
            char::from(OLC_PADDING)
        });
    }
    if take <= OLC_SEPARATOR_POSITION {
        code.push(char::from(OLC_SEPARATOR));
    }
    code
}

/// Decoded full code: SW corner, NE corner, and significant digit count.
pub(super) struct OlcArea {
    pub(super) lat_lo: f64,
    pub(super) lng_lo: f64,
    pub(super) lat_hi: f64,
    pub(super) lng_hi: f64,
    length: usize,
}

impl OlcArea {
    const fn center(&self) -> (f64, f64) {
        (
            self.lat_lo.midpoint(self.lat_hi).min(90.0),
            self.lng_lo.midpoint(self.lng_hi).min(180.0),
        )
    }
}

/// Reference `isValid`.
pub(super) fn olc_is_valid(code: &str) -> bool {
    let bytes = code.as_bytes();
    let Some(sep) = bytes.iter().position(|&byte| byte == OLC_SEPARATOR) else {
        return false;
    };
    if code.matches(char::from(OLC_SEPARATOR)).count() > 1
        || bytes.len() == 1
        || sep > OLC_SEPARATOR_POSITION
        || sep % 2 == 1
    {
        return false;
    }
    if let Some(pad) = bytes.iter().position(|&byte| byte == OLC_PADDING) {
        if sep < OLC_SEPARATOR_POSITION || pad == 0 {
            return false;
        }
        let rpad = bytes.len()
            - bytes
                .iter()
                .rev()
                .position(|&byte| byte == OLC_PADDING)
                .expect("pad > 0 above guarantees a padding byte exists");
        let pads = &bytes[pad..rpad];
        if pads.len() % 2 == 1
            || pads.iter().any(|&byte| byte != OLC_PADDING)
            || !code.ends_with(char::from(OLC_SEPARATOR))
        {
            return false;
        }
    }
    if bytes.len() - sep - 1 == 1 {
        return false;
    }
    bytes
        .iter()
        .all(|&byte| byte == OLC_SEPARATOR || byte == OLC_PADDING || olc_value(byte).is_some())
}

/// Reference `isShort` / `isFull`.
pub(super) fn olc_is_short(code: &str) -> bool {
    olc_is_valid(code)
        && code
            .find(char::from(OLC_SEPARATOR))
            .is_some_and(|sep| sep < OLC_SEPARATOR_POSITION)
}

pub(super) fn olc_is_full(code: &str) -> bool {
    if !olc_is_valid(code) || olc_is_short(code) {
        return false;
    }
    let bytes = code.as_bytes();
    let Some(first_lat) = olc_value(bytes[0]) else {
        return false;
    };
    if first_lat * 20 >= 180 {
        return false;
    }
    if bytes.len() > 1
        && let Some(first_lng) = olc_value(bytes[1])
        && first_lng * 20 >= 360
    {
        return false;
    }
    true
}

/// Reference `decode`, for full codes (the caller validates).
pub(super) fn olc_decode(code: &str) -> OlcArea {
    let digits: Vec<i64> = code
        .bytes()
        .filter(|&byte| byte != OLC_SEPARATOR && byte != OLC_PADDING)
        .take(OLC_MAX_DIGITS)
        .map(|byte| olc_value(byte).expect("full code was validated"))
        .collect();
    let mut normal_lat = -90 * OLC_PAIR_PRECISION;
    let mut normal_lng = -180 * OLC_PAIR_PRECISION;
    let mut grid_lat = 0_i64;
    let mut grid_lng = 0_i64;
    let pair_digits = digits.len().min(OLC_PAIR_CODE_LENGTH);
    let mut place = OLC_PAIR_FIRST_PLACE;
    let mut index = 0;
    while index < pair_digits {
        normal_lat += digits[index] * place;
        normal_lng += digits[index + 1] * place;
        if index < pair_digits - 2 {
            place /= 20;
        }
        index += 2;
    }
    let (lat_precision, lng_precision) = if digits.len() > OLC_PAIR_CODE_LENGTH {
        let mut row_place = OLC_GRID_ROWS.pow(4);
        let mut column_place = OLC_GRID_COLUMNS.pow(4);
        for (offset, &digit) in digits[OLC_PAIR_CODE_LENGTH..].iter().enumerate() {
            grid_lat += (digit / OLC_GRID_COLUMNS) * row_place;
            grid_lng += (digit % OLC_GRID_COLUMNS) * column_place;
            if OLC_PAIR_CODE_LENGTH + offset < digits.len() - 1 {
                row_place /= OLC_GRID_ROWS;
                column_place /= OLC_GRID_COLUMNS;
            }
        }
        (
            row_place as f64 / OLC_FINAL_LAT_PRECISION as f64,
            column_place as f64 / OLC_FINAL_LNG_PRECISION as f64,
        )
    } else {
        (
            place as f64 / OLC_PAIR_PRECISION as f64,
            place as f64 / OLC_PAIR_PRECISION as f64,
        )
    };
    // The reference rounds to 14 decimals to shed float noise.
    let round14 = |value: f64| (value * 1e14).round() / 1e14;
    let lat_lo = round14(
        normal_lat as f64 / OLC_PAIR_PRECISION as f64
            + grid_lat as f64 / OLC_FINAL_LAT_PRECISION as f64,
    );
    let lng_lo = round14(
        normal_lng as f64 / OLC_PAIR_PRECISION as f64
            + grid_lng as f64 / OLC_FINAL_LNG_PRECISION as f64,
    );
    OlcArea {
        lat_lo,
        lng_lo,
        lat_hi: round14(lat_lo + lat_precision),
        lng_hi: round14(lng_lo + lng_precision),
        length: digits.len(),
    }
}

pub(super) fn olc_require_full(code: &str) -> PyResult<()> {
    if olc_is_full(code) {
        Ok(())
    } else {
        Err(crate::py::errors::parse_error(
            format!("{code:?} is not a full plus code"),
            crate::error::ParseFormat::PlusCode,
        ))
    }
}

/// Validate a plus-code digit count: even values from 2 to 10, then 11-15.
pub(super) fn validate_pluscode_length(length: i64) -> PyResult<usize> {
    if !(2..=15).contains(&length) {
        return Err(GeometryError::new_err(format!(
            "pluscode length must be between 2 and 15, got {length}"
        )));
    }
    if length < OLC_PAIR_CODE_LENGTH as i64 && length % 2 == 1 {
        return Err(GeometryError::new_err(format!(
            "pluscode length below 10 must be even, got {length}"
        )));
    }
    Ok(length as usize)
}

/// Reference `normalizeLongitude`: into `[-180, 180)`.
pub(super) fn olc_normalize_longitude(longitude: f64) -> f64 {
    (longitude + 180.0).rem_euclid(360.0) - 180.0
}

/// Reference `shorten`.
pub(super) fn olc_shorten(code: &str, latitude: f64, longitude: f64) -> PyResult<String> {
    olc_require_full(code)?;
    if code.contains(char::from(OLC_PADDING)) {
        return Err(GeometryError::new_err(format!(
            "cannot shorten the padded plus code {code:?}"
        )));
    }
    let code = code.to_ascii_uppercase();
    let area = olc_decode(&code);
    if area.length < 6 {
        return Err(GeometryError::new_err(
            "pluscode_shorten requires a code with at least 6 digits",
        ));
    }
    let latitude = latitude.clamp(-90.0, 90.0);
    let longitude = olc_normalize_longitude(longitude);
    let (center_lat, center_lng) = area.center();
    let range = (center_lat - latitude)
        .abs()
        .max((center_lng - longitude).abs());
    for level in (1..=OLC_PAIR_RESOLUTIONS.len() - 2).rev() {
        // 0.3 instead of 0.5 leaves recovery a safety margin.
        if range < OLC_PAIR_RESOLUTIONS[level] * 0.3 {
            return code
                .get((level + 1) * 2..)
                .map(str::to_owned)
                .ok_or_else(|| GeometryError::new_err("invalid plus code"));
        }
    }
    Ok(code)
}

/// Reference `recoverNearest`.
pub(super) fn olc_recover(code: &str, latitude: f64, longitude: f64) -> PyResult<String> {
    if olc_is_full(code) {
        return Ok(code.to_ascii_uppercase());
    }
    if !olc_is_short(code) {
        return Err(crate::py::errors::parse_error(
            format!("{code:?} is not a short plus code"),
            crate::error::ParseFormat::PlusCode,
        ));
    }
    let latitude = latitude.clamp(-90.0, 90.0);
    let longitude = olc_normalize_longitude(longitude);
    let code = code.to_ascii_uppercase();
    let padding = OLC_SEPARATOR_POSITION
        - code
            .find(char::from(OLC_SEPARATOR))
            .expect("short codes carry a separator");
    let resolution = 20_f64.powf(2.0 - padding as f64 / 2.0);
    let half = resolution / 2.0;
    let reference = olc_encode(latitude, longitude, OLC_PAIR_CODE_LENGTH);
    let prefix = reference
        .get(..padding)
        .ok_or_else(|| GeometryError::new_err("invalid plus code"))?;
    let candidate = format!("{prefix}{code}");
    let area = olc_decode(&candidate);
    let (mut center_lat, center_lng) = area.center();
    let mut center_lng = center_lng;
    if latitude + half < center_lat && center_lat - resolution >= -90.0 {
        center_lat -= resolution;
    } else if latitude - half > center_lat && center_lat + resolution <= 90.0 {
        center_lat += resolution;
    }
    if longitude + half < center_lng {
        center_lng -= resolution;
    } else if longitude - half > center_lng {
        center_lng += resolution;
    }
    Ok(olc_encode(center_lat, center_lng, area.length))
}

// --- OSM shortlink
// ------------------------------------------------------------
