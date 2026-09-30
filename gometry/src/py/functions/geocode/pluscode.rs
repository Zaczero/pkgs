use crate::GeometryError;
use crate::py::errors;
use crate::py::functions::geocode::PyResult;

const OLC_ALPHABET: &[u8; 20] = b"23456789CFGHJMPQRVWX";

const OLC_SEPARATOR: u8 = b'+';
const OLC_SEPARATOR_POSITION: usize = 8;
const OLC_PADDING: u8 = b'0';
const OLC_PAIR_CODE_LENGTH: usize = 10;
const OLC_MIN_DIGITS: usize = 2;
const OLC_MAX_DIGITS: usize = 15;
/// `20^3`: integer place value of one full pair block.
const OLC_PAIR_PRECISION: i64 = 8000;
/// `20^4`: place value of the most significant pair digit.
const OLC_PAIR_FIRST_PLACE: i64 = 160_000;
const OLC_GRID_ROWS: i64 = 5;
const OLC_GRID_COLUMNS: i64 = 4;
const OLC_GRID_ROWS_FINE_FACTOR: i64 = OLC_GRID_ROWS.pow(5);
const OLC_GRID_COLUMNS_FINE_FACTOR: i64 = OLC_GRID_COLUMNS.pow(5);
/// `20^3 * 5^5` / `20^3 * 4^5`: integer precision of the finest grid digit.
const OLC_FINAL_LAT_PRECISION: i64 = OLC_PAIR_PRECISION * OLC_GRID_ROWS_FINE_FACTOR;
const OLC_FINAL_LNG_PRECISION: i64 = OLC_PAIR_PRECISION * OLC_GRID_COLUMNS_FINE_FACTOR;
/// Reference `PAIR_RESOLUTIONS_`, degrees per pair level.
const OLC_PAIR_RESOLUTIONS: [f64; 5] = [20.0, 1.0, 0.05, 0.0025, 0.000_125];

const fn olc_value(byte: u8) -> Option<i64> {
    match byte.to_ascii_uppercase() {
        b'2' => Some(0),
        b'3' => Some(1),
        b'4' => Some(2),
        b'5' => Some(3),
        b'6' => Some(4),
        b'7' => Some(5),
        b'8' => Some(6),
        b'9' => Some(7),
        b'C' => Some(8),
        b'F' => Some(9),
        b'G' => Some(10),
        b'H' => Some(11),
        b'J' => Some(12),
        b'M' => Some(13),
        b'P' => Some(14),
        b'Q' => Some(15),
        b'R' => Some(16),
        b'V' => Some(17),
        b'W' => Some(18),
        b'X' => Some(19),
        _ => None,
    }
}

fn olc_digit(value: i64) -> u8 {
    let index = usize::try_from(value).expect("OLC digit is nonnegative");
    *OLC_ALPHABET
        .get(index)
        .expect("OLC digit is bounded by the alphabet")
}

/// Reference `locationToIntegers`: clip latitude, wrap longitude, floor to
/// the finest integer grid.
fn olc_integers(latitude: f64, longitude: f64) -> (i64, i64) {
    let mut lat = (latitude * OLC_FINAL_LAT_PRECISION as f64).floor() as i64;
    lat = lat.wrapping_add(90_i64.wrapping_mul(OLC_FINAL_LAT_PRECISION));
    lat = lat.clamp(
        0,
        180_i64
            .wrapping_mul(OLC_FINAL_LAT_PRECISION)
            .wrapping_sub(1),
    );
    let mut lng = (longitude * OLC_FINAL_LNG_PRECISION as f64).floor() as i64;
    lng = lng.wrapping_add(180_i64.wrapping_mul(OLC_FINAL_LNG_PRECISION));
    lng = lng.rem_euclid(360_i64.wrapping_mul(OLC_FINAL_LNG_PRECISION));
    (lat, lng)
}

/// Reference `encodeIntegers` (the caller validates `length`).
pub(super) fn olc_encode(latitude: f64, longitude: f64, length: usize) -> String {
    let (mut lat, mut lng) = olc_integers(latitude, longitude);
    let mut digits = [0_u8; OLC_MAX_DIGITS];
    if length > OLC_PAIR_CODE_LENGTH {
        for digit in digits.iter_mut().skip(OLC_PAIR_CODE_LENGTH).rev() {
            let index = (lat % OLC_GRID_ROWS)
                .wrapping_mul(OLC_GRID_COLUMNS)
                .wrapping_add(lng % OLC_GRID_COLUMNS);
            *digit = olc_digit(index);
            lat /= OLC_GRID_ROWS;
            lng /= OLC_GRID_COLUMNS;
        }
    } else {
        lat = lat.wrapping_div(OLC_GRID_ROWS_FINE_FACTOR);
        lng = lng.wrapping_div(OLC_GRID_COLUMNS_FINE_FACTOR);
    }
    let pair_digits = digits
        .get_mut(..OLC_PAIR_CODE_LENGTH)
        .expect("fixed pair digits fit the code buffer");
    for pair in pair_digits.as_chunks_mut::<2>().0.iter_mut().rev() {
        let [latitude_digit, longitude_digit] = pair;
        *latitude_digit = olc_digit(lat % 20);
        *longitude_digit = olc_digit(lng % 20);
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
        || sep == 0
        || sep > OLC_SEPARATOR_POSITION
        || sep % 2 == 1
    {
        return false;
    }
    let Some(suffix) = bytes.get(sep.wrapping_add(1)..) else {
        return false;
    };
    let padding = bytes.iter().position(|&byte| byte == OLC_PADDING);
    if let Some(pad) = padding {
        if sep < OLC_SEPARATOR_POSITION || pad == 0 {
            return false;
        }
        let pad_end = bytes.len().wrapping_sub(
            bytes
                .iter()
                .rev()
                .position(|&byte| byte == OLC_PADDING)
                .expect("pad > 0 above guarantees a padding byte exists"),
        );
        let Some(pads) = bytes.get(pad..pad_end) else {
            return false;
        };
        if pads.len() % 2 == 1
            || pads.iter().any(|&byte| byte != OLC_PADDING)
            || pad_end != sep
            || !code.ends_with(char::from(OLC_SEPARATOR))
        {
            return false;
        }
    }
    if suffix.len() == 1 {
        return false;
    }
    // Without padding, the separator is the only non-digit byte. With padding,
    // its validated start is the significant digit count.
    let digit_count = padding.unwrap_or_else(|| bytes.len().wrapping_sub(1));
    if digit_count > OLC_MAX_DIGITS {
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
    let Some(first_lat) = bytes.first().copied().and_then(olc_value) else {
        return false;
    };
    if first_lat.wrapping_mul(20) >= 180 {
        return false;
    }
    if let Some(first_lng) = bytes.get(1).copied().and_then(olc_value)
        && first_lng.wrapping_mul(20) >= 360
    {
        return false;
    }
    true
}

/// Reference `decode`, for full codes (the caller validates).
pub(super) fn olc_decode(code: &str) -> OlcArea {
    let mut digit_storage = [0_i64; OLC_MAX_DIGITS];
    let mut digit_count = 0;
    for byte in code
        .bytes()
        .filter(|&byte| byte != OLC_SEPARATOR && byte != OLC_PADDING)
        .take(OLC_MAX_DIGITS)
    {
        *digit_storage
            .get_mut(digit_count)
            .expect("full code digits fit the fixed OLC buffer") =
            olc_value(byte).expect("full code was validated");
        digit_count = digit_count.wrapping_add(1);
    }
    let digits = digit_storage
        .get(..digit_count)
        .expect("full code digit count fits the fixed OLC buffer");
    let mut normal_lat = -90 * OLC_PAIR_PRECISION;
    let mut normal_lng = -180 * OLC_PAIR_PRECISION;
    let mut grid_lat = 0_i64;
    let mut grid_lng = 0_i64;
    let pair_digits = digits.len().min(OLC_PAIR_CODE_LENGTH);
    let mut place = OLC_PAIR_FIRST_PLACE;
    let pair_values = digits
        .get(..pair_digits)
        .expect("pair digits fit the fixed OLC buffer");
    let mut pairs = pair_values.as_chunks::<2>().0.iter().peekable();
    while let Some(pair) = pairs.next() {
        let [latitude_digit, longitude_digit] = pair;
        normal_lat = normal_lat.wrapping_add(latitude_digit.wrapping_mul(place));
        normal_lng = normal_lng.wrapping_add(longitude_digit.wrapping_mul(place));
        if pairs.peek().is_some() {
            place /= 20;
        }
    }
    let (lat_precision, lng_precision) = if digits.len() > OLC_PAIR_CODE_LENGTH {
        let mut row_place = OLC_GRID_ROWS.pow(4);
        let mut column_place = OLC_GRID_COLUMNS.pow(4);
        let grid_digits = digits
            .get(OLC_PAIR_CODE_LENGTH..)
            .expect("grid digits follow the fixed pair prefix");
        for (offset, &digit) in grid_digits.iter().enumerate() {
            grid_lat = grid_lat.wrapping_add((digit / OLC_GRID_COLUMNS).wrapping_mul(row_place));
            grid_lng = grid_lng.wrapping_add((digit % OLC_GRID_COLUMNS).wrapping_mul(column_place));
            if offset < grid_digits.len().wrapping_sub(1) {
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
        Err(errors::parse_error(
            format!("{code:?} is not a full plus code"),
            crate::error::ParseFormat::PlusCode,
        ))
    }
}

/// Validate a plus-code digit count: even values from 2 to 10, then 11-15.
pub(super) fn validate_pluscode_length(length: i64) -> PyResult<usize> {
    let requested_length = length;
    let Some(length) = usize::try_from(length)
        .ok()
        .filter(|&length| (OLC_MIN_DIGITS..=OLC_MAX_DIGITS).contains(&length))
    else {
        return Err(GeometryError::new_err(format!(
            "pluscode length must be between {OLC_MIN_DIGITS} and {OLC_MAX_DIGITS}, got {requested_length}"
        )));
    };
    if length < OLC_PAIR_CODE_LENGTH && length % 2 == 1 {
        return Err(GeometryError::new_err(format!(
            "pluscode length below 10 must be even, got {length}"
        )));
    }
    Ok(length)
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
    for level in (0..=OLC_PAIR_RESOLUTIONS.len().wrapping_sub(3)).rev() {
        // 0.3 instead of 0.5 leaves recovery a safety margin.
        if range
            < OLC_PAIR_RESOLUTIONS
                .get(level)
                .copied()
                .expect("shortening level is a supported pair resolution")
                * 0.3
        {
            let removed_digits = level.wrapping_add(1).wrapping_mul(2);
            return code
                .get(removed_digits..)
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
        return Err(errors::parse_error(
            format!("{code:?} is not a short plus code"),
            crate::error::ParseFormat::PlusCode,
        ));
    }
    let latitude = latitude.clamp(-90.0, 90.0);
    let longitude = olc_normalize_longitude(longitude);
    let code = code.to_ascii_uppercase();
    let padding = OLC_SEPARATOR_POSITION.wrapping_sub(
        code.find(char::from(OLC_SEPARATOR))
            .expect("short codes carry a separator"),
    );
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
