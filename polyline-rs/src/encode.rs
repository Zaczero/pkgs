use pyo3::prelude::*;

use crate::constants::{ASCII_OFFSET, CHUNK_BITS, CHUNK_MASK, CONTINUATION_BIT, precision_scale};
use crate::errors::PolylineError;
use crate::zigzag::zigzag_encode;

fn extract_coord_pair(coord: &Bound<'_, PyAny>, index: usize) -> PyResult<(f64, f64)> {
    let mut it = coord.try_iter()?;

    let first = it
        .next()
        .ok_or(PolylineError::CoordinateMustContain2Values { index })??;
    let second = it
        .next()
        .ok_or(PolylineError::CoordinateMustContain2Values { index })??;

    Ok((first.extract()?, second.extract()?))
}

fn encode_value(out: &mut Vec<u8>, delta: i32) {
    let mut value = zigzag_encode(delta);
    while value >= u32::from(CONTINUATION_BIT) {
        let chunk = ((value as u8) & CHUNK_MASK) | CONTINUATION_BIT;
        out.push(chunk.wrapping_add(ASCII_OFFSET));
        value >>= CHUNK_BITS;
    }
    out.push((value as u8).wrapping_add(ASCII_OFFSET));
}

fn encode_coordinate(
    out: &mut Vec<u8>,
    coordinate_value: f64,
    scale: f64,
    previous: i32,
    index: usize,
    coordinate: &'static str,
) -> Result<i32, PolylineError> {
    let scaled = coordinate_value * scale;
    let rounded = scaled.round();
    if !rounded.is_finite() || rounded < f64::from(i32::MIN) || rounded > f64::from(i32::MAX) {
        return Err(PolylineError::CoordinateOutOfRange { index, coordinate });
    }

    let current = rounded as i32;
    // The difference of two i32 coordinates fits in i64; admission below
    // rejects it if the wire's signed 32-bit delta cannot represent it.
    let delta = i64::from(current).wrapping_sub(i64::from(previous));
    let delta = i32::try_from(delta)
        .map_err(|_| PolylineError::CoordinateDeltaOutOfRange { index, coordinate })?;
    encode_value(out, delta);
    Ok(current)
}

pub(crate) fn encode<const LATLON: bool>(
    coordinates: &Bound<'_, PyAny>,
    precision: i32,
) -> PyResult<String> {
    let scale =
        precision_scale(precision).ok_or(PolylineError::PrecisionOutOfRange { precision })?;

    let capacity = coordinates.len().map_or(0, |n| n.saturating_mul(12));
    let mut out = Vec::with_capacity(capacity);
    let mut last_lat = 0;
    let mut last_lon = 0;

    for (index, coord) in coordinates.try_iter()?.enumerate() {
        let coord = coord?;
        let (first, second) = extract_coord_pair(&coord, index)?;
        let (lat_f, lon_f) = if LATLON {
            (first, second)
        } else {
            (second, first)
        };

        // `f64::round` preserves this codec's half-away-from-zero rule.
        last_lat = encode_coordinate(&mut out, lat_f, scale.scale, last_lat, index, "latitude")?;
        last_lon = encode_coordinate(&mut out, lon_f, scale.scale, last_lon, index, "longitude")?;
    }

    // Safety: all bytes are ASCII.
    Ok(unsafe { String::from_utf8_unchecked(out) })
}
