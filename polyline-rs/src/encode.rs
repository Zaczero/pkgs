use pyo3::prelude::*;

use crate::constants::{
    ASCII_OFFSET, CHUNK_BITS, CHUNK_MASK, CONTINUATION_BIT, scale_for_precision,
};
use crate::errors::Error;
use crate::zigzag::zigzag_encode;

fn extract_coord_pair(coord: &Bound<'_, PyAny>, index: usize) -> PyResult<(f64, f64)> {
    let mut it = coord.try_iter()?;

    let first = it
        .next()
        .ok_or_else(|| Error::CoordinateMustContain2Values { index }.into_pyerr())??;
    let second = it
        .next()
        .ok_or_else(|| Error::CoordinateMustContain2Values { index }.into_pyerr())??;

    Ok((first.extract()?, second.extract()?))
}

fn encode_value(out: &mut Vec<u8>, delta: i32) {
    let mut value = zigzag_encode(delta);
    while value >= u32::from(CONTINUATION_BIT) {
        let chunk = ((value as u8) & CHUNK_MASK) | CONTINUATION_BIT;
        out.push(chunk + ASCII_OFFSET);
        value >>= CHUNK_BITS;
    }
    out.push(value as u8 + ASCII_OFFSET);
}

pub fn encode<const LATLON: bool>(
    coordinates: &Bound<'_, PyAny>,
    precision: i32,
) -> PyResult<String> {
    let scale = scale_for_precision(precision);

    let capacity = coordinates.len().map_or(0, |n| n * 12);
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

        let lat = (lat_f * scale) as i64;
        let delta_lat = (lat - last_lat) as i32;
        encode_value(&mut out, delta_lat);
        last_lat = lat;

        let lon = (lon_f * scale) as i64;
        let delta_lon = (lon - last_lon) as i32;
        encode_value(&mut out, delta_lon);
        last_lon = lon;
    }

    // Safety: all bytes are ASCII.
    Ok(unsafe { String::from_utf8_unchecked(out) })
}
