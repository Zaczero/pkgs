//! Signed wire-varint admission and checked coordinate reconstruction.

use std::iter::Enumerate;
use std::str::Bytes;

use crate::constants::{ASCII_OFFSET, CHUNK_BITS, CHUNK_MASK, CONTINUATION_BIT, precision_scale};
use crate::errors::PolylineError;
use crate::zigzag::zigzag_decode;

const MAX_CHUNK: u8 = CHUNK_MASK | CONTINUATION_BIT;
const MAX_SHIFT: u32 = u32::BITS - 2;

fn decode_next_value(cursor: &mut Enumerate<Bytes<'_>>) -> Result<i32, PolylineError> {
    let start = cursor.next_index();
    let mut value = 0_u32;

    for shift in (0..u32::BITS).step_by(CHUNK_BITS as usize) {
        let Some((index, byte)) = cursor.next() else {
            return Err(PolylineError::UnterminatedPolylineValue { index: start });
        };

        let b = byte
            .checked_sub(ASCII_OFFSET)
            .filter(|&chunk| chunk <= MAX_CHUNK)
            .ok_or(PolylineError::InvalidPolylineByte { index, byte })?;

        if shift == MAX_SHIFT && b > 0b11 {
            return Err(PolylineError::PolylineValueOverflow { index: start });
        }

        value |= u32::from(b & CHUNK_MASK) << shift;

        if (b & CONTINUATION_BIT) == 0 {
            return Ok(zigzag_decode(value));
        }
    }

    Err(PolylineError::PolylineValueOverflow { index: start })
}

pub(crate) fn decode<const LATLON: bool>(
    line: &str,
    precision: i32,
) -> Result<Vec<(f64, f64)>, PolylineError> {
    let scale =
        precision_scale(precision).ok_or(PolylineError::PrecisionOutOfRange { precision })?;

    let mut cursor = line.trim().bytes().enumerate();
    let mut last_lat = 0_i32;
    let mut last_lon = 0_i32;
    let mut out = Vec::new();

    while cursor.len() != 0 {
        let coord_index = cursor.next_index();
        let dlat = decode_next_value(&mut cursor)?;
        let dlon = decode_next_value(&mut cursor).map_err(|error| {
            if let PolylineError::UnterminatedPolylineValue { .. } = error {
                PolylineError::IncompletePolylineCoordinate { index: coord_index }
            } else {
                error
            }
        })?;
        last_lat = last_lat
            .checked_add(dlat)
            .ok_or(PolylineError::CoordinateOverflow { index: coord_index })?;
        last_lon = last_lon
            .checked_add(dlon)
            .ok_or(PolylineError::CoordinateOverflow { index: coord_index })?;

        let lat = f64::from(last_lat) * scale.inverse_scale;
        let lon = f64::from(last_lon) * scale.inverse_scale;
        out.push(if LATLON { (lat, lon) } else { (lon, lat) });
    }

    Ok(out)
}
