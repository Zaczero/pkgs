use crate::constants::{ASCII_OFFSET, CHUNK_BITS, CHUNK_MASK, CONTINUATION_BIT, precision_scale};
use crate::errors::PolylineError;
use crate::zigzag::zigzag_decode;

const MAX_CHUNK: u8 = CHUNK_MASK | CONTINUATION_BIT;
const MAX_SHIFT: u32 = u32::BITS - 2;

struct DecodeCursor<'a> {
    bytes: &'a [u8],
    index: usize,
}

impl<'a> DecodeCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, index: 0 }
    }

    const fn is_empty(&self) -> bool {
        self.index == self.bytes.len()
    }

    const fn index(&self) -> usize {
        self.index
    }
}

fn decode_next_value(cursor: &mut DecodeCursor<'_>) -> Result<i32, PolylineError> {
    let start = cursor.index;
    let mut value = 0_u32;
    let mut shift = 0_u32;

    loop {
        let index = cursor.index;
        let Some(&byte) = cursor.bytes.get(index) else {
            return Err(PolylineError::UnterminatedPolylineValue { index: start });
        };
        cursor.index = index.wrapping_add(1);

        let b = byte.wrapping_sub(ASCII_OFFSET);
        if b > MAX_CHUNK {
            return Err(PolylineError::InvalidPolylineByte { index, byte });
        }

        if shift == MAX_SHIFT && b > 0b11 {
            return Err(PolylineError::PolylineValueOverflow { index: start });
        }

        value |= u32::from(b & CHUNK_MASK) << shift;

        if (b & CONTINUATION_BIT) == 0 {
            break;
        }

        shift = shift.wrapping_add(CHUNK_BITS);
    }

    Ok(zigzag_decode(value))
}

pub(crate) fn decode<const LATLON: bool>(
    line: &str,
    precision: i32,
) -> Result<Vec<(f64, f64)>, PolylineError> {
    let scale =
        precision_scale(precision).ok_or(PolylineError::PrecisionOutOfRange { precision })?;

    let mut cursor = DecodeCursor::new(line.trim().as_bytes());
    let mut last_lat = 0_i32;
    let mut last_lon = 0_i32;
    let mut out = Vec::new();

    while !cursor.is_empty() {
        let coord_index = cursor.index();
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
