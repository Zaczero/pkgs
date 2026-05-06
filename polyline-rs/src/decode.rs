use pyo3::prelude::*;
use pyo3::types::{PyList, PyListMethods};

use crate::constants::{
    ASCII_OFFSET, CHUNK_BITS, CHUNK_MASK, CONTINUATION_BIT, inv_scale_for_precision,
};
use crate::errors::Error;
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

fn decode_next_value(cursor: &mut DecodeCursor<'_>) -> Result<i32, Error> {
    let start = cursor.index;
    let mut value = 0_u32;
    let mut shift = 0;

    loop {
        if cursor.index == cursor.bytes.len() {
            return Err(Error::UnterminatedPolylineValue { index: start });
        }
        let byte = cursor.bytes[cursor.index];
        cursor.index += 1;

        let b = byte.wrapping_sub(ASCII_OFFSET);
        if b > MAX_CHUNK {
            return Err(Error::InvalidPolylineByte {
                index: cursor.index - 1,
                byte,
            });
        }

        if shift == MAX_SHIFT && b > 0b11 {
            return Err(Error::PolylineValueOverflow { index: start });
        }

        value |= u32::from(b & CHUNK_MASK) << shift;

        if (b & CONTINUATION_BIT) == 0 {
            break;
        }

        shift += CHUNK_BITS;
    }

    Ok(zigzag_decode(value))
}

pub fn decode<'py, const LATLON: bool>(
    py: Python<'py>,
    line: &str,
    precision: i32,
) -> PyResult<Bound<'py, PyList>> {
    let inv_scale = inv_scale_for_precision(precision);

    let mut cursor = DecodeCursor::new(line.as_bytes());
    let mut last_lat = 0_i32;
    let mut last_lon = 0_i32;
    let out = PyList::empty(py);

    while !cursor.is_empty() {
        let coord_index = cursor.index();
        let dlat = decode_next_value(&mut cursor).map_err(Error::into_pyerr)?;
        let dlon = decode_next_value(&mut cursor).map_err(|err| match err {
            Error::UnterminatedPolylineValue { .. } => {
                Error::IncompletePolylineCoordinate { index: coord_index }.into_pyerr()
            },
            err => err.into_pyerr(),
        })?;
        last_lat = last_lat
            .checked_add(dlat)
            .ok_or_else(|| Error::CoordinateOverflow { index: coord_index }.into_pyerr())?;
        last_lon = last_lon
            .checked_add(dlon)
            .ok_or_else(|| Error::CoordinateOverflow { index: coord_index }.into_pyerr())?;

        let lat = f64::from(last_lat) * inv_scale;
        let lon = f64::from(last_lon) * inv_scale;
        out.append(if LATLON { (lat, lon) } else { (lon, lat) })?;
    }

    Ok(out)
}
