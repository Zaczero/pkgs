use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyListMethods};

use crate::constants::{ASCII_OFFSET, CHUNK_BITS, CHUNK_MASK, CONTINUATION_BIT};
use crate::errors::Error;
use crate::zigzag::zigzag_decode;

const MAX_CHUNK: u8 = CHUNK_MASK | CONTINUATION_BIT;
const MAX_SHIFT: u32 = u32::BITS - CHUNK_BITS;

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
        let Some(&byte) = cursor.bytes.get(cursor.index) else {
            return Err(Error::UnterminatedPolylineValue { index: start });
        };
        cursor.index += 1;

        let Some(b) = byte.checked_sub(ASCII_OFFSET).filter(|&b| b <= MAX_CHUNK) else {
            return Err(Error::InvalidPolylineByte {
                index: cursor.index - 1,
                byte,
            });
        };

        if shift > MAX_SHIFT {
            return Err(Error::PolylineValueOverflow { index: start });
        }

        let chunk = u32::from(b & CHUNK_MASK);
        if shift == MAX_SHIFT && chunk > 0b11 {
            return Err(Error::PolylineValueOverflow { index: start });
        }

        value |= chunk << shift;
        shift += CHUNK_BITS;

        if (b & CONTINUATION_BIT) == 0 {
            break;
        }
    }

    Ok(zigzag_decode(value))
}

pub fn decode<'py, const LATLON: bool>(
    py: Python<'py>,
    line: &str,
    precision: i32,
) -> PyResult<Bound<'py, PyList>> {
    let inv_scale = 10_f64.powi(-precision);

    let mut cursor = DecodeCursor::new(line.as_bytes());
    let mut last_lat = 0_i32;
    let mut last_lon = 0_i32;
    let out = PyList::empty(py);

    while !cursor.is_empty() {
        let coord_index = cursor.index();
        let dlat =
            decode_next_value(&mut cursor).map_err(|err| PyValueError::new_err(err.message()))?;
        let dlon = decode_next_value(&mut cursor).map_err(|err| match err {
            Error::UnterminatedPolylineValue { .. } => PyValueError::new_err(
                Error::IncompletePolylineCoordinate { index: coord_index }.message(),
            ),
            err => PyValueError::new_err(err.message()),
        })?;
        last_lat = last_lat.checked_add(dlat).ok_or_else(|| {
            PyValueError::new_err(Error::CoordinateOverflow { index: coord_index }.message())
        })?;
        last_lon = last_lon.checked_add(dlon).ok_or_else(|| {
            PyValueError::new_err(Error::CoordinateOverflow { index: coord_index }.message())
        })?;

        let lat = f64::from(last_lat) * inv_scale;
        let lon = f64::from(last_lon) * inv_scale;
        out.append(if LATLON { (lat, lon) } else { (lon, lat) })?;
    }

    Ok(out)
}
