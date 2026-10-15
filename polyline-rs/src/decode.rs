use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::constants::{ASCII_OFFSET, CHUNK_BITS, CHUNK_MASK, CONTINUATION_BIT, precision_scale};
use crate::errors::PolylineError;
use crate::zigzag::zigzag_decode;

const MAX_CHUNK: u8 = CHUNK_MASK | CONTINUATION_BIT;
const MAX_SHIFT: u32 = u32::BITS - 2;

#[derive(Debug)]
enum DecodeValueError {
    InvalidPolylineByte { index: usize, byte: u8 },
    UnterminatedValue { index: usize },
    PolylineValueOverflow { index: usize },
}

impl DecodeValueError {
    fn into_pyerr(self) -> PyErr {
        let error = match self {
            Self::InvalidPolylineByte { index, byte } => {
                PolylineError::InvalidPolylineByte { index, byte }
            },
            Self::UnterminatedValue { index } => PolylineError::UnterminatedPolylineValue { index },
            Self::PolylineValueOverflow { index } => PolylineError::PolylineValueOverflow { index },
        };
        error.into_pyerr()
    }
}

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

fn decode_next_value(cursor: &mut DecodeCursor<'_>) -> Result<i32, DecodeValueError> {
    let start = cursor.index;
    let mut value = 0_u32;
    let mut shift = 0_u32;

    loop {
        let index = cursor.index;
        let Some(&byte) = cursor.bytes.get(index) else {
            return Err(DecodeValueError::UnterminatedValue { index: start });
        };
        cursor.index = index.wrapping_add(1);

        let b = byte.wrapping_sub(ASCII_OFFSET);
        if b > MAX_CHUNK {
            return Err(DecodeValueError::InvalidPolylineByte { index, byte });
        }

        if shift == MAX_SHIFT && b > 0b11 {
            return Err(DecodeValueError::PolylineValueOverflow { index: start });
        }

        value |= u32::from(b & CHUNK_MASK) << shift;

        if (b & CONTINUATION_BIT) == 0 {
            break;
        }

        shift = shift.wrapping_add(CHUNK_BITS);
    }

    Ok(zigzag_decode(value))
}

pub(crate) fn decode<'py, const LATLON: bool>(
    py: Python<'py>,
    line: &str,
    precision: i32,
) -> PyResult<Bound<'py, PyList>> {
    let scale = precision_scale(precision)
        .ok_or_else(|| PolylineError::PrecisionOutOfRange { precision }.into_pyerr())?;

    let mut cursor = DecodeCursor::new(line.as_bytes());
    let mut last_lat = 0_i32;
    let mut last_lon = 0_i32;
    let out = PyList::empty(py);

    while !cursor.is_empty() {
        let coord_index = cursor.index();
        let dlat = decode_next_value(&mut cursor).map_err(DecodeValueError::into_pyerr)?;
        let dlon = decode_next_value(&mut cursor).map_err(|err| match err {
            DecodeValueError::UnterminatedValue { .. } => {
                PolylineError::IncompletePolylineCoordinate { index: coord_index }.into_pyerr()
            },
            err @ (DecodeValueError::InvalidPolylineByte { .. }
            | DecodeValueError::PolylineValueOverflow { .. }) => err.into_pyerr(),
        })?;
        last_lat = last_lat
            .checked_add(dlat)
            .ok_or_else(|| PolylineError::CoordinateOverflow { index: coord_index }.into_pyerr())?;
        last_lon = last_lon
            .checked_add(dlon)
            .ok_or_else(|| PolylineError::CoordinateOverflow { index: coord_index }.into_pyerr())?;

        let lat = f64::from(last_lat) * scale.inverse_scale;
        let lon = f64::from(last_lon) * scale.inverse_scale;
        out.append(if LATLON { (lat, lon) } else { (lon, lat) })?;
    }

    Ok(out)
}
