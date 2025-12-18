use pyo3::prelude::*;
use pyo3::types::{PyList, PyListMethods};

use crate::constants::{ASCII_OFFSET, CHUNK_BITS, CHUNK_MASK, CONTINUATION_BIT};
use crate::zigzag::zigzag_decode;

fn decode_next_value(cursor: &mut &[u8]) -> Option<i32> {
    let mut value = 0;
    let mut shift = 0;

    loop {
        let (&b0, rest) = cursor.split_first()?;
        *cursor = rest;

        let b = b0 - ASCII_OFFSET;

        value |= u32::from(b & CHUNK_MASK) << shift;
        shift += CHUNK_BITS;

        if (b & CONTINUATION_BIT) == 0 {
            break;
        }
    }

    Some(zigzag_decode(value))
}

pub(crate) fn decode<'py, const LATLON: bool>(
    py: Python<'py>,
    line: &str,
    precision: i32,
) -> PyResult<Bound<'py, PyList>> {
    let inv_scale = 10_f64.powi(-precision);

    let mut cursor = line.as_bytes();
    let mut last_lat = 0;
    let mut last_lon = 0;
    let out = PyList::empty(py);

    while let Some(dlat) = decode_next_value(&mut cursor) {
        let Some(dlon) = decode_next_value(&mut cursor) else {
            break;
        };
        last_lat += dlat;
        last_lon += dlon;

        let lat = last_lat as f64 * inv_scale;
        let lon = last_lon as f64 * inv_scale;
        out.append(if LATLON { (lat, lon) } else { (lon, lat) })?;
    }

    Ok(out)
}
