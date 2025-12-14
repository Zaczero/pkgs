#![feature(likely_unlikely)]

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyListMethods};
use std::hint::{likely, unlikely};

const CHAR_OFFSET: u8 = 63;

fn zigzag_encode(value: i32) -> u32 {
    ((value << 1) ^ (value >> 31)) as u32
}

fn zigzag_decode(value: i32) -> i32 {
    (value >> 1) ^ (-(value & 1))
}

fn extract_coord_pair(coord: &Bound<'_, PyAny>) -> PyResult<(f64, f64)> {
    let mut it = coord.try_iter()?;

    let first = it
        .next()
        .ok_or_else(|| PyValueError::new_err("coordinate must contain 2 values"))??;
    let second = it
        .next()
        .ok_or_else(|| PyValueError::new_err("coordinate must contain 2 values"))??;

    Ok((first.extract()?, second.extract()?))
}

fn encode_value(out: &mut Vec<u8>, delta: i32) {
    let mut value = zigzag_encode(delta);
    loop {
        let mut chunk = (value & 0b1_1111) as u8;
        value >>= 5;
        if value != 0 {
            chunk |= 0x20;
        }
        out.push(chunk + CHAR_OFFSET);
        if value == 0 {
            break;
        }
    }
}

fn encode_impl<const LATLON: bool>(
    coordinates: &Bound<'_, PyAny>,
    precision: i32,
) -> PyResult<String> {
    let scale = 10_f64.powi(precision);
    let mut out = match coordinates.len() {
        Ok(n) => Vec::with_capacity(6 * 2 * n),
        Err(_) => Vec::new(),
    };
    let mut last_lat = 0_i32;
    let mut last_lon = 0_i32;

    for coord in coordinates.try_iter()? {
        let coord: Bound<'_, PyAny> = coord?;
        let (first, second) = extract_coord_pair(&coord)?;
        let (lat_f, lon_f) = if LATLON {
            (first, second)
        } else {
            (second, first)
        };

        let lat = (lat_f * scale) as i32;
        encode_value(&mut out, lat - last_lat);
        last_lat = lat;

        let lon = (lon_f * scale) as i32;
        encode_value(&mut out, lon - last_lon);
        last_lon = lon;
    }

    // Safety: output is always ASCII bytes in the range [63..=126].
    Ok(unsafe { String::from_utf8_unchecked(out) })
}

fn decode_next_value(input: &mut &[u8]) -> Option<i32> {
    let mut value: i32 = 0;
    let mut shift: u32 = 0;

    loop {
        let (&b0, rest) = input.split_first()?;
        *input = rest;

        if unlikely(b0 < CHAR_OFFSET) {
            return None;
        }
        let b = b0 - CHAR_OFFSET;
        value |= ((b & 0b1_1111) as i32) << shift;

        if likely((b & 0x20) == 0) {
            break;
        }
        shift += 5;
        if unlikely(shift > 30) {
            return None;
        }
    }

    Some(zigzag_decode(value))
}

fn decode_impl<'py, const LATLON: bool>(
    py: Python<'py>,
    line: &str,
    precision: i32,
) -> PyResult<Bound<'py, PyList>> {
    let inv_scale = 10_f64.powi(-precision);
    let bytes = line.as_bytes();
    let mut input = bytes;
    let mut last_lat = 0_i32;
    let mut last_lon = 0_i32;
    let out = PyList::empty(py);

    while let (Some(dlat), Some(dlon)) =
        (decode_next_value(&mut input), decode_next_value(&mut input))
    {
        last_lat += dlat;
        last_lon += dlon;

        let lat = last_lat as f64 * inv_scale;
        let lon = last_lon as f64 * inv_scale;
        out.append(if LATLON { (lat, lon) } else { (lon, lat) })?;
    }

    Ok(out)
}

#[pyfunction]
#[pyo3(signature = (coordinates, precision = 5))]
fn encode_lonlat(coordinates: &Bound<'_, PyAny>, precision: i32) -> PyResult<String> {
    encode_impl::<false>(coordinates, precision)
}

#[pyfunction]
#[pyo3(signature = (coordinates, precision = 5))]
fn encode_latlon(coordinates: &Bound<'_, PyAny>, precision: i32) -> PyResult<String> {
    encode_impl::<true>(coordinates, precision)
}

#[pyfunction]
#[pyo3(signature = (polyline, precision = 5))]
fn decode_lonlat<'py>(
    py: Python<'py>,
    polyline: &str,
    precision: i32,
) -> PyResult<Bound<'py, PyList>> {
    decode_impl::<false>(py, polyline, precision)
}

#[pyfunction]
#[pyo3(signature = (polyline, precision = 5))]
fn decode_latlon<'py>(
    py: Python<'py>,
    polyline: &str,
    precision: i32,
) -> PyResult<Bound<'py, PyList>> {
    decode_impl::<true>(py, polyline, precision)
}

#[pymodule(gil_used = false)]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(encode_lonlat, m)?)?;
    m.add_function(wrap_pyfunction!(encode_latlon, m)?)?;
    m.add_function(wrap_pyfunction!(decode_lonlat, m)?)?;
    m.add_function(wrap_pyfunction!(decode_latlon, m)?)?;
    Ok(())
}
