#![feature(likely_unlikely)]

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyString;
use std::hint::unlikely;

// 64 chars to encode 6 bits
const CHARSET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_~";

const X_SCALE: f64 = ((u32::MAX as f64) + 1.0) / 360.0;
const Y_SCALE: f64 = ((u32::MAX as f64) + 1.0) / 180.0;
const X_SCALE_INV: f64 = 360.0 / (u32::MAX as f64 + 1.0);
const Y_SCALE_INV: f64 = 180.0 / (u32::MAX as f64 + 1.0);

const DECODE_INVALID: u8 = 0xFF;
const DECODE_OFFSET: u8 = 0xFE;

// LUT mapping ASCII byte -> packed (x_chunk | (y_chunk << 3)).
// Special values:
// - DECODE_INVALID: not a valid shortlink character
// - DECODE_OFFSET: '-' or '=' offset (legacy) character
const fn build_decode_lut() -> [u8; 256] {
    let mut lut = [DECODE_INVALID; 256];
    lut[b'-' as usize] = DECODE_OFFSET;
    lut[b'=' as usize] = DECODE_OFFSET;

    let mut i = 0;
    while i < 64 {
        let t = i as u32;
        let x_chunk =
            (((t >> 1) & 1) | (((t >> 3) & 1) << 1) | (((t >> 5) & 1) << 2)) as u8;
        let y_chunk = ((t & 1) | (((t >> 2) & 1) << 1) | (((t >> 4) & 1) << 2)) as u8;
        let packed = x_chunk | (y_chunk << 3);

        lut[CHARSET[i] as usize] = packed;
        i += 1;
    }

    // Resolve '@' for backwards compatibility.
    lut[b'@' as usize] = lut[b'~' as usize];
    lut
}

const DECODE_LUT: [u8; 256] = build_decode_lut();

fn interleave_bits(x: u32, y: u32) -> u64 {
    fn part1by1(n: u32) -> u64 {
        let mut x = n as u64;
        x = (x | (x << 16)) & 0x0000_FFFF_0000_FFFF;
        x = (x | (x << 8)) & 0x00FF_00FF_00FF_00FF;
        x = (x | (x << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
        x = (x | (x << 2)) & 0x3333_3333_3333_3333;
        x = (x | (x << 1)) & 0x5555_5555_5555_5555;
        x
    }

    (part1by1(x) << 1) | part1by1(y)
}

#[pyfunction]
fn shortlink_encode(py: Python<'_>, lon: f64, lat: f64, zoom: i8) -> PyResult<Py<PyString>> {
    if unlikely(!(lat >= -90.0 && lat <= 90.0)) {
        return Err(PyValueError::new_err(format!(
            "Invalid latitude: must be between -90 and 90, got {lat}"
        )));
    }
    if unlikely(zoom < 0 || zoom > 22) {
        return Err(PyValueError::new_err(format!(
            "Invalid zoom: must be between 0 and 22, got {zoom}"
        )));
    }

    let x: u32 = ((lon + 180.0).rem_euclid(360.0) * X_SCALE) as u32;
    let y: u32 = ((lat + 90.0) * Y_SCALE) as u32;

    let c: u64 = interleave_bits(x, y);

    let n = zoom as u8 + 8;
    let r = (n % 3) as usize;
    let d = ((n + 2) / 3) as usize; // ceil((zoom+8)/3)
    let mut buf = [0u8; 12]; // max length for zoom<=22
    for i in 0..d {
        let shift = 58 - (i as u32 * 6);
        let digit = ((c >> shift) & 0x3F) as usize;
        buf[i] = CHARSET[digit];
    }
    for i in 0..r {
        buf[d + i] = b'-';
    }
    let len = d + r;

    // Safety: all bytes come from CHARSET or '-', so they are valid UTF-8 ASCII.
    let view = unsafe { std::str::from_utf8_unchecked(&buf[..len]) };
    Ok(PyString::new(py, view).into())
}

#[pyfunction]
fn shortlink_decode(s: &str) -> PyResult<(f64, f64, u8)> {
    let mut x: u32 = 0;
    let mut y: u32 = 0;
    let mut z: u8 = 0;
    let mut z_offset: i8 = 0;

    for c in s.bytes() {
        if unlikely((c & 0x80) != 0) {
            return Err(PyValueError::new_err(
                "Invalid shortlink: expected ASCII string",
            ));
        }

        let packed = DECODE_LUT[c as usize];
        if packed == DECODE_OFFSET {
            z_offset -= 1;
            if unlikely(z_offset <= -3) {
                return Err(PyValueError::new_err(
                    "Invalid shortlink: too many offset characters",
                ));
            }
            continue;
        }
        if unlikely(packed == DECODE_INVALID) {
            return Err(PyValueError::new_err(format!(
                "Invalid shortlink: bad character '{}'",
                c as char
            )));
        }

        x = (x << 3) | (packed & 0b111) as u32;
        y = (y << 3) | (packed >> 3) as u32;
        z += 3;
        if unlikely(z > 32) {
            return Err(PyValueError::new_err("Invalid shortlink: too long"));
        }
    }

    if unlikely(z == 0) {
        return Err(PyValueError::new_err("Invalid shortlink: too short"));
    }

    if unlikely(z < 8) {
        return Err(PyValueError::new_err("Invalid shortlink: too short"));
    }

    let mut zoom = z - 8;
    let adjust = z_offset.rem_euclid(3) as u8;
    if unlikely(zoom < adjust) {
        return Err(PyValueError::new_err("Invalid shortlink: malformed zoom"));
    }
    zoom -= adjust;

    let shift = (32 - z) as u32;
    x <<= shift;
    y <<= shift;

    Ok((
        (x as f64 * X_SCALE_INV) - 180.0,
        (y as f64 * Y_SCALE_INV) - 90.0,
        zoom,
    ))
}

#[pymodule(gil_used = false)]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(shortlink_encode, m)?)?;
    m.add_function(wrap_pyfunction!(shortlink_decode, m)?)?;
    Ok(())
}
