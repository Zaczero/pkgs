#![feature(likely_unlikely)]

mod algorithm;
mod base32;
mod errors;
mod secret;
mod time;
mod totp;

use std::hint::unlikely;
use std::num::NonZeroU32;
use std::str;

use pyo3::prelude::*;
use pyo3::types::{PyInt, PyString, PyStringMethods};

use crate::algorithm::parse_algorithm;
use crate::errors::{Error, validate_digits};
use crate::secret::parse_secret_from_py;
use crate::time::{resolve_counter, time_window_from_time};
use crate::totp::{totp_code, verify};

const MODULUS_BY_DIGITS: [NonZeroU32; 10] = {
    let mut table = [NonZeroU32::MIN; 10];
    let mut value = 1;
    let mut digits = 0;
    while digits < table.len() {
        table[digits] = match NonZeroU32::new(value) {
            Some(value) => value,
            None => panic!("decimal modulus must be non-zero"),
        };
        digits += 1;
        if digits < table.len() {
            value *= 10;
        }
    }
    table
};

const fn modulus_for_digits(digits: u8) -> NonZeroU32 {
    MODULUS_BY_DIGITS[digits as usize]
}

fn format_code_py(py: Python<'_>, mut code: u32, digits: u8) -> Py<PyString> {
    let mut buf = [b'0'; 9];

    for c in buf.iter_mut().rev().take(digits as usize) {
        *c = b'0' + (code % 10) as u8;
        code /= 10;
    }

    // Safety: all bytes are ASCII digits.
    let view = unsafe { str::from_utf8_unchecked(&buf[buf.len() - digits as usize..]) };
    PyString::new(py, view).into()
}

fn parse_code_str(code: &str, digits: u8) -> Option<u32> {
    let mut value = 0;
    let mut seen_digits = 0;

    for &b in code.as_bytes() {
        if !b.is_ascii_digit() {
            continue;
        }
        if unlikely(seen_digits == digits) {
            return None;
        }

        value = value * 10 + u32::from(b - b'0');
        seen_digits += 1;
    }

    (seen_digits == digits).then_some(value)
}

fn parse_code_py(code: &Bound<'_, PyAny>, digits: u8, modulus: NonZeroU32) -> Option<u32> {
    if let Ok(value) = code.cast::<PyString>() {
        if let Ok(s) = value.to_str() {
            return parse_code_str(s, digits);
        }
        return None;
    }

    if let Ok(value) = code.cast::<PyInt>() {
        let Ok(value) = value.extract::<u32>() else {
            return None;
        };
        return (value < modulus.get()).then_some(value);
    }

    None
}

#[pyfunction]
#[pyo3(signature = (time = None, *, step_seconds = 30, t0 = 0))]
fn totp_time_window(time: Option<f64>, step_seconds: i64, t0: i64) -> PyResult<i64> {
    time_window_from_time(time, step_seconds, t0).map_err(Error::into_pyerr)
}

#[pyfunction]
#[pyo3(signature = (secret, *, digits = 6, algorithm = "sha1", time = None, time_window = None, step_seconds = 30, t0 = 0))]
fn totp_generate(
    py: Python<'_>,
    secret: &Bound<'_, PyAny>,
    digits: u8,
    algorithm: &str,
    time: Option<f64>,
    time_window: Option<i64>,
    step_seconds: i64,
    t0: i64,
) -> PyResult<Py<PyString>> {
    validate_digits(digits).map_err(Error::into_pyerr)?;
    let algorithm = parse_algorithm(algorithm).map_err(Error::into_pyerr)?;
    let counter =
        resolve_counter(time, time_window, step_seconds, t0).map_err(Error::into_pyerr)?;

    let secret = parse_secret_from_py(secret)?;
    let modulus = modulus_for_digits(digits);

    let code = totp_code(secret.as_ref(), counter, modulus, algorithm);

    Ok(format_code_py(py, code, digits))
}

#[pyfunction]
#[pyo3(signature = (secret, code, *, digits = 6, algorithm = "sha1", time = None, time_window = None, step_seconds = 30, t0 = 0, window = 1))]
fn totp_verify(
    secret: &Bound<'_, PyAny>,
    code: &Bound<'_, PyAny>,
    digits: u8,
    algorithm: &str,
    time: Option<f64>,
    time_window: Option<i64>,
    step_seconds: i64,
    t0: i64,
    window: u8,
) -> PyResult<bool> {
    validate_digits(digits).map_err(Error::into_pyerr)?;
    let algorithm = parse_algorithm(algorithm).map_err(Error::into_pyerr)?;
    let counter =
        resolve_counter(time, time_window, step_seconds, t0).map_err(Error::into_pyerr)?;

    let modulus = modulus_for_digits(digits);
    let Some(code) = parse_code_py(code, digits, modulus) else {
        return Ok(false);
    };

    let secret = parse_secret_from_py(secret)?;

    Ok(verify(
        secret.as_ref(),
        counter,
        window,
        modulus,
        code,
        algorithm,
    ))
}

#[pymodule]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(totp_time_window, m)?)?;
    m.add_function(wrap_pyfunction!(totp_generate, m)?)?;
    m.add_function(wrap_pyfunction!(totp_verify, m)?)?;
    Ok(())
}
