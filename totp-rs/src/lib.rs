#![feature(likely_unlikely)]
#![feature(const_index)]
#![feature(const_trait_impl)]

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
use pyo3::types::{PyInt, PyString};

use crate::algorithm::parse_algorithm;
use crate::errors::TotpError;
use crate::secret::Secret;
use crate::time::{resolve_counter, time_window_from_time};
use crate::totp::{totp_code, verify};

const MODULUS_BY_DIGITS: [NonZeroU32; 10] = {
    let mut table = [NonZeroU32::MIN; 10];
    let mut value = 1;
    let mut digits = 0;
    while let Some(slot) = table.get_mut(digits) {
        *slot = match NonZeroU32::new(value) {
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

#[derive(Clone, Copy)]
struct Digits {
    count: u8,
    modulus: NonZeroU32,
}

const fn admit_digits(count: u8) -> Result<Digits, TotpError> {
    let Some(&modulus) = MODULUS_BY_DIGITS.get(count as usize) else {
        return Err(TotpError::DigitsOutOfRange { digits: count });
    };
    if count == 0 {
        return Err(TotpError::DigitsOutOfRange { digits: count });
    }
    Ok(Digits { count, modulus })
}

fn format_code_py(py: Python<'_>, mut code: u32, digits: Digits) -> Py<PyString> {
    let mut buf = [b'0'; 9];

    let start = buf.len().wrapping_sub(usize::from(digits.count));
    let view = buf
        .get_mut(start..)
        .expect("admitted digit count fits code buffer");
    for c in view.iter_mut().rev() {
        *c = b'0'.wrapping_add((code % 10) as u8);
        code /= 10;
    }

    // Safety: all bytes are ASCII digits.
    let view = unsafe { str::from_utf8_unchecked(view) };
    PyString::new(py, view).into()
}

fn parse_code_str(code: &str, digits: Digits) -> Option<u32> {
    let mut value = 0_u32;
    let mut seen_digits = 0_u8;

    for &b in code.as_bytes() {
        if !b.is_ascii_digit() {
            continue;
        }
        if unlikely(seen_digits == digits.count) {
            return None;
        }

        value = value
            .wrapping_mul(10)
            .wrapping_add(u32::from(b.wrapping_sub(b'0')));
        seen_digits = seen_digits.wrapping_add(1);
    }

    (seen_digits == digits.count).then_some(value)
}

fn parse_code_py(code: &Bound<'_, PyAny>, digits: Digits) -> Option<u32> {
    if let Ok(value) = code.cast::<PyString>() {
        if let Ok(s) = value.to_str() {
            return code.py().detach(|| parse_code_str(s, digits));
        }
        return None;
    }

    if let Ok(value) = code.cast::<PyInt>() {
        let Ok(value) = value.extract::<u32>() else {
            return None;
        };
        return (value < digits.modulus.get()).then_some(value);
    }

    None
}

#[pyfunction]
#[pyo3(signature = (time = None, *, step_seconds = 30, t0 = 0))]
fn totp_time_window(time: Option<f64>, step_seconds: i64, t0: i64) -> PyResult<i64> {
    time_window_from_time(time, step_seconds, t0).map_err(TotpError::into_pyerr)
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
    let digits = admit_digits(digits).map_err(TotpError::into_pyerr)?;
    let algorithm = parse_algorithm(algorithm).map_err(TotpError::into_pyerr)?;
    let counter =
        resolve_counter(time, time_window, step_seconds, t0).map_err(TotpError::into_pyerr)?;

    let secret = Secret::from_py(secret)?;
    let code = py
        .detach(|| {
            let secret = secret.decode()?;
            Ok::<_, TotpError>(totp_code(
                secret.as_ref(),
                counter,
                digits.modulus,
                algorithm,
            ))
        })
        .map_err(TotpError::into_pyerr)?;

    Ok(format_code_py(py, code, digits))
}

#[pyfunction]
#[pyo3(signature = (secret, code, *, digits = 6, algorithm = "sha1", time = None, time_window = None, step_seconds = 30, t0 = 0, window = 1))]
fn totp_verify(
    py: Python<'_>,
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
    let digits = admit_digits(digits).map_err(TotpError::into_pyerr)?;
    let algorithm = parse_algorithm(algorithm).map_err(TotpError::into_pyerr)?;
    let counter =
        resolve_counter(time, time_window, step_seconds, t0).map_err(TotpError::into_pyerr)?;

    let Some(code) = parse_code_py(code, digits) else {
        return Ok(false);
    };

    let secret = Secret::from_py(secret)?;
    py.detach(|| {
        let secret = secret.decode()?;
        Ok(verify(
            secret.as_ref(),
            counter,
            window,
            digits.modulus,
            code,
            algorithm,
        ))
    })
    .map_err(TotpError::into_pyerr)
}

#[pymodule]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(totp_time_window, m)?)?;
    m.add_function(wrap_pyfunction!(totp_generate, m)?)?;
    m.add_function(wrap_pyfunction!(totp_verify, m)?)?;
    Ok(())
}
