#![feature(likely_unlikely)]

use core::hint::unlikely;
use hmac::digest::{FixedOutputReset, KeyInit};
use hmac::{Hmac, Mac};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyBytesMethods, PyInt, PyString, PyStringMethods};
use sha1::Sha1;
use sha2::{Sha256, Sha512};
use std::borrow::Cow;
use std::time::{SystemTime, UNIX_EPOCH};
use subtle::ConstantTimeEq;

#[derive(Clone, Copy)]
enum Algorithm {
    Sha1,
    Sha256,
    Sha512,
}

type SecretBytes<'a> = Cow<'a, [u8]>;

fn validate_digits(digits: u32) -> PyResult<u32> {
    if unlikely(digits == 0 || digits > 9) {
        return Err(PyValueError::new_err("digits must be in the range [1..=9]"));
    }
    Ok(digits)
}

fn validate_step_seconds(step_seconds: i64) -> PyResult<i64> {
    if unlikely(step_seconds <= 0) {
        return Err(PyValueError::new_err("step_seconds must be positive"));
    }
    Ok(step_seconds)
}

fn parse_algorithm(algorithm: &str) -> PyResult<Algorithm> {
    match algorithm {
        "sha1" => Ok(Algorithm::Sha1),
        "sha256" => Ok(Algorithm::Sha256),
        "sha512" => Ok(Algorithm::Sha512),
        _ => Err(PyValueError::new_err(
            "algorithm must be one of: sha1, sha256, sha512",
        )),
    }
}

fn time_seconds_from_py(time: Option<f64>) -> PyResult<i64> {
    match time {
        None => {
            let duration = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| PyRuntimeError::new_err("system time is before UNIX_EPOCH"))?;
            i64::try_from(duration.as_secs())
                .map_err(|_| PyRuntimeError::new_err("system time is too large"))
        }
        Some(value) => {
            if unlikely(!value.is_finite()) {
                return Err(PyValueError::new_err("time must be a finite number"));
            }
            let secs = value.floor();
            if unlikely(secs < (i64::MIN as f64) || secs > (i64::MAX as f64)) {
                return Err(PyValueError::new_err("time is out of range"));
            }
            Ok(secs as i64)
        }
    }
}

fn time_window_from_time(time_seconds: i64, step_seconds: i64, t0: i64) -> PyResult<u64> {
    let diff = time_seconds
        .checked_sub(t0)
        .ok_or_else(|| PyValueError::new_err("time - t0 is out of range"))?;
    let counter = diff.div_euclid(step_seconds);
    if unlikely(counter < 0) {
        return Err(PyValueError::new_err("time_window must be >= 0"));
    }
    Ok(counter as u64)
}

fn resolve_counter(
    time: Option<f64>,
    time_window: Option<i64>,
    step_seconds: i64,
    t0: i64,
) -> PyResult<u64> {
    if unlikely(time.is_some() && time_window.is_some()) {
        return Err(PyValueError::new_err(
            "time and time_window cannot both be set",
        ));
    }

    match time_window {
        Some(window) => {
            if unlikely(window < 0) {
                return Err(PyValueError::new_err("time_window must be >= 0"));
            }
            Ok(window as u64)
        }
        None => {
            let step_seconds = validate_step_seconds(step_seconds)?;
            let time_seconds = time_seconds_from_py(time)?;
            time_window_from_time(time_seconds, step_seconds, t0)
        }
    }
}

fn dynamic_truncate(digest: &[u8]) -> u32 {
    let offset = (digest[digest.len() - 1] & 0x0f) as usize;
    let p = &digest[offset..offset + 4];
    (u32::from(p[0] & 0x7f) << 24)
        | (u32::from(p[1]) << 16)
        | (u32::from(p[2]) << 8)
        | u32::from(p[3])
}

fn totp_code_with<M: Mac + KeyInit>(secret: &[u8], counter_bytes: &[u8; 8], modulus: u32) -> u32 {
    let mut mac = <M as KeyInit>::new_from_slice(secret).expect("HMAC accepts any key size");
    mac.update(counter_bytes);
    let digest = mac.finalize().into_bytes();
    let truncated = dynamic_truncate(&digest);
    truncated % modulus
}

fn totp_code(secret: &[u8], counter: u64, modulus: u32, algorithm: Algorithm) -> u32 {
    let counter_bytes = counter.to_be_bytes();
    match algorithm {
        Algorithm::Sha1 => totp_code_with::<Hmac<Sha1>>(secret, &counter_bytes, modulus),
        Algorithm::Sha256 => totp_code_with::<Hmac<Sha256>>(secret, &counter_bytes, modulus),
        Algorithm::Sha512 => totp_code_with::<Hmac<Sha512>>(secret, &counter_bytes, modulus),
    }
}

fn totp_code_with_reset<M: Mac + FixedOutputReset>(mac: &mut M, counter: u64, modulus: u32) -> u32 {
    Mac::update(mac, &counter.to_be_bytes());
    let digest = mac.finalize_reset().into_bytes();
    let truncated = dynamic_truncate(&digest);
    truncated % modulus
}

fn verify_with_mac<M: Mac + FixedOutputReset>(
    mut mac: M,
    base_counter: u64,
    window: u8,
    modulus: u32,
    provided_bytes: &[u8; 4],
) -> bool {
    let mut matches = |candidate: Option<u64>| -> bool {
        let Some(candidate) = candidate else {
            return false;
        };
        totp_code_with_reset(&mut mac, candidate, modulus)
            .to_be_bytes()
            .ct_eq(provided_bytes)
            .unwrap_u8()
            == 1
    };

    if matches(Some(base_counter)) {
        return true;
    }

    let window = u64::from(window);
    for offset in 1..=window {
        if matches(base_counter.checked_sub(offset)) {
            return true;
        }
        if matches(base_counter.checked_add(offset)) {
            return true;
        }
    }

    false
}

fn format_code_py(py: Python<'_>, mut code: u32, digits: u32) -> Py<PyString> {
    let digits = digits as usize;
    let mut buf = [b'0'; 9];
    let start = 9 - digits;
    for i in (start..9).rev() {
        buf[i] = b'0' + (code % 10) as u8;
        code /= 10;
    }

    // Safety: all bytes are ASCII digits ('0'..='9'), so this is valid UTF-8.
    let view = unsafe { std::str::from_utf8_unchecked(&buf[start..]) };
    PyString::new(py, view).into()
}

fn parse_numeric_code_str(code: &str, digits: u32) -> Option<u32> {
    let trimmed = code.trim();
    let bytes = trimmed.as_bytes();
    if bytes.len() != digits as usize {
        return None;
    }

    let mut value: u32 = 0;
    for &b in bytes {
        if !b.is_ascii_digit() {
            return None;
        }
        value = value * 10 + u32::from(b - b'0');
    }
    Some(value)
}

fn parse_code_from_py(code: &Bound<'_, PyAny>, digits: u32, modulus: u32) -> PyResult<Option<u32>> {
    if let Ok(value) = code.cast::<PyString>() {
        return Ok(parse_numeric_code_str(value.to_str()?, digits));
    }
    if let Ok(value) = code.cast::<PyInt>() {
        let Ok(value) = value.extract::<u32>() else {
            return Ok(None);
        };
        return Ok((value < modulus).then_some(value));
    }
    Ok(None)
}

const BASE32_INVALID: u8 = 0xFF;

const fn build_base32_decode_lut() -> [u8; 256] {
    let mut lut = [BASE32_INVALID; 256];
    let mut i = 0u8;
    while i < 26 {
        lut[(b'A' + i) as usize] = i;
        lut[(b'a' + i) as usize] = i;
        i += 1;
    }
    i = 0;
    while i < 6 {
        lut[(b'2' + i) as usize] = 26 + i;
        i += 1;
    }
    lut
}

const BASE32_DECODE_LUT: [u8; 256] = build_base32_decode_lut();

fn decode_base32_secret(encoded: &str) -> PyResult<Vec<u8>> {
    let trimmed = encoded.trim();
    let bytes = trimmed.as_bytes();

    let mut end = bytes.len();
    while end > 0 && bytes[end - 1] == b'=' {
        end -= 1;
    }
    let bytes = &bytes[..end];

    let rem = bytes.len() % 8;
    if unlikely(rem == 1 || rem == 3 || rem == 6) {
        return Err(PyValueError::new_err("secret is not valid base32"));
    }

    let mut out = Vec::with_capacity(bytes.len() * 5 / 8);
    let mut acc: u32 = 0;
    let mut bits: u8 = 0;

    for &b in bytes {
        let v = BASE32_DECODE_LUT[b as usize];
        if unlikely(v == BASE32_INVALID) {
            return Err(PyValueError::new_err("secret is not valid base32"));
        }
        acc = (acc << 5) | (v as u32);
        bits += 5;
        while bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1u32 << bits) - 1;
        }
    }

    if unlikely(bits != 0 && acc != 0) {
        return Err(PyValueError::new_err("secret is not valid base32"));
    }

    Ok(out)
}

fn parse_secret_from_py<'a>(secret: &'a Bound<'_, PyAny>) -> PyResult<SecretBytes<'a>> {
    if let Ok(value) = secret.cast::<PyBytes>() {
        return Ok(Cow::Borrowed(value.as_bytes()));
    }
    if let Ok(value) = secret.cast::<PyString>() {
        return Ok(Cow::Owned(decode_base32_secret(value.to_str()?)?));
    }
    Err(PyValueError::new_err(
        "secret must be bytes or base32 string",
    ))
}

#[pyfunction]
#[pyo3(signature = (time = None, *, step_seconds = 30, t0 = 0))]
fn totp_time_window(time: Option<f64>, step_seconds: i64, t0: i64) -> PyResult<u64> {
    let step_seconds = validate_step_seconds(step_seconds)?;
    let time_seconds = time_seconds_from_py(time)?;
    time_window_from_time(time_seconds, step_seconds, t0)
}

#[pyfunction]
#[pyo3(signature = (secret, *, digits = 6, algorithm = "sha1", time = None, time_window = None, step_seconds = 30, t0 = 0))]
fn totp_generate(
    py: Python<'_>,
    secret: &Bound<'_, PyAny>,
    digits: u32,
    algorithm: &str,
    time: Option<f64>,
    time_window: Option<i64>,
    step_seconds: i64,
    t0: i64,
) -> PyResult<Py<PyString>> {
    let secret = parse_secret_from_py(secret)?;
    let digits = validate_digits(digits)?;
    let algorithm = parse_algorithm(algorithm)?;
    let modulus = 10_u32.pow(digits);
    let counter = resolve_counter(time, time_window, step_seconds, t0)?;

    let code = totp_code(secret.as_ref(), counter, modulus, algorithm);
    Ok(format_code_py(py, code, digits))
}

#[pyfunction]
#[pyo3(signature = (secret, code, *, digits = 6, algorithm = "sha1", time = None, time_window = None, step_seconds = 30, t0 = 0, window = 1))]
fn totp_verify(
    secret: &Bound<'_, PyAny>,
    code: &Bound<'_, PyAny>,
    digits: u32,
    algorithm: &str,
    time: Option<f64>,
    time_window: Option<i64>,
    step_seconds: i64,
    t0: i64,
    window: u8,
) -> PyResult<bool> {
    let secret = parse_secret_from_py(secret)?;
    let digits = validate_digits(digits)?;
    let algorithm = parse_algorithm(algorithm)?;
    let modulus = 10_u32.pow(digits);

    let Some(provided) = parse_code_from_py(code, digits, modulus)? else {
        return Ok(false);
    };

    let base_counter = resolve_counter(time, time_window, step_seconds, t0)?;
    let provided_bytes = provided.to_be_bytes();

    Ok(match algorithm {
        Algorithm::Sha1 => verify_with_mac(
            <Hmac<Sha1> as KeyInit>::new_from_slice(secret.as_ref())
                .expect("HMAC accepts any key size"),
            base_counter,
            window,
            modulus,
            &provided_bytes,
        ),
        Algorithm::Sha256 => verify_with_mac(
            <Hmac<Sha256> as KeyInit>::new_from_slice(secret.as_ref())
                .expect("HMAC accepts any key size"),
            base_counter,
            window,
            modulus,
            &provided_bytes,
        ),
        Algorithm::Sha512 => verify_with_mac(
            <Hmac<Sha512> as KeyInit>::new_from_slice(secret.as_ref())
                .expect("HMAC accepts any key size"),
            base_counter,
            window,
            modulus,
            &provided_bytes,
        ),
    })
}

#[pymodule(gil_used = false)]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(totp_time_window, m)?)?;
    m.add_function(wrap_pyfunction!(totp_generate, m)?)?;
    m.add_function(wrap_pyfunction!(totp_verify, m)?)?;
    Ok(())
}
