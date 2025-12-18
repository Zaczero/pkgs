#![feature(likely_unlikely)]

mod constants;
mod core;
mod errors;

use crate::constants::MAX_ZIDS_AT_ONCE;
use crate::core::{reserve_sequences, zid_from_time_and_sequence};
use crate::errors::Error;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;
use std::hint::unlikely;

#[pyfunction]
fn zid() -> u64 {
    core::zid()
}

#[pyfunction]
fn zids(py: Python<'_>, n: usize) -> PyResult<Bound<'_, PyList>> {
    if unlikely(n == 0) {
        return Ok(PyList::empty(py));
    }
    if unlikely(n > MAX_ZIDS_AT_ONCE) {
        return Err(PyValueError::new_err(
            Error::TooManyZIDsAtOnce { attempted: n }.message(),
        ));
    }

    let (time, start_seq) = reserve_sequences((n - 1) as u16);

    PyList::new(
        py,
        (0..n).map(|i| zid_from_time_and_sequence(time, (u32::from(start_seq) + i as u32) as u16)),
    )
}

#[pyfunction]
fn parse_zid_timestamp(zid: u64) -> u64 {
    core::parse_zid_timestamp(zid)
}

#[pymodule(gil_used = false)]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(zid, m)?)?;
    m.add_function(wrap_pyfunction!(zids, m)?)?;
    m.add_function(wrap_pyfunction!(parse_zid_timestamp, m)?)?;
    Ok(())
}
