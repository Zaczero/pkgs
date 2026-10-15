#![feature(likely_unlikely)]

mod constants;
mod core;
mod errors;

use std::hint::unlikely;

use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::constants::MAX_ZIDS_AT_ONCE;
use crate::core::{reserve_sequences, zid_from_time_and_sequence};
use crate::errors::ZidError;

#[pyfunction]
/// Generate a process-local, monotonically reserved signed-63-bit identifier.
///
/// Logical milliseconds advance on sequence exhaustion and never move backward.
/// Raises OverflowError when the 47-bit timestamp horizon is exhausted.
/// Forked children must exec before generation; there is no automatic reset.
fn zid() -> PyResult<u64> {
    core::zid().map_err(ZidError::into_pyerr)
}

#[pyfunction]
/// Reserve n contiguous increasing identifiers at one logical millisecond.
///
/// Accepts 0 through 65,536; zero returns an empty list without reserving.
/// Raises ValueError for larger counts, OverflowError for negative counts or
/// counts beyond the platform's usize range or an exhausted timestamp horizon.
/// Forked children must exec before generation; there is no automatic reset.
fn zids(py: Python<'_>, n: usize) -> PyResult<Bound<'_, PyList>> {
    if unlikely(n == 0) {
        return Ok(PyList::empty(py));
    }
    if unlikely(n > MAX_ZIDS_AT_ONCE) {
        return Err(ZidError::TooManyZIDsAtOnce { attempted: n }.into_pyerr());
    }

    let (time, start_seq) =
        reserve_sequences(n.wrapping_sub(1) as u16).map_err(ZidError::into_pyerr)?;

    PyList::new(
        py,
        (0..n).map(|i| zid_from_time_and_sequence(time, start_seq.wrapping_add(i as u16))),
    )
}

#[pyfunction]
/// Extract the logical UNIX millisecond timestamp from an unsigned 64-bit integer.
#[expect(
    clippy::missing_const_for_fn,
    reason = "PyO3 exports this function through a runtime wrapper"
)]
fn parse_zid_timestamp(zid: u64) -> u64 {
    core::parse_zid_timestamp(zid)
}

#[pymodule]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(zid, m)?)?;
    m.add_function(wrap_pyfunction!(zids, m)?)?;
    m.add_function(wrap_pyfunction!(parse_zid_timestamp, m)?)?;
    Ok(())
}
