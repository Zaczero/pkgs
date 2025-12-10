#![feature(likely_unlikely)]

use parking_lot::Mutex;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;
use rand::Rng;
use std::hint::{likely, unlikely};
use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};

const BUFFER_SIZE: usize = 128 * 1024; // 128 KiB

#[inline]
fn make_zid(time: u64, sequence: u16) -> u64 {
    (time << 16) | (sequence as u64)
}

struct State {
    buffer: Box<[u8; BUFFER_SIZE]>,
    buffer_pos: usize,
    time: u64,
    sequence: u16,
}

impl State {
    fn next_rand_sequence(&mut self) {
        if unlikely(self.buffer_pos + 2 > BUFFER_SIZE) {
            rand::rng().fill(self.buffer.as_mut_slice());
            self.buffer_pos = 0;
        }
        self.sequence = u16::from_be_bytes([
            self.buffer[self.buffer_pos],
            self.buffer[self.buffer_pos + 1],
        ]);
        self.buffer_pos += 2;
    }

}

static STATE: LazyLock<Mutex<State>> = LazyLock::new(|| {
    Mutex::new(State {
        // All-zeros is valid for [u8; N]
        buffer: unsafe { Box::new_zeroed().assume_init() },
        buffer_pos: BUFFER_SIZE, // Fill on first call
        time: 0,
        sequence: 0,
    })
});

#[inline]
fn time() -> u64 {
    let time128 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    debug_assert!(time128 <= 0x7FFF_FFFF_FFFF, "Time value is too large");
    time128 as u64
}

#[pyfunction]
fn zid() -> u64 {
    let time = time();
    let mut state = STATE.lock();

    if likely(state.time == time) {
        state.sequence = state.sequence.wrapping_add(1);
    } else {
        state.next_rand_sequence();
        state.time = time;
    }
    make_zid(state.time, state.sequence)
}

#[pyfunction]
fn zids(py: Python<'_>, n: usize) -> PyResult<Bound<'_, PyList>> {
    if unlikely(n == 0) {
        return Ok(PyList::empty(py));
    }
    if unlikely(n > (u16::MAX as usize) + 1) {
        return Err(PyValueError::new_err(format!(
            "Only up to 65536 ZIDs can be generated at once (attempted {n})"
        )));
    }

    let (time, start_seq) = {
        let time = time();
        let mut state = STATE.lock();

        if likely(state.time == time) {
            state.sequence = state.sequence.wrapping_add(1);
        } else {
            state.next_rand_sequence();
            state.time = time;
        }

        let start_seq = state.sequence;
        state.sequence = state.sequence.wrapping_add((n - 1) as u16);
        (state.time, start_seq)
    };

    PyList::new(py, (0..n).map(|i| make_zid(time, start_seq.wrapping_add(i as u16))))
}

#[pyfunction]
#[inline]
fn parse_zid_timestamp(zid: u64) -> u64 {
    zid >> 16
}

#[pymodule(gil_used = false)]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(zid, m)?)?;
    m.add_function(wrap_pyfunction!(zids, m)?)?;
    m.add_function(wrap_pyfunction!(parse_zid_timestamp, m)?)?;
    Ok(())
}
