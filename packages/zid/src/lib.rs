use pyo3::exceptions::PyOverflowError;
use pyo3::prelude::*;
use rand::rngs::OsRng;
use rand::RngCore;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

struct State {
    buffer: Vec<u8>,
    buffer_pos: usize,
    buffer_size: usize,
    time: u64,
    sequence: u16,
}

static STATE: Mutex<State> = Mutex::new(State {
    buffer: Vec::new(),
    buffer_pos: 0,
    buffer_size: 128 * 1024, // 128 KiB
    time: 0,
    sequence: 0,
});

#[pyfunction]
#[pyo3(name = "zid")]
fn zid() -> PyResult<u64> {
    let time128 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    if time128 > 0x7FFF_FFFF_FFFF {
        return Err(PyOverflowError::new_err("Time value is too large"));
    }

    let time = time128 as u64;
    let mut state = STATE.lock().unwrap();

    if state.time == time {
        state.sequence = state.sequence.wrapping_add(1);
    } else {
        if state.buffer_pos + 2 > state.buffer.len() {
            let buffer_size = state.buffer_size;
            state.buffer.resize(buffer_size, 0);
            state.buffer_pos = 0;
            OsRng.fill_bytes(&mut state.buffer);
        }

        state.time = time;
        state.sequence = u16::from_be_bytes([
            state.buffer[state.buffer_pos],
            state.buffer[state.buffer_pos + 1],
        ]);
        state.buffer_pos += 2;
    }

    Ok((state.time << 16) | (state.sequence as u64))
}

#[pyfunction]
fn parse_zid_timestamp(zid: u64) -> PyResult<u64> {
    Ok((zid >> 16) as u64)
}

#[pyfunction]
fn set_random_buffer_size(size: usize) -> PyResult<()> {
    STATE.lock().unwrap().buffer_size = size;
    Ok(())
}

#[pymodule]
#[pyo3(name = "_lib")]
fn lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(zid, m)?)?;
    m.add_function(wrap_pyfunction!(parse_zid_timestamp, m)?)?;
    m.add_function(wrap_pyfunction!(set_random_buffer_size, m)?)?;
    Ok(())
}
