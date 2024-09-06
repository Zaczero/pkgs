use pyo3::exceptions::PyOverflowError;
use pyo3::prelude::*;
use pyo3::types::PyLong;
use rand::rngs::OsRng;
use rand::RngCore;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_RANDOM_BUFFER_SIZE: usize = 128 * 1024; // 128 KiB

struct State {
    buffer: Vec<u8>,
    buffer_pos: usize,
    time: u64,
    sequence: u16,
}

static STATE: Mutex<State> = Mutex::new(State {
    buffer: Vec::new(),
    buffer_pos: 0,
    time: 0,
    sequence: 0,
});

#[pyfunction]
#[pyo3(name = "zid")]
#[pyo3(pass_module)]
fn zid_fn(m: &Bound<'_, PyModule>) -> PyResult<u64> {
    let time128 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    if time128 > 0x7FFFFFFF {
        return Err(PyOverflowError::new_err("Time value is too large"));
    }

    let time = time128 as u64;
    let mut state = STATE.lock().unwrap();

    if state.time == time {
        state.sequence = state.sequence.wrapping_add(1);
    } else {
        if state.buffer_pos + 2 > state.buffer.len() {
            let buffer_size = m
                .dict()
                .into_any()
                .get_item("RANDOM_BUFFER_SIZE")?
                .downcast_into::<PyLong>()?
                .extract::<usize>()?;
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
fn parse_zid_timestamp(zid: u64) -> u64 {
    (zid >> 16) as u64
}

#[pymodule]
fn zid(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.dict()
        .set_item("RANDOM_BUFFER_SIZE", DEFAULT_RANDOM_BUFFER_SIZE)?;
    m.add_function(wrap_pyfunction!(zid_fn, m)?)?;
    m.add_function(wrap_pyfunction!(parse_zid_timestamp, m)?)?;
    Ok(())
}
