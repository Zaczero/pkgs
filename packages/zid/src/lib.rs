use pyo3::exceptions::{PyOverflowError, PyValueError};
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

impl State {
    fn next_rand_sequence(&mut self) -> () {
        if self.buffer_pos + 2 > self.buffer.len() {
            let buffer_size = self.buffer_size;
            self.buffer.resize(buffer_size, 0);
            self.buffer_pos = 0;
            OsRng.fill_bytes(&mut self.buffer);
        }
        self.sequence = u16::from_be_bytes([
            self.buffer[self.buffer_pos],
            self.buffer[self.buffer_pos + 1],
        ]);
        self.buffer_pos += 2;
    }

    fn zid(&self) -> u64 {
        (self.time << 16) | (self.sequence as u64)
    }
}

static STATE: Mutex<State> = Mutex::new(State {
    buffer: Vec::new(),
    buffer_pos: 0,
    buffer_size: 128 * 1024, // 128 KiB
    time: 0,
    sequence: 0,
});

fn time() -> PyResult<u64> {
    let time128 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    if time128 > 0x7FFF_FFFF_FFFF {
        return Err(PyOverflowError::new_err("Time value is too large"));
    }
    Ok(time128 as u64)
}

#[pyfunction]
fn zid() -> PyResult<u64> {
    let time = time()?;
    let mut state = STATE.lock().unwrap();

    if state.time == time {
        state.sequence = state.sequence.wrapping_add(1);
    } else {
        state.next_rand_sequence();
        state.time = time;
    }
    Ok(state.zid())
}

#[pyfunction()]
fn zids(n: usize) -> PyResult<Vec<u64>> {
    if n == 0 {
        return Ok(Vec::new());
    }
    if n > (u16::MAX as usize) + 1 {
        return Err(PyValueError::new_err(format!(
            "Only up to 65536 ZIDs can be generated at once (attempted {n})"
        )));
    }

    let time = time()?;
    let mut zids = Vec::with_capacity(n);
    let mut state = STATE.lock().unwrap();

    if state.time == time {
        state.sequence = state.sequence.wrapping_add(1);
    } else {
        state.next_rand_sequence();
        state.time = time;
    }
    zids.push(state.zid());

    for _ in 1..n {
        state.sequence = state.sequence.wrapping_add(1);
        zids.push(state.zid());
    }

    Ok(zids)
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
    m.add_function(wrap_pyfunction!(zids, m)?)?;
    m.add_function(wrap_pyfunction!(parse_zid_timestamp, m)?)?;
    m.add_function(wrap_pyfunction!(set_random_buffer_size, m)?)?;
    Ok(())
}
