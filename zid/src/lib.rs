#![feature(likely_unlikely)]

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;
use rand::RngCore;
use std::cell::UnsafeCell;
use std::hint::unlikely;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const RAND_BUFFER_SIZE: usize = 8 * 1024; // 8 KiB
const MAX_TIME_MILLIS: u64 = 0x7FFF_FFFF_FFFF;
const MAX_ZIDS_AT_ONCE: usize = (u16::MAX as usize) + 1;

struct RandBuffer {
    buffer: [u8; RAND_BUFFER_SIZE],
    pos: usize,
}

impl RandBuffer {
    const fn new() -> Self {
        Self {
            buffer: [0; RAND_BUFFER_SIZE],
            pos: RAND_BUFFER_SIZE,
        }
    }

    #[inline]
    fn next_u16(&mut self) -> u16 {
        if unlikely(self.pos + 2 > RAND_BUFFER_SIZE) {
            rand::rng().fill_bytes(&mut self.buffer);
            self.pos = 0;
        }
        let value = u16::from_be_bytes([self.buffer[self.pos], self.buffer[self.pos + 1]]);
        self.pos += 2;
        value
    }
}

thread_local! {
    static RAND_BUFFER: UnsafeCell<RandBuffer> = UnsafeCell::new(RandBuffer::new());
}

#[inline]
fn next_rand_u16() -> u16 {
    // Safety: `RAND_BUFFER` is thread-local, so this `UnsafeCell` is only accessed from the
    // current thread, and we don't leak references outside this closure.
    RAND_BUFFER.with(|cell| unsafe { (*cell.get()).next_u16() })
}

static LAST_ZID: AtomicU64 = AtomicU64::new(0);

#[inline]
fn time() -> u64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System clock is before UNIX_EPOCH");
    let time = duration.as_secs() * 1000 + u64::from(duration.subsec_millis());
    debug_assert!(time <= MAX_TIME_MILLIS, "Time value is too large");
    time
}

#[inline]
fn make_zid(time: u64, sequence: u16) -> u64 {
    (time << 16) | (sequence as u64)
}

#[inline]
fn reserve_sequences(additional: u16) -> (u64, u16) {
    let now = time();
    loop {
        let last = LAST_ZID.load(Ordering::Relaxed);
        let last_time = last >> 16;
        let last_seq = last as u16;

        let zid_time = if unlikely(last_time > now) {
            last_time
        } else {
            now
        };
        let start_seq = if last_time == zid_time {
            last_seq.wrapping_add(1)
        } else {
            next_rand_u16()
        };

        let end_seq = start_seq.wrapping_add(additional);
        let next_last = make_zid(zid_time, end_seq);

        match LAST_ZID.compare_exchange_weak(last, next_last, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => return (zid_time, start_seq),
            Err(_) => std::hint::spin_loop(),
        }
    }
}

#[pyfunction]
fn zid() -> u64 {
    let (time, seq) = reserve_sequences(0);
    make_zid(time, seq)
}

#[pyfunction]
fn zids(py: Python<'_>, n: usize) -> PyResult<Bound<'_, PyList>> {
    if unlikely(n == 0) {
        return Ok(PyList::empty(py));
    }
    if unlikely(n > MAX_ZIDS_AT_ONCE) {
        return Err(PyValueError::new_err(format!(
            "Only up to {MAX_ZIDS_AT_ONCE} ZIDs can be generated at once (attempted {n})"
        )));
    }

    let (time, start_seq) = reserve_sequences((n - 1) as u16);

    PyList::new(
        py,
        (0..n).map(|i| make_zid(time, start_seq.wrapping_add(i as u16))),
    )
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
