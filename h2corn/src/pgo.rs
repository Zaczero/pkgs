//! Image-local, one-shot collection for the instrumented wheel's fork workers.

use std::ffi::{CString, c_char, c_int};
use std::process;
use std::sync::Mutex;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

unsafe extern "C" {
    fn __llvm_profile_set_filename(filename: *const c_char);
    fn __llvm_profile_reset_counters();
    fn __llvm_profile_dump() -> c_int;
}

struct Collection {
    owner: u32,
    // LLVM retains this pointer even after the terminal dump.
    _filename: CString,
    finished: bool,
}

static COLLECTION: Mutex<Option<Collection>> = Mutex::new(None);

#[pyfunction(name = "_pgo_begin")]
pub(crate) fn begin(filename: &str) -> PyResult<()> {
    let filename = CString::new(filename)
        .map_err(|_| PyValueError::new_err("profile filename contains a NUL byte"))?;
    let mut state = COLLECTION
        .try_lock()
        .map_err(|_| PyRuntimeError::new_err("profile control is already in progress"))?;
    if state.is_some() {
        return Err(PyRuntimeError::new_err("profile collection already began"));
    }
    // SAFETY: The generator calls this once after fork, before starting native
    // worker threads. The static owner retains the filename until process exit.
    unsafe {
        __llvm_profile_set_filename(filename.as_ptr());
        __llvm_profile_reset_counters();
    }
    *state = Some(Collection {
        owner: process::id(),
        _filename: filename,
        finished: false,
    });
    drop(state);
    Ok(())
}

#[pyfunction(name = "_pgo_finish")]
pub(crate) fn finish() -> PyResult<()> {
    let mut state = COLLECTION
        .try_lock()
        .map_err(|_| PyRuntimeError::new_err("profile control is already in progress"))?;
    let collection = state
        .as_mut()
        .ok_or_else(|| PyRuntimeError::new_err("profile collection has not begun"))?;
    if collection.owner != process::id() || collection.finished {
        return Err(PyRuntimeError::new_err(
            "profile collection is not owned or has ended",
        ));
    }
    // LLVM marks a failed dump as dumped too; this transaction cannot be retried.
    collection.finished = true;
    // SAFETY: The worker has drained its requests and Python loop shards. This
    // is the only dump caller, the image/filename stay alive, and no reset races
    // the serializer. Persistent idle native threads make this an approximate
    // terminal snapshot rather than a stop-the-world profile.
    let status = unsafe { __llvm_profile_dump() };
    drop(state);
    if status != 0 {
        return Err(PyRuntimeError::new_err("LLVM profile dump failed"));
    }
    Ok(())
}
