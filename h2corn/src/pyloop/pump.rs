//! The pump callable: a plain loop callback that performs all Python work
//! batched under one GIL hold — app task starts, duck-future resolutions,
//! and cold-path awaitable spawns.

use std::ptr::null_mut;
use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::ffi;
use pyo3::prelude::*;

use super::{PumpEvent, RustFuture, Shard, SlotHandle};
use crate::error::H2CornError;
use crate::runtime::AppState;

enum DoneSlot {
    App(SlotHandle<Result<(), H2CornError>>),
    Value(SlotHandle<PyResult<Py<PyAny>>>),
}

#[pyclass(frozen)]
pub(super) struct Pump {
    shard: Shard,
    /// Reused drain buffer; the pump runs on one thread, the mutex is
    /// uncontended and only satisfies `frozen`'s `Sync` requirement.
    batch: Mutex<Vec<PumpEvent>>,
}

/// Done-callback delivering a finished task's outcome to its slot.
#[pyclass(frozen)]
struct TaskDone {
    slot: DoneSlot,
}

impl Pump {
    pub(super) fn into_callable(py: Python<'_>, shard: Shard) -> PyResult<Py<PyAny>> {
        Ok(Py::new(py, Self {
            shard,
            batch: Mutex::new(Vec::new()),
        })?
        .into_any())
    }
}

#[pymethods]
impl TaskDone {
    fn __call__(&self, py: Python<'_>, task: &Bound<'_, PyAny>) {
        match &self.slot {
            DoneSlot::App(slot) => slot.fill(task_outcome(py, task)),
            DoneSlot::Value(slot) => {
                slot.fill(
                    task.call_method0(pyo3::intern!(py, "result"))
                        .map(Bound::unbind),
                );
            },
        }
    }
}

#[pymethods]
impl Pump {
    fn __call__(&self, py: Python<'_>) {
        self.shard.drain_doorbell();
        self.shard.disarm();
        let mut batch = self.batch.lock();
        let more = self.shard.drain_batch(&mut batch);
        if more {
            // Re-arm before processing so the remainder runs next iteration
            // and producers do not pay a redundant threadsafe wakeup.
            self.shard.reschedule_local(py);
        }
        for event in batch.drain(..) {
            process_event(py, self.shard, event);
        }
    }
}

fn process_event(py: Python<'_>, shard: Shard, event: PumpEvent) {
    match event {
        PumpEvent::StartTask {
            app,
            build_args,
            slot,
        } => {
            if let Err(err) = start_task(py, shard, &app, build_args, &slot) {
                slot.fill(Err(H2CornError::from(err)));
            }
        },
        PumpEvent::Resolve { fut, payload } => RustFuture::resolve(&fut, py, payload),
        PumpEvent::SpawnAwaitable { awaitable, slot } => {
            if let Err(err) = spawn_awaitable(py, shard, awaitable, &slot) {
                slot.fill(Err(err));
            }
        },
    }
}

/// Build args, call the app, and run it as an eagerly-started asyncio Task.
fn start_task(
    py: Python<'_>,
    shard: Shard,
    app: &AppState,
    build_args: super::BuildArgs,
    slot: &SlotHandle<Result<(), H2CornError>>,
) -> PyResult<()> {
    let (scope, receive, send) = build_args(py, app, shard)?;
    let args = [scope.as_ptr(), receive.as_ptr(), send.as_ptr()];
    let coroutine = unsafe {
        // SAFETY: the GIL is held; the argument array contains three valid
        // borrowed object pointers which outlive the call; no kwargs are
        // passed; `PyObject_Vectorcall` returns a new owned reference or
        // sets a Python exception.
        Bound::from_owned_ptr_or_err(
            py,
            ffi::PyObject_Vectorcall(
                app.app.as_ptr(),
                args.as_ptr().cast_mut(),
                args.len(),
                null_mut(),
            ),
        )?
    };

    let task = shard.construct_task(py, &coroutine)?;

    // Eager tasks for fully-buffered requests complete inside the
    // constructor: deliver the outcome now — zero callbacks, zero Handles.
    if task_is_done(py, &task)? {
        slot.fill(task_outcome(py, &task));
        return Ok(());
    }

    slot.set_task(task.clone().unbind());
    let done = Py::new(py, TaskDone {
        slot: DoneSlot::App(Arc::clone(slot)),
    })?;
    task.call_method1(pyo3::intern!(py, "add_done_callback"), (done,))?;
    Ok(())
}

/// Cold path: run an arbitrary awaitable (shutdown trigger) as a task.
fn spawn_awaitable(
    py: Python<'_>,
    shard: Shard,
    awaitable: Py<PyAny>,
    slot: &SlotHandle<PyResult<Py<PyAny>>>,
) -> PyResult<()> {
    let task = shard.ensure_future(py, awaitable)?;
    let done = Py::new(py, TaskDone {
        slot: DoneSlot::Value(Arc::clone(slot)),
    })?;
    task.call_method1(pyo3::intern!(py, "add_done_callback"), (done,))?;
    Ok(())
}

fn task_is_done(py: Python<'_>, task: &Bound<'_, PyAny>) -> PyResult<bool> {
    task.call_method0(pyo3::intern!(py, "done"))?.is_truthy()
}

fn task_outcome(py: Python<'_>, task: &Bound<'_, PyAny>) -> Result<(), H2CornError> {
    match task.call_method0(pyo3::intern!(py, "result")) {
        Ok(_) => Ok(()),
        Err(err) => Err(H2CornError::from(err)),
    }
}
