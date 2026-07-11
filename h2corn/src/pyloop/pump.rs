//! The pump callable: a plain loop callback that performs all Python work
//! batched under one GIL hold — app task starts, duck-future resolutions,
//! and cold-path awaitable spawns.

use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::PyDictMethods;

use super::{PumpEvent, RustFuture, Shard, ShardHandle, SlotHandle};
use crate::app_call::AppCallArgs;
use crate::error::H2CornError;
use crate::runtime::AppState;

enum DoneSlot {
    App(SlotHandle<Result<(), H2CornError>>),
    Value(SlotHandle<PyResult<Py<PyAny>>>),
}

#[pyclass(frozen)]
pub(super) struct Pump {
    shard: Weak<ShardHandle>,
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
    pub(super) fn into_callable(py: Python<'_>, shard: &Shard) -> PyResult<Py<PyAny>> {
        let pump = Py::new(py, Self {
            shard: Arc::downgrade(shard),
            batch: Mutex::new(Vec::new()),
        })?;
        Ok(pump.bind(py).getattr(pyo3::intern!(py, "run"))?.unbind())
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
    fn run(&self, py: Python<'_>) {
        let Some(shard) = self.shard.upgrade() else {
            return;
        };
        shard.drain_doorbell();
        shard.disarm();
        let mut batch = self.batch.lock();
        let more = shard.drain_batch(&mut batch);
        if more {
            // Re-arm before processing so the remainder runs next iteration
            // and producers do not pay a redundant threadsafe wakeup.
            shard.reschedule_local(py);
        }
        for event in batch.drain(..) {
            process_event(py, &shard, event);
        }
    }
}

fn process_event(py: Python<'_>, shard: &Shard, event: PumpEvent) {
    match event {
        PumpEvent::StartTask { app, args, slot } => {
            if let Err(err) = start_task(py, shard, &app, *args, &slot) {
                slot.fill(Err(H2CornError::from(err)));
            }
        },
        PumpEvent::Resolve { fut, payload } => RustFuture::resolve(&fut, py, payload),
        PumpEvent::SpawnAwaitable { awaitable, slot } => {
            if let Err(err) = spawn_awaitable(py, shard, awaitable, &slot) {
                slot.fill(Err(err));
            }
        },
        PumpEvent::CallAwaitable { build, slot } => match build(py, Arc::clone(shard)) {
            Ok(awaitable) => {
                if let Err(err) = spawn_awaitable(py, shard, awaitable, &slot) {
                    slot.fill(Err(err));
                }
            },
            Err(err) => slot.fill(Err(err)),
        },
        PumpEvent::ReleaseApp { app, done } => {
            drop(app);
            let _ = done.send(());
        },
        PumpEvent::CancelTask { task } => {
            let _ = task.call_method0(py, pyo3::intern!(py, "cancel"));
        },
        PumpEvent::Detach => shard.detach(py),
        PumpEvent::StopLoop => shard.stop_loop(py),
    }
}

/// Build args, call the app, and run it as an eagerly-started asyncio Task.
fn start_task(
    py: Python<'_>,
    shard: &Shard,
    app: &AppState,
    args: AppCallArgs,
    slot: &SlotHandle<Result<(), H2CornError>>,
) -> PyResult<()> {
    let args = args.build(py, Arc::clone(shard))?;
    if let Some(state) = shard.copy_scope_state(py)? {
        args.scope.set_item(pyo3::intern!(py, "state"), state)?;
    }
    // pyo3's tuple `call1` moves the owned bounds straight into an
    // offset-flag `PyObject_Vectorcall` — no Python tuple, no extra
    // refcount traffic.
    let coroutine = app
        .app
        .bind(py)
        .call1((args.scope, args.receive, args.send))?;

    let task = shard.construct_task(py, &coroutine)?;

    // Eager tasks for fully-buffered requests complete inside the
    // constructor: deliver the outcome now — zero callbacks, zero Handles.
    if task_is_done(py, &task)? {
        slot.fill(task_outcome(py, &task));
        return Ok(());
    }

    if let Some(task) = slot.set_task(task.clone().unbind()) {
        let _ = task.call_method0(py, pyo3::intern!(py, "cancel"));
        return Ok(());
    }
    let done = Py::new(py, TaskDone {
        slot: DoneSlot::App(Arc::clone(slot)),
    })?;
    task.call_method1(pyo3::intern!(py, "add_done_callback"), (done,))?;
    Ok(())
}

/// Cold path: run an arbitrary awaitable (shutdown trigger) as a task.
fn spawn_awaitable(
    py: Python<'_>,
    shard: &Shard,
    awaitable: Py<PyAny>,
    slot: &SlotHandle<PyResult<Py<PyAny>>>,
) -> PyResult<()> {
    let task = shard.ensure_future(py, awaitable)?;
    if let Some(task) = slot.set_task(task.clone().unbind()) {
        let _ = task.call_method0(py, pyo3::intern!(py, "cancel"));
        return Ok(());
    }
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
