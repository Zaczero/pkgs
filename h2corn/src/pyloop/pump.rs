//! The pump callable: a plain loop callback that performs all Python work
//! batched under one GIL hold — app task starts, duck-future resolutions,
//! and cold-path awaitable spawns.

use std::mem;
use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::PyDictMethods;

use super::{PumpEvent, RustFuture, Shard, ShardHandle, SlotDropAck, SlotHandle};
use crate::app_call::AppCallArgs;
use crate::error::H2CornError;
use crate::runtime::RequestTaskGuard;

enum DoneSlot {
    App(SlotHandle<Result<(), H2CornError>, RequestTaskGuard>),
    Value(SlotHandle<PyResult<Py<PyAny>>>),
    AcknowledgedValue(SlotHandle<PyResult<Py<PyAny>>, SlotDropAck>),
}

#[pyclass(frozen, name = "_Pump")]
pub(super) struct Pump {
    shard: Weak<ShardHandle>,
    /// Reused drain buffer; the pump runs on one thread, the mutex is
    /// uncontended and only satisfies `frozen`'s `Sync` requirement.
    batch: Mutex<Vec<PumpEvent>>,
}

/// Done-callback delivering a finished task's outcome to its slot.
///
/// The slot owner is taken on the call so the guard — and the scoped
/// runtime owner it holds — releases at callback time, even if this
/// callback object itself stays reachable afterwards (e.g. retained by a
/// cancelled Task's reference cycle until a GC pass).
#[pyclass(frozen, name = "_TaskDone")]
struct TaskDone {
    slot: Mutex<Option<DoneSlot>>,
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
        let Some(slot) = self.slot.lock().take() else {
            return;
        };
        match slot {
            DoneSlot::App(slot) => slot.fill(task_outcome(py, task)),
            DoneSlot::Value(slot) => {
                slot.fill(
                    task.call_method0(pyo3::intern!(py, "result"))
                        .map(Bound::unbind),
                );
            },
            DoneSlot::AcknowledgedValue(slot) => {
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
        // Never hold the buffer's synchronization guard while calling into
        // Python. The mutex exists only to make this loop-affine cache `Sync`;
        // event processing can schedule arbitrary callbacks and does not need
        // access to the cache itself.
        let mut batch = mem::take(&mut *self.batch.lock());
        let more = shard.drain_batch(&mut batch);
        if more {
            // Re-arm before processing so the remainder runs next iteration
            // and producers do not pay a redundant threadsafe wakeup.
            shard.reschedule_local(py);
        }
        while let Some(event) = batch.pop() {
            process_event(py, &shard, event);
        }
        *self.batch.lock() = batch;
    }
}

fn process_event(py: Python<'_>, shard: &Shard, event: PumpEvent) {
    match event {
        PumpEvent::StartTask { args, slot } => {
            if let Err(err) = start_task(py, shard, *args, &slot) {
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
        PumpEvent::CallCancellableAwaitable { build, slot } => match build(py, Arc::clone(shard)) {
            Ok(awaitable) => {
                if let Err(err) = spawn_cancellable_awaitable(py, shard, awaitable, &slot) {
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
    args: AppCallArgs,
    slot: &SlotHandle<Result<(), H2CornError>, RequestTaskGuard>,
) -> PyResult<()> {
    let args = args.build(py, Arc::clone(shard))?;
    if let Some(state) = shard.copy_scope_state(py)? {
        args.scope.set_item(pyo3::intern!(py, "state"), state)?;
    }
    // pyo3's tuple `call1` moves the owned bounds straight into an
    // offset-flag `PyObject_Vectorcall` — no Python tuple, no extra
    // refcount traffic.
    let coroutine = slot
        .guard()
        .app()
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

    // The slot guard owns request admission until this Python task's done
    // callback runs. Attach it before publishing the task so guard release is
    // never skipped, even when Rust abandoned the request before the pump
    // constructed the task.
    let done = Py::new(py, TaskDone {
        slot: Mutex::new(Some(DoneSlot::App(Arc::clone(slot)))),
    })?;
    task.call_method1(pyo3::intern!(py, "add_done_callback"), (done,))?;
    if let Some(task) = slot.set_task(task.unbind()) {
        let _ = task.call_method0(py, pyo3::intern!(py, "cancel"));
    }
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
    // Abandonment may race task construction. Attach before publishing so
    // loop-side cleanup and guard release are never skipped.
    let done = Py::new(py, TaskDone {
        slot: Mutex::new(Some(DoneSlot::Value(Arc::clone(slot)))),
    })?;
    task.call_method1(pyo3::intern!(py, "add_done_callback"), (done,))?;
    if let Some(task) = slot.set_task(task.unbind()) {
        let _ = task.call_method0(py, pyo3::intern!(py, "cancel"));
    }
    Ok(())
}

fn spawn_cancellable_awaitable(
    py: Python<'_>,
    shard: &Shard,
    awaitable: Py<PyAny>,
    slot: &SlotHandle<PyResult<Py<PyAny>>, SlotDropAck>,
) -> PyResult<()> {
    let task = shard.ensure_future(py, awaitable)?;
    let done = Py::new(py, TaskDone {
        slot: Mutex::new(Some(DoneSlot::AcknowledgedValue(Arc::clone(slot)))),
    })?;
    task.call_method1(pyo3::intern!(py, "add_done_callback"), (done,))?;
    if let Some(task) = slot.set_task(task.clone().unbind()) {
        let _ = task.call_method0(py, pyo3::intern!(py, "cancel"));
    }
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
