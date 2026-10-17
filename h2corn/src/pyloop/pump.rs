//! The pump callable: a plain loop callback that performs all Python work
//! batched under one GIL hold — app task starts, duck-future resolutions,
//! and cold-path awaitable spawns.

use std::mem;
use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use pyo3::exceptions::asyncio::CancelledError;
use pyo3::prelude::*;

use crate::app_call::AppCallArgs;
use crate::error::{H2CornError, SendAfterCloseError, into_pyerr};
use crate::pyloop::{PumpEvent, RustFuture, Shard, ShardHandle, TaskSlot, UnraisableError};
use crate::runtime::RequestTaskGuard;

enum DoneSlot {
    App(Arc<TaskSlot<Result<(), H2CornError>, RequestTaskGuard>>),
    Value(Arc<TaskSlot<PyResult<Py<PyAny>>>>),
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
            DoneSlot::App(slot) => {
                fill_app_slot(py, &slot, task_outcome(py, task), Some(task));
            },
            DoneSlot::Value(slot) => {
                fill_value_slot(
                    py,
                    &slot,
                    task.call_method0(pyo3::intern!(py, "result"))
                        .map(Bound::unbind),
                    Some(task),
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
                fill_app_slot(py, &slot, Err(H2CornError::from(err)), None);
            }
        },
        PumpEvent::Resolve { fut, payload } => RustFuture::resolve(&fut, py, payload),
        PumpEvent::SpawnAwaitable { awaitable, slot } => {
            if let Err(err) = spawn_awaitable(py, shard, awaitable, &slot) {
                fill_value_slot(py, &slot, Err(into_pyerr(err)), None);
            }
        },
        PumpEvent::CallAwaitable { build, slot } => match build(py, Arc::clone(shard)) {
            Ok(awaitable) => {
                if let Err(err) = spawn_awaitable(py, shard, awaitable, &slot) {
                    fill_value_slot(py, &slot, Err(into_pyerr(err)), None);
                }
            },
            Err(err) => fill_value_slot(py, &slot, Err(err), None),
        },
        PumpEvent::ReleaseApp { app, done } => {
            drop(app);
            let _send_result = done.send(());
        },
        PumpEvent::CancelTask { task } => {
            cancel_task(py, task.bind(py));
        },
        PumpEvent::ReportUnraisable { error } => {
            let error = match error {
                UnraisableError::Python(error) => error,
                UnraisableError::H2Corn(error) => into_pyerr(error),
            };
            report_unaccepted_error(py, error, None);
        },
        PumpEvent::CallTrigger { trigger } => {
            // Nothing is waiting on an embedder's readiness or retirement
            // callback, so a failure has nowhere to propagate — which is what
            // `sys.unraisablehook` exists for. Dropping it left a server
            // accepting traffic while its readiness publication had failed.
            if let Err(err) = trigger.call0(py) {
                err.write_unraisable(py, Some(trigger.bind(py)));
            }
        },
        PumpEvent::Detach => shard.detach(py),
        PumpEvent::StopLoop => shard.stop_loop(py),
    }
}

/// Build args, call the app, and schedule the request as an asyncio Task.
fn start_task(
    py: Python<'_>,
    shard: &Shard,
    args: AppCallArgs,
    slot: &Arc<TaskSlot<Result<(), H2CornError>, RequestTaskGuard>>,
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
        fill_app_slot(py, slot, task_outcome(py, &task), Some(&task));
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
        cancel_task(py, task.bind(py));
    }
    Ok(())
}

/// Cold path: run an arbitrary awaitable (shutdown trigger) as a task.
fn spawn_awaitable(
    py: Python<'_>,
    shard: &Shard,
    awaitable: Py<PyAny>,
    slot: &Arc<TaskSlot<PyResult<Py<PyAny>>>>,
) -> PyResult<()> {
    let task = shard.ensure_future(py, awaitable)?;
    // Abandonment may race task construction. Attach before publishing so
    // loop-side cleanup and guard release are never skipped.
    let done = Py::new(py, TaskDone {
        slot: Mutex::new(Some(DoneSlot::Value(Arc::clone(slot)))),
    })?;
    task.call_method1(pyo3::intern!(py, "add_done_callback"), (done,))?;
    if let Some(task) = slot.set_task(task.unbind()) {
        cancel_task(py, task.bind(py));
    }
    Ok(())
}

fn task_is_done(py: Python<'_>, task: &Bound<'_, PyAny>) -> PyResult<bool> {
    task.call_method0(pyo3::intern!(py, "done"))?.is_truthy()
}

fn task_outcome(py: Python<'_>, task: &Bound<'_, PyAny>) -> Result<(), H2CornError> {
    match task.call_method0(pyo3::intern!(py, "result")) {
        Ok(_) => Ok(()),
        // An application that let our own closed-stream error escape did not
        // fail: the ASGI spec has the server raise that error, and says the
        // server should not then log the exception it raised itself. Reporting
        // it blamed the application for h2corn's own signal -- and whether the
        // application ever saw it depended on a race the response driver won on
        // some interpreters and lost on others.
        Err(err) if err.is_instance_of::<SendAfterCloseError>(py) => Ok(()),
        Err(err) => Err(H2CornError::from(err)),
    }
}

fn cancel_task(py: Python<'_>, task: &Bound<'_, PyAny>) {
    if let Err(err) = task.call_method0(pyo3::intern!(py, "cancel")) {
        err.write_unraisable(py, Some(task));
    }
}

fn fill_app_slot(
    py: Python<'_>,
    slot: &Arc<TaskSlot<Result<(), H2CornError>, RequestTaskGuard>>,
    outcome: Result<(), H2CornError>,
    object: Option<&Bound<'_, PyAny>>,
) {
    if let Err(Err(error)) = slot.fill(outcome) {
        report_unaccepted_error(py, into_pyerr(error), object);
    }
}

fn fill_value_slot(
    py: Python<'_>,
    slot: &Arc<TaskSlot<PyResult<Py<PyAny>>>>,
    outcome: PyResult<Py<PyAny>>,
    object: Option<&Bound<'_, PyAny>>,
) {
    if let Err(Err(error)) = slot.fill(outcome) {
        report_unaccepted_error(py, error, object);
    }
}

fn report_unaccepted_error(py: Python<'_>, error: PyErr, object: Option<&Bound<'_, PyAny>>) {
    if !error.is_instance_of::<CancelledError>(py) {
        error.write_unraisable(py, object);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::Mutex;
    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyList};

    use super::{DoneSlot, TaskDone, fill_value_slot, process_event};
    use crate::error::H2CornError;
    use crate::pyloop::{PumpEvent, ShardHandle, TaskSlot};
    use crate::runtime::test_fixtures;

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn abandoned_task_cancel_failure_reaches_unraisable_hook() {
        Python::initialize();
        Python::attach(|py| {
            let locals = PyDict::new(py);
            let reported = PyList::empty(py);
            let sys = PyModule::import(py, "sys").unwrap();
            let original_hook = sys.getattr("unraisablehook").unwrap();
            sys.setattr("unraisablehook", reported.getattr("append").unwrap())
                .unwrap();
            py.run(
                pyo3::ffi::c_str!(
                    "class Task:\n\
                     \x20   def cancel(self):\n\
                     \x20       raise RuntimeError('planned cancel failure')\n\
                     task = Task()"
                ),
                None,
                Some(&locals),
            )
            .unwrap();
            let task = locals.get_item("task").unwrap().unwrap().unbind();
            let shard = ShardHandle::test_stub(py);

            process_event(py, &shard, PumpEvent::CancelTask { task });

            assert_eq!(reported.len(), 1);
            let unraisable = reported.get_item(0).unwrap();
            assert_eq!(
                unraisable
                    .getattr("exc_value")
                    .unwrap()
                    .str()
                    .unwrap()
                    .to_string_lossy(),
                "planned cancel failure"
            );
            assert!(
                unraisable
                    .getattr("object")
                    .unwrap()
                    .is(locals.get_item("task").unwrap().unwrap())
            );
            sys.setattr("unraisablehook", original_hook).unwrap();
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn abandoned_task_cancel_success_does_not_call_unraisable_hook() {
        Python::initialize();
        Python::attach(|py| {
            let locals = PyDict::new(py);
            let reported = PyList::empty(py);
            let sys = PyModule::import(py, "sys").unwrap();
            let original_hook = sys.getattr("unraisablehook").unwrap();
            sys.setattr("unraisablehook", reported.getattr("append").unwrap())
                .unwrap();
            py.run(
                pyo3::ffi::c_str!(
                    "class Task:\n\
                     \x20   def cancel(self):\n\
                     \x20       return True\n\
                     task = Task()"
                ),
                None,
                Some(&locals),
            )
            .unwrap();
            let task = locals.get_item("task").unwrap().unwrap().unbind();
            let shard = ShardHandle::test_stub(py);

            process_event(py, &shard, PumpEvent::CancelTask { task });

            assert!(reported.is_empty());
            sys.setattr("unraisablehook", original_hook).unwrap();
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn failed_task_outcome_without_a_waiter_reaches_unraisable_hook() {
        Python::initialize();
        Python::attach(|py| {
            let locals = PyDict::new(py);
            let reported = PyList::empty(py);
            let sys = PyModule::import(py, "sys").unwrap();
            let original_hook = sys.getattr("unraisablehook").unwrap();
            sys.setattr("unraisablehook", reported.getattr("append").unwrap())
                .unwrap();
            py.run(
                pyo3::ffi::c_str!(
                    "class Task:\n\
                     \x20   def result(self):\n\
                     \x20       raise RuntimeError('planned abandoned task failure')\n\
                     task = Task()"
                ),
                None,
                Some(&locals),
            )
            .unwrap();
            let shard = ShardHandle::test_stub(py);
            let slot = TaskSlot::<PyResult<Py<PyAny>>>::new();
            let waiter = Arc::clone(&slot).wait(Arc::clone(&shard));
            drop(waiter);
            let callback = Py::new(py, TaskDone {
                slot: Mutex::new(Some(DoneSlot::Value(Arc::clone(&slot)))),
            })
            .unwrap();
            callback
                .bind(py)
                .call1((locals.get_item("task").unwrap().unwrap(),))
                .unwrap();

            assert_eq!(reported.len(), 1);
            assert_eq!(
                reported
                    .get_item(0)
                    .unwrap()
                    .getattr("exc_value")
                    .unwrap()
                    .str()
                    .unwrap()
                    .to_string_lossy(),
                "planned abandoned task failure"
            );
            sys.setattr("unraisablehook", original_hook).unwrap();
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn failed_application_task_without_a_waiter_reaches_unraisable_hook() {
        Python::initialize();
        Python::attach(|py| {
            let locals = PyDict::new(py);
            let reported = PyList::empty(py);
            let sys = PyModule::import(py, "sys").unwrap();
            let original_hook = sys.getattr("unraisablehook").unwrap();
            sys.setattr("unraisablehook", reported.getattr("append").unwrap())
                .unwrap();
            py.run(
                pyo3::ffi::c_str!(
                    "class Task:\n\
                     \x20   def result(self):\n\
                     \x20       raise RuntimeError('planned abandoned application failure')\n\
                     task = Task()"
                ),
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            let app = test_fixtures::app_runtime(py);
            let slot = TaskSlot::with_guard(test_fixtures::request_task_guard(&app));
            let waiter = Arc::clone(&slot).wait(app.main_shard());
            drop(waiter);
            let callback = Py::new(py, TaskDone {
                slot: Mutex::new(Some(DoneSlot::App(Arc::clone(&slot)))),
            })
            .unwrap();
            callback
                .bind(py)
                .call1((locals.get_item("task").unwrap().unwrap(),))
                .unwrap();

            assert_eq!(reported.len(), 1);
            assert_eq!(
                reported
                    .get_item(0)
                    .unwrap()
                    .getattr("exc_value")
                    .unwrap()
                    .str()
                    .unwrap()
                    .to_string_lossy(),
                "planned abandoned application failure"
            );
            sys.setattr("unraisablehook", original_hook).unwrap();
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn cancelled_task_outcome_without_a_waiter_stays_silent() {
        Python::initialize();
        Python::attach(|py| {
            let locals = PyDict::new(py);
            let reported = PyList::empty(py);
            let sys = PyModule::import(py, "sys").unwrap();
            let original_hook = sys.getattr("unraisablehook").unwrap();
            sys.setattr("unraisablehook", reported.getattr("append").unwrap())
                .unwrap();
            py.run(
                pyo3::ffi::c_str!(
                    "import asyncio\n\
                     class Task:\n\
                     \x20   def result(self):\n\
                     \x20       raise asyncio.CancelledError('expected cancellation')\n\
                     task = Task()"
                ),
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            let shard = ShardHandle::test_stub(py);
            let slot = TaskSlot::<PyResult<Py<PyAny>>>::new();
            let waiter = Arc::clone(&slot).wait(Arc::clone(&shard));
            drop(waiter);
            let callback = Py::new(py, TaskDone {
                slot: Mutex::new(Some(DoneSlot::Value(Arc::clone(&slot)))),
            })
            .unwrap();
            callback
                .bind(py)
                .call1((locals.get_item("task").unwrap().unwrap(),))
                .unwrap();

            assert!(
                reported.is_empty(),
                "unexpected unraisable: {}",
                reported
                    .get_item(0)
                    .unwrap()
                    .getattr("exc_type")
                    .unwrap()
                    .getattr("__name__")
                    .unwrap()
                    .extract::<String>()
                    .unwrap()
            );
            sys.setattr("unraisablehook", original_hook).unwrap();
        });
    }

    #[test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "process_event consumes and reports the owned Python exception on its loop thread"
    )]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn completed_failure_abandoned_before_poll_reaches_unraisable_hook() {
        Python::initialize();
        Python::attach(|py| {
            let reported = PyList::empty(py);
            let sys = PyModule::import(py, "sys").unwrap();
            let original_hook = sys.getattr("unraisablehook").unwrap();
            sys.setattr("unraisablehook", reported.getattr("append").unwrap())
                .unwrap();
            let shard = ShardHandle::test_stub(py);
            let slot = TaskSlot::<PyResult<Py<PyAny>>>::new();
            let waiter = Arc::clone(&slot).wait(Arc::clone(&shard));

            fill_value_slot(
                py,
                &slot,
                Err(pyo3::exceptions::PyRuntimeError::new_err(
                    "planned completed failure",
                )),
                None,
            );
            drop(waiter);
            let event = shard
                .queue
                .lock()
                .pop_front()
                .expect("abandoning a completed error queues its report");
            process_event(py, &shard, event);

            assert_eq!(reported.len(), 1);
            assert_eq!(
                reported
                    .get_item(0)
                    .unwrap()
                    .getattr("exc_value")
                    .unwrap()
                    .str()
                    .unwrap()
                    .to_string_lossy(),
                "planned completed failure"
            );
            sys.setattr("unraisablehook", original_hook).unwrap();
        });
    }

    #[test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "process_event converts the native error under Python attachment on the owning loop"
    )]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn completed_application_failure_abandoned_before_poll_reaches_unraisable_hook() {
        Python::initialize();
        Python::attach(|py| {
            let reported = PyList::empty(py);
            let sys = PyModule::import(py, "sys").unwrap();
            let original_hook = sys.getattr("unraisablehook").unwrap();
            sys.setattr("unraisablehook", reported.getattr("append").unwrap())
                .unwrap();
            let app = test_fixtures::app_runtime(py);
            let slot = TaskSlot::with_guard(test_fixtures::request_task_guard(&app));
            let waiter = Arc::clone(&slot).wait(app.main_shard());
            slot.fill(Err(H2CornError::from(
                pyo3::exceptions::PyRuntimeError::new_err("planned completed app failure"),
            )))
            .expect("waiter accepts the first task outcome");

            drop(waiter);
            let event = app
                .main_shard()
                .queue
                .lock()
                .pop_front()
                .expect("abandoning a completed app error queues its report");
            process_event(py, &app.main_shard(), event);

            assert_eq!(reported.len(), 1);
            assert_eq!(
                reported
                    .get_item(0)
                    .unwrap()
                    .getattr("exc_value")
                    .unwrap()
                    .str()
                    .unwrap()
                    .to_string_lossy(),
                "planned completed app failure"
            );
            sys.setattr("unraisablehook", original_hook).unwrap();
        });
    }
}
