//! `RustFuture`: a Rust-owned object implementing the asyncio Future duck
//! protocol (`_asyncio_future_blocking` + the method surface consumed by
//! `Task.__step`/`Task.__wakeup`). Replaces per-await `asyncio.Future`s and
//! their `call_soon_threadsafe` wakeups: resolution happens in the pump,
//! which invokes the stored done-callbacks directly.

use std::fmt::{self, Formatter};
use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;
use pyo3::exceptions::asyncio::{CancelledError, InvalidStateError};
use pyo3::exceptions::{PyBaseException, PyStopIteration, PyTypeError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyTraceback, PyType};
use smallvec::SmallVec;
use tokio::task::AbortHandle;

use crate::pyloop::Shard;

/// Builds a future's result under the GIL on the loop thread.
pub(super) type Convert = Box<dyn for<'py> FnOnce(Python<'py>) -> PyResult<Py<PyAny>> + Send>;

/// Work delivered to the pump to resolve a pending [`RustFuture`].
pub(crate) enum ResolvePayload {
    /// A consumed event that must be given back if the future was cancelled
    /// while this payload was in flight (body events must never be lost —
    /// `wait_for(receive(), ...)` is a common pattern).
    Op(Box<dyn ResolveOp + Send>),
    /// A result with no compensation on cancellation (e.g. send-completion:
    /// once the message entered the channel it counts as sent).
    Simple(Convert),
}

/// Exclusive either-convert-or-requeue ownership of a consumed event.
pub(crate) trait ResolveOp {
    /// Build the Python result under the GIL (e.g. the `http.request` dict).
    fn convert(self: Box<Self>, py: Python<'_>) -> PyResult<Py<PyAny>>;
    /// Give the event back to its source after a cancellation race.
    fn requeue(self: Box<Self>);
}

impl ResolvePayload {
    fn convert(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Self::Op(op) => op.convert(py),
            Self::Simple(convert) => convert(py),
        }
    }

    fn requeue(self) {
        if let Self::Op(op) = self {
            op.requeue();
        }
    }
}

type Callbacks = SmallVec<[(Py<PyAny>, Py<PyAny>); 1]>;

enum FutureState {
    Pending {
        callbacks: Callbacks,
        callback_removal_revision: u64,
        abort: Option<AbortHandle>,
    },
    Ready(Py<PyAny>),
    Failed(Py<PyAny>),
    Cancelled {
        msg: Option<Py<PyAny>>,
    },
}

/// Duck future returned by slow-path `receive()`/`send()` and awaited by the
/// app task. Created pending; resolved exclusively by the pump.
#[pyclass(frozen, name = "_RustFuture")]
pub(crate) struct RustFuture {
    state: Mutex<FutureState>,
    blocking: AtomicBool,
    shard: Shard,
}

impl RustFuture {
    /// Attach the waiter task's abort handle as the Rust-side cancellation
    /// hook. Called once right after creation.
    pub(crate) fn set_abort(&self, handle: AbortHandle) {
        let mut state = self.state.lock();
        match &mut *state {
            FutureState::Pending { abort, .. } => {
                debug_assert!(abort.is_none(), "future waiter is installed once");
                *abort = Some(handle);
            },
            FutureState::Cancelled { .. } => {
                drop(state);
                handle.abort();
            },
            FutureState::Ready(_) | FutureState::Failed(_) => {},
        }
    }

    /// Pump-only: resolve and invoke stored callbacks directly. The caller
    /// must be a plain loop callback (never a running task).
    pub(super) fn resolve(self_: &Py<Self>, py: Python<'_>, payload: ResolvePayload) {
        let callbacks = {
            let mut state = self_.get().state.lock();
            match &mut *state {
                // Convert under the lock: all state transitions happen on
                // the loop thread and `convert` builds plain objects (never
                // re-entering this future), so no ordering window exists in
                // which a cancellation could lose the event.
                FutureState::Pending { callbacks, .. } => {
                    let callbacks = mem::take(callbacks);
                    *state = match payload.convert(py) {
                        Ok(value) => FutureState::Ready(value),
                        Err(err) => FutureState::Failed(err.value(py).clone().unbind().into_any()),
                    };
                    callbacks
                },
                // Cancelled while the payload was in flight: hand the event
                // back so the next receive() observes it.
                FutureState::Cancelled { .. } => {
                    drop(state);
                    payload.requeue();
                    return;
                },
                FutureState::Ready(_) | FutureState::Failed(_) => return,
            }
        };
        for (callback, context) in callbacks {
            // Mirrors asyncio Handle semantics: run each callback in the
            // context captured at registration time.
            if let Err(err) = context
                .bind(py)
                .call_method1(pyo3::intern!(py, "run"), (callback, self_))
            {
                err.write_unraisable(py, Some(self_.bind(py)));
            }
        }
    }

    fn take_step_value(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.current_result(py) {
            Ok(value) => Err(PyStopIteration::new_err((value,))),
            Err(err) => Err(err),
        }
    }

    fn current_result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &*self.state.lock() {
            FutureState::Pending { .. } => Err(InvalidStateError::new_err("result is not set")),
            FutureState::Ready(value) => Ok(value.clone_ref(py)),
            FutureState::Failed(exc) => Err(PyErr::from_value(exc.bind(py).clone())),
            FutureState::Cancelled { msg } => Err(cancelled_error(py, msg.as_ref())),
        }
    }
}

#[pymethods]
impl RustFuture {
    #[getter("_asyncio_future_blocking")]
    fn blocking(&self) -> bool {
        self.blocking.load(Ordering::Relaxed)
    }

    #[setter("_asyncio_future_blocking")]
    fn set_blocking(&self, value: bool) {
        self.blocking.store(value, Ordering::Relaxed);
    }

    fn get_loop(&self, py: Python<'_>) -> Py<PyAny> {
        self.shard.event_loop().clone_ref(py)
    }

    #[pyo3(signature = (callback, *, context = None))]
    fn add_done_callback(
        self_: &Bound<'_, Self>,
        callback: Py<PyAny>,
        context: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        let py = self_.py();
        let context = match context {
            Some(context) => context,
            None => copy_context(py)?,
        };
        {
            let mut state = self_.get().state.lock();
            if let FutureState::Pending { callbacks, .. } = &mut *state {
                callbacks.push((callback, context));
                return Ok(());
            }
        }
        schedule_done_callback(&self_.get().shard, py, &callback, self_.as_any(), &context)
    }

    fn remove_done_callback(&self, py: Python<'_>, callback: &Bound<'_, PyAny>) -> PyResult<usize> {
        loop {
            // Python equality is arbitrary user code: it can re-enter this
            // future and, on free-threaded builds, another thread can mutate
            // the callback list concurrently. Snapshot owned references under
            // the mutex, but perform every comparison after releasing it.
            let (revision, snapshot) = {
                let state = self.state.lock();
                let FutureState::Pending {
                    callbacks,
                    callback_removal_revision,
                    ..
                } = &*state
                else {
                    return Ok(0);
                };
                let revision = *callback_removal_revision;
                let snapshot = callbacks
                    .iter()
                    .map(|(existing, _)| (existing.clone_ref(py), false))
                    .collect::<SmallVec<[_; 1]>>();
                drop(state);
                (revision, snapshot)
            };

            let mut snapshot = snapshot;
            for (existing, keep) in &mut snapshot {
                *keep = !existing.bind(py).eq(callback)?;
            }

            let mut state = self.state.lock();
            let FutureState::Pending {
                callbacks,
                callback_removal_revision,
                ..
            } = &mut *state
            else {
                // Completion won the race and took ownership of the callback
                // list before this removal could commit.
                return Ok(0);
            };
            if *callback_removal_revision != revision {
                // Another removal changed the snapshot. Retry so the final
                // mutation applies to one coherent list. Adds only append and
                // deliberately survive a removal already in progress.
                drop(state);
                continue;
            }

            debug_assert!(callbacks.len() >= snapshot.len());
            let before = callbacks.len();
            let mut retired = Callbacks::new();
            let mut index = 0;
            callbacks.retain_mut(|entry| {
                let keep = snapshot.get(index).is_none_or(|entry| entry.1);
                index = index.wrapping_add(1);
                if !keep {
                    // `retain_mut` drops rejected slots in place, so move
                    // Python-owned references out before it does.
                    retired.push(mem::replace(entry, (py.None(), py.None())));
                }
                keep
            });
            let removed = before.wrapping_sub(callbacks.len());
            if removed != 0 {
                *callback_removal_revision = callback_removal_revision.wrapping_add(1);
            }
            drop(state);
            drop(retired);
            return Ok(removed);
        }
    }

    #[pyo3(signature = (msg = None))]
    fn cancel(self_: &Bound<'_, Self>, msg: Option<Py<PyAny>>) -> PyResult<bool> {
        let py = self_.py();
        let (callbacks, abort) = {
            let mut state = self_.get().state.lock();
            match &mut *state {
                FutureState::Pending {
                    callbacks, abort, ..
                } => {
                    let callbacks = mem::take(callbacks);
                    let abort = abort.take();
                    *state = FutureState::Cancelled { msg };
                    drop(state);
                    (callbacks, abort)
                },
                FutureState::Ready(_) | FutureState::Failed(_) | FutureState::Cancelled { .. } => {
                    return Ok(false);
                },
            }
        };
        if let Some(handle) = abort {
            handle.abort();
        }
        // cancel() is typically invoked from inside Task.__step — callbacks
        // MUST be deferred to a plain loop callback (`_enter_task` guard).
        for (callback, context) in callbacks {
            schedule_done_callback(&self_.get().shard, py, &callback, self_.as_any(), &context)?;
        }
        Ok(true)
    }

    fn result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.current_result(py)
    }

    fn exception(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let state = self.state.lock();
        match &*state {
            FutureState::Pending { .. } => Err(InvalidStateError::new_err("exception is not set")),
            FutureState::Ready(_) => Ok(None),
            FutureState::Failed(exc) => Ok(Some(exc.clone_ref(py))),
            FutureState::Cancelled { msg } => Err(cancelled_error(py, msg.as_ref())),
        }
    }

    fn done(&self) -> bool {
        !matches!(&*self.state.lock(), FutureState::Pending { .. })
    }

    fn cancelled(&self) -> bool {
        matches!(&*self.state.lock(), FutureState::Cancelled { .. })
    }

    const fn __await__(self_: Py<Self>) -> Py<Self> {
        self_
    }

    const fn __iter__(self_: Py<Self>) -> Py<Self> {
        self_
    }

    fn __next__(self_: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        let py = self_.py();
        let this = self_.get();
        if matches!(&*this.state.lock(), FutureState::Pending { .. }) {
            // First poll while pending: yield self with the blocking flag
            // set, so Task.__step registers __wakeup and suspends.
            this.blocking.store(true, Ordering::Relaxed);
            return Ok(self_.as_any().clone().unbind());
        }
        this.take_step_value(py)
    }

    #[pyo3(signature = (_value = None))]
    fn send(self_: &Bound<'_, Self>, _value: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        Self::__next__(self_)
    }

    #[pyo3(signature = (exc_type, exc_val = None, exc_tb = None))]
    fn throw(
        &self,
        exc_type: &Bound<'_, PyAny>,
        exc_val: Option<Py<PyAny>>,
        exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let py = exc_type.py();
        let traceback = match exc_tb {
            Some(traceback) => Some(
                traceback
                    .bind(py)
                    .cast::<PyTraceback>()
                    .map_err(|_| {
                        PyTypeError::new_err("throw() third argument must be a traceback object")
                    })?
                    .clone(),
            ),
            None => None,
        };

        let error = if exc_type.is_instance_of::<PyBaseException>() {
            if exc_val.is_some() {
                return Err(PyTypeError::new_err(
                    "instance exception may not have a separate value",
                ));
            }
            PyErr::from_value(exc_type.clone())
        } else {
            let Ok(exception_type) = exc_type.cast::<PyType>() else {
                return Err(invalid_throw_exception_type(exc_type)?);
            };
            if !exception_type.is_subclass_of::<PyBaseException>()? {
                return Err(invalid_throw_exception_type(exc_type)?);
            }

            match exc_val {
                None => PyErr::from_type(exception_type.clone(), ()),
                Some(value) => {
                    let value = value.bind(py);
                    if value.is_instance(exception_type.as_any())? {
                        PyErr::from_value(value.clone())
                    } else {
                        PyErr::from_type(exception_type.clone(), value.clone().unbind())
                    }
                },
            }
        };

        if let Some(traceback) = traceback {
            error.set_traceback(py, Some(traceback));
        }
        Err(error)
    }

    fn close(self_: &Bound<'_, Self>) -> PyResult<()> {
        Self::cancel(self_, None).map(drop)
    }
}

impl fmt::Debug for RustFuture {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let state = match &*self.state.lock() {
            FutureState::Pending { .. } => "pending",
            FutureState::Ready(_) => "ready",
            FutureState::Failed(_) => "failed",
            FutureState::Cancelled { .. } => "cancelled",
        };
        f.debug_struct("RustFuture")
            .field("state", &state)
            .finish_non_exhaustive()
    }
}

fn invalid_throw_exception_type(exc_type: &Bound<'_, PyAny>) -> PyResult<PyErr> {
    let type_name = exc_type.get_type().name()?.extract::<String>()?;
    Ok(PyTypeError::new_err(format!(
        "exceptions must be classes or instances deriving from BaseException, not {type_name}"
    )))
}

fn cancelled_error(py: Python<'_>, msg: Option<&Py<PyAny>>) -> PyErr {
    msg.map_or_else(
        || CancelledError::new_err(()),
        |msg| CancelledError::new_err((msg.clone_ref(py),)),
    )
}

fn copy_context(py: Python<'_>) -> PyResult<Py<PyAny>> {
    static COPY_CONTEXT: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    let func = COPY_CONTEXT.get_or_try_init(py, || {
        Ok::<_, PyErr>(py.import("contextvars")?.getattr("copy_context")?.unbind())
    })?;
    func.call0(py)
}

/// Create a pending future plus its resolve handle.
pub(crate) fn new_rust_future(py: Python<'_>, shard: Shard) -> PyResult<Py<RustFuture>> {
    Py::new(py, RustFuture {
        state: Mutex::new(FutureState::Pending {
            callbacks: SmallVec::new(),
            callback_removal_revision: 0,
            abort: None,
        }),
        blocking: AtomicBool::new(false),
        shard,
    })
}

/// Schedule `callback(future)` on the next loop iteration under `context`,
/// matching asyncio.Future semantics (and the re-entrancy rule: done-future
/// callbacks never run inline).
fn schedule_done_callback(
    shard: &Shard,
    py: Python<'_>,
    callback: &Py<PyAny>,
    future: &Bound<'_, PyAny>,
    context: &Py<PyAny>,
) -> PyResult<()> {
    let kwargs = PyDict::new(py);
    kwargs.set_item(pyo3::intern!(py, "context"), context)?;
    shard
        .call_soon()
        .bind(py)
        .call((callback.bind(py), future), Some(&kwargs))
        .map(drop)
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use pyo3::ffi::c_str;
    use pyo3::types::{PyAnyMethods as _, PyDict, PyDictMethods as _};
    use pyo3::{PyResult, Python};

    use super::{FutureState, new_rust_future};
    use crate::pyloop::ShardHandle;

    fn run_throw_test(py: Python<'_>, test: &CStr) -> PyResult<()> {
        let future = new_rust_future(py, ShardHandle::test_stub(py))?;
        let locals = PyDict::new(py);
        locals.set_item("future", &future)?;
        drop(future);
        py.run(
            c_str!(
                r#"
import warnings

async def dormant():
    await object()

def observe_native(*args):
    coroutine = dormant()
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            coroutine.throw(*args)
    except BaseException as error:
        return error
    else:
        raise AssertionError("native coroutine.throw() returned")
    finally:
        coroutine.close()

def observe_future(*args):
    try:
        future.throw(*args)
    except BaseException as error:
        return error
    else:
        raise AssertionError("_RustFuture.throw() returned")

def assert_matches_native(*args):
    expected = observe_native(*args)
    actual = observe_future(*args)
    assert type(actual) is type(expected), (type(actual), type(expected))
    assert actual.args == expected.args, (actual.args, expected.args)
    return expected, actual
"#
            ),
            Some(&locals),
            Some(&locals),
        )?;
        py.run(test, Some(&locals), Some(&locals))
    }

    #[test]
    fn callback_equality_can_reenter_done() {
        Python::initialize();
        Python::attach(|py| {
            let future = new_rust_future(py, ShardHandle::test_stub(py))
                .expect("test future must be created");
            let locals = PyDict::new(py);
            locals
                .set_item("future", &future)
                .expect("future must enter test locals");
            py.run(
                c_str!(
                    r#"
class ReentrantEquality:
    observed_done = None

    def __call__(self, future):
        pass

    def __eq__(self, other):
        self.observed_done = future.done()
        return True

registered = ReentrantEquality()
future.add_done_callback(registered)
assert future.remove_done_callback(object()) == 1
assert registered.observed_done is False
"#
                ),
                Some(&locals),
                Some(&locals),
            )
            .expect("reentrant equality must not deadlock");

            let state = future.get().state.lock();
            let FutureState::Pending { callbacks, .. } = &*state else {
                panic!("future must remain pending");
            };
            assert!(callbacks.is_empty());
            drop(state);
        });
    }

    #[test]
    fn removed_context_finalizer_can_reenter_future() {
        const CHILD_MARKER: &str = "H2CORN_CALLBACK_FINALIZER_CHILD";
        if std::env::var_os(CHILD_MARKER).is_some() {
            Python::initialize();
            Python::attach(|py| {
                let future = new_rust_future(py, ShardHandle::test_stub(py))
                    .expect("test future must be created");
                let locals = PyDict::new(py);
                locals
                    .set_item("future", &future)
                    .expect("future must enter test locals");
                py.run(
                    c_str!(
                        r#"
finalized = []

class ReenteringContext:
    def __del__(self):
        finalized.append(future.done())

callback = lambda _future: None
context = ReenteringContext()
future.add_done_callback(callback, context=context)
del context
assert finalized == []
assert future.remove_done_callback(callback) == 1
assert finalized == [False]
"#
                    ),
                    Some(&locals),
                    Some(&locals),
                )
                .expect("removed callback context must finalize outside the future lock");
            });
            return;
        }

        Python::initialize();
        Python::attach(|py| {
            let executable = std::env::current_exe().expect("test executable must be known");
            let locals = PyDict::new(py);
            locals
                .set_item("test_executable", executable.to_string_lossy().as_ref())
                .expect("test executable must enter child locals");
            py.run(
                c_str!(
                    r#"
import os
import subprocess

environment = os.environ.copy()
environment["H2CORN_CALLBACK_FINALIZER_CHILD"] = "1"
result = subprocess.run(
    [test_executable, "removed_context_finalizer_can_reenter_future"],
    capture_output=True,
    text=True,
    timeout=5,
    env=environment,
)
assert result.returncode == 0, (result.stdout, result.stderr)
"#
                ),
                Some(&locals),
                Some(&locals),
            )
            .expect("bounded child must prove finalizer reentry completes");
        });
    }

    #[test]
    fn concurrent_add_survives_remove_snapshot() {
        Python::initialize();
        Python::attach(|py| {
            let future = new_rust_future(py, ShardHandle::test_stub(py))
                .expect("test future must be created");
            let locals = PyDict::new(py);
            locals
                .set_item("future", &future)
                .expect("future must enter test locals");
            py.run(
                c_str!(
                    r#"
import threading

entered_equality = threading.Event()
release_equality = threading.Event()

class BlockingEquality:
    def __call__(self, future):
        pass

    def __eq__(self, other):
        entered_equality.set()
        if not release_equality.wait(5):
            raise AssertionError("concurrent callback registration did not run")
        return True

registered = BlockingEquality()
added_during_comparison = lambda future: None
future.add_done_callback(registered)

def add_callback():
    assert entered_equality.wait(5)
    future.add_done_callback(added_during_comparison)
    release_equality.set()

thread = threading.Thread(target=add_callback)
thread.start()
assert future.remove_done_callback(added_during_comparison) == 1
thread.join(5)
assert not thread.is_alive()
assert future.remove_done_callback(added_during_comparison) == 1
"#
                ),
                Some(&locals),
                Some(&locals),
            )
            .expect("concurrent add and removal must preserve the new callback");

            let state = future.get().state.lock();
            let FutureState::Pending { callbacks, .. } = &*state else {
                panic!("future must remain pending");
            };
            assert!(callbacks.is_empty());
            drop(state);
        });
    }

    #[test]
    fn concurrent_removals_commit_one_coherent_snapshot() {
        Python::initialize();
        Python::attach(|py| {
            let future = new_rust_future(py, ShardHandle::test_stub(py))
                .expect("test future must be created");
            let locals = PyDict::new(py);
            locals
                .set_item("future", &future)
                .expect("future must enter test locals");
            py.run(
                c_str!(
                    r#"
import threading

both_comparing = threading.Barrier(2)

class SynchronizesEquality:
    def __call__(self, future):
        pass

    def __eq__(self, other):
        both_comparing.wait(5)
        return True

future.add_done_callback(SynchronizesEquality())
target = object()
removed_counts = []

def remove_callback():
    removed_counts.append(future.remove_done_callback(target))

threads = [threading.Thread(target=remove_callback) for _ in range(2)]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join(5)
assert all(not thread.is_alive() for thread in threads)
assert sorted(removed_counts) == [0, 1]
"#
                ),
                Some(&locals),
                Some(&locals),
            )
            .expect("concurrent removals must not double-remove one registration");

            let state = future.get().state.lock();
            let FutureState::Pending { callbacks, .. } = &*state else {
                panic!("future must remain pending");
            };
            assert!(callbacks.is_empty());
            drop(state);
        });
    }

    #[test]
    fn equality_error_does_not_partially_remove_callbacks() {
        Python::initialize();
        Python::attach(|py| {
            let future = new_rust_future(py, ShardHandle::test_stub(py))
                .expect("test future must be created");
            let locals = PyDict::new(py);
            locals
                .set_item("future", &future)
                .expect("future must enter test locals");
            py.run(
                c_str!(
                    r#"
class Matches:
    def __call__(self, future):
        pass

    def __eq__(self, other):
        return True

class Raises:
    def __call__(self, future):
        pass

    def __eq__(self, other):
        raise RuntimeError("comparison failed")

first = Matches()
second = Raises()
future.add_done_callback(first)
future.add_done_callback(second)
try:
    future.remove_done_callback(object())
except RuntimeError as error:
    assert str(error) == "comparison failed"
else:
    raise AssertionError("comparison error was swallowed")
"#
                ),
                Some(&locals),
                Some(&locals),
            )
            .expect("comparison error must reach Python");

            let state = future.get().state.lock();
            let FutureState::Pending { callbacks, .. } = &*state else {
                panic!("future must remain pending");
            };
            assert_eq!(callbacks.len(), 2);
            drop(state);
        });
    }

    #[test]
    fn reentrant_cancel_wins_callback_removal_race() {
        Python::initialize();
        Python::attach(|py| {
            let event_loop = py
                .import("asyncio")
                .and_then(|asyncio| asyncio.call_method0("new_event_loop"))
                .expect("test event loop must be created");
            let shard =
                ShardHandle::from_event_loop(py, &event_loop).expect("test shard must be created");
            let future = new_rust_future(py, shard).expect("test future must be created");
            let locals = PyDict::new(py);
            locals
                .set_item("future", &future)
                .expect("future must enter test locals");
            locals
                .set_item("event_loop", &event_loop)
                .expect("loop must enter test locals");
            py.run(
                c_str!(
                    r#"
import asyncio

callback_calls = 0

class CancelsDuringEquality:
    def __call__(self, future):
        global callback_calls
        callback_calls += 1

    def __eq__(self, other):
        assert future.cancel()
        return True

registered = CancelsDuringEquality()
future.add_done_callback(registered)
assert future.remove_done_callback(object()) == 0
assert future.done() and future.cancelled()
event_loop.run_until_complete(asyncio.sleep(0))
assert callback_calls == 1
event_loop.close()
"#
                ),
                Some(&locals),
                Some(&locals),
            )
            .expect("completion must take callback ownership exactly once");
        });
    }

    #[test]
    fn throw_preserves_exception_instance() {
        Python::initialize();
        Python::attach(|py| {
            run_throw_test(
                py,
                c_str!(
                    r#"
error = ValueError("same instance")
expected, actual = assert_matches_native(error)
assert expected is error
assert actual is error
assert not future.done()
"#
                ),
            )
            .expect("throwing an exception instance must preserve its identity");
        });
    }

    #[test]
    fn throw_preserves_matching_exception_instance() {
        Python::initialize();
        Python::attach(|py| {
            run_throw_test(
                py,
                c_str!(
                    r#"
error = ValueError("same instance")
expected, actual = assert_matches_native(ValueError, error)
assert expected is error
assert actual is error
"#
                ),
            )
            .expect("a matching exception value must be preserved");
        });
    }

    #[test]
    fn throw_constructs_class_without_value_like_native_coroutine() {
        Python::initialize();
        Python::attach(|py| {
            run_throw_test(
                py,
                c_str!(
                    r#"
expected, actual = assert_matches_native(ValueError)
assert actual.args == ()
"#
                ),
            )
            .expect("an exception class without a value must be instantiated");
        });
    }

    #[test]
    fn throw_constructs_class_from_value_like_native_coroutine() {
        Python::initialize();
        Python::attach(|py| {
            run_throw_test(
                py,
                c_str!(
                    r#"
expected, actual = assert_matches_native(ValueError, "single value")
assert actual.args == ("single value",)
"#
                ),
            )
            .expect("an exception class must be called with its supplied value");
        });
    }

    #[test]
    fn throw_wraps_nonmatching_exception_instance_like_native_coroutine() {
        Python::initialize();
        Python::attach(|py| {
            run_throw_test(
                py,
                c_str!(
                    r#"
other_exception = TypeError("constructor argument")
expected, actual = assert_matches_native(ValueError, other_exception)
assert actual.args == (other_exception,)
assert actual.args[0] is other_exception
"#
                ),
            )
            .expect("a nonmatching exception instance must be passed to the class");
        });
    }

    #[test]
    fn throw_constructs_class_from_tuple_like_native_coroutine() {
        Python::initialize();
        Python::attach(|py| {
            run_throw_test(
                py,
                c_str!(
                    r#"
expected, actual = assert_matches_native(ValueError, ("first", "second"))
assert actual.args == ("first", "second")
"#
                ),
            )
            .expect("a tuple value must be unpacked as exception arguments");
        });
    }

    #[test]
    fn throw_attaches_traceback_like_native_coroutine() {
        Python::initialize();
        Python::attach(|py| {
            run_throw_test(
                py,
                c_str!(
                    r#"
try:
    raise LookupError("traceback source")
except LookupError as source:
    supplied_traceback = source.__traceback__

expected, actual = assert_matches_native(RuntimeError, "with traceback", supplied_traceback)

def contains_traceback(error, target):
    traceback = error.__traceback__
    while traceback is not None:
        if traceback is target:
            return True
        traceback = traceback.tb_next
    return False

assert contains_traceback(expected, supplied_traceback)
assert contains_traceback(actual, supplied_traceback)
"#
                ),
            )
            .expect("the supplied traceback must remain attached");
        });
    }

    #[test]
    fn throw_rejects_instance_with_separate_value_like_native_coroutine() {
        Python::initialize();
        Python::attach(|py| {
            run_throw_test(
                py,
                c_str!(
                    r#"
error = ValueError("instance")
expected, actual = assert_matches_native(error, "separate value")
"#
                ),
            )
            .expect("an exception instance cannot have a separate value");
        });
    }

    #[test]
    fn throw_rejects_non_exception_type_like_native_coroutine() {
        Python::initialize();
        Python::attach(|py| {
            run_throw_test(
                py,
                c_str!(
                    r#"
expected, actual = assert_matches_native(object)
"#
                ),
            )
            .expect("throw must reject non-exception types");
        });
    }

    #[test]
    fn throw_rejects_non_traceback_like_native_coroutine() {
        Python::initialize();
        Python::attach(|py| {
            run_throw_test(
                py,
                c_str!(
                    r#"
expected, actual = assert_matches_native(RuntimeError, "value", object())

class ConstructorFailure(Exception):
    def __new__(cls, *args):
        raise LookupError("constructor must not run")

expected, actual = assert_matches_native(ConstructorFailure, "value", object())
"#
                ),
            )
            .expect("throw must reject a non-traceback third argument");
        });
    }
}
