//! `RustFuture`: a Rust-owned object implementing the asyncio Future duck
//! protocol (`_asyncio_future_blocking` + the method surface consumed by
//! `Task.__step`/`Task.__wakeup`). Replaces per-await `asyncio.Future`s and
//! their `call_soon_threadsafe` wakeups: resolution happens in the pump,
//! which invokes the stored done-callbacks directly.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;
use pyo3::exceptions::PyStopIteration;
use pyo3::exceptions::asyncio::{CancelledError, InvalidStateError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyTuple};
use smallvec::SmallVec;

use super::Shard;

/// Builds a future's result under the GIL on the loop thread.
pub type Convert = Box<dyn for<'py> FnOnce(Python<'py>) -> PyResult<Py<PyAny>> + Send>;

/// Work delivered to the pump to resolve a pending [`RustFuture`].
pub enum ResolvePayload {
    /// A consumed event that must be given back if the future was cancelled
    /// while this payload was in flight (body events must never be lost —
    /// `wait_for(receive(), ...)` is a common pattern).
    Op(Box<dyn ResolveOp + Send>),
    /// A result with no compensation on cancellation (e.g. send-completion:
    /// once the message entered the channel it counts as sent).
    Simple(Convert),
}

/// Exclusive either-convert-or-requeue ownership of a consumed event.
pub trait ResolveOp {
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

/// What `cancel()` aborts on the Rust side.
pub enum AbortHook {
    None,
    /// Abort the tokio waiter task feeding this future. `AbortHandle::abort`
    /// is sync and thread-safe; mpsc `recv().await` is cancel-safe, so an
    /// aborted waiter never consumes an event.
    Tokio(tokio::task::AbortHandle),
}

type Callbacks = SmallVec<[(Py<PyAny>, Py<PyAny>); 1]>;

enum FutState {
    Pending { callbacks: Callbacks },
    Ready(Py<PyAny>),
    Failed(Py<PyAny>),
    Cancelled { msg: Option<Py<PyAny>> },
}

struct FutShared {
    state: Mutex<FutState>,
    abort: Mutex<AbortHook>,
}

/// Duck future returned by slow-path `receive()`/`send()` and awaited by the
/// app task. Created pending; resolved exclusively by the pump.
#[pyclass(frozen)]
pub struct RustFuture {
    shared: Arc<FutShared>,
    blocking: AtomicBool,
    shard: Shard,
}

impl RustFuture {
    /// Attach the Rust-side cancellation hook (e.g. the waiter task's abort
    /// handle). Called once right after creation.
    pub fn set_abort(&self, hook: AbortHook) {
        *self.shared.abort.lock() = hook;
    }

    /// Pump-only: resolve and invoke stored callbacks directly. The caller
    /// must be a plain loop callback (never a running task).
    pub(super) fn resolve(self_: &Py<Self>, py: Python<'_>, payload: ResolvePayload) {
        let this = self_.get();
        let callbacks = {
            let mut state = this.shared.state.lock();
            match &mut *state {
                // Convert under the lock: all state transitions happen on
                // the loop thread and `convert` builds plain objects (never
                // re-entering this future), so no ordering window exists in
                // which a cancellation could lose the event.
                FutState::Pending { callbacks } => {
                    let callbacks = std::mem::take(callbacks);
                    *state = match payload.convert(py) {
                        Ok(value) => FutState::Ready(value),
                        Err(err) => FutState::Failed(err.value(py).clone().unbind().into_any()),
                    };
                    callbacks
                },
                // Cancelled while the payload was in flight: hand the event
                // back so the next receive() observes it.
                FutState::Cancelled { .. } => {
                    drop(state);
                    payload.requeue();
                    return;
                },
                FutState::Ready(_) | FutState::Failed(_) => return,
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
        self.current_result(py)
            .map_or_else(Err, |value| Err(PyStopIteration::new_err((value,))))
    }

    fn current_result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &*self.shared.state.lock() {
            FutState::Pending { .. } => Err(InvalidStateError::new_err("result is not set")),
            FutState::Ready(value) => Ok(value.clone_ref(py)),
            FutState::Failed(exc) => Err(PyErr::from_value(exc.bind(py).clone())),
            FutState::Cancelled { msg } => Err(cancelled_error(py, msg.as_ref())),
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
        let this = self_.get();
        let context = match context {
            Some(context) => context,
            None => copy_context(py)?,
        };
        {
            let mut state = this.shared.state.lock();
            if let FutState::Pending { callbacks } = &mut *state {
                callbacks.push((callback, context));
                return Ok(());
            }
        }
        // Done futures schedule the callback for the next loop iteration,
        // matching asyncio.Future semantics (and the re-entrancy rule).
        let kwargs = PyDict::new(py);
        kwargs.set_item(pyo3::intern!(py, "context"), context)?;
        this.shard.call_soon().bind(py).call(
            PyTuple::new(py, [callback.bind(py), self_.as_any()])?,
            Some(&kwargs),
        )?;
        Ok(())
    }

    fn remove_done_callback(&self, py: Python<'_>, callback: &Bound<'_, PyAny>) -> PyResult<usize> {
        let mut error = None;
        let removed = {
            let mut state = self.shared.state.lock();
            if let FutState::Pending { callbacks } = &mut *state {
                let before = callbacks.len();
                callbacks.retain(|(existing, _)| match existing.bind(py).eq(callback) {
                    Ok(equal) => !equal,
                    Err(err) => {
                        error.get_or_insert(err);
                        true
                    },
                });
                before - callbacks.len()
            } else {
                0
            }
        };
        error.map_or(Ok(removed), Err)
    }

    #[pyo3(signature = (msg = None))]
    fn cancel(self_: &Bound<'_, Self>, msg: Option<Py<PyAny>>) -> PyResult<bool> {
        let py = self_.py();
        let this = self_.get();
        let callbacks = {
            let mut state = this.shared.state.lock();
            match &mut *state {
                FutState::Pending { callbacks } => {
                    let callbacks = std::mem::take(callbacks);
                    *state = FutState::Cancelled { msg };
                    callbacks
                },
                _ => return Ok(false),
            }
        };
        let hook = std::mem::replace(&mut *this.shared.abort.lock(), AbortHook::None);
        match hook {
            AbortHook::None => {},
            AbortHook::Tokio(handle) => handle.abort(),
        }
        // cancel() is typically invoked from inside Task.__step — callbacks
        // MUST be deferred to a plain loop callback (`_enter_task` guard).
        for (callback, context) in callbacks {
            let kwargs = PyDict::new(py);
            kwargs.set_item(pyo3::intern!(py, "context"), context)?;
            this.shard.call_soon().bind(py).call(
                PyTuple::new(py, [callback.bind(py), self_.as_any()])?,
                Some(&kwargs),
            )?;
        }
        Ok(true)
    }

    fn result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.current_result(py)
    }

    fn exception(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let state = self.shared.state.lock();
        match &*state {
            FutState::Pending { .. } => Err(InvalidStateError::new_err("exception is not set")),
            FutState::Ready(_) => Ok(None),
            FutState::Failed(exc) => Ok(Some(exc.clone_ref(py))),
            FutState::Cancelled { msg } => Err(cancelled_error(py, msg.as_ref())),
        }
    }

    fn done(&self) -> bool {
        !matches!(&*self.shared.state.lock(), FutState::Pending { .. })
    }

    fn cancelled(&self) -> bool {
        matches!(&*self.shared.state.lock(), FutState::Cancelled { .. })
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
        if matches!(&*this.shared.state.lock(), FutState::Pending { .. }) {
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
        let _ = (exc_val, exc_tb);
        Err(PyErr::from_value(
            exc_type.call0().unwrap_or_else(|_| exc_type.clone()),
        ))
    }

    fn close(&self) {
        let mut state = self.shared.state.lock();
        if matches!(&*state, FutState::Pending { .. }) {
            *state = FutState::Cancelled { msg: None };
        }
    }
}

impl std::fmt::Debug for RustFuture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match &*self.shared.state.lock() {
            FutState::Pending { .. } => "pending",
            FutState::Ready(_) => "ready",
            FutState::Failed(_) => "failed",
            FutState::Cancelled { .. } => "cancelled",
        };
        f.debug_struct("RustFuture")
            .field("state", &state)
            .finish_non_exhaustive()
    }
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
pub fn new_rust_future(py: Python<'_>, shard: Shard) -> PyResult<Py<RustFuture>> {
    Py::new(py, RustFuture {
        shared: Arc::new(FutShared {
            state: Mutex::new(FutState::Pending {
                callbacks: SmallVec::new(),
            }),
            abort: Mutex::new(AbortHook::None),
        }),
        blocking: AtomicBool::new(false),
        shard,
    })
}
