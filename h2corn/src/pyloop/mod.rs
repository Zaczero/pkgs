//! Loop-affine batched pump: the only bridge between tokio threads and the
//! Python event loop.
//!
//! Tokio threads never attach to Python on the request path. They push
//! [`PumpEvent`]s into a shard queue and ring a doorbell — one
//! `call_soon_threadsafe(pump)` per empty→non-empty transition. The pump runs
//! as a plain loop callback with the GIL already held: it builds scopes,
//! vectorcalls the app, constructs eagerly-started `asyncio.Task`s, and
//! resolves [`RustFuture`]s by invoking their stored done-callbacks directly.
//!
//! Invariant (the `CPython` `_enter_task` re-entrancy guard): task creation and
//! duck-future resolution happen ONLY inside the pump callback — never inline
//! from app `send()`/`receive()` context, and never while another task is
//! being stepped.

mod future;
mod pump;
mod slot;

use std::collections::VecDeque;
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

pub use future::{AbortHook, ResolveOp, ResolvePayload, RustFuture, new_rust_future};
use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyAnyMethods, PyTuple};
pub use slot::{SlotFuture, TaskSlot};

use crate::error::H2CornError;
use crate::runtime::AppState;

/// Maximum events processed per pump invocation before yielding back to the
/// loop via `call_soon`, so app callbacks and timers interleave fairly.
const PUMP_BATCH_MAX: usize = 64;

/// The tokio runtime owned by h2corn (replaces pyo3-async-runtimes' global).
static TOKIO_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

/// Per-request closure that builds `(scope, receive, send)` under the GIL on
/// the loop thread. Same shape as the old `start_app_call` callback so call
/// sites stay unchanged.
pub type BuildArgs = Box<
    dyn for<'py> FnOnce(
            Python<'py>,
            &AppState,
            Shard,
        )
            -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>, Bound<'py, PyAny>)>
        + Send,
>;

/// Shards live for the worker's whole life (the event loop they wrap never
/// shuts down before process exit), so they are leaked to `'static` — every
/// per-request handle copy is then a plain pointer copy instead of
/// cross-thread `Arc` refcount traffic.
pub type Shard = &'static ShardHandle;

pub type SlotHandle<T> = std::sync::Arc<TaskSlot<T>>;

pub enum PumpEvent {
    /// Start an ASGI app call: build args, vectorcall the app, create an
    /// eager task, deliver the outcome through the slot.
    StartTask {
        app: AppState,
        build_args: BuildArgs,
        slot: SlotHandle<Result<(), H2CornError>>,
    },
    /// Resolve a pending duck future with a payload produced by Rust I/O.
    Resolve {
        fut: Py<RustFuture>,
        payload: ResolvePayload,
    },
    /// Run an arbitrary Python awaitable as a task and deliver its result
    /// (cold path: the shutdown trigger).
    SpawnAwaitable {
        awaitable: Py<PyAny>,
        slot: SlotHandle<PyResult<Py<PyAny>>>,
    },
}

/// One Python event loop plus everything needed to feed it from tokio
/// threads. Exactly one per worker on GIL builds; one per loop shard on
/// free-threaded builds.
pub struct ShardHandle {
    queue: Mutex<VecDeque<PumpEvent>>,
    armed: AtomicBool,
    /// Doorbell rung by tokio threads with a plain `write(2)` — no Python
    /// attach ever happens off the loop thread. The loop watches the fd via
    /// `loop.add_reader(fd, pump)`.
    doorbell: OwnedFd,
    call_soon_threadsafe: Py<PyAny>,
    call_soon: Py<PyAny>,
    event_loop: Py<PyAny>,
    /// `asyncio.tasks.Task` type object (C-accelerated).
    task_type: Py<PyAny>,
    /// Kwnames for direct Task construction: `("loop", "eager_start",
    /// "name")` on 3.12+, `("loop", "name")` on 3.11.
    task_kwnames: Py<PyTuple>,
    /// Interned `"h2corn.request"` task name.
    task_name: Py<PyAny>,
    /// `asyncio.ensure_future` (cold path: [`PumpEvent::SpawnAwaitable`]).
    ensure_future: Py<PyAny>,
    /// Whether Task construction passes `eager_start=True` (3.12+).
    eager: bool,
    /// The bound pump callable handed to `call_soon*`. Set once right after
    /// construction (the pump pyclass needs the shard and vice versa).
    pump: PyOnceLock<Py<PyAny>>,
}

impl ShardHandle {
    /// Capture the running event loop and cache every Python object the hot
    /// path needs. Must be called on the loop thread with the loop running.
    pub fn from_running_loop(py: Python<'_>) -> PyResult<Shard> {
        let asyncio = py.import("asyncio")?;
        let event_loop = asyncio.call_method0("get_running_loop")?;
        Self::from_event_loop(py, &event_loop)
    }

    /// Build a shard around an event loop that is not necessarily running
    /// yet (shard threads construct the handle before `run_forever`).
    pub fn from_event_loop(py: Python<'_>, event_loop: &Bound<'_, PyAny>) -> PyResult<Shard> {
        let asyncio = py.import("asyncio")?;
        let event_loop = event_loop.clone();
        let doorbell = rustix::event::eventfd(
            0,
            rustix::event::EventfdFlags::CLOEXEC | rustix::event::EventfdFlags::NONBLOCK,
        )
        .map_err(|err| {
            PyRuntimeError::new_err(format!("failed to create shard doorbell: {err}"))
        })?;
        let tasks_mod = py.import("asyncio.tasks")?;
        let task_type = tasks_mod.getattr("Task")?;
        let eager = py.version_info() >= (3, 12);
        let task_kwnames = if eager {
            PyTuple::new(py, ["loop", "eager_start", "name"])?
        } else {
            PyTuple::new(py, ["loop", "name"])?
        };

        let call_soon_threadsafe = event_loop.getattr("call_soon_threadsafe")?.unbind();
        let call_soon = event_loop.getattr("call_soon")?.unbind();
        let shard: Shard = Box::leak(Box::new(Self {
            queue: Mutex::new(VecDeque::new()),
            armed: AtomicBool::new(false),
            doorbell,
            call_soon_threadsafe,
            call_soon,
            event_loop: event_loop.unbind(),
            task_type: task_type.unbind(),
            task_kwnames: task_kwnames.unbind(),
            task_name: pyo3::intern!(py, "h2corn.request")
                .clone()
                .into_any()
                .unbind(),
            ensure_future: asyncio.getattr("ensure_future")?.unbind(),
            eager,
            pump: PyOnceLock::new(),
        }));

        let pump = pump::Pump::into_callable(py, shard)?;
        // The pump doubles as the doorbell reader callback.
        shard.event_loop.bind(py).call_method1(
            pyo3::intern!(py, "add_reader"),
            (shard.doorbell.as_raw_fd(), pump.bind(py)),
        )?;
        shard
            .pump
            .set(py, pump)
            .map_err(|_| PyRuntimeError::new_err("pump already initialized"))?;
        Ok(shard)
    }

    /// Test-only shard whose Python fields are inert placeholders; suitable
    /// for code paths that carry a shard without scheduling on it.
    #[cfg(test)]
    pub(crate) fn test_stub(py: Python<'_>) -> Shard {
        Box::leak(Box::new(Self {
            queue: Mutex::new(VecDeque::new()),
            armed: AtomicBool::new(false),
            doorbell: rustix::event::eventfd(
                0,
                rustix::event::EventfdFlags::CLOEXEC | rustix::event::EventfdFlags::NONBLOCK,
            )
            .expect("test eventfd"),
            call_soon_threadsafe: py.None(),
            call_soon: py.None(),
            event_loop: py.None(),
            task_type: py.None(),
            task_kwnames: PyTuple::empty(py).unbind(),
            task_name: py.None(),
            ensure_future: py.None(),
            eager: false,
            pump: PyOnceLock::new(),
        }))
    }

    /// Construct `asyncio.tasks.Task(coro, loop=..., [eager_start=True,]
    /// name="h2corn.request")` via a single vectorcall with cached kwnames.
    /// Pump-only: must run from a plain loop callback.
    pub(super) fn construct_task<'py>(
        &self,
        py: Python<'py>,
        coroutine: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Positional: (coro,); keyword values follow in `task_kwnames`
        // order — ("loop", "eager_start", "name") on 3.12+, ("loop", "name")
        // on 3.11 (the unused tail slot is simply not read).
        let args: [*mut ffi::PyObject; 4] = if self.eager {
            [
                coroutine.as_ptr(),
                self.event_loop.as_ptr(),
                // SAFETY: `Py_True` returns the immortal True singleton.
                unsafe { ffi::Py_True() },
                self.task_name.as_ptr(),
            ]
        } else {
            [
                coroutine.as_ptr(),
                self.event_loop.as_ptr(),
                self.task_name.as_ptr(),
                std::ptr::null_mut(),
            ]
        };
        // SAFETY: the GIL is held; all read pointers are live borrowed
        // objects; `task_kwnames` names exactly the keyword values that
        // follow the single positional argument; the call returns a new
        // owned reference or sets a Python exception.
        unsafe {
            Bound::from_owned_ptr_or_err(
                py,
                ffi::PyObject_Vectorcall(
                    self.task_type.as_ptr(),
                    args.as_ptr().cast_mut(),
                    1,
                    self.task_kwnames.as_ptr(),
                ),
            )
        }
    }

    /// Cold path: `asyncio.ensure_future(awaitable, loop=...)`.
    pub(super) fn ensure_future<'py>(
        &self,
        py: Python<'py>,
        awaitable: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        use pyo3::types::PyDict;
        let kwargs = PyDict::new(py);
        kwargs.set_item(pyo3::intern!(py, "loop"), self.event_loop.bind(py))?;
        self.ensure_future
            .bind(py)
            .call((awaitable,), Some(&kwargs))
    }

    pub(super) fn disarm(&self) {
        self.armed.store(false, Ordering::Release);
    }

    /// Schedule another pump pass on the same thread (remainder batches),
    /// suppressing redundant cross-thread doorbells from producers.
    pub(super) fn reschedule_local(&self, py: Python<'_>) {
        if !self.armed.swap(true, Ordering::AcqRel) {
            let _ = self.schedule_pump(py, &self.call_soon);
        }
    }

    /// Enqueue an event from any thread and ring the doorbell if the pump is
    /// not already scheduled. The ring is a single `write(2)` on the
    /// eventfd — tokio threads never attach to Python.
    pub fn push(&self, event: PumpEvent) {
        self.queue.lock().push_back(event);
        if !self.armed.swap(true, Ordering::AcqRel) {
            self.ring();
        }
    }

    fn ring(&self) {
        // A full eventfd counter (EAGAIN) still leaves the fd readable, and
        // a closing loop stops reading: both are safe to ignore.
        let _ = rustix::io::write(&self.doorbell, &1_u64.to_ne_bytes());
    }

    /// Drain the doorbell counter; called by the pump before processing.
    pub(super) fn drain_doorbell(&self) {
        let mut buf = [0_u8; 8];
        let _ = rustix::io::read(&self.doorbell, &mut buf);
    }

    fn schedule_pump(&self, py: Python<'_>, scheduler: &Py<PyAny>) -> PyResult<()> {
        let pump = self
            .pump
            .get(py)
            .expect("pump is initialized before events are pushed");
        // SAFETY: the GIL is held; both pointers are live owned objects;
        // `PyObject_CallOneArg` returns a new reference or sets an exception.
        let result = unsafe { ffi::PyObject_CallOneArg(scheduler.as_ptr(), pump.as_ptr()) };
        if result.is_null() {
            return Err(PyErr::fetch(py));
        }
        // SAFETY: non-null result from a successful call is a new owned
        // reference (an asyncio.Handle we do not need).
        unsafe { ffi::Py_DECREF(result) };
        Ok(())
    }

    pub(crate) const fn event_loop(&self) -> &Py<PyAny> {
        &self.event_loop
    }

    pub(crate) const fn call_soon(&self) -> &Py<PyAny> {
        &self.call_soon
    }

    /// Stop this shard's event loop from any thread. Errors are swallowed:
    /// a loop already closed during teardown is the expected benign case.
    pub fn stop(&self) {
        Python::attach(|py| {
            let result = self
                .event_loop
                .bind(py)
                .call_method1(
                    pyo3::intern!(py, "remove_reader"),
                    (self.doorbell.as_raw_fd(),),
                )
                .and_then(|_| self.event_loop.bind(py).getattr(pyo3::intern!(py, "stop")))
                .and_then(|stop| self.call_soon_threadsafe.bind(py).call1((stop,)));
            drop(result);
        });
    }

    /// Drain up to [`PUMP_BATCH_MAX`] events; report whether more remain.
    fn drain_batch(&self, batch: &mut Vec<PumpEvent>) -> bool {
        let mut queue = self.queue.lock();
        let take = queue.len().min(PUMP_BATCH_MAX);
        batch.extend(queue.drain(..take));
        !queue.is_empty()
    }
}

/// A loop shard running on its own Python thread (free-threaded builds).
pub struct ShardThread {
    pub shard: Shard,
    join: std::thread::JoinHandle<()>,
}

impl ShardThread {
    /// Stop the shard's loop and join its thread.
    pub fn shutdown(self) {
        self.shard.stop();
        let _ = self.join.join();
    }
}

/// Spawn a dedicated Python thread running a fresh event loop with its own
/// pump. Returns once the shard handle is constructed; the loop runs until
/// [`ShardThread::shutdown`].
pub fn spawn_shard_thread(index: usize) -> PyResult<ShardThread> {
    let (tx, rx) = std::sync::mpsc::channel::<PyResult<Shard>>();
    let join = std::thread::Builder::new()
        .name(format!("h2corn-loop-{index}"))
        .spawn(move || {
            Python::attach(|py| {
                let setup = || -> PyResult<(Shard, Py<PyAny>)> {
                    let asyncio = py.import("asyncio")?;
                    let event_loop = asyncio.call_method0("new_event_loop")?;
                    asyncio.call_method1("set_event_loop", (&event_loop,))?;
                    let shard = ShardHandle::from_event_loop(py, &event_loop)?;
                    Ok((shard, event_loop.unbind()))
                };
                match setup() {
                    Ok((shard, event_loop)) => {
                        let _ = tx.send(Ok(shard));
                        // Blocks this thread until ShardHandle::stop.
                        let result = event_loop.bind(py).call_method0("run_forever");
                        drop(result);
                        let _ = event_loop.bind(py).call_method0("close");
                    },
                    Err(err) => {
                        let _ = tx.send(Err(err));
                    },
                }
            });
        })
        .map_err(|err| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("failed to spawn loop thread: {err}"))
        })?;
    let shard = rx.recv().map_err(|_| {
        pyo3::exceptions::PyRuntimeError::new_err("loop thread exited before initializing")
    })??;
    Ok(ShardThread { shard, join })
}

/// Initialize the global tokio runtime exactly once.
pub fn init_runtime(
    build: impl FnOnce() -> tokio::runtime::Runtime,
) -> &'static tokio::runtime::Runtime {
    TOKIO_RUNTIME.get_or_init(build)
}

/// The global tokio runtime. Panics if [`init_runtime`] has not run.
pub fn runtime() -> &'static tokio::runtime::Runtime {
    TOKIO_RUNTIME
        .get()
        .expect("tokio runtime is initialized during server startup")
}
