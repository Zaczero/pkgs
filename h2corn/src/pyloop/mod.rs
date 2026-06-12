//! Loop-affine batched pump: the only bridge between tokio threads and the
//! Python event loop.
//!
//! Tokio threads push [`PumpEvent`]s into a shard queue and ring a doorbell,
//! at most once per empty→non-empty transition. On POSIX the ring is a plain
//! `write(2)` on an fd the loop watches — tokio threads never attach to
//! Python on the request path; on Windows it is one coalesced
//! `call_soon_threadsafe(pump)` (see [`Doorbell`]). The pump runs as a plain
//! loop callback with the GIL already held: it builds scopes,
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
#[cfg(unix)]
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

pub use future::{AbortHook, ResolveOp, ResolvePayload, RustFuture, new_rust_future};
use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyAnyMethods, PyBool, PyTuple};
pub use slot::{SlotFuture, TaskSlot};

use crate::error::H2CornError;
use crate::python::py_vectorcall_kwnames;
use crate::runtime::AppState;

/// Maximum events processed per pump invocation before yielding back to the
/// loop via `call_soon`, so app callbacks and timers interleave fairly.
const PUMP_BATCH_MAX: usize = 64;

/// The tokio runtime owned by h2corn (replaces pyo3-async-runtimes' global).
static TOKIO_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

/// Per-request closure that builds `(scope, receive, send)` under the GIL on
/// the loop thread.
pub type BuildArgs = Box<
    dyn for<'py> FnOnce(
            Python<'py>,
            AppState,
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

/// Cross-thread pump wakeup, chosen per platform so the Linux hot path
/// stays a single `write(2)`:
///
/// - **Linux**: an eventfd watched via `loop.add_reader` — ring is one syscall,
///   no Python attach off the loop thread.
/// - **Other POSIX** (macOS, BSDs): a non-blocking pipe watched via
///   `loop.add_reader` (kqueue) — same architecture, different fd.
/// - **Windows**: asyncio's Proactor loop has no `add_reader`, so the ring is
///   `call_soon_threadsafe(pump)` — one coalesced Python attach per
///   empty→non-empty queue transition (never per event). The pump-only
///   invariant is unaffected: task creation and future resolution still happen
///   only inside the pump callback.
#[cfg(unix)]
struct Doorbell {
    #[cfg(target_os = "linux")]
    fd: OwnedFd,
    #[cfg(not(target_os = "linux"))]
    read: OwnedFd,
    #[cfg(not(target_os = "linux"))]
    write: OwnedFd,
}

#[cfg(unix)]
impl Doorbell {
    fn new() -> rustix::io::Result<Self> {
        #[cfg(target_os = "linux")]
        {
            use rustix::event::{EventfdFlags, eventfd};
            let fd = eventfd(0, EventfdFlags::CLOEXEC | EventfdFlags::NONBLOCK)?;
            Ok(Self { fd })
        }
        #[cfg(not(target_os = "linux"))]
        {
            // Apple platforms have no `pipe2`, so rustix offers no
            // `pipe_with` there: set CLOEXEC/NONBLOCK with explicit fcntls
            // (startup-only, once per shard).
            let (read, write) = rustix::pipe::pipe()?;
            for fd in [&read, &write] {
                rustix::io::fcntl_setfd(fd, rustix::io::FdFlags::CLOEXEC)?;
                rustix::fs::fcntl_setfl(fd, rustix::fs::OFlags::NONBLOCK)?;
            }
            Ok(Self { read, write })
        }
    }

    /// The fd the event loop watches with `add_reader`.
    fn reader_fd(&self) -> RawFd {
        #[cfg(target_os = "linux")]
        return self.fd.as_raw_fd();
        #[cfg(not(target_os = "linux"))]
        return self.read.as_raw_fd();
    }

    /// Ring from any thread with a plain `write(2)`. A full counter/pipe
    /// (`EAGAIN`) still leaves the fd readable, and a closing loop stops
    /// reading: both are safe to ignore.
    fn ring(&self) {
        #[cfg(target_os = "linux")]
        let _ = rustix::io::write(&self.fd, &1_u64.to_ne_bytes());
        #[cfg(not(target_os = "linux"))]
        let _ = rustix::io::write(&self.write, &[1_u8]);
    }

    /// Drain pending wakeups; called by the pump before processing. The
    /// `armed` flag coalesces rings, so one read clears everything on Linux
    /// (eventfd counter) and a short loop suffices on the pipe path.
    fn drain(&self) {
        #[cfg(target_os = "linux")]
        {
            let mut buf = [0_u8; 8];
            let _ = rustix::io::read(&self.fd, &mut buf);
        }
        #[cfg(not(target_os = "linux"))]
        {
            let mut buf = [0_u8; 64];
            while matches!(rustix::io::read(&self.read, &mut buf), Ok(n) if n == buf.len()) {}
        }
    }
}

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
    /// POSIX-only wakeup fd; see [`Doorbell`] for the per-platform ring
    /// strategy (Windows rings through `call_soon_threadsafe` instead).
    #[cfg(unix)]
    doorbell: Doorbell,
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
        #[cfg(unix)]
        let doorbell = Doorbell::new().map_err(|err| {
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
            #[cfg(unix)]
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
        // The pump doubles as the doorbell reader callback. On Windows there
        // is no fd to watch (Proactor has no `add_reader`); rings go through
        // `call_soon_threadsafe` instead.
        #[cfg(unix)]
        shard.event_loop.bind(py).call_method1(
            pyo3::intern!(py, "add_reader"),
            (shard.doorbell.reader_fd(), pump.bind(py)),
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
            #[cfg(unix)]
            doorbell: Doorbell::new().expect("test doorbell"),
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
        // Positional (coro,); keyword values follow in `task_kwnames`
        // order — ("loop", "eager_start", "name") on 3.12+, ("loop", "name")
        // on 3.11.
        let task_type = self.task_type.bind(py);
        let event_loop = self.event_loop.bind(py);
        let task_name = self.task_name.bind(py);
        let kwnames = self.task_kwnames.bind(py);
        if self.eager {
            let eager_start = PyBool::new(py, true).to_owned().into_any();
            py_vectorcall_kwnames(
                task_type,
                1,
                [coroutine, event_loop, &eager_start, task_name],
                kwnames,
            )
        } else {
            py_vectorcall_kwnames(task_type, 1, [coroutine, event_loop, task_name], kwnames)
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
    /// not already scheduled. On POSIX the ring is a single `write(2)` —
    /// tokio threads never attach to Python; on Windows it is one coalesced
    /// `call_soon_threadsafe(pump)` per empty→non-empty transition.
    pub fn push(&self, event: PumpEvent) {
        self.queue.lock().push_back(event);
        if !self.armed.swap(true, Ordering::AcqRel) {
            self.ring();
        }
    }

    fn ring(&self) {
        #[cfg(unix)]
        self.doorbell.ring();
        // A closed loop raises from `call_soon_threadsafe`: same benign
        // teardown case as a POSIX loop that stopped reading the fd.
        #[cfg(windows)]
        Python::attach(|py| {
            let _ = self.schedule_pump(py, &self.call_soon_threadsafe);
        });
    }

    /// Drain pending doorbell wakeups; called by the pump before processing.
    pub(super) fn drain_doorbell(&self) {
        #[cfg(unix)]
        self.doorbell.drain();
    }

    fn schedule_pump(&self, py: Python<'_>, scheduler: &Py<PyAny>) -> PyResult<()> {
        let pump = self
            .pump
            .get(py)
            .expect("pump is initialized before events are pushed");
        // pyo3's tuple `call1` compiles to `PyObject_CallOneArg` — the same
        // offset-flag vectorcall a manual FFI call would make. The returned
        // asyncio.Handle is dropped.
        scheduler.bind(py).call1((pump,)).map(drop)
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
            #[cfg(unix)]
            let removed = self.event_loop.bind(py).call_method1(
                pyo3::intern!(py, "remove_reader"),
                (self.doorbell.reader_fd(),),
            );
            #[cfg(windows)]
            let removed: PyResult<()> = Ok(());
            let result = removed
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
