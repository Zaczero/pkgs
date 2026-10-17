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

use std::any::Any;
use std::collections::VecDeque;
#[cfg(unix)]
use std::os::fd::{AsRawFd as _, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::sync::{Arc, OnceLock};
use std::thread::{Builder as ThreadBuilder, JoinHandle};

use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBool, PyDict};
#[cfg(unix)]
use rustix::io::{Result as RustixResult, read, retry_on_intr, write};
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio::task::JoinHandle as TokioJoinHandle;

use crate::app_call::AppCallArgs;
use crate::bridge::ReadyNone;
use crate::error::{H2CornError, unexpected_join_error};
pub(crate) use crate::pyloop::future::{ResolveOp, ResolvePayload, RustFuture, new_rust_future};
pub(crate) use crate::pyloop::slot::{SlotFuture, TaskSlot};
use crate::runtime::{AppRuntimeHandle, RequestTaskGuard};

/// Maximum events processed per pump invocation before yielding back to the
/// loop via `call_soon`, so app callbacks and timers interleave fairly.
const PUMP_BATCH_MAX: usize = 64;

/// The process-global Tokio runtime owned by h2corn.
static TOKIO_RUNTIME: OnceLock<TokioRuntime> = OnceLock::new();

struct TokioRuntime {
    worker_threads: usize,
    runtime: Runtime,
}

pub(crate) type BuildAwaitable =
    Box<dyn for<'py> FnOnce(Python<'py>, Shard) -> PyResult<Py<PyAny>> + Send>;

pub(crate) enum UnraisableError {
    Python(PyErr),
    H2Corn(H2CornError),
}

/// Scoped ownership of one loop-affine bridge. Requests clone this handle only
/// when they cross an ownership boundary; the pump keeps a `Weak` reference so
/// a completed embedded `serve()` can release the doorbell and Python caches.
pub(crate) type Shard = Arc<ShardHandle>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SecondaryLoopFactory {
    Asyncio,
    Uvloop,
    /// An embedded/custom main loop with no safe general constructor. Such a
    /// server remains single-loop instead of silently mixing implementations.
    Custom,
}

impl SecondaryLoopFactory {
    fn from_module(module: &str) -> Self {
        if module == "uvloop" || module.starts_with("uvloop.") {
            Self::Uvloop
        } else if module == "asyncio" || module.starts_with("asyncio.") {
            Self::Asyncio
        } else {
            Self::Custom
        }
    }

    fn classify(event_loop: &Bound<'_, PyAny>) -> PyResult<Self> {
        let module_obj = event_loop.get_type().module()?;
        let module: &str = module_obj.extract()?;
        Ok(Self::from_module(module))
    }

    fn create(self, py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
        match self {
            Self::Asyncio => py.import("asyncio")?.call_method0("new_event_loop"),
            Self::Uvloop => py.import("uvloop")?.call_method0("new_event_loop"),
            Self::Custom => Err(PyRuntimeError::new_err(
                "custom event loops cannot create secondary loop threads",
            )),
        }
    }
}

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
    fn new() -> RustixResult<Self> {
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
    ///
    /// `EINTR` is not: the caller has already set `armed`, so later producers
    /// will not ring again, and an interrupted write that never delivered its
    /// byte would leave the queue with nothing to wake the pump.
    fn ring(&self) {
        #[cfg(target_os = "linux")]
        let _write_result = retry_on_intr(|| write(&self.fd, &1_u64.to_ne_bytes()));
        #[cfg(not(target_os = "linux"))]
        let _write_result = retry_on_intr(|| write(&self.write, &[1_u8]));
    }

    /// Drain pending wakeups; called by the pump before processing. The
    /// `armed` flag coalesces rings, so one read clears everything on Linux
    /// (eventfd counter) and a short loop suffices on the pipe path.
    fn drain(&self) {
        #[cfg(target_os = "linux")]
        {
            let mut buf = [0_u8; 8];
            let _read_result = read(&self.fd, &mut buf);
        }
        #[cfg(not(target_os = "linux"))]
        {
            let mut buf = [0_u8; 64];
            while matches!(read(&self.read, &mut buf), Ok(n) if n == buf.len()) {}
        }
    }
}

pub(crate) enum PumpEvent {
    /// Start an ASGI app call: build args, vectorcall the app, create an
    /// eager task, deliver the outcome through the slot.
    StartTask {
        args: Box<AppCallArgs>,
        slot: Arc<TaskSlot<Result<(), H2CornError>, RequestTaskGuard>>,
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
        slot: Arc<TaskSlot<PyResult<Py<PyAny>>>>,
    },
    /// Construct and run an awaitable on this loop. Used for transactional
    /// secondary-loop lifecycle operations; construction is loop-affine.
    CallAwaitable {
        build: BuildAwaitable,
        slot: Arc<TaskSlot<PyResult<Py<PyAny>>>>,
    },
    /// Drop a fallback shared-app owner on the main loop, then acknowledge so
    /// secondary shards can be stopped without cross-loop final destruction.
    ReleaseApp {
        app: AppRuntimeHandle,
        done: oneshot::Sender<()>,
    },
    /// Cancel an abandoned Python task on the event loop that owns it.
    CancelTask { task: Py<PyAny> },
    /// Report a failed slot result whose waiter was abandoned after completion.
    ReportUnraisable { error: UnraisableError },
    /// Call a zero-argument Python callable on the loop thread — the
    /// supervisor notifications (ready, retire) whose Rust-side trigger can
    /// fire from any tokio thread, which must never attach to Python.
    CallTrigger { trigger: Py<PyAny> },
    /// Unregister this shard's doorbell from its owning loop. Used by the
    /// caller-owned main loop when an embedded serve completes.
    Detach,
    /// Unregister the doorbell and stop a dedicated secondary loop. The event
    /// is processed on that loop, never through cross-thread Python calls.
    StopLoop,
}

/// One Python event loop plus everything needed to feed it from tokio
/// threads. Exactly one per worker on GIL builds; one per loop shard on
/// free-threaded builds.
pub(crate) struct ShardHandle {
    queue: Mutex<VecDeque<PumpEvent>>,
    armed: AtomicBool,
    /// POSIX-only wakeup fd; see [`Doorbell`] for the per-platform ring
    /// strategy (Windows rings through `call_soon_threadsafe` instead).
    #[cfg(unix)]
    doorbell: Doorbell,
    #[cfg(windows)]
    call_soon_threadsafe: Py<PyAny>,
    call_soon: Py<PyAny>,
    event_loop: Py<PyAny>,
    task_factory_accessor: TaskFactoryAccessor,
    /// `asyncio.tasks.Task` type object (C-accelerated).
    task_type: Py<PyAny>,
    /// Immutable keyword arguments reused by every Task construction on this
    /// shard. CPython's type call must materialize the positional tuple, but
    /// the loop/eager/name dictionary need not be rebuilt per request.
    task_kwargs: Py<PyDict>,
    /// Arguments for the loop's `create_task` path, without its bound `loop`.
    create_task_kwargs: Py<PyDict>,
    /// `asyncio.ensure_future` (cold path: [`PumpEvent::SpawnAwaitable`]).
    ensure_future: Py<PyAny>,
    /// The bound pump callable handed to `call_soon*`. Set once right after
    /// construction (the pump pyclass needs the shard and vice versa).
    pump: PyOnceLock<Py<PyAny>>,
    /// Cached `None`-resolving awaitable for synchronously-successful `send()`;
    /// handed out by reference, so a buffered send allocates nothing. Every
    /// ASGI response performs ≥2 sends, so this is the highest-frequency
    /// per-request awaitable — a shared constant beats allocating each time.
    ready_none: Py<ReadyNone>,
    /// Mutable lifespan state owned by this loop. The pump shallow-copies it
    /// into each request scope; no cross-loop asyncio object is shared.
    scope_state: PyOnceLock<Py<PyDict>>,
}

enum TaskFactoryAccessor {
    Stock,
    Custom {
        get_task_factory: Py<PyAny>,
        stock_create_task: bool,
    },
}

impl ShardHandle {
    /// Capture the running event loop and cache every Python object the hot
    /// path needs. Must be called on the loop thread with the loop running.
    pub(crate) fn from_running_loop(py: Python<'_>) -> PyResult<(Shard, SecondaryLoopFactory)> {
        let asyncio = py.import("asyncio")?;
        let event_loop = asyncio.call_method0("get_running_loop")?;
        let factory = SecondaryLoopFactory::classify(&event_loop)?;
        Ok((Self::from_event_loop(py, &event_loop)?, factory))
    }

    /// Build a shard around an event loop that is not necessarily running
    /// yet (shard threads construct the handle before `run_forever`).
    pub(crate) fn from_event_loop(
        py: Python<'_>,
        event_loop: &Bound<'_, PyAny>,
    ) -> PyResult<Shard> {
        let asyncio = py.import("asyncio")?;
        let event_loop = event_loop.clone();
        let base_event_loop = asyncio.getattr("BaseEventLoop")?;
        let base_get_task_factory = base_event_loop.getattr("get_task_factory")?;
        let base_create_task = base_event_loop.getattr("create_task")?;
        let event_loop_type = event_loop.get_type();
        let stock_get_task_factory = event_loop_type
            .getattr("get_task_factory")?
            .is(&base_get_task_factory);
        let stock_create_task = event_loop_type
            .getattr("create_task")?
            .is(&base_create_task);
        let task_factory_accessor = if stock_get_task_factory && stock_create_task {
            TaskFactoryAccessor::Stock
        } else {
            TaskFactoryAccessor::Custom {
                get_task_factory: event_loop.getattr("get_task_factory")?.unbind(),
                stock_create_task,
            }
        };
        #[cfg(unix)]
        let doorbell = Doorbell::new().map_err(|err| {
            PyRuntimeError::new_err(format!("failed to create shard doorbell: {err}"))
        })?;
        let tasks_mod = py.import("asyncio.tasks")?;
        let task_type = tasks_mod.getattr("Task")?;
        let eager = py.version_info() >= (3, 12);
        let task_kwargs = PyDict::new(py);
        task_kwargs.set_item(pyo3::intern!(py, "loop"), &event_loop)?;
        task_kwargs.set_item(
            pyo3::intern!(py, "name"),
            pyo3::intern!(py, "h2corn.request"),
        )?;
        if eager {
            task_kwargs.set_item(pyo3::intern!(py, "eager_start"), PyBool::new(py, true))?;
        }
        let create_task_kwargs = PyDict::new(py);
        create_task_kwargs.set_item(
            pyo3::intern!(py, "name"),
            pyo3::intern!(py, "h2corn.request"),
        )?;
        // Only the stock signature guarantees this keyword; custom loop methods may reject it.
        if stock_create_task && py.version_info() >= (3, 14) {
            create_task_kwargs.set_item(pyo3::intern!(py, "eager_start"), PyBool::new(py, true))?;
        }

        #[cfg(windows)]
        let call_soon_threadsafe = event_loop.getattr("call_soon_threadsafe")?.unbind();
        let call_soon = event_loop.getattr("call_soon")?.unbind();
        let shard: Shard = Arc::new(Self {
            queue: Mutex::new(VecDeque::new()),
            armed: AtomicBool::new(false),
            #[cfg(unix)]
            doorbell,
            #[cfg(windows)]
            call_soon_threadsafe,
            call_soon,
            event_loop: event_loop.unbind(),
            task_factory_accessor,
            task_type: task_type.unbind(),
            task_kwargs: task_kwargs.unbind(),
            create_task_kwargs: create_task_kwargs.unbind(),
            ensure_future: asyncio.getattr("ensure_future")?.unbind(),
            pump: PyOnceLock::new(),
            ready_none: Py::new(py, ReadyNone)?,
            scope_state: PyOnceLock::new(),
        });

        let pump = pump::Pump::into_callable(py, &shard)?;
        shard
            .pump
            .set(py, pump)
            .map_err(|_| PyRuntimeError::new_err("pump already initialized"))?;
        // The pump doubles as the doorbell reader callback. On Windows there
        // is no fd to watch (Proactor has no `add_reader`); rings go through
        // `call_soon_threadsafe` instead.
        #[cfg(unix)]
        shard.event_loop.bind(py).call_method1(
            pyo3::intern!(py, "add_reader"),
            (
                shard.doorbell.reader_fd(),
                shard.pump.get(py).expect("pump was initialized").bind(py),
            ),
        )?;
        Ok(shard)
    }

    /// Test-only shard whose Python fields are inert placeholders; suitable
    /// for code paths that carry a shard without scheduling on it.
    #[cfg(test)]
    pub(crate) fn test_stub(py: Python<'_>) -> Shard {
        Arc::new(Self {
            queue: Mutex::new(VecDeque::new()),
            armed: AtomicBool::new(false),
            #[cfg(unix)]
            doorbell: Doorbell::new().expect("test doorbell"),
            #[cfg(windows)]
            call_soon_threadsafe: py.None(),
            call_soon: py.None(),
            event_loop: py.None(),
            task_factory_accessor: TaskFactoryAccessor::Stock,
            task_type: py.None(),
            task_kwargs: PyDict::new(py).unbind(),
            create_task_kwargs: PyDict::new(py).unbind(),
            ensure_future: py.None(),
            pump: PyOnceLock::new(),
            ready_none: Py::new(py, ReadyNone).expect("test ready_none"),
            scope_state: PyOnceLock::new(),
        })
    }

    /// Construct Task directly only with no factory and the stock create_task
    /// implementation; otherwise use the loop's task-creation contract.
    /// Pump-only: must run from a plain loop callback.
    pub(super) fn construct_task<'py>(
        &self,
        py: Python<'py>,
        coroutine: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (factory_is_none, stock_create_task) = match &self.task_factory_accessor {
            TaskFactoryAccessor::Stock => {
                // This equals BaseEventLoop.get_task_factory() because setup verified both methods.
                (
                    self.event_loop
                        .bind(py)
                        .getattr(pyo3::intern!(py, "_task_factory"))?
                        .is_none(),
                    true,
                )
            },
            TaskFactoryAccessor::Custom {
                get_task_factory,
                stock_create_task,
            } => (
                get_task_factory.bind(py).call0()?.is_none(),
                *stock_create_task,
            ),
        };

        if factory_is_none && stock_create_task {
            self.task_type
                .bind(py)
                .call((coroutine,), Some(self.task_kwargs.bind(py)))
        } else {
            self.event_loop.bind(py).call_method(
                pyo3::intern!(py, "create_task"),
                (coroutine,),
                Some(self.create_task_kwargs.bind(py)),
            )
        }
    }

    /// Cold path: `asyncio.ensure_future(awaitable, loop=...)`.
    pub(super) fn ensure_future<'py>(
        &self,
        py: Python<'py>,
        awaitable: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
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
        if !self.armed.swap(true, Ordering::AcqRel)
            && let Err(error) = self.schedule_pump(py, &self.call_soon)
        {
            error.write_unraisable(py, Some(self.call_soon.bind(py)));
        }
    }

    /// Enqueue an event from any thread and ring the doorbell if the pump is
    /// not already scheduled. On POSIX the ring is a single `write(2)` —
    /// tokio threads never attach to Python; on Windows it is one coalesced
    /// `call_soon_threadsafe(pump)` per empty→non-empty transition.
    pub(crate) fn push(&self, event: PumpEvent) {
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
            if let Err(error) = self.schedule_pump(py, &self.call_soon_threadsafe) {
                error.write_unraisable(py, Some(self.call_soon_threadsafe.bind(py)));
            }
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

    /// This shard's cached `None`-resolving awaitable singleton.
    pub(crate) const fn ready_none(&self) -> &Py<ReadyNone> {
        &self.ready_none
    }

    pub(crate) fn install_scope_state(&self, py: Python<'_>, state: Py<PyDict>) -> PyResult<()> {
        self.scope_state
            .set(py, state)
            .map_err(|_| PyRuntimeError::new_err("lifespan state already initialized"))
    }

    pub(super) fn copy_scope_state<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        let Some(state) = self.scope_state.get(py) else {
            return Ok(None);
        };
        // Copied even when empty. The lifespan spec says the namespace is
        // shallow-copied into every connection scope, and an application that
        // reads `scope["state"]` directly — rather than through Starlette,
        // which does `scope.setdefault("state", {})` first — would otherwise
        // get a `KeyError` purely because startup happened to store nothing.
        // Costs a measured ~430 instructions per request; a compatibility
        // break is not worth that.
        state.bind(py).copy().map(Some)
    }

    /// Unregister the POSIX doorbell. Pump-only: event-loop reader ownership
    /// is loop-affine even on free-threaded Python.
    pub(crate) fn detach(&self, py: Python<'_>) {
        #[cfg(unix)]
        {
            if let Err(error) = self.event_loop.bind(py).call_method1(
                pyo3::intern!(py, "remove_reader"),
                (self.doorbell.reader_fd(),),
            ) {
                error.write_unraisable(py, Some(self.event_loop.bind(py)));
            }
        }
        #[cfg(windows)]
        let _ = py;
    }

    pub(super) fn stop_loop(&self, py: Python<'_>) {
        self.detach(py);
        if let Err(error) = self
            .event_loop
            .bind(py)
            .call_method0(pyo3::intern!(py, "stop"))
        {
            error.write_unraisable(py, Some(self.event_loop.bind(py)));
        }
    }

    /// Drain up to [`PUMP_BATCH_MAX`] events into a LIFO work buffer in
    /// reverse order; report whether more remain. Popping the buffer therefore
    /// preserves the queue's FIFO order without forfeiting its allocation.
    fn drain_batch(&self, batch: &mut Vec<PumpEvent>) -> bool {
        let mut queue = self.queue.lock();
        let take = queue.len().min(PUMP_BATCH_MAX);
        batch.extend(queue.drain(..take).rev());
        !queue.is_empty()
    }
}

/// A loop shard running on its own Python thread (free-threaded builds).
pub(crate) struct ShardThread {
    shard: Option<Shard>,
    join: Option<JoinHandle<()>>,
}

impl ShardThread {
    pub(crate) const fn shard(&self) -> &Shard {
        self.shard.as_ref().expect("live shard thread has a handle")
    }

    /// Stop the shard's loop and join its thread.
    pub(crate) fn shutdown(mut self) -> Result<(), String> {
        self.shutdown_inner()
    }

    fn shutdown_inner(&mut self) -> Result<(), String> {
        if let Some(shard) = self.shard.take() {
            shard.push(PumpEvent::StopLoop);
            // The loop thread retains its own handle until after `loop.close()`.
            // Drop this cross-thread owner before joining so Python caches and the
            // doorbell are finally destroyed on their owning loop thread.
            drop(shard);
        }
        self.join.take().map_or(Ok(()), |join| {
            join.join().map_err(|panic| panic_message(panic.as_ref()))
        })
    }
}

impl Drop for ShardThread {
    fn drop(&mut self) {
        if let Err(detail) = self.shutdown_inner() {
            report_shard_thread_panic(&detail);
        }
    }
}

fn panic_message(panic: &(dyn Any + Send)) -> String {
    panic.downcast_ref::<String>().map_or_else(
        || {
            panic.downcast_ref::<&'static str>().map_or_else(
                || "non-string panic payload".to_owned(),
                |message| (*message).to_owned(),
            )
        },
        String::clone,
    )
}

pub(crate) fn report_shard_thread_panic(detail: &str) {
    crate::log::Event::Failed.emit(format_args!("shard thread panicked: {detail}"), |fields| {
        fields.str("subject", "shard thread");
        fields.str("detail", detail);
    });
}

/// Spawn a dedicated Python thread running a fresh event loop with its own
/// pump. Returns once the shard handle is constructed; the loop runs until
/// [`ShardThread::shutdown`].
pub(crate) fn spawn_shard_thread(
    index: usize,
    factory: SecondaryLoopFactory,
) -> PyResult<ShardThread> {
    let (tx, rx) = channel::<PyResult<Shard>>();
    let join = ThreadBuilder::new()
        .name(format!("h2corn-loop-{index}"))
        .spawn(move || {
            Python::attach(|py| {
                let setup = || -> PyResult<(Shard, Py<PyAny>)> {
                    let event_loop = factory.create(py)?;
                    let actual = SecondaryLoopFactory::classify(&event_loop)?;
                    if actual != factory {
                        if let Err(error) = event_loop.call_method0("close") {
                            error.write_unraisable(py, Some(&event_loop));
                        }
                        return Err(PyRuntimeError::new_err(format!(
                            "{factory:?} secondary-loop factory returned {actual:?} loop"
                        )));
                    }
                    let shard = ShardHandle::from_event_loop(py, &event_loop)?;
                    Ok((shard, event_loop.unbind()))
                };
                match setup() {
                    Ok((shard, event_loop)) => {
                        drop(tx.send(Ok(Arc::clone(&shard))));
                        // Blocks this thread until ShardHandle::stop.
                        if let Err(error) = event_loop.bind(py).call_method0("run_forever") {
                            error.write_unraisable(py, Some(event_loop.bind(py)));
                        }
                        if let Err(error) = event_loop.bind(py).call_method0("close") {
                            error.write_unraisable(py, Some(event_loop.bind(py)));
                        }
                        drop(shard);
                    },
                    Err(err) => {
                        drop(tx.send(Err(err)));
                    },
                }
            });
        })
        .map_err(|err| PyRuntimeError::new_err(format!("failed to spawn loop thread: {err}")))?;
    let initialized = rx
        .recv()
        .map_err(|_| PyRuntimeError::new_err("loop thread exited before initializing"));
    match initialized {
        Ok(Ok(shard)) => Ok(ShardThread {
            shard: Some(shard),
            join: Some(join),
        }),
        Ok(Err(err)) | Err(err) => {
            if let Err(panic) = join.join() {
                report_shard_thread_panic(&panic_message(panic.as_ref()));
            }
            Err(err)
        },
    }
}

/// Initialize the global Tokio runtime with its first requested worker count.
///
/// A panicking builder publishes neither the runtime nor its worker count, so
/// the next caller can retry initialization.
pub(crate) fn init_runtime(
    worker_threads: usize,
    build: impl FnOnce(usize) -> Runtime,
) -> Result<(), usize> {
    let initialized = TOKIO_RUNTIME.get_or_init(|| TokioRuntime {
        worker_threads,
        runtime: build(worker_threads),
    });
    if initialized.worker_threads == worker_threads {
        Ok(())
    } else {
        Err(initialized.worker_threads)
    }
}

/// The global tokio runtime. Panics if [`init_runtime`] has not run.
pub(crate) fn runtime() -> &'static Runtime {
    &TOKIO_RUNTIME
        .get()
        .expect("tokio runtime is initialized during server startup")
        .runtime
}

/// Construct and await a lifecycle operation on its owning Python loop.
pub(crate) fn call_awaitable<B>(shard: Shard, build: B) -> SlotFuture<PyResult<Py<PyAny>>>
where
    B: for<'py> FnOnce(Python<'py>, Shard) -> PyResult<Py<PyAny>> + Send + 'static,
{
    let slot = TaskSlot::new();
    shard.push(PumpEvent::CallAwaitable {
        build: Box::new(build),
        slot: Arc::clone(&slot),
    });
    Arc::clone(&slot).wait(shard)
}

pub(crate) fn observe_waiter(
    task: TokioJoinHandle<()>,
    shard: Shard,
    future: Py<RustFuture>,
    subject: &'static str,
) {
    runtime().spawn(async move {
        if let Err(error) = task.await
            && let Some(error) = unexpected_join_error(error)
        {
            let detail = format!("background task failed: {error}");
            crate::server::report_failure(subject, Err(H2CornError::from(error)));
            shard.push(PumpEvent::Resolve {
                fut: future,
                payload: ResolvePayload::Simple(Box::new(move |_| {
                    Err(PyRuntimeError::new_err(detail))
                })),
            });
        }
    });
}

pub(crate) async fn release_app(shard: Shard, app: AppRuntimeHandle) -> Result<(), H2CornError> {
    let (done, wait) = oneshot::channel();
    shard.push(PumpEvent::ReleaseApp { app, done });
    wait.await.map_err(|_| {
        H2CornError::from(PyRuntimeError::new_err(
            "Python loop stopped before releasing the application",
        ))
    })
}

#[cfg(test)]
mod tests {
    use std::process::Command;
    #[cfg(Py_GIL_DISABLED)]
    use std::sync::Arc;

    use super::SecondaryLoopFactory;

    #[test]
    fn module_family_selection_is_explicit_and_conservative() {
        assert_eq!(
            SecondaryLoopFactory::from_module("asyncio.unix_events"),
            SecondaryLoopFactory::Asyncio
        );
        assert_eq!(
            SecondaryLoopFactory::from_module("asyncio.windows_events"),
            SecondaryLoopFactory::Asyncio
        );
        assert_eq!(
            SecondaryLoopFactory::from_module("uvloop"),
            SecondaryLoopFactory::Uvloop
        );
        assert_eq!(
            SecondaryLoopFactory::from_module("vendor.custom_loop"),
            SecondaryLoopFactory::Custom
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn shard_thread_shutdown_returns_join_panic() {
        const CHILD: &str = "H2CORN_SHARD_JOIN_CHILD";
        const TEST: &str = "pyloop::tests::shard_thread_shutdown_returns_join_panic";
        if std::env::var_os(CHILD).is_some() {
            pyo3::Python::initialize();
            pyo3::Python::attach(|py| {
                let shard = super::ShardHandle::test_stub(py);
                let join = std::thread::spawn(|| panic!("injected shard thread panic"));
                let detail = super::ShardThread {
                    shard: Some(shard),
                    join: Some(join),
                }
                .shutdown()
                .expect_err("thread panic must be returned to the shutdown owner");
                super::report_shard_thread_panic(&detail);
            });
            return;
        }

        let output = Command::new(std::env::current_exe().expect("test executable path"))
            .args(["--exact", TEST, "--nocapture"])
            .env(CHILD, "1")
            .output()
            .expect("child test process starts");
        assert!(
            output.status.success(),
            "child failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(stderr.matches("shard thread panicked:").count(), 1);
        assert!(stderr.contains("injected shard thread panic"));
    }

    #[cfg(Py_GIL_DISABLED)]
    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn dropping_shard_thread_stops_loop_and_releases_handle() {
        pyo3::Python::initialize();
        let thread = super::spawn_shard_thread(999, super::SecondaryLoopFactory::Asyncio)
            .expect("secondary loop starts");
        let weak = Arc::downgrade(thread.shard());
        drop(thread);
        assert!(weak.upgrade().is_none());
    }
}
