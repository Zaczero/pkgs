use std::mem::{swap, take};
use std::num::{NonZeroU32, NonZeroU64};
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;
use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, watch};

use crate::app_call::AppCallArgs;
use crate::config::ServerConfig;
use crate::error::H2CornError;
use crate::h2_frame::{ErrorCode, StreamId};
use crate::http::scope::{ScopeOverrides, resolve_scope_overrides, scope_view_from_parts};
use crate::http::types::RequestHead;
use crate::proxy_protocol::ConnectionInfo;
use crate::pyloop::{PumpEvent, Shard, SlotFuture, TaskSlot};

pub(crate) struct AppRuntime {
    pub app: Py<PyAny>,
    /// Loop shards; exactly one on GIL builds, `loop_threads` on
    /// free-threaded builds. Index 0 is the main (caller's) loop, which
    /// also owns lifespan and the shutdown trigger.
    pub shards: Box<[Shard]>,
    next_shard: AtomicUsize,
    pub limits: Option<Arc<RuntimeLimits>>,
}

impl AppRuntime {
    pub(crate) fn new(
        app: Py<PyAny>,
        shards: Box<[Shard]>,
        limits: Option<Arc<RuntimeLimits>>,
    ) -> Self {
        debug_assert!(!shards.is_empty());
        Self {
            app,
            shards,
            next_shard: AtomicUsize::new(0),
            limits,
        }
    }

    /// The caller's loop: lifespan, shutdown trigger, server-done future.
    pub(crate) fn main_shard(&self) -> Shard {
        Arc::clone(&self.shards[0])
    }

    /// Round-robin shard pick; every Python object of one request binds to
    /// the picked shard so the request runs entirely on one loop.
    pub(crate) fn pick_shard(&self) -> Shard {
        match self.shards.as_ref() {
            [single] => Arc::clone(single),
            shards => {
                let index = self.next_shard.fetch_add(1, Ordering::Relaxed);
                Arc::clone(&shards[index % shards.len()])
            },
        }
    }

    pub(crate) fn into_teardown(self) -> (Py<PyAny>, Option<Arc<RuntimeLimits>>, Box<[Shard]>) {
        (self.app, self.limits, self.shards)
    }
}

/// Scoped worker state. Clones are confined to connection/request ownership
/// boundaries and disappear when an embedded `serve()` has fully drained.
pub(crate) type AppRuntimeHandle = Arc<AppRuntime>;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum ShutdownKind {
    #[default]
    Stop,
    Restart,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum ShutdownState {
    #[default]
    Running,
    Graceful(ShutdownKind),
}

impl ShutdownState {
    pub(crate) const fn kind(self) -> Option<ShutdownKind> {
        match self {
            Self::Running => None,
            Self::Graceful(kind) => Some(kind),
        }
    }
}

impl ShutdownKind {
    pub(crate) fn from_wire(value: &str) -> Option<Self> {
        match value {
            "stop" => Some(Self::Stop),
            "restart" => Some(Self::Restart),
            _ => None,
        }
    }
}

pub(crate) struct ConnectionScopeCache {
    default_server: PyOnceLock<Py<PyAny>>,
    default_client: PyOnceLock<Option<Py<PyAny>>>,
}

impl Default for ConnectionScopeCache {
    fn default() -> Self {
        Self {
            default_server: PyOnceLock::new(),
            default_client: PyOnceLock::new(),
        }
    }
}

pub(crate) struct RuntimeLimits {
    concurrency: Option<Arc<Semaphore>>,
    max_requests: Option<NonZeroU64>,
    completed_tasks: AtomicU64,
    retire_requested: AtomicBool,
    retire_trigger: Option<Py<PyAny>>,
}

impl RuntimeLimits {
    pub(crate) fn new(config: &ServerConfig, retire_trigger: Option<Py<PyAny>>) -> Option<Self> {
        if config.limit_concurrency.is_none() && config.max_requests.is_none() {
            return None;
        }
        Some(Self {
            concurrency: config
                .limit_concurrency
                .map(|limit| Arc::new(Semaphore::new(limit.get()))),
            max_requests: config.max_requests,
            completed_tasks: AtomicU64::new(0),
            retire_requested: AtomicBool::new(false),
            retire_trigger,
        })
    }

    fn on_task_complete(&self) {
        if let Some(limit) = self.max_requests {
            if self.completed_tasks.fetch_add(1, Ordering::Relaxed) + 1 < limit.get() {
                return;
            }
            if self.retire_requested.swap(true, Ordering::Relaxed) {
                return;
            }
            if let Some(trigger) = self.retire_trigger.as_ref() {
                Python::attach(|py| {
                    let _ = trigger.call0(py);
                });
            }
        }
    }
}

#[derive(Default)]
pub(crate) struct RequestAdmission {
    // Fields drop in declaration order: return the concurrency permit before
    // completion can request worker retirement.
    _permit: Option<OwnedSemaphorePermit>,
    _completion: RequestCompletion,
}

#[derive(Default)]
struct RequestCompletion(Option<Arc<RuntimeLimits>>);

impl Drop for RequestCompletion {
    fn drop(&mut self) {
        if let Some(limits) = self.0.as_ref() {
            limits.on_task_complete();
        }
    }
}

pub(crate) struct ConnectionShared {
    pub app: AppRuntimeHandle,
    pub config: Arc<ServerConfig>,
    pub info: ConnectionInfo,
    scope_cache: ConnectionScopeCache,
}

#[derive(Clone)]
pub(crate) struct ConnectionContext {
    shared: Arc<ConnectionShared>,
    pub shutdown: watch::Receiver<ShutdownState>,
}

impl ConnectionContext {
    pub(crate) fn new(
        app: AppRuntimeHandle,
        config: Arc<ServerConfig>,
        info: ConnectionInfo,
        shutdown: watch::Receiver<ShutdownState>,
    ) -> Self {
        Self {
            shared: Arc::new(ConnectionShared {
                app,
                config,
                info,
                scope_cache: ConnectionScopeCache::default(),
            }),
            shutdown,
        }
    }

    pub(crate) fn default_server_scope_value<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        let value = self.scope_cache.default_server.get_or_init(py, || {
            self.with_default_scope_endpoints(|_, server| {
                server
                    .into_pyobject(py)
                    .expect("server scope tuple should be constructible")
                    .into_any()
                    .unbind()
            })
        });
        value.clone_ref(py).into_bound(py)
    }

    pub(crate) fn default_client_scope_value<'py>(
        &self,
        py: Python<'py>,
    ) -> Option<Bound<'py, PyAny>> {
        let value = self.scope_cache.default_client.get_or_init(py, || {
            self.with_default_scope_endpoints(|client, _| {
                client.map(|client| {
                    client
                        .into_pyobject(py)
                        .expect("client scope tuple should be constructible")
                        .into_any()
                        .unbind()
                })
            })
        });
        value
            .as_ref()
            .map(|value| value.clone_ref(py).into_bound(py))
    }

    fn with_default_scope_endpoints<T>(
        &self,
        f: impl FnOnce(Option<(&str, u16)>, (&str, Option<u16>)) -> T,
    ) -> T {
        let view = scope_view_from_parts("", &self.config, &self.info, None);
        f(view.client, view.server)
    }
}

impl Deref for ConnectionContext {
    type Target = ConnectionShared;

    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}

pub(crate) struct RequestContext {
    pub connection: ConnectionContext,
    pub request: RequestHead,
    pub(crate) scope_overrides: Option<Box<ScopeOverrides>>,
}

impl RequestContext {
    /// Boxed at creation: moving this by value through the
    /// per-request future chain would otherwise replicate it in every
    /// suspended layer of the spawned task.
    pub(crate) fn new(connection: ConnectionContext, request: RequestHead) -> Box<Self> {
        let scope_overrides =
            resolve_scope_overrides(&request, &connection.config, &connection.info);
        Box::new(Self {
            connection,
            request,
            scope_overrides,
        })
    }
}

#[derive(Debug)]
pub(crate) struct ReleasedH2InputCredit {
    pub(crate) stream_id: StreamId,
    pub(crate) bytes: NonZeroU32,
}

/// Cross-thread handoff from ASGI input consumption back to the owning H2
/// connection. Releases are coalesced behind one notification so the
/// connection wakes once and touches only streams that made progress.
#[derive(Debug, Default)]
pub(crate) struct H2InputCreditQueue {
    released: Mutex<Vec<ReleasedH2InputCredit>>,
    pending: AtomicBool,
    notify: Notify,
}

impl H2InputCreditQueue {
    pub(crate) fn credit(
        self: &Arc<Self>,
        stream_id: StreamId,
        bytes: NonZeroU32,
    ) -> H2InputCredit {
        H2InputCredit {
            flow: Arc::clone(self),
            stream_id,
            remaining_bytes: Some(bytes),
        }
    }

    fn release(&self, stream_id: StreamId, bytes: NonZeroU32) {
        let mut released = self.released.lock();
        if let Some(last) = released.last_mut()
            && last.stream_id == stream_id
            && let Some(combined) = last.bytes.get().checked_add(bytes.get())
        {
            // Both operands are non-zero, so a non-overflowing sum is too.
            last.bytes = NonZeroU32::new(combined).expect("sum of non-zero credits is non-zero");
        } else {
            released.push(ReleasedH2InputCredit { stream_id, bytes });
        }
        // The queue and its wakeup state are one synchronization domain. If
        // the transition happened after unlocking, a drainer could clear
        // `pending` after this producer suppressed its notification and leave
        // receive-window credit stranded indefinitely.
        let notify = !self.pending.swap(true, Ordering::AcqRel);
        drop(released);
        if notify {
            self.notify.notify_one();
        }
    }

    pub(crate) fn has_pending(&self) -> bool {
        self.pending.load(Ordering::Acquire)
    }

    pub(crate) async fn notified(&self) {
        self.notify.notified().await;
    }

    pub(crate) fn drain_into(&self, target: &mut Vec<ReleasedH2InputCredit>) {
        self.drain_into_inner(target, || {});
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "the queue lock must cover the pending-state transition to prevent lost wakeups"
    )]
    fn drain_into_inner(&self, target: &mut Vec<ReleasedH2InputCredit>, drained: impl FnOnce()) {
        debug_assert!(target.is_empty());
        let mut released = self.released.lock();
        swap(&mut *released, target);
        drained();
        self.pending.store(false, Ordering::Release);
    }
}

/// Ownership token for bytes charged against an H2 receive window. Moving
/// the token through the channel and cancellation requeue keeps the charge;
/// successful ASGI materialization or any drop path returns it exactly once.
#[derive(Debug)]
pub(crate) struct H2InputCredit {
    flow: Arc<H2InputCreditQueue>,
    stream_id: StreamId,
    remaining_bytes: Option<NonZeroU32>,
}

impl H2InputCredit {
    /// Merge adjacent credit from the same stream into this ownership token.
    /// Request-body batching never crosses a stream, so a mismatch is an
    /// internal flow-control corruption rather than a recoverable condition.
    fn merge(&mut self, mut other: Self) {
        debug_assert!(
            Arc::ptr_eq(&self.flow, &other.flow) && self.stream_id == other.stream_id,
            "HTTP/2 input credit cannot cross a connection or stream"
        );
        let left = self
            .remaining_bytes
            .expect("live credit has a non-zero charge");
        let right = other
            .remaining_bytes
            .take()
            .expect("live credit has a non-zero charge");
        let combined = left
            .get()
            .checked_add(right.get())
            .expect("batched credit cannot exceed the receive-window charge");
        self.remaining_bytes = NonZeroU32::new(combined);
    }

    pub(crate) fn release(self) {
        drop(self);
    }
}

impl Drop for H2InputCredit {
    fn drop(&mut self) {
        if let Some(bytes) = self.remaining_bytes.take() {
            self.flow.release(self.stream_id, bytes);
        }
    }
}

#[derive(Debug)]
pub(crate) enum StreamInput {
    Data {
        body: Bytes,
        credit: Option<H2InputCredit>,
    },
    /// Two already-backlogged HTTP body frames. The boxed handles retain the
    /// original payloads without copying or growing every channel slot; Python
    /// materialization writes both directly into one bytes object.
    HttpDataBatch {
        bodies: Box<[Bytes; 2]>,
        credit: Option<H2InputCredit>,
    },
    EndStream,
    Reset(ErrorCode),
}

impl StreamInput {
    pub(crate) const fn data(body: Bytes) -> Self {
        Self::Data { body, credit: None }
    }

    pub(crate) const fn h2_data(body: Bytes, credit: H2InputCredit) -> Self {
        Self::Data {
            body,
            credit: Some(credit),
        }
    }

    pub(crate) fn try_batch_http(&mut self, next: Self, max_bytes: usize) -> Result<(), Self> {
        let Self::Data {
            body: first_body,
            credit: first_credit,
        } = self
        else {
            return Err(next);
        };
        let (second_body, second_credit) = match next {
            Self::Data { body, credit } => (body, credit),
            other => return Err(other),
        };
        if first_body.len().saturating_add(second_body.len()) > max_bytes {
            return Err(Self::Data {
                body: second_body,
                credit: second_credit,
            });
        }

        let mut credit = first_credit.take();
        match (&mut credit, second_credit) {
            (Some(first), Some(second)) => first.merge(second),
            (None, Some(second)) => credit = Some(second),
            (Some(_) | None, None) => {},
        }
        let first_body = take(first_body);
        *self = Self::HttpDataBatch {
            bodies: Box::new([first_body, second_body]),
            credit,
        };
        Ok(())
    }
}

pub(crate) fn try_acquire_request_admission(app: &AppRuntime) -> Option<RequestAdmission> {
    let Some(limits) = app.limits.as_ref() else {
        return Some(RequestAdmission {
            _permit: None,
            _completion: RequestCompletion(None),
        });
    };
    let permit = if let Some(semaphore) = limits.concurrency.as_ref() {
        Some(semaphore.clone().try_acquire_owned().ok()?)
    } else {
        None
    };
    Some(RequestAdmission {
        _permit: permit,
        _completion: RequestCompletion(limits.max_requests.map(|_| Arc::clone(limits))),
    })
}

/// Hand the request to the pump: the scope build, the app vectorcall, and
/// the eager Task all run on the loop thread. This function never touches
/// Python and never fails — startup errors arrive through the returned
/// future like any other app failure.
pub(crate) fn start_app_call(
    app: AppRuntimeHandle,
    args: Box<AppCallArgs>,
) -> SlotFuture<Result<(), H2CornError>> {
    let slot = TaskSlot::new();
    let shard = app.pick_shard();
    shard.push(PumpEvent::StartTask {
        app,
        args,
        slot: Arc::clone(&slot),
    });
    slot.wait(shard)
}

#[cfg(test)]
pub(crate) mod test_fixtures {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::time::Duration;

    use pyo3::Python;
    use tokio::sync::watch;

    use super::{AppRuntime, ConnectionContext, ShutdownState};
    use crate::config::{
        BindTarget, Http1Config, Http2Config, ProxyConfig, ResponseHeaderConfig, ServerConfig,
        WebSocketConfig,
    };
    use crate::h2_frame::DEFAULT_MAX_FRAME_SIZE;
    use crate::proxy_protocol::{ConnectionInfo, ConnectionPeer, ProxyProtocolMode, ServerAddr};
    use crate::pyloop::ShardHandle;

    pub(crate) fn server_config() -> Arc<ServerConfig> {
        Arc::new(ServerConfig {
            binds: Box::new([BindTarget::Tcp {
                host: Box::from("127.0.0.1"),
                port: 8000,
            }]),
            access_log: false,
            root_path: Box::from(""),
            limit_request_fields: None,
            http1: Http1Config {
                enabled: true,
                ..Default::default()
            },
            http2: Http2Config {
                max_concurrent_streams: 8,
                max_header_list_size: None,
                max_header_block_size: None,
                max_inbound_frame_size: NonZeroU32::new(DEFAULT_MAX_FRAME_SIZE as u32)
                    .expect("default HTTP/2 frame size is non-zero"),
                initial_stream_window_size: NonZeroU32::new(1 << 20).expect("non-zero"),
                initial_connection_window_size: NonZeroU32::new(2 << 20).expect("non-zero"),
                timeout_response_stall: None,
            },
            max_request_body_size: None,
            timeout_graceful_shutdown: Duration::from_secs(30),
            timeout_keep_alive: None,
            timeout_request_header: None,
            timeout_request_body_idle: None,
            limit_concurrency: None,
            limit_connections: None,
            max_requests: None,
            runtime_threads: 2,
            loop_threads: 1,
            websocket: WebSocketConfig::default(),
            proxy: ProxyConfig {
                trust_headers: false,
                trusted_peers: Box::new([]),
                protocol: ProxyProtocolMode::Off,
            },
            tls: None,
            timeout_handshake: Duration::from_secs(5),
            response_headers: ResponseHeaderConfig::default(),
        })
    }

    pub(crate) fn connection_context(py: Python<'_>) -> ConnectionContext {
        let app: super::AppRuntimeHandle = Arc::new(AppRuntime::new(
            py.None(),
            Box::new([ShardHandle::test_stub(py)]),
            None,
        ));
        let info = ConnectionInfo::from_peer(
            ConnectionPeer::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 54321)),
            Some(ServerAddr {
                host: "127.0.0.1".into(),
                port: Some(8000),
            }),
            false,
        );
        let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownState::Running);
        ConnectionContext::new(app, server_config(), info, shutdown_rx)
    }
}

#[cfg(test)]
mod tests {

    use std::num::{NonZeroU32, NonZeroU64};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, mpsc};
    use std::thread;
    use std::time::Duration;

    use tokio::sync::Semaphore;
    use tokio::time::timeout;

    use super::{H2InputCreditQueue, RequestAdmission, RequestCompletion, RuntimeLimits};
    use crate::h2_frame::StreamId;

    #[test]
    fn request_admission_releases_permit_and_records_completion_once() {
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&semaphore).try_acquire_owned().unwrap();
        let limits = Arc::new(RuntimeLimits {
            concurrency: Some(Arc::clone(&semaphore)),
            max_requests: NonZeroU64::new(2),
            completed_tasks: AtomicU64::new(0),
            retire_requested: AtomicBool::new(false),
            retire_trigger: None,
        });
        let admission = RequestAdmission {
            _permit: Some(permit),
            _completion: RequestCompletion(Some(Arc::clone(&limits))),
        };
        assert_eq!(semaphore.available_permits(), 0);
        drop(admission);
        assert_eq!(semaphore.available_permits(), 1);
        assert_eq!(limits.completed_tasks.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn explicit_release_enqueues_exactly_once() {
        let flow = Arc::new(H2InputCreditQueue::default());
        flow.credit(StreamId::new(1).unwrap(), NonZeroU32::new(16).unwrap())
            .release();

        let mut released = Vec::new();
        flow.drain_into(&mut released);
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].stream_id, StreamId::new(1).unwrap());
        assert_eq!(released[0].bytes.get(), 16);
    }

    #[test]
    fn merged_credit_releases_the_exact_combined_charge_once() {
        let flow = Arc::new(H2InputCreditQueue::default());
        let stream_id = StreamId::new(1).unwrap();
        let mut credit = flow.credit(stream_id, NonZeroU32::new(7).unwrap());
        credit.merge(flow.credit(stream_id, NonZeroU32::new(11).unwrap()));
        assert!(!flow.has_pending(), "merging must retain ownership");
        assert_eq!(
            Arc::strong_count(&flow),
            2,
            "merging must release the consumed token's flow reference"
        );

        credit.release();
        assert_eq!(Arc::strong_count(&flow), 1);

        let mut released = Vec::new();
        flow.drain_into(&mut released);
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].stream_id, stream_id);
        assert_eq!(released[0].bytes.get(), 18);
    }

    #[tokio::test]
    async fn concurrent_release_after_drain_keeps_its_wakeup() {
        let flow = Arc::new(H2InputCreditQueue::default());
        let stream_id = StreamId::new(1).unwrap();
        flow.credit(stream_id, NonZeroU32::new(7).unwrap())
            .release();
        // Consume the first release's permit so only the concurrent release
        // can satisfy the notification checked below.
        flow.notified().await;

        let (start_tx, start_rx) = mpsc::channel();
        let (attempting_tx, attempting_rx) = mpsc::channel();
        let (finished_tx, finished_rx) = mpsc::channel();
        let producer_flow = Arc::clone(&flow);
        let producer = thread::spawn(move || {
            start_rx.recv().unwrap();
            attempting_tx.send(()).unwrap();
            producer_flow
                .credit(stream_id, NonZeroU32::new(11).unwrap())
                .release();
            finished_tx.send(()).unwrap();
        });

        let mut first = Vec::new();
        flow.drain_into_inner(&mut first, || {
            start_tx.send(()).unwrap();
            attempting_rx.recv().unwrap();
            assert!(
                finished_rx.try_recv().is_err(),
                "a producer cannot change wakeup state while the queue drains"
            );
        });
        producer.join().unwrap();

        timeout(Duration::from_secs(1), flow.notified())
            .await
            .expect("the concurrent release must publish a new wakeup");
        let mut second = Vec::new();
        flow.drain_into(&mut second);
        assert_eq!(first[0].bytes.get(), 7);
        assert_eq!(second[0].bytes.get(), 11);
    }

    #[cfg(Py_GIL_DISABLED)]
    mod free_threaded {

        use std::sync::{Arc, Barrier};

        use pyo3::{PyResult, Python};

        use super::super::test_fixtures;

        #[test]
        fn connection_scope_cache_initializes_concurrently_while_attached() {
            const THREADS: usize = 8;
            const ITERATIONS: usize = 1_000;

            Python::initialize();
            let connection = Arc::new(Python::attach(test_fixtures::connection_context));
            let barrier = Arc::new(Barrier::new(THREADS));
            let workers = (0..THREADS)
                .map(|_| {
                    let connection = Arc::clone(&connection);
                    let barrier = Arc::clone(&barrier);
                    std::thread::spawn(move || -> PyResult<(usize, usize)> {
                        barrier.wait();
                        Python::attach(|py| {
                            let mut pointers = None;
                            for _ in 0..ITERATIONS {
                                let server = connection.default_server_scope_value(py);
                                let client = connection
                                    .default_client_scope_value(py)
                                    .expect("test connection has a client address");
                                let current = (server.as_ptr() as usize, client.as_ptr() as usize);
                                assert_eq!(*pointers.get_or_insert(current), current);
                            }
                            Ok(pointers.expect("at least one cache lookup runs"))
                        })
                    })
                })
                .collect::<Vec<_>>();

            let pointers = workers
                .into_iter()
                .map(|worker| {
                    worker
                        .join()
                        .expect("scope-cache worker does not panic")
                        .expect("scope-cache lookup succeeds")
                })
                .collect::<Vec<_>>();
            assert!(pointers.windows(2).all(|pair| pair[0] == pair[1]));
        }
    }
}
