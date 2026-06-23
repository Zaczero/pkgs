use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use pyo3::prelude::*;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

use crate::config::ServerConfig;
use crate::error::H2CornError;
use crate::frame::ErrorCode;
use crate::http::scope::{ScopeOverrides, resolve_scope_overrides, scope_view_from_parts};
use crate::http::types::RequestHead;
use crate::proxy::ConnectionInfo;
use crate::pyloop::{PumpEvent, Shard, SlotFuture, TaskSlot};

pub(crate) struct SharedApp {
    pub app: Py<PyAny>,
    /// Loop shards; exactly one on GIL builds, `loop_threads` on
    /// free-threaded builds. Index 0 is the main (caller's) loop, which
    /// also owns lifespan and the shutdown trigger.
    pub shards: Box<[Shard]>,
    next_shard: AtomicUsize,
    pub limits: Option<Arc<RuntimeLimits>>,
}

impl SharedApp {
    pub(crate) fn new(app: Py<PyAny>, shards: Box<[Shard]>, limits: Option<Arc<RuntimeLimits>>) -> Self {
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
        self.shards[0]
    }

    /// Round-robin shard pick; every Python object of one request binds to
    /// the picked shard so the request runs entirely on one loop.
    pub(crate) fn pick_shard(&self) -> Shard {
        match self.shards.as_ref() {
            [single] => single,
            shards => {
                let index = self.next_shard.fetch_add(1, Ordering::Relaxed);
                shards[index % shards.len()]
            },
        }
    }
}

/// Leaked to `'static` like the shards and `ServerConfig`: the app outlives
/// every connection of its `serve()` call, so per-request handle copies are
/// plain pointer copies instead of cross-thread `Arc` refcount traffic.
pub(crate) type AppState = &'static SharedApp;

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

#[derive(Default)]
pub(crate) struct ConnectionScopeCache {
    default_server: OnceLock<Py<PyAny>>,
    default_client: OnceLock<Option<Py<PyAny>>>,
}

pub(crate) struct RuntimeLimits {
    concurrency: Option<Arc<Semaphore>>,
    max_requests: Option<NonZeroU64>,
    completed_tasks: AtomicU64,
    retire_requested: AtomicBool,
    retire_trigger: Option<Py<PyAny>>,
}

impl RuntimeLimits {
    pub(crate) fn new(
        config: &'static ServerConfig,
        retire_trigger: Option<Py<PyAny>>,
    ) -> Option<Self> {
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
    permit: Option<OwnedSemaphorePermit>,
    limits: Option<Arc<RuntimeLimits>>,
}

impl Drop for RequestAdmission {
    fn drop(&mut self) {
        let _ = self.permit.take();
        if let Some(limits) = self.limits.as_ref() {
            limits.on_task_complete();
        }
    }
}

#[derive(Clone)]
pub(crate) struct ConnectionContext {
    pub app: AppState,
    pub config: &'static ServerConfig,
    pub info: Arc<ConnectionInfo>,
    pub shutdown: watch::Receiver<ShutdownState>,
    pub(crate) scope_cache: Arc<ConnectionScopeCache>,
}

impl ConnectionContext {
    pub(crate) fn new(
        app: AppState,
        config: &'static ServerConfig,
        info: Arc<ConnectionInfo>,
        shutdown: watch::Receiver<ShutdownState>,
    ) -> Self {
        Self {
            app,
            config,
            info,
            shutdown,
            scope_cache: Arc::default(),
        }
    }

    pub(crate) fn default_server_scope_value<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        let value = self.scope_cache.default_server.get_or_init(|| {
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
        let value = self.scope_cache.default_client.get_or_init(|| {
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
        let overrides = ScopeOverrides::default();
        let view = scope_view_from_parts("", self.config, &self.info, &overrides);
        f(view.client, view.server)
    }
}

pub(crate) struct RequestContext {
    pub connection: ConnectionContext,
    pub request: RequestHead,
    pub(crate) scope_overrides: ScopeOverrides,
}

impl RequestContext {
    /// Boxed at creation: at 608 bytes, moving this by value through the
    /// per-request future chain would otherwise replicate it in every
    /// suspended layer of the spawned task.
    pub(crate) fn new(connection: ConnectionContext, request: RequestHead) -> Box<Self> {
        let scope_overrides =
            resolve_scope_overrides(&request, connection.config, &connection.info);
        Box::new(Self {
            connection,
            request,
            scope_overrides,
        })
    }
}

#[derive(Debug)]
pub(crate) enum StreamInput {
    Data(Bytes),
    EndStream,
    Reset(ErrorCode),
}

pub(crate) fn try_acquire_request_admission(app: AppState) -> Option<RequestAdmission> {
    let Some(limits) = app.limits.as_ref() else {
        return Some(RequestAdmission {
            permit: None,
            limits: None,
        });
    };
    let permit = if let Some(semaphore) = limits.concurrency.as_ref() {
        Some(semaphore.clone().try_acquire_owned().ok()?)
    } else {
        None
    };
    Some(RequestAdmission {
        permit,
        limits: limits.max_requests.map(|_| Arc::clone(limits)),
    })
}

/// Hand the request to the pump: the scope build, the app vectorcall, and
/// the eager Task all run on the loop thread. This function never touches
/// Python and never fails — startup errors arrive through the returned
/// future like any other app failure.
pub(crate) fn start_app_call<B>(app: AppState, build_args: B) -> SlotFuture<Result<(), H2CornError>>
where
    B: for<'py> FnOnce(
            Python<'py>,
            AppState,
            Shard,
        )
            -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>, Bound<'py, PyAny>)>
        + Send
        + 'static,
{
    let slot = TaskSlot::new();
    let shard = app.pick_shard();
    shard.push(PumpEvent::StartTask {
        app,
        build_args: Box::new(build_args),
        slot: Arc::clone(&slot),
    });
    slot.wait()
}

#[cfg(test)]
pub(crate) mod test_fixtures {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::time::Duration;

    use pyo3::Python;
    use tokio::sync::watch;

    use super::{ConnectionContext, SharedApp, ShutdownState};
    use crate::config::{
        BindTarget, Http1Config, Http2Config, ProxyConfig, ResponseHeaderConfig, ServerConfig,
        WebSocketConfig,
    };
    use crate::frame::DEFAULT_MAX_FRAME_SIZE;
    use crate::proxy::{ConnectionInfo, ConnectionPeer, ProxyProtocolMode, ServerAddr};

    pub(crate) fn server_config() -> &'static ServerConfig {
        Box::leak(Box::new(ServerConfig {
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
        }))
    }

    pub(crate) fn connection_context(py: Python<'_>) -> ConnectionContext {
        let app: super::AppState = Box::leak(Box::new(SharedApp::new(
            py.None(),
            Box::new([crate::pyloop::ShardHandle::test_stub(py)]),
            None,
        )));
        let info = Arc::new(ConnectionInfo::from_peer(
            ConnectionPeer::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 54321)),
            Some(ServerAddr {
                host: "127.0.0.1".into(),
                port: Some(8000),
            }),
            false,
        ));
        let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownState::Running);
        ConnectionContext::new(app, server_config(), info, shutdown_rx)
    }
}
