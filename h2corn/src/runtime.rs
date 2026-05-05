use std::future::Future;
use std::num::NonZeroU64;
use std::pin::Pin;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use bytes::Bytes;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3_async_runtimes::TaskLocals;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

use crate::config::ServerConfig;
use crate::error::H2CornError;
use crate::frame::ErrorCode;
use crate::http::scope::{ScopeOverrides, resolve_scope_overrides, scope_view_from_parts};
use crate::http::types::RequestHead;
use crate::proxy::ConnectionInfo;

pub struct SharedApp {
    pub app: Py<PyAny>,
    pub locals: TaskLocals,
    pub limits: Option<Arc<RuntimeLimits>>,
}

pub type AppState = Arc<SharedApp>;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ShutdownKind {
    #[default]
    Stop,
    Restart,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ShutdownState {
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
pub struct ConnectionScopeCache {
    default_server: OnceLock<Py<PyAny>>,
    default_client: OnceLock<Option<Py<PyAny>>>,
}

pub struct RuntimeLimits {
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
pub struct RequestAdmission {
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
pub struct ConnectionContext {
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

pub struct RequestContext {
    pub connection: ConnectionContext,
    pub request: RequestHead,
    pub(crate) scope_overrides: ScopeOverrides,
}

impl RequestContext {
    pub(crate) fn new(connection: ConnectionContext, request: RequestHead) -> Self {
        let scope_overrides =
            resolve_scope_overrides(&request, connection.config, &connection.info);
        Self {
            connection,
            request,
            scope_overrides,
        }
    }
}

#[derive(Debug)]
pub enum StreamInput {
    Data(Bytes),
    EndStream,
    Reset(ErrorCode),
}

struct AppCallFuture<F>(F);

impl<F> Future for AppCallFuture<F>
where
    F: Future<Output = PyResult<Py<PyAny>>>,
{
    type Output = Result<(), H2CornError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: `AppCallFuture` is a transparent newtype around `F`, so
        // projecting the pin to the inner future preserves the original pinning
        // guarantees and does not move `F`.
        let future = unsafe { self.map_unchecked_mut(|this| &mut this.0) };
        match future.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(_)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(err)) => Poll::Ready(Err(H2CornError::from(err))),
        }
    }
}

pub fn try_acquire_request_admission(app: &AppState) -> Option<RequestAdmission> {
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

pub fn start_app_call<B>(
    app: &AppState,
    build_args: B,
) -> Result<impl Future<Output = Result<(), H2CornError>> + use<B>, H2CornError>
where
    B: for<'py> FnOnce(
        Python<'py>,
        &AppState,
    ) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>, Bound<'py, PyAny>)>,
{
    let app = Arc::clone(app);
    let locals = app.locals.clone();
    let future = Python::attach(|py| {
        let (scope, receive, send) = build_args(py, &app)?;
        let args = [scope.as_ptr(), receive.as_ptr(), send.as_ptr()];
        let coroutine = unsafe {
            // SAFETY: the GIL is held; the argument array contains three valid
            // borrowed Python object pointers which outlive the call; no kwargs
            // are passed; and `PyObject_Vectorcall` returns a new owned
            // reference or sets a Python exception.
            Bound::from_owned_ptr_or_err(
                py,
                ffi::PyObject_Vectorcall(
                    app.app.as_ptr(),
                    args.as_ptr().cast_mut(),
                    args.len(),
                    null_mut(),
                ),
            )?
        };
        pyo3_async_runtimes::into_future_with_locals(&locals, coroutine)
    })?;

    Ok(AppCallFuture(future))
}
