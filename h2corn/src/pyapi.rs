//! Python FFI entry: config extraction, serve_fds, and related helpers.

use std::iter::repeat_with;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use pyo3::conversion::FromPyObjectOwned;
use pyo3::exceptions::{PyOSError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use smallvec::SmallVec;
use tokio::runtime::Builder as TokioRuntimeBuilder;
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinSet, spawn_blocking};

use crate::config;
use crate::config::{
    BindTarget, ClientCertMode, ConfiguredResponseHeader, ForwardedFields, Http1Config,
    Http2Config, ProxyConfig, ResponseHeaderConfig, ServerConfig, TlsConfig, WebSocketConfig,
};
use crate::error::{ConfigError, H2CornError, IntoPyResult as _, into_pyerr};
use crate::http::header::{configured_response_field_is_forbidden, lowercase_header_name_is_valid};
use crate::http::header_value::header_value_is_valid;
use crate::log::emit_banner as emit_access_banner;
use crate::proxy_protocol::{ProxyProtocolMode, TrustedPeer, parse_trusted_peer};
use crate::pyloop::{
    PumpEvent, ResolvePayload, RustFuture, SecondaryLoopFactory, Shard, ShardHandle, ShardThread,
    SlotFuture, call_awaitable, init_runtime, new_rust_future, release_app, runtime,
    spawn_shard_thread,
};
use crate::runtime::{AppRuntime, AppRuntimeHandle, RuntimeLimits};
use crate::server::{
    ListenerFd, OwnFdsError, PythonRawHandle, QuiesceFd, own_serve_fds, serve_from_fds,
};
use crate::tls::{TlsMaterial, build_tls_config};

const TOKIO_EVENT_INTERVAL: u32 = 31;
const TOKIO_GLOBAL_QUEUE_INTERVAL: u32 = 31;

/// Validated, immutable TLS acceptor state prepared once from PEM bytes.
///
/// Built while the process can still read the key files; workers reuse the
/// same value after privilege drop and never reopen the paths.
#[pyclass(frozen, name = "_PreparedTls", skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct PreparedTls(Option<TlsConfig>);

struct PyConfig<'py>(&'py Bound<'py, PyAny>);

#[derive(Clone, Copy)]
struct SecondaryLifespanConfig {
    required: bool,
    startup_timeout: Option<f64>,
    shutdown_timeout: Option<f64>,
}

/// The runner handed into Rust ownership, and the startup it is waiting on.
type SecondaryLifespanStart = (
    oneshot::Receiver<PyResult<Py<PyAny>>>,
    SlotFuture<PyResult<Py<PyAny>>>,
);

struct SecondaryRunner {
    index: usize,
    shard: Shard,
    runner: Py<PyAny>,
}

/// Exact, immutable Python-to-Rust ownership handoff for an active primary
/// lifespan runner. Implementation object for secondary-loop state — not public
/// API; the Python name is private.
#[pyclass(frozen, name = "_LifespanHandoff")]
pub(crate) struct LifespanHandoff {
    app: Py<PyAny>,
    state: Py<PyDict>,
    config: SecondaryLifespanConfig,
}

#[pymethods]
impl LifespanHandoff {
    #[new]
    const fn new(
        app: Py<PyAny>,
        state: Py<PyDict>,
        required: bool,
        startup_timeout: Option<f64>,
        shutdown_timeout: Option<f64>,
    ) -> Self {
        Self {
            app,
            state,
            config: SecondaryLifespanConfig {
                required,
                startup_timeout,
                shutdown_timeout,
            },
        }
    }
}

struct ServeTask {
    app: AppRuntimeHandle,
    fds: Box<[ListenerFd]>,
    config: Arc<ServerConfig>,
    shutdown_trigger: Py<PyAny>,
    ready_trigger: Option<Py<PyAny>>,
    quiesce_fd: Option<QuiesceFd>,
    lifespan_config: Option<SecondaryLifespanConfig>,
    secondary_apps: Vec<Py<PyAny>>,
    shard_threads: Vec<ShardThread>,
    main_shard: Shard,
    resolve_fut: Py<RustFuture>,
}

impl<'py> PyConfig<'py> {
    fn attr(&self, name: &str) -> PyResult<Bound<'py, PyAny>> {
        self.0.getattr(name)
    }

    fn get<T>(&self, name: &str) -> PyResult<T>
    where
        T: FromPyObjectOwned<'py>,
    {
        self.attr(name)?.extract::<T>().map_err(Into::into)
    }

    fn nonzero_u64(&self, name: &str) -> PyResult<Option<NonZeroU64>> {
        Ok(self.get::<Option<u64>>(name)?.and_then(NonZeroU64::new))
    }

    fn nonzero_usize(&self, name: &str) -> PyResult<Option<NonZeroUsize>> {
        Ok(self.get::<Option<usize>>(name)?.and_then(NonZeroUsize::new))
    }

    fn boxed_str(&self, name: &str) -> PyResult<Box<str>> {
        Ok(Box::from(self.attr(name)?.extract::<&str>()?))
    }

    fn duration(&self, name: &'static str) -> PyResult<Duration> {
        duration_from_seconds(name, self.get::<f64>(name)?)
    }

    fn optional_duration(&self, name: &'static str) -> PyResult<Option<Duration>> {
        optional_duration(name, self.get::<f64>(name)?)
    }

    /// The `Server` value this configuration sends, resolved once here so the
    /// response path only has a value to push or not.
    fn server_header(&self) -> PyResult<Option<Bytes>> {
        match self.attr("server_header")?.extract::<&str>()? {
            "off" => Ok(None),
            "on" => Ok(Some(Bytes::from_static(config::SERVER_NAME.as_bytes()))),
            "full" => Ok(Some(Bytes::from_static(
                config::SERVER_NAME_AND_VERSION.as_bytes(),
            ))),
            value => Err(into_pyerr(ConfigError::invalid_server_header_mode(value))),
        }
    }

    fn proxy_protocol(&self) -> PyResult<ProxyProtocolMode> {
        match self.attr("proxy_protocol")?.extract::<&str>()? {
            "off" => Ok(ProxyProtocolMode::Off),
            "v1" => Ok(ProxyProtocolMode::V1),
            "v2" => Ok(ProxyProtocolMode::V2),
            value => Err(into_pyerr(ConfigError::invalid_proxy_protocol_mode(value))),
        }
    }

    fn cert_reqs(&self) -> PyResult<ClientCertMode> {
        match self.attr("cert_reqs")?.extract::<&str>()? {
            "none" => Ok(ClientCertMode::None),
            "optional" => Ok(ClientCertMode::Optional),
            "required" => Ok(ClientCertMode::Required),
            value => Err(into_pyerr(ConfigError::invalid_client_cert_mode(value))),
        }
    }

    fn trusted_peers(&self) -> PyResult<Box<[TrustedPeer]>> {
        let peers = self
            .attr("forwarded_allow_ips")?
            .try_iter()?
            .map(|item| {
                let item = item?;
                parse_trusted_peer(item.extract::<&str>()?).into_pyresult()
            })
            .collect::<PyResult<SmallVec<[TrustedPeer; 4]>>>()?;
        Ok(peers.into_vec().into_boxed_slice())
    }

    fn forwarded_fields(&self) -> PyResult<ForwardedFields> {
        let mut fields = ForwardedFields::empty();
        for item in self.attr("forwarded_fields")?.try_iter()? {
            let item = item?;
            let field = item.extract::<&str>()?;
            let flag = match field {
                "for" => ForwardedFields::FOR,
                "proto" => ForwardedFields::PROTO,
                "host" => ForwardedFields::HOST,
                "port" => ForwardedFields::PORT,
                "prefix" => ForwardedFields::PREFIX,
                "forwarded" => ForwardedFields::FORWARDED,
                other => {
                    return Err(PyValueError::new_err(format!(
                        "invalid forwarded_fields entry: {other:?}"
                    )));
                },
            };
            fields.insert(flag);
        }
        Ok(fields)
    }

    fn websocket_message_size_limit(&self) -> PyResult<Option<NonZeroUsize>> {
        Ok(NonZeroUsize::new(self.get("websocket_max_message_size")?))
    }

    fn binds(&self) -> PyResult<Box<[BindTarget]>> {
        let raw_binds = self.get::<Vec<String>>("bind")?;
        raw_binds
            .iter()
            .map(|raw| parse_bind_target(raw))
            .collect::<PyResult<Vec<_>>>()
            .map(Vec::into_boxed_slice)
    }

    fn http1(&self) -> PyResult<Http1Config> {
        Ok(Http1Config {
            enabled: self.get("http1")?,
            limit_request_head_size: self.nonzero_usize("limit_request_head_size")?,
            limit_request_line: self.nonzero_usize("limit_request_line")?,
            limit_request_field_size: self.nonzero_usize("limit_request_field_size")?,
        })
    }

    fn http2(&self) -> PyResult<Http2Config> {
        Ok(Http2Config {
            max_concurrent_streams: self.get("max_concurrent_streams")?,
            max_header_list_size: self
                .get::<Option<u32>>("h2_max_header_list_size")?
                .and_then(|value| NonZeroUsize::new(value as usize)),
            max_header_block_size: self.nonzero_usize("h2_max_header_block_size")?,
            max_inbound_frame_size: NonZeroU32::new(self.get("h2_max_inbound_frame_size")?)
                .expect("configured inbound frame size is non-zero"),
            initial_stream_window_size: NonZeroU32::new(self.get("h2_initial_stream_window_size")?)
                .expect("configured stream window is non-zero"),
            initial_connection_window_size: NonZeroU32::new(
                self.get("h2_initial_connection_window_size")?,
            )
            .expect("configured connection window is non-zero"),
            timeout_response_stall: self.optional_duration("h2_timeout_response_stall")?,
        })
    }

    fn websocket(&self) -> PyResult<WebSocketConfig> {
        let keep_alive = match self.optional_duration("websocket_ping_interval")? {
            None => None,
            Some(interval) => Some(crate::config::WebSocketKeepAlive {
                interval,
                timeout: self.optional_duration("websocket_ping_timeout")?,
            }),
        };
        Ok(WebSocketConfig {
            max_message_size: self.websocket_message_size_limit()?,
            per_message_deflate: self.get("websocket_per_message_deflate")?,
            keep_alive,
        })
    }

    fn proxy(&self) -> PyResult<ProxyConfig> {
        Ok(ProxyConfig {
            trust_headers: self.get("proxy_headers")?,
            trusted_peers: self.trusted_peers()?,
            forwarded_fields: self.forwarded_fields()?,
            protocol: self.proxy_protocol()?,
        })
    }

    fn response_headers(&self) -> PyResult<ResponseHeaderConfig> {
        let raw_headers = self.get::<Vec<String>>("response_headers")?;
        let mut headers = Vec::with_capacity(raw_headers.len());
        for raw in raw_headers {
            let Some((name, value)) = raw.split_once(':') else {
                return Err(into_pyerr(ConfigError::invalid_response_header_format(
                    &raw,
                )));
            };
            let name = name.trim();
            let value = value.trim();
            if !lowercase_header_name_is_valid(name.as_bytes()) {
                return Err(into_pyerr(ConfigError::invalid_response_header_name(name)));
            }
            if configured_response_field_is_forbidden(name.as_bytes()) {
                return Err(into_pyerr(ConfigError::invalid_response_header_name(name)));
            }
            if !header_value_is_valid(value.as_bytes()) {
                return Err(into_pyerr(ConfigError::invalid_response_header_value(name)));
            }
            headers.push(ConfiguredResponseHeader::new(
                Bytes::copy_from_slice(name.as_bytes()),
                Bytes::copy_from_slice(value.as_bytes()),
            ));
        }
        Ok(ResponseHeaderConfig {
            server_header: self.server_header()?,
            date_header: self.get("date_header")?,
            extra_headers: headers.into_boxed_slice(),
        })
    }

    /// Build the acceptor from already-read material.
    ///
    /// `Config.__post_init__` owns every "these settings go together" rule,
    /// so nothing is re-checked here: this turns bytes into an acceptor.
    fn tls(&self, material: &TlsMaterial, http1: bool) -> PyResult<TlsConfig> {
        build_tls_config(material, self.cert_reqs()?, http1).map_err(Into::into)
    }

    fn server_config(&self, tls: Option<TlsConfig>) -> PyResult<ServerConfig> {
        let max_request_body_size = self.nonzero_u64("max_request_body_size")?;
        let binds = self.binds()?;
        let http1 = self.http1()?;

        Ok(ServerConfig {
            binds,
            access_log: self.get("access_log")?,
            log_format: self.get("log_format")?,
            root_path: self.boxed_str("root_path")?,
            root_path_scope: crate::python::PyOnceLock::new(),
            limit_request_fields: self.nonzero_usize("limit_request_fields")?,
            http1,
            http2: self.http2()?,
            max_request_body_size,
            timeout_graceful_shutdown: self.duration("timeout_graceful_shutdown")?,
            timeout_keep_alive: self.optional_duration("timeout_keep_alive")?,
            timeout_request_header: self.optional_duration("timeout_request_header")?,
            timeout_request_body_idle: self.optional_duration("timeout_request_body_idle")?,
            limit_concurrency: self.nonzero_usize("limit_concurrency")?,
            limit_connections: self.nonzero_usize("limit_connections")?,
            max_requests: self.nonzero_u64("max_requests")?,
            runtime_threads: self.get("runtime_threads")?,
            loop_threads: self.get("loop_threads")?,
            websocket: self.websocket()?,
            proxy: self.proxy()?,
            tls,
            timeout_handshake: self.optional_duration("timeout_handshake")?,
            response_headers: self.response_headers()?,
        })
    }
}

/// Convert a configured number of seconds into a `Duration`.
///
/// `Duration::from_secs_f64` panics on NaN, on a negative value, and on any
/// finite value too large to represent — all three of which a configuration
/// file can contain — so the fallible conversion is the one to use.
fn duration_from_seconds(name: &'static str, seconds: f64) -> PyResult<Duration> {
    Duration::try_from_secs_f64(seconds)
        .map_err(|_| into_pyerr(ConfigError::invalid_duration(name)))
}

fn optional_duration(name: &'static str, seconds: f64) -> PyResult<Option<Duration>> {
    if seconds == 0.0 {
        return Ok(None);
    }
    duration_from_seconds(name, seconds).map(Some)
}

fn parse_bind_target(raw: &str) -> PyResult<BindTarget> {
    if let Some(path) = raw.strip_prefix("unix:") {
        if path.is_empty() {
            return Err(into_pyerr(ConfigError::invalid_bind_target(
                "unix",
                raw,
                "path must not be empty",
            )));
        }
        return Ok(BindTarget::Unix {
            path: Box::from(path),
        });
    }
    if let Some(fd) = raw.strip_prefix("fd://") {
        let fd = fd.parse::<i64>().map_err(|_| {
            into_pyerr(ConfigError::invalid_bind_target("fd", raw, "invalid value"))
        })?;
        if fd < 0 {
            return Err(into_pyerr(ConfigError::invalid_bind_target(
                "fd",
                raw,
                "must be non-negative",
            )));
        }
        return Ok(BindTarget::Fd { fd });
    }
    let (host, port) = if let Some(rest) = raw.strip_prefix('[') {
        let (host, port) = rest.rsplit_once("]:").ok_or_else(|| {
            into_pyerr(ConfigError::invalid_bind_target(
                "TCP",
                raw,
                "expected host:port",
            ))
        })?;
        (host, port)
    } else {
        raw.rsplit_once(':').ok_or_else(|| {
            into_pyerr(ConfigError::invalid_bind_target(
                "TCP",
                raw,
                "expected host:port",
            ))
        })?
    };
    if host.is_empty() {
        return Err(into_pyerr(ConfigError::invalid_bind_target(
            "TCP",
            raw,
            "host must not be empty",
        )));
    }
    Ok(BindTarget::Tcp {
        host: Box::from(host),
        port: port.parse::<u16>().map_err(|_| {
            into_pyerr(ConfigError::invalid_bind_target("TCP", raw, "invalid port"))
        })?,
    })
}

fn init_tokio_runtime(worker_threads: usize) -> PyResult<()> {
    static TOKIO_WORKER_THREADS: OnceLock<usize> = OnceLock::new();

    if let Some(initialized_threads) = TOKIO_WORKER_THREADS.get() {
        if *initialized_threads != worker_threads {
            return Err(into_pyerr(
                ConfigError::runtime_threads_already_initialized(
                    *initialized_threads,
                    worker_threads,
                ),
            ));
        }
    } else {
        let _ = TOKIO_WORKER_THREADS.set(worker_threads);
    }

    init_runtime(|| {
        TokioRuntimeBuilder::new_multi_thread()
            .worker_threads(worker_threads)
            .event_interval(TOKIO_EVENT_INTERVAL)
            .global_queue_interval(TOKIO_GLOBAL_QUEUE_INTERVAL)
            .enable_all()
            .build()
            .expect("tokio runtime construction succeeds")
    });
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (config, tls_material=None))]
/// Convert PEM material into an immutable native TLS acceptor, or plaintext.
///
/// Runs the same `server_config` extraction serving uses so `--check-config`
/// rejects bad cert/key/CA bytes before any worker starts.
///
/// Parameters
/// ----------
/// config : Config
///     The server configuration to extract from.
/// tls_material : _TlsMaterial, optional
///     PEM certificate, key and CA bytes; ``None`` serves plaintext.
///
/// Returns
/// -------
/// _PreparedTls
///     The prepared acceptor, to be handed to every worker.
pub(crate) fn prepare_tls(
    config: &Bound<'_, PyAny>,
    tls_material: Option<TlsMaterial>,
) -> PyResult<PreparedTls> {
    let py_config = PyConfig(config);
    let tls = match tls_material {
        Some(material) => Some(py_config.tls(&material, py_config.get("http1")?)?),
        None => None,
    };
    // Discard the full config: the goal is to exercise extraction. The
    // prepared acceptor is what survives into workers.
    let _ = py_config.server_config(tls.clone())?;
    Ok(PreparedTls(tls))
}

#[pyfunction]
/// Print the startup banner for a validated server configuration.
///
/// Parameters
/// ----------
/// config : Config
///     The validated server configuration.
/// tls : _PreparedTls
///     The prepared acceptor, which decides whether the banner says HTTPS.
///
/// Returns
/// -------
/// None
pub(crate) fn emit_banner(config: &Bound<'_, PyAny>, tls: &PreparedTls) -> PyResult<()> {
    let config = PyConfig(config).server_config(None)?;
    emit_access_banner(&config, tls.0.is_some());
    Ok(())
}

#[cfg(Py_GIL_DISABLED)]
fn runtime_gil_enabled(py: Python<'_>) -> PyResult<bool> {
    py.import("sys")?
        .getattr(pyo3::intern!(py, "_is_gil_enabled"))?
        .call0()?
        .extract()
}

#[cfg(not(Py_GIL_DISABLED))]
const fn runtime_gil_enabled(_py: Python<'_>) -> bool {
    true
}

fn effective_loop_count(
    gil_enabled: bool,
    requested: usize,
    factory: SecondaryLoopFactory,
) -> PyResult<usize> {
    if gil_enabled {
        return Ok(1);
    }
    if requested > 1 && matches!(factory, SecondaryLoopFactory::Custom) {
        return Err(PyValueError::new_err(
            "loop_threads > 1 requires loop='asyncio' or loop='uvloop'; the running custom event loop cannot be recreated safely on secondary threads",
        ));
    }
    Ok(requested)
}

fn extract_lifespan_handoff(
    py: Python<'_>,
    app: Py<PyAny>,
    handoff: Option<Py<LifespanHandoff>>,
    main_shard: &Shard,
) -> PyResult<(Py<PyAny>, Option<SecondaryLifespanConfig>)> {
    let Some(handoff) = handoff else {
        return Ok((app, None));
    };
    let handoff = handoff.borrow(py);
    main_shard.install_scope_state(py, handoff.state.clone_ref(py))?;
    Ok((handoff.app.clone_ref(py), Some(handoff.config)))
}

fn start_secondary_lifespan(
    index: usize,
    shard: Shard,
    app: Py<PyAny>,
    config: SecondaryLifespanConfig,
) -> SecondaryLifespanStart {
    let (owner_tx, owner_rx) = oneshot::channel();
    let start = call_awaitable(shard, move |py, shard| {
        let module = py.import("h2corn._lifespan")?;
        let runner = module.getattr("LifespanRunner")?.call1((app,))?;
        let state = runner.getattr("state")?.cast_into::<PyDict>()?.unbind();
        shard.install_scope_state(py, state)?;
        // Cross the runner into Rust ownership before its startup awaitable
        // begins, so rollback can stop/discard it even when startup fails or
        // outlives a peer failure.
        let owned = runner.clone().unbind();
        if owner_tx.send(Ok(owned)).is_err() {
            return Err(PyRuntimeError::new_err(
                "secondary lifespan owner channel closed before delivery",
            ));
        }
        let kwargs = PyDict::new(py);
        kwargs.set_item("required", config.required)?;
        kwargs.set_item("startup_timeout", config.startup_timeout)?;
        module
            .getattr("start_lifespan_runner")?
            .call((runner,), Some(&kwargs))
            .map(Bound::unbind)
    });
    let _ = index;
    (owner_rx, start)
}

async fn stop_secondary_lifespan(
    shard: Shard,
    runner: Py<PyAny>,
    shutdown_timeout: Option<f64>,
) -> Result<(), H2CornError> {
    call_awaitable(shard, move |py, _| {
        let module = py.import("h2corn._lifespan")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("shutdown_timeout", shutdown_timeout)?;
        module
            .getattr("stop_lifespan_runner")?
            .call((runner,), Some(&kwargs))
            .map(Bound::unbind)
    })
    .await
    .map(drop)
    .map_err(H2CornError::from)
}

/// Map a lifespan `JoinSet` failure back to the secondary loop index recorded
/// at spawn time.
fn secondary_loop_index(task_indices: &[(tokio::task::Id, usize)], err: &JoinError) -> usize {
    let id = err.id();
    task_indices
        .iter()
        .find_map(|&(task_id, index)| (task_id == id).then_some(index))
        .expect("every joined secondary lifespan has a recorded loop index")
}

async fn start_secondary_lifespans(
    shards: &[Shard],
    apps: Vec<Py<PyAny>>,
    config: SecondaryLifespanConfig,
) -> (Vec<SecondaryRunner>, Option<H2CornError>) {
    let mut starts = JoinSet::new();
    let mut task_indices = Vec::with_capacity(apps.len());
    for (index, (shard, app)) in shards.iter().skip(1).cloned().zip(apps).enumerate() {
        let (owner_rx, start) = start_secondary_lifespan(index, Arc::clone(&shard), app, config);
        let task = starts.spawn(async move {
            // Retain every delivered owner before awaiting startup so a peer
            // failure can still roll this runner back.
            let owner = match owner_rx.await {
                Ok(Ok(runner)) => Some(runner),
                Ok(Err(err)) => return (index, shard, None, Err(H2CornError::from(err))),
                Err(_) => {
                    return (
                        index,
                        shard,
                        None,
                        Err(H2CornError::from(PyRuntimeError::new_err(
                            "secondary lifespan owner was not delivered",
                        ))),
                    );
                },
            };
            match start.await {
                Ok(_runner) => (index, shard, owner, Ok(())),
                Err(err) => (index, shard, owner, Err(H2CornError::from(err))),
            }
        });
        task_indices.push((task.id(), index));
    }

    // Await every bounded startup outcome. Never abort_all: a higher-index
    // runner that already crossed construction must still receive shutdown.
    let mut outcomes = Vec::with_capacity(starts.len());
    while let Some(joined) = starts.join_next_with_id().await {
        match joined {
            Ok((_, outcome)) => outcomes.push(outcome),
            Err(err) => {
                let index = secondary_loop_index(&task_indices, &err);
                outcomes.push((index, Arc::clone(&shards[index + 1]), None, Err(err.into())));
            },
        }
    }
    outcomes.sort_unstable_by_key(|(index, ..)| *index);

    // Classify each outcome into an optional retained owner and an optional
    // error in one pass. Collect runners only on the path that needs them so
    // Py owners are not held past the all-success return.
    let (runner_slots, error_slots): (Vec<_>, Vec<_>) = outcomes
        .into_iter()
        .map(|(index, shard, owner, outcome)| match (owner, outcome) {
            (Some(runner), Ok(())) => (
                Some(SecondaryRunner {
                    index,
                    shard,
                    runner,
                }),
                None,
            ),
            (Some(runner), Err(err)) => (
                // Startup failed after the owner crossed into Rust: keep it
                // for rollback so discard/shutdown still run on its loop.
                Some(SecondaryRunner {
                    index,
                    shard,
                    runner,
                }),
                Some((index, err)),
            ),
            (None, Err(err)) => (None, Some((index, err))),
            (None, Ok(())) => (
                None,
                Some((
                    index,
                    H2CornError::from(PyRuntimeError::new_err(
                        "secondary lifespan completed startup without an owner",
                    )),
                )),
            ),
        })
        .unzip();

    if error_slots.iter().all(Option::is_none) {
        return (runner_slots.into_iter().flatten().collect(), None);
    }

    // Prefer the lowest-index startup error, then any rollback failure.
    let first_startup_error = {
        let mut errors: Vec<(usize, H2CornError)> = error_slots.into_iter().flatten().collect();
        errors.sort_unstable_by_key(|(index, _)| *index);
        errors.into_iter().next().map(|(_, err)| err)
    };
    // Roll back every retained runner, including successful higher-index ones.
    // Collect into the call so Py owners are not bound past the await.
    let shutdown_error = stop_secondary_lifespans(
        runner_slots.into_iter().flatten().collect(),
        config.shutdown_timeout,
    )
    .await;
    (Vec::new(), first_startup_error.or(shutdown_error))
}

async fn stop_secondary_lifespans(
    runners: Vec<SecondaryRunner>,
    shutdown_timeout: Option<f64>,
) -> Option<H2CornError> {
    let mut stops = JoinSet::new();
    let mut task_indices = Vec::with_capacity(runners.len());
    for SecondaryRunner {
        index,
        shard,
        runner,
    } in runners
    {
        let task = stops.spawn(async move {
            let outcome = stop_secondary_lifespan(shard, runner, shutdown_timeout).await;
            (index, outcome)
        });
        task_indices.push((task.id(), index));
    }

    let mut outcomes = Vec::with_capacity(stops.len());
    let mut errors = Vec::new();
    while let Some(joined) = stops.join_next_with_id().await {
        match joined {
            Ok((_, outcome)) => outcomes.push(outcome),
            Err(err) => {
                let index = secondary_loop_index(&task_indices, &err);
                errors.push((index, err.into()));
            },
        }
    }
    outcomes.sort_unstable_by_key(|(index, _)| *index);
    for (index, outcome) in outcomes {
        if let Err(err) = outcome {
            errors.push((index, err));
        }
    }
    errors.sort_unstable_by_key(|(index, _)| *index);
    errors.into_iter().next().map(|(_, err)| err)
}

async fn run_serve_task(task: ServeTask) {
    let ServeTask {
        app,
        fds,
        config,
        shutdown_trigger,
        ready_trigger,
        quiesce_fd,
        lifespan_config,
        secondary_apps,
        shard_threads,
        main_shard,
        resolve_fut,
    } = task;
    let mut fds = Some(fds);
    // Bind secondary owners where startup runs, not via declare-then-reassign.
    let (secondary_runners, mut result) = match lifespan_config {
        Some(cfg) => {
            let (runners, error) =
                start_secondary_lifespans(&app.shards, secondary_apps, cfg).await;
            (runners, error.map_or(Ok(()), Err))
        },
        None => (Vec::new(), Ok(())),
    };
    if result.is_ok() {
        result = serve_from_fds(
            Arc::clone(&app),
            fds.take()
                .expect("listener ownership is transferred exactly once"),
            config,
            shutdown_trigger,
            ready_trigger,
            quiesce_fd,
        )
        .await;
    }
    // Connection joins settle native ownership, but a hard-aborted H2 child
    // can still be mid-destruction, and dropping a request SlotFuture only
    // queues Task.cancel() on its Python loop. Wait until every scoped
    // AppRuntime owner is gone before lifespan shutdown or loop teardown;
    // applications may catch CancelledError and perform async cleanup before
    // acknowledging completion.
    app.wait_for_scoped_owners().await;
    if let Some(cfg) = lifespan_config {
        let shutdown_error =
            stop_secondary_lifespans(secondary_runners, cfg.shutdown_timeout).await;
        if result.is_ok()
            && let Some(err) = shutdown_error
        {
            result = Err(err);
        }
    }

    // Every connection, request, and secondary lifespan has drained. Drop
    // shard Arc copies before stopping their loop threads; retain Python
    // values for main-loop destruction.
    let loop_owned = match Arc::try_unwrap(app) {
        Ok(shared) => {
            let (python_app, limits, shards) = shared.into_teardown();
            drop(shards);
            Some((python_app, limits))
        },
        Err(app) => {
            let owners = Arc::strong_count(&app);
            result = Err(H2CornError::from(PyRuntimeError::new_err(format!(
                "server teardown retained {owners} AppRuntime owners after draining"
            ))));
            release_app(Arc::clone(&main_shard), app).await;
            None
        },
    };
    if !shard_threads.is_empty() {
        let _ = spawn_blocking(move || drop(shard_threads)).await;
    }
    main_shard.push(PumpEvent::Resolve {
        fut: resolve_fut,
        payload: ResolvePayload::Simple(Box::new(move |py| {
            drop(loop_owned);
            result.into_pyresult().map(|()| py.None())
        })),
    });
    main_shard.push(PumpEvent::Detach);
}

#[pyfunction]
#[pyo3(signature = (app, fds, config, shutdown_trigger, retire_trigger=None, lifespan_handoff=None, ready_trigger=None, quiesce_fd=None, *, prepared_tls))]
/// Adopt listener file descriptors and run one worker until shutdown.
///
/// Borrows every descriptor in `fds` and `quiesce_fd` for this synchronous call,
/// duplicating them before returning the awaitable. The caller retains and may
/// close the sources immediately after return; native duplicates are closed
/// when serving ends or startup fails.
///
/// `prepared_tls` is required: PEM is converted once in `prepare_tls` and
/// reused here. There is no path that reopens certificate files in a worker.
///
/// Parameters
/// ----------
/// app : object
///     The ASGI application to serve.
/// fds : list of int
///     Listener descriptors to borrow and duplicate.
/// config : Config
///     The validated server configuration.
/// shutdown_trigger : object
///     Awaited to begin a graceful shutdown.
/// retire_trigger : object, optional
///     Awaited to stop accepting while draining in-flight requests.
/// lifespan_handoff : _LifespanHandoff, optional
///     Carries lifespan state from the parent process.
/// ready_trigger : object, optional
///     Resolved once the worker is accepting connections.
/// quiesce_fd : int, optional
///     Descriptor signalling quiesce; borrowed and duplicated on Unix.
/// prepared_tls : _PreparedTls
///     The acceptor built once by ``prepare_tls``.
///
/// Returns
/// -------
/// Awaitable[None]
///     Completes when the worker has stopped serving.
#[expect(
    clippy::needless_pass_by_value,
    reason = "PyO3 requires PyRef by value for class arguments"
)]
pub(crate) fn serve_fds<'py>(
    py: Python<'py>,
    app: Py<PyAny>,
    fds: Vec<PythonRawHandle>,
    config: &Bound<'py, PyAny>,
    shutdown_trigger: Py<PyAny>,
    retire_trigger: Option<Py<PyAny>>,
    lifespan_handoff: Option<Py<LifespanHandoff>>,
    ready_trigger: Option<Py<PyAny>>,
    quiesce_fd: Option<PythonRawHandle>,
    prepared_tls: PyRef<'_, PreparedTls>,
) -> PyResult<Bound<'py, PyAny>> {
    // Acquire RAII ownership before any fallible parsing/runtime setup. Every
    // early return and partial listener adoption now closes the remainder.
    let (fds, quiesce_fd) = own_serve_fds(fds, quiesce_fd).map_err(|error| match error {
        OwnFdsError::Structural(message) => PyValueError::new_err(message),
        OwnFdsError::Io(error) => PyOSError::new_err(error),
    })?;
    let py_config = PyConfig(config);
    let config = py_config.server_config(prepared_tls.0.clone())?;
    init_tokio_runtime(config.runtime_threads)?;
    let config = Arc::new(config);

    let (main_shard, loop_factory) = ShardHandle::from_running_loop(py)?;
    let (app, lifespan_config) =
        match extract_lifespan_handoff(py, app, lifespan_handoff, &main_shard) {
            Ok(handoff) => handoff,
            Err(err) => {
                main_shard.detach(py);
                return Err(err);
            },
        };
    let limits = RuntimeLimits::new(&config, &main_shard, retire_trigger).map(Arc::new);

    // Extra loop shards only run on free-threaded builds; on a GIL build the
    // loop_threads setting is a no-op (the GIL would serialize the loops
    // anyway), so it is silently capped to the single main loop.
    #[cfg(Py_GIL_DISABLED)]
    let gil_enabled = match runtime_gil_enabled(py) {
        Ok(enabled) => enabled,
        Err(err) => {
            main_shard.detach(py);
            return Err(err);
        },
    };
    #[cfg(not(Py_GIL_DISABLED))]
    let gil_enabled = runtime_gil_enabled(py);
    let loop_count = match effective_loop_count(gil_enabled, config.loop_threads, loop_factory) {
        Ok(count) => count,
        Err(err) => {
            main_shard.detach(py);
            return Err(err);
        },
    };
    let mut shard_threads: Vec<ShardThread> = Vec::new();
    let mut shards = vec![Arc::clone(&main_shard)];
    for index in 1..loop_count {
        let thread = match spawn_shard_thread(index, loop_factory) {
            Ok(thread) => thread,
            Err(err) => {
                for thread in shard_threads {
                    thread.shutdown();
                }
                main_shard.detach(py);
                return Err(err);
            },
        };
        shards.push(Arc::clone(thread.shard()));
        shard_threads.push(thread);
    }
    let secondary_apps = if lifespan_config.is_some() {
        repeat_with(|| app.clone_ref(py))
            .take(shards.len() - 1)
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let app: AppRuntimeHandle = Arc::new(AppRuntime::new(app, shards.into_boxed_slice(), limits));

    // The Python side awaits this duck future; it resolves when the server
    // future completes (shutdown or fatal error).
    let server_done = match new_rust_future(py, Arc::clone(&main_shard)) {
        Ok(future) => future,
        Err(err) => {
            drop(app);
            drop(shard_threads);
            main_shard.detach(py);
            return Err(err);
        },
    };
    runtime().spawn(run_serve_task(ServeTask {
        app,
        fds,
        config,
        shutdown_trigger,
        ready_trigger,
        quiesce_fd,
        lifespan_config,
        secondary_apps,
        shard_threads,
        main_shard,
        resolve_fut: server_done.clone_ref(py),
    }));
    Ok(server_done.into_bound(py).into_any())
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;

    use pyo3::types::{PyAny, PyDict, PyModule, PyTuple};

    use super::*;
    use crate::config::LogFormat;
    use crate::proxy_protocol::{Cidr, TrustedPeer};

    fn config_stub(py: Python<'_>) -> Bound<'_, PyAny> {
        let builtins = PyModule::import(py, "builtins").expect("builtins imports");
        let ty = builtins.getattr("type").expect("type builtin exists");
        let cls = ty
            .call1(("ConfigStub", PyTuple::empty(py), PyDict::new(py)))
            .expect("stub class is created");
        cls.call0().expect("stub config is instantiated")
    }

    #[test]
    fn runtime_gil_detection_matches_the_interpreter() {
        Python::initialize();
        Python::attach(|py| {
            #[cfg(Py_GIL_DISABLED)]
            let detected = runtime_gil_enabled(py).unwrap();
            #[cfg(not(Py_GIL_DISABLED))]
            let detected = runtime_gil_enabled(py);
            #[cfg(Py_GIL_DISABLED)]
            let expected = py
                .import("sys")
                .unwrap()
                .getattr("_is_gil_enabled")
                .unwrap()
                .call0()
                .unwrap()
                .extract::<bool>()
                .unwrap();
            #[cfg(not(Py_GIL_DISABLED))]
            let expected = true;
            assert_eq!(detected, expected);
        });
    }

    #[test]
    fn unsupported_custom_multiloop_is_rejected_and_gil_builds_stay_single_loop() {
        effective_loop_count(false, 4, SecondaryLoopFactory::Custom).unwrap_err();
        assert_eq!(
            effective_loop_count(true, 4, SecondaryLoopFactory::Asyncio).unwrap(),
            1
        );
        assert_eq!(
            effective_loop_count(false, 4, SecondaryLoopFactory::Uvloop).unwrap(),
            4
        );
    }

    #[test]
    fn typed_lifespan_handoff_installs_exact_app_state_and_config() {
        Python::initialize();
        Python::attach(|py| {
            let wrapper = config_stub(py).unbind();
            let original = config_stub(py).unbind();
            let state = PyDict::new(py);
            state.set_item("ready", true).unwrap();
            let handoff = Py::new(
                py,
                LifespanHandoff::new(
                    original.clone_ref(py),
                    state.unbind(),
                    true,
                    Some(1.5),
                    Some(2.5),
                ),
            )
            .unwrap();
            let shard = ShardHandle::test_stub(py);

            let (app, config) =
                extract_lifespan_handoff(py, wrapper, Some(handoff), &shard).unwrap();
            assert!(app.bind(py).is(original.bind(py)));
            let config = config.expect("typed handoff enables secondary lifespan");
            assert!(config.required);
            assert_eq!(config.startup_timeout, Some(1.5));
            assert_eq!(config.shutdown_timeout, Some(2.5));
        });
    }

    fn set_core_options(config: &Bound<'_, PyAny>) {
        config.setattr("bind", ("127.0.0.9:48123",)).unwrap();
        config.setattr("max_requests", 41).unwrap();
        config.setattr("max_requests_jitter", 7).unwrap();
        config.setattr("timeout_worker_healthcheck", 0.0).unwrap();
        config.setattr("access_log", false).unwrap();
        config.setattr("log_format", "json").unwrap();
        config.setattr("root_path", "/api").unwrap();
        config.setattr("max_request_body_size", 987_654).unwrap();
        config.setattr("runtime_threads", 7).unwrap();
        config.setattr("loop_threads", 1).unwrap();
    }

    fn set_http_options(config: &Bound<'_, PyAny>) {
        config.setattr("http1", false).unwrap();
        config.setattr("limit_request_head_size", 8_193).unwrap();
        config.setattr("limit_request_line", 4_097).unwrap();
        config.setattr("limit_request_fields", 17).unwrap();
        config.setattr("limit_request_field_size", 211).unwrap();
        config.setattr("max_concurrent_streams", 321).unwrap();
        config.setattr("h2_max_header_list_size", 65_432).unwrap();
        config.setattr("h2_max_header_block_size", 76_543).unwrap();
        config
            .setattr("h2_max_inbound_frame_size", 0x0001_0000)
            .unwrap();
        config
            .setattr("h2_initial_stream_window_size", 0x0008_0000)
            .unwrap();
        config
            .setattr("h2_initial_connection_window_size", 0x0010_0000)
            .unwrap();
    }

    fn set_timeout_and_limit_options(config: &Bound<'_, PyAny>) {
        config.setattr("timeout_handshake", 1.25).unwrap();
        config.setattr("timeout_graceful_shutdown", 12.5).unwrap();
        config.setattr("timeout_keep_alive", 2.5).unwrap();
        config.setattr("timeout_request_header", 4.5).unwrap();
        config.setattr("timeout_request_body_idle", 5.5).unwrap();
        config.setattr("h2_timeout_response_stall", 6.0).unwrap();
        config.setattr("timeout_lifespan_startup", 6.5).unwrap();
        config.setattr("timeout_lifespan_shutdown", 8.5).unwrap();
        config.setattr("limit_concurrency", 23).unwrap();
        config.setattr("limit_connections", 29).unwrap();
    }

    fn set_websocket_and_proxy_options(config: &Bound<'_, PyAny>, py: Python<'_>) {
        config
            .setattr("websocket_max_message_size", 54_321)
            .unwrap();
        config
            .setattr("websocket_per_message_deflate", false)
            .unwrap();
        config.setattr("websocket_ping_interval", 9.5).unwrap();
        config.setattr("websocket_ping_timeout", 11.5).unwrap();
        config.setattr("proxy_headers", true).unwrap();
        config
            .setattr("forwarded_allow_ips", ("127.0.0.1", "10.0.0.0/8", "unix"))
            .unwrap();
        config
            .setattr(
                "forwarded_fields",
                ("for", "proto", "host", "port", "prefix"),
            )
            .unwrap();
        config.setattr("proxy_protocol", "v2").unwrap();
        config.setattr("certfile", py.None()).unwrap();
        config.setattr("keyfile", py.None()).unwrap();
        config.setattr("ca_certs", py.None()).unwrap();
        config.setattr("cert_reqs", "none").unwrap();
        config.setattr("server_header", "on").unwrap();
        config.setattr("date_header", false).unwrap();
        config
            .setattr("response_headers", ("x-demo: 1", "x-extra: 2"))
            .unwrap();
    }

    fn assert_core_config(extracted: &ServerConfig) {
        match &extracted.binds[..] {
            [BindTarget::Tcp { host, port }] => {
                assert_eq!(host.as_ref(), "127.0.0.9");
                assert_eq!(*port, 48_123);
            },
            _ => panic!("expected one TCP bind"),
        }
        assert!(!extracted.access_log);
        assert_eq!(extracted.log_format, LogFormat::Json);
        assert_eq!(extracted.root_path.as_ref(), "/api");
        assert_eq!(
            extracted.max_request_body_size.map(NonZeroU64::get),
            Some(987_654)
        );
        assert_eq!(extracted.max_requests.map(NonZeroU64::get), Some(41));
        assert_eq!(extracted.runtime_threads, 7);
    }

    fn assert_http_config(extracted: &ServerConfig) {
        assert!(!extracted.http1.enabled);
        assert_eq!(
            extracted
                .http1
                .limit_request_head_size
                .map(NonZeroUsize::get),
            Some(8_193),
        );
        assert_eq!(
            extracted.http1.limit_request_line.map(NonZeroUsize::get),
            Some(4_097),
        );
        assert_eq!(
            extracted.limit_request_fields.map(NonZeroUsize::get),
            Some(17)
        );
        assert_eq!(
            extracted
                .http1
                .limit_request_field_size
                .map(NonZeroUsize::get),
            Some(211),
        );
        assert_eq!(extracted.http2.max_concurrent_streams.get(), 321);
        assert_eq!(
            extracted.http2.max_header_list_size.map(NonZeroUsize::get),
            Some(65_432),
        );
        assert_eq!(
            extracted.http2.max_header_block_size.map(NonZeroUsize::get),
            Some(76_543),
        );
        assert_eq!(extracted.http2.max_inbound_frame_size.get(), 0x0001_0000);
        assert_eq!(
            extracted.http2.initial_stream_window_size.get(),
            0x0008_0000
        );
        assert_eq!(
            extracted.http2.initial_connection_window_size.get(),
            0x0010_0000
        );
    }

    fn assert_timeout_and_limit_config(extracted: &ServerConfig) {
        assert_eq!(
            extracted.timeout_handshake,
            Some(Duration::from_secs_f64(1.25))
        );
        assert_eq!(
            extracted.timeout_graceful_shutdown,
            Duration::from_secs_f64(12.5),
        );
        assert_eq!(
            extracted.timeout_keep_alive,
            Some(Duration::from_secs_f64(2.5))
        );
        assert_eq!(
            extracted.timeout_request_header,
            Some(Duration::from_secs_f64(4.5))
        );
        assert_eq!(
            extracted.timeout_request_body_idle,
            Some(Duration::from_secs_f64(5.5))
        );
        assert_eq!(
            extracted.http2.timeout_response_stall,
            Some(Duration::from_secs_f64(6.0))
        );
        assert_eq!(extracted.limit_concurrency.map(NonZeroUsize::get), Some(23));
        assert_eq!(extracted.limit_connections.map(NonZeroUsize::get), Some(29));
    }

    fn assert_websocket_and_proxy_config(extracted: &ServerConfig) {
        assert_eq!(
            extracted.websocket.max_message_size.map(NonZeroUsize::get),
            Some(54_321),
        );
        assert!(!extracted.websocket.per_message_deflate);
        let keep_alive = extracted
            .websocket
            .keep_alive
            .as_ref()
            .expect("ping interval 9.5 enables keepalive");
        assert_eq!(keep_alive.interval, Duration::from_secs_f64(9.5));
        assert_eq!(keep_alive.timeout, Some(Duration::from_secs_f64(11.5)));
        assert!(extracted.proxy.trust_headers);
        assert_eq!(
            extracted.proxy.forwarded_fields,
            ForwardedFields::FOR
                | ForwardedFields::PROTO
                | ForwardedFields::HOST
                | ForwardedFields::PORT
                | ForwardedFields::PREFIX,
        );
        assert_eq!(extracted.proxy.protocol, ProxyProtocolMode::V2);
        assert_eq!(extracted.proxy.trusted_peers.len(), 3);
        assert_eq!(
            extracted.response_headers.server_header.as_deref(),
            Some(config::SERVER_NAME.as_bytes())
        );
        assert!(!extracted.response_headers.date_header);
        assert_eq!(extracted.response_headers.extra_headers.len(), 2);
        assert!(matches!(
            extracted.proxy.trusted_peers[0],
            TrustedPeer::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST))
        ));
        assert!(matches!(
            extracted.proxy.trusted_peers[1],
            TrustedPeer::Cidr(Cidr::V4 {
                network: 0x0A00_0000,
                mask: 0xFF00_0000,
            })
        ));
        assert!(matches!(
            extracted.proxy.trusted_peers[2],
            TrustedPeer::Unix
        ));
    }

    #[test]
    fn extract_server_config_matches_python_stub() {
        Python::initialize();
        Python::attach(|py| {
            let config = config_stub(py);
            set_core_options(&config);
            set_http_options(&config);
            set_timeout_and_limit_options(&config);
            set_websocket_and_proxy_options(&config, py);

            let extracted = PyConfig(&config)
                .server_config(None)
                .expect("config extraction succeeds");

            assert_core_config(&extracted);
            assert_http_config(&extracted);
            assert_timeout_and_limit_config(&extracted);
            assert_websocket_and_proxy_config(&extracted);
        });
    }

    #[test]
    fn parse_bind_target_rejects_empty_unix_path_and_negative_fd() {
        Python::initialize();
        Python::attach(|_| {
            parse_bind_target("unix:").unwrap_err();
            parse_bind_target("fd://-1").unwrap_err();
            parse_bind_target(":8080").unwrap_err();
        });
    }
}
