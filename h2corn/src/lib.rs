// Retained for the WebSocket masking SIMD fast path in `websocket::codec::mask`.
#![feature(portable_simd)]

mod async_util;
mod bridge;
mod buffered_events;
mod config;
mod console;
mod error;
mod ext;
mod frame;
mod h1;
mod h2;
mod header_value;
mod hpack;
mod http;
mod proxy;
mod python;
mod runtime;
mod sendfile;
mod server;
mod smallvec_deque;
mod tls;
mod websocket;

use bytes::Bytes;
use config::{
    BindTarget, ClientCertMode, Http1Config, Http2Config, ProxyConfig, ResponseHeaderConfig,
    ServerConfig, TlsConfig, WebSocketConfig,
};
use error::{ConfigError, IntoPyResult, into_pyerr};
use header_value::header_value_is_valid;
use parking_lot::RwLock;
use proxy::{ProxyProtocolMode, TrustedPeer, parse_trusted_peer};
use pyo3::{
    conversion::FromPyObjectOwned, exceptions::PyValueError, prelude::*, sync::OnceExt,
    types::PyAnyMethods,
};
use pyo3_async_runtimes::tokio::{
    future_into_py_with_locals, get_current_locals, init as init_tokio,
};
use smallvec::SmallVec;
use std::{
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    path::PathBuf,
    sync::{Arc, Once, OnceLock},
    time::Duration,
};
use tokio::runtime::Builder as TokioRuntimeBuilder;

use crate::runtime::SharedApp;

const TOKIO_EVENT_INTERVAL: u32 = 31;
const TOKIO_GLOBAL_QUEUE_INTERVAL: u32 = 31;

fn finite_duration(name: &'static str, seconds: f64) -> PyResult<Duration> {
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(into_pyerr(ConfigError::invalid_finite_duration(name)));
    }
    Ok(Duration::from_secs_f64(seconds))
}

fn optional_duration(name: &'static str, seconds: f64) -> PyResult<Option<Duration>> {
    if seconds == 0.0 {
        return Ok(None);
    }
    finite_duration(name, seconds).map(Some)
}

struct PyConfig<'py>(&'py Bound<'py, PyAny>);

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
        finite_duration(name, self.get::<f64>(name)?)
    }

    fn optional_duration(&self, name: &'static str) -> PyResult<Option<Duration>> {
        optional_duration(name, self.get::<f64>(name)?)
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
            value => Err(PyValueError::new_err(format!(
                "invalid cert_reqs mode: {value:?}"
            ))),
        }
    }

    fn optional_path(&self, name: &str) -> PyResult<Option<PathBuf>> {
        let value = self.attr(name)?;
        if value.is_none() {
            return Ok(None);
        }
        Ok(Some(PathBuf::from(
            value.call_method0("__fspath__")?.extract::<String>()?,
        )))
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

    fn websocket_message_size_limit(
        &self,
        max_request_body_size: Option<NonZeroU64>,
    ) -> PyResult<Option<NonZeroUsize>> {
        if let Some(value) = self.get::<Option<usize>>("websocket_max_message_size")? {
            return Ok(NonZeroUsize::new(value));
        }
        Ok(max_request_body_size.map(|limit| {
            let limit = limit.get().min(usize::MAX as u64) as usize;
            NonZeroUsize::new(limit).expect("clamped body limit is positive")
        }))
    }

    fn binds(&self) -> PyResult<Box<[BindTarget]>> {
        let raw_binds = self.get::<Vec<String>>("bind")?;
        let mut fd_is_unix = self.get::<Vec<bool>>("_bind_fd_is_unix")?;
        if fd_is_unix.is_empty() {
            fd_is_unix.resize(raw_binds.len(), false);
        }
        if raw_binds.len() != fd_is_unix.len() {
            return Err(into_pyerr(ConfigError::BindMetadataLengthMismatch));
        }
        raw_binds
            .iter()
            .zip(fd_is_unix)
            .map(|(raw, is_unix)| parse_bind_target(raw, is_unix))
            .collect::<PyResult<Vec<_>>>()
            .map(Vec::into_boxed_slice)
    }

    fn http1(&self) -> PyResult<Http1Config> {
        Ok(Http1Config {
            enabled: self.get("http1")?,
            limit_request_head_size: self.nonzero_usize("limit_request_head_size")?,
            limit_request_line: self.nonzero_usize("limit_request_line")?,
            limit_request_fields: self.nonzero_usize("limit_request_fields")?,
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
        })
    }

    fn websocket(&self, max_request_body_size: Option<NonZeroU64>) -> PyResult<WebSocketConfig> {
        Ok(WebSocketConfig {
            message_size_limit: self.websocket_message_size_limit(max_request_body_size)?,
            per_message_deflate: self.get("websocket_per_message_deflate")?,
            ping_interval: self.optional_duration("websocket_ping_interval")?,
            ping_timeout: self.optional_duration("websocket_ping_timeout")?,
        })
    }

    fn proxy(&self) -> PyResult<ProxyConfig> {
        Ok(ProxyConfig {
            trust_headers: self.get("proxy_headers")?,
            trusted_peers: self.trusted_peers()?,
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
            if !http::header::response_header_name_is_valid(name.as_bytes()) {
                return Err(into_pyerr(ConfigError::invalid_response_header_name(name)));
            }
            if !header_value_is_valid(value.as_bytes()) {
                return Err(into_pyerr(ConfigError::invalid_response_header_value(name)));
            }
            headers.push((
                Bytes::copy_from_slice(name.as_bytes()),
                Bytes::copy_from_slice(value.as_bytes()),
            ));
        }
        Ok(ResponseHeaderConfig {
            server_header: self.get("server_header")?,
            date_header: self.get("date_header")?,
            extra_headers: headers.into_boxed_slice(),
            cached_date: RwLock::default(),
        })
    }

    fn tls(&self, http1: bool, binds: &[BindTarget]) -> PyResult<Option<TlsConfig>> {
        let certfile = self.optional_path("certfile")?;
        let keyfile = self.optional_path("keyfile")?;
        let ca_certs = self.optional_path("ca_certs")?;
        let cert_reqs = self.cert_reqs()?;
        let (certfile, keyfile) = match (certfile, keyfile) {
            (None, None) => {
                if ca_certs.is_some() || cert_reqs != ClientCertMode::None {
                    return Err(PyValueError::new_err(
                        "client certificate verification requires certfile and keyfile",
                    ));
                }
                return Ok(None);
            }
            (Some(certfile), Some(keyfile)) => (certfile, keyfile),
            _ => {
                return Err(PyValueError::new_err(
                    "certfile and keyfile must be configured together",
                ));
            }
        };
        if ca_certs.is_some() && cert_reqs == ClientCertMode::None {
            return Err(PyValueError::new_err(
                "ca_certs requires cert_reqs to be optional or required",
            ));
        }
        if cert_reqs != ClientCertMode::None && ca_certs.is_none() {
            return Err(PyValueError::new_err(
                "cert_reqs optional/required requires ca_certs",
            ));
        }
        if binds.iter().any(|bind| {
            matches!(
                bind,
                BindTarget::Unix { .. } | BindTarget::Fd { is_unix: true, .. }
            )
        }) {
            return Err(PyValueError::new_err(
                "TLS is supported only on TCP listeners",
            ));
        }
        tls::build_tls_config(&certfile, &keyfile, ca_certs.as_deref(), cert_reqs, http1)
            .map(Some)
            .map_err(Into::into)
    }

    fn server_config(&self) -> PyResult<ServerConfig> {
        let max_request_body_size = self.nonzero_u64("max_request_body_size")?;
        let binds = self.binds()?;
        let http1 = self.http1()?;
        let tls = self.tls(http1.enabled, &binds)?;

        Ok(ServerConfig {
            binds,
            access_log: self.get("access_log")?,
            root_path: self.boxed_str("root_path")?,
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
            websocket: self.websocket(max_request_body_size)?,
            proxy: self.proxy()?,
            tls,
            timeout_handshake: self.duration("timeout_handshake")?,
            response_headers: self.response_headers()?,
        })
    }
}

fn parse_bind_target(raw: &str, fd_is_unix: bool) -> PyResult<BindTarget> {
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
        return Ok(BindTarget::Fd {
            fd,
            is_unix: fd_is_unix,
        });
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

fn init_tokio_runtime(py: Python<'_>, worker_threads: usize) -> PyResult<()> {
    static TOKIO_INIT: Once = Once::new();
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

    TOKIO_INIT.call_once_py_attached(py, || {
        let mut builder = TokioRuntimeBuilder::new_multi_thread();
        builder.worker_threads(worker_threads);
        builder.event_interval(TOKIO_EVENT_INTERVAL);
        builder.global_queue_interval(TOKIO_GLOBAL_QUEUE_INTERVAL);
        builder.enable_all();
        init_tokio(builder);
    });
    Ok(())
}

#[pyfunction]
fn emit_banner(config: &Bound<'_, PyAny>) -> PyResult<()> {
    let config = PyConfig(config).server_config()?;
    console::emit_banner(&config);
    Ok(())
}

#[pyfunction]
fn validate_config(config: &Bound<'_, PyAny>) -> PyResult<()> {
    let _ = PyConfig(config).server_config()?;
    Ok(())
}

#[pyfunction]
fn serve_fds<'py>(
    py: Python<'py>,
    app: Py<PyAny>,
    fds: Vec<i64>,
    config: &Bound<'py, PyAny>,
    shutdown_trigger: Py<PyAny>,
    retire_trigger: Option<Py<PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    let config = PyConfig(config).server_config()?;
    init_tokio_runtime(py, config.runtime_threads)?;
    let config: &'static ServerConfig = Box::leak(Box::new(config));

    let locals = get_current_locals(py)?;
    let limits = runtime::RuntimeLimits::new(config, retire_trigger).map(Arc::new);
    let app = Arc::new(SharedApp {
        app,
        locals: locals.clone(),
        limits,
    });
    future_into_py_with_locals(py, locals, async move {
        server::serve_from_fds(app, fds.into_boxed_slice(), config, shutdown_trigger)
            .await
            .into_pyresult()?;
        Python::attach(|py| Ok(py.None()))
    })
}

#[pymodule]
#[pyo3(name = "_lib")]
fn h2corn_lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(emit_banner, m)?)?;
    m.add_function(wrap_pyfunction!(validate_config, m)?)?;
    m.add_function(wrap_pyfunction!(serve_fds, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr},
        time::Duration,
    };

    use pyo3::types::{PyDict, PyModule, PyTuple};

    use super::*;
    use crate::proxy::{Cidr, TrustedPeer};

    #[test]
    fn extract_server_config_matches_python_stub() {
        Python::attach(|py| {
            let builtins = PyModule::import(py, "builtins").expect("builtins imports");
            let ty = builtins.getattr("type").expect("type builtin exists");
            let cls = ty
                .call1(("ConfigStub", PyTuple::empty(py), PyDict::new(py)))
                .expect("stub class is created");
            let config = cls.call0().expect("stub config is instantiated");

            config.setattr("bind", ("127.0.0.9:48123",)).unwrap();
            config.setattr("_bind_fd_is_unix", (false,)).unwrap();
            config.setattr("max_requests", 41).unwrap();
            config.setattr("max_requests_jitter", 7).unwrap();
            config.setattr("timeout_worker_healthcheck", 0.0).unwrap();
            config.setattr("access_log", false).unwrap();
            config.setattr("root_path", "/api").unwrap();
            config.setattr("http1", false).unwrap();
            config.setattr("limit_request_head_size", 8_193).unwrap();
            config.setattr("limit_request_line", 4_097).unwrap();
            config.setattr("limit_request_fields", 17).unwrap();
            config.setattr("limit_request_field_size", 211).unwrap();
            config.setattr("max_concurrent_streams", 321).unwrap();
            config.setattr("h2_max_header_list_size", 65_432).unwrap();
            config.setattr("h2_max_header_block_size", 76_543).unwrap();
            config.setattr("h2_max_inbound_frame_size", 65_536).unwrap();
            config.setattr("max_request_body_size", 987_654).unwrap();
            config.setattr("timeout_handshake", 1.25).unwrap();
            config.setattr("timeout_graceful_shutdown", 12.5).unwrap();
            config.setattr("timeout_keep_alive", 2.5).unwrap();
            config.setattr("timeout_request_header", 4.5).unwrap();
            config.setattr("timeout_request_body_idle", 5.5).unwrap();
            config.setattr("timeout_lifespan_startup", 6.5).unwrap();
            config.setattr("timeout_lifespan_shutdown", 8.5).unwrap();
            config.setattr("limit_concurrency", 23).unwrap();
            config.setattr("limit_connections", 29).unwrap();
            config.setattr("runtime_threads", 7).unwrap();
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
            config.setattr("proxy_protocol", "v2").unwrap();
            config.setattr("certfile", py.None()).unwrap();
            config.setattr("keyfile", py.None()).unwrap();
            config.setattr("ca_certs", py.None()).unwrap();
            config.setattr("cert_reqs", "none").unwrap();
            config.setattr("server_header", true).unwrap();
            config.setattr("date_header", false).unwrap();
            config
                .setattr("response_headers", ("x-demo: 1", "x-extra: 2"))
                .unwrap();

            let extracted = PyConfig(&config)
                .server_config()
                .expect("config extraction succeeds");

            match &extracted.binds[..] {
                [BindTarget::Tcp { host, port }] => {
                    assert_eq!(host.as_ref(), "127.0.0.9");
                    assert_eq!(*port, 48_123);
                }
                _ => panic!("expected one TCP bind"),
            }
            assert!(!extracted.access_log);
            assert_eq!(extracted.root_path.as_ref(), "/api");

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
                extracted.http1.limit_request_fields.map(NonZeroUsize::get),
                Some(17),
            );
            assert_eq!(
                extracted
                    .http1
                    .limit_request_field_size
                    .map(NonZeroUsize::get),
                Some(211),
            );

            assert_eq!(extracted.http2.max_concurrent_streams, 321);
            assert_eq!(
                extracted.http2.max_header_list_size.map(NonZeroUsize::get),
                Some(65_432),
            );
            assert_eq!(
                extracted.http2.max_header_block_size.map(NonZeroUsize::get),
                Some(76_543),
            );
            assert_eq!(extracted.http2.max_inbound_frame_size.get(), 65_536);

            assert_eq!(
                extracted.max_request_body_size.map(NonZeroU64::get),
                Some(987_654),
            );
            assert_eq!(extracted.timeout_handshake, Duration::from_secs_f64(1.25));
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
            assert_eq!(extracted.limit_concurrency.map(NonZeroUsize::get), Some(23));
            assert_eq!(extracted.limit_connections.map(NonZeroUsize::get), Some(29));
            assert_eq!(extracted.max_requests.map(NonZeroU64::get), Some(41));
            assert_eq!(extracted.runtime_threads, 7);

            assert_eq!(
                extracted
                    .websocket
                    .message_size_limit
                    .map(NonZeroUsize::get),
                Some(54_321),
            );
            assert!(!extracted.websocket.per_message_deflate);
            assert_eq!(
                extracted.websocket.ping_interval,
                Some(Duration::from_secs_f64(9.5)),
            );
            assert_eq!(
                extracted.websocket.ping_timeout,
                Some(Duration::from_secs_f64(11.5)),
            );

            assert!(extracted.proxy.trust_headers);
            assert_eq!(extracted.proxy.protocol, ProxyProtocolMode::V2);
            assert_eq!(extracted.proxy.trusted_peers.len(), 3);
            assert!(extracted.response_headers.server_header);
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
        });
    }

    #[test]
    fn parse_bind_target_rejects_empty_unix_path_and_negative_fd() {
        Python::attach(|_| {
            assert!(parse_bind_target("unix:", false).is_err());
            assert!(parse_bind_target("fd://-1", false).is_err());
            assert!(parse_bind_target(":8080", false).is_err());
        });
    }
}
