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
mod hpack;
mod http;
mod proxy;
mod python;
mod runtime;
mod sendfile;
mod server;
mod smallvec_deque;
mod websocket;

use bytes::Bytes;
use config::{
    Http1Config, Http2Config, ProxyConfig, ResponseHeaderConfig, ServerBind, ServerConfig,
    WebSocketConfig,
};
use error::IntoPyResult;
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
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
    sync::{Arc, Once},
    time::Duration,
};
use tokio::runtime::Builder as TokioRuntimeBuilder;

use crate::runtime::SharedApp;

const TOKIO_EVENT_INTERVAL: u32 = 31;
const TOKIO_GLOBAL_QUEUE_INTERVAL: u32 = 31;

fn finite_duration(name: &str, seconds: f64) -> PyResult<Duration> {
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(PyValueError::new_err(format!(
            "{name} must be a finite non-negative number",
        )));
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
            value => Err(PyValueError::new_err(format!(
                "invalid proxy_protocol mode: {value}",
            ))),
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

    fn bind(&self) -> PyResult<ServerBind> {
        if let Some(fd) = self.get::<Option<i64>>("fd")? {
            return Ok(ServerBind::Fd {
                fd,
                is_unix: self.get::<Option<bool>>("_fd_is_unix")?.unwrap_or(false),
            });
        }
        if let Some(path) = self.get::<Option<PathBuf>>("uds")? {
            return Ok(ServerBind::Unix {
                path: Box::from(
                    path.to_str()
                        .ok_or_else(|| PyValueError::new_err("uds must be a valid UTF-8 path"))?,
                ),
            });
        }

        Ok(ServerBind::Tcp {
            host: self.boxed_str("host")?,
            port: self.get("port")?,
        })
    }

    fn http1(&self) -> PyResult<Http1Config> {
        Ok(Http1Config {
            enabled: self.get("http1")?,
            limit_request_line: self.nonzero_usize("limit_request_line")?,
            limit_request_fields: self.nonzero_usize("limit_request_fields")?,
            limit_request_field_size: self.nonzero_usize("limit_request_field_size")?,
        })
    }

    fn http2(&self) -> PyResult<Http2Config> {
        Ok(Http2Config {
            max_concurrent_streams: self.get("max_concurrent_streams")?,
            max_header_list_size: self.nonzero_usize("h2_max_header_list_size")?,
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
                return Err(PyValueError::new_err(format!(
                    "invalid response header {raw:?}: expected `name: value`",
                )));
            };
            let name = name.trim();
            let value = value.trim();
            if !http::header::response_header_name_is_valid(name.as_bytes()) {
                return Err(PyValueError::new_err(format!(
                    "invalid response header name {name:?}",
                )));
            }
            if !http::header::header_value_is_valid(value.as_bytes()) {
                return Err(PyValueError::new_err(format!(
                    "invalid response header value for {name:?}",
                )));
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

    fn server_config(&self) -> PyResult<ServerConfig> {
        let max_request_body_size = self.nonzero_u64("max_request_body_size")?;

        Ok(ServerConfig {
            bind: self.bind()?,
            access_log: self.get("access_log")?,
            root_path: self.boxed_str("root_path")?,
            http1: self.http1()?,
            http2: self.http2()?,
            max_request_body_size,
            timeout_graceful_shutdown: self.duration("timeout_graceful_shutdown")?,
            timeout_keep_alive: self.optional_duration("timeout_keep_alive")?,
            timeout_read: self.optional_duration("timeout_read")?,
            limit_concurrency: self
                .nonzero_usize("limit_concurrency")?
                .map(NonZeroUsize::get),
            max_requests: self.nonzero_u64("max_requests")?.map(NonZeroU64::get),
            websocket: self.websocket(max_request_body_size)?,
            proxy: self.proxy()?,
            timeout_handshake: self.duration("timeout_handshake")?,
            response_headers: self.response_headers()?,
        })
    }
}

fn init_tokio_runtime(py: Python<'_>) {
    static TOKIO_INIT: Once = Once::new();

    TOKIO_INIT.call_once_py_attached(py, || {
        let mut builder = TokioRuntimeBuilder::new_multi_thread();
        builder.worker_threads(2);
        builder.event_interval(TOKIO_EVENT_INTERVAL);
        builder.global_queue_interval(TOKIO_GLOBAL_QUEUE_INTERVAL);
        builder.enable_all();
        init_tokio(builder);
    });
}

#[pyfunction]
fn emit_banner(config: &Bound<'_, PyAny>) -> PyResult<()> {
    let config = PyConfig(config).server_config()?;
    console::emit_banner(&config);
    Ok(())
}

#[pyfunction]
fn serve_fd<'py>(
    py: Python<'py>,
    app: Py<PyAny>,
    fd: i64,
    config: &Bound<'py, PyAny>,
    shutdown_trigger: Py<PyAny>,
    retire_trigger: Option<Py<PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    init_tokio_runtime(py);
    let config: &'static ServerConfig = Box::leak(Box::new(PyConfig(config).server_config()?));

    let locals = get_current_locals(py)?;
    let limits = runtime::RuntimeLimits::new(config, retire_trigger).map(Arc::new);
    let app = Arc::new(SharedApp {
        app,
        locals: locals.clone(),
        limits,
    });
    future_into_py_with_locals(py, locals, async move {
        server::serve_from_fd(app, fd, config, shutdown_trigger)
            .await
            .into_pyresult()?;
        Python::attach(|py| Ok(py.None()))
    })
}

#[pymodule]
#[pyo3(name = "_lib")]
fn h2corn_lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(emit_banner, m)?)?;
    m.add_function(wrap_pyfunction!(serve_fd, m)?)?;
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

            config.setattr("uds", py.None()).unwrap();
            config.setattr("fd", py.None()).unwrap();
            config.setattr("_fd_is_unix", py.None()).unwrap();
            config.setattr("host", "127.0.0.9").unwrap();
            config.setattr("port", 48_123).unwrap();
            config.setattr("max_requests", 41).unwrap();
            config.setattr("max_requests_jitter", 7).unwrap();
            config.setattr("timeout_worker_healthcheck", 0.0).unwrap();
            config.setattr("access_log", false).unwrap();
            config.setattr("root_path", "/api").unwrap();
            config.setattr("http1", false).unwrap();
            config.setattr("limit_request_line", 4_097).unwrap();
            config.setattr("limit_request_fields", 17).unwrap();
            config.setattr("limit_request_field_size", 211).unwrap();
            config.setattr("max_concurrent_streams", 321).unwrap();
            config.setattr("h2_max_header_list_size", 65_432).unwrap();
            config.setattr("max_request_body_size", 987_654).unwrap();
            config.setattr("timeout_handshake", 1.25).unwrap();
            config.setattr("timeout_graceful_shutdown", 12.5).unwrap();
            config.setattr("timeout_keep_alive", 2.5).unwrap();
            config.setattr("timeout_read", 4.5).unwrap();
            config.setattr("timeout_lifespan_startup", 6.5).unwrap();
            config.setattr("timeout_lifespan_shutdown", 8.5).unwrap();
            config.setattr("limit_concurrency", 23).unwrap();
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
            config.setattr("server_header", true).unwrap();
            config.setattr("date_header", false).unwrap();
            config
                .setattr("response_headers", ("x-demo: 1", "x-extra: 2"))
                .unwrap();

            let extracted = PyConfig(&config)
                .server_config()
                .expect("config extraction succeeds");

            match extracted.bind {
                ServerBind::Tcp { host, port } => {
                    assert_eq!(host.as_ref(), "127.0.0.9");
                    assert_eq!(port, 48_123);
                }
                ServerBind::Unix { .. } | ServerBind::Fd { .. } => {
                    panic!("expected TCP bind")
                }
            }
            assert!(!extracted.access_log);
            assert_eq!(extracted.root_path.as_ref(), "/api");

            assert!(!extracted.http1.enabled);
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
            assert_eq!(extracted.timeout_read, Some(Duration::from_secs_f64(4.5)));
            assert_eq!(extracted.limit_concurrency, Some(23));
            assert_eq!(extracted.max_requests, Some(41));

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
}
