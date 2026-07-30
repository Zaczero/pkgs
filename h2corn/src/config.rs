use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use pyo3::Py;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyString;
use tokio_rustls::TlsAcceptor;

use crate::h2_frame::DEFAULT_MAX_FRAME_SIZE;
use crate::http::types::ResponseHeaderKind;
use crate::proxy_protocol::{ProxyProtocolMode, TrustedPeer};

pub(crate) const PATHSEND_PRELOAD_MAX: usize = 128 * 1024;
pub(crate) const PATHSEND_READ_BUFFER_SIZE: usize = 128 * 1024;

/// How this server names itself when asked to.
pub(crate) const SERVER_NAME: &str = "h2corn";
/// The same, with the version — Apache spells this level `Full`.
pub(crate) const SERVER_NAME_AND_VERSION: &str = concat!("h2corn/", env!("CARGO_PKG_VERSION"));

#[derive(Clone, Debug, Default)]
pub(crate) struct Http1Config {
    pub enabled: bool,
    pub limit_request_head_size: Option<NonZeroUsize>,
    pub limit_request_line: Option<NonZeroUsize>,
    pub limit_request_field_size: Option<NonZeroUsize>,
}

#[derive(Clone, Debug)]
pub(crate) struct Http2Config {
    /// Never zero: a connection that admits no streams serves nothing, so
    /// the configuration rejects it rather than advertising it.
    pub max_concurrent_streams: NonZeroU32,
    pub max_header_list_size: Option<NonZeroUsize>,
    pub max_header_block_size: Option<NonZeroUsize>,
    pub max_inbound_frame_size: NonZeroU32,
    /// Receive-side flow-control window advertised per stream. Bounds the
    /// worst-case buffered request bytes per stream.
    pub initial_stream_window_size: NonZeroU32,
    /// Receive-side connection flow-control window.
    pub initial_connection_window_size: NonZeroU32,
    pub timeout_response_stall: Option<Duration>,
}

impl Default for Http2Config {
    fn default() -> Self {
        Self {
            max_concurrent_streams: NonZeroU32::new(256).expect("non-zero"),
            max_header_list_size: None,
            max_header_block_size: None,
            max_inbound_frame_size: NonZeroU32::new(DEFAULT_MAX_FRAME_SIZE as u32)
                .expect("default HTTP/2 frame size is non-zero"),
            initial_stream_window_size: NonZeroU32::new(8 << 20)
                .expect("default stream window is non-zero"),
            initial_connection_window_size: NonZeroU32::new(8 << 20)
                .expect("default connection window is non-zero"),
            timeout_response_stall: None,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ProxyConfig {
    pub trust_headers: bool,
    pub trusted_peers: Box<[TrustedPeer]>,
    pub protocol: ProxyProtocolMode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClientCertMode {
    None,
    Optional,
    Required,
}

#[derive(Clone)]
pub(crate) struct TlsConfig {
    pub acceptor: TlsAcceptor,
    /// The leaf certificate this server presents, PEM-encoded once at startup
    /// for `scope["extensions"]["tls"]["server_cert"]`.
    pub server_certificate: Arc<str>,
}

/// Server-initiated WebSocket keepalive. Absent entirely when pings are off
/// (`websocket_ping_interval == 0`); the configured timeout is irrelevant in
/// that state and is not retained.
#[derive(Clone, Copy, Debug)]
pub(crate) struct WebSocketKeepAlive {
    pub interval: Duration,
    pub timeout: Option<Duration>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct WebSocketConfig {
    pub max_message_size: Option<NonZeroUsize>,
    pub per_message_deflate: bool,
    pub keep_alive: Option<WebSocketKeepAlive>,
}

#[derive(Clone, Debug)]
pub(crate) enum BindTarget {
    Tcp { host: Box<str>, port: u16 },
    Unix { path: Box<str> },
    Fd { fd: i64 },
}

#[derive(Debug, Default)]
pub(crate) struct ResponseHeaderConfig {
    /// The `Server` value to send, already resolved from the configured mode:
    /// `None` for `off`, the name for `on`, name and version for `full`.
    pub server_header: Option<Bytes>,
    pub date_header: bool,
    pub extra_headers: Box<[ConfiguredResponseHeader]>,
}

#[derive(Debug)]
pub(crate) struct ConfiguredResponseHeader {
    pub name: Bytes,
    pub value: Bytes,
    pub kind: ResponseHeaderKind,
}

impl ConfiguredResponseHeader {
    pub(crate) fn new(name: Bytes, value: Bytes) -> Self {
        let kind = ResponseHeaderKind::from_bytes(name.as_ref());
        Self { name, value, kind }
    }
}

pub(crate) struct ServerConfig {
    pub binds: Box<[BindTarget]>,
    pub access_log: bool,
    pub root_path: Box<str>,
    /// `root_path` as the scope sees it, built once per configuration.
    ///
    /// Every request under a mounted application carries the same string, and
    /// building it each time measured 521 instructions of the request's work.
    /// A forwarded prefix produces a different string and is not cached — see
    /// `build_base_scope`, which uses this only for the configured value
    /// itself.
    pub root_path_scope: PyOnceLock<Py<PyString>>,
    pub limit_request_fields: Option<NonZeroUsize>,
    pub http1: Http1Config,
    pub http2: Http2Config,
    pub max_request_body_size: Option<NonZeroU64>,
    pub timeout_graceful_shutdown: Duration,
    pub timeout_keep_alive: Option<Duration>,
    pub timeout_request_header: Option<Duration>,
    pub timeout_request_body_idle: Option<Duration>,
    pub limit_concurrency: Option<NonZeroUsize>,
    pub limit_connections: Option<NonZeroUsize>,
    pub max_requests: Option<NonZeroU64>,
    pub runtime_threads: usize,
    pub loop_threads: usize,
    pub websocket: WebSocketConfig,
    pub proxy: ProxyConfig,
    pub tls: Option<TlsConfig>,
    pub timeout_handshake: Option<Duration>,
    pub response_headers: ResponseHeaderConfig,
}
