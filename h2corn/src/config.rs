use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::time::Duration;

use bytes::Bytes;
use tokio_rustls::TlsAcceptor;

use crate::h2_frame::DEFAULT_MAX_FRAME_SIZE;
use crate::http::types::ResponseHeaderKind;
use crate::proxy_protocol::{ProxyProtocolMode, TrustedPeer};

pub(crate) const PATHSEND_PRELOAD_MAX: usize = 128 * 1024;
pub(crate) const PATHSEND_READ_BUFFER_SIZE: usize = 64 * 1024;

#[derive(Clone, Debug, Default)]
pub(crate) struct Http1Config {
    pub enabled: bool,
    pub limit_request_head_size: Option<NonZeroUsize>,
    pub limit_request_line: Option<NonZeroUsize>,
    pub limit_request_field_size: Option<NonZeroUsize>,
}

#[derive(Clone, Debug)]
pub(crate) struct Http2Config {
    pub max_concurrent_streams: u32,
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
            max_concurrent_streams: 0,
            max_header_list_size: None,
            max_header_block_size: None,
            max_inbound_frame_size: NonZeroU32::new(DEFAULT_MAX_FRAME_SIZE as u32)
                .expect("default HTTP/2 frame size is non-zero"),
            initial_stream_window_size: NonZeroU32::new(1 << 20)
                .expect("default stream window is non-zero"),
            initial_connection_window_size: NonZeroU32::new(2 << 20)
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
}

#[derive(Clone, Debug, Default)]
pub(crate) struct WebSocketConfig {
    pub max_message_size: Option<NonZeroUsize>,
    pub per_message_deflate: bool,
    pub ping_interval: Option<Duration>,
    pub ping_timeout: Option<Duration>,
}

#[derive(Clone, Debug)]
pub(crate) enum BindTarget {
    Tcp { host: Box<str>, port: u16 },
    Unix { path: Box<str> },
    Fd { fd: i64, is_unix: bool },
}

#[derive(Debug, Default)]
pub(crate) struct ResponseHeaderConfig {
    pub server_header: bool,
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
    pub timeout_handshake: Duration,
    pub response_headers: ResponseHeaderConfig,
}
