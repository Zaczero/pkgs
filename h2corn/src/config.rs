use crate::proxy::{ProxyProtocolMode, TrustedPeer};
use bytes::Bytes;
use parking_lot::RwLock;
use std::{
    num::{NonZeroU64, NonZeroUsize},
    time::Duration,
};

pub const INITIAL_CONNECTION_WINDOW_SIZE: u32 = 16 << 20;
pub const INITIAL_STREAM_WINDOW_SIZE: u32 = 16 << 20;
pub const PATHSEND_BUFFER_SIZE: usize = 128 * 1024;

#[derive(Clone, Debug, Default)]
pub struct Http1Config {
    pub enabled: bool,
    pub limit_request_line: Option<NonZeroUsize>,
    pub limit_request_fields: Option<NonZeroUsize>,
    pub limit_request_field_size: Option<NonZeroUsize>,
}

#[derive(Clone, Debug, Default)]
pub struct Http2Config {
    pub max_concurrent_streams: usize,
    pub max_header_list_size: Option<NonZeroUsize>,
}

#[derive(Clone, Debug)]
pub struct ProxyConfig {
    pub trust_headers: bool,
    pub trusted_peers: Box<[TrustedPeer]>,
    pub protocol: ProxyProtocolMode,
}

#[derive(Clone, Debug, Default)]
pub struct WebSocketConfig {
    pub message_size_limit: Option<NonZeroUsize>,
    pub per_message_deflate: bool,
    pub ping_interval: Option<Duration>,
    pub ping_timeout: Option<Duration>,
}

#[derive(Clone, Debug)]
pub enum ServerBind {
    Tcp { host: Box<str>, port: u16 },
    Unix { path: Box<str> },
    Fd { fd: i64, is_unix: bool },
}

#[derive(Debug, Default)]
pub struct CachedDateValue {
    pub unix_seconds: u64,
    pub value: Bytes,
}

#[derive(Debug, Default)]
pub struct ResponseHeaderConfig {
    pub server_header: bool,
    pub date_header: bool,
    pub extra_headers: Box<[(Bytes, Bytes)]>,
    pub cached_date: RwLock<CachedDateValue>,
}

pub struct ServerConfig {
    pub bind: ServerBind,
    pub access_log: bool,
    pub root_path: Box<str>,
    pub http1: Http1Config,
    pub http2: Http2Config,
    pub max_request_body_size: Option<NonZeroU64>,
    pub timeout_graceful_shutdown: Duration,
    pub timeout_keep_alive: Option<Duration>,
    pub timeout_read: Option<Duration>,
    pub limit_concurrency: Option<usize>,
    pub max_requests: Option<u64>,
    pub websocket: WebSocketConfig,
    pub proxy: ProxyConfig,
    pub timeout_handshake: Duration,
    pub response_headers: ResponseHeaderConfig,
}
