use std::path::{Path, PathBuf};
use std::{fmt, io};

use pyo3::exceptions::{PyOSError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::{PyErr, PyResult};
use thiserror::Error;
use tokio::task::JoinError;

/// Crate-wide error: a single pointer wide so every `Result<T, H2CornError>`
/// on the request path (and every future holding one) stays small; the
/// payload is boxed because errors are cold.
#[derive(Debug, Error)]
#[error(transparent)]
pub(crate) struct H2CornError(Box<ErrorKind>);

#[derive(Debug, Error)]
pub(crate) enum ErrorKind {
    #[error(transparent)]
    Python(#[from] PyErr),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Join(#[from] JoinError),
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Asgi(#[from] AsgiError),
    #[error(transparent)]
    Http1(#[from] Http1Error),
    #[error(transparent)]
    HttpResponse(#[from] HttpResponseError),
    #[error(transparent)]
    H2(#[from] H2Error),
    #[error(transparent)]
    Pathsend(#[from] PathsendError),
    #[error(transparent)]
    Proxy(#[from] ProxyError),
    #[error(transparent)]
    WebSocket(#[from] WebSocketError),
}

impl<E> From<E> for H2CornError
where
    E: Into<ErrorKind>,
{
    fn from(err: E) -> Self {
        Self(Box::new(err.into()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FailureDomain {
    Configuration,
    PeerProtocol,
    TransportIo,
    AppContract,
    InternalInvariant,
}

impl H2CornError {
    pub(crate) const fn kind(&self) -> &ErrorKind {
        &self.0
    }

    pub(crate) fn into_kind(self) -> ErrorKind {
        *self.0
    }

    pub(crate) const fn failure_domain(&self) -> FailureDomain {
        match self.kind() {
            ErrorKind::Io(_) => FailureDomain::TransportIo,
            ErrorKind::Join(_) => FailureDomain::InternalInvariant,
            ErrorKind::Config(_) => FailureDomain::Configuration,
            ErrorKind::Python(_) | ErrorKind::Asgi(_) | ErrorKind::HttpResponse(_) => {
                FailureDomain::AppContract
            },
            ErrorKind::Http1(_) | ErrorKind::H2(_) | ErrorKind::Proxy(_) => {
                FailureDomain::PeerProtocol
            },
            ErrorKind::Pathsend(err) => err.failure_domain(),
            ErrorKind::WebSocket(err) => err.failure_domain(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AsgiContainer {
    Message,
    HttpResponseStart,
    HttpResponseBody,
    HttpResponsePathsend,
    HttpResponseTrailers,
    WebSocketAccept,
    WebSocketSend,
    WebSocketClose,
    WebSocketHttpResponseStart,
    WebSocketHttpResponseBody,
}

impl fmt::Display for AsgiContainer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Message => "ASGI message",
            Self::HttpResponseStart => "http.response.start",
            Self::HttpResponseBody => "http.response.body",
            Self::HttpResponsePathsend => "http.response.pathsend",
            Self::HttpResponseTrailers => "http.response.trailers",
            Self::WebSocketAccept => "websocket.accept",
            Self::WebSocketSend => "websocket.send",
            Self::WebSocketClose => "websocket.close",
            Self::WebSocketHttpResponseStart => "websocket.http.response.start",
            Self::WebSocketHttpResponseBody => "websocket.http.response.body",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AsgiChannel {
    Http,
    WebSocket,
}

impl fmt::Display for AsgiChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Http => "HTTP",
            Self::WebSocket => "websocket",
        })
    }
}

#[derive(Debug, Error)]
pub(crate) enum ConfigError {
    #[error("invalid trusted proxy entry: {value:?}")]
    InvalidTrustedProxyEntry { value: Box<str> },
    #[error("invalid trusted proxy CIDR prefix: {value:?}")]
    InvalidTrustedProxyCidrPrefix { value: Box<str> },
    #[error("{name} must be a non-negative number of seconds that fits a duration")]
    InvalidDuration { name: &'static str },
    #[error("invalid proxy_protocol mode: {value:?}")]
    InvalidProxyProtocolMode { value: Box<str> },
    #[error("invalid server_header mode: {value:?}")]
    InvalidServerHeaderMode { value: Box<str> },
    #[error("invalid cert_reqs mode: {value:?}")]
    InvalidClientCertMode { value: Box<str> },
    #[error("invalid response header {value:?}: expected 'name: value'")]
    InvalidResponseHeaderFormat { value: Box<str> },
    #[error("invalid response header name: {value:?}")]
    InvalidResponseHeaderName { value: Box<str> },
    #[error("invalid response header value for {name:?}")]
    InvalidResponseHeaderValue { name: Box<str> },
    #[error("invalid {kind} bind target {value:?}: {detail}")]
    InvalidBindTarget {
        kind: &'static str,
        value: Box<str>,
        detail: &'static str,
    },
    #[error(
        "runtime_threads is process-global and was already initialized with {initialized_threads}; cannot change it to {worker_threads}"
    )]
    RuntimeThreadsAlreadyInitialized {
        initialized_threads: usize,
        worker_threads: usize,
    },
}

impl ConfigError {
    pub(crate) fn invalid_trusted_proxy_entry(value: &str) -> Self {
        Self::InvalidTrustedProxyEntry {
            value: value.into(),
        }
    }

    pub(crate) fn invalid_trusted_proxy_cidr_prefix(value: &str) -> Self {
        Self::InvalidTrustedProxyCidrPrefix {
            value: value.into(),
        }
    }

    pub(crate) const fn invalid_duration(name: &'static str) -> Self {
        Self::InvalidDuration { name }
    }

    pub(crate) fn invalid_proxy_protocol_mode(value: &str) -> Self {
        Self::InvalidProxyProtocolMode {
            value: value.into(),
        }
    }

    pub(crate) fn invalid_server_header_mode(value: &str) -> Self {
        Self::InvalidServerHeaderMode {
            value: value.into(),
        }
    }

    pub(crate) fn invalid_client_cert_mode(value: &str) -> Self {
        Self::InvalidClientCertMode {
            value: value.into(),
        }
    }

    pub(crate) fn invalid_response_header_format(value: &str) -> Self {
        Self::InvalidResponseHeaderFormat {
            value: value.into(),
        }
    }

    pub(crate) fn invalid_response_header_name(value: &str) -> Self {
        Self::InvalidResponseHeaderName {
            value: value.into(),
        }
    }

    pub(crate) fn invalid_response_header_value(name: &str) -> Self {
        Self::InvalidResponseHeaderValue { name: name.into() }
    }

    pub(crate) fn invalid_bind_target(
        kind: &'static str,
        value: &str,
        detail: &'static str,
    ) -> Self {
        Self::InvalidBindTarget {
            kind,
            value: value.into(),
            detail,
        }
    }

    pub(crate) const fn runtime_threads_already_initialized(
        initialized_threads: usize,
        worker_threads: usize,
    ) -> Self {
        Self::RuntimeThreadsAlreadyInitialized {
            initialized_threads,
            worker_threads,
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum AsgiError {
    #[error("ASGI send called after the stream closed")]
    SendAfterClose,
    #[error("{container} is missing required field: {field}")]
    MissingField {
        container: AsgiContainer,
        field: &'static str,
    },
    #[error("{container} {field} must be {expected}, got {actual}")]
    InvalidFieldType {
        container: AsgiContainer,
        field: &'static str,
        expected: &'static str,
        actual: Box<str>,
    },
    #[error("unsupported {channel} ASGI outbound message: {message_type}")]
    UnsupportedOutboundMessage {
        channel: AsgiChannel,
        message_type: Box<str>,
    },
    #[error("websocket.send must set exactly one of text or bytes")]
    WebSocketSendRequiresExactlyOnePayload,
}

impl AsgiError {
    pub(crate) const fn missing_field(container: AsgiContainer, field: &'static str) -> Self {
        Self::MissingField { container, field }
    }

    pub(crate) const fn invalid_field_type(
        container: AsgiContainer,
        field: &'static str,
        expected: &'static str,
        actual: Box<str>,
    ) -> Self {
        Self::InvalidFieldType {
            container,
            field,
            expected,
            actual,
        }
    }

    pub(crate) fn unsupported_outbound_message(channel: AsgiChannel, message_type: &str) -> Self {
        Self::UnsupportedOutboundMessage {
            channel,
            message_type: message_type.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Error)]
pub(crate) enum Http1Error {
    #[error("HTTP/1.1 request head did not arrive within timeout_request_header")]
    RequestHeadTimedOut,
    #[error("keep-alive connection idled out before the next request")]
    KeepAliveTimedOut,
    #[error("HTTP/1.1 request body timed out")]
    RequestBodyTimedOut,
    #[error("connection closed while reading the HTTP/1.1 request head")]
    RequestHeadClosed,
    #[error("empty HTTP/1.1 request head")]
    EmptyRequestHead,
    #[error("malformed HTTP/1.1 header line")]
    MalformedHeaderLine,
    #[error("invalid HTTP/1.1 header name")]
    InvalidHeaderName,
    #[error("invalid HTTP/1.1 header value")]
    InvalidHeaderValue,
    #[error("absolute-form request target authority conflicted with Host")]
    ConflictingAbsoluteFormAuthority,
    #[error("invalid Content-Length header")]
    InvalidContentLength,
    #[error("HTTP/1.1 request body was too large")]
    RequestBodyTooLarge,
    #[error("HTTP/1.1 request body exceeded the configured limit")]
    RequestBodyLimitExceeded,
    #[error("connection closed while reading the HTTP/1.1 request body")]
    RequestBodyClosed,
    #[error("connection closed while reading the chunked request body")]
    ChunkedBodyClosed,
    #[error("connection closed while reading chunked request trailers")]
    ChunkedTrailersClosed,
    #[error("connection closed while reading a chunked request chunk")]
    ChunkClosed,
    #[error("chunked request chunk was missing CRLF")]
    ChunkMissingCrlf,
    #[error("trailer field exceeds the configured maximum size")]
    TrailerFieldTooLarge,
    #[error("more trailer fields than the configured maximum")]
    TooManyTrailerFields,
    #[error("invalid HTTP/1.1 request line")]
    InvalidRequestLine,
    #[error("invalid HTTP/1.1 request method")]
    InvalidRequestMethod,
    #[error("request target was not valid UTF-8")]
    RequestTargetNotUtf8,
    #[error("invalid absolute-form HTTP/1.1 request target")]
    InvalidAbsoluteFormTarget,
    #[error("HTTP/1.1 request target form is not allowed for this method")]
    InvalidRequestTargetForm,
    #[error("invalid absolute-form authority")]
    InvalidAbsoluteFormAuthority,
    #[error("invalid chunked request chunk size")]
    InvalidChunkSize,
    #[error("invalid HTTP2-Settings payload length")]
    InvalidHttp2SettingsPayloadLength,
    #[error("invalid HTTP2-Settings base64url payload")]
    InvalidHttp2SettingsBase64UrlPayload,
}

#[derive(Debug, Error)]
pub(crate) enum HttpResponseError {
    #[error("http.response.start received more than once")]
    StartAlreadyReceived,
    #[error(
        "http.response.start declared trailers for a request that did not advertise TE: trailers"
    )]
    TrailersNotAdvertised,
    #[error("http.response.body received before response start")]
    BodyBeforeStart,
    #[error("http.response.pathsend received before response start")]
    PathsendBeforeStart,
    #[error("http.response.pathsend must not be mixed with http.response.body")]
    PathsendMixedWithBody,
    #[error(
        "http.response.trailers received before the response body completed with trailers enabled"
    )]
    TrailersBeforeBodyCompleted,
    #[error("ASGI app returned before starting the response")]
    AppReturnedWithoutStartingResponse,
    #[error("ASGI app returned before completing the response")]
    AppReturnedWithoutCompletingResponse,
    #[error("response header names must be non-empty lowercase ASCII tokens")]
    InvalidResponseHeaderName,
    #[error("response header values contain invalid bytes")]
    InvalidResponseHeaderValue,
    #[error("response trailers contain a field that is forbidden after the body")]
    InvalidResponseTrailerField,
    #[error("{container} status must be a three-digit code, got {status}")]
    StatusMustBeThreeDigitCode {
        container: AsgiContainer,
        status: Box<str>,
    },
    #[error(
        "{container} status must be a final response code; ASGI has no way to send an informational {status}"
    )]
    InformationalStatusUnsupported {
        container: AsgiContainer,
        status: u16,
    },
}

#[derive(Debug, Error)]
pub(crate) enum H2Error {
    #[error("plaintext connection preamble did not arrive within timeout_handshake")]
    PlaintextHandshakeTimedOut,
    #[error("TLS handshake did not complete within timeout_handshake")]
    TlsHandshakeTimedOut,
    #[error("HTTP/2 handshake did not complete within timeout_handshake")]
    Http2HandshakeTimedOut,
    #[error("invalid SETTINGS_ENABLE_PUSH value")]
    SettingsEnablePushInvalid,
    #[error("invalid SETTINGS_MAX_FRAME_SIZE value")]
    SettingsMaxFrameSizeInvalid,
    #[error("invalid SETTINGS_ENABLE_CONNECT_PROTOCOL value")]
    SettingsEnableConnectProtocolInvalid,
    #[error("SETTINGS_INITIAL_WINDOW_SIZE exceeds the protocol limit")]
    SettingsInitialWindowSizeExceededLimit,
    #[error("SETTINGS_INITIAL_WINDOW_SIZE would overflow an active stream send window")]
    SettingsInitialWindowAdjustmentOverflow,
    #[error("SETTINGS_MAX_FRAME_SIZE is outside the valid range")]
    SettingsMaxFrameSizeOutOfRange,
    #[error("connection closed while reading an HTTP/2 frame header")]
    FrameHeaderClosed,
    #[error("frame length {payload_len} exceeds peer max frame size {max_frame_size}")]
    FrameLengthExceedsPeerMax {
        payload_len: usize,
        max_frame_size: usize,
    },
    #[error("connection closed while reading an HTTP/2 frame payload")]
    FramePayloadClosed,
    #[error("SETTINGS frame must use stream 0")]
    SettingsMustUseStreamZero,
    #[error("SETTINGS ack frame must have an empty payload")]
    SettingsAckPayloadNotEmpty,
    #[error("SETTINGS payload length must be a multiple of 6")]
    SettingsPayloadLengthInvalid,
    #[error("HEADERS frame with PADDED flag had no pad length")]
    HeadersPaddedMissingPadLength,
    #[error("HEADERS frame with PRIORITY flag was too short")]
    HeadersPriorityTooShort,
    #[error("HEADERS frame padding exceeded payload length")]
    HeadersPaddingExceedsPayload,
    #[error("invalid request stream id")]
    InvalidRequestStreamId,
    #[error("client stream ids must be strictly increasing")]
    ClientStreamIdsNotIncreasing,
    #[error("received HEADERS on a closed stream")]
    HeadersOnClosedStream,
    #[error("unexpected CONTINUATION frame")]
    UnexpectedContinuationFrame,
    #[error("CONTINUATION stream id did not match the open header block")]
    ContinuationStreamIdMismatch,
    #[error("DATA frames must not use stream 0")]
    DataMustNotUseStreamZero,
    #[error("received DATA on an idle stream")]
    DataOnIdleStream,
    #[error("receive flow-control window underflow")]
    ReceiveFlowControlWindowUnderflow,
    #[error("send flow-control window overflow")]
    SendFlowControlWindowOverflow,
    #[error("received a frame larger than the advertised max frame size")]
    FrameExceedsAdvertisedMaxSize,
    #[error("received a non-CONTINUATION frame while a header block was open")]
    HeaderBlockInterrupted,
    #[error("field block exceeds the configured maximum size")]
    HeaderBlockTooLarge,
    #[error("first client frame after the preface must be SETTINGS")]
    FirstClientFrameMustBeSettings,
    #[error("first client SETTINGS frame must not be an ACK")]
    FirstClientSettingsMustNotAck,
    #[error("invalid peer SETTINGS frame: {detail}")]
    InvalidPeerSettings { detail: Box<str> },
    #[error("PING frame must use stream 0")]
    PingMustUseStreamZero,
    #[error("PING payload must be 8 bytes")]
    PingPayloadInvalidLength,
    #[error("WINDOW_UPDATE payload must be 4 bytes")]
    WindowUpdatePayloadInvalidLength,
    #[error("WINDOW_UPDATE increment must be greater than zero")]
    WindowUpdateIncrementZero,
    #[error("received WINDOW_UPDATE on an idle stream")]
    WindowUpdateOnIdleStream,
    #[error("RST_STREAM frame must not use stream 0")]
    RstStreamMustNotUseStreamZero,
    #[error("RST_STREAM payload must be 4 bytes")]
    RstStreamPayloadInvalidLength,
    #[error("received RST_STREAM on an idle stream")]
    RstStreamOnIdleStream,
    #[error("peer exceeded the stream reset rate limit (rapid reset)")]
    PeerResetFlood,
    #[error("invalid GOAWAY frame")]
    InvalidGoawayFrame,
    #[error("PRIORITY frame must not use stream 0")]
    PriorityMustNotUseStreamZero,
    #[error("PRIORITY payload must be 5 bytes")]
    PriorityPayloadInvalidLength,
    #[error("client sent an unexpected PUSH_PROMISE frame")]
    UnexpectedPushPromise,
    #[error("padded DATA frame was missing padding")]
    DataPaddedMissingPadding,
    #[error("DATA padding exceeded frame payload")]
    DataPaddingExceedsPayload,
    #[error("cannot send response headers more than once per stream")]
    ResponseHeadersAlreadySent,
    #[error("cannot send response trailers on a closed or unopened stream")]
    ResponseTrailersOnClosedOrUnopenedStream,
    #[error("cannot send response trailers more than once per stream")]
    ResponseTrailersAlreadySent,
    #[error("cannot send DATA before response headers")]
    DataBeforeResponseHeaders,
    #[error("cannot send DATA on a closed stream")]
    DataOnClosedStream,
    #[error("cannot send path data before response headers")]
    PathDataBeforeResponseHeaders,
    #[error("cannot send path data on a closed stream")]
    PathDataOnClosedStream,
    #[error("connection writer was closed")]
    ConnectionWriterClosed,
    #[error("stream channel was closed")]
    StreamChannelClosed,
    #[error("incomplete HPACK header block")]
    IncompleteHpackHeaderBlock,
    #[error("HPACK decode error: {detail}")]
    HpackDecode { detail: Box<str> },
    #[error("invalid HTTP/2 request field")]
    InvalidRequestField,
}

impl H2Error {
    pub(crate) const fn frame_length_exceeds_peer_max(
        payload_len: usize,
        max_frame_size: usize,
    ) -> Self {
        Self::FrameLengthExceedsPeerMax {
            payload_len,
            max_frame_size,
        }
    }

    pub(crate) fn invalid_peer_settings(detail: impl fmt::Display) -> Self {
        Self::InvalidPeerSettings {
            detail: detail.to_string().into_boxed_str(),
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum PathsendError {
    #[error("http.response.pathsend failed for file {path:?}: {source}")]
    OpenFailed {
        path: Box<str>,
        #[source]
        source: io::Error,
    },
    #[error("http.response.pathsend rejected non-regular file {path:?}")]
    NotRegularFile { path: PathBuf },
}

impl PathsendError {
    pub(crate) fn open_failed(path: &Path, source: io::Error) -> Self {
        Self::OpenFailed {
            path: path.display().to_string().into_boxed_str(),
            source,
        }
    }

    const fn failure_domain(&self) -> FailureDomain {
        // The application chose the filesystem path. Even an I/O error on
        // that path is actionable application state, not peer transport noise.
        FailureDomain::AppContract
    }
}

#[derive(Debug, Error)]
pub(crate) enum ProxyError {
    #[error("PROXY protocol requires the connection peer to be trusted")]
    ProtocolRequiresTrustedPeer,
    #[error("connection closed before the PROXY or HTTP/2 preface arrived")]
    ClosedBeforeProxyOrHttp2Preface,
    #[error("invalid PROXY v2 header")]
    InvalidProxyV2Header,
    #[error("connection closed while reading the PROXY v2 header")]
    ClosedWhileReadingProxyV2Header,
    #[error("PROXY v1 header exceeded 107 bytes")]
    ProxyV1HeaderTooLong,
    #[error("connection closed while reading the PROXY v1 header")]
    ClosedWhileReadingProxyV1Header,
    #[error("expected a PROXY v1 header before the HTTP/2 preface")]
    ExpectedProxyV1HeaderBeforeHttp2Preface,
    #[error("expected a PROXY v2 header before the HTTP/2 preface")]
    ExpectedProxyV2HeaderBeforeHttp2Preface,
    #[error("connection closed before the HTTP/2 prior-knowledge preface arrived")]
    ClosedBeforeHttp2Preface,
    #[error("client did not start with the HTTP/2 prior-knowledge preface")]
    InvalidHttp2Preface,
    #[error("connection closed before any request bytes arrived")]
    ClosedBeforeAnyRequestBytes,
    #[error("connection closed before protocol detection completed")]
    ClosedBeforeProtocolDetection,
    #[error("invalid PROXY v1 header")]
    InvalidProxyV1Header,
    #[error("PROXY v1 header must end with CRLF")]
    ProxyV1HeaderMissingCrlf,
    #[error("unsupported PROXY v1 transport; expected TCP4 or TCP6")]
    UnsupportedProxyV1Transport,
    #[error("invalid PROXY v1 source address")]
    InvalidProxyV1SourceAddress,
    #[error("invalid PROXY v1 destination address")]
    InvalidProxyV1DestinationAddress,
    #[error("PROXY v1 address family did not match the declared transport")]
    ProxyV1AddressFamilyMismatch,
    #[error("invalid PROXY port")]
    InvalidProxyPort,
    #[error("unsupported PROXY v2 version")]
    UnsupportedProxyV2Version,
    #[error("truncated PROXY v2 header")]
    TruncatedProxyV2Header,
    #[error("unsupported PROXY v2 command")]
    UnsupportedProxyV2Command,
    #[error("unsupported PROXY v2 transport; expected STREAM")]
    UnsupportedProxyV2Transport,
    #[error("invalid PROXY v2 IPv4 payload")]
    InvalidProxyV2Ipv4Payload,
    #[error("invalid PROXY v2 IPv6 payload")]
    InvalidProxyV2Ipv6Payload,
    #[error("unsupported PROXY v2 address family")]
    UnsupportedProxyV2AddressFamily,
}

#[derive(Debug, Error)]
pub(crate) enum WebSocketError {
    #[error(transparent)]
    Protocol(#[from] WebSocketProtocolError),
    #[error("websocket handshake timed out")]
    HandshakeTimedOut,
    #[error("websocket permessage-deflate compression failed")]
    CompressionFailed,
    #[error("websocket close reason must not exceed 123 bytes")]
    CloseReasonTooLong,
    #[error("websocket close code is invalid")]
    CloseCodeInvalid,
    #[error(
        "websocket.accept headers must not include pseudo headers, sec-websocket-protocol, or sec-websocket-extensions"
    )]
    AcceptHeadersForbidden,
    #[error("websocket.accept subprotocol must not be empty")]
    AcceptSubprotocolEmpty,
    #[error("websocket.accept subprotocol must be requested by the client")]
    AcceptSubprotocolNotRequested,
    #[error(
        "application stopped receiving before the connection closed; a {frame_kind} frame was dropped"
    )]
    ReceiveChannelClosed { frame_kind: WebSocketFrameKind },
    #[error("websocket app returned before handshake")]
    AppEndedBeforeHandshake,
    #[error(
        "unexpected {message_type} {context}; the app must send {} first",
        context.expected_message_types()
    )]
    UnexpectedEvent {
        context: WebSocketEventContext,
        message_type: Box<str>,
    },
}

impl WebSocketError {
    pub(crate) const fn receive_channel_closed(frame_kind: WebSocketFrameKind) -> Self {
        Self::ReceiveChannelClosed { frame_kind }
    }

    pub(crate) fn unexpected_event(context: WebSocketEventContext, message_type: &str) -> Self {
        Self::UnexpectedEvent {
            context,
            message_type: message_type.into(),
        }
    }

    pub(crate) fn unexpected_initial_event(message_type: &str) -> Self {
        Self::unexpected_event(WebSocketEventContext::BeforeHandshake, message_type)
    }

    pub(crate) fn unexpected_outbound_event_after_accept(message_type: &str) -> Self {
        Self::unexpected_event(WebSocketEventContext::AfterAccept, message_type)
    }

    pub(crate) fn unexpected_denial_body_event(message_type: &str) -> Self {
        Self::unexpected_event(WebSocketEventContext::DuringDenialResponse, message_type)
    }

    const fn failure_domain(&self) -> FailureDomain {
        match self {
            Self::Protocol(_) => FailureDomain::PeerProtocol,
            Self::HandshakeTimedOut
            | Self::CloseReasonTooLong
            | Self::CloseCodeInvalid
            | Self::AcceptHeadersForbidden
            | Self::AcceptSubprotocolEmpty
            | Self::AcceptSubprotocolNotRequested
            | Self::AppEndedBeforeHandshake
            | Self::UnexpectedEvent { .. }
            | Self::ReceiveChannelClosed { .. } => FailureDomain::AppContract,
            Self::CompressionFailed => FailureDomain::InternalInvariant,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WebSocketFrameKind {
    Text,
    Binary,
}

impl fmt::Display for WebSocketFrameKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Text => "text",
            Self::Binary => "binary",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WebSocketEventContext {
    BeforeHandshake,
    AfterAccept,
    DuringDenialResponse,
}

impl fmt::Display for WebSocketEventContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::BeforeHandshake => "before handshake",
            Self::AfterAccept => "after accept",
            Self::DuringDenialResponse => "during denial response",
        })
    }
}

impl WebSocketEventContext {
    const fn expected_message_types(self) -> &'static str {
        match self {
            Self::BeforeHandshake => "websocket.accept or websocket.close",
            Self::AfterAccept => "websocket.send or websocket.close",
            Self::DuringDenialResponse => "websocket.http.response.body",
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum WebSocketProtocolError {
    #[error("websocket frame used non-canonical 16-bit length encoding")]
    NonCanonical16BitLengthEncoding,
    #[error("websocket frame used the reserved high bit in 64-bit length encoding")]
    ReservedHighBitIn64BitLengthEncoding,
    #[error("websocket frame was too large")]
    FrameTooLarge,
    #[error("websocket message exceeded the configured limit")]
    MessageTooLarge,
    #[error("websocket frame used non-canonical 64-bit length encoding")]
    NonCanonical64BitLengthEncoding,
    #[error("websocket extensions are not negotiated")]
    ExtensionsNotNegotiated,
    #[error("invalid permessage-deflate payload")]
    InvalidCompressedPayload,
    #[error("websocket continuation frames must not use RSV1")]
    CompressedContinuationFrame,
    #[error("websocket control frames must not use RSV1")]
    CompressedControlFrame,
    #[error("client websocket frames must be masked")]
    ClientFramesMustBeMasked,
    #[error("unsupported websocket opcode")]
    UnsupportedOpcode,
    #[error("received a new websocket data frame before the fragmented message completed")]
    DataBeforeFragmentCompletion,
    #[error("unexpected websocket continuation frame")]
    UnexpectedContinuationFrame,
    #[error("invalid websocket control frame")]
    InvalidControlFrame,
    #[error("websocket close frame payload was truncated")]
    CloseFramePayloadTruncated,
    #[error("websocket close frame contained an invalid close code")]
    CloseFrameInvalidCode,
    #[error("unsupported websocket control opcode")]
    UnsupportedControlOpcode,
    #[error("websocket frame payload was not valid UTF-8: {detail}")]
    InvalidUtf8 { detail: Box<str> },
}

impl WebSocketProtocolError {
    pub(crate) fn invalid_utf8(detail: impl Into<Box<str>>) -> Self {
        Self::InvalidUtf8 {
            detail: detail.into(),
        }
    }
}

pub(crate) trait ErrorExt: Into<H2CornError> + Sized {
    fn into_error(self) -> H2CornError {
        self.into()
    }

    fn err<T>(self) -> Result<T, H2CornError> {
        Err(self.into_error())
    }
}

impl<E> ErrorExt for E where E: Into<H2CornError> {}

pub(crate) trait IntoPyResult<T> {
    fn into_pyresult(self) -> PyResult<T>;
}

impl<T> IntoPyResult<T> for Result<T, H2CornError> {
    fn into_pyresult(self) -> PyResult<T> {
        self.map_err(into_pyerr)
    }
}

pub(crate) fn into_pyerr<E>(err: E) -> PyErr
where
    E: Into<H2CornError>,
{
    match err.into().into_kind() {
        ErrorKind::Python(err) => err,
        ErrorKind::Config(err) => PyValueError::new_err(err.to_string()),
        ErrorKind::Asgi(AsgiError::SendAfterClose) => {
            PyOSError::new_err(AsgiError::SendAfterClose.to_string())
        },
        ErrorKind::Asgi(err @ AsgiError::InvalidFieldType { .. }) => {
            PyTypeError::new_err(err.to_string())
        },
        ErrorKind::Asgi(
            err @ (AsgiError::MissingField { .. }
            | AsgiError::WebSocketSendRequiresExactlyOnePayload),
        ) => PyValueError::new_err(err.to_string()),
        ErrorKind::HttpResponse(
            err @ (HttpResponseError::InvalidResponseHeaderName
            | HttpResponseError::InvalidResponseHeaderValue
            | HttpResponseError::InvalidResponseTrailerField
            | HttpResponseError::StatusMustBeThreeDigitCode { .. }
            | HttpResponseError::InformationalStatusUnsupported { .. }),
        ) => PyValueError::new_err(err.to_string()),
        ErrorKind::WebSocket(
            err @ (WebSocketError::CloseReasonTooLong
            | WebSocketError::CloseCodeInvalid
            | WebSocketError::AcceptSubprotocolEmpty),
        ) => PyValueError::new_err(err.to_string()),
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use pyo3::Python;
    use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};

    use super::{
        AsgiContainer, AsgiError, FailureDomain, H2CornError, HttpResponseError, PathsendError,
        WebSocketError, WebSocketEventContext, WebSocketFrameKind, into_pyerr,
    };

    #[test]
    fn unexpected_websocket_events_name_the_asgi_type_without_payload() {
        let error = WebSocketError::unexpected_event(
            WebSocketEventContext::BeforeHandshake,
            "websocket.send",
        );

        assert_eq!(
            error.to_string(),
            "unexpected websocket.send before handshake; the app must send websocket.accept or websocket.close first"
        );
        assert!(!error.to_string().contains("sk-live-SECRET-TOKEN-12345"));
    }

    #[test]
    fn pathsend_failures_and_abandoned_websocket_receivers_are_reportable() {
        let pathsend = H2CornError::from(PathsendError::open_failed(
            std::path::Path::new("/tmp/missing"),
            io::Error::from(io::ErrorKind::NotFound),
        ));
        assert_eq!(pathsend.failure_domain(), FailureDomain::AppContract);

        let receiver = H2CornError::from(WebSocketError::receive_channel_closed(
            WebSocketFrameKind::Text,
        ));
        assert_eq!(receiver.failure_domain(), FailureDomain::AppContract);
        assert_eq!(
            receiver.to_string(),
            "application stopped receiving before the connection closed; a text frame was dropped"
        );
    }

    #[test]
    fn asgi_value_and_sequence_errors_have_stable_python_types() {
        Python::initialize();
        Python::attach(|py| {
            let missing = into_pyerr(AsgiError::missing_field(
                AsgiContainer::HttpResponseStart,
                "status",
            ));
            assert!(missing.is_instance_of::<PyValueError>(py));

            let type_error = into_pyerr(AsgiError::invalid_field_type(
                AsgiContainer::HttpResponseStart,
                "status",
                "an int",
                "str".into(),
            ));
            assert!(type_error.is_instance_of::<PyTypeError>(py));
            assert_eq!(
                type_error.to_string(),
                "TypeError: http.response.start status must be an int, got str"
            );

            let invalid_status = into_pyerr(HttpResponseError::StatusMustBeThreeDigitCode {
                container: AsgiContainer::HttpResponseStart,
                status: "99".into(),
            });
            assert!(invalid_status.is_instance_of::<PyValueError>(py));
            assert_eq!(
                invalid_status.to_string(),
                "ValueError: http.response.start status must be a three-digit code, got 99"
            );

            let out_of_order = into_pyerr(HttpResponseError::BodyBeforeStart);
            assert!(out_of_order.is_instance_of::<PyRuntimeError>(py));

            let unexpected = into_pyerr(WebSocketError::unexpected_initial_event("websocket.send"));
            assert!(unexpected.is_instance_of::<PyRuntimeError>(py));
        });
    }
}
