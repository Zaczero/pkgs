use std::{fmt, io, path::Path};

use pyo3::{
    PyErr, PyResult,
    exceptions::{PyOSError, PyRuntimeError, PyValueError},
};
use thiserror::Error;
use tokio::task::JoinError;

#[derive(Debug, Error)]
pub enum H2CornError {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FailureDomain {
    Configuration,
    PeerProtocol,
    TransportIo,
    AppContract,
    InternalInvariant,
}

impl H2CornError {
    pub(crate) fn failure_domain(&self) -> FailureDomain {
        match self {
            Self::Io(_) => FailureDomain::TransportIo,
            Self::Join(_) => FailureDomain::InternalInvariant,
            Self::Config(_) => FailureDomain::Configuration,
            Self::Python(_) | Self::Asgi(_) | Self::HttpResponse(_) => FailureDomain::AppContract,
            Self::Http1(_) | Self::H2(_) | Self::Proxy(_) => FailureDomain::PeerProtocol,
            Self::Pathsend(err) => err.failure_domain(),
            Self::WebSocket(err) => err.failure_domain(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AsgiContainer {
    Message,
    HttpResponseStart,
    HttpResponsePathsend,
    WebSocketHttpResponseStart,
}

impl fmt::Display for AsgiContainer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Message => "ASGI message",
            Self::HttpResponseStart => "http.response.start",
            Self::HttpResponsePathsend => "http.response.pathsend",
            Self::WebSocketHttpResponseStart => "websocket.http.response.start",
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
            Self::WebSocket => "WebSocket",
        })
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid trusted proxy entry: {value}")]
    InvalidTrustedProxyEntry { value: Box<str> },
    #[error("invalid trusted proxy CIDR prefix: {value}")]
    InvalidTrustedProxyCidrPrefix { value: Box<str> },
    #[error("{name} must be a finite non-negative number")]
    InvalidFiniteDuration { name: &'static str },
    #[error("invalid proxy_protocol mode: {value:?}")]
    InvalidProxyProtocolMode { value: Box<str> },
    #[error("bind metadata length does not match configured listeners")]
    BindMetadataLengthMismatch,
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

    pub(crate) fn invalid_finite_duration(name: &'static str) -> Self {
        Self::InvalidFiniteDuration { name }
    }

    pub(crate) fn invalid_proxy_protocol_mode(value: &str) -> Self {
        Self::InvalidProxyProtocolMode {
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

    pub(crate) fn runtime_threads_already_initialized(
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
pub enum AsgiError {
    #[error("ASGI send called after the stream closed")]
    SendAfterClose,
    #[error("{container} is missing required field: {field}")]
    MissingField {
        container: AsgiContainer,
        field: &'static str,
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
    pub(crate) fn missing_field(container: AsgiContainer, field: &'static str) -> Self {
        Self::MissingField { container, field }
    }

    pub(crate) fn unsupported_http_outbound_message(message_type: &str) -> Self {
        Self::UnsupportedOutboundMessage {
            channel: AsgiChannel::Http,
            message_type: message_type.into(),
        }
    }

    pub(crate) fn unsupported_websocket_outbound_message(message_type: &str) -> Self {
        Self::UnsupportedOutboundMessage {
            channel: AsgiChannel::WebSocket,
            message_type: message_type.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Error)]
pub enum Http1Error {
    #[error("h2c upgrade with a request body is unsupported")]
    H2cUpgradeWithRequestBody,
    #[error("HTTP/1.1 request head timed out")]
    RequestHeadTimedOut,
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
    #[error("invalid HTTP/1.1 request line")]
    InvalidRequestLine,
    #[error("invalid HTTP/1.1 request method")]
    InvalidRequestMethod,
    #[error("request target was not valid UTF-8")]
    RequestTargetNotUtf8,
    #[error("invalid absolute-form HTTP/1.1 request target")]
    InvalidAbsoluteFormTarget,
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
pub enum HttpResponseError {
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
    #[error("response status must be a three-digit code")]
    StatusMustBeThreeDigitCode,
}

#[derive(Debug, Error)]
pub enum H2Error {
    #[error("HTTP/2 handshake timed out")]
    ConnectionHandshakeTimedOut,
    #[error("invalid SETTINGS_ENABLE_PUSH value")]
    SettingsEnablePushInvalid,
    #[error("invalid SETTINGS_MAX_FRAME_SIZE value")]
    SettingsMaxFrameSizeInvalid,
    #[error("invalid SETTINGS_ENABLE_CONNECT_PROTOCOL value")]
    SettingsEnableConnectProtocolInvalid,
    #[error("SETTINGS_INITIAL_WINDOW_SIZE exceeds the protocol limit")]
    SettingsInitialWindowSizeExceededLimit,
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
    #[error("received a frame larger than the advertised max frame size")]
    FrameExceedsAdvertisedMaxSize,
    #[error("received a non-CONTINUATION frame while a header block was open")]
    HeaderBlockInterrupted,
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
}

impl H2Error {
    pub(crate) fn frame_length_exceeds_peer_max(payload_len: usize, max_frame_size: usize) -> Self {
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
pub enum PathsendError {
    #[error("http.response.pathsend failed for file {path}: {source}")]
    OpenFailed {
        path: Box<str>,
        #[source]
        source: io::Error,
    },
}

impl PathsendError {
    pub(crate) fn open_failed(path: &Path, source: io::Error) -> Self {
        Self::OpenFailed {
            path: path.display().to_string().into_boxed_str(),
            source,
        }
    }

    fn failure_domain(&self) -> FailureDomain {
        FailureDomain::TransportIo
    }
}

#[derive(Debug, Error)]
pub enum ProxyError {
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
pub enum WebSocketError {
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
    #[error("websocket receive channel closed while delivering a {frame_kind} frame")]
    ReceiveChannelClosed { frame_kind: WebSocketFrameKind },
    #[error("websocket app returned before handshake")]
    AppEndedBeforeHandshake,
    #[error("unexpected websocket event {context}: {event}")]
    UnexpectedEvent {
        context: WebSocketEventContext,
        event: Box<str>,
    },
}

impl WebSocketError {
    pub(crate) fn receive_channel_closed(frame_kind: WebSocketFrameKind) -> Self {
        Self::ReceiveChannelClosed { frame_kind }
    }

    pub(crate) fn unexpected_event(context: WebSocketEventContext, event: impl fmt::Debug) -> Self {
        Self::UnexpectedEvent {
            context,
            event: format!("{event:?}").into_boxed_str(),
        }
    }

    pub(crate) fn unexpected_initial_event(event: impl fmt::Debug) -> Self {
        Self::unexpected_event(WebSocketEventContext::BeforeHandshake, event)
    }

    pub(crate) fn unexpected_outbound_event_after_accept(event: impl fmt::Debug) -> Self {
        Self::unexpected_event(WebSocketEventContext::AfterAccept, event)
    }

    pub(crate) fn unexpected_denial_body_event(event: impl fmt::Debug) -> Self {
        Self::unexpected_event(WebSocketEventContext::DuringDenialResponse, event)
    }

    fn failure_domain(&self) -> FailureDomain {
        match self {
            Self::Protocol(_) => FailureDomain::PeerProtocol,
            Self::HandshakeTimedOut
            | Self::CloseReasonTooLong
            | Self::CloseCodeInvalid
            | Self::AcceptHeadersForbidden
            | Self::AcceptSubprotocolEmpty
            | Self::AcceptSubprotocolNotRequested
            | Self::AppEndedBeforeHandshake
            | Self::UnexpectedEvent { .. } => FailureDomain::AppContract,
            Self::CompressionFailed | Self::ReceiveChannelClosed { .. } => {
                FailureDomain::InternalInvariant
            }
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

#[derive(Debug, Error)]
pub enum WebSocketProtocolError {
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

pub(crate) fn into_pyerr<E>(err: E) -> PyErr
where
    E: Into<H2CornError>,
{
    match err.into() {
        H2CornError::Python(err) => err,
        H2CornError::Config(err) => PyValueError::new_err(err.to_string()),
        H2CornError::Asgi(AsgiError::SendAfterClose) => {
            PyOSError::new_err(AsgiError::SendAfterClose.to_string())
        }
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

pub trait IntoPyResult<T> {
    fn into_pyresult(self) -> PyResult<T>;
}

impl<T> IntoPyResult<T> for Result<T, H2CornError> {
    fn into_pyresult(self) -> PyResult<T> {
        self.map_err(into_pyerr)
    }
}
