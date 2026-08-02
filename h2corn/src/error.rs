use std::path::{Path, PathBuf};
use std::{fmt, io};

use pyo3::exceptions::{PyOSError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::{PyErr, PyResult};
use thiserror::Error;
use tokio::task::JoinError;

use crate::hpack::DecoderError;
use crate::websocket::{WebSocketCloseCode, close_code};

/// Crate-wide error: a single pointer wide so every `Result<T, H2CornError>`
/// on the request path (and every future holding one) stays small; the
/// payload is boxed because errors are cold.
#[derive(Debug, Error)]
#[error(transparent)]
pub(crate) struct H2CornError(Box<ErrorKind>);

const _: () = assert!(std::mem::size_of::<H2CornError>() == std::mem::size_of::<usize>());

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
            ErrorKind::Http1(err) => err.failure_domain(),
            ErrorKind::H2(err) => err.failure_domain(),
            ErrorKind::Pathsend(err) => err.failure_domain(),
            ErrorKind::Proxy(err) => err.failure_domain(),
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
    #[cfg(unix)]
    HttpResponseZeroCopySend,
    HttpResponseTrailers,
    HttpResponseEarlyHint,
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
            #[cfg(unix)]
            Self::HttpResponseZeroCopySend => "http.response.zerocopysend",
            Self::HttpResponseTrailers => "http.response.trailers",
            Self::HttpResponseEarlyHint => "http.response.early_hint",
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
            Self::Http => "http",
            Self::WebSocket => "websocket",
        })
    }
}

// Error text names the offending field, never a peer-supplied value. Local
// configuration errors may include their values because an operator supplied
// them and needs to correct them; HTTP/1, HTTP/2, and PROXY errors must not
// echo untrusted request data. `FrameLengthExceedsPeerMax` and
// `InvalidPeerSettings` deliberately expose bounded protocol diagnostics in
// HTTP/2 GOAWAY debug data. `HpackDecode` was the third exception before this
// pass; fixed HPACK variants now replace it without echoing peer text.
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
    #[error("keep-alive connection did not receive the next request within timeout_keep_alive")]
    KeepAliveTimedOut,
    #[error("HTTP/1.1 request body did not receive data within timeout_request_body_idle")]
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
    #[error("unsupported transfer coding")]
    UnsupportedTransferCoding,
    #[error("HTTP/1.1 request body exceeds max_request_body_size")]
    RequestBodyTooLarge,
    #[error("HTTP/1.1 request body exceeded max_request_body_size")]
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
    #[error("trailer field exceeds limit_request_field_size")]
    TrailerFieldTooLarge,
    #[error("trailer field count exceeds limit_request_fields")]
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

impl Http1Error {
    const fn failure_domain(self) -> FailureDomain {
        match self {
            Self::RequestHeadClosed
            | Self::RequestBodyClosed
            | Self::ChunkedBodyClosed
            | Self::ChunkedTrailersClosed
            | Self::ChunkClosed => FailureDomain::TransportIo,
            Self::RequestHeadTimedOut
            | Self::KeepAliveTimedOut
            | Self::RequestBodyTimedOut
            | Self::EmptyRequestHead
            | Self::MalformedHeaderLine
            | Self::InvalidHeaderName
            | Self::InvalidHeaderValue
            | Self::ConflictingAbsoluteFormAuthority
            | Self::InvalidContentLength
            | Self::UnsupportedTransferCoding
            | Self::RequestBodyTooLarge
            | Self::RequestBodyLimitExceeded
            | Self::ChunkMissingCrlf
            | Self::TrailerFieldTooLarge
            | Self::TooManyTrailerFields
            | Self::InvalidRequestLine
            | Self::InvalidRequestMethod
            | Self::RequestTargetNotUtf8
            | Self::InvalidAbsoluteFormTarget
            | Self::InvalidRequestTargetForm
            | Self::InvalidAbsoluteFormAuthority
            | Self::InvalidChunkSize
            | Self::InvalidHttp2SettingsPayloadLength
            | Self::InvalidHttp2SettingsBase64UrlPayload => FailureDomain::PeerProtocol,
        }
    }
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
    #[cfg(unix)]
    #[error("http.response.zerocopysend received before response start")]
    ZeroCopySendBeforeStart,
    #[cfg(unix)]
    #[error("http.response.zerocopysend file must be a regular file")]
    ZeroCopySendNotRegularFile,
    #[cfg(unix)]
    #[error("http.response.zerocopysend file must be open for reading")]
    ZeroCopySendNotReadable,
    #[cfg(unix)]
    #[error(
        "http.response.zerocopysend file does not report a usable size; pass an explicit count"
    )]
    ZeroCopySendLengthUnknown,
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
        status: i64,
    },
    #[error(
        "{container} status must be a three-digit code, got an integer outside the signed 64-bit range"
    )]
    StatusOutsideSigned64BitRange { container: AsgiContainer },
    #[error(
        "{container} status must be a final response code; ASGI has no way to send an informational {status}"
    )]
    InformationalStatusUnsupported {
        container: AsgiContainer,
        status: u16,
    },
    #[error(
        "http.response.early_hint must follow http.response.start; an early hint precedes the final response but not the response itself"
    )]
    EarlyHintBeforeStart,
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
    #[error("SETTINGS ACK frame must have an empty payload")]
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
    #[error("CONTINUATION stream id did not match the open field block")]
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
    #[error("received a non-CONTINUATION frame while a field block was open")]
    FieldBlockInterrupted,
    #[error("field block exceeds h2_max_header_block_size")]
    FieldBlockTooLarge,
    #[error("field block was still incomplete at timeout_request_header")]
    FieldBlockAbandoned,
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
    #[error("incomplete HPACK field block")]
    IncompleteHpackFieldBlock,
    #[error("HPACK table index was invalid")]
    InvalidHpackTableIndex,
    #[error("HPACK Huffman code was invalid")]
    InvalidHpackHuffmanCode,
    #[error("HPACK dynamic table size was invalid")]
    InvalidHpackDynamicTableSize,
    #[error("HPACK integer overflow")]
    HpackIntegerOverflow,
    #[error("invalid HTTP/2 request pseudo-field")]
    InvalidRequestPseudoField,
    #[error("HTTP/2 request pseudo-field appeared after a regular field")]
    RequestPseudoFieldAfterRegularField,
    #[error("duplicate HTTP/2 request pseudo-field")]
    DuplicateRequestPseudoField,
    #[error("invalid HTTP/2 :method")]
    InvalidRequestMethod,
    #[error("invalid HTTP/2 :scheme")]
    InvalidRequestScheme,
    #[error("invalid HTTP/2 :authority")]
    InvalidRequestAuthority,
    #[error("invalid HTTP/2 :path")]
    InvalidRequestPath,
    #[error("invalid HTTP/2 request field")]
    InvalidRequestField,
    #[error("invalid HTTP/2 Host field")]
    InvalidRequestHost,
    #[error("invalid HTTP/2 content-length field")]
    InvalidRequestContentLength,
    #[error("conflicting HTTP/2 content-length fields")]
    ConflictingRequestContentLength,
    #[error("duplicate HTTP/2 Host field")]
    DuplicateRequestHost,
    #[error("HTTP/2 Host field disagreed with :authority")]
    ConflictingRequestAuthority,
    #[error("HTTP/2 request is missing :method")]
    MissingRequestMethod,
    #[error("HTTP/2 request is missing :scheme")]
    MissingRequestScheme,
    #[error("HTTP/2 request is missing :path")]
    MissingRequestPath,
    #[error("HTTP/2 :protocol is only valid with CONNECT")]
    ProtocolOnNonConnect,
    #[error("HTTP/2 CONNECT request must not include :scheme or :path")]
    ConnectWithSchemeOrPath,
    #[error("HTTP/2 CONNECT request is missing :authority")]
    MissingConnectAuthority,
    #[error("invalid HTTP/2 CONNECT :authority")]
    InvalidConnectAuthority,
    #[error("HTTP/2 request content-length did not match END_STREAM")]
    RequestContentLengthMismatch,
    #[error("HTTP/2 trailers must not contain pseudo-fields")]
    PseudoFieldInTrailers,
    #[error("HTTP/2 trailers contain a forbidden field")]
    ForbiddenTrailerField,
}

impl H2Error {
    const fn failure_domain(&self) -> FailureDomain {
        match self {
            Self::FrameHeaderClosed | Self::FramePayloadClosed => FailureDomain::TransportIo,
            Self::ResponseHeadersAlreadySent
            | Self::ResponseTrailersOnClosedOrUnopenedStream
            | Self::ResponseTrailersAlreadySent
            | Self::DataBeforeResponseHeaders
            | Self::DataOnClosedStream
            | Self::PathDataBeforeResponseHeaders
            | Self::PathDataOnClosedStream => FailureDomain::AppContract,
            Self::ConnectionWriterClosed | Self::StreamChannelClosed => {
                FailureDomain::InternalInvariant
            },
            Self::PlaintextHandshakeTimedOut
            | Self::TlsHandshakeTimedOut
            | Self::Http2HandshakeTimedOut
            | Self::SettingsEnablePushInvalid
            | Self::SettingsMaxFrameSizeInvalid
            | Self::SettingsEnableConnectProtocolInvalid
            | Self::SettingsInitialWindowSizeExceededLimit
            | Self::SettingsInitialWindowAdjustmentOverflow
            | Self::SettingsMaxFrameSizeOutOfRange
            | Self::FrameLengthExceedsPeerMax { .. }
            | Self::SettingsMustUseStreamZero
            | Self::SettingsAckPayloadNotEmpty
            | Self::SettingsPayloadLengthInvalid
            | Self::HeadersPaddedMissingPadLength
            | Self::HeadersPriorityTooShort
            | Self::HeadersPaddingExceedsPayload
            | Self::InvalidRequestStreamId
            | Self::ClientStreamIdsNotIncreasing
            | Self::HeadersOnClosedStream
            | Self::UnexpectedContinuationFrame
            | Self::ContinuationStreamIdMismatch
            | Self::DataMustNotUseStreamZero
            | Self::DataOnIdleStream
            | Self::ReceiveFlowControlWindowUnderflow
            | Self::SendFlowControlWindowOverflow
            | Self::FrameExceedsAdvertisedMaxSize
            | Self::FieldBlockInterrupted
            | Self::FieldBlockTooLarge
            | Self::FieldBlockAbandoned
            | Self::FirstClientFrameMustBeSettings
            | Self::FirstClientSettingsMustNotAck
            | Self::InvalidPeerSettings { .. }
            | Self::PingMustUseStreamZero
            | Self::PingPayloadInvalidLength
            | Self::WindowUpdatePayloadInvalidLength
            | Self::WindowUpdateIncrementZero
            | Self::WindowUpdateOnIdleStream
            | Self::RstStreamMustNotUseStreamZero
            | Self::RstStreamPayloadInvalidLength
            | Self::RstStreamOnIdleStream
            | Self::PeerResetFlood
            | Self::InvalidGoawayFrame
            | Self::PriorityMustNotUseStreamZero
            | Self::PriorityPayloadInvalidLength
            | Self::UnexpectedPushPromise
            | Self::DataPaddedMissingPadding
            | Self::DataPaddingExceedsPayload
            | Self::IncompleteHpackFieldBlock
            | Self::InvalidHpackTableIndex
            | Self::InvalidHpackHuffmanCode
            | Self::InvalidHpackDynamicTableSize
            | Self::HpackIntegerOverflow
            | Self::InvalidRequestPseudoField
            | Self::RequestPseudoFieldAfterRegularField
            | Self::DuplicateRequestPseudoField
            | Self::InvalidRequestMethod
            | Self::InvalidRequestScheme
            | Self::InvalidRequestAuthority
            | Self::InvalidRequestPath
            | Self::InvalidRequestField
            | Self::InvalidRequestHost
            | Self::InvalidRequestContentLength
            | Self::ConflictingRequestContentLength
            | Self::DuplicateRequestHost
            | Self::ConflictingRequestAuthority
            | Self::MissingRequestMethod
            | Self::MissingRequestScheme
            | Self::MissingRequestPath
            | Self::ProtocolOnNonConnect
            | Self::ConnectWithSchemeOrPath
            | Self::MissingConnectAuthority
            | Self::InvalidConnectAuthority
            | Self::RequestContentLengthMismatch
            | Self::PseudoFieldInTrailers
            | Self::ForbiddenTrailerField => FailureDomain::PeerProtocol,
        }
    }

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

impl From<DecoderError> for H2Error {
    fn from(error: DecoderError) -> Self {
        match error {
            DecoderError::NeedMore(_) => Self::IncompleteHpackFieldBlock,
            DecoderError::InvalidTableIndex => Self::InvalidHpackTableIndex,
            DecoderError::InvalidHuffmanCode => Self::InvalidHpackHuffmanCode,
            DecoderError::InvalidMaxDynamicSize => Self::InvalidHpackDynamicTableSize,
            DecoderError::IntegerOverflow => Self::HpackIntegerOverflow,
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

impl ProxyError {
    const fn failure_domain(&self) -> FailureDomain {
        match self {
            Self::ProtocolRequiresTrustedPeer => FailureDomain::Configuration,
            Self::ClosedBeforeProxyOrHttp2Preface
            | Self::ClosedWhileReadingProxyV2Header
            | Self::ClosedWhileReadingProxyV1Header
            | Self::ClosedBeforeHttp2Preface
            | Self::ClosedBeforeAnyRequestBytes
            | Self::ClosedBeforeProtocolDetection => FailureDomain::TransportIo,
            Self::InvalidProxyV2Header
            | Self::ProxyV1HeaderTooLong
            | Self::ExpectedProxyV1HeaderBeforeHttp2Preface
            | Self::ExpectedProxyV2HeaderBeforeHttp2Preface
            | Self::InvalidHttp2Preface
            | Self::InvalidProxyV1Header
            | Self::ProxyV1HeaderMissingCrlf
            | Self::UnsupportedProxyV1Transport
            | Self::InvalidProxyV1SourceAddress
            | Self::InvalidProxyV1DestinationAddress
            | Self::ProxyV1AddressFamilyMismatch
            | Self::InvalidProxyPort
            | Self::UnsupportedProxyV2Version
            | Self::TruncatedProxyV2Header
            | Self::UnsupportedProxyV2Command
            | Self::UnsupportedProxyV2Transport
            | Self::InvalidProxyV2Ipv4Payload
            | Self::InvalidProxyV2Ipv6Payload
            | Self::UnsupportedProxyV2AddressFamily => FailureDomain::PeerProtocol,
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum WebSocketError {
    #[error(transparent)]
    Protocol(#[from] WebSocketProtocolError),
    #[error("websocket handshake did not complete within timeout_handshake")]
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
    #[error("websocket message exceeded websocket_max_message_size")]
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

    pub(crate) const fn close_code(&self) -> WebSocketCloseCode {
        match self {
            Self::InvalidUtf8 { .. } => close_code::INVALID_FRAME_PAYLOAD_DATA,
            Self::MessageTooLarge => close_code::MESSAGE_TOO_BIG,
            _ => close_code::PROTOCOL_ERROR,
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

pyo3::create_exception!(
    h2corn._lib,
    SendAfterCloseError,
    PyOSError,
    "Raised when an application sends on a stream the server has already closed."
);

pub(crate) fn into_pyerr<E>(err: E) -> PyErr
where
    E: Into<H2CornError>,
{
    match err.into().into_kind() {
        ErrorKind::Python(err) => err,
        ErrorKind::Io(err) => err.into(),
        ErrorKind::Join(err) => PyRuntimeError::new_err(format!("background task failed: {err}")),
        ErrorKind::Config(err) => PyValueError::new_err(err.to_string()),
        // A wrong Python type in an outbound message.
        ErrorKind::Asgi(err @ AsgiError::InvalidFieldType { .. }) => {
            PyTypeError::new_err(err.to_string())
        },
        // Use of a stream the transport has already closed.
        ErrorKind::Asgi(AsgiError::SendAfterClose) => {
            SendAfterCloseError::new_err(AsgiError::SendAfterClose.to_string())
        },
        ErrorKind::Asgi(
            err @ (AsgiError::MissingField { .. }
            | AsgiError::WebSocketSendRequiresExactlyOnePayload),
        ) => PyValueError::new_err(err.to_string()),
        ErrorKind::Asgi(err @ AsgiError::UnsupportedOutboundMessage { .. }) => {
            PyRuntimeError::new_err(err.to_string())
        },
        // Separate arms rather than three more alternatives below: `#[cfg]`
        // applies to a match arm but not to one alternative of an `|` pattern,
        // and these variants do not exist off Unix.
        #[cfg(unix)]
        ErrorKind::HttpResponse(
            err @ (HttpResponseError::ZeroCopySendNotRegularFile
            | HttpResponseError::ZeroCopySendNotReadable
            | HttpResponseError::ZeroCopySendLengthUnknown),
        ) => PyValueError::new_err(err.to_string()),
        ErrorKind::HttpResponse(
            err @ (HttpResponseError::InvalidResponseHeaderName
            | HttpResponseError::InvalidResponseHeaderValue
            | HttpResponseError::InvalidResponseTrailerField
            | HttpResponseError::StatusMustBeThreeDigitCode { .. }
            | HttpResponseError::StatusOutsideSigned64BitRange { .. }
            | HttpResponseError::InformationalStatusUnsupported { .. }
            | HttpResponseError::EarlyHintBeforeStart),
        ) => PyValueError::new_err(err.to_string()),
        // Messages that are individually well formed but arrive in an order
        // the response state machine does not allow.
        #[cfg(unix)]
        ErrorKind::HttpResponse(err @ HttpResponseError::ZeroCopySendBeforeStart) => {
            PyRuntimeError::new_err(err.to_string())
        },
        ErrorKind::HttpResponse(
            err @ (HttpResponseError::StartAlreadyReceived
            | HttpResponseError::TrailersNotAdvertised
            | HttpResponseError::BodyBeforeStart
            | HttpResponseError::PathsendBeforeStart
            | HttpResponseError::PathsendMixedWithBody
            | HttpResponseError::TrailersBeforeBodyCompleted
            | HttpResponseError::AppReturnedWithoutStartingResponse
            | HttpResponseError::AppReturnedWithoutCompletingResponse),
        ) => PyRuntimeError::new_err(err.to_string()),
        ErrorKind::WebSocket(
            err @ (WebSocketError::CloseReasonTooLong
            | WebSocketError::CloseCodeInvalid
            | WebSocketError::AcceptSubprotocolEmpty
            | WebSocketError::AcceptHeadersForbidden
            | WebSocketError::AcceptSubprotocolNotRequested),
        ) => PyValueError::new_err(err.to_string()),
        // Peer, transport and sequencing conditions. None of these describe a
        // value the application supplied, so none of them are `ValueError`.
        ErrorKind::WebSocket(
            err @ (WebSocketError::Protocol(_)
            | WebSocketError::HandshakeTimedOut
            | WebSocketError::CompressionFailed
            | WebSocketError::ReceiveChannelClosed { .. }
            | WebSocketError::AppEndedBeforeHandshake
            | WebSocketError::UnexpectedEvent { .. }),
        ) => PyRuntimeError::new_err(err.to_string()),
        ErrorKind::Pathsend(PathsendError::OpenFailed { path, source }) => {
            let error = PyRuntimeError::new_err(format!(
                "http.response.pathsend failed for file {path:?}: {source}"
            ));
            pyo3::Python::attach(|py| error.set_cause(py, Some(source.into())));
            error
        },
        // The application named a path that cannot be sent.
        ErrorKind::Pathsend(err @ PathsendError::NotRegularFile { .. }) => {
            PyValueError::new_err(err.to_string())
        },
        // Wire-level failures of the peer's own making. They reach an
        // application only as the reason its stream ended, never as a verdict
        // on something it passed to `send()`, so they stay `RuntimeError` as a
        // group rather than being classified variant by variant.
        ErrorKind::Http1(err) => PyRuntimeError::new_err(err.to_string()),
        ErrorKind::H2(err) => PyRuntimeError::new_err(err.to_string()),
        ErrorKind::Proxy(err) => PyRuntimeError::new_err(err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use pyo3::Python;
    use pyo3::exceptions::{PyOSError, PyRuntimeError, PyTypeError, PyValueError};

    use super::{
        AsgiChannel, AsgiContainer, AsgiError, ConfigError, FailureDomain, H2CornError, H2Error,
        Http1Error, HttpResponseError, PathsendError, ProxyError, WebSocketError,
        WebSocketEventContext, WebSocketFrameKind, WebSocketProtocolError, into_pyerr,
    };

    macro_rules! rendered {
        ($messages:expr; $($error:expr),+ $(,)?) => {
            $(
                $messages.push(($error).to_string());
            )+
        };
    }

    const ERROR_MESSAGE_GROUPS: &[fn(&mut Vec<String>)] = &[
        config_error_messages,
        asgi_error_messages,
        http1_error_messages,
        response_error_messages,
        h2_error_messages,
        pathsend_and_proxy_error_messages,
        websocket_error_messages,
    ];

    fn config_error_messages(messages: &mut Vec<String>) {
        rendered!(messages;
            ConfigError::InvalidTrustedProxyEntry { value: "value".into() },
            ConfigError::InvalidTrustedProxyCidrPrefix { value: "value".into() },
            ConfigError::InvalidDuration { name: "timeout_handshake" },
            ConfigError::InvalidProxyProtocolMode { value: "value".into() },
            ConfigError::InvalidServerHeaderMode { value: "value".into() },
            ConfigError::InvalidClientCertMode { value: "value".into() },
            ConfigError::InvalidResponseHeaderFormat { value: "value".into() },
            ConfigError::InvalidResponseHeaderName { value: "value".into() },
            ConfigError::InvalidResponseHeaderValue { name: "name".into() },
            ConfigError::InvalidBindTarget { kind: "TCP", value: "value".into(), detail: "detail" },
            ConfigError::RuntimeThreadsAlreadyInitialized { initialized_threads: 1, worker_threads: 2 },
        );
    }

    fn asgi_error_messages(messages: &mut Vec<String>) {
        rendered!(messages;
            AsgiError::SendAfterClose,
            AsgiError::MissingField { container: AsgiContainer::Message, field: "field" },
            AsgiError::InvalidFieldType { container: AsgiContainer::Message, field: "field", expected: "a str", actual: "int".into() },
            AsgiError::UnsupportedOutboundMessage { channel: AsgiChannel::Http, message_type: "message".into() },
            AsgiError::UnsupportedOutboundMessage { channel: AsgiChannel::WebSocket, message_type: "message".into() },
            AsgiError::WebSocketSendRequiresExactlyOnePayload,
        );
    }

    fn http1_error_messages(messages: &mut Vec<String>) {
        rendered!(messages;
            Http1Error::RequestHeadTimedOut, Http1Error::KeepAliveTimedOut, Http1Error::RequestBodyTimedOut,
            Http1Error::RequestHeadClosed, Http1Error::EmptyRequestHead, Http1Error::MalformedHeaderLine,
            Http1Error::InvalidHeaderName, Http1Error::InvalidHeaderValue, Http1Error::ConflictingAbsoluteFormAuthority,
            Http1Error::InvalidContentLength, Http1Error::RequestBodyTooLarge, Http1Error::RequestBodyLimitExceeded,
            Http1Error::RequestBodyClosed, Http1Error::ChunkedBodyClosed, Http1Error::ChunkedTrailersClosed,
            Http1Error::ChunkClosed, Http1Error::ChunkMissingCrlf, Http1Error::TrailerFieldTooLarge,
            Http1Error::TooManyTrailerFields, Http1Error::InvalidRequestLine, Http1Error::InvalidRequestMethod,
            Http1Error::RequestTargetNotUtf8, Http1Error::InvalidAbsoluteFormTarget,
            Http1Error::InvalidRequestTargetForm, Http1Error::InvalidAbsoluteFormAuthority,
            Http1Error::InvalidChunkSize, Http1Error::InvalidHttp2SettingsPayloadLength,
            Http1Error::InvalidHttp2SettingsBase64UrlPayload,
        );
    }

    fn response_error_messages(messages: &mut Vec<String>) {
        rendered!(messages;
            HttpResponseError::StartAlreadyReceived, HttpResponseError::TrailersNotAdvertised,
            HttpResponseError::BodyBeforeStart, HttpResponseError::PathsendBeforeStart,
            HttpResponseError::PathsendMixedWithBody, HttpResponseError::TrailersBeforeBodyCompleted,
            HttpResponseError::AppReturnedWithoutStartingResponse, HttpResponseError::AppReturnedWithoutCompletingResponse,
            HttpResponseError::InvalidResponseHeaderName, HttpResponseError::InvalidResponseHeaderValue,
            HttpResponseError::InvalidResponseTrailerField,
            HttpResponseError::StatusMustBeThreeDigitCode { container: AsgiContainer::HttpResponseStart, status: 99 },
            HttpResponseError::StatusOutsideSigned64BitRange { container: AsgiContainer::HttpResponseStart },
            HttpResponseError::InformationalStatusUnsupported { container: AsgiContainer::HttpResponseStart, status: 100 },
        );
        #[cfg(unix)]
        rendered!(messages;
            HttpResponseError::ZeroCopySendBeforeStart, HttpResponseError::ZeroCopySendNotRegularFile,
            HttpResponseError::ZeroCopySendNotReadable, HttpResponseError::ZeroCopySendLengthUnknown,
        );
    }

    fn h2_error_messages(messages: &mut Vec<String>) {
        rendered!(messages;
            H2Error::PlaintextHandshakeTimedOut, H2Error::TlsHandshakeTimedOut, H2Error::Http2HandshakeTimedOut,
            H2Error::SettingsEnablePushInvalid, H2Error::SettingsMaxFrameSizeInvalid, H2Error::SettingsEnableConnectProtocolInvalid,
            H2Error::SettingsInitialWindowSizeExceededLimit, H2Error::SettingsInitialWindowAdjustmentOverflow,
            H2Error::SettingsMaxFrameSizeOutOfRange, H2Error::FrameHeaderClosed,
            H2Error::FrameLengthExceedsPeerMax { payload_len: 1, max_frame_size: 0 }, H2Error::FramePayloadClosed,
            H2Error::SettingsMustUseStreamZero, H2Error::SettingsAckPayloadNotEmpty, H2Error::SettingsPayloadLengthInvalid,
            H2Error::HeadersPaddedMissingPadLength, H2Error::HeadersPriorityTooShort, H2Error::HeadersPaddingExceedsPayload,
            H2Error::InvalidRequestStreamId, H2Error::ClientStreamIdsNotIncreasing, H2Error::HeadersOnClosedStream,
            H2Error::UnexpectedContinuationFrame, H2Error::ContinuationStreamIdMismatch, H2Error::DataMustNotUseStreamZero,
            H2Error::DataOnIdleStream, H2Error::ReceiveFlowControlWindowUnderflow, H2Error::SendFlowControlWindowOverflow,
            H2Error::FrameExceedsAdvertisedMaxSize, H2Error::FieldBlockInterrupted, H2Error::FieldBlockTooLarge,
            H2Error::FirstClientFrameMustBeSettings, H2Error::FirstClientSettingsMustNotAck,
            H2Error::InvalidPeerSettings { detail: "detail".into() }, H2Error::PingMustUseStreamZero,
            H2Error::PingPayloadInvalidLength, H2Error::WindowUpdatePayloadInvalidLength,
            H2Error::WindowUpdateIncrementZero, H2Error::WindowUpdateOnIdleStream,
            H2Error::RstStreamMustNotUseStreamZero, H2Error::RstStreamPayloadInvalidLength,
            H2Error::RstStreamOnIdleStream, H2Error::PeerResetFlood, H2Error::InvalidGoawayFrame,
            H2Error::PriorityMustNotUseStreamZero, H2Error::PriorityPayloadInvalidLength,
            H2Error::UnexpectedPushPromise, H2Error::DataPaddedMissingPadding, H2Error::DataPaddingExceedsPayload,
            H2Error::ResponseHeadersAlreadySent, H2Error::ResponseTrailersOnClosedOrUnopenedStream,
            H2Error::ResponseTrailersAlreadySent, H2Error::DataBeforeResponseHeaders, H2Error::DataOnClosedStream,
            H2Error::PathDataBeforeResponseHeaders, H2Error::PathDataOnClosedStream, H2Error::ConnectionWriterClosed,
            H2Error::StreamChannelClosed, H2Error::IncompleteHpackFieldBlock, H2Error::InvalidHpackTableIndex,
            H2Error::InvalidHpackHuffmanCode, H2Error::InvalidHpackDynamicTableSize, H2Error::HpackIntegerOverflow,
            H2Error::InvalidRequestPseudoField, H2Error::RequestPseudoFieldAfterRegularField,
            H2Error::DuplicateRequestPseudoField, H2Error::InvalidRequestMethod, H2Error::InvalidRequestScheme,
            H2Error::InvalidRequestAuthority, H2Error::InvalidRequestPath, H2Error::InvalidRequestField,
            H2Error::InvalidRequestHost, H2Error::InvalidRequestContentLength, H2Error::ConflictingRequestContentLength,
            H2Error::DuplicateRequestHost, H2Error::ConflictingRequestAuthority, H2Error::MissingRequestMethod,
            H2Error::MissingRequestScheme, H2Error::MissingRequestPath, H2Error::ProtocolOnNonConnect,
            H2Error::ConnectWithSchemeOrPath, H2Error::MissingConnectAuthority, H2Error::InvalidConnectAuthority,
            H2Error::RequestContentLengthMismatch, H2Error::PseudoFieldInTrailers, H2Error::ForbiddenTrailerField,
        );
    }

    fn pathsend_and_proxy_error_messages(messages: &mut Vec<String>) {
        rendered!(messages;
            PathsendError::OpenFailed { path: "path".into(), source: io::Error::other("source") },
            PathsendError::NotRegularFile { path: "path".into() },
            ProxyError::ProtocolRequiresTrustedPeer, ProxyError::ClosedBeforeProxyOrHttp2Preface,
            ProxyError::InvalidProxyV2Header, ProxyError::ClosedWhileReadingProxyV2Header,
            ProxyError::ProxyV1HeaderTooLong, ProxyError::ClosedWhileReadingProxyV1Header,
            ProxyError::ExpectedProxyV1HeaderBeforeHttp2Preface, ProxyError::ExpectedProxyV2HeaderBeforeHttp2Preface,
            ProxyError::ClosedBeforeHttp2Preface, ProxyError::InvalidHttp2Preface,
            ProxyError::ClosedBeforeAnyRequestBytes, ProxyError::ClosedBeforeProtocolDetection,
            ProxyError::InvalidProxyV1Header, ProxyError::ProxyV1HeaderMissingCrlf,
            ProxyError::UnsupportedProxyV1Transport, ProxyError::InvalidProxyV1SourceAddress,
            ProxyError::InvalidProxyV1DestinationAddress, ProxyError::ProxyV1AddressFamilyMismatch,
            ProxyError::InvalidProxyPort, ProxyError::UnsupportedProxyV2Version, ProxyError::TruncatedProxyV2Header,
            ProxyError::UnsupportedProxyV2Command, ProxyError::UnsupportedProxyV2Transport,
            ProxyError::InvalidProxyV2Ipv4Payload, ProxyError::InvalidProxyV2Ipv6Payload,
            ProxyError::UnsupportedProxyV2AddressFamily,
        );
    }

    fn websocket_error_messages(messages: &mut Vec<String>) {
        rendered!(messages;
            WebSocketError::Protocol(WebSocketProtocolError::NonCanonical16BitLengthEncoding),
            WebSocketError::HandshakeTimedOut, WebSocketError::CompressionFailed,
            WebSocketError::CloseReasonTooLong, WebSocketError::CloseCodeInvalid,
            WebSocketError::AcceptHeadersForbidden, WebSocketError::AcceptSubprotocolEmpty,
            WebSocketError::AcceptSubprotocolNotRequested,
            WebSocketError::ReceiveChannelClosed { frame_kind: WebSocketFrameKind::Text },
            WebSocketError::ReceiveChannelClosed { frame_kind: WebSocketFrameKind::Binary },
            WebSocketError::AppEndedBeforeHandshake,
            WebSocketError::UnexpectedEvent { context: WebSocketEventContext::BeforeHandshake, message_type: "message".into() },
            WebSocketProtocolError::NonCanonical16BitLengthEncoding,
            WebSocketProtocolError::ReservedHighBitIn64BitLengthEncoding, WebSocketProtocolError::FrameTooLarge,
            WebSocketProtocolError::MessageTooLarge, WebSocketProtocolError::NonCanonical64BitLengthEncoding,
            WebSocketProtocolError::ExtensionsNotNegotiated, WebSocketProtocolError::InvalidCompressedPayload,
            WebSocketProtocolError::CompressedContinuationFrame, WebSocketProtocolError::CompressedControlFrame,
            WebSocketProtocolError::ClientFramesMustBeMasked, WebSocketProtocolError::UnsupportedOpcode,
            WebSocketProtocolError::DataBeforeFragmentCompletion, WebSocketProtocolError::UnexpectedContinuationFrame,
            WebSocketProtocolError::InvalidControlFrame, WebSocketProtocolError::CloseFramePayloadTruncated,
            WebSocketProtocolError::CloseFrameInvalidCode, WebSocketProtocolError::UnsupportedControlOpcode,
            WebSocketProtocolError::InvalidUtf8 { detail: "detail".into() },
        );
    }

    #[test]
    fn rendered_error_messages_follow_style_policy() {
        const PROTOCOL_NOUNS: &[&str] = &[
            "HTTP/1.1",
            "HTTP/2",
            "SETTINGS",
            "PROXY",
            "ASGI",
            "TLS",
            "DATA",
            "PING",
            "HEADERS",
            "CONTINUATION",
            "RST_STREAM",
            "WINDOW_UPDATE",
            "PRIORITY",
            "HPACK",
        ];

        let mut messages = Vec::new();
        for group in ERROR_MESSAGE_GROUPS {
            group(&mut messages);
        }

        for message in messages {
            assert!(!message.ends_with('.'), "{message}");
            assert!(
                message.chars().next().is_some_and(char::is_lowercase)
                    || PROTOCOL_NOUNS.iter().any(|noun| message.starts_with(noun)),
                "{message}"
            );
            assert!(!message.contains('{'), "{message}");
        }
    }

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
    fn http_and_proxy_errors_have_precise_failure_domains() {
        let cases = [
            (
                H2CornError::from(Http1Error::RequestHeadClosed),
                FailureDomain::TransportIo,
            ),
            (
                H2CornError::from(Http1Error::RequestBodyClosed),
                FailureDomain::TransportIo,
            ),
            (
                H2CornError::from(Http1Error::ChunkedBodyClosed),
                FailureDomain::TransportIo,
            ),
            (
                H2CornError::from(Http1Error::ChunkedTrailersClosed),
                FailureDomain::TransportIo,
            ),
            (
                H2CornError::from(Http1Error::ChunkClosed),
                FailureDomain::TransportIo,
            ),
            (
                H2CornError::from(H2Error::FrameHeaderClosed),
                FailureDomain::TransportIo,
            ),
            (
                H2CornError::from(H2Error::FramePayloadClosed),
                FailureDomain::TransportIo,
            ),
            (
                H2CornError::from(H2Error::ResponseHeadersAlreadySent),
                FailureDomain::AppContract,
            ),
            (
                H2CornError::from(H2Error::ResponseTrailersOnClosedOrUnopenedStream),
                FailureDomain::AppContract,
            ),
            (
                H2CornError::from(H2Error::ResponseTrailersAlreadySent),
                FailureDomain::AppContract,
            ),
            (
                H2CornError::from(H2Error::DataBeforeResponseHeaders),
                FailureDomain::AppContract,
            ),
            (
                H2CornError::from(H2Error::DataOnClosedStream),
                FailureDomain::AppContract,
            ),
            (
                H2CornError::from(H2Error::PathDataBeforeResponseHeaders),
                FailureDomain::AppContract,
            ),
            (
                H2CornError::from(H2Error::PathDataOnClosedStream),
                FailureDomain::AppContract,
            ),
            (
                H2CornError::from(H2Error::ConnectionWriterClosed),
                FailureDomain::InternalInvariant,
            ),
            (
                H2CornError::from(H2Error::StreamChannelClosed),
                FailureDomain::InternalInvariant,
            ),
            (
                H2CornError::from(ProxyError::ProtocolRequiresTrustedPeer),
                FailureDomain::Configuration,
            ),
            (
                H2CornError::from(ProxyError::ClosedBeforeProxyOrHttp2Preface),
                FailureDomain::TransportIo,
            ),
            (
                H2CornError::from(ProxyError::ClosedWhileReadingProxyV2Header),
                FailureDomain::TransportIo,
            ),
            (
                H2CornError::from(ProxyError::ClosedWhileReadingProxyV1Header),
                FailureDomain::TransportIo,
            ),
            (
                H2CornError::from(ProxyError::ClosedBeforeHttp2Preface),
                FailureDomain::TransportIo,
            ),
            (
                H2CornError::from(ProxyError::ClosedBeforeAnyRequestBytes),
                FailureDomain::TransportIo,
            ),
            (
                H2CornError::from(ProxyError::ClosedBeforeProtocolDetection),
                FailureDomain::TransportIo,
            ),
        ];

        for (error, domain) in cases {
            assert_eq!(error.failure_domain(), domain, "{error}");
        }
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
                status: 99,
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

    /// The Python type an application catches is public API, so every variant
    /// earns one by decision. `into_pyerr` has no catch-all arm: adding a
    /// variant to any of these enums fails the build until it is classified,
    /// which is how `failure_domain` already works.
    ///
    /// The WebSocket accept values below are the ones a wildcard used to
    /// swallow — an application saw `send()` fail with `RuntimeError` for a
    /// malformed value it had supplied.
    #[test]
    fn every_application_facing_error_has_a_decided_python_type() {
        Python::initialize();
        Python::attach(|py| {
            let value_errors: [H2CornError; 8] = [
                WebSocketError::AcceptHeadersForbidden.into(),
                WebSocketError::AcceptSubprotocolNotRequested.into(),
                WebSocketError::AcceptSubprotocolEmpty.into(),
                WebSocketError::CloseCodeInvalid.into(),
                WebSocketError::CloseReasonTooLong.into(),
                HttpResponseError::InvalidResponseHeaderName.into(),
                AsgiError::WebSocketSendRequiresExactlyOnePayload.into(),
                PathsendError::NotRegularFile {
                    path: "/dev/null".into(),
                }
                .into(),
            ];
            for error in value_errors {
                let rendered = error.to_string();
                assert!(
                    into_pyerr(error).is_instance_of::<PyValueError>(py),
                    "{rendered} must reach the application as ValueError"
                );
            }

            let runtime_errors: [H2CornError; 5] = [
                WebSocketError::HandshakeTimedOut.into(),
                WebSocketError::CompressionFailed.into(),
                WebSocketError::AppEndedBeforeHandshake.into(),
                HttpResponseError::StartAlreadyReceived.into(),
                HttpResponseError::TrailersNotAdvertised.into(),
            ];
            for error in runtime_errors {
                let rendered = error.to_string();
                assert!(
                    into_pyerr(error).is_instance_of::<PyRuntimeError>(py),
                    "{rendered} must reach the application as RuntimeError"
                );
            }

            assert!(
                into_pyerr(AsgiError::SendAfterClose).is_instance_of::<PyOSError>(py),
                "a closed stream is an OS-level condition, not a bad value"
            );
        });
    }

    #[test]
    fn python_errors_keep_io_types_and_pathsend_causes() {
        Python::initialize();
        Python::attach(|py| {
            let io_error = into_pyerr(io::Error::new(io::ErrorKind::PermissionDenied, "denied"));
            assert!(io_error.is_instance_of::<PyOSError>(py));

            let pathsend = into_pyerr(PathsendError::open_failed(
                std::path::Path::new("/tmp/secret"),
                io::Error::new(io::ErrorKind::NotFound, "missing"),
            ));
            assert!(pathsend.is_instance_of::<PyRuntimeError>(py));
            let cause = pathsend
                .cause(py)
                .expect("pathsend preserves its I/O cause");
            assert!(cause.is_instance_of::<PyOSError>(py));
            assert!(cause.to_string().contains("missing"));
        });
    }
}
