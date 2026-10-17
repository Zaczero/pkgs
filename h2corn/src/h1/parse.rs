use std::num::{NonZeroU64, NonZeroUsize};
use std::str;
use std::sync::LazyLock;

use bytes::{Buf as _, Bytes, BytesMut};
use http::{Method, Uri};
use memchr::{memchr, memchr2, memmem};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, BufWriter};
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};

use crate::ascii;
use crate::async_util::send_if_open;
use crate::config::ServerConfig;
use crate::error::{ErrorExt as _, ErrorKind, H2CornError, Http1Error};
use crate::h1::http::write_empty_response;
use crate::h1::{
    ConnectionPersistence, ParsedRequest, RequestBodyKind, RequestRoute, UpgradeRequest,
};
use crate::h2_frame::{PeerSettings, SETTING_ENTRY_LEN, parse_settings_payload};
use crate::http::body::{RequestBodyFinish, RequestBodyProgress, RequestBodyState};
use crate::http::header::{
    ConnectionHeaderTokens, TransferCoding, classify_transfer_coding, header_contains_token,
    parse_connection_header_tokens, parse_content_length_header, request_authority_is_valid,
    request_header_name_needs_lowercase, request_path_is_valid, request_scheme_is_valid,
    trailer_field_name_is_forbidden,
};
use crate::http::header_meta::{ParsedWebSocketKey, ParsedWebSocketVersion, RequestHeaderMeta};
use crate::http::header_value::header_value_is_valid;
use crate::http::planner::reject_before_launch;
use crate::http::types::{
    BytesStr, ConnectAuthority, H1RequestHeaders, HttpStatusCode, HttpVersion,
    KnownRequestHeaderName, RequestHead, RequestHeaders, RequestTarget as DomainRequestTarget,
    parse_request_method, status_code,
};
use crate::runtime::StreamInput;

const HEADER_TERMINATOR: &[u8; 4] = b"\r\n\r\n";
const LINE_TERMINATOR: &[u8; 2] = b"\r\n";
const CHUNK_BUFFER_SIZE: usize = 8192;
const MAX_CHUNK_SIZE_LINE_BYTES: usize = 16 * 1024;
const MAX_CHUNK_SIZE_LINE_WIRE_BYTES: usize = MAX_CHUNK_SIZE_LINE_BYTES + LINE_TERMINATOR.len();
const CHUNK_DELIVERY_BATCH_BYTES: usize = 64 * 1024;
const MAX_TRAILER_SECTION_BYTES: usize = 64 * 1024;
static HEADER_TERMINATOR_FINDER: LazyLock<memmem::Finder<'static>> =
    LazyLock::new(|| memmem::Finder::new(HEADER_TERMINATOR));
static LINE_TERMINATOR_FINDER: LazyLock<memmem::Finder<'static>> =
    LazyLock::new(|| memmem::Finder::new(LINE_TERMINATOR));

struct BufferedTerminatorFinder<'a> {
    finder: &'a memmem::Finder<'static>,
    search_start: usize,
    overlap: usize,
}

impl<'a> BufferedTerminatorFinder<'a> {
    const fn new(finder: &'a memmem::Finder<'static>, needle_len: usize) -> Self {
        Self {
            finder,
            search_start: 0,
            overlap: needle_len.saturating_sub(1),
        }
    }

    fn find(&mut self, buffer: &[u8]) -> Option<usize> {
        let start = self.search_start.min(buffer.len());
        if let Some(offset) = buffer.get(start..).and_then(|tail| self.finder.find(tail)) {
            return Some(start.wrapping_add(offset));
        }
        self.search_start = buffer.len().saturating_sub(self.overlap);
        None
    }

    const fn reset(&mut self) {
        self.search_start = 0;
    }
}

#[derive(Default)]
struct UpgradeHeaderFlags {
    websocket: bool,
    h2c: bool,
}

struct HeaderParseState {
    headers: H1RequestHeaders,
    host_header_index: Option<usize>,
    connection: ConnectionHeaderTokens,
    upgrade: UpgradeHeaderFlags,
    chunked: bool,
    expect_continue: bool,
    http2_settings: Option<PeerSettings>,
    header_field_count: usize,
    header_meta: RequestHeaderMeta,
    collect_proxy_headers: bool,
}

/// The configured field policy, applied to trailers as well as headers.
///
/// Trailer values are discarded — ASGI does not expose request trailers — but
/// the grammar and the operator's limits still apply. Draining them as bare
/// CRLF-terminated lines admitted arbitrary text and any number of fields
/// through a second parser that knew nothing about `limit_request_fields`.
#[derive(Clone, Copy)]
pub(super) struct FieldLimits {
    pub(super) max_fields: Option<usize>,
    pub(super) max_field_size: Option<usize>,
}

/// Coalesce decoded chunked-body bytes before handing them to Python.
///
/// Chunk framing is a transport detail: exposing every legal one-byte chunk
/// turns a small request into an unbounded stream of Python calls. Fixed-size
/// bodies keep their existing direct path because they already arrive as a
/// single framing unit.
struct ChunkDeliveryBatch {
    pending: BytesMut,
}

impl ChunkDeliveryBatch {
    fn new() -> Self {
        Self {
            pending: BytesMut::new(),
        }
    }

    async fn flush(&mut self, tx: &mpsc::Sender<StreamInput>, body: &mut RequestBodyState) {
        if self.pending.is_empty() || !body.should_deliver() {
            self.pending.clear();
            return;
        }

        let chunk = self.pending.split().freeze();
        if !send_if_open(tx, StreamInput::data(chunk)).await {
            body.stop_delivering();
        }
    }

    async fn push(
        &mut self,
        buffer: &mut BytesMut,
        chunk_len: usize,
        tx: &mpsc::Sender<StreamInput>,
        body: &mut RequestBodyState,
    ) -> Result<(), H2CornError> {
        match body.record_chunk(chunk_len as u64) {
            RequestBodyProgress::Continue => {},
            RequestBodyProgress::SizeLimitExceeded => {
                self.flush(tx, body).await;
                return Http1Error::RequestBodyLimitExceeded.err();
            },
            RequestBodyProgress::ContentLengthExceeded => {
                self.flush(tx, body).await;
                return Http1Error::RequestBodyTooLarge.err();
            },
        }

        if !body.should_deliver() {
            buffer.advance(chunk_len);
            return Ok(());
        }

        // A large contiguous region is already a good application payload;
        // preserve it without an intermediate copy when there is no partial
        // batch to join first.
        if self.pending.is_empty() && chunk_len >= CHUNK_DELIVERY_BATCH_BYTES {
            let chunk = buffer.split_to(chunk_len).freeze();
            if !send_if_open(tx, StreamInput::data(chunk)).await {
                body.stop_delivering();
            }
            return Ok(());
        }

        if self.pending.is_empty() {
            self.pending.reserve(CHUNK_DELIVERY_BATCH_BYTES);
        }
        self.pending.extend_from_slice(&buffer.split_to(chunk_len));
        if self.pending.len() >= CHUNK_DELIVERY_BATCH_BYTES {
            self.flush(tx, body).await;
        }
        Ok(())
    }
}

/// A request target in one of the four forms of RFC 9112 §3.2, already checked
/// against the method that may use it: authority-form only for `CONNECT`,
/// asterisk-form only for `OPTIONS`, and neither of the other two for
/// `CONNECT`. Constructing one is the only way past that check.
enum ParsedRequestTarget<'a> {
    Origin(&'a [u8]),
    Absolute(Uri),
    Asterisk,
    Authority(&'a [u8]),
}

struct RequestLineParts<'a> {
    method: Method,
    target: &'a [u8],
}

impl HeaderParseState {
    fn new(head: Bytes, collect_proxy_headers: bool) -> Self {
        Self {
            headers: H1RequestHeaders::new(head),
            host_header_index: None,
            connection: ConnectionHeaderTokens::default(),
            upgrade: UpgradeHeaderFlags::default(),
            chunked: false,
            expect_continue: false,
            http2_settings: None,
            header_field_count: 0,
            header_meta: RequestHeaderMeta::default(),
            collect_proxy_headers,
        }
    }

    fn header_too_large(
        &mut self,
        line: &[u8],
        limit_request_fields: Option<usize>,
        limit_request_field_size: Option<usize>,
    ) -> bool {
        let too_many_header_fields = if let Some(limit) = limit_request_fields {
            self.header_field_count = self.header_field_count.saturating_add(1);
            self.header_field_count > limit
        } else {
            false
        };
        too_many_header_fields || limit_request_field_size.is_some_and(|limit| line.len() > limit)
    }

    fn push_header(&mut self, line: &[u8]) -> Result<(), H2CornError> {
        let Some((name, [b':', raw_value @ ..])) =
            memchr(b':', line).and_then(|at| line.split_at_checked(at))
        else {
            return Http1Error::MalformedHeaderLine.err();
        };
        let value = trim_ows(raw_value);
        let index = self.headers.len();
        let known_name = self.headers.push(name, value).map_err(|()| {
            if request_header_name_needs_lowercase(name).is_none() {
                Http1Error::InvalidHeaderName
            } else {
                Http1Error::InvalidHeaderValue
            }
        })?;
        let Some(known_name) = known_name else {
            return Ok(());
        };

        self.apply_known_header(known_name, value, index)
    }

    fn apply_known_header(
        &mut self,
        known_name: KnownRequestHeaderName,
        value: &[u8],
        index: usize,
    ) -> Result<(), H2CornError> {
        self.header_meta.observe_known_header_slice(
            known_name,
            value,
            index,
            self.collect_proxy_headers,
        );
        match known_name {
            KnownRequestHeaderName::Host => {
                if !request_authority_is_valid(value) {
                    return Http1Error::InvalidRequestTargetForm.err();
                }
                if self.host_header_index.is_some() {
                    return Http1Error::ConflictingAbsoluteFormAuthority.err();
                }
                self.host_header_index = Some(index);
            },
            KnownRequestHeaderName::Connection => {
                self.connection |= parse_connection_header_tokens(value);
            },
            KnownRequestHeaderName::Upgrade => {
                // RFC 9110 §7.8: Upgrade is a list of protocols in order of
                // preference, so a client that also offers something we do
                // not speak still asked for the one we do.
                self.upgrade.websocket |= header_contains_token(value, b"websocket");
                self.upgrade.h2c |= header_contains_token(value, b"h2c");
            },
            KnownRequestHeaderName::Te => {
                if header_contains_token(value, b"trailers") {
                    self.header_meta.set_accepts_trailers();
                }
            },
            KnownRequestHeaderName::ContentLength => {
                if self.chunked {
                    return Http1Error::InvalidContentLength.err();
                }
                let parsed =
                    parse_content_length_header(value).ok_or(Http1Error::InvalidContentLength)?;
                if self.header_meta.try_set_content_length(parsed).is_err() {
                    return Http1Error::InvalidContentLength.err();
                }
            },
            KnownRequestHeaderName::TransferEncoding => {
                // A repeated field line is a second application of chunked,
                // and Content-Length alongside Transfer-Encoding is RFC 9112
                // 6.3 rule 3. Both are unframed, so both stay 400.
                if self.chunked || self.header_meta.content_length().is_some() {
                    return Http1Error::MalformedHeaderLine.err();
                }
                match classify_transfer_coding(value) {
                    TransferCoding::Chunked => {
                        self.chunked = true;
                        self.header_meta.set_content_length(None);
                    },
                    TransferCoding::Unsupported => {
                        return Http1Error::UnsupportedTransferCoding.err();
                    },
                    TransferCoding::Malformed => {
                        return Http1Error::MalformedHeaderLine.err();
                    },
                }
            },
            KnownRequestHeaderName::Expect => {
                // RFC 9110 10.1.1 makes Expect a list, and repeated field
                // lines extend that list. Whole-value equality missed every
                // list form, and *assigning* let a later `Expect: foo` clear
                // an earlier `100-continue` -- either way the client waits for
                // an interim response that never comes, until it times out.
                self.expect_continue |= header_contains_token(value, b"100-continue");
            },
            KnownRequestHeaderName::Http2Settings => {
                let settings = parse_http2_settings(value)?;
                if self.http2_settings.replace(settings).is_some() {
                    return Http1Error::MalformedHeaderLine.err();
                }
            },
            KnownRequestHeaderName::Accept
            | KnownRequestHeaderName::AcceptEncoding
            | KnownRequestHeaderName::AcceptLanguage
            | KnownRequestHeaderName::Authorization
            | KnownRequestHeaderName::CacheControl
            | KnownRequestHeaderName::ContentType
            | KnownRequestHeaderName::Cookie
            | KnownRequestHeaderName::Forwarded
            | KnownRequestHeaderName::IfModifiedSince
            | KnownRequestHeaderName::IfNoneMatch
            | KnownRequestHeaderName::KeepAlive
            | KnownRequestHeaderName::Origin
            | KnownRequestHeaderName::Pragma
            | KnownRequestHeaderName::ProxyConnection
            | KnownRequestHeaderName::Referer
            | KnownRequestHeaderName::SecWebSocketVersion
            | KnownRequestHeaderName::SecWebSocketKey
            | KnownRequestHeaderName::SecWebSocketProtocol
            | KnownRequestHeaderName::SecWebSocketExtensions
            | KnownRequestHeaderName::UserAgent
            | KnownRequestHeaderName::XForwardedFor
            | KnownRequestHeaderName::XForwardedProto
            | KnownRequestHeaderName::XForwardedHost
            | KnownRequestHeaderName::XForwardedPort
            | KnownRequestHeaderName::XForwardedPrefix => {},
        }
        Ok(())
    }
}

enum HeadParseOutcome<T> {
    Parsed(T),
    Reject(HttpStatusCode),
}

struct ParsedHead {
    request: ParsedRequest,
    expect_continue: bool,
}

struct HeadLineCursor<'a> {
    remaining: &'a [u8],
    finished: bool,
}

impl<'a> HeadLineCursor<'a> {
    const fn new(head: &'a [u8]) -> Self {
        Self {
            remaining: head,
            finished: false,
        }
    }

    fn next_line(&mut self) -> HeadParseOutcome<Option<&'a [u8]>> {
        if self.finished {
            return HeadParseOutcome::Parsed(None);
        }
        let Some(delimiter_start) = memchr2(b'\r', b'\n', self.remaining) else {
            self.finished = true;
            return HeadParseOutcome::Parsed(Some(self.remaining));
        };
        let (line, delimiter) = self.remaining.split_at(delimiter_start);
        let [b'\r', b'\n', rest @ ..] = delimiter else {
            return HeadParseOutcome::Reject(status_code::BAD_REQUEST);
        };
        self.remaining = rest;
        HeadParseOutcome::Parsed(Some(line))
    }
}

async fn read_request_head<R, W>(
    reader: &mut R,
    buffer: &mut BytesMut,
    writer: &mut BufWriter<W>,
    config: &ServerConfig,
    timeout_duration: Option<Duration>,
    first_request: bool,
    limit_request_head_size: Option<usize>,
) -> Result<Option<Bytes>, H2CornError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut header_search =
        BufferedTerminatorFinder::new(&HEADER_TERMINATOR_FINDER, HEADER_TERMINATOR.len());
    let header_len = loop {
        if let Some(end) = header_search.find(buffer) {
            if limit_request_head_size
                .is_some_and(|limit| end.wrapping_add(HEADER_TERMINATOR.len()) > limit)
            {
                write_empty_response(
                    writer,
                    config,
                    status_code::REQUEST_HEADER_FIELDS_TOO_LARGE,
                    true,
                )
                .await?;
                return Ok(None);
            }
            break end;
        }
        let read_cap = limit_request_head_size.map(|limit| limit.saturating_sub(buffer.len()));
        if read_cap == Some(0) {
            write_empty_response(
                writer,
                config,
                status_code::REQUEST_HEADER_FIELDS_TOO_LARGE,
                true,
            )
            .await?;
            return Ok(None);
        }
        let timeout_error = if !first_request && buffer.is_empty() {
            Http1Error::KeepAliveTimedOut
        } else {
            Http1Error::RequestHeadTimedOut
        };
        if !read_more(reader, buffer, timeout_duration, timeout_error, read_cap).await? {
            if buffer.is_empty() {
                return Ok(None);
            }
            return Http1Error::RequestHeadClosed.err();
        }
    };

    let mut head = buffer.split_to(header_len.wrapping_add(HEADER_TERMINATOR.len()));
    head.truncate(header_len);
    Ok(Some(head.freeze()))
}

fn parse_request_line_or_reject(
    request_line: &[u8],
    limit_request_line: Option<usize>,
) -> Result<HeadParseOutcome<RequestLineParts<'_>>, H2CornError> {
    if limit_request_line.is_some_and(|limit| request_line.len() > limit) {
        return Ok(HeadParseOutcome::Reject(status_code::URI_TOO_LONG));
    }
    let (method, target, version) = parse_request_line(request_line)?;
    match version {
        b"HTTP/1.1" => {},
        // Every other version is refused with 400, including HTTP/1.0. Not
        // 505: RFC 9110 15.6.6 scopes that status to the *major* version, and
        // major version 1 is supported. h2corn does not serve HTTP/1.0 -- that
        // needs close-delimited framing, no chunked, no keep-alive-by-default
        // and no 100-continue, a second framing regime through the whole
        // response path -- but saying so with 505 misstated the reason.
        _ => return Ok(HeadParseOutcome::Reject(status_code::BAD_REQUEST)),
    }
    Ok(HeadParseOutcome::Parsed(RequestLineParts {
        method,
        target,
    }))
}

fn parse_headers_or_reject(
    lines: &mut HeadLineCursor<'_>,
    head: &Bytes,
    limit_request_fields: Option<usize>,
    limit_request_field_size: Option<usize>,
    collect_proxy_headers: bool,
) -> HeadParseOutcome<HeaderParseState> {
    let mut header_state = HeaderParseState::new(head.clone(), collect_proxy_headers);
    loop {
        let line = match lines.next_line() {
            HeadParseOutcome::Parsed(Some(line)) => line,
            HeadParseOutcome::Parsed(None) => break,
            HeadParseOutcome::Reject(status) => return HeadParseOutcome::Reject(status),
        };
        if line.is_empty() {
            continue;
        }
        if header_state.header_too_large(line, limit_request_fields, limit_request_field_size) {
            return HeadParseOutcome::Reject(status_code::REQUEST_HEADER_FIELDS_TOO_LARGE);
        }
        if let Err(err) = header_state.push_header(line) {
            return HeadParseOutcome::Reject(match err.into_kind() {
                // RFC 9112 6.1: the message was framed, we just do not
                // implement the coding. Every other rejection here is a
                // malformed field line.
                ErrorKind::Http1(Http1Error::UnsupportedTransferCoding) => {
                    status_code::NOT_IMPLEMENTED
                },
                ErrorKind::Python(_)
                | ErrorKind::Io(_)
                | ErrorKind::Join(_)
                | ErrorKind::Config(_)
                | ErrorKind::Asgi(_)
                | ErrorKind::Http1(_)
                | ErrorKind::HttpResponse(_)
                | ErrorKind::H2(_)
                | ErrorKind::Pathsend(_)
                | ErrorKind::Proxy(_)
                | ErrorKind::WebSocket(_) => status_code::BAD_REQUEST,
            });
        }
    }
    HeadParseOutcome::Parsed(header_state)
}

fn parsed_request_from_head(
    head: &Bytes,
    line: RequestLineParts<'_>,
    mut header_state: HeaderParseState,
    scheme: &'static str,
) -> Result<HeadParseOutcome<ParsedHead>, H2CornError> {
    let scheme = BytesStr::from_static(scheme);
    // Every way a target can fail to parse is the client's, and the framing is
    // intact by the time we get here, so each one is answerable with a status
    // rather than a dropped connection.
    let Ok(request_target) = parse_request_target(
        &line.method,
        line.target,
        &mut header_state.headers,
        header_state.host_header_index,
    ) else {
        return Ok(HeadParseOutcome::Reject(status_code::BAD_REQUEST));
    };
    if !matches!(
        request_target,
        ParsedRequestTarget::Absolute(_) | ParsedRequestTarget::Authority(_)
    ) && header_state.host_header_index.is_none()
    {
        return Ok(HeadParseOutcome::Reject(status_code::BAD_REQUEST));
    }
    let target = match request_target {
        // The authority is the whole target: `CONNECT` asks for a tunnel to it,
        // which the shared planner answers with 501 for both protocols.
        ParsedRequestTarget::Authority(authority) => {
            let authority = BytesStr::try_from(head.slice_ref(authority))
                .map_err(|_| Http1Error::RequestTargetNotUtf8.into_error())?;
            let Ok(authority) = ConnectAuthority::try_from(authority) else {
                return Ok(HeadParseOutcome::Reject(status_code::BAD_REQUEST));
            };
            DomainRequestTarget::connect(authority)
        },
        ParsedRequestTarget::Origin(path) => DomainRequestTarget::normal(
            scheme,
            BytesStr::try_from(head.slice_ref(path))
                .map_err(|_| Http1Error::RequestTargetNotUtf8.into_error())?,
        ),
        ParsedRequestTarget::Absolute(uri) => DomainRequestTarget::normal(
            scheme,
            uri.path_and_query()
                .map_or(BytesStr::from_static("/"), |path_and_query| {
                    BytesStr::from(path_and_query.as_str())
                }),
        ),
        ParsedRequestTarget::Asterisk => {
            DomainRequestTarget::normal(scheme, BytesStr::from_static("*"))
        },
    };
    let HeaderParseState {
        headers,
        connection,
        upgrade,
        chunked,
        http2_settings,
        header_meta,
        expect_continue,
        ..
    } = header_state;
    // One source of truth: `Content-Length: 0` means no body, and that is a
    // derivation rather than a second assignment kept in step by hand.
    let body_kind = if chunked {
        RequestBodyKind::Chunked
    } else {
        header_meta
            .content_length()
            .and_then(NonZeroU64::new)
            .map_or(RequestBodyKind::None, RequestBodyKind::ContentLength)
    };
    let request = RequestHead {
        http_version: HttpVersion::Http1_1,
        method: line.method,
        target,
        headers: RequestHeaders::from_h1(headers),
        header_meta,
    };
    let route = if upgrade.websocket && connection.upgrade() {
        if body_kind != RequestBodyKind::None {
            return Ok(HeadParseOutcome::Reject(status_code::BAD_REQUEST));
        }
        let bad_request = RequestRoute::Upgrade(UpgradeRequest::WebSocketBadRequest);
        match request.header_meta.websocket() {
            Some(websocket)
                if websocket.version == ParsedWebSocketVersion::Supported
                    && request.method == Method::GET
                    && let ParsedWebSocketKey::Valid(key) = websocket.key
                    && let Some(meta) = websocket.request.clone().into_valid() =>
            {
                RequestRoute::Upgrade(UpgradeRequest::WebSocket {
                    key,
                    meta: Box::new(meta),
                })
            },
            Some(websocket) if !websocket.version.is_unsupported() => bad_request,
            _ => RequestRoute::Upgrade(UpgradeRequest::WebSocketUnsupportedVersion),
        }
    } else if let Some(settings) = http2_settings
        && upgrade.h2c
        && connection.upgrade()
        && connection.http2_settings()
    {
        if body_kind == RequestBodyKind::None {
            RequestRoute::Upgrade(UpgradeRequest::H2c { settings })
        } else {
            RequestRoute::Http(body_kind)
        }
    } else {
        RequestRoute::Http(body_kind)
    };

    Ok(HeadParseOutcome::Parsed(ParsedHead {
        request: ParsedRequest {
            request,
            route,
            persistence: if connection.close() {
                ConnectionPersistence::Close
            } else {
                ConnectionPersistence::KeepAlive
            },
        },
        expect_continue,
    }))
}

pub(super) async fn read_request<R, W>(
    reader: &mut R,
    buffer: &mut BytesMut,
    writer: &mut BufWriter<W>,
    config: &ServerConfig,
    scheme: &'static str,
    timeout_duration: Option<Duration>,
    first_request: bool,
    collect_proxy_headers: bool,
) -> Result<Option<ParsedRequest>, H2CornError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let http1 = &config.http1;
    let limit_request_head_size = http1.limit_request_head_size.map(NonZeroUsize::get);
    let limit_request_line = http1.limit_request_line.map(NonZeroUsize::get);
    let limit_request_fields = config.limit_request_fields.map(NonZeroUsize::get);
    let limit_request_field_size = http1.limit_request_field_size.map(NonZeroUsize::get);

    let Some(head) = read_request_head(
        reader,
        buffer,
        writer,
        config,
        timeout_duration,
        first_request,
        limit_request_head_size,
    )
    .await?
    else {
        return Ok(None);
    };
    let mut lines = HeadLineCursor::new(head.as_ref());
    let mut next_line = || match lines.next_line() {
        HeadParseOutcome::Parsed(line) => Ok(line),
        HeadParseOutcome::Reject(status) => Err(status),
    };
    let mut first = match next_line() {
        Ok(line) => line,
        Err(status) => {
            write_empty_response(writer, config, status, true).await?;
            return Ok(None);
        },
    };
    // RFC 9112 2.2: a server SHOULD ignore at least one empty line received
    // before the request-line. Clients have historically left a stray CRLF
    // after a request body, and dropping the connection over it is the least
    // robust thing available.
    if first == Some(&b""[..]) {
        first = match next_line() {
            Ok(line) => line,
            Err(status) => {
                write_empty_response(writer, config, status, true).await?;
                return Ok(None);
            },
        };
    }
    let Some(request_line) = first else {
        return Http1Error::EmptyRequestHead.err();
    };
    let line = match parse_request_line_or_reject(request_line, limit_request_line)? {
        HeadParseOutcome::Parsed(line) => line,
        HeadParseOutcome::Reject(status) => {
            write_empty_response(writer, config, status, true).await?;
            return Ok(None);
        },
    };
    let header_state = match parse_headers_or_reject(
        &mut lines,
        &head,
        limit_request_fields,
        limit_request_field_size,
        collect_proxy_headers,
    ) {
        HeadParseOutcome::Parsed(header_state) => header_state,
        HeadParseOutcome::Reject(status) => {
            write_empty_response(writer, config, status, true).await?;
            return Ok(None);
        },
    };

    let parsed = match parsed_request_from_head(&head, line, header_state, scheme)? {
        HeadParseOutcome::Parsed(parsed) => parsed,
        HeadParseOutcome::Reject(status) => {
            write_empty_response(writer, config, status, true).await?;
            return Ok(None);
        },
    };
    // Only invite a body the server is actually willing to read. A request
    // already destined for 413 or 501 used to be told "100 Continue" first,
    // so the client uploaded a body purely to have it refused.
    if parsed.expect_continue
        && parsed
            .request
            .route
            .body_kind()
            .is_some_and(|body| body != RequestBodyKind::None)
        && reject_before_launch(&parsed.request.request, config.max_request_body_size).is_ok()
    {
        writer.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").await?;
        writer.flush().await?;
    }

    Ok(Some(parsed.request))
}

pub(super) async fn read_fixed_body<R>(
    reader: &mut R,
    buffer: &mut BytesMut,
    len: u64,
    tx: &mpsc::Sender<StreamInput>,
    body: &mut RequestBodyState,
    timeout_duration: Option<Duration>,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin,
{
    let mut remaining = usize::try_from(len).map_err(|_| Http1Error::RequestBodyTooLarge)?;

    while remaining > 0 {
        if buffer.is_empty()
            && !read_more(
                reader,
                buffer,
                timeout_duration,
                Http1Error::RequestBodyTimedOut,
                None,
            )
            .await?
        {
            return Http1Error::RequestBodyClosed.err();
        }
        let chunk_len = usize::min(buffer.len(), remaining);
        consume_body_bytes(buffer, chunk_len, tx, body).await?;
        remaining = remaining.saturating_sub(chunk_len);
    }

    Ok(())
}

pub(super) async fn read_chunked_body<R>(
    reader: &mut R,
    buffer: &mut BytesMut,
    tx: &mpsc::Sender<StreamInput>,
    body: &mut RequestBodyState,
    timeout_duration: Option<Duration>,
    limits: FieldLimits,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin,
{
    let mut batch = ChunkDeliveryBatch::new();
    loop {
        let size = match read_chunk_size_line(reader, buffer, timeout_duration).await {
            Ok(size) => size,
            Err(error) => {
                batch.flush(tx, body).await;
                return Err(error);
            },
        };
        if size == 0 {
            if let Err(error) =
                drain_chunked_trailers(reader, buffer, timeout_duration, limits).await
            {
                batch.flush(tx, body).await;
                return Err(error);
            }
            batch.flush(tx, body).await;
            return match body.finish() {
                RequestBodyFinish::Complete => Ok(()),
                RequestBodyFinish::ContentLengthMismatch => Http1Error::RequestBodyClosed.err(),
            };
        }
        match body.preview_chunk(size as u64) {
            RequestBodyProgress::Continue => {},
            RequestBodyProgress::SizeLimitExceeded => {
                batch.flush(tx, body).await;
                return Http1Error::RequestBodyLimitExceeded.err();
            },
            RequestBodyProgress::ContentLengthExceeded => {
                batch.flush(tx, body).await;
                return Http1Error::RequestBodyTooLarge.err();
            },
        }

        let mut remaining = size;
        while remaining > 0 {
            if buffer.is_empty()
                && !read_more(
                    reader,
                    buffer,
                    timeout_duration,
                    Http1Error::RequestBodyTimedOut,
                    None,
                )
                .await?
            {
                batch.flush(tx, body).await;
                return Http1Error::ChunkClosed.err();
            }
            let chunk_len = buffer.len().min(remaining);
            batch.push(buffer, chunk_len, tx, body).await?;
            remaining = remaining.saturating_sub(chunk_len);
        }

        if let Err(error) = consume_chunk_crlf(reader, buffer, timeout_duration).await {
            batch.flush(tx, body).await;
            return Err(error);
        }
    }
}

async fn consume_body_bytes(
    buffer: &mut BytesMut,
    chunk_len: usize,
    tx: &mpsc::Sender<StreamInput>,
    body: &mut RequestBodyState,
) -> Result<(), H2CornError> {
    match body.record_chunk(chunk_len as u64) {
        RequestBodyProgress::Continue => {},
        RequestBodyProgress::SizeLimitExceeded => Http1Error::RequestBodyLimitExceeded.err()?,
        RequestBodyProgress::ContentLengthExceeded => Http1Error::RequestBodyTooLarge.err()?,
    }
    if body.should_deliver() {
        let chunk = buffer.split_to(chunk_len).freeze();
        if !send_if_open(tx, StreamInput::data(chunk)).await {
            body.stop_delivering();
        }
    } else {
        buffer.advance(chunk_len);
    }
    Ok(())
}

async fn read_chunk_size_line<R>(
    reader: &mut R,
    buffer: &mut BytesMut,
    timeout_duration: Option<Duration>,
) -> Result<usize, H2CornError>
where
    R: AsyncRead + Unpin,
{
    let mut line_search =
        BufferedTerminatorFinder::new(&LINE_TERMINATOR_FINDER, LINE_TERMINATOR.len());
    let line_end = loop {
        if let Some(end) = line_search.find(buffer) {
            if end > MAX_CHUNK_SIZE_LINE_BYTES {
                return Http1Error::InvalidChunkSize.err();
            }
            break end;
        }
        let read_cap = MAX_CHUNK_SIZE_LINE_WIRE_BYTES.saturating_sub(buffer.len());
        if read_cap == 0 {
            return Http1Error::InvalidChunkSize.err();
        }
        if !read_more(
            reader,
            buffer,
            timeout_duration,
            Http1Error::RequestBodyTimedOut,
            Some(read_cap),
        )
        .await?
        {
            return Http1Error::ChunkedBodyClosed.err();
        }
    };
    let Some(line) = buffer.get(..line_end) else {
        return Http1Error::InvalidChunkSize.err();
    };
    let size = parse_chunk_size(line)?;
    buffer.advance(line_end.wrapping_add(LINE_TERMINATOR.len()));
    Ok(size)
}

async fn consume_chunk_crlf<R>(
    reader: &mut R,
    buffer: &mut BytesMut,
    timeout_duration: Option<Duration>,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin,
{
    while buffer.len() < 2 {
        if !read_more(
            reader,
            buffer,
            timeout_duration,
            Http1Error::RequestBodyTimedOut,
            None,
        )
        .await?
        {
            return Http1Error::ChunkClosed.err();
        }
    }
    if buffer.first_chunk::<2>() != Some(b"\r\n") {
        return Http1Error::ChunkMissingCrlf.err();
    }
    buffer.advance(2);
    Ok(())
}

fn validate_trailer_line(line: &[u8], limits: FieldLimits) -> Result<(), H2CornError> {
    if limits
        .max_field_size
        .is_some_and(|limit| line.len() > limit)
    {
        return Http1Error::TrailerFieldTooLarge.err();
    }
    let Some((name, [b':', raw_value @ ..])) =
        memchr(b':', line).and_then(|at| line.split_at_checked(at))
    else {
        return Http1Error::MalformedHeaderLine.err();
    };
    if request_header_name_needs_lowercase(name).is_none() {
        return Http1Error::InvalidHeaderName.err();
    }
    if trailer_field_name_is_forbidden(name) {
        return Http1Error::MalformedHeaderLine.err();
    }
    if !header_value_is_valid(raw_value) {
        return Http1Error::InvalidHeaderValue.err();
    }
    Ok(())
}

/// RFC 9112 OWS is exactly SP / HTAB. `trim_ascii` also removes illegal
/// controls, which would turn malformed wire bytes into valid field values.
fn trim_ows(value: &[u8]) -> &[u8] {
    let start = value
        .iter()
        .position(|byte| !matches!(byte, b' ' | b'\t'))
        .unwrap_or(value.len());
    let end = value
        .iter()
        .rposition(|byte| !matches!(byte, b' ' | b'\t'))
        .map_or(start, |index| index.wrapping_add(1));
    value.get(start..end).unwrap_or_default()
}

async fn drain_chunked_trailers<R>(
    reader: &mut R,
    buffer: &mut BytesMut,
    timeout_duration: Option<Duration>,
    limits: FieldLimits,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin,
{
    let mut fields = 0_usize;
    let mut line_search =
        BufferedTerminatorFinder::new(&LINE_TERMINATOR_FINDER, LINE_TERMINATOR.len());
    let mut section_bytes = 0_usize;
    loop {
        let Some(end) = line_search.find(buffer) else {
            let buffered_bytes = section_bytes.saturating_add(buffer.len());
            if buffered_bytes >= MAX_TRAILER_SECTION_BYTES {
                return Http1Error::MalformedHeaderLine.err();
            }
            if !read_more(
                reader,
                buffer,
                timeout_duration,
                Http1Error::RequestBodyTimedOut,
                Some(MAX_TRAILER_SECTION_BYTES.saturating_sub(buffered_bytes)),
            )
            .await?
            {
                return Http1Error::ChunkedTrailersClosed.err();
            }
            continue;
        };
        let line_len = end.wrapping_add(LINE_TERMINATOR.len());
        section_bytes = section_bytes.saturating_add(line_len);
        if section_bytes > MAX_TRAILER_SECTION_BYTES {
            return Http1Error::MalformedHeaderLine.err();
        }
        if end == 0 {
            buffer.advance(line_len);
            return Ok(());
        }
        fields = fields.saturating_add(1);
        if limits.max_fields.is_some_and(|limit| fields > limit) {
            return Http1Error::TooManyTrailerFields.err();
        }
        let Some(line) = buffer.get(..end) else {
            return Http1Error::MalformedHeaderLine.err();
        };
        validate_trailer_line(line, limits)?;
        buffer.advance(line_len);
        line_search.reset();
    }
}

fn parse_request_line(line: &[u8]) -> Result<(Method, &[u8], &[u8]), H2CornError> {
    let Some((method_bytes, [b' ', after_method @ ..])) =
        memchr(b' ', line).and_then(|at| line.split_at_checked(at))
    else {
        return Http1Error::InvalidRequestLine.err();
    };
    let Some((target, [b' ', version @ ..])) =
        memchr(b' ', after_method).and_then(|at| after_method.split_at_checked(at))
    else {
        return Http1Error::InvalidRequestLine.err();
    };
    let method =
        parse_request_method(method_bytes).map_err(|_| Http1Error::InvalidRequestMethod)?;
    Ok((method, target, version))
}

fn parse_request_target<'a>(
    method: &Method,
    target: &'a [u8],
    headers: &mut H1RequestHeaders,
    host_header_index: Option<usize>,
) -> Result<ParsedRequestTarget<'a>, H2CornError> {
    // `CONNECT` names a tunnel endpoint, never a resource: the other three
    // forms carry no authority to tunnel to, and admitting one would hand the
    // application an ordinary request for `/` instead (RFC 9112 §3.4).
    if method == Method::CONNECT {
        // Authority-form is the host and port of the tunnel destination and
        // nothing else (RFC 9112 §3.2.3), which also makes the authority the
        // whole target — so it borrows the head rather than being rebuilt.
        if !request_authority_is_valid(target) {
            return Http1Error::InvalidRequestTargetForm.err();
        }
        return Ok(ParsedRequestTarget::Authority(target));
    }
    match target {
        // Asterisk-form addresses the server itself, which only `OPTIONS` may
        // ask about (RFC 9112 §3.2.4).
        b"*" if method == Method::OPTIONS => return Ok(ParsedRequestTarget::Asterisk),
        b"*" => return Http1Error::InvalidRequestTargetForm.err(),
        [b'/', ..] if request_path_is_valid(method, target) => {
            return Ok(ParsedRequestTarget::Origin(target));
        },
        [b'/', ..] => return Http1Error::InvalidRequestTargetForm.err(),
        _ => {},
    }
    if !raw_absolute_form_target_is_valid(method, target) {
        return Http1Error::InvalidAbsoluteFormTarget.err();
    }
    let uri = str::from_utf8(target)
        .map_err(|_| Http1Error::RequestTargetNotUtf8)?
        .parse::<Uri>()
        .map_err(|_| Http1Error::InvalidAbsoluteFormTarget)?;
    // Absolute-form is the only remaining legal form, and it is absolute only
    // with a scheme; a bare `host:port` is authority-form, which no method but
    // `CONNECT` may send.
    let Some(scheme) = uri.scheme() else {
        return Http1Error::InvalidRequestTargetForm.err();
    };
    let Some(authority) = uri.authority() else {
        return Http1Error::InvalidAbsoluteFormTarget.err();
    };
    if !request_scheme_is_valid(scheme.as_str().as_bytes())
        || !request_authority_is_valid(authority.as_str().as_bytes())
        || uri
            .path_and_query()
            .is_some_and(|path| !request_path_is_valid(method, path.as_str().as_bytes()))
    {
        return Http1Error::InvalidAbsoluteFormTarget.err();
    }
    if let Some(host_header_index) = host_header_index
        && let Some(authority) = uri.authority()
        && !headers
            .get(host_header_index)
            .expect("recorded host index must exist")
            .value()
            .eq_ignore_ascii_case(authority.as_str().as_bytes())
    {
        return Http1Error::ConflictingAbsoluteFormAuthority.err();
    }
    if host_header_index.is_none()
        && let Some(authority) = uri.authority()
        && !headers.push_synthetic(KnownRequestHeaderName::Host, authority.as_str().as_bytes())
    {
        return Http1Error::InvalidAbsoluteFormAuthority.err();
    }
    Ok(ParsedRequestTarget::Absolute(uri))
}

/// Validate the absolute-form grammar directly on the request-target bytes.
///
/// This intentionally precedes `Uri` parsing: `Uri` has no fragment field and
/// therefore cannot report a `#fragment` that appeared on the wire.
fn raw_absolute_form_target_is_valid(method: &Method, target: &[u8]) -> bool {
    let Some((scheme, [b':', after_scheme @ ..])) =
        memchr(b':', target).and_then(|at| target.split_at_checked(at))
    else {
        return false;
    };
    let Some(after_authority) = after_scheme.strip_prefix(b"//") else {
        return false;
    };
    let authority_end = after_authority
        .iter()
        .position(|byte| matches!(*byte, b'/' | b'?' | b'#'))
        .unwrap_or(after_authority.len());
    let (authority, path_and_query) = after_authority.split_at(authority_end);

    !target.contains(&b'#')
        && request_scheme_is_valid(scheme)
        && request_authority_is_valid(authority)
        && (path_and_query.is_empty() || request_path_is_valid(method, path_and_query))
}

#[expect(
    clippy::indexing_slicing,
    reason = "a u8 index cannot leave a 256-entry table"
)]
fn parse_chunk_size(line: &[u8]) -> Result<usize, H2CornError> {
    let mut value = 0_usize;
    let mut index = 0;
    while let Some(&byte) = line.get(index) {
        let digit = ascii::HEX_VALUE[usize::from(byte)];
        if digit == ascii::INVALID_VALUE {
            break;
        }
        value = value
            .checked_mul(16)
            .and_then(|value| value.checked_add(usize::from(digit)))
            .ok_or(Http1Error::InvalidChunkSize)?;
        index = index.wrapping_add(1);
    }
    if index == 0 {
        return Http1Error::InvalidChunkSize.err();
    }
    let mut trailing_whitespace = false;
    while line
        .get(index)
        .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
    {
        trailing_whitespace = true;
        index = index.wrapping_add(1);
    }
    if index == line.len() {
        return if trailing_whitespace {
            Http1Error::InvalidChunkSize.err()
        } else {
            Ok(value)
        };
    }

    loop {
        if line.get(index) != Some(&b';') {
            return Http1Error::InvalidChunkSize.err();
        }
        index = index.wrapping_add(1);
        while line
            .get(index)
            .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
        {
            index = index.wrapping_add(1);
        }
        let token_start = index;
        while line
            .get(index)
            .is_some_and(|byte| request_header_name_needs_lowercase(&[*byte]).is_some())
        {
            index = index.wrapping_add(1);
        }
        if index == token_start {
            return Http1Error::InvalidChunkSize.err();
        }
        trailing_whitespace = false;
        while line
            .get(index)
            .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
        {
            trailing_whitespace = true;
            index = index.wrapping_add(1);
        }
        if line.get(index) == Some(&b'=') {
            index = index.wrapping_add(1);
            while line
                .get(index)
                .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
            {
                index = index.wrapping_add(1);
            }
            if line.get(index) == Some(&b'\"') {
                index = index.wrapping_add(1);
                let mut closed = false;
                while let Some(&byte) = line.get(index) {
                    index = index.wrapping_add(1);
                    match byte {
                        b'\"' => {
                            closed = true;
                            break;
                        },
                        b'\\' => {
                            let Some(&escaped) = line.get(index) else {
                                return Http1Error::InvalidChunkSize.err();
                            };
                            if escaped.is_ascii_control() || escaped == 0x7F {
                                return Http1Error::InvalidChunkSize.err();
                            }
                            index = index.wrapping_add(1);
                        },
                        byte if byte.is_ascii_control() || byte == 0x7F => {
                            return Http1Error::InvalidChunkSize.err();
                        },
                        _ => {},
                    }
                }
                if !closed {
                    return Http1Error::InvalidChunkSize.err();
                }
            } else {
                let value_start = index;
                while line
                    .get(index)
                    .is_some_and(|byte| request_header_name_needs_lowercase(&[*byte]).is_some())
                {
                    index = index.wrapping_add(1);
                }
                if index == value_start {
                    return Http1Error::InvalidChunkSize.err();
                }
            }
            trailing_whitespace = false;
            while line
                .get(index)
                .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
            {
                trailing_whitespace = true;
                index = index.wrapping_add(1);
            }
        }
        if index == line.len() {
            return if trailing_whitespace {
                Http1Error::InvalidChunkSize.err()
            } else {
                Ok(value)
            };
        }
    }
}

fn parse_http2_settings(value: &[u8]) -> Result<PeerSettings, H2CornError> {
    let decoded = crate::base64::decode_url(value.trim_ascii())
        .map_err(|()| Http1Error::InvalidHttp2SettingsBase64UrlPayload.into_error())?;
    if !decoded.len().is_multiple_of(SETTING_ENTRY_LEN) {
        return Http1Error::InvalidHttp2SettingsPayloadLength.err();
    }
    parse_settings_payload(decoded.as_ref())
}

async fn read_more<R>(
    reader: &mut R,
    buffer: &mut BytesMut,
    timeout_duration: Option<Duration>,
    timeout_error: Http1Error,
    max_bytes: Option<usize>,
) -> Result<bool, H2CornError>
where
    R: AsyncRead + Unpin,
{
    let read = if let Some(max_bytes) = max_bytes {
        debug_assert!(max_bytes != 0);
        buffer.reserve(max_bytes.min(CHUNK_BUFFER_SIZE));
        if let Some(timeout_duration) = timeout_duration {
            let mut limited = reader.take(max_bytes as u64);
            timeout(timeout_duration, limited.read_buf(buffer))
                .await
                .map_err(|_| timeout_error.into_error())??
        } else {
            reader.take(max_bytes as u64).read_buf(buffer).await?
        }
    } else if let Some(timeout_duration) = timeout_duration {
        buffer.reserve(CHUNK_BUFFER_SIZE);
        timeout(timeout_duration, reader.read_buf(buffer))
            .await
            .map_err(|_| timeout_error.into_error())??
    } else {
        buffer.reserve(CHUNK_BUFFER_SIZE);
        reader.read_buf(buffer).await?
    };
    Ok(read != 0)
}

#[cfg(test)]
mod tests;
