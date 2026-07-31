use std::num::{NonZeroU64, NonZeroUsize};
use std::str;
use std::sync::LazyLock;

use bytes::{Buf as _, Bytes, BytesMut};
use http::{Method, Uri};
use memchr::{memchr, memchr2, memmem};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, BufWriter};
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};

use super::http::write_empty_response;
use super::{ConnectionPersistence, ParsedRequest, RequestBodyKind, RequestRoute, UpgradeRequest};
use crate::ascii;
use crate::async_util::send_if_open;
use crate::config::ServerConfig;
use crate::error::{ErrorExt as _, H2CornError, Http1Error};
use crate::h2_frame::{PeerSettings, SETTING_ENTRY_LEN, parse_settings_payload};
use crate::http::body::{RequestBodyFinish, RequestBodyProgress, RequestBodyState};
use crate::http::header::{
    ConnectionHeaderTokens, header_contains_token, header_is_single_token,
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
        if let Some(offset) = self.finder.find(&buffer[start..]) {
            return Some(start + offset);
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
    body_kind: RequestBodyKind,
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
            body_kind: RequestBodyKind::None,
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
            self.header_field_count += 1;
            self.header_field_count > limit
        } else {
            false
        };
        too_many_header_fields || limit_request_field_size.is_some_and(|limit| line.len() > limit)
    }

    fn push_header(&mut self, line: &[u8]) -> Result<(), H2CornError> {
        let Some(colon) = memchr(b':', line) else {
            return Http1Error::MalformedHeaderLine.err();
        };
        let name = &line[..colon];
        let raw_value = &line[colon + 1..];
        let value = trim_ows(raw_value);
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

        self.header_meta.observe_known_header_slice(
            known_name,
            value,
            self.headers.len() - 1,
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
                self.host_header_index = Some(self.headers.len() - 1);
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
                if self.body_kind == RequestBodyKind::Chunked {
                    return Http1Error::InvalidContentLength.err();
                }
                let parsed =
                    parse_content_length_header(value).ok_or(Http1Error::InvalidContentLength)?;
                if self
                    .header_meta
                    .content_length()
                    .is_some_and(|existing| existing != parsed)
                {
                    return Http1Error::InvalidContentLength.err();
                }
                self.header_meta.set_content_length(Some(parsed));
                self.body_kind = NonZeroU64::new(parsed)
                    .map_or(RequestBodyKind::None, RequestBodyKind::ContentLength);
            },
            KnownRequestHeaderName::TransferEncoding => {
                if self.body_kind == RequestBodyKind::Chunked
                    || self.header_meta.content_length().is_some()
                    || !header_is_single_token(value, b"chunked")
                {
                    return Http1Error::MalformedHeaderLine.err();
                }
                self.body_kind = RequestBodyKind::Chunked;
                self.header_meta.set_content_length(None);
            },
            KnownRequestHeaderName::Expect => {
                self.expect_continue = value.eq_ignore_ascii_case(b"100-continue");
            },
            KnownRequestHeaderName::Http2Settings => {
                let settings = parse_http2_settings(value)?;
                if self.http2_settings.replace(settings).is_some() {
                    return Http1Error::MalformedHeaderLine.err();
                }
            },
            _ => {},
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
            if limit_request_head_size.is_some_and(|limit| end + HEADER_TERMINATOR.len() > limit) {
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

    let mut head = buffer.split_to(header_len + HEADER_TERMINATOR.len());
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
        [b'H', b'T', b'T', b'P', b'/', b'1', b'.', ..] => {
            return Ok(HeadParseOutcome::Reject(
                status_code::HTTP_VERSION_NOT_SUPPORTED,
            ));
        },
        _ => {
            return Ok(HeadParseOutcome::Reject(status_code::BAD_REQUEST));
        },
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
        if header_state.push_header(line).is_err() {
            return HeadParseOutcome::Reject(status_code::BAD_REQUEST);
        }
    }
    HeadParseOutcome::Parsed(header_state)
}

#[expect(
    clippy::too_many_lines,
    reason = "the complete HTTP/1 request transition keeps parsing and route selection together"
)]
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
        body_kind,
        http2_settings,
        header_meta,
        expect_continue,
        ..
    } = header_state;
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
    let Some(request_line) = (match lines.next_line() {
        HeadParseOutcome::Parsed(line) => line,
        HeadParseOutcome::Reject(status) => {
            write_empty_response(writer, config, status, true).await?;
            return Ok(None);
        },
    }) else {
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
        remaining -= chunk_len;
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
            remaining -= chunk_len;
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
    let size = parse_chunk_size(&buffer[..line_end])?;
    buffer.advance(line_end + LINE_TERMINATOR.len());
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
    if &buffer[..2] != b"\r\n" {
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
    let Some(colon) = memchr(b':', line) else {
        return Http1Error::MalformedHeaderLine.err();
    };
    let name = &line[..colon];
    if request_header_name_needs_lowercase(name).is_none() {
        return Http1Error::InvalidHeaderName.err();
    }
    if trailer_field_name_is_forbidden(name) {
        return Http1Error::MalformedHeaderLine.err();
    }
    if !header_value_is_valid(&line[colon + 1..]) {
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
        .map_or(start, |index| index + 1);
    &value[start..end]
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
    let mut section_bytes = 0;
    loop {
        let Some(end) = line_search.find(buffer) else {
            let buffered_bytes = section_bytes + buffer.len();
            if buffered_bytes >= MAX_TRAILER_SECTION_BYTES {
                return Http1Error::MalformedHeaderLine.err();
            }
            if !read_more(
                reader,
                buffer,
                timeout_duration,
                Http1Error::RequestBodyTimedOut,
                Some(MAX_TRAILER_SECTION_BYTES - buffered_bytes),
            )
            .await?
            {
                return Http1Error::ChunkedTrailersClosed.err();
            }
            continue;
        };
        let line_len = end + LINE_TERMINATOR.len();
        section_bytes = section_bytes.saturating_add(line_len);
        if section_bytes > MAX_TRAILER_SECTION_BYTES {
            return Http1Error::MalformedHeaderLine.err();
        }
        if end == 0 {
            buffer.advance(line_len);
            return Ok(());
        }
        fields += 1;
        if limits.max_fields.is_some_and(|limit| fields > limit) {
            return Http1Error::TooManyTrailerFields.err();
        }
        validate_trailer_line(&buffer[..end], limits)?;
        buffer.advance(line_len);
        line_search.reset();
    }
}

fn parse_request_line(line: &[u8]) -> Result<(Method, &[u8], &[u8]), H2CornError> {
    let Some(first_space) = memchr(b' ', line) else {
        return Http1Error::InvalidRequestLine.err();
    };
    let Some(second_space) =
        memchr(b' ', &line[first_space + 1..]).map(|offset| first_space + 1 + offset)
    else {
        return Http1Error::InvalidRequestLine.err();
    };
    let method =
        parse_request_method(&line[..first_space]).map_err(|_| Http1Error::InvalidRequestMethod)?;
    Ok((
        method,
        &line[first_space + 1..second_space],
        &line[second_space + 1..],
    ))
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
    let Some(scheme_end) = memchr(b':', target) else {
        return false;
    };
    let Some(after_authority) = target[scheme_end + 1..].strip_prefix(b"//") else {
        return false;
    };
    let authority_end = after_authority
        .iter()
        .position(|byte| matches!(*byte, b'/' | b'?' | b'#'))
        .unwrap_or(after_authority.len());
    let (authority, path_and_query) = after_authority.split_at(authority_end);

    !target.contains(&b'#')
        && request_scheme_is_valid(&target[..scheme_end])
        && request_authority_is_valid(authority)
        && (path_and_query.is_empty() || request_path_is_valid(method, path_and_query))
}

#[expect(
    clippy::too_many_lines,
    reason = "the grammar stays auditable when token, quoted-string, and BWS states share one cursor"
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
            .checked_shl(4)
            .and_then(|value| value.checked_add(usize::from(digit)))
            .ok_or(Http1Error::InvalidChunkSize)?;
        index += 1;
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
        index += 1;
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
        index += 1;
        while line
            .get(index)
            .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
        {
            index += 1;
        }
        let token_start = index;
        while line
            .get(index)
            .is_some_and(|byte| request_header_name_needs_lowercase(&[*byte]).is_some())
        {
            index += 1;
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
            index += 1;
        }
        if line.get(index) == Some(&b'=') {
            index += 1;
            while line
                .get(index)
                .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
            {
                index += 1;
            }
            if line.get(index) == Some(&b'\"') {
                index += 1;
                let mut closed = false;
                while let Some(&byte) = line.get(index) {
                    index += 1;
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
                            index += 1;
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
                    index += 1;
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
                index += 1;
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
    let decoded = base64url_decode(value.trim_ascii())?;
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

pub(super) fn base64url_decode(src: &[u8]) -> Result<Vec<u8>, H2CornError> {
    let mut out = Vec::with_capacity((src.len() * 3) / 4 + 3);
    let mut block = [0_u8; 4];
    let mut used = 0;
    for &byte in src {
        let value = ascii::BASE64URL_VALUE[usize::from(byte)];
        if value == ascii::INVALID_VALUE {
            return Http1Error::InvalidHttp2SettingsBase64UrlPayload.err();
        }
        block[used] = value;
        used += 1;
        if used == 4 {
            out.push((block[0] << 2) | (block[1] >> 4));
            out.push((block[1] << 4) | (block[2] >> 2));
            out.push((block[2] << 6) | block[3]);
            used = 0;
        }
    }
    match used {
        0 => {},
        2 => out.push((block[0] << 2) | (block[1] >> 4)),
        3 => {
            out.push((block[0] << 2) | (block[1] >> 4));
            out.push((block[1] << 4) | (block[2] >> 2));
        },
        _ => {
            return Http1Error::InvalidHttp2SettingsBase64UrlPayload.err();
        },
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroU64};
    use std::time::Duration;

    use bytes::BytesMut;
    use http::Method;
    use tokio::io::{AsyncWriteExt as _, BufWriter, duplex, sink};
    use tokio::spawn;
    use tokio::sync::mpsc;

    use super::{
        ChunkDeliveryBatch, HeadLineCursor, HeadParseOutcome, MAX_CHUNK_SIZE_LINE_BYTES,
        MAX_TRAILER_SECTION_BYTES, drain_chunked_trailers, parse_chunk_size, parse_http2_settings,
        read_chunk_size_line, read_chunked_body, read_request, read_request_head,
    };
    use crate::config::{
        BindTarget, Http1Config, Http2Config, ProxyConfig, ResponseHeaderConfig, ServerConfig,
        WebSocketConfig,
    };
    use crate::error::{ErrorKind, H2CornError, Http1Error};
    use crate::h1::{ConnectionPersistence, RequestBodyKind, RequestRoute, UpgradeRequest};
    use crate::h2_frame;
    use crate::http::body::RequestBodyState;
    use crate::http::types::RequestAuthority;
    use crate::proxy_protocol::ProxyProtocolMode;
    use crate::runtime::StreamInput;

    fn test_server_config() -> &'static ServerConfig {
        Box::leak(Box::new(ServerConfig {
            binds: Box::new([BindTarget::Tcp {
                host: Box::from("127.0.0.1"),
                port: 8000,
            }]),
            access_log: false,
            root_path: Box::from(""),
            root_path_scope: crate::python::PyOnceLock::new(),
            limit_request_fields: None,
            http1: Http1Config {
                enabled: true,
                ..Default::default()
            },
            http2: Http2Config {
                max_concurrent_streams: NonZeroU32::new(8).expect("non-zero"),
                max_header_list_size: None,
                max_header_block_size: None,
                max_inbound_frame_size: NonZeroU32::new(h2_frame::DEFAULT_MAX_FRAME_SIZE as u32)
                    .expect("default HTTP/2 frame size is non-zero"),
                initial_stream_window_size: NonZeroU32::new(1 << 20).expect("non-zero"),
                initial_connection_window_size: NonZeroU32::new(2 << 20).expect("non-zero"),
                timeout_response_stall: None,
            },
            max_request_body_size: None,
            timeout_graceful_shutdown: Duration::from_secs(30),
            timeout_keep_alive: None,
            timeout_request_header: None,
            timeout_request_body_idle: None,
            limit_concurrency: None,
            limit_connections: None,
            max_requests: None,
            runtime_threads: 2,
            loop_threads: 1,
            websocket: WebSocketConfig::default(),
            proxy: ProxyConfig {
                trust_headers: false,
                trusted_peers: Box::new([]),
                protocol: ProxyProtocolMode::Off,
            },
            tls: None,
            timeout_handshake: Some(Duration::from_secs(5)),
            response_headers: ResponseHeaderConfig::default(),
        }))
    }

    async fn read_test_request(
        request: &[u8],
    ) -> Result<Option<super::ParsedRequest>, H2CornError> {
        let (mut client, mut server) = duplex(512);
        let mut writer = BufWriter::new(sink());
        let request = request.to_vec();
        let write_task = spawn(async move {
            client
                .write_all(&request)
                .await
                .expect("request write succeeds");
            client.shutdown().await.expect("request shutdown succeeds");
        });

        let parsed = read_request(
            &mut server,
            &mut BytesMut::new(),
            &mut writer,
            test_server_config(),
            "http",
            None,
            true,
            false,
        )
        .await;
        write_task.await.expect("writer task finishes");
        parsed
    }

    async fn parse_test_request(request: &[u8]) -> super::ParsedRequest {
        read_test_request(request)
            .await
            .expect("request parse succeeds")
            .expect("request is present")
    }

    #[tokio::test]
    async fn untrusted_proxy_header_reaches_the_asgi_header_list_without_metadata() {
        let parsed = parse_test_request(
            b"GET / HTTP/1.1\r\nHost: example.com\r\nX-Forwarded-For: 198.51.100.9\r\n\r\n",
        )
        .await;

        assert!(
            parsed
                .request
                .headers
                .iter()
                .any(|header| header.value() == b"198.51.100.9")
        );
        assert!(parsed.request.header_meta.proxy_headers().is_none());
    }

    const fn unlimited_fields() -> super::FieldLimits {
        super::FieldLimits {
            max_fields: None,
            max_field_size: None,
        }
    }

    fn base64url_encode(src: &[u8]) -> Vec<u8> {
        const TABLE: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

        let mut out = Vec::with_capacity(src.len().div_ceil(3) * 4);
        let (chunks, remainder) = src.as_chunks::<3>();
        for &[c0, c1, c2] in chunks {
            out.push(TABLE[usize::from(c0 >> 2)]);
            out.push(TABLE[usize::from(((c0 & 0x03) << 4) | (c1 >> 4))]);
            out.push(TABLE[usize::from(((c1 & 0x0F) << 2) | (c2 >> 6))]);
            out.push(TABLE[usize::from(c2 & 0x3F)]);
        }

        match remainder {
            &[a] => {
                out.push(TABLE[usize::from(a >> 2)]);
                out.push(TABLE[usize::from((a & 0x03) << 4)]);
            },
            &[a, b] => {
                out.push(TABLE[usize::from(a >> 2)]);
                out.push(TABLE[usize::from(((a & 0x03) << 4) | (b >> 4))]);
                out.push(TABLE[usize::from((b & 0x0F) << 2)]);
            },
            [] => {},
            _ => unreachable!("remainder from as_chunks::<3>() is at most 2 bytes"),
        }

        out
    }

    #[test]
    fn parse_http2_settings_rejects_zero_max_frame_size() {
        let encoded = base64url_encode(&[0x00, 0x05, 0x00, 0x00, 0x00, 0x00]);
        let err = parse_http2_settings(&encoded).unwrap_err();
        assert_eq!(err.to_string(), "invalid SETTINGS_MAX_FRAME_SIZE value");
    }

    #[tokio::test]
    async fn request_head_with_an_empty_field_section_is_accepted() {
        let (mut client, mut server) = duplex(64);
        client
            .write_all(b"GET / HTTP/1.1\r\n\r\n")
            .await
            .expect("request write succeeds");
        client.shutdown().await.expect("request shutdown succeeds");
        let mut writer = BufWriter::new(sink());
        let head = read_request_head(
            &mut server,
            &mut BytesMut::new(),
            &mut writer,
            test_server_config(),
            None,
            true,
            None,
        )
        .await
        .expect("request head reads")
        .expect("request head is present");

        let mut lines = HeadLineCursor::new(&head);
        let HeadParseOutcome::Parsed(Some(request_line)) = lines.next_line() else {
            panic!("request line is available");
        };
        assert_eq!(request_line, b"GET / HTTP/1.1");
        assert!(matches!(lines.next_line(), HeadParseOutcome::Parsed(None)));
    }

    #[tokio::test]
    async fn read_request_accepts_a_head_split_mid_crlf() {
        let (mut client, mut server) = duplex(1);
        let write_task = spawn(async move {
            for chunk in [
                b"GET / HTTP/1.1\r".as_slice(),
                b"\nHost: example.com\r".as_slice(),
                b"\n\r".as_slice(),
                b"\n".as_slice(),
            ] {
                client
                    .write_all(chunk)
                    .await
                    .expect("request write succeeds");
            }
            client.shutdown().await.expect("request shutdown succeeds");
        });

        let mut writer = BufWriter::new(sink());
        let parsed = read_request(
            &mut server,
            &mut BytesMut::new(),
            &mut writer,
            test_server_config(),
            "http",
            None,
            true,
            false,
        )
        .await;
        write_task.await.expect("writer task finishes");

        assert!(parsed.expect("request parse succeeds").is_some());
    }

    #[tokio::test]
    async fn request_head_timeouts_distinguish_idle_keep_alive_from_partial_headers() {
        let (_client, mut server) = duplex(64);
        let mut writer = BufWriter::new(sink());
        let mut buffer = BytesMut::new();
        let Err(idle) = read_request(
            &mut server,
            &mut buffer,
            &mut writer,
            test_server_config(),
            "http",
            Some(Duration::ZERO),
            false,
            false,
        )
        .await
        else {
            panic!("an idle keep-alive connection times out");
        };
        assert_eq!(
            idle.to_string(),
            "keep-alive connection did not receive the next request within timeout_keep_alive"
        );

        let (_client, mut server) = duplex(64);
        let mut writer = BufWriter::new(sink());
        let mut buffer = BytesMut::from(&b"GET"[..]);
        let Err(partial) = read_request(
            &mut server,
            &mut buffer,
            &mut writer,
            test_server_config(),
            "http",
            Some(Duration::ZERO),
            false,
            false,
        )
        .await
        else {
            panic!("a partial request head times out");
        };
        assert_eq!(
            partial.to_string(),
            "HTTP/1.1 request head did not arrive within timeout_request_header"
        );
    }

    #[tokio::test]
    async fn read_request_accepts_extension_method() {
        let parsed =
            parse_test_request(b"PROPFIND /items HTTP/1.1\r\nHost: example.com\r\n\r\n").await;

        assert_eq!(
            parsed.request.method,
            Method::from_bytes(b"PROPFIND").expect("extension method is valid")
        );
    }

    #[tokio::test]
    async fn read_request_admits_each_target_form_only_for_its_methods() {
        // RFC 9112 §3.2: authority-form belongs to CONNECT, asterisk-form to
        // OPTIONS, and the other two to neither.
        for request in [
            b"CONNECT /tunnel HTTP/1.1\r\nHost: example.com\r\n\r\n".as_slice(),
            b"CONNECT * HTTP/1.1\r\nHost: example.com\r\n\r\n",
            b"CONNECT http://example.com/p HTTP/1.1\r\nHost: example.com\r\n\r\n",
            b"GET * HTTP/1.1\r\nHost: example.com\r\n\r\n",
            b"GET example.com:443 HTTP/1.1\r\nHost: example.com\r\n\r\n",
            b"GET p/q HTTP/1.1\r\nHost: example.com\r\n\r\n",
        ] {
            assert!(
                read_test_request(request)
                    .await
                    .expect("a malformed target is answered, not fatal")
                    .is_none(),
                "expected rejection of {:?}",
                str::from_utf8(request).expect("test input is UTF-8")
            );
        }

        assert!(
            parse_test_request(b"OPTIONS * HTTP/1.1\r\nHost: example.com\r\n\r\n")
                .await
                .request
                .target
                .path_and_query()
                .is_some_and(|target| target.as_str() == "*")
        );
    }

    #[tokio::test]
    async fn read_request_parses_connect_as_a_tunnel_to_its_authority() {
        // Without an authority form the target parsed as an ordinary `/`, and
        // the tunnel request reached the application instead of a 501.
        let parsed = parse_test_request(
            b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n",
        )
        .await;

        assert_eq!(parsed.request.method, Method::CONNECT);
        assert!(parsed.request.is_connect());
        assert_eq!(
            parsed
                .request
                .target
                .authority()
                .map(RequestAuthority::as_str),
            Some("example.com:443")
        );
    }

    #[tokio::test]
    async fn read_request_maps_portless_connect_to_a_bad_request_response() {
        let parsed =
            read_test_request(b"CONNECT example.com HTTP/1.1\r\nHost: example.com\r\n\r\n")
                .await
                .expect("an invalid tunnel authority receives a response");

        assert!(parsed.is_none());
    }

    #[tokio::test]
    async fn read_request_keeps_content_length_body_shape() {
        let parsed = parse_test_request(
            b"POST /upload HTTP/1.1\r\nHost: example.com\r\nContent-Length: 7\r\n\r\npayload",
        )
        .await;

        assert!(matches!(
            parsed.route,
            RequestRoute::Http(RequestBodyKind::ContentLength(value)) if value.get() == 7
        ));
        assert_eq!(parsed.persistence, ConnectionPersistence::KeepAlive);
    }

    #[tokio::test]
    async fn read_request_keeps_chunked_body_shape() {
        let parsed = parse_test_request(
            b"POST /upload HTTP/1.1\r\nHost: example.com\r\nTransfer-Encoding: chunked\r\n\r\n",
        )
        .await;

        assert!(matches!(
            parsed.route,
            RequestRoute::Http(RequestBodyKind::Chunked)
        ));
        assert_eq!(parsed.persistence, ConnectionPersistence::KeepAlive);
    }

    #[tokio::test]
    async fn read_request_keeps_connection_close_disposition() {
        let parsed =
            parse_test_request(b"GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n")
                .await;

        assert!(matches!(
            parsed.route,
            RequestRoute::Http(RequestBodyKind::None)
        ));
        assert_eq!(parsed.persistence, ConnectionPersistence::Close);
    }

    #[tokio::test]
    async fn read_request_classifies_valid_websocket_upgrade() {
        let parsed = parse_test_request(
            concat!(
                "GET /ws HTTP/1.1\r\n",
                "Host: example.com\r\n",
                "Connection: Upgrade\r\n",
                "Upgrade: websocket\r\n",
                "Sec-WebSocket-Version: 13\r\n",
                "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
                "\r\n",
            )
            .as_bytes(),
        )
        .await;

        assert!(matches!(
            parsed.route,
            RequestRoute::Upgrade(UpgradeRequest::WebSocket { .. })
        ));
    }

    #[tokio::test]
    async fn read_request_classifies_unsupported_websocket_version() {
        let parsed = parse_test_request(
            concat!(
                "GET /ws HTTP/1.1\r\n",
                "Host: example.com\r\n",
                "Connection: Upgrade\r\n",
                "Upgrade: websocket\r\n",
                "Sec-WebSocket-Version: 12\r\n",
                "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
                "\r\n",
            )
            .as_bytes(),
        )
        .await;

        assert!(matches!(
            parsed.route,
            RequestRoute::Upgrade(UpgradeRequest::WebSocketUnsupportedVersion)
        ));
    }

    #[tokio::test]
    async fn read_request_classifies_bad_websocket_handshake() {
        let parsed = parse_test_request(
            concat!(
                "POST /ws HTTP/1.1\r\n",
                "Host: example.com\r\n",
                "Connection: Upgrade\r\n",
                "Upgrade: websocket\r\n",
                "Sec-WebSocket-Version: 13\r\n",
                "\r\n",
            )
            .as_bytes(),
        )
        .await;

        assert!(matches!(
            parsed.route,
            RequestRoute::Upgrade(UpgradeRequest::WebSocketBadRequest)
        ));
    }

    #[tokio::test]
    async fn read_request_rejects_whitespace_before_header_colon() {
        let parsed = read_test_request(b"GET / HTTP/1.1\r\nHost : example.com\r\n\r\n")
            .await
            .expect("request parse succeeds");

        assert!(parsed.is_none());
    }

    #[tokio::test]
    async fn read_request_rejects_raw_header_value_controls_after_ows_trimming() {
        let parsed =
            read_test_request(b"GET / HTTP/1.1\r\nHost: example.com\r\nX-Demo: \x0cvalue\r\n\r\n")
                .await
                .expect("malformed request receives a response");

        assert!(parsed.is_none());
    }

    #[tokio::test]
    async fn read_request_rejects_a_second_cr_before_the_line_terminator() {
        let parsed =
            read_test_request(b"GET / HTTP/1.1\r\nHost: example.com\r\nX-Demo: value\r\r\n\r\n")
                .await
                .expect("malformed request receives a response");

        assert!(parsed.is_none());
    }

    #[tokio::test]
    async fn read_request_rejects_duplicate_host_even_when_identical() {
        let parsed = read_test_request(
            concat!(
                "GET / HTTP/1.1\r\n",
                "Host: example.com\r\n",
                "Host: example.com\r\n",
                "\r\n",
            )
            .as_bytes(),
        )
        .await
        .expect("request parse succeeds");

        assert!(parsed.is_none());
    }

    #[tokio::test]
    async fn read_request_rejects_duplicate_websocket_key() {
        let parsed = parse_test_request(
            concat!(
                "GET /ws HTTP/1.1\r\n",
                "Host: example.com\r\n",
                "Connection: Upgrade\r\n",
                "Upgrade: websocket\r\n",
                "Sec-WebSocket-Version: 13\r\n",
                "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
                "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
                "\r\n",
            )
            .as_bytes(),
        )
        .await;

        assert!(matches!(
            parsed.route,
            RequestRoute::Upgrade(UpgradeRequest::WebSocketBadRequest)
        ));
    }

    #[tokio::test]
    async fn read_request_classifies_h2c_upgrade() {
        let parsed = parse_test_request(
            concat!(
                "GET / HTTP/1.1\r\n",
                "Host: example.com\r\n",
                "Connection: Upgrade, HTTP2-Settings\r\n",
                "Upgrade: h2c\r\n",
                "HTTP2-Settings:\r\n",
                "\r\n",
            )
            .as_bytes(),
        )
        .await;

        assert!(matches!(
            parsed.route,
            RequestRoute::Upgrade(UpgradeRequest::H2c { .. })
        ));
    }

    #[tokio::test]
    async fn read_request_classifies_h2c_bodies_and_rejects_duplicate_settings() {
        let content_length = parse_test_request(
            concat!(
                "POST / HTTP/1.1\r\n",
                "Host: example.com\r\n",
                "Connection: Upgrade, HTTP2-Settings\r\n",
                "Upgrade: h2c\r\n",
                "HTTP2-Settings:\r\n",
                "Content-Length: 1\r\n",
                "\r\n",
                "x",
            )
            .as_bytes(),
        )
        .await;
        assert!(matches!(
            content_length.route,
            RequestRoute::Http(RequestBodyKind::ContentLength(_))
        ));

        let chunked = parse_test_request(
            concat!(
                "POST / HTTP/1.1\r\n",
                "Host: example.com\r\n",
                "Connection: Upgrade, HTTP2-Settings\r\n",
                "Upgrade: h2c\r\n",
                "HTTP2-Settings:\r\n",
                "Transfer-Encoding: chunked\r\n",
                "\r\n",
            )
            .as_bytes(),
        )
        .await;
        assert!(matches!(
            chunked.route,
            RequestRoute::Http(RequestBodyKind::Chunked)
        ));

        let no_body = parse_test_request(
            concat!(
                "GET / HTTP/1.1\r\n",
                "Host: example.com\r\n",
                "Connection: Upgrade, HTTP2-Settings\r\n",
                "Upgrade: h2c\r\n",
                "HTTP2-Settings:\r\n",
                "Content-Length: 0\r\n",
                "\r\n",
            )
            .as_bytes(),
        )
        .await;
        assert!(matches!(
            no_body.route,
            RequestRoute::Upgrade(UpgradeRequest::H2c { .. })
        ));

        for settings in ["", "AAEAAAAB"] {
            let request = format!(
                concat!(
                    "GET / HTTP/1.1\r\n",
                    "Host: example.com\r\n",
                    "Connection: Upgrade, HTTP2-Settings\r\n",
                    "Upgrade: h2c\r\n",
                    "HTTP2-Settings:\r\n",
                    "HTTP2-Settings: {settings}\r\n",
                    "\r\n",
                ),
                settings = settings,
            );
            assert!(
                read_test_request(request.as_bytes())
                    .await
                    .expect("malformed upgrade is a client error")
                    .is_none(),
                "a duplicate HTTP2-Settings header must be rejected: {settings:?}"
            );
        }
    }

    #[tokio::test]
    async fn read_chunked_body_accepts_empty_trailer_block() {
        let (mut client, mut server) = duplex(64);
        let writer = spawn(async move {
            client
                .write_all(b"3\r\nabc\r\n0\r\n\r\n")
                .await
                .expect("duplex write succeeds");
        });
        let mut buffer = BytesMut::new();
        let (tx, mut rx) = mpsc::channel(4);

        let mut body = RequestBodyState::new(None, None, None);
        read_chunked_body(
            &mut server,
            &mut buffer,
            &tx,
            &mut body,
            None,
            unlimited_fields(),
        )
        .await
        .expect("empty trailer block is accepted");
        writer.await.expect("writer task finishes");

        match rx.try_recv().expect("body chunk is forwarded") {
            StreamInput::Data { body, credit: None } => assert_eq!(body.as_ref(), b"abc"),
            _ => panic!("expected body data event"),
        }
        rx.try_recv().unwrap_err();
    }

    #[tokio::test]
    async fn chunk_delivery_batch_allocates_only_for_small_decoded_fragments() {
        let (tx, mut rx) = mpsc::channel(4);
        let mut body = RequestBodyState::new(None, None, None);

        let empty = ChunkDeliveryBatch::new();
        assert_eq!(
            empty.pending.capacity(),
            0,
            "an empty body has no batch allocation"
        );
        assert!(
            rx.try_recv().is_err(),
            "an empty body makes no delivery call"
        );

        let mut small = ChunkDeliveryBatch::new();
        let mut small_input = BytesMut::from(&[b'a'; 1024][..]);
        small
            .push(&mut small_input, 1024, &tx, &mut body)
            .await
            .expect("small fragment is accepted");
        assert!(
            small.pending.capacity() >= super::CHUNK_DELIVERY_BATCH_BYTES,
            "the first small copy reserves one full batch"
        );
        small.flush(&tx, &mut body).await;
        assert!(matches!(rx.try_recv(), Ok(StreamInput::Data { body, .. }) if body.len() == 1024));
        assert!(
            rx.try_recv().is_err(),
            "one small fragment makes one delivery call"
        );

        let mut direct = ChunkDeliveryBatch::new();
        let mut direct_input =
            BytesMut::from(vec![b'b'; super::CHUNK_DELIVERY_BATCH_BYTES].as_slice());
        direct
            .push(
                &mut direct_input,
                super::CHUNK_DELIVERY_BATCH_BYTES,
                &tx,
                &mut body,
            )
            .await
            .expect("full batch is accepted");
        assert_eq!(
            direct.pending.capacity(),
            0,
            "a contiguous full batch stays zero-copy"
        );
        assert!(
            matches!(rx.try_recv(), Ok(StreamInput::Data { body, .. }) if body.len() == super::CHUNK_DELIVERY_BATCH_BYTES)
        );
        assert!(
            rx.try_recv().is_err(),
            "one full fragment makes one delivery call"
        );

        let mut tiny = ChunkDeliveryBatch::new();
        for _ in 0..super::CHUNK_DELIVERY_BATCH_BYTES - 1 {
            let mut input = BytesMut::from(&b"c"[..]);
            tiny.push(&mut input, 1, &tx, &mut body)
                .await
                .expect("one-byte fragment is accepted");
        }
        assert!(
            tiny.pending.capacity() >= super::CHUNK_DELIVERY_BATCH_BYTES,
            "many tiny fragments reuse the one full batch allocation"
        );
        let mut input = BytesMut::from(&b"c"[..]);
        tiny.push(&mut input, 1, &tx, &mut body)
            .await
            .expect("the final one-byte fragment is accepted");
        assert!(
            matches!(rx.try_recv(), Ok(StreamInput::Data { body, .. }) if body.len() == super::CHUNK_DELIVERY_BATCH_BYTES)
        );
        assert!(
            rx.try_recv().is_err(),
            "many tiny fragments retain one delivery call"
        );
    }

    #[tokio::test]
    async fn read_chunked_body_accepts_extensions_and_ignores_trailers() {
        let (mut client, mut server) = duplex(128);
        let writer = spawn(async move {
            client
                .write_all(b"3;foo=bar\r\nabc\r\n4;baz=qux\r\ndefg\r\n0\r\nX-Test: yes\r\n\r\n")
                .await
                .expect("duplex write succeeds");
        });
        let mut buffer = BytesMut::new();
        let (tx, mut rx) = mpsc::channel(8);

        let mut body = RequestBodyState::new(None, None, None);
        read_chunked_body(
            &mut server,
            &mut buffer,
            &tx,
            &mut body,
            None,
            unlimited_fields(),
        )
        .await
        .expect("chunk extensions and trailers are accepted");
        writer.await.expect("writer task finishes");

        let StreamInput::Data {
            body: chunk,
            credit: None,
        } = rx.try_recv().expect("coalesced body chunk exists")
        else {
            panic!("expected coalesced body chunk");
        };
        assert_eq!(chunk.as_ref(), b"abcdefg");
        rx.try_recv().unwrap_err();
    }

    #[tokio::test]
    async fn read_chunked_body_batches_tiny_wire_chunks_by_decoded_size() {
        const CHUNKS: usize = 30_000;

        let mut request = Vec::with_capacity(CHUNKS * 6 + 5);
        for _ in 0..CHUNKS {
            request.extend_from_slice(b"1\r\na\r\n");
        }
        request.extend_from_slice(b"0\r\n\r\n");

        let (mut client, mut server) = duplex(1024);
        let writer = spawn(async move {
            client
                .write_all(&request)
                .await
                .expect("duplex write succeeds");
        });
        let mut buffer = BytesMut::new();
        let (tx, mut rx) = mpsc::channel(2);
        let mut body = RequestBodyState::new(None, None, None);

        read_chunked_body(
            &mut server,
            &mut buffer,
            &tx,
            &mut body,
            None,
            unlimited_fields(),
        )
        .await
        .expect("tiny chunks are accepted");
        writer.await.expect("writer task finishes");

        let StreamInput::Data {
            body: chunk,
            credit: None,
        } = rx.try_recv().expect("one batched body event exists")
        else {
            panic!("expected body data event");
        };
        assert_eq!(chunk.as_ref(), vec![b'a'; CHUNKS]);
        rx.try_recv().unwrap_err();
    }

    #[tokio::test]
    async fn read_chunk_size_line_rejects_overlong_buffered_line() {
        let (_client, mut server) = duplex(1);
        let mut buffer =
            BytesMut::from(format!("{}\r\n", "f".repeat(MAX_CHUNK_SIZE_LINE_BYTES + 1)).as_bytes());

        let err = read_chunk_size_line(&mut server, &mut buffer, None)
            .await
            .expect_err("overlong buffered chunk size line is rejected");

        assert!(matches!(
            err.kind(),
            ErrorKind::Http1(Http1Error::InvalidChunkSize)
        ));
    }

    #[tokio::test]
    async fn read_chunk_size_line_accepts_the_exact_wire_boundary() {
        let line = format!("1;{}\r\n", "a".repeat(MAX_CHUNK_SIZE_LINE_BYTES - 2));
        assert_eq!(line.len(), MAX_CHUNK_SIZE_LINE_BYTES + 2);

        let (_client, mut server) = duplex(1);
        let mut buffered = BytesMut::from(line.as_bytes());
        assert_eq!(
            read_chunk_size_line(&mut server, &mut buffered, None)
                .await
                .expect("the exact limit is accepted"),
            1
        );

        let (mut client, mut server) = duplex(1);
        let writer = spawn(async move {
            for byte in line.bytes() {
                client
                    .write_all(&[byte])
                    .await
                    .expect("byte-split line writes");
            }
        });
        let mut split = BytesMut::new();
        assert_eq!(
            read_chunk_size_line(&mut server, &mut split, None)
                .await
                .expect("the byte-split exact limit is accepted"),
            1
        );
        writer.await.expect("writer task completes");
    }

    #[tokio::test]
    async fn drain_chunked_trailers_rejects_oversized_total_section() {
        let (_client, mut server) = duplex(1);
        let mut payload = Vec::new();
        while payload.len() <= MAX_TRAILER_SECTION_BYTES {
            payload.extend_from_slice(b"X-Test: abcdefghijklmnop\r\n");
        }
        payload.extend_from_slice(b"\r\n");
        let mut buffer = BytesMut::from(payload.as_slice());

        let err = drain_chunked_trailers(&mut server, &mut buffer, None, unlimited_fields())
            .await
            .expect_err("oversized trailer section is rejected");

        assert!(matches!(
            err.kind(),
            ErrorKind::Http1(Http1Error::MalformedHeaderLine)
        ));
    }

    #[tokio::test]
    async fn read_chunked_body_rejects_announced_chunk_over_limit_before_consuming_data() {
        let (mut client, mut server) = duplex(64);
        let writer = spawn(async move {
            client
                .write_all(b"5\r\nhello\r\n")
                .await
                .expect("duplex write succeeds");
        });
        let mut buffer = BytesMut::new();
        let (tx, mut rx) = mpsc::channel(4);

        let mut body = RequestBodyState::new(None, None, NonZeroU64::new(4));
        let err = read_chunked_body(
            &mut server,
            &mut buffer,
            &tx,
            &mut body,
            None,
            unlimited_fields(),
        )
        .await
        .expect_err("announced chunk beyond configured limit is rejected");
        writer.await.expect("writer task finishes");

        assert!(matches!(
            err.kind(),
            ErrorKind::Http1(Http1Error::RequestBodyLimitExceeded)
        ));
        rx.try_recv().unwrap_err();
        assert_eq!(&buffer[..], b"hello\r\n");
    }

    #[test]
    fn parse_chunk_size_accepts_hex_extensions_and_rejects_whitespace() {
        assert_eq!(parse_chunk_size(b"1a").unwrap(), 0x1A);
        assert_eq!(parse_chunk_size(b"1A;foo=bar").unwrap(), 0x1A);
        parse_chunk_size(b" \t1A").unwrap_err();
    }

    #[test]
    fn parse_chunk_size_enforces_extension_grammar() {
        for line in [
            b"1;foo; bar = token;baz=\"quoted \\\"value\\\"\"".as_slice(),
            b"F \t; X\t=\t\"quoted\"",
        ] {
            assert!(parse_chunk_size(line).is_ok(), "{line:?} is legal");
        }
        for line in [
            b" 1".as_slice(),
            b"0x1",
            b"1;",
            b"1;=value",
            b"1;\0",
            b"1;foo=\"unterminated",
            b"1;foo ",
            b"1;foo=bar ",
        ] {
            assert!(parse_chunk_size(line).is_err(), "{line:?} is illegal");
        }
    }

    #[test]
    fn parse_chunk_size_rejects_empty_and_invalid_values() {
        assert!(matches!(
            parse_chunk_size(b"").unwrap_err().kind(),
            ErrorKind::Http1(Http1Error::InvalidChunkSize)
        ));
        assert!(matches!(
            parse_chunk_size(b";foo=bar").unwrap_err().kind(),
            ErrorKind::Http1(Http1Error::InvalidChunkSize)
        ));
        assert!(matches!(
            parse_chunk_size(b"1 g").unwrap_err().kind(),
            ErrorKind::Http1(Http1Error::InvalidChunkSize)
        ));
    }
}
