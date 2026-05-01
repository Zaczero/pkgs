use std::{
    iter,
    num::{NonZeroU64, NonZeroUsize},
    str,
    sync::LazyLock,
};

use bytes::{Buf, Bytes, BytesMut};
use http::{Method, Uri};
use memchr::{memchr, memchr_iter, memmem};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};

use crate::async_util::send_if_open;
use crate::config::ServerConfig;
use crate::error::{ErrorExt, H2CornError, Http1Error};
use crate::frame::{PeerSettings, SETTING_ENTRY_LEN, parse_settings_payload};
use crate::hpack::BytesStr;
use crate::http::header_meta::RequestHeaderMeta;
use crate::http::types::{
    HttpVersion, KnownRequestHeaderName, RequestHead, RequestHeaderName, RequestHeaderValue,
    RequestHeaders, RequestTarget as DomainRequestTarget, parse_request_method, status_code,
};
use crate::http::{
    body::{RequestBodyFinish, RequestBodyProgress, RequestBodyState},
    header::{
        header_contains_token, header_is_single_token, parse_connection_header_tokens,
        parse_content_length_header,
    },
};
use crate::runtime::StreamInput;

use super::http::write_empty_response;
use super::{ConnectionPersistence, ParsedRequest, RequestBodyKind, UpgradeRequest};

const HEADER_TERMINATOR: &[u8; 4] = b"\r\n\r\n";
const LINE_TERMINATOR: &[u8; 2] = b"\r\n";
const CHUNK_BUFFER_SIZE: usize = 8192;
const MAX_CHUNK_SIZE_LINE_BYTES: usize = 16 * 1024;
const MAX_TRAILER_SECTION_BYTES: usize = 64 * 1024;
const INVALID_HEX_DIGIT: u8 = 0xFF;
const HEX_DIGIT_TABLE: [u8; 256] = {
    let mut table = [INVALID_HEX_DIGIT; 256];

    let mut byte = b'0';
    while byte <= b'9' {
        table[byte as usize] = byte - b'0';
        byte += 1;
    }

    let mut byte = b'a';
    while byte <= b'f' {
        table[byte as usize] = byte - b'a' + 10;
        byte += 1;
    }

    let mut byte = b'A';
    while byte <= b'F' {
        table[byte as usize] = byte - b'A' + 10;
        byte += 1;
    }

    table
};
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

fn head_lines(head: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut next_start = 0;
    let mut newlines = memchr_iter(b'\n', head);
    let mut finished = false;
    iter::from_fn(move || {
        if let Some(end) = newlines.next() {
            let line = &head[next_start..end];
            next_start = end + 1;
            return Some(line.strip_suffix(b"\r").unwrap_or(line));
        }
        if finished {
            return None;
        }
        finished = true;
        let line = &head[next_start..];
        Some(line.strip_suffix(b"\r").unwrap_or(line))
    })
}

#[derive(Default)]
struct ConnectionHeaderFlags {
    close: bool,
    upgrade: bool,
    http2_settings: bool,
}

#[derive(Default)]
struct UpgradeHeaderFlags {
    websocket: bool,
    h2c: bool,
}

struct HeaderParseState {
    headers: RequestHeaders,
    host_header_index: Option<usize>,
    connection: ConnectionHeaderFlags,
    upgrade: UpgradeHeaderFlags,
    body_kind: RequestBodyKind,
    expect_continue: bool,
    http2_settings: Option<PeerSettings>,
    header_field_count: usize,
    header_meta: RequestHeaderMeta,
}

enum ParsedRequestTarget<'a> {
    Origin(&'a [u8]),
    Absolute(Uri),
    Asterisk,
}

struct RequestLineParts<'a> {
    method: Method,
    target: &'a [u8],
    websocket_method_supported: bool,
}

impl HeaderParseState {
    fn new() -> Self {
        Self {
            headers: RequestHeaders::with_capacity(16),
            host_header_index: None,
            connection: ConnectionHeaderFlags::default(),
            upgrade: UpgradeHeaderFlags::default(),
            body_kind: RequestBodyKind::None,
            expect_continue: false,
            http2_settings: None,
            header_field_count: 0,
            header_meta: RequestHeaderMeta::default(),
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

    fn push_header(&mut self, head: &Bytes, line: &[u8]) -> Result<(), H2CornError> {
        let Some(colon) = memchr(b':', line) else {
            return Http1Error::MalformedHeaderLine.err();
        };
        let name = &line[..colon];
        let value = line[colon + 1..].trim_ascii();
        let header_name =
            RequestHeaderName::from_h1(head, name).ok_or(Http1Error::InvalidHeaderName)?;
        let header_value =
            RequestHeaderValue::from_h1(head, value).ok_or(Http1Error::InvalidHeaderValue)?;
        let value = header_value.as_bytes();

        let known_name = match &header_name {
            RequestHeaderName::Known(name) => Some(*name),
            RequestHeaderName::Other(_) => None,
        };
        if let Some(known_name) = known_name {
            self.header_meta.observe_known_header(
                known_name,
                header_value.inner(),
                self.headers.len(),
            );
        }

        match known_name {
            Some(KnownRequestHeaderName::Host) => {
                if self.host_header_index.is_some() {
                    return Http1Error::ConflictingAbsoluteFormAuthority.err();
                }
                self.host_header_index = Some(self.headers.len());
            }
            Some(KnownRequestHeaderName::Connection) => {
                let tokens = parse_connection_header_tokens(value);
                self.connection.close |= tokens.close;
                self.connection.upgrade |= tokens.upgrade;
                self.connection.http2_settings |= tokens.http2_settings;
            }
            Some(KnownRequestHeaderName::Upgrade) => {
                self.upgrade.websocket |= value.eq_ignore_ascii_case(b"websocket");
                self.upgrade.h2c |= value.eq_ignore_ascii_case(b"h2c");
            }
            Some(KnownRequestHeaderName::Te) => {
                self.header_meta.accepts_trailers |= header_contains_token(value, b"trailers");
            }
            Some(KnownRequestHeaderName::ContentLength) => {
                if self.body_kind == RequestBodyKind::Chunked {
                    return Http1Error::InvalidContentLength.err();
                }
                let parsed =
                    parse_content_length_header(value).ok_or(Http1Error::InvalidContentLength)?;
                if self
                    .header_meta
                    .content_length
                    .is_some_and(|existing| existing != parsed)
                {
                    return Http1Error::InvalidContentLength.err();
                }
                self.header_meta.content_length = Some(parsed);
                self.body_kind = NonZeroU64::new(parsed)
                    .map_or(RequestBodyKind::None, RequestBodyKind::ContentLength);
            }
            Some(KnownRequestHeaderName::TransferEncoding) => {
                if self.body_kind == RequestBodyKind::Chunked
                    || self.header_meta.content_length.is_some()
                    || !header_is_single_token(value, b"chunked")
                {
                    return Http1Error::MalformedHeaderLine.err();
                }
                self.body_kind = RequestBodyKind::Chunked;
                self.header_meta.content_length = None;
            }
            Some(KnownRequestHeaderName::Expect) => {
                self.expect_continue = value.eq_ignore_ascii_case(b"100-continue");
            }
            Some(KnownRequestHeaderName::Http2Settings) if self.http2_settings.is_none() => {
                self.http2_settings = Some(parse_http2_settings(value)?);
            }
            _ => {}
        }

        self.headers.push((header_name, header_value));
        Ok(())
    }
}

async fn read_request_head<R, W>(
    reader: &mut R,
    buffer: &mut BytesMut,
    writer: &mut BufWriter<W>,
    config: &ServerConfig,
    timeout_duration: Option<Duration>,
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
        if !read_more(
            reader,
            buffer,
            timeout_duration,
            Http1Error::RequestHeadTimedOut,
            read_cap,
        )
        .await?
        {
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

async fn parse_request_line_or_reject<'a, W>(
    request_line: &'a [u8],
    writer: &mut BufWriter<W>,
    config: &ServerConfig,
    limit_request_line: Option<usize>,
) -> Result<Option<RequestLineParts<'a>>, H2CornError>
where
    W: AsyncWrite + Unpin,
{
    if limit_request_line.is_some_and(|limit| request_line.len() > limit) {
        write_empty_response(writer, config, status_code::URI_TOO_LONG, true).await?;
        return Ok(None);
    }
    let (method, target, version) = parse_request_line(request_line)?;
    match version {
        b"HTTP/1.1" => {}
        [b'H', b'T', b'T', b'P', b'/', b'1', b'.', ..] => {
            write_empty_response(writer, config, 505, true).await?;
            return Ok(None);
        }
        _ => {
            write_empty_response(writer, config, status_code::BAD_REQUEST, true).await?;
            return Ok(None);
        }
    }
    let websocket_method_supported = method == Method::GET;
    Ok(Some(RequestLineParts {
        method,
        target,
        websocket_method_supported,
    }))
}

async fn parse_headers_or_reject<W>(
    lines: impl Iterator<Item = &'_ [u8]>,
    head: &Bytes,
    writer: &mut BufWriter<W>,
    config: &ServerConfig,
    limit_request_fields: Option<usize>,
    limit_request_field_size: Option<usize>,
) -> Result<Option<HeaderParseState>, H2CornError>
where
    W: AsyncWrite + Unpin,
{
    let mut header_state = HeaderParseState::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if header_state.header_too_large(line, limit_request_fields, limit_request_field_size) {
            write_empty_response(
                writer,
                config,
                status_code::REQUEST_HEADER_FIELDS_TOO_LARGE,
                true,
            )
            .await?;
            return Ok(None);
        }
        if header_state.push_header(head, line).is_err() {
            write_empty_response(writer, config, status_code::BAD_REQUEST, true).await?;
            return Ok(None);
        }
    }
    Ok(Some(header_state))
}

async fn parsed_request_from_head<W>(
    head: &Bytes,
    line: RequestLineParts<'_>,
    mut header_state: HeaderParseState,
    writer: &mut BufWriter<W>,
    config: &ServerConfig,
    secure: bool,
) -> Result<Option<ParsedRequest>, H2CornError>
where
    W: AsyncWrite + Unpin,
{
    let scheme = BytesStr::from_static(if secure { "https" } else { "http" });
    let request_target = match parse_request_target(
        line.target,
        &mut header_state.headers,
        header_state.host_header_index,
    ) {
        Ok(request_target) => request_target,
        Err(H2CornError::Http1(Http1Error::ConflictingAbsoluteFormAuthority)) => {
            write_empty_response(writer, config, status_code::BAD_REQUEST, true).await?;
            return Ok(None);
        }
        Err(err) => return Err(err),
    };
    if !matches!(request_target, ParsedRequestTarget::Absolute(_))
        && header_state.host_header_index.is_none()
    {
        write_empty_response(writer, config, status_code::BAD_REQUEST, true).await?;
        return Ok(None);
    }
    let path_and_query = match request_target {
        ParsedRequestTarget::Origin(path) => BytesStr::try_from(head.slice_ref(path))
            .map_err(|_| Http1Error::RequestTargetNotUtf8.into_error())?,
        ParsedRequestTarget::Absolute(uri) => uri
            .path_and_query()
            .map_or(BytesStr::from_static("/"), |path_and_query| {
                BytesStr::from(path_and_query.as_str())
            }),
        ParsedRequestTarget::Asterisk => BytesStr::from_static("*"),
    };
    let HeaderParseState {
        headers,
        connection,
        upgrade,
        body_kind,
        http2_settings,
        header_meta,
        ..
    } = header_state;
    let request = RequestHead {
        http_version: HttpVersion::Http1_1,
        method: line.method,
        target: DomainRequestTarget::normal(scheme, path_and_query),
        headers,
        header_meta,
    };

    let websocket_requested = upgrade.websocket && connection.upgrade;
    let upgrade = if websocket_requested {
        let websocket = &request.header_meta.websocket;
        if !websocket.version_supported {
            UpgradeRequest::WebSocketUnsupportedVersion
        } else if line.websocket_method_supported
            && !websocket.key_duplicate
            && let Some(key) = websocket.key
        {
            UpgradeRequest::WebSocket {
                key,
                meta: websocket.request.clone(),
            }
        } else {
            UpgradeRequest::WebSocketBadRequest
        }
    } else if let Some(settings) = http2_settings
        && upgrade.h2c
        && connection.upgrade
        && connection.http2_settings
    {
        UpgradeRequest::H2c { settings }
    } else {
        UpgradeRequest::None
    };

    Ok(Some(ParsedRequest {
        request,
        upgrade,
        body_kind,
        persistence: if connection.close {
            ConnectionPersistence::Close
        } else {
            ConnectionPersistence::KeepAlive
        },
    }))
}

pub(super) async fn read_request<R, W>(
    reader: &mut R,
    buffer: &mut BytesMut,
    writer: &mut BufWriter<W>,
    config: &ServerConfig,
    secure: bool,
    timeout_duration: Option<Duration>,
) -> Result<Option<ParsedRequest>, H2CornError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let http1 = &config.http1;
    let limit_request_head_size = http1.limit_request_head_size.map(NonZeroUsize::get);
    let limit_request_line = http1.limit_request_line.map(NonZeroUsize::get);
    let limit_request_fields = http1.limit_request_fields.map(NonZeroUsize::get);
    let limit_request_field_size = http1.limit_request_field_size.map(NonZeroUsize::get);

    let Some(head) = read_request_head(
        reader,
        buffer,
        writer,
        config,
        timeout_duration,
        limit_request_head_size,
    )
    .await?
    else {
        return Ok(None);
    };
    let mut lines = head_lines(head.as_ref());
    let Some(request_line) = lines.next() else {
        return Http1Error::EmptyRequestHead.err();
    };
    let Some(line) =
        parse_request_line_or_reject(request_line, writer, config, limit_request_line).await?
    else {
        return Ok(None);
    };
    let Some(header_state) = parse_headers_or_reject(
        lines,
        &head,
        writer,
        config,
        limit_request_fields,
        limit_request_field_size,
    )
    .await?
    else {
        return Ok(None);
    };

    if header_state.expect_continue && header_state.body_kind != RequestBodyKind::None {
        writer.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").await?;
        writer.flush().await?;
    }

    parsed_request_from_head(&head, line, header_state, writer, config, secure).await
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
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin,
{
    loop {
        let size = read_chunk_size_line(reader, buffer, timeout_duration).await?;
        if size == 0 {
            drain_chunked_trailers(reader, buffer, timeout_duration).await?;
            return match body.finish() {
                RequestBodyFinish::Complete => Ok(()),
                RequestBodyFinish::ContentLengthMismatch => Http1Error::RequestBodyClosed.err(),
            };
        }
        match body.preview_chunk(size as u64) {
            RequestBodyProgress::Continue => {}
            RequestBodyProgress::SizeLimitExceeded => {
                return Http1Error::RequestBodyLimitExceeded.err();
            }
            RequestBodyProgress::ContentLengthExceeded => {
                return Http1Error::RequestBodyTooLarge.err();
            }
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
                return Http1Error::ChunkClosed.err();
            }
            let chunk_len = usize::min(buffer.len(), remaining);
            consume_body_bytes(buffer, chunk_len, tx, body).await?;
            remaining -= chunk_len;
        }

        consume_chunk_crlf(reader, buffer, timeout_duration).await?;
    }
}

async fn consume_body_bytes(
    buffer: &mut BytesMut,
    chunk_len: usize,
    tx: &mpsc::Sender<StreamInput>,
    body: &mut RequestBodyState,
) -> Result<(), H2CornError> {
    match body.record_chunk(chunk_len as u64) {
        RequestBodyProgress::Continue => {}
        RequestBodyProgress::SizeLimitExceeded => Http1Error::RequestBodyLimitExceeded.err()?,
        RequestBodyProgress::ContentLengthExceeded => Http1Error::RequestBodyTooLarge.err()?,
    }
    if body.should_deliver() {
        let chunk = buffer.split_to(chunk_len).freeze();
        if !send_if_open(tx, StreamInput::Data(chunk)).await {
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
        let read_cap = MAX_CHUNK_SIZE_LINE_BYTES.saturating_sub(buffer.len());
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

async fn drain_chunked_trailers<R>(
    reader: &mut R,
    buffer: &mut BytesMut,
    timeout_duration: Option<Duration>,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin,
{
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
        buffer.advance(line_len);
        line_search.reset();
        if end == 0 {
            return Ok(());
        }
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
    target: &'a [u8],
    headers: &mut RequestHeaders,
    host_header_index: Option<usize>,
) -> Result<ParsedRequestTarget<'a>, H2CornError> {
    match target {
        b"*" => return Ok(ParsedRequestTarget::Asterisk),
        [b'/', ..] => return Ok(ParsedRequestTarget::Origin(target)),
        _ => {}
    }
    let uri = str::from_utf8(target)
        .map_err(|_| Http1Error::RequestTargetNotUtf8)?
        .parse::<Uri>()
        .map_err(|_| Http1Error::InvalidAbsoluteFormTarget)?;
    if let Some(host_header_index) = host_header_index
        && let Some(authority) = uri.authority()
        && !headers[host_header_index]
            .1
            .as_bytes()
            .eq_ignore_ascii_case(authority.as_str().as_bytes())
    {
        return Http1Error::ConflictingAbsoluteFormAuthority.err();
    }
    if host_header_index.is_none()
        && let Some(authority) = uri.authority()
    {
        headers.push((
            KnownRequestHeaderName::Host.into(),
            RequestHeaderValue::from_bytes(Bytes::copy_from_slice(authority.as_str().as_bytes()))
                .ok_or(Http1Error::InvalidAbsoluteFormAuthority)?,
        ));
    }
    Ok(ParsedRequestTarget::Absolute(uri))
}

fn parse_chunk_size(line: &[u8]) -> Result<usize, H2CornError> {
    let mut value = 0_usize;
    let mut saw_digit = false;

    for &byte in line.trim_ascii() {
        if byte == b';' {
            break;
        }
        let digit = HEX_DIGIT_TABLE[byte as usize];
        if digit == INVALID_HEX_DIGIT {
            return Http1Error::InvalidChunkSize.err();
        }
        value = value
            .checked_shl(4)
            .and_then(|value| value.checked_add(usize::from(digit)))
            .ok_or(Http1Error::InvalidChunkSize)?;
        saw_digit = true;
    }

    if !saw_digit {
        return Http1Error::InvalidChunkSize.err();
    }
    Ok(value)
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
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => {
                return Http1Error::InvalidHttp2SettingsBase64UrlPayload.err();
            }
        };
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
        0 => {}
        2 => out.push((block[0] << 2) | (block[1] >> 4)),
        3 => {
            out.push((block[0] << 2) | (block[1] >> 4));
            out.push((block[1] << 4) | (block[2] >> 2));
        }
        _ => {
            return Http1Error::InvalidHttp2SettingsBase64UrlPayload.err();
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use http::Method;
    use std::{num::NonZeroU32, time::Duration};
    use tokio::io::{AsyncWriteExt, BufWriter, duplex};
    use tokio::spawn;
    use tokio::sync::mpsc;

    use super::{
        MAX_CHUNK_SIZE_LINE_BYTES, MAX_TRAILER_SECTION_BYTES, drain_chunked_trailers,
        parse_chunk_size, parse_http2_settings, read_chunk_size_line, read_chunked_body,
        read_request,
    };
    use crate::config::{BindTarget, Http1Config, Http2Config, ProxyConfig, ServerConfig};
    use crate::error::{H2CornError, Http1Error};
    use crate::frame;
    use crate::h1::{ConnectionPersistence, RequestBodyKind, UpgradeRequest};
    use crate::http::body::RequestBodyState;
    use crate::proxy::ProxyProtocolMode;
    use crate::runtime::StreamInput;

    fn test_server_config() -> &'static ServerConfig {
        Box::leak(Box::new(ServerConfig {
            binds: Box::new([BindTarget::Tcp {
                host: Box::from("127.0.0.1"),
                port: 8000,
            }]),
            access_log: false,
            root_path: Box::from(""),
            http1: Http1Config {
                enabled: true,
                ..Default::default()
            },
            http2: Http2Config {
                max_concurrent_streams: 8,
                max_header_list_size: None,
                max_header_block_size: None,
                max_inbound_frame_size: NonZeroU32::new(frame::DEFAULT_MAX_FRAME_SIZE as u32)
                    .expect("default HTTP/2 frame size is non-zero"),
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
            websocket: crate::config::WebSocketConfig::default(),
            proxy: ProxyConfig {
                trust_headers: false,
                trusted_peers: Box::new([]),
                protocol: ProxyProtocolMode::Off,
            },
            tls: None,
            timeout_handshake: Duration::from_secs(5),
            response_headers: crate::config::ResponseHeaderConfig::default(),
        }))
    }

    async fn read_test_request(
        request: &[u8],
    ) -> Result<Option<super::ParsedRequest>, H2CornError> {
        let (mut client, mut server) = duplex(512);
        let mut writer = BufWriter::new(tokio::io::sink());
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
            false,
            None,
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

    fn base64url_encode(src: &[u8]) -> Vec<u8> {
        const TABLE: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

        let mut out = Vec::with_capacity(src.len().div_ceil(3) * 4);
        let (chunks, remainder) = src.as_chunks::<3>();
        for &[c0, c1, c2] in chunks {
            out.push(TABLE[usize::from(c0 >> 2)]);
            out.push(TABLE[usize::from(((c0 & 0x03) << 4) | (c1 >> 4))]);
            out.push(TABLE[usize::from(((c1 & 0x0f) << 2) | (c2 >> 6))]);
            out.push(TABLE[usize::from(c2 & 0x3f)]);
        }

        match remainder {
            &[a] => {
                out.push(TABLE[usize::from(a >> 2)]);
                out.push(TABLE[usize::from((a & 0x03) << 4)]);
            }
            &[a, b] => {
                out.push(TABLE[usize::from(a >> 2)]);
                out.push(TABLE[usize::from(((a & 0x03) << 4) | (b >> 4))]);
                out.push(TABLE[usize::from((b & 0x0f) << 2)]);
            }
            [] => {}
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
    async fn read_request_accepts_extension_method() {
        let parsed =
            parse_test_request(b"PROPFIND /items HTTP/1.1\r\nHost: example.com\r\n\r\n").await;

        assert_eq!(
            parsed.request.method,
            Method::from_bytes(b"PROPFIND").expect("extension method is valid")
        );
    }

    #[tokio::test]
    async fn read_request_keeps_content_length_body_shape() {
        let parsed = parse_test_request(
            b"POST /upload HTTP/1.1\r\nHost: example.com\r\nContent-Length: 7\r\n\r\npayload",
        )
        .await;

        assert_eq!(
            parsed.body_kind,
            RequestBodyKind::ContentLength(7.try_into().unwrap())
        );
        assert_eq!(parsed.persistence, ConnectionPersistence::KeepAlive);
        assert!(matches!(parsed.upgrade, UpgradeRequest::None));
    }

    #[tokio::test]
    async fn read_request_keeps_chunked_body_shape() {
        let parsed = parse_test_request(
            b"POST /upload HTTP/1.1\r\nHost: example.com\r\nTransfer-Encoding: chunked\r\n\r\n",
        )
        .await;

        assert_eq!(parsed.body_kind, RequestBodyKind::Chunked);
        assert_eq!(parsed.persistence, ConnectionPersistence::KeepAlive);
        assert!(matches!(parsed.upgrade, UpgradeRequest::None));
    }

    #[tokio::test]
    async fn read_request_keeps_connection_close_disposition() {
        let parsed =
            parse_test_request(b"GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n")
                .await;

        assert_eq!(parsed.body_kind, RequestBodyKind::None);
        assert_eq!(parsed.persistence, ConnectionPersistence::Close);
        assert!(matches!(parsed.upgrade, UpgradeRequest::None));
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

        assert!(matches!(parsed.upgrade, UpgradeRequest::WebSocket { .. }));
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
            parsed.upgrade,
            UpgradeRequest::WebSocketUnsupportedVersion
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
            parsed.upgrade,
            UpgradeRequest::WebSocketBadRequest
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
            parsed.upgrade,
            UpgradeRequest::WebSocketBadRequest
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

        assert!(matches!(parsed.upgrade, UpgradeRequest::H2c { .. }));
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
        read_chunked_body(&mut server, &mut buffer, &tx, &mut body, None)
            .await
            .expect("empty trailer block is accepted");
        writer.await.expect("writer task finishes");

        match rx.try_recv().expect("body chunk is forwarded") {
            StreamInput::Data(chunk) => assert_eq!(chunk.as_ref(), b"abc"),
            _ => panic!("expected body data event"),
        }
        rx.try_recv().unwrap_err();
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
        read_chunked_body(&mut server, &mut buffer, &tx, &mut body, None)
            .await
            .expect("chunk extensions and trailers are accepted");
        writer.await.expect("writer task finishes");

        let StreamInput::Data(first) = rx.try_recv().expect("first body chunk exists") else {
            panic!("expected first body chunk");
        };
        let StreamInput::Data(second) = rx.try_recv().expect("second body chunk exists") else {
            panic!("expected second body chunk");
        };
        assert_eq!(first.as_ref(), b"abc");
        assert_eq!(second.as_ref(), b"defg");
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
            err,
            H2CornError::Http1(Http1Error::InvalidChunkSize)
        ));
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

        let err = drain_chunked_trailers(&mut server, &mut buffer, None)
            .await
            .expect_err("oversized trailer section is rejected");

        assert!(matches!(
            err,
            H2CornError::Http1(Http1Error::MalformedHeaderLine)
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

        let mut body = RequestBodyState::new(None, None, Some(4));
        let err = read_chunked_body(&mut server, &mut buffer, &tx, &mut body, None)
            .await
            .expect_err("announced chunk beyond configured limit is rejected");
        writer.await.expect("writer task finishes");

        assert!(matches!(
            err,
            H2CornError::Http1(Http1Error::RequestBodyLimitExceeded)
        ));
        rx.try_recv().unwrap_err();
        assert_eq!(&buffer[..], b"hello\r\n");
    }

    #[test]
    fn parse_chunk_size_accepts_hex_extensions_and_whitespace() {
        assert_eq!(parse_chunk_size(b"1a").unwrap(), 0x1a);
        assert_eq!(parse_chunk_size(b"1A;foo=bar").unwrap(), 0x1a);
        assert_eq!(parse_chunk_size(b" \t1A\r").unwrap(), 0x1a);
    }

    #[test]
    fn parse_chunk_size_rejects_empty_and_invalid_values() {
        assert!(matches!(
            parse_chunk_size(b""),
            Err(H2CornError::Http1(Http1Error::InvalidChunkSize))
        ));
        assert!(matches!(
            parse_chunk_size(b";foo=bar"),
            Err(H2CornError::Http1(Http1Error::InvalidChunkSize))
        ));
        assert!(matches!(
            parse_chunk_size(b"1 g"),
            Err(H2CornError::Http1(Http1Error::InvalidChunkSize))
        ));
    }
}
