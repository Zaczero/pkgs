use std::fs::File;
use std::io;
use std::sync::Arc;

use http::StatusCode as StandardStatusCode;
use itoa::Buffer as ItoaBuffer;
use smallvec::SmallVec;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};

use crate::access_log::ResponseLogState;
use crate::bridge::PayloadBytes;
use crate::config::ServerConfig;
use crate::error::H2CornError;
use crate::http::digits;
use crate::http::header::apply_default_response_headers;
use crate::http::pathsend::{PATHSEND_SENDFILE_MIN, PathStreamer};
use crate::http::response::{FinalResponseBody, HttpResponseTransport, ResponseStart};
use crate::http::types::{HttpStatusCode, ResponseHeaderKind, ResponseHeaders, status_code};
use crate::sendfile::WriteTarget;

const RESPONSE_BUF_CAPACITY: usize = 512;

type ResponseBuf = SmallVec<[u8; RESPONSE_BUF_CAPACITY]>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FileTransferMode {
    Buffered,
    Sendfile,
}

impl FileTransferMode {
    const fn for_target<W: WriteTarget>(len: usize) -> Self {
        if W::SUPPORTS_SENDFILE && len >= PATHSEND_SENDFILE_MIN {
            Self::Sendfile
        } else {
            Self::Buffered
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BodyFraming {
    KnownLength(usize),
    Chunked,
}

pub(super) struct H1HttpTransport<'a, W> {
    writer: &'a mut BufWriter<W>,
    config: Arc<ServerConfig>,
    close_after: bool,
    response_log: ResponseLogState,
}

impl<'a, W> H1HttpTransport<'a, W>
where
    W: AsyncWrite + Unpin,
{
    pub(super) fn new(
        writer: &'a mut BufWriter<W>,
        config: Arc<ServerConfig>,
        close_after: bool,
    ) -> Self {
        Self {
            writer,
            config,
            close_after,
            response_log: ResponseLogState::default(),
        }
    }

    pub(super) async fn write_empty_response(
        &mut self,
        status: HttpStatusCode,
        close_after: bool,
    ) -> Result<(), H2CornError> {
        write_empty_response(self.writer, &self.config, status, close_after).await
    }
}

impl<W> HttpResponseTransport for H1HttpTransport<'_, W>
where
    W: WriteTarget,
{
    async fn send_final_response(
        &mut self,
        mut start: ResponseStart,
        body: FinalResponseBody,
    ) -> Result<(), H2CornError> {
        self.response_log.started(start.status());
        if !matches!(
            body,
            FinalResponseBody::Empty | FinalResponseBody::Suppressed { .. }
        ) {
            self.response_log.sent_body(body.len());
        }
        start.apply_default_headers(&self.config);
        let (status, headers) = start.into_status_headers();
        write_final_response(self.writer, status, headers, body, self.close_after).await
    }

    async fn start_streaming_response(
        &mut self,
        mut start: ResponseStart,
    ) -> Result<(), H2CornError> {
        self.response_log.started(start.status());
        start.apply_default_headers(&self.config);
        let (status, headers) = start.into_status_headers();
        write_response_head(
            self.writer,
            status,
            &headers,
            self.close_after,
            BodyFraming::Chunked,
        )
        .await
    }

    async fn send_streaming_body(&mut self, body: PayloadBytes) -> Result<(), H2CornError> {
        if body.is_empty() {
            return Ok(());
        }
        self.response_log.sent_body(body.len());
        write_chunk(self.writer, body.as_ref()).await
    }

    async fn send_streaming_file(&mut self, file: File, len: usize) -> Result<(), H2CornError> {
        self.response_log.sent_body(len);
        let mut streamer = PathStreamer::new(file, len, false);
        while !streamer.is_drained() {
            if streamer.needs_fill() {
                streamer.fill().await?;
            }
            let chunk = streamer.remaining();
            if !chunk.is_empty() {
                write_chunk(self.writer, chunk).await?;
                streamer.consume(chunk.len());
            }
        }
        self.writer.flush().await?;
        Ok(())
    }

    async fn finish_streaming_response(&mut self) -> Result<(), H2CornError> {
        finish_chunked_response(self.writer).await
    }

    async fn finish_streaming_with_trailers(
        &mut self,
        trailers: ResponseHeaders,
    ) -> Result<(), H2CornError> {
        finish_chunked_with_trailers(self.writer, &trailers).await
    }

    async fn send_internal_error_response(&mut self) -> Result<(), H2CornError> {
        self.response_log.internal_error();
        write_empty_response(
            self.writer,
            &self.config,
            status_code::INTERNAL_SERVER_ERROR,
            true,
        )
        .await
    }

    async fn abort_incomplete_response(&mut self) -> Result<(), H2CornError> {
        Ok(())
    }

    fn response_log_state(&self) -> ResponseLogState {
        self.response_log
    }
}

pub(super) async fn write_simple_response<W>(
    writer: &mut BufWriter<W>,
    config: &ServerConfig,
    status: HttpStatusCode,
    mut headers: ResponseHeaders,
    body: &[u8],
    close_after: bool,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    apply_default_response_headers(&mut headers, config);
    write_response_head(
        writer,
        status,
        &headers,
        close_after,
        BodyFraming::KnownLength(body.len()),
    )
    .await?;
    if !body.is_empty() {
        writer.write_all(body).await?;
    }
    writer.flush().await?;
    Ok(())
}

pub(super) async fn write_final_response<W>(
    writer: &mut BufWriter<W>,
    status: HttpStatusCode,
    headers: ResponseHeaders,
    body: FinalResponseBody,
    close_after: bool,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    match body {
        FinalResponseBody::Empty => {
            write_response_head(
                writer,
                status,
                &headers,
                close_after,
                BodyFraming::KnownLength(0),
            )
            .await?;
            writer.flush().await?;
            Ok(())
        },
        FinalResponseBody::Bytes(body) => {
            write_response_head(
                writer,
                status,
                &headers,
                close_after,
                BodyFraming::KnownLength(body.len()),
            )
            .await?;
            if !body.is_empty() {
                writer.write_all(body.as_ref()).await?;
            }
            writer.flush().await?;
            Ok(())
        },
        FinalResponseBody::File { file, len } => {
            write_response_head(
                writer,
                status,
                &headers,
                close_after,
                BodyFraming::KnownLength(len),
            )
            .await?;
            let mut file = *file;
            match FileTransferMode::for_target::<W>(len) {
                FileTransferMode::Buffered => {
                    // Small files: one buffered read + ordinary writes beat a
                    // per-response sendfile setup (measured: sendfile's loopback
                    // skb handling is the hot path at this size). Transports that
                    // cannot sendfile stay on this 128 KiB rolling path at every
                    // size instead of falling into Tokio's generic 8 KiB copy.
                    let mut streamer = PathStreamer::new(file, len, true);
                    while !streamer.is_drained() {
                        streamer.fill().await?;
                        let chunk = streamer.remaining();
                        if chunk.is_empty() {
                            break;
                        }
                        let chunk_len = chunk.len();
                        writer.write_all(chunk).await?;
                        streamer.consume(chunk_len);
                    }
                    writer.flush().await?;
                },
                FileTransferMode::Sendfile => {
                    writer.flush().await?;
                    let mut offset = 0_u64;
                    W::send_file(writer, &mut file, &mut offset, len).await?;
                },
            }
            Ok(())
        },
        FinalResponseBody::Suppressed { len } => {
            write_response_head(
                writer,
                status,
                &headers,
                close_after,
                BodyFraming::KnownLength(len),
            )
            .await?;
            writer.flush().await?;
            Ok(())
        },
    }
}

pub(super) async fn write_empty_response<W>(
    writer: &mut BufWriter<W>,
    config: &ServerConfig,
    status: HttpStatusCode,
    close_after: bool,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    let headers = ResponseHeaders::new();
    write_simple_response(writer, config, status, headers, &[], close_after).await
}

pub(super) async fn write_response_head<W>(
    writer: &mut BufWriter<W>,
    status: HttpStatusCode,
    headers: &ResponseHeaders,
    close_after: bool,
    framing: BodyFraming,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    let mut out = ResponseBuf::new();
    append_status_line(&mut out, status);
    append_response_headers(&mut out, headers, close_after, framing);
    writer.write_all(out.as_slice()).await?;
    Ok(())
}

pub(super) async fn write_chunk<W>(
    writer: &mut BufWriter<W>,
    chunk: &[u8],
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    let mut prefix = [0_u8; 18];
    let prefix = chunk_prefix(chunk.len(), &mut prefix);
    // Small chunks coalesce in the BufWriter (one syscall per flushed batch).
    // A chunk at/over the buffer capacity would bypass the buffer anyway, so
    // emit `prefix + chunk + CRLF` as a single `writev` instead of a
    // buffer-flush plus a direct write — halving write syscalls for
    // large-chunk streaming.
    if chunk.len() < super::H1_WRITER_BUFFER_CAPACITY {
        writer.write_all(prefix).await?;
        writer.write_all(chunk).await?;
        writer.write_all(b"\r\n").await?;
        return Ok(());
    }
    writer.flush().await?;
    let mut slices = [
        io::IoSlice::new(prefix),
        io::IoSlice::new(chunk),
        io::IoSlice::new(b"\r\n"),
    ];
    write_all_vectored(writer.get_mut(), &mut slices).await
}

/// Drive `write_vectored` to completion over `slices`, advancing past partial
/// writes. The caller must have flushed any buffered writer first so output
/// order is preserved.
async fn write_all_vectored<W>(
    writer: &mut W,
    slices: &mut [io::IoSlice<'_>],
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    let mut remaining: usize = slices.iter().map(|slice| slice.len()).sum();
    let mut bufs = slices;
    while remaining > 0 {
        let written = writer.write_vectored(bufs).await?;
        if written == 0 {
            return Err(H2CornError::from(io::Error::from(io::ErrorKind::WriteZero)));
        }
        remaining -= written;
        if remaining == 0 {
            break;
        }
        io::IoSlice::advance_slices(&mut bufs, written);
    }
    Ok(())
}

pub(super) async fn finish_chunked_response<W>(writer: &mut BufWriter<W>) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(b"0\r\n\r\n").await?;
    writer.flush().await?;
    Ok(())
}

pub(super) async fn finish_chunked_with_trailers<W>(
    writer: &mut BufWriter<W>,
    trailers: &ResponseHeaders,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(b"0\r\n").await?;
    let mut out = ResponseBuf::new();
    append_header_lines(&mut out, trailers);
    out.extend_from_slice(b"\r\n");
    writer.write_all(out.as_slice()).await?;
    writer.flush().await?;
    Ok(())
}

pub(super) async fn write_h2c_upgrade_response<W>(
    writer: &mut BufWriter<W>,
    config: &ServerConfig,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    let mut out = ResponseBuf::new();
    out.extend_from_slice(
        b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: h2c\r\n",
    );
    let mut headers = ResponseHeaders::new();
    apply_default_response_headers(&mut headers, config);
    append_header_lines(&mut out, &headers);
    out.extend_from_slice(b"\r\n");
    writer.write_all(out.as_slice()).await?;
    writer.flush().await?;
    Ok(())
}

pub(super) fn append_header_lines(dst: &mut ResponseBuf, headers: &ResponseHeaders) {
    for (name, value) in headers {
        if matches!(
            name.kind(),
            ResponseHeaderKind::ContentLength
                | ResponseHeaderKind::TransferEncoding
                | ResponseHeaderKind::Connection
        ) {
            continue;
        }
        append_header_line(dst, name.as_bytes(), value.as_bytes());
    }
}

fn append_status_line(dst: &mut ResponseBuf, status: HttpStatusCode) {
    if let Some(line) = common_status_line(status) {
        dst.extend_from_slice(line);
        return;
    }
    dst.extend_from_slice(b"HTTP/1.1 ");
    append_status_code(dst, status);
    dst.push(b' ');
    if let Some(reason) = StandardStatusCode::from_u16(status.get())
        .ok()
        .and_then(|status| status.canonical_reason())
    {
        dst.extend_from_slice(reason.as_bytes());
    }
    dst.extend_from_slice(b"\r\n");
}

const fn common_status_line(status: HttpStatusCode) -> Option<&'static [u8]> {
    match status {
        status_code::SWITCHING_PROTOCOLS => Some(b"HTTP/1.1 101 Switching Protocols\r\n"),
        status_code::OK => Some(b"HTTP/1.1 200 OK\r\n"),
        status_code::NO_CONTENT => Some(b"HTTP/1.1 204 No Content\r\n"),
        status_code::PARTIAL_CONTENT => Some(b"HTTP/1.1 206 Partial Content\r\n"),
        status_code::NOT_MODIFIED => Some(b"HTTP/1.1 304 Not Modified\r\n"),
        status_code::BAD_REQUEST => Some(b"HTTP/1.1 400 Bad Request\r\n"),
        status_code::FORBIDDEN => Some(b"HTTP/1.1 403 Forbidden\r\n"),
        status_code::NOT_FOUND => Some(b"HTTP/1.1 404 Not Found\r\n"),
        status_code::PAYLOAD_TOO_LARGE => Some(b"HTTP/1.1 413 Payload Too Large\r\n"),
        status_code::URI_TOO_LONG => Some(b"HTTP/1.1 414 URI Too Long\r\n"),
        status_code::UPGRADE_REQUIRED => Some(b"HTTP/1.1 426 Upgrade Required\r\n"),
        status_code::REQUEST_HEADER_FIELDS_TOO_LARGE => {
            Some(b"HTTP/1.1 431 Request Header Fields Too Large\r\n")
        },
        status_code::INTERNAL_SERVER_ERROR => Some(b"HTTP/1.1 500 Internal Server Error\r\n"),
        status_code::NOT_IMPLEMENTED => Some(b"HTTP/1.1 501 Not Implemented\r\n"),
        status_code::SERVICE_UNAVAILABLE => Some(b"HTTP/1.1 503 Service Unavailable\r\n"),
        _ => None,
    }
}

fn append_status_code(dst: &mut ResponseBuf, status: HttpStatusCode) {
    dst.extend_from_slice(&digits::three_digit_bytes(status.get()));
}

fn append_response_headers(
    dst: &mut ResponseBuf,
    headers: &ResponseHeaders,
    close_after: bool,
    framing: BodyFraming,
) {
    append_header_lines(dst, headers);
    match framing {
        BodyFraming::KnownLength(len) => {
            append_header_line_with_decimal(dst, b"Content-Length", len);
        },
        BodyFraming::Chunked => {
            dst.extend_from_slice(b"Transfer-Encoding: chunked\r\n");
        },
    }
    if close_after {
        dst.extend_from_slice(b"Connection: close\r\n");
    }
    dst.extend_from_slice(b"\r\n");
}

fn append_header_line(dst: &mut ResponseBuf, name: &[u8], value: &[u8]) {
    dst.extend_from_slice(name);
    dst.extend_from_slice(b": ");
    dst.extend_from_slice(value);
    dst.extend_from_slice(b"\r\n");
}

fn append_header_line_with_decimal(dst: &mut ResponseBuf, name: &[u8], value: usize) {
    dst.extend_from_slice(name);
    dst.extend_from_slice(b": ");
    append_decimal_usize(dst, value);
    dst.extend_from_slice(b"\r\n");
}

fn append_decimal_usize(dst: &mut ResponseBuf, value: usize) {
    let mut buf = ItoaBuffer::new();
    dst.extend_from_slice(buf.format(value).as_bytes());
}

fn chunk_prefix(mut value: usize, buf: &mut [u8; 18]) -> &[u8] {
    let mut cursor = buf.len();
    buf[cursor - 1] = b'\n';
    buf[cursor - 2] = b'\r';
    cursor -= 2;
    loop {
        cursor -= 1;
        let digit = (value & 0xF) as u8;
        buf[cursor] = if digit < 10 {
            b'0' + digit
        } else {
            b'A' + (digit - 10)
        };
        value >>= 4;
        if value == 0 {
            return &buf[cursor..];
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::io::{AsyncWrite, BufWriter};
    use tokio::net::tcp::OwnedWriteHalf;

    use super::{FileTransferMode, PATHSEND_SENDFILE_MIN, ResponseBuf, append_status_line};
    use crate::http::types::{HttpStatusCode, status_code};
    use crate::sendfile::WriteTarget;

    struct BufferedOnly;

    impl AsyncWrite for BufferedOnly {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            unreachable!("the transfer-mode test performs no I/O")
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            unreachable!("the transfer-mode test performs no I/O")
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            unreachable!("the transfer-mode test performs no I/O")
        }
    }

    impl WriteTarget for BufferedOnly {
        const SUPPORTS_SENDFILE: bool = false;

        async fn send_file(
            _writer: &mut BufWriter<Self>,
            _file: &mut File,
            _offset: &mut u64,
            _len: usize,
        ) -> io::Result<()> {
            unreachable!("a buffered-only target cannot select sendfile")
        }
    }

    #[test]
    fn status_line_keeps_common_precomputed_and_standard_fallback_paths() {
        let mut out = ResponseBuf::new();
        append_status_line(&mut out, status_code::OK);
        assert_eq!(out.as_slice(), b"HTTP/1.1 200 OK\r\n");

        out.clear();
        append_status_line(&mut out, HttpStatusCode::new(418).unwrap());
        assert_eq!(out.as_slice(), b"HTTP/1.1 418 I'm a teapot\r\n");

        out.clear();
        append_status_line(&mut out, HttpStatusCode::new(999).unwrap());
        assert_eq!(out.as_slice(), b"HTTP/1.1 999 \r\n");
    }

    #[test]
    fn targets_without_sendfile_never_select_the_sendfile_tier() {
        assert_eq!(
            FileTransferMode::for_target::<BufferedOnly>(usize::MAX),
            FileTransferMode::Buffered,
        );
        assert_eq!(
            FileTransferMode::for_target::<OwnedWriteHalf>(PATHSEND_SENDFILE_MIN,),
            if cfg!(target_os = "linux") {
                FileTransferMode::Sendfile
            } else {
                FileTransferMode::Buffered
            },
        );
    }
}
