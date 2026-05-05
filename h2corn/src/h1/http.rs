use std::io;

use http::StatusCode as StandardStatusCode;
use itoa::Buffer as ItoaBuffer;
use smallvec::SmallVec;
use tokio::fs::File;
#[cfg(unix)]
use tokio::io::copy;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::net::tcp::OwnedWriteHalf as TcpOwnedWriteHalf;
#[cfg(unix)]
use tokio::net::unix::OwnedWriteHalf as UnixOwnedWriteHalf;

use crate::bridge::PayloadBytes;
use crate::config::ServerConfig;
use crate::console::ResponseLogState;
use crate::error::H2CornError;
use crate::http::digits;
use crate::http::header::apply_default_response_headers;
use crate::http::pathsend::PathStreamer;
use crate::http::response::{FinalResponseBody, HttpResponseTransport, ResponseStart};
use crate::http::types::{HttpStatusCode, ResponseHeaders, status_code};
use crate::sendfile::sendfile_all_tcp;

const RESPONSE_BUF_CAPACITY: usize = 512;

type ResponseBuf = SmallVec<[u8; RESPONSE_BUF_CAPACITY]>;

pub trait H1WriteTarget: AsyncWrite + Unpin + Send + Sync + 'static {
    async fn send_file_body(
        writer: &mut BufWriter<Self>,
        file: &mut File,
        len: usize,
    ) -> io::Result<()>
    where
        Self: Sized;
}

impl H1WriteTarget for TcpOwnedWriteHalf {
    async fn send_file_body(
        writer: &mut BufWriter<Self>,
        file: &mut File,
        len: usize,
    ) -> io::Result<()> {
        writer.flush().await?;
        let mut offset = 0_u64;
        sendfile_all_tcp(writer, file, &mut offset, len).await
    }
}

#[cfg(unix)]
impl H1WriteTarget for UnixOwnedWriteHalf {
    async fn send_file_body(
        writer: &mut BufWriter<Self>,
        file: &mut File,
        _len: usize,
    ) -> io::Result<()> {
        writer.flush().await?;
        copy(file, writer.get_mut()).await?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BodyFraming {
    KnownLength(usize),
    Chunked,
}

pub(super) struct H1HttpTransport<'a, W> {
    writer: &'a mut BufWriter<W>,
    config: &'static ServerConfig,
    close_after: bool,
    response_log: ResponseLogState,
}

impl<'a, W> H1HttpTransport<'a, W>
where
    W: AsyncWrite + Unpin,
{
    pub(super) fn new(
        writer: &'a mut BufWriter<W>,
        config: &'static ServerConfig,
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
        write_empty_response(self.writer, self.config, status, close_after).await
    }
}

impl<W> HttpResponseTransport for H1HttpTransport<'_, W>
where
    W: H1WriteTarget,
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
        start.apply_default_headers(self.config);
        let (status, headers) = start.into_status_headers();
        write_final_response(self.writer, status, headers, body, self.close_after).await
    }

    async fn start_streaming_response(
        &mut self,
        mut start: ResponseStart,
    ) -> Result<(), H2CornError> {
        self.response_log.started(start.status());
        start.apply_default_headers(self.config);
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
            self.config,
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
    W: H1WriteTarget,
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
        FinalResponseBody::File { mut file, len } => {
            write_response_head(
                writer,
                status,
                &headers,
                close_after,
                BodyFraming::KnownLength(len),
            )
            .await?;
            W::send_file_body(writer, &mut file, len).await?;
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
    writer
        .write_all(chunk_prefix(chunk.len(), &mut prefix))
        .await?;
    writer.write_all(chunk).await?;
    writer.write_all(b"\r\n").await?;
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
        let name = name.as_bytes();
        if name == b"content-length" || name == b"transfer-encoding" || name == b"connection" {
            continue;
        }
        append_header_line(dst, name, value.as_bytes());
    }
}

fn append_status_line(dst: &mut ResponseBuf, status: HttpStatusCode) {
    dst.extend_from_slice(b"HTTP/1.1 ");
    append_status_code(dst, status);
    dst.push(b' ');
    if let Some(reason) = reason_phrase(status) {
        dst.extend_from_slice(reason.as_bytes());
    }
    dst.extend_from_slice(b"\r\n");
}

fn reason_phrase(status: HttpStatusCode) -> Option<&'static str> {
    match status {
        status_code::SWITCHING_PROTOCOLS => Some("Switching Protocols"),
        status_code::OK => Some("OK"),
        status_code::NO_CONTENT => Some("No Content"),
        status_code::PARTIAL_CONTENT => Some("Partial Content"),
        status_code::NOT_MODIFIED => Some("Not Modified"),
        status_code::BAD_REQUEST => Some("Bad Request"),
        status_code::FORBIDDEN => Some("Forbidden"),
        status_code::NOT_FOUND => Some("Not Found"),
        status_code::PAYLOAD_TOO_LARGE => Some("Payload Too Large"),
        status_code::URI_TOO_LONG => Some("URI Too Long"),
        status_code::UPGRADE_REQUIRED => Some("Upgrade Required"),
        status_code::REQUEST_HEADER_FIELDS_TOO_LARGE => Some("Request Header Fields Too Large"),
        status_code::INTERNAL_SERVER_ERROR => Some("Internal Server Error"),
        status_code::NOT_IMPLEMENTED => Some("Not Implemented"),
        status_code::SERVICE_UNAVAILABLE => Some("Service Unavailable"),
        _ => StandardStatusCode::from_u16(status)
            .ok()
            .and_then(|status_code| status_code.canonical_reason()),
    }
}

fn append_status_code(dst: &mut ResponseBuf, status: HttpStatusCode) {
    if (100..=999).contains(&status) {
        dst.extend_from_slice(&digits::three_digit_bytes(status));
    } else {
        append_decimal_usize(dst, usize::from(status));
    }
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
    use super::reason_phrase;
    use crate::http::types::status_code;

    #[test]
    fn reason_phrase_keeps_common_fast_path_and_fallback() {
        assert_eq!(reason_phrase(status_code::OK), Some("OK"));
        assert_eq!(
            reason_phrase(status_code::REQUEST_HEADER_FIELDS_TOO_LARGE),
            Some("Request Header Fields Too Large")
        );
        assert_eq!(reason_phrase(418), Some("I'm a teapot"));
        assert_eq!(reason_phrase(999), None);
    }
}
