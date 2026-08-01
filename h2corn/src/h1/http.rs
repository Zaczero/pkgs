use std::io;

use http::StatusCode as StandardStatusCode;
use itoa::Buffer as ItoaBuffer;
use smallvec::SmallVec;
use tokio::io::{AsyncWrite, AsyncWriteExt as _, BufWriter};

use super::ConnectionPersistence;
use crate::async_util::write_all_vectored;
use crate::config::ServerConfig;
use crate::error::H2CornError;
use crate::http::digits;
use crate::http::header::{
    ResponseConnectionDirective, apply_default_response_headers,
};
use crate::http::pathsend::{PATHSEND_SENDFILE_MIN, PathStreamer};
use crate::http::response::{FinalResponseBody, HttpResponseTransport, ResponseAction};
use crate::http::types::{
    HttpStatusCode, ResponseField, ResponseHeaderKind, ResponseHeaders, ResponseTrailers,
    common_status_codes, status_code,
};
use crate::sendfile::WriteTarget;

macro_rules! common_status_line_match {
    ($(($constant:ident, $line:expr, $hpack:expr)),+ $(,)?) => {
        const fn common_status_line(status: HttpStatusCode) -> Option<&'static [u8]> {
            match status {
                $(status_code::$constant => Some($line),)+
                _ => None,
            }
        }
    };
}

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
    None,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StreamingBodyFraming {
    Idle,
    Chunked,
    KnownLength,
}

pub(super) struct H1ResponseState {
    close_after: bool,
    streaming_framing: StreamingBodyFraming,
    /// Whether any status-bearing response action has been lowered. The
    /// connection loop needs this after a concurrent body-limit failure so
    /// it does not emit a second status line; request logging lives elsewhere.
    response_started: bool,
}

impl H1ResponseState {
    pub(super) const fn new(close_after: bool) -> Self {
        Self {
            close_after,
            streaming_framing: StreamingBodyFraming::Idle,
            response_started: false,
        }
    }

    pub(super) const fn response_started(&self) -> bool {
        self.response_started
    }

    /// Fold the app's response-level connection request into the request's
    /// persistence decision at the boundary that controls the next parse.
    pub(super) const fn persistence(
        &self,
        request_persistence: ConnectionPersistence,
    ) -> ConnectionPersistence {
        if self.close_after {
            ConnectionPersistence::Close
        } else {
            request_persistence
        }
    }

    pub(super) async fn write_empty_response<W>(
        &mut self,
        writer: &mut BufWriter<W>,
        config: &ServerConfig,
        status: HttpStatusCode,
        close_after: bool,
    ) -> Result<(), H2CornError>
    where
        W: AsyncWrite + Unpin,
    {
        self.response_started = true;
        write_empty_response(writer, config, status, close_after).await
    }
}

pub(super) struct H1HttpTransport<'a, W> {
    writer: &'a mut BufWriter<W>,
    config: &'a ServerConfig,
    state: H1ResponseState,
}

impl<'a, W> H1HttpTransport<'a, W>
where
    W: AsyncWrite + Unpin,
{
    pub(super) const fn new(
        writer: &'a mut BufWriter<W>,
        config: &'a ServerConfig,
        close_after: bool,
    ) -> Self {
        Self {
            writer,
            config,
            state: H1ResponseState::new(close_after),
        }
    }

    pub(super) const fn response_started(&self) -> bool {
        self.state.response_started()
    }

    /// Fold the app's response-level connection request into the request's
    /// persistence decision at the boundary that controls the next parse.
    pub(super) const fn persistence(
        &self,
        request_persistence: ConnectionPersistence,
    ) -> ConnectionPersistence {
        self.state.persistence(request_persistence)
    }

    pub(super) async fn write_empty_response(
        &mut self,
        status: HttpStatusCode,
        close_after: bool,
    ) -> Result<(), H2CornError> {
        self.state
            .write_empty_response(self.writer, self.config, status, close_after)
            .await
    }
}

impl H1ResponseState {
    async fn write_streaming_body<W>(
        &mut self,
        writer: &mut BufWriter<W>,
        body: &[u8],
    ) -> Result<(), H2CornError>
    where
        W: WriteTarget,
    {
        if body.is_empty() {
            return Ok(());
        }
        match &mut self.streaming_framing {
            StreamingBodyFraming::Chunked => write_chunk(writer, body).await,
            // The shared response controller has already checked the declared
            // length before this transport receives the action.
            StreamingBodyFraming::KnownLength => {
                writer.write_all(body).await?;
                Ok(())
            },
            StreamingBodyFraming::Idle => unreachable!("body follows response start"),
        }
    }

    async fn write_streaming_file<W>(
        &mut self,
        writer: &mut BufWriter<W>,
        file: Box<std::fs::File>,
        len: usize,
    ) -> Result<(), H2CornError>
    where
        W: WriteTarget,
    {
        let mut streamer = PathStreamer::new(*file, len, false);
        while !streamer.is_drained() {
            if streamer.needs_fill() {
                streamer.fill().await?;
            }
            let chunk = streamer.remaining();
            if !chunk.is_empty() {
                match &mut self.streaming_framing {
                    StreamingBodyFraming::Chunked => write_chunk(writer, chunk).await?,
                    StreamingBodyFraming::KnownLength => writer.write_all(chunk).await?,
                    StreamingBodyFraming::Idle => unreachable!("path follows response start"),
                }
                streamer.consume(chunk.len());
            }
        }
        writer.flush().await?;
        Ok(())
    }

    async fn finish_streaming<W>(&self, writer: &mut BufWriter<W>) -> Result<(), H2CornError>
    where
        W: WriteTarget,
    {
        match self.streaming_framing {
            StreamingBodyFraming::Chunked => finish_chunked_response(writer).await?,
            // The controller emits Finish only after it has checked the final
            // byte count, so known-length framing has no local terminal work.
            StreamingBodyFraming::KnownLength => writer.flush().await?,
            StreamingBodyFraming::Idle => unreachable!("finish follows response start"),
        }
        if self.close_after {
            writer.get_mut().shutdown().await?;
        }
        Ok(())
    }

    pub(super) async fn apply_response_action<W>(
        &mut self,
        writer: &mut BufWriter<W>,
        config: &ServerConfig,
        action: ResponseAction,
    ) -> Result<(), H2CornError>
    where
        W: WriteTarget,
    {
        match action {
            // 103 is deliberately unadvertised on HTTP/1: RFC 8297 names
            // interoperability and security risks with clients that mishandle
            // an interim response, and a pipelined peer that ignores it
            // desynchronizes. The scope never offers the extension here, so
            // reaching this arm means the application invented the message.
            ResponseAction::EarlyHint(_) => Ok(()),
            ResponseAction::Final { mut start, body } => {
                self.response_started = true;
                let directive = start.directive();
                self.close_after |= directive == ResponseConnectionDirective::Close;
                start.apply_default_headers(config);
                let (status, headers) = start.into_status_headers();
                write_final_response(writer, status, headers, body, self.close_after).await?;
                if self.close_after {
                    writer.get_mut().shutdown().await?;
                }
                Ok(())
            },
            ResponseAction::Start {
                mut start,
                expects_trailers,
            } => {
                self.response_started = true;
                let directive = start.directive();
                self.close_after |= directive == ResponseConnectionDirective::Close;
                let length = start.prepare_streaming(&config.response_headers, expects_trailers);
                let (status, headers) = start.into_status_headers();
                self.streaming_framing = if length.is_some() {
                    StreamingBodyFraming::KnownLength
                } else {
                    StreamingBodyFraming::Chunked
                };
                write_response_head(
                    writer,
                    status,
                    &headers,
                    self.close_after,
                    length.map_or(BodyFraming::Chunked, BodyFraming::KnownLength),
                )
                .await
            },
            ResponseAction::Body(body) => self.write_streaming_body(writer, body.as_ref()).await,
            ResponseAction::File { file, len } => {
                self.write_streaming_file(writer, file, len).await
            },
            ResponseAction::Finish => self.finish_streaming(writer).await,
            ResponseAction::FinishWithTrailers(trailers) => {
                finish_chunked_with_trailers(writer, &trailers).await
            },
            ResponseAction::InternalError => {
                self.response_started = true;
                write_empty_response(writer, config, status_code::INTERNAL_SERVER_ERROR, true).await
            },
            // Nothing to write: an HTTP/1 response the application abandoned
            // mid-body is truncated by closing the connection, which the
            // caller does.
            ResponseAction::AbortIncomplete => {
                writer.flush().await?;
                writer.get_mut().shutdown().await?;
                Ok(())
            },
        }
    }

    pub(super) async fn flush_buffered<W>(
        &self,
        writer: &mut BufWriter<W>,
    ) -> Result<(), H2CornError>
    where
        W: WriteTarget,
    {
        writer.flush().await?;
        Ok(())
    }
}

impl<W> HttpResponseTransport for H1HttpTransport<'_, W>
where
    W: WriteTarget,
{
    async fn apply_response_action(&mut self, action: ResponseAction) -> Result<(), H2CornError> {
        self.state
            .apply_response_action(self.writer, self.config, action)
            .await
    }

    async fn flush_buffered(&mut self) -> Result<(), H2CornError> {
        // A no-op when the batch already flushed (final responses, large
        // vectored chunks); otherwise this is what makes a small streamed
        // chunk visible to the client before the app awaits again.
        self.state.flush_buffered(self.writer).await
    }
}

common_status_codes!(common_status_line_match);

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
            let framing = BodyFraming::KnownLength(body.len());
            // A body at or over the buffer capacity bypasses the `BufWriter`
            // anyway, so buffering the head would cost a flush syscall and then
            // a direct body write. One `writev` sends both, the same treatment
            // `write_chunk` already gives large streaming chunks.
            if body.len() >= super::H1_WRITER_BUFFER_CAPACITY {
                let head = build_response_head(status, &headers, close_after, framing);
                writer.flush().await?;
                let mut slices = [
                    io::IoSlice::new(head.as_slice()),
                    io::IoSlice::new(body.as_ref()),
                ];
                write_all_vectored(writer.get_mut(), &mut slices).await?;
                return Ok(());
            }
            write_response_head(writer, status, &headers, close_after, framing).await?;
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
                    // Small files use the ordinary buffered-write path.
                    // Transports that cannot sendfile keep the same bounded
                    // rolling reader at every size instead of falling back to
                    // Tokio's generic 8 KiB copy buffer.
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
    writer.write_all(build_response_head(status, headers, close_after, framing).as_slice())
        .await?;
    Ok(())
}

fn build_response_head(
    status: HttpStatusCode,
    headers: &ResponseHeaders,
    close_after: bool,
    framing: BodyFraming,
) -> ResponseBuf {
    let framing = if status.forbids_content_length() {
        BodyFraming::None
    } else {
        framing
    };
    let mut out = ResponseBuf::new();
    append_status_line(&mut out, status);
    append_response_headers(&mut out, headers, close_after, framing);
    out
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
    // Caller already flushed the BufWriter so write order is preserved.
    write_all_vectored(writer.get_mut(), &mut slices).await?;
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
    trailers: &ResponseTrailers,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(b"0\r\n").await?;
    let mut out = ResponseBuf::new();
    append_response_fields(&mut out, trailers.as_fields());
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
    append_response_fields(dst, headers);
}

fn append_response_fields(dst: &mut ResponseBuf, headers: &[ResponseField]) {
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
    if headers
        .iter()
        .any(|(name, _)| name.as_bytes() == b"upgrade")
    {
        // Application `Upgrade` advertisements were admitted only after a
        // paired `Connection: upgrade` directive was validated at ingress.
        // The directive itself is transport-owned and was removed there, so
        // regenerate the fixed HTTP/1 syntax here.
        dst.extend_from_slice(b"connection: upgrade\r\n");
    }
    match framing {
        BodyFraming::KnownLength(len) => {
            append_header_line_with_decimal(dst, b"content-length", len);
        },
        BodyFraming::Chunked => {
            dst.extend_from_slice(b"transfer-encoding: chunked\r\n");
        },
        BodyFraming::None => {},
    }
    if close_after {
        dst.extend_from_slice(b"connection: close\r\n");
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
    use crate::http::types::{HttpStatusCode, common_status_codes, status_code};
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
    fn every_common_status_has_its_declared_http1_line() {
        let mut out = ResponseBuf::new();
        macro_rules! assert_common_status_lines {
            ($(($constant:ident, $line:expr, $hpack:expr)),+ $(,)?) => {
                $(
                    out.clear();
                    append_status_line(&mut out, status_code::$constant);
                    assert_eq!(out.as_slice(), $line, stringify!($constant));
                )+
            };
        }
        common_status_codes!(assert_common_status_lines);
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
    fn status_line_preserves_standard_canonical_reasons() {
        for code in 100..600 {
            let status = HttpStatusCode::new(code).unwrap();
            let expected = http::StatusCode::from_u16(code)
                .ok()
                .and_then(|status| status.canonical_reason())
                .map_or_else(
                    || format!("HTTP/1.1 {code} \r\n"),
                    |reason| format!("HTTP/1.1 {code} {reason}\r\n"),
                );
            let mut out = ResponseBuf::new();
            append_status_line(&mut out, status);
            assert_eq!(out.as_slice(), expected.as_bytes(), "{code}");
        }
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
