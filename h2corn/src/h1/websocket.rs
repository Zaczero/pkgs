use std::num::NonZeroUsize;
use std::sync::Arc;
use std::{io, mem};

use bytes::BytesMut;
use smallvec::SmallVec;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, BufWriter};

use super::http::{H1ResponseState, append_header_lines, write_empty_response};
use crate::config::ServerConfig;
use crate::error::H2CornError;
use crate::http::header::apply_default_response_headers;
use crate::http::response::{HttpResponseTransport, ResponseAction};
use crate::http::types::{HttpStatusCode, ResponseHeaders, status_code};
use crate::sendfile::WriteTarget;
use crate::websocket::session::{
    AcceptedWebSocketState, AcceptedWebSocketTransport, EncodedWebSocketFrame, FrameFlushMode,
    TransportRead, WebSocketContext, WebSocketHandshakeTransport, append_ws_accept_headers,
    run_websocket, take_pending_close_frame,
};
use crate::websocket::{WEBSOCKET_KEY_LEN, WebSocketCodec, WebSocketKey, websocket_accept};

const INITIAL_FRAME_BUF_CAPACITY: usize = 256;
/// Replacement capacity for the WebSocket read buffer. The buffer is handed to
/// the codec whole on every read so the payload can be unmasked in place, so a
/// fresh one is allocated each time rather than carved from a shared tail.
const READ_BUF_CAPACITY: usize = 4096;
const HANDSHAKE_BUF_CAPACITY: usize = 512;

type HandshakeBuf = SmallVec<[u8; HANDSHAKE_BUF_CAPACITY]>;

struct H1WebSocketTransport<R, W> {
    config: Arc<ServerConfig>,
    key: WebSocketKey,
    reader: R,
    buffer: BytesMut,
    frame_buf: BytesMut,
    pending_frames: SmallVec<[EncodedWebSocketFrame; 4]>,
    response: H1ResponseState,
    writer: BufWriter<W>,
}

impl<R, W> WebSocketHandshakeTransport for H1WebSocketTransport<R, W>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: WriteTarget,
{
    fn accept_status(&self) -> HttpStatusCode {
        status_code::SWITCHING_PROTOCOLS
    }

    async fn send_empty_response(&mut self, status: HttpStatusCode) -> Result<(), H2CornError> {
        write_empty_response(&mut self.writer, &self.config, status, true).await
    }

    async fn send_accept(
        &mut self,
        subprotocol: Option<&str>,
        headers: ResponseHeaders,
        per_message_deflate: bool,
    ) -> Result<(), H2CornError> {
        write_websocket_accept(
            &mut self.writer,
            &self.config,
            &self.key,
            subprotocol,
            headers,
            per_message_deflate,
        )
        .await
    }
}

impl<R, W> HttpResponseTransport for H1WebSocketTransport<R, W>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: WriteTarget,
{
    async fn apply_response_action(&mut self, action: ResponseAction) -> Result<(), H2CornError> {
        self.response
            .apply_response_action(&mut self.writer, &self.config, action)
            .await
    }

    async fn flush_buffered(&mut self) -> Result<(), H2CornError> {
        self.response.flush_buffered(&mut self.writer).await
    }
}

impl<R, W> AcceptedWebSocketTransport for H1WebSocketTransport<R, W>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    fn websocket_codec(&mut self, max_message_size: Option<NonZeroUsize>) -> WebSocketCodec {
        let mut codec = WebSocketCodec::with_options(max_message_size);
        codec.push_segment(mem::take(&mut self.buffer).freeze());
        codec
    }

    async fn send_frame(
        &mut self,
        frame: EncodedWebSocketFrame,
        flush: FrameFlushMode,
    ) -> Result<(), H2CornError> {
        self.pending_frames.push(frame);
        if flush == FrameFlushMode::Immediate {
            self.flush_buffered_frames().await?;
        }
        Ok(())
    }

    async fn flush_buffered_frames(&mut self) -> Result<(), H2CornError> {
        if self.pending_frames.is_empty() {
            return Ok(());
        }
        write_websocket_frames_vectored(&mut self.writer, &self.pending_frames).await?;
        self.pending_frames.clear();
        Ok(())
    }

    fn frame_buf(&mut self) -> &mut BytesMut {
        &mut self.frame_buf
    }

    async fn read_into_codec(
        &mut self,
        codec: &mut WebSocketCodec,
    ) -> Result<TransportRead, H2CornError> {
        let read = self.reader.read_buf(&mut self.buffer).await?;
        if read == 0 {
            return Ok(TransportRead::PeerGone);
        }
        hand_read_buffer_to_codec(&mut self.buffer, codec);
        Ok(TransportRead::Progress)
    }

    async fn finish_session(
        &mut self,
        state: &mut AcceptedWebSocketState,
    ) -> Result<(), H2CornError> {
        let Some(frame) = take_pending_close_frame(state, self.frame_buf())? else {
            debug_assert!(!state.has_queued_close());
            return Ok(());
        };
        self.send_frame(
            EncodedWebSocketFrame::Contiguous(frame),
            FrameFlushMode::Immediate,
        )
        .await?;
        Ok(())
    }
}

/// Hand the accumulated read buffer to the codec, retaining no handle to it.
///
/// The codec unmasks a payload in place only when it solely owns the segment.
/// `BytesMut::split()` reads like a handover but promotes the allocation to
/// shared and leaves the caller holding the second handle, which silently
/// disables that for the whole session and costs an allocation plus a full
/// payload copy per message. Replacing the buffer outright is what keeps the
/// guarantee, so the ownership transfer lives here rather than inline.
fn hand_read_buffer_to_codec(buffer: &mut BytesMut, codec: &mut WebSocketCodec) {
    codec.push_segment(mem::replace(buffer, BytesMut::with_capacity(READ_BUF_CAPACITY)).freeze());
}

pub(super) async fn handle_request<R, W>(
    context: WebSocketContext,
    key: WebSocketKey,
    reader: R,
    buffer: BytesMut,
    writer: BufWriter<W>,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: WriteTarget,
{
    let mut transport = H1WebSocketTransport {
        config: Arc::clone(&context.request.connection.config),
        key,
        reader,
        buffer,
        frame_buf: BytesMut::with_capacity(INITIAL_FRAME_BUF_CAPACITY),
        pending_frames: SmallVec::new(),
        response: H1ResponseState::new(true),
        writer,
    };
    run_websocket(&mut transport, context).await
}

async fn write_websocket_frames_vectored<W>(
    writer: &mut BufWriter<W>,
    frames: &[EncodedWebSocketFrame],
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    writer.flush().await?;
    let mut slices = SmallVec::<[io::IoSlice<'_>; 8]>::new();
    for frame in frames {
        let (header, payload) = frame.segments();
        slices.push(io::IoSlice::new(header));
        if let Some(payload) = payload.filter(|payload| !payload.is_empty()) {
            slices.push(io::IoSlice::new(payload));
        }
    }
    crate::async_util::write_all_vectored(writer.get_mut(), slices.as_mut_slice()).await?;
    writer.get_mut().flush().await?;
    Ok(())
}

async fn write_websocket_accept<W>(
    writer: &mut BufWriter<W>,
    config: &ServerConfig,
    key: &[u8; WEBSOCKET_KEY_LEN],
    subprotocol: Option<&str>,
    mut headers: ResponseHeaders,
    per_message_deflate: bool,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    let mut out = HandshakeBuf::new();
    out.extend_from_slice(b"HTTP/1.1 101 Switching Protocols\r\n");
    out.extend_from_slice(b"Connection: Upgrade\r\n");
    out.extend_from_slice(b"Upgrade: websocket\r\n");
    out.extend_from_slice(b"Sec-WebSocket-Accept: ");
    out.extend_from_slice(&websocket_accept(key));
    out.extend_from_slice(b"\r\n");
    append_ws_accept_headers(&mut headers, subprotocol, per_message_deflate);
    apply_default_response_headers(&mut headers, config);
    append_header_lines(&mut out, &headers);
    out.extend_from_slice(b"\r\n");
    writer.write_all(out.as_slice()).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use bytes::Bytes;
    use tokio::io::{AsyncWrite, BufWriter};

    use super::write_websocket_frames_vectored;
    use crate::bridge::PayloadBytes;
    use crate::websocket::session::EncodedWebSocketFrame;

    #[derive(Default)]
    struct PartialVectoredWriter {
        bytes: Vec<u8>,
        vectored_calls: usize,
    }

    impl AsyncWrite for PartialVectoredWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let written = buf.len().min(3);
            self.bytes.extend_from_slice(&buf[..written]);
            Poll::Ready(Ok(written))
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[io::IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            self.vectored_calls += 1;
            let mut remaining = 3;
            let mut written = 0;
            for buf in bufs {
                let take = remaining.min(buf.len());
                self.bytes.extend_from_slice(&buf[..take]);
                written += take;
                remaining -= take;
                if remaining == 0 {
                    break;
                }
            }
            Poll::Ready(Ok(written))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }
    }

    /// The read loop must hand the codec a buffer it keeps no handle to.
    ///
    /// `BytesMut::split()` looks like a handover but promotes the allocation to
    /// shared and leaves the session holding the second handle for the rest of
    /// the connection, so `is_unique()` was permanently false and the codec's
    /// in-place unmask never fired. The test that guarded that optimisation
    /// built its own vector-backed `Bytes`, which is unique whatever the
    /// transport does, so it passed throughout. Reintroducing `split()` here
    /// turns this red: splitting a vector-backed buffer promotes it too.
    #[test]
    fn the_read_loop_hands_the_codec_a_solely_owned_buffer() {
        let mut buffer = bytes::BytesMut::from(&[0x82, 0x81, 1, 2, 3, 4, 5][..]);
        let mut codec = crate::websocket::WebSocketCodec::default();

        super::hand_read_buffer_to_codec(&mut buffer, &mut codec);

        assert_eq!(
            codec.front_segment_is_unique(),
            Some(true),
            "the session still holds a handle to the buffer it handed over"
        );
    }

    #[tokio::test]
    async fn h1_vectored_frames_preserve_segments_and_handle_partial_writes() {
        let payload = Bytes::from_static(b"payload");
        let payload_ptr = payload.as_ptr();
        let frames = [
            EncodedWebSocketFrame::Contiguous(Bytes::from_static(b"\x89\x00")),
            EncodedWebSocketFrame::segmented(0x2, PayloadBytes::from(payload), false),
        ];
        assert_eq!(frames[1].segments().1.unwrap().as_ptr(), payload_ptr);

        let mut writer = BufWriter::new(PartialVectoredWriter::default());
        write_websocket_frames_vectored(&mut writer, &frames)
            .await
            .unwrap();

        assert_eq!(writer.get_ref().bytes, b"\x89\x00\x82\x07payload");
        assert!(writer.get_ref().vectored_calls > 1);
    }
}
