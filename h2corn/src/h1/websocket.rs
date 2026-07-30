#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "the generated SHA-1 macros stay next to their WebSocket accept call site"
)]

mod accept {
    use crate::websocket::WEBSOCKET_KEY_LEN;

    pub(super) const LEN: usize = 28;
    pub(super) const GUID: &[u8; 36] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    pub(super) const INPUT_LEN: usize = WEBSOCKET_KEY_LEN + GUID.len();
    pub(super) const INITIAL_STATE: [u32; 5] = [
        0x6745_2301_u32,
        0xEFCD_AB89,
        0x98BA_DCFE,
        0x1032_5476,
        0xC3D2_E1F0,
    ];
    pub(super) const FIRST_BLOCK_TEMPLATE: [u32; 16] = {
        let mut block = [0; 16];
        let mut index = 0;
        while index < GUID.len() / 4 {
            let offset = index * 4;
            block[WEBSOCKET_KEY_LEN / 4 + index] = u32::from_be_bytes([
                GUID[offset],
                GUID[offset + 1],
                GUID[offset + 2],
                GUID[offset + 3],
            ]);
            index += 1;
        }
        block[15] = 0x8000_0000;
        block
    };
    #[cfg(test)]
    pub(super) const SECOND_BLOCK: [u32; 16] = {
        let mut block = [0; 16];
        block[15] = (INPUT_LEN as u32) << 3;
        block
    };
}

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::{io, mem};

use bytes::BytesMut;
use smallvec::SmallVec;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};

use super::http::{H1ResponseState, append_header_lines, write_empty_response};
use crate::base64;
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
use crate::websocket::{WEBSOCKET_KEY_LEN, WebSocketCodec, WebSocketKey};

const INITIAL_FRAME_BUF_CAPACITY: usize = 256;
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
        codec.push_segment(self.buffer.split().freeze());
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

/// One SHA-1 round with the register roles made explicit. `sha1_compress!`
/// expands every schedule update and round at its call site: the WebSocket
/// accept input is always exactly two blocks, so a generic round loop would
/// only add branches and indexed schedule traffic to this fixed operation.
macro_rules! sha1_round {
    ($a:ident, $b:ident, $c:ident, $d:ident, $e:ident; $word:expr; $mix:expr; $round_constant:expr) => {{
        let temp = $a
            .rotate_left(5)
            .wrapping_add($mix)
            .wrapping_add($e)
            .wrapping_add($round_constant)
            .wrapping_add($word);
        $e = $d;
        $d = $c;
        $c = $b.rotate_left(30);
        $b = $a;
        $a = temp;
    }};
}

/// Expands one SHA-1 compression with its sixteen-word ring in registers.
/// The macro is invoked for both known-size blocks in `websocket_accept`,
/// yielding all 160 rounds directly in that hot function.
macro_rules! sha1_compress {
    ($a:ident, $b:ident, $c:ident, $d:ident, $e:ident; $w0:ident, $w1:ident, $w2:ident, $w3:ident, $w4:ident, $w5:ident, $w6:ident, $w7:ident, $w8:ident, $w9:ident, $w10:ident, $w11:ident, $w12:ident, $w13:ident, $w14:ident, $w15:ident) => {
        sha1_round!($a, $b, $c, $d, $e; $w0; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w1; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w2; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w3; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w4; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w5; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w6; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w7; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w8; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w9; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w10; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w11; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w12; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w13; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w14; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        sha1_round!($a, $b, $c, $d, $e; $w15; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        $w0 = ($w13 ^ $w8 ^ $w2 ^ $w0).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w0; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        $w1 = ($w14 ^ $w9 ^ $w3 ^ $w1).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w1; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        $w2 = ($w15 ^ $w10 ^ $w4 ^ $w2).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w2; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        $w3 = ($w0 ^ $w11 ^ $w5 ^ $w3).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w3; ($b & $c) | ((!$b) & $d); 0x5A82_7999);
        $w4 = ($w1 ^ $w12 ^ $w6 ^ $w4).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w4; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w5 = ($w2 ^ $w13 ^ $w7 ^ $w5).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w5; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w6 = ($w3 ^ $w14 ^ $w8 ^ $w6).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w6; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w7 = ($w4 ^ $w15 ^ $w9 ^ $w7).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w7; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w8 = ($w5 ^ $w0 ^ $w10 ^ $w8).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w8; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w9 = ($w6 ^ $w1 ^ $w11 ^ $w9).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w9; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w10 = ($w7 ^ $w2 ^ $w12 ^ $w10).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w10; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w11 = ($w8 ^ $w3 ^ $w13 ^ $w11).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w11; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w12 = ($w9 ^ $w4 ^ $w14 ^ $w12).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w12; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w13 = ($w10 ^ $w5 ^ $w15 ^ $w13).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w13; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w14 = ($w11 ^ $w6 ^ $w0 ^ $w14).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w14; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w15 = ($w12 ^ $w7 ^ $w1 ^ $w15).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w15; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w0 = ($w13 ^ $w8 ^ $w2 ^ $w0).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w0; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w1 = ($w14 ^ $w9 ^ $w3 ^ $w1).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w1; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w2 = ($w15 ^ $w10 ^ $w4 ^ $w2).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w2; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w3 = ($w0 ^ $w11 ^ $w5 ^ $w3).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w3; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w4 = ($w1 ^ $w12 ^ $w6 ^ $w4).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w4; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w5 = ($w2 ^ $w13 ^ $w7 ^ $w5).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w5; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w6 = ($w3 ^ $w14 ^ $w8 ^ $w6).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w6; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w7 = ($w4 ^ $w15 ^ $w9 ^ $w7).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w7; $b ^ $c ^ $d; 0x6ED9_EBA1);
        $w8 = ($w5 ^ $w0 ^ $w10 ^ $w8).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w8; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w9 = ($w6 ^ $w1 ^ $w11 ^ $w9).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w9; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w10 = ($w7 ^ $w2 ^ $w12 ^ $w10).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w10; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w11 = ($w8 ^ $w3 ^ $w13 ^ $w11).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w11; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w12 = ($w9 ^ $w4 ^ $w14 ^ $w12).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w12; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w13 = ($w10 ^ $w5 ^ $w15 ^ $w13).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w13; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w14 = ($w11 ^ $w6 ^ $w0 ^ $w14).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w14; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w15 = ($w12 ^ $w7 ^ $w1 ^ $w15).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w15; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w0 = ($w13 ^ $w8 ^ $w2 ^ $w0).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w0; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w1 = ($w14 ^ $w9 ^ $w3 ^ $w1).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w1; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w2 = ($w15 ^ $w10 ^ $w4 ^ $w2).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w2; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w3 = ($w0 ^ $w11 ^ $w5 ^ $w3).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w3; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w4 = ($w1 ^ $w12 ^ $w6 ^ $w4).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w4; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w5 = ($w2 ^ $w13 ^ $w7 ^ $w5).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w5; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w6 = ($w3 ^ $w14 ^ $w8 ^ $w6).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w6; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w7 = ($w4 ^ $w15 ^ $w9 ^ $w7).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w7; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w8 = ($w5 ^ $w0 ^ $w10 ^ $w8).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w8; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w9 = ($w6 ^ $w1 ^ $w11 ^ $w9).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w9; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w10 = ($w7 ^ $w2 ^ $w12 ^ $w10).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w10; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w11 = ($w8 ^ $w3 ^ $w13 ^ $w11).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w11; ($b & $c) | ($b & $d) | ($c & $d); 0x8F1B_BCDC);
        $w12 = ($w9 ^ $w4 ^ $w14 ^ $w12).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w12; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w13 = ($w10 ^ $w5 ^ $w15 ^ $w13).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w13; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w14 = ($w11 ^ $w6 ^ $w0 ^ $w14).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w14; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w15 = ($w12 ^ $w7 ^ $w1 ^ $w15).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w15; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w0 = ($w13 ^ $w8 ^ $w2 ^ $w0).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w0; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w1 = ($w14 ^ $w9 ^ $w3 ^ $w1).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w1; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w2 = ($w15 ^ $w10 ^ $w4 ^ $w2).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w2; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w3 = ($w0 ^ $w11 ^ $w5 ^ $w3).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w3; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w4 = ($w1 ^ $w12 ^ $w6 ^ $w4).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w4; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w5 = ($w2 ^ $w13 ^ $w7 ^ $w5).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w5; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w6 = ($w3 ^ $w14 ^ $w8 ^ $w6).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w6; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w7 = ($w4 ^ $w15 ^ $w9 ^ $w7).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w7; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w8 = ($w5 ^ $w0 ^ $w10 ^ $w8).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w8; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w9 = ($w6 ^ $w1 ^ $w11 ^ $w9).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w9; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w10 = ($w7 ^ $w2 ^ $w12 ^ $w10).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w10; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w11 = ($w8 ^ $w3 ^ $w13 ^ $w11).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w11; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w12 = ($w9 ^ $w4 ^ $w14 ^ $w12).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w12; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w13 = ($w10 ^ $w5 ^ $w15 ^ $w13).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w13; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w14 = ($w11 ^ $w6 ^ $w0 ^ $w14).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w14; $b ^ $c ^ $d; 0xCA62_C1D6);
        $w15 = ($w12 ^ $w7 ^ $w1 ^ $w15).rotate_left(1);
        sha1_round!($a, $b, $c, $d, $e; $w15; $b ^ $c ^ $d; 0xCA62_C1D6);
    };
}

fn websocket_accept(key: &WebSocketKey) -> [u8; accept::LEN] {
    let mut h0 = accept::INITIAL_STATE[0];
    let mut h1 = accept::INITIAL_STATE[1];
    let mut h2 = accept::INITIAL_STATE[2];
    let mut h3 = accept::INITIAL_STATE[3];
    let mut h4 = accept::INITIAL_STATE[4];

    let mut w0 = u32::from_be_bytes([key[0], key[1], key[2], key[3]]);
    let mut w1 = u32::from_be_bytes([key[4], key[5], key[6], key[7]]);
    let mut w2 = u32::from_be_bytes([key[8], key[9], key[10], key[11]]);
    let mut w3 = u32::from_be_bytes([key[12], key[13], key[14], key[15]]);
    let mut w4 = u32::from_be_bytes([key[16], key[17], key[18], key[19]]);
    let mut w5 = u32::from_be_bytes([key[20], key[21], key[22], key[23]]);
    let mut w6 = accept::FIRST_BLOCK_TEMPLATE[6];
    let mut w7 = accept::FIRST_BLOCK_TEMPLATE[7];
    let mut w8 = accept::FIRST_BLOCK_TEMPLATE[8];
    let mut w9 = accept::FIRST_BLOCK_TEMPLATE[9];
    let mut w10 = accept::FIRST_BLOCK_TEMPLATE[10];
    let mut w11 = accept::FIRST_BLOCK_TEMPLATE[11];
    let mut w12 = accept::FIRST_BLOCK_TEMPLATE[12];
    let mut w13 = accept::FIRST_BLOCK_TEMPLATE[13];
    let mut w14 = accept::FIRST_BLOCK_TEMPLATE[14];
    let mut w15 = accept::FIRST_BLOCK_TEMPLATE[15];
    sha1_compress!(
        h0, h1, h2, h3, h4;
        w0, w1, w2, w3, w4, w5, w6, w7, w8, w9, w10, w11, w12, w13, w14, w15
    );

    let state0 = accept::INITIAL_STATE[0].wrapping_add(h0);
    let state1 = accept::INITIAL_STATE[1].wrapping_add(h1);
    let state2 = accept::INITIAL_STATE[2].wrapping_add(h2);
    let state3 = accept::INITIAL_STATE[3].wrapping_add(h3);
    let state4 = accept::INITIAL_STATE[4].wrapping_add(h4);
    let mut h0 = state0;
    let mut h1 = state1;
    let mut h2 = state2;
    let mut h3 = state3;
    let mut h4 = state4;
    w0 = 0;
    w1 = 0;
    w2 = 0;
    w3 = 0;
    w4 = 0;
    w5 = 0;
    w6 = 0;
    w7 = 0;
    w8 = 0;
    w9 = 0;
    w10 = 0;
    w11 = 0;
    w12 = 0;
    w13 = 0;
    w14 = 0;
    w15 = (accept::INPUT_LEN as u32) << 3;
    sha1_compress!(
        h0, h1, h2, h3, h4;
        w0, w1, w2, w3, w4, w5, w6, w7, w8, w9, w10, w11, w12, w13, w14, w15
    );

    let [w0, w1, w2, w3, w4] = [
        state0.wrapping_add(h0),
        state1.wrapping_add(h1),
        state2.wrapping_add(h2),
        state3.wrapping_add(h3),
        state4.wrapping_add(h4),
    ];
    let mut out = [0_u8; accept::LEN];
    let (out_groups, out_remainder) = out.as_chunks_mut::<4>();
    debug_assert!(out_remainder.is_empty());
    out_groups[0] = encode_b64_triplet((w0 >> 24) as u8, (w0 >> 16) as u8, (w0 >> 8) as u8);
    out_groups[1] = encode_b64_triplet(w0 as u8, (w1 >> 24) as u8, (w1 >> 16) as u8);
    out_groups[2] = encode_b64_triplet((w1 >> 8) as u8, w1 as u8, (w2 >> 24) as u8);
    out_groups[3] = encode_b64_triplet((w2 >> 16) as u8, (w2 >> 8) as u8, w2 as u8);
    out_groups[4] = encode_b64_triplet((w3 >> 24) as u8, (w3 >> 16) as u8, (w3 >> 8) as u8);
    out_groups[5] = encode_b64_triplet(w3 as u8, (w4 >> 24) as u8, (w4 >> 16) as u8);
    out_groups[6] = encode_b64_remainder((w4 >> 8) as u8, w4 as u8);

    out
}

#[cfg(test)]
fn reference_websocket_accept(key: &WebSocketKey) -> [u8; accept::LEN] {
    let mut state = accept::INITIAL_STATE;
    let mut first = accept::FIRST_BLOCK_TEMPLATE;
    let (key_words, remainder) = key.as_chunks::<4>();
    debug_assert!(remainder.is_empty());
    for (dst, word) in first[..6].iter_mut().zip(key_words) {
        *dst = u32::from_be_bytes(*word);
    }
    reference_compress_sha1_block(&mut state, first);
    reference_compress_sha1_block(&mut state, accept::SECOND_BLOCK);

    let [w0, w1, w2, w3, w4] = state;
    let mut out = [0_u8; accept::LEN];
    let (out_groups, out_remainder) = out.as_chunks_mut::<4>();
    debug_assert!(out_remainder.is_empty());
    out_groups[0] = encode_b64_triplet((w0 >> 24) as u8, (w0 >> 16) as u8, (w0 >> 8) as u8);
    out_groups[1] = encode_b64_triplet(w0 as u8, (w1 >> 24) as u8, (w1 >> 16) as u8);
    out_groups[2] = encode_b64_triplet((w1 >> 8) as u8, w1 as u8, (w2 >> 24) as u8);
    out_groups[3] = encode_b64_triplet((w2 >> 16) as u8, (w2 >> 8) as u8, w2 as u8);
    out_groups[4] = encode_b64_triplet((w3 >> 24) as u8, (w3 >> 16) as u8, (w3 >> 8) as u8);
    out_groups[5] = encode_b64_triplet(w3 as u8, (w4 >> 24) as u8, (w4 >> 16) as u8);
    out_groups[6] = encode_b64_remainder((w4 >> 8) as u8, w4 as u8);
    out
}

#[cfg(test)]
fn reference_compress_sha1_block(state: &mut [u32; 5], block: [u32; 16]) {
    let mut words = block;
    let [mut h0, mut h1, mut h2, mut h3, mut h4] = *state;

    for index in 0..80 {
        let word = if index < 16 {
            words[index]
        } else {
            let word = (words[(index + 13) & 15]
                ^ words[(index + 8) & 15]
                ^ words[(index + 2) & 15]
                ^ words[index & 15])
                .rotate_left(1);
            words[index & 15] = word;
            word
        };
        let (mix, round_constant) = match index {
            0..=19 => ((h1 & h2) | ((!h1) & h3), 0x5A82_7999),
            20..=39 => (h1 ^ h2 ^ h3, 0x6ED9_EBA1),
            40..=59 => ((h1 & h2) | (h1 & h3) | (h2 & h3), 0x8F1B_BCDC),
            _ => (h1 ^ h2 ^ h3, 0xCA62_C1D6),
        };
        let temp = h0
            .rotate_left(5)
            .wrapping_add(mix)
            .wrapping_add(h4)
            .wrapping_add(round_constant)
            .wrapping_add(word);
        h4 = h3;
        h3 = h2;
        h2 = h1.rotate_left(30);
        h1 = h0;
        h0 = temp;
    }

    state[0] = state[0].wrapping_add(h0);
    state[1] = state[1].wrapping_add(h1);
    state[2] = state[2].wrapping_add(h2);
    state[3] = state[3].wrapping_add(h3);
    state[4] = state[4].wrapping_add(h4);
}

const fn encode_b64_triplet(b0: u8, b1: u8, b2: u8) -> [u8; 4] {
    [
        base64::TABLE[(b0 >> 2) as usize],
        base64::TABLE[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize],
        base64::TABLE[(((b1 & 0x0F) << 2) | (b2 >> 6)) as usize],
        base64::TABLE[(b2 & 0x3F) as usize],
    ]
}

const fn encode_b64_remainder(b0: u8, b1: u8) -> [u8; 4] {
    [
        base64::TABLE[(b0 >> 2) as usize],
        base64::TABLE[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize],
        base64::TABLE[((b1 & 0x0F) << 2) as usize],
        b'=',
    ]
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use bytes::Bytes;
    use tokio::io::{AsyncWrite, BufWriter};

    use super::{reference_websocket_accept, websocket_accept, write_websocket_frames_vectored};
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

    #[test]
    fn websocket_accept_matches_rfc_example() {
        assert_eq!(
            websocket_accept(b"dGhlIHNhbXBsZSBub25jZQ=="),
            *b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=",
        );
    }

    #[test]
    fn websocket_accept_matches_reference_for_one_million_varied_keys() {
        let mut state = 0xD1B5_4A32_D192_ED03_u64;
        let mut key = [0_u8; crate::websocket::WEBSOCKET_KEY_LEN];
        for _ in 0..1_000_000 {
            for byte in &mut key {
                // SplitMix64 gives every byte position a varied stream without
                // introducing a test-only randomness dependency.
                state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
                let mut mixed = state;
                mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
                *byte = (mixed ^ (mixed >> 31)) as u8;
            }
            assert_eq!(websocket_accept(&key), reference_websocket_accept(&key));
        }
    }
}
