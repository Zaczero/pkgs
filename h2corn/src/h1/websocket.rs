use std::{mem, num::NonZeroUsize};

use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};

use super::http::{
    BodyFraming, H1WriteTarget, append_header_lines, finish_chunked_response, write_chunk,
    write_empty_response, write_final_response, write_response_head,
};
use crate::bridge::PayloadBytes;
use crate::config::ServerConfig;
use crate::error::H2CornError;
use crate::http::header::apply_default_response_headers;
use crate::http::response::FinalResponseBody;
use crate::http::types::{HttpStatusCode, ResponseHeaders, status_code};
use crate::websocket::session::{
    AcceptedWebSocketState, AcceptedWebSocketTransport, CloseState, FrameFlushMode, TransportRead,
    WebSocketContext, WebSocketHandshakeTransport, append_ws_accept_headers, run_websocket,
    take_pending_close_frame,
};
use crate::websocket::{WEBSOCKET_KEY_LEN, WebSocketCodec, WebSocketKey};

const INITIAL_FRAME_BUF_CAPACITY: usize = 256;
const HANDSHAKE_BUF_CAPACITY: usize = 512;

type HandshakeBuf = SmallVec<[u8; HANDSHAKE_BUF_CAPACITY]>;

pub(super) async fn handle_request<R, W>(
    context: WebSocketContext,
    key: WebSocketKey,
    reader: R,
    buffer: BytesMut,
    writer: BufWriter<W>,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: H1WriteTarget,
{
    let mut transport = H1WebSocketTransport {
        config: context.request.connection.config,
        key,
        reader,
        buffer,
        frame_buf: BytesMut::with_capacity(INITIAL_FRAME_BUF_CAPACITY),
        frame_flush_pending: false,
        writer,
    };
    run_websocket(&mut transport, context).await
}

struct H1WebSocketTransport<R, W> {
    config: &'static ServerConfig,
    key: WebSocketKey,
    reader: R,
    buffer: BytesMut,
    frame_buf: BytesMut,
    frame_flush_pending: bool,
    writer: BufWriter<W>,
}

impl<R, W> WebSocketHandshakeTransport for H1WebSocketTransport<R, W>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: H1WriteTarget,
{
    fn accept_status(&self) -> HttpStatusCode {
        status_code::SWITCHING_PROTOCOLS
    }

    async fn send_empty_response(&mut self, status: HttpStatusCode) -> Result<(), H2CornError> {
        write_empty_response(&mut self.writer, self.config, status, true).await
    }

    async fn send_accept(
        &mut self,
        subprotocol: Option<&str>,
        headers: ResponseHeaders,
        per_message_deflate: bool,
    ) -> Result<(), H2CornError> {
        write_websocket_accept(
            &mut self.writer,
            self.config,
            &self.key,
            subprotocol,
            headers,
            per_message_deflate,
        )
        .await
    }

    async fn send_final_denial_response(
        &mut self,
        status: HttpStatusCode,
        headers: ResponseHeaders,
        body: FinalResponseBody,
    ) -> Result<(), H2CornError> {
        write_final_response(&mut self.writer, status, headers, body, true).await
    }

    async fn start_denial_response(
        &mut self,
        status: HttpStatusCode,
        mut headers: ResponseHeaders,
    ) -> Result<(), H2CornError> {
        apply_default_response_headers(&mut headers, self.config);
        write_response_head(
            &mut self.writer,
            status,
            &headers,
            true,
            BodyFraming::Chunked,
        )
        .await
    }

    async fn send_denial_body(&mut self, body: PayloadBytes) -> Result<(), H2CornError> {
        if !body.is_empty() {
            write_chunk(&mut self.writer, body.as_ref()).await?;
        }
        Ok(())
    }

    async fn finish_denial_response(&mut self) -> Result<(), H2CornError> {
        finish_chunked_response(&mut self.writer).await
    }
}

impl<R, W> AcceptedWebSocketTransport for H1WebSocketTransport<R, W>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    fn websocket_codec(&mut self, max_message_size: Option<NonZeroUsize>) -> WebSocketCodec {
        let mut codec = WebSocketCodec::segmented(max_message_size);
        codec.push_segment(mem::take(&mut self.buffer).freeze());
        codec
    }

    async fn send_frame(&mut self, frame: Bytes, flush: FrameFlushMode) -> Result<(), H2CornError> {
        self.writer.write_all(frame.as_ref()).await?;
        if flush == FrameFlushMode::Immediate {
            self.writer.flush().await?;
            self.frame_flush_pending = false;
        } else {
            self.frame_flush_pending = true;
        }
        Ok(())
    }

    async fn flush_buffered_frames(&mut self) -> Result<(), H2CornError> {
        if !self.frame_flush_pending {
            return Ok(());
        }
        self.writer.flush().await?;
        self.frame_flush_pending = false;
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
            assert!(state.close_state != CloseState::CloseQueued);
            return Ok(());
        };
        self.send_frame(frame, FrameFlushMode::Immediate).await?;
        Ok(())
    }
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

const WEBSOCKET_ACCEPT_LEN: usize = 28;
const WEBSOCKET_ACCEPT_GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const B64_TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn websocket_accept(key: &WebSocketKey) -> [u8; WEBSOCKET_ACCEPT_LEN] {
    const INPUT_LEN: usize = WEBSOCKET_KEY_LEN + WEBSOCKET_ACCEPT_GUID.len();
    let mut state = [
        0x6745_2301_u32,
        0xEFCD_AB89,
        0x98BA_DCFE,
        0x1032_5476,
        0xC3D2_E1F0,
    ];

    let mut first = [0_u32; 16];
    let (key_words, remainder) = key.as_chunks::<4>();
    debug_assert!(remainder.is_empty());
    for (dst, word) in first[..6].iter_mut().zip(key_words) {
        *dst = u32::from_be_bytes(*word);
    }
    let (guid_words, remainder) = WEBSOCKET_ACCEPT_GUID.as_chunks::<4>();
    debug_assert!(remainder.is_empty());
    for (dst, word) in first[6..15].iter_mut().zip(guid_words) {
        *dst = u32::from_be_bytes(*word);
    }
    first[15] = 0x8000_0000;
    compress_sha1_block(&mut state, first);

    let mut second = [0_u32; 16];
    second[15] = (INPUT_LEN as u32) << 3;
    compress_sha1_block(&mut state, second);

    let [w0, w1, w2, w3, w4] = state;
    let mut out = [0_u8; WEBSOCKET_ACCEPT_LEN];
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

fn compress_sha1_block(state: &mut [u32; 5], block: [u32; 16]) {
    let mut words = block;
    let [mut a, mut b, mut c, mut d, mut e] = *state;

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
        let (f, k) = match index {
            0..=19 => ((b & c) | ((!b) & d), 0x5A82_7999),
            20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
            40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
            _ => (b ^ c ^ d, 0xCA62_C1D6),
        };
        let temp = a
            .rotate_left(5)
            .wrapping_add(f)
            .wrapping_add(e)
            .wrapping_add(k)
            .wrapping_add(word);
        e = d;
        d = c;
        c = b.rotate_left(30);
        b = a;
        a = temp;
    }

    state[0] = state[0].wrapping_add(a);
    state[1] = state[1].wrapping_add(b);
    state[2] = state[2].wrapping_add(c);
    state[3] = state[3].wrapping_add(d);
    state[4] = state[4].wrapping_add(e);
}

fn encode_b64_triplet(b0: u8, b1: u8, b2: u8) -> [u8; 4] {
    [
        B64_TABLE[(b0 >> 2) as usize],
        B64_TABLE[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize],
        B64_TABLE[(((b1 & 0x0F) << 2) | (b2 >> 6)) as usize],
        B64_TABLE[(b2 & 0x3F) as usize],
    ]
}

fn encode_b64_remainder(b0: u8, b1: u8) -> [u8; 4] {
    [
        B64_TABLE[(b0 >> 2) as usize],
        B64_TABLE[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize],
        B64_TABLE[((b1 & 0x0F) << 2) as usize],
        b'=',
    ]
}

#[cfg(test)]
mod tests {
    use super::websocket_accept;

    #[test]
    fn websocket_accept_matches_rfc_example() {
        assert_eq!(
            websocket_accept(b"dGhlIHNhbXBsZSBub25jZQ=="),
            *b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=",
        );
    }
}
