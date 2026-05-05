use std::num::NonZeroUsize;

use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;

use super::ConnectionHandle;
use super::http::send_final_response;
use crate::bridge::PayloadBytes;
use crate::error::H2CornError;
use crate::frame::{ErrorCode, StreamId};
use crate::http::response::FinalResponseBody;
use crate::http::types::{HttpStatusCode, ResponseHeaders, status_code};
use crate::runtime::StreamInput;
use crate::websocket::WebSocketCodec;
use crate::websocket::session::{
    AcceptedWebSocketState, AcceptedWebSocketTransport, CloseState, FrameFlushMode, TransportRead,
    WebSocketContext, WebSocketHandshakeTransport, append_ws_accept_headers, run_websocket,
    take_pending_close_frame,
};

const INITIAL_FRAME_BUF_CAPACITY: usize = 256;

struct H2WebSocketTransport {
    connection: ConnectionHandle,
    stream_id: StreamId,
    stream_rx: mpsc::Receiver<StreamInput>,
    frame_buf: BytesMut,
}

impl WebSocketHandshakeTransport for H2WebSocketTransport {
    fn accept_status(&self) -> HttpStatusCode {
        status_code::OK
    }

    async fn send_empty_response(&mut self, status: HttpStatusCode) -> Result<(), H2CornError> {
        self.connection
            .send_headers(self.stream_id, status, ResponseHeaders::new(), true)
            .await
    }

    async fn send_accept(
        &mut self,
        subprotocol: Option<&str>,
        headers: ResponseHeaders,
        per_message_deflate: bool,
    ) -> Result<(), H2CornError> {
        let mut response_headers = headers;
        append_ws_accept_headers(&mut response_headers, subprotocol, per_message_deflate);
        self.connection
            .send_headers(self.stream_id, status_code::OK, response_headers, false)
            .await
    }

    async fn send_final_denial_response(
        &mut self,
        status: HttpStatusCode,
        headers: ResponseHeaders,
        body: FinalResponseBody,
    ) -> Result<(), H2CornError> {
        send_final_response(&self.connection, self.stream_id, status, headers, body).await
    }

    async fn start_denial_response(
        &mut self,
        status: HttpStatusCode,
        headers: ResponseHeaders,
    ) -> Result<(), H2CornError> {
        self.connection
            .send_headers(self.stream_id, status, headers, false)
            .await
    }

    async fn send_denial_body(&mut self, body: PayloadBytes) -> Result<(), H2CornError> {
        self.connection.send_data(self.stream_id, body, false).await
    }

    async fn finish_denial_response(&mut self) -> Result<(), H2CornError> {
        self.connection
            .send_data(self.stream_id, Bytes::new(), true)
            .await
    }

    async fn abort_denial_response(&mut self) -> Result<(), H2CornError> {
        let _ = self
            .connection
            .reset_stream(self.stream_id, ErrorCode::INTERNAL_ERROR)
            .await;
        Ok(())
    }
}

impl AcceptedWebSocketTransport for H2WebSocketTransport {
    fn websocket_codec(&mut self, max_message_size: Option<NonZeroUsize>) -> WebSocketCodec {
        WebSocketCodec::segmented(max_message_size)
    }

    async fn send_frame(
        &mut self,
        frame: Bytes,
        _flush: FrameFlushMode,
    ) -> Result<(), H2CornError> {
        self.connection
            .send_data(self.stream_id, frame, false)
            .await
    }

    async fn flush_buffered_frames(&mut self) -> Result<(), H2CornError> {
        self.connection.flush_buffered_output(self.stream_id).await
    }

    fn frame_buf(&mut self) -> &mut BytesMut {
        &mut self.frame_buf
    }

    async fn read_into_codec(
        &mut self,
        codec: &mut WebSocketCodec,
    ) -> Result<TransportRead, H2CornError> {
        match self.stream_rx.recv().await {
            Some(StreamInput::Data(data)) => {
                codec.push_segment(data);
                Ok(TransportRead::Progress)
            },
            Some(StreamInput::EndStream) => Ok(TransportRead::PeerGone),
            Some(StreamInput::Reset(code)) => Ok(TransportRead::PeerReset {
                reason: format!("stream reset: {code}").into(),
            }),
            None => Ok(TransportRead::PeerGoneSilent),
        }
    }

    async fn finish_session(
        &mut self,
        state: &mut AcceptedWebSocketState,
    ) -> Result<(), H2CornError> {
        if let Some(frame) = take_pending_close_frame(state, self.frame_buf())? {
            self.connection
                .send_data(self.stream_id, frame, false)
                .await?;
            self.connection
                .send_data(self.stream_id, Bytes::new(), true)
                .await?;
            return Ok(());
        }

        assert_ne!(state.close_state, CloseState::CloseQueued);
        if state.close_state == CloseState::Open {
            let _ = self
                .connection
                .reset_stream(self.stream_id, ErrorCode::NO_ERROR)
                .await;
        }
        Ok(())
    }
}

pub(super) async fn handle_request(
    context: WebSocketContext,
    stream_id: StreamId,
    stream_rx: mpsc::Receiver<StreamInput>,
    connection: ConnectionHandle,
) -> Result<(), H2CornError> {
    let mut transport = H2WebSocketTransport {
        connection,
        stream_id,
        stream_rx,
        frame_buf: BytesMut::with_capacity(INITIAL_FRAME_BUF_CAPACITY),
    };
    run_websocket(&mut transport, context).await
}
