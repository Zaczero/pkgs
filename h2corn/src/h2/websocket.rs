use std::num::NonZeroUsize;

use bytes::BytesMut;
use tokio::sync::mpsc;

use super::H2WriterHandle;
use super::http::H2HttpTransport;
use crate::error::H2CornError;
use crate::h2_frame::{ErrorCode, StreamId};
use crate::http::response::{HttpResponseTransport, ResponseAction, ResponseActions};
use crate::http::types::{HttpStatusCode, ResponseHeaders, status_code};
use crate::runtime::{H2InputCredit, StreamInput};
use crate::websocket::WebSocketCodec;
use crate::websocket::session::{
    AcceptedWebSocketState, AcceptedWebSocketTransport, EncodedWebSocketFrame, FrameFlushMode,
    TransportRead, WebSocketContext, WebSocketHandshakeTransport, append_ws_accept_headers,
    run_websocket, take_pending_close_frame,
};

const INITIAL_FRAME_BUF_CAPACITY: usize = 256;

struct H2WebSocketTransport {
    connection: H2WriterHandle,
    stream_id: StreamId,
    stream_rx: mpsc::Receiver<StreamInput>,
    frame_buf: BytesMut,
    /// DATA credit stays owned by the WebSocket session until its decoded
    /// message has entered the byte-bounded ASGI queue. Releasing at codec
    /// ingestion lets a paused application turn the H2 receive window into
    /// an unbounded source of complete WebSocket messages.
    pending_input_credits: Vec<H2InputCredit>,
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
}

impl HttpResponseTransport for H2WebSocketTransport {
    async fn apply_response_action(&mut self, action: ResponseAction) -> Result<(), H2CornError> {
        H2HttpTransport::new(&self.connection, self.stream_id)
            .apply_response_action(action)
            .await
    }

    async fn flush_buffered(&mut self) -> Result<(), H2CornError> {
        H2HttpTransport::new(&self.connection, self.stream_id)
            .flush_buffered()
            .await
    }

    async fn apply_response_actions(
        &mut self,
        actions: &mut ResponseActions,
    ) -> Result<(), H2CornError> {
        H2HttpTransport::new(&self.connection, self.stream_id)
            .apply_response_actions(actions)
            .await
    }
}

impl AcceptedWebSocketTransport for H2WebSocketTransport {
    fn websocket_codec(&mut self, max_message_size: Option<NonZeroUsize>) -> WebSocketCodec {
        WebSocketCodec::with_options(max_message_size)
    }

    async fn send_frame(
        &mut self,
        frame: EncodedWebSocketFrame,
        _flush: FrameFlushMode,
    ) -> Result<(), H2CornError> {
        match frame {
            EncodedWebSocketFrame::Contiguous(frame) => {
                self.connection
                    .send_data(self.stream_id, frame, false)
                    .await
            },
            EncodedWebSocketFrame::Segmented { header, payload } => {
                self.connection
                    .send_websocket_data(self.stream_id, header, payload)
                    .await
            },
        }
    }

    async fn flush_buffered_frames(&mut self) -> Result<(), H2CornError> {
        self.connection.flush_buffered_output(self.stream_id).await
    }

    fn frame_buf(&mut self) -> &mut BytesMut {
        &mut self.frame_buf
    }

    fn release_consumed_input(&mut self) {
        self.pending_input_credits.clear();
    }

    async fn read_into_codec(
        &mut self,
        codec: &mut WebSocketCodec,
    ) -> Result<TransportRead, H2CornError> {
        match self.stream_rx.recv().await {
            Some(StreamInput::Data { body, credit }) => {
                codec.push_segment(body);
                if let Some(credit) = credit {
                    self.pending_input_credits.push(credit);
                }
                Ok(TransportRead::Progress)
            },
            Some(StreamInput::BufferedData { body, credit }) => {
                codec.push_segment(body.freeze());
                if let Some(credit) = credit {
                    self.pending_input_credits.push(credit);
                }
                Ok(TransportRead::Progress)
            },
            Some(StreamInput::DataBatch { bodies, credit, .. }) => {
                for body in bodies {
                    codec.push_segment(body);
                }
                if let Some(credit) = credit {
                    self.pending_input_credits.push(credit);
                }
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
            // END_STREAM rides on the close frame itself: one DATA frame on
            // the wire, and clients observe the close payload and stream end
            // atomically in the same flight.
            self.connection
                .send_data(self.stream_id, frame, true)
                .await?;
            return Ok(());
        }

        debug_assert!(!state.has_queued_close());
        if state.should_reset_h2_stream() {
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
    connection: H2WriterHandle,
) -> Result<(), H2CornError> {
    let mut transport = H2WebSocketTransport {
        connection,
        stream_id,
        stream_rx,
        frame_buf: BytesMut::with_capacity(INITIAL_FRAME_BUF_CAPACITY),
        pending_input_credits: Vec::new(),
    };
    run_websocket(&mut transport, context).await
}
