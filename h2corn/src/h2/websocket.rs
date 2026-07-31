use std::num::NonZeroUsize;

use bytes::BytesMut;
use tokio::sync::mpsc;

use super::H2WriterHandle;
use super::http::H2HttpTransport;
use crate::error::{H2CornError, H2Error};
use crate::h2_frame::{ErrorCode, StreamId};
use crate::bridge::HttpOutboundEvent;
use crate::http::app::AdmittedHttpOutboundEvent;
use crate::http::response::{
    HttpResponseTransport, ResponseAction, ResponseActions, ResponseBytePermit,
};
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

impl H2WebSocketTransport {
    /// WebSocket DATA is charged to the same connection-wide outbound budget
    /// as ordinary response bodies.
    ///
    /// Without it a peer that advertises a zero stream window and keeps the
    /// application sending turns every queued frame into retained memory. The
    /// writer's *command* channel does not help: it bounds commands, not
    /// bytes, and its slot frees as soon as the command reaches the per-stream
    /// pending queue — where the payload then sits, unwritable, until the
    /// response-stall timeout. The *byte* permit taken here is a different
    /// thing: it rides on the pending chunk and is returned only as those
    /// bytes reach the socket.
    async fn acquire_outbound_credit(
        &self,
        len: usize,
    ) -> Result<Option<ResponseBytePermit>, H2CornError> {
        self.connection
            .response_byte_budget()
            .acquire(len)
            .await
            .map_err(|_| H2CornError::from(H2Error::StreamChannelClosed))
    }
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
    /// Delegated, not defaulted: a WebSocket denial body is driven straight
    /// from the handshake loop and would otherwise reach the writer with no
    /// byte credit at all — the same hole that let accepted WebSocket data
    /// bypass the budget.
    async fn admit_outbound_event(
        &self,
        event: HttpOutboundEvent,
    ) -> Result<AdmittedHttpOutboundEvent, H2CornError> {
        H2HttpTransport::new(&self.connection, self.stream_id)
            .admit_outbound_event(event)
            .await
    }

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
                let credit = self.acquire_outbound_credit(frame.len()).await?;
                self.connection
                    .send_data(self.stream_id, frame, credit, false)
                    .await
            },
            EncodedWebSocketFrame::Segmented { header, payload } => {
                let credit = self
                    .acquire_outbound_credit(header.as_slice().len() + payload.len())
                    .await?;
                self.connection
                    .send_websocket_data(self.stream_id, header, payload, credit)
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
            let close_credit = self.acquire_outbound_credit(frame.len()).await?;
            // END_STREAM rides on the close frame itself: one DATA frame on
            // the wire, and clients observe the close payload and stream end
            // atomically in the same flight.
            self.connection
                .send_data(self.stream_id, frame, close_credit, true)
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
