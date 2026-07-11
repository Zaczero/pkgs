mod accepted;
mod handshake;

use std::mem;
use std::num::{NonZeroU16, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::sync::watch;
use tokio::time::timeout;

use self::accepted::run_accepted_session;
use self::handshake::{
    HandshakeEvent, drive_denial_response, fail_handshake, receive_handshake_event,
};
use super::app::start_websocket_app;
use super::codec::{
    EncodedFrameHeader, MAX_CLOSE_REASON_LEN, WebSocketCodec, encode_close_frame_into,
    encode_frame_header, validate_close_code,
};
use super::request::{validate_accept_headers, validate_accepted_subprotocol};
use super::{PERMESSAGE_DEFLATE_RESPONSE, WebSocketCloseCode, WebSocketRequestMeta, close_code};
use crate::access_log::WebSocketAccessLogState;
use crate::bridge::PayloadBytes;
use crate::error::{ErrorExt, H2CornError, WebSocketError};
use crate::hpack::BytesStr;
use crate::http::response::FinalResponseBody;
use crate::http::types::{HttpStatusCode, ResponseHeaders, status_code};
use crate::runtime::{RequestAdmission, RequestContext, ShutdownKind, ShutdownState};

#[derive(Debug, Default)]
pub(crate) struct AcceptedWebSocketState {
    close: CloseLifecycle,
}

#[derive(Debug, Default)]
enum CloseLifecycle {
    #[default]
    Open,
    Queued {
        frame: PendingClose,
        reported_code: NonZeroU16,
    },
    Sent(NonZeroU16),
    PeerGone(NonZeroU16),
    PeerReset(NonZeroU16),
}

#[derive(Debug)]
pub(crate) struct PendingClose {
    code: WebSocketCloseCode,
    reason: Box<str>,
}

impl PendingClose {
    fn new(code: WebSocketCloseCode, reason: &str) -> Result<Self, H2CornError> {
        validate_close_code(code)?;
        if reason.len() > MAX_CLOSE_REASON_LEN {
            return WebSocketError::CloseReasonTooLong.err();
        }
        Ok(Self {
            code,
            reason: reason.into(),
        })
    }

    pub(crate) const fn code(&self) -> WebSocketCloseCode {
        self.code
    }

    pub(crate) fn reason(&self) -> &str {
        &self.reason
    }
}

impl AcceptedWebSocketState {
    pub(crate) const fn close_code_or(&self, fallback: WebSocketCloseCode) -> WebSocketCloseCode {
        match &self.close {
            CloseLifecycle::Open => fallback,
            CloseLifecycle::Queued { reported_code, .. } => reported_code.get(),
            CloseLifecycle::Sent(code)
            | CloseLifecycle::PeerGone(code)
            | CloseLifecycle::PeerReset(code) => code.get(),
        }
    }

    pub(super) fn queue_close_if_open(
        &mut self,
        code: WebSocketCloseCode,
        reason: &str,
    ) -> Result<(), H2CornError> {
        if matches!(
            self.close,
            CloseLifecycle::Queued { .. } | CloseLifecycle::Sent(_)
        ) {
            validate_close_code(code)?;
            let reported_code = NonZeroU16::new(code).expect("validated close codes are non-zero");
            match &mut self.close {
                CloseLifecycle::Queued {
                    reported_code: current,
                    ..
                }
                | CloseLifecycle::Sent(current) => *current = reported_code,
                CloseLifecycle::Open
                | CloseLifecycle::PeerGone(_)
                | CloseLifecycle::PeerReset(_) => unreachable!(),
            }
            return Ok(());
        }
        // `PendingClose::new` validates both the code and the reason.
        let frame = PendingClose::new(code, reason)?;
        self.close = CloseLifecycle::Queued {
            reported_code: NonZeroU16::new(code).expect("validated close codes are non-zero"),
            frame,
        };
        Ok(())
    }

    pub(crate) fn take_pending_close(&mut self) -> Option<PendingClose> {
        match mem::replace(&mut self.close, CloseLifecycle::Open) {
            CloseLifecycle::Queued {
                frame,
                reported_code,
            } => {
                self.close = CloseLifecycle::Sent(reported_code);
                Some(frame)
            },
            lifecycle => {
                self.close = lifecycle;
                None
            },
        }
    }

    pub(super) const fn close_started(&self) -> bool {
        matches!(
            self.close,
            CloseLifecycle::Queued { .. } | CloseLifecycle::Sent(_)
        )
    }

    pub(crate) const fn has_queued_close(&self) -> bool {
        matches!(self.close, CloseLifecycle::Queued { .. })
    }

    pub(crate) const fn should_reset_h2_stream(&self) -> bool {
        matches!(
            self.close,
            CloseLifecycle::Open | CloseLifecycle::PeerGone(_)
        )
    }

    pub(crate) const fn is_peer_reset(&self) -> bool {
        matches!(self.close, CloseLifecycle::PeerReset(_))
    }

    pub(super) fn mark_peer_gone(&mut self, code: WebSocketCloseCode) {
        self.close = CloseLifecycle::PeerGone(
            NonZeroU16::new(code).expect("terminal websocket close codes are non-zero"),
        );
    }

    pub(super) fn mark_peer_reset(&mut self, code: WebSocketCloseCode) {
        self.close = CloseLifecycle::PeerReset(
            NonZeroU16::new(code).expect("terminal websocket close codes are non-zero"),
        );
    }
}

#[derive(Debug)]
pub(crate) enum TransportRead {
    Progress,
    PeerGone,
    PeerGoneSilent,
    PeerReset { reason: BytesStr },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FrameFlushMode {
    Buffered,
    Immediate,
}

#[derive(Debug)]
pub(crate) enum EncodedWebSocketFrame {
    Contiguous(Bytes),
    Segmented {
        header: EncodedFrameHeader,
        payload: PayloadBytes,
    },
}

impl EncodedWebSocketFrame {
    pub(crate) fn segmented(opcode: u8, payload: PayloadBytes, compressed: bool) -> Self {
        let header = encode_frame_header(opcode, payload.len(), compressed);
        Self::Segmented { header, payload }
    }

    pub(crate) fn segments(&self) -> (&[u8], Option<&[u8]>) {
        match self {
            Self::Contiguous(frame) => (frame.as_ref(), None),
            Self::Segmented { header, payload } => (header.as_slice(), Some(payload.as_ref())),
        }
    }
}

pub(crate) trait WebSocketHandshakeTransport {
    fn accept_status(&self) -> HttpStatusCode;

    async fn send_empty_response(&mut self, status: HttpStatusCode) -> Result<(), H2CornError>;

    async fn send_internal_error_response(&mut self) -> Result<(), H2CornError> {
        self.send_empty_response(status_code::INTERNAL_SERVER_ERROR)
            .await
    }

    async fn send_accept(
        &mut self,
        subprotocol: Option<&str>,
        headers: ResponseHeaders,
        per_message_deflate: bool,
    ) -> Result<(), H2CornError>;

    async fn send_forbidden_response(&mut self) -> Result<(), H2CornError> {
        self.send_empty_response(status_code::FORBIDDEN).await
    }

    async fn send_final_denial_response(
        &mut self,
        status: HttpStatusCode,
        headers: ResponseHeaders,
        body: FinalResponseBody,
    ) -> Result<(), H2CornError>;

    async fn start_denial_response(
        &mut self,
        status: HttpStatusCode,
        headers: ResponseHeaders,
    ) -> Result<(), H2CornError>;

    async fn send_denial_body(&mut self, body: PayloadBytes) -> Result<(), H2CornError>;

    async fn finish_denial_response(&mut self) -> Result<(), H2CornError>;

    async fn abort_denial_response(&mut self) -> Result<(), H2CornError> {
        Ok(())
    }
}

pub(crate) trait AcceptedWebSocketTransport {
    fn websocket_codec(&mut self, max_message_size: Option<NonZeroUsize>) -> WebSocketCodec;

    async fn send_frame(
        &mut self,
        frame: EncodedWebSocketFrame,
        flush: FrameFlushMode,
    ) -> Result<(), H2CornError>;

    async fn flush_buffered_frames(&mut self) -> Result<(), H2CornError> {
        Ok(())
    }

    fn frame_buf(&mut self) -> &mut BytesMut;

    async fn read_into_codec(
        &mut self,
        codec: &mut WebSocketCodec,
    ) -> Result<TransportRead, H2CornError>;

    async fn finish_session(
        &mut self,
        state: &mut AcceptedWebSocketState,
    ) -> Result<(), H2CornError>;
}

pub(crate) struct WebSocketContext {
    pub(crate) request: Box<RequestContext>,
    pub(crate) admission: RequestAdmission,
    pub(crate) meta: WebSocketRequestMeta,
}

pub(super) struct AcceptedSessionConfig {
    pub(super) max_message_size: Option<NonZeroUsize>,
    pub(super) per_message_deflate: bool,
    pub(super) ping_interval: Option<Duration>,
    pub(super) ping_timeout: Option<Duration>,
    pub(super) shutdown: watch::Receiver<ShutdownState>,
}

pub(crate) fn append_ws_accept_headers(
    headers: &mut ResponseHeaders,
    subprotocol: Option<&str>,
    per_message_deflate: bool,
) {
    headers.reserve(usize::from(subprotocol.is_some()) + usize::from(per_message_deflate));
    if let Some(subprotocol) = subprotocol {
        headers.push((
            Bytes::from_static(b"sec-websocket-protocol").into(),
            Bytes::copy_from_slice(subprotocol.as_bytes()).into(),
        ));
    }
    if per_message_deflate {
        headers.push((
            Bytes::from_static(b"sec-websocket-extensions").into(),
            Bytes::from_static(PERMESSAGE_DEFLATE_RESPONSE).into(),
        ));
    }
}

pub(crate) fn take_pending_close_frame(
    state: &mut AcceptedWebSocketState,
    frame_buf: &mut BytesMut,
) -> Result<Option<Bytes>, H2CornError> {
    if let Some(close) = state.take_pending_close() {
        encode_close_frame_into(close.code(), close.reason(), frame_buf)?;
        return Ok(Some(frame_buf.split().freeze()));
    }
    Ok(None)
}

pub(crate) const fn shutdown_close_code(kind: ShutdownKind) -> WebSocketCloseCode {
    match kind {
        ShutdownKind::Stop => close_code::GOING_AWAY,
        ShutdownKind::Restart => close_code::SERVICE_RESTART,
    }
}

pub(crate) async fn run_websocket<T>(
    transport: &mut T,
    context: WebSocketContext,
) -> Result<(), H2CornError>
where
    T: WebSocketHandshakeTransport + AcceptedWebSocketTransport,
{
    let request = &context.request;
    let connection = &request.connection;
    let config = Arc::clone(&connection.config);
    let access_log = WebSocketAccessLogState::new(request);
    let per_message_deflate =
        config.websocket.per_message_deflate && context.meta.per_message_deflate;
    let timeout_handshake = config.timeout_handshake;
    let max_message_size: Option<NonZeroUsize> = config.websocket.max_message_size;
    let timeout_graceful_shutdown = config.timeout_graceful_shutdown;
    let ping_interval = config.websocket.ping_interval;
    let ping_timeout = config.websocket.ping_timeout;
    let shutdown = connection.shutdown.clone();

    let mut running_app = start_websocket_app(context);

    let first = match timeout(timeout_handshake, receive_handshake_event(&mut running_app)).await {
        Ok(result) => match result {
            Ok(first) => first,
            Err(err) => return fail_handshake(transport, &mut running_app, &access_log, err).await,
        },
        Err(_) => {
            return fail_handshake(
                transport,
                &mut running_app,
                &access_log,
                WebSocketError::HandshakeTimedOut,
            )
            .await;
        },
    };
    match first {
        HandshakeEvent::Accept {
            subprotocol,
            headers,
        } => {
            let subprotocol = subprotocol.as_ref().map(AsRef::as_ref);
            if let Err(err) =
                validate_accepted_subprotocol(&running_app.requested_subprotocols, subprotocol)
                    .and_then(|()| validate_accept_headers(&headers))
            {
                return fail_handshake(transport, &mut running_app, &access_log, err).await;
            }

            running_app.send_buffer.begin_accepted_drain();
            transport
                .send_accept(subprotocol, headers, per_message_deflate)
                .await?;
            access_log.emit_http_response(transport.accept_status(), 0);
        },
        HandshakeEvent::Close => {
            transport.send_forbidden_response().await?;
            access_log.emit_http_response(status_code::FORBIDDEN, 0);
            running_app.task.settle(timeout_graceful_shutdown).await?;
            return Ok(());
        },
        HandshakeEvent::DenialStart { status, headers } => {
            let (tx_bytes, _) =
                drive_denial_response(transport, status, headers, &mut running_app).await?;
            access_log.emit_http_response(status, tx_bytes);
            running_app.task.settle(timeout_graceful_shutdown).await?;
            return Ok(());
        },
    }

    let session_started = Instant::now();
    let mut outcome = run_accepted_session(transport, &mut running_app, AcceptedSessionConfig {
        max_message_size,
        per_message_deflate,
        ping_interval,
        ping_timeout,
        shutdown,
    })
    .await?;

    transport.finish_session(&mut outcome.state).await?;
    running_app.task.settle(timeout_graceful_shutdown).await?;
    access_log.emit_session(
        outcome
            .state
            .close_code_or(if outcome.state.is_peer_reset() {
                close_code::ABNORMAL_CLOSURE
            } else {
                close_code::NO_STATUS_RECEIVED
            }),
        session_started.elapsed(),
        outcome.rx_bytes,
        outcome.tx_bytes,
    );
    let result = outcome.result;
    drop(running_app);
    result
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use bytes::BytesMut;

    use super::{
        AcceptedWebSocketState, EncodedWebSocketFrame, PERMESSAGE_DEFLATE_RESPONSE,
        append_ws_accept_headers, take_pending_close_frame,
    };

    #[test]
    fn encoded_frame_owner_stays_within_inline_queue_budget() {
        assert!(size_of::<EncodedWebSocketFrame>() <= 64);
    }

    #[test]
    fn accepted_close_lifecycle_preserves_state_layout_budget() {
        assert!(size_of::<AcceptedWebSocketState>() <= 32);
    }

    #[test]
    fn accept_headers_append_subprotocol_and_deflate_once() {
        let mut headers = Vec::new();

        append_ws_accept_headers(&mut headers, Some("chat"), true);

        assert_eq!(headers.len(), 2);
        assert_eq!(headers[0].0.as_ref(), b"sec-websocket-protocol");
        assert_eq!(headers[0].1.as_ref(), b"chat");
        assert_eq!(headers[1].0.as_ref(), b"sec-websocket-extensions");
        assert_eq!(headers[1].1.as_ref(), PERMESSAGE_DEFLATE_RESPONSE,);
    }

    #[test]
    fn take_pending_close_frame_marks_close_sent() {
        let mut state = AcceptedWebSocketState::default();
        let mut frame_buf = BytesMut::new();

        state
            .queue_close_if_open(1000, "bye")
            .expect("close is queued");

        let frame = take_pending_close_frame(&mut state, &mut frame_buf)
            .expect("close frame encodes")
            .expect("queued close produces a frame");

        assert!(!state.has_queued_close());
        assert_eq!(frame.as_ref(), b"\x88\x05\x03\xe8bye");
        assert!(frame_buf.is_empty());
    }

    #[test]
    fn queue_close_if_open_rejects_invalid_code_after_close_started() {
        let mut state = AcceptedWebSocketState::default();

        state
            .queue_close_if_open(1000, "bye")
            .expect("close is queued");

        assert!(state.queue_close_if_open(0, "").is_err());
        assert_eq!(state.close_code_or(1001), 1000);
    }

    #[test]
    fn queued_close_keeps_first_frame_and_latest_reported_code() {
        let mut state = AcceptedWebSocketState::default();
        state
            .queue_close_if_open(1000, "first")
            .expect("first close is queued");
        state
            .queue_close_if_open(1001, "second")
            .expect("a later valid close is harmless");

        let close = state.take_pending_close().expect("close remains queued");
        assert_eq!(close.code(), 1000);
        assert_eq!(close.reason(), "first");
        assert_eq!(state.close_code_or(1002), 1001);
        assert!(state.take_pending_close().is_none());
    }

    #[test]
    fn peer_terminal_states_cannot_contain_a_queued_close() {
        let mut gone = AcceptedWebSocketState::default();
        gone.mark_peer_gone(1005);
        assert!(!gone.has_queued_close());
        assert!(gone.should_reset_h2_stream());

        let mut reset = AcceptedWebSocketState::default();
        reset.mark_peer_reset(1006);
        assert!(!reset.has_queued_close());
        assert!(!reset.should_reset_h2_stream());
        assert!(reset.is_peer_reset());
    }
}
