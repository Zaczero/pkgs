mod accepted;
mod handshake;

use std::num::{NonZeroU16, NonZeroUsize};
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use tokio::sync::watch;
use tokio::time::timeout;

use self::accepted::run_accepted_session;
use self::handshake::{
    HandshakeEvent, drive_denial_response, fail_handshake, receive_handshake_event, settle_app_task,
};
use super::app::start_websocket_app;
use super::codec::{
    MAX_CLOSE_REASON_LEN, WebSocketCodec, encode_close_frame_into, validate_close_code,
};
use super::request::{validate_accept_headers, validate_accepted_subprotocol};
use super::{PERMESSAGE_DEFLATE_RESPONSE, WebSocketCloseCode, WebSocketRequestMeta, close_code};
use crate::bridge::PayloadBytes;
use crate::console::WebSocketAccessLogState;
use crate::error::{ErrorExt, H2CornError, WebSocketError};
use crate::hpack::BytesStr;
use crate::http::response::FinalResponseBody;
use crate::http::types::{HttpStatusCode, ResponseHeaders, status_code};
use crate::runtime::{RequestAdmission, RequestContext, ShutdownKind, ShutdownState};

#[derive(Debug, Default)]
pub struct AcceptedWebSocketState {
    pub(crate) close_state: CloseState,
    pub(crate) close_code: Option<NonZeroU16>,
    pending_close: Option<PendingClose>,
}

#[derive(Debug)]
pub struct PendingClose {
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
    pub(super) const fn set_close_code(&mut self, code: WebSocketCloseCode) {
        self.close_code = NonZeroU16::new(code);
    }

    pub(crate) fn close_code_or(&self, fallback: WebSocketCloseCode) -> WebSocketCloseCode {
        self.close_code.map_or(fallback, NonZeroU16::get)
    }

    pub(super) fn queue_close(
        &mut self,
        code: WebSocketCloseCode,
        reason: &str,
    ) -> Result<(), H2CornError> {
        self.pending_close = Some(PendingClose::new(code, reason)?);
        self.close_state = CloseState::CloseQueued;
        self.set_close_code(code);
        Ok(())
    }

    pub(super) fn queue_close_if_open(
        &mut self,
        code: WebSocketCloseCode,
        reason: &str,
    ) -> Result<(), H2CornError> {
        validate_close_code(code)?;
        if self.close_started() {
            self.set_close_code(code);
            return Ok(());
        }
        self.queue_close(code, reason)
    }

    pub(crate) const fn take_pending_close(&mut self) -> Option<PendingClose> {
        self.pending_close.take()
    }

    pub(super) const fn close_started(&self) -> bool {
        matches!(
            self.close_state,
            CloseState::CloseQueued | CloseState::CloseSent
        )
    }

    pub(crate) const fn mark_close_sent(&mut self) {
        self.close_state = CloseState::CloseSent;
    }

    pub(super) const fn mark_peer_reset(&mut self, code: WebSocketCloseCode) {
        self.set_close_code(code);
        self.close_state = CloseState::PeerReset;
    }
}

pub fn append_ws_accept_headers(
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

pub fn take_pending_close_frame(
    state: &mut AcceptedWebSocketState,
    frame_buf: &mut BytesMut,
) -> Result<Option<Bytes>, H2CornError> {
    if let Some(close) = state.take_pending_close() {
        encode_close_frame_into(close.code(), close.reason(), frame_buf)?;
        state.mark_close_sent();
        return Ok(Some(frame_buf.split().freeze()));
    }
    Ok(None)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CloseState {
    #[default]
    Open,
    CloseQueued,
    CloseSent,
    PeerReset,
}

#[derive(Debug)]
pub enum TransportRead {
    Progress,
    PeerGone,
    PeerGoneSilent,
    PeerReset { reason: BytesStr },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameFlushMode {
    Buffered,
    Immediate,
}

pub trait WebSocketHandshakeTransport {
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

pub trait AcceptedWebSocketTransport {
    fn websocket_codec(&mut self, max_message_size: Option<NonZeroUsize>) -> WebSocketCodec;

    async fn send_frame(&mut self, frame: Bytes, flush: FrameFlushMode) -> Result<(), H2CornError>;

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

pub struct WebSocketContext {
    pub(crate) request: RequestContext,
    pub(crate) admission: RequestAdmission,
    pub(crate) meta: WebSocketRequestMeta,
}

pub(super) struct AcceptedSessionConfig {
    pub(super) max_message_size: Option<NonZeroUsize>,
    pub(super) per_message_deflate: bool,
    pub(super) ping_interval: Option<std::time::Duration>,
    pub(super) ping_timeout: Option<std::time::Duration>,
    pub(super) shutdown: watch::Receiver<ShutdownState>,
}

pub const fn shutdown_close_code(kind: ShutdownKind) -> WebSocketCloseCode {
    match kind {
        ShutdownKind::Stop => close_code::GOING_AWAY,
        ShutdownKind::Restart => close_code::SERVICE_RESTART,
    }
}

pub async fn run_websocket<T>(
    transport: &mut T,
    context: WebSocketContext,
) -> Result<(), H2CornError>
where
    T: WebSocketHandshakeTransport + AcceptedWebSocketTransport,
{
    let request = &context.request;
    let connection = &request.connection;
    let config = connection.config;
    let access_log = WebSocketAccessLogState::new(request);
    let per_message_deflate =
        config.websocket.per_message_deflate && context.meta.per_message_deflate;
    let timeout_handshake = config.timeout_handshake;
    let max_message_size: Option<NonZeroUsize> = config.websocket.message_size_limit;
    let timeout_graceful_shutdown = config.timeout_graceful_shutdown;
    let ping_interval = config.websocket.ping_interval;
    let ping_timeout = config.websocket.ping_timeout;
    let shutdown = connection.shutdown.clone();

    let mut running_app = start_websocket_app(context)?;

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
        }
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
        }
        HandshakeEvent::Close => {
            transport.send_forbidden_response().await?;
            access_log.emit_http_response(status_code::FORBIDDEN, 0);
            settle_app_task(&mut running_app, false, timeout_graceful_shutdown).await?;
            return Ok(());
        }
        HandshakeEvent::DenialStart { status, headers } => {
            let (tx_bytes, app_finished) =
                drive_denial_response(transport, status, headers, &mut running_app).await?;
            access_log.emit_http_response(status, tx_bytes);
            settle_app_task(&mut running_app, app_finished, timeout_graceful_shutdown).await?;
            return Ok(());
        }
    }

    let session_started = Instant::now();
    let mut outcome = run_accepted_session(
        transport,
        &mut running_app,
        AcceptedSessionConfig {
            max_message_size,
            per_message_deflate,
            ping_interval,
            ping_timeout,
            shutdown,
        },
    )
    .await?;

    transport.finish_session(&mut outcome.state).await?;
    settle_app_task(
        &mut running_app,
        outcome.app_finished,
        timeout_graceful_shutdown,
    )
    .await?;
    access_log.emit_session(
        outcome
            .state
            .close_code_or(if outcome.state.close_state == CloseState::PeerReset {
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
    use bytes::BytesMut;

    use super::{
        AcceptedWebSocketState, CloseState, PERMESSAGE_DEFLATE_RESPONSE, append_ws_accept_headers,
        take_pending_close_frame,
    };

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

        state.queue_close(1000, "bye").expect("close is queued");

        let frame = take_pending_close_frame(&mut state, &mut frame_buf)
            .expect("close frame encodes")
            .expect("queued close produces a frame");

        assert_eq!(state.close_state, CloseState::CloseSent);
        assert_eq!(frame.as_ref(), b"\x88\x05\x03\xe8bye");
        assert!(frame_buf.is_empty());
    }

    #[test]
    fn queue_close_if_open_rejects_invalid_code_after_close_started() {
        let mut state = AcceptedWebSocketState::default();

        state.queue_close(1000, "bye").expect("close is queued");

        assert!(state.queue_close_if_open(0, "").is_err());
        assert_eq!(state.close_code_or(1001), 1000);
    }
}
