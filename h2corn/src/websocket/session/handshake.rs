use std::time::Duration;

use pyo3::pybacked::PyBackedStr;
use tokio::task::JoinError;
use tokio::time::timeout;

use crate::bridge::{HttpOutboundEvent, PayloadBytes, WebSocketOutboundEvent};
use crate::console::{ResponseLogState, WebSocketAccessLogState};
use crate::error::{ErrorExt, H2CornError, WebSocketError};
use crate::http::response::{
    FinalResponseBody, HttpResponseTransport, ResponseActions, ResponseController, ResponseStart,
    apply_http_event, finalize_response,
};
use crate::http::types::{HttpStatusCode, ResponseHeaders, status_code};

use super::super::app::RunningWebSocketApp;
use super::WebSocketHandshakeTransport;

pub(super) enum HandshakeEvent {
    Accept {
        subprotocol: Option<PyBackedStr>,
        headers: ResponseHeaders,
    },
    Close,
    DenialStart {
        status: HttpStatusCode,
        headers: ResponseHeaders,
    },
}

fn parse_handshake_event(event: WebSocketOutboundEvent) -> Result<HandshakeEvent, H2CornError> {
    match event {
        WebSocketOutboundEvent::Accept {
            subprotocol,
            headers,
        } => Ok(HandshakeEvent::Accept {
            subprotocol,
            headers,
        }),
        WebSocketOutboundEvent::Close { .. } => Ok(HandshakeEvent::Close),
        WebSocketOutboundEvent::HttpResponseStart { status, headers } => {
            Ok(HandshakeEvent::DenialStart { status, headers })
        }
        other => WebSocketError::unexpected_initial_event(&other).err(),
    }
}

fn parse_denial_body_event(
    event: WebSocketOutboundEvent,
) -> Result<HttpOutboundEvent, H2CornError> {
    match event {
        WebSocketOutboundEvent::HttpResponseBody { body, more_body } => {
            Ok(HttpOutboundEvent::Body { body, more_body })
        }
        other => WebSocketError::unexpected_denial_body_event(&other).err(),
    }
}

struct DenialHttpTransport<'a, T> {
    transport: &'a mut T,
    tx_bytes: u64,
}

impl<T> HttpResponseTransport for DenialHttpTransport<'_, T>
where
    T: WebSocketHandshakeTransport,
{
    async fn send_final_response(
        &mut self,
        start: ResponseStart,
        body: FinalResponseBody,
    ) -> Result<(), H2CornError> {
        self.tx_bytes = self.tx_bytes.saturating_add(body.len() as u64);
        let (status, headers) = start.into_status_headers();
        self.transport
            .send_final_denial_response(status, headers, body)
            .await
    }

    async fn start_streaming_response(&mut self, start: ResponseStart) -> Result<(), H2CornError> {
        let (status, headers) = start.into_status_headers();
        self.transport.start_denial_response(status, headers).await
    }

    async fn send_streaming_body(&mut self, body: PayloadBytes) -> Result<(), H2CornError> {
        self.tx_bytes = self.tx_bytes.saturating_add(body.len() as u64);
        self.transport.send_denial_body(body).await
    }

    async fn send_streaming_file(
        &mut self,
        _file: tokio::fs::File,
        _len: usize,
    ) -> Result<(), H2CornError> {
        unreachable!("websocket denial responses never send files")
    }

    async fn finish_streaming_response(&mut self) -> Result<(), H2CornError> {
        self.transport.finish_denial_response().await
    }

    async fn finish_streaming_with_trailers(
        &mut self,
        _trailers: ResponseHeaders,
    ) -> Result<(), H2CornError> {
        unreachable!("websocket denial responses never send trailers")
    }

    async fn send_internal_error_response(&mut self) -> Result<(), H2CornError> {
        self.transport.send_internal_error_response().await
    }

    async fn abort_incomplete_response(&mut self) -> Result<(), H2CornError> {
        self.transport.abort_denial_response().await
    }

    fn response_log_state(&self) -> ResponseLogState {
        ResponseLogState::default()
    }
}

pub(super) fn flatten_app_result(
    result: Result<Result<(), H2CornError>, JoinError>,
) -> Result<(), H2CornError> {
    match result {
        Ok(result) => result,
        Err(err) => err.err(),
    }
}

pub(super) async fn receive_handshake_event(
    running_app: &mut RunningWebSocketApp,
) -> Result<HandshakeEvent, H2CornError> {
    loop {
        if let Some(event) = running_app.send_buffer.take_ready() {
            return parse_handshake_event(event);
        }

        tokio::select! {
            () = running_app.send_buffer.wait_ready() => {}
            result = &mut running_app.app_task => {
                flatten_app_result(result)
                    .and_then(|()| WebSocketError::AppEndedBeforeHandshake.err())?;
            }
        }
    }
}

pub(super) async fn drive_denial_response<T>(
    transport: &mut T,
    status: HttpStatusCode,
    headers: ResponseHeaders,
    running_app: &mut RunningWebSocketApp,
) -> Result<(u64, bool), H2CornError>
where
    T: WebSocketHandshakeTransport,
{
    let mut response = ResponseController::new(false, false);
    let mut actions = ResponseActions::new();
    let mut transport = DenialHttpTransport {
        transport,
        tx_bytes: 0,
    };
    apply_http_event(
        &mut response,
        &mut transport,
        &mut actions,
        HttpOutboundEvent::Start {
            status,
            headers,
            trailers: false,
        },
    )
    .await?;

    loop {
        if response.is_complete() {
            break;
        }

        if let Some(outbound) = running_app.send_buffer.take_ready() {
            let flush_result = match parse_denial_body_event(outbound) {
                Ok(event) => {
                    apply_http_event(&mut response, &mut transport, &mut actions, event).await
                }
                Err(err) => Err(err),
            };
            if let Err(err) = flush_result {
                finalize_response(&mut response, &mut transport, &mut actions, Err(err)).await?;
                return Ok((transport.tx_bytes, false));
            }
            continue;
        }

        tokio::select! {
            () = running_app.send_buffer.wait_ready() => {}
            result = &mut running_app.app_task => {
                finalize_response(
                    &mut response,
                    &mut transport,
                    &mut actions,
                    flatten_app_result(result),
                )
                .await?;
                return Ok((transport.tx_bytes, true));
            }
        }
    }

    Ok((transport.tx_bytes, false))
}

pub(super) async fn fail_handshake<T, E>(
    transport: &mut T,
    running_app: &mut RunningWebSocketApp,
    access_log: &WebSocketAccessLogState,
    err: E,
) -> Result<(), H2CornError>
where
    T: WebSocketHandshakeTransport,
    E: Into<H2CornError>,
{
    let err = err.into();
    transport.send_internal_error_response().await?;
    if !running_app.app_task.is_finished() {
        abort_app_task(running_app).await;
    }
    access_log.emit_http_response(status_code::INTERNAL_SERVER_ERROR, 0);
    Err(err)
}

pub(super) async fn abort_app_task(running_app: &mut RunningWebSocketApp) {
    running_app.app_task.abort();
    let _ = (&mut running_app.app_task).await;
}

pub(super) async fn settle_app_task(
    running_app: &mut RunningWebSocketApp,
    app_finished: bool,
    timeout_graceful_shutdown: Duration,
) -> Result<(), H2CornError> {
    if app_finished || running_app.app_task.is_finished() {
        return Ok(());
    }
    if let Ok(result) = timeout(timeout_graceful_shutdown, &mut running_app.app_task).await {
        flatten_app_result(result)
    } else {
        abort_app_task(running_app).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{future::pending, mem::size_of_val};

    use bytes::Bytes;
    use tokio::sync::mpsc;

    use super::{HandshakeEvent, drive_denial_response, receive_handshake_event};
    use crate::bridge::WebSocketSendDisposition;
    use crate::bridge::{
        PayloadBytes, WebSocketInboundEvent, WebSocketOutboundEvent, WebSocketSendState,
    };
    use crate::error::{H2CornError, HttpResponseError};
    use crate::http::response::FinalResponseBody;
    use crate::http::types::{ResponseHeaders, status_code};
    use crate::runtime::RequestAdmission;
    use crate::websocket::RequestedSubprotocols;
    use crate::websocket::app::RunningWebSocketApp;

    #[derive(Default)]
    struct RecordingHandshakeTransport {
        calls: Vec<&'static str>,
        body_chunks: Vec<Bytes>,
        final_bodies: Vec<Bytes>,
    }

    use crate::websocket::session::WebSocketHandshakeTransport;

    impl WebSocketHandshakeTransport for RecordingHandshakeTransport {
        fn accept_status(&self) -> u16 {
            status_code::SWITCHING_PROTOCOLS
        }

        async fn send_empty_response(&mut self, _status: u16) -> Result<(), H2CornError> {
            self.calls.push("send_empty_response");
            Ok(())
        }

        async fn send_accept(
            &mut self,
            _subprotocol: Option<&str>,
            _headers: ResponseHeaders,
            _per_message_deflate: bool,
        ) -> Result<(), H2CornError> {
            self.calls.push("send_accept");
            Ok(())
        }

        async fn send_final_denial_response(
            &mut self,
            _status: u16,
            _headers: ResponseHeaders,
            body: FinalResponseBody,
        ) -> Result<(), H2CornError> {
            self.calls.push("send_final_denial_response");
            self.final_bodies.push(match body {
                FinalResponseBody::Empty => Bytes::new(),
                FinalResponseBody::Bytes(body) => Bytes::copy_from_slice(body.as_ref()),
                FinalResponseBody::File { .. } | FinalResponseBody::Suppressed { .. } => {
                    unreachable!("websocket denial responses never send files or suppressed bodies")
                }
            });
            Ok(())
        }

        async fn start_denial_response(
            &mut self,
            _status: u16,
            _headers: ResponseHeaders,
        ) -> Result<(), H2CornError> {
            self.calls.push("start_denial_response");
            Ok(())
        }

        async fn send_denial_body(&mut self, body: PayloadBytes) -> Result<(), H2CornError> {
            self.calls.push("send_denial_body");
            self.body_chunks.push(Bytes::copy_from_slice(body.as_ref()));
            Ok(())
        }

        async fn finish_denial_response(&mut self) -> Result<(), H2CornError> {
            self.calls.push("finish_denial_response");
            Ok(())
        }

        async fn abort_denial_response(&mut self) -> Result<(), H2CornError> {
            self.calls.push("abort_denial_response");
            Ok(())
        }
    }

    fn running_app_with_buffered_event(
        event: WebSocketOutboundEvent,
        app_task: tokio::task::JoinHandle<Result<(), H2CornError>>,
    ) -> RunningWebSocketApp {
        let (recv_tx, _recv_rx) = mpsc::channel::<WebSocketInboundEvent>(1);
        let (_send_tx, send_rx) = mpsc::channel(1);
        let (send_state, send_buffer) = WebSocketSendState::new();
        assert!(matches!(
            send_state.push_or_forward(event),
            WebSocketSendDisposition::Buffered
        ));

        RunningWebSocketApp {
            recv_tx,
            requested_subprotocols: RequestedSubprotocols::default(),
            send_state,
            send_buffer,
            send_rx,
            app_task,
            _admission: RequestAdmission::default(),
        }
    }

    #[tokio::test]
    async fn initial_handshake_event_prefers_buffered_messages() {
        let mut running_app = running_app_with_buffered_event(
            WebSocketOutboundEvent::Close {
                code: 1000,
                reason: None,
            },
            tokio::spawn(async {
                pending::<()>().await;
                Ok(())
            }),
        );

        let event = receive_handshake_event(&mut running_app)
            .await
            .expect("buffered close event is returned");
        assert!(matches!(event, HandshakeEvent::Close));

        running_app.app_task.abort();
        let _ = (&mut running_app.app_task).await;
        drop(running_app);
    }

    #[tokio::test]
    async fn unary_denial_response_collapses_into_final_response() {
        let mut transport = RecordingHandshakeTransport::default();
        let (recv_tx, _recv_rx) = mpsc::channel::<WebSocketInboundEvent>(1);
        let (send_tx, send_rx) = mpsc::channel(1);
        let (send_state, send_buffer) = WebSocketSendState::new();
        assert!(matches!(
            send_state.push_or_forward(WebSocketOutboundEvent::HttpResponseBody {
                body: PayloadBytes::from(Bytes::from_static(b"denied")),
                more_body: false,
            }),
            WebSocketSendDisposition::Buffered
        ));
        let mut running_app = RunningWebSocketApp {
            recv_tx,
            requested_subprotocols: RequestedSubprotocols::default(),
            send_state,
            send_buffer,
            send_rx,
            app_task: tokio::spawn(async { Ok(()) }),
            _admission: RequestAdmission::default(),
        };
        let _keep_sender_alive = send_tx;

        let (tx_bytes, app_finished) = drive_denial_response(
            &mut transport,
            status_code::FORBIDDEN,
            Vec::new(),
            &mut running_app,
        )
        .await
        .expect("unary denial response is emitted");

        assert_eq!(tx_bytes, 6);
        assert!(!app_finished);
        assert_eq!(transport.calls, ["send_final_denial_response"]);
        assert_eq!(transport.final_bodies, [Bytes::from_static(b"denied")]);
    }

    #[tokio::test]
    async fn empty_denial_response_finishes_as_final_response_when_app_ends() {
        let mut transport = RecordingHandshakeTransport::default();
        let (recv_tx, _recv_rx) = mpsc::channel::<WebSocketInboundEvent>(1);
        let (send_tx, send_rx) = mpsc::channel(1);
        let (send_state, send_buffer) = WebSocketSendState::new();
        let mut running_app = RunningWebSocketApp {
            recv_tx,
            requested_subprotocols: RequestedSubprotocols::default(),
            send_state,
            send_buffer,
            send_rx,
            app_task: tokio::spawn(async { Ok(()) }),
            _admission: RequestAdmission::default(),
        };
        let _keep_sender_alive = send_tx;

        let (tx_bytes, app_finished) = drive_denial_response(
            &mut transport,
            status_code::FORBIDDEN,
            Vec::new(),
            &mut running_app,
        )
        .await
        .expect("empty denial response is emitted");

        assert_eq!(tx_bytes, 0);
        assert!(app_finished);
        assert_eq!(transport.calls, ["send_final_denial_response"]);
        assert_eq!(transport.final_bodies, [Bytes::new()]);
    }

    #[tokio::test]
    async fn incomplete_denial_response_aborts_transport_when_app_ends() {
        let mut transport = RecordingHandshakeTransport::default();
        let (recv_tx, _recv_rx) = mpsc::channel::<WebSocketInboundEvent>(1);
        let (send_tx, send_rx) = mpsc::channel(1);
        let (send_state, send_buffer) = WebSocketSendState::new();
        assert!(matches!(
            send_state.push_or_forward(WebSocketOutboundEvent::HttpResponseBody {
                body: PayloadBytes::from(Bytes::from_static(b"denied")),
                more_body: true,
            }),
            WebSocketSendDisposition::Buffered
        ));
        let mut running_app = RunningWebSocketApp {
            recv_tx,
            requested_subprotocols: RequestedSubprotocols::default(),
            send_state,
            send_buffer,
            send_rx,
            app_task: tokio::spawn(async { Ok(()) }),
            _admission: RequestAdmission::default(),
        };
        let _keep_sender_alive = send_tx;

        let err = drive_denial_response(
            &mut transport,
            status_code::FORBIDDEN,
            Vec::new(),
            &mut running_app,
        )
        .await
        .expect_err("incomplete denial response is rejected");

        assert!(matches!(
            err,
            H2CornError::HttpResponse(HttpResponseError::AppReturnedWithoutCompletingResponse)
        ));
        assert_eq!(
            transport.calls,
            [
                "start_denial_response",
                "send_denial_body",
                "abort_denial_response"
            ]
        );
        assert_eq!(transport.body_chunks, [Bytes::from_static(b"denied")]);
        drop(running_app);
    }

    #[cfg(target_pointer_width = "64")]
    #[tokio::test]
    async fn denial_response_future_stays_within_size_budget() {
        let mut transport = RecordingHandshakeTransport::default();
        let (recv_tx, _recv_rx) = mpsc::channel::<WebSocketInboundEvent>(1);
        let (send_tx, send_rx) = mpsc::channel(1);
        let (send_state, send_buffer) = WebSocketSendState::new();
        assert!(matches!(
            send_state.push_or_forward(WebSocketOutboundEvent::HttpResponseBody {
                body: PayloadBytes::from(Bytes::from_static(b"denied")),
                more_body: false,
            }),
            WebSocketSendDisposition::Buffered
        ));
        let mut running_app = RunningWebSocketApp {
            recv_tx,
            requested_subprotocols: RequestedSubprotocols::default(),
            send_state,
            send_buffer,
            send_rx,
            app_task: tokio::spawn(async {
                pending::<()>().await;
                Ok(())
            }),
            _admission: RequestAdmission::default(),
        };
        let _keep_sender_alive = send_tx;

        let future = drive_denial_response(
            &mut transport,
            status_code::FORBIDDEN,
            Vec::new(),
            &mut running_app,
        );

        assert!(size_of_val(&future) <= 3344);
        drop(future);
        running_app.app_task.abort();
        let _ = (&mut running_app.app_task).await;
        drop(running_app);
    }
}
