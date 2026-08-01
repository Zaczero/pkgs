use pyo3::pybacked::PyBackedStr;

use super::super::app::{AppStep, RunningWebSocketApp};
use super::WebSocketHandshakeTransport;
use crate::bridge::{HttpOutboundEvent, WebSocketOutboundEvent};
use crate::error::{ErrorExt as _, H2CornError, WebSocketError};
use crate::http::header::ResponseConnectionDirective;
use crate::http::response::{
    HttpResponseTransport, ResponseActions, ResponseController, apply_http_event, finalize_response,
};
use crate::http::types::{FinalResponseStatus, ResponseHeaders, status_code};
use crate::log::{ResponseLogState, WebSocketAccessLogState};

pub(super) enum HandshakeEvent {
    Accept {
        subprotocol: Option<PyBackedStr>,
        headers: ResponseHeaders,
    },
    Close,
    DenialStart {
        status: FinalResponseStatus,
        headers: ResponseHeaders,
        directive: ResponseConnectionDirective,
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
        WebSocketOutboundEvent::HttpResponseStart {
            status,
            headers,
            directive,
        } => Ok(HandshakeEvent::DenialStart {
            status,
            headers,
            directive,
        }),
        other => WebSocketError::unexpected_initial_event(other.message_type()).err(),
    }
}

fn parse_denial_body_event(
    event: WebSocketOutboundEvent,
) -> Result<HttpOutboundEvent, H2CornError> {
    match event {
        WebSocketOutboundEvent::HttpResponseBody { body, more_body } => {
            Ok(HttpOutboundEvent::Body { body, more_body })
        },
        other => WebSocketError::unexpected_denial_body_event(other.message_type()).err(),
    }
}

pub(super) async fn receive_handshake_event(
    running_app: &mut RunningWebSocketApp,
) -> Result<HandshakeEvent, H2CornError> {
    match running_app.next_handshake_step().await {
        AppStep::Event(event) => parse_handshake_event(event),
        AppStep::Done(Err(err)) => Err(err),
        AppStep::Done(Ok(())) => WebSocketError::AppEndedBeforeHandshake.err(),
    }
}

pub(super) async fn drive_denial_response<T>(
    transport: &mut T,
    status: FinalResponseStatus,
    headers: ResponseHeaders,
    directive: ResponseConnectionDirective,
    running_app: &mut RunningWebSocketApp,
) -> Result<(u64, bool), H2CornError>
where
    T: HttpResponseTransport,
{
    let mut response = ResponseController::new(false, false);
    let mut actions = ResponseActions::new();
    let mut response_log = ResponseLogState::default();
    apply_http_event(
        &mut response,
        transport,
        &mut actions,
        &mut response_log,
        HttpOutboundEvent::Start {
            status,
            headers,
            trailers: false,
            directive,
        },
    )
    .await?;

    while !response.is_complete() {
        match running_app.next_handshake_step().await {
            AppStep::Event(outbound) => {
                let flush_result = match parse_denial_body_event(outbound) {
                    Ok(event) => {
                        apply_http_event(
                            &mut response,
                            transport,
                            &mut actions,
                            &mut response_log,
                            event,
                        )
                        .await
                    },
                    Err(err) => Err(err),
                };
                if let Err(err) = flush_result {
                    finalize_response(
                        &mut response,
                        transport,
                        &mut actions,
                        &mut response_log,
                        Err(err),
                    )
                    .await?;
                    return Ok((response_log.response_body_bytes, false));
                }
            },
            AppStep::Done(result) => {
                finalize_response(
                    &mut response,
                    transport,
                    &mut actions,
                    &mut response_log,
                    result,
                )
                .await?;
                return Ok((response_log.response_body_bytes, true));
            },
        }
    }

    Ok((response_log.response_body_bytes, false))
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
    running_app.task.abort();
    access_log.emit_http_response(status_code::INTERNAL_SERVER_ERROR, 0);
    Err(err)
}

#[cfg(test)]
mod tests {
    use std::mem::size_of_val;

    use bytes::Bytes;
    use pyo3::Python;
    use tokio::sync::mpsc;

    use super::{HandshakeEvent, drive_denial_response, receive_handshake_event};
    use crate::bridge::{
        PayloadBytes, WebSocketOutboundEvent, WebSocketSendDisposition, WebSocketSendState,
        websocket_inbound_channel,
    };
    use crate::error::{ErrorKind, H2CornError, HttpResponseError};
    use crate::http::header::ResponseConnectionDirective;
    use crate::http::response::{FinalResponseBody, HttpResponseTransport, ResponseAction};
    use crate::http::types::{FinalResponseStatus, HttpStatusCode, ResponseHeaders, status_code};
    use crate::pyloop::TaskSlot;
    use crate::runtime::{AppRuntimeHandle, test_fixtures};
    use crate::websocket::app::{RunningWebSocketApp, WebSocketAppTask};
    use crate::websocket::close_code;
    use crate::websocket::session::WebSocketHandshakeTransport;

    #[derive(Default)]
    struct RecordingHandshakeTransport {
        calls: Vec<&'static str>,
        body_chunks: Vec<Bytes>,
        final_bodies: Vec<Bytes>,
    }

    impl HttpResponseTransport for RecordingHandshakeTransport {
        async fn apply_response_action(
            &mut self,
            action: ResponseAction,
        ) -> Result<(), H2CornError> {
            match action {
                // A WebSocket scope never advertises the extension, so a hint
                // cannot reach the handshake-denial transport.
                ResponseAction::EarlyHint(_) => {},
                ResponseAction::Final { body, .. } => {
                    self.calls.push("send_final_denial_response");
                    self.final_bodies.push(match body {
                        FinalResponseBody::Empty => Bytes::new(),
                        FinalResponseBody::Bytes(body) => Bytes::copy_from_slice(body.as_ref()),
                        FinalResponseBody::File { .. } | FinalResponseBody::Suppressed { .. } => {
                            unreachable!(
                                "websocket denial responses never send files or suppressed bodies"
                            )
                        },
                    });
                },
                ResponseAction::Start { .. } => self.calls.push("start_denial_response"),
                ResponseAction::Body(body) => {
                    self.calls.push("send_denial_body");
                    self.body_chunks.push(Bytes::copy_from_slice(body.as_ref()));
                },
                ResponseAction::Finish => self.calls.push("finish_denial_response"),
                ResponseAction::InternalError => self.calls.push("send_internal_error_response"),
                ResponseAction::AbortIncomplete => self.calls.push("abort_denial_response"),
                ResponseAction::File { .. } | ResponseAction::FinishWithTrailers(_) => {
                    unreachable!("websocket denials do not admit files or trailers")
                },
            }
            Ok(())
        }

        async fn flush_buffered(&mut self) -> Result<(), H2CornError> {
            self.calls.push("flush");
            Ok(())
        }
    }

    impl WebSocketHandshakeTransport for RecordingHandshakeTransport {
        fn accept_status(&self) -> HttpStatusCode {
            status_code::SWITCHING_PROTOCOLS
        }

        async fn send_empty_response(
            &mut self,
            _status: HttpStatusCode,
        ) -> Result<(), H2CornError> {
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
    }

    fn final_status(status: HttpStatusCode) -> FinalResponseStatus {
        FinalResponseStatus::new(status).expect("test status is final")
    }

    fn test_app() -> AppRuntimeHandle {
        Python::initialize();
        Python::attach(test_fixtures::app_runtime)
    }

    /// Production app task whose slot resolves immediately with `Ok(())`.
    fn completed_app_task(app: &AppRuntimeHandle) -> WebSocketAppTask {
        let slot = TaskSlot::with_guard(test_fixtures::request_task_guard(app));
        let task = WebSocketAppTask::new(std::sync::Arc::clone(&slot).wait(app.main_shard()));
        slot.fill(Ok(()));
        task
    }

    /// Production app task whose slot never resolves; abandonment queues
    /// `CancelTask` on the test-stub shard harmlessly.
    fn pending_app_task(app: &AppRuntimeHandle) -> WebSocketAppTask {
        let slot = TaskSlot::with_guard(test_fixtures::request_task_guard(app));
        WebSocketAppTask::new(slot.wait(app.main_shard()))
    }

    fn running_app_with_buffered_event(
        event: WebSocketOutboundEvent,
        task: WebSocketAppTask,
    ) -> RunningWebSocketApp {
        let (recv_tx, _recv_rx) = websocket_inbound_channel(1);
        let (_send_tx, send_rx) = mpsc::channel(1);
        let (send_state, send_buffer) = WebSocketSendState::new();
        assert!(matches!(
            send_state.push_or_forward(event),
            WebSocketSendDisposition::Buffered
        ));

        RunningWebSocketApp {
            recv_tx,
            send_state,
            send_buffer,
            send_rx,
            task,
        }
    }

    #[tokio::test]
    async fn initial_handshake_event_prefers_buffered_messages() {
        let app = test_app();
        let mut running_app = running_app_with_buffered_event(
            WebSocketOutboundEvent::Close {
                code: close_code::NORMAL,
                reason: None,
            },
            pending_app_task(&app),
        );

        let event = receive_handshake_event(&mut running_app)
            .await
            .expect("buffered close event is returned");
        assert!(matches!(event, HandshakeEvent::Close));

        running_app.task.abort();
        drop(running_app);
    }

    #[tokio::test]
    async fn unary_denial_response_collapses_into_final_response() {
        let mut transport = RecordingHandshakeTransport::default();
        let (recv_tx, _recv_rx) = websocket_inbound_channel(1);
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
            send_state,
            send_buffer,
            send_rx,
            task: completed_app_task(&test_app()),
        };
        let _keep_sender_alive = send_tx;

        let (tx_bytes, app_finished) = drive_denial_response(
            &mut transport,
            final_status(status_code::FORBIDDEN),
            Vec::new(),
            ResponseConnectionDirective::default(),
            &mut running_app,
        )
        .await
        .expect("unary denial response is emitted");

        assert_eq!(tx_bytes, 6);
        assert!(!app_finished);
        assert_eq!(transport.calls, ["send_final_denial_response", "flush"]);
        assert_eq!(transport.final_bodies, [Bytes::from_static(b"denied")]);
    }

    #[tokio::test]
    async fn forwarded_denial_body_precedes_app_completion() {
        let mut transport = RecordingHandshakeTransport::default();
        let status = final_status(HttpStatusCode::new(401).expect("401 is a valid status"));
        let (recv_tx, _recv_rx) = websocket_inbound_channel(1);
        let (send_tx, send_rx) = mpsc::channel(1);
        let (send_state, send_buffer) = WebSocketSendState::new();
        assert!(matches!(
            send_state.push_or_forward(WebSocketOutboundEvent::HttpResponseStart {
                status,
                headers: ResponseHeaders::new(),
                directive: ResponseConnectionDirective::default(),
            }),
            WebSocketSendDisposition::Buffered
        ));
        send_buffer
            .take_ready()
            .expect("the handshake start consumed the inline event");
        let body = match send_state.push_or_forward(WebSocketOutboundEvent::HttpResponseBody {
            body: PayloadBytes::from(Bytes::from_static(b"denied")),
            more_body: false,
        }) {
            WebSocketSendDisposition::Forward(event) => event,
            WebSocketSendDisposition::Buffered | WebSocketSendDisposition::Closed => {
                unreachable!("the second handshake event must enter the bounded channel")
            },
        };
        send_tx
            .try_send(body)
            .expect("the forwarded denial body fits the empty bounded channel");
        let mut running_app = RunningWebSocketApp {
            recv_tx,
            send_state,
            send_buffer,
            send_rx,
            task: completed_app_task(&test_app()),
        };
        let _keep_sender_alive = send_tx;

        let (tx_bytes, app_finished) = drive_denial_response(
            &mut transport,
            status,
            ResponseHeaders::new(),
            ResponseConnectionDirective::default(),
            &mut running_app,
        )
        .await
        .expect("the forwarded unary denial body is emitted");

        assert_eq!(tx_bytes, 6);
        assert!(!app_finished);
        assert_eq!(transport.calls, ["send_final_denial_response", "flush"]);
        assert_eq!(transport.final_bodies, [Bytes::from_static(b"denied")]);
    }

    #[tokio::test]
    async fn empty_denial_response_finishes_as_final_response_when_app_ends() {
        let mut transport = RecordingHandshakeTransport::default();
        let (recv_tx, _recv_rx) = websocket_inbound_channel(1);
        let (send_tx, send_rx) = mpsc::channel(1);
        let (send_state, send_buffer) = WebSocketSendState::new();
        let mut running_app = RunningWebSocketApp {
            recv_tx,
            send_state,
            send_buffer,
            send_rx,
            task: completed_app_task(&test_app()),
        };
        let _keep_sender_alive = send_tx;

        let (tx_bytes, app_finished) = drive_denial_response(
            &mut transport,
            final_status(status_code::FORBIDDEN),
            Vec::new(),
            ResponseConnectionDirective::default(),
            &mut running_app,
        )
        .await
        .expect("empty denial response is emitted");

        assert_eq!(tx_bytes, 0);
        assert!(app_finished);
        assert_eq!(transport.calls, ["send_final_denial_response", "flush"]);
        assert_eq!(transport.final_bodies, [Bytes::new()]);
    }

    #[tokio::test]
    async fn incomplete_denial_response_aborts_transport_when_app_ends() {
        let mut transport = RecordingHandshakeTransport::default();
        let (recv_tx, _recv_rx) = websocket_inbound_channel(1);
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
            send_state,
            send_buffer,
            send_rx,
            task: completed_app_task(&test_app()),
        };
        let _keep_sender_alive = send_tx;

        let err = drive_denial_response(
            &mut transport,
            final_status(status_code::FORBIDDEN),
            Vec::new(),
            ResponseConnectionDirective::default(),
            &mut running_app,
        )
        .await
        .expect_err("incomplete denial response is rejected");

        assert!(matches!(
            err.kind(),
            ErrorKind::HttpResponse(HttpResponseError::AppReturnedWithoutCompletingResponse)
        ));
        // Each applied batch ends with a flush, so a streamed denial body
        // reaches the client instead of waiting in the write buffer.
        assert_eq!(transport.calls, [
            "start_denial_response",
            "send_denial_body",
            "flush",
            "abort_denial_response",
            "flush"
        ]);
        assert_eq!(transport.body_chunks, [Bytes::from_static(b"denied")]);
        drop(running_app);
    }

    #[cfg(target_pointer_width = "64")]
    #[tokio::test]
    async fn denial_response_future_stays_within_size_budget() {
        let mut transport = RecordingHandshakeTransport::default();
        let (recv_tx, _recv_rx) = websocket_inbound_channel(1);
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
            send_state,
            send_buffer,
            send_rx,
            task: pending_app_task(&test_app()),
        };
        let _keep_sender_alive = send_tx;

        let future = drive_denial_response(
            &mut transport,
            final_status(status_code::FORBIDDEN),
            Vec::new(),
            ResponseConnectionDirective::default(),
            &mut running_app,
        );

        assert!(size_of_val(&future) <= 3344);
        drop(future);
        running_app.task.abort();
        drop(running_app);
    }
}
