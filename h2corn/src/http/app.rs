mod buffered;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use http::Method;
use tokio::sync::mpsc;

use crate::app_call::AppCallArgs;
use crate::bridge::RequestInputShared;
use crate::error::H2CornError;
pub(crate) use crate::http::app::buffered::{
    AdmittedHttpOutboundEvent, HttpSendBuffer, HttpSendDisposition, HttpSendState, HttpSendWaiter,
    outbound_charge,
};
use crate::http::response::{
    HttpResponseTransport, ResponseActions, ResponseController, apply_admitted_http_event,
    finalize_response,
};
use crate::log::ResponseLogState;
use crate::runtime::{RequestAdmission, RequestContext, StreamInput, start_app_call};

pub(crate) enum HttpRequestBody {
    NoBody,
    Single(Bytes),
    Stream {
        rx: mpsc::Receiver<StreamInput>,
        disconnect: Arc<RequestInputShared>,
    },
}

pub(crate) struct HttpRequestState {
    response: ResponseController,
    send_buffer: HttpSendBuffer,
}

pub(crate) struct RunningHttpRequest<F> {
    pub(crate) state: HttpRequestState,
    pub(crate) app_task: F,
}

/// Poll an app driver exactly once without yielding to the scheduler.
///
/// Connection protocols use this before work that can reject synchronously,
/// ensuring the admitted app call has been handed to its loop while retaining
/// normal concurrent driving once it suspends.
pub(crate) fn poll_app_task_once<F>(mut task: Pin<&mut F>) -> Poll<F::Output>
where
    F: Future,
{
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    task.as_mut().poll(&mut cx)
}

pub(crate) fn start_asgi_http_request(
    ctx: Box<RequestContext>,
    request_body: HttpRequestBody,
    admission: RequestAdmission,
    send: (HttpSendState, HttpSendBuffer),
) -> RunningHttpRequest<impl Future<Output = Result<(), H2CornError>> + Send> {
    let (send_state, send_buffer) = send;
    let head_only = ctx.request.method == Method::HEAD;
    let supports_response_trailers = ctx.request.accepts_trailers();
    // The guard keeps the connection alive, which keeps the app runtime and
    // the teardown tracker alive with it.
    let connection = Arc::clone(&ctx.connection);

    let app_task = start_app_call(
        connection,
        AppCallArgs::http(ctx, request_body, send_state),
        admission,
    );

    RunningHttpRequest {
        state: HttpRequestState {
            response: ResponseController::new(head_only, supports_response_trailers),
            send_buffer,
        },
        app_task,
    }
}

pub(crate) async fn run_asgi_http_request<T>(
    ctx: Box<RequestContext>,
    request_body: HttpRequestBody,
    admission: RequestAdmission,
    transport: &mut T,
    response_log: &mut ResponseLogState,
) -> Result<(), H2CornError>
where
    T: HttpResponseTransport,
{
    let send_state = transport.new_http_send_state();
    let started = start_asgi_http_request(ctx, request_body, admission, send_state);
    drive_http_request(started, transport, response_log).await
}

pub(crate) async fn drive_http_request<T, F>(
    started: RunningHttpRequest<F>,
    transport: &mut T,
    response_log: &mut ResponseLogState,
) -> Result<(), H2CornError>
where
    T: HttpResponseTransport,
    F: Future<Output = Result<(), H2CornError>> + Send,
{
    let RunningHttpRequest { state, app_task } = started;
    tokio::pin!(app_task);
    drive_pinned_http_request(state, app_task.as_mut(), transport, response_log).await
}

pub(crate) async fn drive_pinned_http_request<T, F>(
    state: HttpRequestState,
    app_task: Pin<&mut F>,
    transport: &mut T,
    response_log: &mut ResponseLogState,
) -> Result<(), H2CornError>
where
    T: HttpResponseTransport,
    F: Future<Output = Result<(), H2CornError>> + Send,
{
    let HttpRequestState {
        mut response,
        mut send_buffer,
    } = state;
    let mut actions = ResponseActions::new();
    drive_response(
        &mut response,
        &mut send_buffer,
        &mut actions,
        app_task,
        transport,
        response_log,
    )
    .await
}

pub(crate) async fn try_complete_http_request<T>(
    state: HttpRequestState,
    transport: &mut T,
    response_log: &mut ResponseLogState,
    app_result: Result<(), H2CornError>,
) -> Result<(), H2CornError>
where
    T: HttpResponseTransport,
{
    let HttpRequestState {
        mut response,
        mut send_buffer,
    } = state;
    let mut actions = ResponseActions::new();
    finish_response(
        &mut response,
        &mut send_buffer,
        &mut actions,
        app_result,
        transport,
        response_log,
    )
    .await
}

async fn drive_response<T, F>(
    response: &mut ResponseController,
    send_buffer: &mut HttpSendBuffer,
    actions: &mut ResponseActions,
    mut app_task: Pin<&mut F>,
    transport: &mut T,
    response_log: &mut ResponseLogState,
) -> Result<(), H2CornError>
where
    T: HttpResponseTransport,
    F: Future<Output = Result<(), H2CornError>> + Send,
{
    loop {
        if response.is_complete() {
            // The response is terminal but the app task may still be running.
            // Reject and wake any later send before awaiting that task; events
            // accepted before closure remain drainable by `finish_response`.
            send_buffer.close_outbound();
            break;
        }

        tokio::select! {
            maybe_event = send_buffer.wait_ready() => {
                let Some(event) = maybe_event else {
                    break;
                };
                if let Err(err) =
                    apply_admitted_http_event(response, transport, actions, response_log, event)
                        .await
                {
                    send_buffer.close_send_gate();
                    return finalize_response(response, transport, actions, response_log, Err(err))
                        .await;
                }
            }
            app_result = app_task.as_mut() => {
                return finish_response(
                    response,
                    send_buffer,
                    actions,
                    app_result,
                    transport,
                    response_log,
                )
                .await;
            }
        }
    }

    finish_response(
        response,
        send_buffer,
        actions,
        app_task.await,
        transport,
        response_log,
    )
    .await
}

async fn finish_response<T>(
    response: &mut ResponseController,
    send_buffer: &mut HttpSendBuffer,
    actions: &mut ResponseActions,
    app_result: Result<(), H2CornError>,
    transport: &mut T,
    response_log: &mut ResponseLogState,
) -> Result<(), H2CornError>
where
    T: HttpResponseTransport,
{
    while let Some(event) = send_buffer.take_ready() {
        if response.is_complete() {
            // Reached only by events admitted before the send gate closed --
            // a later send is now rejected at admission instead. Draining
            // rather than applying them keeps an otherwise reusable H1
            // connection, or a sibling H2 stream, out of the blast radius.
            continue;
        }
        if let Err(err) =
            apply_admitted_http_event(response, transport, actions, response_log, event).await
        {
            send_buffer.close_send_gate();
            return finalize_response(response, transport, actions, response_log, Err(err)).await;
        }
    }
    send_buffer.close_send_gate();
    finalize_response(response, transport, actions, response_log, app_result).await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use tokio::sync::Notify;
    use tokio::time::timeout;

    use super::{HttpSendDisposition, HttpSendState, drive_response};
    use crate::bridge::{HttpOutboundEvent, PayloadBytes};
    use crate::error::{ErrorKind, H2CornError, HttpResponseError};
    use crate::http::header::ResponseConnectionDirective;
    use crate::http::response::{
        HttpResponseTransport, ResponseAction, ResponseActions, ResponseController,
    };
    use crate::http::types::{FinalResponseStatus, ResponseHeaders, status_code};
    use crate::log::ResponseLogState;

    #[derive(Default)]
    struct RecordingTransport {
        calls: Vec<&'static str>,
    }

    struct FinalizationBarrierTransport {
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl HttpResponseTransport for FinalizationBarrierTransport {
        async fn apply_response_action(
            &mut self,
            _action: ResponseAction,
        ) -> Result<(), H2CornError> {
            self.entered.notify_one();
            self.release.notified().await;
            Ok(())
        }

        async fn flush_buffered(&mut self) -> Result<(), H2CornError> {
            Ok(())
        }
    }

    impl HttpResponseTransport for RecordingTransport {
        async fn apply_response_action(
            &mut self,
            action: ResponseAction,
        ) -> Result<(), H2CornError> {
            self.calls.push(match action {
                ResponseAction::EarlyHint(_) => "early_hint",
                ResponseAction::Final { .. } => "final",
                ResponseAction::Start { .. } => "start",
                ResponseAction::Body(_) => "body",
                ResponseAction::File { .. } => "file",
                ResponseAction::Finish => "finish",
                ResponseAction::FinishWithTrailers(_) => "trailers",
                ResponseAction::InternalError => "internal_error",
                ResponseAction::AbortIncomplete => "abort",
            });
            Ok(())
        }

        async fn flush_buffered(&mut self) -> Result<(), H2CornError> {
            self.calls.push("flush");
            Ok(())
        }
    }

    fn streaming_response() -> ResponseController {
        let mut response = ResponseController::new(false, false);
        let mut applied = ResponseActions::new();
        response
            .handle_start(
                FinalResponseStatus::new(status_code::OK).expect("test status is final"),
                ResponseHeaders::new(),
                false,
                ResponseConnectionDirective::default(),
            )
            .expect("response starts");
        response
            .handle_body(
                &mut applied,
                PayloadBytes::from(Bytes::from_static(b"prefix")),
                true,
            )
            .expect("response enters streaming mode");
        response
    }

    async fn app_send(state: &HttpSendState, event: HttpOutboundEvent) -> bool {
        match state.push_or_forward(event) {
            HttpSendDisposition::Buffered | HttpSendDisposition::Sent => true,
            HttpSendDisposition::Backpressured(waiter) => waiter.send().await,
            HttpSendDisposition::Closed => false,
        }
    }

    fn body_event(body: &'static [u8], more_body: bool) -> HttpOutboundEvent {
        HttpOutboundEvent::Body {
            body: PayloadBytes::from(Bytes::from_static(body)),
            more_body,
        }
    }

    #[tokio::test]
    async fn immediate_post_terminal_sends_fail_while_driver_is_starved() {
        let mut response = ResponseController::new(false, false);
        let (send_state, mut send_buffer) = HttpSendState::new();
        assert!(send_buffer.take_ready().is_none());
        let app_task = async move {
            assert!(
                app_send(&send_state, HttpOutboundEvent::Start {
                    status: FinalResponseStatus::new(status_code::OK)
                        .expect("test status is final"),
                    headers: ResponseHeaders::new(),
                    trailers: false,
                    directive: ResponseConnectionDirective::default(),
                },)
                .await
            );
            assert!(app_send(&send_state, body_event(b"prefix", true)).await);
            assert!(app_send(&send_state, body_event(b"final", false)).await);
            for _ in 0..3 {
                assert!(!app_send(&send_state, body_event(b"extra", false)).await);
            }
            Ok(())
        };
        tokio::pin!(app_task);
        let mut actions = ResponseActions::new();
        let mut transport = RecordingTransport::default();
        let mut response_log = ResponseLogState::default();

        timeout(
            Duration::from_secs(1),
            drive_response(
                &mut response,
                &mut send_buffer,
                &mut actions,
                app_task.as_mut(),
                &mut transport,
                &mut response_log,
            ),
        )
        .await
        .expect("response completion cannot deadlock a buffered app send")
        .expect("post-terminal sends are rejected before the driver runs");
        // Flushes are batching, not contract: pinning where they land makes an
        // unrelated transport change red. What this guards is that the three
        // rejected sends produced no transport write of their own.
        let written: Vec<&str> = transport
            .calls
            .iter()
            .copied()
            .filter(|call| *call != "flush")
            .collect();
        assert_eq!(written, ["start", "body", "body", "finish"]);
    }

    #[tokio::test]
    async fn preterminal_contract_error_survives_logical_close() {
        let mut response = ResponseController::new(false, false);
        let (send_state, mut send_buffer) = HttpSendState::new();
        let start = || HttpOutboundEvent::Start {
            status: FinalResponseStatus::new(status_code::OK).expect("test status is final"),
            headers: ResponseHeaders::new(),
            trailers: false,
            directive: ResponseConnectionDirective::default(),
        };
        assert!(app_send(&send_state, start()).await);
        assert!(app_send(&send_state, start()).await);

        let mut actions = ResponseActions::new();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let mut transport = FinalizationBarrierTransport {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        };
        let mut response_log = ResponseLogState::default();
        let finalization = super::finish_response(
            &mut response,
            &mut send_buffer,
            &mut actions,
            Ok(()),
            &mut transport,
            &mut response_log,
        );
        let late_send = async {
            entered.notified().await;
            let rejected = matches!(
                send_state.push_or_forward(body_event(b"late", false)),
                HttpSendDisposition::Closed
            );
            release.notify_one();
            rejected
        };
        let (result, rejected) = tokio::join!(finalization, late_send);
        let err = result.expect_err("the queued duplicate start remains authoritative");
        assert!(matches!(
            err.kind(),
            ErrorKind::HttpResponse(HttpResponseError::StartAlreadyReceived)
        ));
        assert!(
            rejected,
            "error finalization closes the application send gate"
        );
    }

    #[tokio::test]
    async fn driver_finalization_closes_send_gate_before_transport_await() {
        let mut response = ResponseController::new(false, false);
        let (send_state, mut send_buffer) = HttpSendState::new();
        assert!(
            app_send(&send_state, HttpOutboundEvent::Start {
                status: FinalResponseStatus::new(status_code::OK).expect("test status is final"),
                headers: ResponseHeaders::new(),
                trailers: false,
                directive: ResponseConnectionDirective::default(),
            })
            .await
        );

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let mut transport = FinalizationBarrierTransport {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        };
        let mut actions = ResponseActions::new();
        let mut response_log = ResponseLogState::default();
        let finalization = super::finish_response(
            &mut response,
            &mut send_buffer,
            &mut actions,
            Ok(()),
            &mut transport,
            &mut response_log,
        );
        let late_send = async {
            entered.notified().await;
            let rejected = matches!(
                send_state.push_or_forward(body_event(b"late", false)),
                HttpSendDisposition::Closed
            );
            release.notify_one();
            rejected
        };
        let (result, rejected) = tokio::join!(finalization, late_send);

        result.expect("driver-induced completion finishes");
        assert!(
            rejected,
            "the bridge maps this closed disposition to SendAfterCloseError"
        );
    }

    #[tokio::test]
    async fn normal_final_send_and_app_return_still_finish_in_order() {
        let mut response = streaming_response();
        let (send_state, mut send_buffer) = HttpSendState::new();
        assert!(send_buffer.take_ready().is_none());
        let app_task = async move {
            assert!(app_send(&send_state, body_event(b"final", false)).await);
            Ok(())
        };
        tokio::pin!(app_task);
        let mut actions = ResponseActions::new();
        let mut transport = RecordingTransport::default();
        let mut response_log = ResponseLogState::default();

        drive_response(
            &mut response,
            &mut send_buffer,
            &mut actions,
            app_task.as_mut(),
            &mut transport,
            &mut response_log,
        )
        .await
        .expect("normal final send completes");

        assert!(response.is_complete());
        // The trailing flush is what makes each applied batch visible to the
        // client before the application awaits again.
        assert_eq!(transport.calls, ["body", "finish", "flush"]);
    }
}
