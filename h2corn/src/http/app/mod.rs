mod buffered;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

pub(crate) use buffered::{HttpSendDisposition, HttpSendState};
use bytes::Bytes;
use http::Method;
use tokio::sync::mpsc;

use self::buffered::HttpSendBuffer;
use super::response::{
    HttpResponseTransport, ResponseActions, ResponseController, apply_http_event, finalize_response,
};
use crate::access_log::ResponseLogState;
use crate::app_call::AppCallArgs;
use crate::bridge::RequestInputShared;
use crate::error::H2CornError;
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
) -> RunningHttpRequest<impl Future<Output = Result<(), H2CornError>> + Send> {
    let head_only = ctx.request.method == Method::HEAD;
    let supports_response_trailers = ctx.request.accepts_trailers();
    let (send_state, send_buffer) = HttpSendState::new();
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
    let started = start_asgi_http_request(ctx, request_body, admission);
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
                    apply_http_event(response, transport, actions, response_log, event).await
                {
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
            // ASGI send is intentionally fire-and-forget until the transport
            // closes. Events already accepted before finalization must not
            // poison an otherwise reusable H1 connection or sibling H2
            // stream; only a later real close reports OSError to the app.
            continue;
        }
        if let Err(err) = apply_http_event(response, transport, actions, response_log, event).await
        {
            return finalize_response(response, transport, actions, response_log, Err(err)).await;
        }
    }
    finalize_response(response, transport, actions, response_log, app_result).await
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use tokio::time::timeout;

    use super::{HttpSendDisposition, HttpSendState, drive_response};
    use crate::access_log::ResponseLogState;
    use crate::bridge::{HttpOutboundEvent, PayloadBytes};
    use crate::error::H2CornError;
    use crate::http::header::ResponseHeaderControl;
    use crate::http::response::{
        HttpResponseTransport, ResponseAction, ResponseActions, ResponseController,
    };
    use crate::http::types::{FinalResponseStatus, ResponseHeaders, status_code};

    #[derive(Default)]
    struct RecordingTransport {
        calls: Vec<&'static str>,
    }

    impl HttpResponseTransport for RecordingTransport {
        async fn apply_response_action(
            &mut self,
            action: ResponseAction,
        ) -> Result<(), H2CornError> {
            self.calls.push(match action {
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
                ResponseHeaderControl::default(),
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
            HttpSendDisposition::Backpressured { tx, event } => tx.send(event).await.is_ok(),
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
    async fn response_completion_ignores_events_accepted_before_close() {
        let mut response = streaming_response();
        let (send_state, mut send_buffer) = HttpSendState::new();
        assert!(send_buffer.take_ready().is_none());
        let app_task = async move {
            assert!(app_send(&send_state, body_event(b"final", false)).await);
            for _ in 0..3 {
                assert!(app_send(&send_state, body_event(b"extra", false)).await);
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
        .expect("accepted post-completion events are ignored");
        assert_eq!(
            transport.calls.get(..2),
            Some(["body", "finish"].as_slice())
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
