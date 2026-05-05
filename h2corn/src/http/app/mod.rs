mod buffered;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub use buffered::HttpSendState;
use bytes::Bytes;
use http::Method;
use pyo3::Py;
use tokio::sync::mpsc;

use self::buffered::HttpSendBuffer;
use super::response::{
    HttpResponseTransport, ResponseActions, ResponseController, apply_http_event, finalize_response,
};
use crate::bridge::{PyHttpReceive, PyHttpSend};
use crate::error::H2CornError;
use crate::http::scope::build_http_scope;
use crate::runtime::{RequestAdmission, RequestContext, StreamInput, start_app_call};

pub enum HttpRequestBody {
    NoBody,
    Single(Bytes),
    Stream(mpsc::Receiver<StreamInput>),
}

pub struct HttpRequestState {
    response: ResponseController,
    send_buffer: HttpSendBuffer,
    _admission: RequestAdmission,
}

pub struct RunningHttpRequest<F> {
    pub(crate) state: HttpRequestState,
    pub(crate) app_task: F,
}

pub fn start_asgi_http_request(
    ctx: RequestContext,
    request_body: HttpRequestBody,
    admission: RequestAdmission,
) -> Result<RunningHttpRequest<impl Future<Output = Result<(), H2CornError>> + Send>, H2CornError> {
    let head_only = ctx.request.method == Method::HEAD;
    let supports_response_trailers = ctx.request.accepts_trailers();
    let (send_state, send_buffer) = HttpSendState::new();
    let app = Arc::clone(&ctx.connection.app);

    let app_task = start_app_call(&app, move |py, app| {
        let scope = build_http_scope(py, &ctx)?.into_any();
        let receive = match request_body {
            HttpRequestBody::NoBody => PyHttpReceive::new_no_body(app.locals.clone()),
            HttpRequestBody::Single(body) => PyHttpReceive::new_single(app.locals.clone(), body),
            HttpRequestBody::Stream(stream_rx) => {
                PyHttpReceive::new_stream(app.locals.clone(), stream_rx)
            },
        };
        let receive = Py::new(py, receive)?.into_bound(py).into_any();
        let send = Py::new(py, PyHttpSend::new(app.locals.clone(), send_state))?
            .into_bound(py)
            .into_any();
        Ok((scope, receive, send))
    })?;

    Ok(RunningHttpRequest {
        state: HttpRequestState {
            response: ResponseController::new(head_only, supports_response_trailers),
            send_buffer,
            _admission: admission,
        },
        app_task,
    })
}

pub async fn run_asgi_http_request<T>(
    ctx: RequestContext,
    request_body: HttpRequestBody,
    admission: RequestAdmission,
    transport: &mut T,
) -> Result<(), H2CornError>
where
    T: HttpResponseTransport,
{
    let started = match start_asgi_http_request(ctx, request_body, admission) {
        Ok(started) => started,
        Err(err) => {
            transport.send_internal_error_response().await?;
            return Err(err);
        },
    };

    drive_http_request(started, transport).await
}

pub async fn drive_http_request<T, F>(
    started: RunningHttpRequest<F>,
    transport: &mut T,
) -> Result<(), H2CornError>
where
    T: HttpResponseTransport,
    F: Future<Output = Result<(), H2CornError>> + Send,
{
    let RunningHttpRequest { state, app_task } = started;
    tokio::pin!(app_task);
    drive_pinned_http_request(state, app_task.as_mut(), transport).await
}

pub async fn drive_pinned_http_request<T, F>(
    state: HttpRequestState,
    app_task: Pin<&mut F>,
    transport: &mut T,
) -> Result<(), H2CornError>
where
    T: HttpResponseTransport,
    F: Future<Output = Result<(), H2CornError>> + Send,
{
    let HttpRequestState {
        mut response,
        mut send_buffer,
        ..
    } = state;
    let mut actions = ResponseActions::new();
    drive_response(
        &mut response,
        &mut send_buffer,
        &mut actions,
        app_task,
        transport,
    )
    .await
}

pub async fn try_complete_http_request<T>(
    state: HttpRequestState,
    transport: &mut T,
    app_result: Result<(), H2CornError>,
) -> Result<(), H2CornError>
where
    T: HttpResponseTransport,
{
    let HttpRequestState {
        mut response,
        mut send_buffer,
        ..
    } = state;
    let mut actions = ResponseActions::new();
    finish_response(
        &mut response,
        &mut send_buffer,
        &mut actions,
        app_result,
        transport,
    )
    .await
}

async fn drive_response<T, F>(
    response: &mut ResponseController,
    send_buffer: &mut HttpSendBuffer,
    actions: &mut ResponseActions,
    mut app_task: Pin<&mut F>,
    transport: &mut T,
) -> Result<(), H2CornError>
where
    T: HttpResponseTransport,
    F: Future<Output = Result<(), H2CornError>> + Send,
{
    loop {
        if response.is_complete() {
            break;
        }

        tokio::select! {
            maybe_event = send_buffer.wait_ready(response.needs_live_stream()) => {
                let Some(event) = maybe_event else {
                    break;
                };
                if let Err(err) = apply_http_event(response, transport, actions, event).await {
                    return finalize_response(response, transport, actions, Err(err)).await;
                }
            }
            app_result = app_task.as_mut() => {
                return finish_response(response, send_buffer, actions, app_result, transport).await;
            }
        }
    }

    finish_response(response, send_buffer, actions, app_task.await, transport).await
}

async fn finish_response<T>(
    response: &mut ResponseController,
    send_buffer: &mut HttpSendBuffer,
    actions: &mut ResponseActions,
    app_result: Result<(), H2CornError>,
    transport: &mut T,
) -> Result<(), H2CornError>
where
    T: HttpResponseTransport,
{
    while let Some(event) = send_buffer.take_ready(response.needs_live_stream()) {
        if let Err(err) = apply_http_event(response, transport, actions, event).await {
            return finalize_response(response, transport, actions, Err(err)).await;
        }
    }
    finalize_response(response, transport, actions, app_result).await
}
