use std::fs::File;
use std::num::NonZeroU64;
use std::task::Poll;

use bytes::Bytes;
use tokio::sync::mpsc;

use super::state::RequestTaskCancellation;
use super::websocket::handle_request as handle_websocket_request;
use super::writer::{WriterCommand, WriterCommandBatch};
use super::{H2WriterHandle, InboundStream, RequestSpawnContext};
use crate::access_log::{HttpAccessLogState, ResponseLogState};
use crate::bridge::{PayloadBytes, RequestBodyCounter};
use crate::config::{ResponseHeaderConfig, ServerConfig};
use crate::error::H2CornError;
use crate::h2_frame::{ErrorCode, StreamId};
use crate::http::app::{
    HttpRequestBody, RunningHttpRequest, drive_pinned_http_request, poll_app_task_once,
    start_asgi_http_request, try_complete_http_request,
};
use crate::http::execution::{AppRequestInput, RequestExecution, prepare_request_execution};
use crate::http::planner::{RequestRejection, plan_request};
use crate::http::response::{
    FinalResponseBody, HttpResponseTransport, ResponseAction, ResponseActions, ResponseStart,
};
use crate::http::run_request::run_http_request;
use crate::http::types::{HttpStatusCode, RequestHead, ResponseHeaders, status_code};
use crate::runtime::{RequestAdmission, RequestContext, StreamInput};
use crate::websocket::{WebSocketContext, validate_websocket_request};

struct H2HttpTransport<'a> {
    connection: &'a H2WriterHandle,
    stream_id: StreamId,
    response_log: ResponseLogState,
}

struct H2HttpRequestContext {
    ctx: Box<RequestContext>,
    connection: H2WriterHandle,
    admission: RequestAdmission,
    input: AppRequestInput,
}

impl H2HttpTransport<'_> {
    async fn send_response_action(&mut self, action: ResponseAction) -> Result<(), H2CornError> {
        log_response_action(&mut self.response_log, &action);
        let mut commands = WriterCommandBatch::new();
        append_response_action(
            self.stream_id,
            &mut commands,
            action,
            self.connection.config(),
        );
        self.connection
            .send_commands(self.stream_id, commands)
            .await
    }
}

impl HttpResponseTransport for H2HttpTransport<'_> {
    async fn send_final_response(
        &mut self,
        start: ResponseStart,
        body: FinalResponseBody,
    ) -> Result<(), H2CornError> {
        self.send_response_action(ResponseAction::Final { start, body })
            .await
    }

    async fn start_streaming_response(&mut self, start: ResponseStart) -> Result<(), H2CornError> {
        self.send_response_action(ResponseAction::Start { start })
            .await
    }

    async fn send_streaming_body(&mut self, body: PayloadBytes) -> Result<(), H2CornError> {
        self.send_response_action(ResponseAction::Body(body)).await
    }

    async fn send_streaming_file(&mut self, file: File, len: usize) -> Result<(), H2CornError> {
        self.send_response_action(ResponseAction::File {
            file: Box::new(file),
            len,
        })
        .await
    }

    async fn finish_streaming_response(&mut self) -> Result<(), H2CornError> {
        self.send_response_action(ResponseAction::Finish).await
    }

    async fn finish_streaming_with_trailers(
        &mut self,
        trailers: ResponseHeaders,
    ) -> Result<(), H2CornError> {
        self.send_response_action(ResponseAction::FinishWithTrailers(trailers))
            .await
    }

    async fn send_internal_error_response(&mut self) -> Result<(), H2CornError> {
        self.send_response_action(ResponseAction::InternalError)
            .await
    }

    async fn abort_incomplete_response(&mut self) -> Result<(), H2CornError> {
        self.send_response_action(ResponseAction::AbortIncomplete)
            .await
    }

    fn response_log_state(&self) -> ResponseLogState {
        self.response_log
    }

    async fn apply_response_actions(
        &mut self,
        actions: &mut ResponseActions,
    ) -> Result<(), H2CornError> {
        if actions.is_empty() {
            return Ok(());
        }

        let mut commands = WriterCommandBatch::new();

        for action in actions.drain(..) {
            log_response_action(&mut self.response_log, &action);
            append_response_action(
                self.stream_id,
                &mut commands,
                action,
                self.connection.config(),
            );
        }

        self.connection
            .send_commands(self.stream_id, commands)
            .await
    }
}

pub(super) async fn spawn_request_stream(
    stream_id: StreamId,
    request: RequestHead,
    end_stream: bool,
    connection: &H2WriterHandle,
    context: RequestSpawnContext<'_>,
) -> Result<(), H2CornError> {
    if let Some(RequestRejection { status, headers }) =
        prepare_and_spawn_request(stream_id, request, end_stream, connection, context)
    {
        connection
            .send_headers(stream_id, status, headers, true)
            .await?;
    }
    Ok(())
}

/// Perform the common request planning, registration, and task launch without
/// suspension. Only the uncommon immediate-response path is returned to the
/// connection future for asynchronous writer backpressure.
fn prepare_and_spawn_request(
    stream_id: StreamId,
    request: RequestHead,
    end_stream: bool,
    connection: &H2WriterHandle,
    mut context: RequestSpawnContext<'_>,
) -> Option<RequestRejection> {
    let config = &context.connection.config;
    let content_length = request.content_length();
    let websocket = request.protocol_is_websocket().then(|| {
        validate_websocket_request(&request).map_err(|rejection| RequestRejection {
            status: rejection.status,
            headers: rejection.headers,
        })
    });
    let plan = match plan_request(
        &request,
        websocket,
        end_stream,
        config.access_log,
        config.max_request_body_size,
    ) {
        Ok(plan) => plan,
        Err(rejection) => return Some(rejection),
    };
    let Some(prepared) = prepare_request_execution(&context.connection.app, plan) else {
        return Some(RequestRejection {
            status: status_code::SERVICE_UNAVAILABLE,
            headers: ResponseHeaders::new(),
        });
    };
    let request = RequestContext::new(context.connection.clone(), request);
    match prepared {
        RequestExecution::Http { admission, input } => {
            let (input_tx, app_input) = input.split();
            let cancellation = app_input.disconnect_signal().map_or(
                RequestTaskCancellation::Immediate,
                RequestTaskCancellation::AfterPendingReceive,
            );
            register_inbound_stream(
                &mut context,
                stream_id,
                InboundStream::new(
                    input_tx,
                    true,
                    end_stream,
                    content_length,
                    app_input.body_bytes_read(),
                    config.max_request_body_size.map(NonZeroU64::get),
                    config.http2.initial_stream_window_size.get(),
                ),
            );
            spawn_http_request(
                context.tasks,
                stream_id,
                cancellation,
                H2HttpRequestContext {
                    ctx: request,
                    connection: connection.clone(),
                    admission,
                    input: app_input,
                },
            );
        },
        RequestExecution::WebSocket {
            meta,
            admission,
            input,
        } => {
            register_inbound_stream(
                &mut context,
                stream_id,
                InboundStream::new(
                    Some(input.tx.clone()),
                    false,
                    end_stream,
                    content_length,
                    None,
                    config.max_request_body_size.map(NonZeroU64::get),
                    config.http2.initial_stream_window_size.get(),
                ),
            );
            spawn_websocket_request(
                context.tasks,
                WebSocketContext {
                    request,
                    admission,
                    meta,
                },
                stream_id,
                input.rx,
                connection.clone(),
            );
        },
    }
    None
}

/// Keep construction of the sizeable HTTP task future out of the H2
/// connection poll frame. This adds one request-scoped allocation; the exact
/// no-box A/B is recorded in the performance ledger because LTO otherwise
/// reconstructs the task future on the connection stack.
fn spawn_http_request(
    tasks: &mut super::state::RequestTasks,
    stream_id: StreamId,
    cancellation: RequestTaskCancellation,
    task: H2HttpRequestContext,
) {
    tasks.spawn(stream_id, cancellation, async move {
        let _ = Box::pin(handle_http_request(stream_id, task)).await;
    });
}

/// As above, isolate the mutually exclusive WebSocket future construction so
/// the connection stack never reserves space for both request engines.
fn spawn_websocket_request(
    tasks: &mut super::state::RequestTasks,
    context: WebSocketContext,
    stream_id: StreamId,
    input: mpsc::Receiver<StreamInput>,
    connection: H2WriterHandle,
) {
    // WebSocket sessions are long-lived and their protocol future is much
    // larger than an ordinary HTTP request. One session-scoped box keeps that
    // rare state out of the H2 connection poll stack; Tokio's task allocation
    // then contains only the pointer. Ordinary HTTP requests remain on the
    // existing single-allocation Tokio task path.
    tasks.spawn(stream_id, RequestTaskCancellation::Immediate, async move {
        let _ = Box::pin(handle_websocket_request(
            context, stream_id, input, connection,
        ))
        .await;
    });
}

fn register_inbound_stream(
    context: &mut RequestSpawnContext<'_>,
    stream_id: StreamId,
    stream: InboundStream,
) {
    context.streams.insert(stream_id, stream);
}

async fn handle_http_request(
    stream_id: StreamId,
    task: H2HttpRequestContext,
) -> Result<(), H2CornError> {
    let mut transport = H2HttpTransport {
        connection: &task.connection,
        stream_id,
        response_log: ResponseLogState::default(),
    };
    let AppRequestInput::Stream {
        rx: request_body_rx,
        body_bytes_read,
        disconnect,
    } = task.input
    else {
        return run_no_body_http_request(task.ctx, task.admission, &mut transport).await;
    };
    run_http_request(
        task.ctx,
        HttpRequestBody::Stream {
            rx: request_body_rx,
            disconnect,
        },
        task.admission,
        &mut transport,
        move || body_bytes_read.as_ref().map_or(0, RequestBodyCounter::load),
    )
    .await
}

async fn run_no_body_http_request(
    ctx: Box<RequestContext>,
    admission: RequestAdmission,
    transport: &mut H2HttpTransport<'_>,
) -> Result<(), H2CornError> {
    let access_log = HttpAccessLogState::new(&ctx);
    let RunningHttpRequest { state, app_task } =
        start_asgi_http_request(ctx, HttpRequestBody::NoBody, admission);
    tokio::pin!(app_task);
    let result = match poll_app_task_once(app_task.as_mut()) {
        Poll::Ready(app_result) => try_complete_http_request(state, transport, app_result).await,
        Poll::Pending => drive_pinned_http_request(state, app_task.as_mut(), transport).await,
    };
    access_log.emit_http_response(transport.response_log_state(), || 0);
    result
}

fn push_final_response_commands(
    stream_id: StreamId,
    commands: &mut WriterCommandBatch,
    mut start: ResponseStart,
    body: FinalResponseBody,
    defaults: &ResponseHeaderConfig,
) {
    start.prepare_known_length(defaults, body.len());
    let (status, headers) = start.into_status_headers();
    match body {
        FinalResponseBody::Empty | FinalResponseBody::Suppressed { .. } => {
            commands.push_back(WriterCommand::SendHeaders {
                stream_id,
                status,
                headers,
                end_stream: true,
            });
        },
        FinalResponseBody::Bytes(body) => commands.push_back(WriterCommand::SendFinal {
            stream_id,
            status,
            headers,
            data: body,
        }),
        FinalResponseBody::File { file, len } => {
            commands.push_back(WriterCommand::SendHeaders {
                stream_id,
                status,
                headers,
                end_stream: false,
            });
            commands.push_back(WriterCommand::SendPath {
                stream_id,
                file,
                len,
                end_stream: true,
            });
        },
    }
}

fn append_response_action(
    stream_id: StreamId,
    commands: &mut WriterCommandBatch,
    action: ResponseAction,
    config: &ServerConfig,
) {
    match action {
        ResponseAction::Final { start, body } => {
            push_final_response_commands(
                stream_id,
                commands,
                start,
                body,
                &config.response_headers,
            );
        },
        ResponseAction::Start { mut start } => {
            start.apply_default_headers(config);
            let (status, headers) = start.into_status_headers();
            commands.push_back(WriterCommand::SendHeaders {
                stream_id,
                status,
                headers,
                end_stream: false,
            });
        },
        ResponseAction::Body(body) => commands.push_back(WriterCommand::SendData {
            stream_id,
            data: body,
            end_stream: false,
        }),
        ResponseAction::File { file, len } => commands.push_back(WriterCommand::SendPath {
            stream_id,
            file,
            len,
            end_stream: false,
        }),
        ResponseAction::Finish => commands.push_back(WriterCommand::SendData {
            stream_id,
            data: Bytes::new().into(),
            end_stream: true,
        }),
        ResponseAction::FinishWithTrailers(headers) => {
            commands.push_back(WriterCommand::SendTrailers { stream_id, headers });
        },
        ResponseAction::InternalError => commands.push_back(WriterCommand::SendHeaders {
            stream_id,
            status: status_code::INTERNAL_SERVER_ERROR,
            headers: ResponseHeaders::new(),
            end_stream: true,
        }),
        ResponseAction::AbortIncomplete => commands.push_back(WriterCommand::SendReset {
            stream_id,
            error_code: ErrorCode::INTERNAL_ERROR,
        }),
    }
}

fn log_response_action(response_log: &mut ResponseLogState, action: &ResponseAction) {
    match action {
        ResponseAction::Final { start, body } => {
            response_log.started(start.status());
            if !matches!(
                body,
                FinalResponseBody::Empty | FinalResponseBody::Suppressed { .. }
            ) {
                response_log.sent_body(body.len());
            }
        },
        ResponseAction::Start { start } => response_log.started(start.status()),
        ResponseAction::Body(body) => response_log.sent_body(body.len()),
        ResponseAction::File { len, .. } => response_log.sent_body(*len),
        ResponseAction::InternalError => response_log.internal_error(),
        ResponseAction::Finish
        | ResponseAction::FinishWithTrailers(_)
        | ResponseAction::AbortIncomplete => {},
    }
}

pub(super) async fn send_final_response(
    connection: &H2WriterHandle,
    stream_id: StreamId,
    status: HttpStatusCode,
    headers: ResponseHeaders,
    body: FinalResponseBody,
) -> Result<(), H2CornError> {
    let start = ResponseStart::new(status, headers);
    let mut commands = WriterCommandBatch::new();
    push_final_response_commands(
        stream_id,
        &mut commands,
        start,
        body,
        &connection.config().response_headers,
    );
    connection.send_commands(stream_id, commands).await
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::fs;
    use std::fs::File;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        FinalResponseBody, WriterCommand, WriterCommandBatch, push_final_response_commands,
    };
    use crate::config::ResponseHeaderConfig;
    use crate::h2_frame::StreamId;
    use crate::http::header::inspect_response_headers;
    use crate::http::response::ResponseStart;
    use crate::http::types::{ResponseHeaders, status_code};

    #[test]
    fn final_file_response_batches_headers_then_pathsend() {
        let path = temp_dir().join(format!(
            "h2corn-final-response-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos(),
        ));
        fs::write(&path, b"x").unwrap();
        let file = File::open(&path).unwrap();

        let mut commands = WriterCommandBatch::new();
        push_final_response_commands(
            StreamId::new(1).unwrap(),
            &mut commands,
            ResponseStart::new(status_code::OK, ResponseHeaders::new()),
            FinalResponseBody::File {
                file: Box::new(file),
                len: 1,
            },
            &ResponseHeaderConfig::default(),
        );

        match commands
            .pop_front()
            .expect("file responses start with headers")
        {
            WriterCommand::SendHeaders {
                status,
                headers,
                end_stream,
                ..
            } => {
                assert_eq!(status, status_code::OK);
                assert!(!end_stream);
                assert_eq!(inspect_response_headers(&headers).content_length(), Some(1));
            },
            other => panic!("expected headers before pathsend, got {other:?}"),
        }
        assert!(matches!(
            commands.pop_front(),
            Some(WriterCommand::SendPath { .. })
        ));
        assert!(commands.is_empty());

        let _ = fs::remove_file(path);
    }

    #[test]
    fn empty_final_response_keeps_single_end_stream_headers_command() {
        let mut commands = WriterCommandBatch::new();
        push_final_response_commands(
            StreamId::new(1).unwrap(),
            &mut commands,
            ResponseStart::new(status_code::OK, ResponseHeaders::new()),
            FinalResponseBody::Empty,
            &ResponseHeaderConfig::default(),
        );

        match commands
            .pop_front()
            .expect("empty responses send one headers frame")
        {
            WriterCommand::SendHeaders {
                headers,
                end_stream,
                ..
            } => {
                assert!(end_stream);
                assert_eq!(inspect_response_headers(&headers).content_length(), Some(0));
            },
            other => panic!("expected one terminal headers command, got {other:?}"),
        }
        assert!(commands.is_empty());
    }
}
