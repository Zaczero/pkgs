use std::{
    num::NonZeroU64,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
};

use bytes::Bytes;
use tokio::fs::File;
use tokio::sync::mpsc;
use tokio::task::spawn;

use crate::async_util::send_best_effort;
use crate::bridge::PayloadBytes;
use crate::config::ServerConfig;
use crate::console::{HttpAccessLogState, ResponseLogState, run_http_request};
use crate::error::H2CornError;
use crate::frame::{ErrorCode, StreamId};
use crate::http::app::{
    HttpRequestBody, RunningHttpRequest, drive_http_request, start_asgi_http_request,
    try_complete_http_request,
};
use crate::http::execution::{RequestExecution, RequestInputChannels, prepare_request_execution};
use crate::http::pathsend::PathStreamer;
use crate::http::planner::{RequestRejection, RequestRoute, plan_request};
use crate::http::response::{
    FinalResponseBody, HttpResponseTransport, ResponseAction, ResponseActions, ResponseStart,
};
use crate::http::types::{HttpStatusCode, RequestHead, ResponseHeaders, status_code};
use crate::runtime::{RequestAdmission, RequestContext, StreamInput};
use crate::websocket::{WebSocketContext, WebSocketRequestMeta, validate_websocket_request};

use super::websocket::handle_request as handle_websocket_request;
use super::writer::{WriterCommand, WriterCommandBatch};
use super::{ConnectionHandle, InboundStream, RequestSpawnContext};

struct HttpRequestTask {
    ctx: RequestContext,
    connection: ConnectionHandle,
    admission: RequestAdmission,
    body_bytes_read: Option<Arc<AtomicU64>>,
    request_body_rx: Option<mpsc::Receiver<StreamInput>>,
}

pub(super) async fn spawn_request_stream(
    stream_id: StreamId,
    request: RequestHead,
    end_stream: bool,
    connection: &ConnectionHandle,
    mut context: RequestSpawnContext<'_>,
) -> Result<(), H2CornError> {
    let config = context.connection.config;
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
        Err(RequestRejection { status, headers }) => {
            connection
                .send_headers(stream_id, status, headers, true)
                .await?;
            return Ok(());
        }
    };
    let Some(prepared) = prepare_request_execution(&context.connection.app, plan) else {
        connection
            .send_headers(
                stream_id,
                status_code::SERVICE_UNAVAILABLE,
                ResponseHeaders::new(),
                true,
            )
            .await?;
        return Ok(());
    };
    let RequestExecution {
        route,
        admission,
        input,
    } = prepared;
    register_inbound_stream(
        &mut context,
        stream_id,
        end_stream,
        content_length,
        &route,
        &input,
        config.max_request_body_size.map(NonZeroU64::get),
    );
    launch_request_stream(
        &context,
        stream_id,
        route,
        request,
        admission,
        input,
        connection.clone(),
    );
    let startup_tx = context.streams.get(&stream_id.get()).and_then(|stream| {
        stream
            .state
            .request_is_closed()
            .then(|| stream.input.clone())
            .flatten()
    });
    if let Some(tx) = startup_tx {
        send_best_effort(&tx, StreamInput::EndStream).await;
    }

    Ok(())
}

fn register_inbound_stream(
    context: &mut RequestSpawnContext<'_>,
    stream_id: StreamId,
    end_stream: bool,
    content_length: Option<u64>,
    route: &RequestRoute<WebSocketRequestMeta>,
    input: &RequestInputChannels,
    max_request_body_size: Option<u64>,
) {
    context.streams.insert(
        stream_id.get(),
        InboundStream::new(
            input.tx.clone(),
            *route == RequestRoute::Http,
            end_stream,
            content_length,
            input.body_bytes_read.clone(),
            max_request_body_size,
        ),
    );
}

fn launch_request_stream(
    context: &RequestSpawnContext<'_>,
    stream_id: StreamId,
    route: RequestRoute<WebSocketRequestMeta>,
    request: RequestHead,
    admission: RequestAdmission,
    input: RequestInputChannels,
    connection: ConnectionHandle,
) {
    let request = RequestContext::new(context.connection.clone(), request);
    match route {
        RequestRoute::WebSocket(meta) => spawn(handle_websocket_request(
            WebSocketContext {
                request,
                admission,
                meta,
            },
            stream_id,
            input
                .rx
                .expect("websocket requests always require a stream input channel"),
            connection,
        )),
        RequestRoute::Http => spawn(handle_http_request(
            stream_id,
            HttpRequestTask {
                ctx: request,
                connection,
                admission,
                body_bytes_read: input.body_bytes_read,
                request_body_rx: input.rx,
            },
        )),
    };
}

async fn handle_http_request(
    stream_id: StreamId,
    task: HttpRequestTask,
) -> Result<(), H2CornError> {
    let mut transport = H2HttpTransport {
        connection: &task.connection,
        stream_id,
        response_log: ResponseLogState::default(),
    };
    if task.request_body_rx.is_none() {
        return run_no_body_http_request(task.ctx, task.admission, &mut transport).await;
    }
    run_http_request(
        task.ctx,
        task.request_body_rx
            .map_or(HttpRequestBody::NoBody, HttpRequestBody::Stream),
        task.admission,
        &mut transport,
        move || {
            task.body_bytes_read
                .as_ref()
                .map_or(0, |body_bytes| body_bytes.load(Ordering::Relaxed))
        },
    )
    .await
}

fn poll_app_task_once<F>(mut task: Pin<&mut F>) -> Poll<Result<(), H2CornError>>
where
    F: Future<Output = Result<(), H2CornError>>,
{
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    task.as_mut().poll(&mut cx)
}

async fn run_no_body_http_request(
    ctx: RequestContext,
    admission: RequestAdmission,
    transport: &mut H2HttpTransport<'_>,
) -> Result<(), H2CornError> {
    let access_log = HttpAccessLogState::new(&ctx);
    let started = match start_asgi_http_request(ctx, HttpRequestBody::NoBody, admission) {
        Ok(started) => started,
        Err(err) => {
            transport.send_internal_error_response().await?;
            access_log.emit_http_response(transport.response_log_state(), || 0);
            return Err(err);
        }
    };

    let RunningHttpRequest { state, app_task } = started;
    let mut app_task = Box::pin(app_task);
    let result = match poll_app_task_once(app_task.as_mut()) {
        Poll::Ready(app_result) => try_complete_http_request(state, transport, app_result).await,
        Poll::Pending => {
            drive_http_request(RunningHttpRequest { state, app_task }, transport).await
        }
    };
    access_log.emit_http_response(transport.response_log_state(), || 0);
    result
}

struct H2HttpTransport<'a> {
    connection: &'a ConnectionHandle,
    stream_id: StreamId,
    response_log: ResponseLogState,
}

fn final_response_commands(
    stream_id: StreamId,
    mut start: ResponseStart,
    body: FinalResponseBody,
) -> WriterCommandBatch {
    start.canonicalize_known_length(body.len());
    let (status, headers) = start.into_status_headers();
    let mut commands = WriterCommandBatch::new();
    match body {
        FinalResponseBody::Empty | FinalResponseBody::Suppressed { .. } => {
            commands.push_back(WriterCommand::SendHeaders {
                stream_id,
                status,
                headers,
                end_stream: true,
            })
        }
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
                streamer: PathStreamer::new(file, len, true),
            });
        }
    }
    commands
}

fn append_response_action(
    stream_id: StreamId,
    commands: &mut WriterCommandBatch,
    action: ResponseAction,
    config: &'static ServerConfig,
) {
    match action {
        ResponseAction::Final { start, body } => {
            let mut start = start;
            start.apply_default_headers(config);
            let mut batch = final_response_commands(stream_id, start, body);
            while let Some(command) = batch.pop_front() {
                commands.push_back(command);
            }
        }
        ResponseAction::Start { mut start } => {
            start.apply_default_headers(config);
            let (status, headers) = start.into_status_headers();
            commands.push_back(WriterCommand::SendHeaders {
                stream_id,
                status,
                headers,
                end_stream: false,
            })
        }
        ResponseAction::Body(body) => commands.push_back(WriterCommand::SendData {
            stream_id,
            data: body,
            end_stream: false,
        }),
        ResponseAction::File { file, len } => commands.push_back(WriterCommand::SendPath {
            stream_id,
            streamer: PathStreamer::new(file, len, false),
        }),
        ResponseAction::Finish => commands.push_back(WriterCommand::SendData {
            stream_id,
            data: Bytes::new().into(),
            end_stream: true,
        }),
        ResponseAction::FinishWithTrailers(headers) => {
            commands.push_back(WriterCommand::SendTrailers { stream_id, headers })
        }
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
        }
        ResponseAction::Start { start } => response_log.started(start.status()),
        ResponseAction::Body(body) => response_log.sent_body(body.len()),
        ResponseAction::File { len, .. } => response_log.sent_body(*len),
        ResponseAction::InternalError => response_log.internal_error(),
        ResponseAction::Finish
        | ResponseAction::FinishWithTrailers(_)
        | ResponseAction::AbortIncomplete => {}
    }
}

pub(super) async fn send_final_response(
    connection: &ConnectionHandle,
    stream_id: StreamId,
    status: HttpStatusCode,
    headers: ResponseHeaders,
    body: FinalResponseBody,
) -> Result<(), H2CornError> {
    let mut start = ResponseStart::new(status, headers);
    start.apply_default_headers(connection.config());
    let commands = final_response_commands(stream_id, start, body);
    connection.send_commands(stream_id, commands).await
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
        self.send_response_action(ResponseAction::File { file, len })
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

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use tokio::fs::File;

    use super::{FinalResponseBody, WriterCommand, final_response_commands};
    use crate::frame::StreamId;
    use crate::http::header::inspect_response_headers;
    use crate::http::response::ResponseStart;
    use crate::http::types::{ResponseHeaders, status_code};

    #[test]
    fn final_file_response_batches_headers_then_pathsend() {
        let path = std::env::temp_dir().join(format!(
            "h2corn-final-response-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos(),
        ));
        fs::write(&path, b"x").unwrap();
        let file = File::from_std(fs::File::open(&path).unwrap());

        let mut commands = final_response_commands(
            StreamId::new(1).unwrap(),
            ResponseStart::new(status_code::OK, ResponseHeaders::new()),
            FinalResponseBody::File { file, len: 1 },
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
            }
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
        let mut commands = final_response_commands(
            StreamId::new(1).unwrap(),
            ResponseStart::new(status_code::OK, ResponseHeaders::new()),
            FinalResponseBody::Empty,
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
            }
            other => panic!("expected one terminal headers command, got {other:?}"),
        }
        assert!(commands.is_empty());
    }
}
