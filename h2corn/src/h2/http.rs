use std::task::Poll;

use bytes::Bytes;
use tokio::sync::mpsc;

use super::state::RequestTaskCancellation;
use super::websocket::handle_request as handle_websocket_request;
use super::writer::{WriterCommand, WriterCommandBatch};
use super::{H2WriterHandle, InboundStream, RequestSpawnContext};
use crate::access_log::{HttpAccessLogState, ResponseLogState};
use crate::bridge::{HttpOutboundEvent, RequestBodyCounter};
use crate::config::{ResponseHeaderConfig, ServerConfig};
use crate::error::{H2CornError, H2Error};
use crate::h2_frame::{ErrorCode, StreamId};
use crate::http::app::{
    AdmittedHttpOutboundEvent,
    HttpRequestBody, HttpSendBuffer, HttpSendState, RunningHttpRequest, drive_pinned_http_request,
    poll_app_task_once, start_asgi_http_request, try_complete_http_request,
};
use crate::http::execution::{AppRequestInput, RequestExecution, prepare_request_execution};
use crate::http::planner::{RejectedResponse, plan_request};
use crate::http::response::{
    FinalResponseBody, HttpResponseTransport, ResponseAction, ResponseActions, ResponseStart,
};
use crate::http::run_request::run_http_request;
use crate::http::types::{RequestHead, ResponseHeaders, status_code};
use crate::runtime::{RequestAdmission, RequestContext, StreamInput};
use crate::websocket::{WebSocketContext, validate_websocket_request};

pub(super) struct H2HttpTransport<'a> {
    connection: &'a H2WriterHandle,
    stream_id: StreamId,
}

impl<'a> H2HttpTransport<'a> {
    pub(super) const fn new(connection: &'a H2WriterHandle, stream_id: StreamId) -> Self {
        Self {
            connection,
            stream_id,
        }
    }
}

struct H2HttpRequestContext {
    ctx: Box<RequestContext>,
    connection: H2WriterHandle,
    admission: RequestAdmission,
    input: AppRequestInput,
}

impl HttpResponseTransport for H2HttpTransport<'_> {
    fn new_http_send_state(&self) -> (HttpSendState, HttpSendBuffer) {
        HttpSendState::with_response_budget(self.connection.response_byte_budget())
    }

    async fn admit_outbound_event(
        &self,
        event: HttpOutboundEvent,
    ) -> Result<AdmittedHttpOutboundEvent, H2CornError> {
        let HttpOutboundEvent::Body { body, .. } = &event else {
            return Ok(AdmittedHttpOutboundEvent::unbounded(event));
        };
        let credit = self
            .connection
            .response_byte_budget()
            .acquire(body.len())
            .await
            .map_err(|_| H2CornError::from(H2Error::StreamChannelClosed))?;
        Ok(AdmittedHttpOutboundEvent::with_credit(event, credit))
    }

    async fn apply_response_action(&mut self, action: ResponseAction) -> Result<(), H2CornError> {
        let mut commands = WriterCommandBatch::new();
        append_response_action(
            self.stream_id,
            &mut commands,
            action,
            self.connection.config(),
        )?;
        self.connection
            .send_commands(self.stream_id, commands)
            .await
    }

    async fn flush_buffered(&mut self) -> Result<(), H2CornError> {
        // Nothing to do: every action is handed to the connection's writer
        // task, which flushes once per batch it drains. There is no
        // per-response buffer held here.
        Ok(())
    }

    async fn apply_response_actions(
        &mut self,
        actions: &mut ResponseActions,
    ) -> Result<(), H2CornError> {
        if actions.is_empty() {
            return Ok(());
        }

        // One batch, one handoff to the writer task: the per-action default
        // would send each command on its own.
        let mut commands = WriterCommandBatch::new();
        for action in actions.drain(..) {
            append_response_action(
                self.stream_id,
                &mut commands,
                action,
                self.connection.config(),
            )?;
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
    if let Some(RejectedResponse { status, headers }) =
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
) -> Option<RejectedResponse> {
    let config = &context.connection.config;
    let content_length = request.content_length();
    let websocket = request
        .protocol_is_websocket()
        .then(|| validate_websocket_request(&request));
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
        return Some(RejectedResponse {
            status: status_code::SERVICE_UNAVAILABLE,
            headers: ResponseHeaders::new(),
        });
    };
    let request = RequestContext::new(std::sync::Arc::clone(context.connection), request);
    match prepared {
        RequestExecution::Http {
            mut admission,
            input,
        } => {
            context.tasks.track_admission(stream_id, &mut admission);
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
                    config.max_request_body_size,
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
            mut admission,
            input,
        } => {
            context.tasks.track_admission(stream_id, &mut admission);
            register_inbound_stream(
                &mut context,
                stream_id,
                InboundStream::new(
                    Some(input.tx.clone()),
                    false,
                    end_stream,
                    content_length,
                    None,
                    config.max_request_body_size,
                    config.http2.initial_stream_window_size.get(),
                ),
            );
            spawn_websocket_request(
                context.tasks,
                WebSocketContext {
                    request,
                    admission,
                    meta,
                    shutdown: context.shutdown.clone(),
                },
                stream_id,
                input.rx,
                connection.clone(),
            );
        },
    }
    None
}

/// Keep construction of the sizeable HTTP task future behind a pointer at the
/// task-spawn boundary instead of embedding its state in the H2 connection's
/// poll frame. This deliberately pays one request-scoped allocation to bound
/// the long-lived connection task's stack footprint.
fn spawn_http_request(
    tasks: &mut super::state::RequestTasks,
    stream_id: StreamId,
    cancellation: RequestTaskCancellation,
    task: H2HttpRequestContext,
) {
    tasks.spawn(stream_id, cancellation, async move {
        crate::server::report_request_failure(Box::pin(handle_http_request(stream_id, task)).await);
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
    // then contains only the pointer.
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
    let mut response_log = ResponseLogState::default();
    let RunningHttpRequest { state, app_task } = start_asgi_http_request(
        ctx,
        HttpRequestBody::NoBody,
        admission,
        transport.new_http_send_state(),
    );
    tokio::pin!(app_task);
    let result = match poll_app_task_once(app_task.as_mut()) {
        Poll::Ready(app_result) => {
            try_complete_http_request(state, transport, &mut response_log, app_result).await
        },
        Poll::Pending => {
            drive_pinned_http_request(state, app_task.as_mut(), transport, &mut response_log).await
        },
    };
    access_log.emit_http_response(response_log, || 0);
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
        FinalResponseBody::Bytes(body) => {
            let (data, credit) = body.into_parts();
            commands.push_back(WriterCommand::SendFinal {
                stream_id,
                status,
                headers,
                data,
                credit,
            });
        },
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
) -> Result<(), H2CornError> {
    match action {
        ResponseAction::Final { mut start, body } => {
            if start.status() == status_code::UPGRADE_REQUIRED {
                return Err(H2CornError::from(pyo3::exceptions::PyValueError::new_err(
                    "HTTP/2 responses cannot advertise an Upgrade",
                )));
            }
            start.strip_http2_only_fields();
            push_final_response_commands(
                stream_id,
                commands,
                start,
                body,
                &config.response_headers,
            );
        },
        ResponseAction::Start { mut start } => {
            if start.status() == status_code::UPGRADE_REQUIRED {
                return Err(H2CornError::from(pyo3::exceptions::PyValueError::new_err(
                    "HTTP/2 responses cannot advertise an Upgrade",
                )));
            }
            let _declared_length = start.prepare_streaming(&config.response_headers);
            start.strip_http2_only_fields();
            let (status, headers) = start.into_status_headers();
            commands.push_back(WriterCommand::SendHeaders {
                stream_id,
                status,
                headers,
                end_stream: false,
            });
        },
        ResponseAction::Body(body) => {
            let (data, credit) = body.into_parts();
            commands.push_back(WriterCommand::SendData {
                stream_id,
                data,
                credit,
                end_stream: false,
            });
        },
        ResponseAction::File { file, len } => commands.push_back(WriterCommand::SendPath {
            stream_id,
            file,
            len,
            end_stream: false,
        }),
        ResponseAction::Finish => commands.push_back(WriterCommand::SendData {
            stream_id,
            data: Bytes::new().into(),
            credit: None,
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
    Ok(())
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
                // The header the peer actually receives, rather than a
                // scanner accessor that exists only to be asserted on.
                assert_eq!(
                    headers
                        .iter()
                        .find(|(name, _)| name.as_ref() == b"content-length")
                        .map(|(_, value)| value.as_ref()),
                    Some(b"1".as_slice())
                );
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
                // The header the peer actually receives, rather than a
                // scanner accessor that exists only to be asserted on.
                assert_eq!(
                    headers
                        .iter()
                        .find(|(name, _)| name.as_ref() == b"content-length")
                        .map(|(_, value)| value.as_ref()),
                    Some(b"0".as_slice())
                );
            },
            other => panic!("expected one terminal headers command, got {other:?}"),
        }
        assert!(commands.is_empty());
    }
}
