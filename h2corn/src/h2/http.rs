use std::task::Poll;

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::bridge::{HttpOutboundEvent, RequestBodyCounter};
use crate::config::{ResponseHeaderConfig, ServerConfig};
use crate::error::{H2CornError, H2Error};
use crate::h2::state::RequestTaskCancellation;
use crate::h2::websocket::handle_request as handle_websocket_request;
use crate::h2::writer::{WriterCommand, WriterCommandBatch};
use crate::h2::{H2WriterHandle, InboundStream, RequestSpawnContext};
use crate::h2_frame::{ErrorCode, StreamId};
use crate::http::app::{
    AdmittedHttpOutboundEvent, HttpRequestBody, HttpSendBuffer, HttpSendState, RunningHttpRequest,
    drive_pinned_http_request, outbound_charge, poll_app_task_once, start_asgi_http_request,
    try_complete_http_request,
};
use crate::http::execution::{AppRequestInput, RequestExecution, prepare_request_execution};
use crate::http::planner::{RejectedResponse, plan_request};
use crate::http::policy::ResolvedRequestPolicy;
use crate::http::response::{
    FinalResponseBody, HttpResponseTransport, ResponseAction, ResponseActions, ResponseStart,
};
use crate::http::run_request::run_http_request;
use crate::http::types::{RequestHead, ResponseHeaders, status_code};
use crate::log::{HttpAccessLogState, ResponseLogState};
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
    fn log_format(&self) -> crate::config::LogFormat {
        self.connection.config().log_format
    }

    fn new_http_send_state(&self) -> (HttpSendState, HttpSendBuffer) {
        HttpSendState::with_response_failure(
            self.connection.response_byte_budget(),
            self.connection.failure_notification(self.stream_id),
        )
    }

    async fn admit_outbound_event(
        &self,
        event: HttpOutboundEvent,
    ) -> Result<AdmittedHttpOutboundEvent, H2CornError> {
        // Same policy as the buffered send path, so the two cannot disagree
        // about what an event costs.
        let Some(charge) = outbound_charge(&event) else {
            return Ok(AdmittedHttpOutboundEvent::unbounded(event));
        };
        let credit = self
            .connection
            .response_byte_budget()
            .acquire(charge)
            .await
            .map_err(|_| H2CornError::from(H2Error::StreamChannelClosed))?;
        Ok(AdmittedHttpOutboundEvent::with_credit(event, credit))
    }

    async fn apply_response_action(&mut self, action: ResponseAction) -> Result<(), H2CornError> {
        if matches!(action, ResponseAction::AbortIncomplete) {
            self.connection.abort_response(self.stream_id);
            return Ok(());
        }
        let mut commands = WriterCommandBatch::new();
        append_response_action(
            self.stream_id,
            &mut commands,
            action,
            self.connection.config(),
        )?;
        self.connection
            .send_commands(self.stream_id, &mut commands)
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
        if actions
            .iter()
            .any(|action| matches!(action, ResponseAction::AbortIncomplete))
        {
            actions.clear();
            self.connection.abort_response(self.stream_id);
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
            .send_commands(self.stream_id, &mut commands)
            .await
    }
}

/// Perform the common request planning, registration, and task launch without
/// suspension. Return a pre-admission rejection to the connection owner for
/// ordered output.
#[expect(
    clippy::significant_drop_tightening,
    reason = "the match transfers each prepared admission and input owner into its request task"
)]
pub(super) fn spawn_request_stream(
    stream_id: StreamId,
    request: RequestHead,
    end_stream: bool,
    policy: ResolvedRequestPolicy,
    connection: &H2WriterHandle,
    mut context: RequestSpawnContext<'_>,
) -> Option<RejectedResponse> {
    let config = &context.connection.config;
    let content_length = request.content_length();
    let websocket = request
        .protocol_is_websocket()
        .then(|| validate_websocket_request(&request));
    let prepared = match plan_request(
        &request,
        websocket,
        end_stream,
        config.access_log,
        policy.max_request_body_size,
    )
    .and_then(|plan| {
        prepare_request_execution(&context.connection.app, plan).ok_or_else(|| RejectedResponse {
            status: status_code::SERVICE_UNAVAILABLE,
            headers: ResponseHeaders::new(),
        })
    }) {
        Ok(prepared) => prepared,
        Err(rejection) => {
            register_inbound_stream(
                &mut context,
                stream_id,
                InboundStream::new(
                    None,
                    true,
                    end_stream,
                    content_length,
                    None,
                    ResolvedRequestPolicy {
                        max_request_body_size: None,
                        ..policy
                    },
                    config.http2.initial_stream_window_size.get(),
                ),
            );
            return Some(rejection);
        },
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
                    policy,
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
            let (input_tx, input_rx, _, receive_signal) = input.into_parts();
            register_inbound_stream(
                &mut context,
                stream_id,
                InboundStream::new(
                    Some(input_tx),
                    false,
                    end_stream,
                    content_length,
                    None,
                    policy,
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
                    receive_signal: Some(receive_signal),
                },
                stream_id,
                input_rx,
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
    let log_format = task.ctx.connection.config.log_format;
    tasks.spawn(stream_id, cancellation, async move {
        crate::server::report_request_failure(
            log_format,
            Box::pin(handle_http_request(stream_id, task)).await,
        );
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
    let cancellation = context
        .receive_signal
        .as_ref()
        .map_or(RequestTaskCancellation::Immediate, |signal| {
            RequestTaskCancellation::AfterPendingReceive(std::sync::Arc::clone(signal))
        });
    let log_format = context.request.connection.config.log_format;
    tasks.spawn(stream_id, cancellation, async move {
        crate::server::report_request_failure(
            log_format,
            Box::pin(handle_websocket_request(
                context, stream_id, input, connection,
            ))
            .await,
        );
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
    let mut transport = H2HttpTransport::new(&task.connection, stream_id);
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
        FinalResponseBody::File(segment) => {
            commands.push_back(WriterCommand::SendHeaders {
                stream_id,
                status,
                headers,
                end_stream: false,
            });
            commands.push_back(WriterCommand::SendFile {
                stream_id,
                segment,
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
        ResponseAction::EarlyHint(links) => {
            commands.push_back(WriterCommand::SendEarlyHint { stream_id, links });
        },
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
        ResponseAction::Start { mut start, .. } => {
            if start.status() == status_code::UPGRADE_REQUIRED {
                return Err(H2CornError::from(pyo3::exceptions::PyValueError::new_err(
                    "HTTP/2 responses cannot advertise an Upgrade",
                )));
            }
            // HTTP/2 frames trailers in their own HEADERS frame, so a declared
            // content-length stays: it describes the body either way.
            let _declared_length = start.prepare_streaming(&config.response_headers, false);
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
        ResponseAction::File(segment) => commands.push_back(WriterCommand::SendFile {
            stream_id,
            segment,
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
        ResponseAction::AbortIncomplete => commands.push_back(WriterCommand::ResetStream {
            stream_id,
            error_code: ErrorCode::INTERNAL_ERROR,
        }),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;
    use std::sync::Arc;

    use super::{
        FinalResponseBody, H2HttpTransport, WriterCommand, WriterCommandBatch,
        push_final_response_commands,
    };
    use crate::bridge::HttpOutboundEvent;
    use crate::config::ResponseHeaderConfig;
    use crate::h2::writer::{GatedWriter, ResponseClose, init_writer};
    use crate::h2_frame::StreamId;
    use crate::http::app::HttpSendDisposition;
    use crate::http::header::ResponseConnectionDirective;
    use crate::http::response::{
        FileSegment, HttpResponseTransport as _, ResponseAction, ResponseStart,
    };
    use crate::http::types::{FinalResponseStatus, ResponseHeaders, status_code};
    use crate::runtime::test_fixtures;

    #[expect(
        clippy::significant_drop_tightening,
        reason = "the transport only borrows the connection; its sender pair escapes the transport scope"
    )]
    #[tokio::test]
    async fn producer_failure_reaches_queued_output_after_send_buffer_teardown() {
        let target = GatedWriter::new(usize::MAX, true);
        let wire = Arc::clone(&target.0);
        let (mut writer, connection) =
            init_writer(target, Arc::new(test_fixtures::server_config_parts()), None)
                .await
                .unwrap();
        wire.lock().bytes.clear();
        let id = StreamId::new(1).unwrap();
        writer.register_stream(id, None);
        let (send, buffer) = {
            let mut transport = H2HttpTransport::new(&connection, id);
            let (send, buffer) = transport.new_http_send_state();
            for event in [
                HttpOutboundEvent::Start {
                    status: FinalResponseStatus::new(status_code::OK).unwrap(),
                    headers: ResponseHeaders::new(),
                    trailers: false,
                    directive: ResponseConnectionDirective::default(),
                },
                HttpOutboundEvent::Body {
                    body: bytes::Bytes::new().into(),
                    more_body: false,
                },
            ] {
                assert!(matches!(
                    send.push_or_forward(event),
                    HttpSendDisposition::Buffered | HttpSendDisposition::Sent
                ));
            }
            transport
                .apply_response_action(ResponseAction::Final {
                    start: ResponseStart::new(status_code::OK, ResponseHeaders::new()),
                    body: FinalResponseBody::Empty,
                })
                .await
                .unwrap();
            (send, buffer)
        };
        drop(connection);
        drop(buffer);
        send.failure_notification()
            .expect("HTTP/2 publishes producer failure independently of command admission")
            .application_failed();
        drop(send);
        writer.drain_app_writes().unwrap();
        writer.flush().await.unwrap();
        assert_eq!(writer.take_response_closes().as_slice(), &[(
            ResponseClose::ProducerFailed,
            id
        )]);
        drop(writer);
        assert_eq!(wire.lock().bytes.as_slice(), &[
            0, 0, 4, 3, 0, 0, 0, 0, 1, 0, 0, 0, 2
        ]);
    }

    #[test]
    fn final_file_response_batches_headers_then_pathsend() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"x").unwrap();

        let mut commands = WriterCommandBatch::new();
        push_final_response_commands(
            StreamId::new(1).unwrap(),
            &mut commands,
            ResponseStart::new(status_code::OK, ResponseHeaders::new()),
            FinalResponseBody::File(FileSegment::new(file.reopen().unwrap(), 0, 1, None)),
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
            other @ (WriterCommand::SendFinal { .. }
            | WriterCommand::SendTrailers { .. }
            | WriterCommand::SendEarlyHint { .. }
            | WriterCommand::SendData { .. }
            | WriterCommand::SendWebSocketData { .. }
            | WriterCommand::SendFile { .. }
            | WriterCommand::FlushStream(_)
            | WriterCommand::SendReset { .. }
            | WriterCommand::ResetStream { .. }
            | WriterCommand::SendSettingsAck
            | WriterCommand::StopRequest(_)
            | WriterCommand::SendWindowUpdate { .. }
            | WriterCommand::PingAck(_)
            | WriterCommand::Goaway { .. }) => {
                panic!("expected headers before pathsend, got {other:?}")
            },
        }
        assert!(matches!(
            commands.pop_front(),
            Some(WriterCommand::SendFile { .. })
        ));
        assert!(commands.is_empty());
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
            other @ (WriterCommand::SendFinal { .. }
            | WriterCommand::SendTrailers { .. }
            | WriterCommand::SendEarlyHint { .. }
            | WriterCommand::SendData { .. }
            | WriterCommand::SendWebSocketData { .. }
            | WriterCommand::SendFile { .. }
            | WriterCommand::FlushStream(_)
            | WriterCommand::SendReset { .. }
            | WriterCommand::ResetStream { .. }
            | WriterCommand::SendSettingsAck
            | WriterCommand::StopRequest(_)
            | WriterCommand::SendWindowUpdate { .. }
            | WriterCommand::PingAck(_)
            | WriterCommand::Goaway { .. }) => {
                panic!("expected one terminal headers command, got {other:?}")
            },
        }
        assert!(commands.is_empty());
    }
}
