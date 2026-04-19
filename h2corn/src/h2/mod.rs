use std::{
    collections::HashMap,
    future::pending,
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use nohash_hasher::BuildNoHashHasher;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;
use tokio::task::yield_now;
use tokio::time::{Instant as TokioInstant, sleep_until, timeout};

use crate::async_util::{send_best_effort, send_with_backpressure};
use crate::config::{INITIAL_CONNECTION_WINDOW_SIZE, INITIAL_STREAM_WINDOW_SIZE};
use crate::error::{ErrorExt, H2CornError, H2Error};
use crate::frame::{
    self, ErrorCode, FrameFlags, FrameType, PeerSettings, RawFrame, StreamId, WindowIncrement,
};
use crate::http::body::RequestBodyProgress;
use crate::http::types::{RequestHead, ResponseHeaders};
use crate::proxy::read_h2_preface;
use crate::runtime::{ConnectionContext, ShutdownState, StreamInput};

mod http;
mod request;
mod state;
mod websocket;
mod writer;

use http::spawn_request_stream;
use request::{
    PendingHeaders, RequestHeadError, decode_request_head, decode_trailer_block,
    parse_header_block_fragment, resolve_request_head,
};
use state::{
    ConnectionDrainState, H2ConnectionState, InboundStream, ReceiveState, RequestInputClose,
    RequestSpawnContext,
};
pub(crate) use writer::H2WriteTarget;
use writer::{ConnectionHandle, WindowTarget, WriterState, init_writer};

type StreamMap<T> = HashMap<u32, T, BuildNoHashHasher<u32>>;
const CONNECTION_WINDOW_UPDATE_THRESHOLD: u32 = INITIAL_CONNECTION_WINDOW_SIZE / 2;
const STREAM_WINDOW_UPDATE_THRESHOLD: u32 = INITIAL_STREAM_WINDOW_SIZE / 2;

fn new_stream_map<T>(capacity: usize) -> StreamMap<T> {
    HashMap::with_capacity_and_hasher(capacity, BuildNoHashHasher::default())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PriorityDependency {
    Root,
    Stream(StreamId),
}

impl PriorityDependency {
    fn from_wire(raw: u32) -> Self {
        StreamId::new(raw & frame::STREAM_ID_MASK).map_or(Self::Root, Self::Stream)
    }
}

enum H2PeerFailure {
    Goaway {
        error_code: ErrorCode,
        error: H2CornError,
    },
    Reset {
        stream_id: StreamId,
        error_code: ErrorCode,
    },
}

impl H2PeerFailure {
    fn connection(error_code: ErrorCode, error: impl Into<H2CornError>) -> Self {
        Self::Goaway {
            error_code,
            error: error.into(),
        }
    }

    fn protocol(error: impl Into<H2CornError>) -> Self {
        Self::connection(ErrorCode::PROTOCOL_ERROR, error)
    }

    fn frame_size(error: impl Into<H2CornError>) -> Self {
        Self::connection(ErrorCode::FRAME_SIZE_ERROR, error)
    }

    fn stream(stream_id: StreamId, error_code: ErrorCode) -> Self {
        Self::Reset {
            stream_id,
            error_code,
        }
    }
}

async fn apply_peer_failure<W>(
    writer: &mut WriterState<W>,
    last_stream_id: Option<StreamId>,
    failure: H2PeerFailure,
) -> Result<(), H2CornError>
where
    W: H2WriteTarget,
{
    match failure {
        H2PeerFailure::Goaway { error_code, error } => {
            let message = error.to_string().into_bytes();
            writer
                .goaway(last_stream_id, error_code, message, true)
                .await?;
            Err(error)
        }
        H2PeerFailure::Reset {
            stream_id,
            error_code,
        } => {
            writer.reset_stream(stream_id, error_code).await?;
            Ok(())
        }
    }
}

async fn begin_graceful_shutdown<W>(
    writer: &mut WriterState<W>,
    last_stream_id: Option<StreamId>,
    drain_state: &mut ConnectionDrainState,
    timeout_graceful_shutdown: Duration,
) -> Result<(), H2CornError>
where
    W: H2WriteTarget,
{
    if *drain_state != ConnectionDrainState::Accepting {
        return Ok(());
    }
    *drain_state = ConnectionDrainState::Draining {
        deadline: Some(Instant::now() + timeout_graceful_shutdown),
    };
    writer
        .goaway(last_stream_id, ErrorCode::NO_ERROR, Vec::new(), false)
        .await?;
    Ok(())
}

struct RequestStartContext<'a, W> {
    writer: &'a mut WriterState<W>,
    connection: &'a ConnectionHandle,
    spawn: RequestSpawnContext<'a>,
    last_client_stream_id: Option<StreamId>,
}

async fn start_request_stream_from_block<W>(
    ctx: RequestStartContext<'_, W>,
    stream_id: StreamId,
    end_stream: bool,
    decode_result: Result<RequestHead, RequestHeadError>,
) -> Result<(), H2CornError>
where
    W: H2WriteTarget,
{
    let request = match resolve_request_head(end_stream, decode_result) {
        Ok(request) => request,
        Err(RequestHeadError::Reject { status }) => {
            ctx.writer
                .send_headers(stream_id, status, ResponseHeaders::new(), true)
                .await?;
            return Ok(());
        }
        Err(RequestHeadError::Connection { error_code, error }) => {
            apply_peer_failure(
                ctx.writer,
                ctx.last_client_stream_id,
                H2PeerFailure::connection(error_code, error),
            )
            .await?;
            return Ok(());
        }
        Err(RequestHeadError::Stream { error_code }) => {
            apply_peer_failure(
                ctx.writer,
                ctx.last_client_stream_id,
                H2PeerFailure::stream(stream_id, error_code),
            )
            .await?;
            return Ok(());
        }
    };

    spawn_request_stream(stream_id, request, end_stream, ctx.connection, ctx.spawn).await?;
    Ok(())
}

pub(crate) async fn serve_h2_upgraded_connection<R, W>(
    reader: R,
    writer: W,
    connection: ConnectionContext,
    shutdown: watch::Receiver<ShutdownState>,
    upgraded: UpgradedH2Request,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + H2WriteTarget,
{
    let mut reader = frame::FrameReader::with_buffer(reader, upgraded.buffer);
    timeout(
        connection.config.timeout_handshake,
        read_h2_preface(&mut reader),
    )
    .await
    .map_err(|_| H2Error::ConnectionHandshakeTimedOut)??;
    let peer_settings = upgraded.peer_settings;
    let mut connection =
        start_h2_connection(reader, writer, connection, shutdown, Some(peer_settings)).await?;
    seed_upgraded_request(&mut connection, upgraded.request, upgraded.body).await?;
    run_h2_connection(Box::new(connection)).await
}

pub(crate) struct UpgradedH2Request {
    pub buffer: BytesMut,
    pub request: RequestHead,
    pub body: Bytes,
    pub peer_settings: PeerSettings,
}

pub(crate) async fn serve_connection<R, W>(
    reader: frame::FrameReader<R>,
    writer: W,
    context: ConnectionContext,
    shutdown: watch::Receiver<ShutdownState>,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + H2WriteTarget,
{
    let state = start_h2_connection(reader, writer, context, shutdown, None).await?;
    run_h2_connection(Box::new(state)).await
}

async fn start_h2_connection<R, W>(
    reader: frame::FrameReader<R>,
    writer: W,
    context: ConnectionContext,
    shutdown: watch::Receiver<ShutdownState>,
    peer_settings: Option<PeerSettings>,
) -> Result<H2ConnectionState<R, W>, H2CornError>
where
    W: AsyncWrite + Unpin + Send + 'static + H2WriteTarget,
{
    let (writer, connection) = init_writer(writer, context.config, peer_settings).await?;

    let drain_state = if shutdown.borrow().kind().is_some() {
        ConnectionDrainState::Draining {
            deadline: Some(Instant::now() + context.config.timeout_graceful_shutdown),
        }
    } else {
        ConnectionDrainState::Accepting
    };
    Ok(H2ConnectionState::new(
        reader,
        connection,
        writer,
        context,
        shutdown,
        peer_settings.map_or(frame::DEFAULT_MAX_FRAME_SIZE, |settings| {
            settings.max_frame_size()
        }),
        drain_state,
    ))
}

async fn seed_upgraded_request<R, W>(
    connection: &mut H2ConnectionState<R, W>,
    request: RequestHead,
    body: Bytes,
) -> Result<(), H2CornError> {
    let stream_id = StreamId::new(1).expect("value is non-zero");
    spawn_request_stream(
        stream_id,
        request,
        body.is_empty(),
        &connection.connection,
        RequestSpawnContext {
            streams: &mut connection.streams,
            connection: &connection.context,
        },
    )
    .await?;
    if !body.is_empty()
        && let Some(tx) = connection
            .streams
            .get(&stream_id.get())
            .and_then(|stream| stream.input.as_ref())
    {
        send_with_backpressure(tx, StreamInput::Data(body), || H2Error::StreamChannelClosed)
            .await?;
        send_best_effort(tx, StreamInput::EndStream).await;
    }
    connection.last_client_stream_id = Some(stream_id);
    Ok(())
}

async fn handle_headers_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: RawFrame,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: H2WriteTarget,
{
    let stream_id = match frame.header.stream_id {
        Some(stream_id) if stream_id.get() & 1 != 0 => stream_id,
        _ => {
            apply_peer_failure(
                &mut state.writer,
                state.last_client_stream_id,
                H2PeerFailure::protocol(H2Error::InvalidRequestStreamId),
            )
            .await?;
            return Ok(());
        }
    };
    let stream = state.streams.get(&stream_id.get());
    let has_stream = stream.is_some();
    let receive_state =
        stream.map_or_else(|| state.receive_state(stream_id), |stream| stream.state);

    if !has_stream
        && state
            .last_client_stream_id
            .is_some_and(|last| stream_id.get() < last.get())
    {
        apply_peer_failure(
            &mut state.writer,
            state.last_client_stream_id,
            H2PeerFailure::protocol(H2Error::ClientStreamIdsNotIncreasing),
        )
        .await?;
        return Ok(());
    }
    if state.should_refuse_new_streams() && !has_stream {
        state
            .writer
            .reset_stream(stream_id, ErrorCode::REFUSED_STREAM)
            .await?;
        return Ok(());
    }

    let fragment = match parse_header_block_fragment(&frame) {
        Ok(fragment) => fragment,
        Err(err) => {
            apply_peer_failure(
                &mut state.writer,
                state.last_client_stream_id,
                H2PeerFailure::protocol(err),
            )
            .await?;
            return Ok(());
        }
    };
    if matches!(
        fragment.stream_dependency,
        Some(PriorityDependency::Stream(stream_dependency)) if stream_dependency == stream_id
    ) {
        apply_peer_failure(
            &mut state.writer,
            state.last_client_stream_id,
            H2PeerFailure::stream(stream_id, ErrorCode::PROTOCOL_ERROR),
        )
        .await?;
        return Ok(());
    }

    match receive_state {
        ReceiveState::Open | ReceiveState::ResponseClosed => {
            if !fragment.end_headers || !fragment.end_stream {
                state
                    .writer
                    .reset_stream(stream_id, ErrorCode::PROTOCOL_ERROR)
                    .await?;
                return Ok(());
            }
            match decode_trailer_block(&mut state.decoder, fragment.block) {
                Ok(()) => {
                    match state.finish_request_input(stream_id) {
                        Some(RequestInputClose::ContentLengthMismatch) => {
                            state
                                .writer
                                .reset_stream(stream_id, ErrorCode::PROTOCOL_ERROR)
                                .await?;
                            state.remove_stream(stream_id);
                        }
                        Some(RequestInputClose::Closed { tx: Some(tx), .. }) => {
                            send_best_effort(&tx, StreamInput::EndStream).await;
                        }
                        Some(RequestInputClose::Closed { tx: None, .. }) => {}
                        None => {}
                    }
                    return Ok(());
                }
                Err(RequestHeadError::Connection { error_code, error }) => {
                    apply_peer_failure(
                        &mut state.writer,
                        state.last_client_stream_id,
                        H2PeerFailure::connection(error_code, error),
                    )
                    .await?;
                    return Ok(());
                }
                Err(RequestHeadError::Stream { error_code }) => {
                    apply_peer_failure(
                        &mut state.writer,
                        state.last_client_stream_id,
                        H2PeerFailure::stream(stream_id, error_code),
                    )
                    .await?;
                    return Ok(());
                }
                Err(RequestHeadError::Reject { .. }) => {
                    apply_peer_failure(
                        &mut state.writer,
                        state.last_client_stream_id,
                        H2PeerFailure::stream(stream_id, ErrorCode::PROTOCOL_ERROR),
                    )
                    .await?;
                    return Ok(());
                }
            }
        }
        ReceiveState::RequestClosed => {
            apply_peer_failure(
                &mut state.writer,
                state.last_client_stream_id,
                H2PeerFailure::stream(stream_id, ErrorCode::STREAM_CLOSED),
            )
            .await?;
            return Ok(());
        }
        ReceiveState::Closed => {
            apply_peer_failure(
                &mut state.writer,
                state.last_client_stream_id,
                H2PeerFailure::connection(ErrorCode::STREAM_CLOSED, H2Error::HeadersOnClosedStream),
            )
            .await?;
            return Ok(());
        }
        ReceiveState::Idle => {}
    }

    if state.active_stream_count() >= state.context.config.http2.max_concurrent_streams {
        state.last_client_stream_id = Some(stream_id);
        apply_peer_failure(
            &mut state.writer,
            state.last_client_stream_id,
            H2PeerFailure::stream(stream_id, ErrorCode::REFUSED_STREAM),
        )
        .await?;
        return Ok(());
    }
    state.last_client_stream_id = Some(stream_id);

    if fragment.end_headers {
        start_request_stream_from_block(
            RequestStartContext {
                writer: &mut state.writer,
                connection: &state.connection,
                spawn: RequestSpawnContext {
                    streams: &mut state.streams,
                    connection: &state.context,
                },
                last_client_stream_id: state.last_client_stream_id,
            },
            stream_id,
            fragment.end_stream,
            decode_request_head(
                &mut state.decoder,
                fragment.block,
                state.context.config.http2.max_header_list_size,
            ),
        )
        .await?;
    } else {
        state.pending_headers = Some(PendingHeaders {
            stream_id,
            end_stream: fragment.end_stream,
            block: fragment.block.into(),
        });
    }

    Ok(())
}

async fn handle_continuation_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: RawFrame,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: H2WriteTarget,
{
    let Some(mut pending) = state.pending_headers.take() else {
        apply_peer_failure(
            &mut state.writer,
            state.last_client_stream_id,
            H2PeerFailure::protocol(H2Error::UnexpectedContinuationFrame),
        )
        .await?;
        return Ok(());
    };
    if frame.header.stream_id != Some(pending.stream_id) {
        apply_peer_failure(
            &mut state.writer,
            state.last_client_stream_id,
            H2PeerFailure::protocol(H2Error::ContinuationStreamIdMismatch),
        )
        .await?;
        return Ok(());
    }

    pending.block.extend_from_slice(frame.payload.as_ref());
    if frame.header.flags.contains(FrameFlags::END_HEADERS) {
        start_request_stream_from_block(
            RequestStartContext {
                writer: &mut state.writer,
                connection: &state.connection,
                spawn: RequestSpawnContext {
                    streams: &mut state.streams,
                    connection: &state.context,
                },
                last_client_stream_id: state.last_client_stream_id,
            },
            pending.stream_id,
            pending.end_stream,
            decode_request_head(
                &mut state.decoder,
                pending.block.freeze(),
                state.context.config.http2.max_header_list_size,
            ),
        )
        .await?;
    } else {
        state.pending_headers = Some(pending);
    }

    Ok(())
}

async fn handle_data_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: RawFrame,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: H2WriteTarget,
{
    handle_data_frame_with_accounting(state, frame).await
}

async fn handle_data_frame_with_accounting<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: RawFrame,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: H2WriteTarget,
{
    let (data, end_stream) = match parse_data_payload(&frame) {
        Ok(parsed) => parsed,
        Err(err) => {
            apply_peer_failure(
                &mut state.writer,
                state.last_client_stream_id,
                H2PeerFailure::protocol(err),
            )
            .await?;
            return Ok(());
        }
    };
    let Some(stream_id) = frame.header.stream_id else {
        apply_peer_failure(
            &mut state.writer,
            state.last_client_stream_id,
            H2PeerFailure::protocol(H2Error::DataMustNotUseStreamZero),
        )
        .await?;
        return Ok(());
    };

    let Some(stream) = state.streams.get_mut(&stream_id.get()) else {
        return match state.receive_state(stream_id) {
            ReceiveState::Idle => {
                apply_peer_failure(
                    &mut state.writer,
                    state.last_client_stream_id,
                    H2PeerFailure::protocol(H2Error::DataOnIdleStream),
                )
                .await
            }
            ReceiveState::Open
            | ReceiveState::RequestClosed
            | ReceiveState::ResponseClosed
            | ReceiveState::Closed => {
                apply_peer_failure(
                    &mut state.writer,
                    state.last_client_stream_id,
                    H2PeerFailure::stream(stream_id, ErrorCode::STREAM_CLOSED),
                )
                .await?;
                Ok(())
            }
        };
    };
    if stream.state.request_is_closed() {
        apply_peer_failure(
            &mut state.writer,
            state.last_client_stream_id,
            H2PeerFailure::stream(stream_id, ErrorCode::STREAM_CLOSED),
        )
        .await?;
        state.remove_stream(stream_id);
        return Ok(());
    }

    let data_len = data.len() as u32;
    if state.connection_window.receive(data_len).is_err()
        || stream.receive_window.receive(data_len).is_err()
    {
        apply_peer_failure(
            &mut state.writer,
            state.last_client_stream_id,
            H2PeerFailure::connection(
                ErrorCode::FLOW_CONTROL_ERROR,
                H2Error::ReceiveFlowControlWindowUnderflow,
            ),
        )
        .await?;
        return Ok(());
    }

    if data_len != 0 {
        match stream.body.record_chunk(u64::from(data_len)) {
            RequestBodyProgress::Continue => {}
            RequestBodyProgress::SizeLimitExceeded | RequestBodyProgress::ContentLengthExceeded => {
                if let Some(tx) = stream.input.take() {
                    send_best_effort(&tx, StreamInput::Reset(ErrorCode::PROTOCOL_ERROR)).await;
                }
                apply_peer_failure(
                    &mut state.writer,
                    state.last_client_stream_id,
                    H2PeerFailure::stream(stream_id, ErrorCode::PROTOCOL_ERROR),
                )
                .await?;
                state.remove_stream(stream_id);
                return Ok(());
            }
        }
        let stream_window_update = stream
            .receive_window
            .take_update(STREAM_WINDOW_UPDATE_THRESHOLD);
        let forward_to_app = stream.body.should_deliver();
        if forward_to_app {
            let Some(tx) = stream.input.as_ref() else {
                stream.body.stop_delivering();
                return Ok(());
            };
            if send_with_backpressure(tx, StreamInput::Data(data), || H2Error::StreamChannelClosed)
                .await
                .is_err()
            {
                stream.body.stop_delivering();
            }
        }
        if let Some(increment) = state
            .connection_window
            .take_update(CONNECTION_WINDOW_UPDATE_THRESHOLD)
        {
            state
                .writer
                .send_window_update(WindowTarget::Connection, increment)
                .await?;
        }
        if let Some(increment) = stream_window_update {
            state
                .writer
                .send_window_update(WindowTarget::Stream(stream_id), increment)
                .await?;
        }
    }

    if end_stream {
        match state.finish_request_input(stream_id) {
            Some(RequestInputClose::ContentLengthMismatch) => {
                apply_peer_failure(
                    &mut state.writer,
                    state.last_client_stream_id,
                    H2PeerFailure::stream(stream_id, ErrorCode::PROTOCOL_ERROR),
                )
                .await?;
                state.remove_stream(stream_id);
                return Ok(());
            }
            Some(RequestInputClose::Closed { tx: Some(tx), .. }) => {
                send_best_effort(&tx, StreamInput::EndStream).await;
            }
            Some(RequestInputClose::Closed { tx: None, .. }) => {}
            None => {}
        }
    }

    Ok(())
}

async fn run_h2_connection<R, W>(mut state: Box<H2ConnectionState<R, W>>) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + H2WriteTarget,
{
    if state.drain_state != ConnectionDrainState::Accepting {
        state
            .writer
            .goaway(
                state.last_client_stream_id,
                ErrorCode::NO_ERROR,
                Vec::new(),
                false,
            )
            .await?;
    }

    loop {
        let mut stop_after_flush = false;
        match ingest_connection_input(&mut state).await? {
            IngestEvent::Continue => {}
            IngestEvent::PeerClosed
            | IngestEvent::Deadline
            | IngestEvent::KeepAliveTimeout
            | IngestEvent::ReadTimeout => {
                stop_after_flush = true;
            }
            IngestEvent::ShutdownChanged => {
                if state.shutdown.borrow().kind().is_some() {
                    begin_graceful_shutdown(
                        &mut state.writer,
                        state.last_client_stream_id,
                        &mut state.drain_state,
                        state.context.config.timeout_graceful_shutdown,
                    )
                    .await?;
                }
            }
            IngestEvent::Frame(frame) => {
                if advance_connection_with_peer_frame(&mut state, frame).await? {
                    stop_after_flush = true;
                }
            }
        }

        flush_connection_egress(&mut state).await?;
        if stop_after_flush || state.should_stop() {
            break;
        }
    }

    let _ = state
        .writer
        .goaway(
            state.last_client_stream_id,
            ErrorCode::NO_ERROR,
            Vec::new(),
            true,
        )
        .await;
    state.writer.close_ingress().await;
    drop(state.connection);
    Ok(())
}

enum IngestEvent {
    Continue,
    ShutdownChanged,
    Frame(RawFrame),
    PeerClosed,
    Deadline,
    KeepAliveTimeout,
    ReadTimeout,
}

async fn ingest_connection_input<R, W>(
    state: &mut H2ConnectionState<R, W>,
) -> Result<IngestEvent, H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + H2WriteTarget,
{
    let continue_writing = state.writer.has_ready_streams();
    let outbound_notified = state.writer.outbound_notified();
    tokio::pin!(outbound_notified);
    let keep_alive_deadline = (state.active_stream_count() == 0)
        .then_some(state.context.config.timeout_keep_alive)
        .flatten()
        .map(|timeout_duration| TokioInstant::now() + timeout_duration);
    let read_deadline = state
        .awaiting_request_input()
        .then_some(state.context.config.timeout_read)
        .flatten()
        .map(|timeout_duration| TokioInstant::now() + timeout_duration);
    let keep_alive_timeout = async {
        if let Some(deadline) = keep_alive_deadline {
            sleep_until(deadline).await;
        } else {
            pending::<()>().await;
        }
    };
    let read_timeout = async {
        if let Some(deadline) = read_deadline {
            sleep_until(deadline).await;
        } else {
            pending::<()>().await;
        }
    };

    if state.writer.has_queued_app_writes().await {
        return Ok(IngestEvent::Continue);
    }

    if let Some(deadline) = state.drain_state.deadline() {
        tokio::select! {
            _ = sleep_until(TokioInstant::from_std(deadline)) => Ok(IngestEvent::Deadline),
            _ = keep_alive_timeout, if keep_alive_deadline.is_some() => Ok(IngestEvent::KeepAliveTimeout),
            _ = read_timeout, if read_deadline.is_some() => Ok(IngestEvent::ReadTimeout),
            _ = &mut outbound_notified => Ok(IngestEvent::Continue),
            _ = yield_now(), if continue_writing => Ok(IngestEvent::Continue),
            frame = state.reader.read_frame(frame::MAX_FRAME_SIZE_UPPER_BOUND) => {
                Ok(frame?.map_or(IngestEvent::PeerClosed, IngestEvent::Frame))
            },
        }
    } else {
        tokio::select! {
            _ = state.shutdown.changed() => Ok(IngestEvent::ShutdownChanged),
            _ = keep_alive_timeout, if keep_alive_deadline.is_some() => Ok(IngestEvent::KeepAliveTimeout),
            _ = read_timeout, if read_deadline.is_some() => Ok(IngestEvent::ReadTimeout),
            _ = &mut outbound_notified => Ok(IngestEvent::Continue),
            _ = yield_now(), if continue_writing => Ok(IngestEvent::Continue),
            frame = state.reader.read_frame(frame::MAX_FRAME_SIZE_UPPER_BOUND) => {
                Ok(frame?.map_or(IngestEvent::PeerClosed, IngestEvent::Frame))
            },
        }
    }
}

async fn flush_connection_egress<R, W>(
    state: &mut H2ConnectionState<R, W>,
) -> Result<(), H2CornError>
where
    W: H2WriteTarget,
{
    apply_writer_response_closes(state).await;

    if state.writer.drain_app_writes().await? {
        apply_writer_response_closes(state).await;
    }

    if state.writer.has_ready_streams() {
        let _ = state.writer.flush_pending_output().await?;
        state.writer.flush().await?;
        apply_writer_response_closes(state).await;
    } else if state.writer.needs_flush() {
        state.writer.flush().await?;
    }

    Ok(())
}

async fn advance_connection_with_peer_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: RawFrame,
) -> Result<bool, H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + H2WriteTarget,
{
    apply_writer_response_closes(state).await;

    if frame.header.len > state.peer_max_frame_size {
        let error_code = ErrorCode::FRAME_SIZE_ERROR;
        if frame.header.stream_id.is_none()
            || matches!(
                frame.header.frame_type,
                FrameType::HEADERS
                    | FrameType::CONTINUATION
                    | FrameType::PUSH_PROMISE
                    | FrameType::SETTINGS
            )
        {
            apply_peer_failure(
                &mut state.writer,
                state.last_client_stream_id,
                H2PeerFailure::connection(error_code, H2Error::FrameExceedsAdvertisedMaxSize),
            )
            .await?;
            return Ok(false);
        }
        apply_peer_failure(
            &mut state.writer,
            state.last_client_stream_id,
            H2PeerFailure::stream(
                frame
                    .header
                    .stream_id
                    .expect("connection stream is already excluded"),
                error_code,
            ),
        )
        .await?;
        return Ok(false);
    }

    if state.pending_headers.is_some() && frame.header.frame_type != FrameType::CONTINUATION {
        apply_peer_failure(
            &mut state.writer,
            state.last_client_stream_id,
            H2PeerFailure::protocol(H2Error::HeaderBlockInterrupted),
        )
        .await?;
        return Ok(false);
    }

    if !state.saw_client_settings {
        if frame.header.frame_type != FrameType::SETTINGS {
            apply_peer_failure(
                &mut state.writer,
                state.last_client_stream_id,
                H2PeerFailure::protocol(H2Error::FirstClientFrameMustBeSettings),
            )
            .await?;
            return Ok(false);
        }
        if frame.header.flags.contains(FrameFlags::ACK) {
            apply_peer_failure(
                &mut state.writer,
                state.last_client_stream_id,
                H2PeerFailure::protocol(H2Error::FirstClientSettingsMustNotAck),
            )
            .await?;
            return Ok(false);
        }
        state.saw_client_settings = true;
    }

    match frame.header.frame_type {
        FrameType::SETTINGS => {
            if frame.header.stream_id.is_some() {
                apply_peer_failure(
                    &mut state.writer,
                    state.last_client_stream_id,
                    H2PeerFailure::protocol(H2Error::SettingsMustUseStreamZero),
                )
                .await?;
                return Ok(false);
            }
            if frame.header.flags.contains(FrameFlags::ACK) {
                if !frame.payload.is_empty() {
                    apply_peer_failure(
                        &mut state.writer,
                        state.last_client_stream_id,
                        H2PeerFailure::frame_size(H2Error::SettingsAckPayloadNotEmpty),
                    )
                    .await?;
                    return Ok(false);
                }
                return Ok(false);
            }
            if !frame.payload.len().is_multiple_of(6) {
                apply_peer_failure(
                    &mut state.writer,
                    state.last_client_stream_id,
                    H2PeerFailure::frame_size(H2Error::SettingsPayloadLengthInvalid),
                )
                .await?;
                return Ok(false);
            }
            let settings = match frame::parse_settings(&frame) {
                Ok(settings) => settings,
                Err(err) => {
                    apply_peer_failure(
                        &mut state.writer,
                        state.last_client_stream_id,
                        H2PeerFailure::protocol(H2Error::invalid_peer_settings(err)),
                    )
                    .await?;
                    return Ok(false);
                }
            };
            state.peer_max_frame_size = settings.max_frame_size();
            state.writer.send_settings_ack().await?;
            state.writer.update_peer_settings(settings).await?;
        }
        FrameType::PING => {
            if frame.header.stream_id.is_some() {
                apply_peer_failure(
                    &mut state.writer,
                    state.last_client_stream_id,
                    H2PeerFailure::protocol(H2Error::PingMustUseStreamZero),
                )
                .await?;
                return Ok(false);
            }
            if frame.payload.len() != 8 {
                apply_peer_failure(
                    &mut state.writer,
                    state.last_client_stream_id,
                    H2PeerFailure::frame_size(H2Error::PingPayloadInvalidLength),
                )
                .await?;
                return Ok(false);
            }
            if !frame.header.flags.contains(FrameFlags::ACK) {
                let payload = frame.payload[..8]
                    .try_into()
                    .expect("PING payload length is validated");
                state.writer.ping_ack(payload).await?;
            }
        }
        FrameType::WINDOW_UPDATE => {
            if frame.payload.len() != 4 {
                apply_peer_failure(
                    &mut state.writer,
                    state.last_client_stream_id,
                    H2PeerFailure::frame_size(H2Error::WindowUpdatePayloadInvalidLength),
                )
                .await?;
                return Ok(false);
            }
            let Some(increment) = WindowIncrement::new(
                u32::from_be_bytes(
                    frame.payload[..4]
                        .try_into()
                        .expect("WINDOW_UPDATE payload length is validated"),
                ) & frame::MAX_FLOW_CONTROL_WINDOW,
            ) else {
                if frame.header.stream_id.is_none() {
                    apply_peer_failure(
                        &mut state.writer,
                        state.last_client_stream_id,
                        H2PeerFailure::protocol(H2Error::WindowUpdateIncrementZero),
                    )
                    .await?;
                    return Ok(false);
                }
                apply_peer_failure(
                    &mut state.writer,
                    state.last_client_stream_id,
                    H2PeerFailure::stream(
                        frame
                            .header
                            .stream_id
                            .expect("zero-stream case is already handled"),
                        ErrorCode::PROTOCOL_ERROR,
                    ),
                )
                .await?;
                return Ok(false);
            };
            if let Some(stream_id) = frame.header.stream_id {
                if state.receive_state(stream_id).is_idle() {
                    apply_peer_failure(
                        &mut state.writer,
                        state.last_client_stream_id,
                        H2PeerFailure::protocol(H2Error::WindowUpdateOnIdleStream),
                    )
                    .await?;
                    return Ok(false);
                }
                state
                    .writer
                    .grant_send_window(WindowTarget::Stream(stream_id), increment)
                    .await?;
            } else {
                if state
                    .writer
                    .grant_send_window(WindowTarget::Connection, increment)
                    .await?
                {
                    return Ok(true);
                }
            }
        }
        FrameType::RST_STREAM => {
            let Some(stream_id) = frame.header.stream_id else {
                apply_peer_failure(
                    &mut state.writer,
                    state.last_client_stream_id,
                    H2PeerFailure::protocol(H2Error::RstStreamMustNotUseStreamZero),
                )
                .await?;
                return Ok(false);
            };
            if frame.payload.len() != 4 {
                apply_peer_failure(
                    &mut state.writer,
                    state.last_client_stream_id,
                    H2PeerFailure::frame_size(H2Error::RstStreamPayloadInvalidLength),
                )
                .await?;
                return Ok(false);
            }
            if state.receive_state(stream_id).is_idle() {
                apply_peer_failure(
                    &mut state.writer,
                    state.last_client_stream_id,
                    H2PeerFailure::protocol(H2Error::RstStreamOnIdleStream),
                )
                .await?;
                return Ok(false);
            }
            let error_code = ErrorCode::new(u32::from_be_bytes(
                frame.payload[..4]
                    .try_into()
                    .expect("RST_STREAM payload length is validated"),
            ));
            if let Some(mut stream) = state.remove_stream(stream_id)
                && let Some(tx) = stream.input.take()
            {
                send_best_effort(&tx, StreamInput::Reset(error_code)).await;
            }
            state.writer.drop_ingress_stream(stream_id).await;
            state.writer.peer_reset(stream_id).await?;
        }
        FrameType::GOAWAY => {
            if frame.header.stream_id.is_some() || frame.payload.len() < 8 {
                apply_peer_failure(
                    &mut state.writer,
                    state.last_client_stream_id,
                    H2PeerFailure::protocol(H2Error::InvalidGoawayFrame),
                )
                .await?;
                return Ok(false);
            }
            if state.drain_state == ConnectionDrainState::Accepting {
                state.drain_state = ConnectionDrainState::Draining { deadline: None };
            }
        }
        FrameType::PRIORITY => {
            let Some(stream_id) = frame.header.stream_id else {
                apply_peer_failure(
                    &mut state.writer,
                    state.last_client_stream_id,
                    H2PeerFailure::protocol(H2Error::PriorityMustNotUseStreamZero),
                )
                .await?;
                return Ok(false);
            };
            if frame.payload.len() != 5 {
                apply_peer_failure(
                    &mut state.writer,
                    state.last_client_stream_id,
                    H2PeerFailure::frame_size(H2Error::PriorityPayloadInvalidLength),
                )
                .await?;
                return Ok(false);
            }
            if matches!(
                parse_priority_dependency(frame.payload.as_ref())?,
                PriorityDependency::Stream(stream_dependency) if stream_dependency == stream_id
            ) {
                apply_peer_failure(
                    &mut state.writer,
                    state.last_client_stream_id,
                    H2PeerFailure::stream(stream_id, ErrorCode::PROTOCOL_ERROR),
                )
                .await?;
            }
        }
        FrameType::PUSH_PROMISE => {
            apply_peer_failure(
                &mut state.writer,
                state.last_client_stream_id,
                H2PeerFailure::protocol(H2Error::UnexpectedPushPromise),
            )
            .await?;
            return Ok(false);
        }
        FrameType::HEADERS => {
            handle_headers_frame(state, frame).await?;
        }
        FrameType::CONTINUATION => {
            handle_continuation_frame(state, frame).await?;
        }
        FrameType::DATA => {
            handle_data_frame(state, frame).await?;
        }
        _ => {}
    }
    Ok(false)
}

fn parse_data_payload(frame: &RawFrame) -> Result<(Bytes, bool), H2CornError> {
    let mut start = 0;
    let mut end = frame.payload.len();
    if frame.header.flags.contains(FrameFlags::PADDED) {
        if frame.payload.is_empty() {
            return H2Error::DataPaddedMissingPadding.err();
        }
        let pad_len = usize::from(frame.payload[0]);
        start += 1;
        if pad_len > end.saturating_sub(start) {
            return H2Error::DataPaddingExceedsPayload.err();
        }
        end -= pad_len;
    }
    Ok((
        frame.payload.slice(start..end),
        frame.header.flags.contains(FrameFlags::END_STREAM),
    ))
}

fn parse_priority_dependency(payload: &[u8]) -> Result<PriorityDependency, H2CornError> {
    if payload.len() != 5 {
        return H2Error::PriorityPayloadInvalidLength.err();
    }
    Ok(PriorityDependency::from_wire(u32::from_be_bytes(
        payload[..4]
            .try_into()
            .expect("priority payload length is validated"),
    )))
}

async fn apply_writer_response_closes<R, W>(state: &mut H2ConnectionState<R, W>)
where
    W: H2WriteTarget,
{
    for response_close in state.writer.take_response_closes() {
        state.writer.drop_ingress_stream(response_close).await;
        state.apply_response_close(response_close);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        pin::Pin,
        task::{Context, Poll},
    };

    use tokio::io::{AsyncWrite, BufWriter};

    use super::*;

    #[derive(Default)]
    struct RecordingWriter {
        bytes: Vec<u8>,
    }

    impl AsyncWrite for RecordingWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.bytes.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl writer::H2WriteTarget for RecordingWriter {
        const SUPPORTS_SENDFILE: bool = false;

        async fn write_file_chunk(
            _writer: &mut BufWriter<Self>,
            _header: [u8; 9],
            _file: &mut tokio::fs::File,
            _offset: &mut u64,
            _len: usize,
        ) -> io::Result<()> {
            unreachable!("test writer never uses sendfile")
        }
    }

    fn parse_window_updates(bytes: &[u8]) -> Vec<(u32, u32)> {
        let mut cursor = 0;
        let mut updates = Vec::new();

        while cursor + 9 <= bytes.len() {
            let len = ((bytes[cursor] as usize) << 16)
                | ((bytes[cursor + 1] as usize) << 8)
                | bytes[cursor + 2] as usize;
            let frame_type = bytes[cursor + 3];
            let stream_id = u32::from_be_bytes([
                bytes[cursor + 5],
                bytes[cursor + 6],
                bytes[cursor + 7],
                bytes[cursor + 8],
            ]) & frame::STREAM_ID_MASK;

            if frame_type == 8 && len == 4 {
                let increment = u32::from_be_bytes([
                    bytes[cursor + 9],
                    bytes[cursor + 10],
                    bytes[cursor + 11],
                    bytes[cursor + 12],
                ]) & frame::MAX_FLOW_CONTROL_WINDOW;
                updates.push((stream_id, increment));
            }

            cursor += 9 + len;
        }

        updates
    }

    #[tokio::test]
    async fn receive_window_updates_wait_for_threshold() {
        let recording = RecordingWriter::default();
        let mut writer = writer::WriterState::new_test(recording);
        let stream_id = StreamId::new(1).unwrap();
        let mut receive_window = state::ReceiveWindowState::new(INITIAL_STREAM_WINDOW_SIZE);

        receive_window
            .receive(STREAM_WINDOW_UPDATE_THRESHOLD - 1)
            .unwrap();
        if let Some(increment) = receive_window.take_update(STREAM_WINDOW_UPDATE_THRESHOLD) {
            writer
                .send_window_update(WindowTarget::Stream(stream_id), increment)
                .await
                .unwrap();
        }
        writer.flush().await.unwrap();
        assert!(parse_window_updates(&writer.test_writer_ref().bytes).is_empty());
        assert!(
            receive_window
                .take_update(STREAM_WINDOW_UPDATE_THRESHOLD)
                .is_none()
        );

        receive_window.receive(1).unwrap();
        if let Some(increment) = receive_window.take_update(STREAM_WINDOW_UPDATE_THRESHOLD) {
            writer
                .send_window_update(WindowTarget::Stream(stream_id), increment)
                .await
                .unwrap();
        }
        writer.flush().await.unwrap();

        assert_eq!(
            parse_window_updates(&writer.test_writer_ref().bytes),
            vec![(1, STREAM_WINDOW_UPDATE_THRESHOLD)]
        );
        assert!(
            receive_window
                .take_update(STREAM_WINDOW_UPDATE_THRESHOLD)
                .is_none()
        );
    }

    #[test]
    fn receive_window_update_threshold_is_half_window() {
        assert_eq!(
            CONNECTION_WINDOW_UPDATE_THRESHOLD,
            INITIAL_CONNECTION_WINDOW_SIZE / 2
        );
        assert_eq!(
            STREAM_WINDOW_UPDATE_THRESHOLD,
            INITIAL_STREAM_WINDOW_SIZE / 2
        );
    }
}
