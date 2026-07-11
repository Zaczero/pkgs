mod deadline;
mod http;
mod request;
mod state;
mod websocket;
mod writer;

use std::collections::HashMap;
use std::future::{Future, pending};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use http::spawn_request_stream;
use nohash_hasher::BuildNoHashHasher;
use request::{
    HeaderBlockFragment, PendingHeaders, RequestHeadError, decode_request_head,
    decode_trailer_block, parse_header_block_fragment, resolve_request_head,
};
use smallvec::SmallVec;
use state::{
    ClientPrefaceState, ConnectionDrainState, H2ConnectionState, InboundStream, ReceiveState,
    RequestInputClose, RequestInputDeadline, RequestSpawnContext, StreamLifecycle,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, watch};
use tokio::task::yield_now;
use tokio::time::{Instant as TokioInstant, sleep_until, timeout, timeout_at};
use writer::{ConnectionHandle, WindowTarget, WriterState, init_writer};

use crate::async_util::{send_best_effort, send_with_backpressure};
use crate::error::{ErrorExt, ErrorKind, H2CornError, H2Error};
use crate::h2_frame::{
    self, ErrorCode, FrameFlags, FrameType, PeerSettings, RawFrame, StreamId, WindowIncrement,
};
use crate::http::body::RequestBodyProgress;
use crate::http::types::{RequestHead, ResponseHeaders};
use crate::proxy_protocol::read_h2_preface;
use crate::runtime::{ConnectionContext, ShutdownState, StreamInput};
use crate::sendfile::WriteTarget;

/// Initial capacity for per-connection stream-keyed containers: the
/// concurrency cap is not the working set — idle and low-traffic connections
/// hold a handful of streams, so preallocating for the cap (default 256)
/// wastes memory on every idle connection. Start small; grow toward the cap
/// on demand.
const LAZY_STREAM_CAPACITY: usize = 8;
/// Explicit cooperative bound for a peer whose complete frames are already
/// buffered. Timers are selected first on every turn, and after this many
/// consecutive frames the connection yields to other Tokio tasks.
const MAX_CONSECUTIVE_PEER_FRAMES: usize = 32;

type StreamMap<T> = HashMap<StreamId, T, BuildNoHashHasher<StreamId>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PriorityDependency {
    Root,
    Stream(StreamId),
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

enum IngestEvent {
    Continue,
    ShutdownChanged,
    Frame(RawFrame),
    FrameLengthExceeded(H2Error),
    PeerClosed,
    Deadline(ConnectionDeadline),
}

#[derive(Clone, Copy, Debug)]
enum ConnectionDeadline {
    Drain(TokioInstant),
    KeepAlive(TokioInstant),
    RequestInput(RequestInputDeadline),
    ResponseStall(TokioInstant),
}

impl ConnectionDeadline {
    const fn instant(self) -> TokioInstant {
        match self {
            Self::Drain(instant) | Self::KeepAlive(instant) | Self::ResponseStall(instant) => {
                instant
            },
            Self::RequestInput(deadline) => deadline.instant(),
        }
    }
}

pub(crate) struct UpgradedH2Request {
    pub buffer: BytesMut,
    pub request: RequestHead,
    pub body: Bytes,
    pub peer_settings: PeerSettings,
}

struct RequestStartContext<'a, W> {
    writer: &'a mut WriterState<W>,
    connection: &'a ConnectionHandle,
    spawn: RequestSpawnContext<'a>,
    last_client_stream_id: Option<StreamId>,
}

impl PriorityDependency {
    fn from_wire(raw: u32) -> Self {
        StreamId::new(raw & h2_frame::STREAM_ID_MASK).map_or(Self::Root, Self::Stream)
    }
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

    const fn stream(stream_id: StreamId, error_code: ErrorCode) -> Self {
        Self::Reset {
            stream_id,
            error_code,
        }
    }
}

impl<R, W> H2ConnectionState<R, W>
where
    W: WriteTarget,
{
    /// Send the `GOAWAY`/`RST_STREAM` for a peer protocol failure and propagate
    /// connection-fatal errors.
    async fn peer_failure(&mut self, failure: H2PeerFailure) -> Result<(), H2CornError> {
        apply_peer_failure(&mut self.writer, self.last_client_stream_id, failure).await
    }
}

impl<R, W> H2ConnectionState<R, W> {
    /// Validate, account, and deliver one DATA frame in a single stream-map
    /// lookup. Window updates and terminal actions are returned, never sent
    /// while the map entry is borrowed.
    fn receive_data_frame(
        &mut self,
        stream_id: StreamId,
        data: Bytes,
        flow_control_len: u32,
        end_stream: bool,
    ) -> DataIngress {
        let connection_window_threshold = self
            .context
            .config
            .http2
            .initial_connection_window_size
            .get()
            / 2;
        let stream_window_threshold =
            self.context.config.http2.initial_stream_window_size.get() / 2;
        let data_len = u32::try_from(data.len()).expect("DATA frame length fits u32");
        let discarded_len = flow_control_len
            .checked_sub(data_len)
            .expect("parsed DATA payload cannot exceed its flow-control length");

        if self.connection_window.receive(flow_control_len).is_err() {
            return DataIngress::FlowControlViolation;
        }

        let Some(stream) = self.streams.get_mut(&stream_id) else {
            return if self.receive_state(stream_id).is_idle() {
                DataIngress::IdleStream
            } else {
                self.connection_window.release(flow_control_len);
                DataIngress::StreamClosed {
                    connection_update: self
                        .connection_window
                        .take_update(connection_window_threshold),
                }
            };
        };
        if stream.state.request_is_closed() {
            self.request_deadlines
                .cancel(state::RequestInputDeadlineKey::Body(stream_id));
            self.streams.remove(&stream_id);
            self.connection_window.release(flow_control_len);
            return DataIngress::StreamClosed {
                connection_update: self
                    .connection_window
                    .take_update(connection_window_threshold),
            };
        }
        if stream.receive_window.receive(flow_control_len).is_err() {
            return DataIngress::FlowControlViolation;
        }
        if stream.counts_toward_read_timeout
            && let Some(timeout) = self.context.config.timeout_request_body_idle
        {
            self.request_deadlines.schedule(
                state::RequestInputDeadlineKey::Body(stream_id),
                TokioInstant::now() + timeout,
            );
        }

        if discarded_len != 0 {
            self.connection_window.release(discarded_len);
            stream.receive_window.release(discarded_len);
        }

        if !data.is_empty() {
            match stream.body.record_chunk(data.len() as u64) {
                RequestBodyProgress::Continue => {},
                RequestBodyProgress::SizeLimitExceeded
                | RequestBodyProgress::ContentLengthExceeded => {
                    self.connection_window.release(data_len);
                    stream.receive_window.release(data_len);
                    let reset_tx = stream.delivery.take_sender();
                    self.request_deadlines
                        .cancel(state::RequestInputDeadlineKey::Body(stream_id));
                    self.streams.remove(&stream_id);
                    return DataIngress::BodyLimitExceeded {
                        reset_tx,
                        connection_update: self
                            .connection_window
                            .take_update(connection_window_threshold),
                    };
                },
            }
            let credit = self.input_flow.credit(
                stream_id,
                NonZeroU32::new(data_len).expect("non-empty DATA length is non-zero"),
            );
            stream.push_body_data(data, credit);
        }

        let end = end_stream.then(|| match self.finish_request_input(stream_id) {
            Some(RequestInputClose::ContentLengthMismatch) => {
                self.streams.remove(&stream_id);
                DataEnd::ContentLengthMismatch
            },
            Some(RequestInputClose::Closed { .. }) | None => DataEnd::Settled,
        });

        let connection_update = self
            .connection_window
            .take_update(connection_window_threshold);
        let stream_update = self.streams.get_mut(&stream_id).and_then(|stream| {
            (!stream.state.request_is_closed())
                .then(|| stream.receive_window.take_update(stream_window_threshold))
                .flatten()
        });

        DataIngress::Accepted {
            connection_update,
            stream_update,
            end,
        }
    }
}

/// Outcome of ingesting one DATA frame under a single stream-map lookup;
/// the caller performs the async writer/app actions it names.
enum DataIngress {
    Accepted {
        connection_update: Option<WindowIncrement>,
        stream_update: Option<WindowIncrement>,
        end: Option<DataEnd>,
    },
    /// DATA on a never-opened stream: connection protocol error.
    IdleStream,
    /// DATA on a closed or input-finished stream: reset it.
    StreamClosed {
        connection_update: Option<WindowIncrement>,
    },
    /// Receive flow-control window underflow: connection error.
    FlowControlViolation,
    /// Request body exceeded its limit: reset stream and app input.
    BodyLimitExceeded {
        reset_tx: Option<mpsc::Sender<StreamInput>>,
        connection_update: Option<WindowIncrement>,
    },
}

enum DataEnd {
    Settled,
    /// Declared content-length was not met: reset the stream.
    ContentLengthMismatch,
}

fn earlier_deadline(
    current: Option<ConnectionDeadline>,
    candidate: Option<ConnectionDeadline>,
) -> Option<ConnectionDeadline> {
    match (current, candidate) {
        (Some(current), Some(candidate)) => Some(if current.instant() <= candidate.instant() {
            current
        } else {
            candidate
        }),
        (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
        (None, None) => None,
    }
}

fn new_stream_map<T>(capacity: usize) -> StreamMap<T> {
    HashMap::with_capacity_and_hasher(
        capacity.min(LAZY_STREAM_CAPACITY),
        BuildNoHashHasher::default(),
    )
}

async fn flush_released_input_credits<R, W>(
    state: &mut H2ConnectionState<R, W>,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    if !state.input_flow.has_pending() {
        return Ok(());
    }

    let connection_window_threshold = state
        .context
        .config
        .http2
        .initial_connection_window_size
        .get()
        / 2;
    let mut stream_updates = SmallVec::<[(StreamId, WindowIncrement); 8]>::new();
    let mut streams_with_credit = SmallVec::<[StreamId; 8]>::new();
    let mut ready_streams = SmallVec::<[StreamId; 8]>::new();
    let stream_window_threshold = state.context.config.http2.initial_stream_window_size.get() / 2;

    loop {
        state
            .input_flow
            .drain_into(&mut state.released_input_credits);
        for release in &state.released_input_credits {
            state.connection_window.release(release.len.get());
            let stream_id = release.stream_id;
            if let Some(stream) = state.streams.get_mut(&stream_id) {
                stream.receive_window.release(release.len.get());
                streams_with_credit.push(stream_id);
                ready_streams.push(stream_id);
            }
        }
        state.released_input_credits.clear();

        ready_streams.sort_unstable();
        ready_streams.dedup();
        for stream_id in ready_streams.drain(..) {
            let remove = state.streams.get_mut(&stream_id).is_some_and(|stream| {
                stream.delivery.flush() && stream.state == StreamLifecycle::Closed
            });
            if remove {
                state.streams.remove(&stream_id);
            }
        }

        if !state.input_flow.has_pending() {
            break;
        }
    }

    let connection_update = state
        .connection_window
        .take_update(connection_window_threshold);
    streams_with_credit.sort_unstable();
    streams_with_credit.dedup();
    for stream_id in streams_with_credit {
        let Some(stream) = state.streams.get_mut(&stream_id) else {
            continue;
        };
        if stream.state.request_is_closed() {
            continue;
        }
        if let Some(increment) = stream.receive_window.take_update(stream_window_threshold) {
            stream_updates.push((stream_id, increment));
        }
    }

    if let Some(increment) = connection_update {
        state
            .writer
            .send_window_update(WindowTarget::Connection, increment)
            .await?;
    }

    for (stream_id, increment) in stream_updates {
        state
            .writer
            .send_window_update(WindowTarget::Stream(stream_id), increment)
            .await?;
    }

    Ok(())
}

async fn apply_peer_failure<W>(
    writer: &mut WriterState<W>,
    last_stream_id: Option<StreamId>,
    failure: H2PeerFailure,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    match failure {
        H2PeerFailure::Goaway { error_code, error } => {
            let message = error.to_string().into_bytes();
            writer
                .goaway(last_stream_id, error_code, message, true)
                .await?;
            Err(error)
        },
        H2PeerFailure::Reset {
            stream_id,
            error_code,
        } => {
            writer.reset_stream(stream_id, error_code).await?;
            Ok(())
        },
    }
}

async fn begin_graceful_shutdown<W>(
    writer: &mut WriterState<W>,
    last_stream_id: Option<StreamId>,
    drain_state: &mut ConnectionDrainState,
    timeout_graceful_shutdown: Duration,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
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

async fn start_request_stream_from_block<W>(
    ctx: RequestStartContext<'_, W>,
    stream_id: StreamId,
    end_stream: bool,
    decode_result: Result<RequestHead, RequestHeadError>,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    let request = match resolve_request_head(end_stream, decode_result) {
        Ok(request) => request,
        Err(RequestHeadError::Reject { status }) => {
            ctx.writer
                .send_headers(stream_id, status, ResponseHeaders::new(), true)
                .await?;
            return Ok(());
        },
        Err(RequestHeadError::Connection { error_code, error }) => {
            apply_peer_failure(
                ctx.writer,
                ctx.last_client_stream_id,
                H2PeerFailure::connection(error_code, error),
            )
            .await?;
            return Ok(());
        },
        Err(RequestHeadError::Stream { error_code }) => {
            apply_peer_failure(
                ctx.writer,
                ctx.last_client_stream_id,
                H2PeerFailure::stream(stream_id, error_code),
            )
            .await?;
            return Ok(());
        },
    };

    spawn_request_stream(stream_id, request, end_stream, ctx.connection, ctx.spawn).await?;
    Ok(())
}

pub(crate) fn serve_h2_upgraded_connection<R, W>(
    reader: R,
    writer: W,
    connection: ConnectionContext,
    secure: bool,
    shutdown: watch::Receiver<ShutdownState>,
    upgraded: UpgradedH2Request,
) -> impl Future<Output = Result<(), H2CornError>>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + WriteTarget,
{
    Box::pin(drive_h2_upgraded_connection(
        reader, writer, connection, secure, shutdown, upgraded,
    ))
}

async fn drive_h2_upgraded_connection<R, W>(
    reader: R,
    writer: W,
    connection: ConnectionContext,
    secure: bool,
    shutdown: watch::Receiver<ShutdownState>,
    upgraded: UpgradedH2Request,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + WriteTarget,
{
    let mut reader = h2_frame::FrameReader::with_buffer(reader, upgraded.buffer);
    timeout(
        connection.config.timeout_handshake,
        read_h2_preface(&mut reader),
    )
    .await
    .map_err(|_| H2Error::ConnectionHandshakeTimedOut)??;
    let peer_settings = upgraded.peer_settings;
    let mut connection = start_h2_connection(
        reader,
        writer,
        connection,
        secure,
        shutdown,
        Some(peer_settings),
    )
    .await?;
    seed_upgraded_request(&mut connection, upgraded.request, upgraded.body).await?;
    run_h2_connection(Box::new(connection)).await
}

pub(crate) fn serve_connection<R, W>(
    reader: h2_frame::FrameReader<R>,
    writer: W,
    context: ConnectionContext,
    secure: bool,
    shutdown: watch::Receiver<ShutdownState>,
) -> impl Future<Output = Result<(), H2CornError>>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + WriteTarget,
{
    // Store the outer handshake/driver future once per connection.
    // `Pin<Box<F>>` retains static dispatch while preventing the complete H2
    // state machine from being copied into each generic caller and Tokio task.
    Box::pin(drive_connection(reader, writer, context, secure, shutdown))
}

async fn drive_connection<R, W>(
    reader: h2_frame::FrameReader<R>,
    writer: W,
    context: ConnectionContext,
    secure: bool,
    shutdown: watch::Receiver<ShutdownState>,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + WriteTarget,
{
    let state = start_h2_connection(reader, writer, context, secure, shutdown, None).await?;
    run_h2_connection(Box::new(state)).await
}

async fn start_h2_connection<R, W>(
    reader: h2_frame::FrameReader<R>,
    writer: W,
    context: ConnectionContext,
    secure: bool,
    shutdown: watch::Receiver<ShutdownState>,
    peer_settings: Option<PeerSettings>,
) -> Result<H2ConnectionState<R, W>, H2CornError>
where
    W: AsyncWrite + Unpin + Send + 'static + WriteTarget,
{
    let (writer, connection) =
        init_writer(writer, Arc::clone(&context.config), peer_settings).await?;

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
        secure,
        shutdown,
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
            tasks: &mut connection.request_tasks,
        },
    )
    .await?;
    connection.refresh_body_deadline(stream_id);
    if !body.is_empty()
        && let Some(tx) = connection
            .streams
            .get(&stream_id)
            .and_then(|stream| stream.delivery.sender())
    {
        send_with_backpressure(tx, StreamInput::data(body), || H2Error::StreamChannelClosed)
            .await?;
        send_best_effort(tx, StreamInput::EndStream).await;
    }
    connection.last_client_stream_id = Some(stream_id);
    Ok(())
}

async fn finish_trailer_block<R, W>(
    state: &mut H2ConnectionState<R, W>,
    stream_id: StreamId,
    block: Bytes,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    let header_limits = state.header_limits();
    match decode_trailer_block(&mut state.decoder, block, header_limits) {
        Ok(()) => match state.finish_request_input(stream_id) {
            Some(RequestInputClose::ContentLengthMismatch) => {
                state
                    .writer
                    .reset_stream(stream_id, ErrorCode::PROTOCOL_ERROR)
                    .await?;
                state.remove_stream(stream_id);
                state.request_tasks.cancel(stream_id).await;
            },
            Some(RequestInputClose::Closed { .. }) | None => {},
        },
        Err(RequestHeadError::Connection { error_code, error }) => {
            state
                .peer_failure(H2PeerFailure::connection(error_code, error))
                .await?;
        },
        Err(RequestHeadError::Stream { error_code }) => {
            state
                .peer_failure(H2PeerFailure::stream(stream_id, error_code))
                .await?;
        },
        Err(RequestHeadError::Reject { .. }) => {
            state
                .peer_failure(H2PeerFailure::stream(stream_id, ErrorCode::PROTOCOL_ERROR))
                .await?;
        },
    }
    Ok(())
}

async fn handle_trailing_headers<R, W>(
    state: &mut H2ConnectionState<R, W>,
    stream_id: StreamId,
    fragment: HeaderBlockFragment,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    if !fragment.end_headers || !fragment.end_stream {
        state
            .writer
            .reset_stream(stream_id, ErrorCode::PROTOCOL_ERROR)
            .await?;
        return Ok(());
    }
    finish_trailer_block(state, stream_id, fragment.block).await
}

async fn reject_headers_for_state<R, W>(
    state: &mut H2ConnectionState<R, W>,
    stream_id: StreamId,
    receive_state: ReceiveState,
) -> Result<bool, H2CornError>
where
    W: WriteTarget,
{
    match receive_state {
        ReceiveState::RequestClosed => {
            state
                .peer_failure(H2PeerFailure::stream(stream_id, ErrorCode::STREAM_CLOSED))
                .await?;
            Ok(true)
        },
        ReceiveState::Closed => {
            state
                .peer_failure(H2PeerFailure::connection(
                    ErrorCode::STREAM_CLOSED,
                    H2Error::HeadersOnClosedStream,
                ))
                .await?;
            Ok(true)
        },
        ReceiveState::Idle | ReceiveState::Open | ReceiveState::ResponseClosed => Ok(false),
    }
}

async fn start_or_buffer_headers<R, W>(
    state: &mut H2ConnectionState<R, W>,
    stream_id: StreamId,
    fragment: HeaderBlockFragment,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: WriteTarget,
{
    if fragment.end_headers {
        let header_limits = state.header_limits();
        if state.header_block_too_large(fragment.block.len()) {
            state
                .peer_failure(H2PeerFailure::stream(stream_id, ErrorCode::PROTOCOL_ERROR))
                .await?;
            return Ok(());
        }
        start_request_stream_from_block(
            RequestStartContext {
                writer: &mut state.writer,
                connection: &state.connection,
                spawn: RequestSpawnContext {
                    streams: &mut state.streams,
                    connection: &state.context,
                    tasks: &mut state.request_tasks,
                },
                last_client_stream_id: state.last_client_stream_id,
            },
            stream_id,
            fragment.end_stream,
            decode_request_head(
                &mut state.decoder,
                fragment.block,
                header_limits,
                state.secure,
            ),
        )
        .await?;
        state.refresh_body_deadline(stream_id);
    } else {
        if state.header_block_too_large(fragment.block.len()) {
            state
                .peer_failure(H2PeerFailure::stream(stream_id, ErrorCode::PROTOCOL_ERROR))
                .await?;
            return Ok(());
        }
        state.pending_headers = Some(PendingHeaders {
            stream_id,
            end_stream: fragment.end_stream,
            block: fragment.block.into(),
        });
        state.refresh_header_deadline(stream_id);
    }
    Ok(())
}

async fn handle_headers_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: RawFrame,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: WriteTarget,
{
    let stream_id = match frame.header.stream_id {
        Some(stream_id) if stream_id.is_client_initiated() => stream_id,
        _ => {
            state
                .peer_failure(H2PeerFailure::protocol(H2Error::InvalidRequestStreamId))
                .await?;
            return Ok(());
        },
    };
    let stream = state.streams.get(&stream_id);
    let has_stream = stream.is_some();
    let receive_state = stream.map_or_else(
        || state.receive_state(stream_id),
        |stream| stream.state.into(),
    );

    if !has_stream
        && state
            .last_client_stream_id
            .is_some_and(|last| stream_id < last)
    {
        state
            .peer_failure(H2PeerFailure::protocol(
                H2Error::ClientStreamIdsNotIncreasing,
            ))
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

    let fragment = match parse_header_block_fragment(frame) {
        Ok(fragment) => fragment,
        Err(err) => {
            state.peer_failure(H2PeerFailure::protocol(err)).await?;
            return Ok(());
        },
    };
    if matches!(
        fragment.stream_dependency,
        Some(PriorityDependency::Stream(stream_dependency)) if stream_dependency == stream_id
    ) {
        state
            .peer_failure(H2PeerFailure::stream(stream_id, ErrorCode::PROTOCOL_ERROR))
            .await?;
        return Ok(());
    }

    match receive_state {
        ReceiveState::Open | ReceiveState::ResponseClosed => {
            handle_trailing_headers(state, stream_id, fragment).await?;
            return Ok(());
        },
        ReceiveState::Idle => {},
        ReceiveState::RequestClosed | ReceiveState::Closed => {
            reject_headers_for_state(state, stream_id, receive_state).await?;
            return Ok(());
        },
    }

    if state.active_stream_count() >= state.context.config.http2.max_concurrent_streams as usize {
        state.last_client_stream_id = Some(stream_id);
        state
            .peer_failure(H2PeerFailure::stream(stream_id, ErrorCode::REFUSED_STREAM))
            .await?;
        return Ok(());
    }
    state.last_client_stream_id = Some(stream_id);

    start_or_buffer_headers(state, stream_id, fragment).await
}

async fn handle_continuation_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: RawFrame,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: WriteTarget,
{
    let Some(mut pending) = state.pending_headers.take() else {
        state
            .peer_failure(H2PeerFailure::protocol(
                H2Error::UnexpectedContinuationFrame,
            ))
            .await?;
        return Ok(());
    };
    state.cancel_header_deadline(pending.stream_id);
    if frame.header.stream_id != Some(pending.stream_id) {
        state
            .peer_failure(H2PeerFailure::protocol(
                H2Error::ContinuationStreamIdMismatch,
            ))
            .await?;
        return Ok(());
    }

    pending.block.extend_from_slice(frame.payload.as_ref());
    if state.header_block_too_large(pending.block.len()) {
        state
            .peer_failure(H2PeerFailure::stream(
                pending.stream_id,
                ErrorCode::PROTOCOL_ERROR,
            ))
            .await?;
        return Ok(());
    }
    if frame.header.flags.contains(FrameFlags::END_HEADERS) {
        let header_limits = state.header_limits();
        start_request_stream_from_block(
            RequestStartContext {
                writer: &mut state.writer,
                connection: &state.connection,
                spawn: RequestSpawnContext {
                    streams: &mut state.streams,
                    connection: &state.context,
                    tasks: &mut state.request_tasks,
                },
                last_client_stream_id: state.last_client_stream_id,
            },
            pending.stream_id,
            pending.end_stream,
            decode_request_head(
                &mut state.decoder,
                pending.block.freeze(),
                header_limits,
                state.secure,
            ),
        )
        .await?;
        state.refresh_body_deadline(pending.stream_id);
    } else {
        state.refresh_header_deadline(pending.stream_id);
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
    W: WriteTarget,
{
    let header = frame.header;
    let flow_control_len =
        u32::try_from(frame.payload.len()).expect("HTTP/2 wire payload lengths fit in 24 bits");
    let (data, end_stream) = match parse_data_payload(frame.payload, header.flags) {
        Ok(parsed) => parsed,
        Err(err) => {
            state.peer_failure(H2PeerFailure::protocol(err)).await?;
            return Ok(());
        },
    };
    let Some(stream_id) = header.stream_id else {
        state
            .peer_failure(H2PeerFailure::protocol(H2Error::DataMustNotUseStreamZero))
            .await?;
        return Ok(());
    };

    match state.receive_data_frame(stream_id, data, flow_control_len, end_stream) {
        DataIngress::IdleStream => {
            state
                .peer_failure(H2PeerFailure::protocol(H2Error::DataOnIdleStream))
                .await
        },
        DataIngress::StreamClosed { connection_update } => {
            if let Some(increment) = connection_update {
                state
                    .writer
                    .send_window_update(WindowTarget::Connection, increment)
                    .await?;
            }
            state
                .peer_failure(H2PeerFailure::stream(stream_id, ErrorCode::STREAM_CLOSED))
                .await
        },
        DataIngress::FlowControlViolation => {
            state
                .peer_failure(H2PeerFailure::connection(
                    ErrorCode::FLOW_CONTROL_ERROR,
                    H2Error::ReceiveFlowControlWindowUnderflow,
                ))
                .await
        },
        DataIngress::BodyLimitExceeded {
            reset_tx,
            connection_update,
        } => {
            if let Some(increment) = connection_update {
                state
                    .writer
                    .send_window_update(WindowTarget::Connection, increment)
                    .await?;
            }
            if let Some(tx) = reset_tx {
                send_best_effort(&tx, StreamInput::Reset(ErrorCode::PROTOCOL_ERROR)).await;
            }
            state
                .peer_failure(H2PeerFailure::stream(stream_id, ErrorCode::PROTOCOL_ERROR))
                .await?;
            state.request_tasks.cancel(stream_id).await;
            Ok(())
        },
        DataIngress::Accepted {
            connection_update,
            stream_update,
            end,
        } => {
            if let Some(increment) = connection_update {
                state
                    .writer
                    .send_window_update(WindowTarget::Connection, increment)
                    .await?;
            }
            if let Some(increment) = stream_update {
                state
                    .writer
                    .send_window_update(WindowTarget::Stream(stream_id), increment)
                    .await?;
            }
            match end {
                None | Some(DataEnd::Settled) => Ok(()),
                Some(DataEnd::ContentLengthMismatch) => {
                    state
                        .peer_failure(H2PeerFailure::stream(stream_id, ErrorCode::PROTOCOL_ERROR))
                        .await
                },
            }
        },
    }
}

async fn run_h2_connection<R, W>(mut state: Box<H2ConnectionState<R, W>>) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + WriteTarget,
{
    match drive_h2_connection(&mut state).await {
        Ok(()) => {
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
        },
        Err(error) => {
            // The fatal GOAWAY was already written. Linger briefly
            // (nginx-style) so it actually reaches the peer: dropping the
            // socket with unread inbound bytes makes the kernel RST the
            // connection, and the peer's kernel then discards the unread
            // GOAWAY from its receive buffer.
            linger_after_fatal_goaway(&mut state).await;
            Err(error)
        },
    }
}

/// Bounded lingering close: half-close the write side, then discard inbound
/// bytes until the peer closes or the linger window expires.
async fn linger_after_fatal_goaway<R, W>(state: &mut H2ConnectionState<R, W>)
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + WriteTarget,
{
    const FATAL_GOAWAY_LINGER: Duration = Duration::from_secs(1);

    state.writer.shutdown_write().await;
    let deadline = TokioInstant::now() + FATAL_GOAWAY_LINGER;
    loop {
        let buffered = state.reader.buffered().len();
        state.reader.consume(buffered);
        match timeout_at(deadline, state.reader.read_more_capped(64 * 1024)).await {
            Ok(Ok(true)) => {},
            _ => break, // peer closed, read error, or linger window expired
        }
    }
}

async fn drive_h2_connection<R, W>(state: &mut H2ConnectionState<R, W>) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + WriteTarget,
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

    let mut consecutive_peer_frames = 0;
    loop {
        state.request_tasks.reap_completed();
        let mut stop_after_flush = false;
        let event = ingest_connection_input(state).await?;
        if !matches!(&event, IngestEvent::Frame(_)) {
            consecutive_peer_frames = 0;
        }
        match event {
            IngestEvent::Continue => {},
            IngestEvent::PeerClosed => {
                // Close body producers before cancelling the connection's
                // application tasks. Pending receive() calls queue their
                // disconnect first; abandoned tasks cancel immediately.
                state.streams.clear();
                state.request_tasks.cancel_all().await;
                stop_after_flush = true;
            },
            IngestEvent::FrameLengthExceeded(error) => {
                state
                    .writer
                    .goaway(
                        state.last_client_stream_id,
                        ErrorCode::FRAME_SIZE_ERROR,
                        error.to_string().into_bytes(),
                        true,
                    )
                    .await?;
                stop_after_flush = true;
            },
            IngestEvent::Deadline(deadline) => match deadline {
                ConnectionDeadline::Drain(_) | ConnectionDeadline::KeepAlive(_) => {
                    stop_after_flush = true;
                },
                ConnectionDeadline::RequestInput(_) => {
                    if let Some(deadline) =
                        state.pop_expired_request_input_deadline(TokioInstant::now())
                    {
                        handle_request_input_timeout(state, deadline).await?;
                    }
                },
                ConnectionDeadline::ResponseStall(_) => {
                    if let Some((stream_id, _)) = state
                        .writer
                        .pop_expired_response_stall_deadline(TokioInstant::now())
                    {
                        state
                            .writer
                            .reset_stream(stream_id, ErrorCode::CANCEL)
                            .await?;
                        state.remove_stream(stream_id);
                        state.request_tasks.cancel(stream_id).await;
                    }
                },
            },
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
            },
            IngestEvent::Frame(frame) => {
                consecutive_peer_frames += 1;
                if advance_connection_with_peer_frame(state, frame).await? {
                    stop_after_flush = true;
                }
            },
        }

        flush_connection_egress(state).await?;
        if stop_after_flush || state.should_stop() {
            return Ok(());
        }
        if consecutive_peer_frames >= MAX_CONSECUTIVE_PEER_FRAMES {
            yield_now().await;
            consecutive_peer_frames = 0;
        }
    }
}

async fn handle_request_input_timeout<R, W>(
    state: &mut H2ConnectionState<R, W>,
    deadline: RequestInputDeadline,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    match deadline {
        RequestInputDeadline::Headers(stream_id, _) => {
            if state
                .pending_headers
                .as_ref()
                .is_some_and(|pending| pending.stream_id == stream_id)
            {
                state.pending_headers = None;
                state
                    .writer
                    .reset_stream(stream_id, ErrorCode::CANCEL)
                    .await?;
            }
        },
        RequestInputDeadline::Body(stream_id, _) => {
            if let Some(stream) = state.streams.get_mut(&stream_id)
                && !stream.state.request_is_closed()
            {
                if let Some(tx) = stream.delivery.take_sender() {
                    send_best_effort(&tx, StreamInput::Reset(ErrorCode::CANCEL)).await;
                }
                state
                    .writer
                    .reset_stream(stream_id, ErrorCode::CANCEL)
                    .await?;
                state.remove_stream(stream_id);
                state.request_tasks.cancel(stream_id).await;
            }
        },
    }
    Ok(())
}

fn map_frame_ingest_result(
    frame: Result<Option<RawFrame>, H2CornError>,
) -> Result<IngestEvent, H2CornError> {
    match frame {
        Ok(Some(frame)) => Ok(IngestEvent::Frame(frame)),
        Ok(None) => Ok(IngestEvent::PeerClosed),
        Err(err) => match err.into_kind() {
            ErrorKind::H2(error @ H2Error::FrameLengthExceedsPeerMax { .. }) => {
                Ok(IngestEvent::FrameLengthExceeded(error))
            },
            kind => Err(kind.into()),
        },
    }
}

async fn ingest_connection_input<R, W>(
    state: &mut H2ConnectionState<R, W>,
) -> Result<IngestEvent, H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + WriteTarget,
{
    if state.writer.has_queued_app_writes() || state.input_flow.has_pending() {
        return Ok(IngestEvent::Continue);
    }

    let continue_writing = state.writer.has_ready_streams();
    let keep_alive_deadline = (state.active_stream_count() == 0)
        .then_some(state.context.config.timeout_keep_alive)
        .flatten()
        .map(|timeout_duration| {
            ConnectionDeadline::KeepAlive(TokioInstant::now() + timeout_duration)
        });
    let request_input_deadline = state.next_request_input_deadline();
    let response_stall_deadline = state.writer.next_response_stall_deadline();
    let drain_deadline = state
        .drain_state
        .deadline()
        .map(TokioInstant::from_std)
        .map(ConnectionDeadline::Drain);
    let mut next_deadline = earlier_deadline(drain_deadline, keep_alive_deadline);
    next_deadline = earlier_deadline(
        next_deadline,
        request_input_deadline.map(ConnectionDeadline::RequestInput),
    );
    next_deadline = earlier_deadline(
        next_deadline,
        response_stall_deadline.map(|(_, instant)| ConnectionDeadline::ResponseStall(instant)),
    );
    let outbound_notified = state.writer.outbound_notified();
    tokio::pin!(outbound_notified);
    let input_flow = Arc::clone(&state.input_flow);
    let input_notified = input_flow.notified();
    tokio::pin!(input_notified);
    let connection_timeout = async {
        if let Some(deadline) = next_deadline {
            sleep_until(deadline.instant()).await;
        } else {
            pending::<()>().await;
        }
    };

    tokio::select! {
        biased;
        () = connection_timeout, if next_deadline.is_some() => Ok(IngestEvent::Deadline(
            next_deadline.expect("timer is only polled when a deadline is present"),
        )),
        _ = state.shutdown.changed(), if drain_deadline.is_none() => Ok(IngestEvent::ShutdownChanged),
        () = &mut outbound_notified => Ok(IngestEvent::Continue),
        () = &mut input_notified => Ok(IngestEvent::Continue),
        () = yield_now(), if continue_writing => Ok(IngestEvent::Continue),
        frame = state.reader.read_frame(state.local_max_frame_size) => map_frame_ingest_result(frame),
    }
}

async fn flush_connection_egress<R, W>(
    state: &mut H2ConnectionState<R, W>,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    apply_writer_response_closes(state).await;
    flush_released_input_credits(state).await?;

    if state.writer.drain_app_writes().await? {
        apply_writer_response_closes(state).await;
        flush_released_input_credits(state).await?;
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

/// Wire-shape validation for `SETTINGS`; `Ok(None)` is a valid ACK.
fn validate_settings_frame(frame: &RawFrame) -> Result<Option<PeerSettings>, H2PeerFailure> {
    if frame.header.stream_id.is_some() {
        return Err(H2PeerFailure::protocol(H2Error::SettingsMustUseStreamZero));
    }
    if frame.header.flags.contains(FrameFlags::ACK) {
        if !frame.payload.is_empty() {
            return Err(H2PeerFailure::frame_size(
                H2Error::SettingsAckPayloadNotEmpty,
            ));
        }
        return Ok(None);
    }
    if !frame
        .payload
        .len()
        .is_multiple_of(h2_frame::SETTING_ENTRY_LEN)
    {
        return Err(H2PeerFailure::frame_size(
            H2Error::SettingsPayloadLengthInvalid,
        ));
    }
    h2_frame::parse_settings_payload(frame.payload.as_ref())
        .map(Some)
        .map_err(|err| H2PeerFailure::protocol(H2Error::invalid_peer_settings(err)))
}

async fn handle_settings_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: RawFrame,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    match validate_settings_frame(&frame) {
        Err(failure) => state.peer_failure(failure).await,
        Ok(None) => Ok(()),
        Ok(Some(settings)) => {
            state.writer.send_settings_ack().await?;
            state.writer.update_peer_settings(settings).await
        },
    }
}

/// Wire-shape validation for `PING`; `Ok(None)` is a valid ACK.
fn validate_ping_frame(frame: &RawFrame) -> Result<Option<[u8; 8]>, H2PeerFailure> {
    if frame.header.stream_id.is_some() {
        return Err(H2PeerFailure::protocol(H2Error::PingMustUseStreamZero));
    }
    let Ok(payload) = <[u8; 8]>::try_from(frame.payload.as_ref()) else {
        return Err(H2PeerFailure::frame_size(H2Error::PingPayloadInvalidLength));
    };
    Ok((!frame.header.flags.contains(FrameFlags::ACK)).then_some(payload))
}

async fn handle_ping_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: RawFrame,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    match validate_ping_frame(&frame) {
        Err(failure) => state.peer_failure(failure).await,
        Ok(None) => Ok(()),
        Ok(Some(payload)) => state.writer.ping_ack(payload).await,
    }
}

/// Wire-shape validation for `WINDOW_UPDATE`: length and non-zero increment.
fn validate_window_update_frame(
    frame: &RawFrame,
) -> Result<(Option<StreamId>, WindowIncrement), H2PeerFailure> {
    let Ok(raw) = <[u8; 4]>::try_from(frame.payload.as_ref()) else {
        return Err(H2PeerFailure::frame_size(
            H2Error::WindowUpdatePayloadInvalidLength,
        ));
    };
    let stream_id = frame.header.stream_id;
    WindowIncrement::new(u32::from_be_bytes(raw) & h2_frame::MAX_FLOW_CONTROL_WINDOW).map_or_else(
        || {
            Err(stream_id.map_or_else(
                || H2PeerFailure::protocol(H2Error::WindowUpdateIncrementZero),
                |stream_id| H2PeerFailure::stream(stream_id, ErrorCode::PROTOCOL_ERROR),
            ))
        },
        |increment| Ok((stream_id, increment)),
    )
}

async fn handle_window_update_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: RawFrame,
) -> Result<bool, H2CornError>
where
    W: WriteTarget,
{
    let (stream_id, increment) = match validate_window_update_frame(&frame) {
        Ok(update) => update,
        Err(failure) => {
            state.peer_failure(failure).await?;
            return Ok(false);
        },
    };
    if let Some(stream_id) = stream_id {
        if state.receive_state(stream_id).is_idle() {
            state
                .peer_failure(H2PeerFailure::protocol(H2Error::WindowUpdateOnIdleStream))
                .await?;
            return Ok(false);
        }
        state
            .writer
            .grant_send_window(WindowTarget::Stream(stream_id), increment)
            .await?;
        Ok(false)
    } else {
        state
            .writer
            .grant_send_window(WindowTarget::Connection, increment)
            .await
    }
}

/// Wire-shape validation for `RST_STREAM`: non-zero stream and 4-byte payload.
fn validate_rst_stream_frame(frame: &RawFrame) -> Result<(StreamId, ErrorCode), H2PeerFailure> {
    let Some(stream_id) = frame.header.stream_id else {
        return Err(H2PeerFailure::protocol(
            H2Error::RstStreamMustNotUseStreamZero,
        ));
    };
    let Ok(raw) = <[u8; 4]>::try_from(frame.payload.as_ref()) else {
        return Err(H2PeerFailure::frame_size(
            H2Error::RstStreamPayloadInvalidLength,
        ));
    };
    Ok((stream_id, ErrorCode::new(u32::from_be_bytes(raw))))
}

async fn handle_rst_stream_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: RawFrame,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    let (stream_id, error_code) = match validate_rst_stream_frame(&frame) {
        Ok(reset) => reset,
        Err(failure) => return state.peer_failure(failure).await,
    };
    if state.receive_state(stream_id).is_idle() {
        state
            .peer_failure(H2PeerFailure::protocol(H2Error::RstStreamOnIdleStream))
            .await?;
        return Ok(());
    }
    if state.reset_guard.note_reset() {
        return state
            .peer_failure(H2PeerFailure::connection(
                ErrorCode::ENHANCE_YOUR_CALM,
                H2Error::PeerResetFlood,
            ))
            .await;
    }
    if let Some(mut stream) = state.remove_stream(stream_id)
        && let Some(tx) = stream.delivery.take_sender()
    {
        send_best_effort(&tx, StreamInput::Reset(error_code)).await;
    }
    state.request_tasks.cancel(stream_id).await;
    state.writer.drop_ingress_stream(stream_id).await;
    state.writer.peer_reset(stream_id).await
}

/// Wire-shape validation for `PRIORITY`, including the self-dependency rule.
fn validate_priority_frame(frame: &RawFrame) -> Result<(), H2PeerFailure> {
    let Some(stream_id) = frame.header.stream_id else {
        return Err(H2PeerFailure::protocol(
            H2Error::PriorityMustNotUseStreamZero,
        ));
    };
    let Some((dependency, [_weight])) = frame.payload.as_ref().split_first_chunk::<4>() else {
        return Err(H2PeerFailure::frame_size(
            H2Error::PriorityPayloadInvalidLength,
        ));
    };
    match PriorityDependency::from_wire(u32::from_be_bytes(*dependency)) {
        PriorityDependency::Stream(dependency) if dependency == stream_id => {
            Err(H2PeerFailure::stream(stream_id, ErrorCode::PROTOCOL_ERROR))
        },
        _ => Ok(()),
    }
}

async fn handle_priority_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: RawFrame,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    match validate_priority_frame(&frame) {
        Ok(()) => Ok(()),
        Err(failure) => state.peer_failure(failure).await,
    }
}

async fn reject_oversized_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: &RawFrame,
) -> Result<bool, H2CornError>
where
    W: WriteTarget,
{
    if frame.payload.len() <= state.local_max_frame_size {
        return Ok(false);
    }
    let error_code = ErrorCode::FRAME_SIZE_ERROR;
    match frame.header.stream_id {
        None => {
            state
                .peer_failure(H2PeerFailure::connection(
                    error_code,
                    H2Error::FrameExceedsAdvertisedMaxSize,
                ))
                .await?;
        },
        Some(_stream_id)
            if matches!(
                frame.header.frame_type,
                FrameType::HEADERS
                    | FrameType::CONTINUATION
                    | FrameType::PUSH_PROMISE
                    | FrameType::SETTINGS
            ) =>
        {
            state
                .peer_failure(H2PeerFailure::connection(
                    error_code,
                    H2Error::FrameExceedsAdvertisedMaxSize,
                ))
                .await?;
        },
        Some(stream_id) => {
            state
                .peer_failure(H2PeerFailure::stream(stream_id, error_code))
                .await?;
        },
    }
    Ok(true)
}

async fn validate_frame_order<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: &RawFrame,
) -> Result<bool, H2CornError>
where
    W: WriteTarget,
{
    if state.pending_headers.is_some() && frame.header.frame_type != FrameType::CONTINUATION {
        state
            .peer_failure(H2PeerFailure::protocol(H2Error::HeaderBlockInterrupted))
            .await?;
        return Ok(false);
    }
    if state.client_preface == ClientPrefaceState::Active {
        return Ok(true);
    }
    if frame.header.frame_type != FrameType::SETTINGS {
        state
            .peer_failure(H2PeerFailure::protocol(
                H2Error::FirstClientFrameMustBeSettings,
            ))
            .await?;
        return Ok(false);
    }
    if frame.header.flags.contains(FrameFlags::ACK) {
        state
            .peer_failure(H2PeerFailure::protocol(
                H2Error::FirstClientSettingsMustNotAck,
            ))
            .await?;
        return Ok(false);
    }
    state.client_preface = ClientPrefaceState::Active;
    Ok(true)
}

async fn handle_goaway_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: RawFrame,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    if frame.header.stream_id.is_some() || frame.payload.len() < 8 {
        state
            .peer_failure(H2PeerFailure::protocol(H2Error::InvalidGoawayFrame))
            .await?;
        return Ok(());
    }
    if state.drain_state == ConnectionDrainState::Accepting {
        state.drain_state = ConnectionDrainState::Draining { deadline: None };
    }
    Ok(())
}

async fn reject_push_promise<R, W>(state: &mut H2ConnectionState<R, W>) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    state
        .peer_failure(H2PeerFailure::protocol(H2Error::UnexpectedPushPromise))
        .await
}

async fn advance_connection_with_peer_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: RawFrame,
) -> Result<bool, H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + WriteTarget,
{
    apply_writer_response_closes(state).await;

    if reject_oversized_frame(state, &frame).await? {
        return Ok(false);
    }
    if !validate_frame_order(state, &frame).await? {
        return Ok(false);
    }

    match frame.header.frame_type {
        FrameType::SETTINGS => {
            handle_settings_frame(state, frame).await?;
        },
        FrameType::PING => {
            handle_ping_frame(state, frame).await?;
        },
        FrameType::WINDOW_UPDATE => return handle_window_update_frame(state, frame).await,
        FrameType::RST_STREAM => {
            handle_rst_stream_frame(state, frame).await?;
        },
        FrameType::GOAWAY => {
            handle_goaway_frame(state, frame).await?;
        },
        FrameType::PRIORITY => {
            handle_priority_frame(state, frame).await?;
        },
        FrameType::PUSH_PROMISE => {
            reject_push_promise(state).await?;
        },
        FrameType::HEADERS => {
            handle_headers_frame(state, frame).await?;
        },
        FrameType::CONTINUATION => {
            handle_continuation_frame(state, frame).await?;
        },
        FrameType::DATA => {
            handle_data_frame(state, frame).await?;
        },
        _ => {},
    }
    Ok(false)
}

fn parse_data_payload(payload: Bytes, flags: FrameFlags) -> Result<(Bytes, bool), H2CornError> {
    let mut start = 0;
    let mut end = payload.len();
    if flags.contains(FrameFlags::PADDED) {
        if payload.is_empty() {
            return H2Error::DataPaddedMissingPadding.err();
        }
        let pad_len = usize::from(payload[0]);
        start += 1;
        if pad_len > end.saturating_sub(start) {
            return H2Error::DataPaddingExceedsPayload.err();
        }
        end -= pad_len;
    }
    Ok((
        if start == 0 && end == payload.len() {
            payload
        } else {
            payload.slice(start..end)
        },
        flags.contains(FrameFlags::END_STREAM),
    ))
}

async fn apply_writer_response_closes<R, W>(state: &mut H2ConnectionState<R, W>)
where
    W: WriteTarget,
{
    for response_close in state.writer.take_response_closes() {
        state.writer.drop_ingress_stream(response_close).await;
        state.apply_response_close(response_close);
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::io::{AsyncWrite, BufWriter};

    use super::*;
    use crate::h2_frame::FrameHeader;

    const INITIAL_STREAM_WINDOW_SIZE: u32 = 1 << 20;
    const INITIAL_CONNECTION_WINDOW_SIZE: u32 = 2 << 20;
    const STREAM_WINDOW_UPDATE_THRESHOLD: u32 = INITIAL_STREAM_WINDOW_SIZE / 2;
    const CONNECTION_WINDOW_UPDATE_THRESHOLD: u32 = INITIAL_CONNECTION_WINDOW_SIZE / 2;

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

    impl WriteTarget for RecordingWriter {
        const SUPPORTS_SENDFILE: bool = false;

        async fn send_file(
            _writer: &mut BufWriter<Self>,
            _file: &mut File,
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
            ]) & h2_frame::STREAM_ID_MASK;

            if frame_type == 8 && len == 4 {
                let increment = u32::from_be_bytes([
                    bytes[cursor + 9],
                    bytes[cursor + 10],
                    bytes[cursor + 11],
                    bytes[cursor + 12],
                ]) & h2_frame::MAX_FLOW_CONTROL_WINDOW;
                updates.push((stream_id, increment));
            }

            cursor += 9 + len;
        }

        updates
    }

    fn raw_frame(
        frame_type: FrameType,
        flags: FrameFlags,
        stream_id: Option<StreamId>,
        payload: &[u8],
    ) -> RawFrame {
        RawFrame {
            header: FrameHeader {
                frame_type,
                flags,
                stream_id,
            },
            payload: Bytes::copy_from_slice(payload),
        }
    }

    #[test]
    fn settings_validation_maps_wire_shape_errors() {
        let on_stream = raw_frame(FrameType::SETTINGS, FrameFlags::EMPTY, StreamId::new(1), &[
        ]);
        assert!(matches!(
            validate_settings_frame(&on_stream),
            Err(H2PeerFailure::Goaway { error_code, .. }) if error_code == ErrorCode::PROTOCOL_ERROR
        ));

        let ack_with_payload = raw_frame(FrameType::SETTINGS, FrameFlags::ACK, None, &[0]);
        assert!(matches!(
            validate_settings_frame(&ack_with_payload),
            Err(H2PeerFailure::Goaway { error_code, .. })
                if error_code == ErrorCode::FRAME_SIZE_ERROR
        ));

        let misaligned = raw_frame(FrameType::SETTINGS, FrameFlags::EMPTY, None, &[0; 5]);
        assert!(matches!(
            validate_settings_frame(&misaligned),
            Err(H2PeerFailure::Goaway { error_code, .. })
                if error_code == ErrorCode::FRAME_SIZE_ERROR
        ));

        let ack = raw_frame(FrameType::SETTINGS, FrameFlags::ACK, None, &[]);
        assert!(matches!(validate_settings_frame(&ack), Ok(None)));
    }

    #[test]
    fn ping_validation_extracts_payload_and_skips_acks() {
        let ping = raw_frame(FrameType::PING, FrameFlags::EMPTY, None, &[7; 8]);
        assert_eq!(validate_ping_frame(&ping).ok().flatten(), Some([7; 8]));

        let ack = raw_frame(FrameType::PING, FrameFlags::ACK, None, &[7; 8]);
        assert!(matches!(validate_ping_frame(&ack), Ok(None)));

        let short = raw_frame(FrameType::PING, FrameFlags::EMPTY, None, &[7; 4]);
        assert!(matches!(
            validate_ping_frame(&short),
            Err(H2PeerFailure::Goaway { error_code, .. })
                if error_code == ErrorCode::FRAME_SIZE_ERROR
        ));
    }

    #[test]
    fn window_update_zero_increment_splits_by_stream() {
        let conn_zero = raw_frame(FrameType::WINDOW_UPDATE, FrameFlags::EMPTY, None, &[0; 4]);
        assert!(matches!(
            validate_window_update_frame(&conn_zero),
            Err(H2PeerFailure::Goaway { error_code, .. })
                if error_code == ErrorCode::PROTOCOL_ERROR
        ));

        let stream_zero = raw_frame(
            FrameType::WINDOW_UPDATE,
            FrameFlags::EMPTY,
            StreamId::new(3),
            &[0; 4],
        );
        assert!(matches!(
            validate_window_update_frame(&stream_zero),
            Err(H2PeerFailure::Reset { error_code, .. })
                if error_code == ErrorCode::PROTOCOL_ERROR
        ));

        let grant = raw_frame(
            FrameType::WINDOW_UPDATE,
            FrameFlags::EMPTY,
            StreamId::new(3),
            &1_u32.to_be_bytes(),
        );
        let Ok((stream_id, increment)) = validate_window_update_frame(&grant) else {
            panic!("valid window update is accepted");
        };
        assert_eq!(stream_id, StreamId::new(3));
        assert_eq!(increment.get(), 1);
    }

    #[test]
    fn priority_validation_rejects_self_dependency() {
        let mut payload = 3_u32.to_be_bytes().to_vec();
        payload.push(0);
        let self_dependent = raw_frame(
            FrameType::PRIORITY,
            FrameFlags::EMPTY,
            StreamId::new(3),
            &payload,
        );
        assert!(matches!(
            validate_priority_frame(&self_dependent),
            Err(H2PeerFailure::Reset { error_code, .. })
                if error_code == ErrorCode::PROTOCOL_ERROR
        ));

        let other_dependency = raw_frame(
            FrameType::PRIORITY,
            FrameFlags::EMPTY,
            StreamId::new(5),
            &payload,
        );
        assert!(validate_priority_frame(&other_dependency).is_ok());
    }

    #[tokio::test]
    async fn receive_window_updates_wait_for_released_threshold() {
        let recording = RecordingWriter::default();
        let mut writer = writer::WriterState::new_test(recording);
        let stream_id = StreamId::new(1).unwrap();
        let mut receive_window = state::ReceiveWindowState::new(INITIAL_STREAM_WINDOW_SIZE);

        receive_window
            .receive(STREAM_WINDOW_UPDATE_THRESHOLD - 1)
            .unwrap();
        assert!(
            receive_window
                .take_update(STREAM_WINDOW_UPDATE_THRESHOLD)
                .is_none()
        );
        receive_window.release(STREAM_WINDOW_UPDATE_THRESHOLD - 1);
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
        assert!(
            receive_window
                .take_update(STREAM_WINDOW_UPDATE_THRESHOLD)
                .is_none()
        );
        receive_window.release(1);
        if let Some(increment) = receive_window.take_update(STREAM_WINDOW_UPDATE_THRESHOLD) {
            writer
                .send_window_update(WindowTarget::Stream(stream_id), increment)
                .await
                .unwrap();
        }
        writer.flush().await.unwrap();

        assert_eq!(parse_window_updates(&writer.test_writer_ref().bytes), vec![
            (1, STREAM_WINDOW_UPDATE_THRESHOLD)
        ]);
        assert!(
            receive_window
                .take_update(STREAM_WINDOW_UPDATE_THRESHOLD)
                .is_none()
        );
        drop(writer);
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
