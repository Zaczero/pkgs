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
    HeaderBlockFragment, HeaderBlockTarget, PendingHeaderBlock, RequestHeadError,
    decode_discarded_header_block, decode_request_head, decode_trailer_block,
    parse_header_block_fragment, resolve_request_head,
};
use smallvec::SmallVec;
use state::{
    ClientPrefaceState, ConnectionDrainState, H2ConnectionState, InboundStream, ReceiveState,
    RequestInputClose, RequestInputDeadline, RequestSpawnContext, StreamLifecycle,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;
use tokio::task::yield_now;
use tokio::time::{Instant as TokioInstant, sleep_until, timeout_at};
use writer::{
    GrantSendWindowError, H2WriterHandle, ResponseClose, WindowTarget, WriterState, init_writer,
};

use crate::async_util::{send_best_effort, send_with_backpressure, with_optional_timeout};
use crate::error::{ErrorExt, ErrorKind, H2CornError, H2Error};
use crate::h2_frame::{
    self, ErrorCode, FrameFlags, FrameHeader, FrameRead, FrameReadError, FrameType, PeerSettings,
    RawFrame, StreamId, WindowIncrement,
};
use crate::hpack::DecoderError;
use crate::http::body::RequestBodyProgress;
use crate::http::types::{RequestHead, ResponseHeaders};
use crate::proxy_protocol::read_h2_preface;
use crate::runtime::{ConnectionContext, ShutdownState, StreamInput};
use crate::sendfile::WriteTarget;

/// Explicit cooperative bound for a peer whose complete frames are already
/// buffered. Timers are selected first on every turn, and after this many
/// consecutive frames the connection yields to other Tokio tasks.
const MAX_CONSECUTIVE_PEER_FRAMES: usize = 32;
/// Replenish upload credit before a bandwidth-delay product drains it. A
/// custom smaller receive window cannot wait for this much credit or it would
/// deadlock after its first full window, so it refreshes at its full size.
const INPUT_WINDOW_UPDATE_THRESHOLD: u32 = 256 * 1024;

type StreamMap<T> = HashMap<StreamId, T, BuildNoHashHasher<StreamId>>;

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
    Frame(FrameRead),
    FrameLengthExceeded {
        stream_id: Option<StreamId>,
        error: H2Error,
    },
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

enum SettingsFrame {
    Ack,
    Update(PeerSettings),
}

enum PingFrame {
    Ack,
    Request([u8; 8]),
}

struct WindowUpdateFrame {
    stream_id: Option<StreamId>,
    increment: WindowIncrement,
}

struct ResetStreamFrame {
    stream_id: StreamId,
    error_code: ErrorCode,
}

struct PriorityFrame;

struct GoawayFrame;

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
    connection: &'a H2WriterHandle,
    spawn: RequestSpawnContext<'a>,
    last_client_stream_id: Option<StreamId>,
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
        match failure {
            H2PeerFailure::Reset {
                stream_id,
                error_code,
            } => self.reset_request_stream(stream_id, error_code).await,
            failure @ H2PeerFailure::Goaway { .. } => {
                apply_peer_failure(&mut self.writer, self.last_client_stream_id, failure).await
            },
        }
    }

    /// Apply the complete local side of a stream error. A wire RST alone is
    /// not a lifecycle transition: the inbound producer and application task
    /// must close too, including an app suspended away from receive().
    async fn reset_request_stream(
        &mut self,
        stream_id: StreamId,
        error_code: ErrorCode,
    ) -> Result<(), H2CornError> {
        let result = self.writer.reset_stream(stream_id, error_code).await;
        if let Some(mut stream) = self.remove_stream(stream_id) {
            stream.delivery.stop_with(StreamInput::Reset(error_code));
        }
        self.request_tasks.cancel(stream_id).await;
        result
    }
}

impl<R, W> H2ConnectionState<R, W> {
    /// Per-frame flow-control arithmetic: the receive-credit thresholds that
    /// trigger a window update, and how the frame's payload splits into
    /// delivered and padding bytes.
    fn data_accounting(&self, payload_len: usize, flow_control_len: u32) -> DataAccounting {
        let http2 = &self.context.config.http2;
        let data_len = u32::try_from(payload_len).expect("DATA frame length fits u32");
        DataAccounting {
            connection_window_threshold: input_window_update_threshold(
                http2.initial_connection_window_size,
            ),
            stream_window_threshold: input_window_update_threshold(
                http2.initial_stream_window_size,
            ),
            data_len,
            discarded_len: flow_control_len
                .checked_sub(data_len)
                .expect("parsed DATA payload cannot exceed its flow-control length"),
        }
    }

    /// Validate, account, and deliver one DATA frame.
    ///
    /// An ordinary frame touches the stream map once: its window update is
    /// taken while the entry is already borrowed. Only the frame that ends the
    /// stream looks again, because whether a window update may still be sent
    /// depends on state `finish_request_input` has yet to change.
    ///
    /// Window updates and terminal actions are returned, never sent while the
    /// map entry is borrowed.
    /// Give back the connection credit a frame consumed, and report any window
    /// update that releasing it unblocks.
    ///
    /// Used wherever a frame is accounted for against the connection but never
    /// delivered to a stream — a closed stream, or one that just overran its
    /// own window. Without this the connection window would shrink by every
    /// such frame until it stalled.
    fn release_connection_credit(
        &mut self,
        flow_control_len: u32,
        threshold: u32,
    ) -> Option<WindowIncrement> {
        self.connection_window.release(flow_control_len);
        self.connection_window.take_update(threshold)
    }

    fn receive_data_frame(
        &mut self,
        stream_id: StreamId,
        data: Bytes,
        flow_control_len: u32,
        end_stream: bool,
    ) -> DataIngress {
        let DataAccounting {
            connection_window_threshold,
            stream_window_threshold,
            data_len,
            discarded_len,
        } = self.data_accounting(data.len(), flow_control_len);

        if self.connection_window.receive(flow_control_len).is_err() {
            return DataIngress::FlowControlViolation;
        }

        let Some(stream) = self.streams.get_mut(&stream_id) else {
            return if self.receive_state(stream_id).is_idle() {
                DataIngress::IdleStream
            } else {
                DataIngress::StreamClosed {
                    connection_update: self
                        .release_connection_credit(flow_control_len, connection_window_threshold),
                }
            };
        };
        if stream.state.request_is_closed() {
            self.request_deadlines
                .cancel(state::RequestInputDeadlineKey::body(stream_id));
            self.remove_stream(stream_id);
            return DataIngress::StreamClosed {
                connection_update: self
                    .release_connection_credit(flow_control_len, connection_window_threshold),
            };
        }
        if stream.receive_window.receive(flow_control_len).is_err() {
            // A stream that overruns its own window is a stream error
            // (RFC 9113 §5.2); only the connection window is a connection
            // error. The connection credit this frame consumed is released
            // again, or every reset would shrink the connection window.
            return DataIngress::StreamFlowControlViolation {
                stream_id,
                connection_update: self
                    .release_connection_credit(flow_control_len, connection_window_threshold),
            };
        }
        if stream.counts_toward_read_timeout
            && let Some(timeout) = self.context.config.timeout_request_body_idle
        {
            self.request_deadlines.schedule(
                state::RequestInputDeadlineKey::body(stream_id),
                TokioInstant::now() + timeout,
            );
        }

        if discarded_len != 0 {
            self.connection_window.release(discarded_len);
            stream.receive_window.release(discarded_len);
        }

        // Taken here for an ordinary frame: the request cannot be closed at
        // this point (a closed one returned above), so the guard the
        // end-of-stream path needs cannot apply.
        let open_stream_update = (!end_stream)
            .then(|| stream.receive_window.take_update(stream_window_threshold))
            .flatten();

        if !data.is_empty() {
            match stream.body.record_chunk(data.len() as u64) {
                RequestBodyProgress::Continue => {},
                RequestBodyProgress::SizeLimitExceeded
                | RequestBodyProgress::ContentLengthExceeded => {
                    self.connection_window.release(data_len);
                    stream.receive_window.release(data_len);
                    stream
                        .delivery
                        .stop_with(StreamInput::Reset(ErrorCode::PROTOCOL_ERROR));
                    self.request_deadlines
                        .cancel(state::RequestInputDeadlineKey::body(stream_id));
                    self.remove_stream(stream_id);
                    return DataIngress::BodyLimitExceeded {
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
                self.remove_stream(stream_id);
                DataEnd::ContentLengthMismatch
            },
            Some(RequestInputClose::Closed) | None => DataEnd::Settled,
        });

        let connection_update = self
            .connection_window
            .take_update(connection_window_threshold);
        let stream_update = if end_stream {
            self.streams.get_mut(&stream_id).and_then(|stream| {
                (!stream.state.request_is_closed())
                    .then(|| stream.receive_window.take_update(stream_window_threshold))
                    .flatten()
            })
        } else {
            open_stream_update
        };

        DataIngress::Accepted {
            connection_update,
            stream_update,
            end,
        }
    }
}

struct DataAccounting {
    connection_window_threshold: u32,
    stream_window_threshold: u32,
    data_len: u32,
    discarded_len: u32,
}

/// Outcome of ingesting one DATA frame. Nothing is written while the stream
/// map is borrowed: the caller performs the async writer/app actions this
/// names once the borrow has ended.
enum DataIngress {
    Accepted {
        connection_update: Option<WindowIncrement>,
        stream_update: Option<WindowIncrement>,
        end: Option<DataEnd>,
    },
    /// The stream overran its own receive window: reset that stream only.
    StreamFlowControlViolation {
        stream_id: StreamId,
        connection_update: Option<WindowIncrement>,
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

fn input_window_update_threshold(initial_window: NonZeroU32) -> u32 {
    initial_window.get().min(INPUT_WINDOW_UPDATE_THRESHOLD)
}

/// Per-connection stream-keyed containers start empty and grow with the
/// streams that actually arrive.
///
/// Reserving up front for even a handful of streams measured **17 KB of
/// resident memory per idle connection** — 34 KB became 17 KB across 2,000
/// idle HTTP/2 connections — because the entries are large (a pending-write
/// record alone is 512 B) and a reservation for 8 rounds up to 14 slots. The
/// concurrency cap is not the working set: most connections carry one or two
/// streams at a time, and growing on first use measured free (33,341 vs
/// 33,351 instructions/request, inside a 34-instruction noise floor).
fn new_stream_map<T>() -> StreamMap<T> {
    HashMap::with_hasher(BuildNoHashHasher::default())
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

    let connection_window_threshold =
        input_window_update_threshold(state.context.config.http2.initial_connection_window_size);
    let mut stream_updates = SmallVec::<[(StreamId, WindowIncrement); 8]>::new();
    let mut streams_with_credit = SmallVec::<[StreamId; 8]>::new();
    let mut ready_streams = SmallVec::<[StreamId; 8]>::new();
    let stream_window_threshold =
        input_window_update_threshold(state.context.config.http2.initial_stream_window_size);

    loop {
        state
            .input_flow
            .drain_into(&mut state.released_input_credits);
        for release in &state.released_input_credits {
            state.connection_window.release(release.bytes.get());
            let stream_id = release.stream_id;
            if let Some(stream) = state.streams.get_mut(&stream_id) {
                stream.receive_window.release(release.bytes.get());
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
                state.remove_stream(stream_id);
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
    shutdown: watch::Receiver<ShutdownState>,
    upgraded: UpgradedH2Request,
) -> impl Future<Output = Result<(), H2CornError>>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + WriteTarget,
{
    Box::pin(drive_h2_upgraded_connection(
        reader, writer, connection, shutdown, upgraded,
    ))
}

async fn drive_h2_upgraded_connection<R, W>(
    reader: R,
    writer: W,
    connection: ConnectionContext,
    shutdown: watch::Receiver<ShutdownState>,
    upgraded: UpgradedH2Request,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + WriteTarget,
{
    let mut reader = h2_frame::BufferedConnectionReader::with_buffer(reader, upgraded.buffer);
    with_optional_timeout(
        connection.config.timeout_handshake,
        read_h2_preface(&mut reader),
    )
    .await
    .map_err(|_| H2Error::Http2HandshakeTimedOut)??;
    let peer_settings = upgraded.peer_settings;
    let mut connection =
        start_h2_connection(reader, writer, connection, shutdown, Some(peer_settings)).await?;
    seed_upgraded_request(&mut connection, upgraded.request, upgraded.body).await?;
    run_h2_connection(Box::new(connection)).await
}

pub(crate) fn serve_connection<R, W>(
    reader: h2_frame::BufferedConnectionReader<R>,
    writer: W,
    context: ConnectionContext,
    shutdown: watch::Receiver<ShutdownState>,
) -> impl Future<Output = Result<(), H2CornError>>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + WriteTarget,
{
    // Store the outer handshake/driver future once per connection.
    // `Pin<Box<F>>` retains static dispatch while preventing the complete H2
    // state machine from being copied into each generic caller and Tokio task.
    Box::pin(drive_connection(reader, writer, context, shutdown))
}

async fn drive_connection<R, W>(
    reader: h2_frame::BufferedConnectionReader<R>,
    writer: W,
    context: ConnectionContext,
    shutdown: watch::Receiver<ShutdownState>,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + WriteTarget,
{
    let state = start_h2_connection(reader, writer, context, shutdown, None).await?;
    run_h2_connection(Box::new(state)).await
}

async fn start_h2_connection<R, W>(
    reader: h2_frame::BufferedConnectionReader<R>,
    writer: W,
    context: ConnectionContext,
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
            shutdown: &connection.shutdown,
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

/// Admit one HEADERS/CONTINUATION fragment under a single owner for the whole
/// block. Nonterminal fragments refresh the header deadline; `END_HEADERS`
/// cancels it and dispatches by the target fixed at the first fragment.
async fn accept_header_fragment<R, W>(
    state: &mut H2ConnectionState<R, W>,
    stream_id: StreamId,
    target: HeaderBlockTarget,
    fragment: HeaderBlockFragment,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: WriteTarget,
{
    if state.header_block_too_large(fragment.block.len()) {
        state.peer_failure(header_block_too_large()).await?;
        return Ok(());
    }
    if fragment.end_headers {
        finish_header_block(state, stream_id, target, fragment.block).await
    } else {
        state.pending_headers = Some(PendingHeaderBlock {
            stream_id,
            target,
            block: fragment.block.into(),
        });
        state.refresh_header_deadline(stream_id);
        Ok(())
    }
}

async fn finish_header_block<R, W>(
    state: &mut H2ConnectionState<R, W>,
    stream_id: StreamId,
    target: HeaderBlockTarget,
    block: Bytes,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: WriteTarget,
{
    match target {
        HeaderBlockTarget::RequestHead { end_stream } => {
            let header_limits = state.header_limits();
            start_request_stream_from_block(
                RequestStartContext {
                    writer: &mut state.writer,
                    connection: &state.connection,
                    spawn: RequestSpawnContext {
                        streams: &mut state.streams,
                        connection: &state.context,
                        tasks: &mut state.request_tasks,
                        shutdown: &state.shutdown,
                    },
                    last_client_stream_id: state.last_client_stream_id,
                },
                stream_id,
                end_stream,
                decode_request_head(
                    &mut state.decoder,
                    block,
                    header_limits,
                    state.context.security.is_tls(),
                ),
            )
            .await?;
            state.refresh_body_deadline(stream_id);
        },
        HeaderBlockTarget::Trailers { end_stream } => {
            let header_limits = state.header_limits();
            match decode_trailer_block(&mut state.decoder, block, header_limits) {
                Ok(()) => {
                    if end_stream {
                        match state.finish_request_input(stream_id) {
                            Some(RequestInputClose::ContentLengthMismatch) => {
                                state
                                    .reset_request_stream(stream_id, ErrorCode::PROTOCOL_ERROR)
                                    .await?;
                            },
                            Some(RequestInputClose::Closed) | None => {},
                        }
                    } else {
                        // END_STREAM is required on trailers, but only after the
                        // compressed block has been fully applied to the table.
                        state
                            .reset_request_stream(stream_id, ErrorCode::PROTOCOL_ERROR)
                            .await?;
                    }
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
        },
        HeaderBlockTarget::RejectedNew { error_code } => {
            if let Err(err) = decode_discarded_header_block(&mut state.decoder, block) {
                state
                    .peer_failure(discarded_block_compression_failure(err))
                    .await?;
                return Ok(());
            }
            state.writer.reset_stream(stream_id, error_code).await?;
        },
        HeaderBlockTarget::RejectedTracked { error_code } => {
            let decode_err = decode_discarded_header_block(&mut state.decoder, block).err();
            // Always finalize the application owner; a subsequent compression
            // failure still tears the connection down after that cleanup.
            state.reset_request_stream(stream_id, error_code).await?;
            if let Some(err) = decode_err {
                state
                    .peer_failure(discarded_block_compression_failure(err))
                    .await?;
            }
        },
    }
    Ok(())
}

fn discarded_block_compression_failure(err: DecoderError) -> H2PeerFailure {
    let error = match err {
        DecoderError::NeedMore(_) => H2Error::IncompleteHpackHeaderBlock,
        other => H2Error::HpackDecode {
            detail: format!("{other:?}").into_boxed_str(),
        },
    };
    H2PeerFailure::connection(ErrorCode::COMPRESSION_ERROR, error)
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

    // 1. Parse HEADERS framing and priority fields.
    let fragment = match parse_header_block_fragment(frame) {
        Ok(fragment) => fragment,
        Err(err) => {
            state.peer_failure(H2PeerFailure::protocol(err)).await?;
            return Ok(());
        },
    };

    // 2. Classify the target for this entire block.
    if receive_state == ReceiveState::Idle {
        state.last_client_stream_id = Some(stream_id);
    }
    let target = match receive_state {
        ReceiveState::Open | ReceiveState::ResponseClosed => HeaderBlockTarget::Trailers {
            end_stream: fragment.end_stream,
        },
        ReceiveState::RequestClosed => HeaderBlockTarget::RejectedTracked {
            error_code: ErrorCode::STREAM_CLOSED,
        },
        ReceiveState::Closed => {
            state
                .peer_failure(H2PeerFailure::connection(
                    ErrorCode::STREAM_CLOSED,
                    H2Error::HeadersOnClosedStream,
                ))
                .await?;
            return Ok(());
        },
        ReceiveState::Idle => {
            if state.should_refuse_new_streams() {
                HeaderBlockTarget::RejectedNew {
                    error_code: ErrorCode::REFUSED_STREAM,
                }
            } else {
                // A task can complete while this connection is asleep waiting
                // for the next frame. Reap again at the admission boundary
                // before enforcing the independent server-work budget.
                state.request_tasks.reap_completed();
                if state.protocol_active_stream_count()
                    >= state.context.config.http2.max_concurrent_streams.get() as usize
                    || state.request_tasks.is_at_capacity()
                {
                    HeaderBlockTarget::RejectedNew {
                        error_code: ErrorCode::REFUSED_STREAM,
                    }
                } else {
                    HeaderBlockTarget::RequestHead {
                        end_stream: fragment.end_stream,
                    }
                }
            }
        },
    };

    // 3–6. Admit/buffer, refresh deadline, and finish on END_HEADERS.
    accept_header_fragment(state, stream_id, target, fragment).await
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
    // Every CONTINUATION must use the pending stream.
    if frame.header.stream_id != Some(pending.stream_id) {
        state.cancel_header_deadline(pending.stream_id);
        state
            .peer_failure(H2PeerFailure::protocol(
                H2Error::ContinuationStreamIdMismatch,
            ))
            .await?;
        return Ok(());
    }

    pending.block.extend_from_slice(frame.payload.as_ref());
    if state.header_block_too_large(pending.block.len()) {
        state.cancel_header_deadline(pending.stream_id);
        state.peer_failure(header_block_too_large()).await?;
        return Ok(());
    }
    if frame.header.flags.contains(FrameFlags::END_HEADERS) {
        state.cancel_header_deadline(pending.stream_id);
        finish_header_block(
            state,
            pending.stream_id,
            pending.target,
            pending.block.freeze(),
        )
        .await?;
    } else {
        // Nonterminal fragment: one deadline refresh for the open block.
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
        DataIngress::StreamFlowControlViolation {
            stream_id,
            connection_update,
        } => {
            if let Some(increment) = connection_update {
                state
                    .writer
                    .send_window_update(WindowTarget::Connection, increment)
                    .await?;
            }
            state
                .reset_request_stream(stream_id, ErrorCode::FLOW_CONTROL_ERROR)
                .await
        },
        DataIngress::BodyLimitExceeded { connection_update } => {
            if let Some(increment) = connection_update {
                state
                    .writer
                    .send_window_update(WindowTarget::Connection, increment)
                    .await?;
            }
            state
                .peer_failure(H2PeerFailure::stream(stream_id, ErrorCode::PROTOCOL_ERROR))
                .await?;
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
                        .await?;
                    Ok(())
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
    let result = drive_h2_connection(&mut state).await;
    // Every ordinary exit closes application input before cancelling tasks.
    // `Drop` remains only the panic/abort backstop.
    state.streams.clear();
    state.request_tasks.cancel_all().await;
    match result {
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
        if matches!(&event, IngestEvent::Frame(_)) {
            consecutive_peer_frames += 1;
        } else {
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
            IngestEvent::FrameLengthExceeded { stream_id, error } => {
                if let Some(stream_id) = stream_id {
                    state
                        .peer_failure(H2PeerFailure::stream(
                            stream_id,
                            ErrorCode::FRAME_SIZE_ERROR,
                        ))
                        .await?;
                } else {
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
                }
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
                            .reset_request_stream(stream_id, ErrorCode::CANCEL)
                            .await?;
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

/// Refusing to decode a field block ends the connection, not just the stream.
///
/// HPACK's dynamic table is connection-wide: a block that is never fed to the
/// decoder leaves it behind the peer's encoder, and the *next* perfectly valid
/// request on that connection then fails to decode. Resetting only the stream
/// looked tidier and produced exactly that — a `GOAWAY(COMPRESSION_ERROR)` on
/// an innocent later request, with the application never seeing either one.
/// RFC 9113 §4.3 puts the compression context at connection scope for this
/// reason.
fn header_block_too_large() -> H2PeerFailure {
    H2PeerFailure::connection(ErrorCode::COMPRESSION_ERROR, H2Error::HeaderBlockTooLarge)
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
            let Some(pending) = state.pending_headers.take() else {
                return Ok(());
            };
            if pending.stream_id != stream_id {
                state.pending_headers = Some(pending);
                return Ok(());
            }
            // Tracked streams still own an application task; wire RST alone
            // would leave it alive. New/refused blocks have no owner yet.
            match pending.target {
                HeaderBlockTarget::Trailers { .. } | HeaderBlockTarget::RejectedTracked { .. } => {
                    state
                        .reset_request_stream(stream_id, ErrorCode::CANCEL)
                        .await?;
                },
                HeaderBlockTarget::RequestHead { .. } | HeaderBlockTarget::RejectedNew { .. } => {
                    state
                        .writer
                        .reset_stream(stream_id, ErrorCode::CANCEL)
                        .await?;
                },
            }
        },
        RequestInputDeadline::Body(stream_id, _) => {
            if let Some(stream) = state.streams.get_mut(&stream_id)
                && !stream.state.request_is_closed()
            {
                state
                    .reset_request_stream(stream_id, ErrorCode::CANCEL)
                    .await?;
            }
        },
    }
    Ok(())
}

fn map_frame_ingest_result(
    frame: Result<Option<FrameRead>, FrameReadError>,
) -> Result<IngestEvent, H2CornError> {
    match frame {
        Ok(Some(frame)) => Ok(IngestEvent::Frame(frame)),
        Ok(None) => Ok(IngestEvent::PeerClosed),
        Err(FrameReadError::DeclaredPayloadLength { stream_id, error }) => {
            Ok(IngestEvent::FrameLengthExceeded { stream_id, error })
        },
        Err(FrameReadError::Other(err)) => match err.into_kind() {
            ErrorKind::H2(
                error @ (H2Error::FrameLengthExceedsPeerMax { .. }
                | H2Error::SettingsAckPayloadNotEmpty
                | H2Error::SettingsPayloadLengthInvalid
                | H2Error::PingPayloadInvalidLength
                | H2Error::WindowUpdatePayloadInvalidLength
                | H2Error::RstStreamPayloadInvalidLength
                | H2Error::PriorityPayloadInvalidLength
                | H2Error::InvalidGoawayFrame),
            ) => Ok(IngestEvent::FrameLengthExceeded {
                stream_id: None,
                error,
            }),
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
    // Once the server has assigned a graceful-drain deadline, that deadline is
    // the sole bound for retained local request work. Keep-alive still bounds
    // ordinary idle connections and peer-initiated GOAWAY drains (which have
    // no server deadline).
    let keep_alive_deadline = (state.drain_state.deadline().is_none() && state.wire_is_idle())
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
    let has_request_tasks = state.request_tasks.active_count() != 0;
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
        () = state.request_tasks.wait_for_completion(), if has_request_tasks => Ok(IngestEvent::Continue),
        () = &mut outbound_notified => Ok(IngestEvent::Continue),
        () = &mut input_notified => Ok(IngestEvent::Continue),
        () = yield_now(), if continue_writing => Ok(IngestEvent::Continue),
        frame = state.reader.read_frame(state.local_max_frame_size.as_usize(), is_handled_frame) => map_frame_ingest_result(frame),
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
fn validate_settings_frame(frame: &RawFrame) -> Result<SettingsFrame, H2PeerFailure> {
    if frame.header.stream_id.is_some() {
        return Err(H2PeerFailure::protocol(H2Error::SettingsMustUseStreamZero));
    }
    if frame.header.flags.contains(FrameFlags::ACK) {
        if !frame.payload.is_empty() {
            return Err(H2PeerFailure::frame_size(
                H2Error::SettingsAckPayloadNotEmpty,
            ));
        }
        return Ok(SettingsFrame::Ack);
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
        .map(SettingsFrame::Update)
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
        Ok(SettingsFrame::Ack) => Ok(()),
        Ok(SettingsFrame::Update(settings)) => {
            if state.writer.update_peer_settings(settings).is_err() {
                return state
                    .peer_failure(H2PeerFailure::connection(
                        ErrorCode::FLOW_CONTROL_ERROR,
                        H2Error::SettingsInitialWindowAdjustmentOverflow,
                    ))
                    .await;
            }
            state.writer.send_settings_ack().await
        },
    }
}

/// Wire-shape validation for `PING`; `Ok(None)` is a valid ACK.
fn validate_ping_frame(frame: &RawFrame) -> Result<PingFrame, H2PeerFailure> {
    if frame.header.stream_id.is_some() {
        return Err(H2PeerFailure::protocol(H2Error::PingMustUseStreamZero));
    }
    let Ok(payload) = <[u8; 8]>::try_from(frame.payload.as_ref()) else {
        return Err(H2PeerFailure::frame_size(H2Error::PingPayloadInvalidLength));
    };
    Ok(if frame.header.flags.contains(FrameFlags::ACK) {
        PingFrame::Ack
    } else {
        PingFrame::Request(payload)
    })
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
        Ok(PingFrame::Ack) => Ok(()),
        Ok(PingFrame::Request(payload)) => state.writer.ping_ack(payload).await,
    }
}

/// Wire-shape validation for `WINDOW_UPDATE`: length and non-zero increment.
fn validate_window_update_frame(frame: &RawFrame) -> Result<WindowUpdateFrame, H2PeerFailure> {
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
        |increment| {
            Ok(WindowUpdateFrame {
                stream_id,
                increment,
            })
        },
    )
}

async fn handle_window_update_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: RawFrame,
) -> Result<bool, H2CornError>
where
    W: WriteTarget,
{
    let WindowUpdateFrame {
        stream_id,
        increment,
    } = match validate_window_update_frame(&frame) {
        Ok(update) => update,
        Err(failure) => {
            state.peer_failure(failure).await?;
            return Ok(false);
        },
    };
    if let Some(stream_id) = stream_id {
        match state.receive_state(stream_id) {
            ReceiveState::Idle => {
                state
                    .peer_failure(H2PeerFailure::protocol(H2Error::WindowUpdateOnIdleStream))
                    .await?;
                return Ok(false);
            },
            // RFC 9113 §5.1 permits updates that raced with stream closure.
            // They have no send-window owner left, so ignoring them is the
            // only action that cannot recreate writer state.
            ReceiveState::ResponseClosed | ReceiveState::Closed => return Ok(false),
            ReceiveState::Open | ReceiveState::RequestClosed => {},
        }
        if let Err(GrantSendWindowError::Stream(stream_id)) = state
            .writer
            .grant_send_window(WindowTarget::Stream(stream_id), increment)
        {
            state
                .peer_failure(H2PeerFailure::stream(
                    stream_id,
                    ErrorCode::FLOW_CONTROL_ERROR,
                ))
                .await?;
        }
        Ok(false)
    } else {
        match state
            .writer
            .grant_send_window(WindowTarget::Connection, increment)
        {
            Ok(()) => Ok(false),
            Err(GrantSendWindowError::Connection) => state
                .peer_failure(H2PeerFailure::connection(
                    ErrorCode::FLOW_CONTROL_ERROR,
                    H2Error::SendFlowControlWindowOverflow,
                ))
                .await
                .map(|()| true),
            Err(GrantSendWindowError::Stream(_)) => {
                unreachable!("connection window update cannot name a stream")
            },
        }
    }
}

/// Wire-shape validation for `RST_STREAM`: non-zero stream and 4-byte payload.
fn validate_rst_stream_frame(frame: &RawFrame) -> Result<ResetStreamFrame, H2PeerFailure> {
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
    Ok(ResetStreamFrame {
        stream_id,
        error_code: ErrorCode::new(u32::from_be_bytes(raw)),
    })
}

async fn handle_rst_stream_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: RawFrame,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    let ResetStreamFrame {
        stream_id,
        error_code,
    } = match validate_rst_stream_frame(&frame) {
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
    if let Some(mut stream) = state.remove_stream(stream_id) {
        stream.delivery.stop_with(StreamInput::Reset(error_code));
    }
    state.request_tasks.cancel(stream_id).await;
    state.writer.peer_reset(stream_id).await
}

/// Wire-shape validation for `PRIORITY`.
///
/// RFC 9113 deprecates the frame and permits it in every stream state. Its
/// dependency tree is no longer interpreted, so only stream zero and the
/// fixed wire length remain meaningful here.
fn validate_priority_frame(frame: &RawFrame) -> Result<PriorityFrame, H2PeerFailure> {
    let Some(_stream_id) = frame.header.stream_id else {
        return Err(H2PeerFailure::protocol(
            H2Error::PriorityMustNotUseStreamZero,
        ));
    };
    if frame.payload.len() != 5 {
        return Err(H2PeerFailure::frame_size(
            H2Error::PriorityPayloadInvalidLength,
        ));
    }
    Ok(PriorityFrame)
}

async fn handle_priority_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: RawFrame,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    match validate_priority_frame(&frame) {
        Ok(PriorityFrame) => Ok(()),
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
    if frame.payload.len() <= state.local_max_frame_size.as_usize() {
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
    header: FrameHeader,
) -> Result<bool, H2CornError>
where
    W: WriteTarget,
{
    if state.pending_headers.is_some() && header.frame_type != FrameType::CONTINUATION {
        state
            .peer_failure(H2PeerFailure::protocol(H2Error::HeaderBlockInterrupted))
            .await?;
        return Ok(false);
    }
    if state.client_preface == ClientPrefaceState::Active {
        return Ok(true);
    }
    if header.frame_type != FrameType::SETTINGS {
        state
            .peer_failure(H2PeerFailure::protocol(
                H2Error::FirstClientFrameMustBeSettings,
            ))
            .await?;
        return Ok(false);
    }
    if header.flags.contains(FrameFlags::ACK) {
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

fn validate_goaway_frame(frame: &RawFrame) -> Result<GoawayFrame, H2PeerFailure> {
    if frame.header.stream_id.is_some() || frame.payload.len() < 8 {
        return Err(H2PeerFailure::protocol(H2Error::InvalidGoawayFrame));
    }
    Ok(GoawayFrame)
}

async fn handle_goaway_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: RawFrame,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    let GoawayFrame = match validate_goaway_frame(&frame) {
        Ok(frame) => frame,
        Err(failure) => return state.peer_failure(failure).await,
    };
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
    frame: FrameRead,
) -> Result<bool, H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + WriteTarget,
{
    match frame {
        FrameRead::Handled(frame) => advance_connection_with_handled_frame(state, frame).await,
        FrameRead::Ignored(header) => advance_connection_with_ignored_frame(state, header).await,
    }
}

async fn advance_connection_with_handled_frame<R, W>(
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
    if !validate_frame_order(state, frame.header).await? {
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
        _ => unreachable!("the reader discards frames outside the dispatcher vocabulary"),
    }
    Ok(false)
}

async fn advance_connection_with_ignored_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    header: FrameHeader,
) -> Result<bool, H2CornError>
where
    W: WriteTarget,
{
    apply_writer_response_closes(state).await;
    Ok(!validate_frame_order(state, header).await?)
}

/// The reader uses this exact dispatcher vocabulary to decide whether a
/// payload must be materialized. Add a type here before adding its dispatch
/// arm, otherwise an extension frame is intentionally consumed and ignored.
const fn is_handled_frame(frame_type: FrameType) -> bool {
    matches!(
        frame_type,
        FrameType::SETTINGS
            | FrameType::PING
            | FrameType::WINDOW_UPDATE
            | FrameType::RST_STREAM
            | FrameType::GOAWAY
            | FrameType::PRIORITY
            | FrameType::PUSH_PROMISE
            | FrameType::HEADERS
            | FrameType::CONTINUATION
            | FrameType::DATA
    )
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
    for (kind, stream_id) in state.writer.take_response_closes() {
        match kind {
            ResponseClose::Clean => {},
            ResponseClose::Abort => {
                state.writer.drop_ingress_stream(stream_id).await;
            },
        }
        state.apply_response_close(stream_id);
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

    const INITIAL_STREAM_WINDOW_SIZE: u32 = 8 << 20;
    const STREAM_WINDOW_UPDATE_THRESHOLD: u32 = INPUT_WINDOW_UPDATE_THRESHOLD;

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
        assert!(matches!(
            validate_settings_frame(&ack),
            Ok(SettingsFrame::Ack)
        ));
    }

    #[test]
    fn ping_validation_extracts_payload_and_skips_acks() {
        let ping = raw_frame(FrameType::PING, FrameFlags::EMPTY, None, &[7; 8]);
        assert!(matches!(
            validate_ping_frame(&ping),
            Ok(PingFrame::Request(payload)) if payload == [7; 8]
        ));

        let ack = raw_frame(FrameType::PING, FrameFlags::ACK, None, &[7; 8]);
        assert!(matches!(validate_ping_frame(&ack), Ok(PingFrame::Ack)));

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
        let Ok(WindowUpdateFrame {
            stream_id,
            increment,
        }) = validate_window_update_frame(&grant)
        else {
            panic!("valid window update is accepted");
        };
        assert_eq!(stream_id, StreamId::new(3));
        assert_eq!(increment.get(), 1);
    }

    #[test]
    fn priority_validation_accepts_self_dependency() {
        let mut payload = 3_u32.to_be_bytes().to_vec();
        payload.push(0);
        let self_dependent = raw_frame(
            FrameType::PRIORITY,
            FrameFlags::EMPTY,
            StreamId::new(3),
            &payload,
        );
        assert!(validate_priority_frame(&self_dependent).is_ok());

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
    fn receive_window_update_threshold_is_fixed_except_for_smaller_windows() {
        assert_eq!(
            input_window_update_threshold(NonZeroU32::new(INITIAL_STREAM_WINDOW_SIZE).unwrap()),
            INPUT_WINDOW_UPDATE_THRESHOLD
        );
        assert_eq!(
            input_window_update_threshold(NonZeroU32::new(0xFFFF).unwrap()),
            0xFFFF
        );
    }
}
