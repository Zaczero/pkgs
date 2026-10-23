mod deadline;
mod http;
mod request;
mod state;
mod websocket;
mod writer;

use std::collections::HashMap;
use std::future::pending;
use std::hash::{BuildHasherDefault, Hasher};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;
use tokio::task::yield_now;
use tokio::time::{Instant as TokioInstant, sleep_until, timeout_at};

use crate::async_util::with_optional_timeout;
use crate::error::{ErrorExt as _, ErrorKind, H2CornError, H2Error};
use crate::h2::http::spawn_request_stream;
use crate::h2::request::{
    HeaderBlockFragment, HeaderBlockTarget, PendingHeaderBlock, RequestHeadError,
    decode_discarded_header_block, decode_request_head, decode_trailer_block,
    parse_header_block_fragment, resolve_request_head,
};
use crate::h2::state::{
    ClientPrefaceState, ConnectionDrainState, H2ConnectionState, InboundStream, RequestInputClose,
    RequestInputDeadline, RequestSpawnContext, StreamLifecycle,
};
use crate::h2::writer::{
    H2WriterHandle, ResponseClose, WindowOverflow, WindowTarget, WriterState, init_writer,
};
use crate::h2_frame::{
    self, ErrorCode, FrameFlags, FrameHeader, FrameRead, FrameReadError, FrameType,
    HandledFrameKind, PeerSettings, RawFrame, StreamId, WindowIncrement,
};
use crate::hpack::DecoderError;
use crate::http::body::RequestBodyProgress;
use crate::http::policy::ResolvedRequestPolicy;
use crate::http::types::{HttpStatusCode, RequestHead, ResponseHeaders};
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

type StreamMap<T> = HashMap<StreamId, T, BuildHasherDefault<StreamIdHasher>>;

/// Hasher for peer-chosen HTTP/2 stream identifiers.
///
/// An identity hash is tempting for a `u32` key, but the peer picks these:
/// hashbrown takes its 7-bit control byte from the *top* of the hash, so an
/// identity hash leaves it permanently zero and every SIMD group match hits,
/// and a client numbering its streams `1, 65537, 131073, ...` lands every entry
/// in one bucket chain. `max_concurrent_streams` bounds the damage, so this is
/// an amplifier rather than a vulnerability -- but the key is not trusted, and
/// one multiply restores both the control byte and the bucket spread.
#[derive(Default)]
struct StreamIdHasher(u64);

impl Hasher for StreamIdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("stream-map keys hash themselves as a single integer")
    }

    fn write_u32(&mut self, value: u32) {
        self.0 = u64::from(value).wrapping_mul(0x9E37_79B9_7F4A_7C15);
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

enum IngestEvent {
    Continue,
    ShutdownChanged,
    Frame(FrameRead),
    FrameLengthExceeded {
        reset_stream: Option<StreamId>,
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
    WriteStall(TokioInstant),
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

impl ConnectionDeadline {
    const fn instant(self) -> TokioInstant {
        match self {
            Self::Drain(instant)
            | Self::KeepAlive(instant)
            | Self::ResponseStall(instant)
            | Self::WriteStall(instant) => instant,
            Self::RequestInput(deadline) => deadline.instant(),
        }
    }
}

pub(crate) struct UpgradedH2Request {
    pub buffer: BytesMut,
    pub request: RequestHead,
    pub body: Bytes,
    pub policy: ResolvedRequestPolicy,
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
    fn peer_failure(&mut self, failure: H2PeerFailure) -> Result<(), H2CornError> {
        match failure {
            H2PeerFailure::Reset {
                stream_id,
                error_code,
            } => self.reset_request_stream(stream_id, error_code),
            failure @ H2PeerFailure::Goaway { .. } => {
                apply_peer_failure(&mut self.writer, self.last_client_stream_id, failure)
            },
        }
    }

    /// Apply the complete local side of a stream error. A wire RST alone is
    /// not a lifecycle transition: the inbound producer and application task
    /// must close too, including an app suspended away from receive().
    fn reset_request_stream(
        &mut self,
        stream_id: StreamId,
        error_code: ErrorCode,
    ) -> Result<(), H2CornError> {
        let result = self.writer.reset_stream(stream_id, error_code);
        if let Some(mut stream) = self.remove_stream(stream_id) {
            stream.delivery.stop_with(StreamInput::Reset(error_code));
        }
        self.request_tasks.cancel(stream_id);
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
            return if self.receive_state(stream_id).is_none() {
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
        if discarded_len != 0 {
            self.connection_window.release(discarded_len);
            if !stream.delivery.is_stopped() {
                stream.receive_window.release(discarded_len);
            }
        }

        // Taken here for an ordinary frame: the request cannot be closed at
        // this point (a closed one returned above), so the guard the
        // end-of-stream path needs cannot apply.
        let open_stream_update = (!end_stream && !stream.delivery.is_stopped())
            .then(|| stream.receive_window.take_update(stream_window_threshold))
            .flatten();

        if !data.is_empty() {
            if matches!(
                stream.body.record_chunk(data.len() as u64),
                RequestBodyProgress::SizeLimitExceeded | RequestBodyProgress::ContentLengthExceeded
            ) {
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
            }
            if stream.is_http
                && !stream.delivery.is_stopped()
                && let Some(timeout) = stream.timeout_request_body_idle
            {
                self.request_deadlines.reschedule_after(
                    state::RequestInputDeadlineKey::body(stream_id),
                    TokioInstant::now(),
                    timeout,
                );
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
            self.streams
                .get_mut(&stream_id)
                .filter(|stream| !stream.state.request_is_closed())
                .and_then(|stream| stream.receive_window.take_update(stream_window_threshold))
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
    HashMap::with_hasher(BuildHasherDefault::default())
}

fn flush_released_input_credits<R, W>(
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

    {
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
        if stream.state.request_is_closed() || stream.delivery.is_stopped() {
            continue;
        }
        if let Some(increment) = stream.receive_window.take_update(stream_window_threshold) {
            stream_updates.push((stream_id, increment));
        }
    }

    if let Some(increment) = connection_update {
        state
            .writer
            .send_window_update(WindowTarget::Connection, increment)?;
    }

    for (stream_id, increment) in stream_updates {
        state
            .writer
            .send_window_update(WindowTarget::Stream(stream_id), increment)?;
    }

    Ok(())
}

fn apply_peer_failure<W>(
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
            writer.goaway(last_stream_id, error_code, message, true)?;
            Err(error)
        },
        H2PeerFailure::Reset {
            stream_id,
            error_code,
        } => {
            writer.reset_stream(stream_id, error_code)?;
            Ok(())
        },
    }
}

fn begin_graceful_shutdown<W>(
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
    *drain_state = ConnectionDrainState::ServerDraining {
        deadline: Instant::now().checked_add(timeout_graceful_shutdown),
    };
    writer.goaway(last_stream_id, ErrorCode::NO_ERROR, Vec::new(), false)?;
    Ok(())
}

fn reject_request_head<W>(
    ctx: &mut RequestStartContext<'_, W>,
    stream_id: StreamId,
    end_stream: bool,
    content_length: Option<u64>,
    status: HttpStatusCode,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    ctx.spawn.streams.insert(
        stream_id,
        InboundStream::new(
            None,
            true,
            end_stream,
            content_length,
            None,
            ResolvedRequestPolicy::default(),
            ctx.spawn
                .connection
                .config
                .http2
                .initial_stream_window_size
                .get(),
        ),
    );
    ctx.writer
        .send_headers(stream_id, status, ResponseHeaders::new(), true)?;
    Ok(())
}

fn start_request_stream_from_block<W>(
    mut ctx: RequestStartContext<'_, W>,
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
            return reject_request_head(&mut ctx, stream_id, end_stream, None, status);
        },
        Err(RequestHeadError::Connection { error_code, error }) => {
            apply_peer_failure(
                ctx.writer,
                ctx.last_client_stream_id,
                H2PeerFailure::connection(error_code, error),
            )?;
            return Ok(());
        },
        Err(RequestHeadError::Stream { error_code, error }) => {
            crate::server::report_request_failure(
                ctx.spawn.connection.config.log_format,
                Err(error.into()),
            );
            apply_peer_failure(
                ctx.writer,
                ctx.last_client_stream_id,
                H2PeerFailure::stream(stream_id, error_code),
            )?;
            return Ok(());
        },
    };

    let policy = match ctx
        .spawn
        .connection
        .config
        .request_policies
        .resolve(&request)
    {
        Ok(policy) => policy,
        Err(status) => {
            return reject_request_head(
                &mut ctx,
                stream_id,
                end_stream,
                request.content_length(),
                status,
            );
        },
    };
    ctx.writer
        .register_stream(stream_id, policy.h2_timeout_response_stall);
    if let Some(rejected) = spawn_request_stream(
        stream_id,
        request,
        end_stream,
        policy,
        ctx.connection,
        ctx.spawn,
    ) {
        ctx.writer
            .send_headers(stream_id, rejected.status, rejected.headers, true)?;
    }
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
    seed_upgraded_request(
        &mut connection,
        upgraded.request,
        upgraded.body,
        upgraded.policy,
    )?;
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
        ConnectionDrainState::ServerDraining {
            deadline: Instant::now().checked_add(context.config.timeout_graceful_shutdown),
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

fn seed_upgraded_request<R, W>(
    connection: &mut H2ConnectionState<R, W>,
    request: RequestHead,
    body: Bytes,
    policy: ResolvedRequestPolicy,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    let stream_id = StreamId::new(1).expect("value is non-zero");
    connection
        .writer
        .register_stream(stream_id, policy.h2_timeout_response_stall);
    if let Some(rejected) = spawn_request_stream(
        stream_id,
        request,
        body.is_empty(),
        policy,
        &connection.connection,
        RequestSpawnContext {
            streams: &mut connection.streams,
            connection: &connection.context,
            tasks: &mut connection.request_tasks,
            shutdown: &connection.shutdown,
        },
    ) {
        connection
            .writer
            .send_headers(stream_id, rejected.status, rejected.headers, true)?;
    }
    if !body.is_empty()
        && let Some(stream) = connection.streams.get_mut(&stream_id)
    {
        let progress = stream.body.record_chunk(body.len() as u64);
        if matches!(
            progress,
            RequestBodyProgress::SizeLimitExceeded | RequestBodyProgress::ContentLengthExceeded
        ) {
            connection.reset_request_stream(stream_id, ErrorCode::PROTOCOL_ERROR)?;
        } else {
            stream.delivery.push(StreamInput::data(body));
            if matches!(
                connection.finish_request_input(stream_id),
                Some(RequestInputClose::ContentLengthMismatch)
            ) {
                connection.reset_request_stream(stream_id, ErrorCode::PROTOCOL_ERROR)?;
            }
        }
    }
    connection.refresh_body_deadline(stream_id);
    connection.last_client_stream_id = Some(stream_id);
    Ok(())
}

/// Admit one HEADERS/CONTINUATION fragment under a single owner for the whole
/// block. Nonterminal fragments refresh the header deadline; `END_HEADERS`
/// cancels it and dispatches by the target fixed at the first fragment.
fn accept_header_fragment<R, W>(
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
        state.peer_failure(header_block_too_large())?;
        return Ok(());
    }
    if fragment.end_headers {
        finish_header_block(state, stream_id, target, fragment.block)
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

fn finish_header_block<R, W>(
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
                    state.context.config.proxy.trust_headers,
                ),
            )?;
            state.refresh_body_deadline(stream_id);
        },
        HeaderBlockTarget::Trailers { end_stream } => {
            let header_limits = state.header_limits();
            match decode_trailer_block(&mut state.decoder, block, header_limits) {
                Ok(()) => {
                    if end_stream {
                        match state.finish_request_input(stream_id) {
                            Some(RequestInputClose::ContentLengthMismatch) => {
                                state.reset_request_stream(stream_id, ErrorCode::PROTOCOL_ERROR)?;
                            },
                            Some(RequestInputClose::Closed) | None => {},
                        }
                    } else {
                        // END_STREAM is required on trailers, but only after the
                        // compressed block has been fully applied to the table.
                        state.reset_request_stream(stream_id, ErrorCode::PROTOCOL_ERROR)?;
                    }
                },
                Err(RequestHeadError::Connection { error_code, error }) => {
                    state.peer_failure(H2PeerFailure::connection(error_code, error))?;
                },
                Err(RequestHeadError::Stream { error_code, error }) => {
                    crate::server::report_request_failure(
                        state.context.config.log_format,
                        Err(error.into()),
                    );
                    state.peer_failure(H2PeerFailure::stream(stream_id, error_code))?;
                },
                Err(RequestHeadError::Reject { .. }) => {
                    state.peer_failure(H2PeerFailure::stream(
                        stream_id,
                        ErrorCode::PROTOCOL_ERROR,
                    ))?;
                },
            }
        },
        HeaderBlockTarget::RejectedNew { error_code } => {
            if let Err(err) = decode_discarded_header_block(&mut state.decoder, block) {
                state.peer_failure(discarded_block_compression_failure(err))?;
                return Ok(());
            }
            state.writer.reset_stream(stream_id, error_code)?;
        },
        HeaderBlockTarget::RejectedTracked { error_code } => {
            let decode_err = decode_discarded_header_block(&mut state.decoder, block).err();
            // Always finalize the application owner; a subsequent compression
            // failure still tears the connection down after that cleanup.
            state.reset_request_stream(stream_id, error_code)?;
            if let Some(err) = decode_err {
                state.peer_failure(discarded_block_compression_failure(err))?;
            }
        },
    }
    Ok(())
}

fn discarded_block_compression_failure(err: DecoderError) -> H2PeerFailure {
    H2PeerFailure::connection(ErrorCode::COMPRESSION_ERROR, H2Error::from(err))
}

fn handle_headers_frame<R, W>(
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
            state.peer_failure(H2PeerFailure::protocol(H2Error::InvalidRequestStreamId))?;
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
        state.peer_failure(H2PeerFailure::protocol(
            H2Error::ClientStreamIdsNotIncreasing,
        ))?;
        return Ok(());
    }

    // 1. Parse HEADERS framing and priority fields.
    let fragment = match parse_header_block_fragment(frame) {
        Ok(fragment) => fragment,
        Err(err) => {
            state.peer_failure(H2PeerFailure::protocol(err))?;
            return Ok(());
        },
    };

    // 2. Classify the target for this entire block.
    if receive_state.is_none() {
        state.last_client_stream_id = Some(stream_id);
    }
    let target = match receive_state {
        Some(StreamLifecycle::Open | StreamLifecycle::ResponseClosed) => {
            HeaderBlockTarget::Trailers {
                end_stream: fragment.end_stream,
            }
        },
        Some(StreamLifecycle::RequestClosed) => HeaderBlockTarget::RejectedTracked {
            error_code: ErrorCode::STREAM_CLOSED,
        },
        Some(StreamLifecycle::Closed) => {
            state.peer_failure(H2PeerFailure::connection(
                ErrorCode::STREAM_CLOSED,
                H2Error::HeadersOnClosedStream,
            ))?;
            return Ok(());
        },
        None => {
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
    accept_header_fragment(state, stream_id, target, fragment)
}

fn handle_continuation_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: &RawFrame,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: WriteTarget,
{
    let Some(mut pending) = state.pending_headers.take() else {
        state.peer_failure(H2PeerFailure::protocol(
            H2Error::UnexpectedContinuationFrame,
        ))?;
        return Ok(());
    };
    // Every CONTINUATION must use the pending stream.
    if frame.header.stream_id != Some(pending.stream_id) {
        state.cancel_header_deadline(pending.stream_id);
        state.peer_failure(H2PeerFailure::protocol(
            H2Error::ContinuationStreamIdMismatch,
        ))?;
        return Ok(());
    }

    pending.block.extend_from_slice(frame.payload.as_ref());
    if state.header_block_too_large(pending.block.len()) {
        state.cancel_header_deadline(pending.stream_id);
        state.peer_failure(header_block_too_large())?;
        return Ok(());
    }
    if frame.header.flags.contains(FrameFlags::END_HEADERS) {
        state.cancel_header_deadline(pending.stream_id);
        finish_header_block(
            state,
            pending.stream_id,
            pending.target,
            pending.block.freeze(),
        )?;
    } else {
        // Nonterminal fragment: one deadline refresh for the open block.
        state.refresh_header_deadline(pending.stream_id);
        state.pending_headers = Some(pending);
    }

    Ok(())
}

fn handle_data_frame<R, W>(
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
            state.peer_failure(H2PeerFailure::protocol(err))?;
            return Ok(());
        },
    };
    let Some(stream_id) = header.stream_id else {
        state.peer_failure(H2PeerFailure::protocol(H2Error::DataMustNotUseStreamZero))?;
        return Ok(());
    };

    match state.receive_data_frame(stream_id, data, flow_control_len, end_stream) {
        DataIngress::IdleStream => {
            state.peer_failure(H2PeerFailure::protocol(H2Error::DataOnIdleStream))
        },
        DataIngress::StreamClosed { connection_update } => {
            if let Some(increment) = connection_update {
                state
                    .writer
                    .send_window_update(WindowTarget::Connection, increment)?;
            }
            state.peer_failure(H2PeerFailure::stream(stream_id, ErrorCode::STREAM_CLOSED))
        },
        DataIngress::FlowControlViolation => state.peer_failure(H2PeerFailure::connection(
            ErrorCode::FLOW_CONTROL_ERROR,
            H2Error::ReceiveFlowControlWindowUnderflow,
        )),
        DataIngress::StreamFlowControlViolation {
            stream_id,
            connection_update,
        } => {
            if let Some(increment) = connection_update {
                state
                    .writer
                    .send_window_update(WindowTarget::Connection, increment)?;
            }
            state.reset_request_stream(stream_id, ErrorCode::FLOW_CONTROL_ERROR)
        },
        DataIngress::BodyLimitExceeded { connection_update } => {
            if let Some(increment) = connection_update {
                state
                    .writer
                    .send_window_update(WindowTarget::Connection, increment)?;
            }
            state.peer_failure(H2PeerFailure::stream(stream_id, ErrorCode::PROTOCOL_ERROR))?;
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
                    .send_window_update(WindowTarget::Connection, increment)?;
            }
            if let Some(increment) = stream_update {
                state
                    .writer
                    .send_window_update(WindowTarget::Stream(stream_id), increment)?;
            }
            match end {
                None | Some(DataEnd::Settled) => Ok(()),
                Some(DataEnd::ContentLengthMismatch) => {
                    state.peer_failure(H2PeerFailure::stream(
                        stream_id,
                        ErrorCode::PROTOCOL_ERROR,
                    ))?;
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
    state.request_tasks.begin_cancel_all();
    let result = match result {
        Ok(()) => {
            let deadline = state.drain_state.server_deadline().map_or_else(
                || {
                    TokioInstant::now()
                        .checked_add(state.context.config.timeout_graceful_shutdown)
                        .expect("configured shutdown duration fits the clock")
                },
                TokioInstant::from_std,
            );

            timeout_at(deadline, async {
                state.writer.goaway(
                    state.last_client_stream_id,
                    ErrorCode::NO_ERROR,
                    Vec::new(),
                    true,
                )?;
                loop {
                    let active = state.request_tasks.active_count() != 0;
                    tokio::select! {
                        result = state.writer.flush() => break result,
                        () = state.request_tasks.wait_for_completion(), if active => {},
                    }
                }
            })
            .await
            .unwrap_or_else(|_| {
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "HTTP/2 shutdown output stalled",
                )
                .into())
            })
        },
        Err(error) => {
            // The fatal GOAWAY was already written. Linger briefly
            // (nginx-style) so it actually reaches the peer: dropping the
            // socket with unread inbound bytes makes the kernel RST the
            // connection, and the peer's kernel then discards the unread
            // GOAWAY from its receive buffer.
            state.writer.begin_closing();
            if !matches!(error.kind(), ErrorKind::Io(_))
                && let Err(shutdown_error) = linger_after_fatal_goaway(&mut state).await
            {
                crate::server::report_failure(
                    state.context.config.log_format,
                    "connection shutdown",
                    Err(shutdown_error),
                );
            }
            Err(error)
        },
    };
    state.writer.begin_closing();
    let mut tasks = state.into_request_tasks();
    tasks.cancel_all().await;
    result
}

/// Bounded lingering close: half-close the write side, then discard inbound
/// bytes until the peer closes or the linger window expires.
async fn linger_after_fatal_goaway<R, W>(
    state: &mut H2ConnectionState<R, W>,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + WriteTarget,
{
    const FATAL_GOAWAY_LINGER: Duration = Duration::from_secs(1);

    let deadline = TokioInstant::now()
        .checked_add(FATAL_GOAWAY_LINGER)
        .expect("GOAWAY linger fits monotonic clock");
    let shutdown_result = timeout_at(deadline, async {
        loop {
            let active = state.request_tasks.active_count() != 0;
            tokio::select! {
                result = state.writer.shutdown_write() => break result,
                () = state.request_tasks.wait_for_completion(), if active => {},
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "HTTP/2 closing output stalled",
        )
        .into())
    });
    loop {
        let buffered = state.reader.buffered().len();
        state.reader.consume(buffered);
        match timeout_at(deadline, state.reader.read_more_capped(64 * 1024)).await {
            Ok(Ok(true)) => {},
            _ => break, // peer closed, read error, or linger window expired
        }
    }
    shutdown_result
}

async fn drive_h2_connection<R, W>(state: &mut H2ConnectionState<R, W>) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + WriteTarget,
{
    if state.drain_state != ConnectionDrainState::Accepting {
        state.writer.goaway(
            state.last_client_stream_id,
            ErrorCode::NO_ERROR,
            Vec::new(),
            false,
        )?;
    }

    let mut consecutive_peer_frames = 0_usize;
    loop {
        state.request_tasks.reap_completed();
        for stream_id in state.request_tasks.take_retired_inputs() {
            if let Some(stream) = state.streams.get_mut(&stream_id) {
                stream.delivery.retire();
                state
                    .request_deadlines
                    .cancel(state::RequestInputDeadlineKey::body(stream_id));
            }
            stop_retired_request(state, stream_id)?;
        }
        prepare_connection_egress(state).await?;
        let mut stop_after_flush = false;
        let event = ingest_connection_input(state).await?;
        if matches!(&event, IngestEvent::Frame(_)) {
            consecutive_peer_frames = consecutive_peer_frames.wrapping_add(1);
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
                state.request_tasks.begin_cancel_all();
                stop_after_flush = true;
            },
            IngestEvent::FrameLengthExceeded {
                reset_stream,
                error,
            } => {
                // RFC 9113 6.3 makes a wrong-length PRIORITY a stream error,
                // but 6.4 forbids sending RST_STREAM for an idle stream and
                // requires a peer that receives one to fail the connection. An
                // idle stream therefore has to be answered at connection
                // scope; the sibling WINDOW_UPDATE and RST_STREAM paths below
                // resolve the same conflict the same way.
                if let Some(stream_id) =
                    reset_stream.filter(|stream_id| state.receive_state(*stream_id).is_some())
                {
                    state.peer_failure(H2PeerFailure::stream(
                        stream_id,
                        ErrorCode::FRAME_SIZE_ERROR,
                    ))?;
                } else {
                    // Through `peer_failure` rather than writing GOAWAY here:
                    // that path owns the fatal linger and stops the connection,
                    // where writing the frame directly left the loop running to
                    // emit a second, contradictory `GOAWAY(NO_ERROR)` after the
                    // error one.
                    state.peer_failure(H2PeerFailure::frame_size(error))?;
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
                        handle_request_input_timeout(state, deadline)?;
                    }
                },
                ConnectionDeadline::ResponseStall(_) => {
                    if let Some((stream_id, _)) = state
                        .writer
                        .pop_expired_response_stall_deadline(TokioInstant::now())
                    {
                        state.reset_request_stream(stream_id, ErrorCode::CANCEL)?;
                    }
                },
                ConnectionDeadline::WriteStall(_) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "HTTP/2 transport output stalled",
                    )
                    .into());
                },
            },
            IngestEvent::ShutdownChanged => {
                if state.shutdown.borrow().kind().is_some() {
                    begin_graceful_shutdown(
                        &mut state.writer,
                        state.last_client_stream_id,
                        &mut state.drain_state,
                        state.context.config.timeout_graceful_shutdown,
                    )?;
                }
            },
            IngestEvent::Frame(frame) => {
                if advance_connection_with_peer_frame(state, frame)? {
                    stop_after_flush = true;
                }
            },
        }

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
    H2PeerFailure::connection(ErrorCode::COMPRESSION_ERROR, H2Error::FieldBlockTooLarge)
}

/// A field block still incomplete at the header deadline is abandoned, which
/// desynchronizes the connection-wide decoder exactly as refusing to decode
/// one does. See [`header_block_too_large`].
fn header_block_abandoned() -> H2PeerFailure {
    H2PeerFailure::connection(ErrorCode::COMPRESSION_ERROR, H2Error::FieldBlockAbandoned)
}

fn handle_request_input_timeout<R, W>(
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
            // Abandoning the fragments ends the connection for the same reason
            // `header_block_too_large` does: the peer's encoder has already
            // applied this block's dynamic-table insertions, so a decoder that
            // never sees the bytes is permanently behind it. Resetting only the
            // stalled stream leaves the connection looking healthy until the
            // next valid request on it fails to decode.
            state.peer_failure(header_block_abandoned())?;
        },
        RequestInputDeadline::Body(stream_id, _) => {
            if let Some(stream) = state.streams.get_mut(&stream_id)
                && !stream.state.request_is_closed()
            {
                state.reset_request_stream(stream_id, ErrorCode::CANCEL)?;
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
        Err(FrameReadError::DeclaredPayloadLength {
            reset_stream,
            error,
        }) => Ok(IngestEvent::FrameLengthExceeded {
            reset_stream,
            error,
        }),
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
                reset_stream: None,
                error,
            }),
            kind @ (ErrorKind::Python(_)
            | ErrorKind::Io(_)
            | ErrorKind::Join(_)
            | ErrorKind::Config(_)
            | ErrorKind::Asgi(_)
            | ErrorKind::Http1(_)
            | ErrorKind::HttpResponse(_)
            | ErrorKind::H2(_)
            | ErrorKind::Pathsend(_)
            | ErrorKind::Proxy(_)
            | ErrorKind::WebSocket(_)) => Err(kind.into()),
        },
    }
}

fn next_connection_deadline<R, W>(state: &H2ConnectionState<R, W>) -> Option<ConnectionDeadline>
where
    W: WriteTarget,
{
    // A server drain owns its shutdown bound even when it cannot be represented.
    // Keep-alive remains available to ordinary idle and peer-drained connections.
    let keep_alive_deadline = (state.drain_state.uses_keep_alive_timeout() && state.wire_is_idle())
        .then_some(state.context.config.timeout_keep_alive)
        .flatten()
        .and_then(|timeout_duration| {
            TokioInstant::now()
                .checked_add(timeout_duration)
                .map(ConnectionDeadline::KeepAlive)
        });
    let request_input_deadline = state.next_request_input_deadline();
    let response_stall_deadline = state.writer.next_response_stall_deadline();
    let drain_deadline = state
        .drain_state
        .server_deadline()
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
    earlier_deadline(
        next_deadline,
        state
            .writer
            .next_write_deadline()
            .map(ConnectionDeadline::WriteStall),
    )
}

async fn ingest_connection_input<R, W>(
    state: &mut H2ConnectionState<R, W>,
) -> Result<IngestEvent, H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + WriteTarget,
{
    let next_deadline = next_connection_deadline(state);
    let draining = state.drain_state.server_deadline().is_some();
    let outbound_notified = state.connection.outbound_notified();
    tokio::pin!(outbound_notified);
    let input_notified = state.input_flow.notified();
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
        _ = state.shutdown.changed(), if !draining => Ok(IngestEvent::ShutdownChanged),
        () = state.request_tasks.wait_for_completion(), if has_request_tasks => Ok(IngestEvent::Continue),
        result = std::future::poll_fn(|cx| {
            let output = state.writer.poll_egress(cx);
            let frame = std::pin::pin!(state.reader.read_frame(state.local_max_frame_size.as_usize())).as_mut().poll(cx);
            if let Poll::Ready(Err(error)) = output { return Poll::Ready(Err(error)); }
            if let Poll::Ready(frame) = frame { return Poll::Ready(map_frame_ingest_result(frame)); }
            if output.is_ready() { Poll::Ready(Ok(IngestEvent::Continue)) } else { Poll::Pending }
        }) => result,
        () = &mut outbound_notified => Ok(IngestEvent::Continue),
        () = &mut input_notified => Ok(IngestEvent::Continue),
    }
}

async fn prepare_connection_egress<R, W>(
    state: &mut H2ConnectionState<R, W>,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    apply_writer_feedback(state)?;
    flush_released_input_credits(state)?;

    state.writer.drain_app_writes()?;
    if (state.writer.needs_flush() || state.writer.has_ready_streams())
        && next_connection_deadline(state)
            .is_none_or(|deadline| deadline.instant() > TokioInstant::now())
    {
        std::future::poll_fn(|cx| {
            Poll::Ready(match state.writer.poll_egress(cx) {
                Poll::Ready(result) => result,
                Poll::Pending => Ok(()),
            })
        })
        .await?;
    }
    apply_writer_feedback(state)?;
    flush_released_input_credits(state)?;

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

fn handle_settings_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: &RawFrame,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    match validate_settings_frame(frame) {
        Err(failure) => state.peer_failure(failure),
        Ok(SettingsFrame::Ack) => Ok(()),
        Ok(SettingsFrame::Update(settings)) => {
            if state.writer.update_peer_settings(settings).is_err() {
                return state.peer_failure(H2PeerFailure::connection(
                    ErrorCode::FLOW_CONTROL_ERROR,
                    H2Error::SettingsInitialWindowAdjustmentOverflow,
                ));
            }
            state.writer.send_settings_ack()
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

fn handle_ping_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: &RawFrame,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    match validate_ping_frame(frame) {
        Err(failure) => state.peer_failure(failure),
        Ok(PingFrame::Ack) => Ok(()),
        Ok(PingFrame::Request(payload)) => state.writer.ping_ack(payload),
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

fn handle_window_update_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: &RawFrame,
) -> Result<bool, H2CornError>
where
    W: WriteTarget,
{
    let WindowUpdateFrame {
        stream_id,
        increment,
    } = match validate_window_update_frame(frame) {
        Ok(update) => update,
        Err(failure) => {
            state.peer_failure(failure)?;
            return Ok(false);
        },
    };
    if let Some(stream_id) = stream_id {
        match state.receive_state(stream_id) {
            None => {
                state.peer_failure(H2PeerFailure::protocol(H2Error::WindowUpdateOnIdleStream))?;
                return Ok(false);
            },
            // RFC 9113 §5.1 permits updates that raced with stream closure.
            // They have no send-window owner left, so ignoring them is the
            // only action that cannot recreate writer state.
            Some(lifecycle) if !stream_owns_send_window(lifecycle) => return Ok(false),
            Some(_) => {},
        }
        if state
            .writer
            .grant_stream_window(stream_id, increment)
            .is_err()
        {
            state.peer_failure(H2PeerFailure::stream(
                stream_id,
                ErrorCode::FLOW_CONTROL_ERROR,
            ))?;
        }
        Ok(false)
    } else {
        match state.writer.grant_connection_window(increment) {
            Ok(()) => Ok(false),
            Err(WindowOverflow) => state
                .peer_failure(H2PeerFailure::connection(
                    ErrorCode::FLOW_CONTROL_ERROR,
                    H2Error::SendFlowControlWindowOverflow,
                ))
                .map(|()| true),
        }
    }
}

const fn stream_owns_send_window(lifecycle: StreamLifecycle) -> bool {
    matches!(
        lifecycle,
        StreamLifecycle::Open | StreamLifecycle::RequestClosed
    )
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

fn handle_rst_stream_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: &RawFrame,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    let ResetStreamFrame {
        stream_id,
        error_code,
    } = match validate_rst_stream_frame(frame) {
        Ok(reset) => reset,
        Err(failure) => return state.peer_failure(failure),
    };
    match state.receive_state(stream_id) {
        None => {
            state.peer_failure(H2PeerFailure::protocol(H2Error::RstStreamOnIdleStream))?;
            return Ok(());
        },
        Some(StreamLifecycle::Closed) => return Ok(()),
        Some(
            StreamLifecycle::Open
            | StreamLifecycle::RequestClosed
            | StreamLifecycle::ResponseClosed,
        ) => {},
    }
    if state.reset_guard.note_reset() {
        return state.peer_failure(H2PeerFailure::connection(
            ErrorCode::ENHANCE_YOUR_CALM,
            H2Error::PeerResetFlood,
        ));
    }
    if let Some(mut stream) = state.remove_stream(stream_id) {
        stream.delivery.stop_with(StreamInput::Reset(error_code));
    }
    state.request_tasks.cancel(stream_id);
    state.writer.retire_stream(stream_id);
    Ok(())
}

/// Wire-shape validation for `PRIORITY`.
///
/// RFC 9113 deprecates the frame and permits it in every stream state. Its
/// dependency tree is no longer interpreted, so only stream zero and the
/// fixed wire length remain meaningful here.
fn validate_priority_frame(frame: &RawFrame) -> Result<(), H2PeerFailure> {
    let Some(_stream_id) = frame.header.stream_id else {
        return Err(H2PeerFailure::protocol(
            H2Error::PriorityMustNotUseStreamZero,
        ));
    };
    // The payload length is not re-checked here, unlike the sibling
    // fixed-length validators. `validate_declared_payload_length` rejects a
    // wrong-length PRIORITY from the frame header, and its error scope depends
    // on stream state — a local check could only contradict it, which is what
    // it used to do. A PRIORITY that reached here carries no state to corrupt
    // in any case: RFC 9113 deprecates the frame and this handler ignores it.
    Ok(())
}

fn handle_priority_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: &RawFrame,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    match validate_priority_frame(frame) {
        Ok(()) => Ok(()),
        Err(failure) => state.peer_failure(failure),
    }
}

fn reject_oversized_frame<R, W>(
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
            state.peer_failure(H2PeerFailure::connection(
                error_code,
                H2Error::FrameExceedsAdvertisedMaxSize,
            ))?;
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
            state.peer_failure(H2PeerFailure::connection(
                error_code,
                H2Error::FrameExceedsAdvertisedMaxSize,
            ))?;
        },
        Some(stream_id) => {
            state.peer_failure(H2PeerFailure::stream(stream_id, error_code))?;
        },
    }
    Ok(true)
}

fn validate_frame_order<R, W>(
    state: &mut H2ConnectionState<R, W>,
    header: FrameHeader,
) -> Result<bool, H2CornError>
where
    W: WriteTarget,
{
    if state.pending_headers.is_some() && header.frame_type != FrameType::CONTINUATION {
        state.peer_failure(H2PeerFailure::protocol(H2Error::FieldBlockInterrupted))?;
        return Ok(false);
    }
    if state.client_preface == ClientPrefaceState::Active {
        return Ok(true);
    }
    if header.frame_type != FrameType::SETTINGS {
        state.peer_failure(H2PeerFailure::protocol(
            H2Error::FirstClientFrameMustBeSettings,
        ))?;
        return Ok(false);
    }
    if header.flags.contains(FrameFlags::ACK) {
        state.peer_failure(H2PeerFailure::protocol(
            H2Error::FirstClientSettingsMustNotAck,
        ))?;
        return Ok(false);
    }
    state.client_preface = ClientPrefaceState::Active;
    Ok(true)
}

fn validate_goaway_frame(frame: &RawFrame) -> Result<(), H2PeerFailure> {
    if frame.header.stream_id.is_some() || frame.payload.len() < 8 {
        return Err(H2PeerFailure::protocol(H2Error::InvalidGoawayFrame));
    }
    Ok(())
}

fn handle_goaway_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: &RawFrame,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    let () = match validate_goaway_frame(frame) {
        Ok(frame) => frame,
        Err(failure) => return state.peer_failure(failure),
    };
    if state.drain_state == ConnectionDrainState::Accepting {
        state.drain_state = ConnectionDrainState::PeerDraining;
    }
    Ok(())
}

fn reject_push_promise<R, W>(state: &mut H2ConnectionState<R, W>) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    state.peer_failure(H2PeerFailure::protocol(H2Error::UnexpectedPushPromise))
}

fn advance_connection_with_peer_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: FrameRead,
) -> Result<bool, H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + WriteTarget,
{
    match frame {
        FrameRead::Handled(frame, kind) => {
            advance_connection_with_handled_frame(state, frame, kind)
        },
        FrameRead::Ignored(header) => advance_connection_with_ignored_frame(state, header),
    }
}

fn advance_connection_with_handled_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    frame: RawFrame,
    kind: HandledFrameKind,
) -> Result<bool, H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + WriteTarget,
{
    apply_writer_feedback(state)?;

    if reject_oversized_frame(state, &frame)? {
        return Ok(false);
    }
    if !validate_frame_order(state, frame.header)? {
        return Ok(false);
    }

    match kind {
        HandledFrameKind::Settings => {
            handle_settings_frame(state, &frame)?;
        },
        HandledFrameKind::Ping => {
            handle_ping_frame(state, &frame)?;
        },
        HandledFrameKind::WindowUpdate => return handle_window_update_frame(state, &frame),
        HandledFrameKind::RstStream => {
            handle_rst_stream_frame(state, &frame)?;
        },
        HandledFrameKind::Goaway => {
            handle_goaway_frame(state, &frame)?;
        },
        HandledFrameKind::Priority => {
            handle_priority_frame(state, &frame)?;
        },
        HandledFrameKind::PushPromise => {
            reject_push_promise(state)?;
        },
        HandledFrameKind::Headers => {
            handle_headers_frame(state, frame)?;
        },
        HandledFrameKind::Continuation => {
            handle_continuation_frame(state, &frame)?;
        },
        HandledFrameKind::Data => {
            handle_data_frame(state, frame)?;
        },
    }
    Ok(false)
}

fn advance_connection_with_ignored_frame<R, W>(
    state: &mut H2ConnectionState<R, W>,
    header: FrameHeader,
) -> Result<bool, H2CornError>
where
    W: WriteTarget,
{
    apply_writer_feedback(state)?;
    Ok(!validate_frame_order(state, header)?)
}

fn parse_data_payload(payload: Bytes, flags: FrameFlags) -> Result<(Bytes, bool), H2CornError> {
    let data = if flags.contains(FrameFlags::PADDED) {
        let Some((&padding, rest)) = payload.split_first() else {
            return H2Error::DataPaddedMissingPadding.err();
        };
        let Some(data_len) = rest.len().checked_sub(usize::from(padding)) else {
            return H2Error::DataPaddingExceedsPayload.err();
        };
        rest.get(..data_len)
            .expect("padding was subtracted from this DATA payload")
    } else {
        payload.as_ref()
    };
    Ok((
        if data.len() == payload.len() {
            payload
        } else {
            payload.slice_ref(data)
        },
        flags.contains(FrameFlags::END_STREAM),
    ))
}

fn apply_writer_feedback<R, W>(state: &mut H2ConnectionState<R, W>) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    for (target, increment) in state.writer.take_input_credits() {
        match target {
            WindowTarget::Connection => state.connection_window.commit_update(increment),
            WindowTarget::Stream(id) => {
                if let Some(stream) = state.streams.get_mut(&id) {
                    stream.receive_window.commit_update(increment);
                }
            },
        }
    }
    for (kind, stream_id) in state.writer.take_response_closes() {
        state.apply_response_close(stream_id);
        if matches!(kind, ResponseClose::Abort | ResponseClose::ProducerFailed) {
            if let Some(mut stream) = state.remove_stream(stream_id) {
                stream
                    .delivery
                    .stop_with(StreamInput::Reset(ErrorCode::CANCEL));
            }
            state.request_tasks.retire_input(stream_id);
            if kind == ResponseClose::Abort {
                state.request_tasks.cancel(stream_id);
            }
        } else {
            stop_retired_request(state, stream_id)?;
        }
    }
    Ok(())
}

fn stop_retired_request<R, W: WriteTarget>(
    state: &mut H2ConnectionState<R, W>,
    stream_id: StreamId,
) -> Result<(), H2CornError> {
    let stop = state.streams.get(&stream_id).is_some_and(|stream| {
        stream.is_http
            && stream.delivery.is_stopped()
            && stream.state == StreamLifecycle::ResponseClosed
    });
    if stop {
        state.remove_stream(stream_id);
        state.writer.stop_request(stream_id)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io;
    use std::pin::Pin;
    use std::task::Context;

    use tokio::io::BufWriter;

    use super::*;
    use crate::http::types::status_code;

    const INITIAL_STREAM_WINDOW_SIZE: u32 = 8 << 20;
    const STREAM_WINDOW_UPDATE_THRESHOLD: u32 = INPUT_WINDOW_UPDATE_THRESHOLD;

    #[derive(Clone, Copy)]
    enum BlockedOutputEvent {
        Data,
        Reset,
        InputTimeout,
        WriteTimeout,
        Shutdown,
    }

    #[rstest::rstest]
    #[case::data(BlockedOutputEvent::Data)]
    #[case::reset(BlockedOutputEvent::Reset)]
    #[case::input_timeout(BlockedOutputEvent::InputTimeout)]
    #[case::write_timeout(BlockedOutputEvent::WriteTimeout)]
    #[case::shutdown(BlockedOutputEvent::Shutdown)]
    #[tokio::test(start_paused = true)]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    async fn blocked_socket_preserves_input_and_control_progress(
        #[case] event: BlockedOutputEvent,
    ) {
        assert_blocked_output_progress(event).await;
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "the context moves into connection state, which the pinned driver borrows until scope exit"
    )]
    async fn assert_blocked_output_progress(event: BlockedOutputEvent) {
        use pyo3::Python;
        use tokio::io::{AsyncWriteExt as _, duplex};
        use tokio::sync::{mpsc, oneshot};

        use crate::h2::state::RequestTaskCancellation;
        use crate::runtime::{ShutdownKind, test_fixtures};

        struct Finished(Option<oneshot::Sender<()>>);
        impl Drop for Finished {
            fn drop(&mut self) {
                if let Some(tx) = self.0.take() {
                    let _sent = tx.send(());
                }
            }
        }

        Python::initialize();
        let mut config = test_fixtures::server_config_parts();
        config.timeout_graceful_shutdown = Duration::ZERO;
        if matches!(event, BlockedOutputEvent::WriteTimeout) {
            config.http2.timeout_write = Some(Duration::from_secs(10));
        }
        let context = Python::attach(|py| {
            test_fixtures::connection_context_with(py, std::sync::Arc::new(config))
        });
        let target = writer::GatedWriter::new(usize::MAX, true);
        let gate = Arc::clone(&target.0);
        let (mut output, handle) = writer::init_writer(target, Arc::clone(&context.config), None)
            .await
            .unwrap();
        gate.lock().bytes.clear();
        gate.lock().allowance = 7;
        output
            .send_headers(
                StreamId::new(1).unwrap(),
                status_code::OK,
                ResponseHeaders::new(),
                false,
            )
            .unwrap();
        let (mut peer, input) = duplex(256);
        peer.write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0]).await.unwrap();
        let (shutdown, shutdown_rx) = watch::channel(ShutdownState::Running);
        let mut state = H2ConnectionState::new(
            h2_frame::BufferedConnectionReader::new(input),
            handle,
            output,
            context,
            shutdown_rx,
            ConnectionDrainState::Accepting,
        );
        let id = StreamId::new(1).unwrap();
        state.last_client_stream_id = Some(id);
        let (tx, mut rx) = mpsc::channel(2);
        state.streams.insert(
            id,
            InboundStream::new(
                Some(tx),
                true,
                false,
                None,
                None,
                ResolvedRequestPolicy::default(),
                8 << 20,
            ),
        );
        if matches!(event, BlockedOutputEvent::InputTimeout) {
            state.request_deadlines.reschedule_after(
                state::RequestInputDeadlineKey::body(id),
                TokioInstant::now(),
                Duration::from_secs(10),
            );
        }
        let (done_tx, mut done) = oneshot::channel();
        let finished = Finished(Some(done_tx));
        state
            .request_tasks
            .spawn(id, RequestTaskCancellation::Immediate, async move {
                let _finished = finished;
                std::future::pending::<()>().await;
            });
        let mut drive = std::pin::pin!(drive_h2_connection(&mut state));
        let first = std::future::poll_fn(|cx| Poll::Ready(drive.as_mut().poll(cx))).await;
        assert!(first.is_pending());
        assert_eq!(
            gate.lock().bytes.len(),
            7,
            "the socket accepted a strict frame prefix before blocking"
        );
        tokio::time::timeout(Duration::from_secs(60), async {
        match event {
            BlockedOutputEvent::Data => {
                peer.write_all(&[0, 0, 3, 0, 0, 0, 0, 0, 1, 0x71, 0x72, 0x73])
                    .await
                    .unwrap();
                tokio::select! {
                    input = rx.recv() => assert!(matches!(input, Some(StreamInput::Data { body, .. }) if body.as_ref() == b"qrs")),
                    result = &mut drive => panic!("connection stopped before input delivery: {result:?}"),
                }
            },
            BlockedOutputEvent::Reset => {
                peer.write_all(&[0, 0, 4, 3, 0, 0, 0, 0, 1, 0, 0, 0, 8])
                    .await
                    .unwrap();
                tokio::select! {
                    result = &mut done => result.unwrap(),
                    result = &mut drive => panic!("connection stopped before reset settlement: {result:?}"),
                }
            },
            BlockedOutputEvent::InputTimeout => {
                tokio::time::advance(Duration::from_secs(10)).await;
                tokio::select! {
                    result = &mut done => result.unwrap(),
                    result = &mut drive => panic!("connection stopped before timed-out stream settlement: {result:?}"),
                }
            },
            BlockedOutputEvent::Shutdown => {
                shutdown.send_replace(ShutdownState::Graceful(ShutdownKind::Stop));
                drive.await.unwrap();
            },
            BlockedOutputEvent::WriteTimeout => {
                tokio::time::advance(Duration::from_secs(10)).await;
                let error = drive.await.unwrap_err();
                assert!(
                    matches!(error.kind(), ErrorKind::Io(error) if error.kind() == std::io::ErrorKind::TimedOut)
                );
            },
        }
        }).await.expect("connection input and control must progress while socket output is pending");
        assert_eq!(
            gate.lock().bytes.len(),
            7,
            "local progress must not require an unblocked write"
        );
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "connection state retains the writer while the test advances its input and output phases"
    )]
    #[tokio::test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    async fn prepared_flush_after_producer_failure_resumes_admitted_sibling() {
        use pyo3::Python;
        use tokio::io::duplex;

        use crate::h2::writer::{WriterCommand, WriterCommandBatch};
        use crate::http::response::ResponseByteBudget;
        use crate::runtime::test_fixtures;

        Python::initialize();
        let mut config = test_fixtures::server_config_parts();
        config.http2.timeout_write = None;
        config.timeout_keep_alive = None;
        let context =
            Python::attach(|py| test_fixtures::connection_context_with(py, Arc::new(config)));
        let target = writer::GatedWriter::new(usize::MAX, true);
        let gate = Arc::clone(&target.0);
        let (mut output, handle) = writer::init_writer(target, Arc::clone(&context.config), None)
            .await
            .unwrap();
        let failed = StreamId::new(1).unwrap();
        let sibling = StreamId::new(3).unwrap();
        output.register_stream(failed, None);
        output
            .send_headers(failed, status_code::OK, ResponseHeaders::new(), false)
            .unwrap();
        output.flush().await.unwrap();
        gate.lock().bytes.clear();
        gate.lock().allowance = 12;
        let (_peer, input) = duplex(256);
        let (_shutdown, shutdown_rx) = watch::channel(ShutdownState::Running);
        let mut state = H2ConnectionState::new(
            h2_frame::BufferedConnectionReader::new(input),
            handle,
            output,
            context,
            shutdown_rx,
            ConnectionDrainState::Accepting,
        );
        state.streams.insert(
            failed,
            InboundStream::new(
                None,
                true,
                false,
                None,
                None,
                ResolvedRequestPolicy::default(),
                8 << 20,
            ),
        );
        let budget = ResponseByteBudget::new(32 * 1024);
        let mut commands = WriterCommandBatch::new();
        commands.push_back(WriterCommand::SendData {
            stream_id: failed,
            data: bytes::Bytes::from(vec![b'x'; 32 * 1024]).into(),
            credit: budget.acquire(32 * 1024).await.unwrap(),
            end_stream: false,
        });
        state
            .connection
            .send_commands(failed, &mut commands)
            .await
            .unwrap();
        prepare_connection_egress(&mut state).await.unwrap();
        assert_eq!(gate.lock().bytes.len(), 12);

        state
            .connection
            .failure_notification(failed)
            .unwrap()
            .application_failed();
        prepare_connection_egress(&mut state).await.unwrap();
        assert!(!state.streams.contains_key(&failed));
        budget.try_acquire(32 * 1024).unwrap_err();

        state.writer.register_stream(sibling, None);
        let mut commands = WriterCommandBatch::new();
        commands.push_back(WriterCommand::SendFinal {
            stream_id: sibling,
            status: status_code::OK,
            headers: ResponseHeaders::new(),
            data: bytes::Bytes::from_static(b"sibling").into(),
            credit: None,
        });
        state
            .connection
            .send_commands(sibling, &mut commands)
            .await
            .unwrap();
        assert!(matches!(
            ingest_connection_input(&mut state).await.unwrap(),
            IngestEvent::Continue
        ));
        prepare_connection_egress(&mut state).await.unwrap();
        gate.lock().allowance = usize::MAX;
        prepare_connection_egress(&mut state).await.unwrap();
        let event = std::future::poll_fn(|cx| {
            Poll::Ready(
                std::pin::pin!(ingest_connection_input(&mut state))
                    .as_mut()
                    .poll(cx),
            )
        })
        .await;
        assert!(
            matches!(event, Poll::Ready(Ok(IngestEvent::Continue))),
            "a prepared flush must expose admitted sibling output without another peer event"
        );
        prepare_connection_egress(&mut state).await.unwrap();
        let output = gate.lock().bytes.clone();
        let mut reader = h2_frame::BufferedConnectionReader::new(output.as_slice());
        let mut frames = Vec::new();
        while let Some(frame) = reader.read_frame(16 * 1024).await.unwrap() {
            let FrameRead::Handled(frame, _) = frame else {
                panic!("unexpected extension frame");
            };
            frames.push(frame);
        }
        assert_eq!(frames.len(), 4);
        assert_eq!(frames[0].header.frame_type, FrameType::DATA);
        assert_eq!(frames[0].header.stream_id, Some(failed));
        assert_eq!(frames[0].payload.as_ref(), &[b'x'; 16 * 1024]);
        assert_eq!(frames[1].header.frame_type, FrameType::RST_STREAM);
        assert_eq!(frames[1].header.stream_id, Some(failed));
        assert_eq!(
            frames[1].payload.as_ref(),
            ErrorCode::INTERNAL_ERROR.to_be_bytes()
        );
        assert_eq!(frames[2].header.frame_type, FrameType::HEADERS);
        assert_eq!(frames[2].header.stream_id, Some(sibling));
        assert_eq!(frames[2].payload.as_ref(), &[0x88]);
        assert_eq!(frames[3].header.frame_type, FrameType::DATA);
        assert_eq!(frames[3].header.stream_id, Some(sibling));
        assert!(frames[3].header.flags.contains(FrameFlags::END_STREAM));
        assert_eq!(frames[3].payload.as_ref(), b"sibling");
        budget.try_acquire(32 * 1024).unwrap();
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "the context moves into connection state, whose writer is inspected after the driver returns"
    )]
    #[tokio::test(start_paused = true)]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    async fn expired_output_deadline_precedes_newly_writable_transport() {
        use pyo3::Python;
        use tokio::io::duplex;

        use crate::runtime::test_fixtures;

        Python::initialize();
        let mut config = test_fixtures::server_config_parts();
        config.http2.timeout_write = Some(Duration::from_secs(10));
        config.timeout_keep_alive = Some(Duration::from_secs(1));
        let context =
            Python::attach(|py| test_fixtures::connection_context_with(py, Arc::new(config)));
        let target = writer::GatedWriter::new(usize::MAX, true);
        let gate = Arc::clone(&target.0);
        let (mut output, handle) = writer::init_writer(target, Arc::clone(&context.config), None)
            .await
            .unwrap();
        gate.lock().bytes.clear();
        let id = StreamId::new(1).unwrap();
        output
            .send_headers(id, status_code::OK, ResponseHeaders::new(), true)
            .unwrap();
        let (_peer, input) = duplex(256);
        let (_shutdown, shutdown_rx) = watch::channel(ShutdownState::Running);
        let mut state = H2ConnectionState::new(
            h2_frame::BufferedConnectionReader::new(input),
            handle,
            output,
            context,
            shutdown_rx,
            ConnectionDrainState::Accepting,
        );
        tokio::time::advance(Duration::from_secs(10)).await;
        let error = drive_h2_connection(&mut state).await.unwrap_err();
        assert!(
            matches!(error.kind(), ErrorKind::Io(error) if error.kind() == std::io::ErrorKind::TimedOut)
        );
        assert!(gate.lock().bytes.is_empty());
        assert!(state.writer.take_response_closes().is_empty());
    }

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

    fn parse_window_updates(mut bytes: &[u8]) -> Vec<(u32, u32)> {
        let mut updates = Vec::new();

        while !bytes.is_empty() {
            let (&[a, b, c, frame_type, _, s0, s1, s2, s3], rest) = bytes
                .split_first_chunk::<9>()
                .expect("complete HTTP/2 header");
            let len = u32::from_be_bytes([0, a, b, c]) as usize;
            let (payload, rest) = rest.split_at_checked(len).expect("complete HTTP/2 payload");
            let stream_id = u32::from_be_bytes([s0, s1, s2, s3]) & h2_frame::STREAM_ID_MASK;

            if frame_type == 8 && len == 4 {
                let increment =
                    u32::from_be_bytes(payload.try_into().expect("four-byte window update"))
                        & h2_frame::MAX_FLOW_CONTROL_WINDOW;
                updates.push((stream_id, increment));
            }

            bytes = rest;
        }

        updates
    }

    #[test]
    fn only_streams_with_an_open_response_own_a_send_window() {
        assert!(stream_owns_send_window(StreamLifecycle::Open));
        assert!(stream_owns_send_window(StreamLifecycle::RequestClosed));
        assert!(!stream_owns_send_window(StreamLifecycle::ResponseClosed));
        assert!(!stream_owns_send_window(StreamLifecycle::Closed));
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
