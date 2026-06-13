use std::collections::VecDeque;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

use tokio::sync::{mpsc, watch};
use tokio::time::Instant as TokioInstant;

use super::StreamMap;
use super::request::{HeaderLimits, PendingHeaders};
use super::writer::{ConnectionHandle, WriterState};
use crate::frame::{self, StreamId, WindowIncrement};
use crate::h2::new_stream_map;
use crate::hpack::Decoder;
use crate::http::body::{RequestBodyFinish, RequestBodyState};
use crate::runtime::{ConnectionContext, ShutdownState, StreamInput};

/// Generous for legitimate cancellation bursts (browser navigations, gRPC
/// deadline storms); rapid-reset attacks send thousands per second.
const RESET_RATE_LIMIT: u32 = 200;
const RESET_RATE_WINDOW: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Debug)]
pub(super) struct InboundStream {
    pub(super) delivery: InputDelivery,
    pub(super) counts_toward_read_timeout: bool,
    pub(super) receive_window: ReceiveWindowState,
    pub(super) state: ReceiveState,
    pub(super) body: RequestBodyState,
    pub(super) last_input_read_at: TokioInstant,
}

/// App-input delivery state for one stream. Every input — including the
/// terminal `EndStream` — flows through here, so a backlogged body can never
/// be truncated or reordered by stream completion.
#[derive(Debug)]
pub(super) enum InputDelivery {
    /// Channel open and keeping up.
    Open(mpsc::Sender<StreamInput>),
    /// The app receiver fell behind: inputs queue here and flush in arrival
    /// order before any new push.
    Backlogged(QueuedInput),
    /// No receiver (none allocated, app gone, or delivery abandoned).
    Stopped,
}

#[derive(Debug)]
pub(super) struct QueuedInput {
    tx: mpsc::Sender<StreamInput>,
    queue: VecDeque<StreamInput>,
}

impl InputDelivery {
    pub(super) fn new(input: Option<mpsc::Sender<StreamInput>>) -> Self {
        input.map_or(Self::Stopped, Self::Open)
    }

    /// Deliver or queue one input; drops it when delivery has stopped.
    pub(super) fn push(&mut self, value: StreamInput) {
        match self {
            Self::Open(tx) => match crate::async_util::try_push(tx, value) {
                crate::async_util::TryPush::Sent => {},
                crate::async_util::TryPush::Full(value) => {
                    let Self::Open(tx) = std::mem::replace(self, Self::Stopped) else {
                        unreachable!("matched Open above");
                    };
                    *self = Self::Backlogged(QueuedInput {
                        tx,
                        queue: VecDeque::from([value]),
                    });
                },
                crate::async_util::TryPush::Closed(_) => *self = Self::Stopped,
            },
            Self::Backlogged(queued) => queued.queue.push_back(value),
            Self::Stopped => {},
        }
    }

    /// Push as much backlog as the channel accepts; returns to `Open` once
    /// drained. True when no backlog remains afterwards.
    pub(super) fn flush(&mut self) -> bool {
        let Self::Backlogged(queued) = self else {
            return true;
        };
        match queued.flush() {
            QueueFlush::Drained => {
                let Self::Backlogged(queued) = std::mem::replace(self, Self::Stopped) else {
                    unreachable!("matched Backlogged above");
                };
                *self = Self::Open(queued.tx);
                true
            },
            QueueFlush::Pending => false,
            QueueFlush::Closed => {
                *self = Self::Stopped;
                true
            },
        }
    }

    pub(super) const fn has_backlog(&self) -> bool {
        matches!(self, Self::Backlogged(_))
    }

    /// Abandon delivery, returning the sender for a best-effort terminal
    /// message (reset paths drop any backlog deliberately).
    pub(super) fn take_sender(&mut self) -> Option<mpsc::Sender<StreamInput>> {
        match std::mem::replace(self, Self::Stopped) {
            Self::Open(tx) => Some(tx),
            Self::Backlogged(queued) => Some(queued.tx),
            Self::Stopped => None,
        }
    }

    pub(super) const fn sender(&self) -> Option<&mpsc::Sender<StreamInput>> {
        match self {
            Self::Open(tx) => Some(tx),
            Self::Backlogged(queued) => Some(&queued.tx),
            Self::Stopped => None,
        }
    }
}

pub(super) enum QueueFlush {
    Drained,
    Pending,
    Closed,
}

impl QueuedInput {
    pub(super) fn flush(&mut self) -> QueueFlush {
        while let Some(value) = self.queue.pop_front() {
            match crate::async_util::try_push(&self.tx, value) {
                crate::async_util::TryPush::Sent => {},
                crate::async_util::TryPush::Full(value) => {
                    self.queue.push_front(value);
                    return QueueFlush::Pending;
                },
                crate::async_util::TryPush::Closed(_) => return QueueFlush::Closed,
            }
        }
        QueueFlush::Drained
    }
}

#[derive(Debug)]
pub(super) struct ReceiveWindowState {
    recv_window: i64,
    pending_update: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ReceiveState {
    Idle,
    Open,
    RequestClosed,
    ResponseClosed,
    Closed,
}

impl ReceiveState {
    pub(super) fn is_idle(self) -> bool {
        self == Self::Idle
    }

    pub(super) const fn request_is_closed(self) -> bool {
        matches!(self, Self::RequestClosed | Self::Closed)
    }
}

/// Per-connection `RST_STREAM` rate accounting (CVE-2023-44487 "rapid reset"
/// class): open+RST floods spawn request work without ever counting toward
/// `max_concurrent_streams`, so peer resets are capped per fixed window.
#[derive(Debug)]
pub(super) struct RapidResetGuard {
    window_start: TokioInstant,
    resets: u32,
}

impl RapidResetGuard {
    pub(super) fn new() -> Self {
        Self {
            window_start: TokioInstant::now(),
            resets: 0,
        }
    }

    /// Count one peer `RST_STREAM`; true when the flood threshold is breached.
    pub(super) fn note_reset(&mut self) -> bool {
        let now = TokioInstant::now();
        if now - self.window_start >= RESET_RATE_WINDOW {
            self.window_start = now;
            self.resets = 0;
        }
        self.resets += 1;
        self.resets > RESET_RATE_LIMIT
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ConnectionDrainState {
    Accepting,
    Draining { deadline: Option<Instant> },
}

impl ConnectionDrainState {
    pub(super) const fn deadline(self) -> Option<Instant> {
        match self {
            Self::Accepting => None,
            Self::Draining { deadline } => deadline,
        }
    }
}

pub(super) struct RequestSpawnContext<'a> {
    pub(super) streams: &'a mut StreamMap<InboundStream>,
    pub(super) connection: &'a ConnectionContext,
}

pub(super) struct H2ConnectionState<R, W> {
    pub(super) reader: frame::FrameReader<R>,
    pub(super) connection: ConnectionHandle,
    pub(super) writer: WriterState<W>,
    pub(super) context: ConnectionContext,
    pub(super) secure: bool,
    pub(super) shutdown: watch::Receiver<ShutdownState>,
    pub(super) decoder: Decoder,
    pub(super) streams: StreamMap<InboundStream>,
    pub(super) pending_headers: Option<PendingHeaders>,
    pub(super) last_client_stream_id: Option<StreamId>,
    pub(super) connection_window: ReceiveWindowState,
    pub(super) local_max_frame_size: usize,
    pub(super) saw_client_settings: bool,
    pub(super) drain_state: ConnectionDrainState,
    pub(super) reset_guard: RapidResetGuard,
    /// Backlogged app inputs of streams whose entries were already removed;
    /// they keep flushing until drained or the receiver goes away.
    pub(super) draining_inputs: Vec<QueuedInput>,
}

pub(super) enum RequestInputClose {
    ContentLengthMismatch,
    Closed {
        remove_stream: bool,
        terminal: TerminalDelivery,
    },
}

/// How the terminal `EndStream` reaches the app when request input closes.
pub(super) enum TerminalDelivery {
    /// No backlog: the caller sends `EndStream` directly.
    SendNow(mpsc::Sender<StreamInput>),
    /// `EndStream` was appended behind the stream's backlog in place.
    Queued,
    /// The stream entry is going away: the backlog (terminated by
    /// `EndStream`) continues draining at the connection level.
    Drain(QueuedInput),
    /// Delivery already stopped.
    None,
}

impl ReceiveWindowState {
    pub(super) fn new(initial_window: u32) -> Self {
        Self {
            recv_window: i64::from(initial_window),
            pending_update: 0,
        }
    }

    pub(super) fn receive(&mut self, len: u32) -> Result<(), ()> {
        self.recv_window -= i64::from(len);
        if self.recv_window < 0 {
            return Err(());
        }
        self.pending_update += len;
        Ok(())
    }

    pub(super) fn take_update(&mut self, threshold: u32) -> Option<WindowIncrement> {
        if self.pending_update < threshold {
            return None;
        }

        let increment = WindowIncrement::new(self.pending_update)
            .expect("pending window update is only incremented by positive DATA lengths");
        self.pending_update = 0;
        self.recv_window += std::num::NonZeroI64::from(increment).get();
        Some(increment)
    }
}

impl InboundStream {
    pub(super) fn new(
        input: Option<mpsc::Sender<StreamInput>>,
        counts_toward_read_timeout: bool,
        end_stream: bool,
        expected_content_length: Option<u64>,
        body_bytes: Option<Arc<AtomicU64>>,
        max_request_body_size: Option<u64>,
        initial_window: u32,
    ) -> Self {
        Self {
            delivery: InputDelivery::new(input),
            counts_toward_read_timeout,
            receive_window: ReceiveWindowState::new(initial_window),
            state: if end_stream {
                ReceiveState::RequestClosed
            } else {
                ReceiveState::Open
            },
            body: RequestBodyState::new(expected_content_length, body_bytes, max_request_body_size),
            last_input_read_at: TokioInstant::now(),
        }
    }

    pub(super) fn mark_request_closed(&mut self) -> bool {
        self.state = match self.state {
            ReceiveState::Open => ReceiveState::RequestClosed,
            ReceiveState::ResponseClosed => ReceiveState::Closed,
            state => state,
        };
        self.state == ReceiveState::Closed
    }

    pub(super) fn mark_response_closed(&mut self) -> bool {
        self.state = match self.state {
            ReceiveState::Open => ReceiveState::ResponseClosed,
            ReceiveState::RequestClosed => ReceiveState::Closed,
            state => state,
        };
        self.state == ReceiveState::Closed
    }

    fn finish_request_input(&mut self) -> RequestInputClose {
        if self.body.finish() == RequestBodyFinish::ContentLengthMismatch {
            return RequestInputClose::ContentLengthMismatch;
        }

        let remove_stream = self.mark_request_closed();
        let terminal = match std::mem::replace(&mut self.delivery, InputDelivery::Stopped) {
            InputDelivery::Open(tx) => TerminalDelivery::SendNow(tx),
            InputDelivery::Backlogged(mut queued) => {
                queued.queue.push_back(StreamInput::EndStream);
                if remove_stream {
                    TerminalDelivery::Drain(queued)
                } else {
                    self.delivery = InputDelivery::Backlogged(queued);
                    TerminalDelivery::Queued
                }
            },
            InputDelivery::Stopped => TerminalDelivery::None,
        };
        RequestInputClose::Closed {
            remove_stream,
            terminal,
        }
    }
}

impl<R, W> H2ConnectionState<R, W> {
    pub(super) fn new(
        reader: frame::FrameReader<R>,
        connection: ConnectionHandle,
        writer: WriterState<W>,
        context: ConnectionContext,
        secure: bool,
        shutdown: watch::Receiver<ShutdownState>,
        drain_state: ConnectionDrainState,
    ) -> Self {
        let stream_capacity = context.config.http2.max_concurrent_streams as usize;
        let local_max_frame_size = context.config.http2.max_inbound_frame_size.get() as usize;
        let initial_connection_window = context.config.http2.initial_connection_window_size.get();
        Self {
            reader,
            connection,
            writer,
            context,
            secure,
            shutdown,
            decoder: Decoder::new(frame::DEFAULT_HEADER_TABLE_SIZE),
            streams: new_stream_map(stream_capacity),
            pending_headers: None,
            last_client_stream_id: None,
            connection_window: ReceiveWindowState::new(initial_connection_window),
            local_max_frame_size,
            saw_client_settings: false,
            drain_state,
            reset_guard: RapidResetGuard::new(),
            draining_inputs: Vec::new(),
        }
    }

    pub(super) fn active_stream_count(&self) -> usize {
        self.streams.len() + usize::from(self.pending_headers.is_some())
    }

    pub(super) const fn header_limits(&self) -> HeaderLimits {
        HeaderLimits::new(
            self.context.config.http2.max_header_list_size,
            self.context.config.limit_request_fields,
        )
    }

    pub(super) fn header_block_too_large(&self, len: usize) -> bool {
        self.context
            .config
            .http2
            .max_header_block_size
            .is_some_and(|limit| len > limit.get())
    }

    pub(super) fn next_request_input_deadline(
        &self,
        timeout_request_header: Option<std::time::Duration>,
        timeout_request_body_idle: Option<std::time::Duration>,
    ) -> Option<RequestInputDeadline> {
        let header_deadline = match (self.pending_headers.as_ref(), timeout_request_header) {
            (Some(pending), Some(timeout_duration)) => Some(RequestInputDeadline::Headers(
                pending.stream_id,
                pending.last_fragment_at + timeout_duration,
            )),
            _ => None,
        };
        let body_deadline = timeout_request_body_idle.and_then(|timeout_duration| {
            self.streams
                .iter()
                .filter(|(_, stream)| {
                    stream.counts_toward_read_timeout && !stream.state.request_is_closed()
                })
                .map(|(&stream_id, stream)| {
                    RequestInputDeadline::Body(
                        StreamId::new(stream_id).expect("stored stream id is non-zero"),
                        stream.last_input_read_at + timeout_duration,
                    )
                })
                .min_by_key(|deadline| deadline.instant())
        });
        match (header_deadline, body_deadline) {
            (Some(header), Some(body)) => Some(if header.instant() <= body.instant() {
                header
            } else {
                body
            }),
            (Some(header), None) => Some(header),
            (None, Some(body)) => Some(body),
            (None, None) => None,
        }
    }

    pub(super) fn receive_state(&self, stream_id: StreamId) -> ReceiveState {
        self.streams.get(&stream_id.get()).map_or_else(
            || missing_receive_state(stream_id, self.last_client_stream_id),
            |stream| stream.state,
        )
    }

    pub(super) fn apply_response_close(&mut self, stream_id: StreamId) {
        if let Entry::Occupied(mut entry) = self.streams.entry(stream_id.get())
            && entry.get_mut().mark_response_closed()
        {
            entry.remove();
        }
    }

    pub(super) fn finish_request_input(
        &mut self,
        stream_id: StreamId,
    ) -> Option<RequestInputClose> {
        match self.streams.entry(stream_id.get()) {
            Entry::Occupied(mut entry) => {
                let mut close = entry.get_mut().finish_request_input();
                if matches!(close, RequestInputClose::Closed {
                    remove_stream: true,
                    ..
                }) {
                    entry.remove();
                }
                // A removed stream's backlog drains at the connection level;
                // only swap out the terminal when it is actually `Drain`, so
                // the common `SendNow`/`Queued` cases reach the caller intact.
                if let RequestInputClose::Closed {
                    terminal: terminal @ TerminalDelivery::Drain(_),
                    ..
                } = &mut close
                    && let TerminalDelivery::Drain(queued) =
                        std::mem::replace(terminal, TerminalDelivery::None)
                {
                    self.draining_inputs.push(queued);
                }
                Some(close)
            },
            Entry::Vacant(_) => None,
        }
    }

    pub(super) fn remove_stream(&mut self, stream_id: StreamId) -> Option<InboundStream> {
        self.streams.remove(&stream_id.get())
    }

    pub(super) fn should_stop(&self) -> bool {
        if self.drain_state == ConnectionDrainState::Accepting {
            return false;
        }
        self.active_stream_count() == 0
            || self
                .drain_state
                .deadline()
                .is_some_and(|deadline| Instant::now() >= deadline)
    }

    pub(super) fn should_refuse_new_streams(&self) -> bool {
        self.drain_state != ConnectionDrainState::Accepting
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum RequestInputDeadline {
    Headers(StreamId, TokioInstant),
    Body(StreamId, TokioInstant),
}

impl RequestInputDeadline {
    pub(super) const fn instant(self) -> TokioInstant {
        match self {
            Self::Headers(_, instant) | Self::Body(_, instant) => instant,
        }
    }
}

fn missing_receive_state(
    stream_id: StreamId,
    last_client_stream_id: Option<StreamId>,
) -> ReceiveState {
    if stream_id.get() & 1 == 1 && last_client_stream_id.is_none_or(|last| stream_id > last) {
        ReceiveState::Idle
    } else {
        ReceiveState::Closed
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tokio::sync::mpsc;

    use super::{InboundStream, InputDelivery, RequestInputClose, TerminalDelivery};
    use crate::runtime::StreamInput;

    fn data(byte: &'static [u8]) -> StreamInput {
        StreamInput::Data(Bytes::from_static(byte))
    }

    fn assert_data(input: StreamInput, expected: &[u8]) {
        match input {
            StreamInput::Data(bytes) => assert_eq!(bytes.as_ref(), expected),
            other => panic!("expected data input, got {other:?}"),
        }
    }

    /// Regression: the terminal `EndStream` must queue behind backlogged
    /// `Data` — a side-channel send would truncate a backpressured body yet
    /// present it as complete.
    #[tokio::test]
    async fn end_stream_queues_behind_backlogged_data() {
        let (tx, mut rx) = mpsc::channel(1);
        let mut stream = InboundStream::new(Some(tx), true, false, None, None, None, 0xFFFF);

        stream.delivery.push(data(b"a"));
        stream.delivery.push(data(b"b"));
        assert!(stream.delivery.has_backlog());

        let close = stream.finish_request_input();
        let RequestInputClose::Closed {
            remove_stream: false,
            terminal: TerminalDelivery::Queued,
        } = close
        else {
            panic!("backlogged finish must queue the terminal input in place");
        };

        assert_data(rx.recv().await.expect("first chunk"), b"a");
        assert!(!stream.delivery.flush());
        assert_data(rx.recv().await.expect("second chunk"), b"b");
        assert!(stream.delivery.flush());
        assert!(matches!(
            rx.recv().await.expect("terminal input"),
            StreamInput::EndStream
        ));
    }

    /// When the stream entry is removed at input close, the backlog continues
    /// draining at the connection level instead of being dropped.
    #[tokio::test]
    async fn fully_closed_stream_hands_backlog_to_connection_drain() {
        let (tx, mut rx) = mpsc::channel(1);
        let mut stream = InboundStream::new(Some(tx), true, false, None, None, None, 0xFFFF);
        stream.mark_response_closed();

        stream.delivery.push(data(b"a"));
        stream.delivery.push(data(b"b"));

        let close = stream.finish_request_input();
        let RequestInputClose::Closed {
            remove_stream: true,
            terminal: TerminalDelivery::Drain(mut queued),
        } = close
        else {
            panic!("fully closed stream must hand its backlog off for draining");
        };

        assert_data(rx.recv().await.expect("first chunk"), b"a");
        assert!(matches!(queued.flush(), super::QueueFlush::Pending) || rx.try_recv().is_ok());
        queued.flush();
        assert_data(rx.recv().await.expect("second chunk"), b"b");
        queued.flush();
        assert!(matches!(
            rx.recv().await.expect("terminal input"),
            StreamInput::EndStream
        ));
    }

    /// Empty-backlog completion still delivers the terminal directly.
    #[tokio::test]
    async fn idle_delivery_completion_sends_terminal_directly() {
        let (tx, _rx) = mpsc::channel(4);
        let mut stream = InboundStream::new(Some(tx), true, false, None, None, None, 0xFFFF);

        let close = stream.finish_request_input();
        assert!(matches!(close, RequestInputClose::Closed {
            remove_stream: false,
            terminal: TerminalDelivery::SendNow(_),
        }));
        assert!(matches!(stream.delivery, InputDelivery::Stopped));
    }
}
