use std::collections::VecDeque;
use std::collections::hash_map::Entry;
use std::future::Future;
use std::mem::{replace, size_of};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant as TokioInstant;

use super::StreamMap;
use super::deadline::DeadlineQueue;
use super::request::{HeaderLimits, PendingHeaders};
use super::writer::{H2WriterHandle, WriterState};
use crate::async_util::{TryPush, try_push};
use crate::bridge::{RequestBodyCounter, RequestInputShared};
use crate::h2::new_stream_map;
use crate::h2_frame::{self, StreamId, WindowIncrement};
use crate::hpack::Decoder;
use crate::http::body::{RequestBodyFinish, RequestBodyState};
use crate::runtime::{
    ConnectionContext, H2InputCredit, H2InputCreditQueue, ReleasedH2InputCredit, RequestAdmission,
    RequestSettlementCompletion, ShutdownState, StreamInput,
};

/// Generous for legitimate cancellation bursts (browser navigations, gRPC
/// deadline storms); rapid-reset attacks send thousands per second.
const RESET_RATE_LIMIT: u32 = 200;
const RESET_RATE_WINDOW: Duration = Duration::from_secs(10);
/// Bound one coalesced app-input chunk to 64 KiB even when the configured
/// inbound frame limit is larger. Tiny fragments copy into these chunks to
/// bound per-frame ownership metadata; larger fragments batch zero-copy.
const INPUT_CHUNK_MAX_BYTES: usize = 64 * 1024;
const INPUT_COPY_MAX_FRAGMENT_BYTES: usize = 128;

#[derive(Debug)]
pub(super) struct InboundStream {
    pub(super) delivery: InputDelivery,
    pub(super) counts_toward_read_timeout: bool,
    pub(super) receive_window: ReceiveWindowState,
    pub(super) state: StreamLifecycle,
    pub(super) body: RequestBodyState,
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
            Self::Open(tx) => match try_push(tx, value) {
                TryPush::Sent => {},
                TryPush::Full(value) => {
                    // Clone only after the bounded channel proves full. The
                    // assignment drops the original owner, so the transition
                    // preserves one sender without touching its refcount on
                    // the ordinary successful-delivery path.
                    let tx = tx.clone();
                    *self = Self::Backlogged(QueuedInput {
                        tx,
                        queue: VecDeque::from([value]),
                    });
                },
                TryPush::Closed(_) => *self = Self::Stopped,
            },
            Self::Backlogged(queued) => queued.queue.push_back(value),
            Self::Stopped => {},
        }
    }

    /// Deliver already-classified tiny input, compacting only after the app
    /// falls behind. Callers keep this branch out of the larger-frame path.
    fn push_compacting(&mut self, value: StreamInput, max_fragment_bytes: usize) {
        if let Self::Backlogged(queued) = self {
            queued.push_compacting(value, max_fragment_bytes);
        } else {
            self.push(value);
        }
    }

    /// Deliver non-tiny body input. Adjacent frames batch without copying so
    /// one receive consumes up to one bounded chunk of original segments.
    fn push_batched(
        &mut self,
        value: StreamInput,
        min_fragment_bytes: usize,
        max_batch_bytes: usize,
    ) {
        if let Self::Backlogged(queued) = self {
            queued.push_batched(value, min_fragment_bytes, max_batch_bytes);
        } else {
            self.push(value);
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
                // Backlog recovery is already the exceptional path. A
                // clone/drop pair here avoids a dummy enum state and the
                // impossible recovery branch it required.
                let tx = queued.tx.clone();
                *self = Self::Open(tx);
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

    /// Abandon delivery and enqueue a terminal event only when it fits
    /// immediately. A reset must never await a full application channel: the
    /// app may be suspended away from receive(), and cancellation is what
    /// releases that state. Dropping the final sender remains the fail-closed
    /// disconnect signal when queued body input already occupies the channel.
    pub(super) fn stop_with(&mut self, terminal: StreamInput) {
        let sender = match replace(self, Self::Stopped) {
            Self::Open(tx) => Some(tx),
            Self::Backlogged(queued) => Some(queued.tx),
            Self::Stopped => None,
        };
        if let Some(sender) = sender {
            let _ = sender.try_send(terminal);
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
    fn push_batched(
        &mut self,
        value: StreamInput,
        min_fragment_bytes: usize,
        max_batch_bytes: usize,
    ) {
        Self::push_batched_queue(&mut self.queue, value, min_fragment_bytes, max_batch_bytes);
    }

    pub(super) fn flush(&mut self) -> QueueFlush {
        while let Some(value) = self.queue.pop_front() {
            match try_push(&self.tx, value) {
                TryPush::Sent => {},
                TryPush::Full(value) => {
                    self.queue.push_front(value);
                    return QueueFlush::Pending;
                },
                TryPush::Closed(_) => return QueueFlush::Closed,
            }
        }
        QueueFlush::Drained
    }

    fn push_compacting(&mut self, value: StreamInput, max_fragment_bytes: usize) {
        Self::push_compacting_queue(&mut self.queue, value, max_fragment_bytes);
    }

    fn push_compacting_queue(
        queue: &mut VecDeque<StreamInput>,
        value: StreamInput,
        max_fragment_bytes: usize,
    ) {
        let Some(previous) = queue.back_mut() else {
            queue.push_back(value);
            return;
        };
        match previous.try_coalesce_data(value, max_fragment_bytes, INPUT_CHUNK_MAX_BYTES) {
            Ok(()) => {},
            Err(value) => queue.push_back(value),
        }
    }

    fn push_batched_queue(
        queue: &mut VecDeque<StreamInput>,
        value: StreamInput,
        min_fragment_bytes: usize,
        max_batch_bytes: usize,
    ) {
        let Some(previous) = queue.back_mut() else {
            queue.push_back(value);
            return;
        };
        match previous.try_batch_data(value, min_fragment_bytes, max_batch_bytes) {
            Ok(()) => {},
            Err(value) => queue.push_back(value),
        }
    }
}

#[derive(Debug)]
pub(super) struct ReceiveWindowState {
    recv_window: i64,
    pending_update: Option<WindowIncrement>,
}

/// State of a stream that is present in the connection's stream map.
///
/// `Idle` deliberately does not exist here: idle is a property of an absent
/// peer stream ID, not a lifecycle a materialized stream can enter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(super) enum StreamLifecycle {
    Open,
    RequestClosed,
    ResponseClosed,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(super) enum ReceiveState {
    Idle,
    Open,
    RequestClosed,
    ResponseClosed,
    Closed,
}

const _: () = assert!(size_of::<StreamLifecycle>() == 1);
const _: () = assert!(size_of::<ReceiveState>() == 1);

impl StreamLifecycle {
    pub(super) const fn request_is_closed(self) -> bool {
        matches!(self, Self::RequestClosed | Self::Closed)
    }
}

impl From<StreamLifecycle> for ReceiveState {
    fn from(state: StreamLifecycle) -> Self {
        match state {
            StreamLifecycle::Open => Self::Open,
            StreamLifecycle::RequestClosed => Self::RequestClosed,
            StreamLifecycle::ResponseClosed => Self::ResponseClosed,
            StreamLifecycle::Closed => Self::Closed,
        }
    }
}

impl ReceiveState {
    pub(super) fn is_idle(self) -> bool {
        self == Self::Idle
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClosedStreamDisposition {
    Keep,
    Retain,
    Remove,
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

/// HTTP/2 connection-preface progress. A connection cannot be both awaiting
/// and past the mandatory initial client SETTINGS frame, so represent that
/// phase directly instead of coupling meaning to a boolean.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(super) enum ClientPrefaceState {
    AwaitingSettings,
    Active,
}

const _: () = assert!(size_of::<ClientPrefaceState>() == 1);

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
    pub(super) tasks: &'a mut RequestTasks,
}

/// Connection-owned request tasks.
///
/// Tokio tasks are otherwise detached when their `JoinHandle` is dropped.  A
/// peer that closes the connection while an application is suspended outside
/// `receive()` would therefore leave the Python task alive indefinitely.  We
/// retain their join handles and receive completion ids through a bounded
/// channel: completed task allocations are reclaimed by Tokio, while this map
/// is reaped on every connection turn and aborted as one scope when the
/// connection state is dropped. The task budget is deliberately separate from
/// RFC stream concurrency: one retained generation may drain request input
/// while one protocol-active generation continues, but work stays bounded.
pub(super) struct RequestTasks {
    active: StreamMap<RequestTask>,
    limit: usize,
    completed_tx: mpsc::Sender<StreamId>,
    completed_rx: mpsc::Receiver<StreamId>,
}

struct RequestTask {
    task: Option<JoinHandle<()>>,
    cancellation: Option<RequestTaskCancellation>,
}

pub(super) enum RequestTaskCancellation {
    Immediate,
    AfterPendingReceive(Arc<RequestInputShared>),
}

impl RequestTask {
    async fn cancel(&mut self) {
        let Some(mut task) = self.task.take() else {
            return;
        };
        if let Some(RequestTaskCancellation::AfterPendingReceive(disconnect)) =
            self.cancellation.take()
            && let Some(pending) = disconnect.pending_resolution()
        {
            pending.wait().await;
        }
        task.abort();
        let _ = (&mut task).await;
    }
}

impl RequestTasks {
    fn new(max_concurrent_streams: usize) -> Self {
        let limit = max_concurrent_streams.saturating_mul(2).max(2);
        // A task owns one reserved slot until its completion id is consumed.
        // The second generation covers tasks synchronously cancelled and
        // joined between connection-turn reaps.
        let completion_capacity = limit.saturating_mul(2);
        let (completed_tx, completed_rx) = mpsc::channel(completion_capacity);
        Self {
            active: new_stream_map(limit),
            limit,
            completed_tx,
            completed_rx,
        }
    }

    pub(super) fn track_admission(&self, stream_id: StreamId, admission: &mut RequestAdmission) {
        admission.track_completion(self.reserve_completion(stream_id));
    }

    fn reserve_completion(&self, stream_id: StreamId) -> RequestSettlementCompletion {
        let permit = self
            .completed_tx
            .clone()
            .try_reserve_owned()
            .expect("completion capacity covers every admitted request task");
        RequestSettlementCompletion::new(stream_id, permit)
    }

    pub(super) fn spawn<F>(
        &mut self,
        stream_id: StreamId,
        cancellation: RequestTaskCancellation,
        future: F,
    ) where
        F: Future<Output = ()> + Send + 'static,
    {
        debug_assert!(self.active.len() < self.limit);
        let task = tokio::spawn(future);
        let previous = self.active.insert(stream_id, RequestTask {
            task: Some(task),
            cancellation: Some(cancellation),
        });
        debug_assert!(previous.is_none(), "HTTP/2 stream ids are never reused");
    }

    pub(super) async fn cancel(&mut self, stream_id: StreamId) {
        if let Some(task) = self.active.get_mut(&stream_id) {
            task.cancel().await;
        }
        // Settlement may already have won the race while cancellation was
        // awaited. Reap only completion ids; never release the budget merely
        // because the native wrapper was aborted while Python cleanup lives.
        self.reap_completed();
    }

    pub(super) async fn cancel_all(&mut self) {
        let active = replace(&mut self.active, new_stream_map(self.limit));
        for (_, mut task) in active {
            task.cancel().await;
        }
        self.reap_completed();
    }

    pub(super) fn reap_completed(&mut self) {
        while let Ok(stream_id) = self.completed_rx.try_recv() {
            self.active.remove(&stream_id);
        }
    }

    pub(super) async fn wait_for_completion(&mut self) {
        let stream_id = self
            .completed_rx
            .recv()
            .await
            .expect("RequestTasks retains the completion sender");
        self.active.remove(&stream_id);
        self.reap_completed();
    }

    pub(super) fn active_count(&self) -> usize {
        self.active.len()
    }

    pub(super) fn is_at_capacity(&self) -> bool {
        self.active_count() >= self.limit
    }
}

impl Drop for RequestTasks {
    fn drop(&mut self) {
        for (_, mut task) in self.active.drain() {
            if let Some(task) = task.task.take() {
                task.abort();
            }
        }
    }
}

pub(super) struct H2ConnectionState<R, W> {
    pub(super) reader: h2_frame::BufferedConnectionReader<R>,
    pub(super) connection: H2WriterHandle,
    pub(super) writer: WriterState<W>,
    pub(super) context: ConnectionContext,
    pub(super) secure: bool,
    pub(super) shutdown: watch::Receiver<ShutdownState>,
    pub(super) decoder: Decoder,
    pub(super) streams: StreamMap<InboundStream>,
    /// Protocol-closed streams retained only while their ASGI input backlog
    /// drains. RFC 9113 excludes these from SETTINGS_MAX_CONCURRENT_STREAMS.
    retained_closed_streams: usize,
    pub(super) pending_headers: Option<PendingHeaders>,
    pub(super) last_client_stream_id: Option<StreamId>,
    pub(super) connection_window: ReceiveWindowState,
    pub(super) local_max_frame_size: usize,
    pub(super) client_preface: ClientPrefaceState,
    pub(super) drain_state: ConnectionDrainState,
    pub(super) reset_guard: RapidResetGuard,
    pub(super) input_flow: Arc<H2InputCreditQueue>,
    pub(super) released_input_credits: Vec<ReleasedH2InputCredit>,
    pub(super) request_deadlines: DeadlineQueue<RequestInputDeadlineKey>,
    pub(super) request_tasks: RequestTasks,
}

pub(super) enum RequestInputClose {
    ContentLengthMismatch,
    Closed,
}

impl ReceiveWindowState {
    pub(super) fn new(initial_window: u32) -> Self {
        Self {
            recv_window: i64::from(initial_window),
            pending_update: None,
        }
    }

    pub(super) fn receive(&mut self, len: u32) -> Result<(), ()> {
        self.recv_window -= i64::from(len);
        if self.recv_window < 0 {
            return Err(());
        }
        Ok(())
    }

    pub(super) const fn release(&mut self, len: u32) {
        if len == 0 {
            return;
        }
        let accumulated = match self.pending_update {
            Some(pending) => pending
                .get()
                .checked_add(len)
                .expect("released credit cannot exceed the receive-window charge"),
            None => len,
        };
        self.pending_update = Some(
            WindowIncrement::new(accumulated)
                .expect("released credit cannot exceed the HTTP/2 flow-control maximum"),
        );
    }

    pub(super) fn take_update(&mut self, threshold: u32) -> Option<WindowIncrement> {
        let increment = self
            .pending_update
            .filter(|increment| increment.get() >= threshold)?;
        self.pending_update = None;
        self.recv_window += i64::from(increment.get());
        Some(increment)
    }
}

const _: () = assert!(size_of::<ReceiveWindowState>() == 16);

impl InboundStream {
    fn closed_disposition(&self, was_closed: bool) -> ClosedStreamDisposition {
        if self.state != StreamLifecycle::Closed {
            ClosedStreamDisposition::Keep
        } else if !self.delivery.has_backlog() {
            ClosedStreamDisposition::Remove
        } else if was_closed {
            ClosedStreamDisposition::Keep
        } else {
            ClosedStreamDisposition::Retain
        }
    }

    pub(super) fn new(
        input: Option<mpsc::Sender<StreamInput>>,
        counts_toward_read_timeout: bool,
        end_stream: bool,
        expected_content_length: Option<u64>,
        body_bytes: Option<RequestBodyCounter>,
        max_request_body_size: Option<u64>,
        initial_window: u32,
    ) -> Self {
        let mut delivery = InputDelivery::new(input);
        if end_stream {
            delivery.push(StreamInput::EndStream);
        }
        Self {
            delivery,
            counts_toward_read_timeout,
            receive_window: ReceiveWindowState::new(initial_window),
            state: if end_stream {
                StreamLifecycle::RequestClosed
            } else {
                StreamLifecycle::Open
            },
            body: RequestBodyState::new(expected_content_length, body_bytes, max_request_body_size),
        }
    }

    pub(super) fn push_body_data(&mut self, body: Bytes, credit: H2InputCredit) {
        let body_len = body.len();
        let input = StreamInput::h2_data(body, credit);
        if body_len <= INPUT_COPY_MAX_FRAGMENT_BYTES {
            self.delivery
                .push_compacting(input, INPUT_COPY_MAX_FRAGMENT_BYTES);
        } else if body_len <= INPUT_CHUNK_MAX_BYTES / 2 {
            self.delivery
                .push_batched(input, INPUT_COPY_MAX_FRAGMENT_BYTES, INPUT_CHUNK_MAX_BYTES);
        } else {
            self.delivery.push(input);
        }
    }

    pub(super) fn mark_request_closed(&mut self) -> bool {
        self.state = match self.state {
            StreamLifecycle::Open => StreamLifecycle::RequestClosed,
            StreamLifecycle::ResponseClosed => StreamLifecycle::Closed,
            state => state,
        };
        self.state == StreamLifecycle::Closed
    }

    pub(super) fn mark_response_closed(&mut self) -> bool {
        self.state = match self.state {
            StreamLifecycle::Open => StreamLifecycle::ResponseClosed,
            StreamLifecycle::RequestClosed => StreamLifecycle::Closed,
            state => state,
        };
        self.state == StreamLifecycle::Closed
    }

    fn finish_request_input(&mut self) -> RequestInputClose {
        if self.body.finish() == RequestBodyFinish::ContentLengthMismatch {
            return RequestInputClose::ContentLengthMismatch;
        }

        self.mark_request_closed();
        self.delivery.push(StreamInput::EndStream);
        RequestInputClose::Closed
    }
}

impl<R, W> H2ConnectionState<R, W> {
    pub(super) fn new(
        reader: h2_frame::BufferedConnectionReader<R>,
        connection: H2WriterHandle,
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
            decoder: Decoder::new(h2_frame::DEFAULT_HEADER_TABLE_SIZE),
            streams: new_stream_map(stream_capacity),
            retained_closed_streams: 0,
            pending_headers: None,
            last_client_stream_id: None,
            connection_window: ReceiveWindowState::new(initial_connection_window),
            local_max_frame_size,
            client_preface: ClientPrefaceState::AwaitingSettings,
            drain_state,
            reset_guard: RapidResetGuard::new(),
            input_flow: Arc::default(),
            released_input_credits: Vec::new(),
            request_deadlines: DeadlineQueue::default(),
            request_tasks: RequestTasks::new(stream_capacity),
        }
    }

    pub(super) fn protocol_active_stream_count(&self) -> usize {
        self.streams
            .len()
            .checked_sub(self.retained_closed_streams)
            .expect("retained closed stream accounting cannot exceed the stream map")
            + usize::from(self.pending_headers.is_some())
    }

    pub(super) fn wire_is_idle(&self) -> bool {
        self.protocol_active_stream_count() == 0
    }

    fn has_outstanding_local_work(&self) -> bool {
        !self.streams.is_empty()
            || self.pending_headers.is_some()
            || self.request_tasks.active_count() != 0
    }

    pub(super) fn header_limits(&self) -> HeaderLimits {
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

    pub(super) fn next_request_input_deadline(&mut self) -> Option<RequestInputDeadline> {
        self.request_deadlines
            .next()
            .map(|(key, instant)| key.with_instant(instant))
    }

    pub(super) fn pop_expired_request_input_deadline(
        &mut self,
        now: TokioInstant,
    ) -> Option<RequestInputDeadline> {
        self.request_deadlines
            .pop_expired(now)
            .map(|(key, instant)| key.with_instant(instant))
    }

    pub(super) fn refresh_header_deadline(&mut self, stream_id: StreamId) {
        let key = RequestInputDeadlineKey::headers(stream_id);
        if let Some(timeout) = self.context.config.timeout_request_header {
            self.request_deadlines
                .schedule(key, TokioInstant::now() + timeout);
        } else {
            self.request_deadlines.cancel(key);
        }
    }

    pub(super) fn cancel_header_deadline(&mut self, stream_id: StreamId) {
        self.request_deadlines
            .cancel(RequestInputDeadlineKey::headers(stream_id));
    }

    pub(super) fn refresh_body_deadline(&mut self, stream_id: StreamId) {
        let key = RequestInputDeadlineKey::body(stream_id);
        let should_schedule = self.streams.get(&stream_id).is_some_and(|stream| {
            stream.counts_toward_read_timeout && !stream.state.request_is_closed()
        });
        match (
            should_schedule,
            self.context.config.timeout_request_body_idle,
        ) {
            (true, Some(timeout)) => self
                .request_deadlines
                .schedule(key, TokioInstant::now() + timeout),
            _ => self.request_deadlines.cancel(key),
        }
    }

    pub(super) fn receive_state(&self, stream_id: StreamId) -> ReceiveState {
        self.streams.get(&stream_id).map_or_else(
            || missing_receive_state(stream_id, self.last_client_stream_id),
            |stream| stream.state.into(),
        )
    }

    pub(super) fn apply_response_close(&mut self, stream_id: StreamId) {
        if let Entry::Occupied(mut entry) = self.streams.entry(stream_id) {
            let was_closed = entry.get().state == StreamLifecycle::Closed;
            entry.get_mut().mark_response_closed();
            match entry.get().closed_disposition(was_closed) {
                ClosedStreamDisposition::Keep => {},
                ClosedStreamDisposition::Retain => self.retained_closed_streams += 1,
                ClosedStreamDisposition::Remove => {
                    entry.remove();
                },
            }
        }
    }

    pub(super) fn finish_request_input(
        &mut self,
        stream_id: StreamId,
    ) -> Option<RequestInputClose> {
        let result = match self.streams.entry(stream_id) {
            Entry::Occupied(mut entry) => {
                let was_closed = entry.get().state == StreamLifecycle::Closed;
                let close = entry.get_mut().finish_request_input();
                match entry.get().closed_disposition(was_closed) {
                    ClosedStreamDisposition::Keep => {},
                    ClosedStreamDisposition::Retain => self.retained_closed_streams += 1,
                    ClosedStreamDisposition::Remove => {
                        entry.remove();
                    },
                }
                Some(close)
            },
            Entry::Vacant(_) => None,
        };
        self.request_deadlines
            .cancel(RequestInputDeadlineKey::body(stream_id));
        result
    }

    pub(super) fn remove_stream(&mut self, stream_id: StreamId) -> Option<InboundStream> {
        self.request_deadlines
            .cancel(RequestInputDeadlineKey::body(stream_id));
        let stream = self.streams.remove(&stream_id)?;
        if stream.state == StreamLifecycle::Closed {
            self.retained_closed_streams = self
                .retained_closed_streams
                .checked_sub(1)
                .expect("every retained closed stream is counted exactly once");
        }
        Some(stream)
    }

    pub(super) fn should_stop(&self) -> bool {
        if self.drain_state == ConnectionDrainState::Accepting {
            return false;
        }
        !self.has_outstanding_local_work()
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

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub(super) struct RequestInputDeadlineKey(NonZeroU32);

impl RequestInputDeadlineKey {
    const BODY_TAG: u32 = 1 << 31;

    pub(super) const fn headers(stream_id: StreamId) -> Self {
        Self(NonZeroU32::new(stream_id.get()).expect("a stream identifier is nonzero"))
    }

    pub(super) const fn body(stream_id: StreamId) -> Self {
        let raw = Self::BODY_TAG | stream_id.get();
        Self(NonZeroU32::new(raw).expect("a tagged stream identifier is nonzero"))
    }

    const fn with_instant(self, instant: TokioInstant) -> RequestInputDeadline {
        let raw = self.0.get();
        let stream_id = StreamId::new(raw & h2_frame::STREAM_ID_MASK)
            .expect("a deadline key contains a valid stream identifier");
        if raw & Self::BODY_TAG == 0 {
            RequestInputDeadline::Headers(stream_id, instant)
        } else {
            RequestInputDeadline::Body(stream_id, instant)
        }
    }
}

// Hash derives through the one wrapped integer, so the identity hasher is
// collision-free for this packed key just as it is for `StreamId`.
impl nohash_hasher::IsEnabled for RequestInputDeadlineKey {}

const _: () = assert!(size_of::<RequestInputDeadlineKey>() == size_of::<u32>());

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
    if stream_id.is_client_initiated() && last_client_stream_id.is_none_or(|last| stream_id > last)
    {
        ReceiveState::Idle
    } else {
        ReceiveState::Closed
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::num::NonZeroU32;
    use std::sync::Arc;

    use bytes::Bytes;
    use tokio::sync::{mpsc, oneshot};

    use super::{
        ClosedStreamDisposition, INPUT_CHUNK_MAX_BYTES, INPUT_COPY_MAX_FRAGMENT_BYTES,
        InboundStream, InputDelivery, ReceiveWindowState, RequestInputClose, RequestInputDeadline,
        RequestInputDeadlineKey, RequestTaskCancellation, RequestTasks,
    };
    use crate::h2_frame::{STREAM_ID_MASK, StreamId, WindowIncrement};
    use crate::runtime::{H2InputCreditQueue, StreamInput};

    fn data(byte: &'static [u8]) -> StreamInput {
        StreamInput::data(Bytes::from_static(byte))
    }

    fn assert_data(input: StreamInput, expected: &[u8]) {
        match input {
            StreamInput::Data { body, credit: None } => assert_eq!(body.as_ref(), expected),
            other => panic!("expected data input, got {other:?}"),
        }
    }

    fn assert_buffered_data(input: StreamInput, expected: &[u8]) {
        match input {
            StreamInput::BufferedData { body, credit: None } => {
                assert_eq!(body.as_ref(), expected);
            },
            other => panic!("expected buffered data input, got {other:?}"),
        }
    }

    fn assert_data_batch(input: StreamInput, expected: &[&[u8]]) {
        match input {
            StreamInput::DataBatch {
                bodies,
                body_bytes,
                credit: None,
            } => {
                assert_eq!(bodies.len(), expected.len());
                assert_eq!(
                    body_bytes,
                    expected.iter().map(|body| body.len()).sum::<usize>()
                );
                for (body, expected) in bodies.iter().zip(expected) {
                    assert_eq!(body.as_ref(), *expected);
                }
            },
            other => panic!("expected HTTP data batch, got {other:?}"),
        }
    }

    #[test]
    fn packed_request_deadline_key_round_trips_kind_and_stream_id() {
        let instant = tokio::time::Instant::now();
        for raw in [1, STREAM_ID_MASK] {
            let stream_id = StreamId::new(raw).expect("test stream identifier is valid");
            let headers = RequestInputDeadlineKey::headers(stream_id);
            let body = RequestInputDeadlineKey::body(stream_id);
            assert!(headers < body, "the body tag occupies the high bit");
            assert!(matches!(
                headers.with_instant(instant),
                RequestInputDeadline::Headers(actual, at)
                    if actual == stream_id && at == instant
            ));
            assert!(matches!(
                body.with_instant(instant),
                RequestInputDeadline::Body(actual, at)
                    if actual == stream_id && at == instant
            ));
        }
    }
    #[tokio::test]
    async fn request_task_scope_aborts_every_live_task_on_drop() {
        struct DropSignal(Option<oneshot::Sender<()>>);

        impl Drop for DropSignal {
            fn drop(&mut self) {
                if let Some(done) = self.0.take() {
                    let _ = done.send(());
                }
            }
        }

        let stream_id = StreamId::new(1).expect("non-zero stream id");
        let (started_tx, started_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let mut tasks = RequestTasks::new(1);
        tasks.spawn(stream_id, RequestTaskCancellation::Immediate, async move {
            let _drop_signal = DropSignal(Some(dropped_tx));
            let _ = started_tx.send(());
            pending::<()>().await;
        });
        started_rx.await.expect("request task started");
        assert_eq!(tasks.active_count(), 1);

        drop(tasks);
        dropped_rx
            .await
            .expect("request task was aborted and dropped");
    }

    #[tokio::test]
    async fn request_task_budget_allows_one_bounded_retained_generation() {
        let first = StreamId::new(1).unwrap();
        let second = StreamId::new(3).unwrap();
        let mut tasks = RequestTasks::new(1);
        let first_settlement = tasks.reserve_completion(first);
        let _second_settlement = tasks.reserve_completion(second);
        tasks.spawn(first, RequestTaskCancellation::Immediate, pending());
        tasks.spawn(second, RequestTaskCancellation::Immediate, pending());
        assert_eq!(tasks.active_count(), 2);
        assert!(tasks.is_at_capacity());

        tasks.cancel(first).await;
        tasks.reap_completed();
        assert_eq!(
            tasks.active_count(),
            2,
            "native cancellation cannot bypass the Python-settlement budget"
        );

        drop(first_settlement);
        tasks.reap_completed();
        assert_eq!(tasks.active_count(), 1);
        assert!(!tasks.is_at_capacity());
    }

    #[test]
    fn closed_stream_retention_is_identical_for_both_close_orders() {
        fn backlogged_stream() -> (InboundStream, mpsc::Receiver<StreamInput>) {
            let (tx, rx) = mpsc::channel(1);
            let mut stream = InboundStream::new(Some(tx), true, false, None, None, None, 0xFFFF);
            stream.delivery.push(data(b"a"));
            stream.delivery.push(data(b"b"));
            assert!(stream.delivery.has_backlog());
            (stream, rx)
        }

        let (mut response_first, _rx) = backlogged_stream();
        assert!(!response_first.mark_response_closed());
        let was_closed = response_first.state == super::StreamLifecycle::Closed;
        response_first.finish_request_input();
        assert_eq!(
            response_first.closed_disposition(was_closed),
            ClosedStreamDisposition::Retain,
        );

        let (mut request_first, _rx) = backlogged_stream();
        request_first.finish_request_input();
        let was_closed = request_first.state == super::StreamLifecycle::Closed;
        request_first.mark_response_closed();
        assert_eq!(
            request_first.closed_disposition(was_closed),
            ClosedStreamDisposition::Retain,
        );
        assert_eq!(
            request_first.closed_disposition(true),
            ClosedStreamDisposition::Keep,
            "an already-retained closed stream is never counted twice",
        );
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
        let RequestInputClose::Closed = close else {
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

    #[tokio::test]
    async fn reset_never_waits_for_a_full_application_channel() {
        let (tx, mut rx) = mpsc::channel(1);
        let mut delivery = InputDelivery::new(Some(tx));
        delivery.push(data(b"in-channel"));
        delivery.push(data(b"backlog"));
        assert!(delivery.has_backlog());

        delivery.stop_with(StreamInput::Reset(crate::h2_frame::ErrorCode::CANCEL));
        assert!(matches!(delivery, InputDelivery::Stopped));
        assert_data(
            rx.recv()
                .await
                .expect("already queued body remains ordered"),
            b"in-channel",
        );
        assert!(
            rx.recv().await.is_none(),
            "a full channel fails closed by disconnecting instead of blocking reset"
        );
    }

    #[tokio::test]
    async fn backlogged_data_coalesces_without_delaying_open_delivery() {
        let (tx, mut rx) = mpsc::channel(1);
        let mut delivery = InputDelivery::new(Some(tx));

        delivery.push_compacting(data(b"a"), INPUT_COPY_MAX_FRAGMENT_BYTES);
        assert!(matches!(delivery, InputDelivery::Open(_)));
        delivery.push_compacting(data(b"b"), INPUT_COPY_MAX_FRAGMENT_BYTES);
        delivery.push_compacting(data(b"c"), INPUT_COPY_MAX_FRAGMENT_BYTES);
        delivery.push_compacting(data(b"d"), INPUT_COPY_MAX_FRAGMENT_BYTES);
        delivery.push_compacting(data(b"e"), INPUT_COPY_MAX_FRAGMENT_BYTES);

        let InputDelivery::Backlogged(queued) = &delivery else {
            panic!("full app channel must create a backlog");
        };
        assert_eq!(queued.queue.len(), 1);

        assert_data(rx.recv().await.expect("open-path body"), b"a");
        assert!(delivery.flush());
        assert_buffered_data(rx.recv().await.expect("coalesced backlog"), b"bcde");
    }

    #[tokio::test]
    async fn backlogged_http_medium_frames_batch_without_copying() {
        let (tx, mut rx) = mpsc::channel(1);
        let mut delivery = InputDelivery::new(Some(tx));
        let first = Bytes::from(vec![b'a'; 8 * 1024]);
        let second = Bytes::from(vec![b'b'; 8 * 1024]);
        let third = Bytes::from(vec![b'c'; 8 * 1024]);

        delivery.push_batched(
            data(b"open"),
            INPUT_COPY_MAX_FRAGMENT_BYTES,
            INPUT_CHUNK_MAX_BYTES,
        );
        delivery.push_batched(
            StreamInput::data(first.clone()),
            INPUT_COPY_MAX_FRAGMENT_BYTES,
            INPUT_CHUNK_MAX_BYTES,
        );
        delivery.push_batched(
            StreamInput::data(second.clone()),
            INPUT_COPY_MAX_FRAGMENT_BYTES,
            INPUT_CHUNK_MAX_BYTES,
        );
        delivery.push_batched(
            StreamInput::data(third.clone()),
            INPUT_COPY_MAX_FRAGMENT_BYTES,
            INPUT_CHUNK_MAX_BYTES,
        );

        let InputDelivery::Backlogged(queued) = &delivery else {
            panic!("full app channel must create a backlog");
        };
        assert_eq!(queued.queue.len(), 1);
        assert_data(rx.recv().await.expect("open-path body"), b"open");
        assert!(delivery.flush());
        assert_data_batch(rx.recv().await.expect("batched backlog"), &[
            first.as_ref(),
            second.as_ref(),
            third.as_ref(),
        ]);
    }

    #[test]
    fn websocket_backlog_copies_tiny_and_batches_medium_segments() {
        let (_rx_guard, mut delivery) = {
            let (tx, rx) = mpsc::channel(1);
            (rx, InputDelivery::new(Some(tx)))
        };
        delivery.push(data(b"open"));
        delivery.push_compacting(data(b"ws-a"), INPUT_COPY_MAX_FRAGMENT_BYTES);
        delivery.push_compacting(data(b"ws-b"), INPUT_COPY_MAX_FRAGMENT_BYTES);
        let InputDelivery::Backlogged(queued) = &delivery else {
            panic!("full app channel must create a backlog");
        };
        assert_eq!(queued.queue.len(), 1);
        assert!(matches!(
            queued.queue.front(),
            Some(StreamInput::BufferedData { body, .. }) if body.as_ref() == b"ws-aws-b"
        ));

        let (tx, _rx) = mpsc::channel(1);
        let mut delivery = InputDelivery::new(Some(tx));
        let first = Bytes::from(vec![b'a'; 8 * 1024]);
        let second = Bytes::from(vec![b'b'; 8 * 1024]);
        delivery.push(data(b"open"));
        delivery.push_batched(
            StreamInput::data(first.clone()),
            INPUT_COPY_MAX_FRAGMENT_BYTES,
            INPUT_CHUNK_MAX_BYTES,
        );
        delivery.push_batched(
            StreamInput::data(second.clone()),
            INPUT_COPY_MAX_FRAGMENT_BYTES,
            INPUT_CHUNK_MAX_BYTES,
        );
        let InputDelivery::Backlogged(queued) = &delivery else {
            panic!("full app channel must create a backlog");
        };
        assert_eq!(queued.queue.len(), 1);
        let input = queued.queue.front().expect("batched input");
        let StreamInput::DataBatch {
            bodies, body_bytes, ..
        } = input
        else {
            panic!("medium segments must retain zero-copy ownership");
        };
        assert_eq!(*body_bytes, first.len() + second.len());
        assert_eq!(bodies[0].as_ptr(), first.as_ptr());
        assert_eq!(bodies[1].as_ptr(), second.as_ptr());
    }

    async fn fragmented_backlog_is_byte_bounded(counts_toward_read_timeout: bool) {
        const FRAME_COUNT: usize = 2 * 1024 * 1024;

        let flow = Arc::new(H2InputCreditQueue::default());
        let stream_id = StreamId::new(1).expect("non-zero stream id");
        let (tx, mut rx) = mpsc::channel(1);
        let mut stream = InboundStream::new(
            Some(tx),
            counts_toward_read_timeout,
            false,
            None,
            None,
            None,
            0x7FFF_FFFF,
        );

        for index in 0..FRAME_COUNT {
            let body = if index & 1 == 0 {
                Bytes::from_static(b"a")
            } else {
                Bytes::from_static(b"b")
            };
            stream.push_body_data(body, flow.credit(stream_id, NonZeroU32::new(1).unwrap()));
        }
        assert!(!flow.has_pending(), "queued input must retain every credit");

        let InputDelivery::Backlogged(queued) = &stream.delivery else {
            panic!("a stalled one-slot app channel must have a backlog");
        };
        let expected_nodes = (FRAME_COUNT - 1).div_ceil(super::INPUT_CHUNK_MAX_BYTES);
        assert_eq!(queued.queue.len(), expected_nodes);
        assert!(queued.queue.len() <= 32, "two MiB needs at most 32 chunks");

        stream.delivery.push(StreamInput::EndStream);
        let mut consumed = 0;
        loop {
            let input = rx.recv().await.expect("input remains connected");
            match input {
                StreamInput::Data { body, credit } => {
                    for byte in body {
                        assert_eq!(byte, if consumed & 1 == 0 { b'a' } else { b'b' });
                        consumed += 1;
                    }
                    credit.expect("H2 data retains credit").release();
                },
                StreamInput::BufferedData { body, credit } => {
                    for byte in body {
                        assert_eq!(byte, if consumed & 1 == 0 { b'a' } else { b'b' });
                        consumed += 1;
                    }
                    credit.expect("H2 data retains merged credit").release();
                },
                StreamInput::DataBatch { bodies, credit, .. } => {
                    for body in bodies {
                        for byte in body {
                            assert_eq!(byte, if consumed & 1 == 0 { b'a' } else { b'b' });
                            consumed += 1;
                        }
                    }
                    credit.expect("H2 data retains merged credit").release();
                },
                StreamInput::EndStream => break,
                StreamInput::Reset(code) => panic!("unexpected reset: {code}"),
            }
            stream.delivery.flush();
        }
        assert_eq!(consumed, FRAME_COUNT, "coalescing preserves every byte");
        assert!(!stream.delivery.has_backlog());

        let mut released = Vec::new();
        flow.drain_into(&mut released);
        assert_eq!(released.len(), 1, "same-stream releases coalesce");
        assert_eq!(released[0].stream_id, stream_id);
        assert_eq!(released[0].bytes.get() as usize, FRAME_COUNT);
        assert!(!flow.has_pending());
    }

    #[tokio::test]
    async fn fragmented_http_backlog_has_bounded_nodes_and_exact_credit() {
        fragmented_backlog_is_byte_bounded(true).await;
    }

    #[tokio::test]
    async fn fragmented_websocket_backlog_has_bounded_nodes_and_exact_credit() {
        fragmented_backlog_is_byte_bounded(false).await;
    }

    /// A fully protocol-closed stream remains addressable by released-credit
    /// notifications until its terminal backlog has drained.
    #[tokio::test]
    async fn fully_closed_stream_retains_terminal_backlog() {
        let (tx, mut rx) = mpsc::channel(1);
        let mut stream = InboundStream::new(Some(tx), true, false, None, None, None, 0xFFFF);
        stream.mark_response_closed();

        stream.delivery.push(data(b"a"));
        stream.delivery.push(data(b"b"));

        let close = stream.finish_request_input();
        let RequestInputClose::Closed = close else {
            panic!("fully closed stream must stay until its backlog drains");
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

    /// Empty-backlog completion uses the same nonblocking delivery path.
    #[tokio::test]
    async fn idle_delivery_completion_queues_terminal_in_channel() {
        let (tx, mut rx) = mpsc::channel(4);
        let mut stream = InboundStream::new(Some(tx), true, false, None, None, None, 0xFFFF);

        let close = stream.finish_request_input();
        assert!(matches!(close, RequestInputClose::Closed));
        assert!(matches!(stream.delivery, InputDelivery::Open(_)));
        assert!(matches!(rx.recv().await, Some(StreamInput::EndStream)));
    }

    #[test]
    fn receive_window_replenishes_only_released_bytes() {
        let mut window = ReceiveWindowState::new(100);
        window.receive(60).expect("frame fits the window");
        assert!(window.take_update(50).is_none());

        window.release(49);
        assert!(window.take_update(50).is_none());
        window.release(1);
        assert_eq!(window.take_update(50).map(WindowIncrement::get), Some(50));
        assert!(window.receive(91).is_err());
    }

    #[test]
    fn h2_credit_releases_exactly_once() {
        let flow = Arc::new(H2InputCreditQueue::default());
        let stream_id = StreamId::new(1).expect("non-zero stream id");
        flow.credit(stream_id, NonZeroU32::new(7).unwrap())
            .release();

        assert!(flow.has_pending());
        let mut released = Vec::new();
        flow.drain_into(&mut released);
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].stream_id, stream_id);
        assert_eq!(released[0].bytes.get(), 7);
        assert!(!flow.has_pending());
    }

    #[test]
    fn receiver_close_releases_channel_and_backlog_credit() {
        let flow = Arc::new(H2InputCreditQueue::default());
        let stream_id = StreamId::new(1).expect("non-zero stream id");
        let (tx, rx) = mpsc::channel(1);
        let mut delivery = InputDelivery::new(Some(tx));
        delivery.push(StreamInput::h2_data(
            Bytes::from_static(b"a"),
            flow.credit(stream_id, NonZeroU32::new(4).unwrap()),
        ));
        delivery.push(StreamInput::h2_data(
            Bytes::from_static(b"b"),
            flow.credit(stream_id, NonZeroU32::new(5).unwrap()),
        ));
        assert!(delivery.has_backlog());

        drop(rx);
        assert!(flow.has_pending());
        assert!(delivery.flush());
        assert!(matches!(delivery, InputDelivery::Stopped));

        let mut released = Vec::new();
        flow.drain_into(&mut released);
        assert_eq!(
            released
                .iter()
                .map(|release| release.bytes.get())
                .sum::<u32>(),
            9,
        );
    }
}
