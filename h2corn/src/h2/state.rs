use std::collections::VecDeque;
use std::collections::hash_map::Entry;
use std::future::Future;
use std::mem::{replace, size_of};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{mpsc, watch};
use tokio::task::AbortHandle;
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
    ConnectionContext, H2InputCredit, H2InputCreditQueue, ReleasedH2InputCredit, ShutdownState,
    StreamInput,
};

/// Generous for legitimate cancellation bursts (browser navigations, gRPC
/// deadline storms); rapid-reset attacks send thousands per second.
const RESET_RATE_LIMIT: u32 = 200;
const RESET_RATE_WINDOW: Duration = Duration::from_secs(10);
/// Bound a batched ASGI body event to 64 KiB even when the configured inbound
/// frame limit is larger. Batching is restricted to input that is already
/// backlogged, so this costs neither prompt delivery nor a payload copy.
const HTTP_BODY_BATCH_MAX_BYTES: usize = 64 * 1024;

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

    /// Deliver HTTP body input, pairing adjacent frames only after the bounded
    /// app channel is already full. The common keeping-up path is identical to
    /// `push`; a slow app gets fewer Python receive events without waiting for
    /// another frame or concatenating payload storage.
    pub(super) fn push_http_body(&mut self, value: StreamInput) {
        if let Self::Backlogged(queued) = self
            && let Some(previous) = queued.queue.back_mut()
        {
            match previous.try_batch_http(value, HTTP_BODY_BATCH_MAX_BYTES) {
                Ok(()) => return,
                Err(value) => {
                    queued.queue.push_back(value);
                    return;
                },
            }
        }
        self.push(value);
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

    /// Abandon delivery, returning the sender for a best-effort terminal
    /// message (reset paths drop any backlog deliberately).
    pub(super) fn take_sender(&mut self) -> Option<mpsc::Sender<StreamInput>> {
        match replace(self, Self::Stopped) {
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
/// retain only the cheap abort handles and receive completion ids through a
/// bounded channel: completed task allocations are reclaimed by Tokio, while
/// this map is reaped on every connection turn and aborted as one scope when
/// the connection state is dropped.
pub(super) struct RequestTasks {
    active: StreamMap<RequestTask>,
    completed_tx: mpsc::Sender<StreamId>,
    completed_rx: mpsc::Receiver<StreamId>,
}

struct RequestTask {
    abort: AbortHandle,
    cancellation: RequestTaskCancellation,
}

pub(super) enum RequestTaskCancellation {
    Immediate,
    AfterPendingReceive(Arc<RequestInputShared>),
}

impl RequestTask {
    async fn cancel(self) {
        if let RequestTaskCancellation::AfterPendingReceive(disconnect) = self.cancellation
            && let Some(pending) = disconnect.pending_resolution()
        {
            pending.wait().await;
        }
        self.abort.abort();
    }
}

struct RequestTaskCompletion {
    stream_id: StreamId,
    completed: mpsc::Sender<StreamId>,
}

impl Drop for RequestTaskCompletion {
    fn drop(&mut self) {
        // The channel covers two generations of the maximum concurrent
        // request count. Completion is produced once per active task and the
        // connection drains it before ingesting another peer frame, so Full
        // is only possible while the connection itself is being dropped.
        let _ = self.completed.try_send(self.stream_id);
    }
}

impl RequestTasks {
    fn new(max_concurrent_streams: usize) -> Self {
        // One connection turn reaps before ingesting at most one peer frame.
        // Two generations therefore cover every live task plus tasks that
        // completed during the current turn, without an unbounded queue.
        let completion_capacity = max_concurrent_streams.saturating_mul(2).max(2);
        let (completed_tx, completed_rx) = mpsc::channel(completion_capacity);
        Self {
            active: new_stream_map(max_concurrent_streams),
            completed_tx,
            completed_rx,
        }
    }

    pub(super) fn spawn<F>(
        &mut self,
        stream_id: StreamId,
        cancellation: RequestTaskCancellation,
        future: F,
    ) where
        F: Future<Output = ()> + Send + 'static,
    {
        let completion = RequestTaskCompletion {
            stream_id,
            completed: self.completed_tx.clone(),
        };
        let task = tokio::spawn(async move {
            let _completion = completion;
            future.await;
        });
        let previous = self.active.insert(stream_id, RequestTask {
            abort: task.abort_handle(),
            cancellation,
        });
        debug_assert!(previous.is_none(), "HTTP/2 stream ids are never reused");
    }

    pub(super) async fn cancel(&mut self, stream_id: StreamId) {
        if let Some(task) = self.active.remove(&stream_id) {
            task.cancel().await;
        }
    }

    pub(super) async fn cancel_all(&mut self) {
        let capacity = self.active.capacity();
        let active = replace(&mut self.active, new_stream_map(capacity));
        for (_, task) in active {
            task.cancel().await;
        }
        self.reap_completed();
    }

    pub(super) fn reap_completed(&mut self) {
        while let Ok(stream_id) = self.completed_rx.try_recv() {
            self.active.remove(&stream_id);
        }
    }

    #[cfg(test)]
    pub(super) fn active_count(&self) -> usize {
        self.active.len()
    }
}

impl Drop for RequestTasks {
    fn drop(&mut self) {
        for (_, task) in self.active.drain() {
            task.abort.abort();
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
    Closed { remove_stream: bool },
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
        let input = StreamInput::h2_data(body, credit);
        if self.counts_toward_read_timeout {
            self.delivery.push_http_body(input);
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

        let fully_closed = self.mark_request_closed();
        self.delivery.push(StreamInput::EndStream);
        RequestInputClose::Closed {
            remove_stream: fully_closed && !self.delivery.has_backlog(),
        }
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

    pub(super) fn active_stream_count(&self) -> usize {
        self.streams.len() + usize::from(self.pending_headers.is_some())
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
        let key = RequestInputDeadlineKey::Headers(stream_id);
        if let Some(timeout) = self.context.config.timeout_request_header {
            self.request_deadlines
                .schedule(key, TokioInstant::now() + timeout);
        } else {
            self.request_deadlines.cancel(key);
        }
    }

    pub(super) fn cancel_header_deadline(&mut self, stream_id: StreamId) {
        self.request_deadlines
            .cancel(RequestInputDeadlineKey::Headers(stream_id));
    }

    pub(super) fn refresh_body_deadline(&mut self, stream_id: StreamId) {
        let key = RequestInputDeadlineKey::Body(stream_id);
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
        if let Entry::Occupied(mut entry) = self.streams.entry(stream_id)
            && entry.get_mut().mark_response_closed()
            && !entry.get().delivery.has_backlog()
        {
            entry.remove();
        }
    }

    pub(super) fn finish_request_input(
        &mut self,
        stream_id: StreamId,
    ) -> Option<RequestInputClose> {
        let result = match self.streams.entry(stream_id) {
            Entry::Occupied(mut entry) => {
                let close = entry.get_mut().finish_request_input();
                if matches!(close, RequestInputClose::Closed {
                    remove_stream: true,
                }) {
                    entry.remove();
                }
                Some(close)
            },
            Entry::Vacant(_) => None,
        };
        self.request_deadlines
            .cancel(RequestInputDeadlineKey::Body(stream_id));
        result
    }

    pub(super) fn remove_stream(&mut self, stream_id: StreamId) -> Option<InboundStream> {
        self.request_deadlines
            .cancel(RequestInputDeadlineKey::Body(stream_id));
        self.streams.remove(&stream_id)
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

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) enum RequestInputDeadlineKey {
    Headers(StreamId),
    Body(StreamId),
}

impl RequestInputDeadlineKey {
    const fn with_instant(self, instant: TokioInstant) -> RequestInputDeadline {
        match self {
            Self::Headers(stream_id) => RequestInputDeadline::Headers(stream_id, instant),
            Self::Body(stream_id) => RequestInputDeadline::Body(stream_id, instant),
        }
    }
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
        InboundStream, InputDelivery, ReceiveWindowState, RequestInputClose,
        RequestTaskCancellation, RequestTasks,
    };
    use crate::h2_frame::{StreamId, WindowIncrement};
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

    fn assert_batch(input: StreamInput, expected: [&[u8]; 2]) {
        match input {
            StreamInput::HttpDataBatch {
                bodies,
                credit: None,
            } => {
                assert_eq!(bodies[0].as_ref(), expected[0]);
                assert_eq!(bodies[1].as_ref(), expected[1]);
            },
            other => panic!("expected HTTP data batch, got {other:?}"),
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

    #[tokio::test]
    async fn backlogged_http_body_pairs_without_delaying_open_delivery() {
        let (tx, mut rx) = mpsc::channel(1);
        let mut delivery = InputDelivery::new(Some(tx));

        delivery.push_http_body(data(b"a"));
        assert!(matches!(delivery, InputDelivery::Open(_)));
        delivery.push_http_body(data(b"b"));
        delivery.push_http_body(data(b"c"));
        delivery.push_http_body(data(b"d"));
        delivery.push_http_body(data(b"e"));

        let InputDelivery::Backlogged(queued) = &delivery else {
            panic!("full app channel must create a backlog");
        };
        assert_eq!(queued.queue.len(), 2);

        assert_data(rx.recv().await.expect("open-path body"), b"a");
        assert!(!delivery.flush());
        assert_batch(rx.recv().await.expect("first batch"), [b"b", b"c"]);
        assert!(delivery.flush());
        assert_batch(rx.recv().await.expect("second batch"), [b"d", b"e"]);
    }

    #[test]
    fn websocket_delivery_and_large_http_frames_remain_segmented() {
        let (_rx_guard, mut delivery) = {
            let (tx, rx) = mpsc::channel(1);
            (rx, InputDelivery::new(Some(tx)))
        };
        delivery.push(data(b"open"));
        delivery.push(data(b"ws-a"));
        delivery.push(data(b"ws-b"));
        let InputDelivery::Backlogged(queued) = &delivery else {
            panic!("full app channel must create a backlog");
        };
        assert_eq!(
            queued.queue.len(),
            2,
            "ordinary WebSocket push must not batch"
        );

        let (tx, _rx) = mpsc::channel(1);
        let mut delivery = InputDelivery::new(Some(tx));
        delivery.push_http_body(data(b"open"));
        delivery.push_http_body(StreamInput::data(Bytes::from(vec![0; 40 * 1024])));
        delivery.push_http_body(StreamInput::data(Bytes::from(vec![0; 40 * 1024])));
        let InputDelivery::Backlogged(queued) = &delivery else {
            panic!("full app channel must create a backlog");
        };
        assert_eq!(queued.queue.len(), 2, "batch byte ceiling must be strict");
        assert!(
            queued
                .queue
                .iter()
                .all(|input| matches!(input, StreamInput::Data { .. }))
        );
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
        let RequestInputClose::Closed {
            remove_stream: false,
        } = close
        else {
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
        assert!(matches!(close, RequestInputClose::Closed {
            remove_stream: false,
        }));
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
