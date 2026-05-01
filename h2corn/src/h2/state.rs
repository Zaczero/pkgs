use std::{
    collections::VecDeque,
    collections::hash_map::Entry,
    sync::{Arc, atomic::AtomicU64},
    time::Instant,
};

use tokio::sync::{mpsc, watch};
use tokio::time::Instant as TokioInstant;

use crate::config::{INITIAL_CONNECTION_WINDOW_SIZE, INITIAL_STREAM_WINDOW_SIZE};
use crate::frame::{self, StreamId, WindowIncrement};
use crate::h2::new_stream_map;
use crate::hpack::Decoder;
use crate::http::body::{RequestBodyFinish, RequestBodyState};
use crate::runtime::{ConnectionContext, ShutdownState, StreamInput};

use super::{
    StreamMap,
    request::PendingHeaders,
    writer::{ConnectionHandle, WriterState},
};

#[derive(Debug)]
pub(super) struct InboundStream {
    pub(super) input: Option<mpsc::Sender<StreamInput>>,
    pub(super) counts_toward_read_timeout: bool,
    pub(super) receive_window: ReceiveWindowState,
    pub(super) state: ReceiveState,
    pub(super) body: RequestBodyState,
    pub(super) pending_input: VecDeque<StreamInput>,
    pub(super) last_input_read_at: TokioInstant,
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
}

pub(super) enum RequestInputClose {
    ContentLengthMismatch,
    Closed {
        remove_stream: bool,
        tx: Option<mpsc::Sender<StreamInput>>,
    },
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
    ) -> Self {
        Self {
            input,
            counts_toward_read_timeout,
            receive_window: ReceiveWindowState::new(INITIAL_STREAM_WINDOW_SIZE),
            state: if end_stream {
                ReceiveState::RequestClosed
            } else {
                ReceiveState::Open
            },
            body: RequestBodyState::new(expected_content_length, body_bytes, max_request_body_size),
            pending_input: VecDeque::new(),
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

        RequestInputClose::Closed {
            remove_stream: self.mark_request_closed(),
            tx: self.input.take(),
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
            connection_window: ReceiveWindowState::new(INITIAL_CONNECTION_WINDOW_SIZE),
            local_max_frame_size,
            saw_client_settings: false,
            drain_state,
        }
    }

    pub(super) fn active_stream_count(&self) -> usize {
        self.streams.len() + usize::from(self.pending_headers.is_some())
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
                let close = entry.get_mut().finish_request_input();
                if matches!(
                    close,
                    RequestInputClose::Closed {
                        remove_stream: true,
                        ..
                    }
                ) {
                    entry.remove();
                }
                Some(close)
            }
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
