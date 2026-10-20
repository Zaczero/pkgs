use std::collections::VecDeque;
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::{io, mem};

use bytes::BytesMut;
use parking_lot::Mutex;
use smallvec::SmallVec;
use tokio::io::{AsyncWrite as _, AsyncWriteExt as _, BufWriter};
use tokio::sync::futures::Notified;
use tokio::time::Instant;

use crate::async_util::with_optional_timeout;
use crate::bridge::PayloadBytes;
use crate::config::ServerConfig;
use crate::error::H2CornError;
use crate::h2::deadline::DeadlineQueue;
use crate::h2::writer::header_encode::HeaderEncodeState;
use crate::h2::writer::ingress::{QueuedCommandBatch, QueuedStreamCommands, WriterIngress};
use crate::h2::writer::stream_state::{
    BodyItem, ReadyStreamQueue, StreamBodyState, StreamWriteState, StreamWriteStates, writer_stream,
};
use crate::h2::writer::wire::{
    ActiveOutput, BYTE_QUANTUM, DataCursor, DataFrame, FRAME_QUANTUM, HeaderCursor, poll_control,
};
use crate::h2::writer::{
    FRAME_BUFFER_CAPACITY, H2_OUTBOUND_DATA_FRAME_SIZE_TARGET, H2_OUTBOUND_RESPONSE_BYTE_CAPACITY,
    H2_WRITER_BUFFER_CAPACITY, ResponseClose, ResponseCloseBatch, WRITER_CHANNEL_CAPACITY,
    WindowTarget, WriterCommand, WriterCommandBatch,
};
use crate::h2_frame::{
    self, ErrorCode, FramePayloadLen, PeerSettings, Settings, StreamId, WindowIncrement,
};
use crate::http::header::apply_default_response_headers;
use crate::http::pathsend::FileStreamer;
use crate::http::response::{ResponseByteBudget, ResponseBytePermit};
use crate::http::types::{HttpStatusCode, ResponseHeaders};
use crate::sendfile::WriteTarget;
use crate::websocket::EncodedFrameHeader;

#[derive(Clone)]
pub(crate) struct H2WriterHandle {
    ingress: Arc<WriterIngress>,
    config: Arc<ServerConfig>,
    response_budget: ResponseByteBudget,
}

pub(crate) struct WriterState<W> {
    ingress: Arc<WriterIngress>,
    writer: BufWriter<W>,
    config: Arc<ServerConfig>,
    streams: StreamWriteStates,
    ready_streams: ReadyStreamQueue,
    file_read_wake: Option<Arc<FileReadWake>>,
    drained_app_writes: Vec<(StreamId, QueuedStreamCommands)>,
    commands: VecDeque<WriterCommand>,
    active: Option<ActiveOutput>,
    frame_buf: BytesMut,
    header_state: HeaderEncodeState,
    response_closes: ResponseCloseBatch,
    unflushed_closes: SmallVec<[StreamId; 8]>,
    input_credits: SmallVec<[(WindowTarget, WindowIncrement); 8]>,
    connection_send_window: i64,
    initial_stream_send_window: i64,
    peer_max_frame_size: FramePayloadLen,
    response_deadlines: DeadlineQueue<StreamId>,
    dirty: bool,
    flushing: bool,
    output_started: Option<Instant>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WindowOverflow;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InitialWindowAdjustmentOverflow;

enum BodyPreparation {
    Ready,
    Yield,
    Blocked,
}

// A file can complete after its stream was examined but before a yielded
// scan resumes. Keep that notification until the driver starts a fresh pass.
struct FileReadWake {
    ready: AtomicBool,
    driver: Mutex<Waker>,
}

impl Wake for FileReadWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.ready.store(true, Ordering::Release);
        let driver = self.driver.lock().clone();
        driver.wake();
    }
}

impl H2WriterHandle {
    pub(crate) fn failure_notification(
        &self,
        stream_id: StreamId,
    ) -> Option<Arc<dyn crate::runtime::ApplicationFailure>> {
        self.ingress
            .gate(stream_id)
            .map(|gate| gate as Arc<dyn crate::runtime::ApplicationFailure>)
    }

    pub(crate) fn abort_response(&self, stream_id: StreamId) {
        if let Some(failure) = self.failure_notification(stream_id) {
            failure.application_failed();
        }
    }
    pub(crate) fn outbound_notified(&self) -> Notified<'_> {
        self.ingress.notify.notified()
    }
    pub(crate) fn response_byte_budget(&self) -> ResponseByteBudget {
        self.response_budget.clone()
    }
    pub(crate) fn config(&self) -> &ServerConfig {
        &self.config
    }
    pub(crate) async fn send_commands(
        &self,
        stream_id: StreamId,
        commands: WriterCommandBatch,
    ) -> Result<(), H2CornError> {
        self.ingress.enqueue_batch(stream_id, commands).await
    }
    pub(crate) async fn send_headers(
        &self,
        stream_id: StreamId,
        status: HttpStatusCode,
        mut headers: ResponseHeaders,
        end_stream: bool,
    ) -> Result<(), H2CornError> {
        apply_default_response_headers(&mut headers, &self.config);
        self.ingress
            .enqueue(stream_id, WriterCommand::SendHeaders {
                stream_id,
                status,
                headers,
                end_stream,
            })
            .await
    }
    pub(crate) async fn send_data(
        &self,
        stream_id: StreamId,
        data: impl Into<PayloadBytes>,
        credit: Option<ResponseBytePermit>,
        end_stream: bool,
    ) -> Result<(), H2CornError> {
        self.ingress
            .enqueue(stream_id, WriterCommand::SendData {
                stream_id,
                data: data.into(),
                credit,
                end_stream,
            })
            .await
    }
    pub(crate) async fn send_websocket_data(
        &self,
        stream_id: StreamId,
        header: EncodedFrameHeader,
        payload: PayloadBytes,
        credit: Option<ResponseBytePermit>,
    ) -> Result<(), H2CornError> {
        self.ingress
            .enqueue(stream_id, WriterCommand::SendWebSocketData {
                stream_id,
                data: Box::new(super::WebSocketData::new(header, payload)),
                credit,
            })
            .await
    }
    pub(crate) async fn flush_buffered_output(
        &self,
        stream_id: StreamId,
    ) -> Result<(), H2CornError> {
        self.ingress
            .enqueue(stream_id, WriterCommand::FlushStream(stream_id))
            .await
    }
    pub(crate) async fn reset_stream(
        &self,
        stream_id: StreamId,
        error_code: ErrorCode,
    ) -> Result<(), H2CornError> {
        self.ingress
            .enqueue(stream_id, WriterCommand::ResetStream {
                stream_id,
                error_code,
            })
            .await
    }
}

impl<W: WriteTarget> WriterState<W> {
    fn new(writer: W, config: Arc<ServerConfig>) -> Self {
        Self {
            ingress: WriterIngress::new(),
            writer: BufWriter::with_capacity(H2_WRITER_BUFFER_CAPACITY, writer),
            config,
            streams: StreamWriteStates::new(),
            ready_streams: ReadyStreamQueue::new(),
            file_read_wake: None,
            drained_app_writes: Vec::new(),
            commands: VecDeque::new(),
            active: None,
            frame_buf: BytesMut::with_capacity(FRAME_BUFFER_CAPACITY),
            header_state: HeaderEncodeState::new(),
            response_closes: ResponseCloseBatch::new(),
            unflushed_closes: SmallVec::new(),
            input_credits: SmallVec::new(),
            connection_send_window: i64::from(h2_frame::DEFAULT_WINDOW_SIZE),
            initial_stream_send_window: i64::from(h2_frame::DEFAULT_WINDOW_SIZE),
            peer_max_frame_size: FramePayloadLen::constant(h2_frame::DEFAULT_MAX_FRAME_SIZE),
            response_deadlines: DeadlineQueue::default(),
            dirty: false,
            flushing: false,
            output_started: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_test(writer: W) -> Self {
        use crate::runtime::test_fixtures;
        Self::new(writer, test_fixtures::server_config())
    }
    #[cfg(test)]
    pub(crate) fn test_writer_ref(&self) -> &W {
        self.writer.get_ref()
    }

    pub(crate) fn register_stream(
        &mut self,
        stream_id: StreamId,
        timeout_response_stall: Option<std::time::Duration>,
    ) {
        self.ingress.register(stream_id);
        self.streams.get_or_insert_with(stream_id, || {
            let mut stream = StreamWriteState::new(self.initial_stream_send_window);
            stream.timeout_response_stall = timeout_response_stall;
            stream
        });
    }

    fn refresh_response_deadline(&mut self, stream_id: StreamId) {
        if let Some(stream) = self.streams.get(stream_id)
            && let Some(timeout) = stream.timeout_response_stall
            && stream.has_pending_output()
            && (self.connection_send_window <= 0 || stream.send_window <= 0)
            && let Some(since) = stream.pending_body_since()
        {
            self.response_deadlines
                .reschedule_after(stream_id, since, timeout);
        } else {
            self.response_deadlines.cancel(stream_id);
        }
    }

    fn queue_command(&mut self, command: WriterCommand) -> Result<(), H2CornError> {
        if self.commands.len() == WRITER_CHANNEL_CAPACITY {
            return Err(io::Error::other("HTTP/2 pending control output exceeded capacity").into());
        }
        self.commands.push_back(command);
        Ok(())
    }

    fn process_command(&mut self, command: WriterCommand) -> Result<(), H2CornError> {
        if let WriterCommand::SendHeaders { stream_id, .. }
        | WriterCommand::SendFinal { stream_id, .. } = &command
        {
            self.ingress.register(*stream_id);
        }
        if self.active.is_some() {
            self.queue_command(command)?;
        } else {
            self.apply_command(command)?;
        }
        Ok(())
    }

    fn begin_headers(&mut self, stream_id: StreamId, end_stream: bool) {
        self.active = Some(ActiveOutput::Headers(HeaderCursor::new(
            stream_id,
            end_stream,
            self.peer_max_frame_size,
        )));
        self.output_started.get_or_insert_with(Instant::now);
    }

    fn apply_command(&mut self, command: WriterCommand) -> Result<(), H2CornError> {
        match command {
            WriterCommand::SendHeaders {
                stream_id,
                status,
                headers,
                end_stream,
            } => {
                let stream = writer_stream(
                    &mut self.streams,
                    stream_id,
                    self.initial_stream_send_window,
                );
                if stream.open_response(end_stream).is_err() {
                    self.retire_stream(stream_id);
                    return self.queue_command(WriterCommand::SendReset {
                        stream_id,
                        error_code: ErrorCode::INTERNAL_ERROR,
                    });
                }
                self.header_state.encode_response(status, &headers);
                self.begin_headers(stream_id, end_stream);
            },
            WriterCommand::SendFinal {
                stream_id,
                status,
                headers,
                data,
                credit,
            } => {
                let end_stream = data.is_empty();
                self.apply_command(WriterCommand::SendHeaders {
                    stream_id,
                    status,
                    headers,
                    end_stream,
                })?;
                if !end_stream {
                    self.streams
                        .get_mut(stream_id)
                        .expect("response has headers")
                        .queue_data(data, credit, true)?;
                    self.schedule(stream_id, false);
                }
            },
            WriterCommand::SendEarlyHint { stream_id, links } => {
                self.header_state.encode_early_hint(&links);
                self.begin_headers(stream_id, false);
            },
            WriterCommand::SendData {
                stream_id,
                data,
                credit,
                end_stream,
            } => {
                if let Some(stream) = self.streams.get_mut(stream_id) {
                    if stream.queue_data(data, credit, end_stream).is_err() {
                        self.retire_stream(stream_id);
                        return self.queue_command(WriterCommand::SendReset {
                            stream_id,
                            error_code: ErrorCode::INTERNAL_ERROR,
                        });
                    }
                    self.schedule(stream_id, false);
                }
            },
            WriterCommand::SendWebSocketData {
                stream_id,
                data,
                credit,
            } => {
                if let Some(stream) = self.streams.get_mut(stream_id) {
                    stream.queue_websocket_data(data, credit)?;
                    self.schedule(stream_id, false);
                }
            },
            WriterCommand::SendFile {
                stream_id,
                segment,
                end_stream,
            } => {
                if let Some(stream) = self.streams.get_mut(stream_id) {
                    stream.queue_file(FileStreamer::with_credit(
                        segment.file,
                        segment.start,
                        segment.len,
                        end_stream,
                        segment.credit.map(Box::new),
                    ))?;
                    self.schedule(stream_id, false);
                }
            },
            WriterCommand::SendTrailers { stream_id, headers } => {
                if let Some(stream) = self.streams.get_mut(stream_id) {
                    stream.queue_trailers(headers)?;
                    self.schedule(stream_id, true);
                }
            },
            WriterCommand::SendReset {
                stream_id,
                error_code,
            } => {
                self.frame_buf.clear();
                h2_frame::append_rst_stream(&mut self.frame_buf, stream_id, error_code);
                self.active = Some(ActiveOutput::Control {
                    written: 0,
                    input_credit: None,
                });
            },
            WriterCommand::ResetStream {
                stream_id,
                error_code,
            } => {
                self.retire_stream(stream_id);
                self.queue_command(WriterCommand::SendReset {
                    stream_id,
                    error_code,
                })?;
            },
            WriterCommand::StopRequest(stream_id) => {
                self.frame_buf.clear();
                h2_frame::append_rst_stream(&mut self.frame_buf, stream_id, ErrorCode::NO_ERROR);
                self.active = Some(ActiveOutput::Control {
                    written: 0,
                    input_credit: None,
                });
            },
            WriterCommand::SendSettingsAck => {
                self.frame_buf.clear();
                h2_frame::append_settings_ack(&mut self.frame_buf);
                self.active = Some(ActiveOutput::Control {
                    written: 0,
                    input_credit: None,
                });
            },
            WriterCommand::SendWindowUpdate { target, increment } => {
                self.frame_buf.clear();
                h2_frame::append_window_update(
                    &mut self.frame_buf,
                    match target {
                        WindowTarget::Connection => None,
                        WindowTarget::Stream(stream_id) => Some(stream_id),
                    },
                    increment,
                );
                self.active = Some(ActiveOutput::Control {
                    written: 0,
                    input_credit: Some((target, increment)),
                });
            },
            WriterCommand::PingAck(payload) => {
                self.frame_buf.clear();
                h2_frame::append_ping_ack(&mut self.frame_buf, payload);
                self.active = Some(ActiveOutput::Control {
                    written: 0,
                    input_credit: None,
                });
            },
            WriterCommand::Goaway {
                last_stream_id,
                error_code,
                debug,
            } => {
                self.frame_buf.clear();
                h2_frame::append_goaway(&mut self.frame_buf, last_stream_id, error_code, &debug);
                self.active = Some(ActiveOutput::Control {
                    written: 0,
                    input_credit: None,
                });
            },
            WriterCommand::FlushStream(_) => self.flushing = self.dirty,
        }
        if self.active.is_some() {
            self.output_started.get_or_insert_with(Instant::now);
        }
        Ok(())
    }

    fn schedule(&mut self, stream_id: StreamId, front: bool) {
        if let Some(stream) = self.streams.get_mut(stream_id) {
            self.ready_streams.schedule(stream, stream_id, front);
        }
        self.refresh_response_deadline(stream_id);
    }

    pub(crate) fn retire_stream(&mut self, stream_id: StreamId) {
        self.retire_response(stream_id, ResponseClose::Abort);
    }

    fn retire_response(&mut self, stream_id: StreamId, kind: ResponseClose) {
        self.drained_app_writes.retain(|(id, _)| *id != stream_id);
        let owned = self.ingress.gate(stream_id).is_some()
            || self.streams.contains_key(stream_id)
            || self.unflushed_closes.contains(&stream_id);
        self.ingress.drop_stream(stream_id);
        self.streams.remove(stream_id);
        self.response_deadlines.cancel(stream_id);
        self.unflushed_closes.retain(|id| *id != stream_id);
        self.commands
            .retain(|command| command.response_stream_id() != Some(stream_id));
        if let Some(active) = &mut self.active {
            active.retire_stream(stream_id);
        }
        if owned && !self.response_closes.iter().any(|(_, id)| *id == stream_id) {
            self.response_closes.push((kind, stream_id));
        }
    }

    pub(crate) fn drain_app_writes(&mut self) -> Result<bool, H2CornError> {
        self.apply_aborts()?;
        if self.active.is_some() || self.flushing || !self.commands.is_empty() {
            return Ok(false);
        }
        let mut drained = mem::take(&mut self.drained_app_writes);
        if drained.is_empty() {
            self.ingress.drain_into(&mut drained);
            drained.reverse();
        }
        let mut progressed = false;
        'streams: while !drained.is_empty() {
            while let Some(mut batch) = drained.last_mut().and_then(|entry| entry.1.pop_front()) {
                while let Some(command) = batch.commands.pop_front() {
                    if let WriterCommand::FlushStream(id) = command {
                        let pending = self
                            .streams
                            .get(id)
                            .is_some_and(StreamWriteState::has_pending_output);
                        if pending || self.dirty {
                            if !pending {
                                self.flushing = true;
                            }
                            let mut commands = WriterCommandBatch::new();
                            commands.push_back(command);
                            while let Some(next) = batch.commands.pop_front() {
                                commands.push_back(next);
                            }
                            batch.commands = commands;
                            let (stream_id, mut queued) =
                                drained.pop().expect("drained stream exists");
                            prepend_batch(&mut queued, batch);
                            self.ingress
                                .restore_drained(std::iter::once((stream_id, queued)));
                            continue 'streams;
                        }
                    }
                    if command
                        .response_stream_id()
                        .is_none_or(|id| self.streams.contains_key(id))
                    {
                        self.apply_command(command)?;
                        progressed = true;
                    }
                    if self.active.is_some() || self.flushing {
                        if !batch.commands.is_empty() {
                            prepend_batch(
                                &mut drained.last_mut().expect("drained stream exists").1,
                                batch,
                            );
                        }
                        if drained.last().is_some_and(|entry| entry.1.is_empty()) {
                            drained.pop();
                        }
                        self.drained_app_writes = drained;
                        return Ok(progressed);
                    }
                }
            }
            drained.pop();
        }
        self.drained_app_writes = drained;
        Ok(progressed)
    }

    fn apply_aborts(&mut self) -> Result<(), H2CornError> {
        for stream_id in self.ingress.take_aborts() {
            if self.streams.contains_key(stream_id) || self.unflushed_closes.contains(&stream_id) {
                self.retire_response(stream_id, ResponseClose::ProducerFailed);
                self.queue_command(WriterCommand::SendReset {
                    stream_id,
                    error_code: ErrorCode::INTERNAL_ERROR,
                })?;
            }
        }
        Ok(())
    }

    fn prepare_body(&mut self, cx: &Context<'_>) -> Result<BodyPreparation, H2CornError> {
        let turns = self.ready_streams.begin_scan().min(FRAME_QUANTUM);
        for _ in 0..turns {
            let Some(stream_id) = self.ready_streams.pop_scheduled(&mut self.streams) else {
                break;
            };
            let stream = self
                .streams
                .get_mut(stream_id)
                .expect("scheduled stream exists");
            let body = stream.take_body();
            let StreamBodyState::Body(mut queue) = body else {
                if let Some(trailers) = stream.take_trailers_if_body_idle() {
                    self.header_state.encode_trailers(&trailers);
                    self.begin_headers(stream_id, true);
                    return Ok(BodyPreparation::Ready);
                }
                continue;
            };
            let limit = self
                .connection_send_window
                .min(i64::from(stream.send_window));
            let empty_item = match queue.front() {
                Some(BodyItem::Chunk(chunk)) => chunk.remaining_len() == 0,
                Some(BodyItem::File(file)) => file.is_drained(),
                None => true,
            };
            if limit <= 0 && !empty_item {
                stream.restore_body(StreamBodyState::Body(queue));
                self.refresh_response_deadline(stream_id);
                continue;
            }
            let mut sendfile_remaining = 0;
            let sendfile = if let Some(BodyItem::File(file)) = queue.front_mut() {
                if W::SUPPORTS_SENDFILE
                    && let Some(cursor) = file.sendfile_cursor()
                {
                    sendfile_remaining = cursor.remaining();
                    true
                } else {
                    if file.needs_fill() {
                        let wake = self.file_read_wake.get_or_insert_with(|| {
                            Arc::new(FileReadWake {
                                ready: AtomicBool::new(false),
                                driver: Mutex::new(cx.waker().clone()),
                            })
                        });
                        let waker = Waker::from(Arc::clone(wake));
                        match file.poll_fill(&mut Context::from_waker(&waker)) {
                            Poll::Pending => {
                                stream.restore_body(StreamBodyState::Body(queue));
                                self.ready_streams.defer(stream, stream_id);
                                continue;
                            },
                            Poll::Ready(Err(_)) => {
                                self.retire_stream(stream_id);
                                self.queue_command(WriterCommand::SendReset {
                                    stream_id,
                                    error_code: ErrorCode::INTERNAL_ERROR,
                                })?;
                                continue;
                            },
                            Poll::Ready(Ok(())) => {},
                        }
                    }
                    false
                }
            } else {
                false
            };
            let mut cursor = DataCursor::new(stream_id, queue, sendfile);
            let maximum = self
                .peer_max_frame_size
                .min_usize(H2_OUTBOUND_DATA_FRAME_SIZE_TARGET);
            let mut budget = usize::try_from(limit.max(0))
                .expect("nonnegative send window")
                .min(BYTE_QUANTUM.max(maximum.as_usize()));
            'sources: for (source, item) in cursor.body.iter().enumerate() {
                let (remaining, end_stream) = match item {
                    BodyItem::Chunk(chunk) => (chunk.remaining_len(), chunk.end_stream),
                    BodyItem::File(file) if source == 0 => (
                        if sendfile {
                            sendfile_remaining
                        } else {
                            file.remaining().len()
                        },
                        file.end_stream && (sendfile || file.buffered_is_final()),
                    ),
                    BodyItem::File(_) => break,
                };
                let mut offset = 0;
                loop {
                    let len = maximum
                        .min_usize(remaining.wrapping_sub(offset))
                        .min_usize(budget);
                    let end = offset.wrapping_add(len.as_usize());
                    let terminal = end_stream && end == remaining;
                    cursor
                        .frames
                        .push(DataFrame::new(stream_id, source, offset, len, terminal));
                    cursor.end_stream = terminal;
                    budget = budget.wrapping_sub(len.as_usize());
                    offset = end;
                    if terminal || budget == 0 || cursor.frames.len() == FRAME_QUANTUM || sendfile {
                        break 'sources;
                    }
                    if offset == remaining {
                        break;
                    }
                }
                if matches!(item, BodyItem::File(_)) {
                    break;
                }
            }
            self.active = Some(ActiveOutput::Data(cursor));
            self.output_started.get_or_insert_with(Instant::now);
            return Ok(BodyPreparation::Ready);
        }
        Ok(
            if self.ready_streams.has_unexamined() || !self.commands.is_empty() {
                BodyPreparation::Yield
            } else {
                BodyPreparation::Blocked
            },
        )
    }

    fn finish_active(&mut self) {
        let mut resumed = None;
        match self.active.as_mut().expect("completed output exists") {
            ActiveOutput::Headers(headers) => {
                if headers.end_stream && !headers.retired {
                    self.streams.remove(headers.stream_id);
                    self.response_deadlines.cancel(headers.stream_id);
                    self.unflushed_closes.push(headers.stream_id);
                }
            },
            ActiveOutput::Data(data) => {
                data.consume();
                if !data.retired {
                    if data.end_stream {
                        self.streams.remove(data.stream_id);
                        self.response_deadlines.cancel(data.stream_id);
                        self.unflushed_closes.push(data.stream_id);
                    } else if let Some(stream) = self.streams.get_mut(data.stream_id) {
                        stream.restore_body(StreamBodyState::Body(mem::take(&mut data.body)));
                        stream.note_body_progress(Instant::now());
                        let ready = stream.has_pending_output()
                            && ((self.connection_send_window > 0 && stream.send_window > 0)
                                || stream.can_write_without_window());
                        if ready {
                            self.ready_streams.schedule(stream, data.stream_id, false);
                        }
                        resumed = Some((data.stream_id, stream.take_trailers_if_body_idle()));
                    }
                }
            },
            ActiveOutput::Control { input_credit, .. } => {
                if let Some(credit) = input_credit.take() {
                    self.input_credits.push(credit);
                }
                self.frame_buf.clear();
            },
        }
        self.active = None;
        if let Some((stream_id, trailers)) = resumed {
            if let Some(trailers) = trailers {
                self.header_state.encode_trailers(&trailers);
                self.begin_headers(stream_id, true);
            }
            self.refresh_response_deadline(stream_id);
        }
    }

    fn poll_output(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), H2CornError>> {
        self.apply_aborts()?;
        if let Some(wake) = &self.file_read_wake {
            let mut driver = wake.driver.lock();
            if !driver.will_wake(cx.waker()) {
                driver.clone_from(cx.waker());
            }
            drop(driver);
            if wake.ready.swap(false, Ordering::AcqRel) {
                self.ready_streams.restart_scan();
            }
        }
        let mut turns = 0_usize;
        let mut bytes = 0_usize;
        loop {
            if self.flushing {
                match Pin::new(&mut self.writer).poll_flush(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                    Poll::Ready(Ok(())) => {},
                }
                self.dirty = false;
                self.flushing = false;
                if self.active.is_none() {
                    self.output_started = None;
                }
                let terminal = mem::take(&mut self.unflushed_closes);
                for stream_id in terminal {
                    if self.ingress.complete_stream(stream_id) {
                        self.response_closes.push((ResponseClose::Clean, stream_id));
                    } else {
                        self.retire_response(stream_id, ResponseClose::ProducerFailed);
                        self.queue_command(WriterCommand::SendReset {
                            stream_id,
                            error_code: ErrorCode::INTERNAL_ERROR,
                        })?;
                    }
                }
                return Poll::Ready(Ok(()));
            }
            let replan = if let Some(ActiveOutput::Data(data)) = &mut self.active {
                let available = self.streams.get(data.stream_id).map_or(0, |stream| {
                    self.connection_send_window
                        .min(i64::from(stream.send_window))
                });
                data.constrain(available, self.peer_max_frame_size)
            } else {
                false
            };
            if replan {
                let Some(ActiveOutput::Data(data)) = self.active.take() else {
                    unreachable!()
                };
                if let Some(stream) = self.streams.get_mut(data.stream_id) {
                    stream.restore_body(StreamBodyState::Body(data.body));
                    stream.note_body_progress(Instant::now());
                    if self.connection_send_window > 0 && stream.send_window > 0 {
                        self.ready_streams.schedule(stream, data.stream_id, false);
                    }
                    self.refresh_response_deadline(data.stream_id);
                }
                if !self.dirty {
                    self.output_started = None;
                }
                continue;
            }
            if let Some(active) = &mut self.active {
                let data_stream = match active {
                    ActiveOutput::Data(data) => Some(data.stream_id),
                    ActiveOutput::Headers(_) | ActiveOutput::Control { .. } => None,
                };
                let result = match active {
                    ActiveOutput::Headers(headers) => headers.poll_write(
                        &mut self.writer,
                        self.header_state.block(),
                        self.peer_max_frame_size,
                        cx,
                    ),
                    ActiveOutput::Data(data) => data.poll_write(&mut self.writer, cx),
                    ActiveOutput::Control { written, .. } => {
                        poll_control(&mut self.writer, &self.frame_buf, written, cx)
                    },
                };
                match result {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                    Poll::Ready(Ok(progress)) => {
                        self.dirty |= progress.bytes != 0;
                        self.connection_send_window = self
                            .connection_send_window
                            .wrapping_sub(progress.flow_bytes as i64);
                        if let Some(stream) = data_stream.and_then(|id| self.streams.get_mut(id)) {
                            stream.send_window = stream.send_window.wrapping_sub(
                                i32::try_from(progress.flow_bytes)
                                    .expect("one DATA batch fits the send window"),
                            );
                        }
                        turns = turns.wrapping_add(1);
                        bytes = bytes.wrapping_add(progress.bytes);
                        if progress.complete {
                            self.finish_active();
                        }
                        if turns >= FRAME_QUANTUM || bytes >= BYTE_QUANTUM {
                            self.flushing = true;
                        }
                    },
                }
            } else if let Some(command) = self.commands.pop_front() {
                self.apply_command(command)?;
                turns = turns.wrapping_add(1);
            } else {
                match self.prepare_body(cx)? {
                    BodyPreparation::Ready => {},
                    BodyPreparation::Yield => {
                        self.flushing |= self.dirty;
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    },
                    BodyPreparation::Blocked => {
                        if !self.drained_app_writes.is_empty() && self.drain_app_writes()? {
                            turns = turns.wrapping_add(1);
                            if turns >= FRAME_QUANTUM {
                                self.flushing = true;
                            }
                        } else if self.dirty {
                            self.flushing = true;
                        } else {
                            self.output_started = None;
                            return Poll::Pending;
                        }
                    },
                }
            }
        }
    }

    pub(crate) fn poll_egress(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), H2CornError>> {
        let result = self.poll_output(cx);
        if matches!(result, Poll::Ready(Err(_))) {
            self.ingress.close();
            self.commands.clear();
            self.drained_app_writes.clear();
            self.active = None;
            self.dirty = false;
            self.flushing = false;
            self.output_started = None;
            self.response_closes.extend(
                self.unflushed_closes
                    .drain(..)
                    .map(|id| (ResponseClose::Abort, id)),
            );
            for (id, _) in self.streams.drain() {
                self.response_closes.push((ResponseClose::Abort, id));
            }
        }
        result
    }

    pub(crate) fn has_ready_streams(&self) -> bool {
        !self.ready_streams.is_empty()
    }
    pub(crate) fn needs_flush(&self) -> bool {
        self.dirty
            || self.flushing
            || self.active.is_some()
            || !self.commands.is_empty()
            || !self.drained_app_writes.is_empty()
    }
    #[cfg(test)]
    pub(crate) fn has_queued_app_writes(&self) -> bool {
        !self.drained_app_writes.is_empty() || self.ingress.has_pending()
    }
    pub(crate) fn next_response_stall_deadline(&self) -> Option<(StreamId, Instant)> {
        self.response_deadlines.next()
    }
    pub(crate) fn pop_expired_response_stall_deadline(
        &mut self,
        now: Instant,
    ) -> Option<(StreamId, Instant)> {
        self.response_deadlines.pop_expired(now)
    }
    pub(crate) fn next_write_deadline(&self) -> Option<Instant> {
        self.output_started
            .and_then(|since| since.checked_add(self.config.http2.timeout_write?))
    }
    pub(crate) async fn flush(&mut self) -> Result<(), H2CornError> {
        while self.needs_flush() || self.has_ready_streams() {
            poll_fn(|cx| {
                let result = self.poll_egress(cx);
                if result.is_pending() && !self.needs_flush() && !self.has_ready_streams() {
                    Poll::Ready(Ok(()))
                } else {
                    result
                }
            })
            .await?;
            if !self.needs_flush() && !self.has_ready_streams() {
                break;
            }
        }
        Ok(())
    }
    pub(crate) async fn shutdown_write(&mut self) -> Result<(), H2CornError> {
        self.flush().await?;
        self.writer.shutdown().await.map_err(Into::into)
    }
    pub(crate) fn take_response_closes(&mut self) -> ResponseCloseBatch {
        mem::take(&mut self.response_closes)
    }

    pub(crate) fn take_input_credits(&mut self) -> SmallVec<[(WindowTarget, WindowIncrement); 8]> {
        mem::take(&mut self.input_credits)
    }
    pub(crate) fn send_settings_ack(&mut self) -> Result<(), H2CornError> {
        self.queue_command(WriterCommand::SendSettingsAck)
    }
    pub(crate) fn send_headers(
        &mut self,
        stream_id: StreamId,
        status: HttpStatusCode,
        mut headers: ResponseHeaders,
        end_stream: bool,
    ) -> Result<(), H2CornError> {
        self.register_stream(
            stream_id,
            self.config
                .request_policies
                .default_policy()
                .h2_timeout_response_stall,
        );
        apply_default_response_headers(&mut headers, &self.config);
        self.process_command(WriterCommand::SendHeaders {
            stream_id,
            status,
            headers,
            end_stream,
        })
    }
    pub(crate) fn send_window_update(
        &mut self,
        target: WindowTarget,
        increment: WindowIncrement,
    ) -> Result<(), H2CornError> {
        self.queue_command(WriterCommand::SendWindowUpdate { target, increment })
    }
    pub(crate) fn ping_ack(&mut self, payload: [u8; 8]) -> Result<(), H2CornError> {
        self.queue_command(WriterCommand::PingAck(payload))
    }
    pub(crate) fn goaway(
        &mut self,
        last_stream_id: Option<StreamId>,
        error_code: ErrorCode,
        debug: Vec<u8>,
        close: bool,
    ) -> Result<(), H2CornError> {
        if close {
            self.begin_closing();
        }
        self.queue_command(WriterCommand::Goaway {
            last_stream_id,
            error_code,
            debug,
        })?;
        Ok(())
    }
    pub(crate) fn begin_closing(&mut self) {
        if self.ingress.is_closed() {
            return;
        }
        let mut streams: Vec<_> = self
            .streams
            .keys()
            .copied()
            .chain(self.unflushed_closes.iter().copied())
            .collect();
        streams.sort_unstable();
        streams.dedup();
        for id in streams {
            self.retire_stream(id);
        }
        self.ingress.close();
        self.commands.clear();
    }
    pub(crate) fn stop_request(&mut self, stream_id: StreamId) -> Result<(), H2CornError> {
        self.queue_command(WriterCommand::StopRequest(stream_id))
    }
    pub(crate) fn reset_stream(
        &mut self,
        stream_id: StreamId,
        error_code: ErrorCode,
    ) -> Result<(), H2CornError> {
        self.retire_stream(stream_id);
        self.queue_command(WriterCommand::SendReset {
            stream_id,
            error_code,
        })
    }
    pub(crate) fn update_peer_settings(
        &mut self,
        settings: PeerSettings,
    ) -> Result<(), InitialWindowAdjustmentOverflow> {
        let next = settings
            .initial_window_size
            .map_or(self.initial_stream_send_window, i64::from);
        let delta = next.wrapping_sub(self.initial_stream_send_window);
        let max = i64::from(h2_frame::MAX_FLOW_CONTROL_WINDOW);
        if delta > 0
            && self
                .streams
                .values()
                .any(|stream| i64::from(stream.send_window) > max.wrapping_sub(delta))
        {
            return Err(InitialWindowAdjustmentOverflow);
        }
        for stream in self.streams.values_mut() {
            stream.send_window = i32::try_from(i64::from(stream.send_window).wrapping_add(delta))
                .expect("SETTINGS window adjustment was preflighted");
        }
        self.initial_stream_send_window = next;
        if let Some(size) = settings.max_frame_size {
            self.peer_max_frame_size = size;
        }
        if let Some(size) = settings.header_table_size {
            self.header_state.update_max_size(size);
        }
        if delta != 0 {
            self.reschedule_pending_streams();
        }
        Ok(())
    }

    fn reschedule_pending_streams(&mut self) {
        let mut ids: SmallVec<[StreamId; 8]> = self.streams.keys().copied().collect();
        ids.sort_unstable();
        for id in ids {
            let stream = self.streams.get_mut(id).expect("collected stream exists");
            if stream.has_pending_output() && !stream.is_closed() {
                self.ready_streams.schedule(stream, id, false);
            }
            self.refresh_response_deadline(id);
        }
    }

    pub(crate) fn grant_connection_window(
        &mut self,
        increment: WindowIncrement,
    ) -> Result<(), WindowOverflow> {
        let next = self
            .connection_send_window
            .wrapping_add(i64::from(increment.get()));
        if next > i64::from(h2_frame::MAX_FLOW_CONTROL_WINDOW) {
            return Err(WindowOverflow);
        }
        self.connection_send_window = next;
        self.reschedule_pending_streams();
        Ok(())
    }
    pub(crate) fn grant_stream_window(
        &mut self,
        stream_id: StreamId,
        increment: WindowIncrement,
    ) -> Result<(), WindowOverflow> {
        let Some(stream) = self.streams.get_mut(stream_id) else {
            return Ok(());
        };
        stream.send_window = stream
            .send_window
            .checked_add(increment.get() as i32)
            .ok_or(WindowOverflow)?;
        if stream.has_pending_output() && !stream.is_closed() {
            self.ready_streams.schedule(stream, stream_id, false);
        }
        self.refresh_response_deadline(stream_id);
        Ok(())
    }
}

fn prepend_batch(queue: &mut QueuedStreamCommands, batch: QueuedCommandBatch) {
    let mut restored = QueuedStreamCommands::new();
    restored.push_back(batch);
    while let Some(batch) = queue.pop_front() {
        restored.push_back(batch);
    }
    *queue = restored;
}

fn initial_settings(config: &ServerConfig) -> Settings {
    Settings {
        header_table_size: Some(h2_frame::DEFAULT_HEADER_TABLE_SIZE as u32),
        enable_push: Some(false),
        max_concurrent_streams: Some(config.http2.max_concurrent_streams.get()),
        initial_window_size: Some(config.http2.initial_stream_window_size.get()),
        max_frame_size: Some(config.http2.max_inbound_frame_size),
        max_header_list_size: config
            .http2
            .max_header_list_size
            .map(|size| u32::try_from(size.get()).unwrap_or(u32::MAX)),
        enable_connect_protocol: Some(true),
    }
}

pub(crate) async fn init_writer<W: WriteTarget>(
    writer: W,
    config: Arc<ServerConfig>,
    settings: Option<PeerSettings>,
) -> Result<(WriterState<W>, H2WriterHandle), H2CornError> {
    let mut state = WriterState::new(writer, Arc::clone(&config));
    h2_frame::append_settings(&mut state.frame_buf, initial_settings(&config));
    let window = config.http2.initial_connection_window_size.get();
    if window > h2_frame::DEFAULT_WINDOW_SIZE {
        h2_frame::append_window_update(
            &mut state.frame_buf,
            None,
            WindowIncrement::new(window.wrapping_sub(h2_frame::DEFAULT_WINDOW_SIZE))
                .expect("positive initial credit"),
        );
    }
    state.active = Some(ActiveOutput::Control {
        written: 0,
        input_credit: None,
    });
    state.output_started = Some(Instant::now());
    if let Some(settings) = settings {
        state
            .update_peer_settings(settings)
            .expect("empty initial stream set");
    }
    with_optional_timeout(config.http2.timeout_write, state.flush())
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "HTTP/2 initial output stalled"))??;
    let handle = H2WriterHandle {
        ingress: Arc::clone(&state.ingress),
        config,
        response_budget: ResponseByteBudget::new(H2_OUTBOUND_RESPONSE_BYTE_CAPACITY),
    };
    Ok((state, handle))
}

#[cfg(test)]
#[path = "tests.rs"]
pub(super) mod tests;

#[cfg(test)]
#[path = "body_tests.rs"]
mod body_tests;
