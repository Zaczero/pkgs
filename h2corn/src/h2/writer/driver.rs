use std::fs::File;
use std::future::Future;
use std::mem;
use std::mem::take;
use std::num::NonZeroUsize;
use std::sync::Arc;
#[cfg(test)]
use std::{num::NonZeroU32, time::Duration};

use bytes::BytesMut;
use tokio::io::{AsyncWrite, AsyncWriteExt as _, BufWriter};
use tokio::sync::futures::Notified;
use tokio::time::Instant as TokioInstant;

use super::flush::{
    FlushPassResult, FlushTracking, flush_pending_data_tracked, outbound_data_frame_size,
    send_limit, write_frame, write_frame_buf,
};
use super::header_encode::{HeaderEncodeState, write_header_block};
use super::ingress::{QueuedStreamCommands, WriterIngress};
use super::stream_state::{
    ReadyStreamQueue, StreamWriteState, notify_response_abort, notify_response_complete,
    writer_stream,
};
use super::{
    FRAME_BUFFER_CAPACITY, H2_OUTBOUND_RESPONSE_BYTE_CAPACITY, H2_WRITER_BUFFER_CAPACITY,
    ResponseCloseBatch, ResponseDeadlineUpdateBatch, WebSocketData, WindowTarget, WriterCommand,
    WriterCommandBatch,
};
use crate::bridge::PayloadBytes;
use crate::config::ServerConfig;
#[cfg(test)]
use crate::config::{
    BindTarget, Http1Config, Http2Config, ProxyConfig, ResponseHeaderConfig, WebSocketConfig,
};
use crate::error::H2CornError;
use crate::h2::deadline::DeadlineQueue;
use crate::h2::{StreamMap, new_stream_map};
use crate::h2_frame::{
    self, ErrorCode, FramePayload, FramePayloadLen, PeerSettings, Settings, StreamId,
    WindowIncrement,
};
use crate::http::header::apply_default_response_headers;
use crate::http::pathsend::PathStreamer;
use crate::http::response::{ResponseByteBudget, ResponseBytePermit};
use crate::http::types::{HttpStatusCode, ResponseHeaders, ResponseTrailers};
#[cfg(test)]
use crate::proxy_protocol::ProxyProtocolMode;
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
    frame_buf: BytesMut,
    config: Arc<ServerConfig>,
    streams: StreamMap<StreamWriteState>,
    ready_streams: ReadyStreamQueue,
    drained_app_writes: Vec<(StreamId, QueuedStreamCommands)>,
    response_closes: ResponseCloseBatch,
    // These values model aggregate SETTINGS deltas and signed debt across
    // streams, so they deliberately stay wider than a per-stream window.
    connection_send_window: i64,
    initial_stream_send_window: i64,
    peer_max_frame_size: FramePayloadLen,
    header_state: HeaderEncodeState,
    response_deadlines: DeadlineQueue<StreamId>,
    response_deadline_updates: ResponseDeadlineUpdateBatch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GrantSendWindowError {
    Stream(StreamId),
    Connection,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InitialWindowAdjustmentOverflow;

struct WriterSendParts<'a, W> {
    writer: &'a mut BufWriter<W>,
    frame_buf: &'a mut BytesMut,
    streams: &'a mut StreamMap<StreamWriteState>,
    ready_streams: &'a mut ReadyStreamQueue,
    response_closes: &'a mut ResponseCloseBatch,
    connection_send_window: &'a mut i64,
    initial_stream_send_window: i64,
    peer_max_frame_size: FramePayloadLen,
    header_state: &'a mut HeaderEncodeState,
}

struct WriterLoopParts<'a, W> {
    writer: &'a mut BufWriter<W>,
    frame_buf: &'a mut BytesMut,
    streams: &'a mut StreamMap<StreamWriteState>,
    ready_streams: &'a mut ReadyStreamQueue,
    response_closes: &'a mut ResponseCloseBatch,
    connection_send_window: &'a mut i64,
    initial_stream_send_window: &'a mut i64,
    peer_max_frame_size: &'a mut FramePayloadLen,
    header_state: &'a mut HeaderEncodeState,
    response_deadline_updates: &'a mut ResponseDeadlineUpdateBatch,
}

impl<W> WriterLoopParts<'_, W> {
    const fn send_context(&mut self) -> WriterSendParts<'_, W> {
        WriterSendParts {
            writer: self.writer,
            frame_buf: self.frame_buf,
            streams: self.streams,
            ready_streams: self.ready_streams,
            response_closes: self.response_closes,
            connection_send_window: self.connection_send_window,
            initial_stream_send_window: *self.initial_stream_send_window,
            peer_max_frame_size: *self.peer_max_frame_size,
            header_state: self.header_state,
        }
    }
}

impl H2WriterHandle {
    pub(crate) fn response_byte_budget(&self) -> ResponseByteBudget {
        self.response_budget.clone()
    }

    fn send_command(
        &self,
        stream_id: StreamId,
        command: WriterCommand,
    ) -> impl Future<Output = Result<(), H2CornError>> + '_ {
        self.ingress.enqueue(stream_id, command)
    }

    pub(crate) fn send_commands(
        &self,
        stream_id: StreamId,
        commands: WriterCommandBatch,
    ) -> impl Future<Output = Result<(), H2CornError>> + '_ {
        self.ingress.enqueue_batch(stream_id, commands)
    }

    pub(crate) fn config(&self) -> &ServerConfig {
        &self.config
    }

    pub(crate) fn send_headers(
        &self,
        stream_id: StreamId,
        status: HttpStatusCode,
        mut headers: ResponseHeaders,
        end_stream: bool,
    ) -> impl Future<Output = Result<(), H2CornError>> + '_ {
        apply_default_response_headers(&mut headers, &self.config);
        self.send_command(stream_id, WriterCommand::SendHeaders {
            stream_id,
            status,
            headers,
            end_stream,
        })
    }

    pub(crate) fn send_data(
        &self,
        stream_id: StreamId,
        data: impl Into<PayloadBytes>,
        credit: Option<ResponseBytePermit>,
        end_stream: bool,
    ) -> impl Future<Output = Result<(), H2CornError>> + '_ {
        self.send_command(stream_id, WriterCommand::SendData {
            stream_id,
            data: data.into(),
            credit,
            end_stream,
        })
    }

    pub(crate) fn send_websocket_data(
        &self,
        stream_id: StreamId,
        header: EncodedFrameHeader,
        payload: PayloadBytes,
        credit: Option<ResponseBytePermit>,
    ) -> impl Future<Output = Result<(), H2CornError>> + '_ {
        self.send_command(stream_id, WriterCommand::SendWebSocketData {
            stream_id,
            data: Box::new(WebSocketData::new(header, payload)),
            credit,
        })
    }

    pub(crate) fn flush_buffered_output(
        &self,
        stream_id: StreamId,
    ) -> impl Future<Output = Result<(), H2CornError>> + '_ {
        self.send_command(stream_id, WriterCommand::FlushBufferedOutput)
    }

    pub(crate) fn reset_stream(
        &self,
        stream_id: StreamId,
        error_code: ErrorCode,
    ) -> impl Future<Output = Result<(), H2CornError>> + '_ {
        self.send_command(stream_id, WriterCommand::SendReset {
            stream_id,
            error_code,
        })
    }
}

impl<W> WriterSendParts<'_, W> {
    const fn outbound_data_frame_size(&self) -> FramePayloadLen {
        outbound_data_frame_size(self.peer_max_frame_size)
    }
}

impl<W> WriterState<W>
where
    W: WriteTarget,
{
    #[cfg(test)]
    pub(crate) fn new_test(writer: W) -> Self {
        let max_concurrent_streams = 8_u32;
        Self {
            ingress: WriterIngress::new(),
            writer: BufWriter::new(writer),
            frame_buf: BytesMut::with_capacity(FRAME_BUFFER_CAPACITY),
            config: Arc::new(ServerConfig {
                binds: Box::new([BindTarget::Tcp {
                    host: Box::from("127.0.0.1"),
                    port: 8000,
                }]),
                access_log: false,
                root_path: Box::from(""),
                root_path_scope: crate::python::PyOnceLock::new(),
                limit_request_fields: None,
                http1: Http1Config::default(),
                http2: Http2Config {
                    max_concurrent_streams: NonZeroU32::new(max_concurrent_streams)
                        .expect("test fixtures configure a non-zero stream limit"),
                    max_header_list_size: None,
                    max_header_block_size: None,
                    max_inbound_frame_size: NonZeroU32::new(
                        h2_frame::DEFAULT_MAX_FRAME_SIZE as u32,
                    )
                    .expect("default HTTP/2 frame size is non-zero"),
                    initial_stream_window_size: NonZeroU32::new(1 << 20).expect("non-zero"),
                    initial_connection_window_size: NonZeroU32::new(2 << 20).expect("non-zero"),
                    timeout_response_stall: None,
                },
                max_request_body_size: None,
                timeout_graceful_shutdown: Duration::from_secs(30),
                timeout_keep_alive: None,
                timeout_request_header: None,
                timeout_request_body_idle: None,
                limit_concurrency: None,
                limit_connections: None,
                max_requests: None,
                runtime_threads: 2,
                loop_threads: 1,
                websocket: WebSocketConfig::default(),
                proxy: ProxyConfig {
                    trust_headers: false,
                    trusted_peers: Box::new([]),
                    protocol: ProxyProtocolMode::Off,
                },
                tls: None,
                timeout_handshake: Some(Duration::from_secs(5)),
                response_headers: ResponseHeaderConfig::default(),
            }),
            streams: new_stream_map(),
            ready_streams: ReadyStreamQueue::new(),
            drained_app_writes: Vec::with_capacity(max_concurrent_streams as usize),
            response_closes: ResponseCloseBatch::new(),
            connection_send_window: i64::from(h2_frame::DEFAULT_WINDOW_SIZE),
            initial_stream_send_window: i64::from(h2_frame::DEFAULT_WINDOW_SIZE),
            peer_max_frame_size: const { FramePayloadLen::constant(h2_frame::DEFAULT_MAX_FRAME_SIZE) },
            header_state: HeaderEncodeState::new(),
            response_deadlines: DeadlineQueue::default(),
            response_deadline_updates: ResponseDeadlineUpdateBatch::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_writer_ref(&self) -> &W {
        self.writer.get_ref()
    }

    const fn command_context(&mut self) -> WriterLoopParts<'_, W> {
        WriterLoopParts {
            writer: &mut self.writer,
            frame_buf: &mut self.frame_buf,
            streams: &mut self.streams,
            ready_streams: &mut self.ready_streams,
            response_closes: &mut self.response_closes,
            connection_send_window: &mut self.connection_send_window,
            initial_stream_send_window: &mut self.initial_stream_send_window,
            peer_max_frame_size: &mut self.peer_max_frame_size,
            header_state: &mut self.header_state,
            response_deadline_updates: &mut self.response_deadline_updates,
        }
    }

    async fn process_command(&mut self, command: WriterCommand) -> Result<bool, H2CornError> {
        let response_stream_id = command.response_stream_id();
        let result = {
            let mut context = self.command_context();
            process_writer_command(&mut context, command).await
        };
        if let Some(stream_id) = response_stream_id {
            self.refresh_response_deadline(stream_id);
        }
        self.apply_response_deadline_updates();
        result
    }

    fn refresh_response_deadline(&mut self, stream_id: StreamId) {
        let deadline = self
            .config
            .http2
            .timeout_response_stall
            .and_then(|timeout| {
                self.streams
                    .get(&stream_id)
                    .and_then(StreamWriteState::pending_body_since)
                    .map(|since| since + timeout)
            });
        if let Some(deadline) = deadline {
            self.response_deadlines.schedule(stream_id, deadline);
        } else {
            self.response_deadlines.cancel(stream_id);
        }
    }

    fn apply_response_deadline_updates(&mut self) {
        let updates = take(&mut self.response_deadline_updates);
        for stream_id in updates.iter().copied() {
            self.refresh_response_deadline(stream_id);
        }
        self.response_deadline_updates = updates;
        self.response_deadline_updates.clear();
    }

    pub(crate) async fn drain_app_writes(&mut self) -> Result<bool, H2CornError> {
        let mut drained = mem::take(&mut self.drained_app_writes);
        self.ingress.drain_into(&mut drained).await;
        if drained.is_empty() {
            self.drained_app_writes = drained;
            return Ok(false);
        }

        for index in 0..drained.len() {
            while let Some(mut queued_batch) = drained[index].1.pop_front() {
                while let Some(command) = queued_batch.commands.pop_front() {
                    if self.process_command(command).await? {
                        if !queued_batch.commands.is_empty() {
                            let mut remainder = QueuedStreamCommands::new();
                            remainder.push_back(queued_batch);
                            while let Some(queued_batch) = drained[index].1.pop_front() {
                                remainder.push_back(queued_batch);
                            }
                            drained[index].1 = remainder;
                        }
                        let remainder = drained.split_off(index);
                        self.ingress.restore_drained(remainder).await;
                        drained.clear();
                        self.drained_app_writes = drained;
                        return Ok(true);
                    }
                }
            }
        }

        drained.clear();
        self.drained_app_writes = drained;
        Ok(true)
    }

    pub(crate) fn has_ready_streams(&self) -> bool {
        !self.ready_streams.is_empty()
    }

    pub(crate) fn needs_flush(&self) -> bool {
        !self.writer.buffer().is_empty()
    }

    pub(crate) fn has_queued_app_writes(&self) -> bool {
        self.ingress.has_pending()
    }

    pub(crate) fn outbound_notified(&self) -> Notified<'_> {
        self.ingress.notify.notified()
    }

    pub(crate) fn next_response_stall_deadline(&self) -> Option<(StreamId, TokioInstant)> {
        self.response_deadlines.next()
    }

    pub(crate) fn pop_expired_response_stall_deadline(
        &mut self,
        now: TokioInstant,
    ) -> Option<(StreamId, TokioInstant)> {
        self.response_deadlines.pop_expired(now)
    }

    pub(crate) async fn flush(&mut self) -> Result<(), H2CornError> {
        self.writer.flush().await?;
        Ok(())
    }

    /// Flush and half-close the write side (TCP FIN / TLS `close_notify`),
    /// prompting the peer to read pending output and close.
    pub(crate) async fn shutdown_write(&mut self) {
        let _ = self.writer.shutdown().await;
    }

    pub(crate) async fn close_ingress(&self) {
        self.ingress.close().await;
    }

    pub(crate) async fn drop_ingress_stream(&self, stream_id: StreamId) {
        self.ingress.drop_stream(stream_id).await;
    }

    pub(crate) fn take_response_closes(&mut self) -> ResponseCloseBatch {
        mem::take(&mut self.response_closes)
    }

    pub(crate) async fn send_settings_ack(&mut self) -> Result<(), H2CornError> {
        self.process_command(WriterCommand::SendSettingsAck).await?;
        Ok(())
    }

    pub(crate) async fn send_headers(
        &mut self,
        stream_id: StreamId,
        status: HttpStatusCode,
        mut headers: ResponseHeaders,
        end_stream: bool,
    ) -> Result<(), H2CornError> {
        apply_default_response_headers(&mut headers, &self.config);
        self.process_command(WriterCommand::SendHeaders {
            stream_id,
            status,
            headers,
            end_stream,
        })
        .await?;
        Ok(())
    }

    pub(crate) fn update_peer_settings(
        &mut self,
        settings: PeerSettings,
    ) -> Result<(), InitialWindowAdjustmentOverflow> {
        let next_initial_window = settings
            .initial_window_size
            .map_or(self.initial_stream_send_window, i64::from);
        let delta = next_initial_window - self.initial_stream_send_window;
        let max_window = i64::from(h2_frame::MAX_FLOW_CONTROL_WINDOW);
        if delta > 0
            && self
                .streams
                .values()
                .any(|stream| i64::from(stream.send_window) > max_window - delta)
        {
            return Err(InitialWindowAdjustmentOverflow);
        }

        // The preflight makes this single state transition atomic: a rejected
        // SETTINGS frame leaves every stream and every peer-owned knob alone.
        if delta != 0 {
            #[expect(
                clippy::iter_over_hash_type,
                reason = "order-independent: a uniform delta is applied to every stream, and the \
                          preflight above makes partial application impossible"
            )]
            for stream in self.streams.values_mut() {
                stream.send_window = i32::try_from(i64::from(stream.send_window) + delta)
                    .expect("peer SETTINGS preserve the signed 31-bit stream window");
            }
            self.initial_stream_send_window = next_initial_window;
        }
        if let Some(size) = settings.max_frame_size {
            self.peer_max_frame_size = size;
        }
        if let Some(size) = settings.header_table_size {
            self.header_state.update_max_size(size);
        }
        Ok(())
    }

    pub(crate) async fn peer_reset(&mut self, stream_id: StreamId) -> Result<(), H2CornError> {
        self.ingress.drop_stream(stream_id).await;
        self.process_command(WriterCommand::PeerReset { stream_id })
            .await?;
        Ok(())
    }

    pub(crate) fn grant_send_window(
        &mut self,
        target: WindowTarget,
        increment: WindowIncrement,
    ) -> Result<(), GrantSendWindowError> {
        let increment = i64::from(increment.get());
        let max_window = i64::from(h2_frame::MAX_FLOW_CONTROL_WINDOW);
        match target {
            WindowTarget::Connection => {
                if self.connection_send_window > max_window - increment {
                    return Err(GrantSendWindowError::Connection);
                }
                self.connection_send_window += increment;
            },
            WindowTarget::Stream(stream_id) => {
                let stream = self
                    .streams
                    .entry(stream_id)
                    .or_insert_with(|| StreamWriteState::new(self.initial_stream_send_window));
                if i64::from(stream.send_window) > max_window - increment {
                    return Err(GrantSendWindowError::Stream(stream_id));
                }
                stream.send_window = i32::try_from(i64::from(stream.send_window) + increment)
                    .expect("a checked window update preserves the signed 31-bit stream window");
                if stream.has_pending_output() && !stream.is_closed() {
                    self.ready_streams.schedule(stream, stream_id, false);
                }
            },
        }
        Ok(())
    }

    pub(crate) async fn send_window_update(
        &mut self,
        target: WindowTarget,
        increment: WindowIncrement,
    ) -> Result<(), H2CornError> {
        self.process_command(WriterCommand::SendWindowUpdate { target, increment })
            .await?;
        Ok(())
    }

    pub(crate) async fn ping_ack(&mut self, payload: [u8; 8]) -> Result<(), H2CornError> {
        self.process_command(WriterCommand::PingAck(payload))
            .await?;
        Ok(())
    }

    pub(crate) async fn goaway(
        &mut self,
        last_stream_id: Option<StreamId>,
        error_code: ErrorCode,
        debug: Vec<u8>,
        close: bool,
    ) -> Result<bool, H2CornError> {
        self.process_command(WriterCommand::Goaway {
            last_stream_id,
            error_code,
            debug,
            close,
        })
        .await
    }

    pub(crate) async fn reset_stream(
        &mut self,
        stream_id: StreamId,
        error_code: ErrorCode,
    ) -> Result<(), H2CornError> {
        self.ingress.drop_stream(stream_id).await;
        self.process_command(WriterCommand::SendReset {
            stream_id,
            error_code,
        })
        .await?;
        Ok(())
    }

    pub(crate) async fn flush_pending_output(&mut self) -> Result<FlushPassResult, H2CornError> {
        let result = flush_pending_data_tracked(
            &mut self.writer,
            &mut self.streams,
            &mut self.ready_streams,
            &mut self.connection_send_window,
            self.peer_max_frame_size,
            &mut self.header_state,
            FlushTracking {
                deadline_updates: &mut self.response_deadline_updates,
                response_closes: &mut self.response_closes,
            },
        )
        .await;
        self.apply_response_deadline_updates();
        result
    }
}

async fn force_reset_stream<W>(
    writer: &mut W,
    frame_buf: &mut BytesMut,
    streams: &mut StreamMap<StreamWriteState>,
    response_closes: &mut ResponseCloseBatch,
    stream_id: StreamId,
    error_code: ErrorCode,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    streams.remove(&stream_id);
    h2_frame::append_rst_stream(frame_buf, stream_id, error_code);
    notify_response_abort(response_closes, stream_id);
    write_frame_buf(writer, frame_buf).await
}

async fn handle_send_headers<W>(
    context: &mut WriterSendParts<'_, W>,
    stream_id: StreamId,
    status: HttpStatusCode,
    headers: ResponseHeaders,
    end_stream: bool,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    let block = context.header_state.encode_response(status, &headers);

    if write_header_block(
        context.writer,
        stream_id,
        end_stream,
        block,
        context.peer_max_frame_size,
    )
    .await
    .is_err()
    {
        notify_response_abort(context.response_closes, stream_id);
        return Ok(());
    }

    let stream = writer_stream(
        context.streams,
        stream_id,
        context.initial_stream_send_window,
    );
    if stream.open_response(end_stream).is_err() {
        let _ = force_reset_stream(
            context.writer,
            context.frame_buf,
            context.streams,
            context.response_closes,
            stream_id,
            ErrorCode::INTERNAL_ERROR,
        )
        .await;
        return Ok(());
    }
    if end_stream {
        // The whole response went out with these headers, so there is nothing
        // left for this stream to send. Body-bearing responses are removed by
        // the flush pass once drained; a header-only one — empty, HEAD, 204,
        // 304 — never enters it, and used to sit in the map for the life of
        // the connection.
        context.streams.remove(&stream_id);
        notify_response_complete(context.response_closes, stream_id);
    }

    Ok(())
}

async fn handle_send_trailers<W>(
    context: &mut WriterSendParts<'_, W>,
    stream_id: StreamId,
    headers: ResponseTrailers,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    let stream = writer_stream(
        context.streams,
        stream_id,
        context.initial_stream_send_window,
    );
    if stream.queue_trailers(headers).is_err() {
        let _ = force_reset_stream(
            context.writer,
            context.frame_buf,
            context.streams,
            context.response_closes,
            stream_id,
            ErrorCode::INTERNAL_ERROR,
        )
        .await;
        return Ok(());
    }
    context.ready_streams.schedule(stream, stream_id, true);
    Ok(())
}

async fn handle_send_data<W>(
    context: &mut WriterSendParts<'_, W>,
    stream_id: StreamId,
    data: PayloadBytes,
    credit: Option<ResponseBytePermit>,
    end_stream: bool,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    let stream = writer_stream(
        context.streams,
        stream_id,
        context.initial_stream_send_window,
    );
    if stream.queue_data(data, credit, end_stream).is_err() {
        let _ = force_reset_stream(
            context.writer,
            context.frame_buf,
            context.streams,
            context.response_closes,
            stream_id,
            ErrorCode::INTERNAL_ERROR,
        )
        .await;
        return Ok(());
    }
    context.ready_streams.schedule(stream, stream_id, false);
    Ok(())
}

async fn handle_send_websocket_data<W>(
    context: &mut WriterSendParts<'_, W>,
    stream_id: StreamId,
    data: Box<WebSocketData>,
    credit: Option<ResponseBytePermit>,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    let stream = writer_stream(
        context.streams,
        stream_id,
        context.initial_stream_send_window,
    );
    if stream.queue_websocket_data(data, credit).is_err() {
        let _ = force_reset_stream(
            context.writer,
            context.frame_buf,
            context.streams,
            context.response_closes,
            stream_id,
            ErrorCode::INTERNAL_ERROR,
        )
        .await;
        return Ok(());
    }
    context.ready_streams.schedule(stream, stream_id, false);
    Ok(())
}

async fn handle_send_final<W>(
    context: &mut WriterSendParts<'_, W>,
    stream_id: StreamId,
    status: HttpStatusCode,
    headers: ResponseHeaders,
    data: PayloadBytes,
    mut credit: Option<ResponseBytePermit>,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    let end_stream = data.is_empty();
    // The single-shot path needs the whole body to fit one DATA frame within
    // both windows; `take` then carries that proof into the frame itself.
    let single_shot = (!end_stream && !context.streams.contains_key(&stream_id))
        .then(|| {
            send_limit(
                *context.connection_send_window,
                context.initial_stream_send_window,
                context.outbound_data_frame_size(),
            )
        })
        .flatten()
        .map(|limit| FramePayload::take(data.as_ref(), limit))
        .filter(|(_, rest)| rest.is_empty())
        .map(|(payload, _)| payload);
    if let Some(payload) = single_shot {
        let block = context.header_state.encode_response(status, &headers);

        if write_header_block(
            context.writer,
            stream_id,
            false,
            block,
            context.peer_max_frame_size,
        )
        .await
        .is_err()
        {
            notify_response_abort(context.response_closes, stream_id);
            return Ok(());
        }

        // Single-shot DATA frame into the BufWriter: small responses
        // coalesce with the HEADERS frame into one sendto on flush.
        if let Err(error) = write_frame(
            context.writer,
            h2_frame::FrameHeader {
                frame_type: h2_frame::FrameType::DATA,
                flags: h2_frame::FrameFlags::END_STREAM,
                stream_id: Some(stream_id),
            },
            payload,
        )
        .await
        {
            notify_response_abort(context.response_closes, stream_id);
            return Err(error);
        }
        *context.connection_send_window -= i64::from(payload.len().get());
        if let Some(credit) = &mut credit {
            credit.release_written(payload.len().as_usize());
        }

        notify_response_complete(context.response_closes, stream_id);
        return Ok(());
    }

    handle_send_headers(context, stream_id, status, headers, end_stream).await?;
    if end_stream {
        return Ok(());
    }
    handle_send_data(context, stream_id, data, credit, true).await
}

async fn handle_send_path<W>(
    context: &mut WriterSendParts<'_, W>,
    stream_id: StreamId,
    file: Box<File>,
    len: usize,
    end_stream: bool,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    let stream = writer_stream(
        context.streams,
        stream_id,
        context.initial_stream_send_window,
    );
    if stream
        .queue_path(PathStreamer::new(*file, len, end_stream))
        .is_err()
    {
        let _ = force_reset_stream(
            context.writer,
            context.frame_buf,
            context.streams,
            context.response_closes,
            stream_id,
            ErrorCode::INTERNAL_ERROR,
        )
        .await;
        return Ok(());
    }
    context.ready_streams.schedule(stream, stream_id, false);
    Ok(())
}

async fn handle_send_reset<W>(
    context: &mut WriterLoopParts<'_, W>,
    stream_id: StreamId,
    error_code: ErrorCode,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    force_reset_stream(
        context.writer,
        context.frame_buf,
        context.streams,
        context.response_closes,
        stream_id,
        error_code,
    )
    .await
}

async fn flush_buffered_writer_output<W>(
    context: &mut WriterLoopParts<'_, W>,
) -> Result<(), H2CornError>
where
    W: WriteTarget,
{
    let _ = flush_pending_data_tracked(
        context.writer,
        context.streams,
        context.ready_streams,
        context.connection_send_window,
        *context.peer_max_frame_size,
        context.header_state,
        FlushTracking {
            deadline_updates: context.response_deadline_updates,
            response_closes: context.response_closes,
        },
    )
    .await?;
    context.writer.flush().await?;
    Ok(())
}

async fn send_window_update<W>(
    context: &mut WriterLoopParts<'_, W>,
    target: WindowTarget,
    increment: WindowIncrement,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    h2_frame::append_window_update(
        context.frame_buf,
        match target {
            WindowTarget::Connection => None,
            WindowTarget::Stream(stream_id) => Some(stream_id),
        },
        increment,
    );
    write_frame_buf(context.writer, context.frame_buf).await
}

async fn process_writer_command<W>(
    context: &mut WriterLoopParts<'_, W>,
    command: WriterCommand,
) -> Result<bool, H2CornError>
where
    W: WriteTarget,
{
    match command {
        WriterCommand::SendSettingsAck => {
            h2_frame::append_settings_ack(context.frame_buf);
            write_frame_buf(context.writer, context.frame_buf).await?;
        },
        WriterCommand::SendHeaders {
            stream_id,
            status,
            headers,
            end_stream,
        } => {
            let mut send = context.send_context();
            handle_send_headers(&mut send, stream_id, status, headers, end_stream).await?;
        },
        WriterCommand::SendFinal {
            stream_id,
            status,
            headers,
            data,
            credit,
        } => {
            let mut send = context.send_context();
            handle_send_final(&mut send, stream_id, status, headers, data, credit).await?;
        },
        WriterCommand::SendTrailers { stream_id, headers } => {
            let mut send = context.send_context();
            handle_send_trailers(&mut send, stream_id, headers).await?;
        },
        WriterCommand::SendData {
            stream_id,
            data,
            credit,
            end_stream,
        } => {
            handle_send_data(
                &mut context.send_context(),
                stream_id,
                data,
                credit,
                end_stream,
            )
            .await?;
        },
        WriterCommand::SendWebSocketData {
            stream_id,
            data,
            credit,
        } => {
            handle_send_websocket_data(&mut context.send_context(), stream_id, data, credit).await?;
        },
        WriterCommand::SendPath {
            stream_id,
            file,
            len,
            end_stream,
        } => {
            let mut send = context.send_context();
            handle_send_path(&mut send, stream_id, file, len, end_stream).await?;
        },
        WriterCommand::FlushBufferedOutput => {
            flush_buffered_writer_output(context).await?;
            return Ok(true);
        },
        WriterCommand::SendReset {
            stream_id,
            error_code,
        } => {
            handle_send_reset(context, stream_id, error_code).await?;
        },
        WriterCommand::PeerReset { stream_id } => {
            context.streams.remove(&stream_id);
        },
        WriterCommand::SendWindowUpdate { target, increment } => {
            send_window_update(context, target, increment).await?;
        },
        WriterCommand::PingAck(payload) => {
            h2_frame::append_ping_ack(context.frame_buf, payload);
            write_frame_buf(context.writer, context.frame_buf).await?;
        },
        WriterCommand::Goaway {
            last_stream_id,
            error_code,
            debug,
            close,
        } => {
            h2_frame::append_goaway(context.frame_buf, last_stream_id, error_code, &debug);
            write_frame_buf(context.writer, context.frame_buf).await?;
            if close {
                context.writer.flush().await?;
                return Ok(true);
            }
        },
    }

    Ok(false)
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
            .map(NonZeroUsize::get)
            .map(|value| u32::try_from(value).unwrap_or(u32::MAX)),
        enable_connect_protocol: Some(true),
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "writer ingress is intentionally kept with the initialized writer state"
)]
pub(crate) async fn init_writer<W>(
    writer: W,
    config: Arc<ServerConfig>,
    initial_peer_settings: Option<PeerSettings>,
) -> Result<(WriterState<W>, H2WriterHandle), H2CornError>
where
    W: WriteTarget,
{
    let ingress = WriterIngress::new();
    let mut writer = BufWriter::with_capacity(H2_WRITER_BUFFER_CAPACITY, writer);
    let mut frame_buf = BytesMut::with_capacity(FRAME_BUFFER_CAPACITY);
    let initial_settings = initial_settings(&config);
    h2_frame::append_settings(&mut frame_buf, initial_settings);
    write_frame_buf(&mut writer, &mut frame_buf).await?;
    let initial_connection_window = config.http2.initial_connection_window_size.get();
    if initial_connection_window > h2_frame::DEFAULT_WINDOW_SIZE {
        h2_frame::append_window_update(
            &mut frame_buf,
            None,
            WindowIncrement::new(initial_connection_window - h2_frame::DEFAULT_WINDOW_SIZE)
                .expect("increment is positive"),
        );
        write_frame_buf(&mut writer, &mut frame_buf).await?;
    }
    writer.flush().await?;

    let mut writer_state = WriterState {
        ingress,
        writer,
        frame_buf,
        config: Arc::clone(&config),
        streams: new_stream_map(),
        ready_streams: ReadyStreamQueue::new(),
        drained_app_writes: Vec::new(),
        response_closes: ResponseCloseBatch::new(),
        connection_send_window: i64::from(h2_frame::DEFAULT_WINDOW_SIZE),
        initial_stream_send_window: i64::from(h2_frame::DEFAULT_WINDOW_SIZE),
        peer_max_frame_size: const { FramePayloadLen::constant(h2_frame::DEFAULT_MAX_FRAME_SIZE) },
        header_state: HeaderEncodeState::new(),
        response_deadlines: DeadlineQueue::default(),
        response_deadline_updates: ResponseDeadlineUpdateBatch::new(),
    };

    if let Some(settings) = initial_peer_settings {
        // Same application as a mid-connection SETTINGS frame; there are no
        // streams yet, so the window-delta loop is a no-op.
        writer_state
            .update_peer_settings(settings)
            .expect("an empty writer cannot overflow a SETTINGS window adjustment");
    }

    let connection = H2WriterHandle {
        ingress: Arc::clone(&writer_state.ingress),
        config,
        response_budget: ResponseByteBudget::new(H2_OUTBOUND_RESPONSE_BYTE_CAPACITY),
    };
    Ok((writer_state, connection))
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io;
    use std::num::NonZeroUsize;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll};

    use bytes::BytesMut;
    use tokio::io::{AsyncWrite, BufWriter};

    use super::{
        HeaderEncodeState, PeerSettings, ReadyStreamQueue, ResponseCloseBatch, StreamWriteState,
        WindowTarget, WriterCommand, WriterCommandBatch, WriterSendParts, WriterState,
        handle_send_headers,
    };
    use crate::bridge::PayloadBytes;
    use crate::h2::{ResponseClose, new_stream_map};
    use crate::h2_frame::{self, ErrorCode, FramePayloadLen, StreamId, WindowIncrement};
    use crate::http::response::ResponseByteBudget;
    use crate::http::types::{ResponseHeaders, status_code};
    use crate::sendfile::WriteTarget;

    #[derive(Default)]
    struct TestWriter;

    impl AsyncWrite for TestWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl WriteTarget for TestWriter {
        const SUPPORTS_SENDFILE: bool = false;

        async fn send_file(
            _writer: &mut BufWriter<Self>,
            _file: &mut File,
            _offset: &mut u64,
            _len: usize,
        ) -> io::Result<()> {
            unreachable!("writer-state tests never send files")
        }
    }

    struct FailingWriter {
        fail_writes: Arc<AtomicBool>,
    }

    impl AsyncWrite for FailingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            if self.fail_writes.load(Ordering::Relaxed) {
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "test write failure",
                )))
            } else {
                Poll::Ready(Ok(buf.len()))
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl WriteTarget for FailingWriter {
        const SUPPORTS_SENDFILE: bool = false;

        async fn send_file(
            _writer: &mut BufWriter<Self>,
            _file: &mut File,
            _offset: &mut u64,
            _len: usize,
        ) -> io::Result<()> {
            unreachable!("failure tests never send files")
        }
    }

    #[test]
    fn initial_settings_saturate_an_oversized_header_list_limit() {
        let mut config = Arc::into_inner(WriterState::new_test(TestWriter).config)
            .expect("test writer owns its configuration");
        let max_wire_value = usize::try_from(u32::MAX).expect("64-bit test host");
        config.http2.max_header_list_size = NonZeroUsize::new(max_wire_value + 1);

        assert_eq!(
            super::initial_settings(&config).max_header_list_size,
            Some(u32::MAX)
        );
    }

    #[tokio::test]
    async fn completed_response_does_not_discard_remaining_global_commands() {
        let stream_id = StreamId::new(1).expect("test stream id is valid");
        let mut writer = WriterState::new_test(TestWriter);
        let mut commands = WriterCommandBatch::new();
        commands.push_back(WriterCommand::SendHeaders {
            stream_id,
            status: status_code::OK,
            headers: ResponseHeaders::new(),
            end_stream: false,
        });
        commands.push_back(WriterCommand::SendData {
            stream_id,
            data: PayloadBytes::from(bytes::Bytes::from_static(b"complete")),
            credit: None,
            end_stream: true,
        });
        commands.push_back(WriterCommand::FlushBufferedOutput);
        commands.push_back(WriterCommand::SendSettingsAck);
        writer
            .ingress
            .enqueue_batch(stream_id, commands)
            .await
            .expect("writer accepts one submitted command batch");

        assert!(
            writer
                .drain_app_writes()
                .await
                .expect("the flush command is handled")
        );
        assert_eq!(writer.take_response_closes().as_slice(), &[(
            ResponseClose::Clean,
            stream_id
        )]);
        assert!(
            writer.has_queued_app_writes(),
            "the post-flush command remains queued"
        );

        assert!(
            writer
                .drain_app_writes()
                .await
                .expect("the remaining command is handled")
        );
        assert!(!writer.has_queued_app_writes());
        drop(writer);
    }

    #[tokio::test]
    async fn reset_marks_queued_stream_cleanup_as_abort() {
        let stream_id = StreamId::new(1).expect("test stream id is valid");
        let mut writer = WriterState::new_test(TestWriter);
        writer
            .ingress
            .enqueue(stream_id, WriterCommand::SendSettingsAck)
            .await
            .expect("queued command enters ingress");

        writer
            .process_command(WriterCommand::SendReset {
                stream_id,
                error_code: ErrorCode::CANCEL,
            })
            .await
            .expect("reset frame writes");
        assert_eq!(writer.take_response_closes().as_slice(), &[(
            ResponseClose::Abort,
            stream_id
        )]);
        assert!(writer.has_queued_app_writes());

        writer.drop_ingress_stream(stream_id).await;
        assert!(
            !writer
                .drain_app_writes()
                .await
                .expect("discarded ingress marker drains without a command")
        );
        assert!(!writer.has_queued_app_writes());
        drop(writer);
    }

    #[tokio::test]
    async fn failed_headers_and_data_writes_mark_streams_as_abort() {
        let stream_id = StreamId::new(1).expect("test stream id is valid");
        let fail_writes = Arc::new(AtomicBool::new(true));
        let mut raw_writer = BufWriter::with_capacity(1, FailingWriter {
            fail_writes: Arc::clone(&fail_writes),
        });
        let mut frame_buf = BytesMut::new();
        let mut streams = new_stream_map();
        let mut ready_streams = ReadyStreamQueue::new();
        let mut response_closes = ResponseCloseBatch::new();
        let mut connection_send_window = i64::from(h2_frame::DEFAULT_WINDOW_SIZE);
        let mut header_state = HeaderEncodeState::new();
        let mut context = WriterSendParts {
            writer: &mut raw_writer,
            frame_buf: &mut frame_buf,
            streams: &mut streams,
            ready_streams: &mut ready_streams,
            response_closes: &mut response_closes,
            connection_send_window: &mut connection_send_window,
            initial_stream_send_window: i64::from(h2_frame::DEFAULT_WINDOW_SIZE),
            peer_max_frame_size: FramePayloadLen::constant(h2_frame::DEFAULT_MAX_FRAME_SIZE),
            header_state: &mut header_state,
        };
        handle_send_headers(
            &mut context,
            stream_id,
            status_code::OK,
            ResponseHeaders::new(),
            false,
        )
        .await
        .expect("a failed HEADERS write is handled as a stream abort");
        assert_eq!(response_closes.as_slice(), &[(
            ResponseClose::Abort,
            stream_id
        )]);

        let fail_writes = Arc::new(AtomicBool::new(false));
        let mut writer = WriterState::new_test(FailingWriter {
            fail_writes: Arc::clone(&fail_writes),
        });
        writer
            .process_command(WriterCommand::SendHeaders {
                stream_id,
                status: status_code::OK,
                headers: ResponseHeaders::new(),
                end_stream: false,
            })
            .await
            .expect("headers enter the buffered writer");
        writer
            .flush()
            .await
            .expect("headers reach the peer before data fails");
        fail_writes.store(true, Ordering::Relaxed);
        writer
            .process_command(WriterCommand::SendData {
                stream_id,
                data: PayloadBytes::from(bytes::Bytes::from_static(b"body")),
                credit: None,
                end_stream: true,
            })
            .await
            .expect("data enters the stream queue");
        writer.flush_pending_output().await.unwrap_err();
        assert_eq!(writer.take_response_closes().as_slice(), &[(
            ResponseClose::Abort,
            stream_id
        )]);
    }

    #[tokio::test]
    async fn response_byte_credit_survives_ingress_drain_until_data_is_written() {
        let stream_id = StreamId::new(1).expect("test stream id is valid");
        let mut writer = WriterState::new_test(TestWriter);
        writer
            .send_headers(stream_id, status_code::OK, ResponseHeaders::new(), false)
            .await
            .expect("response headers are queued");
        writer.connection_send_window = 0;
        writer
            .streams
            .get_mut(&stream_id)
            .expect("headers create stream state")
            .send_window = 0;

        let budget = ResponseByteBudget::new(64 * 1024);
        let credit = budget
            .acquire(64 * 1024)
            .await
            .expect("budget stays open")
            .expect("non-empty body consumes credit");
        writer
            .ingress
            .enqueue(stream_id, WriterCommand::SendData {
                stream_id,
                data: PayloadBytes::from(bytes::Bytes::from(vec![b'x'; 64 * 1024])),
                credit: Some(credit),
                end_stream: false,
            })
            .await
            .expect("writer accepts body command");
        writer
            .drain_app_writes()
            .await
            .expect("ingress moves body into stream state");

        let first_next = tokio::spawn({
            let budget = budget.clone();
            async move { budget.acquire(64 * 1024).await }
        });
        let second_next = tokio::spawn({
            let budget = budget.clone();
            async move { budget.acquire(64 * 1024).await }
        });
        tokio::task::yield_now().await;
        assert!(
            !first_next.is_finished() && !second_next.is_finished(),
            "draining command ingress must not release payload credit"
        );

        writer
            .grant_send_window(
                WindowTarget::Connection,
                WindowIncrement::new(64 * 1024).expect("positive test increment"),
            )
            .expect("connection window grant is valid");
        writer
            .grant_send_window(
                WindowTarget::Stream(stream_id),
                WindowIncrement::new(64 * 1024).expect("positive test increment"),
            )
            .expect("stream window grant is valid");
        writer
            .flush_pending_output()
            .await
            .expect("granted DATA frame writes");
        drop(writer);

        let resumed = tokio::time::timeout(std::time::Duration::from_secs(1), first_next)
            .await
            .expect("written bytes wake one waiting body")
            .expect("first waiter task joins")
            .expect("budget remains open");
        assert!(
            resumed.is_some(),
            "one full frame's credit admits the first waiting body"
        );
        assert!(
            !second_next.is_finished(),
            "one 64 KiB window grant must not admit a second 64 KiB body"
        );
        drop(resumed);
        second_next.abort();
    }

    #[tokio::test]
    async fn response_byte_credit_reset_wakes_waiting_producer() {
        let stream_id = StreamId::new(1).expect("test stream id is valid");
        let budget = ResponseByteBudget::new(64);
        let mut writer = WriterState::new_test(TestWriter);
        writer
            .send_headers(stream_id, status_code::OK, ResponseHeaders::new(), false)
            .await
            .expect("response headers are queued");
        let credit = budget
            .acquire(64)
            .await
            .expect("budget stays open")
            .expect("body consumes credit");
        writer
            .process_command(WriterCommand::SendData {
                stream_id,
                data: PayloadBytes::from(bytes::Bytes::from_static(b"body")),
                credit: Some(credit),
                end_stream: false,
            })
            .await
            .expect("body enters pending stream state");
        let next = tokio::spawn({
            let budget = budget.clone();
            async move { budget.acquire(64).await }
        });
        tokio::task::yield_now().await;
        assert!(!next.is_finished(), "pending stream body keeps its credit");
        writer
            .reset_stream(stream_id, ErrorCode::CANCEL)
            .await
            .expect("local reset succeeds");
        drop(writer);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), next)
                .await
                .expect("reset releases a blocked producer")
                .expect("producer task joins")
                .expect("budget remains open")
                .is_some()
        );
    }

    #[tokio::test]
    async fn response_byte_credit_cancellation_wakes_waiting_producer() {
        let stream_id = StreamId::new(1).expect("test stream id is valid");
        let budget = ResponseByteBudget::new(64);
        let writer = WriterState::new_test(TestWriter);
        let credit = budget
            .acquire(64)
            .await
            .expect("budget stays open")
            .expect("body consumes credit");
        writer
            .ingress
            .enqueue(stream_id, WriterCommand::SendData {
                stream_id,
                data: PayloadBytes::from(bytes::Bytes::from_static(b"body")),
                credit: Some(credit),
                end_stream: false,
            })
            .await
            .expect("cancellable app write enters ingress");
        let next = tokio::spawn({
            let budget = budget.clone();
            async move { budget.acquire(64).await }
        });
        tokio::task::yield_now().await;
        assert!(!next.is_finished(), "undrained app write keeps its credit");
        writer.drop_ingress_stream(stream_id).await;
        drop(writer);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), next)
                .await
                .expect("cancelling an app write releases a blocked producer")
                .expect("producer task joins")
                .expect("budget remains open")
                .is_some()
        );
    }

    #[tokio::test]
    async fn response_byte_credit_connection_teardown_wakes_waiting_producer() {
        let stream_id = StreamId::new(1).expect("test stream id is valid");
        let budget = ResponseByteBudget::new(64);
        let mut writer = WriterState::new_test(TestWriter);
        let credit = budget
            .acquire(64)
            .await
            .expect("budget stays open")
            .expect("body consumes credit");
        writer
            .send_headers(stream_id, status_code::OK, ResponseHeaders::new(), false)
            .await
            .expect("response headers are queued");
        writer
            .process_command(WriterCommand::SendData {
                stream_id,
                data: PayloadBytes::from(bytes::Bytes::from_static(b"body")),
                credit: Some(credit),
                end_stream: false,
            })
            .await
            .expect("body enters stream state before teardown");
        let next = tokio::spawn({
            let budget = budget.clone();
            async move { budget.acquire(64).await }
        });
        tokio::task::yield_now().await;
        assert!(!next.is_finished(), "pending stream body keeps its credit");
        drop(writer);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), next)
                .await
                .expect("connection teardown releases a blocked producer")
                .expect("producer task joins")
                .expect("budget remains open")
                .is_some()
        );
    }

    #[test]
    fn settings_initial_window_update_is_atomic_at_every_boundary() {
        let mut writer = WriterState::new_test(TestWriter);
        let first = StreamId::new(1).expect("stream id is non-zero");
        let second = StreamId::new(3).expect("stream id is non-zero");
        let default = i64::from(h2_frame::DEFAULT_WINDOW_SIZE);
        let max = i64::from(h2_frame::MAX_FLOW_CONTROL_WINDOW);
        let default_stream = i32::try_from(default).expect("HTTP/2 window fits i32");
        let max_stream = i32::try_from(max).expect("HTTP/2 window fits i32");
        writer.streams = new_stream_map();
        writer
            .streams
            .insert(first, StreamWriteState::new(default + 1));
        writer
            .streams
            .insert(second, StreamWriteState::new(default));

        let unchanged_frame_size = writer.peer_max_frame_size;
        let overflow = PeerSettings {
            header_table_size: Some(0),
            initial_window_size: Some(h2_frame::MAX_FLOW_CONTROL_WINDOW),
            max_frame_size: FramePayloadLen::new(h2_frame::DEFAULT_MAX_FRAME_SIZE * 2),
        };
        assert!(writer.update_peer_settings(overflow).is_err());
        assert_eq!(writer.initial_stream_send_window, default);
        assert_eq!(writer.streams[&first].send_window, default_stream + 1);
        assert_eq!(writer.streams[&second].send_window, default_stream);
        assert_eq!(writer.peer_max_frame_size, unchanged_frame_size);

        writer
            .streams
            .get_mut(&first)
            .expect("first stream exists")
            .send_window = default_stream;
        writer
            .update_peer_settings(PeerSettings {
                header_table_size: None,
                initial_window_size: Some(h2_frame::MAX_FLOW_CONTROL_WINDOW),
                max_frame_size: None,
            })
            .expect("the exact maximum is legal");
        assert_eq!(writer.initial_stream_send_window, max);
        assert_eq!(writer.streams[&first].send_window, max_stream);
        assert_eq!(writer.streams[&second].send_window, max_stream);

        writer
            .update_peer_settings(PeerSettings {
                header_table_size: None,
                initial_window_size: Some(0),
                max_frame_size: None,
            })
            .expect("zero is legal");
        assert_eq!(writer.initial_stream_send_window, 0);
        assert_eq!(writer.streams[&first].send_window, 0);
        assert_eq!(writer.streams[&second].send_window, 0);
    }

    #[test]
    fn settings_changes_preserve_signed_stream_window_debt() {
        let stream_id = StreamId::new(1).expect("stream id is non-zero");
        let max = i64::from(h2_frame::MAX_FLOW_CONTROL_WINDOW);
        let max_stream = i32::try_from(max).expect("HTTP/2 window fits i32");
        let mut writer = WriterState::new_test(TestWriter);
        writer.initial_stream_send_window = max;
        writer.streams.insert(stream_id, StreamWriteState::new(max));
        writer
            .streams
            .get_mut(&stream_id)
            .expect("stream exists")
            .send_window = 0;

        writer
            .update_peer_settings(PeerSettings {
                header_table_size: None,
                initial_window_size: Some(0),
                max_frame_size: None,
            })
            .expect("lowering SETTINGS_INITIAL_WINDOW_SIZE is valid");
        assert_eq!(writer.streams[&stream_id].send_window, -max_stream);

        writer
            .update_peer_settings(PeerSettings {
                header_table_size: None,
                initial_window_size: Some(h2_frame::MAX_FLOW_CONTROL_WINDOW),
                max_frame_size: None,
            })
            .expect("restoring SETTINGS_INITIAL_WINDOW_SIZE is valid");
        assert_eq!(writer.streams[&stream_id].send_window, 0);
    }
}
