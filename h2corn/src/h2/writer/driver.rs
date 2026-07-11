use std::collections::VecDeque;
use std::collections::hash_map::Entry;
use std::fs::File;
use std::future::Future;
use std::mem;
use std::mem::take;
use std::num::NonZeroUsize;
use std::sync::Arc;
#[cfg(test)]
use std::{num::NonZeroU32, time::Duration};

use bytes::BytesMut;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::futures::Notified;
use tokio::time::Instant as TokioInstant;

use super::flush::{
    FlushPassResult, FlushTracking, flush_pending_data_tracked, outbound_data_frame_size,
    send_limit, write_frame, write_frame_buf,
};
use super::header_encode::{HeaderEncodeState, write_header_block};
use super::ingress::{QueuedStreamCommands, WriterIngress};
use super::stream_state::{StreamWriteState, notify_response_close, writer_stream};
use super::{
    FRAME_BUFFER_CAPACITY, H2_WRITER_BUFFER_CAPACITY, PrefixedData, ResponseCloseBatch,
    ResponseDeadlineUpdateBatch, WindowTarget, WriterCommand, WriterCommandBatch,
};
use crate::bridge::PayloadBytes;
use crate::config::ServerConfig;
#[cfg(test)]
use crate::config::{
    BindTarget, Http1Config, Http2Config, ProxyConfig, ResponseHeaderConfig, WebSocketConfig,
};
use crate::error::H2CornError;
use crate::h2::deadline::DeadlineQueue;
use crate::h2::{LAZY_STREAM_CAPACITY, StreamMap, new_stream_map};
use crate::h2_frame::{self, ErrorCode, PeerSettings, Settings, StreamId, WindowIncrement};
use crate::http::header::apply_default_response_headers;
use crate::http::pathsend::PathStreamer;
use crate::http::types::{HttpStatusCode, ResponseHeaders};
#[cfg(test)]
use crate::proxy_protocol::ProxyProtocolMode;
use crate::sendfile::WriteTarget;

#[derive(Clone)]
pub(crate) struct ConnectionHandle {
    ingress: Arc<WriterIngress>,
    config: Arc<ServerConfig>,
}

pub(crate) struct WriterState<W> {
    ingress: Arc<WriterIngress>,
    writer: BufWriter<W>,
    frame_buf: BytesMut,
    config: Arc<ServerConfig>,
    streams: StreamMap<StreamWriteState>,
    ready_streams: VecDeque<StreamId>,
    drained_app_writes: Vec<(StreamId, QueuedStreamCommands)>,
    response_closes: ResponseCloseBatch,
    connection_send_window: i64,
    initial_stream_send_window: i64,
    peer_max_frame_size: usize,
    header_state: HeaderEncodeState,
    response_deadlines: DeadlineQueue<StreamId>,
    response_deadline_updates: ResponseDeadlineUpdateBatch,
}

struct WriterSendParts<'a, W> {
    writer: &'a mut BufWriter<W>,
    frame_buf: &'a mut BytesMut,
    streams: &'a mut StreamMap<StreamWriteState>,
    ready_streams: &'a mut VecDeque<StreamId>,
    response_closes: &'a mut ResponseCloseBatch,
    connection_send_window: &'a mut i64,
    initial_stream_send_window: i64,
    peer_max_frame_size: usize,
    header_state: &'a mut HeaderEncodeState,
}

struct WriterLoopParts<'a, W> {
    writer: &'a mut BufWriter<W>,
    frame_buf: &'a mut BytesMut,
    streams: &'a mut StreamMap<StreamWriteState>,
    ready_streams: &'a mut VecDeque<StreamId>,
    response_closes: &'a mut ResponseCloseBatch,
    connection_send_window: &'a mut i64,
    initial_stream_send_window: &'a mut i64,
    peer_max_frame_size: &'a mut usize,
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

impl ConnectionHandle {
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
        end_stream: bool,
    ) -> impl Future<Output = Result<(), H2CornError>> + '_ {
        self.send_command(stream_id, WriterCommand::SendData {
            stream_id,
            data: data.into(),
            end_stream,
        })
    }

    pub(crate) fn send_prefixed_data(
        &self,
        stream_id: StreamId,
        prefix: &[u8],
        payload: PayloadBytes,
        end_stream: bool,
    ) -> impl Future<Output = Result<(), H2CornError>> + '_ {
        self.send_command(stream_id, WriterCommand::SendPrefixedData {
            stream_id,
            data: Box::new(PrefixedData::new(prefix, payload)),
            end_stream,
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
    fn outbound_data_frame_size(&self) -> usize {
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
            ingress: WriterIngress::new(max_concurrent_streams as usize),
            writer: BufWriter::new(writer),
            frame_buf: BytesMut::with_capacity(FRAME_BUFFER_CAPACITY),
            config: Arc::new(ServerConfig {
                binds: Box::new([BindTarget::Tcp {
                    host: Box::from("127.0.0.1"),
                    port: 8000,
                }]),
                access_log: false,
                root_path: Box::from(""),
                limit_request_fields: None,
                http1: Http1Config::default(),
                http2: Http2Config {
                    max_concurrent_streams,
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
                timeout_handshake: Duration::from_secs(5),
                response_headers: ResponseHeaderConfig::default(),
            }),
            streams: new_stream_map(max_concurrent_streams as usize),
            ready_streams: VecDeque::with_capacity(max_concurrent_streams as usize),
            drained_app_writes: Vec::with_capacity(max_concurrent_streams as usize),
            response_closes: ResponseCloseBatch::new(),
            connection_send_window: i64::from(h2_frame::DEFAULT_WINDOW_SIZE),
            initial_stream_send_window: i64::from(h2_frame::DEFAULT_WINDOW_SIZE),
            peer_max_frame_size: h2_frame::DEFAULT_MAX_FRAME_SIZE,
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

    pub(crate) fn next_response_stall_deadline(&mut self) -> Option<(StreamId, TokioInstant)> {
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

    pub(crate) async fn update_peer_settings(
        &mut self,
        settings: PeerSettings,
    ) -> Result<(), H2CornError> {
        self.process_command(WriterCommand::UpdatePeerSettings(settings))
            .await?;
        Ok(())
    }

    pub(crate) async fn peer_reset(&mut self, stream_id: StreamId) -> Result<(), H2CornError> {
        self.ingress.drop_stream(stream_id).await;
        self.process_command(WriterCommand::PeerReset { stream_id })
            .await?;
        Ok(())
    }

    pub(crate) async fn grant_send_window(
        &mut self,
        target: WindowTarget,
        increment: WindowIncrement,
    ) -> Result<bool, H2CornError> {
        self.process_command(WriterCommand::GrantSendWindow { target, increment })
            .await
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
    write_frame_buf(writer, frame_buf).await?;
    notify_response_close(response_closes, stream_id);
    Ok(())
}

fn apply_peer_settings_to_writer(
    settings: PeerSettings,
    streams: &mut StreamMap<StreamWriteState>,
    initial_stream_send_window: &mut i64,
    peer_max_frame_size: &mut usize,
    header_state: &mut HeaderEncodeState,
) {
    if let Some(size) = settings.initial_window_size {
        let delta = i64::from(size) - *initial_stream_send_window;
        *initial_stream_send_window = i64::from(size);
        for stream in streams.values_mut() {
            stream.send_window += delta;
        }
    }
    if let Some(size) = settings.max_frame_size {
        *peer_max_frame_size = size.get() as usize;
    }
    if let Some(size) = settings.header_table_size {
        header_state.update_max_size(size);
    }
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
        context.streams.remove(&stream_id);
        notify_response_close(context.response_closes, stream_id);
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
        notify_response_close(context.response_closes, stream_id);
    }

    Ok(())
}

async fn handle_send_trailers<W>(
    context: &mut WriterSendParts<'_, W>,
    stream_id: StreamId,
    headers: ResponseHeaders,
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
    stream.schedule(context.ready_streams, stream_id, true);
    Ok(())
}

async fn handle_send_data<W>(
    context: &mut WriterSendParts<'_, W>,
    stream_id: StreamId,
    data: PayloadBytes,
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
    if stream.queue_data(data, end_stream).is_err() {
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
    stream.schedule(context.ready_streams, stream_id, false);
    Ok(())
}

async fn handle_send_prefixed_data<W>(
    context: &mut WriterSendParts<'_, W>,
    stream_id: StreamId,
    data: Box<PrefixedData>,
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
    if stream.queue_prefixed_data(data, end_stream).is_err() {
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
    stream.schedule(context.ready_streams, stream_id, false);
    Ok(())
}

async fn handle_send_final<W>(
    context: &mut WriterSendParts<'_, W>,
    stream_id: StreamId,
    status: HttpStatusCode,
    headers: ResponseHeaders,
    data: PayloadBytes,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    let end_stream = data.is_empty();
    if !end_stream
        && !context.streams.contains_key(&stream_id)
        && send_limit(
            *context.connection_send_window,
            context.initial_stream_send_window,
            context.outbound_data_frame_size(),
        )
        .is_some_and(|limit| data.len() <= limit)
    {
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
            notify_response_close(context.response_closes, stream_id);
            return Ok(());
        }

        // Single-shot DATA frame into the BufWriter: small responses
        // coalesce with the HEADERS frame into one sendto on flush.
        write_frame(
            context.writer,
            h2_frame::FrameHeader {
                frame_type: h2_frame::FrameType::DATA,
                flags: h2_frame::FrameFlags::END_STREAM,
                stream_id: Some(stream_id),
            },
            data.as_ref(),
        )
        .await?;
        *context.connection_send_window -= data.len() as i64;

        notify_response_close(context.response_closes, stream_id);
        return Ok(());
    }

    handle_send_headers(context, stream_id, status, headers, end_stream).await?;
    if end_stream {
        return Ok(());
    }
    handle_send_data(context, stream_id, data, true).await
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
    stream.schedule(context.ready_streams, stream_id, false);
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

async fn handle_grant_stream_window<W>(
    context: &mut WriterLoopParts<'_, W>,
    stream_id: StreamId,
    increment: i64,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    let overflow = match context.streams.entry(stream_id) {
        Entry::Occupied(mut entry) => {
            let stream = entry.get_mut();
            if stream.send_window > i64::from(h2_frame::MAX_FLOW_CONTROL_WINDOW) - increment {
                true
            } else {
                stream.send_window += increment;
                if stream.has_pending_output() && !stream.is_closed() {
                    stream.schedule(context.ready_streams, stream_id, false);
                }
                false
            }
        },
        Entry::Vacant(entry) => {
            let stream = entry.insert(StreamWriteState::new(*context.initial_stream_send_window));
            if stream.send_window > i64::from(h2_frame::MAX_FLOW_CONTROL_WINDOW) - increment {
                true
            } else {
                stream.send_window += increment;
                false
            }
        },
    };
    if overflow {
        context.streams.remove(&stream_id);
        h2_frame::append_rst_stream(context.frame_buf, stream_id, ErrorCode::FLOW_CONTROL_ERROR);
        write_frame_buf(context.writer, context.frame_buf).await?;
        notify_response_close(context.response_closes, stream_id);
    }
    Ok(())
}

async fn handle_grant_connection_window<W>(
    context: &mut WriterLoopParts<'_, W>,
    increment: i64,
) -> Result<bool, H2CornError>
where
    W: AsyncWrite + Unpin,
{
    if *context.connection_send_window > i64::from(h2_frame::MAX_FLOW_CONTROL_WINDOW) - increment {
        h2_frame::append_goaway(
            context.frame_buf,
            None,
            ErrorCode::FLOW_CONTROL_ERROR,
            b"connection flow-control window overflow",
        );
        write_frame_buf(context.writer, context.frame_buf).await?;
        context.writer.flush().await?;
        return Ok(true);
    }
    *context.connection_send_window += increment;
    Ok(false)
}

async fn handle_grant_send_window<W>(
    context: &mut WriterLoopParts<'_, W>,
    target: WindowTarget,
    increment: WindowIncrement,
) -> Result<bool, H2CornError>
where
    W: AsyncWrite + Unpin,
{
    let increment = i64::from(increment.get());
    match target {
        WindowTarget::Stream(stream_id) => {
            handle_grant_stream_window(context, stream_id, increment).await?;
            Ok(false)
        },
        WindowTarget::Connection => handle_grant_connection_window(context, increment).await,
    }
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

#[expect(
    clippy::too_many_lines,
    reason = "one exhaustive match keeps the closed writer-command dispatcher auditable"
)]
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
        WriterCommand::UpdatePeerSettings(settings) => {
            apply_peer_settings_to_writer(
                settings,
                context.streams,
                context.initial_stream_send_window,
                context.peer_max_frame_size,
                context.header_state,
            );
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
        } => {
            let mut send = context.send_context();
            handle_send_final(&mut send, stream_id, status, headers, data).await?;
        },
        WriterCommand::SendTrailers { stream_id, headers } => {
            let mut send = context.send_context();
            handle_send_trailers(&mut send, stream_id, headers).await?;
        },
        WriterCommand::SendData {
            stream_id,
            data,
            end_stream,
        } => {
            handle_send_data(&mut context.send_context(), stream_id, data, end_stream).await?;
        },
        WriterCommand::SendPrefixedData {
            stream_id,
            data,
            end_stream,
        } => {
            handle_send_prefixed_data(&mut context.send_context(), stream_id, data, end_stream)
                .await?;
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
        WriterCommand::GrantSendWindow { target, increment } => {
            if handle_grant_send_window(context, target, increment).await? {
                return Ok(true);
            }
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

#[expect(
    clippy::significant_drop_tightening,
    reason = "writer ingress is intentionally kept with the initialized writer state"
)]
pub(crate) async fn init_writer<W>(
    writer: W,
    config: Arc<ServerConfig>,
    initial_peer_settings: Option<PeerSettings>,
) -> Result<(WriterState<W>, ConnectionHandle), H2CornError>
where
    W: WriteTarget,
{
    let ingress = WriterIngress::new(config.http2.max_concurrent_streams as usize);
    let mut writer = BufWriter::with_capacity(H2_WRITER_BUFFER_CAPACITY, writer);
    let mut frame_buf = BytesMut::with_capacity(FRAME_BUFFER_CAPACITY);
    let initial_settings = Settings {
        header_table_size: Some(h2_frame::DEFAULT_HEADER_TABLE_SIZE as u32),
        enable_push: Some(false),
        max_concurrent_streams: Some(config.http2.max_concurrent_streams),
        initial_window_size: Some(config.http2.initial_stream_window_size.get()),
        max_frame_size: Some(config.http2.max_inbound_frame_size),
        max_header_list_size: config
            .http2
            .max_header_list_size
            .map(NonZeroUsize::get)
            .map(|value| value as u32),
        enable_connect_protocol: Some(true),
    };
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

    let stream_capacity = config.http2.max_concurrent_streams as usize;
    let mut writer_state = WriterState {
        ingress,
        writer,
        frame_buf,
        config: Arc::clone(&config),
        streams: new_stream_map(stream_capacity),
        ready_streams: VecDeque::with_capacity(stream_capacity.min(LAZY_STREAM_CAPACITY)),
        drained_app_writes: Vec::with_capacity(stream_capacity.min(LAZY_STREAM_CAPACITY)),
        response_closes: ResponseCloseBatch::new(),
        connection_send_window: i64::from(h2_frame::DEFAULT_WINDOW_SIZE),
        initial_stream_send_window: i64::from(h2_frame::DEFAULT_WINDOW_SIZE),
        peer_max_frame_size: h2_frame::DEFAULT_MAX_FRAME_SIZE,
        header_state: HeaderEncodeState::new(),
        response_deadlines: DeadlineQueue::default(),
        response_deadline_updates: ResponseDeadlineUpdateBatch::new(),
    };

    if let Some(settings) = initial_peer_settings {
        if let Some(size) = settings.initial_window_size {
            writer_state.initial_stream_send_window = i64::from(size);
        }
        if let Some(size) = settings.max_frame_size {
            writer_state.peer_max_frame_size = size.get() as usize;
        }
        if let Some(size) = settings.header_table_size {
            writer_state.header_state.update_max_size(size);
        }
    }

    let connection = ConnectionHandle {
        ingress: Arc::clone(&writer_state.ingress),
        config,
    };
    Ok((writer_state, connection))
}
