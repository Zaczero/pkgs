use std::{
    collections::{VecDeque, hash_map::Entry},
    future::Future,
    mem,
    num::NonZeroUsize,
    sync::Arc,
};
#[cfg(test)]
use std::{num::NonZeroU32, time::Duration};

use bytes::BytesMut;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::futures::Notified;

use crate::bridge::PayloadBytes;
#[cfg(test)]
use crate::config::{BindTarget, Http2Config, ProxyConfig};
use crate::config::{INITIAL_CONNECTION_WINDOW_SIZE, INITIAL_STREAM_WINDOW_SIZE, ServerConfig};
use crate::error::H2CornError;
use crate::frame::{self, ErrorCode, PeerSettings, Settings, StreamId, WindowIncrement};
use crate::h2::{StreamMap, new_stream_map};
use crate::http::header::apply_default_response_headers;
use crate::http::pathsend::PathStreamer;
use crate::http::types::{HttpStatusCode, ResponseHeaders};
#[cfg(test)]
use crate::proxy::ProxyProtocolMode;

use super::flush::{
    FlushPassResult, flush_pending_data, outbound_data_frame_size, send_limit, write_data_chunk,
    write_frame_buf,
};
use super::header_encode::{HeaderEncodeState, write_header_block};
use super::ingress::{QueuedStreamCommands, WriterIngress};
use super::stream_state::{StreamWriteState, notify_response_close, writer_stream};
use super::{
    FRAME_BUFFER_CAPACITY, H2_WRITER_BUFFER_CAPACITY, H2WriteTarget, ResponseCloseBatch,
    WindowTarget, WriterCommand, WriterCommandBatch,
};

#[derive(Clone)]
pub(crate) struct ConnectionHandle {
    ingress: Arc<WriterIngress>,
    config: &'static ServerConfig,
}

pub(crate) struct WriterState<W> {
    ingress: Arc<WriterIngress>,
    writer: BufWriter<W>,
    frame_buf: BytesMut,
    config: &'static ServerConfig,
    streams: StreamMap<StreamWriteState>,
    ready_streams: VecDeque<u32>,
    response_closes: ResponseCloseBatch,
    connection_send_window: i64,
    initial_stream_send_window: i64,
    peer_max_frame_size: usize,
    header_state: HeaderEncodeState,
}

struct WriterSendParts<'a, W> {
    writer: &'a mut BufWriter<W>,
    frame_buf: &'a mut BytesMut,
    streams: &'a mut StreamMap<StreamWriteState>,
    ready_streams: &'a mut VecDeque<u32>,
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
    ready_streams: &'a mut VecDeque<u32>,
    response_closes: &'a mut ResponseCloseBatch,
    connection_send_window: &'a mut i64,
    initial_stream_send_window: &'a mut i64,
    peer_max_frame_size: &'a mut usize,
    header_state: &'a mut HeaderEncodeState,
}

impl<W> WriterLoopParts<'_, W> {
    fn send_context(&mut self) -> WriterSendParts<'_, W> {
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

impl<W> WriterSendParts<'_, W> {
    fn outbound_data_frame_size(&self) -> usize {
        outbound_data_frame_size(self.peer_max_frame_size)
    }
}

async fn force_reset_stream<W>(
    writer: &mut W,
    frame_buf: &mut BytesMut,
    streams: &mut StreamMap<StreamWriteState>,
    response_closes: &mut ResponseCloseBatch,
    stream_id: StreamId,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    streams.remove(&stream_id.get());
    frame::append_rst_stream(frame_buf, stream_id, ErrorCode::INTERNAL_ERROR);
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
    let Ok(block) = context.header_state.encode_response(status, &headers) else {
        let _ = force_reset_stream(
            context.writer,
            context.frame_buf,
            context.streams,
            context.response_closes,
            stream_id,
        )
        .await;
        return Ok(());
    };

    if write_header_block(
        context.writer,
        stream_id,
        end_stream,
        block.as_ref(),
        context.peer_max_frame_size,
    )
    .await
    .is_err()
    {
        context.streams.remove(&stream_id.get());
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
        && !context.streams.contains_key(&stream_id.get())
        && send_limit(
            *context.connection_send_window,
            context.initial_stream_send_window,
            context.outbound_data_frame_size(),
        )
        .is_some_and(|limit| data.len() <= limit)
    {
        let Ok(block) = context.header_state.encode_response(status, &headers) else {
            notify_response_close(context.response_closes, stream_id);
            return Ok(());
        };

        if write_header_block(
            context.writer,
            stream_id,
            false,
            block.as_ref(),
            context.peer_max_frame_size,
        )
        .await
        .is_err()
        {
            notify_response_close(context.response_closes, stream_id);
            return Ok(());
        }

        let mut stream_send_window = context.initial_stream_send_window;
        write_data_chunk(
            context.writer,
            stream_id,
            data.as_ref(),
            true,
            context.connection_send_window,
            &mut stream_send_window,
        )
        .await?;

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
    streamer: PathStreamer,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    let stream = writer_stream(
        context.streams,
        stream_id,
        context.initial_stream_send_window,
    );
    if stream.queue_path(streamer).is_err() {
        let _ = force_reset_stream(
            context.writer,
            context.frame_buf,
            context.streams,
            context.response_closes,
            stream_id,
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
    context.streams.remove(&stream_id.get());
    frame::append_rst_stream(context.frame_buf, stream_id, error_code);
    write_frame_buf(context.writer, context.frame_buf).await?;
    notify_response_close(context.response_closes, stream_id);
    Ok(())
}

async fn process_writer_command<W>(
    context: &mut WriterLoopParts<'_, W>,
    command: WriterCommand,
) -> Result<bool, H2CornError>
where
    W: H2WriteTarget,
{
    match command {
        WriterCommand::SendSettingsAck => {
            frame::append_settings_ack(context.frame_buf);
            write_frame_buf(context.writer, context.frame_buf).await?;
        }
        WriterCommand::UpdatePeerSettings(settings) => {
            apply_peer_settings_to_writer(
                settings,
                context.streams,
                context.initial_stream_send_window,
                context.peer_max_frame_size,
                context.header_state,
            );
        }
        WriterCommand::SendHeaders {
            stream_id,
            status,
            headers,
            end_stream,
        } => {
            let mut send = context.send_context();
            handle_send_headers(&mut send, stream_id, status, headers, end_stream).await?;
        }
        WriterCommand::SendFinal {
            stream_id,
            status,
            headers,
            data,
        } => {
            let mut send = context.send_context();
            handle_send_final(&mut send, stream_id, status, headers, data).await?;
        }
        WriterCommand::SendTrailers { stream_id, headers } => {
            let mut send = context.send_context();
            handle_send_trailers(&mut send, stream_id, headers).await?;
        }
        WriterCommand::SendData {
            stream_id,
            data,
            end_stream,
        } => {
            let mut send = context.send_context();
            handle_send_data(&mut send, stream_id, data, end_stream).await?;
        }
        WriterCommand::SendPath {
            stream_id,
            streamer,
        } => {
            let mut send = context.send_context();
            handle_send_path(&mut send, stream_id, streamer).await?;
        }
        WriterCommand::FlushBufferedOutput => {
            let _ = flush_pending_data(
                context.writer,
                context.streams,
                context.ready_streams,
                context.connection_send_window,
                *context.peer_max_frame_size,
                context.header_state,
                context.response_closes,
            )
            .await?;
            context.writer.flush().await?;
            return Ok(true);
        }
        WriterCommand::SendReset {
            stream_id,
            error_code,
        } => {
            handle_send_reset(context, stream_id, error_code).await?;
        }
        WriterCommand::PeerReset { stream_id } => {
            context.streams.remove(&stream_id.get());
        }
        WriterCommand::GrantSendWindow { target, increment } => {
            let increment = i64::from(increment.get());
            match target {
                WindowTarget::Stream(stream_id) => {
                    let stream_key = stream_id.get();
                    let overflow = match context.streams.entry(stream_key) {
                        Entry::Occupied(mut entry) => {
                            let stream = entry.get_mut();
                            if stream.send_window
                                > i64::from(frame::MAX_FLOW_CONTROL_WINDOW) - increment
                            {
                                true
                            } else {
                                stream.send_window += increment;
                                if stream.has_pending_output() && !stream.is_closed() {
                                    stream.schedule(context.ready_streams, stream_id, false);
                                }
                                false
                            }
                        }
                        Entry::Vacant(entry) => {
                            let stream = entry
                                .insert(StreamWriteState::new(*context.initial_stream_send_window));
                            if stream.send_window
                                > i64::from(frame::MAX_FLOW_CONTROL_WINDOW) - increment
                            {
                                true
                            } else {
                                stream.send_window += increment;
                                false
                            }
                        }
                    };
                    if overflow {
                        context.streams.remove(&stream_key);
                        frame::append_rst_stream(
                            context.frame_buf,
                            stream_id,
                            ErrorCode::FLOW_CONTROL_ERROR,
                        );
                        write_frame_buf(context.writer, context.frame_buf).await?;
                        notify_response_close(context.response_closes, stream_id);
                    }
                }
                WindowTarget::Connection => {
                    if *context.connection_send_window
                        > i64::from(frame::MAX_FLOW_CONTROL_WINDOW) - increment
                    {
                        frame::append_goaway(
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
                }
            }
        }
        WriterCommand::SendWindowUpdate { target, increment } => {
            frame::append_window_update(
                context.frame_buf,
                match target {
                    WindowTarget::Connection => None,
                    WindowTarget::Stream(stream_id) => Some(stream_id),
                },
                increment,
            );
            write_frame_buf(context.writer, context.frame_buf).await?;
        }
        WriterCommand::PingAck(payload) => {
            frame::append_ping_ack(context.frame_buf, payload);
            write_frame_buf(context.writer, context.frame_buf).await?;
        }
        WriterCommand::Goaway {
            last_stream_id,
            error_code,
            debug,
            close,
        } => {
            frame::append_goaway(context.frame_buf, last_stream_id, error_code, &debug);
            write_frame_buf(context.writer, context.frame_buf).await?;
            if close {
                context.writer.flush().await?;
                return Ok(true);
            }
        }
    }

    Ok(false)
}

pub(crate) async fn init_writer<W>(
    writer: W,
    config: &'static ServerConfig,
    initial_peer_settings: Option<PeerSettings>,
) -> Result<(WriterState<W>, ConnectionHandle), H2CornError>
where
    W: H2WriteTarget,
{
    let ingress = WriterIngress::new(config.http2.max_concurrent_streams as usize);
    let connection = ConnectionHandle {
        ingress: Arc::clone(&ingress),
        config,
    };
    let mut writer = BufWriter::with_capacity(H2_WRITER_BUFFER_CAPACITY, writer);
    let mut frame_buf = BytesMut::with_capacity(FRAME_BUFFER_CAPACITY);
    let initial_settings = Settings {
        header_table_size: Some(frame::DEFAULT_HEADER_TABLE_SIZE as u32),
        enable_push: Some(false),
        max_concurrent_streams: Some(config.http2.max_concurrent_streams),
        initial_window_size: Some(INITIAL_STREAM_WINDOW_SIZE),
        max_frame_size: Some(config.http2.max_inbound_frame_size),
        max_header_list_size: config
            .http2
            .max_header_list_size
            .map(NonZeroUsize::get)
            .map(|value| value as u32),
        enable_connect_protocol: Some(true),
    };
    frame::append_settings(&mut frame_buf, initial_settings);
    write_frame_buf(&mut writer, &mut frame_buf).await?;
    if INITIAL_CONNECTION_WINDOW_SIZE > frame::DEFAULT_WINDOW_SIZE {
        frame::append_window_update(
            &mut frame_buf,
            None,
            WindowIncrement::new(INITIAL_CONNECTION_WINDOW_SIZE - frame::DEFAULT_WINDOW_SIZE)
                .expect("increment is positive"),
        );
        write_frame_buf(&mut writer, &mut frame_buf).await?;
    }
    writer.flush().await?;

    let mut writer_state = WriterState {
        ingress,
        writer,
        frame_buf,
        config,
        streams: new_stream_map(config.http2.max_concurrent_streams as usize),
        ready_streams: VecDeque::with_capacity(config.http2.max_concurrent_streams as usize),
        response_closes: ResponseCloseBatch::new(),
        connection_send_window: i64::from(frame::DEFAULT_WINDOW_SIZE),
        initial_stream_send_window: i64::from(frame::DEFAULT_WINDOW_SIZE),
        peer_max_frame_size: frame::DEFAULT_MAX_FRAME_SIZE,
        header_state: HeaderEncodeState::new(),
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

    Ok((writer_state, connection))
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

    pub(crate) fn config(&self) -> &'static ServerConfig {
        self.config
    }

    pub(crate) fn send_headers(
        &self,
        stream_id: StreamId,
        status: HttpStatusCode,
        mut headers: ResponseHeaders,
        end_stream: bool,
    ) -> impl Future<Output = Result<(), H2CornError>> + '_ {
        apply_default_response_headers(&mut headers, self.config);
        self.send_command(
            stream_id,
            WriterCommand::SendHeaders {
                stream_id,
                status,
                headers,
                end_stream,
            },
        )
    }

    pub(crate) fn send_data(
        &self,
        stream_id: StreamId,
        data: impl Into<PayloadBytes>,
        end_stream: bool,
    ) -> impl Future<Output = Result<(), H2CornError>> + '_ {
        self.send_command(
            stream_id,
            WriterCommand::SendData {
                stream_id,
                data: data.into(),
                end_stream,
            },
        )
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
        self.send_command(
            stream_id,
            WriterCommand::SendReset {
                stream_id,
                error_code,
            },
        )
    }
}

impl<W> WriterState<W>
where
    W: H2WriteTarget,
{
    #[cfg(test)]
    pub(crate) fn new_test(writer: W) -> Self {
        let max_concurrent_streams = 8_u32;
        Self {
            ingress: WriterIngress::new(max_concurrent_streams as usize),
            writer: BufWriter::new(writer),
            frame_buf: BytesMut::with_capacity(FRAME_BUFFER_CAPACITY),
            config: Box::leak(Box::new(ServerConfig {
                binds: Box::new([BindTarget::Tcp {
                    host: Box::from("127.0.0.1"),
                    port: 8000,
                }]),
                access_log: false,
                root_path: Box::from(""),
                http1: Default::default(),
                http2: Http2Config {
                    max_concurrent_streams,
                    max_header_list_size: None,
                    max_header_block_size: None,
                    max_inbound_frame_size: NonZeroU32::new(frame::DEFAULT_MAX_FRAME_SIZE as u32)
                        .expect("default HTTP/2 frame size is non-zero"),
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
                websocket: Default::default(),
                proxy: ProxyConfig {
                    trust_headers: false,
                    trusted_peers: Box::new([]),
                    protocol: ProxyProtocolMode::Off,
                },
                timeout_handshake: Duration::from_secs(5),
                response_headers: Default::default(),
            })),
            streams: new_stream_map(max_concurrent_streams as usize),
            ready_streams: VecDeque::with_capacity(max_concurrent_streams as usize),
            response_closes: ResponseCloseBatch::new(),
            connection_send_window: i64::from(frame::DEFAULT_WINDOW_SIZE),
            initial_stream_send_window: i64::from(frame::DEFAULT_WINDOW_SIZE),
            peer_max_frame_size: frame::DEFAULT_MAX_FRAME_SIZE,
            header_state: HeaderEncodeState::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_writer_ref(&self) -> &W {
        self.writer.get_ref()
    }

    fn command_context(&mut self) -> WriterLoopParts<'_, W> {
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
        }
    }

    async fn process_command(&mut self, command: WriterCommand) -> Result<bool, H2CornError> {
        let mut context = self.command_context();
        process_writer_command(&mut context, command).await
    }

    pub(crate) async fn drain_app_writes(&mut self) -> Result<bool, H2CornError> {
        let mut drained = self.ingress.drain().await;
        if drained.is_empty() {
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
                        return Ok(true);
                    }
                }
            }
        }

        Ok(true)
    }

    pub(crate) fn has_ready_streams(&self) -> bool {
        !self.ready_streams.is_empty()
    }

    pub(crate) fn needs_flush(&self) -> bool {
        !self.writer.buffer().is_empty()
    }

    pub(crate) async fn has_queued_app_writes(&self) -> bool {
        self.ingress.has_pending().await
    }

    pub(crate) fn outbound_notified(&self) -> Notified<'_> {
        self.ingress.notify.notified()
    }

    pub(crate) async fn flush(&mut self) -> Result<(), H2CornError> {
        self.writer.flush().await?;
        Ok(())
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
        apply_default_response_headers(&mut headers, self.config);
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
        flush_pending_data(
            &mut self.writer,
            &mut self.streams,
            &mut self.ready_streams,
            &mut self.connection_send_window,
            self.peer_max_frame_size,
            &mut self.header_state,
            &mut self.response_closes,
        )
        .await
    }
}
