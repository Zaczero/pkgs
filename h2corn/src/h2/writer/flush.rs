use std::collections::VecDeque;

#[cfg(test)]
use bytes::Bytes;
use bytes::BytesMut;
#[cfg(test)]
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
#[cfg(test)]
use tokio::fs::File;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};

#[cfg(test)]
use crate::config::{INITIAL_CONNECTION_WINDOW_SIZE, INITIAL_STREAM_WINDOW_SIZE};
use crate::error::H2CornError;
use crate::frame::{self, ErrorCode, FrameFlags, FrameHeader, FrameType, StreamId};
use crate::h2::StreamMap;
#[cfg(test)]
use crate::h2::new_stream_map;
use crate::http::pathsend::PathStreamer;

use super::header_encode::{HeaderEncodeState, write_header_block};
#[cfg(test)]
use super::stream_state::writer_stream;
use super::stream_state::{PendingChunks, StreamBodyState, StreamWriteState};
use super::{
    FAIR_WRITE_QUANTUM, H2_OUTBOUND_DATA_FRAME_SIZE_TARGET, H2WriteTarget, ResponseCloseBatch,
};

pub(super) fn send_limit(
    connection_send_window: i64,
    stream_send_window: i64,
    data_frame_size: usize,
) -> Option<usize> {
    if connection_send_window <= 0 || stream_send_window <= 0 {
        return None;
    }
    Some(usize::min(
        data_frame_size,
        usize::min(connection_send_window as usize, stream_send_window as usize),
    ))
}

pub(super) fn outbound_data_frame_size(peer_max_frame_size: usize) -> usize {
    peer_max_frame_size.min(H2_OUTBOUND_DATA_FRAME_SIZE_TARGET)
}

fn fair_write_quantum(max_frame_size: usize) -> usize {
    max_frame_size.max(FAIR_WRITE_QUANTUM)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FlushBodyProgress {
    Continue,
    ConnectionBlocked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChunkAdvance {
    KeepFront,
    PopFront,
}

enum ChunkFlushStep {
    Stop,
    ConnectionBlocked,
    Continue(ChunkAdvance),
    FinishStream,
    Yield(ChunkAdvance),
}

struct FlushBodyParts<'a, W> {
    writer: &'a mut BufWriter<W>,
    ready_streams: &'a mut VecDeque<u32>,
    connection_send_window: &'a mut i64,
    data_frame_size: usize,
    stream_budget: usize,
    response_closes: &'a mut ResponseCloseBatch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FlushPassResult {
    Continue,
    ConnectionBlocked,
}

impl<W> FlushBodyParts<'_, W> {
    fn next_body_write_len(&mut self, limit: usize, remaining_len: usize) -> usize {
        let chunk_len = limit.min(remaining_len).min(self.stream_budget);
        self.stream_budget -= chunk_len;
        chunk_len
    }

    fn budget_exhausted(&self) -> bool {
        self.stream_budget == 0
    }
}

pub(super) async fn write_data_chunk<W>(
    writer: &mut BufWriter<W>,
    stream_id: StreamId,
    chunk: &[u8],
    end_stream: bool,
    connection_send_window: &mut i64,
    stream_send_window: &mut i64,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    write_frame(
        writer,
        FrameHeader {
            len: chunk.len(),
            frame_type: FrameType::DATA,
            flags: if end_stream {
                FrameFlags::END_STREAM
            } else {
                FrameFlags::EMPTY
            },
            stream_id: Some(stream_id),
        },
        chunk,
    )
    .await?;

    let chunk_len = chunk.len() as i64;
    *connection_send_window -= chunk_len;
    *stream_send_window -= chunk_len;
    Ok(())
}

async fn flush_chunk_body<W>(
    context: &mut FlushBodyParts<'_, W>,
    pending: &mut PendingChunks,
    stream_id: StreamId,
    stream: &mut StreamWriteState,
) -> Result<FlushBodyProgress, H2CornError>
where
    W: AsyncWrite + Unpin,
{
    loop {
        if stream.is_closed() {
            break;
        }

        let step = match pending.front_mut() {
            None => ChunkFlushStep::Stop,
            Some(front) => {
                if front.remaining().is_empty() {
                    write_empty_data_frame(context.writer, stream_id, front.end_stream).await?;
                    if front.end_stream {
                        ChunkFlushStep::FinishStream
                    } else {
                        ChunkFlushStep::Continue(ChunkAdvance::PopFront)
                    }
                } else if *context.connection_send_window <= 0 {
                    stream.schedule(context.ready_streams, stream_id, true);
                    ChunkFlushStep::ConnectionBlocked
                } else if let Some(limit) = send_limit(
                    *context.connection_send_window,
                    stream.send_window,
                    context.data_frame_size,
                ) {
                    let remaining = front.remaining();
                    let chunk_len = context.next_body_write_len(limit, remaining.len());
                    let end_stream = front.end_stream && chunk_len == remaining.len();

                    write_data_chunk(
                        context.writer,
                        stream_id,
                        &remaining[..chunk_len],
                        end_stream,
                        context.connection_send_window,
                        &mut stream.send_window,
                    )
                    .await?;

                    front.consume(chunk_len);
                    if end_stream {
                        ChunkFlushStep::FinishStream
                    } else if front.is_drained() {
                        if context.budget_exhausted() {
                            ChunkFlushStep::Yield(ChunkAdvance::PopFront)
                        } else {
                            ChunkFlushStep::Continue(ChunkAdvance::PopFront)
                        }
                    } else if context.budget_exhausted() {
                        ChunkFlushStep::Yield(ChunkAdvance::KeepFront)
                    } else {
                        ChunkFlushStep::Continue(ChunkAdvance::KeepFront)
                    }
                } else {
                    ChunkFlushStep::Stop
                }
            }
        };

        match step {
            ChunkFlushStep::Stop => break,
            ChunkFlushStep::ConnectionBlocked => {
                return Ok(FlushBodyProgress::ConnectionBlocked);
            }
            ChunkFlushStep::Continue(advance) => {
                if advance == ChunkAdvance::PopFront {
                    pending.pop_front();
                }
            }
            ChunkFlushStep::FinishStream => {
                pending.pop_front();
                stream.finish(stream_id, context.response_closes);
            }
            ChunkFlushStep::Yield(advance) => {
                if advance == ChunkAdvance::PopFront {
                    pending.pop_front();
                }
                break;
            }
        }
    }

    Ok(FlushBodyProgress::Continue)
}

async fn flush_path_body<W>(
    context: &mut FlushBodyParts<'_, W>,
    streamer: &mut PathStreamer,
    stream_id: StreamId,
    stream: &mut StreamWriteState,
) -> Result<FlushBodyProgress, H2CornError>
where
    W: AsyncWrite + Unpin + H2WriteTarget,
{
    loop {
        if stream.is_closed() {
            return Ok(FlushBodyProgress::Continue);
        }

        if W::SUPPORTS_SENDFILE && streamer.needs_fill() {
            if *context.connection_send_window <= 0 {
                stream.schedule(context.ready_streams, stream_id, true);
                return Ok(FlushBodyProgress::ConnectionBlocked);
            }
            let Some(limit) = send_limit(
                *context.connection_send_window,
                stream.send_window,
                context.data_frame_size,
            ) else {
                return Ok(FlushBodyProgress::Continue);
            };

            let remaining = streamer.sendfile_remaining_len();
            let chunk_len = context.next_body_write_len(limit, remaining);
            let end_stream = streamer.end_stream && chunk_len == remaining;
            let header = frame::encode_frame_header(FrameHeader {
                len: chunk_len,
                frame_type: FrameType::DATA,
                flags: if end_stream {
                    FrameFlags::END_STREAM
                } else {
                    FrameFlags::EMPTY
                },
                stream_id: Some(stream_id),
            });
            let (file, offset) = streamer.sendfile_parts();
            W::write_file_chunk(context.writer, header, file, offset, chunk_len).await?;

            let chunk_len = chunk_len as i64;
            *context.connection_send_window -= chunk_len;
            stream.send_window -= chunk_len;
            streamer.advance_after_sendfile(chunk_len as usize);

            if end_stream {
                stream.finish(stream_id, context.response_closes);
                return Ok(FlushBodyProgress::Continue);
            }
            if context.budget_exhausted() {
                return Ok(FlushBodyProgress::Continue);
            }
            continue;
        }

        if streamer.needs_fill()
            && let Err(_err) = streamer.fill().await
        {
            stream.finish(stream_id, context.response_closes);
            write_stream_reset(context.writer, stream_id, ErrorCode::INTERNAL_ERROR).await?;
            return Ok(FlushBodyProgress::Continue);
        }

        if streamer.is_drained() {
            if streamer.end_stream {
                write_empty_data_frame(context.writer, stream_id, true).await?;
                stream.finish(stream_id, context.response_closes);
            }
            return Ok(FlushBodyProgress::Continue);
        }

        if *context.connection_send_window <= 0 {
            stream.schedule(context.ready_streams, stream_id, true);
            return Ok(FlushBodyProgress::ConnectionBlocked);
        }
        let Some(limit) = send_limit(
            *context.connection_send_window,
            stream.send_window,
            context.data_frame_size,
        ) else {
            return Ok(FlushBodyProgress::Continue);
        };

        let remaining = streamer.remaining();
        let chunk_len = context.next_body_write_len(limit, remaining.len());
        let end_stream =
            streamer.end_stream && streamer.is_drained() && chunk_len == remaining.len();
        write_data_chunk(
            context.writer,
            stream_id,
            &remaining[..chunk_len],
            end_stream,
            context.connection_send_window,
            &mut stream.send_window,
        )
        .await?;

        streamer.consume(chunk_len);

        if end_stream {
            stream.finish(stream_id, context.response_closes);
            return Ok(FlushBodyProgress::Continue);
        }
        if context.budget_exhausted() {
            return Ok(FlushBodyProgress::Continue);
        }
    }
}

pub(crate) async fn flush_pending_data<W>(
    writer: &mut BufWriter<W>,
    streams: &mut StreamMap<StreamWriteState>,
    ready_streams: &mut VecDeque<u32>,
    connection_send_window: &mut i64,
    peer_max_frame_size: usize,
    header_state: &mut HeaderEncodeState,
    response_closes: &mut ResponseCloseBatch,
) -> Result<FlushPassResult, H2CornError>
where
    W: H2WriteTarget,
{
    let mut finished = smallvec::SmallVec::<[StreamId; 8]>::new();
    let mut result = FlushPassResult::Continue;
    let data_frame_size = outbound_data_frame_size(peer_max_frame_size);

    let mut ready_turns = ready_streams.len();

    while ready_turns > 0 {
        ready_turns -= 1;
        let Some(stream_id_raw) = ready_streams.pop_front() else {
            break;
        };
        let stream_id = unsafe { StreamId::new_unchecked(stream_id_raw) };
        let Some(stream) = streams.get_mut(&stream_id_raw) else {
            continue;
        };
        stream.scheduled = false;

        if *connection_send_window <= 0 && stream.has_pending_output() {
            stream.schedule(ready_streams, stream_id, true);
            result = FlushPassResult::ConnectionBlocked;
            break;
        }

        let mut context = FlushBodyParts {
            writer,
            ready_streams,
            connection_send_window,
            data_frame_size,
            stream_budget: fair_write_quantum(data_frame_size),
            response_closes,
        };
        let mut body = stream.take_body();
        let progress = match &mut body {
            StreamBodyState::Idle => FlushBodyProgress::Continue,
            StreamBodyState::Chunks(pending) => {
                flush_chunk_body(&mut context, pending, stream_id, stream).await?
            }
            StreamBodyState::Path(streamer) => {
                flush_path_body(&mut context, streamer, stream_id, stream).await?
            }
        };

        stream.restore_body(body);
        if let Some(trailers) = stream.take_trailers_if_body_idle() {
            let block = header_state.encode_trailers(&trailers)?;
            write_header_block(writer, stream_id, true, block.as_ref(), peer_max_frame_size)
                .await?;
            stream.finish(stream_id, response_closes);
        }

        if progress == FlushBodyProgress::ConnectionBlocked {
            result = FlushPassResult::ConnectionBlocked;
            break;
        }

        if stream.is_closed() {
            finished.push(stream_id);
        } else if stream.has_pending_output()
            && *connection_send_window > 0
            && stream.send_window > 0
        {
            stream.schedule(ready_streams, stream_id, false);
        }
    }

    for stream_id in finished {
        streams.remove(&stream_id.get());
    }
    Ok(result)
}

fn write_empty_data_frame<W>(
    writer: &mut W,
    stream_id: StreamId,
    end_stream: bool,
) -> impl Future<Output = Result<(), H2CornError>> + '_
where
    W: AsyncWrite + Unpin,
{
    write_frame(
        writer,
        FrameHeader {
            len: 0,
            frame_type: FrameType::DATA,
            flags: if end_stream {
                FrameFlags::END_STREAM
            } else {
                FrameFlags::EMPTY
            },
            stream_id: Some(stream_id),
        },
        &[],
    )
}

async fn write_stream_reset<W>(
    writer: &mut W,
    stream_id: StreamId,
    error_code: ErrorCode,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    write_frame(
        writer,
        FrameHeader {
            len: 4,
            frame_type: FrameType::RST_STREAM,
            flags: FrameFlags::EMPTY,
            stream_id: Some(stream_id),
        },
        &error_code.to_be_bytes(),
    )
    .await
}

pub(super) async fn write_frame<W>(
    writer: &mut W,
    header: FrameHeader,
    payload: &[u8],
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    let header = frame::encode_frame_header(header);
    writer.write_all(&header).await?;
    if !payload.is_empty() {
        writer.write_all(payload).await?;
    }
    Ok(())
}

pub(crate) async fn write_frame_buf<W>(
    writer: &mut W,
    frame_buf: &mut BytesMut,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(frame_buf.as_ref()).await?;
    frame_buf.clear();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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

    impl H2WriteTarget for RecordingWriter {
        const SUPPORTS_SENDFILE: bool = false;

        async fn write_file_chunk(
            _writer: &mut BufWriter<Self>,
            _header: [u8; 9],
            _file: &mut File,
            _offset: &mut u64,
            _len: usize,
        ) -> io::Result<()> {
            unreachable!("test writer never uses sendfile")
        }
    }

    fn parse_data_stream_ids(bytes: &[u8]) -> Vec<u32> {
        let mut cursor = 0;
        let mut stream_ids = Vec::new();

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
            ]) & frame::STREAM_ID_MASK;

            if frame_type == 0 {
                stream_ids.push(stream_id);
            }

            cursor += 9 + len;
        }

        stream_ids
    }

    #[tokio::test]
    async fn flush_pending_data_requeues_after_fair_write_quantum() {
        let stream_a = StreamId::new(1).unwrap();
        let stream_b = StreamId::new(3).unwrap();

        let mut streams = new_stream_map(2);
        let mut ready_streams = VecDeque::new();
        let mut response_closes = ResponseCloseBatch::new();
        let mut connection_send_window = i64::from(INITIAL_CONNECTION_WINDOW_SIZE);
        let mut header_state = HeaderEncodeState::new();

        let stream = writer_stream(
            &mut streams,
            stream_a,
            i64::from(INITIAL_STREAM_WINDOW_SIZE),
        );
        stream.open_response(false).unwrap();
        stream
            .queue_data(
                Bytes::from(vec![b'a'; FAIR_WRITE_QUANTUM + 32]).into(),
                true,
            )
            .unwrap();
        stream.schedule(&mut ready_streams, stream_a, false);

        let stream = writer_stream(
            &mut streams,
            stream_b,
            i64::from(INITIAL_STREAM_WINDOW_SIZE),
        );
        stream.open_response(false).unwrap();
        stream
            .queue_data(Bytes::from_static(b"tiny").into(), true)
            .unwrap();
        stream.schedule(&mut ready_streams, stream_b, false);

        let recording = RecordingWriter::default();
        let mut writer = BufWriter::new(recording);

        flush_pending_data(
            &mut writer,
            &mut streams,
            &mut ready_streams,
            &mut connection_send_window,
            FAIR_WRITE_QUANTUM,
            &mut header_state,
            &mut response_closes,
        )
        .await
        .unwrap();
        writer.flush().await.unwrap();

        let stream_ids = parse_data_stream_ids(&writer.get_ref().bytes);
        assert_eq!(stream_ids, vec![stream_a.get(), stream_b.get()]);
        assert_eq!(ready_streams, VecDeque::from([stream_a.get()]));
        assert!(streams.contains_key(&stream_a.get()));
        assert_eq!(response_closes.as_slice(), &[stream_b]);
        response_closes.clear();

        flush_pending_data(
            &mut writer,
            &mut streams,
            &mut ready_streams,
            &mut connection_send_window,
            FAIR_WRITE_QUANTUM,
            &mut header_state,
            &mut response_closes,
        )
        .await
        .unwrap();
        writer.flush().await.unwrap();

        let recorded = writer.into_inner();
        let stream_ids = parse_data_stream_ids(&recorded.bytes);
        assert_eq!(
            stream_ids,
            vec![stream_a.get(), stream_b.get(), stream_a.get()]
        );
        assert!(ready_streams.is_empty());
        assert!(streams.is_empty());
        assert_eq!(response_closes.as_slice(), &[stream_a]);
    }
}
