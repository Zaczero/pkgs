use std::collections::VecDeque;
#[cfg(test)]
use std::env::temp_dir;
#[cfg(test)]
use std::fs::{File, remove_file, write};
use std::io;
#[cfg(test)]
use std::process::id;
#[cfg(test)]
use std::{
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(test)]
use bytes::Bytes;
use bytes::BytesMut;
use smallvec::SmallVec;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::time::Instant;

use super::header_encode::{HeaderEncodeState, write_header_block};
#[cfg(test)]
use super::stream_state::writer_stream;
use super::stream_state::{PendingChunk, PendingChunks, StreamBodyState, StreamWriteState};
use super::{
    FAIR_WRITE_QUANTUM, H2_OUTBOUND_DATA_FRAME_SIZE_TARGET, ResponseCloseBatch,
    ResponseDeadlineUpdateBatch,
};
use crate::error::H2CornError;
use crate::h2::StreamMap;
#[cfg(test)]
use crate::h2::new_stream_map;
use crate::h2_frame::{
    self, ErrorCode, FRAME_HEADER_LEN, FrameFlags, FrameHeader, FrameType, StreamId,
};
use crate::http::pathsend::PathStreamer;
use crate::sendfile::WriteTarget;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FlushBodyProgress {
    Continue,
    ConnectionBlocked,
}

struct FlushBodyParts<'a, W> {
    writer: &'a mut BufWriter<W>,
    ready_streams: &'a mut VecDeque<StreamId>,
    connection_send_window: &'a mut i64,
    data_frame_size: usize,
    stream_budget: usize,
    response_closes: &'a mut ResponseCloseBatch,
}

pub(super) struct FlushTracking<'a> {
    pub(super) deadline_updates: &'a mut ResponseDeadlineUpdateBatch,
    pub(super) response_closes: &'a mut ResponseCloseBatch,
}

/// One vectored-write's worth of DATA frames collected over body segments.
struct FrameBatch<'a> {
    headers: SmallVec<[[u8; FRAME_HEADER_LEN]; 9]>,
    payloads: SmallVec<[&'a [u8]; 18]>,
    payload_counts: SmallVec<[u8; 9]>,
    /// Window consumption (sum of payload lengths).
    total: usize,
    /// Leading segments fully consumed by this batch.
    drained_segments: usize,
    /// Bytes consumed from the first segment after the drained ones.
    tail_consumed: usize,
    ended_stream: bool,
    /// Stopped because the connection send window is exhausted (the caller
    /// must reschedule the stream at the front and break the flush pass).
    connection_blocked: bool,
}

impl<'a> FrameBatch<'a> {
    fn push(&mut self, header: [u8; FRAME_HEADER_LEN], payload: &'a [u8]) {
        self.headers.push(header);
        if payload.is_empty() {
            self.payload_counts.push(0);
        } else {
            self.payloads.push(payload);
            self.payload_counts.push(1);
        }
    }

    fn push_pair(&mut self, header: [u8; FRAME_HEADER_LEN], first: &'a [u8], second: &'a [u8]) {
        self.headers.push(header);
        let mut count = 0;
        if !first.is_empty() {
            self.payloads.push(first);
            count += 1;
        }
        if !second.is_empty() {
            self.payloads.push(second);
            count += 1;
        }
        self.payload_counts.push(count);
    }
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

    const fn budget_exhausted(&self) -> bool {
        self.stream_budget == 0
    }
}

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

async fn flush_chunk_body<W>(
    context: &mut FlushBodyParts<'_, W>,
    pending: &mut PendingChunks,
    stream_id: StreamId,
    stream: &mut StreamWriteState,
) -> Result<FlushBodyProgress, H2CornError>
where
    W: AsyncWrite + Unpin,
{
    if stream.is_closed() {
        return Ok(FlushBodyProgress::Continue);
    }

    // Collect ALL window/budget-allowed frames across the queued chunks,
    // then emit them in one vectored write: with peer-capped frames a
    // multi-chunk streamed body becomes a single writev instead of a
    // flush+write pair per frame.
    let (total, drained_segments, tail_consumed, ended_stream, connection_blocked) = {
        let batch = collect_chunk_data_frames(context, stream, stream_id, pending.iter());
        if !batch.headers.is_empty() {
            write_frames_vectored(
                context.writer,
                &batch.headers,
                &batch.payloads,
                &batch.payload_counts,
            )
            .await?;
        }
        (
            batch.total,
            batch.drained_segments,
            batch.tail_consumed,
            batch.ended_stream,
            batch.connection_blocked,
        )
    };

    if total != 0 || drained_segments != 0 {
        *context.connection_send_window -= total as i64;
        stream.send_window -= total as i64;
        for _ in 0..drained_segments {
            pending.pop_front();
        }
        if tail_consumed != 0
            && let Some(front) = pending.front_mut()
        {
            front.consume(tail_consumed);
        }
        stream.note_body_progress(Instant::now());
    }

    if ended_stream {
        stream.finish(stream_id, context.response_closes);
    } else if connection_blocked {
        stream.schedule(context.ready_streams, stream_id, true);
        return Ok(FlushBodyProgress::ConnectionBlocked);
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
    W: AsyncWrite + Unpin + WriteTarget,
{
    loop {
        if stream.is_closed() {
            return Ok(FlushBodyProgress::Continue);
        }

        if W::SUPPORTS_SENDFILE && streamer.prefers_sendfile && streamer.needs_fill() {
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
            let header = h2_frame::encode_frame_header(
                FrameHeader {
                    frame_type: FrameType::DATA,
                    flags: if end_stream {
                        FrameFlags::END_STREAM
                    } else {
                        FrameFlags::EMPTY
                    },
                    stream_id: Some(stream_id),
                },
                chunk_len,
            );
            let (file, offset) = streamer.sendfile_parts();
            context.writer.write_all(&header).await?;
            context.writer.flush().await?;
            W::send_file(context.writer, file, offset, chunk_len).await?;

            let chunk_len = chunk_len as i64;
            *context.connection_send_window -= chunk_len;
            stream.send_window -= chunk_len;
            streamer.advance_after_sendfile(chunk_len as usize);
            stream.note_body_progress(Instant::now());

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

        // Emit as many DATA frames as the windows, fair budget, and frame
        // size allow in ONE vectored write: with peer-capped frames
        // (typically 16 KiB) a 128 KiB buffer fill becomes a single writev
        // instead of 8 flush+write pairs.
        let (total, ended_stream, connection_blocked) = {
            let remaining = streamer.remaining();
            let may_end = streamer.end_stream && streamer.sendfile_remaining_len() == 0;
            let batch = collect_data_frames(context, stream, stream_id, [(remaining, may_end)]);
            if !batch.headers.is_empty() {
                write_frames_vectored(
                    context.writer,
                    &batch.headers,
                    &batch.payloads,
                    &batch.payload_counts,
                )
                .await?;
            }
            (batch.total, batch.ended_stream, batch.connection_blocked)
        };

        if total != 0 {
            *context.connection_send_window -= total as i64;
            stream.send_window -= total as i64;
            streamer.consume(total);
            stream.note_body_progress(Instant::now());
        }

        if ended_stream {
            stream.finish(stream_id, context.response_closes);
            return Ok(FlushBodyProgress::Continue);
        }
        if connection_blocked {
            stream.schedule(context.ready_streams, stream_id, true);
            return Ok(FlushBodyProgress::ConnectionBlocked);
        }
        if total == 0 || context.budget_exhausted() {
            return Ok(FlushBodyProgress::Continue);
        }
    }
}

fn data_frame_header(len: usize, end_stream: bool, stream_id: StreamId) -> [u8; FRAME_HEADER_LEN] {
    h2_frame::encode_frame_header(
        FrameHeader {
            frame_type: FrameType::DATA,
            flags: if end_stream {
                FrameFlags::END_STREAM
            } else {
                FrameFlags::EMPTY
            },
            stream_id: Some(stream_id),
        },
        len,
    )
}

/// Collect DATA frames over body segments until the windows, the fair-write
/// budget, or the segments themselves are exhausted. Each frame stays within
/// one segment (mirroring per-chunk framing); an empty segment becomes an
/// empty DATA frame so it can carry `END_STREAM`.
fn collect_data_frames<'a, W>(
    context: &mut FlushBodyParts<'_, W>,
    stream: &StreamWriteState,
    stream_id: StreamId,
    segments: impl IntoIterator<Item = (&'a [u8], bool)>,
) -> FrameBatch<'a> {
    let mut batch = FrameBatch {
        headers: SmallVec::new(),
        payloads: SmallVec::new(),
        payload_counts: SmallVec::new(),
        total: 0,
        drained_segments: 0,
        tail_consumed: 0,
        ended_stream: false,
        connection_blocked: false,
    };
    'segments: for (segment, may_end) in segments {
        if segment.is_empty() {
            batch.push(data_frame_header(0, may_end, stream_id), &[]);
            batch.drained_segments += 1;
            if may_end {
                batch.ended_stream = true;
                break;
            }
            continue;
        }
        let mut pos = 0;
        loop {
            if *context.connection_send_window - batch.total as i64 <= 0 {
                batch.connection_blocked = true;
                batch.tail_consumed = pos;
                break 'segments;
            }
            let Some(limit) = send_limit(
                *context.connection_send_window - batch.total as i64,
                stream.send_window - batch.total as i64,
                context.data_frame_size,
            ) else {
                batch.tail_consumed = pos;
                break 'segments;
            };
            let chunk_len = context.next_body_write_len(limit, segment.len() - pos);
            let end_stream = may_end && pos + chunk_len == segment.len();
            batch.push(
                data_frame_header(chunk_len, end_stream, stream_id),
                &segment[pos..pos + chunk_len],
            );
            batch.total += chunk_len;
            pos += chunk_len;
            if end_stream {
                batch.drained_segments += 1;
                batch.ended_stream = true;
                break 'segments;
            }
            if pos == segment.len() {
                batch.drained_segments += 1;
                if context.budget_exhausted() {
                    break 'segments;
                }
                continue 'segments;
            }
            if context.budget_exhausted() {
                batch.tail_consumed = pos;
                break 'segments;
            }
        }
    }
    batch
}

/// Collect DATA frames from logical chunks that may have two physically
/// separate buffers. A prefixed WebSocket message therefore remains one H2
/// DATA frame when it fits the peer/window limit, while writev retains the
/// original payload owner.
fn collect_chunk_data_frames<'a, W>(
    context: &mut FlushBodyParts<'_, W>,
    stream: &StreamWriteState,
    stream_id: StreamId,
    chunks: impl IntoIterator<Item = &'a PendingChunk>,
) -> FrameBatch<'a> {
    let mut batch = FrameBatch {
        headers: SmallVec::new(),
        payloads: SmallVec::new(),
        payload_counts: SmallVec::new(),
        total: 0,
        drained_segments: 0,
        tail_consumed: 0,
        ended_stream: false,
        connection_blocked: false,
    };
    'chunks: for chunk in chunks {
        let remaining_len = chunk.remaining_len();
        if remaining_len == 0 {
            batch.push(data_frame_header(0, chunk.end_stream, stream_id), &[]);
            batch.drained_segments += 1;
            if chunk.end_stream {
                batch.ended_stream = true;
                break;
            }
            continue;
        }

        let mut pos = 0;
        loop {
            if *context.connection_send_window - batch.total as i64 <= 0 {
                batch.connection_blocked = true;
                batch.tail_consumed = pos;
                break 'chunks;
            }
            let Some(limit) = send_limit(
                *context.connection_send_window - batch.total as i64,
                stream.send_window - batch.total as i64,
                context.data_frame_size,
            ) else {
                batch.tail_consumed = pos;
                break 'chunks;
            };
            let chunk_len = context.next_body_write_len(limit, remaining_len - pos);
            let end_stream = chunk.end_stream && pos + chunk_len == remaining_len;
            let (first, second) = chunk.remaining_slices(pos, chunk_len);
            batch.push_pair(
                data_frame_header(chunk_len, end_stream, stream_id),
                first,
                second,
            );
            batch.total += chunk_len;
            pos += chunk_len;
            if end_stream {
                batch.drained_segments += 1;
                batch.ended_stream = true;
                break 'chunks;
            }
            if pos == remaining_len {
                batch.drained_segments += 1;
                if context.budget_exhausted() {
                    break 'chunks;
                }
                continue 'chunks;
            }
            if context.budget_exhausted() {
                batch.tail_consumed = pos;
                break 'chunks;
            }
        }
    }
    batch
}

/// Write `[header|payload]*` as one vectored write sequence: flush the
/// buffered writer first, then drive `write_vectored` over the raw target
/// with `IoSlice::advance_slices` handling partial writes.
async fn write_frames_vectored<W>(
    writer: &mut BufWriter<W>,
    headers: &[[u8; FRAME_HEADER_LEN]],
    payloads: &[&[u8]],
    payload_counts: &[u8],
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    writer.flush().await?;
    let mut slices: SmallVec<[io::IoSlice<'_>; 18]> = SmallVec::new();
    let mut payload_index = 0;
    for (header, payload_count) in headers.iter().zip(payload_counts) {
        slices.push(io::IoSlice::new(header));
        for payload in &payloads[payload_index..payload_index + usize::from(*payload_count)] {
            slices.push(io::IoSlice::new(payload));
        }
        payload_index += usize::from(*payload_count);
    }
    debug_assert_eq!(payload_index, payloads.len());
    let mut remaining: usize = slices.iter().map(|slice| slice.len()).sum();
    let mut bufs = slices.as_mut_slice();
    while remaining > 0 {
        let written = writer.get_mut().write_vectored(bufs).await?;
        if written == 0 {
            return Err(H2CornError::from(io::Error::from(io::ErrorKind::WriteZero)));
        }
        remaining -= written;
        if remaining == 0 {
            break;
        }
        io::IoSlice::advance_slices(&mut bufs, written);
    }
    Ok(())
}

pub(super) async fn flush_pending_data_tracked<W>(
    writer: &mut BufWriter<W>,
    streams: &mut StreamMap<StreamWriteState>,
    ready_streams: &mut VecDeque<StreamId>,
    connection_send_window: &mut i64,
    peer_max_frame_size: usize,
    header_state: &mut HeaderEncodeState,
    tracking: FlushTracking<'_>,
) -> Result<FlushPassResult, H2CornError>
where
    W: WriteTarget,
{
    let mut finished = smallvec::SmallVec::<[StreamId; 8]>::new();
    let mut result = FlushPassResult::Continue;
    let data_frame_size = outbound_data_frame_size(peer_max_frame_size);

    let mut ready_turns = ready_streams.len();

    while ready_turns > 0 {
        ready_turns -= 1;
        let Some(stream_id) = ready_streams.pop_front() else {
            break;
        };
        let Some(stream) = streams.get_mut(&stream_id) else {
            continue;
        };
        let deadline_before = stream.pending_body_since();
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
            response_closes: tracking.response_closes,
        };
        let mut body = stream.take_body();
        let progress = match &mut body {
            StreamBodyState::Idle => FlushBodyProgress::Continue,
            StreamBodyState::Chunks(pending) => {
                flush_chunk_body(&mut context, pending, stream_id, stream).await?
            },
            StreamBodyState::Path(streamer) => {
                flush_path_body(&mut context, streamer, stream_id, stream).await?
            },
        };

        stream.restore_body(body);
        if let Some(trailers) = stream.take_trailers_if_body_idle() {
            let block = header_state.encode_trailers(&trailers);
            write_header_block(writer, stream_id, true, block, peer_max_frame_size).await?;
            stream.finish(stream_id, tracking.response_closes);
        }

        if deadline_before != stream.pending_body_since()
            && !tracking.deadline_updates.contains(&stream_id)
        {
            tracking.deadline_updates.push(stream_id);
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
        streams.remove(&stream_id);
        if !tracking.deadline_updates.contains(&stream_id) {
            tracking.deadline_updates.push(stream_id);
        }
    }
    Ok(result)
}

#[cfg(test)]
async fn flush_pending_data<W>(
    writer: &mut BufWriter<W>,
    streams: &mut StreamMap<StreamWriteState>,
    ready_streams: &mut VecDeque<StreamId>,
    connection_send_window: &mut i64,
    peer_max_frame_size: usize,
    header_state: &mut HeaderEncodeState,
    response_closes: &mut ResponseCloseBatch,
) -> Result<FlushPassResult, H2CornError>
where
    W: WriteTarget,
{
    let mut deadline_updates = ResponseDeadlineUpdateBatch::new();
    flush_pending_data_tracked(
        writer,
        streams,
        ready_streams,
        connection_send_window,
        peer_max_frame_size,
        header_state,
        FlushTracking {
            deadline_updates: &mut deadline_updates,
            response_closes,
        },
    )
    .await
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
    let header = h2_frame::encode_frame_header(header, payload.len());
    writer.write_all(&header).await?;
    if !payload.is_empty() {
        writer.write_all(payload).await?;
    }
    Ok(())
}

pub(super) async fn write_frame_buf<W>(
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
    use crate::bridge::PayloadBytes;
    use crate::h2::writer::PrefixedData;

    const INITIAL_STREAM_WINDOW_SIZE: u32 = 1 << 20;
    const INITIAL_CONNECTION_WINDOW_SIZE: u32 = 2 << 20;

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
            ]) & h2_frame::STREAM_ID_MASK;

            if frame_type == 0 {
                stream_ids.push(stream_id);
            }

            cursor += 9 + len;
        }

        stream_ids
    }

    fn parse_data_frames(bytes: &[u8]) -> Vec<(usize, u8, u32)> {
        let mut cursor = 0;
        let mut frames = Vec::new();

        while cursor + 9 <= bytes.len() {
            let len = ((bytes[cursor] as usize) << 16)
                | ((bytes[cursor + 1] as usize) << 8)
                | bytes[cursor + 2] as usize;
            let frame_type = bytes[cursor + 3];
            let flags = bytes[cursor + 4];
            let stream_id = u32::from_be_bytes([
                bytes[cursor + 5],
                bytes[cursor + 6],
                bytes[cursor + 7],
                bytes[cursor + 8],
            ]) & h2_frame::STREAM_ID_MASK;

            if frame_type == 0 {
                frames.push((len, flags, stream_id));
            }

            cursor += 9 + len;
        }

        frames
    }

    #[tokio::test]
    async fn prefixed_payload_is_one_h2_data_frame_without_joining_buffers() {
        let stream_id = StreamId::new(1).unwrap();
        let mut streams = new_stream_map(1);
        let mut ready_streams = VecDeque::new();
        let mut response_closes = ResponseCloseBatch::new();
        let mut connection_send_window = i64::from(INITIAL_CONNECTION_WINDOW_SIZE);
        let mut header_state = HeaderEncodeState::new();

        let payload = Bytes::from_static(b"body");
        let stream = writer_stream(
            &mut streams,
            stream_id,
            i64::from(INITIAL_STREAM_WINDOW_SIZE),
        );
        stream.open_response(false).unwrap();
        stream
            .queue_prefixed_data(
                Box::new(PrefixedData::new(b"\x82\x04", PayloadBytes::from(payload))),
                false,
            )
            .unwrap();
        stream.schedule(&mut ready_streams, stream_id, false);

        let mut writer = BufWriter::new(RecordingWriter::default());
        flush_pending_data(
            &mut writer,
            &mut streams,
            &mut ready_streams,
            &mut connection_send_window,
            16 * 1024,
            &mut header_state,
            &mut response_closes,
        )
        .await
        .unwrap();
        writer.flush().await.unwrap();

        assert_eq!(parse_data_frames(&writer.get_ref().bytes), vec![(
            6,
            0,
            stream_id.get()
        )]);
        assert_eq!(parse_data_payload(&writer.get_ref().bytes), b"\x82\x04body");
    }

    #[tokio::test]
    async fn prefixed_payload_resumes_across_prefix_window_boundary() {
        let stream_id = StreamId::new(1).unwrap();
        let mut streams = new_stream_map(1);
        let mut ready_streams = VecDeque::new();
        let mut response_closes = ResponseCloseBatch::new();
        let mut connection_send_window = i64::from(INITIAL_CONNECTION_WINDOW_SIZE);
        let mut header_state = HeaderEncodeState::new();

        let stream = writer_stream(&mut streams, stream_id, 1);
        stream.open_response(false).unwrap();
        stream
            .queue_prefixed_data(
                Box::new(PrefixedData::new(
                    b"\x82\x04",
                    PayloadBytes::from(Bytes::from_static(b"body")),
                )),
                true,
            )
            .unwrap();
        stream.schedule(&mut ready_streams, stream_id, false);

        let mut writer = BufWriter::new(RecordingWriter::default());
        flush_pending_data(
            &mut writer,
            &mut streams,
            &mut ready_streams,
            &mut connection_send_window,
            16 * 1024,
            &mut header_state,
            &mut response_closes,
        )
        .await
        .unwrap();
        writer.flush().await.unwrap();
        assert_eq!(parse_data_frames(&writer.get_ref().bytes), vec![(1, 0, 1)]);

        let stream = streams.get_mut(&stream_id).unwrap();
        stream.send_window += 8;
        stream.schedule(&mut ready_streams, stream_id, false);
        flush_pending_data(
            &mut writer,
            &mut streams,
            &mut ready_streams,
            &mut connection_send_window,
            16 * 1024,
            &mut header_state,
            &mut response_closes,
        )
        .await
        .unwrap();
        writer.flush().await.unwrap();

        assert_eq!(parse_data_frames(&writer.get_ref().bytes), vec![
            (1, 0, 1),
            (5, FrameFlags::END_STREAM.bits(), 1)
        ]);
        assert_eq!(parse_data_payload(&writer.get_ref().bytes), b"\x82\x04body");
        assert!(streams.is_empty());
    }

    #[tokio::test]
    async fn pathsend_final_buffered_chunk_carries_end_stream() {
        let stream_id = StreamId::new(1).unwrap();
        let path = temp_dir().join(format!("h2corn-flush-pathsend-{}", id()));
        write(&path, b"abc").unwrap();
        let file = File::open(&path).unwrap();

        let mut streams = new_stream_map(1);
        let mut ready_streams = VecDeque::new();
        let mut response_closes = ResponseCloseBatch::new();
        let mut connection_send_window = i64::from(INITIAL_CONNECTION_WINDOW_SIZE);
        let mut header_state = HeaderEncodeState::new();

        let stream = writer_stream(
            &mut streams,
            stream_id,
            i64::from(INITIAL_STREAM_WINDOW_SIZE),
        );
        stream.open_response(false).unwrap();
        stream
            .queue_path(PathStreamer::new(file, b"abc".len(), true))
            .unwrap();
        stream.schedule(&mut ready_streams, stream_id, false);

        let recording = RecordingWriter::default();
        let mut writer = BufWriter::new(recording);

        flush_pending_data(
            &mut writer,
            &mut streams,
            &mut ready_streams,
            &mut connection_send_window,
            INITIAL_STREAM_WINDOW_SIZE as usize,
            &mut header_state,
            &mut response_closes,
        )
        .await
        .unwrap();
        writer.flush().await.unwrap();

        let frames = parse_data_frames(&writer.get_ref().bytes);
        assert_eq!(frames, vec![(
            b"abc".len(),
            FrameFlags::END_STREAM.bits(),
            stream_id.get()
        )]);
        assert!(ready_streams.is_empty());
        assert!(streams.is_empty());
        assert_eq!(response_closes.as_slice(), &[stream_id]);

        remove_file(path).unwrap();
    }

    /// Reconstruct the concatenated DATA payload bytes for one stream.
    fn parse_data_payload(bytes: &[u8]) -> Vec<u8> {
        let mut cursor = 0;
        let mut payload = Vec::new();
        while cursor + 9 <= bytes.len() {
            let len = ((bytes[cursor] as usize) << 16)
                | ((bytes[cursor + 1] as usize) << 8)
                | bytes[cursor + 2] as usize;
            if bytes[cursor + 3] == 0 {
                payload.extend_from_slice(&bytes[cursor + 9..cursor + 9 + len]);
            }
            cursor += 9 + len;
        }
        payload
    }

    /// A multi-chunk streamed body is emitted as one cross-chunk frame batch:
    /// frames stay within chunks, bytes arrive identical, and `END_STREAM`
    /// lands exactly on the final frame.
    #[tokio::test]
    async fn multi_chunk_body_flushes_in_one_cross_chunk_batch() {
        let stream_id = StreamId::new(1).unwrap();
        let frame_size = 16 * 1024;

        let mut streams = new_stream_map(1);
        let mut ready_streams = VecDeque::new();
        let mut response_closes = ResponseCloseBatch::new();
        let mut connection_send_window = i64::from(INITIAL_CONNECTION_WINDOW_SIZE);
        let mut header_state = HeaderEncodeState::new();

        let stream = writer_stream(
            &mut streams,
            stream_id,
            i64::from(INITIAL_STREAM_WINDOW_SIZE),
        );
        stream.open_response(false).unwrap();
        let chunk_a = vec![b'a'; 20 * 1024];
        let chunk_b = vec![b'b'; 10 * 1024];
        let chunk_c = vec![b'c'; 4];
        stream
            .queue_data(Bytes::from(chunk_a.clone()).into(), false)
            .unwrap();
        stream
            .queue_data(Bytes::from(chunk_b.clone()).into(), false)
            .unwrap();
        stream
            .queue_data(Bytes::from(chunk_c.clone()).into(), true)
            .unwrap();
        stream.schedule(&mut ready_streams, stream_id, false);

        let recording = RecordingWriter::default();
        let mut writer = BufWriter::new(recording);

        flush_pending_data(
            &mut writer,
            &mut streams,
            &mut ready_streams,
            &mut connection_send_window,
            frame_size,
            &mut header_state,
            &mut response_closes,
        )
        .await
        .unwrap();
        writer.flush().await.unwrap();

        let bytes = &writer.get_ref().bytes;
        // Frames never span chunks: 20K -> 16K + 4K, 10K, 4-byte END_STREAM.
        assert_eq!(parse_data_frames(bytes), vec![
            (frame_size, 0, stream_id.get()),
            (4 * 1024, 0, stream_id.get()),
            (10 * 1024, 0, stream_id.get()),
            (4, FrameFlags::END_STREAM.bits(), stream_id.get()),
        ]);
        let expected: Vec<u8> = [chunk_a, chunk_b, chunk_c].concat();
        assert_eq!(parse_data_payload(bytes), expected);
        assert!(streams.is_empty());
        assert_eq!(response_closes.as_slice(), &[stream_id]);
        assert_eq!(
            connection_send_window,
            i64::from(INITIAL_CONNECTION_WINDOW_SIZE) - expected.len() as i64
        );
    }

    /// Empty chunks become empty DATA frames in the same batch; `END_STREAM`
    /// may ride on an empty frame.
    #[tokio::test]
    async fn empty_chunks_emit_empty_data_frames_in_batch() {
        let stream_id = StreamId::new(1).unwrap();

        let mut streams = new_stream_map(1);
        let mut ready_streams = VecDeque::new();
        let mut response_closes = ResponseCloseBatch::new();
        let mut connection_send_window = i64::from(INITIAL_CONNECTION_WINDOW_SIZE);
        let mut header_state = HeaderEncodeState::new();

        let stream = writer_stream(
            &mut streams,
            stream_id,
            i64::from(INITIAL_STREAM_WINDOW_SIZE),
        );
        stream.open_response(false).unwrap();
        stream
            .queue_data(Bytes::from_static(b"").into(), false)
            .unwrap();
        stream
            .queue_data(Bytes::from_static(b"body").into(), false)
            .unwrap();
        stream
            .queue_data(Bytes::from_static(b"").into(), true)
            .unwrap();
        stream.schedule(&mut ready_streams, stream_id, false);

        let recording = RecordingWriter::default();
        let mut writer = BufWriter::new(recording);

        flush_pending_data(
            &mut writer,
            &mut streams,
            &mut ready_streams,
            &mut connection_send_window,
            16 * 1024,
            &mut header_state,
            &mut response_closes,
        )
        .await
        .unwrap();
        writer.flush().await.unwrap();

        assert_eq!(parse_data_frames(&writer.get_ref().bytes), vec![
            (0, 0, stream_id.get()),
            (4, 0, stream_id.get()),
            (0, FrameFlags::END_STREAM.bits(), stream_id.get()),
        ]);
        assert!(streams.is_empty());
        assert_eq!(response_closes.as_slice(), &[stream_id]);
    }

    /// A stream-window-limited batch consumes the chunk partially and the
    /// remainder flushes after a window grant.
    #[tokio::test]
    async fn stream_window_limits_batch_and_resumes_after_grant() {
        let stream_id = StreamId::new(1).unwrap();
        let window = 8 * 1024_i64;
        let body = vec![b'z'; 12 * 1024];

        let mut streams = new_stream_map(1);
        let mut ready_streams = VecDeque::new();
        let mut response_closes = ResponseCloseBatch::new();
        let mut connection_send_window = i64::from(INITIAL_CONNECTION_WINDOW_SIZE);
        let mut header_state = HeaderEncodeState::new();

        let stream = writer_stream(&mut streams, stream_id, window);
        stream.open_response(false).unwrap();
        stream
            .queue_data(Bytes::from(body.clone()).into(), true)
            .unwrap();
        stream.schedule(&mut ready_streams, stream_id, false);

        let recording = RecordingWriter::default();
        let mut writer = BufWriter::new(recording);

        flush_pending_data(
            &mut writer,
            &mut streams,
            &mut ready_streams,
            &mut connection_send_window,
            16 * 1024,
            &mut header_state,
            &mut response_closes,
        )
        .await
        .unwrap();
        writer.flush().await.unwrap();

        assert_eq!(parse_data_frames(&writer.get_ref().bytes), vec![(
            window as usize,
            0,
            stream_id.get()
        )]);
        assert!(response_closes.as_slice().is_empty());

        // Window grant -> the remaining 4 KiB flushes with END_STREAM.
        let stream = streams.get_mut(&stream_id).unwrap();
        stream.send_window += window;
        stream.schedule(&mut ready_streams, stream_id, false);

        flush_pending_data(
            &mut writer,
            &mut streams,
            &mut ready_streams,
            &mut connection_send_window,
            16 * 1024,
            &mut header_state,
            &mut response_closes,
        )
        .await
        .unwrap();
        writer.flush().await.unwrap();

        let bytes = &writer.get_ref().bytes;
        assert_eq!(parse_data_frames(bytes), vec![
            (window as usize, 0, stream_id.get()),
            (4 * 1024, FrameFlags::END_STREAM.bits(), stream_id.get()),
        ]);
        assert_eq!(parse_data_payload(bytes), body);
        assert!(streams.is_empty());
        assert_eq!(response_closes.as_slice(), &[stream_id]);
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
        assert_eq!(ready_streams, VecDeque::from([stream_a]));
        assert!(streams.contains_key(&stream_a));
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
        assert_eq!(stream_ids, vec![
            stream_a.get(),
            stream_b.get(),
            stream_a.get()
        ]);
        assert!(ready_streams.is_empty());
        assert!(streams.is_empty());
        assert_eq!(response_closes.as_slice(), &[stream_a]);
    }
}
