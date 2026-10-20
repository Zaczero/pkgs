use std::fs::File;
use std::io::{self, Write as _};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use tokio::io::{AsyncWrite, AsyncWriteExt as _, BufWriter};

use super::{StreamWriteStates, WriterState};
use crate::bridge::PayloadBytes;
use crate::error::H2CornError;
use crate::h2::writer::header_encode::HeaderEncodeState;
use crate::h2::writer::stream_state::{ReadyStreamQueue, writer_stream};
use crate::h2::writer::wire::BYTE_QUANTUM;
use crate::h2::writer::{ResponseClose, ResponseCloseBatch, WebSocketData};
use crate::h2_frame::{self, FrameFlags, FramePayloadLen, StreamId};
use crate::http::pathsend::FileStreamer;
use crate::sendfile::WriteTarget;
use crate::websocket::session::EncodedWebSocketFrame;

const INITIAL_STREAM_WINDOW_SIZE: u32 = 1 << 20;
const INITIAL_CONNECTION_WINDOW_SIZE: u32 = 2 << 20;
const FAIR_WRITE_QUANTUM: usize = BYTE_QUANTUM;

#[expect(
    clippy::significant_drop_tightening,
    reason = "the writer and stream owners are moved back into the caller at the end"
)]
async fn flush_pending_data(
    writer: &mut BufWriter<RecordingWriter>,
    streams: &mut StreamWriteStates,
    ready_streams: &mut ReadyStreamQueue,
    connection_send_window: &mut i64,
    peer_max_frame_size: FramePayloadLen,
    header_state: &mut HeaderEncodeState,
    response_closes: &mut ResponseCloseBatch,
) -> Result<(), H2CornError> {
    let buffered = std::mem::replace(writer, BufWriter::new(RecordingWriter::default()));
    let mut state = WriterState::new_test(RecordingWriter::default());
    state.writer = buffered;
    state.streams = std::mem::replace(streams, StreamWriteStates::new());
    let mut ids: Vec<_> = state.streams.keys().copied().collect();
    ids.sort_unstable();
    for id in ids {
        state.ingress.register(id);
    }
    state.ready_streams = std::mem::replace(ready_streams, ReadyStreamQueue::new());
    state.connection_send_window = *connection_send_window;
    state.peer_max_frame_size = peer_max_frame_size;
    state.header_state = std::mem::replace(header_state, HeaderEncodeState::new());
    state.flush().await?;
    *writer = state.writer;
    *streams = state.streams;
    *ready_streams = state.ready_streams;
    *connection_send_window = state.connection_send_window;
    *header_state = state.header_state;
    response_closes.extend(state.response_closes);
    Ok(())
}

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
    data_frames(bytes)
        .map(|(_, _, stream_id)| stream_id)
        .collect()
}

fn parse_data_frames(bytes: &[u8]) -> Vec<(usize, u8, u32)> {
    data_frames(bytes)
        .map(|(payload, flags, stream_id)| (payload.len(), flags, stream_id))
        .collect()
}

fn data_frames(mut bytes: &[u8]) -> impl Iterator<Item = (&[u8], u8, u32)> {
    std::iter::from_fn(move || {
        while !bytes.is_empty() {
            let (&[a, b, c, kind, flags, s0, s1, s2, s3], rest) = bytes
                .split_first_chunk::<9>()
                .expect("complete HTTP/2 frame header");
            let len = u32::from_be_bytes([0, a, b, c]) as usize;
            let (payload, rest) = rest.split_at_checked(len).expect("complete HTTP/2 payload");
            bytes = rest;
            if kind == 0 {
                let stream_id = u32::from_be_bytes([s0, s1, s2, s3]) & h2_frame::STREAM_ID_MASK;
                return Some((payload, flags, stream_id));
            }
        }
        None
    })
}

fn binary_websocket_frame(payload: &'static [u8]) -> WebSocketData {
    match EncodedWebSocketFrame::segmented(
        0x2,
        PayloadBytes::from(Bytes::from_static(payload)),
        false,
    ) {
        EncodedWebSocketFrame::Segmented { header, payload } => WebSocketData::new(header, payload),
        EncodedWebSocketFrame::Contiguous(_) => {
            unreachable!("segmented constructor yields Segmented")
        },
    }
}

#[tokio::test]
async fn websocket_data_is_one_h2_data_frame_without_joining_buffers() {
    let stream_id = StreamId::new(1).unwrap();
    let mut streams = StreamWriteStates::new();
    let mut ready_streams = ReadyStreamQueue::new();
    let mut response_closes = ResponseCloseBatch::new();
    let mut connection_send_window = i64::from(INITIAL_CONNECTION_WINDOW_SIZE);
    let mut header_state = HeaderEncodeState::new();

    let payload = b"body";
    let stream = writer_stream(
        &mut streams,
        stream_id,
        i64::from(INITIAL_STREAM_WINDOW_SIZE),
    );
    stream.open_response(false).unwrap();
    stream
        .queue_websocket_data(Box::new(binary_websocket_frame(payload)), None)
        .unwrap();
    ready_streams.schedule(stream, stream_id, false);

    let mut writer = BufWriter::new(RecordingWriter::default());
    flush_pending_data(
        &mut writer,
        &mut streams,
        &mut ready_streams,
        &mut connection_send_window,
        FramePayloadLen::constant(16 * 1024),
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
    // WebSocket DATA never carries END_STREAM.
    assert_eq!(
        parse_data_frames(&writer.get_ref().bytes)[0].1 & FrameFlags::END_STREAM.bits(),
        0
    );
    assert!(!streams.is_empty());
}

#[tokio::test]
async fn websocket_data_resumes_across_header_window_boundary_without_end_stream() {
    let stream_id = StreamId::new(1).unwrap();
    let mut streams = StreamWriteStates::new();
    let mut ready_streams = ReadyStreamQueue::new();
    let mut response_closes = ResponseCloseBatch::new();
    let mut connection_send_window = i64::from(INITIAL_CONNECTION_WINDOW_SIZE);
    let mut header_state = HeaderEncodeState::new();

    let stream = writer_stream(&mut streams, stream_id, 1);
    stream.open_response(false).unwrap();
    stream
        .queue_websocket_data(Box::new(binary_websocket_frame(b"body")), None)
        .unwrap();
    ready_streams.schedule(stream, stream_id, false);

    let mut writer = BufWriter::new(RecordingWriter::default());
    flush_pending_data(
        &mut writer,
        &mut streams,
        &mut ready_streams,
        &mut connection_send_window,
        FramePayloadLen::constant(16 * 1024),
        &mut header_state,
        &mut response_closes,
    )
    .await
    .unwrap();
    writer.flush().await.unwrap();
    assert_eq!(parse_data_frames(&writer.get_ref().bytes), vec![(1, 0, 1)]);

    let stream = streams.get_mut(stream_id).unwrap();
    stream.send_window += 8;
    ready_streams.schedule(stream, stream_id, false);
    flush_pending_data(
        &mut writer,
        &mut streams,
        &mut ready_streams,
        &mut connection_send_window,
        FramePayloadLen::constant(16 * 1024),
        &mut header_state,
        &mut response_closes,
    )
    .await
    .unwrap();
    writer.flush().await.unwrap();

    // Only the valid WebSocket shape: no END_STREAM on either DATA frame.
    assert_eq!(parse_data_frames(&writer.get_ref().bytes), vec![
        (1, 0, 1),
        (5, 0, 1)
    ]);
    assert_eq!(parse_data_payload(&writer.get_ref().bytes), b"\x82\x04body");
    // Stream remains open after WebSocket DATA (close is a separate path).
    assert!(!streams.is_empty());
}

#[tokio::test]
async fn pathsend_final_buffered_chunk_carries_end_stream() {
    let stream_id = StreamId::new(1).unwrap();
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(b"abc").unwrap();

    let mut streams = StreamWriteStates::new();
    let mut ready_streams = ReadyStreamQueue::new();
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
        .queue_file(FileStreamer::new(
            file.reopen().unwrap(),
            0,
            b"abc".len(),
            true,
        ))
        .unwrap();
    ready_streams.schedule(stream, stream_id, false);

    let recording = RecordingWriter::default();
    let mut writer = BufWriter::new(recording);

    flush_pending_data(
        &mut writer,
        &mut streams,
        &mut ready_streams,
        &mut connection_send_window,
        FramePayloadLen::constant(INITIAL_STREAM_WINDOW_SIZE as usize),
        &mut header_state,
        &mut response_closes,
    )
    .await
    .unwrap();
    writer.flush().await.unwrap();

    let frames = parse_data_frames(&writer.get_ref().bytes);
    let (last, preceding) = frames
        .split_last()
        .expect("file data reaches the transport");
    assert!(
        preceding
            .iter()
            .all(|&(_, flags, id)| flags == 0 && id == stream_id.get())
    );
    assert_eq!(
        (last.1, last.2),
        (FrameFlags::END_STREAM.bits(), stream_id.get())
    );
    assert_eq!(parse_data_payload(&writer.get_ref().bytes), b"abc");
    assert!(ready_streams.is_empty());
    assert!(streams.is_empty());
    assert_eq!(response_closes.as_slice(), &[(
        ResponseClose::Clean,
        stream_id
    )]);
}

/// Reconstruct the concatenated DATA payload bytes for one stream.
fn parse_data_payload(bytes: &[u8]) -> Vec<u8> {
    data_frames(bytes)
        .flat_map(|(payload, ..)| payload.iter().copied())
        .collect()
}

/// A multi-chunk streamed body is emitted as one cross-chunk frame batch:
/// frames stay within chunks, bytes arrive identical, and `END_STREAM`
/// lands exactly on the final frame.
#[tokio::test]
async fn multi_chunk_body_flushes_in_one_cross_chunk_batch() {
    let stream_id = StreamId::new(1).unwrap();
    let frame_size = 16 * 1024;

    let mut streams = StreamWriteStates::new();
    let mut ready_streams = ReadyStreamQueue::new();
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
        .queue_data(Bytes::from(chunk_a.clone()).into(), None, false)
        .unwrap();
    stream
        .queue_data(Bytes::from(chunk_b.clone()).into(), None, false)
        .unwrap();
    stream
        .queue_data(Bytes::from(chunk_c.clone()).into(), None, true)
        .unwrap();
    ready_streams.schedule(stream, stream_id, false);

    let recording = RecordingWriter::default();
    let mut writer = BufWriter::new(recording);

    flush_pending_data(
        &mut writer,
        &mut streams,
        &mut ready_streams,
        &mut connection_send_window,
        FramePayloadLen::constant(frame_size),
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
    assert_eq!(response_closes.as_slice(), &[(
        ResponseClose::Clean,
        stream_id
    )]);
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

    let mut streams = StreamWriteStates::new();
    let mut ready_streams = ReadyStreamQueue::new();
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
        .queue_data(Bytes::from_static(b"").into(), None, false)
        .unwrap();
    stream
        .queue_data(Bytes::from_static(b"body").into(), None, false)
        .unwrap();
    stream
        .queue_data(Bytes::from_static(b"").into(), None, true)
        .unwrap();
    ready_streams.schedule(stream, stream_id, false);

    let recording = RecordingWriter::default();
    let mut writer = BufWriter::new(recording);

    flush_pending_data(
        &mut writer,
        &mut streams,
        &mut ready_streams,
        &mut connection_send_window,
        FramePayloadLen::constant(16 * 1024),
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
    assert_eq!(response_closes.as_slice(), &[(
        ResponseClose::Clean,
        stream_id
    )]);
}

/// A stream-window-limited batch consumes the chunk partially and the
/// remainder flushes after a window grant.
#[tokio::test]
async fn stream_window_limits_batch_and_resumes_after_grant() {
    let stream_id = StreamId::new(1).unwrap();
    let window = 8 * 1024_i64;
    let body = vec![b'z'; 12 * 1024];

    let mut streams = StreamWriteStates::new();
    let mut ready_streams = ReadyStreamQueue::new();
    let mut response_closes = ResponseCloseBatch::new();
    let mut connection_send_window = i64::from(INITIAL_CONNECTION_WINDOW_SIZE);
    let mut header_state = HeaderEncodeState::new();

    let stream = writer_stream(&mut streams, stream_id, window);
    stream.open_response(false).unwrap();
    stream
        .queue_data(Bytes::from(body.clone()).into(), None, true)
        .unwrap();
    ready_streams.schedule(stream, stream_id, false);

    let recording = RecordingWriter::default();
    let mut writer = BufWriter::new(recording);

    flush_pending_data(
        &mut writer,
        &mut streams,
        &mut ready_streams,
        &mut connection_send_window,
        FramePayloadLen::constant(16 * 1024),
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
    let stream = streams.get_mut(stream_id).unwrap();
    stream.send_window += i32::try_from(window).expect("test window fits i32");
    ready_streams.schedule(stream, stream_id, false);

    flush_pending_data(
        &mut writer,
        &mut streams,
        &mut ready_streams,
        &mut connection_send_window,
        FramePayloadLen::constant(16 * 1024),
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
    assert_eq!(response_closes.as_slice(), &[(
        ResponseClose::Clean,
        stream_id
    )]);
}

#[tokio::test]
async fn a_chunk_that_exactly_drains_the_connection_window_stays_wakeable() {
    // The collector stops at a file segment, so a leading chunk that
    // consumes the last of the shared window ends the batch by iterator
    // exhaustion rather than by hitting a blocked chunk -- and so does not
    // report `connection_blocked`. The stream is then left with a file
    // still queued and no credit. Nothing but a connection WINDOW_UPDATE
    // can move it, so that update has to be what wakes it.
    let stream_id = StreamId::new(1).unwrap();

    let mut streams = StreamWriteStates::new();
    let mut ready_streams = ReadyStreamQueue::new();
    let mut response_closes = ResponseCloseBatch::new();
    let mut connection_send_window = 4_i64;
    let mut header_state = HeaderEncodeState::new();

    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(b"file-body").unwrap();
    let stream = writer_stream(&mut streams, stream_id, i64::from(u16::MAX));
    stream.open_response(false).unwrap();
    stream
        .queue_data(Bytes::from_static(b"body").into(), None, false)
        .unwrap();
    stream
        .queue_file(FileStreamer::new(
            file.reopen().expect("temp file opens"),
            0,
            b"file-body".len(),
            true,
        ))
        .unwrap();
    ready_streams.schedule(stream, stream_id, false);

    let recording = RecordingWriter::default();
    let mut writer = BufWriter::new(recording);
    flush_pending_data(
        &mut writer,
        &mut streams,
        &mut ready_streams,
        &mut connection_send_window,
        FramePayloadLen::constant(FAIR_WRITE_QUANTUM),
        &mut header_state,
        &mut response_closes,
    )
    .await
    .unwrap();

    // The four chunk bytes consumed the whole shared window, and the file
    // is still pending.
    assert_eq!(connection_send_window, 0);
    assert!(
        streams
            .get(stream_id)
            .expect("test stream exists")
            .has_pending_output()
    );
    // And it is NOT left in the ready queue: requeueing a stream with no
    // shared credit makes the writer spin against zero, retrying a flush
    // that cannot make progress until the peer grants more.
    assert_eq!(ready_streams.iter().collect::<Vec<_>>(), []);
}

#[tokio::test]
async fn flush_pending_data_requeues_after_fair_write_quantum() {
    let stream_a = StreamId::new(1).unwrap();
    let stream_b = StreamId::new(3).unwrap();

    let mut streams = StreamWriteStates::new();
    let mut ready_streams = ReadyStreamQueue::new();
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
            None,
            true,
        )
        .unwrap();
    ready_streams.schedule(stream, stream_a, false);

    let stream = writer_stream(
        &mut streams,
        stream_b,
        i64::from(INITIAL_STREAM_WINDOW_SIZE),
    );
    stream.open_response(false).unwrap();
    stream
        .queue_data(Bytes::from_static(b"tiny").into(), None, true)
        .unwrap();
    ready_streams.schedule(stream, stream_b, false);

    let recording = RecordingWriter::default();
    let mut writer = BufWriter::new(recording);

    flush_pending_data(
        &mut writer,
        &mut streams,
        &mut ready_streams,
        &mut connection_send_window,
        FramePayloadLen::constant(FAIR_WRITE_QUANTUM),
        &mut header_state,
        &mut response_closes,
    )
    .await
    .unwrap();
    writer.flush().await.unwrap();

    let stream_ids = parse_data_stream_ids(&writer.get_ref().bytes);
    assert_eq!(stream_ids, vec![
        stream_a.get(),
        stream_b.get(),
        stream_a.get()
    ]);
    assert!(ready_streams.is_empty());
    assert!(streams.is_empty());
    assert_eq!(response_closes.as_slice(), &[
        (ResponseClose::Clean, stream_b),
        (ResponseClose::Clean, stream_a),
    ]);
}
