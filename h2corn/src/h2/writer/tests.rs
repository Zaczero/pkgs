use std::fs::File;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use tokio::io::{AsyncWrite, BufWriter};

use super::{PeerSettings, StreamWriteState, WriterCommand, WriterCommandBatch, WriterState};
use crate::bridge::PayloadBytes;
use crate::h2::{ResponseClose, new_stream_map};
use crate::h2_frame::{self, ErrorCode, FramePayloadLen, StreamId, WindowIncrement};
use crate::http::pathsend::FileStreamer;
use crate::http::response::ResponseByteBudget;
use crate::http::types::{ResponseHeaders, status_code};
use crate::runtime::{ApplicationFailure as _, test_fixtures};
use crate::sendfile::WriteTarget;

pub(crate) struct Gate {
    pub(crate) bytes: Vec<u8>,
    pub(crate) allowance: usize,
    pub(crate) flushable: bool,
    waker: Option<std::task::Waker>,
}

pub(crate) struct GatedWriter(pub(crate) Arc<parking_lot::Mutex<Gate>>);

impl GatedWriter {
    pub(crate) fn new(allowance: usize, flushable: bool) -> Self {
        Self(Arc::new(parking_lot::Mutex::new(Gate {
            bytes: Vec::new(),
            allowance,
            flushable,
            waker: None,
        })))
    }
}

impl AsyncWrite for GatedWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut gate = self.0.lock();
        if gate.allowance == 0 {
            gate.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let written = bytes.len().min(gate.allowance);
        gate.bytes.extend_from_slice(&bytes[..written]);
        gate.allowance = gate.allowance.wrapping_sub(written);
        drop(gate);
        Poll::Ready(Ok(written))
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut gate = self.0.lock();
        if gate.flushable {
            Poll::Ready(Ok(()))
        } else {
            gate.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

impl WriteTarget for GatedWriter {
    const SUPPORTS_SENDFILE: bool = false;
}

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::test]
async fn bounded_ready_scan_wakes_unexamined_writable_tail() {
    let target = GatedWriter::new(usize::MAX, true);
    let gate = Arc::clone(&target.0);
    let mut writer = WriterState::new_test(target);
    let ids: Vec<_> = (0_u32..65)
        .map(|index| StreamId::new(index.wrapping_mul(2).wrapping_add(1)).unwrap())
        .collect();
    for &id in &ids {
        writer
            .send_headers(id, status_code::OK, ResponseHeaders::new(), false)
            .unwrap();
    }
    writer.flush().await.unwrap();
    gate.lock().bytes.clear();
    writer.connection_send_window = 0;
    for &id in &ids {
        writer.streams.get_mut(&id).unwrap().send_window = 0;
        writer
            .process_command(WriterCommand::SendData {
                stream_id: id,
                data: bytes::Bytes::from_static(b"probe").into(),
                credit: None,
                end_stream: true,
            })
            .unwrap();
    }
    let tail = *ids.last().unwrap();
    writer
        .grant_stream_window(tail, WindowIncrement::new(5).unwrap())
        .unwrap();
    writer
        .grant_connection_window(WindowIncrement::new(5).unwrap())
        .unwrap();
    let wakes = Arc::new(WakeCount::default());
    let waker = Waker::from(Arc::clone(&wakes));
    let mut cx = Context::from_waker(&waker);
    assert!(writer.poll_egress(&mut cx).is_pending());
    assert_eq!(
        wakes.0.load(Ordering::Relaxed),
        1,
        "unexamined streams require a continuation"
    );
    assert!(gate.lock().bytes.is_empty());
    assert!(writer.poll_egress(&mut cx).is_ready());
    assert!(
        frames(&gate.lock().bytes)
            .iter()
            .any(|frame| frame.0 == 0 && frame.2 == tail.get() && frame.3.as_ref() == b"probe")
    );
    assert_eq!(writer.take_response_closes().as_slice(), &[(
        ResponseClose::Clean,
        tail
    )]);
    drop(writer);
}

#[test]
fn bounded_ready_scan_sleeps_after_all_file_reads_are_pending() {
    use std::io::Write as _;

    use tokio::runtime::Builder;
    use tokio::sync::oneshot;
    use tokio::task::spawn_blocking;

    let runtime = Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (started, running) = oneshot::channel();
        let (release, waiting) = std::sync::mpsc::channel();
        let worker = spawn_blocking(move || {
            started.send(()).unwrap();
            let _received = waiting.recv();
        });
        running.await.unwrap();
        let target = GatedWriter::new(usize::MAX, true);
        let gate = Arc::clone(&target.0);
        let mut writer = WriterState::new_test(target);
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"owned").unwrap();
        let ids: Vec<_> = (0_u32..65)
            .map(|index| StreamId::new(index.wrapping_mul(2).wrapping_add(1)).unwrap())
            .collect();
        for &id in &ids {
            writer
                .send_headers(id, status_code::OK, ResponseHeaders::new(), false)
                .unwrap();
        }
        writer.flush().await.unwrap();
        gate.lock().bytes.clear();
        for &id in &ids {
            writer
                .streams
                .get_mut(&id)
                .unwrap()
                .queue_file(FileStreamer::new(file.reopen().unwrap(), 0, 5, true))
                .unwrap();
            writer.schedule(id, false);
        }
        let wakes = Arc::new(WakeCount::default());
        let waker = Waker::from(Arc::clone(&wakes));
        let mut cx = Context::from_waker(&waker);
        assert!(writer.poll_egress(&mut cx).is_pending());
        assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
        assert!(writer.poll_egress(&mut cx).is_pending());
        assert_eq!(
            wakes.0.load(Ordering::Relaxed),
            1,
            "a fully examined pass waits for file completion"
        );
        release.send(()).unwrap();
        worker.await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), writer.flush())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(writer.take_response_closes().len(), 65);
        let output = frames(&gate.lock().bytes);
        assert_eq!(
            output
                .iter()
                .filter(|frame| frame.0 == 0 && frame.3.as_ref() == b"owned")
                .count(),
            65
        );
        drop(writer);
    });
}

#[rstest::rstest]
#[case::zero(false)]
#[case::negative(true)]
#[tokio::test(start_paused = true)]
async fn settings_credit_resumes_parked_body_and_cancels_stall_deadline(#[case] negative: bool) {
    let target = GatedWriter::new(usize::MAX, true);
    let gate = Arc::clone(&target.0);
    let mut writer = WriterState::new_test(target);
    let id = StreamId::new(1).unwrap();
    writer
        .update_peer_settings(PeerSettings {
            initial_window_size: Some(if negative { 3 } else { 0 }),
            ..PeerSettings::default()
        })
        .unwrap();
    writer.register_stream(id, Some(Duration::from_secs(10)));
    writer
        .send_headers(id, status_code::OK, ResponseHeaders::new(), false)
        .unwrap();
    if negative {
        writer
            .process_command(WriterCommand::SendData {
                stream_id: id,
                data: bytes::Bytes::from_static(b"old").into(),
                credit: None,
                end_stream: false,
            })
            .unwrap();
    }
    writer.flush().await.unwrap();
    writer
        .update_peer_settings(PeerSettings {
            initial_window_size: Some(0),
            ..PeerSettings::default()
        })
        .unwrap();
    assert_eq!(
        writer
            .streams
            .get(&id)
            .expect("registered response stream")
            .send_window,
        if negative { -3 } else { 0 }
    );
    gate.lock().bytes.clear();
    writer
        .process_command(WriterCommand::SendData {
            stream_id: id,
            data: bytes::Bytes::from_static(b"probe").into(),
            credit: None,
            end_stream: true,
        })
        .unwrap();
    assert!(
        writer
            .poll_egress(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(writer.next_response_stall_deadline().is_some());
    writer
        .update_peer_settings(PeerSettings {
            initial_window_size: Some(16),
            ..PeerSettings::default()
        })
        .unwrap();
    assert!(
        writer.next_response_stall_deadline().is_none(),
        "usable SETTINGS credit cancels the old flow-control deadline"
    );
    tokio::time::advance(Duration::from_secs(20)).await;
    assert!(
        writer
            .pop_expired_response_stall_deadline(tokio::time::Instant::now())
            .is_none()
    );
    writer.flush().await.unwrap();
    assert!(
        frames(&gate.lock().bytes)
            .iter()
            .any(|frame| frame.0 == 0 && frame.2 == 1 && frame.3.as_ref() == b"probe")
    );
    assert_eq!(writer.take_response_closes().as_slice(), &[(
        ResponseClose::Clean,
        id
    )]);
    drop(writer);
}

pub(crate) fn release(gate: &Arc<parking_lot::Mutex<Gate>>) {
    let mut state = gate.lock();
    state.allowance = usize::MAX;
    state.flushable = true;
    let waker = state.waker.take();
    drop(state);
    if let Some(waker) = waker {
        waker.wake();
    }
}

fn frames(bytes: &[u8]) -> Vec<(u8, u8, u32, bytes::Bytes)> {
    let mut rest = bytes;
    let mut frames = Vec::new();
    while !rest.is_empty() {
        let (&[a, b, c, kind, flags, s0, s1, s2, s3], following) = rest
            .split_first_chunk::<9>()
            .expect("complete HTTP/2 frame header");
        let length = u32::from_be_bytes([0, a, b, c]) as usize;
        let (payload, following) = following.split_at(length);
        frames.push((
            kind,
            flags,
            u32::from_be_bytes([s0, s1, s2, s3]),
            bytes::Bytes::copy_from_slice(payload),
        ));
        rest = following;
    }
    frames
}

#[tokio::test]
async fn blocked_inner_flush_keeps_terminal_completion_pending() {
    let target = GatedWriter::new(usize::MAX, false);
    let gate = Arc::clone(&target.0);
    let mut writer = WriterState::new_test(target);
    let id = StreamId::new(1).unwrap();
    writer
        .send_headers(id, status_code::OK, ResponseHeaders::new(), true)
        .unwrap();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(writer.poll_egress(&mut cx).is_pending());
    assert!(writer.writer.buffer().is_empty());
    assert!(writer.needs_flush());
    assert!(writer.take_response_closes().is_empty());
    let prefix = gate.lock().bytes.clone();
    release(&gate);
    writer.flush().await.unwrap();
    assert_eq!(
        gate.lock().bytes,
        prefix,
        "flushing must not replay accepted plaintext"
    );
    assert_eq!(writer.take_response_closes().as_slice(), &[(
        ResponseClose::Clean,
        id
    )]);
}

#[tokio::test]
async fn reset_during_partial_data_finishes_only_the_active_frame() {
    let target = GatedWriter::new(usize::MAX, true);
    let gate = Arc::clone(&target.0);
    let mut writer = WriterState::new_test(target);
    let id = StreamId::new(1).unwrap();
    writer
        .send_headers(id, status_code::OK, ResponseHeaders::new(), false)
        .unwrap();
    writer.flush().await.unwrap();
    gate.lock().bytes.clear();
    gate.lock().allowance = 12;
    let budget = ResponseByteBudget::new(32 * 1024);
    let credit = budget.acquire(32 * 1024).await.unwrap();
    writer
        .process_command(WriterCommand::SendData {
            stream_id: id,
            data: bytes::Bytes::from(vec![0x62; 32 * 1024]).into(),
            credit,
            end_stream: true,
        })
        .unwrap();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(writer.poll_egress(&mut cx).is_pending());
    assert_eq!(gate.lock().bytes.len(), 12);
    writer.reset_stream(id, ErrorCode::CANCEL).unwrap();
    assert_eq!(writer.take_response_closes().as_slice(), &[(
        ResponseClose::Abort,
        id
    )]);
    release(&gate);
    writer.flush().await.unwrap();
    let output = frames(&gate.lock().bytes);
    assert_eq!(output.len(), 2);
    assert_eq!((output[0].0, output[0].1, output[0].2), (0, 0, 1));
    assert_eq!(output[0].3.as_ref(), &[0x62; 16 * 1024]);
    assert_eq!((output[1].0, output[1].2), (3, 1));
    assert_eq!(output[1].3.as_ref(), &ErrorCode::CANCEL.to_be_bytes());
    assert_eq!(
        writer.connection_send_window,
        i64::from(h2_frame::DEFAULT_WINDOW_SIZE) - 16 * 1024
    );
    budget.try_acquire(32 * 1024).unwrap();
}

#[rstest::rstest]
#[case::window(8_000, &[8_000], Some(24_000))]
#[case::frame_size(0xFFFF, &[0x4000, 15_616], None)]
#[tokio::test]
async fn settings_replans_unstarted_data_without_spending_uncommitted_credit(
    #[case] window: u32,
    #[case] expected: &[usize],
    #[case] grant: Option<u32>,
) {
    let target = GatedWriter::new(usize::MAX, true);
    let gate = Arc::clone(&target.0);
    let mut writer = WriterState::new_test(target);
    let id = StreamId::new(1).unwrap();
    writer
        .send_headers(id, status_code::OK, ResponseHeaders::new(), false)
        .unwrap();
    writer.flush().await.unwrap();
    writer.writer = BufWriter::with_capacity(1, GatedWriter(Arc::clone(&gate)));
    writer.peer_max_frame_size = FramePayloadLen::constant(64 * 1024);
    gate.lock().bytes.clear();
    gate.lock().allowance = 0;
    writer
        .process_command(WriterCommand::SendData {
            stream_id: id,
            data: bytes::Bytes::from(vec![0x53; 32_000]).into(),
            credit: None,
            end_stream: true,
        })
        .unwrap();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(writer.poll_egress(&mut cx).is_pending());
    assert!(gate.lock().bytes.is_empty());
    assert_eq!(writer.connection_send_window, 0xFFFF);
    assert!(
        writer
            .grant_connection_window(WindowIncrement::new(0x7FFF_FFFF).unwrap())
            .is_err()
    );
    writer
        .update_peer_settings(PeerSettings {
            initial_window_size: Some(window),
            max_frame_size: Some(FramePayloadLen::constant(16 * 1024)),
            ..PeerSettings::default()
        })
        .unwrap();
    release(&gate);
    writer.flush().await.unwrap();
    let first = frames(&gate.lock().bytes);
    assert_eq!(
        first.iter().map(|frame| frame.3.len()).collect::<Vec<_>>(),
        expected
    );
    if let Some(grant) = grant {
        assert_eq!(first[0].1 & 1, 0);
        writer
            .grant_stream_window(id, WindowIncrement::new(grant).unwrap())
            .unwrap();
        writer.flush().await.unwrap();
    }
    let output = frames(&gate.lock().bytes);
    assert_eq!(
        output.iter().map(|frame| frame.3.len()).sum::<usize>(),
        32_000
    );
    assert_eq!(output.last().unwrap().1 & 1, 1);
    assert_eq!(writer.connection_send_window, 0xFFFF - 32_000);
}

#[tokio::test]
async fn empty_terminal_file_finishes_after_the_last_connection_credit() {
    let target = GatedWriter::new(usize::MAX, true);
    let gate = Arc::clone(&target.0);
    let mut writer = WriterState::new_test(target);
    let id = StreamId::new(1).unwrap();
    writer
        .send_headers(id, status_code::OK, ResponseHeaders::new(), false)
        .unwrap();
    writer.flush().await.unwrap();
    gate.lock().bytes.clear();
    writer.connection_send_window = 4;
    writer.streams.get_mut(&id).unwrap().send_window = 4;
    writer
        .process_command(WriterCommand::SendData {
            stream_id: id,
            data: bytes::Bytes::from_static(b"body").into(),
            credit: None,
            end_stream: false,
        })
        .unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    writer
        .streams
        .get_mut(&id)
        .unwrap()
        .queue_file(FileStreamer::new(file.reopen().unwrap(), 0, 0, true))
        .unwrap();
    writer.flush().await.unwrap();
    let output = frames(&gate.lock().bytes);
    assert_eq!(
        output
            .iter()
            .map(|frame| (frame.0, frame.1, frame.3.len()))
            .collect::<Vec<_>>(),
        [(0, 0, 4), (0, 1, 0)]
    );
    assert_eq!(writer.take_response_closes().as_slice(), &[(
        ResponseClose::Clean,
        id
    )]);
}

#[tokio::test(start_paused = true)]
async fn partial_progress_does_not_renew_the_socket_write_bound() {
    let target = GatedWriter::new(7, true);
    let gate = Arc::clone(&target.0);
    let mut config = test_fixtures::server_config_parts();
    config.http2.timeout_write = Some(std::time::Duration::from_secs(10));
    config.http2.timeout_response_stall = None;
    let mut writer = WriterState::new(target, Arc::new(config));
    writer
        .send_headers(
            StreamId::new(1).unwrap(),
            status_code::OK,
            ResponseHeaders::new(),
            true,
        )
        .unwrap();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(writer.poll_egress(&mut cx).is_pending());
    let deadline = writer.next_write_deadline().unwrap();
    tokio::time::advance(std::time::Duration::from_secs(4)).await;
    gate.lock().allowance = 1;
    assert!(writer.poll_egress(&mut cx).is_pending());
    assert_eq!(writer.next_write_deadline(), Some(deadline));
    assert!(writer.next_response_stall_deadline().is_none());
    drop(writer);
}

#[tokio::test]
async fn response_stall_policy_does_not_enable_a_disabled_socket_timer() {
    let mut config = test_fixtures::server_config_parts();
    config.http2.timeout_write = None;
    config.http2.timeout_response_stall = Some(std::time::Duration::from_secs(1));
    let mut writer = WriterState::new(GatedWriter::new(0, true), Arc::new(config));
    writer
        .send_headers(
            StreamId::new(1).unwrap(),
            status_code::OK,
            ResponseHeaders::new(),
            true,
        )
        .unwrap();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(writer.poll_egress(&mut cx).is_pending());
    assert!(writer.next_write_deadline().is_none());
    drop(writer);
}

#[tokio::test]
async fn application_failure_retires_terminal_output_before_flush() {
    let target = GatedWriter::new(usize::MAX, false);
    let gate = Arc::clone(&target.0);
    let mut writer = WriterState::new_test(target);
    let id = StreamId::new(1).unwrap();
    writer
        .send_headers(id, status_code::OK, ResponseHeaders::new(), true)
        .unwrap();
    let failure = writer.ingress.gate(id).unwrap();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(writer.poll_egress(&mut cx).is_pending());
    failure.application_failed();
    drop(failure);
    assert!(writer.take_response_closes().is_empty());
    release(&gate);
    writer.flush().await.unwrap();
    assert_eq!(writer.take_response_closes().as_slice(), &[(
        ResponseClose::ProducerFailed,
        id
    )]);
    drop(writer);
    assert!(
        frames(&gate.lock().bytes)
            .iter()
            .any(|frame| frame.0 == 3 && frame.2 == 1)
    );
}

#[tokio::test]
async fn application_failure_after_flush_preserves_clean_completion() {
    let target = GatedWriter::new(usize::MAX, true);
    let gate = Arc::clone(&target.0);
    let mut writer = WriterState::new_test(target);
    let id = StreamId::new(1).unwrap();
    writer
        .send_headers(id, status_code::OK, ResponseHeaders::new(), true)
        .unwrap();
    let failure = writer.ingress.gate(id).unwrap();
    writer.flush().await.unwrap();
    assert_eq!(writer.take_response_closes().as_slice(), &[(
        ResponseClose::Clean,
        id
    )]);
    let bytes = gate.lock().bytes.clone();
    failure.application_failed();
    drop(failure);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(writer.poll_egress(&mut cx).is_pending());
    assert!(writer.take_response_closes().is_empty());
    assert_eq!(gate.lock().bytes, bytes);
    drop(writer);
}

#[tokio::test]
async fn reset_preserves_continuations_and_dynamic_header_history() {
    let target = GatedWriter::new(12, true);
    let gate = Arc::clone(&target.0);
    let mut writer = WriterState::new_test(target);
    let first = StreamId::new(1).unwrap();
    let second = StreamId::new(3).unwrap();
    let headers = vec![
        (
            bytes::Bytes::from_static(b"x-large").into(),
            bytes::Bytes::from(vec![b'~'; 40_000]).into(),
        ),
        (
            bytes::Bytes::from_static(b"x-shared").into(),
            bytes::Bytes::from_static(b"retained-value").into(),
        ),
    ];
    writer
        .send_headers(first, status_code::OK, headers, false)
        .unwrap();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(writer.poll_egress(&mut cx).is_pending());
    writer.reset_stream(first, ErrorCode::CANCEL).unwrap();
    writer
        .send_headers(
            second,
            status_code::OK,
            vec![(
                bytes::Bytes::from_static(b"x-shared").into(),
                bytes::Bytes::from_static(b"retained-value").into(),
            )],
            true,
        )
        .unwrap();
    release(&gate);
    writer.flush().await.unwrap();
    let output = frames(&gate.lock().bytes);
    let reset = output.iter().position(|frame| frame.0 == 3).unwrap();
    assert!(reset >= 3);
    assert_eq!(output[0].0, 1);
    assert!(
        output[1..reset]
            .iter()
            .all(|frame| frame.0 == 9 && frame.2 == 1)
    );
    assert_eq!(output[reset - 1].1 & 4, 4);
    let mut decoder = crate::hpack::Decoder::new(4096);
    let mut first_block = bytes::Bytes::from(
        output[..reset]
            .iter()
            .flat_map(|frame| frame.3.iter().copied())
            .collect::<Vec<_>>(),
    );
    decoder.decode_bytes(&mut first_block, |_| {}).unwrap();
    let mut second_block = output[reset + 1].3.clone();
    let mut fields = Vec::new();
    decoder
        .decode_bytes(&mut second_block, |field| {
            fields.push((field.name().to_vec(), field.value().to_vec()));
        })
        .unwrap();
    assert!(fields.contains(&(b"x-shared".to_vec(), b"retained-value".to_vec())));
    assert_eq!(writer.take_response_closes().as_slice(), &[
        (ResponseClose::Abort, first),
        (ResponseClose::Clean, second),
    ]);
}

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

#[tokio::test]
async fn clean_response_waits_for_transport_flush() {
    let mut writer = WriterState::new_test(TestWriter);
    let stream_id = StreamId::new(1).unwrap();
    writer
        .send_headers(stream_id, status_code::OK, ResponseHeaders::new(), true)
        .unwrap();
    assert!(
        writer.take_response_closes().is_empty(),
        "accepting terminal headers into a buffer is not transport completion"
    );
    writer.flush().await.unwrap();
    writer.flush().await.unwrap();
    assert_eq!(writer.take_response_closes().as_slice(), &[(
        ResponseClose::Clean,
        stream_id
    )]);
}

#[tokio::test]
async fn direct_response_write_preserves_inner_flush_obligation() {
    let target = GatedWriter::new(usize::MAX, false);
    let gate = Arc::clone(&target.0);
    let mut writer = WriterState::new_test(target);
    let stream_id = StreamId::new(1).unwrap();
    writer
        .process_command(WriterCommand::SendFinal {
            stream_id,
            status: status_code::OK,
            headers: ResponseHeaders::new(),
            data: bytes::Bytes::from(vec![0x5A; 16 * 1024]).into(),
            credit: None,
        })
        .unwrap();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(writer.poll_egress(&mut cx).is_pending());
    assert!(writer.writer.buffer().is_empty());
    assert!(
        writer.needs_flush(),
        "an empty outer buffer must not erase a TLS writer's flush obligation"
    );
    release(&gate);
    writer.flush().await.unwrap();
    drop(writer);
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
async fn terminal_data_preserves_connection_control_after_partial_write() {
    let stream_id = StreamId::new(1).expect("test stream id is valid");
    let target = GatedWriter::new(usize::MAX, true);
    let gate = Arc::clone(&target.0);
    let mut writer = WriterState::new_test(target);
    writer
        .send_headers(stream_id, status_code::OK, ResponseHeaders::new(), false)
        .unwrap();
    writer.flush().await.unwrap();
    gate.lock().bytes.clear();
    gate.lock().allowance = 1;
    writer
        .process_command(WriterCommand::SendData {
            stream_id,
            data: PayloadBytes::from(bytes::Bytes::from_static(b"complete")),
            credit: None,
            end_stream: true,
        })
        .unwrap();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(writer.poll_egress(&mut cx).is_pending());
    writer.send_settings_ack().unwrap();
    release(&gate);
    writer.flush().await.unwrap();
    assert_eq!(writer.take_response_closes().as_slice(), &[(
        ResponseClose::Clean,
        stream_id
    )]);
    assert!(!writer.has_queued_app_writes());
    assert!(
        frames(&gate.lock().bytes)
            .iter()
            .any(|frame| (frame.0, frame.1, frame.2) == (4, 1, 0))
    );
    drop(writer);
}

#[tokio::test]
async fn stream_flush_orders_close_after_all_data_without_blocking_siblings() {
    let target = GatedWriter::new(usize::MAX, true);
    let gate = Arc::clone(&target.0);
    let mut writer = WriterState::new_test(target);
    let first = StreamId::new(1).unwrap();
    let sibling = StreamId::new(3).unwrap();
    writer
        .send_headers(first, status_code::OK, ResponseHeaders::new(), false)
        .unwrap();
    writer.flush().await.unwrap();
    writer.streams.get_mut(&first).unwrap().send_window = 4;
    gate.lock().bytes.clear();
    let mut commands = WriterCommandBatch::new();
    commands.push_back(WriterCommand::SendData {
        stream_id: first,
        data: bytes::Bytes::from_static(b"abcdefgh").into(),
        credit: None,
        end_stream: false,
    });
    commands.push_back(WriterCommand::FlushStream(first));
    commands.push_back(WriterCommand::ResetStream {
        stream_id: first,
        error_code: ErrorCode::NO_ERROR,
    });
    writer.ingress.enqueue_batch(first, commands).await.unwrap();
    writer.register_stream(sibling, None);
    writer
        .ingress
        .enqueue(sibling, WriterCommand::SendFinal {
            stream_id: sibling,
            status: status_code::OK,
            headers: ResponseHeaders::new(),
            data: bytes::Bytes::from_static(b"tiny").into(),
            credit: None,
        })
        .await
        .unwrap();
    for _ in 0..3 {
        writer.drain_app_writes().unwrap();
        writer.flush().await.unwrap();
    }
    let before = frames(&gate.lock().bytes);
    assert_eq!(
        before
            .iter()
            .filter(|frame| frame.0 == 0 && frame.2 == 1)
            .flat_map(|frame| frame.3.iter().copied())
            .collect::<Vec<_>>(),
        b"abcd"
    );
    assert!(
        before
            .iter()
            .any(|frame| frame.0 == 0 && frame.2 == 3 && frame.3.as_ref() == b"tiny")
    );
    assert!(!before.iter().any(|frame| frame.0 == 3 && frame.2 == 1));
    writer
        .grant_stream_window(first, WindowIncrement::new(4).unwrap())
        .unwrap();
    while writer.has_queued_app_writes() {
        writer.drain_app_writes().unwrap();
        writer.flush().await.unwrap();
    }
    let after = frames(&gate.lock().bytes);
    drop(writer);
    assert_eq!(
        after
            .iter()
            .filter(|frame| frame.0 == 0 && frame.2 == 1)
            .flat_map(|frame| frame.3.iter().copied())
            .collect::<Vec<_>>(),
        b"abcdefgh"
    );
    let reset = after
        .iter()
        .position(|frame| frame.0 == 3 && frame.2 == 1)
        .unwrap();
    assert!(after[reset + 1..].iter().all(|frame| frame.2 != 1));
}

#[tokio::test]
async fn reset_marks_queued_stream_cleanup_as_abort() {
    let stream_id = StreamId::new(1).expect("test stream id is valid");
    let mut writer = WriterState::new_test(TestWriter);
    writer.register_stream(stream_id, None);
    writer
        .ingress
        .enqueue(stream_id, WriterCommand::SendHeaders {
            stream_id,
            status: status_code::OK,
            headers: ResponseHeaders::new(),
            end_stream: false,
        })
        .await
        .expect("queued command enters ingress");

    writer
        .reset_stream(stream_id, ErrorCode::CANCEL)
        .expect("local reset queues without waiting for output");
    assert_eq!(writer.take_response_closes().as_slice(), &[(
        ResponseClose::Abort,
        stream_id
    )]);
    assert!(
        writer
            .ingress
            .enqueue(stream_id, WriterCommand::FlushStream(stream_id))
            .await
            .is_err()
    );
    writer.flush().await.unwrap();
    assert!(
        writer.take_response_closes().is_empty(),
        "writing the reset cannot publish a second abort"
    );
    drop(writer);
}

#[tokio::test]
async fn failed_headers_and_data_writes_mark_streams_as_abort() {
    let stream_id = StreamId::new(1).expect("test stream id is valid");
    let fail_writes = Arc::new(AtomicBool::new(true));
    let mut writer = WriterState::new_test(FailingWriter {
        fail_writes: Arc::clone(&fail_writes),
    });
    writer
        .send_headers(stream_id, status_code::OK, ResponseHeaders::new(), false)
        .unwrap();
    writer
        .flush()
        .await
        .expect_err("header output failure ends the connection");
    assert!(
        writer
            .take_response_closes()
            .iter()
            .all(|(kind, _)| *kind == ResponseClose::Abort)
    );
    drop(writer);

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
        .expect("data enters the stream queue");
    writer.flush().await.unwrap_err();
    assert!(
        writer
            .take_response_closes()
            .iter()
            .all(|(kind, _)| *kind == ResponseClose::Abort)
    );
    drop(writer);
}

#[tokio::test]
async fn response_byte_credit_survives_ingress_drain_until_data_is_written() {
    let stream_id = StreamId::new(1).expect("test stream id is valid");
    let mut writer = WriterState::new_test(TestWriter);
    writer
        .send_headers(stream_id, status_code::OK, ResponseHeaders::new(), false)
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
        .grant_connection_window(WindowIncrement::new(64 * 1024).expect("positive test increment"))
        .expect("connection window grant is valid");
    writer
        .grant_stream_window(
            stream_id,
            WindowIncrement::new(64 * 1024).expect("positive test increment"),
        )
        .expect("stream window grant is valid");
    writer.flush().await.expect("granted DATA frame writes");
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
        .expect("body enters pending stream state");
    let next = tokio::spawn({
        let budget = budget.clone();
        async move { budget.acquire(64).await }
    });
    tokio::task::yield_now().await;
    assert!(!next.is_finished(), "pending stream body keeps its credit");
    writer
        .reset_stream(stream_id, ErrorCode::CANCEL)
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
    writer.ingress.register(stream_id);
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
    writer.ingress.drop_stream(stream_id);
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
        .expect("response headers are queued");
    writer
        .process_command(WriterCommand::SendData {
            stream_id,
            data: PayloadBytes::from(bytes::Bytes::from_static(b"body")),
            credit: Some(credit),
            end_stream: false,
        })
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
