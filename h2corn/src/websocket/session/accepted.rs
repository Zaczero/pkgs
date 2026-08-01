use std::future::pending;
use std::ops::ControlFlow;
use std::time::Duration;

use bytes::Bytes;
use pyo3::pybacked::PyBackedStr;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::watch;
use tokio::time::{Instant as TokioInstant, sleep_until};

use super::super::app::RunningWebSocketApp;
use super::{
    AcceptedSessionConfig, AcceptedWebSocketState, AcceptedWebSocketTransport,
    EncodedWebSocketFrame, FrameFlushMode, TransportRead, shutdown_close_code,
};
use crate::bridge::{
    PayloadBytes, WEBSOCKET_OUTBOUND_QUEUE_CAPACITY, WebSocketDisconnect, WebSocketInboundMessage,
    WebSocketInboundSender, WebSocketInboundTrySendError, WebSocketOutboundEvent,
};
use crate::config::WebSocketKeepAlive;
use crate::error::{ErrorExt as _, FailureDomain, H2CornError, WebSocketError, WebSocketFrameKind};
use crate::http::types::BytesStr;
use crate::runtime::{ShutdownKind, ShutdownState};
use crate::websocket::codec::{DecodedFrame, DecodedPeerClose, WebSocketCodec, encode_frame_into};
use crate::websocket::deflate::{
    PerMessageDeflateDisabled, PerMessageDeflateEnabled, PerMessageDeflateMode,
};
use crate::websocket::{WebSocketCloseCode, close_code};

/// Frames coalesced into one vectored write before the session looks at
/// anything else. Matches the outbound channel capacity, so one full snapshot
/// still leaves in a single write.
const MAX_OUTBOUND_BATCH_FRAMES: usize = WEBSOCKET_OUTBOUND_QUEUE_CAPACITY;
/// Payload bytes coalesced into one batch, matching the HTTP/2 fair-write
/// quantum so a session cannot hold more outbound data than a stream may.
const MAX_OUTBOUND_BATCH_BYTES: usize = 64 * 1024;

/// Why a batch stopped: the queue ran dry, or the fairness quantum was reached
/// with work still waiting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OutboundBatchEnd {
    Drained,
    Capped,
}

pub(super) struct AcceptedSessionOutcome {
    pub(super) state: AcceptedWebSocketState,
    pub(super) rx_bytes: u64,
    pub(super) tx_bytes: u64,
    pub(super) result: Result<(), H2CornError>,
}

enum AcceptedOutboundEvent {
    SendText(PyBackedStr),
    SendBytes(PayloadBytes),
    Close {
        code: WebSocketCloseCode,
        reason: Option<PyBackedStr>,
    },
}

impl AcceptedOutboundEvent {
    fn payload_len(&self) -> usize {
        match self {
            Self::SendText(text) => text.len(),
            Self::SendBytes(bytes) => bytes.len(),
            // A close frame ends the batch anyway.
            Self::Close { .. } => 0,
        }
    }
}

/// Where the keepalive is in its cycle.
///
/// One ping is outstanding at a time, so exactly one deadline exists at a
/// time. Holding "when to ping next" and "when the pong is overdue" as two
/// independent instants let the next ping re-arm the deadline the previous one
/// was being judged by: with the interval shorter than the timeout — the
/// obvious reading of those two knobs — a peer that never answered was never
/// timed out, because the deadline moved forward with every ping.
enum PingPhase {
    /// Nothing outstanding; the next ping goes out at this instant.
    Idle { next_ping: TokioInstant },
    /// A ping is outstanding and has to be answered by this instant.
    AwaitingPong { deadline: TokioInstant },
}

/// Keepalive schedule for one accepted session. Constructed only when
/// `WebSocketKeepAlive` is configured, so interval-off cannot leave a
/// free-floating timeout behind.
struct PingState {
    phase: PingPhase,
    interval: Duration,
    timeout: Option<Duration>,
}

impl PingState {
    fn new(keep_alive: WebSocketKeepAlive) -> Self {
        Self {
            phase: PingPhase::Idle {
                next_ping: TokioInstant::now() + keep_alive.interval,
            },
            interval: keep_alive.interval,
            timeout: keep_alive.timeout,
        }
    }

    const fn ping_at(&self) -> Option<TokioInstant> {
        match self.phase {
            PingPhase::Idle { next_ping } => Some(next_ping),
            PingPhase::AwaitingPong { .. } => None,
        }
    }

    const fn pong_at(&self) -> Option<TokioInstant> {
        match self.phase {
            PingPhase::AwaitingPong { deadline } => Some(deadline),
            PingPhase::Idle { .. } => None,
        }
    }

    /// Without a timeout there is no way to tell an outstanding ping from an
    /// answered one, so the schedule simply continues.
    fn on_ping_sent(&mut self) {
        self.phase = match self.timeout {
            Some(timeout) => PingPhase::AwaitingPong {
                deadline: TokioInstant::now() + timeout,
            },
            None => PingPhase::Idle {
                next_ping: TokioInstant::now() + self.interval,
            },
        };
    }

    /// Any pong answers the outstanding ping — h2corn's pings carry no payload
    /// to match against, and a peer that is talking is a peer that is alive.
    /// An unsolicited pong does not postpone the next keepalive.
    fn on_pong(&mut self) {
        if matches!(self.phase, PingPhase::AwaitingPong { .. }) {
            self.phase = PingPhase::Idle {
                next_ping: TokioInstant::now() + self.interval,
            };
        }
    }
}

/// The accepted-session driver: owns the codec, compression state, ping
/// schedule, close state, and byte counters for one websocket session, so
/// the event handlers are methods instead of free functions threading
/// borrow-bundle structs.
struct AcceptedSession<'a, T, M: PerMessageDeflateMode> {
    transport: &'a mut T,
    running_app: &'a mut RunningWebSocketApp,
    recv_tx: WebSocketInboundSender,
    shutdown: watch::Receiver<ShutdownState>,
    codec: WebSocketCodec,
    inflater: M::Inflater,
    deflater: M::Deflater,
    state: AcceptedWebSocketState,
    rx_bytes: u64,
    tx_bytes: u64,
    ping: Option<PingState>,
    /// One decoded message waits for byte capacity. While it does, the
    /// session retains transport input and does not decode past it: retaining
    /// more would turn the byte budget into an unbounded data queue, while
    /// discarding would violate ordered delivery. Local outbound, shutdown,
    /// and keepalive events remain independently selectable.
    pending_inbound: Option<WebSocketInboundMessage>,
}

impl<'a, T, M> AcceptedSession<'a, T, M>
where
    T: AcceptedWebSocketTransport,
    M: PerMessageDeflateMode,
{
    fn new(
        transport: &'a mut T,
        running_app: &'a mut RunningWebSocketApp,
        config: AcceptedSessionConfig,
    ) -> Self {
        let recv_tx = running_app.recv_tx.clone();
        let codec = transport.websocket_codec(config.max_message_size);
        Self {
            transport,
            running_app,
            recv_tx,
            shutdown: config.shutdown,
            codec,
            inflater: M::new_inflater(),
            deflater: M::new_deflater(),
            state: AcceptedWebSocketState::default(),
            rx_bytes: 0,
            tx_bytes: 0,
            ping: config.keep_alive.map(PingState::new),
            pending_inbound: None,
        }
    }

    async fn run(mut self) -> Result<AcceptedSessionOutcome, H2CornError> {
        let result = loop {
            if let ControlFlow::Break(result) = self.flush_ready_outbound().await? {
                break result;
            }
            if let ControlFlow::Break(result) = self.drain_decoded_frames().await? {
                break result;
            }
            self.release_consumed_input();
            if let ControlFlow::Break(result) = self.wait_session_event().await? {
                break result;
            }
        };

        Ok(AcceptedSessionOutcome {
            state: self.state,
            rx_bytes: self.rx_bytes,
            tx_bytes: self.tx_bytes,
            result,
        })
    }

    async fn wait_session_event(
        &mut self,
    ) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError> {
        let next_ping = self.ping.as_ref().and_then(PingState::ping_at);
        let pong_deadline = self.ping.as_ref().and_then(PingState::pong_at);
        let ping_sleep = sleep_until_or_pending(next_ping);
        let pong_timeout = sleep_until_or_pending(pong_deadline);
        let pending_inbound_len = self
            .pending_inbound
            .as_ref()
            .map(WebSocketInboundMessage::len);
        // Only the backpressure case needs the sender, so only it pays the
        // refcount traffic; cloning unconditionally cost an atomic pair per
        // message for a branch that is normally not taken.
        let pending_waiter = pending_inbound_len.map(|len| (self.recv_tx.clone(), len));
        let pending_capacity = async move {
            match pending_waiter {
                Some((tx, len)) => tx.acquire_bytes(len).await,
                None => pending::<Result<_, ()>>().await,
            }
        };

        tokio::select! {
            biased;
            () = self.running_app.task.join_store(), if !self.running_app.task.is_joined() => {
                self.finish_after_app().await
            }
            outbound_event = self.running_app.send_rx.recv() => {
                if let Some(outbound_event) = outbound_event {
                    let first = parse_accepted_outbound_event(outbound_event)?;
                    return Ok(self
                        .process_outbound_batch(Some(first))
                        .await?
                        .map_continue(|_| ()));
                }
                // All senders are gone, which means the application is on its
                // way out — the same end as its task completing, reached by
                // whichever the select observed first.
                self.finish_after_app().await
            },
            () = ping_sleep, if next_ping.is_some() => {
                self.send_ping_and_arm_timeout().await?;
                Ok(ControlFlow::Continue(()))
            },
            () = pong_timeout, if pong_deadline.is_some() => {
                self.abort_for_ping_timeout();
                Ok(ControlFlow::Break(Ok(())))
            },
            changed = self.shutdown.changed() => {
                let shutdown_kind = changed.ok().and_then(|()| self.shutdown.borrow().kind());
                if let Some(kind) = shutdown_kind {
                    self.initiate_server_shutdown(kind)?;
                    Ok(ControlFlow::Break(Ok(())))
                } else {
                    Ok(ControlFlow::Continue(()))
                }
            },
            permit = pending_capacity, if pending_inbound_len.is_some() => {
                let message = self.pending_inbound.take().expect("capacity wait requires a pending message");
                let frame_kind = message.frame_kind();
                self.recv_tx.send_reserved(message, permit.map_err(|()| {
                    WebSocketError::receive_channel_closed(frame_kind).into_error()
                })?).map_err(|_| {
                    WebSocketError::receive_channel_closed(frame_kind).into_error()
                })?;
                Ok(ControlFlow::Continue(()))
            }
            read = self.transport.read_into_codec(&mut self.codec), if pending_inbound_len.is_none() => {
                self.handle_transport_read(read?).await
            }
        }
    }

    /// Drain the outbound events that are already available — `first`, then
    /// the buffered handshake queue, then anything sitting in the channel —
    /// flushing buffered frames once at the end of the batch.
    ///
    /// Bounded on both frames and payload bytes: every receive frees a
    /// channel slot the application can refill at once, so without a bound one
    /// batch could in principle grow while the producer keeps up, holding
    /// every encoded frame in memory and delaying reads, pings and shutdown.
    /// Whatever is left simply forms the next batch.
    ///
    /// Measured caveat: a Python producer could not actually sustain it —
    /// removing the bound and flooding from an ASGI app did not starve the
    /// session, because the Rust drain empties the channel faster than
    /// `await send()` can refill it. The bound is kept as an explicit
    /// worst-case cap (and to match the HTTP/2 fair-write quantum), not as a
    /// fix for a demonstrated starvation.
    async fn process_outbound_batch(
        &mut self,
        first: Option<AcceptedOutboundEvent>,
    ) -> Result<ControlFlow<Result<(), H2CornError>, OutboundBatchEnd>, H2CornError> {
        let mut next = first;
        let mut frames = 0_usize;
        let mut payload_bytes = 0_usize;

        let end = loop {
            if frames >= MAX_OUTBOUND_BATCH_FRAMES || payload_bytes >= MAX_OUTBOUND_BATCH_BYTES {
                break OutboundBatchEnd::Capped;
            }
            let outbound = if let Some(outbound) = next.take() {
                outbound
            } else if let Some(outbound) = self.running_app.send_buffer.take_ready() {
                parse_accepted_outbound_event(outbound)?
            } else {
                match self.running_app.send_rx.try_recv() {
                    Ok(outbound) => parse_accepted_outbound_event(outbound)?,
                    Err(TryRecvError::Empty | TryRecvError::Disconnected) => {
                        break OutboundBatchEnd::Drained;
                    },
                }
            };

            if frames > 0 && matches!(outbound, AcceptedOutboundEvent::Close { .. }) {
                self.transport.flush_buffered_frames().await?;
            }

            frames += 1;
            payload_bytes = payload_bytes.saturating_add(outbound.payload_len());
            if let ControlFlow::Break(result) = self.handle_outbound(outbound).await? {
                return Ok(ControlFlow::Break(result));
            }
        };

        if frames > 0 {
            self.transport.flush_buffered_frames().await?;
        }
        Ok(ControlFlow::Continue(end))
    }

    /// End the session because the application can produce nothing more.
    ///
    /// Its task completing and its sender being dropped are two observations of
    /// one event, and the `select!` may see either first. Both must end the
    /// session identically: drain what it queued, surface its result, and send
    /// the normal close — a path that skipped the close left the client with a
    /// bare EOF and the session logged as 1005 for roughly half of a
    /// `send(...); return` app's connections.
    async fn finish_after_app(
        &mut self,
    ) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError> {
        // Messages sent in the same instant as the app returned are still
        // queued; they go onto the wire before the result is acted on.
        if let ControlFlow::Break(result) = self.drain_outbound().await? {
            return Ok(ControlFlow::Break(result));
        }
        if !self.running_app.task.is_joined() {
            self.running_app.task.join_store().await;
        }
        let app_result = self.running_app.task.take_outcome().unwrap_or(Ok(()));
        if !self.state.close_started() {
            self.state.queue_close_if_open(close_code::NORMAL, "")?;
        }
        // Return the application outcome inside the completed session rather
        // than through this method's error channel. The caller must always
        // run transport finalization, task settlement, and access logging
        // after a session has accepted the WebSocket handshake.
        Ok(ControlFlow::Break(app_result))
    }

    /// Write everything the application queued, past the fairness quantum.
    ///
    /// Used only where nothing can produce more and no later batch will run:
    /// there a single bounded batch does not defer the rest, it discards it.
    async fn drain_outbound(
        &mut self,
    ) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError> {
        loop {
            match self.process_outbound_batch(None).await? {
                ControlFlow::Break(result) => return Ok(ControlFlow::Break(result)),
                ControlFlow::Continue(OutboundBatchEnd::Capped) => {},
                ControlFlow::Continue(OutboundBatchEnd::Drained) => {
                    return Ok(ControlFlow::Continue(()));
                },
            }
        }
    }

    async fn flush_ready_outbound(
        &mut self,
    ) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError> {
        let Some(outbound_event) = self
            .running_app
            .send_buffer
            .take_ready()
            .map(parse_accepted_outbound_event)
            .transpose()?
        else {
            return Ok(ControlFlow::Continue(()));
        };
        Ok(self
            .process_outbound_batch(Some(outbound_event))
            .await?
            .map_continue(|_| ()))
    }

    async fn handle_outbound(
        &mut self,
        outbound: AcceptedOutboundEvent,
    ) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError> {
        match outbound {
            AcceptedOutboundEvent::SendText(text) => {
                self.tx_bytes = self.tx_bytes.saturating_add(text.len() as u64);
                self.send_message_frame(0x1, text.into()).await?;
                Ok(ControlFlow::Continue(()))
            },
            AcceptedOutboundEvent::SendBytes(data) => {
                self.tx_bytes = self.tx_bytes.saturating_add(data.len() as u64);
                self.send_message_frame(0x2, data).await?;
                Ok(ControlFlow::Continue(()))
            },
            AcceptedOutboundEvent::Close { code, reason } => {
                match self
                    .state
                    .queue_close_if_open(code, reason.as_ref().map_or("", AsRef::as_ref))
                {
                    Ok(()) => Ok(ControlFlow::Break(Ok(()))),
                    Err(err) => self.fail_session(err),
                }
            },
        }
    }

    /// Send one application message, text or binary.
    ///
    /// The payload owns its bytes, so an uncompressed message is written from
    /// the application's own buffer with no copy — text included, which used
    /// to take a separate path that copied every message into the frame
    /// buffer. Compression produces new bytes and those are owned in turn.
    async fn send_message_frame(
        &mut self,
        opcode: u8,
        payload: PayloadBytes,
    ) -> Result<(), H2CornError> {
        let compressed: Option<Bytes> = M::compress(&mut self.deflater, payload.as_ref())
            .map_err(|_| WebSocketError::CompressionFailed.into_error())?;
        let (payload, compressed) =
            compressed.map_or((payload, false), |compressed| (compressed.into(), true));
        send_segmented_frame(
            self.transport,
            opcode,
            payload,
            FrameFlushMode::Buffered,
            compressed,
        )
        .await
    }

    async fn send_ping_and_arm_timeout(&mut self) -> Result<(), H2CornError> {
        send_encoded_frame(self.transport, 0x9, &[], FrameFlushMode::Immediate, false).await?;
        if let Some(ping) = &mut self.ping {
            ping.on_ping_sent();
        }
        Ok(())
    }

    fn abort_for_ping_timeout(&mut self) {
        self.running_app.close_outbound();
        self.state.mark_peer_reset(close_code::ABNORMAL_CLOSURE);
        self.recv_tx.disconnect(WebSocketDisconnect {
            code: close_code::ABNORMAL_CLOSURE,
            reason: BytesStr::from("ping timeout"),
        });
        self.running_app.task.abort();
    }

    fn initiate_server_shutdown(&mut self, kind: ShutdownKind) -> Result<(), H2CornError> {
        self.terminate_session(shutdown_close_code(kind), None, |state, code| {
            state.queue_close_if_open(code, "")
        })
    }

    /// Stop accepting app sends, advance close state, then publish terminal
    /// disconnect. Publication is synchronous so a full inbound data queue
    /// cannot block peer Close, ping timeout, or transport end.
    fn terminate_session(
        &mut self,
        code: WebSocketCloseCode,
        reason: Option<BytesStr>,
        update_state: impl FnOnce(
            &mut AcceptedWebSocketState,
            WebSocketCloseCode,
        ) -> Result<(), H2CornError>,
    ) -> Result<(), H2CornError> {
        self.running_app.close_outbound();
        update_state(&mut self.state, code)?;
        self.recv_tx.disconnect(WebSocketDisconnect {
            code,
            reason: reason.unwrap_or_default(),
        });
        Ok(())
    }

    async fn drain_decoded_frames(
        &mut self,
    ) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError> {
        // `run` enters here before waiting for another event, and a transport
        // read also enters here directly. Both paths must honor the same
        // stalled data-plane boundary rather than decoding the next frame in
        // the already-buffered input segment.
        if self.pending_inbound.is_some() {
            return Ok(ControlFlow::Continue(()));
        }

        loop {
            let frame = match self
                .codec
                .decode_next_with_inflater::<M>(&mut self.inflater)
            {
                Ok(Some(frame)) => frame,
                Ok(None) => return Ok(ControlFlow::Continue(())),
                Err(error) => {
                    self.terminate_session(error.close_code(), None, |state, code| {
                        state.queue_close_if_open(code, "")
                    })?;
                    self.running_app.task.abort();
                    return Ok(ControlFlow::Break(WebSocketError::Protocol(error).err()));
                },
            };

            if let ControlFlow::Break(result) = self.handle_decoded_frame(frame).await? {
                return Ok(ControlFlow::Break(result));
            }
            if self.pending_inbound.is_some() {
                // This message consumed the last byte ownership. Do not
                // decode a later frame or read more transport input until the
                // application releases capacity and this message is queued.
                return Ok(ControlFlow::Continue(()));
            }
        }
    }

    async fn handle_transport_read(
        &mut self,
        read: TransportRead,
    ) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError> {
        match read {
            TransportRead::Progress => {
                let result = self.drain_decoded_frames().await?;
                if matches!(result, ControlFlow::Continue(())) {
                    self.release_consumed_input();
                }
                Ok(result)
            },
            TransportRead::PeerGone => {
                let code = close_code::NO_STATUS_RECEIVED;
                self.terminate_session(code, None, |state, code| {
                    state.mark_peer_gone(code);
                    Ok(())
                })?;
                Ok(ControlFlow::Break(Ok(())))
            },
            TransportRead::PeerGoneSilent => {
                self.running_app.close_outbound();
                self.state.mark_peer_gone(close_code::NO_STATUS_RECEIVED);
                Ok(ControlFlow::Break(Ok(())))
            },
            TransportRead::PeerReset { reason } => {
                let code = close_code::ABNORMAL_CLOSURE;
                self.terminate_session(code, Some(reason), |state, code| {
                    state.mark_peer_reset(code);
                    Ok(())
                })?;
                Ok(ControlFlow::Break(Ok(())))
            },
        }
    }

    async fn handle_decoded_frame(
        &mut self,
        frame: DecodedFrame,
    ) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError> {
        match frame {
            DecodedFrame::Text(text) => {
                self.rx_bytes = self.rx_bytes.saturating_add(text.len() as u64);
                self.queue_inbound(
                    WebSocketInboundMessage::Text(text),
                    WebSocketFrameKind::Text,
                )
            },
            DecodedFrame::Binary(data) => {
                self.rx_bytes = self.rx_bytes.saturating_add(data.len() as u64);
                self.queue_inbound(
                    WebSocketInboundMessage::Bytes(data),
                    WebSocketFrameKind::Binary,
                )
            },
            DecodedFrame::Ping(payload) => {
                send_encoded_frame(
                    self.transport,
                    0xA,
                    payload.as_ref(),
                    FrameFlushMode::Immediate,
                    false,
                )
                .await?;
                Ok(ControlFlow::Continue(()))
            },
            DecodedFrame::Pong => {
                if let Some(ping) = &mut self.ping {
                    ping.on_pong();
                }
                Ok(ControlFlow::Continue(()))
            },
            DecodedFrame::Close(DecodedPeerClose::Coded { code, reason }) => {
                self.terminate_session(code.get(), reason, |state, code| {
                    state.queue_close_if_open(code, "")
                })?;
                Ok(ControlFlow::Break(Ok(())))
            },
            DecodedFrame::Close(DecodedPeerClose::Empty) => {
                self.running_app.close_outbound();
                self.recv_tx.disconnect(WebSocketDisconnect {
                    code: close_code::NO_STATUS_RECEIVED,
                    reason: BytesStr::default(),
                });
                self.state.queue_empty_close_if_open();
                Ok(ControlFlow::Break(Ok(())))
            },
        }
    }

    fn queue_inbound(
        &mut self,
        message: WebSocketInboundMessage,
        kind: WebSocketFrameKind,
    ) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError> {
        match self.recv_tx.try_send(message) {
            Ok(()) => Ok(ControlFlow::Continue(())),
            Err(WebSocketInboundTrySendError::Full(message)) => {
                debug_assert!(self.pending_inbound.is_none());
                self.pending_inbound = Some(message);
                Ok(ControlFlow::Continue(()))
            },
            Err(WebSocketInboundTrySendError::Closed) => {
                WebSocketError::receive_channel_closed(kind).err()
            },
        }
    }

    fn release_consumed_input(&mut self) {
        if self.pending_inbound.is_none() {
            self.transport.release_consumed_input();
        }
    }

    fn fail_session(
        &mut self,
        err: impl Into<H2CornError>,
    ) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError> {
        let err = err.into();
        self.running_app.close_outbound();
        let close_code = match err.failure_domain() {
            FailureDomain::PeerProtocol => Some(close_code::PROTOCOL_ERROR),
            FailureDomain::AppContract | FailureDomain::InternalInvariant => {
                Some(close_code::INTERNAL_ERROR)
            },
            // No wire remains for a close frame after a transport failure, and
            // configuration is not a peer-visible websocket condition.
            FailureDomain::Configuration | FailureDomain::TransportIo => None,
        };
        if let Some(close_code) = close_code {
            self.state.queue_close_if_open(close_code, "")?;
        }
        self.running_app.task.abort();
        Ok(ControlFlow::Break(Err(err)))
    }
}

fn parse_accepted_outbound_event(
    event: WebSocketOutboundEvent,
) -> Result<AcceptedOutboundEvent, H2CornError> {
    match event {
        WebSocketOutboundEvent::SendText(text) => Ok(AcceptedOutboundEvent::SendText(text)),
        WebSocketOutboundEvent::SendBytes(data) => Ok(AcceptedOutboundEvent::SendBytes(data)),
        WebSocketOutboundEvent::Close { code, reason } => {
            Ok(AcceptedOutboundEvent::Close { code, reason })
        },
        unexpected => {
            WebSocketError::unexpected_outbound_event_after_accept(unexpected.message_type()).err()
        },
    }
}

async fn send_encoded_frame<T>(
    transport: &mut T,
    opcode: u8,
    payload: &[u8],
    flush: FrameFlushMode,
    compressed: bool,
) -> Result<(), H2CornError>
where
    T: AcceptedWebSocketTransport,
{
    let frame = {
        let frame_buf = transport.frame_buf();
        encode_frame_into(opcode, payload, compressed, frame_buf);
        frame_buf.split().freeze()
    };
    transport
        .send_frame(EncodedWebSocketFrame::Contiguous(frame), flush)
        .await
}

async fn send_segmented_frame<T>(
    transport: &mut T,
    opcode: u8,
    payload: PayloadBytes,
    flush: FrameFlushMode,
    compressed: bool,
) -> Result<(), H2CornError>
where
    T: AcceptedWebSocketTransport,
{
    transport
        .send_frame(
            EncodedWebSocketFrame::segmented(opcode, payload, compressed),
            flush,
        )
        .await
}

pub(super) async fn run_accepted_session<T>(
    transport: &mut T,
    running_app: &mut RunningWebSocketApp,
    config: AcceptedSessionConfig,
) -> Result<AcceptedSessionOutcome, H2CornError>
where
    T: AcceptedWebSocketTransport,
{
    if config.per_message_deflate {
        AcceptedSession::<T, PerMessageDeflateEnabled>::new(transport, running_app, config)
            .run()
            .await
    } else {
        AcceptedSession::<T, PerMessageDeflateDisabled>::new(transport, running_app, config)
            .run()
            .await
    }
}

async fn sleep_until_or_pending(deadline: Option<TokioInstant>) {
    if let Some(deadline) = deadline {
        sleep_until(deadline).await;
    } else {
        pending::<()>().await;
    }
}
