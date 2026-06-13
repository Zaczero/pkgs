use std::future::pending;
use std::ops::ControlFlow;

use bytes::Bytes;
use pyo3::pybacked::PyBackedStr;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant as TokioInstant, sleep_until};

use super::super::app::RunningWebSocketApp;
use super::{
    AcceptedSessionConfig, AcceptedWebSocketState, AcceptedWebSocketTransport, FrameFlushMode,
    TransportRead, shutdown_close_code,
};
use crate::async_util::{send_best_effort, send_with_backpressure};
use crate::bridge::{PayloadBytes, WebSocketInboundEvent, WebSocketOutboundEvent};
use crate::error::{ErrorExt, FailureDomain, H2CornError, WebSocketError, WebSocketFrameKind};
use crate::hpack::BytesStr;
use crate::runtime::{ShutdownKind, ShutdownState};
use crate::websocket::codec::{DecodedFrame, WebSocketCodec, encode_frame_into};
use crate::websocket::deflate::{
    PerMessageDeflateDisabled, PerMessageDeflateEnabled, PerMessageDeflateMode,
};
use crate::websocket::{WebSocketCloseCode, close_code};

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

struct PingState {
    next_ping: Option<TokioInstant>,
    pong_deadline: Option<TokioInstant>,
    interval: Option<std::time::Duration>,
    timeout: Option<std::time::Duration>,
}

/// The accepted-session driver: owns the codec, compression state, ping
/// schedule, close state, and byte counters for one websocket session, so
/// the event handlers are methods instead of free functions threading
/// borrow-bundle structs.
struct AcceptedSession<'a, T, M: PerMessageDeflateMode> {
    transport: &'a mut T,
    running_app: &'a mut RunningWebSocketApp,
    recv_tx: mpsc::Sender<WebSocketInboundEvent>,
    shutdown: watch::Receiver<ShutdownState>,
    codec: WebSocketCodec,
    inflater: M::Inflater,
    deflater: M::Deflater,
    state: AcceptedWebSocketState,
    rx_bytes: u64,
    tx_bytes: u64,
    ping: PingState,
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
            ping: PingState {
                next_ping: next_ping_deadline(config.ping_interval),
                pong_deadline: None,
                interval: config.ping_interval,
                timeout: config.ping_timeout,
            },
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
        let ping_sleep = sleep_until_or_pending(self.ping.next_ping);
        let pong_timeout = sleep_until_or_pending(self.ping.pong_deadline);

        tokio::select! {
            () = self.running_app.app.join_store(), if !self.running_app.app.joined() => {
                // Messages the app sent just before returning may have become
                // ready in the same instant as its completion — drain them onto
                // the wire before acting on the result, or the final sends of a
                // `send(...); return` app are lost to select ordering.
                if let ControlFlow::Break(result) = self.process_outbound_batch(None).await? {
                    return Ok(ControlFlow::Break(result));
                }
                self.running_app.app.take_outcome().unwrap_or(Ok(()))?;
                if !self.state.close_started() {
                    self.state.queue_close_if_open(close_code::NORMAL, "")?;
                }
                Ok(ControlFlow::Break(Ok(())))
            }
            outbound_event = self.running_app.send_rx.recv() => {
                if let Some(outbound_event) = outbound_event {
                    let first = parse_accepted_outbound_event(outbound_event)?;
                    return self.process_outbound_batch(Some(first)).await;
                }
                // All senders are gone, but events buffered just before the
                // last sender dropped may still be queued — drain them onto
                // the wire before ending the session, mirroring the
                // app-completion arm above.
                if let ControlFlow::Break(result) = self.process_outbound_batch(None).await? {
                    return Ok(ControlFlow::Break(result));
                }
                Ok(ControlFlow::Break(Ok(())))
            },
            () = ping_sleep, if self.ping.next_ping.is_some() => {
                self.send_ping_and_arm_timeout().await?;
                Ok(ControlFlow::Continue(()))
            },
            () = pong_timeout, if self.ping.pong_deadline.is_some() => {
                self.abort_for_ping_timeout().await;
                Ok(ControlFlow::Break(Ok(())))
            },
            changed = self.shutdown.changed() => {
                let shutdown_kind = changed.ok().and_then(|()| self.shutdown.borrow().kind());
                if let Some(kind) = shutdown_kind {
                    self.initiate_server_shutdown(kind).await?;
                    Ok(ControlFlow::Break(Ok(())))
                } else {
                    Ok(ControlFlow::Continue(()))
                }
            },
            read = self.transport.read_into_codec(&mut self.codec) => {
                self.handle_transport_read(read?).await
            }
        }
    }

    /// Drain every outbound event that is already available — `first`, then
    /// the buffered handshake queue, then anything sitting in the channel —
    /// flushing buffered frames once at the end of the batch.
    async fn process_outbound_batch(
        &mut self,
        first: Option<AcceptedOutboundEvent>,
    ) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError> {
        let mut next = first;
        let mut sent = false;

        loop {
            let outbound = if let Some(outbound) = next.take() {
                outbound
            } else if let Some(outbound) = self.running_app.send_buffer.take_ready() {
                parse_accepted_outbound_event(outbound)?
            } else {
                match self.running_app.send_rx.try_recv() {
                    Ok(outbound) => parse_accepted_outbound_event(outbound)?,
                    Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
                }
            };

            if sent && matches!(outbound, AcceptedOutboundEvent::Close { .. }) {
                self.transport.flush_buffered_frames().await?;
            }

            sent = true;
            if let ControlFlow::Break(result) = self.handle_outbound(outbound).await? {
                return Ok(ControlFlow::Break(result));
            }
        }

        if sent {
            self.transport.flush_buffered_frames().await?;
        }
        Ok(ControlFlow::Continue(()))
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
        self.process_outbound_batch(Some(outbound_event)).await
    }

    async fn handle_outbound(
        &mut self,
        outbound: AcceptedOutboundEvent,
    ) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError> {
        match outbound {
            AcceptedOutboundEvent::SendText(text) => {
                self.tx_bytes = self.tx_bytes.saturating_add(text.len() as u64);
                self.send_message_frame(0x1, text.as_bytes()).await?;
                Ok(ControlFlow::Continue(()))
            },
            AcceptedOutboundEvent::SendBytes(data) => {
                self.tx_bytes = self.tx_bytes.saturating_add(data.len() as u64);
                self.send_message_frame(0x2, data.as_ref()).await?;
                Ok(ControlFlow::Continue(()))
            },
            AcceptedOutboundEvent::Close { code, reason } => {
                match self
                    .state
                    .queue_close_if_open(code, reason.as_ref().map_or("", AsRef::as_ref))
                {
                    Ok(()) => Ok(ControlFlow::Break(Ok(()))),
                    Err(err) => self.fail_session(err).await,
                }
            },
        }
    }

    async fn send_message_frame(&mut self, opcode: u8, payload: &[u8]) -> Result<(), H2CornError> {
        let compressed_payload: Option<Bytes> = M::compress(&mut self.deflater, payload)
            .map_err(|_| WebSocketError::CompressionFailed.into_error())?;
        let (payload, compressed) = compressed_payload
            .as_deref()
            .map_or((payload, false), |payload| (payload, true));
        send_encoded_frame(
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
        self.ping.next_ping = self
            .ping
            .interval
            .map(|interval| TokioInstant::now() + interval);
        if let Some(timeout_duration) = self.ping.timeout {
            self.ping.pong_deadline = Some(TokioInstant::now() + timeout_duration);
        }
        Ok(())
    }

    async fn abort_for_ping_timeout(&mut self) {
        self.running_app.close_outbound();
        self.state.mark_peer_reset(close_code::ABNORMAL_CLOSURE);
        send_best_effort(&self.recv_tx, WebSocketInboundEvent::Disconnect {
            code: close_code::ABNORMAL_CLOSURE,
            reason: Some("ping timeout".into()),
        })
        .await;
        self.running_app.app.abort().await;
    }

    async fn initiate_server_shutdown(&mut self, kind: ShutdownKind) -> Result<(), H2CornError> {
        self.terminate_session(shutdown_close_code(kind), None, |state, code| {
            state.queue_close_if_open(code, "")
        })
        .await
    }

    /// Stop accepting app sends, deliver the disconnect event, and apply the
    /// close-state transition for a session ending on `code`.
    async fn terminate_session(
        &mut self,
        code: WebSocketCloseCode,
        reason: Option<BytesStr>,
        update_state: impl FnOnce(
            &mut AcceptedWebSocketState,
            WebSocketCloseCode,
        ) -> Result<(), H2CornError>,
    ) -> Result<(), H2CornError> {
        self.running_app.close_outbound();
        send_best_effort(&self.recv_tx, WebSocketInboundEvent::Disconnect {
            code,
            reason,
        })
        .await;
        update_state(&mut self.state, code)
    }

    async fn drain_decoded_frames(
        &mut self,
    ) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError> {
        loop {
            let frame = match self
                .codec
                .decode_next_with_inflater::<M>(&mut self.inflater)
            {
                Ok(Some(frame)) => frame,
                Ok(None) => return Ok(ControlFlow::Continue(())),
                Err(err) => {
                    self.terminate_session(err.close_code, None, |state, code| {
                        state.queue_close_if_open(code, "")
                    })
                    .await?;
                    self.running_app.app.abort().await;
                    return Ok(ControlFlow::Break(
                        WebSocketError::Protocol(err.error).err(),
                    ));
                },
            };

            if let ControlFlow::Break(result) = self.handle_decoded_frame(frame).await? {
                return Ok(ControlFlow::Break(result));
            }
        }
    }

    async fn handle_transport_read(
        &mut self,
        read: TransportRead,
    ) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError> {
        match read {
            TransportRead::Progress => self.drain_decoded_frames().await,
            TransportRead::PeerGone => {
                let code = close_code::NO_STATUS_RECEIVED;
                self.terminate_session(code, None, |state, code| {
                    state.set_close_code(code);
                    Ok(())
                })
                .await?;
                Ok(ControlFlow::Break(Ok(())))
            },
            TransportRead::PeerGoneSilent => {
                self.running_app.close_outbound();
                self.state.set_close_code(close_code::NO_STATUS_RECEIVED);
                Ok(ControlFlow::Break(Ok(())))
            },
            TransportRead::PeerReset { reason } => {
                let code = close_code::ABNORMAL_CLOSURE;
                self.terminate_session(code, Some(reason), |state, code| {
                    state.mark_peer_reset(code);
                    Ok(())
                })
                .await?;
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
                send_with_backpressure(
                    &self.recv_tx,
                    WebSocketInboundEvent::ReceiveText(text),
                    || WebSocketError::receive_channel_closed(WebSocketFrameKind::Text),
                )
                .await?;
                Ok(ControlFlow::Continue(()))
            },
            DecodedFrame::Binary(data) => {
                self.rx_bytes = self.rx_bytes.saturating_add(data.len() as u64);
                send_with_backpressure(
                    &self.recv_tx,
                    WebSocketInboundEvent::ReceiveBytes(data),
                    || WebSocketError::receive_channel_closed(WebSocketFrameKind::Binary),
                )
                .await?;
                Ok(ControlFlow::Continue(()))
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
                self.ping.pong_deadline = None;
                Ok(ControlFlow::Continue(()))
            },
            DecodedFrame::Close { code, reason } => {
                self.terminate_session(code, reason, |state, code| {
                    state.set_close_code(code);
                    state.queue_close_if_open(code, "")
                })
                .await?;
                Ok(ControlFlow::Break(Ok(())))
            },
        }
    }

    async fn fail_session(
        &mut self,
        err: impl Into<H2CornError>,
    ) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError> {
        let err = err.into();
        self.running_app.close_outbound();
        let close_code = match err.failure_domain() {
            FailureDomain::PeerProtocol => close_code::PROTOCOL_ERROR,
            FailureDomain::Configuration
            | FailureDomain::TransportIo
            | FailureDomain::AppContract
            | FailureDomain::InternalInvariant => close_code::INTERNAL_ERROR,
        };
        self.state.queue_close_if_open(close_code, "")?;
        self.running_app.app.abort().await;
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
        unexpected => WebSocketError::unexpected_outbound_event_after_accept(&unexpected).err(),
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
    transport.send_frame(frame, flush).await
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

fn next_ping_deadline(ping_interval: Option<std::time::Duration>) -> Option<TokioInstant> {
    let now = TokioInstant::now();
    ping_interval.map(|interval| now + interval)
}

async fn sleep_until_or_pending(deadline: Option<TokioInstant>) {
    if let Some(deadline) = deadline {
        sleep_until(deadline).await;
    } else {
        pending::<()>().await;
    }
}
