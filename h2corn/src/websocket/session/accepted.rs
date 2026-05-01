use std::{future::pending, ops::ControlFlow};

use bytes::Bytes;
use pyo3::pybacked::PyBackedStr;
use tokio::sync::{
    mpsc::{self, error::TryRecvError},
    watch,
};
use tokio::time::{Instant as TokioInstant, sleep_until};

use crate::async_util::{send_best_effort, send_with_backpressure};
use crate::bridge::{PayloadBytes, WebSocketInboundEvent, WebSocketOutboundEvent};
use crate::error::{ErrorExt, FailureDomain, H2CornError, WebSocketError, WebSocketFrameKind};
use crate::hpack::BytesStr;
use crate::runtime::ShutdownKind;
use crate::websocket::codec::{DecodedFrame, WebSocketCodec, encode_frame_into};
use crate::websocket::deflate::{
    PerMessageDeflateDisabled, PerMessageDeflateEnabled, PerMessageDeflateMode,
};
use crate::websocket::{WebSocketCloseCode, close_code};

use super::super::app::RunningWebSocketApp;
use super::handshake::abort_app_task;
use super::{
    AcceptedSessionConfig, AcceptedWebSocketState, AcceptedWebSocketTransport, FrameFlushMode,
    TransportRead, shutdown_close_code,
};

pub(super) struct AcceptedSessionOutcome {
    pub(super) state: AcceptedWebSocketState,
    pub(super) rx_bytes: u64,
    pub(super) tx_bytes: u64,
    pub(super) app_finished: bool,
    pub(super) result: Result<(), H2CornError>,
}

impl AcceptedSessionOutcome {
    const fn new(
        state: AcceptedWebSocketState,
        rx_bytes: u64,
        tx_bytes: u64,
        app_finished: bool,
        result: Result<(), H2CornError>,
    ) -> Self {
        Self {
            state,
            rx_bytes,
            tx_bytes,
            app_finished,
            result,
        }
    }
}

enum AcceptedOutboundEvent {
    SendText(PyBackedStr),
    SendBytes(PayloadBytes),
    Close {
        code: WebSocketCloseCode,
        reason: Option<PyBackedStr>,
    },
}

struct OutboundParts<'a> {
    running_app: &'a mut RunningWebSocketApp,
    state: &'a mut AcceptedWebSocketState,
    tx_bytes: &'a mut u64,
}

struct InboundParts<'a> {
    recv_tx: &'a mpsc::Sender<WebSocketInboundEvent>,
    running_app: &'a mut RunningWebSocketApp,
    state: &'a mut AcceptedWebSocketState,
    rx_bytes: &'a mut u64,
}

struct PingState {
    next_ping: Option<TokioInstant>,
    pong_deadline: Option<TokioInstant>,
    interval: Option<std::time::Duration>,
    timeout: Option<std::time::Duration>,
}

fn parse_accepted_outbound_event(
    event: WebSocketOutboundEvent,
) -> Result<AcceptedOutboundEvent, H2CornError> {
    match event {
        WebSocketOutboundEvent::SendText(text) => Ok(AcceptedOutboundEvent::SendText(text)),
        WebSocketOutboundEvent::SendBytes(data) => Ok(AcceptedOutboundEvent::SendBytes(data)),
        WebSocketOutboundEvent::Close { code, reason } => {
            Ok(AcceptedOutboundEvent::Close { code, reason })
        }
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

async fn process_outbound_batch<T, M>(
    transport: &mut T,
    first: Option<AcceptedOutboundEvent>,
    deflater: &mut M::Deflater,
    context: &mut OutboundParts<'_>,
) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError>
where
    T: AcceptedWebSocketTransport,
    M: PerMessageDeflateMode,
{
    let mut next = first;
    let mut sent = false;

    loop {
        let outbound = if let Some(outbound) = next.take() {
            outbound
        } else if let Some(outbound) = context.running_app.send_buffer.take_ready() {
            parse_accepted_outbound_event(outbound)?
        } else {
            match context.running_app.send_rx.try_recv() {
                Ok(outbound) => parse_accepted_outbound_event(outbound)?,
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        };

        if sent && matches!(outbound, AcceptedOutboundEvent::Close { .. }) {
            transport.flush_buffered_frames().await?;
        }

        sent = true;
        if let ControlFlow::Break(result) =
            handle_outbound::<T, M>(transport, deflater, outbound, context).await?
        {
            return Ok(ControlFlow::Break(result));
        }
    }

    if sent {
        transport.flush_buffered_frames().await?;
    }
    Ok(ControlFlow::Continue(()))
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
        run_accepted_session_with_mode::<T, PerMessageDeflateEnabled>(
            transport,
            running_app,
            config,
        )
        .await
    } else {
        run_accepted_session_with_mode::<T, PerMessageDeflateDisabled>(
            transport,
            running_app,
            config,
        )
        .await
    }
}

async fn run_accepted_session_with_mode<T, M>(
    transport: &mut T,
    running_app: &mut RunningWebSocketApp,
    config: AcceptedSessionConfig,
) -> Result<AcceptedSessionOutcome, H2CornError>
where
    T: AcceptedWebSocketTransport,
    M: PerMessageDeflateMode,
{
    let mut shutdown = config.shutdown;
    let recv_tx = running_app.recv_tx.clone();
    let mut codec = transport.websocket_codec(config.max_message_size);
    let mut inflater = M::new_inflater();
    let mut deflater = M::new_deflater();
    let mut state = AcceptedWebSocketState::default();
    let mut rx_bytes = 0_u64;
    let mut tx_bytes = 0_u64;
    let mut app_finished = false;
    let mut ping = PingState {
        next_ping: next_ping_deadline(config.ping_interval),
        pong_deadline: None,
        interval: config.ping_interval,
        timeout: config.ping_timeout,
    };
    let result: Result<(), H2CornError> = {
        let mut outbound = OutboundParts {
            running_app,
            state: &mut state,
            tx_bytes: &mut tx_bytes,
        };

        'session: loop {
            if let ControlFlow::Break(result) =
                flush_ready_outbound::<T, M>(transport, &mut deflater, &mut outbound).await?
            {
                break 'session result;
            }

            if let ControlFlow::Break(result) = drain_decoded_frames::<T, M>(
                transport,
                &mut codec,
                &mut inflater,
                &mut ping.pong_deadline,
                &mut InboundParts {
                    recv_tx: &recv_tx,
                    running_app: outbound.running_app,
                    state: outbound.state,
                    rx_bytes: &mut rx_bytes,
                },
            )
            .await?
            {
                break 'session result;
            }

            if let ControlFlow::Break(result) = wait_session_event::<T, M>(
                transport,
                &mut shutdown,
                &mut codec,
                &mut inflater,
                &mut deflater,
                &recv_tx,
                &mut outbound,
                &mut rx_bytes,
                &mut app_finished,
                &mut ping,
            )
            .await?
            {
                break 'session result;
            }
        }
    };

    Ok(AcceptedSessionOutcome::new(
        state,
        rx_bytes,
        tx_bytes,
        app_finished,
        result,
    ))
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

async fn wait_session_event<T, M>(
    transport: &mut T,
    shutdown: &mut watch::Receiver<crate::runtime::ShutdownState>,
    codec: &mut WebSocketCodec,
    inflater: &mut M::Inflater,
    deflater: &mut M::Deflater,
    recv_tx: &mpsc::Sender<WebSocketInboundEvent>,
    outbound: &mut OutboundParts<'_>,
    rx_bytes: &mut u64,
    app_finished: &mut bool,
    ping: &mut PingState,
) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError>
where
    T: AcceptedWebSocketTransport,
    M: PerMessageDeflateMode,
{
    let ping_sleep = sleep_until_or_pending(ping.next_ping);
    let pong_timeout = sleep_until_or_pending(ping.pong_deadline);

    tokio::select! {
        result = &mut outbound.running_app.app_task, if !*app_finished => {
            result??;
            *app_finished = true;
            queue_normal_close_after_app_finish(outbound.state)?;
            Ok(ControlFlow::Break(Ok(())))
        }
        outbound_event = outbound.running_app.send_rx.recv() => match outbound_event {
            Some(outbound_event) => {
                process_app_outbound_event::<T, M>(
                    transport, outbound_event, deflater, outbound
                ).await
            }
            None => Ok(ControlFlow::Break(Ok(()))),
        },
        () = ping_sleep, if ping.next_ping.is_some() => {
            send_ping_and_arm_timeout(transport, ping).await?;
            Ok(ControlFlow::Continue(()))
        },
        () = pong_timeout, if ping.pong_deadline.is_some() => {
            abort_for_ping_timeout(recv_tx, outbound).await;
            Ok(ControlFlow::Break(Ok(())))
        },
        changed = shutdown.changed() => {
            let shutdown_kind = changed.ok().and_then(|()| shutdown.borrow().kind());
            if let Some(kind) = shutdown_kind {
                initiate_server_shutdown(recv_tx, outbound, kind).await?;
                Ok(ControlFlow::Break(Ok(())))
            } else {
                Ok(ControlFlow::Continue(()))
            }
        },
        read = transport.read_into_codec(codec) => {
            handle_transport_read::<T, M>(
                transport,
                read?,
                codec,
                inflater,
                &mut ping.pong_deadline,
                &mut InboundParts {
                    recv_tx,
                    running_app: outbound.running_app,
                    state: outbound.state,
                    rx_bytes,
                },
            )
            .await
        }
    }
}

async fn send_ping_and_arm_timeout<T>(
    transport: &mut T,
    ping: &mut PingState,
) -> Result<(), H2CornError>
where
    T: AcceptedWebSocketTransport,
{
    send_encoded_frame(transport, 0x9, &[], FrameFlushMode::Immediate, false).await?;
    ping.next_ping = ping.interval.map(|interval| TokioInstant::now() + interval);
    if let Some(timeout_duration) = ping.timeout {
        ping.pong_deadline = Some(TokioInstant::now() + timeout_duration);
    }
    Ok(())
}

async fn abort_for_ping_timeout(
    recv_tx: &mpsc::Sender<WebSocketInboundEvent>,
    outbound: &mut OutboundParts<'_>,
) {
    outbound.running_app.close_outbound();
    outbound.state.mark_peer_reset(close_code::ABNORMAL_CLOSURE);
    send_best_effort(
        recv_tx,
        WebSocketInboundEvent::Disconnect {
            code: close_code::ABNORMAL_CLOSURE,
            reason: Some("ping timeout".into()),
        },
    )
    .await;
    abort_app_task(outbound.running_app).await;
}

async fn flush_ready_outbound<T, M>(
    transport: &mut T,
    deflater: &mut M::Deflater,
    outbound: &mut OutboundParts<'_>,
) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError>
where
    T: AcceptedWebSocketTransport,
    M: PerMessageDeflateMode,
{
    let Some(outbound_event) = outbound
        .running_app
        .send_buffer
        .take_ready()
        .map(parse_accepted_outbound_event)
        .transpose()?
    else {
        return Ok(ControlFlow::Continue(()));
    };
    process_outbound_batch::<T, M>(transport, Some(outbound_event), deflater, outbound).await
}

fn queue_normal_close_after_app_finish(
    state: &mut AcceptedWebSocketState,
) -> Result<(), H2CornError> {
    if !state.close_started() {
        state.queue_close_if_open(close_code::NORMAL, "")?;
    }
    Ok(())
}

async fn process_app_outbound_event<T, M>(
    transport: &mut T,
    outbound_event: WebSocketOutboundEvent,
    deflater: &mut M::Deflater,
    outbound: &mut OutboundParts<'_>,
) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError>
where
    T: AcceptedWebSocketTransport,
    M: PerMessageDeflateMode,
{
    process_outbound_batch::<T, M>(
        transport,
        Some(parse_accepted_outbound_event(outbound_event)?),
        deflater,
        outbound,
    )
    .await
}

async fn initiate_server_shutdown(
    recv_tx: &mpsc::Sender<WebSocketInboundEvent>,
    outbound: &mut OutboundParts<'_>,
    kind: ShutdownKind,
) -> Result<(), H2CornError> {
    terminate_session(
        outbound.running_app,
        recv_tx,
        outbound.state,
        shutdown_close_code(kind),
        None,
        |state, code| state.queue_close_if_open(code, ""),
    )
    .await?;
    Ok(())
}

async fn terminate_session(
    running_app: &mut RunningWebSocketApp,
    recv_tx: &mpsc::Sender<WebSocketInboundEvent>,
    state: &mut AcceptedWebSocketState,
    code: WebSocketCloseCode,
    reason: Option<BytesStr>,
    update_state: impl FnOnce(
        &mut AcceptedWebSocketState,
        WebSocketCloseCode,
    ) -> Result<(), H2CornError>,
) -> Result<(), H2CornError> {
    running_app.close_outbound();
    send_best_effort(recv_tx, WebSocketInboundEvent::Disconnect { code, reason }).await;
    update_state(state, code)
}

async fn terminate_inbound_session(
    context: &mut InboundParts<'_>,
    code: WebSocketCloseCode,
    reason: Option<BytesStr>,
    update_state: impl FnOnce(
        &mut AcceptedWebSocketState,
        WebSocketCloseCode,
    ) -> Result<(), H2CornError>,
) -> Result<(), H2CornError> {
    terminate_session(
        context.running_app,
        context.recv_tx,
        context.state,
        code,
        reason,
        update_state,
    )
    .await
}

async fn drain_decoded_frames<T, M>(
    transport: &mut T,
    codec: &mut WebSocketCodec,
    inflater: &mut M::Inflater,
    pong_deadline: &mut Option<TokioInstant>,
    context: &mut InboundParts<'_>,
) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError>
where
    T: AcceptedWebSocketTransport,
    M: PerMessageDeflateMode,
{
    loop {
        let frame = match codec.decode_next_with_inflater::<M>(inflater) {
            Ok(Some(frame)) => frame,
            Ok(None) => return Ok(ControlFlow::Continue(())),
            Err(err) => {
                terminate_inbound_session(context, err.close_code, None, |state, code| {
                    state.queue_close_if_open(code, "")
                })
                .await?;
                abort_app_task(context.running_app).await;
                return Ok(ControlFlow::Break(
                    WebSocketError::Protocol(err.error).err(),
                ));
            }
        };

        if let ControlFlow::Break(result) =
            handle_decoded_frame(transport, frame, pong_deadline, context).await?
        {
            return Ok(ControlFlow::Break(result));
        }
    }
}

async fn handle_transport_read<T, M>(
    transport: &mut T,
    read: TransportRead,
    codec: &mut WebSocketCodec,
    inflater: &mut M::Inflater,
    pong_deadline: &mut Option<TokioInstant>,
    context: &mut InboundParts<'_>,
) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError>
where
    T: AcceptedWebSocketTransport,
    M: PerMessageDeflateMode,
{
    match read {
        TransportRead::Progress => {
            drain_decoded_frames::<T, M>(transport, codec, inflater, pong_deadline, context).await
        }
        TransportRead::PeerGone => {
            let code = close_code::NO_STATUS_RECEIVED;
            terminate_inbound_session(context, code, None, |state, code| {
                state.set_close_code(code);
                Ok(())
            })
            .await?;
            Ok(ControlFlow::Break(Ok(())))
        }
        TransportRead::PeerGoneSilent => {
            let code = close_code::NO_STATUS_RECEIVED;
            context.running_app.close_outbound();
            context.state.set_close_code(code);
            Ok(ControlFlow::Break(Ok(())))
        }
        TransportRead::PeerReset { reason } => {
            let code = close_code::ABNORMAL_CLOSURE;
            terminate_inbound_session(context, code, Some(reason), |state, code| {
                state.mark_peer_reset(code);
                Ok(())
            })
            .await?;
            Ok(ControlFlow::Break(Ok(())))
        }
    }
}

async fn handle_decoded_frame<T>(
    transport: &mut T,
    frame: DecodedFrame,
    pong_deadline: &mut Option<TokioInstant>,
    context: &mut InboundParts<'_>,
) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError>
where
    T: AcceptedWebSocketTransport,
{
    match frame {
        DecodedFrame::Text(text) => {
            *context.rx_bytes = context.rx_bytes.saturating_add(text.len() as u64);
            send_with_backpressure(
                context.recv_tx,
                WebSocketInboundEvent::ReceiveText(text),
                || WebSocketError::receive_channel_closed(WebSocketFrameKind::Text),
            )
            .await?;
            Ok(ControlFlow::Continue(()))
        }
        DecodedFrame::Binary(data) => {
            *context.rx_bytes = context.rx_bytes.saturating_add(data.len() as u64);
            send_with_backpressure(
                context.recv_tx,
                WebSocketInboundEvent::ReceiveBytes(data),
                || WebSocketError::receive_channel_closed(WebSocketFrameKind::Binary),
            )
            .await?;
            Ok(ControlFlow::Continue(()))
        }
        DecodedFrame::Ping(payload) => {
            send_encoded_frame(
                transport,
                0xA,
                payload.as_ref(),
                FrameFlushMode::Immediate,
                false,
            )
            .await?;
            Ok(ControlFlow::Continue(()))
        }
        DecodedFrame::Pong => {
            *pong_deadline = None;
            Ok(ControlFlow::Continue(()))
        }
        DecodedFrame::Close { code, reason } => {
            terminate_inbound_session(context, code, reason, |state, code| {
                state.set_close_code(code);
                state.queue_close_if_open(code, "")
            })
            .await?;
            Ok(ControlFlow::Break(Ok(())))
        }
    }
}

fn compress_payload_for_mode<M: PerMessageDeflateMode>(
    deflater: &mut M::Deflater,
    payload: &[u8],
) -> Result<Option<Bytes>, H2CornError> {
    M::compress(deflater, payload).map_err(|_| WebSocketError::CompressionFailed.into_error())
}

async fn send_message_frame<T, M>(
    transport: &mut T,
    deflater: &mut M::Deflater,
    opcode: u8,
    payload: &[u8],
) -> Result<(), H2CornError>
where
    T: AcceptedWebSocketTransport,
    M: PerMessageDeflateMode,
{
    let compressed_payload = compress_payload_for_mode::<M>(deflater, payload)?;
    let (payload, compressed) = compressed_payload
        .as_deref()
        .map_or((payload, false), |payload| (payload, true));
    send_encoded_frame(
        transport,
        opcode,
        payload,
        FrameFlushMode::Buffered,
        compressed,
    )
    .await
}

async fn handle_outbound<T, M>(
    transport: &mut T,
    deflater: &mut M::Deflater,
    outbound: AcceptedOutboundEvent,
    context: &mut OutboundParts<'_>,
) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError>
where
    T: AcceptedWebSocketTransport,
    M: PerMessageDeflateMode,
{
    match outbound {
        AcceptedOutboundEvent::SendText(text) => {
            *context.tx_bytes = context.tx_bytes.saturating_add(text.len() as u64);
            send_message_frame::<T, M>(transport, deflater, 0x1, text.as_bytes()).await?;
            Ok(ControlFlow::Continue(()))
        }
        AcceptedOutboundEvent::SendBytes(data) => {
            *context.tx_bytes = context.tx_bytes.saturating_add(data.len() as u64);
            send_message_frame::<T, M>(transport, deflater, 0x2, data.as_ref()).await?;
            Ok(ControlFlow::Continue(()))
        }
        AcceptedOutboundEvent::Close { code, reason } => {
            match context
                .state
                .queue_close_if_open(code, reason.as_ref().map_or("", AsRef::as_ref))
            {
                Ok(()) => Ok(ControlFlow::Break(Ok(()))),
                Err(err) => fail_session(context.running_app, context.state, err).await,
            }
        }
    }
}

async fn fail_session<E>(
    running_app: &mut RunningWebSocketApp,
    state: &mut AcceptedWebSocketState,
    err: E,
) -> Result<ControlFlow<Result<(), H2CornError>>, H2CornError>
where
    E: Into<H2CornError>,
{
    let err = err.into();
    running_app.close_outbound();
    let close_code = match err.failure_domain() {
        FailureDomain::PeerProtocol => close_code::PROTOCOL_ERROR,
        FailureDomain::Configuration
        | FailureDomain::TransportIo
        | FailureDomain::AppContract
        | FailureDomain::InternalInvariant => close_code::INTERNAL_ERROR,
    };
    state.queue_close_if_open(close_code, "")?;
    abort_app_task(running_app).await;
    Ok(ControlFlow::Break(Err(err)))
}
