mod http;
mod parse;
mod websocket;

use std::{num::NonZeroU64, sync::atomic::Ordering, time::Duration};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncWriteExt, BufWriter};
use tokio::sync::{mpsc, watch};

use self::http::{
    H1HttpTransport, write_empty_response, write_h2c_upgrade_response, write_simple_response,
};
use crate::async_util::send_best_effort;
use crate::config::ServerConfig;
use crate::console::run_http_request;
use crate::error::{ErrorExt, H2CornError, Http1Error};
use crate::frame::{self, PeerSettings};
use crate::h2::{H2WriteTarget, UpgradedH2Request, serve_h2_upgraded_connection};
use crate::http::execution::prepare_request_input;
use crate::http::planner::{
    RequestInputPlan, RequestRoute, plan_request_input, reject_oversized_request,
};
use crate::http::types::{HttpVersion, RequestHead, status_code};
use crate::http::{app::HttpRequestBody, body::RequestBodyState, response::HttpResponseTransport};
use crate::runtime::{
    ConnectionContext, RequestAdmission, RequestContext, ShutdownState, StreamInput,
    try_acquire_request_admission,
};
use crate::websocket::{HandshakeRejection, WebSocketContext, WebSocketKey, WebSocketRequestMeta};
pub use http::H1WriteTarget;
use parse::read_request;

struct ParsedRequest {
    request: RequestHead,
    upgrade: UpgradeRequest,
    body_kind: RequestBodyKind,
    persistence: ConnectionPersistence,
}

enum UpgradeRequest {
    None,
    WebSocket {
        key: WebSocketKey,
        meta: WebSocketRequestMeta,
    },
    WebSocketBadRequest,
    WebSocketUnsupportedVersion,
    H2c {
        settings: PeerSettings,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestBodyKind {
    None,
    ContentLength(NonZeroU64),
    Chunked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConnectionPersistence {
    KeepAlive,
    Close,
}

struct H1UpgradeContext {
    connection: ConnectionContext,
    secure: bool,
    shutdown: watch::Receiver<ShutdownState>,
}

struct H1Io<'a, R, W> {
    reader: &'a mut R,
    buffer: &'a mut BytesMut,
    writer: &'a mut BufWriter<W>,
}

pub async fn serve_connection<R, W>(
    mut reader: R,
    mut buffer: BytesMut,
    writer: W,
    connection: ConnectionContext,
    secure: bool,
    mut shutdown: watch::Receiver<ShutdownState>,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: H1WriteTarget + H2WriteTarget,
{
    let mut writer = BufWriter::with_capacity(frame::DEFAULT_MAX_FRAME_SIZE, writer);
    let mut first_request = true;

    loop {
        if shutdown.borrow().kind().is_some() {
            break;
        }

        let parsed = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_ok() && shutdown.borrow().kind().is_some() {
                    break;
                }
                continue;
            }
            parsed = read_request(
                &mut reader,
                &mut buffer,
                &mut writer,
                connection.config,
                secure,
                request_head_timeout(connection.config, first_request),
            ) => parsed,
        }?;

        let Some(parsed) = parsed else { break };
        first_request = false;
        let ParsedRequest {
            request,
            upgrade,
            body_kind,
            persistence,
        } = parsed;
        match upgrade {
            UpgradeRequest::None => {
                if handle_http_request(
                    RequestContext::new(connection.clone(), request),
                    body_kind,
                    persistence,
                    H1Io {
                        reader: &mut reader,
                        buffer: &mut buffer,
                        writer: &mut writer,
                    },
                )
                .await?
                    == ConnectionPersistence::Close
                {
                    break;
                }
            }
            upgrade => {
                return handle_upgrade_request(
                    request,
                    body_kind,
                    upgrade,
                    reader,
                    buffer,
                    writer,
                    H1UpgradeContext {
                        connection,
                        secure,
                        shutdown,
                    },
                )
                .await;
            }
        }
    }

    writer.flush().await?;
    Ok(())
}

fn request_head_timeout(config: &ServerConfig, first_request: bool) -> Option<Duration> {
    if first_request {
        return config.timeout_request_header;
    }

    match (config.timeout_keep_alive, config.timeout_request_header) {
        (Some(keep_alive), Some(read)) => Some(keep_alive.min(read)),
        (Some(keep_alive), None) => Some(keep_alive),
        (None, Some(read)) => Some(read),
        (None, None) => None,
    }
}

async fn handle_upgrade_request<R, W>(
    request: RequestHead,
    body_kind: RequestBodyKind,
    upgrade: UpgradeRequest,
    reader: R,
    buffer: BytesMut,
    mut writer: BufWriter<W>,
    context: H1UpgradeContext,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: H1WriteTarget + H2WriteTarget,
{
    match upgrade {
        UpgradeRequest::WebSocket { key, meta } => {
            let Some(admission) = try_acquire_request_admission(&context.connection.app) else {
                write_empty_response(
                    &mut writer,
                    context.connection.config,
                    status_code::SERVICE_UNAVAILABLE,
                    true,
                )
                .await?;
                return Ok(());
            };
            Box::pin(websocket::handle_request(
                WebSocketContext {
                    request: RequestContext::new(context.connection, request),
                    admission,
                    meta,
                },
                key,
                reader,
                buffer,
                writer,
            ))
            .await
        }
        UpgradeRequest::WebSocketUnsupportedVersion => {
            let response = HandshakeRejection::unsupported_version();
            write_simple_response(
                &mut writer,
                context.connection.config,
                response.status,
                response.headers,
                &[],
                true,
            )
            .await?;
            Ok(())
        }
        UpgradeRequest::WebSocketBadRequest => {
            write_empty_response(
                &mut writer,
                context.connection.config,
                status_code::BAD_REQUEST,
                true,
            )
            .await?;
            Ok(())
        }
        UpgradeRequest::H2c { settings } => {
            Box::pin(serve_h2c_upgrade_request(
                request, body_kind, settings, reader, buffer, writer, context,
            ))
            .await
        }
        UpgradeRequest::None => Ok(()),
    }
}

async fn serve_h2c_upgrade_request<R, W>(
    mut request: RequestHead,
    body_kind: RequestBodyKind,
    settings: PeerSettings,
    reader: R,
    buffer: BytesMut,
    mut writer: BufWriter<W>,
    context: H1UpgradeContext,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: H1WriteTarget + H2WriteTarget,
{
    request.http_version = HttpVersion::Http2;
    if body_kind != RequestBodyKind::None {
        return Http1Error::H2cUpgradeWithRequestBody.err();
    }
    write_h2c_upgrade_response(&mut writer, context.connection.config).await?;
    let H1UpgradeContext {
        connection,
        secure,
        shutdown,
    } = context;
    serve_h2_upgraded_connection(
        reader,
        writer.into_inner(),
        connection,
        secure,
        shutdown,
        UpgradedH2Request {
            buffer,
            request,
            body: Bytes::new(),
            peer_settings: settings,
        },
    )
    .await
}

async fn handle_http_request<R, W>(
    ctx: RequestContext,
    body_kind: RequestBodyKind,
    persistence: ConnectionPersistence,
    io: H1Io<'_, R, W>,
) -> Result<ConnectionPersistence, H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: H1WriteTarget,
{
    let H1Io {
        reader,
        buffer,
        writer,
    } = io;
    let config = ctx.connection.config;
    let input_plan = plan_request_input(
        &RequestRoute::<()>::Http,
        body_kind == RequestBodyKind::None,
        config.access_log,
    );
    if let Err(rejection) = reject_oversized_request(&ctx.request, config.max_request_body_size) {
        write_simple_response(
            writer,
            config,
            rejection.status,
            rejection.headers,
            &[],
            true,
        )
        .await?;
        return Ok(ConnectionPersistence::Close);
    }

    let Some(admission) = try_acquire_request_admission(&ctx.connection.app) else {
        write_empty_response(
            writer,
            ctx.connection.config,
            status_code::SERVICE_UNAVAILABLE,
            true,
        )
        .await?;
        return Ok(ConnectionPersistence::Close);
    };
    let mut transport = H1HttpTransport::new(
        writer,
        ctx.connection.config,
        persistence == ConnectionPersistence::Close,
    );

    match input_plan {
        RequestInputPlan::None => {
            run_http_request(
                ctx,
                HttpRequestBody::NoBody,
                admission,
                &mut transport,
                || 0,
            )
            .await?;
            Ok(persistence)
        }
        RequestInputPlan::Stream { count_body_bytes } => {
            handle_streaming_http_request(
                ctx,
                body_kind,
                persistence,
                count_body_bytes,
                admission,
                &mut transport,
                H1BodyReadParts { reader, buffer },
            )
            .await
        }
    }
}

struct H1BodyReadParts<'a, R> {
    reader: &'a mut R,
    buffer: &'a mut BytesMut,
}

async fn handle_streaming_http_request<R, W>(
    ctx: RequestContext,
    body_kind: RequestBodyKind,
    persistence: ConnectionPersistence,
    count_body_bytes: bool,
    admission: RequestAdmission,
    transport: &mut H1HttpTransport<'_, W>,
    read_parts: H1BodyReadParts<'_, R>,
) -> Result<ConnectionPersistence, H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: H1WriteTarget,
{
    let config = ctx.connection.config;
    let H1BodyReadParts { reader, buffer } = read_parts;
    if let Some(body) = take_buffered_request_body(body_kind, buffer)? {
        let read_body_bytes = body.len() as u64;
        run_http_request(
            ctx,
            HttpRequestBody::Single(body),
            admission,
            transport,
            move || read_body_bytes,
        )
        .await?;
        return Ok(persistence);
    }

    let input = prepare_request_input(RequestInputPlan::Stream { count_body_bytes });
    let access_log_body_bytes = input.body_bytes_read.clone();
    let tx = input
        .tx
        .expect("streaming request inputs always allocate a send channel");
    let request_body_rx = input
        .rx
        .expect("streaming request inputs always allocate a receive channel");

    let result = {
        let mut app_future = Box::pin(run_http_request(
            ctx,
            HttpRequestBody::Stream(request_body_rx),
            admission,
            transport,
            move || {
                access_log_body_bytes
                    .as_ref()
                    .map_or(0, |bytes| bytes.load(Ordering::Relaxed))
            },
        ));
        let body_future = read_request_body(
            body_kind,
            reader,
            buffer,
            &tx,
            RequestBodyState::new(
                match body_kind {
                    RequestBodyKind::ContentLength(len) => Some(len.get()),
                    RequestBodyKind::Chunked | RequestBodyKind::None => None,
                },
                input.body_bytes_read,
                config.max_request_body_size.map(NonZeroU64::get),
            ),
            config.timeout_request_body_idle,
        );
        tokio::try_join!(app_future.as_mut(), body_future)
    };
    match result {
        Ok(((), ())) => Ok(persistence),
        Err(H2CornError::Http1(Http1Error::RequestBodyLimitExceeded))
            if transport.response_log_state().status.is_none() =>
        {
            transport
                .write_empty_response(status_code::PAYLOAD_TOO_LARGE, true)
                .await?;
            Ok(ConnectionPersistence::Close)
        }
        Err(err) => Err(err),
    }
}

fn take_buffered_request_body(
    body_kind: RequestBodyKind,
    buffer: &mut BytesMut,
) -> Result<Option<Bytes>, H2CornError> {
    let RequestBodyKind::ContentLength(len) = body_kind else {
        return Ok(None);
    };
    let len = usize::try_from(len.get()).map_err(|_| Http1Error::RequestBodyTooLarge)?;
    if buffer.len() < len {
        return Ok(None);
    }
    Ok(Some(buffer.split_to(len).freeze()))
}

async fn read_request_body<R>(
    body_kind: RequestBodyKind,
    reader: &mut R,
    buffer: &mut BytesMut,
    tx: &mpsc::Sender<StreamInput>,
    mut body: RequestBodyState,
    timeout_request_body_idle: Option<std::time::Duration>,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    match body_kind {
        RequestBodyKind::ContentLength(len) => {
            parse::read_fixed_body(
                reader,
                buffer,
                len.get(),
                tx,
                &mut body,
                timeout_request_body_idle,
            )
            .await?;
        }
        RequestBodyKind::Chunked => {
            parse::read_chunked_body(reader, buffer, tx, &mut body, timeout_request_body_idle)
                .await?;
        }
        RequestBodyKind::None => {}
    }
    send_best_effort(tx, StreamInput::EndStream).await;
    Ok(())
}
