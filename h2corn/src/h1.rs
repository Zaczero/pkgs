mod http;
mod parse;
mod websocket;

use std::future::Future;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncWriteExt as _, BufWriter};
use tokio::sync::{mpsc, watch};

use crate::async_util::send_best_effort;
use crate::bridge::RequestBodyCounter;
use crate::config::ServerConfig;
use crate::error::{ErrorKind, H2CornError, Http1Error};
use crate::h1::http::{
    H1HttpTransport, write_empty_response, write_h2c_upgrade_response, write_simple_response,
};
use crate::h1::parse::read_request;
use crate::h2::{UpgradedH2Request, serve_h2_upgraded_connection};
use crate::h2_frame::PeerSettings;
use crate::http::app::{HttpRequestBody, poll_app_task_once};
use crate::http::body::RequestBodyState;
use crate::http::execution::StreamRequestInput;
use crate::http::planner::{RejectedResponse, reject_before_launch};
use crate::http::run_request::run_http_request;
use crate::http::types::{HttpVersion, RequestHead, status_code};
use crate::runtime::{
    ConnectionContext, RequestAdmission, RequestContext, ShutdownState, StreamInput,
    try_acquire_request_admission,
};
use crate::sendfile::WriteTarget;
use crate::websocket::{WebSocketContext, WebSocketKey, WebSocketRequestMeta};

const H1_WRITER_BUFFER_CAPACITY: usize = 8 * 1024;

struct ParsedRequest {
    request: RequestHead,
    route: RequestRoute,
    persistence: ConnectionPersistence,
}

/// A parsed HTTP/1 request has one transport route. Keeping upgrades out of
/// the ordinary-body variant prevents a successful 101 from coexisting with
/// bytes the HTTP parser still owes to the application.
enum RequestRoute {
    Http(RequestBodyKind),
    Upgrade(UpgradeRequest),
}

impl RequestRoute {
    const fn body_kind(&self) -> Option<RequestBodyKind> {
        match self {
            Self::Http(body_kind) => Some(*body_kind),
            Self::Upgrade(_) => None,
        }
    }
}

enum UpgradeRequest {
    WebSocket {
        key: WebSocketKey,
        meta: Box<WebSocketRequestMeta>,
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

/// Body kind for requests that actually stream input — `RequestBodyKind`
/// minus `None`, so the body-reading path has no dead no-body arm.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StreamedBodyKind {
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
    shutdown: watch::Receiver<ShutdownState>,
}

struct H1Io<'a, R, W> {
    reader: &'a mut R,
    buffer: &'a mut BytesMut,
    writer: &'a mut BufWriter<W>,
}

struct H1BodyReadParts<'a, R> {
    reader: &'a mut R,
    buffer: &'a mut BytesMut,
}

pub(crate) fn serve_connection<R, W>(
    reader: R,
    buffer: BytesMut,
    writer: W,
    connection: ConnectionContext,
    shutdown: watch::Receiver<ShutdownState>,
) -> impl Future<Output = Result<(), H2CornError>>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: WriteTarget,
{
    // The connection state machine has several mutually exclusive, sizeable
    // protocol branches. Store it once per accepted connection instead of
    // embedding the full enum of async states in its caller (and therefore in
    // every Tokio task allocation and poll stack frame). The concrete future
    // remains statically dispatched; this is one allocation per connection,
    // never per request or per poll.
    Box::pin(drive_connection(
        reader, buffer, writer, connection, shutdown,
    ))
}

async fn drive_connection<R, W>(
    mut reader: R,
    mut buffer: BytesMut,
    writer: W,
    connection: ConnectionContext,
    shutdown: watch::Receiver<ShutdownState>,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: WriteTarget,
{
    let mut writer = BufWriter::with_capacity(H1_WRITER_BUFFER_CAPACITY, writer);
    let mut first_request = true;
    // Armed once per connection rather than once per request. Constructing
    // `changed()` inside the select registers a waiter on one of tokio's
    // sharded watch `Notify`s and deregisters it on drop -- two acquisitions
    // of a process-wide lock per keep-alive request, whose cost grows with
    // core count. The future is only re-armed after it actually fires.
    // A second handle, because holding the future borrows its receiver for the
    // future's whole life. Cloning preserves the seen-version, so the two
    // observe exactly the same edges.
    let mut shutdown_signal = shutdown.clone();
    let mut shutdown_changed = Box::pin(shutdown_signal.changed());

    loop {
        if shutdown.borrow().kind().is_some() {
            break;
        }

        let parsed = tokio::select! {
            changed = &mut shutdown_changed => {
                if changed.is_ok() && shutdown.borrow().kind().is_some() {
                    break;
                }
                // Dropping first ends the receiver borrow the old future
                // held, so the replacement can take a fresh one.
                drop(shutdown_changed);
                shutdown_changed = Box::pin(shutdown_signal.changed());
                continue;
            }
            parsed = read_request(
                &mut reader,
                &mut buffer,
                &mut writer,
                &connection.config,
                connection.security.h1_scheme(),
                request_head_timeout(&connection.config, first_request),
                first_request,
                connection.info.proxy_headers_trusted,
            ) => parsed,
        }?;

        let Some(parsed) = parsed else { break };
        first_request = false;
        let ParsedRequest {
            request,
            route,
            persistence,
        } = parsed;
        match route {
            RequestRoute::Http(body_kind) => {
                match handle_http_request(
                    RequestContext::new(Arc::clone(&connection), request),
                    &connection.config,
                    body_kind,
                    persistence,
                    H1Io {
                        reader: &mut reader,
                        buffer: &mut buffer,
                        writer: &mut writer,
                    },
                )
                .await
                {
                    Ok(ConnectionPersistence::Close) => break,
                    Ok(ConnectionPersistence::KeepAlive) => {},
                    Err(err) => {
                        crate::server::report_request_failure(Err(err));
                        break;
                    },
                }
            },
            RequestRoute::Upgrade(upgrade) => {
                return handle_upgrade_request(
                    request,
                    upgrade,
                    reader,
                    buffer,
                    writer,
                    H1UpgradeContext {
                        connection,
                        shutdown,
                    },
                )
                .await;
            },
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
    upgrade: UpgradeRequest,
    reader: R,
    buffer: BytesMut,
    mut writer: BufWriter<W>,
    context: H1UpgradeContext,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: WriteTarget,
{
    match upgrade {
        UpgradeRequest::WebSocket { key, meta } => {
            let Some(admission) = try_acquire_request_admission(&context.connection.app) else {
                write_empty_response(
                    &mut writer,
                    &context.connection.config,
                    status_code::SERVICE_UNAVAILABLE,
                    true,
                )
                .await?;
                return Ok(());
            };
            let result = Box::pin(websocket::handle_request(
                WebSocketContext {
                    request: RequestContext::new(context.connection, request),
                    admission,
                    meta: *meta,
                    shutdown: context.shutdown,
                },
                key,
                reader,
                buffer,
                writer,
            ))
            .await;
            match result {
                Ok(()) => Ok(()),
                Err(err) => {
                    crate::server::report_request_failure(Err(err));
                    Ok(())
                },
            }
        },
        UpgradeRequest::WebSocketUnsupportedVersion => {
            let response = RejectedResponse::unsupported_websocket_version();
            write_simple_response(
                &mut writer,
                &context.connection.config,
                response.status,
                response.headers,
                &[],
                true,
            )
            .await?;
            Ok(())
        },
        UpgradeRequest::WebSocketBadRequest => {
            write_empty_response(
                &mut writer,
                &context.connection.config,
                status_code::BAD_REQUEST,
                true,
            )
            .await?;
            Ok(())
        },
        UpgradeRequest::H2c { settings } => {
            Box::pin(serve_h2c_upgrade_request(
                request, settings, reader, buffer, writer, context,
            ))
            .await
        },
    }
}

async fn serve_h2c_upgrade_request<R, W>(
    mut request: RequestHead,
    settings: PeerSettings,
    reader: R,
    buffer: BytesMut,
    mut writer: BufWriter<W>,
    context: H1UpgradeContext,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: WriteTarget,
{
    request.http_version = HttpVersion::Http2;
    write_h2c_upgrade_response(&mut writer, &context.connection.config).await?;
    let H1UpgradeContext {
        connection,
        shutdown,
    } = context;
    serve_h2_upgraded_connection(
        reader,
        writer.into_inner(),
        connection,
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
    ctx: Box<RequestContext>,
    // Borrowed from the connection task's own handle, which outlives every
    // request on the connection. `ctx` is moved onward while the transport
    // still needs the config, and cloning the `Arc` was the shortcut around
    // that -- two atomics per request for a value that cannot go away.
    config: &ServerConfig,
    body_kind: RequestBodyKind,
    persistence: ConnectionPersistence,
    io: H1Io<'_, R, W>,
) -> Result<ConnectionPersistence, H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: WriteTarget,
{
    let H1Io {
        reader,
        buffer,
        writer,
    } = io;
    if let Err(rejection) = reject_before_launch(&ctx.request, config.max_request_body_size) {
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
            &ctx.connection.config,
            status_code::SERVICE_UNAVAILABLE,
            true,
        )
        .await?;
        return Ok(ConnectionPersistence::Close);
    };
    let mut transport =
        H1HttpTransport::new(writer, config, persistence == ConnectionPersistence::Close);

    let body_kind = match body_kind {
        RequestBodyKind::None => {
            run_http_request(
                ctx,
                HttpRequestBody::NoBody,
                admission,
                &mut transport,
                || 0,
            )
            .await?;
            return Ok(transport.persistence(persistence));
        },
        RequestBodyKind::ContentLength(len) => StreamedBodyKind::ContentLength(len),
        RequestBodyKind::Chunked => StreamedBodyKind::Chunked,
    };
    handle_streaming_http_request(
        ctx,
        config,
        body_kind,
        persistence,
        config.access_log,
        admission,
        &mut transport,
        H1BodyReadParts { reader, buffer },
    )
    .await
}

async fn handle_streaming_http_request<R, W>(
    ctx: Box<RequestContext>,
    config: &ServerConfig,
    body_kind: StreamedBodyKind,
    persistence: ConnectionPersistence,
    count_body_bytes: bool,
    admission: RequestAdmission,
    transport: &mut H1HttpTransport<'_, W>,
    read_parts: H1BodyReadParts<'_, R>,
) -> Result<ConnectionPersistence, H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: WriteTarget,
{
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
        return Ok(transport.persistence(persistence));
    }

    let (tx, request_body_rx, body_bytes_read, disconnect) =
        StreamRequestInput::new(count_body_bytes).into_parts();
    let access_log_body_bytes = body_bytes_read.clone();

    let result = {
        let mut app_future = Box::pin(run_http_request(
            ctx,
            HttpRequestBody::Stream {
                rx: request_body_rx,
                disconnect: Arc::clone(&disconnect),
            },
            admission,
            transport,
            move || {
                access_log_body_bytes
                    .as_ref()
                    .map_or(0, RequestBodyCounter::load)
            },
        ));
        // Start request ownership before polling a buffered body parser that
        // can reject synchronously. This is one poll, not a scheduler yield:
        // every admitted request invokes the app while body parsing and app
        // execution remain concurrent afterwards.
        let app_ready = poll_app_task_once(app_future.as_mut());
        let mut body_future = Box::pin(read_request_body(
            body_kind,
            reader,
            buffer,
            &tx,
            RequestBodyState::new(
                match body_kind {
                    StreamedBodyKind::ContentLength(len) => Some(len.get()),
                    StreamedBodyKind::Chunked => None,
                },
                body_bytes_read,
                config.max_request_body_size,
            ),
            config.timeout_request_body_idle,
            parse::FieldLimits {
                max_fields: config.limit_request_fields.map(NonZeroUsize::get),
                max_field_size: config.http1.limit_request_field_size.map(NonZeroUsize::get),
            },
        ));

        match app_ready {
            Poll::Ready(app_result) => {
                // A successfully completed response may still need the body
                // drained before this HTTP/1 connection is reusable. An app
                // failure is terminal: do not let a peer keep the failed
                // request task alive by withholding the rest of its upload.
                app_result?;
                body_future.as_mut().await.map(|()| ((), ()))
            },
            Poll::Pending => tokio::select! {
                app_result = app_future.as_mut() => {
                    app_result?;
                    body_future.as_mut().await.map(|()| ((), ()))
                }
                body_result = body_future.as_mut() => {
                    match body_result {
                        Ok(()) => app_future.as_mut().await.map(|()| ((), ())),
                        Err(err) => {
                            // Closing the sole producer makes an application
                            // already suspended in receive() observe
                            // `http.disconnect`. Wait only until that event has
                            // been queued to its loop, then dropping the app
                            // future queues cancellation behind the resolution.
                            // An app not awaiting receive is cancelled
                            // immediately; no peer-controlled grace timer or
                            // leaked connection task exists.
                            drop(body_future);
                            drop(tx);
                            disconnect.wait_app_started().await;
                            if let Some(pending) = disconnect.pending_resolution() {
                                pending.wait().await;
                            }
                            drop(app_future);
                            Err(err)
                        }
                    }
                }
            },
        }
    };
    match result {
        Ok(((), ())) => Ok(transport.persistence(persistence)),
        Err(err)
            if matches!(
                err.kind(),
                ErrorKind::Http1(Http1Error::RequestBodyLimitExceeded)
            ) && !transport.response_started() =>
        {
            transport
                .write_empty_response(status_code::PAYLOAD_TOO_LARGE, true)
                .await?;
            Ok(ConnectionPersistence::Close)
        },
        Err(err) => Err(err),
    }
}

fn take_buffered_request_body(
    body_kind: StreamedBodyKind,
    buffer: &mut BytesMut,
) -> Result<Option<Bytes>, H2CornError> {
    let StreamedBodyKind::ContentLength(len) = body_kind else {
        return Ok(None);
    };
    let len = usize::try_from(len.get()).map_err(|_| Http1Error::RequestBodyTooLarge)?;
    if buffer.len() < len {
        return Ok(None);
    }
    Ok(Some(buffer.split_to(len).freeze()))
}

async fn read_request_body<R>(
    body_kind: StreamedBodyKind,
    reader: &mut R,
    buffer: &mut BytesMut,
    tx: &mpsc::Sender<StreamInput>,
    mut body: RequestBodyState,
    timeout_request_body_idle: Option<Duration>,
    field_limits: parse::FieldLimits,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    match body_kind {
        StreamedBodyKind::ContentLength(len) => {
            parse::read_fixed_body(
                reader,
                buffer,
                len.get(),
                tx,
                &mut body,
                timeout_request_body_idle,
            )
            .await?;
        },
        StreamedBodyKind::Chunked => {
            parse::read_chunked_body(
                reader,
                buffer,
                tx,
                &mut body,
                timeout_request_body_idle,
                field_limits,
            )
            .await?;
        },
    }
    send_best_effort(tx, StreamInput::EndStream).await;
    Ok(())
}
