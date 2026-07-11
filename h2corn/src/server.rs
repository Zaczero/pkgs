#[cfg(all(test, unix))]
#[path = "server_tests.rs"]
mod tests;

use std::future::{Future, poll_fn};
use std::io;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
#[cfg(unix)]
use std::os::fd::{FromRawFd, OwnedFd};
#[cfg(unix)]
use std::os::unix::net::UnixListener as StdUnixListener;
#[cfg(windows)]
use std::os::windows::io::{FromRawSocket, OwnedSocket};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Buf, BytesMut};
use pyo3::prelude::*;
#[cfg(target_os = "linux")]
use rustix::net::sockopt::set_tcp_quickack;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf, ReadHalf, WriteHalf, split};
use tokio::net::{TcpListener, TcpStream};
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

use crate::config::{BindTarget, ServerConfig};
use crate::error::{ErrorExt, ErrorKind, H2CornError, H2Error, ProxyError};
use crate::h2_frame::{self, ErrorCode, FrameReader};
use crate::proxy_protocol::{
    ConnectionInfo, ConnectionPeer, ConnectionStart, DetectedProtocol, ProxyInfo,
    ProxyProtocolMode, ServerAddr, TrustedPeer, peer_is_trusted, read_h2_preface,
    read_preamble_protocol, read_proxy_v1, read_proxy_v2,
};
use crate::pyloop::{PumpEvent, Shard, TaskSlot};
use crate::runtime::{AppState, ConnectionContext, ShutdownKind, ShutdownState};
use crate::sendfile::WriteTarget;
use crate::{h1, h2, tls};

pub(crate) type ListenerFd = OwnedFd;
#[cfg(windows)]
pub(crate) type ListenerFd = OwnedSocket;
type TlsWriteHalf = WriteHalf<TlsStream<PrefixedIo>>;
type TlsReadHalf = ReadHalf<TlsStream<PrefixedIo>>;
type NegotiatedTlsConnection = (
    Option<ProxyInfo>,
    DetectedProtocol,
    FrameReader<TlsReadHalf>,
    TlsWriteHalf,
);

struct PrefixedIo {
    stream: TcpStream,
    prefix: Option<BytesMut>,
}

struct ConnectionArgs {
    app: AppState,
    config: Arc<ServerConfig>,
    actual_peer: ConnectionPeer,
    actual_server: Option<ServerAddr>,
    shutdown: watch::Receiver<ShutdownState>,
    preamble: ConnectionPreamble,
    http1: bool,
}

impl PrefixedIo {
    fn new(stream: TcpStream, prefix: BytesMut) -> Self {
        let prefix = (!prefix.is_empty()).then_some(prefix);
        Self { stream, prefix }
    }
}

impl AsyncRead for PrefixedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(prefix) = self.prefix.as_mut() {
            let len = buf.remaining().min(prefix.len());
            buf.put_slice(&prefix[..len]);
            prefix.advance(len);
            if prefix.is_empty() {
                self.prefix = None;
            }
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for PrefixedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

impl WriteTarget for TlsWriteHalf {
    const SUPPORTS_SENDFILE: bool = false;
}

enum ListenerSource {
    Tcp(TcpListener),
    #[cfg(unix)]
    Unix {
        listener: UnixListener,
        path: Option<Arc<str>>,
    },
}

enum AcceptedConnection {
    Tcp(TcpStream, SocketAddr),
    #[cfg(unix)]
    Unix {
        stream: UnixStream,
        path: Option<Arc<str>>,
    },
}

impl ListenerSource {
    fn poll_accept_item(&self, cx: &mut Context<'_>) -> Poll<io::Result<AcceptedConnection>> {
        match self {
            Self::Tcp(listener) => match listener.poll_accept(cx) {
                Poll::Ready(Ok((stream, peer))) => {
                    Poll::Ready(Ok(AcceptedConnection::Tcp(stream, peer)))
                },
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                Poll::Pending => Poll::Pending,
            },
            #[cfg(unix)]
            Self::Unix { listener, path } => match listener.poll_accept(cx) {
                Poll::Ready(Ok((stream, _))) => Poll::Ready(Ok(AcceptedConnection::Unix {
                    stream,
                    path: path.clone(),
                })),
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

#[derive(Clone, Copy)]
enum ConnectionPreamble {
    Off,
    V1,
    V2,
}

impl ConnectionPreamble {
    const fn from_mode(mode: ProxyProtocolMode) -> Self {
        match mode {
            ProxyProtocolMode::Off => Self::Off,
            ProxyProtocolMode::V1 => Self::V1,
            ProxyProtocolMode::V2 => Self::V2,
        }
    }

    async fn read_proxy<R>(
        self,
        reader: &mut FrameReader<R>,
        actual_peer: &ConnectionPeer,
        trusted: &[TrustedPeer],
    ) -> Result<Option<ProxyInfo>, H2CornError>
    where
        R: AsyncRead + Unpin + Send,
    {
        match self {
            Self::Off => Ok(None),
            Self::V1 => read_proxy_v1(reader, actual_peer, trusted).await,
            Self::V2 => read_proxy_v2(reader, actual_peer, trusted).await,
        }
    }
}

pub(crate) fn own_listener_fds(fds: Vec<i64>) -> Box<[ListenerFd]> {
    fds.into_iter()
        .map(|fd| {
            #[cfg(unix)]
            // SAFETY: Python transfers each socket exactly once with
            // `socket.detach()` before entering Rust.
            return unsafe { OwnedFd::from_raw_fd(fd as i32) };
            #[cfg(windows)]
            // SAFETY: same transfer invariant as the Unix branch.
            return unsafe { OwnedSocket::from_raw_socket(fd as usize) };
        })
        .collect()
}

pub(crate) async fn serve_from_fds(
    app: AppState,
    fds: Box<[ListenerFd]>,
    config: Arc<ServerConfig>,
    shutdown_trigger: Py<PyAny>,
) -> Result<(), H2CornError> {
    let listeners = adopt_listeners(&config.binds, fds)?;
    serve_listeners(listeners, app, config, shutdown_trigger).await
}

#[cfg(unix)]
fn adopt_unix_listener(fd: ListenerFd) -> io::Result<UnixListener> {
    let listener = StdUnixListener::from(fd);
    UnixListener::from_std(listener)
}

#[cfg(windows)]
fn adopt_tcp_listener(fd: ListenerFd) -> io::Result<TcpListener> {
    let listener = StdTcpListener::from(fd);
    TcpListener::from_std(listener)
}

#[cfg(unix)]
fn adopt_tcp_listener(fd: ListenerFd) -> io::Result<TcpListener> {
    let listener = StdTcpListener::from(fd);
    TcpListener::from_std(listener)
}

fn configure_tcp_stream(stream: &TcpStream) {
    let _ = stream.set_nodelay(true);
    #[cfg(target_os = "linux")]
    let _ = set_tcp_quickack(stream, true);
}

fn spawn_connection(
    tasks: &mut JoinSet<()>,
    app: AppState,
    config: Arc<ServerConfig>,
    accepted: AcceptedConnection,
    shutdown: watch::Receiver<ShutdownState>,
    preamble: ConnectionPreamble,
    http1: bool,
) {
    match accepted {
        AcceptedConnection::Tcp(stream, peer) => {
            configure_tcp_stream(&stream);
            let actual_server = stream.local_addr().ok().map(|addr| ServerAddr {
                host: addr.ip().to_string().into(),
                port: Some(addr.port()),
            });
            if let Some(acceptor) = config.tls.as_ref().map(|tls| tls.acceptor.clone()) {
                tasks.spawn(async move {
                    let _ = serve_tls_connection(stream, acceptor, ConnectionArgs {
                        app,
                        config,
                        actual_peer: ConnectionPeer::Tcp(peer),
                        actual_server,
                        shutdown,
                        preamble,
                        http1,
                    })
                    .await;
                });
            } else {
                let (reader, writer) = stream.into_split();
                tasks.spawn(async move {
                    let _ = serve_connection(reader, writer, ConnectionArgs {
                        app,
                        config,
                        actual_peer: ConnectionPeer::Tcp(peer),
                        actual_server,
                        shutdown,
                        preamble,
                        http1,
                    })
                    .await;
                });
            }
        },
        #[cfg(unix)]
        AcceptedConnection::Unix { stream, path } => {
            debug_assert!(
                config.tls.is_none(),
                "TLS listener mode only supports TCP listeners"
            );
            if config.tls.is_some() {
                return;
            }
            let actual_server = path.map(|path| ServerAddr {
                host: Box::from(path.as_ref()),
                port: None,
            });
            let (reader, writer) = stream.into_split();
            tasks.spawn(async move {
                let _ = serve_connection(reader, writer, ConnectionArgs {
                    app,
                    config,
                    actual_peer: ConnectionPeer::Unix,
                    actual_server,
                    shutdown,
                    preamble,
                    http1,
                })
                .await;
            });
        },
    }
}

async fn accept_one(
    listeners: &[ListenerSource],
    start_index: usize,
) -> io::Result<(usize, AcceptedConnection)> {
    poll_fn(|cx| {
        for offset in 0..listeners.len() {
            let index = (start_index + offset) % listeners.len();
            match listeners[index].poll_accept_item(cx) {
                Poll::Ready(Ok(connection)) => {
                    return Poll::Ready(Ok(((index + 1) % listeners.len(), connection)));
                },
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Pending => {},
            }
        }
        Poll::Pending
    })
    .await
}

async fn serve_listeners(
    listeners: Box<[ListenerSource]>,
    app: AppState,
    config: Arc<ServerConfig>,
    shutdown_trigger: Py<PyAny>,
) -> Result<(), H2CornError> {
    let preamble = ConnectionPreamble::from_mode(config.proxy.protocol);
    let http1 = config.http1.enabled;
    let mut accept_start = 0;
    let shutdown = shutdown_future(shutdown_trigger, app.main_shard());
    tokio::pin!(shutdown);
    let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownState::Running);
    let mut tasks = JoinSet::new();
    let mut shutting_down = false;

    loop {
        if shutting_down {
            if tasks.join_next().await.is_some() {
                continue;
            }
            break;
        }

        let (next_accept_start, accepted) = tokio::select! {
            shutdown_kind = &mut shutdown => {
                shutting_down = true;
                let _ = shutdown_tx.send(ShutdownState::Graceful(shutdown_kind));
                continue;
            }
            accepted = accept_one(&listeners, accept_start),
                if config.limit_connections.is_none_or(|limit| tasks.len() < limit.get()) => accepted?,
            joined = tasks.join_next(), if !tasks.is_empty() => {
                let _ = joined;
                continue;
            }
        };
        accept_start = next_accept_start;

        spawn_connection(
            &mut tasks,
            Arc::clone(&app),
            Arc::clone(&config),
            accepted,
            shutdown_rx.clone(),
            preamble,
            http1,
        );
    }

    Ok(())
}

fn adopt_listeners(
    binds: &[BindTarget],
    fds: Box<[ListenerFd]>,
) -> io::Result<Box<[ListenerSource]>> {
    adopt_all(binds, fds, |bind, fd| match bind {
        BindTarget::Tcp { .. } => Ok(ListenerSource::Tcp(adopt_tcp_listener(fd)?)),
        #[cfg(unix)]
        BindTarget::Unix { path } => Ok(ListenerSource::Unix {
            listener: adopt_unix_listener(fd)?,
            path: Some(Arc::from(path.as_ref())),
        }),
        #[cfg(unix)]
        BindTarget::Fd { is_unix: true, .. } => Ok(ListenerSource::Unix {
            listener: adopt_unix_listener(fd)?,
            path: None,
        }),
        BindTarget::Fd { is_unix: false, .. } => Ok(ListenerSource::Tcp(adopt_tcp_listener(fd)?)),
        #[cfg(not(unix))]
        BindTarget::Unix { .. } | BindTarget::Fd { is_unix: true, .. } => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unix listeners are not supported on this platform",
        )),
    })
}

fn adopt_all<T>(
    binds: &[BindTarget],
    fds: Box<[ListenerFd]>,
    mut adopt: impl FnMut(&BindTarget, ListenerFd) -> io::Result<T>,
) -> io::Result<Box<[T]>> {
    if binds.len() != fds.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "listener FD count does not match configured binds",
        ));
    }
    let mut listeners = Vec::with_capacity(binds.len());
    for (bind, fd) in binds.iter().zip(fds.into_vec()) {
        listeners.push(adopt(bind, fd)?);
    }
    Ok(listeners.into_boxed_slice())
}

async fn serve_connection<R, W>(
    reader: R,
    mut writer: W,
    args: ConnectionArgs,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + WriteTarget,
{
    let ConnectionArgs {
        app,
        config,
        actual_peer,
        actual_server,
        shutdown,
        preamble,
        http1,
    } = args;
    let mut reader = FrameReader::new(reader);
    let proxy_headers_trusted =
        config.proxy.trust_headers && peer_is_trusted(&config.proxy.trusted_peers, &actual_peer);
    let mut info = ConnectionInfo::from_peer(actual_peer, actual_server, proxy_headers_trusted);

    let connection_start = timeout(config.timeout_handshake, async {
        let proxy = preamble
            .read_proxy(&mut reader, &actual_peer, &config.proxy.trusted_peers)
            .await?;
        let protocol = read_preamble_protocol(&mut reader, http1).await?;
        Ok::<_, H2CornError>(ConnectionStart { proxy, protocol })
    })
    .await
    .map_err(|_| H2Error::ConnectionHandshakeTimedOut)?;
    let connection_start = match connection_start {
        Ok(start) => start,
        Err(err)
            if matches!(
                err.kind(),
                ErrorKind::Proxy(ProxyError::InvalidHttp2Preface)
            ) =>
        {
            write_invalid_h2_preface_goaway(&mut writer).await?;
            return ProxyError::InvalidHttp2Preface.err();
        },
        Err(err) => return Err(err),
    };
    if let Some(proxy) = connection_start.proxy {
        info.apply_proxy_info(proxy);
    }
    let info = Arc::new(info);
    let connection_ctx = ConnectionContext::new(app, config, info.clone(), shutdown.clone());

    serve_detected_connection(
        reader,
        writer,
        connection_start.protocol,
        connection_ctx,
        false,
        shutdown,
    )
    .await
}

async fn serve_tls_connection(
    stream: TcpStream,
    acceptor: TlsAcceptor,
    args: ConnectionArgs,
) -> Result<(), H2CornError> {
    let ConnectionArgs {
        app,
        config,
        actual_peer,
        actual_server,
        shutdown,
        preamble,
        http1,
    } = args;
    let proxy_headers_trusted =
        config.proxy.trust_headers && peer_is_trusted(&config.proxy.trusted_peers, &actual_peer);
    let mut info = ConnectionInfo::from_peer(actual_peer, actual_server, proxy_headers_trusted);

    let negotiation: Pin<
        Box<dyn Future<Output = Result<Option<NegotiatedTlsConnection>, H2CornError>> + Send + '_>,
    > = Box::pin(negotiate_tls_connection(
        stream,
        acceptor,
        &config,
        actual_peer,
        preamble,
        http1,
    ));
    let connection_start = timeout(config.timeout_handshake, negotiation)
        .await
        .map_err(|_| H2Error::ConnectionHandshakeTimedOut)?;
    let Some((proxy, protocol, reader, writer)) = connection_start? else {
        return Ok(());
    };
    if let Some(proxy) = proxy {
        info.apply_proxy_info(proxy);
    }
    let info = Arc::new(info);
    let connection_ctx = ConnectionContext::new(app, config, info.clone(), shutdown.clone());

    serve_detected_connection(reader, writer, protocol, connection_ctx, true, shutdown).await
}

/// TLS negotiation is a cold, once-per-connection phase with a large rustls
/// state machine. Keep it behind one connection-scoped box so the long-lived
/// connection task and its hot poll frame retain only a pointer after accept.
async fn negotiate_tls_connection(
    stream: TcpStream,
    acceptor: TlsAcceptor,
    config: &ServerConfig,
    actual_peer: ConnectionPeer,
    preamble: ConnectionPreamble,
    http1: bool,
) -> Result<Option<NegotiatedTlsConnection>, H2CornError> {
    let mut reader = FrameReader::with_buffer(stream, BytesMut::new());
    let proxy = preamble
        .read_proxy(&mut reader, &actual_peer, &config.proxy.trusted_peers)
        .await?;
    let (stream, buffer) = reader.into_parts();
    let stream = PrefixedIo::new(stream, buffer);
    let tls_stream = acceptor.accept(stream).await?;
    let protocol = match tls_stream.get_ref().1.alpn_protocol() {
        Some(protocol) if protocol == tls::ALPN_H2 => DetectedProtocol::Http2,
        Some(protocol) if protocol == tls::ALPN_HTTP1 && http1 => DetectedProtocol::Http1,
        None if http1 => DetectedProtocol::Http1,
        _ => return Ok(None),
    };
    let (reader, writer) = split(tls_stream);
    let mut reader = FrameReader::new(reader);
    if protocol == DetectedProtocol::Http2 {
        read_h2_preface(&mut reader).await?;
    }
    Ok(Some((proxy, protocol, reader, writer)))
}

async fn serve_detected_connection<R, W>(
    reader: FrameReader<R>,
    writer: W,
    protocol: DetectedProtocol,
    connection_ctx: ConnectionContext,
    secure: bool,
    shutdown: watch::Receiver<ShutdownState>,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + WriteTarget,
{
    match protocol {
        DetectedProtocol::Http2 => {
            h2::serve_connection(reader, writer, connection_ctx, secure, shutdown).await
        },
        DetectedProtocol::Http1 => {
            let (reader, buffer) = reader.into_parts();
            h1::serve_connection(reader, buffer, writer, connection_ctx, secure, shutdown).await
        },
    }
}

async fn write_invalid_h2_preface_goaway<W>(writer: &mut W) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    let mut frame = bytes::BytesMut::with_capacity(h2_frame::GOAWAY_FRAME_PREFIX_LEN);
    h2_frame::append_goaway(&mut frame, None, ErrorCode::PROTOCOL_ERROR, b"");
    writer.write_all(frame.as_ref()).await?;
    writer.flush().await?;
    Ok(())
}

async fn shutdown_future(trigger: Py<PyAny>, shard: Shard) -> ShutdownKind {
    let slot = TaskSlot::new();
    shard.push(PumpEvent::SpawnAwaitable {
        awaitable: trigger,
        slot: Arc::clone(&slot),
    });
    if let Ok(value) = slot.wait(shard).await {
        return Python::attach(|py| {
            value
                .bind(py)
                .extract::<&str>()
                .ok()
                .and_then(ShutdownKind::from_wire)
                .unwrap_or_default()
        });
    }
    ShutdownKind::default()
}
