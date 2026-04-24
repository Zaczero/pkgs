#[cfg(unix)]
use std::os::unix::net::UnixListener as StdUnixListener;
use std::{
    future::{Future, poll_fn, ready},
    io,
    net::{SocketAddr, TcpListener as StdTcpListener},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::{Buf, BytesMut};
use pyo3::prelude::*;
use pyo3_async_runtimes::TaskLocals;
#[cfg(target_os = "linux")]
use rustix::net::sockopt::set_tcp_quickack;
use smallvec::SmallVec;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufWriter, ReadBuf, WriteHalf, copy, split};
use tokio::net::{TcpListener, TcpStream};
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

use crate::config::{BindTarget, ServerConfig};
use crate::error::{ErrorExt, H2CornError, H2Error, ProxyError};
use crate::frame::{self, ErrorCode, FrameReader};
use crate::h1::{self, H1WriteTarget};
use crate::h2::{self, H2WriteTarget};
use crate::proxy::{
    ConnectionInfo, ConnectionPeer, ConnectionStart, DetectedProtocol, ProxyInfo,
    ProxyProtocolMode, ServerAddr, TrustedPeer, peer_is_trusted, read_h2_preface,
    read_preamble_protocol, read_proxy_v1, read_proxy_v2,
};
use crate::runtime::{AppState, ConnectionContext, ShutdownKind, ShutdownState};
use crate::tls;

type TlsWriteHalf = WriteHalf<TlsStream<PrefixedIo>>;

struct PrefixedIo {
    stream: TcpStream,
    prefix: Option<BytesMut>,
}

impl PrefixedIo {
    fn new(stream: TcpStream, prefix: BytesMut) -> Self {
        Self {
            stream,
            prefix: Some(prefix),
        }
    }
}

impl AsyncRead for PrefixedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(prefix) = self.prefix.as_mut() {
            if !prefix.is_empty() {
                let len = buf.remaining().min(prefix.len());
                buf.put_slice(&prefix[..len]);
                prefix.advance(len);
                if prefix.is_empty() {
                    self.prefix = None;
                }
                return Poll::Ready(Ok(()));
            }
            self.prefix = None;
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

impl H1WriteTarget for TlsWriteHalf {
    async fn send_file_body(
        writer: &mut BufWriter<Self>,
        file: &mut File,
        _len: usize,
    ) -> io::Result<()> {
        writer.flush().await?;
        copy(file, writer.get_mut()).await?;
        Ok(())
    }
}

impl H2WriteTarget for TlsWriteHalf {
    const SUPPORTS_SENDFILE: bool = false;

    async fn write_file_chunk(
        _writer: &mut BufWriter<Self>,
        _header: [u8; 9],
        _file: &mut File,
        _offset: &mut u64,
        _len: usize,
    ) -> io::Result<()> {
        unreachable!("TLS H2 writer does not use direct sendfile")
    }
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
                }
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

trait ConnectionPreamble {
    fn read_proxy<R>(
        reader: &mut FrameReader<R>,
        actual_peer: &ConnectionPeer,
        trusted: &[TrustedPeer],
    ) -> impl Future<Output = Result<Option<ProxyInfo>, H2CornError>> + Send
    where
        R: AsyncRead + Unpin + Send;
}

struct PreambleOff;
struct PreambleV1;
struct PreambleV2;

impl ConnectionPreamble for PreambleOff {
    fn read_proxy<R>(
        _: &mut FrameReader<R>,
        _: &ConnectionPeer,
        _: &[TrustedPeer],
    ) -> impl Future<Output = Result<Option<ProxyInfo>, H2CornError>> + Send
    where
        R: AsyncRead + Unpin + Send,
    {
        ready(Ok(None))
    }
}

impl ConnectionPreamble for PreambleV1 {
    fn read_proxy<R>(
        reader: &mut FrameReader<R>,
        actual_peer: &ConnectionPeer,
        trusted: &[TrustedPeer],
    ) -> impl Future<Output = Result<Option<ProxyInfo>, H2CornError>> + Send
    where
        R: AsyncRead + Unpin + Send,
    {
        read_proxy_v1(reader, actual_peer, trusted)
    }
}

impl ConnectionPreamble for PreambleV2 {
    fn read_proxy<R>(
        reader: &mut FrameReader<R>,
        actual_peer: &ConnectionPeer,
        trusted: &[TrustedPeer],
    ) -> impl Future<Output = Result<Option<ProxyInfo>, H2CornError>> + Send
    where
        R: AsyncRead + Unpin + Send,
    {
        read_proxy_v2(reader, actual_peer, trusted)
    }
}

macro_rules! serve_with_proxy_protocol {
    (
        $mode:expr,
        $serve:ident,
        $listener:expr,
        $app:expr,
        $config:expr,
        $shutdown:expr,
        $http1:ident,
        $tls:ident
    ) => {
        match $mode {
            ProxyProtocolMode::Off => {
                $serve::<PreambleOff, { $http1 }, { $tls }>($listener, $app, $config, $shutdown)
                    .await
            }
            ProxyProtocolMode::V1 => {
                $serve::<PreambleV1, { $http1 }, { $tls }>($listener, $app, $config, $shutdown)
                    .await
            }
            ProxyProtocolMode::V2 => {
                $serve::<PreambleV2, { $http1 }, { $tls }>($listener, $app, $config, $shutdown)
                    .await
            }
        }
    };
}

pub(crate) async fn serve_from_fds(
    app: AppState,
    fds: Box<[i64]>,
    config: &'static ServerConfig,
    shutdown_trigger: Py<PyAny>,
) -> Result<(), H2CornError> {
    let listeners = adopt_listeners(&config.binds, &fds)?;
    match (config.http1.enabled, config.tls.is_some()) {
        (true, true) => serve_with_proxy_protocol!(
            config.proxy.protocol,
            serve_listeners,
            listeners,
            app,
            config,
            shutdown_trigger,
            true,
            true
        ),
        (true, false) => serve_with_proxy_protocol!(
            config.proxy.protocol,
            serve_listeners,
            listeners,
            app,
            config,
            shutdown_trigger,
            true,
            false
        ),
        (false, true) => serve_with_proxy_protocol!(
            config.proxy.protocol,
            serve_listeners,
            listeners,
            app,
            config,
            shutdown_trigger,
            false,
            true
        ),
        (false, false) => serve_with_proxy_protocol!(
            config.proxy.protocol,
            serve_listeners,
            listeners,
            app,
            config,
            shutdown_trigger,
            false,
            false
        ),
    }
}

#[cfg(unix)]
fn adopt_unix_listener(fd: i64) -> io::Result<UnixListener> {
    use std::os::fd::{FromRawFd, RawFd};

    // SAFETY: `fd` comes from the Python socket builder and ownership is
    // transferred exactly once into this function for listener adoption.
    let listener = unsafe { StdUnixListener::from_raw_fd(fd as RawFd) };
    UnixListener::from_std(listener)
}

#[cfg(windows)]
fn adopt_tcp_listener(fd: i64) -> io::Result<TcpListener> {
    use std::os::windows::io::{FromRawSocket, RawSocket};

    // SAFETY: `fd` comes from the Python socket builder and ownership is
    // transferred exactly once into this function for listener adoption.
    let listener = unsafe { StdTcpListener::from_raw_socket(fd as RawSocket) };
    TcpListener::from_std(listener)
}

#[cfg(unix)]
fn adopt_tcp_listener(fd: i64) -> io::Result<TcpListener> {
    use std::os::fd::{FromRawFd, RawFd};

    // SAFETY: `fd` comes from the Python socket builder and ownership is
    // transferred exactly once into this function for listener adoption.
    let listener = unsafe { StdTcpListener::from_raw_fd(fd as RawFd) };
    TcpListener::from_std(listener)
}

fn configure_tcp_stream(stream: &TcpStream) {
    let _ = stream.set_nodelay(true);
    #[cfg(target_os = "linux")]
    let _ = set_tcp_quickack(stream, true);
}

fn spawn_connection<P, const HTTP1: bool, const TLS: bool>(
    tasks: &mut JoinSet<()>,
    app: &AppState,
    config: &'static ServerConfig,
    accepted: AcceptedConnection,
    shutdown: watch::Receiver<ShutdownState>,
) where
    P: ConnectionPreamble,
{
    match accepted {
        AcceptedConnection::Tcp(stream, peer) => {
            configure_tcp_stream(&stream);
            let actual_server = stream.local_addr().ok().map(|addr| ServerAddr {
                host: addr.ip().to_string().into(),
                port: Some(addr.port()),
            });
            let app = Arc::clone(app);
            if TLS {
                let acceptor = config
                    .tls
                    .as_ref()
                    .expect("TLS listener mode requires TLS config")
                    .acceptor
                    .clone();
                tasks.spawn(async move {
                    let _ = serve_tls_connection::<P, HTTP1>(
                        stream,
                        acceptor,
                        app,
                        config,
                        ConnectionPeer::Tcp(peer),
                        actual_server,
                        shutdown,
                    )
                    .await;
                });
            } else {
                let (reader, writer) = stream.into_split();
                tasks.spawn(async move {
                    let _ = serve_connection::<_, _, P, HTTP1>(
                        reader,
                        writer,
                        app,
                        config,
                        ConnectionPeer::Tcp(peer),
                        actual_server,
                        shutdown,
                    )
                    .await;
                });
            }
        }
        #[cfg(unix)]
        AcceptedConnection::Unix { stream, path } => {
            debug_assert!(!TLS, "TLS listener mode only supports TCP listeners");
            if TLS {
                return;
            }
            let actual_server = path.map(|path| ServerAddr {
                host: Box::from(path.as_ref()),
                port: None,
            });
            let (reader, writer) = stream.into_split();
            let app = Arc::clone(app);
            tasks.spawn(async move {
                let _ = serve_connection::<_, _, P, HTTP1>(
                    reader,
                    writer,
                    app,
                    config,
                    ConnectionPeer::Unix,
                    actual_server,
                    shutdown,
                )
                .await;
            });
        }
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
                }
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Pending => {}
            }
        }
        Poll::Pending
    })
    .await
}

async fn serve_listeners<P, const HTTP1: bool, const TLS: bool>(
    listeners: Box<[ListenerSource]>,
    app: AppState,
    config: &'static ServerConfig,
    shutdown_trigger: Py<PyAny>,
) -> Result<(), H2CornError>
where
    P: ConnectionPreamble,
{
    let mut accept_start = 0;
    let shutdown = shutdown_future(shutdown_trigger, app.locals.clone());
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

        spawn_connection::<P, HTTP1, TLS>(&mut tasks, &app, config, accepted, shutdown_rx.clone());
    }

    Ok(())
}

fn adopt_listeners(binds: &[BindTarget], fds: &[i64]) -> io::Result<Box<[ListenerSource]>> {
    if binds.len() != fds.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "listener FD count does not match configured binds",
        ));
    }
    let mut listeners = SmallVec::<[ListenerSource; 4]>::new();
    for (bind, &fd) in binds.iter().zip(fds.iter()) {
        match bind {
            BindTarget::Tcp { .. } => listeners.push(ListenerSource::Tcp(adopt_tcp_listener(fd)?)),
            #[cfg(unix)]
            BindTarget::Unix { path } => listeners.push(ListenerSource::Unix {
                listener: adopt_unix_listener(fd)?,
                path: Some(Arc::from(path.as_ref())),
            }),
            #[cfg(unix)]
            BindTarget::Fd { is_unix: true, .. } => listeners.push(ListenerSource::Unix {
                listener: adopt_unix_listener(fd)?,
                path: None,
            }),
            BindTarget::Fd { is_unix: false, .. } => {
                listeners.push(ListenerSource::Tcp(adopt_tcp_listener(fd)?));
            }
            #[cfg(not(unix))]
            BindTarget::Unix { .. } | BindTarget::Fd { is_unix: true, .. } => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "unix listeners are not supported on this platform",
                ));
            }
        }
    }
    Ok(listeners.into_vec().into_boxed_slice())
}

async fn serve_connection<R, W, P, const HTTP1: bool>(
    reader: R,
    mut writer: W,
    app: AppState,
    config: &'static ServerConfig,
    actual_peer: ConnectionPeer,
    actual_server: Option<ServerAddr>,
    shutdown: watch::Receiver<ShutdownState>,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + H1WriteTarget + H2WriteTarget,
    P: ConnectionPreamble,
{
    let mut reader = FrameReader::new(reader);
    let proxy_headers_trusted =
        config.proxy.trust_headers && peer_is_trusted(&config.proxy.trusted_peers, &actual_peer);
    let mut info = ConnectionInfo::from_peer(actual_peer, actual_server, proxy_headers_trusted);

    let connection_start = timeout(config.timeout_handshake, async {
        let proxy = P::read_proxy(&mut reader, &actual_peer, &config.proxy.trusted_peers).await?;
        let protocol = read_preamble_protocol::<R, HTTP1>(&mut reader).await?;
        Ok::<_, H2CornError>(ConnectionStart { proxy, protocol })
    })
    .await
    .map_err(|_| H2Error::ConnectionHandshakeTimedOut)?;
    let connection_start = match connection_start {
        Ok(start) => start,
        Err(H2CornError::Proxy(ProxyError::InvalidHttp2Preface)) => {
            write_invalid_h2_preface_goaway(&mut writer).await?;
            return ProxyError::InvalidHttp2Preface.err();
        }
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

async fn serve_tls_connection<P, const HTTP1: bool>(
    stream: TcpStream,
    acceptor: TlsAcceptor,
    app: AppState,
    config: &'static ServerConfig,
    actual_peer: ConnectionPeer,
    actual_server: Option<ServerAddr>,
    shutdown: watch::Receiver<ShutdownState>,
) -> Result<(), H2CornError>
where
    P: ConnectionPreamble,
{
    let proxy_headers_trusted =
        config.proxy.trust_headers && peer_is_trusted(&config.proxy.trusted_peers, &actual_peer);
    let mut info = ConnectionInfo::from_peer(actual_peer, actual_server, proxy_headers_trusted);

    let connection_start = timeout(config.timeout_handshake, async {
        let mut reader = FrameReader::with_buffer(stream, BytesMut::new());
        let proxy = P::read_proxy(&mut reader, &actual_peer, &config.proxy.trusted_peers).await?;
        let (stream, buffer) = reader.into_parts();
        let stream = PrefixedIo::new(stream, buffer);
        let tls_stream = acceptor.accept(stream).await?;
        let protocol = match tls_stream.get_ref().1.alpn_protocol() {
            Some(protocol) if protocol == tls::ALPN_H2 => DetectedProtocol::Http2,
            Some(protocol) if protocol == tls::ALPN_HTTP1 && HTTP1 => DetectedProtocol::Http1,
            None if HTTP1 => DetectedProtocol::Http1,
            _ => return Ok(None),
        };
        let (reader, writer) = split(tls_stream);
        let mut reader = FrameReader::new(reader);
        if protocol == DetectedProtocol::Http2 {
            read_h2_preface(&mut reader).await?;
        }
        Ok::<_, H2CornError>(Some((proxy, protocol, reader, writer)))
    })
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
    W: AsyncWrite + Unpin + Send + 'static + H1WriteTarget + H2WriteTarget,
{
    match protocol {
        DetectedProtocol::Http2 => {
            h2::serve_connection(reader, writer, connection_ctx, shutdown).await
        }
        DetectedProtocol::Http1 => {
            let (reader, buffer) = reader.into_parts();
            h1::serve_connection(reader, buffer, writer, connection_ctx, secure, shutdown).await
        }
    }
}

async fn write_invalid_h2_preface_goaway<W>(writer: &mut W) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    let mut frame = bytes::BytesMut::with_capacity(frame::GOAWAY_FRAME_PREFIX_LEN);
    frame::append_goaway(&mut frame, None, ErrorCode::PROTOCOL_ERROR, b"");
    writer.write_all(frame.as_ref()).await?;
    writer.flush().await?;
    Ok(())
}

async fn shutdown_future(trigger: Py<PyAny>, locals: TaskLocals) -> ShutdownKind {
    let awaitable = Python::attach(|py| {
        pyo3_async_runtimes::into_future_with_locals(&locals, trigger.into_bound(py))
    });
    if let Ok(future) = awaitable
        && let Ok(value) = future.await
    {
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
