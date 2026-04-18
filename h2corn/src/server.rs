use std::{
    future::{Future, poll_fn, ready},
    io, net,
    sync::Arc,
    task::{Context, Poll},
};

use pyo3::prelude::*;
use pyo3_async_runtimes::TaskLocals;
#[cfg(target_os = "linux")]
use rustix::net::sockopt::set_tcp_quickack;
use smallvec::SmallVec;
use tokio::io::{AsyncRead, AsyncWrite};
#[cfg(unix)]
use tokio::net::unix::SocketAddr as UnixSocketAddr;
use tokio::net::{TcpListener, TcpStream};
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::timeout;

#[cfg(unix)]
use crate::config::ServerBind;
use crate::config::ServerConfig;
use crate::error::{H2CornError, H2Error};
use crate::frame::FrameReader;
use crate::h1::{self, H1WriteTarget};
use crate::h2::{self, H2WriteTarget};
use crate::proxy::{
    ConnectionInfo, ConnectionPeer, ConnectionStart, DetectedProtocol, ProxyInfo,
    ProxyProtocolMode, TrustedPeer, peer_is_trusted, read_preamble_protocol, read_proxy_v1,
    read_proxy_v2,
};
use crate::runtime::{AppState, ConnectionContext, ShutdownKind, ShutdownState};

trait AcceptSource {
    type Connection;

    fn poll_accept_item(&self, cx: &mut Context<'_>) -> Poll<io::Result<Self::Connection>>;
}

impl AcceptSource for TcpListener {
    type Connection = (TcpStream, net::SocketAddr);

    fn poll_accept_item(&self, cx: &mut Context<'_>) -> Poll<io::Result<Self::Connection>> {
        self.poll_accept(cx)
    }
}

#[cfg(unix)]
impl AcceptSource for UnixListener {
    type Connection = (UnixStream, UnixSocketAddr);

    fn poll_accept_item(&self, cx: &mut Context<'_>) -> Poll<io::Result<Self::Connection>> {
        self.poll_accept(cx)
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
    ($mode:expr, $serve:ident, $listener:expr, $app:expr, $config:expr, $shutdown:expr, $http1:ident) => {
        match $mode {
            ProxyProtocolMode::Off => {
                $serve::<PreambleOff, { $http1 }>($listener, $app, $config, $shutdown).await
            }
            ProxyProtocolMode::V1 => {
                $serve::<PreambleV1, { $http1 }>($listener, $app, $config, $shutdown).await
            }
            ProxyProtocolMode::V2 => {
                $serve::<PreambleV2, { $http1 }>($listener, $app, $config, $shutdown).await
            }
        }
    };
}

pub(crate) async fn serve_from_fd(
    app: AppState,
    fd: i64,
    config: &'static ServerConfig,
    shutdown_trigger: Py<PyAny>,
) -> Result<(), H2CornError> {
    #[cfg(unix)]
    if matches!(
        &config.bind,
        ServerBind::Unix { .. } | ServerBind::Fd { is_unix: true, .. }
    ) {
        return serve_unix(adopt_unix_listener(fd)?, app, config, shutdown_trigger).await;
    }

    serve_tcp(adopt_tcp_listener(fd)?, app, config, shutdown_trigger).await
}

async fn serve_tcp(
    listener: TcpListener,
    app: AppState,
    config: &'static ServerConfig,
    shutdown_trigger: Py<PyAny>,
) -> Result<(), H2CornError> {
    if config.http1.enabled {
        serve_with_proxy_protocol!(
            config.proxy.protocol,
            serve_tcp_with_preamble,
            listener,
            app,
            config,
            shutdown_trigger,
            true
        )
    } else {
        serve_with_proxy_protocol!(
            config.proxy.protocol,
            serve_tcp_with_preamble,
            listener,
            app,
            config,
            shutdown_trigger,
            false
        )
    }
}

#[cfg(unix)]
fn adopt_unix_listener(fd: i64) -> io::Result<UnixListener> {
    use std::os::fd::{FromRawFd, RawFd};

    // SAFETY: `fd` comes from the Python socket builder and ownership is
    // transferred exactly once into this function for listener adoption.
    let listener = unsafe { std::os::unix::net::UnixListener::from_raw_fd(fd as RawFd) };
    UnixListener::from_std(listener)
}

#[cfg(windows)]
fn adopt_tcp_listener(fd: i64) -> io::Result<TcpListener> {
    use std::os::windows::io::{FromRawSocket, RawSocket};

    // SAFETY: `fd` comes from the Python socket builder and ownership is
    // transferred exactly once into this function for listener adoption.
    let listener = unsafe { net::TcpListener::from_raw_socket(fd as RawSocket) };
    TcpListener::from_std(listener)
}

#[cfg(unix)]
fn adopt_tcp_listener(fd: i64) -> io::Result<TcpListener> {
    use std::os::fd::{FromRawFd, RawFd};

    // SAFETY: `fd` comes from the Python socket builder and ownership is
    // transferred exactly once into this function for listener adoption.
    let listener = unsafe { net::TcpListener::from_raw_fd(fd as RawFd) };
    TcpListener::from_std(listener)
}

fn configure_tcp_stream(stream: &TcpStream) {
    let _ = stream.set_nodelay(true);
    #[cfg(target_os = "linux")]
    let _ = set_tcp_quickack(stream, true);
}

fn spawn_tcp_connection<P, const HTTP1: bool>(
    tasks: &mut JoinSet<()>,
    app: &AppState,
    reader: tokio::net::tcp::OwnedReadHalf,
    writer: tokio::net::tcp::OwnedWriteHalf,
    config: &'static ServerConfig,
    actual_peer: ConnectionPeer,
    shutdown: watch::Receiver<ShutdownState>,
) where
    P: ConnectionPreamble,
{
    let app = Arc::clone(app);
    tasks.spawn(async move {
        let _ =
            serve_connection::<_, _, P, HTTP1>(reader, writer, app, config, actual_peer, shutdown)
                .await;
    });
}

#[cfg(unix)]
fn spawn_unix_connection<P, const HTTP1: bool>(
    tasks: &mut JoinSet<()>,
    app: &AppState,
    reader: tokio::net::unix::OwnedReadHalf,
    writer: tokio::net::unix::OwnedWriteHalf,
    config: &'static ServerConfig,
    shutdown: watch::Receiver<ShutdownState>,
) where
    P: ConnectionPreamble,
{
    let app = Arc::clone(app);
    tasks.spawn(async move {
        let _ = serve_connection::<_, _, P, HTTP1>(
            reader,
            writer,
            app,
            config,
            ConnectionPeer::Unix,
            shutdown,
        )
        .await;
    });
}

fn serve_tcp_with_preamble<P, const HTTP1: bool>(
    listener: TcpListener,
    app: AppState,
    config: &'static ServerConfig,
    shutdown_trigger: Py<PyAny>,
) -> impl Future<Output = Result<(), H2CornError>>
where
    P: ConnectionPreamble,
{
    serve_listener(
        listener,
        app.locals.clone(),
        shutdown_trigger,
        move |(stream, peer), shutdown, tasks| {
            configure_tcp_stream(&stream);
            let (reader, writer) = stream.into_split();
            spawn_tcp_connection::<P, HTTP1>(
                tasks,
                &app,
                reader,
                writer,
                config,
                ConnectionPeer::Tcp(peer),
                shutdown,
            );
        },
    )
}

async fn accept_batch<L>(listener: &L) -> io::Result<SmallVec<[L::Connection; 4]>>
where
    L: AcceptSource,
{
    poll_fn(|cx| {
        let mut accepted = SmallVec::new();
        loop {
            match listener.poll_accept_item(cx) {
                Poll::Ready(Ok(connection)) => accepted.push(connection),
                Poll::Ready(Err(err)) => {
                    if accepted.is_empty() {
                        return Poll::Ready(Err(err));
                    }
                    return Poll::Ready(Ok(accepted));
                }
                Poll::Pending => {
                    if accepted.is_empty() {
                        return Poll::Pending;
                    }
                    return Poll::Ready(Ok(accepted));
                }
            }
        }
    })
    .await
}

async fn serve_listener<L, Spawn>(
    listener: L,
    locals: TaskLocals,
    shutdown_trigger: Py<PyAny>,
    mut spawn_connection: Spawn,
) -> Result<(), H2CornError>
where
    L: AcceptSource,
    Spawn: FnMut(L::Connection, watch::Receiver<ShutdownState>, &mut JoinSet<()>),
{
    let shutdown = shutdown_future(shutdown_trigger, locals.clone());
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

        let accepted = tokio::select! {
            shutdown_kind = &mut shutdown => {
                shutting_down = true;
                let _ = shutdown_tx.send(ShutdownState::Graceful(shutdown_kind));
                continue;
            }
            accepted = accept_batch(&listener) => accepted?,
            joined = tasks.join_next(), if !tasks.is_empty() => {
                let _ = joined;
                continue;
            }
        };

        for connection in accepted {
            spawn_connection(connection, shutdown_rx.clone(), &mut tasks);
        }
    }

    Ok(())
}

#[cfg(unix)]
async fn serve_unix(
    listener: UnixListener,
    app: AppState,
    config: &'static ServerConfig,
    shutdown_trigger: Py<PyAny>,
) -> Result<(), H2CornError> {
    if config.http1.enabled {
        serve_with_proxy_protocol!(
            config.proxy.protocol,
            serve_unix_with_preamble,
            listener,
            app,
            config,
            shutdown_trigger,
            true
        )
    } else {
        serve_with_proxy_protocol!(
            config.proxy.protocol,
            serve_unix_with_preamble,
            listener,
            app,
            config,
            shutdown_trigger,
            false
        )
    }
}

#[cfg(unix)]
fn serve_unix_with_preamble<P, const HTTP1: bool>(
    listener: UnixListener,
    app: AppState,
    config: &'static ServerConfig,
    shutdown_trigger: Py<PyAny>,
) -> impl Future<Output = Result<(), H2CornError>>
where
    P: ConnectionPreamble,
{
    serve_listener(
        listener,
        app.locals.clone(),
        shutdown_trigger,
        move |(stream, _), shutdown, tasks| {
            let (reader, writer) = stream.into_split();
            spawn_unix_connection::<P, HTTP1>(tasks, &app, reader, writer, config, shutdown);
        },
    )
}

async fn serve_connection<R, W, P, const HTTP1: bool>(
    reader: R,
    writer: W,
    app: AppState,
    config: &'static ServerConfig,
    actual_peer: ConnectionPeer,
    shutdown: watch::Receiver<ShutdownState>,
) -> Result<(), H2CornError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static + H1WriteTarget + H2WriteTarget,
    P: ConnectionPreamble,
{
    let mut reader = FrameReader::new(reader);
    let mut info = ConnectionInfo::from_peer(
        actual_peer,
        config.proxy.trust_headers && peer_is_trusted(&config.proxy.trusted_peers, &actual_peer),
    );

    let connection_start = timeout(config.timeout_handshake, async {
        let proxy = P::read_proxy(&mut reader, &actual_peer, &config.proxy.trusted_peers).await?;
        let protocol = read_preamble_protocol::<R, HTTP1>(&mut reader).await?;
        Ok::<_, H2CornError>(ConnectionStart { proxy, protocol })
    })
    .await
    .map_err(|_| H2Error::ConnectionHandshakeTimedOut)??;
    if let Some(proxy) = connection_start.proxy {
        info.apply_proxy_info(proxy);
    }
    let info = Arc::new(info);
    let connection_ctx = ConnectionContext::new(app, config, info.clone(), shutdown.clone());

    match connection_start.protocol {
        DetectedProtocol::Http2 => {
            h2::serve_connection(reader, writer, connection_ctx, shutdown).await
        }
        DetectedProtocol::Http1 => {
            let (reader, buffer) = reader.into_parts();
            h1::serve_connection(reader, buffer, writer, connection_ctx, shutdown).await
        }
    }
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
