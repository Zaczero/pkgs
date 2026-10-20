use std::fs::File;
use std::io;
#[cfg(target_os = "linux")]
use std::io::{Error, ErrorKind};
use std::task::{Context, Poll};

#[cfg(target_os = "linux")]
use rustix::fs::sendfile;
#[cfg(target_os = "linux")]
use tokio::io::Interest;
use tokio::io::{AsyncWrite, BufWriter};
use tokio::net::tcp::OwnedWriteHalf as TcpOwnedWriteHalf;
#[cfg(unix)]
use tokio::net::unix::OwnedWriteHalf as UnixOwnedWriteHalf;

/// Transport capability for serving file bodies. Only targets with a true
/// capability use this method; all others stay on the rolling-read path.
/// The caller owns framing and must flush before calling `send_file`.
pub(crate) trait WriteTarget: AsyncWrite + Unpin + Send + Sync + 'static {
    const SUPPORTS_SENDFILE: bool;

    fn poll_send_file(
        _writer: &mut BufWriter<Self>,
        _file: &File,
        _offset: &mut u64,
        _len: usize,
        _cx: &mut Context<'_>,
    ) -> Poll<io::Result<usize>>
    where
        Self: Sized,
    {
        Poll::Ready(Err(io::ErrorKind::Unsupported.into()))
    }

    /// Send `len` bytes of `file` starting at `*offset`, advancing it.
    async fn send_file(
        _writer: &mut BufWriter<Self>,
        _file: &mut File,
        _offset: &mut u64,
        _len: usize,
    ) -> io::Result<()>
    where
        Self: Sized,
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "transport does not support sendfile",
        ))
    }
}

impl WriteTarget for TcpOwnedWriteHalf {
    const SUPPORTS_SENDFILE: bool = cfg!(target_os = "linux");

    #[cfg(target_os = "linux")]
    fn poll_send_file(
        writer: &mut BufWriter<Self>,
        file: &File,
        offset: &mut u64,
        len: usize,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<usize>> {
        let stream = writer.get_ref().as_ref();
        loop {
            std::task::ready!(stream.poll_write_ready(cx))?;
            match stream.try_io(Interest::WRITABLE, || {
                sendfile(stream, file, Some(offset), len).map_err(io::Error::from)
            }) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {},
                result => return Poll::Ready(result),
            }
        }
    }

    #[cfg(target_os = "linux")]
    async fn send_file(
        writer: &mut BufWriter<Self>,
        file: &mut File,
        offset: &mut u64,
        len: usize,
    ) -> io::Result<()> {
        sendfile_all_tcp(writer, file, offset, len).await
    }
}

#[cfg(unix)]
impl WriteTarget for UnixOwnedWriteHalf {
    const SUPPORTS_SENDFILE: bool = false;
}

#[cfg(target_os = "linux")]
pub(crate) async fn sendfile_all_tcp(
    writer: &BufWriter<TcpOwnedWriteHalf>,
    file: &File,
    offset: &mut u64,
    len: usize,
) -> io::Result<()> {
    let mut remaining = len;
    while remaining != 0 {
        let stream = writer.get_ref().as_ref();
        match stream
            .async_io(Interest::WRITABLE, || {
                sendfile(stream, file, Some(offset), remaining).map_err(io::Error::from)
            })
            .await?
        {
            0 => {
                return Err(Error::new(
                    ErrorKind::WriteZero,
                    "sendfile wrote zero bytes",
                ));
            },
            written => remaining = remaining.wrapping_sub(written),
        }
    }
    Ok(())
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::future::poll_fn;
    use std::io::{Read as _, Write as _};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use rustix::io::{Errno, dup};
    use rustix::net::sockopt::set_socket_send_buffer_size;
    use rustix::net::{SendFlags, Shutdown, send, shutdown};
    use tokio::io::BufWriter;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::runtime::Builder;

    use super::sendfile_all_tcp;

    #[test]
    #[cfg_attr(miri, ignore = "Miri does not emulate SO_SNDBUF or sendfile")]
    fn sendfile_yields_for_backpressure_and_resumes_at_the_exact_offset() {
        const START: usize = 37;
        const LEN: usize = 2 * 1024 * 1024;
        const SEND_BUFFER: usize = 4096;
        const BACKPRESSURE_TIMEOUT: Duration = Duration::from_secs(5);

        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current-thread runtime");
        let (writer_stream, reader_stream) = runtime.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind loopback listener");
            let writer = TcpStream::connect(listener.local_addr().expect("listener address"))
                .await
                .expect("connect loopback writer");
            let (reader, _) = listener.accept().await.expect("accept loopback writer");
            (writer, reader)
        });

        set_socket_send_buffer_size(&writer_stream, SEND_BUFFER)
            .expect("set small TCP send buffer");
        runtime
            .block_on(writer_stream.writable())
            .expect("prime Tokio writable readiness");

        let filler = [0x5A; 1024];
        let mut filler_len = 0;
        loop {
            match send(
                &writer_stream,
                &filler,
                SendFlags::DONTWAIT | SendFlags::NOSIGNAL,
            ) {
                Ok(written) => {
                    assert_ne!(written, 0, "nonempty socket send made no progress");
                    filler_len += written;
                },
                Err(error) if error == Errno::WOULDBLOCK => break,
                Err(error) => panic!("fill TCP send buffer: {error}"),
            }
        }
        assert_ne!(filler_len, 0, "TCP send buffer was not filled");

        let mut source = tempfile::tempfile().expect("create sendfile source");
        let contents: Vec<_> = (0..START + LEN + 19)
            .map(|index| (index.wrapping_mul(37) % 251) as u8)
            .collect();
        source.write_all(&contents).expect("write sendfile source");
        let expected = contents[START..START + LEN].to_vec();
        let mut reader = reader_stream
            .into_std()
            .expect("convert reader to std TCP stream");
        reader
            .set_nonblocking(false)
            .expect("make TCP reader blocking");
        reader
            .set_read_timeout(Some(Duration::from_secs(15)))
            .expect("bound reader wait");
        let abort_stream = dup(&writer_stream).expect("clone writer for bounded cleanup");
        let (_, write_half) = writer_stream.into_split();
        let writer = BufWriter::new(write_half);
        let (reader_gate, reader_wait) = mpsc::channel();
        let reader_thread = thread::spawn(move || {
            reader_wait.recv().expect("release TCP reader");
            let mut received = vec![0; filler_len + LEN];
            reader.read_exact(&mut received).map(|()| received)
        });

        let (pending_tx, pending_rx) = mpsc::channel();
        let release_reader = reader_gate.clone();
        let transfer_thread = thread::spawn(move || {
            runtime.block_on(async move {
                let mut offset = START as u64;
                let mut transfer = Box::pin(sendfile_all_tcp(&writer, &source, &mut offset, LEN));
                let first_poll =
                    poll_fn(|context| std::task::Poll::Ready(transfer.as_mut().poll(context)))
                        .await;
                let result = match first_poll {
                    std::task::Poll::Pending => {
                        pending_tx.send(true).expect("report pending sendfile poll");
                        reader_gate.send(()).expect("release TCP reader");
                        transfer.await
                    },
                    std::task::Poll::Ready(result) => {
                        pending_tx.send(false).expect("report ready sendfile poll");
                        reader_gate.send(()).expect("release TCP reader");
                        drop(transfer);
                        result
                    },
                };
                (result, offset)
            })
        });

        let yielded = pending_rx.recv_timeout(BACKPRESSURE_TIMEOUT);
        if yielded != Ok(true) {
            release_reader.send(()).unwrap_or(());
            shutdown(&abort_stream, Shutdown::Both).unwrap_or(());
        }
        let (result, offset) = transfer_thread.join().expect("join sendfile transfer");
        let received = reader_thread.join().expect("join TCP reader");

        assert_eq!(
            yielded,
            Ok(true),
            "sendfile must return Pending while the socket is backpressured"
        );
        assert!(
            result.is_ok(),
            "sendfile failed at offset {offset}; reader status {:?}, error {:?}",
            received.as_ref().map(Vec::len).map_err(ToString::to_string),
            result.as_ref().err()
        );
        assert_eq!(offset, (START + LEN) as u64);
        let received = received.expect("read filler and sendfile bytes");
        assert!(received[..filler_len].iter().all(|byte| *byte == filler[0]));
        assert_eq!(&received[filler_len..], expected);
    }
}
