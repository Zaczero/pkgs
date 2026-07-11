use std::fs::File;
use std::io;
#[cfg(target_os = "linux")]
use std::io::{Error, ErrorKind};

#[cfg(target_os = "linux")]
use rustix::fs::sendfile;
use tokio::io::{AsyncWrite, BufWriter};
use tokio::net::tcp::OwnedWriteHalf as TcpOwnedWriteHalf;
#[cfg(unix)]
use tokio::net::unix::OwnedWriteHalf as UnixOwnedWriteHalf;

/// Transport capability for serving file bodies. Only targets with a true
/// capability use this method; all others stay on the rolling-read path.
/// The caller owns framing and must flush before calling `send_file`.
pub(crate) trait WriteTarget: AsyncWrite + Unpin + Send + Sync + 'static {
    const SUPPORTS_SENDFILE: bool;

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
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "the shared transport capability passes and advances an explicit offset"
)]
pub(crate) async fn sendfile_all_tcp(
    writer: &mut BufWriter<TcpOwnedWriteHalf>,
    file: &mut File,
    offset: &mut u64,
    len: usize,
) -> io::Result<()> {
    let end = *offset + len as u64;
    while *offset < end {
        writer.get_ref().writable().await?;
        let remaining = (end - *offset) as usize;
        match sendfile(writer.get_ref().as_ref(), &*file, Some(offset), remaining) {
            Ok(0) => {
                return Err(Error::new(
                    ErrorKind::WriteZero,
                    "sendfile wrote zero bytes",
                ));
            },
            Ok(_) => {},
            Err(err) if err.kind() == ErrorKind::WouldBlock => {},
            Err(err) => return Err(err.into()),
        }
    }
    Ok(())
}
