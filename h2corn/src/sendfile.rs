use std::io;
#[cfg(target_os = "linux")]
use std::io::{Error, ErrorKind};

#[cfg(target_os = "linux")]
use rustix::fs::sendfile;
use tokio::fs::File;
#[cfg(not(target_os = "linux"))]
use tokio::io::copy;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::net::tcp::OwnedWriteHalf as TcpOwnedWriteHalf;
#[cfg(unix)]
use tokio::net::unix::OwnedWriteHalf as UnixOwnedWriteHalf;

/// Transport capability for serving file bodies: true zero-copy kernel
/// sendfile on plain TCP, buffered copy everywhere else. The caller owns
/// framing and must flush the writer before calling `send_file`.
pub(crate) trait WriteTarget: AsyncWrite + Unpin + Send + Sync + 'static {
    const SUPPORTS_SENDFILE: bool;

    /// Send `len` bytes of `file` starting at `*offset`, advancing it.
    async fn send_file(
        writer: &mut BufWriter<Self>,
        file: &mut File,
        offset: &mut u64,
        len: usize,
    ) -> io::Result<()>
    where
        Self: Sized;
}

impl WriteTarget for TcpOwnedWriteHalf {
    const SUPPORTS_SENDFILE: bool = cfg!(target_os = "linux");

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

    async fn send_file(
        writer: &mut BufWriter<Self>,
        file: &mut File,
        offset: &mut u64,
        len: usize,
    ) -> io::Result<()> {
        copy_file_range_buffered(writer, file, offset, len).await
    }
}

/// Portable fallback: seek + bounded copy through the buffered writer.
pub(crate) async fn copy_file_range_buffered<W>(
    writer: &mut BufWriter<W>,
    file: &mut File,
    offset: &mut u64,
    len: usize,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    file.seek(io::SeekFrom::Start(*offset)).await?;
    let mut limited = AsyncReadExt::take(file, len as u64);
    let copied = tokio::io::copy(&mut limited, writer).await?;
    *offset += copied;
    writer.flush().await?;
    Ok(())
}

#[cfg(target_os = "linux")]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "signature matches the portable fallback and call sites that advance the offset"
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

#[cfg(not(target_os = "linux"))]
pub(crate) async fn sendfile_all_tcp(
    writer: &mut BufWriter<TcpOwnedWriteHalf>,
    file: &mut File,
    _offset: &mut u64,
    _len: usize,
) -> io::Result<()> {
    copy(file, writer.get_mut()).await?;
    Ok(())
}
