use std::io;

#[cfg(target_os = "linux")]
use std::io::{Error, ErrorKind};

#[cfg(target_os = "linux")]
use rustix::fs::sendfile;
use tokio::fs::File;
use tokio::io::BufWriter;
#[cfg(not(target_os = "linux"))]
use tokio::io::copy;
use tokio::net::tcp::OwnedWriteHalf as TcpOwnedWriteHalf;

#[cfg(target_os = "linux")]
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
            }
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::WouldBlock => {}
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
