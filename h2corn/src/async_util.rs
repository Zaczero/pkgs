use std::io::{self, IoSlice};
use std::time::Duration;

use tokio::io::{AsyncWrite, AsyncWriteExt as _};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::error::Elapsed;
use tokio::time::timeout;

pub(crate) enum TryPush<T> {
    Sent,
    Full(T),
    Closed(T),
}

/// Bound a future, or leave it unbounded when no limit is configured.
///
/// Every timeout option spells "no limit" as zero, which reaches here as
/// `None`. Handing `Duration::ZERO` to `timeout` instead would expire on the
/// first poll and fail everything it was meant to protect.
pub(crate) async fn with_optional_timeout<F>(
    limit: Option<Duration>,
    future: F,
) -> Result<F::Output, Elapsed>
where
    F: Future,
{
    match limit {
        Some(limit) => timeout(limit, future).await,
        None => Ok(future.await),
    }
}

pub(crate) fn try_push<T>(tx: &mpsc::Sender<T>, value: T) -> TryPush<T> {
    match tx.try_send(value) {
        Ok(()) => TryPush::Sent,
        Err(TrySendError::Full(value)) => TryPush::Full(value),
        Err(TrySendError::Closed(value)) => TryPush::Closed(value),
    }
}

pub(crate) async fn send_if_open<T>(tx: &mpsc::Sender<T>, value: T) -> bool {
    match try_push(tx, value) {
        TryPush::Sent => true,
        TryPush::Full(value) => tx.send(value).await.is_ok(),
        TryPush::Closed(_) => false,
    }
}

pub(crate) async fn send_best_effort<T>(tx: &mpsc::Sender<T>, value: T) {
    match try_push(tx, value) {
        TryPush::Sent | TryPush::Closed(_) => {},
        TryPush::Full(value) => {
            drop(tx.send(value).await);
        },
    }
}

/// Drive `write_vectored` to completion over `slices`. Owns only completion:
/// callers keep protocol framing, batching, and buffered-writer flush policy.
pub(crate) async fn write_all_vectored<W>(
    writer: &mut W,
    slices: &mut [IoSlice<'_>],
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut bufs = slices;
    IoSlice::advance_slices(&mut bufs, 0);
    while !bufs.is_empty() {
        let written = writer.write_vectored(bufs).await?;
        if written == 0 {
            return Err(io::Error::from(io::ErrorKind::WriteZero));
        }
        IoSlice::advance_slices(&mut bufs, written);
    }
    Ok(())
}

#[cfg(test)]
mod write_all_vectored_tests {
    use std::io::{self, IoSlice};
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::io::AsyncWrite;

    use super::write_all_vectored;

    #[derive(Default)]
    struct ThrottledWriter {
        bytes: Vec<u8>,
        max_per_call: usize,
        calls: usize,
    }

    impl AsyncWrite for ThrottledWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let take = buf.len().min(self.max_per_call);
            self.bytes.extend_from_slice(&buf[..take]);
            self.calls = self.calls.wrapping_add(1);
            Poll::Ready(Ok(take))
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            let mut remaining = self.max_per_call;
            let mut written = 0_usize;
            for buf in bufs {
                let take = remaining.min(buf.len());
                self.bytes.extend_from_slice(&buf[..take]);
                written = written.wrapping_add(take);
                remaining = remaining.wrapping_sub(take);
                if remaining == 0 {
                    break;
                }
            }
            self.calls = self.calls.wrapping_add(1);
            Poll::Ready(Ok(written))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }
    }

    struct ZeroWriter;

    impl AsyncWrite for ZeroWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(0))
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(0))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn partial_progress_crosses_slice_boundaries() {
        let a = b"abcd";
        let b = b"efgh";
        let mut slices = [
            IoSlice::new(b""),
            IoSlice::new(a),
            IoSlice::new(b""),
            IoSlice::new(b),
            IoSlice::new(b""),
        ];
        let mut writer = ThrottledWriter {
            max_per_call: 3,
            ..ThrottledWriter::default()
        };
        write_all_vectored(&mut writer, &mut slices)
            .await
            .expect("throttled writer eventually completes");
        assert_eq!(writer.bytes, b"abcdefgh");
        assert!(writer.calls > 1);
    }

    #[tokio::test]
    async fn all_empty_slices_succeed_without_write_zero() {
        let mut slices = [IoSlice::new(b""), IoSlice::new(b"")];
        write_all_vectored(&mut ZeroWriter, &mut slices)
            .await
            .expect("empty vectors must not call write_vectored");
    }

    #[tokio::test]
    async fn zero_progress_with_remaining_bytes_is_write_zero() {
        let mut slices = [IoSlice::new(b"x")];
        let err = write_all_vectored(&mut ZeroWriter, &mut slices)
            .await
            .expect_err("zero progress must fail");
        assert_eq!(err.kind(), io::ErrorKind::WriteZero);
    }
}

#[cfg(test)]
mod send_best_effort_tests {
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};

    use tokio::sync::mpsc;

    use super::send_best_effort;

    fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
    }

    #[tokio::test]
    async fn send_best_effort_ignores_close_after_backpressure() {
        let (tx, rx) = mpsc::channel(1);
        tx.try_send(0).expect("channel has one available slot");
        let mut send = std::pin::pin!(send_best_effort(&tx, 1));
        assert!(
            poll_once(send.as_mut()).is_pending(),
            "the queued item must force the best-effort send to wait"
        );

        drop(rx);
        assert!(matches!(poll_once(send.as_mut()), Poll::Ready(()),));
    }
}
