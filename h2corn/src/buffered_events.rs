use parking_lot::Mutex;
pub(crate) use parking_lot::MutexGuard;
use tokio::sync::Notify;

use crate::inline_fifo::InlineFifo;

pub(crate) struct BufferedStateInner<S, T, const N: usize> {
    pub(crate) state: S,
    pub(crate) queue: InlineFifo<T, N>,
}

pub(crate) struct BufferedState<S, T, const N: usize> {
    inner: Mutex<BufferedStateInner<S, T, N>>,
    ready: Notify,
}

impl<S, T, const N: usize> BufferedState<S, T, N> {
    pub(crate) fn new(state: S) -> Self {
        Self {
            inner: Mutex::new(BufferedStateInner {
                state,
                queue: InlineFifo::new(),
            }),
            ready: Notify::new(),
        }
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, BufferedStateInner<S, T, N>> {
        self.inner.lock()
    }

    pub(crate) fn notify_ready(&self) {
        self.ready.notify_one();
    }

    pub(crate) async fn wait_ready(&self) {
        self.ready.notified().await;
    }
}
