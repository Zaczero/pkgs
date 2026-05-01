use parking_lot::Mutex;
pub use parking_lot::MutexGuard;
use tokio::sync::Notify;

use crate::smallvec_deque::SmallVecDeque;

pub struct BufferedStateInner<S, T, const N: usize> {
    pub(crate) state: S,
    pub(crate) queue: SmallVecDeque<T, N>,
}

pub struct BufferedState<S, T, const N: usize> {
    inner: Mutex<BufferedStateInner<S, T, N>>,
    ready: Notify,
}

impl<S, T, const N: usize> BufferedState<S, T, N> {
    pub(crate) fn new(state: S) -> Self {
        Self {
            inner: Mutex::new(BufferedStateInner {
                state,
                queue: SmallVecDeque::new(),
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
