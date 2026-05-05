use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore};

use super::{WRITER_CHANNEL_CAPACITY, WriterCommandBatch};
use crate::error::{ErrorExt, H2CornError, H2Error};
use crate::frame::StreamId;
use crate::h2::{StreamMap, new_stream_map};
use crate::smallvec_deque::SmallVecDeque;

#[derive(Debug)]
pub(super) struct QueuedCommandBatch {
    pub(super) commands: WriterCommandBatch,
    _permit: OwnedSemaphorePermit,
}

pub(super) type QueuedStreamCommands = SmallVecDeque<QueuedCommandBatch, 2>;
pub(super) type DrainedIngressWrites = Vec<(StreamId, QueuedStreamCommands)>;

#[derive(Debug, Default)]
struct PendingAppWrites {
    enqueued: bool,
    commands: QueuedStreamCommands,
}

#[derive(Default)]
struct WriterIngressQueue {
    closed: bool,
    ready_streams: VecDeque<u32>,
    streams: StreamMap<PendingAppWrites>,
}

pub(super) struct WriterIngress {
    queue: Mutex<WriterIngressQueue>,
    has_pending: AtomicBool,
    pub(super) notify: Notify,
    permits: Arc<Semaphore>,
}

impl WriterIngressQueue {
    fn enqueue_batch(&mut self, stream_id: StreamId, batch: QueuedCommandBatch) {
        let stream_id_raw = stream_id.get();
        let newly_enqueued = {
            let stream = self.streams.entry(stream_id_raw).or_default();
            stream.commands.push_back(batch);
            if stream.enqueued {
                false
            } else {
                stream.enqueued = true;
                true
            }
        };
        if newly_enqueued {
            self.ready_streams.push_back(stream_id_raw);
        }
    }

    fn drain_ready_into(&mut self, drained: &mut DrainedIngressWrites) {
        drained.clear();
        drained.reserve(self.ready_streams.len());

        while let Some(stream_id_raw) = self.ready_streams.pop_front() {
            let Some(stream) = self.streams.remove(&stream_id_raw) else {
                continue;
            };
            if stream.commands.is_empty() {
                continue;
            }
            // SAFETY: ready_streams only stores ids enqueued from validated StreamId
            // values.
            let stream_id = unsafe { StreamId::new_unchecked(stream_id_raw) };
            drained.push((stream_id, stream.commands));
        }
    }

    fn restore_drained(&mut self, drained: DrainedIngressWrites) {
        for (stream_id, mut commands) in drained {
            if commands.is_empty() {
                continue;
            }

            let stream_id_raw = stream_id.get();
            let newly_enqueued = {
                let stream = self.streams.entry(stream_id_raw).or_default();
                while let Some(existing) = stream.commands.pop_front() {
                    commands.push_back(existing);
                }
                stream.commands = commands;
                if stream.enqueued {
                    false
                } else {
                    stream.enqueued = true;
                    true
                }
            };
            if newly_enqueued {
                self.ready_streams.push_back(stream_id_raw);
            }
        }
    }

    fn has_pending(&self) -> bool {
        !self.ready_streams.is_empty()
    }

    fn drop_stream(&mut self, stream_id: StreamId) {
        self.streams.remove(&stream_id.get());
    }

    fn close(&mut self) {
        self.closed = true;
        self.ready_streams.clear();
        self.streams.clear();
    }
}

impl WriterIngress {
    pub(super) fn new(max_concurrent_streams: usize) -> Arc<Self> {
        Arc::new(Self {
            queue: Mutex::new(WriterIngressQueue {
                closed: false,
                ready_streams: VecDeque::with_capacity(max_concurrent_streams),
                streams: new_stream_map(max_concurrent_streams),
            }),
            has_pending: AtomicBool::new(false),
            notify: Notify::new(),
            permits: Arc::new(Semaphore::new(WRITER_CHANNEL_CAPACITY)),
        })
    }

    pub(super) async fn enqueue(
        &self,
        stream_id: StreamId,
        command: super::WriterCommand,
    ) -> Result<(), H2CornError> {
        let mut commands = WriterCommandBatch::new();
        commands.push_back(command);
        self.enqueue_batch(stream_id, commands).await
    }

    pub(super) async fn enqueue_batch(
        &self,
        stream_id: StreamId,
        commands: WriterCommandBatch,
    ) -> Result<(), H2CornError> {
        if commands.is_empty() {
            return Ok(());
        }

        let permit = Arc::clone(&self.permits)
            .acquire_many_owned(
                u32::try_from(commands.len()).expect("writer command batch length fits in u32"),
            )
            .await
            .map_err(|_| H2Error::ConnectionWriterClosed)?;

        let mut queue = self.queue.lock().await;
        if queue.closed {
            return H2Error::ConnectionWriterClosed.err();
        }

        queue.enqueue_batch(stream_id, QueuedCommandBatch {
            commands,
            _permit: permit,
        });
        self.has_pending.store(true, Ordering::Release);
        drop(queue);
        self.notify.notify_one();
        Ok(())
    }

    pub(super) async fn drain_into(&self, drained: &mut DrainedIngressWrites) {
        let mut queue = self.queue.lock().await;
        queue.drain_ready_into(drained);
        self.has_pending
            .store(queue.has_pending(), Ordering::Release);
        drop(queue);
    }

    pub(super) async fn restore_drained(&self, drained: DrainedIngressWrites) {
        if drained.is_empty() {
            return;
        }

        let mut queue = self.queue.lock().await;
        queue.restore_drained(drained);
        self.has_pending
            .store(queue.has_pending(), Ordering::Release);
        drop(queue);
    }

    pub(super) fn has_pending(&self) -> bool {
        self.has_pending.load(Ordering::Acquire)
    }

    pub(super) async fn drop_stream(&self, stream_id: StreamId) {
        let mut queue = self.queue.lock().await;
        queue.drop_stream(stream_id);
        drop(queue);
    }

    pub(super) async fn close(&self) {
        let mut queue = self.queue.lock().await;
        queue.close();
        drop(queue);
        self.has_pending.store(false, Ordering::Release);
    }
}
