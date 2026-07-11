use std::collections::VecDeque;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore};

use super::{WRITER_CHANNEL_CAPACITY, WriterCommandBatch};
use crate::error::{ErrorExt, H2CornError, H2Error};
use crate::h2::{LAZY_STREAM_CAPACITY, StreamMap, new_stream_map};
use crate::h2_frame::StreamId;
use crate::inline_fifo::InlineFifo;

#[derive(Debug)]
pub(super) struct QueuedCommandBatch {
    pub(super) commands: WriterCommandBatch,
    _permit: OwnedSemaphorePermit,
}

pub(super) type QueuedStreamCommands = InlineFifo<QueuedCommandBatch, 2>;
pub(super) type DrainedIngressWrites = Vec<(StreamId, QueuedStreamCommands)>;

#[derive(Debug, Default)]
struct PendingAppWrites {
    commands: QueuedStreamCommands,
}

struct OpenWriterIngressQueue {
    ready_streams: VecDeque<StreamId>,
    streams: StreamMap<PendingAppWrites>,
}

enum WriterIngressQueue {
    Open(OpenWriterIngressQueue),
    Closed,
}

pub(super) struct WriterIngress {
    queue: Mutex<WriterIngressQueue>,
    has_pending: AtomicBool,
    pub(super) notify: Notify,
    permits: Arc<Semaphore>,
}

impl OpenWriterIngressQueue {
    fn enqueue_batch(&mut self, stream_id: StreamId, batch: QueuedCommandBatch) {
        let newly_enqueued = match self.streams.entry(stream_id) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().commands.push_back(batch);
                false
            },
            Entry::Vacant(entry) => {
                let mut stream = PendingAppWrites::default();
                stream.commands.push_back(batch);
                entry.insert(stream);
                true
            },
        };
        if newly_enqueued {
            self.ready_streams.push_back(stream_id);
        }
    }

    fn drain_ready_into(&mut self, drained: &mut DrainedIngressWrites) {
        drained.clear();
        drained.reserve(self.ready_streams.len());

        while let Some(stream_id) = self.ready_streams.pop_front() {
            let Some(stream) = self.streams.remove(&stream_id) else {
                continue;
            };
            if stream.commands.is_empty() {
                continue;
            }
            drained.push((stream_id, stream.commands));
        }
    }

    fn restore_drained(&mut self, drained: DrainedIngressWrites) {
        for (stream_id, mut commands) in drained {
            if commands.is_empty() {
                continue;
            }

            let newly_enqueued = match self.streams.entry(stream_id) {
                Entry::Occupied(mut entry) => {
                    let stream = entry.get_mut();
                    while let Some(existing) = stream.commands.pop_front() {
                        commands.push_back(existing);
                    }
                    stream.commands = commands;
                    false
                },
                Entry::Vacant(entry) => {
                    entry.insert(PendingAppWrites { commands });
                    true
                },
            };
            if newly_enqueued {
                self.ready_streams.push_back(stream_id);
            }
        }
    }

    fn has_pending(&self) -> bool {
        !self.ready_streams.is_empty()
    }

    fn drop_stream(&mut self, stream_id: StreamId) {
        self.streams.remove(&stream_id);
    }
}

impl WriterIngressQueue {
    fn has_pending(&self) -> bool {
        match self {
            Self::Open(queue) => queue.has_pending(),
            Self::Closed => false,
        }
    }

    fn close(&mut self) {
        *self = Self::Closed;
    }
}

impl WriterIngress {
    pub(super) fn new(max_concurrent_streams: usize) -> Arc<Self> {
        Arc::new(Self {
            queue: Mutex::new(WriterIngressQueue::Open(OpenWriterIngressQueue {
                ready_streams: VecDeque::with_capacity(
                    max_concurrent_streams.min(LAZY_STREAM_CAPACITY),
                ),
                streams: new_stream_map(max_concurrent_streams),
            })),
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

        let mut guard = self.queue.lock().await;
        let WriterIngressQueue::Open(queue) = &mut *guard else {
            return H2Error::ConnectionWriterClosed.err();
        };
        queue.enqueue_batch(stream_id, QueuedCommandBatch {
            commands,
            _permit: permit,
        });
        self.has_pending.store(true, Ordering::Release);
        drop(guard);
        self.notify.notify_one();
        Ok(())
    }

    pub(super) async fn drain_into(&self, drained: &mut DrainedIngressWrites) {
        let mut queue = self.queue.lock().await;
        match &mut *queue {
            WriterIngressQueue::Open(queue) => queue.drain_ready_into(drained),
            WriterIngressQueue::Closed => drained.clear(),
        }
        self.has_pending
            .store(queue.has_pending(), Ordering::Release);
        drop(queue);
    }

    pub(super) async fn restore_drained(&self, drained: DrainedIngressWrites) {
        if drained.is_empty() {
            return;
        }

        let mut queue = self.queue.lock().await;
        if let WriterIngressQueue::Open(queue) = &mut *queue {
            queue.restore_drained(drained);
        }
        self.has_pending
            .store(queue.has_pending(), Ordering::Release);
        drop(queue);
    }

    pub(super) fn has_pending(&self) -> bool {
        self.has_pending.load(Ordering::Acquire)
    }

    pub(super) async fn drop_stream(&self, stream_id: StreamId) {
        let mut queue = self.queue.lock().await;
        if let WriterIngressQueue::Open(queue) = &mut *queue {
            queue.drop_stream(stream_id);
        }
        drop(queue);
    }

    pub(super) async fn close(&self) {
        let mut queue = self.queue.lock().await;
        queue.close();
        drop(queue);
        self.has_pending.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use tokio::task::yield_now;

    use super::{WRITER_CHANNEL_CAPACITY, WriterIngress};
    use crate::h2::writer::WriterCommand;
    use crate::h2_frame::StreamId;

    #[tokio::test]
    async fn closed_typestate_rejects_new_commands() {
        let ingress = WriterIngress::new(1);
        ingress.close().await;

        assert!(
            ingress
                .enqueue(
                    StreamId::new(1).expect("non-zero stream id"),
                    WriterCommand::FlushBufferedOutput,
                )
                .await
                .is_err()
        );
        assert!(!ingress.has_pending());
        drop(ingress);
    }

    #[tokio::test]
    async fn command_capacity_backpressures_and_close_releases_waiters() {
        let ingress = WriterIngress::new(1);
        let stream_id = StreamId::new(1).expect("non-zero stream id");

        for _ in 0..WRITER_CHANNEL_CAPACITY {
            ingress
                .enqueue(stream_id, WriterCommand::FlushBufferedOutput)
                .await
                .expect("capacity command accepted");
        }
        assert_eq!(ingress.permits.available_permits(), 0);

        let blocked_ingress = ingress.clone();
        let blocked = tokio::spawn(async move {
            blocked_ingress
                .enqueue(stream_id, WriterCommand::FlushBufferedOutput)
                .await
        });
        yield_now().await;
        assert!(!blocked.is_finished(), "command 65 must wait for capacity");

        ingress.close().await;
        assert!(
            blocked
                .await
                .expect("blocked producer task completed")
                .is_err(),
            "shutdown must reject rather than strand a blocked producer"
        );
        assert_eq!(ingress.permits.available_permits(), WRITER_CHANNEL_CAPACITY);
        assert!(!ingress.has_pending());
        drop(ingress);
    }
}
