use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use crate::error::{ErrorExt as _, H2CornError, H2Error};
use crate::h2::writer::{WRITER_CHANNEL_CAPACITY, WriterCommandBatch};
use crate::h2::{StreamMap, new_stream_map};
use crate::h2_frame::StreamId;
use crate::inline_fifo::InlineFifo;
use crate::runtime::ApplicationFailure;

const OPEN: u8 = 0;
const ABORTING: u8 = 1;
const CLOSED: u8 = 2;

#[derive(Debug)]
pub(super) struct StreamGate {
    stream_id: StreamId,
    state: AtomicU8,
    closed: Notify,
    ingress: Weak<WriterIngress>,
}

impl StreamGate {
    fn for_stream(
        previous: Option<Arc<Self>>,
        stream_id: StreamId,
        ingress: &Arc<WriterIngress>,
    ) -> Arc<Self> {
        let gate = Self {
            stream_id,
            state: AtomicU8::new(OPEN),
            closed: Notify::new(),
            ingress: Arc::downgrade(ingress),
        };
        if let Some(mut previous) = previous
            && let Some(unshared) = Arc::get_mut(&mut previous)
        {
            *unshared = gate;
            previous
        } else {
            Arc::new(gate)
        }
    }

    fn is_open(&self) -> bool {
        self.state.load(Ordering::Acquire) == OPEN
    }

    fn close(&self) {
        self.state.store(CLOSED, Ordering::Release);
        self.closed.notify_waiters();
    }

    pub(super) fn complete(&self) -> bool {
        let complete = self
            .state
            .compare_exchange(OPEN, CLOSED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if complete {
            self.closed.notify_waiters();
        }
        complete
    }
}

impl ApplicationFailure for StreamGate {
    fn application_failed(&self) {
        if self
            .state
            .compare_exchange(OPEN, ABORTING, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        self.closed.notify_waiters();
        if let Some(ingress) = self.ingress.upgrade() {
            let mut queue = ingress.queue.lock();
            let mut discarded = None;
            if let WriterIngressQueue::Open(queue) = &mut *queue
                && let Some(stream) = queue.streams.get_mut(&self.stream_id)
            {
                discarded = Some(std::mem::take(&mut stream.commands));
                queue.aborts.push_back(self.stream_id);
                ingress.aborted.store(true, Ordering::Release);
            }
            drop(queue);
            drop(discarded);
            ingress.notify.notify_one();
        }
    }
}

#[derive(Debug)]
pub(super) struct QueuedCommandBatch {
    pub(super) commands: WriterCommandBatch,
    _permit: OwnedSemaphorePermit,
}

pub(super) type QueuedStreamCommands = InlineFifo<QueuedCommandBatch, 2>;
pub(super) type DrainedIngressWrites = Vec<(StreamId, QueuedStreamCommands)>;

#[derive(Debug)]
struct PendingAppWrites {
    commands: QueuedStreamCommands,
    gate: Arc<StreamGate>,
}

struct OpenWriterIngressQueue {
    ready_streams: VecDeque<StreamId>,
    streams: StreamMap<PendingAppWrites>,
    aborts: VecDeque<StreamId>,
    completed_gate: Option<Arc<StreamGate>>,
}

enum WriterIngressQueue {
    Open(OpenWriterIngressQueue),
    Closed,
}

pub(super) struct WriterIngress {
    queue: Mutex<WriterIngressQueue>,
    pub(super) notify: Notify,
    permits: Arc<Semaphore>,
    aborted: AtomicBool,
}

impl OpenWriterIngressQueue {
    fn enqueue_batch(
        &mut self,
        stream_id: StreamId,
        batch: QueuedCommandBatch,
    ) -> Result<(), H2CornError> {
        let Some(stream) = self
            .streams
            .get_mut(&stream_id)
            .filter(|stream| stream.gate.is_open())
        else {
            return H2Error::StreamChannelClosed.err();
        };
        let newly_enqueued = stream.commands.is_empty();
        stream.commands.push_back(batch);
        if newly_enqueued {
            self.ready_streams.push_back(stream_id);
        }
        Ok(())
    }

    fn drain_ready_into(&mut self, drained: &mut DrainedIngressWrites) {
        drained.clear();
        drained.reserve(self.ready_streams.len());

        while let Some(stream_id) = self.ready_streams.pop_front() {
            let Some(stream) = self.streams.get_mut(&stream_id) else {
                continue;
            };
            if stream.commands.is_empty() {
                continue;
            }
            drained.push((stream_id, std::mem::take(&mut stream.commands)));
        }
    }

    fn restore_drained(&mut self, drained: impl Iterator<Item = (StreamId, QueuedStreamCommands)>) {
        for (stream_id, mut commands) in drained {
            if commands.is_empty() {
                continue;
            }

            let Some(stream) = self
                .streams
                .get_mut(&stream_id)
                .filter(|stream| stream.gate.is_open())
            else {
                continue;
            };
            let newly_enqueued = stream.commands.is_empty();
            while let Some(existing) = stream.commands.pop_front() {
                commands.push_back(existing);
            }
            stream.commands = commands;
            if newly_enqueued {
                self.ready_streams.push_back(stream_id);
            }
        }
    }

    #[cfg(test)]
    fn has_pending(&self) -> bool {
        !self.ready_streams.is_empty()
    }

    fn drop_stream(&mut self, stream_id: StreamId) {
        if let Some(stream) = self.streams.remove(&stream_id) {
            stream.gate.close();
        }
    }
}

impl WriterIngressQueue {
    #[cfg(test)]
    fn has_pending(&self) -> bool {
        match self {
            Self::Open(queue) => queue.has_pending(),
            Self::Closed => false,
        }
    }

    fn close(&mut self) {
        if let Self::Open(queue) = self {
            #[expect(
                clippy::iter_over_hash_type,
                reason = "every live response gate closes independently"
            )]
            for stream in queue.streams.values() {
                stream.gate.close();
            }
        }
        *self = Self::Closed;
    }
}

impl WriterIngress {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            queue: Mutex::new(WriterIngressQueue::Open(OpenWriterIngressQueue {
                ready_streams: VecDeque::new(),
                streams: new_stream_map(),
                aborts: VecDeque::new(),
                completed_gate: None,
            })),
            notify: Notify::new(),
            permits: Arc::new(Semaphore::new(WRITER_CHANNEL_CAPACITY)),
            aborted: AtomicBool::new(false),
        })
    }

    pub(super) fn register(self: &Arc<Self>, stream_id: StreamId) {
        let mut queue = self.queue.lock();
        let WriterIngressQueue::Open(queue) = &mut *queue else {
            return;
        };
        let completed_gate = &mut queue.completed_gate;
        queue
            .streams
            .entry(stream_id)
            .or_insert_with(|| PendingAppWrites {
                commands: QueuedStreamCommands::new(),
                gate: StreamGate::for_stream(completed_gate.take(), stream_id, self),
            });
    }

    pub(super) fn gate(&self, stream_id: StreamId) -> Option<Arc<StreamGate>> {
        match &*self.queue.lock() {
            WriterIngressQueue::Open(queue) => queue
                .streams
                .get(&stream_id)
                .map(|stream| Arc::clone(&stream.gate)),
            WriterIngressQueue::Closed => None,
        }
    }

    pub(super) fn complete_stream(&self, stream_id: StreamId) -> bool {
        let mut guard = self.queue.lock();
        let WriterIngressQueue::Open(queue) = &mut *guard else {
            return false;
        };
        if !queue
            .streams
            .get(&stream_id)
            .is_some_and(|stream| stream.gate.complete())
        {
            return false;
        }
        let stream = queue
            .streams
            .remove(&stream_id)
            .expect("completed stream exists");
        queue.completed_gate = Some(stream.gate);
        true
    }

    pub(super) fn take_aborts(&self) -> VecDeque<StreamId> {
        if !self.aborted.load(Ordering::Acquire) {
            return VecDeque::new();
        }
        let mut queue = self.queue.lock();
        self.aborted.store(false, Ordering::Release);
        match &mut *queue {
            WriterIngressQueue::Open(queue) => std::mem::take(&mut queue.aborts),
            WriterIngressQueue::Closed => VecDeque::new(),
        }
    }

    pub(super) fn is_closed(&self) -> bool {
        matches!(*self.queue.lock(), WriterIngressQueue::Closed)
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

        let count = u32::try_from(commands.len()).expect("writer command batch length fits in u32");
        let permit = if let Ok(permit) = Arc::clone(&self.permits).try_acquire_many_owned(count) {
            permit
        } else {
            let gate = self.gate(stream_id).ok_or(H2Error::StreamChannelClosed)?;
            let closed = gate.closed.notified();
            tokio::pin!(closed);
            closed.as_mut().enable();
            if !gate.is_open() {
                return H2Error::StreamChannelClosed.err();
            }
            tokio::select! {
                permit = Arc::clone(&self.permits).acquire_many_owned(count) => permit.map_err(|_| H2Error::ConnectionWriterClosed)?,
                () = &mut closed => return H2Error::StreamChannelClosed.err(),
            }
        };

        let mut guard = self.queue.lock();
        let WriterIngressQueue::Open(queue) = &mut *guard else {
            return H2Error::ConnectionWriterClosed.err();
        };
        queue.enqueue_batch(stream_id, QueuedCommandBatch {
            commands,
            _permit: permit,
        })?;
        drop(guard);
        self.notify.notify_one();
        Ok(())
    }

    pub(super) fn drain_into(&self, drained: &mut DrainedIngressWrites) {
        let mut queue = self.queue.lock();
        match &mut *queue {
            WriterIngressQueue::Open(queue) => queue.drain_ready_into(drained),
            WriterIngressQueue::Closed => drained.clear(),
        }
        drop(queue);
    }

    pub(super) fn restore_drained(
        &self,
        drained: impl ExactSizeIterator<Item = (StreamId, QueuedStreamCommands)>,
    ) {
        if drained.len() == 0 {
            return;
        }

        let mut queue = self.queue.lock();
        if let WriterIngressQueue::Open(queue) = &mut *queue {
            queue.restore_drained(drained);
        }
        drop(queue);
    }

    #[cfg(test)]
    pub(super) fn has_pending(&self) -> bool {
        self.queue.lock().has_pending()
    }

    pub(super) fn drop_stream(&self, stream_id: StreamId) {
        let mut queue = self.queue.lock();
        if let WriterIngressQueue::Open(queue) = &mut *queue {
            queue.drop_stream(stream_id);
        }
        drop(queue);
    }

    pub(super) fn close(&self) {
        let mut queue = self.queue.lock();
        queue.close();
        drop(queue);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::task::yield_now;

    use super::{WRITER_CHANNEL_CAPACITY, WriterIngress};
    use crate::h2::writer::WriterCommand;
    use crate::h2_frame::StreamId;

    #[tokio::test]
    async fn closed_typestate_rejects_new_commands() {
        let ingress = WriterIngress::new();
        ingress.close();

        assert!(
            ingress
                .enqueue(
                    StreamId::new(1).expect("non-zero stream id"),
                    WriterCommand::FlushStream(StreamId::new(1).unwrap()),
                )
                .await
                .is_err()
        );
        assert!(!ingress.has_pending());
        drop(ingress);
    }

    #[tokio::test]
    async fn command_capacity_backpressures_and_close_releases_waiters() {
        let ingress = WriterIngress::new();
        let stream_id = StreamId::new(1).expect("non-zero stream id");
        ingress.register(stream_id);

        for _ in 0..WRITER_CHANNEL_CAPACITY {
            ingress
                .enqueue(stream_id, WriterCommand::FlushStream(stream_id))
                .await
                .expect("capacity command accepted");
        }
        assert_eq!(ingress.permits.available_permits(), 0);

        let blocked_ingress = Arc::clone(&ingress);
        let blocked = tokio::spawn(async move {
            blocked_ingress
                .enqueue(stream_id, WriterCommand::FlushStream(stream_id))
                .await
        });
        yield_now().await;
        assert!(!blocked.is_finished(), "command 65 must wait for capacity");

        ingress.close();
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

    #[tokio::test]
    async fn application_abort_bypasses_full_command_admission_and_wakes_sender() {
        use crate::runtime::ApplicationFailure as _;
        let ingress = WriterIngress::new();
        let stream_id = StreamId::new(1).unwrap();
        ingress.register(stream_id);
        for _ in 0..WRITER_CHANNEL_CAPACITY {
            ingress
                .enqueue(stream_id, WriterCommand::FlushStream(stream_id))
                .await
                .unwrap();
        }
        let waiting = tokio::spawn({
            let ingress = Arc::clone(&ingress);
            async move {
                ingress
                    .enqueue(stream_id, WriterCommand::FlushStream(stream_id))
                    .await
            }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        let failure = ingress.gate(stream_id).unwrap();
        failure.application_failed();
        failure.application_failed();
        drop(failure);
        assert!(waiting.await.unwrap().is_err());
        assert_eq!(ingress.permits.available_permits(), WRITER_CHANNEL_CAPACITY);
        assert_eq!(ingress.take_aborts().into_iter().collect::<Vec<_>>(), vec![
            stream_id
        ]);
        assert!(ingress.take_aborts().is_empty());
        drop(ingress);
    }
}
