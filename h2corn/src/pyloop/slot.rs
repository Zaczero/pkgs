//! Outcome slot connecting a pump-created asyncio Task to the tokio task
//! driving the connection. Replaces the futures oneshot + `PyTaskCompleter`
//! chain from pyo3-async-runtimes.

use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use parking_lot::Mutex;
use pyo3::prelude::*;
use tokio::sync::oneshot;

use super::Shard;

enum SlotState<T> {
    Pending {
        waker: Option<Waker>,
        task: Option<Py<PyAny>>,
    },
    Done(T),
    Taken,
    Abandoned,
}

enum SlotAbandonment {
    Unresolved(Option<Py<PyAny>>),
    Settled,
    AlreadyAbandoned,
}

/// Single-value rendezvous filled by the pump (loop thread) and awaited by a
/// tokio task. Also carries the `asyncio.Task` handle for cancellation.
///
/// `Guard` is slot-owned state that drops with the slot's final owner —
/// request slots use it for teardown accounting keyed to their own drop
/// order (see `runtime::RequestTaskGuard`).
pub(crate) struct TaskSlot<T, Guard = ()> {
    state: Mutex<SlotState<T>>,
    guard: Guard,
}

/// Cold-path acknowledgment sent when every owner of a slot has gone away.
///
/// For an abandoned Python task, the pump's done-callback holds the final
/// slot reference. The receiver therefore resolves only after cancellation
/// has propagated through the awaitable and its done-callback has run.
pub(crate) struct SlotDropAck(Option<oneshot::Sender<()>>);

impl Drop for SlotDropAck {
    fn drop(&mut self) {
        if let Some(done) = self.0.take() {
            let _ = done.send(());
        }
    }
}

pub(crate) type AcknowledgedSlotFuture<T> = SlotFuture<T, SlotDropAck>;
pub(crate) type SlotDropWait = oneshot::Receiver<()>;

impl<T> TaskSlot<T> {
    pub(crate) fn new() -> Arc<Self> {
        Self::with_guard(())
    }
}

impl<T, Guard> TaskSlot<T, Guard> {
    pub(crate) const fn guard(&self) -> &Guard {
        &self.guard
    }

    pub(crate) fn with_guard(guard: Guard) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(SlotState::Pending {
                waker: None,
                task: None,
            }),
            guard,
        })
    }

    /// Fill the slot and wake the awaiting tokio task. Called by the pump.
    pub(crate) fn fill(&self, value: T) {
        let waker = {
            let mut state = self.state.lock();
            match &mut *state {
                SlotState::Pending { waker, .. } => {
                    let waker = waker.take();
                    *state = SlotState::Done(value);
                    waker
                },
                // A second completion (e.g. done-callback after an error was
                // already recorded) is dropped; first outcome wins.
                SlotState::Done(_) | SlotState::Taken | SlotState::Abandoned => None,
            }
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Record the `asyncio.Task` driving this slot. The strong reference is
    /// load-bearing: event loops keep only weak references to tasks, so the
    /// slot anchors the Task for the lifetime of the request that awaits it.
    pub(crate) fn set_task(&self, task: Py<PyAny>) -> Option<Py<PyAny>> {
        let mut state = self.state.lock();
        match &mut *state {
            SlotState::Pending {
                task: task_slot, ..
            } => {
                debug_assert!(task_slot.is_none(), "task slot is written once");
                *task_slot = Some(task);
                None
            },
            SlotState::Abandoned => Some(task),
            SlotState::Done(_) | SlotState::Taken => None,
        }
    }

    /// Abandon an unresolved wait, returning a running Python task that must
    /// be cancelled on its owning loop. If task construction has not happened
    /// yet, [`set_task`](Self::set_task) observes `Abandoned` and returns it to
    /// the pump for immediate cancellation.
    fn abandon(&self) -> SlotAbandonment {
        let mut state = self.state.lock();
        let completed = match &mut *state {
            SlotState::Pending { task, .. } => {
                let task = task.take();
                *state = SlotState::Abandoned;
                return SlotAbandonment::Unresolved(task);
            },
            SlotState::Done(_) => mem::replace(&mut *state, SlotState::Taken),
            SlotState::Taken => return SlotAbandonment::Settled,
            SlotState::Abandoned => return SlotAbandonment::AlreadyAbandoned,
        };
        drop(state);
        let SlotState::Done(value) = completed else {
            unreachable!()
        };
        drop(value);
        SlotAbandonment::Settled
    }

    /// Await the outcome from the tokio side.
    pub(crate) fn wait(self: &Arc<Self>, shard: Shard) -> SlotFuture<T, Guard> {
        SlotFuture {
            slot: Arc::clone(self),
            shard,
            completed: false,
        }
    }
}

impl<T> TaskSlot<T, SlotDropAck> {
    pub(crate) fn with_drop_ack() -> (Arc<Self>, SlotDropWait) {
        let (done, wait) = oneshot::channel();
        (Self::with_guard(SlotDropAck(Some(done))), wait)
    }
}

pub(crate) struct SlotFuture<T, Guard = ()> {
    slot: Arc<TaskSlot<T, Guard>>,
    shard: Shard,
    completed: bool,
}

impl<T, Guard> Future for SlotFuture<T, Guard> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let poll = {
            let mut state = self.slot.state.lock();
            match &mut *state {
                SlotState::Pending { waker, .. } => {
                    match waker {
                        Some(existing) => existing.clone_from(cx.waker()),
                        None => *waker = Some(cx.waker().clone()),
                    }
                    Poll::Pending
                },
                state @ SlotState::Done(_) => {
                    let SlotState::Done(value) = mem::replace(state, SlotState::Taken) else {
                        unreachable!()
                    };
                    Poll::Ready(value)
                },
                SlotState::Taken => panic!("slot future polled after completion"),
                SlotState::Abandoned => panic!("abandoned slot future polled"),
            }
        };
        if poll.is_ready() {
            self.completed = true;
        }
        poll
    }
}

impl<T, Guard> Drop for SlotFuture<T, Guard> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        if let SlotAbandonment::Unresolved(task) = self.slot.abandon()
            && let Some(task) = task
        {
            self.shard.push(super::PumpEvent::CancelTask { task });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::thread;

    use pyo3::Python;

    use super::{SlotAbandonment, SlotState, TaskSlot};
    use crate::pyloop::ShardHandle;

    #[test]
    fn task_created_after_abandonment_is_returned_for_cancellation() {
        Python::initialize();
        Python::attach(|py| {
            let slot = TaskSlot::<()>::new();
            assert!(matches!(slot.abandon(), SlotAbandonment::Unresolved(None)));
            assert!(slot.set_task(py.None()).is_some());
        });
    }

    #[test]
    fn abandoning_a_running_task_returns_its_anchor_once() {
        Python::initialize();
        Python::attach(|py| {
            let slot = TaskSlot::<()>::new();
            assert!(slot.set_task(py.None()).is_none());
            assert!(matches!(
                slot.abandon(),
                SlotAbandonment::Unresolved(Some(_))
            ));
            assert!(matches!(slot.abandon(), SlotAbandonment::AlreadyAbandoned));
        });
    }

    #[test]
    fn dropping_an_unpolled_completed_future_discards_the_value() {
        Python::initialize();
        let shard = Python::attach(ShardHandle::test_stub);
        let slot = TaskSlot::<()>::new();
        slot.fill(());

        drop(slot.wait(shard));
        assert!(matches!(*slot.state.lock(), SlotState::Taken));
    }

    #[test]
    fn completion_abandonment_race_resolves_to_exactly_one_transition() {
        Python::initialize();
        let shard = Python::attach(ShardHandle::test_stub);
        for _ in 0..64 {
            let slot = TaskSlot::<()>::new();
            let future = slot.wait(Arc::clone(&shard));
            let fill_slot = Arc::clone(&slot);
            let barrier = Arc::new(Barrier::new(2));

            thread::scope(|scope| {
                let fill_barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    fill_barrier.wait();
                    fill_slot.fill(());
                });
                scope.spawn(move || {
                    barrier.wait();
                    drop(future);
                });
            });

            assert!(
                matches!(&*slot.state.lock(), SlotState::Taken | SlotState::Abandoned),
                "both racing transitions completed"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn drop_ack_waits_for_the_last_slot_owner() {
        let (slot, dropped) = TaskSlot::<(), super::SlotDropAck>::with_drop_ack();
        let callback_owner = slot.clone();

        drop(slot);
        assert!(dropped.is_empty());

        drop(callback_owner);
        dropped
            .await
            .expect("last slot owner acknowledges its drop");
    }
}
