//! Outcome slot connecting a pump-created asyncio Task to the tokio task
//! driving the connection. Replaces the futures oneshot + `PyTaskCompleter`
//! chain from pyo3-async-runtimes.

use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use parking_lot::Mutex;
use pyo3::prelude::*;

use crate::error::H2CornError;
use crate::pyloop::{PumpEvent, Shard, UnraisableError};

enum SlotState<T> {
    Pending {
        waker: Option<Waker>,
        task: Option<Py<PyAny>>,
    },
    Done(T),
    Taken,
    Abandoned,
}

enum SlotAbandonment<T> {
    Unresolved(Option<Py<PyAny>>),
    Completed(T),
    Settled,
    AlreadyAbandoned,
}

pub(crate) trait SlotValue {
    fn report_unaccepted(self, shard: &Shard);
}

impl SlotValue for () {
    fn report_unaccepted(self, _shard: &Shard) {}
}

impl SlotValue for Result<(), H2CornError> {
    fn report_unaccepted(self, shard: &Shard) {
        if let Err(error) = self {
            shard.push(PumpEvent::ReportUnraisable {
                error: UnraisableError::H2Corn(error),
            });
        }
    }
}

impl SlotValue for PyResult<Py<PyAny>> {
    fn report_unaccepted(self, shard: &Shard) {
        if let Err(error) = self {
            shard.push(PumpEvent::ReportUnraisable {
                error: UnraisableError::Python(error),
            });
        }
    }
}

/// Single-value rendezvous filled by the pump (loop thread) and awaited by a
/// tokio task. Also carries the `asyncio.Task` handle for cancellation.
///
/// `Guard` is slot-owned state that drops with the slot's final owner —
/// request slots use it for teardown accounting keyed to their own drop
/// order (see `runtime::RequestTaskGuard`).
pub(crate) struct TaskSlot<T: SlotValue, Guard = ()> {
    state: Mutex<SlotState<T>>,
    guard: Guard,
}

impl<T: SlotValue> TaskSlot<T> {
    pub(crate) fn new() -> Arc<Self> {
        Self::with_guard(())
    }
}

impl<T: SlotValue, Guard> TaskSlot<T, Guard> {
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

    /// Fill the slot and wake its waiter, returning any outcome no waiter took.
    pub(crate) fn fill(&self, value: T) -> Result<(), T> {
        let (waker, task) = {
            let mut state = self.state.lock();
            match &mut *state {
                SlotState::Pending { waker, task } => {
                    let waker = waker.take();
                    let task = task.take();
                    *state = SlotState::Done(value);
                    drop(state);
                    (waker, task)
                },
                SlotState::Done(_) | SlotState::Taken | SlotState::Abandoned => return Err(value),
            }
        };
        drop(task);
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
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
    fn abandon(&self) -> SlotAbandonment<T> {
        let mut state = self.state.lock();
        let abandonment = match &mut *state {
            SlotState::Pending { task, .. } => {
                let task = task.take();
                *state = SlotState::Abandoned;
                SlotAbandonment::Unresolved(task)
            },
            SlotState::Done(_) => {
                let SlotState::Done(value) = mem::replace(&mut *state, SlotState::Taken) else {
                    unreachable!()
                };
                SlotAbandonment::Completed(value)
            },
            SlotState::Taken => SlotAbandonment::Settled,
            SlotState::Abandoned => SlotAbandonment::AlreadyAbandoned,
        };
        drop(state);
        abandonment
    }

    /// Await the outcome from the tokio side.
    pub(crate) const fn wait(self: Arc<Self>, shard: Shard) -> SlotFuture<T, Guard> {
        SlotFuture {
            slot: self,
            shard,
            completed: false,
        }
    }
}

pub(crate) struct SlotFuture<T: SlotValue, Guard = ()> {
    slot: Arc<TaskSlot<T, Guard>>,
    shard: Shard,
    completed: bool,
}

impl<T: SlotValue, Guard> Future for SlotFuture<T, Guard> {
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
                // Both are `Future::poll` contract violations, reachable only
                // from a bug in this crate: a slot is taken exactly once and an
                // abandoned slot is never polled again. Panicking is the
                // contract's own remedy — returning `Pending` would hang the
                // task and `Ready` would hand out a moved value.
                #[expect(
                    clippy::panic,
                    reason = "polling a completed or abandoned slot violates the Future contract"
                )]
                SlotState::Taken => panic!("slot future polled after completion"),
                #[expect(
                    clippy::panic,
                    reason = "polling a completed or abandoned slot violates the Future contract"
                )]
                SlotState::Abandoned => panic!("abandoned slot future polled"),
            }
        };
        if poll.is_ready() {
            self.completed = true;
        }
        poll
    }
}

impl<T: SlotValue, Guard> Drop for SlotFuture<T, Guard> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        match self.slot.abandon() {
            SlotAbandonment::Unresolved(Some(task)) => {
                self.shard.push(super::PumpEvent::CancelTask { task });
            },
            SlotAbandonment::Unresolved(None)
            | SlotAbandonment::Settled
            | SlotAbandonment::AlreadyAbandoned => {},
            SlotAbandonment::Completed(value) => value.report_unaccepted(&self.shard),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

    use pyo3::Python;

    use super::{SlotAbandonment, SlotState, TaskSlot};
    use crate::pyloop::ShardHandle;

    #[pyo3::pyclass]
    struct SlotAnchor {
        slot: Arc<TaskSlot<()>>,
        dropped_outside_lock: Arc<AtomicBool>,
    }

    impl Drop for SlotAnchor {
        fn drop(&mut self) {
            self.dropped_outside_lock
                .store(self.slot.state.try_lock().is_some(), Ordering::Relaxed);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn completion_releases_task_anchor_outside_slot_lock() {
        Python::initialize();
        Python::attach(|py| {
            let slot = TaskSlot::<()>::new();
            let dropped_outside_lock = Arc::new(AtomicBool::new(false));
            let task = pyo3::Py::new(py, SlotAnchor {
                slot: Arc::clone(&slot),
                dropped_outside_lock: Arc::clone(&dropped_outside_lock),
            })
            .expect("task anchor");
            assert!(slot.set_task(task.into_any()).is_none());
            slot.fill(()).expect("completion");
            assert!(dropped_outside_lock.load(Ordering::Relaxed));
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn task_created_after_abandonment_is_returned_for_cancellation() {
        Python::initialize();
        Python::attach(|py| {
            let slot = TaskSlot::<()>::new();
            assert!(matches!(slot.abandon(), SlotAbandonment::Unresolved(None)));
            assert!(slot.set_task(py.None()).is_some());
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
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
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn dropping_an_unpolled_completed_future_discards_the_value() {
        Python::initialize();
        let shard = Python::attach(ShardHandle::test_stub);
        let slot = TaskSlot::<()>::new();
        slot.fill(()).expect("completed slot accepts its value");

        drop(Arc::clone(&slot).wait(shard));
        assert!(matches!(*slot.state.lock(), SlotState::Taken));
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn completion_abandonment_race_resolves_to_exactly_one_transition() {
        Python::initialize();
        let shard = Python::attach(ShardHandle::test_stub);
        for _ in 0..64 {
            let slot = TaskSlot::<()>::new();
            let future = Arc::clone(&slot).wait(Arc::clone(&shard));
            let fill_slot = Arc::clone(&slot);
            let barrier = Arc::new(Barrier::new(2));

            let fill_result = thread::scope(|scope| {
                let fill_barrier = Arc::clone(&barrier);
                let fill = scope.spawn(move || {
                    fill_barrier.wait();
                    fill_slot.fill(())
                });
                let abandon = scope.spawn(move || {
                    barrier.wait();
                    drop(future);
                });
                abandon.join().expect("waiter abandonment does not panic");
                fill.join().expect("slot completion does not panic")
            });

            let state = slot.state.lock();
            assert!(
                matches!(
                    (&*state, fill_result),
                    (SlotState::Taken, Ok(())) | (SlotState::Abandoned, Err(()))
                ),
                "completion acceptance and abandonment state agree"
            );
            drop(state);
        }
    }
}
