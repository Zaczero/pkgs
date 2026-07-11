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

/// Single-value rendezvous filled by the pump (loop thread) and awaited by a
/// tokio task. Also carries the `asyncio.Task` handle for cancellation.
pub(crate) struct TaskSlot<T> {
    state: Mutex<SlotState<T>>,
}

impl<T> TaskSlot<T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(SlotState::Pending {
                waker: None,
                task: None,
            }),
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
    fn abandon(&self) -> Option<Py<PyAny>> {
        let mut state = self.state.lock();
        let previous = mem::replace(&mut *state, SlotState::Abandoned);
        drop(state);
        match previous {
            SlotState::Pending { task, .. } => task,
            SlotState::Done(_) | SlotState::Taken | SlotState::Abandoned => None,
        }
    }

    /// Await the outcome from the tokio side.
    pub(crate) fn wait(self: &Arc<Self>, shard: Shard) -> SlotFuture<T> {
        SlotFuture {
            slot: Arc::clone(self),
            shard,
            completed: false,
        }
    }
}

pub(crate) struct SlotFuture<T> {
    slot: Arc<TaskSlot<T>>,
    shard: Shard,
    completed: bool,
}

impl<T> Future for SlotFuture<T> {
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

impl<T> Drop for SlotFuture<T> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        if let Some(task) = self.slot.abandon() {
            self.shard.push(super::PumpEvent::CancelTask { task });
        }
    }
}

#[cfg(test)]
mod tests {
    use pyo3::Python;

    use super::TaskSlot;

    #[test]
    fn task_created_after_abandonment_is_returned_for_cancellation() {
        Python::initialize();
        Python::attach(|py| {
            let slot = TaskSlot::<()>::new();
            assert!(slot.abandon().is_none());
            assert!(slot.set_task(py.None()).is_some());
        });
    }

    #[test]
    fn abandoning_a_running_task_returns_its_anchor_once() {
        Python::initialize();
        Python::attach(|py| {
            let slot = TaskSlot::<()>::new();
            assert!(slot.set_task(py.None()).is_none());
            assert!(slot.abandon().is_some());
            assert!(slot.abandon().is_none());
        });
    }
}
