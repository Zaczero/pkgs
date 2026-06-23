//! Outcome slot connecting a pump-created asyncio Task to the tokio task
//! driving the connection. Replaces the futures oneshot + `PyTaskCompleter`
//! chain from pyo3-async-runtimes.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use parking_lot::Mutex;
use pyo3::prelude::*;

enum SlotState<T> {
    Pending(Option<Waker>),
    Done(T),
    Taken,
}

/// Single-value rendezvous filled by the pump (loop thread) and awaited by a
/// tokio task. Also carries the `asyncio.Task` handle for cancellation.
pub(crate) struct TaskSlot<T> {
    state: Mutex<SlotState<T>>,
    task: Mutex<Option<Py<PyAny>>>,
}

impl<T> TaskSlot<T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(SlotState::Pending(None)),
            task: Mutex::new(None),
        })
    }

    /// Fill the slot and wake the awaiting tokio task. Called by the pump.
    pub(crate) fn fill(&self, value: T) {
        let waker = {
            let mut state = self.state.lock();
            match &mut *state {
                SlotState::Pending(waker) => {
                    let waker = waker.take();
                    *state = SlotState::Done(value);
                    waker
                },
                // A second completion (e.g. done-callback after an error was
                // already recorded) is dropped; first outcome wins.
                SlotState::Done(_) | SlotState::Taken => None,
            }
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Record the `asyncio.Task` driving this slot. The strong reference is
    /// load-bearing: event loops keep only weak references to tasks, so the
    /// slot anchors the Task for the lifetime of the request that awaits it.
    pub(crate) fn set_task(&self, task: Py<PyAny>) {
        *self.task.lock() = Some(task);
    }

    /// Await the outcome from the tokio side.
    pub(crate) fn wait(self: &Arc<Self>) -> SlotFuture<T> {
        SlotFuture(Arc::clone(self))
    }
}

pub(crate) struct SlotFuture<T>(Arc<TaskSlot<T>>);

impl<T> Future for SlotFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.0.state.lock();
        match &mut *state {
            SlotState::Pending(waker) => {
                match waker {
                    Some(existing) => existing.clone_from(cx.waker()),
                    None => *waker = Some(cx.waker().clone()),
                }
                Poll::Pending
            },
            state @ SlotState::Done(_) => {
                let SlotState::Done(value) = std::mem::replace(state, SlotState::Taken) else {
                    unreachable!()
                };
                Poll::Ready(value)
            },
            SlotState::Taken => panic!("slot future polled after completion"),
        }
    }
}
