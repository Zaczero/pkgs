//! Checked admission and accounting for the observation-retention allowance.

use std::io;

use serde::Serialize;

use super::{Collected, Invocation};
use crate::cli::Cli;
use crate::runner::{Counter, Diagnostics, Observation, Sample, Task};

const ALLOWANCE: usize = 128 * 1024 * 1024;
const DETAIL_RESERVE: usize = 8 * 1024 * 1024;
const TEXT_BYTES: usize = 256;
// Repetition stops on the first failure. Its outcome and diagnostics must fit even
// after every admitted success, independently of the timings or sampled activity.
const TERMINAL_RESERVE: usize =
    size_of::<Vec<Invocation>>() + 2 * Diagnostics::CAPTURE_BYTES + 2 * TEXT_BYTES;

#[derive(Debug, Default, Serialize)]
pub(crate) struct Summary {
    pub allowance_bytes: usize,
    pub retained_bytes: usize,
    pub task_detail_omitted: bool,
}

pub(super) struct Storage {
    runs: usize,
    invocations_per_run: usize,
    record_bytes: usize,
    detail_remaining: usize,
    task_detail_omitted: bool,
}

impl Storage {
    pub(super) fn new(options: &Cli, commands: usize, counters: usize) -> io::Result<Self> {
        // Namespace and unavailable-counter text are compacted and bounded on admission.
        const COUNTER_BYTES: usize = size_of::<Counter>() + TEXT_BYTES;

        let hooks = [true, options.prepare.is_some(), options.verify.is_some()]
            .into_iter()
            .filter(|&present| present)
            .count();
        let invocations_per_run = commands
            .checked_mul(hooks)
            .ok_or_else(|| io::Error::other("too many commands"))?;
        let record_bytes = counters
            .checked_mul(COUNTER_BYTES)
            .and_then(|bytes| bytes.checked_add(size_of::<Invocation>()))
            .and_then(|bytes| bytes.checked_add(TEXT_BYTES))
            .ok_or_else(|| io::Error::other("counter storage size overflows"))?;
        let bytes_per_run = invocations_per_run
            .checked_mul(record_bytes)
            .ok_or_else(|| io::Error::other("too many commands"))?;
        let capacity = (ALLOWANCE - DETAIL_RESERVE - TERMINAL_RESERVE)
            .checked_div(bytes_per_run)
            .expect("at least one command");
        let fixed = options
            .runs
            .map(std::num::NonZeroUsize::get)
            .or_else(|| options.once.then_some(1));
        let required = options
            .warmup
            .unwrap_or(0)
            .checked_add(fixed.unwrap_or(8))
            .ok_or_else(|| io::Error::other("requested run count overflows"))?;
        if required > capacity {
            return Err(io::Error::other(format!(
                "requested runs exceed the 128 MiB observation allowance; at most {capacity} total runs fit"
            )));
        }
        let runs = fixed.map_or(capacity, |_| required);
        Ok(Self {
            runs,
            invocations_per_run,
            record_bytes,
            detail_remaining: ALLOWANCE
                .checked_sub(TERMINAL_RESERVE)
                .and_then(|bytes| {
                    bytes.checked_sub(
                        runs.checked_mul(bytes_per_run)
                            .expect("admitted run storage fits usize"),
                    )
                })
                .expect("admitted runs leave the detail reserve"),
            task_detail_omitted: false,
        })
    }

    pub(super) const fn runs(&self) -> usize {
        self.runs
    }

    pub(super) fn reserve(&self, records: &mut Vec<Invocation>, runs: usize) {
        assert!(runs <= self.runs, "run plan was admitted before execution");
        let count = runs
            .checked_mul(self.invocations_per_run)
            .expect("admitted invocation count");
        if records.capacity() < count {
            records.reserve_exact(
                count
                    .checked_sub(records.len())
                    .expect("capacity is below the plan"),
            );
        }
        assert!(
            records.capacity()
                <= self
                    .runs
                    .checked_mul(self.invocations_per_run)
                    .expect("admitted invocation count")
        );
    }

    pub(super) fn retain(&mut self, observation: &mut Observation) {
        observation.counters = std::mem::take(&mut observation.counters)
            .into_boxed_slice()
            .into_vec();
        for counter in &mut observation.counters {
            if let Err(reason) = &mut counter.reading {
                *reason = bounded_text(std::mem::take(reason));
            }
        }
        if let Some(namespace) = &mut observation.activity.pid_namespace {
            *namespace = bounded_text(std::mem::take(namespace));
        }
        let mut remaining = self.detail_remaining;
        let mut kept = 0_usize;
        for task in &observation.activity.tasks {
            let next = sum_bytes([size_of::<Task>(), task_bytes(task)]);
            let Some(after) = remaining.checked_sub(next) else {
                break;
            };
            remaining = after;
            kept = kept
                .checked_add(1)
                .expect("retained tasks fit their vector");
        }
        if kept < observation.activity.tasks.len() {
            observation.activity.limited = true;
            self.task_detail_omitted = true;
            observation.activity.tasks.truncate(kept);
        }
        observation.activity.tasks = std::mem::take(&mut observation.activity.tasks)
            .into_boxed_slice()
            .into_vec();
        self.detail_remaining = remaining;
        debug_assert!(
            sum_bytes([size_of::<Invocation>(), metadata_bytes(observation)]) <= self.record_bytes
        );
        debug_assert!(
            sum_bytes([
                observation.diagnostics.first.capacity(),
                observation.diagnostics.last.capacity()
            ]) <= 2 * Diagnostics::CAPTURE_BYTES
        );
    }

    pub(super) fn summary(&self, records: &Vec<Invocation>, stop_reason: &String) -> Summary {
        let retained_bytes = sum_bytes([
            size_of_val(records),
            stop_reason.capacity(),
            records
                .capacity()
                .checked_mul(size_of::<Invocation>())
                .expect("allocated records fit usize"),
            sum_bytes(records.iter().map(|record| {
                match &record.outcome {
                    Collected::Observation(observation) => sum_bytes([
                        metadata_bytes(observation),
                        observation
                            .activity
                            .tasks
                            .capacity()
                            .checked_mul(size_of::<Task>())
                            .expect("allocated tasks fit usize"),
                        sum_bytes(observation.activity.tasks.iter().map(task_bytes)),
                        observation.diagnostics.first.capacity(),
                        observation.diagnostics.last.capacity(),
                    ]),
                    Collected::CollectorError(error) => error.capacity(),
                }
            })),
        ]);
        assert!(retained_bytes <= ALLOWANCE, "admitted observation storage");
        Summary {
            allowance_bytes: ALLOWANCE,
            retained_bytes,
            task_detail_omitted: self.task_detail_omitted,
        }
    }
}

/// Sum allocation extents without allowing accounting to wrap below the allowance.
fn sum_bytes(parts: impl IntoIterator<Item = usize>) -> usize {
    parts
        .into_iter()
        .try_fold(0_usize, usize::checked_add)
        .expect("live allocation extents fit the address space")
}

fn metadata_bytes(observation: &Observation) -> usize {
    sum_bytes([
        observation
            .counters
            .capacity()
            .checked_mul(size_of::<Counter>())
            .expect("allocated counters fit usize"),
        sum_bytes(
            observation
                .counters
                .iter()
                .filter_map(|counter| counter.reading.as_ref().err())
                .map(String::capacity),
        ),
        observation
            .activity
            .pid_namespace
            .as_ref()
            .map_or(0, String::capacity),
    ])
}

fn task_bytes(task: &Task) -> usize {
    sum_bytes([
        task.name.capacity(),
        task.samples
            .capacity()
            .checked_mul(size_of::<Sample>())
            .expect("allocated samples fit usize"),
    ])
}

pub(super) fn bounded_text(mut text: String) -> String {
    const OMITTED: &str = " [truncated]";
    if text.len() > TEXT_BYTES {
        const PREFIX_BYTES: usize = TEXT_BYTES - OMITTED.len();
        let end = text.floor_char_boundary(PREFIX_BYTES);
        text.truncate(end);
        text.push_str(OMITTED);
    }
    text.into_boxed_str().into_string()
}

#[cfg(test)]
mod tests;
