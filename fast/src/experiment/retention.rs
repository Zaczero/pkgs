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
        let hooks = 1_usize
            .wrapping_add(usize::from(options.prepare.is_some()))
            .wrapping_add(usize::from(options.verify.is_some()));
        let invocations_per_run = commands
            .checked_mul(hooks)
            .ok_or_else(|| io::Error::other("too many commands"))?;
        // Namespace and unavailable-counter text are compacted and bounded on admission.
        let record_bytes = size_of::<Invocation>()
            .wrapping_add(TEXT_BYTES)
            .wrapping_add(counters.wrapping_mul(size_of::<Counter>().wrapping_add(TEXT_BYTES)));
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
            .saturating_add(fixed.unwrap_or(8));
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
                .wrapping_sub(TERMINAL_RESERVE)
                .wrapping_sub(runs.wrapping_mul(bytes_per_run)),
            task_detail_omitted: false,
        })
    }

    pub(super) const fn runs(&self) -> usize {
        self.runs
    }

    pub(super) fn reserve(&self, records: &mut Vec<Invocation>, runs: usize) {
        let count = runs.wrapping_mul(self.invocations_per_run);
        assert!(runs <= self.runs, "run plan was admitted before execution");
        if records.capacity() < count {
            records.reserve_exact(count.wrapping_sub(records.len()));
        }
        assert!(records.capacity() <= self.runs.wrapping_mul(self.invocations_per_run));
    }

    pub(super) fn retain(&mut self, observation: &mut Observation) {
        observation.counters = std::mem::take(&mut observation.counters)
            .into_boxed_slice()
            .into_vec();
        for counter in &mut observation.counters {
            if let Some(reason) = &mut counter.unavailable {
                *reason = bounded_text(std::mem::take(reason));
            }
        }
        if let Some(namespace) = &mut observation.activity.pid_namespace {
            *namespace = bounded_text(std::mem::take(namespace));
        }
        let mut bytes = 0;
        let mut kept = 0_usize;
        for task in &observation.activity.tasks {
            let next = size_of::<Task>().wrapping_add(task_bytes(task));
            if next > self.detail_remaining.wrapping_sub(bytes) {
                break;
            }
            bytes = bytes.wrapping_add(next);
            kept = kept.wrapping_add(1);
        }
        if kept < observation.activity.tasks.len() {
            observation.activity.limited = true;
            self.task_detail_omitted = true;
            observation.activity.tasks.truncate(kept);
        }
        observation.activity.tasks = std::mem::take(&mut observation.activity.tasks)
            .into_boxed_slice()
            .into_vec();
        self.detail_remaining = self.detail_remaining.wrapping_sub(bytes);
        debug_assert!(
            size_of::<Invocation>().wrapping_add(metadata_bytes(observation)) <= self.record_bytes
        );
        debug_assert!(
            observation
                .diagnostics
                .first
                .capacity()
                .wrapping_add(observation.diagnostics.last.capacity())
                <= 2 * Diagnostics::CAPTURE_BYTES
        );
    }

    pub(super) fn summary(&self, records: &Vec<Invocation>, stop_reason: &String) -> Summary {
        let retained_bytes = size_of_val(records)
            .wrapping_add(stop_reason.capacity())
            .wrapping_add(records.capacity().wrapping_mul(size_of::<Invocation>()))
            .wrapping_add(
                records
                    .iter()
                    .map(|record| match &record.outcome {
                        Collected::Observation(observation) => metadata_bytes(observation)
                            .wrapping_add(
                                observation
                                    .activity
                                    .tasks
                                    .capacity()
                                    .wrapping_mul(size_of::<Task>()),
                            )
                            .wrapping_add(
                                observation
                                    .activity
                                    .tasks
                                    .iter()
                                    .map(task_bytes)
                                    .sum::<usize>(),
                            )
                            .wrapping_add(observation.diagnostics.first.capacity())
                            .wrapping_add(observation.diagnostics.last.capacity()),
                        Collected::CollectorError(error) => error.capacity(),
                    })
                    .sum::<usize>(),
            );
        assert!(retained_bytes <= ALLOWANCE, "admitted observation storage");
        Summary {
            allowance_bytes: ALLOWANCE,
            retained_bytes,
            task_detail_omitted: self.task_detail_omitted,
        }
    }
}

fn metadata_bytes(observation: &Observation) -> usize {
    observation
        .counters
        .capacity()
        .wrapping_mul(size_of::<Counter>())
        .wrapping_add(
            observation
                .counters
                .iter()
                .filter_map(|counter| counter.unavailable.as_ref())
                .map(String::capacity)
                .sum::<usize>(),
        )
        .wrapping_add(
            observation
                .activity
                .pid_namespace
                .as_ref()
                .map_or(0, String::capacity),
        )
}

const fn task_bytes(task: &Task) -> usize {
    task.name
        .capacity()
        .wrapping_add(task.samples.capacity().wrapping_mul(size_of::<Sample>()))
}

pub(super) fn bounded_text(mut text: String) -> String {
    const OMITTED: &str = " [truncated]";
    if text.len() > TEXT_BYTES {
        let mut end = TEXT_BYTES.wrapping_sub(OMITTED.len());
        while !text.is_char_boundary(end) {
            end = end.wrapping_sub(1);
        }
        text.truncate(end);
        text.push_str(OMITTED);
    }
    text.into_boxed_str().into_string()
}

#[cfg(test)]
mod tests;
