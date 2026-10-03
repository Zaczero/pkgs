use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::io::Read as _;
use std::os::fd::OwnedFd;
use std::time::{Duration, Instant};

use procfs_core::FromRead as _;
use procfs_core::process::Stat;
use serde::Serialize;

use super::{decimal, sys};

#[derive(Debug, Default, Serialize)]
pub struct Activity {
    pub tasks: Vec<Task>,
    pub sweeps: usize,
    pub limited: bool,
    pub clock_ticks_per_second: u64,
    pub pid_namespace: Option<String>,
    pub final_interval_ms: u64,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Sample {
    #[serde(serialize_with = "decimal")]
    pub elapsed_ns: u64,
    #[serde(serialize_with = "decimal")]
    pub cpu_ticks: u64,
}

#[derive(Debug, Serialize)]
pub struct Task {
    pub pid: i32,
    pub tid: i32,
    pub process_start_ticks: u64,
    pub start_ticks: u64,
    pub parent_pid: i32,
    pub last_parent_pid: i32,
    pub name: String,
    pub name_changes: usize,
    pub observations: usize,
    pub maximum_observed_rss_bytes: Option<u64>,
    pub samples: Vec<Sample>,
}

pub(super) struct Sampler {
    activity: Activity,
    identities: BTreeMap<(i32, u64), usize>,
    processes: BTreeMap<i32, OwnedFd>,
    interval: Duration,
    page_size: u64,
    sweep: Option<Sweep>,
}

struct Sweep {
    queue: VecDeque<(i32, Option<i32>)>,
    visited: BTreeSet<i32>,
    directory: Option<(i32, u64, fs::ReadDir)>,
    remaining: usize,
    cpu: Duration,
}

impl Task {
    pub fn cpu_percent(&self, ticks: u64) -> Option<f64> {
        let first = self.samples.first()?;
        let last = self.samples.last()?;
        let elapsed = last.elapsed_ns.checked_sub(first.elapsed_ns)?;
        if elapsed == 0 || ticks == 0 {
            return None;
        }
        Some(
            100.0 * last.cpu_ticks.saturating_sub(first.cpu_ticks) as f64
                / ticks as f64
                / (elapsed as f64 / 1e9),
        )
    }

    fn observe(&mut self, stat: Stat, sample: Sample, rss: Option<u64>) {
        if self.samples.len() == 256 {
            let mut index = 0_usize;
            self.samples.retain(|_| {
                let keep = index.is_multiple_of(2);
                index = index.wrapping_add(1);
                keep
            });
        }
        self.samples.push(sample);
        self.observations = self.observations.wrapping_add(1);
        self.maximum_observed_rss_bytes = self.maximum_observed_rss_bytes.max(rss);
        self.last_parent_pid = stat.ppid;
        if self.name != stat.comm {
            self.name = stat.comm;
            self.name_changes = self.name_changes.wrapping_add(1);
        }
    }
}

impl Sampler {
    pub(super) fn new(enabled: bool) -> Self {
        // SAFETY: sysconf reads process-wide constants without pointer arguments.
        let (ticks, page_size) = unsafe {
            (
                libc::sysconf(libc::_SC_CLK_TCK),
                libc::sysconf(libc::_SC_PAGESIZE),
            )
        };
        Self {
            activity: Activity {
                clock_ticks_per_second: ticks as u64,
                pid_namespace: enabled
                    .then(|| fs::read_link("/proc/self/ns/pid").ok())
                    .flatten()
                    .map(|p| p.to_string_lossy().into_owned()),
                ..Activity::default()
            },
            identities: BTreeMap::new(),
            processes: BTreeMap::new(),
            interval: Duration::from_millis(100),
            page_size: page_size as u64,
            sweep: None,
        }
    }

    pub(super) fn sample(&mut self, root: i32, start: Instant) -> Duration {
        let cpu_start = cpu_time();
        let mut sweep = self.sweep.take().unwrap_or_else(|| Sweep {
            queue: VecDeque::from([(root, None)]),
            visited: BTreeSet::new(),
            directory: None,
            remaining: 1024,
            cpu: Duration::ZERO,
        });
        let mut done = false;
        for _ in 0..16 {
            if sweep.remaining == 0 {
                self.activity.limited = true;
                done = true;
                break;
            }
            sweep.remaining = sweep.remaining.wrapping_sub(1);
            if sweep.directory.is_none() {
                let Some((pid, parent)) = sweep.queue.pop_front() else {
                    done = true;
                    break;
                };
                if !sweep.visited.insert(pid) {
                    continue;
                }
                let Some(directory) = self.open_process(pid, parent) else {
                    continue;
                };
                sweep.directory = Some(directory);
            }
            let (pid, generation, entries) = sweep.directory.as_mut().expect("active directory");
            if let Some(Ok(entry)) = entries.next() {
                let path = entry.path();
                if let Ok(file) = fs::File::open(path.join("children")) {
                    let mut children = String::new();
                    if file.take(64 * 1024).read_to_string(&mut children).is_ok() {
                        if children.len() == 64 * 1024 {
                            children.truncate(children.rfind(' ').unwrap_or(0));
                            self.activity.limited = true;
                        }
                        sweep.queue.extend(
                            children
                                .split_whitespace()
                                .filter_map(|p| p.parse::<i32>().ok())
                                .take(sweep.remaining.saturating_sub(sweep.queue.len()))
                                .map(|child| (child, Some(*pid))),
                        );
                    }
                }
                let Ok(bytes) = fs::read(path.join("stat")) else {
                    continue;
                };
                let Ok(record) = Stat::from_read(bytes.as_slice()) else {
                    continue;
                };
                self.observe(*pid, *generation, record, start.elapsed());
            } else {
                sweep.directory = None;
            }
        }
        let cost = cpu_time().saturating_sub(cpu_start);
        sweep.cpu = sweep.cpu.saturating_add(cost);
        self.interval = self.interval.max(sweep.cpu.saturating_mul(200));
        if done {
            self.activity.sweeps = self.activity.sweeps.wrapping_add(1);
            self.interval
        } else {
            self.sweep = Some(sweep);
            cost.saturating_mul(200)
        }
    }

    fn open_process(&mut self, pid: i32, parent: Option<i32>) -> Option<(i32, u64, fs::ReadDir)> {
        let descriptor = sys::pidfd(pid).ok()?;
        let bytes = fs::read(format!("/proc/{pid}/stat")).ok()?;
        let stat = Stat::from_read(bytes.as_slice()).ok()?;
        if !sys::alive(&descriptor)
            || parent.is_some_and(|parent| {
                stat.ppid != std::process::id() as i32
                    && (stat.ppid != parent || !self.processes.get(&parent).is_some_and(sys::alive))
            })
        {
            return None;
        }
        let entries = fs::read_dir(format!("/proc/{pid}/task")).ok()?;
        if self.processes.len() < 1024 || self.processes.contains_key(&pid) {
            self.processes.insert(pid, descriptor);
        } else {
            self.activity.limited = true;
        }
        Some((pid, stat.starttime, entries))
    }

    fn observe(&mut self, process: i32, generation: u64, stat: Stat, elapsed: Duration) {
        let key = (stat.pid, stat.starttime);
        let sample = Sample {
            elapsed_ns: elapsed.as_nanos() as u64,
            cpu_ticks: stat.utime.saturating_add(stat.stime),
        };
        let memory = (process == stat.pid).then_some(stat.rss.saturating_mul(self.page_size));
        if let Some(&index) = self.identities.get(&key) {
            self.activity
                .tasks
                .get_mut(index)
                .expect("stored task index")
                .observe(stat, sample, memory);
        } else if self.activity.tasks.len() < 1024 {
            self.identities.insert(key, self.activity.tasks.len());
            self.activity.tasks.push(Task {
                pid: process,
                tid: stat.pid,
                process_start_ticks: generation,
                start_ticks: stat.starttime,
                parent_pid: stat.ppid,
                last_parent_pid: stat.ppid,
                name: stat.comm,
                name_changes: 0,
                observations: 1,
                maximum_observed_rss_bytes: memory,
                samples: vec![sample],
            });
        } else {
            self.activity.limited = true;
        }
    }

    pub(super) fn signal(&self, signal: i32) {
        for fd in self.processes.values() {
            sys::signal_pidfd(fd, signal);
        }
    }

    pub(super) fn finish(mut self) -> Activity {
        self.activity.limited |= self.sweep.is_some();
        self.activity.final_interval_ms = self.interval.as_millis() as u64;
        self.activity
    }
}

fn cpu_time() -> Duration {
    // SAFETY: valid output pointer and supported process CPU clock.
    unsafe {
        let mut time: libc::timespec = std::mem::zeroed();
        libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &raw mut time);
        Duration::new(time.tv_sec as u64, time.tv_nsec as u32)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::time::Instant;

    #[test]
    fn large_sweep_yields_and_reports_incomplete_detail() {
        let barrier = Barrier::new(33);
        let (delay, activity) = std::thread::scope(|scope| {
            for _ in 0..32 {
                scope.spawn(|| {
                    barrier.wait();
                    barrier.wait();
                });
            }
            barrier.wait();
            let mut sampler = super::Sampler::new(true);
            let delay = sampler.sample(std::process::id() as i32, Instant::now());
            let activity = sampler.finish();
            barrier.wait();
            (delay, activity)
        });
        assert!(!delay.is_zero());
        assert!(!activity.tasks.is_empty());
        assert_eq!(activity.sweeps, 0);
        assert!(activity.limited);
    }
}
