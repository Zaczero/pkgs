mod activity;
mod perf;
mod sys;

use std::collections::VecDeque;
use std::ffi::{CString, OsStr, OsString};
use std::fs::{self, File};
use std::io::{self, Read as _};
use std::os::fd::{AsRawFd as _, OwnedFd};
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub use activity::{Activity, Sample, Task};
pub use perf::Counter;
use serde::Serialize;
pub use sys::check_jobserver;

#[derive(Clone, Copy, Debug)]
pub enum InvocationKind {
    Command,
    Hook,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Outcome {
    Success,
    Exit { code: i32 },
    Signal { signal: i32 },
    Timeout,
    Interrupted { signal: i32 },
    ExecError { errno: i32 },
    BackgroundDescendants,
}

#[derive(Clone, Debug)]
pub struct Command {
    pub argv: Vec<OsString>,
}

#[derive(Debug, Serialize)]
pub struct Observation {
    #[serde(serialize_with = "decimal")]
    pub elapsed_ns: u64,
    #[serde(serialize_with = "decimal")]
    pub release_ns: u64,
    #[serde(serialize_with = "decimal")]
    pub post_exit_drain_ns: u64,
    pub descendants_survived: bool,
    pub cleanup_incomplete: bool,
    #[serde(serialize_with = "decimal")]
    pub user_ns: u64,
    #[serde(serialize_with = "decimal")]
    pub system_ns: u64,
    pub max_rss_bytes: u64,
    pub launch_max_rss_bytes: Option<u64>,
    pub minor_faults: i64,
    pub major_faults: i64,
    pub voluntary_switches: i64,
    pub involuntary_switches: i64,
    pub outcome: Outcome,
    pub counters: Vec<perf::Counter>,
    pub activity: Activity,
    pub diagnostics: Diagnostics,
}

impl Outcome {
    pub const fn success(self) -> bool {
        matches!(self, Self::Success)
    }
}

#[derive(Debug, Default, Serialize)]
pub struct Diagnostics {
    pub first: Vec<u8>,
    pub last: VecDeque<u8>,
    pub omitted_bytes: u64,
    pub pipe_still_open: bool,
}

impl Diagnostics {
    pub const CAPTURE_BYTES: usize = 32 * 1024;

    pub fn discard(&mut self) {
        self.first = Vec::new();
        self.last = VecDeque::new();
    }

    fn push(&mut self, bytes: &[u8]) {
        const CAPACITY: usize = Diagnostics::CAPTURE_BYTES;
        let front = CAPACITY.wrapping_sub(self.first.len()).min(bytes.len());
        let (first, tail) = bytes.split_at(front);
        self.first.extend_from_slice(first);
        let removed = self
            .last
            .len()
            .wrapping_add(tail.len())
            .saturating_sub(CAPACITY);
        drop(self.last.drain(..removed));
        self.omitted_bytes = self.omitted_bytes.wrapping_add(removed as u64);
        self.last.extend(tail);
    }

    fn drain(&mut self, pipe: &mut File) -> io::Result<bool> {
        let mut buffer = [0_u8; 8192];
        // Bound each readiness turn so an endless writer cannot starve exit/timeout.
        for _ in 0..16 {
            match pipe.read(&mut buffer) {
                Ok(0) => return Ok(true),
                Ok(n) => self.push(buffer.split_at(n).0),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {},
                Err(error) => return Err(error),
            }
        }
        Ok(false)
    }
}

pub struct Runner {
    signals: OwnedFd,
    original_mask: libc::sigset_t,
    pub interrupted: Option<i32>,
    counters: perf::Policy,
    observe: bool,
    null: File,
}

impl Runner {
    pub fn new(observe: bool) -> io::Result<Self> {
        let null = File::options().read(true).write(true).open("/dev/null")?;
        let null = File::from(sys::above_stdio(null.into())?);
        sys::subreaper()?;
        let counters = perf::Policy::new();
        let (signals, original_mask) = sys::signals()?;
        Ok(Self {
            signals,
            original_mask,
            interrupted: None,
            counters,
            observe,
            null,
        })
    }

    pub const fn counter_policy(&self) -> &perf::Policy {
        &self.counters
    }

    pub fn collect_signals(&mut self) -> io::Result<()> {
        while let Some(signal) = sys::read_signal(&self.signals)? {
            if signal != libc::SIGCHLD {
                self.interrupted = Some(signal);
            }
        }
        Ok(())
    }

    #[expect(
        clippy::too_many_lines,
        reason = "One contiguous lifecycle owns launch, completion, cancellation, and cleanup"
    )]
    pub fn invoke(
        &mut self,
        command: &Command,
        input: Option<&Path>,
        timeout: Option<Duration>,
        environment: &[(String, String)],
        kind: InvocationKind,
    ) -> io::Result<Observation> {
        let observe = self.observe && matches!(kind, InvocationKind::Command);
        let argv: Vec<_> = command
            .argv
            .iter()
            .map(|arg| CString::new(arg.as_bytes()).map_err(io::Error::other))
            .collect::<io::Result<_>>()?;
        let program = argv
            .first()
            .ok_or_else(|| io::Error::other("empty command"))?;
        let paths = executable_paths(OsStr::from_bytes(program.as_bytes()))?;
        let mut env: Vec<(OsString, OsString)> = std::env::vars_os().collect();
        for (key, value) in environment {
            env.retain(|(name, _)| name != key.as_str());
            env.push((key.into(), value.into()));
        }
        let env: Vec<_> = env
            .into_iter()
            .map(|(key, value)| {
                let mut bytes = key.as_bytes().to_vec();
                bytes.push(b'=');
                bytes.extend_from_slice(value.as_bytes());
                CString::new(bytes).map_err(io::Error::other)
            })
            .collect::<io::Result<_>>()?;
        let mut argv_ptr: Vec<_> = argv.iter().map(|s| s.as_ptr()).collect();
        argv_ptr.push(std::ptr::null());
        let mut env_ptr: Vec<_> = env.iter().map(|s| s.as_ptr()).collect();
        env_ptr.push(std::ptr::null());
        let input = input
            .map(|path| sys::above_stdio(File::open(path)?.into()).map(File::from))
            .transpose()?;
        if let Some(file) = &input
            && !file.metadata()?.is_file()
        {
            return Err(io::Error::other("--input must name a regular file"));
        }
        let (stderr_read, stderr_write) = sys::pipe()?;
        let (release_read, release_write) = sys::launch_channel()?;
        let (exec_read, exec_write) = sys::pipe()?;
        sys::nonblocking(&stderr_read)?;
        sys::nonblocking(&exec_read)?;
        sys::nonblocking(&release_write)?;
        let mut diagnostics = Diagnostics {
            first: Vec::with_capacity(Diagnostics::CAPTURE_BYTES),
            last: VecDeque::with_capacity(Diagnostics::CAPTURE_BYTES),
            ..Diagnostics::default()
        };
        let mut sampler = activity::Sampler::new(observe);
        let launch = sys::Launch {
            input: input.as_ref().unwrap_or(&self.null).as_raw_fd(),
            output: if matches!(kind, InvocationKind::Hook) {
                stderr_write.as_raw_fd()
            } else {
                self.null.as_raw_fd()
            },
            error: stderr_write.as_raw_fd(),
            release: release_read.as_raw_fd(),
            release_write: release_write.as_raw_fd(),
            exec_error: exec_write.as_raw_fd(),
            mask: &self.original_mask,
            paths: &paths,
            argv: &argv_ptr,
            env: &env_ptr,
        };
        let start = Instant::now();
        let pid = sys::spawn(&launch)?;
        let mut child = sys::Child::new(pid);
        drop(stderr_write);
        drop(release_read);
        drop(exec_write);
        let pidfd = sys::pidfd(pid)?;
        let events = observe.then(|| self.counters.attach(pid));
        let release_ns = start.elapsed().as_nanos() as u64;
        sys::release(&release_write)?;
        let mut launch_channel = File::from(release_write);
        let mut stderr = File::from(stderr_read);
        let mut exec_pipe = File::from(exec_read);
        let mut eof = false;
        let mut reason = None;
        let mut escalation = None;
        let mut next_sample = start
            .checked_add(Duration::from_millis(100))
            .expect("sampling deadline");
        let (status, usage, elapsed) = loop {
            self.collect_signals()?;
            let now = Instant::now();
            if reason.is_none() {
                if let Some(signal) = self.interrupted {
                    reason = Some(Outcome::Interrupted { signal });
                    child.signal(signal);
                    sampler.signal(signal);
                    escalation = now.checked_add(Duration::from_millis(250));
                } else if timeout.is_some_and(|limit| now.duration_since(start) >= limit) {
                    reason = Some(Outcome::Timeout);
                    child.signal(libc::SIGTERM);
                    sampler.signal(libc::SIGTERM);
                    escalation = now.checked_add(Duration::from_millis(250));
                }
            }
            if escalation.is_some_and(|deadline| now >= deadline) {
                child.signal(libc::SIGKILL);
                sampler.signal(libc::SIGKILL);
                escalation = None;
            }
            if let Some((status, usage)) = child.try_wait()? {
                break (status, usage, start.elapsed());
            }
            if !eof {
                eof = diagnostics.drain(&mut stderr)?;
            }
            if observe && now >= next_sample {
                let interval = sampler.sample(pid, start);
                next_sample = Instant::now()
                    .checked_add(interval)
                    .expect("bounded observer deadline");
            }
            let mut deadline = observe.then_some(next_sample);
            if reason.is_none()
                && let Some(limit) = timeout
            {
                let end = start.checked_add(limit).expect("representable timeout");
                deadline = Some(deadline.map_or(end, |d| d.min(end)));
            }
            if let Some(end) = escalation {
                deadline = Some(deadline.map_or(end, |d| d.min(end)));
            }
            sys::poll(&pidfd, &self.signals, (!eof).then_some(&stderr), deadline)?;
        };
        let counters = events.map_or_else(Vec::new, perf::Events::finish);
        let mut launch_rss = [0_u8; 8];
        let launch_max_rss_bytes = match launch_channel.read_exact(&mut launch_rss) {
            Ok(()) => Some(u64::from_ne_bytes(launch_rss)),
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => None,
            Err(error) => return Err(error),
        };
        let mut error_bytes = [0_u8; 4];
        let exec_error = match exec_pipe.read(&mut error_bytes) {
            Ok(4) => Some(i32::from_ne_bytes(error_bytes)),
            Ok(0) => None,
            Ok(_) => return Err(io::Error::other("incomplete exec error record")),
            Err(error) => return Err(error),
        };
        let mut outcome = reason.unwrap_or_else(|| {
            exec_error.map_or_else(
                || {
                    if libc::WIFEXITED(status) {
                        let code = libc::WEXITSTATUS(status);
                        if code == 0 {
                            Outcome::Success
                        } else {
                            Outcome::Exit { code }
                        }
                    } else {
                        Outcome::Signal {
                            signal: libc::WTERMSIG(status),
                        }
                    }
                },
                |errno| Outcome::ExecError { errno },
            )
        });
        let drain_start = Instant::now();
        let drain_deadline = drain_start
            .checked_add(Duration::from_millis(100))
            .expect("drain deadline");
        while !eof && Instant::now() < drain_deadline {
            eof = diagnostics.drain(&mut stderr)?;
            if !eof {
                sys::poll_pipe(&stderr, drain_deadline)?;
            }
        }
        diagnostics.pipe_still_open = !eof;
        let post_exit_drain_ns = drain_start.elapsed().as_nanos() as u64;
        let survivors = sys::reap_descendants()?;
        let mut cleanup_incomplete = false;
        if survivors && matches!(outcome, Outcome::Timeout | Outcome::Interrupted { .. }) {
            sys::kill_adopted()?;
            cleanup_incomplete = sys::reap_descendants()?;
        } else if survivors && outcome.success() {
            outcome = Outcome::BackgroundDescendants;
        }
        Ok(Observation {
            elapsed_ns: u64::try_from(elapsed.as_nanos()).map_err(io::Error::other)?,
            release_ns,
            post_exit_drain_ns,
            descendants_survived: survivors,
            cleanup_incomplete,
            user_ns: sys::timeval_ns(usage.ru_utime),
            system_ns: sys::timeval_ns(usage.ru_stime),
            max_rss_bytes: u64::try_from(usage.ru_maxrss)
                .unwrap_or(0)
                .wrapping_mul(1024),
            launch_max_rss_bytes,
            minor_faults: usage.ru_minflt,
            major_faults: usage.ru_majflt,
            voluntary_switches: usage.ru_nvcsw,
            involuntary_switches: usage.ru_nivcsw,
            outcome,
            counters,
            activity: sampler.finish(),
            diagnostics,
        })
    }
}

impl Drop for Runner {
    fn drop(&mut self) {
        sys::restore_mask(&self.original_mask);
    }
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "Serde serialize_with requires a reference"
)]
pub fn decimal<S: serde::Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&value.to_string())
}

fn executable_paths(program: &OsStr) -> io::Result<Vec<CString>> {
    if program.is_empty() {
        return Err(io::Error::other("empty executable"));
    }
    let paths = if program.as_bytes().contains(&b'/') {
        vec![PathBuf::from(program)]
    } else {
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_else(|| "/bin:/usr/bin".into()))
            .map(|directory| directory.join(program))
            .collect()
    };
    paths
        .into_iter()
        .map(|path| CString::new(path.as_os_str().as_bytes()).map_err(io::Error::other))
        .collect()
}

pub fn shell() -> io::Result<OsString> {
    for path in executable_paths(OsStr::new("sh"))? {
        let path = PathBuf::from(OsStr::from_bytes(path.as_bytes()));
        if fs::metadata(&path).is_ok_and(|meta| meta.is_file()) && sys::executable(&path) {
            return Ok(path.into_os_string());
        }
    }
    Err(io::Error::other("cannot resolve sh through PATH"))
}

#[cfg(test)]
mod tests {
    use super::{Command, InvocationKind, Runner};

    #[test]
    fn launch_rss_exposes_inherited_parent_memory() {
        let mut runner = Runner::new(false).expect("runner");
        let command = Command {
            argv: vec!["true".into()],
        };
        let before = runner
            .invoke(&command, None, None, &[], InvocationKind::Command)
            .expect("baseline invocation");
        let retained = vec![7_u8; 64 * 1024 * 1024];
        std::hint::black_box(&retained);
        let after = runner
            .invoke(&command, None, None, &[], InvocationKind::Command)
            .expect("invocation with retained observations");
        std::hint::black_box(&retained);
        assert!(before.outcome.success() && after.outcome.success());
        let initial = before.launch_max_rss_bytes.expect("initial launch floor");
        let inflated = after.launch_max_rss_bytes.expect("inflated launch floor");
        assert!(
            inflated >= initial.saturating_add(32 * 1024 * 1024),
            "inherited allocation not identified: {initial} -> {inflated}"
        );
        assert!(after.max_rss_bytes >= inflated);
    }
}
