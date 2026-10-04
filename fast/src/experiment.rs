use std::ffi::{OsStr, OsString};
use std::io::{self, IsTerminal as _};
use std::os::unix::ffi::OsStrExt as _;
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::cli::Cli;
use crate::random::Random;
use crate::report::{plural, seconds};
use crate::runner::{self, Command, InvocationKind, Observation, Outcome, Runner};
use crate::statistics::{self, Analysis, MINIMUM_RUNS, Planner};

#[derive(Debug)]
pub(crate) enum Failure {
    Usage(io::Error),
    Collector(io::Error),
}

#[derive(Debug, Serialize)]
#[serde(tag = "encoding", content = "value", rename_all = "snake_case")]
pub(crate) enum Text {
    Utf8(String),
    Bytes(Vec<u8>),
}

impl Text {
    pub(crate) fn new(value: &OsStr) -> Self {
        value.to_str().map_or_else(
            || Self::Bytes(value.as_bytes().to_vec()),
            |text| Self::Utf8(text.into()),
        )
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct Candidate {
    pub id: String,
    pub argv: Vec<Text>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Phase {
    Warmup,
    Pilot,
    Production,
}

impl Phase {
    const fn name(self) -> &'static str {
        match self {
            Self::Warmup => "warmup",
            Self::Pilot => "pilot",
            Self::Production => "production",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Role {
    Command,
    Prepare,
    Verify,
}

#[derive(Debug, Serialize)]
pub(crate) struct Invocation {
    pub candidate: usize,
    pub phase: Phase,
    /// Chronological index across warmup, pilot and production runs.
    pub run: usize,
    pub position: usize,
    pub role: Role,
    #[serde(serialize_with = "runner::decimal")]
    pub started_ns: u64,
    pub observation: Option<Observation>,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct Result {
    pub schema: u32,
    pub version: &'static str,
    pub seed: String,
    pub execution: &'static str,
    pub candidates: Vec<Candidate>,
    pub protocol: Protocol,
    pub context: Context,
    pub plan: Plan,
    pub completed: bool,
    pub stop_reason: String,
    pub exit_code: u8,
    #[serde(serialize_with = "runner::decimal")]
    pub wall_ns: u64,
    pub invocations: Vec<Invocation>,
    pub analysis: Analysis,
}

#[derive(Debug, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub(crate) enum Planning {
    /// `--runs` or `--once`: exactly the requested runs, without a pilot.
    Fixed,
    /// Pilot runs measure cost and variation, then the run count is frozen.
    /// A strict target never adds runs beyond what fits.
    Automatic { target_seconds: f64, strict: bool },
}

#[derive(Debug, Serialize)]
pub(crate) struct Plan {
    #[serde(flatten)]
    pub planning: Planning,
    pub warmup_runs: usize,
    pub pilot_runs: usize,
    /// Leading pilot runs excluded from analysis as an automatic warmup.
    pub discarded_runs: usize,
    /// Runs intended for analysis: kept pilot runs plus production runs.
    pub planned_runs: usize,
}

impl Result {
    /// Runs in which every command succeeded, chronologically, each with one command
    /// invocation per candidate in candidate order.
    fn complete_runs(&self) -> impl Iterator<Item = (Phase, Vec<&Invocation>)> {
        self.invocations
            .chunk_by(|left, right| left.run == right.run)
            .filter_map(|run| {
                let phase = run.first()?.phase;
                let mut slots: Vec<Option<&Invocation>> = vec![None; self.candidates.len()];
                for invocation in run.iter().filter(|i| i.role == Role::Command) {
                    if invocation
                        .observation
                        .as_ref()
                        .is_some_and(|o| o.outcome.success())
                    {
                        *slots
                            .get_mut(invocation.candidate)
                            .expect("recorded command") = Some(invocation);
                    }
                }
                slots
                    .into_iter()
                    .collect::<Option<Vec<_>>>()
                    .map(|complete| (phase, complete))
            })
    }

    /// Complete runs used by the analysis: pilot runs after any discarded warmup, then
    /// production runs.
    pub(crate) fn measured(&self) -> Vec<Vec<&Invocation>> {
        let mut discard = self.plan.discarded_runs;
        self.complete_runs()
            .filter(|(phase, _)| match phase {
                Phase::Warmup => false,
                Phase::Pilot if discard > 0 => {
                    discard = discard.wrapping_sub(1);
                    false
                },
                Phase::Pilot | Phase::Production => true,
            })
            .map(|(_, run)| run)
            .collect()
    }

    /// Keeps task detail for the first measured run only, analyzes the measured runs,
    /// and settles a requested nonregression check.
    fn conclude(&mut self, margin: Option<f64>, check: Option<f64>) {
        let (first, analysis) = {
            let measured = self.measured();
            (
                measured
                    .first()
                    .and_then(|run| run.first())
                    .map(|invocation| invocation.run),
                statistics::analyze(&Self::rows(&measured), margin),
            )
        };
        for invocation in &mut self.invocations {
            if Some(invocation.run) != first
                && let Some(observation) = &mut invocation.observation
            {
                observation.activity.tasks.clear();
                observation.activity.tasks.shrink_to_fit();
            }
        }
        self.analysis = analysis;
        // Failed runs can select against particular timings, so retain only descriptions.
        if matches!(self.exit_code, 1 | 3) {
            self.analysis.withdraw_inference(
                "A failed run ended the experiment; comparisons are descriptive.",
            );
        }
        if self.exit_code != 0 {
            return;
        }
        let Some(margin) = check else {
            return;
        };
        let comparisons: Vec<_> = self
            .analysis
            .comparisons
            .iter()
            .filter(|c| c.denominator == Some(0))
            .collect();
        self.exit_code = if comparisons.is_empty() {
            5
        } else if comparisons
            .iter()
            .any(|c| c.interval.is_some_and(|i| i.lower > 1.0 + margin))
        {
            4
        } else if comparisons
            .iter()
            .all(|c| c.interval.is_some_and(|i| i.upper <= 1.0 + margin))
        {
            0
        } else {
            5
        };
    }

    fn rows(runs: &[Vec<&Invocation>]) -> Vec<Vec<u64>> {
        runs.iter()
            .map(|run| {
                run.iter()
                    .map(|i| i.observation.as_ref().expect("successful run").elapsed_ns)
                    .collect()
            })
            .collect()
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct Context {
    pub architecture: &'static str,
    pub cpu_model: Option<String>,
    pub microcode: Option<String>,
    pub kernel: Option<String>,
    pub allowed_cpus: Option<String>,
    pub inherited_cgroup: Option<String>,
    pub clock_resolution_ns: u64,
}

impl Context {
    fn read() -> Self {
        let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
        let cpu_field = |name: &str| {
            cpuinfo.lines().find_map(|line| {
                let (key, value) = line.split_once(':')?;
                (key.trim() == name).then(|| value.trim().to_owned())
            })
        };
        // SAFETY: CLOCK_MONOTONIC is supported on Linux; output is initialized.
        let clock_resolution_ns = unsafe {
            let mut time: libc::timespec = std::mem::zeroed();
            libc::clock_getres(libc::CLOCK_MONOTONIC, &raw mut time);
            (time.tv_sec as u64)
                .wrapping_mul(1_000_000_000)
                .wrapping_add(time.tv_nsec as u64)
        };
        Self {
            architecture: std::env::consts::ARCH,
            cpu_model: cpu_field("model name"),
            microcode: cpu_field("microcode"),
            kernel: std::fs::read_to_string("/proc/sys/kernel/osrelease")
                .ok()
                .map(|s| s.trim().into()),
            allowed_cpus: std::fs::read_to_string("/proc/self/status")
                .ok()
                .and_then(|s| {
                    s.lines().find_map(|line| {
                        line.strip_prefix("Cpus_allowed_list:")
                            .map(|value| value.trim().into())
                    })
                }),
            inherited_cgroup: std::fs::read_to_string("/proc/self/cgroup").ok(),
            clock_resolution_ns,
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct Protocol {
    pub elapsed: &'static str,
    pub cpu: &'static str,
    pub rss: &'static str,
    pub correctness: &'static str,
    pub counter_policy: serde_json::Value,
    pub input: Option<Text>,
    pub prepare: Option<Text>,
    pub verify: Option<Text>,
    pub timeout_seconds: Option<f64>,
    pub hook_timeout_seconds: f64,
    pub margin: Option<f64>,
    pub check: Option<f64>,
}

struct Experiment<'a> {
    options: &'a Cli,
    runner: Runner,
    commands: Vec<Command>,
    shell: Option<OsString>,
    result: Result,
    rng: Random,
    next_run: usize,
    /// Seconds per completed run, hooks included.
    costs: Vec<f64>,
    /// Runs to execute once the plan is known, warmup and pilot included.
    total_runs: Option<usize>,
    start: Instant,
    progress: Instant,
}

impl Experiment<'_> {
    fn invoke(
        &mut self,
        candidate: usize,
        phase: Phase,
        run: usize,
        position: usize,
        role: Role,
    ) -> bool {
        if let Err(error) = self.runner.collect_signals() {
            self.result.exit_code = 3;
            self.result.stop_reason = error.to_string();
            return false;
        }
        if let Some(signal) = self.runner.interrupted {
            self.result.exit_code = 128_u8.wrapping_add(signal as u8);
            self.result.stop_reason = format!("interrupted by signal {signal}");
            return false;
        }
        let hook = match role {
            Role::Command => None,
            Role::Prepare => self.options.prepare.as_ref(),
            Role::Verify => self.options.verify.as_ref(),
        };
        let command = if let Some(expression) = hook {
            Command {
                argv: vec![
                    self.shell.as_ref().expect("hook shell resolved").clone(),
                    "-c".into(),
                    expression.clone(),
                ],
            }
        } else {
            self.commands
                .get(candidate)
                .expect("scheduled command")
                .clone()
        };
        let environment = if role == Role::Command {
            Vec::new()
        } else {
            vec![
                (
                    "FAST_COMMAND_INDEX".into(),
                    candidate.wrapping_add(1).to_string(),
                ),
                ("FAST_COMMAND_ID".into(), id(candidate)),
                ("FAST_PHASE".into(), phase.name().into()),
                ("FAST_RUN_INDEX".into(), run.wrapping_add(1).to_string()),
            ]
        };
        let timeout = if role == Role::Command {
            self.options.timeout
        } else {
            Some(self.options.timeout.unwrap_or(Duration::from_secs(30)))
        };
        let started_ns = self.start.elapsed().as_nanos() as u64;
        let reading = self.runner.invoke(
            &command,
            self.options.input.as_deref(),
            timeout,
            &environment,
            if role == Role::Command {
                InvocationKind::Command
            } else {
                InvocationKind::Hook
            },
        );
        let (observation, error, success) = match reading {
            Ok(mut observation) => {
                let success = observation.outcome.success();
                if success {
                    observation.diagnostics.discard();
                } else {
                    self.result.exit_code = match observation.outcome {
                        Outcome::Interrupted { signal } => 128_u8.wrapping_add(signal as u8),
                        Outcome::Success
                        | Outcome::Exit { .. }
                        | Outcome::Signal { .. }
                        | Outcome::Timeout
                        | Outcome::ExecError { .. }
                        | Outcome::BackgroundDescendants => 1,
                    };
                    self.result.stop_reason =
                        format!("{} {role:?}: {:?}", id(candidate), observation.outcome);
                }
                // The first measured run is one of the first two after any requested
                // warmup; the automatic warmup decision settles which.
                if role != Role::Command || run >= self.result.plan.warmup_runs.wrapping_add(2) {
                    observation.activity.tasks.clear();
                    observation.activity.tasks.shrink_to_fit();
                }
                (Some(observation), None, success)
            },
            Err(error) => {
                self.result.exit_code = 3;
                self.result.stop_reason = error.to_string();
                (None, Some(error.to_string()), false)
            },
        };
        self.result.invocations.push(Invocation {
            candidate,
            phase,
            run,
            position,
            role,
            started_ns,
            observation,
            error,
        });
        success
    }

    fn run(&mut self, phase: Phase) -> bool {
        let started = Instant::now();
        let run = self.next_run;
        self.next_run = run.wrapping_add(1);
        let mut order: Vec<_> = (0..self.commands.len()).collect();
        self.rng.shuffle(&mut order);
        for (position, candidate) in order.into_iter().enumerate() {
            if self.options.prepare.is_some()
                && !self.invoke(candidate, phase, run, position, Role::Prepare)
            {
                return false;
            }
            if !self.invoke(candidate, phase, run, position, Role::Command) {
                return false;
            }
            if self.options.verify.is_some()
                && !self.invoke(candidate, phase, run, position, Role::Verify)
            {
                return false;
            }
        }
        self.costs.push(started.elapsed().as_secs_f64());
        if self.progress.elapsed() >= Duration::from_secs(10) {
            self.progress_line();
            self.progress = Instant::now();
        }
        true
    }

    /// An appended line, never redrawn: measured runs, a bar, and the time left.
    fn progress_line(&self) {
        const BAR: usize = 20;
        let Some(total) = self.total_runs else {
            self.message(&format!(
                "pilot: {} run{} complete",
                self.next_run,
                plural(self.next_run)
            ));
            return;
        };
        let remaining = total.saturating_sub(self.next_run);
        if remaining == 0 {
            return;
        }
        let planned = self.result.plan.planned_runs;
        let done = planned.saturating_sub(remaining);
        let filled = done
            .saturating_mul(BAR)
            .checked_div(planned)
            .unwrap_or(BAR)
            .min(BAR);
        let left = remaining as f64 * Planner::cost(&self.costs);
        self.message(&format!(
            "run {done} of {planned} {}{} about {} left",
            "━".repeat(filled),
            "─".repeat(BAR.saturating_sub(filled)),
            seconds(left * 1e9)
        ));
    }

    /// Announces a plan that takes a while before its remaining runs start. Under the
    /// default target, a plan past it means the minimum run count was enforced.
    fn announce(&self, target: Option<f64>) {
        let Some(total) = self.total_runs else {
            return;
        };
        let cost = Planner::cost(&self.costs);
        let elapsed = self.start.elapsed().as_secs_f64();
        let left = total.saturating_sub(self.next_run) as f64 * cost;
        let planned = self.result.plan.planned_runs;
        if target.is_some_and(|target| elapsed + left > 1.1 * target) {
            self.message(&format!(
                "one run takes about {}; measuring the minimum of {planned} runs, about {} in \
                 total. Ctrl-C stops early with the completed runs",
                seconds(cost * 1e9),
                seconds((elapsed + left) * 1e9)
            ));
        } else if left >= 5.0 {
            self.message(&format!(
                "measuring {planned} runs, about {} left. Ctrl-C stops early with the completed \
                 runs",
                seconds(left * 1e9)
            ));
        }
    }

    fn message(&self, text: &str) {
        if !self.options.quiet && io::stderr().is_terminal() {
            eprintln!("fast: {text}");
        }
    }

    fn execute(&mut self, capacity: usize) {
        for _ in 0..self.options.warmup.unwrap_or(0) {
            if !self.run(Phase::Warmup) {
                return;
            }
            self.result.plan.warmup_runs = self.next_run;
        }
        // The automatic arm yields the default target, or no target for a strict budget.
        let (production, automatic) = match self.result.plan.planning {
            Planning::Fixed => (self.result.plan.planned_runs, None),
            Planning::Automatic {
                target_seconds,
                strict,
            } => {
                let planner = Planner {
                    target: target_seconds,
                    minimum: if strict { 1 } else { MINIMUM_RUNS },
                    warmup: self.options.warmup.is_none(),
                };
                let mut costs = Vec::new();
                loop {
                    let start = Instant::now();
                    if !self.run(Phase::Pilot) {
                        return;
                    }
                    costs.push(start.elapsed().as_secs_f64());
                    self.result.plan.pilot_runs = costs.len();
                    self.result.plan.discarded_runs = usize::from(planner.discard_first(&costs));
                    if planner.pilot_complete(self.start.elapsed().as_secs_f64(), &costs) {
                        break;
                    }
                }
                let pilot: Vec<_> = self
                    .result
                    .complete_runs()
                    .filter(|(phase, _)| *phase == Phase::Pilot)
                    .map(|(_, run)| run)
                    .collect();
                let additional = planner
                    .additional(
                        self.start.elapsed().as_secs_f64(),
                        &costs,
                        &Result::rows(&pilot),
                    )
                    .min(capacity.saturating_sub(self.next_run));
                self.result.plan.planned_runs = costs
                    .len()
                    .wrapping_sub(self.result.plan.discarded_runs)
                    .wrapping_add(additional);
                (additional, Some((!strict).then_some(target_seconds)))
            },
        };
        self.total_runs = Some(self.next_run.wrapping_add(production));
        if let Some(target) = automatic {
            self.announce(target);
        }
        for index in 0..production {
            if !self.run(Phase::Production) {
                return;
            }
            // A fixed plan learns its run cost from its first run.
            if index == 0 && automatic.is_none() {
                self.announce(None);
            }
        }
        self.result.completed = true;
        self.result.stop_reason = "planned_runs_completed".into();
    }
}

pub(crate) fn id(mut index: usize) -> String {
    let mut bytes = Vec::new();
    loop {
        bytes.push(b'A'.wrapping_add((index % 26) as u8));
        if index < 26 {
            break;
        }
        index = (index / 26).wrapping_sub(1);
    }
    bytes.reverse();
    String::from_utf8(bytes).expect("ASCII candidate ID")
}

fn commands(options: &Cli) -> io::Result<(Vec<Command>, Option<OsString>)> {
    if options.once && options.warmup.is_some_and(|runs| runs > 0) {
        return Err(io::Error::other("--once cannot include warmup runs"));
    }
    if options.separator.is_empty() {
        return Err(io::Error::other("--separator must not be empty"));
    }
    let shell = if options.prepare.is_some() || options.verify.is_some() {
        Some(runner::shell()?)
    } else {
        None
    };
    let commands: Vec<_> = options
        .commands
        .split(|argument| argument == &options.separator)
        .map(|argv| Command {
            argv: argv.to_vec(),
        })
        .collect();
    for (index, command) in commands.iter().enumerate() {
        if command.argv.first().is_none_or(|arg| arg.is_empty())
            || command.argv.iter().any(|arg| arg.as_bytes().contains(&0))
        {
            return Err(io::Error::other(format!(
                "command {} requires a nonempty executable and NUL-free arguments; put a program on each side of the separator",
                id(index),
            )));
        }
    }
    if options.check.is_some() && commands.len() < 2 {
        return Err(io::Error::other("--check requires at least two commands"));
    }
    if let Some(path) = &options.input
        && !std::fs::metadata(path)?.is_file()
    {
        return Err(io::Error::other("--input must name a regular file"));
    }
    Ok((commands, shell))
}

/// Total runs that fit the 128 MiB allowance for retained observations and their
/// bounded diagnostics.
fn capacity(options: &Cli, commands: usize) -> io::Result<usize> {
    let hooks = 1_usize
        .wrapping_add(usize::from(options.prepare.is_some()))
        .wrapping_add(usize::from(options.verify.is_some()));
    let bytes_per_run = commands
        .checked_mul(hooks.wrapping_mul(1024))
        .ok_or_else(|| io::Error::other("too many commands"))?;
    let capacity = (128_usize * 1024 * 1024)
        .checked_div(bytes_per_run)
        .expect("at least one command");
    let explicit = options
        .runs
        .map_or_else(|| usize::from(options.once), std::num::NonZeroUsize::get);
    if explicit
        .saturating_add(options.warmup.unwrap_or(0))
        .saturating_add(8)
        > capacity
    {
        return Err(io::Error::other(format!(
            "requested runs exceed the 128 MiB observation allowance; at most {capacity} runs fit"
        )));
    }
    Ok(capacity)
}

fn protocol(options: &Cli, runner: &Runner) -> io::Result<Protocol> {
    Ok(Protocol {
        elapsed: "CLOCK_MONOTONIC; immediately before fork through parent observation of root exit, before diagnostic tail draining",
        cpu: "wait4 user/system accounting, including descendants waited by the command; not added to sampled CPU",
        rss: "Linux wait4 ru_maxrss high-water quantity; not simultaneous process-tree memory; bytes",
        correctness: if options.verify.is_some() {
            "verification_hook"
        } else {
            "exit_status_only"
        },
        counter_policy: serde_json::to_value(runner.counter_policy())?,
        input: options.input.as_ref().map(|p| Text::new(p.as_os_str())),
        prepare: options.prepare.as_deref().map(Text::new),
        verify: options.verify.as_deref().map(Text::new),
        timeout_seconds: options.timeout.map(|d| d.as_secs_f64()),
        hook_timeout_seconds: options
            .timeout
            .unwrap_or(Duration::from_secs(30))
            .as_secs_f64(),
        margin: options.margin,
        check: options.check,
    })
}

pub(crate) fn run(options: Cli) -> std::result::Result<(Result, Cli), Failure> {
    let (commands, shell) = commands(&options).map_err(Failure::Usage)?;
    let capacity = capacity(&options, commands.len()).map_err(Failure::Usage)?;
    runner::check_jobserver().map_err(Failure::Usage)?;
    let seed = match options.seed {
        Some(seed) => seed,
        None => Random::entropy().map_err(Failure::Collector)?,
    };
    let runner = Runner::new(true).map_err(Failure::Collector)?;
    let fixed = options
        .runs
        .map(std::num::NonZeroUsize::get)
        .or_else(|| options.once.then_some(1));
    let result = Result {
        schema: 2,
        version: env!("CARGO_PKG_VERSION"),
        seed: format!("{seed:016x}"),
        execution: "exact_argv",
        candidates: commands
            .iter()
            .enumerate()
            .map(|(index, command)| Candidate {
                id: id(index),
                argv: command.argv.iter().map(|arg| Text::new(arg)).collect(),
            })
            .collect(),
        protocol: protocol(&options, &runner).map_err(Failure::Collector)?,
        context: Context::read(),
        plan: Plan {
            planning: if fixed.is_some() {
                Planning::Fixed
            } else {
                Planning::Automatic {
                    target_seconds: options
                        .time
                        .unwrap_or(Duration::from_secs(30))
                        .as_secs_f64(),
                    strict: options.time.is_some(),
                }
            },
            warmup_runs: 0,
            pilot_runs: 0,
            discarded_runs: 0,
            planned_runs: fixed.unwrap_or(0),
        },
        completed: false,
        stop_reason: "not_started".into(),
        exit_code: 0,
        wall_ns: 0,
        invocations: Vec::new(),
        analysis: statistics::analyze(&[], options.margin),
    };
    let mut experiment = Experiment {
        options: &options,
        runner,
        commands,
        shell,
        result,
        rng: Random::new(seed),
        next_run: 0,
        costs: Vec::new(),
        total_runs: None,
        start: Instant::now(),
        progress: Instant::now(),
    };
    experiment.execute(capacity);
    experiment.result.wall_ns = experiment.start.elapsed().as_nanos() as u64;
    experiment
        .runner
        .collect_signals()
        .map_err(Failure::Collector)?;
    if let Some(signal) = experiment.runner.interrupted {
        experiment.result.completed = false;
        experiment.result.exit_code = 128_u8.wrapping_add(signal as u8);
        experiment.result.stop_reason = format!("interrupted by signal {signal}");
    }
    experiment.result.conclude(options.margin, options.check);
    Ok((experiment.result, options))
}
