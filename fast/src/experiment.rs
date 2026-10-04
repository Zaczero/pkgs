use std::ffi::{OsStr, OsString};
use std::io::{self, IsTerminal as _};
use std::os::unix::ffi::OsStrExt as _;
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::cli::Cli;
use crate::random::Random;
use crate::runner::{self, Command, InvocationKind, Observation, Outcome, Runner};
use crate::statistics::{self, Analysis};

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
    pub(crate) fn display(&self) -> String {
        match self {
            Self::Utf8(text) => text.chars().flat_map(char::escape_debug).collect(),
            Self::Bytes(bytes) => bytes
                .iter()
                .flat_map(|&byte| std::ascii::escape_default(byte).map(char::from))
                .collect(),
        }
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
    pub block: usize,
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
    pub planned_blocks: usize,
    pub completed: bool,
    pub stop_reason: String,
    pub exit_code: u8,
    pub invocations: Vec<Invocation>,
    pub analysis: Analysis,
}

impl Result {
    pub(crate) fn rows(&self, phase: Phase) -> Vec<Vec<u64>> {
        let mut rows = Vec::new();
        let mut current = vec![None; self.candidates.len()];
        let mut block = 0;
        for invocation in self
            .invocations
            .iter()
            .filter(|i| i.phase == phase && i.role == Role::Command)
        {
            if invocation.block != block {
                if current.iter().all(Option::is_some) {
                    rows.push(current.iter().map(|x| x.expect("complete row")).collect());
                }
                current.fill(None);
                block = invocation.block;
            }
            if let Some(observation) = &invocation.observation
                && observation.outcome.success()
            {
                *current
                    .get_mut(invocation.candidate)
                    .expect("recorded command") = Some(observation.elapsed_ns);
            }
        }
        if current.iter().all(Option::is_some) {
            rows.push(
                current
                    .into_iter()
                    .map(|x| x.expect("complete row"))
                    .collect(),
            );
        }
        rows
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
    start: Instant,
    progress: Instant,
}

impl Experiment<'_> {
    fn invoke(
        &mut self,
        candidate: usize,
        phase: Phase,
        block: usize,
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
                ("FAST_RUN_INDEX".into(), block.wrapping_add(1).to_string()),
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
                if phase != Phase::Production || block != 0 || role != Role::Command {
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
            block,
            position,
            role,
            started_ns,
            observation,
            error,
        });
        success
    }

    fn block(&mut self, phase: Phase, block: usize) -> bool {
        let mut order: Vec<_> = (0..self.commands.len()).collect();
        self.rng.shuffle(&mut order);
        for (position, candidate) in order.into_iter().enumerate() {
            if self.options.prepare.is_some()
                && !self.invoke(candidate, phase, block, position, Role::Prepare)
            {
                return false;
            }
            if !self.invoke(candidate, phase, block, position, Role::Command) {
                return false;
            }
            if self.options.verify.is_some()
                && !self.invoke(candidate, phase, block, position, Role::Verify)
            {
                return false;
            }
        }
        if self.progress.elapsed() >= Duration::from_secs(10) {
            self.message(&format!(
                "{}: {} complete blocks",
                phase.name(),
                block.wrapping_add(1)
            ));
            self.progress = Instant::now();
        }
        true
    }

    fn message(&self, text: &str) {
        if !self.options.quiet && io::stderr().is_terminal() {
            eprintln!("fast: {text}");
        }
    }

    fn execute(&mut self, max_blocks: usize) {
        self.message(
            "running sequential randomized blocks; Ctrl-C stops and prints partial results",
        );
        for block in 0..self.options.warmup {
            if !self.block(Phase::Warmup, block) {
                return;
            }
        }
        let count = if self.options.once {
            1
        } else if let Some(runs) = self.options.runs {
            runs.get()
        } else {
            let target = self
                .options
                .time
                .unwrap_or(Duration::from_secs(30))
                .as_secs_f64();
            let mut costs = Vec::new();
            for block in 0.. {
                let start = Instant::now();
                if !self.block(Phase::Pilot, block) {
                    return;
                }
                costs.push(start.elapsed().as_secs_f64());
                if statistics::pilot_complete(self.start.elapsed().as_secs_f64(), &costs, target) {
                    break;
                }
            }
            let remaining = target - self.start.elapsed().as_secs_f64();
            statistics::production_count(remaining, &costs, &self.result.rows(Phase::Pilot))
                .min(max_blocks)
        };
        self.result.planned_blocks = count;
        self.message(&format!("plan frozen: {count} measured runs per command"));
        for block in 0..count {
            if !self.block(Phase::Production, block) {
                return;
            }
        }
        self.result.completed = true;
        self.result.stop_reason = if count == 0 {
            "pilot_only"
        } else {
            "planned_count_completed"
        }
        .into();
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
    if options.once && options.warmup != 0 {
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

fn capacity(options: &Cli, commands: usize) -> io::Result<usize> {
    let hooks = 1_usize
        .wrapping_add(usize::from(options.prepare.is_some()))
        .wrapping_add(usize::from(options.verify.is_some()));
    let bytes_per_block = commands
        .checked_mul(hooks.wrapping_mul(1024))
        .ok_or_else(|| io::Error::other("too many commands"))?;
    let analysis_bytes = commands
        .checked_mul(20_000 * 8)
        .and_then(|bytes| {
            commands
                .checked_mul(commands)
                .and_then(|pairs| pairs.checked_mul(128))
                .and_then(|extra| bytes.checked_add(extra))
        })
        .ok_or_else(|| io::Error::other("too many comparisons"))?;
    let allowance = (128_usize * 1024 * 1024)
        .checked_sub(analysis_bytes)
        .ok_or_else(|| io::Error::other("comparison exceeds the 128 MiB observation allowance"))?;
    let max_blocks = allowance
        .checked_div(bytes_per_block)
        .expect("at least one command");
    let explicit = options
        .runs
        .map_or_else(|| usize::from(options.once), std::num::NonZeroUsize::get);
    if explicit.saturating_add(options.warmup).saturating_add(8) > max_blocks {
        return Err(io::Error::other(format!(
            "requested observations exceed the 128 MiB allowance; at most {max_blocks} total blocks fit"
        )));
    }
    Ok(max_blocks)
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
    let max_blocks = capacity(&options, commands.len()).map_err(Failure::Usage)?;
    runner::check_jobserver().map_err(Failure::Usage)?;
    let seed = match options.seed {
        Some(seed) => seed,
        None => Random::entropy().map_err(Failure::Collector)?,
    };
    let runner = Runner::new(true).map_err(Failure::Collector)?;
    let result = Result {
        schema: 1,
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
        planned_blocks: options
            .runs
            .map_or_else(|| usize::from(options.once), std::num::NonZeroUsize::get),
        completed: false,
        stop_reason: "not_started".into(),
        exit_code: 0,
        invocations: Vec::new(),
        analysis: statistics::analyze(&[], false, seed, options.margin),
    };
    let mut experiment = Experiment {
        options: &options,
        runner,
        commands,
        shell,
        result,
        rng: Random::new(seed),
        start: Instant::now(),
        progress: Instant::now(),
    };
    experiment.execute(max_blocks.saturating_sub(options.warmup.saturating_add(8)));
    let rows = experiment.result.rows(Phase::Production);
    experiment.result.analysis = statistics::analyze(
        &rows,
        experiment.result.completed,
        seed ^ 0xA076_1D64_78BD_642F,
        options.margin,
    );
    experiment
        .runner
        .collect_signals()
        .map_err(Failure::Collector)?;
    if let Some(signal) = experiment.runner.interrupted {
        experiment.result.completed = false;
        experiment.result.exit_code = 128_u8.wrapping_add(signal as u8);
        experiment.result.stop_reason = format!("interrupted by signal {signal}");
        experiment
            .result
            .analysis
            .withdraw_inference("Interrupted experiment; conclusions are unavailable.");
    }
    if experiment.result.exit_code == 0
        && let Some(margin) = options.check
    {
        let comparisons: Vec<_> = experiment
            .result
            .analysis
            .comparisons
            .iter()
            .filter(|c| c.denominator == Some(0))
            .collect();
        experiment.result.exit_code = if !experiment.result.completed || comparisons.is_empty() {
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
    Ok((experiment.result, options))
}
