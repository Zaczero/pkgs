use std::ffi::OsString;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum};

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum Color {
    Auto,
    Always,
    Never,
}

#[derive(Debug, Parser)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent command-line switches are not mutually exclusive state"
)]
#[command(
    version,
    about = "Run the work. Compare fairly. Print the answer.",
    override_usage = "fast [OPTIONS] [--] PROGRAM [ARG ...] [::: PROGRAM [ARG ...]] ...",
    after_help = "Examples:\n  fast ./old input.dat ::: ./new input.dat\n  fast --input data.json zstd ::: gzip\n  fast --once ./expensive-job\n  fast --runs 100 --check 1% ./old ::: ./new\n  fast --json ./old ::: ./new > result.json\n  fast sh -c 'producer | consumer' ::: sh -c 'producer | improved'\n  fast --separator +++ printf '%s' ::: +++ true\n\nPut fast options before the first program; its arguments are passed unchanged.\nA standalone ::: separates commands, even after an initial --. Use --separator\nwhen a command needs a literal ::: argument; quoting the marker does not escape it.\nCommands run sequentially in randomized complete blocks, without an implicit shell.\nStdin and stdout are null unless --input supplies stdin.\nThe default plans about 30 seconds and at least 3 runs, even for slow commands;\nan explicit --time is a strict budget. --timeout terminates a single run.\nCtrl-C stops early and reports the completed runs. Hooks time out after 30s.\n\nExit: 0 completed (possibly unresolved), 1 workload failure, 2 usage error,\n3 collector failure, 4 regression, 5 inconclusive check, 128+signal interrupted."
)]
pub(crate) struct Cli {
    /// Execute each command exactly once, without a pilot or warmup
    #[arg(long, conflicts_with_all = ["runs", "time"])]
    pub once: bool,
    /// Measure exactly N runs of each command, without planning
    #[arg(long, value_name = "N", conflicts_with = "time")]
    pub runs: Option<NonZeroUsize>,
    /// Fit the planned runs within this total time [default: about 30s, at least 3 runs]
    #[arg(long, value_parser = duration, value_name = "DURATION")]
    pub time: Option<Duration>,
    /// Run every command N times before measuring [default: automatic]
    #[arg(long, value_name = "N")]
    pub warmup: Option<usize>,
    /// Terminate an invocation or hook after this duration
    #[arg(long, value_parser = duration, value_name = "DURATION")]
    pub timeout: Option<Duration>,
    /// Reopen this regular file as stdin for every invocation
    #[arg(long, value_name = "FILE")]
    pub input: Option<PathBuf>,
    /// Run sh -c EXPR before every invocation, outside timing
    #[arg(long, value_name = "EXPR")]
    pub prepare: Option<OsString>,
    /// Run sh -c EXPR after every successful invocation, outside timing
    #[arg(long, value_name = "EXPR")]
    pub verify: Option<OsString>,
    /// Interpret practical equivalence within this symmetric percentage
    #[arg(long, value_parser = percent, value_name = "PERCENT")]
    pub margin: Option<f64>,
    /// Establish no more than this regression versus A
    #[arg(long, value_parser = percent, value_name = "PERCENT")]
    pub check: Option<f64>,
    /// Print spread, all comparisons, and sampled task activity
    #[arg(long)]
    pub details: bool,
    /// Print one JSON document on stdout
    #[arg(long)]
    pub json: bool,
    /// Control color
    #[arg(long, value_enum, default_value = "auto")]
    pub color: Color,
    /// Suppress progress, retaining results and errors
    #[arg(short, long)]
    pub quiet: bool,
    /// Reproduce randomized ordering with this hexadecimal seed
    #[arg(long, value_parser = seed, value_name = "HEX")]
    pub seed: Option<u64>,
    /// Separate commands with this standalone argument
    #[arg(long, default_value = ":::", value_name = "MARKER")]
    pub separator: OsString,
    /// Programs and arguments, separated by :::
    #[arg(required = true, num_args = 1.., trailing_var_arg = true, value_name = "COMMAND")]
    pub commands: Vec<OsString>,
}

fn duration(text: &str) -> Result<Duration, String> {
    let units = [
        ("ns", 1e-9),
        ("us", 1e-6),
        ("ms", 1e-3),
        ("s", 1.0),
        ("m", 60.0),
        ("h", 3600.0),
    ];
    for (suffix, scale) in units {
        if let Some(number) = text.strip_suffix(suffix) {
            let seconds = number
                .parse::<f64>()
                .map_err(|_| "expected a duration such as 250ms, 30s, or 2m")?
                * scale;
            return Duration::try_from_secs_f64(seconds)
                .ok()
                .filter(|value| !value.is_zero() && value.as_nanos() <= u128::from(u64::MAX))
                .ok_or_else(|| "duration must be finite, positive, and representable".into());
        }
    }
    Err("expected a duration with ns, us, ms, s, m, or h suffix".into())
}

fn percent(text: &str) -> Result<f64, String> {
    let value = text
        .trim_end_matches('%')
        .parse::<f64>()
        .map_err(|_| "expected a percentage such as 1%")?;
    if value.is_finite() && (0.0..100.0).contains(&value) {
        Ok(value / 100.0)
    } else {
        Err("percentage must be finite and between 0% (inclusive) and 100%".into())
    }
}

fn seed(text: &str) -> Result<u64, String> {
    u64::from_str_radix(text.strip_prefix("0x").unwrap_or(text), 16)
        .map_err(|_| "expected up to 16 hexadecimal digits".into())
}
