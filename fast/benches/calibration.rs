use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

use clap::{Parser, ValueEnum};
use fast::random::Random;
use fast::statistics::{self, Decision, MINIMUM_RUNS, Planner};
use serde::Serialize;

/// Simulated elapsed-time models; each command's run time is about 1 ms.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Model {
    /// Four independent lognormal commands, log SD 0.1.
    Iid,
    /// Log SDs 0.03, 0.06, 0.09, 0.12.
    Unequal,
    /// Shared AR(1) multiplicative noise across commands.
    Common,
    /// Command-specific AR(1) noise, correlation 0.5.
    Ar,
    /// Command-specific AR(1) noise, correlation 0.9.
    StrongAr,
    /// Lognormal skew, log SD 0.7.
    Skew,
    /// B takes 10% less time.
    Effect,
    /// Two quiet commands, log SD 0.02; B takes 5% less time.
    Pair,
    /// Two quiet commands, log SD 0.02, equal means.
    PairNull,
    /// One command: its mean.
    Single,
    /// Eight independent commands.
    Eight,
    /// The very first run of A takes five times as long.
    Cold,
    /// Linear drift that differs between commands.
    Drift,
    /// Periodic load with command-specific phase.
    Periodic,
    /// A run after A takes 5% longer.
    Carryover,
    /// One run in 1,000 takes 30 times as long, for every command.
    Rare,
    /// One run in 1,000 of B takes 100 times as long.
    AsymmetricTail,
}

#[derive(Parser)]
struct Options {
    #[arg(default_value_t = 10_000)]
    trials: usize,
    /// Measured runs; with --auto, the planning target in runs' worth of time.
    #[arg(default_value_t = 200)]
    runs: usize,
    #[arg(value_enum, default_value = "iid")]
    model: Model,
    #[arg(default_value_t = 4)]
    workers: usize,
    #[arg(default_value = "d91453c274b8109a", value_parser = parse_seed)]
    seed: u64,
    /// Plan the run count as fast's default automatic planning does.
    #[arg(long)]
    auto: bool,
}

#[derive(Clone, Copy, Default, Serialize)]
struct Totals {
    /// Every bounded interval contained its true value.
    covered: usize,
    false_direction: usize,
    detected: usize,
    /// At least one interval was unbounded: no claim.
    unbounded: usize,
    /// At least one comparison had no interval because variation was degenerate.
    insufficient: usize,
    /// At least one comparison was flagged as correlated.
    flagged: usize,
    runs: usize,
}

/// One experiment's floating-point contributions. They are summed in experiment
/// order, so the output does not depend on which worker ran which experiment.
#[derive(Clone, Copy, Default)]
struct Moments {
    log_bias: f64,
    /// Sum of log widths of intervals with a positive lower bound.
    log_width: f64,
    bounded: usize,
}

struct Workload {
    random: Random,
    model: Model,
    common: f64,
    specific: Vec<f64>,
    previous: usize,
    position: usize,
    scale: usize,
}

impl Workload {
    fn new(options: &Options, trial: usize) -> Self {
        let mut random = Random::new(options.seed.wrapping_add(trial as u64));
        let columns = match options.model {
            Model::Single => 1,
            Model::Pair | Model::PairNull => 2,
            Model::Eight => 8,
            Model::Iid
            | Model::Unequal
            | Model::Common
            | Model::Ar
            | Model::StrongAr
            | Model::Skew
            | Model::Effect
            | Model::Cold
            | Model::Drift
            | Model::Periodic
            | Model::Carryover
            | Model::Rare
            | Model::AsymmetricTail => 4,
        };
        let common = normal(&mut random);
        let specific = std::iter::repeat_with(|| normal(&mut random))
            .take(columns)
            .collect();
        Self {
            random,
            model: options.model,
            common,
            specific,
            previous: 0,
            position: 0,
            scale: options.runs,
        }
    }

    fn run(&mut self) -> Vec<u64> {
        self.common = 0.5 * self.common + 0.75_f64.sqrt() * normal(&mut self.random);
        let mut row = vec![0; self.specific.len()];
        let mut order: Vec<_> = (0..self.specific.len()).collect();
        self.random.shuffle(&mut order);
        for column in order {
            let sigma = match self.model {
                Model::Unequal => 0.03 + 0.03 * column as f64,
                Model::Pair | Model::PairNull => 0.02,
                Model::Iid
                | Model::Common
                | Model::Ar
                | Model::StrongAr
                | Model::Skew
                | Model::Effect
                | Model::Single
                | Model::Eight
                | Model::Cold
                | Model::Drift
                | Model::Periodic
                | Model::Carryover
                | Model::Rare
                | Model::AsymmetricTail => 0.1,
            };
            let rho = match self.model {
                Model::Ar => 0.5,
                Model::StrongAr => 0.9,
                Model::Iid
                | Model::Unequal
                | Model::Common
                | Model::Skew
                | Model::Effect
                | Model::Pair
                | Model::PairNull
                | Model::Single
                | Model::Eight
                | Model::Cold
                | Model::Drift
                | Model::Periodic
                | Model::Carryover
                | Model::Rare
                | Model::AsymmetricTail => 0.0,
            };
            let state = self.specific.get_mut(column).expect("command state");
            *state = rho * *state + (1.0 - rho * rho).sqrt() * normal(&mut self.random);
            let mut value = (sigma * *state - 0.5 * sigma * sigma).exp();
            match self.model {
                Model::Common => value *= (0.2 * self.common - 0.02).exp(),
                Model::Skew => value = (0.7 * normal(&mut self.random) - 0.245).exp(),
                Model::Drift => {
                    value *= 1.0 + column as f64 * self.position as f64 / self.scale as f64 * 0.1;
                },
                Model::Periodic => {
                    value *= 1.0 + 0.2 * (self.position as f64 * 0.4 + column as f64).sin();
                },
                Model::Carryover if self.previous == 0 => value *= 1.05,
                Model::Rare if self.random.unit() < 0.001 => value *= 30.0,
                Model::AsymmetricTail if column == 1 && self.random.unit() < 0.001 => {
                    value *= 100.0;
                },
                Model::Effect if column == 1 => value *= 0.9,
                Model::Pair if column == 1 => value *= 0.95,
                Model::Cold if column == 0 && self.position == 0 => value *= 5.0,
                Model::Iid
                | Model::Unequal
                | Model::Ar
                | Model::StrongAr
                | Model::Effect
                | Model::Pair
                | Model::PairNull
                | Model::Single
                | Model::Eight
                | Model::Cold
                | Model::Carryover
                | Model::Rare
                | Model::AsymmetricTail => {},
            }
            *row.get_mut(column).expect("row column") =
                (value * 1_000_000.0).round().max(1.0) as u64;
            self.previous = column;
        }
        self.position = self.position.wrapping_add(1);
        row
    }

    /// Measured runs: a fixed count, or the runs fast's automatic planning keeps.
    fn rows(&mut self, options: &Options) -> Vec<Vec<u64>> {
        if !options.auto {
            return std::iter::repeat_with(|| self.run())
                .take(options.runs)
                .collect();
        }
        let planner = Planner {
            target: options.runs as f64 * self.specific.len() as f64 * 0.001,
            minimum: MINIMUM_RUNS,
            warmup: true,
        };
        let mut pilot = Vec::new();
        let mut costs = Vec::new();
        let mut elapsed = 0.0;
        loop {
            let row = self.run();
            let cost = row.iter().map(|&value| value as f64 / 1e9).sum::<f64>();
            elapsed += cost;
            costs.push(cost);
            pilot.push(row);
            if planner.pilot_complete(elapsed, &costs) {
                break;
            }
        }
        let additional = planner.additional(elapsed, &costs, &pilot);
        let mut rows: Vec<_> = pilot
            .into_iter()
            .skip(usize::from(planner.discard_first(&costs)))
            .collect();
        rows.extend(std::iter::repeat_with(|| self.run()).take(additional));
        rows
    }

    /// True mean of a command relative to 1 ms, over the planned runs where the
    /// model is not stationary.
    fn mean(&self, column: usize) -> f64 {
        let runs = self.scale as f64;
        match self.model {
            Model::Effect if column == 1 => 0.9,
            Model::Pair if column == 1 => 0.95,
            Model::Drift => 1.0 + 0.05 * column as f64 * (runs - 1.0) / runs,
            Model::Periodic => {
                1.0 + (0..self.scale)
                    .map(|run| 0.2 * (run as f64 * 0.4 + column as f64).sin())
                    .sum::<f64>()
                    / runs
            },
            Model::Carryover => {
                1.0 + 0.05
                    * (if column == 0 { 1.0 / 16.0 } else { 5.0 / 16.0 } + 3.0 / (16.0 * runs))
            },
            Model::Rare => 1.029,
            Model::AsymmetricTail if column == 1 => 1.099,
            Model::Iid
            | Model::Unequal
            | Model::Common
            | Model::Ar
            | Model::StrongAr
            | Model::Skew
            | Model::Effect
            | Model::Pair
            | Model::PairNull
            | Model::Single
            | Model::Eight
            | Model::Cold
            | Model::AsymmetricTail => 1.0,
        }
    }
}

impl Totals {
    fn trial(&mut self, options: &Options, trial: usize) -> Moments {
        let mut moments = Moments::default();
        let mut workload = Workload::new(options, trial);
        let rows = workload.rows(options);
        self.runs = self.runs.wrapping_add(rows.len());
        let result = statistics::analyze(&rows, None);
        let mut covered = true;
        let mut wrong = false;
        let mut detected = false;
        let mut unbounded = false;
        let mut insufficient = false;
        let mut flagged = false;
        let count = result.comparisons.len() as f64;
        for comparison in &result.comparisons {
            let truth = comparison.denominator.map_or_else(
                || workload.mean(0) * 1_000_000.0,
                |denominator| workload.mean(comparison.numerator) / workload.mean(denominator),
            );
            moments.log_bias += (comparison.estimate / truth).ln() / count;
            flagged |= comparison.correlated;
            if let Some(interval) = comparison.interval {
                covered &= interval.lower <= truth && interval.upper >= truth;
                if interval.lower > 0.0 {
                    moments.log_width += (interval.upper / interval.lower).ln();
                    moments.bounded = moments.bounded.wrapping_add(1);
                }
                wrong |= comparison.denominator.is_some()
                    && ((comparison.decision == Decision::Less && truth >= 1.0)
                        || (comparison.decision == Decision::More && truth <= 1.0));
                if comparison.numerator == 1 && comparison.denominator == Some(0) {
                    detected = comparison.decision == Decision::Less;
                }
            } else if comparison.decision == Decision::Insufficient {
                insufficient = true;
            } else {
                unbounded = true;
            }
        }
        self.covered = self.covered.wrapping_add(usize::from(covered));
        self.false_direction = self.false_direction.wrapping_add(usize::from(wrong));
        self.detected = self.detected.wrapping_add(usize::from(detected));
        self.unbounded = self.unbounded.wrapping_add(usize::from(unbounded));
        self.insufficient = self.insufficient.wrapping_add(usize::from(insufficient));
        self.flagged = self.flagged.wrapping_add(usize::from(flagged));
        moments
    }

    const fn merge(&mut self, other: Self) {
        self.covered = self.covered.wrapping_add(other.covered);
        self.false_direction = self.false_direction.wrapping_add(other.false_direction);
        self.detected = self.detected.wrapping_add(other.detected);
        self.unbounded = self.unbounded.wrapping_add(other.unbounded);
        self.insufficient = self.insufficient.wrapping_add(other.insufficient);
        self.flagged = self.flagged.wrapping_add(other.flagged);
        self.runs = self.runs.wrapping_add(other.runs);
    }
}

fn main() {
    let options = Options::parse_from(std::env::args_os().filter(|argument| argument != "--bench"));
    assert!(options.trials > 0 && options.workers > 0 && options.runs > 0);
    assert!(
        !options.auto
            || !matches!(
                options.model,
                Model::Drift | Model::Periodic | Model::Carryover
            ),
        "their true means depend on a fixed run count"
    );
    let started = Instant::now();
    let next = AtomicUsize::new(0);
    let (total, moments) = thread::scope(|scope| {
        let handles: Vec<_> = std::iter::repeat_with(|| {
            scope.spawn(|| {
                let mut result = Totals::default();
                let mut moments = Vec::new();
                loop {
                    let trial = next.fetch_add(1, Ordering::Relaxed);
                    if trial >= options.trials {
                        break;
                    }
                    moments.push((trial, result.trial(&options, trial)));
                }
                (result, moments)
            })
        })
        .take(options.workers)
        .collect();
        let mut total = Totals::default();
        let mut moments = vec![Moments::default(); options.trials];
        for handle in handles {
            let (counts, parts) = handle.join().expect("simulation worker");
            total.merge(counts);
            for (trial, part) in parts {
                *moments.get_mut(trial).expect("trial index") = part;
            }
        }
        (total, moments)
    });
    let trials = options.trials as f64;
    let log_bias = moments.iter().map(|m| m.log_bias).sum::<f64>();
    let log_width = moments.iter().map(|m| m.log_width).sum::<f64>();
    let bounded = moments.iter().map(|m| m.bounded).sum::<usize>();
    println!(
        "{}",
        serde_json::json!({
            "method": statistics::analyze(&[], None).method,
            "model": options.model,
            "planning": if options.auto { "automatic" } else { "fixed" },
            "trials": options.trials,
            "runs": options.runs,
            "seed": format!("{:016x}", options.seed),
            "coverage": total.covered as f64 / trials,
            "coverage_wilson_one_sided_95": wilson(total.covered, options.trials),
            "false_direction_wilson_one_sided_95": wilson(total.false_direction, options.trials),
            "mean_runs": total.runs as f64 / trials,
            "mean_log_bias": log_bias / trials,
            "mean_log_width": (bounded > 0).then(|| log_width / bounded as f64),
            "totals": total,
            "seconds": started.elapsed().as_secs_f64(),
        })
    );
}

fn normal(random: &mut Random) -> f64 {
    let u = 1.0 - random.unit();
    let angle = std::f64::consts::TAU * random.unit();
    (-2.0 * u.ln()).sqrt() * angle.cos()
}

fn parse_seed(text: &str) -> Result<u64, String> {
    u64::from_str_radix(text, 16).map_err(|_| "expected hexadecimal seed".into())
}

fn wilson(successes: usize, trials: usize) -> (f64, f64) {
    let p = successes as f64 / trials as f64;
    let z = 1.644_853_626_951_472_2_f64;
    let n = trials as f64;
    let center = (p + z * z / (2.0 * n)) / (1.0 + z * z / n);
    let radius = z * (p * (1.0 - p) / n + z * z / (4.0 * n * n)).sqrt() / (1.0 + z * z / n);
    (center - radius, center + radius)
}
