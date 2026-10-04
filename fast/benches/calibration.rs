use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

use clap::{Parser, ValueEnum};
use fast::random::Random;
use fast::statistics::{self, Decision};
use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
enum Cell {
    Iid,
    Unequal,
    Common,
    Ar,
    Skew,
    Effect,
    Single,
    Eight,
    Auto,
    StrongAr,
    Drift,
    Periodic,
    Carryover,
    Rare,
    AsymmetricTail,
    Cancelled,
}

#[derive(Parser)]
struct Options {
    #[arg(default_value_t = 10_000)]
    trials: usize,
    #[arg(default_value_t = 200)]
    blocks: usize,
    #[arg(value_enum, default_value = "iid")]
    cell: Cell,
    #[arg(default_value_t = 4)]
    workers: usize,
    #[arg(default_value = "d91453c274b8109a", value_parser = parse_seed)]
    seed: u64,
}

#[derive(Clone, Copy, Default, Serialize)]
struct Totals {
    covered: usize,
    false_direction: usize,
    detected: usize,
    insufficient: usize,
    blocks: usize,
    log_bias: f64,
    mean_log_width: f64,
}

struct Workload {
    rng: Random,
    common: f64,
    specific: Vec<f64>,
    previous: usize,
    position: usize,
    scale_blocks: usize,
    cell: Cell,
}

impl Workload {
    fn new(options: &Options, trial: usize) -> Self {
        let mut rng = Random::new(options.seed.wrapping_add(trial as u64));
        let columns = match options.cell {
            Cell::Single => 1,
            Cell::Eight => 8,
            Cell::Iid
            | Cell::Unequal
            | Cell::Common
            | Cell::Ar
            | Cell::Skew
            | Cell::Effect
            | Cell::Auto
            | Cell::StrongAr
            | Cell::Drift
            | Cell::Periodic
            | Cell::Carryover
            | Cell::Rare
            | Cell::AsymmetricTail
            | Cell::Cancelled => 4,
        };
        let common = normal(&mut rng);
        let specific = std::iter::repeat_with(|| normal(&mut rng))
            .take(columns)
            .collect();
        Self {
            rng,
            common,
            specific,
            previous: 0,
            position: 0,
            scale_blocks: options.blocks,
            cell: options.cell,
        }
    }

    fn block(&mut self) -> Vec<u64> {
        self.common = 0.5 * self.common + 0.75_f64.sqrt() * normal(&mut self.rng);
        let mut row = vec![0; self.specific.len()];
        let mut order: Vec<_> = self.specific.iter_mut().zip(&mut row).enumerate().collect();
        self.rng.shuffle(&mut order);
        for (column, (state, output)) in order {
            let sigma = if self.cell == Cell::Unequal {
                0.03 + 0.03 * column as f64
            } else {
                0.1
            };
            let rho = match self.cell {
                Cell::Ar => 0.5,
                Cell::StrongAr => 0.9,
                Cell::Iid
                | Cell::Unequal
                | Cell::Common
                | Cell::Skew
                | Cell::Effect
                | Cell::Single
                | Cell::Eight
                | Cell::Auto
                | Cell::Drift
                | Cell::Periodic
                | Cell::Carryover
                | Cell::Rare
                | Cell::AsymmetricTail
                | Cell::Cancelled => 0.0,
            };
            *state = rho * *state + (1.0_f64 - rho * rho).sqrt() * normal(&mut self.rng);
            let mut value = (sigma * *state - 0.5 * sigma * sigma).exp();
            match self.cell {
                Cell::Common => value *= (0.2 * self.common - 0.02).exp(),
                Cell::Skew => value = (0.7 * normal(&mut self.rng) - 0.245).exp(),
                Cell::Drift => {
                    value *=
                        1.0 + column as f64 * self.position as f64 / self.scale_blocks as f64 * 0.1;
                },
                Cell::Periodic => {
                    value *= 1.0 + 0.2 * (self.position as f64 * 0.4 + column as f64).sin();
                },
                Cell::Carryover if self.previous == 0 => value *= 1.05,
                Cell::Rare if self.rng.unit() < 0.001 => value *= 30.0,
                Cell::AsymmetricTail if column == 1 && self.rng.unit() < 0.001 => {
                    value *= 100.0;
                },
                Cell::Effect if column == 1 => value *= 0.9,
                Cell::Iid
                | Cell::Unequal
                | Cell::Ar
                | Cell::StrongAr
                | Cell::Single
                | Cell::Eight
                | Cell::Auto
                | Cell::Cancelled
                | Cell::Carryover
                | Cell::Rare
                | Cell::AsymmetricTail
                | Cell::Effect => {},
            }
            *output = (value * 1_000_000.0).round().max(1.0) as u64;
            self.previous = column;
        }
        self.position = self.position.wrapping_add(1);
        row
    }

    fn rows(&mut self, count: usize) -> Vec<Vec<u64>> {
        let count = if self.cell == Cell::Auto {
            let target = count as f64 * self.specific.len() as f64 * 0.001;
            let mut pilots = Vec::new();
            let mut costs = Vec::new();
            let mut spent = 0.0;
            loop {
                let row = self.block();
                let cost = row.iter().map(|&v| v as f64 / 1e9).sum::<f64>();
                pilots.push(row);
                costs.push(cost);
                spent += cost;
                if statistics::pilot_complete(spent, &costs, target) {
                    break;
                }
            }
            statistics::production_count(target - spent, &costs, &pilots)
        } else {
            count
        };
        std::iter::repeat_with(|| self.block())
            .take(count)
            .collect()
    }

    fn mean(&self, column: usize) -> f64 {
        match self.cell {
            Cell::Effect if column == 1 => 0.9,
            Cell::Drift => {
                1.0 + 0.05 * column as f64 * self.scale_blocks.wrapping_sub(1) as f64
                    / self.scale_blocks as f64
            },
            Cell::Periodic => {
                1.0 + (0..self.scale_blocks)
                    .map(|b| 0.2 * (b as f64 * 0.4 + column as f64).sin())
                    .sum::<f64>()
                    / self.scale_blocks as f64
            },
            Cell::Carryover => {
                1.0 + 0.05
                    * (if column == 0 { 1.0 / 16.0 } else { 5.0 / 16.0 }
                        + 3.0 / (16.0 * self.scale_blocks as f64))
            },
            Cell::Rare => 1.029,
            Cell::AsymmetricTail if column == 1 => 1.099,
            Cell::Iid
            | Cell::Unequal
            | Cell::Common
            | Cell::Ar
            | Cell::Skew
            | Cell::Single
            | Cell::Eight
            | Cell::Auto
            | Cell::StrongAr
            | Cell::Cancelled
            | Cell::Effect
            | Cell::AsymmetricTail => 1.0,
        }
    }
}

impl Totals {
    fn trial(&mut self, options: &Options, trial: usize) {
        let mut workload = Workload::new(options, trial);
        let rows = workload.rows(options.blocks);
        self.blocks = self.blocks.wrapping_add(rows.len());
        let result = statistics::analyze(
            &rows,
            options.cell != Cell::Cancelled,
            (trial as u64) ^ 0xFEE1_DEAD,
            None,
        );
        let mut covered = true;
        let mut wrong = false;
        let mut detected = false;
        let mut insufficient = false;
        for comparison in &result.comparisons {
            let truth = comparison.denominator.map_or_else(
                || workload.mean(0) * 1_000_000.0,
                |denominator| workload.mean(comparison.numerator) / workload.mean(denominator),
            );
            self.log_bias += (comparison.estimate / truth).ln() / result.comparisons.len() as f64;
            if let Some(interval) = comparison.interval {
                covered &= interval.lower <= truth && interval.upper >= truth;
                self.mean_log_width +=
                    (interval.upper / interval.lower).ln() / result.comparisons.len() as f64;
                wrong |= comparison.denominator.is_some()
                    && ((comparison.decision == Decision::Less && truth >= 1.0)
                        || (comparison.decision == Decision::More && truth <= 1.0));
                if comparison.numerator == 1 && comparison.denominator == Some(0) {
                    detected = comparison.decision == Decision::Less;
                }
            } else {
                insufficient = true;
                covered = false;
            }
        }
        self.covered = self.covered.wrapping_add(usize::from(covered));
        self.false_direction = self.false_direction.wrapping_add(usize::from(wrong));
        self.detected = self.detected.wrapping_add(usize::from(detected));
        self.insufficient = self.insufficient.wrapping_add(usize::from(insufficient));
    }

    fn merge(&mut self, other: Self) {
        self.covered = self.covered.wrapping_add(other.covered);
        self.false_direction = self.false_direction.wrapping_add(other.false_direction);
        self.detected = self.detected.wrapping_add(other.detected);
        self.insufficient = self.insufficient.wrapping_add(other.insufficient);
        self.blocks = self.blocks.wrapping_add(other.blocks);
        self.log_bias += other.log_bias;
        self.mean_log_width += other.mean_log_width;
    }
}

fn main() {
    let options = Options::parse_from(std::env::args_os().filter(|argument| argument != "--bench"));
    assert!(options.trials > 0 && options.workers > 0 && options.blocks > 0);
    let started = Instant::now();
    let next = AtomicUsize::new(0);
    let total = thread::scope(|scope| {
        let handles: Vec<_> = std::iter::repeat_with(|| {
            scope.spawn(|| {
                let mut result = Totals::default();
                loop {
                    let trial = next.fetch_add(1, Ordering::Relaxed);
                    if trial >= options.trials {
                        break;
                    }
                    result.trial(&options, trial);
                }
                result
            })
        })
        .take(options.workers)
        .collect();
        let mut total = Totals::default();
        for handle in handles {
            total.merge(handle.join().expect("simulation worker"));
        }
        total
    });
    println!(
        "{}",
        serde_json::json!({
            "cell":options.cell, "trials":options.trials, "requested_blocks":options.blocks,
            "seed":format!("{:016x}",options.seed), "replicates":20000,
            "coverage":total.covered as f64/options.trials as f64,
            "coverage_wilson_one_sided_95":wilson(total.covered,options.trials),
            "false_direction_wilson_one_sided_95":wilson(total.false_direction,options.trials),
            "mean_blocks":total.blocks as f64/options.trials as f64,
            "mean_log_bias":total.log_bias/options.trials as f64,
            "mean_log_width":total.mean_log_width/options.trials as f64,
            "totals":total, "seconds":started.elapsed().as_secs_f64(),
        })
    );
}

fn normal(rng: &mut Random) -> f64 {
    let u = 1.0 - rng.unit();
    let angle = std::f64::consts::TAU * rng.unit();
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
