//! Per-command resource tables and sampled task activity.

use std::collections::BTreeMap;
use std::io;

use fast::runner::{Activity, Observation, Task};
use unicode_width::UnicodeWidthStr as _;

use super::{HEADING, duration, memory, number, safe, wrapped};
use crate::cli::Cli;
use crate::experiment::{Invocation, id};

struct ResourceRow {
    label: &'static str,
    cells: Vec<String>,
}

/// Observations of one command from the runs the analysis uses.
fn measured<'a>(runs: &[Vec<&'a Invocation>], candidate: usize) -> Vec<&'a Observation> {
    runs.iter()
        .filter_map(|run| run.get(candidate)?.observation.as_ref())
        .collect()
}

fn counter_mean(observations: &[&Observation], name: &str) -> Option<f64> {
    observations
        .iter()
        .try_fold(0.0, |sum, observation| {
            observation
                .counters
                .iter()
                .find(|c| c.name == name)?
                .usable()
                .map(|value| sum + value)
        })
        .map(|sum| sum / observations.len() as f64)
}

fn counter_ratio(observations: &[&Observation], numerator: &str, denominator: &str) -> Option<f64> {
    let mut totals = [0_u128; 2];
    for observation in observations {
        for (name, total) in [numerator, denominator].into_iter().zip(&mut totals) {
            let counter = observation
                .counters
                .iter()
                .find(|counter| counter.name == name)?;
            // Separate events must cover the whole execution, not different multiplexed slices.
            *total = total.wrapping_add(u128::from(counter.fully_scheduled()?));
        }
    }
    let [numerator, denominator] = totals;
    (denominator > 0).then(|| numerator as f64 / denominator as f64)
}

fn counter_variation(observations: &[&Observation], name: &str) -> Option<f64> {
    if observations.len() < 2 {
        return None;
    }
    let mean = counter_mean(observations, name)?;
    if mean <= 0.0 {
        return None;
    }
    let squares = observations.iter().try_fold(0.0, |sum, observation| {
        let value = observation
            .counters
            .iter()
            .find(|counter| counter.name == name)?
            .usable()?;
        Some(sum + (value - mean).powi(2))
    })?;
    Some((squares / observations.len().wrapping_sub(1) as f64).sqrt() / mean)
}

pub(super) fn resources(
    out: &mut impl io::Write,
    runs: &[Vec<&Invocation>],
    options: &Cli,
    width: usize,
) -> io::Result<()> {
    let candidates = runs.first().map_or(0, Vec::len);
    let readings: Vec<_> = (0..candidates).map(|index| measured(runs, index)).collect();
    if readings.iter().all(Vec::is_empty) {
        return Ok(());
    }
    let counters_comparable = readings
        .iter()
        .all(|rows| !rows.is_empty() && counter_mean(rows, "instructions").is_some());
    writeln!(out, "\n{HEADING}Resources{HEADING:#}")?;
    resource_table(out, &readings, width)?;
    resource_details(out, &readings, options, width)?;
    if !counters_comparable {
        wrapped(
            out,
            "Hardware counters were unavailable or insufficiently scheduled for comparison.",
            width,
        )?;
    }
    Ok(())
}

fn resource_details(
    out: &mut impl io::Write,
    readings: &[Vec<&Observation>],
    options: &Cli,
    width: usize,
) -> io::Result<()> {
    let branch_misses_comparable = readings
        .iter()
        .all(|rows| counter_mean(rows, "branch-misses").is_some());
    let cache_comparable = readings
        .iter()
        .all(|rows| counter_mean(rows, "l2-misses").is_some());
    for (index, observations) in readings.iter().enumerate() {
        let Some(selected) = observations.first() else {
            continue;
        };
        let count = observations.len() as f64;
        if selected.activity.tasks.len() > 1 {
            writeln!(
                out,
                "{}: {} tasks observed in run 1; wait-accounted CPU use {:.2} CPUs",
                id(index),
                selected.activity.tasks.len(),
                (selected.user_ns as f64 + selected.system_ns as f64) / selected.elapsed_ns as f64
            )?;
        }
        if options.details {
            writeln!(out, "\n{} resource details:", id(index))?;
            if cache_comparable {
                wrapped(
                    out,
                    &format!(
                        "      L2 request misses {} (excluding L2 prefetch requests)",
                        number(
                            counter_mean(observations, "l2-misses")
                                .expect("comparable cache misses")
                        ),
                    ),
                    width,
                )?;
            }
            if branch_misses_comparable {
                wrapped(
                    out,
                    &format!(
                        "      Branch misses {}",
                        number(
                            counter_mean(observations, "branch-misses").expect("comparable misses")
                        ),
                    ),
                    width,
                )?;
            }
            writeln!(
                out,
                "      Minor/major faults {:.1}/{:.1}; voluntary/involuntary switches {:.1}/{:.1}",
                observations
                    .iter()
                    .map(|o| o.minor_faults as f64)
                    .sum::<f64>()
                    / count,
                observations
                    .iter()
                    .map(|o| o.major_faults as f64)
                    .sum::<f64>()
                    / count,
                observations
                    .iter()
                    .map(|o| o.voluntary_switches as f64)
                    .sum::<f64>()
                    / count,
                observations
                    .iter()
                    .map(|o| o.involuntary_switches as f64)
                    .sum::<f64>()
                    / count
            )?;
            activity(out, &selected.activity, width)?;
        }
    }
    Ok(())
}

fn resource_row(
    label: &'static str,
    readings: &[Vec<&Observation>],
    format: impl Fn(usize, &[&Observation]) -> Option<String>,
) -> Option<ResourceRow> {
    let cells = readings
        .iter()
        .enumerate()
        .map(|(index, observations)| {
            if observations.is_empty() {
                None
            } else {
                format(index, observations)
            }
        })
        .collect::<Option<Vec<_>>>()?;
    Some(ResourceRow { label, cells })
}

fn resource_rows(readings: &[Vec<&Observation>]) -> Vec<ResourceRow> {
    let baseline = counter_mean(readings.first().expect("baseline"), "instructions")
        .filter(|value| *value > 0.0);
    [
        resource_row("CPU time", readings, |_, observations| {
            Some(duration(
                observations
                    .iter()
                    .map(|o| o.user_ns as f64 + o.system_ns as f64)
                    .sum::<f64>()
                    / observations.len() as f64,
            ))
        }),
        resource_row("Instructions (vs A)", readings, |index, observations| {
            let value = counter_mean(observations, "instructions")?;
            Some(if index == 0 {
                number(value)
            } else {
                baseline.map_or_else(
                    || number(value),
                    |base| format!("{} ({:+.1}%)", number(value), 100.0 * (value / base - 1.0)),
                )
            })
        }),
        resource_row("Instruction SD/mean", readings, |_, observations| {
            Some(format!(
                "{:.2}%",
                100.0 * counter_variation(observations, "instructions")?
            ))
        }),
        resource_row("Cycles", readings, |_, observations| {
            counter_mean(observations, "cycles").map(number)
        }),
        resource_row("Instructions/cycle", readings, |_, observations| {
            Some(format!(
                "{:.2}",
                counter_ratio(observations, "instructions", "cycles")?
            ))
        }),
        resource_row("Branch misses/1k instr", readings, |_, observations| {
            Some(format!(
                "{:.2}",
                1000.0 * counter_ratio(observations, "branch-misses", "instructions")?
            ))
        }),
        resource_row("L2 misses/1k instr", readings, |_, observations| {
            Some(format!(
                "{:.2}",
                1000.0 * counter_ratio(observations, "l2-misses", "instructions")?
            ))
        }),
        resource_row("RSS peak/launch", readings, |_, observations| {
            let count = observations.len() as f64;
            let peak = observations
                .iter()
                .map(|o| o.max_rss_bytes as f64)
                .sum::<f64>()
                / count;
            let launch = observations.iter().try_fold(0_u128, |sum, o| {
                o.launch_max_rss_bytes
                    .map(|value| sum.wrapping_add(u128::from(value)))
            });
            Some(launch.map_or_else(
                || format!("{} / n/a", memory(peak)),
                |sum| {
                    format!(
                        "{:.1}/{:.1} MiB",
                        peak / 1_048_576.0,
                        sum as f64 / count / 1_048_576.0
                    )
                },
            ))
        }),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn resource_table(
    out: &mut impl io::Write,
    readings: &[Vec<&Observation>],
    width: usize,
) -> io::Result<()> {
    let rows = resource_rows(readings);
    let label_width = rows.iter().map(|row| row.label.width()).max().unwrap_or(0);
    let mut widths: Vec<_> = (0..readings.len()).map(|index| id(index).width()).collect();
    for row in &rows {
        for (width, cell) in widths.iter_mut().zip(&row.cells) {
            *width = (*width).max(cell.width());
        }
    }
    let occupied = widths.iter().fold(label_width, |sum, width| {
        sum.saturating_add(width.saturating_add(2))
    });
    if width >= 80 && occupied <= width {
        write!(out, "{:label_width$}", "")?;
        for (index, &width) in widths.iter().enumerate() {
            write!(out, "  {:>width$}", id(index))?;
        }
        writeln!(out)?;
        for row in &rows {
            write!(out, "{:<label_width$}", row.label)?;
            for (cell, &width) in row.cells.iter().zip(&widths) {
                write!(out, "  {cell:>width$}")?;
            }
            writeln!(out)?;
        }
    } else {
        for index in 0..readings.len() {
            writeln!(out, "{}", id(index))?;
            for row in &rows {
                wrapped(
                    out,
                    &format!(
                        "  {}  {}",
                        row.label,
                        row.cells.get(index).expect("candidate column")
                    ),
                    width,
                )?;
            }
        }
    }
    Ok(())
}

fn activity(out: &mut impl io::Write, activity: &Activity, width: usize) -> io::Result<()> {
    if activity.tasks.is_empty() {
        return Ok(());
    }
    writeln!(
        out,
        "      Sampled activity from measured run 1 ({} sweeps):",
        activity.sweeps
    )?;
    let mut processes: BTreeMap<(i32, u64), Vec<&Task>> = BTreeMap::new();
    for task in &activity.tasks {
        processes
            .entry((task.pid, task.process_start_ticks))
            .or_default()
            .push(task);
    }
    for ((pid, _), tasks) in processes {
        let parent = tasks.first().expect("process has tasks").parent_pid;
        writeln!(out, "      Process {pid}, observed parent {parent}")?;
        for task in tasks {
            let name = safe(&task.name);
            let cpu = task
                .cpu_percent(activity.clock_ticks_per_second)
                .map_or_else(|| "one observation".into(), |v| format!("{v:.1}% CPU"));
            let padding = " ".repeat(18_usize.saturating_sub(name.width()));
            writeln!(
                out,
                "        ├─ thread {}  {name}{padding}  {cpu}",
                task.tid
            )?;
            if task.samples.len() > 2 {
                let percentages: Vec<_> = task
                    .samples
                    .windows(2)
                    .map(|pair| {
                        let [first, last] = pair else {
                            unreachable!("two samples")
                        };
                        100.0 * last.cpu_ticks.saturating_sub(first.cpu_ticks) as f64
                            / activity.clock_ticks_per_second as f64
                            / (last.elapsed_ns.saturating_sub(first.elapsed_ns) as f64 / 1e9)
                    })
                    .collect();
                let maximum = percentages.iter().copied().fold(0.0_f64, f64::max);
                let alphabet = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
                let strip: String = percentages
                    .chunks(percentages.len().div_ceil(32))
                    .map(|chunk| {
                        let mean = chunk.iter().sum::<f64>() / chunk.len() as f64;
                        *alphabet
                            .get(if maximum > 0.0 {
                                ((mean / maximum) * 7.0).round() as usize
                            } else {
                                0
                            })
                            .expect("normalized level")
                    })
                    .collect();
                writeln!(
                    out,
                    "           {strip}  sampled CPU, own scale 0..{maximum:.1}%"
                )?;
            }
        }
    }
    writeln!(
        out,
        "      100% = one CPU. Short-lived tasks may be missed; no per-thread RSS."
    )?;
    if activity.limited {
        wrapped(
            out,
            "      Task detail is incomplete (sampling or retention limit); timing remains complete.",
            width,
        )?;
    }
    Ok(())
}
