use std::collections::BTreeMap;
use std::io::{self, IsTerminal as _, Write as _};

use anstream::{AutoStream, ColorChoice};
use anstyle::{AnsiColor, Style};
use fast::runner::{Activity, Observation, Task};
use unicode_width::{UnicodeWidthChar as _, UnicodeWidthStr as _};

use crate::cli::{Cli, Color};
use crate::experiment::{Phase, Result, Role, Text, id};
use crate::statistics::{Comparison, Decision};

const HEADING: Style = Style::new()
    .bold()
    .fg_color(Some(anstyle::Color::Ansi(AnsiColor::Cyan)));
const LESS: Style = Style::new().fg_color(Some(anstyle::Color::Ansi(AnsiColor::Green)));
const MORE: Style = Style::new().fg_color(Some(anstyle::Color::Ansi(AnsiColor::Red)));
const UNRESOLVED: Style = Style::new().fg_color(Some(anstyle::Color::Ansi(AnsiColor::Yellow)));

struct ResourceRow {
    label: &'static str,
    cells: Vec<String>,
}

pub(crate) fn print(result: &Result, options: &Cli) -> io::Result<()> {
    drop(diagnostics(result));
    let stdout = io::stdout();
    if options.json {
        let mut out = stdout.lock();
        serde_json::to_writer(&mut out, result)?;
        writeln!(out)?;
        return out.flush();
    }
    let color = match options.color {
        Color::Always => ColorChoice::Always,
        Color::Never => ColorChoice::Never,
        Color::Auto => {
            if stdout.is_terminal()
                && std::env::var_os("TERM").is_none_or(|v| v != "dumb")
                && std::env::var_os("NO_COLOR").is_none_or(|v| v.is_empty())
            {
                ColorChoice::Always
            } else {
                ColorChoice::Never
            }
        },
    };
    let mut out = AutoStream::new(stdout.lock(), color);
    let width = terminal_width();
    let separator = if options.ascii { "-" } else { "·" };
    writeln!(
        out,
        "{HEADING}fast{HEADING:#} {separator} {} command{} {separator} {} measured run{} each",
        result.candidates.len(),
        if result.candidates.len() == 1 {
            ""
        } else {
            "s"
        },
        result.analysis.blocks,
        if result.analysis.blocks == 1 { "" } else { "s" }
    )?;
    writeln!(out)?;
    for candidate in &result.candidates {
        let words: Vec<_> = if result.execution == "sh_c" {
            candidate.argv.iter().skip(2).map(Text::display).collect()
        } else {
            candidate
                .argv
                .iter()
                .map(|arg| format!("'{}'", arg.display().replace('\'', "'\\''")))
                .collect()
        };
        wrapped(
            &mut out,
            &format!("{}  {}", candidate.id, words.join(" ")),
            width,
        )?;
    }
    timing(&mut out, result, options.details, width)?;
    resources(&mut out, result, options, width)?;
    conclusions(&mut out, result, options)?;
    if options.details {
        writeln!(
            out,
            "\nContext: {} Linux {}; allowed CPUs {}; seed {}",
            result.context.architecture,
            result.context.kernel.as_deref().unwrap_or("unknown"),
            result.context.allowed_cpus.as_deref().unwrap_or("unknown"),
            result.seed
        )?;
        if let Some(cpu) = &result.context.cpu_model {
            writeln!(
                out,
                "CPU: {}; microcode {}",
                safe(cpu),
                result.context.microcode.as_deref().unwrap_or("unavailable")
            )?;
        }
        writeln!(
            out,
            "Method: {}; {} resamples per segment length",
            result.analysis.method, result.analysis.replicates
        )?;
    }
    out.flush()
}

fn timing(
    out: &mut impl io::Write,
    result: &Result,
    details: bool,
    width: usize,
) -> io::Result<()> {
    if result.analysis.summaries.is_empty() {
        return partial_timing(out, result, width);
    }
    let has_intervals = result
        .analysis
        .comparisons
        .iter()
        .any(|comparison| comparison.interval.is_some());
    let evidence_heading = if has_intervals {
        "Approx. 95% simultaneous interval"
    } else {
        "Observed range"
    };
    if width < 80 {
        writeln!(out, "\n{HEADING}Timing{HEADING:#}")?;
    } else {
        writeln!(
            out,
            "\n{HEADING}Timing{HEADING:#}          Mean      Run SD       vs A    {evidence_heading}"
        )?;
    }
    for (index, summary) in result.analysis.summaries.iter().enumerate() {
        let comparison = result
            .analysis
            .comparisons
            .iter()
            .find(|c| c.numerator == index && c.denominator == Some(0));
        let (change, interval) = comparison.map_or_else(
            || ("baseline".into(), String::new()),
            |c| {
                (
                    format!("{:+.1}%", 100.0 * (c.estimate - 1.0)),
                    interval_text(c),
                )
            },
        );
        let evidence = if has_intervals {
            interval
        } else {
            format!(
                "{} to {}",
                duration(summary.min_ns as f64),
                duration(summary.max_ns as f64)
            )
        };
        if width < 80 {
            writeln!(
                out,
                "{}  Mean {}; SD {}; {} vs A",
                id(index),
                duration(summary.mean_ns),
                duration(summary.sd_ns),
                change
            )?;
            if !evidence.is_empty() {
                wrapped(out, &format!("   {evidence_heading}: {evidence}"), width)?;
            }
        } else {
            writeln!(
                out,
                "{:<5} {:>14} {:>11} {:>10}    {}",
                id(index),
                duration(summary.mean_ns),
                duration(summary.sd_ns),
                change,
                evidence
            )?;
        }
        if result.candidates.len() == 1
            && let Some(interval) = result.analysis.comparisons.first().and_then(|c| c.interval)
        {
            writeln!(
                out,
                "      Mean-time interval: {} to {}",
                duration(interval.lower),
                duration(interval.upper)
            )?;
        }
        if details {
            writeln!(
                out,
                "      Median {}; range {} to {}",
                duration(summary.median_ns),
                duration(summary.min_ns as f64),
                duration(summary.max_ns as f64)
            )?;
        }
    }
    Ok(())
}

fn partial_timing(out: &mut impl io::Write, result: &Result, width: usize) -> io::Result<()> {
    writeln!(out, "\n{HEADING}Timing{HEADING:#}")?;
    wrapped(
        out,
        "No complete measured block; these are pilot or partial observations:",
        width,
    )?;
    for invocation in result
        .invocations
        .iter()
        .filter(|i| i.role == Role::Command)
    {
        if let Some(reading) = &invocation.observation {
            writeln!(
                out,
                "{} {:?} {}: {} ({:?})",
                id(invocation.candidate),
                invocation.phase,
                invocation.block.wrapping_add(1),
                duration(reading.elapsed_ns as f64),
                reading.outcome
            )?;
        }
    }
    Ok(())
}

fn measured(result: &Result, candidate: usize, phase: Phase, blocks: usize) -> Vec<&Observation> {
    result
        .invocations
        .iter()
        .filter(|i| {
            i.phase == phase
                && i.role == Role::Command
                && i.candidate == candidate
                && i.block < blocks
        })
        .filter_map(|i| i.observation.as_ref())
        .filter(|o| o.outcome.success())
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

fn resources(
    out: &mut impl io::Write,
    result: &Result,
    options: &Cli,
    width: usize,
) -> io::Result<()> {
    let (phase, blocks, heading) = if result.analysis.blocks > 0 {
        (Phase::Production, result.analysis.blocks, "Resources")
    } else {
        (
            Phase::Pilot,
            result.rows(Phase::Pilot).len(),
            "Pilot resources",
        )
    };
    let readings: Vec<_> = (0..result.candidates.len())
        .map(|index| measured(result, index, phase, blocks))
        .collect();
    if readings.iter().all(Vec::is_empty) {
        return Ok(());
    }
    let counters_comparable = readings
        .iter()
        .all(|rows| !rows.is_empty() && counter_mean(rows, "instructions").is_some());
    writeln!(out, "\n{HEADING}{heading}{HEADING:#}")?;
    resource_table(out, &readings, width)?;
    resource_details(out, &readings, options, width)?;
    writeln!(
        out,
        "Counts/times are per-run means; ratios use pooled counts. Resource changes are descriptive."
    )?;
    writeln!(
        out,
        "RSS: wait-accounted peak / inherited launch floor; not a simultaneous tree peak."
    )?;
    if !counters_comparable {
        writeln!(
            out,
            "Hardware counters were unavailable or insufficiently scheduled for comparison."
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
            activity(out, &selected.activity, options.ascii)?;
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

fn activity(out: &mut impl io::Write, activity: &Activity, ascii: bool) -> io::Result<()> {
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
            let branch = if ascii { "|-" } else { "├─" };
            writeln!(
                out,
                "        {branch} thread {}  {name}{padding}  {cpu}",
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
                let alphabet = if ascii {
                    ['.', ':', '-', '=', '+', '*', '#', '@']
                } else {
                    ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█']
                };
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
        writeln!(
            out,
            "      Task detail is incomplete (sampling or retention limit); timing remains complete."
        )?;
    }
    Ok(())
}

fn conclusions(out: &mut impl io::Write, result: &Result, options: &Cli) -> io::Result<()> {
    writeln!(out)?;
    if let Some(note) = &result.analysis.limitation {
        writeln!(out, "{UNRESOLVED}{note}{UNRESOLVED:#}")?;
    }
    for comparison in &result.analysis.comparisons {
        if comparison.decision == Decision::Insufficient && result.analysis.limitation.is_some() {
            continue;
        }
        if comparison.denominator != Some(0) && !options.details {
            continue;
        }
        let Some(denominator) = comparison.denominator else {
            continue;
        };
        let (style, verdict) = match comparison.decision {
            Decision::Less => (LESS, "took less time"),
            Decision::More => (MORE, "took more time"),
            Decision::Equivalent => (LESS, "is within the declared equivalence margin"),
            Decision::Unresolved => (UNRESOLVED, "remains unresolved"),
            Decision::Insufficient => (UNRESOLVED, "has insufficient evidence"),
            Decision::NotApplicable => continue,
        };
        writeln!(
            out,
            "{style}{} {verdict} versus {}.{style:#}{}",
            id(comparison.numerator),
            id(denominator),
            if comparison.sensitivity_stable {
                ""
            } else {
                " Dependence-sensitive."
            }
        )?;
        if options.details && denominator != 0 {
            writeln!(
                out,
                "  {:+.1}% time change; {}",
                (comparison.estimate - 1.0) * 100.0,
                interval_text(comparison)
            )?;
        }
    }
    if result.completed
        && result.analysis.limitation.is_none()
        && result.candidates.len() > 2
        && !options.details
    {
        overall_conclusion(out, result)?;
    }
    if result.analysis.limitation.is_none() {
        writeln!(
            out,
            "Approximate intervals assume stationary, weakly dependent runs."
        )?;
        writeln!(
            out,
            "Drift, strong dependence, or unseen rare tails can invalidate them."
        )?;
    }
    if result.protocol.verify.is_none() {
        writeln!(
            out,
            "Correctness: exit status only; output equivalence was not verified."
        )?;
    }
    let measured = result
        .invocations
        .iter()
        .filter(|i| i.phase == Phase::Production && i.role == Role::Command)
        .count();
    writeln!(
        out,
        "{measured} measured invocation{} - {}",
        if measured == 1 { "" } else { "s" },
        safe(&result.stop_reason.replace('_', " "))
    )?;
    if let Some(margin) = options.check {
        let verdict = match result.exit_code {
            0 => "pass",
            4 => "regression",
            5 => "inconclusive",
            _ => "failed execution",
        };
        writeln!(
            out,
            "Nonregression check ({:.2}%): {verdict}",
            margin * 100.0
        )?;
    }
    Ok(())
}

fn overall_conclusion(out: &mut impl io::Write, result: &Result) -> io::Result<()> {
    let improvements: Vec<_> = result
        .analysis
        .comparisons
        .iter()
        .filter(|c| c.denominator == Some(0) && c.decision == Decision::Less)
        .map(|c| c.numerator)
        .collect();
    for (position, &left) in improvements.iter().enumerate() {
        for &right in improvements.iter().skip(position.wrapping_add(1)) {
            if result.analysis.comparisons.iter().any(|c| {
                c.denominator == Some(left)
                    && c.numerator == right
                    && c.decision == Decision::Unresolved
            }) {
                writeln!(
                    out,
                    "{} and {} remain unresolved against each other.",
                    id(left),
                    id(right)
                )?;
            }
        }
    }
    let winner = (0..result.candidates.len()).find(|&candidate| {
        result
            .analysis
            .comparisons
            .iter()
            .filter(|c| c.numerator == candidate || c.denominator == Some(candidate))
            .all(|c| {
                (c.numerator == candidate && c.decision == Decision::Less)
                    || (c.denominator == Some(candidate) && c.decision == Decision::More)
            })
    });
    if let Some(winner) = winner {
        writeln!(
            out,
            "{} took less time than every other command.",
            id(winner)
        )?;
    }
    Ok(())
}

fn diagnostics(result: &Result) -> io::Result<()> {
    let mut out = io::stderr().lock();
    for invocation in &result.invocations {
        if let Some(observation) = &invocation.observation
            && !observation.outcome.success()
        {
            writeln!(
                out,
                "fast: {} {:?}: {:?}",
                id(invocation.candidate),
                invocation.role,
                observation.outcome
            )?;
            let diagnostic = &observation.diagnostics;
            if !diagnostic.first.is_empty() {
                writeln!(out, "{}", safe(&String::from_utf8_lossy(&diagnostic.first)))?;
            }
            if diagnostic.omitted_bytes > 0 {
                writeln!(
                    out,
                    "[{} diagnostic bytes omitted]",
                    diagnostic.omitted_bytes
                )?;
            }
            if !diagnostic.last.is_empty() {
                let tail: Vec<_> = diagnostic.last.iter().copied().collect();
                writeln!(out, "{}", safe(&String::from_utf8_lossy(&tail)))?;
            }
        }
        if let Some(error) = &invocation.error {
            writeln!(out, "fast: {}", safe(error))?;
        }
    }
    Ok(())
}

fn safe(text: &str) -> String {
    text.chars().flat_map(char::escape_debug).collect()
}

fn terminal_width() -> usize {
    if !io::stdout().is_terminal() {
        return 100;
    }
    // SAFETY: TIOCGWINSZ fills a valid winsize record for stdout.
    unsafe {
        let mut size: libc::winsize = std::mem::zeroed();
        if libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &raw mut size) == 0 && size.ws_col > 0
        {
            usize::from(size.ws_col).max(20)
        } else {
            100
        }
    }
}

fn wrapped(out: &mut impl io::Write, text: &str, width: usize) -> io::Result<()> {
    let mut used = 0_usize;
    for character in text.chars() {
        let cells = character.width().unwrap_or(0);
        if used.wrapping_add(cells) > width {
            write!(out, "\n   ")?;
            used = 3;
        }
        write!(out, "{character}")?;
        used = used.wrapping_add(cells);
    }
    writeln!(out)
}

fn interval_text(comparison: &Comparison) -> String {
    comparison.interval.map_or_else(
        || "unavailable".into(),
        |i| {
            format!(
                "{:+.1}% to {:+.1}%",
                (i.lower - 1.0) * 100.0,
                (i.upper - 1.0) * 100.0
            )
        },
    )
}

fn duration(ns: f64) -> String {
    let (value, unit) = if ns >= 1e9 {
        (ns / 1e9, "s")
    } else if ns >= 1e6 {
        (ns / 1e6, "ms")
    } else if ns >= 1e3 {
        (ns / 1e3, "us")
    } else {
        (ns, "ns")
    };
    format!("{value:.3} {unit}")
}

fn memory(bytes: f64) -> String {
    format!("{:.1} MiB", bytes / (1024.0 * 1024.0))
}
fn number(value: f64) -> String {
    if value >= 1e9 {
        format!("{:.2} G", value / 1e9)
    } else if value >= 1e6 {
        format!("{:.2} M", value / 1e6)
    } else if value >= 1e3 {
        format!("{:.2} k", value / 1e3)
    } else {
        format!("{value:.0}")
    }
}
