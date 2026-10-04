mod resources;

use std::io::{self, IsTerminal as _, Write as _};

use anstream::{AutoStream, ColorChoice};
use anstyle::{AnsiColor, Style};
use unicode_width::{UnicodeWidthChar as _, UnicodeWidthStr as _};

use self::resources::resources;
use crate::cli::{Cli, Color};
use crate::experiment::{Candidate, Invocation, Planning, Result, Role, Text, id};
use crate::runner::Outcome;
use crate::statistics::{Comparison, Decision, partition};

const HEADING: Style = Style::new()
    .bold()
    .fg_color(Some(anstyle::Color::Ansi(AnsiColor::Cyan)));
const LESS: Style = Style::new().fg_color(Some(anstyle::Color::Ansi(AnsiColor::Green)));
const MORE: Style = Style::new().fg_color(Some(anstyle::Color::Ansi(AnsiColor::Red)));
const UNRESOLVED: Style = Style::new().fg_color(Some(anstyle::Color::Ansi(AnsiColor::Yellow)));
const PROGRAM: Style = Style::new().bold();
/// Ratios at or beyond this factor read better as "3.47× slower" than as a percentage.
const FACTOR: f64 = 1.5;
const TIMELINE_CELLS: usize = 24;
/// Upper bounds of a timeline group's mean relative to the command's mean.
const TIMELINE: [(f64, char); 7] = [
    (0.9, '▁'),
    (0.97, '▂'),
    (1.03, '▃'),
    (1.1, '▄'),
    (1.25, '▅'),
    (1.5, '▆'),
    (2.0, '▇'),
];

pub(crate) fn print(result: &Result, options: &Cli) -> io::Result<()> {
    drop(diagnostics(result));
    let stdout = io::stdout();
    if options.json {
        let mut out = stdout.lock();
        serde_json::to_writer(&mut out, result)?;
        writeln!(out)?;
        return out.flush();
    }
    let color = match resolve_color(options.color) {
        Color::Always => ColorChoice::Always,
        Color::Never => ColorChoice::Never,
        Color::Auto => ColorChoice::Auto,
    };
    let mut out = AutoStream::new(stdout.lock(), color);
    let width = terminal_width();
    let runs = result.measured();
    let commands = result.candidates.len();
    writeln!(
        out,
        "{HEADING}fast{HEADING:#} · {commands} command{} · {} run{}{} · {}\n",
        plural(commands),
        result.analysis.runs,
        plural(result.analysis.runs),
        if commands > 1 { " each" } else { "" },
        seconds(result.wall_ns as f64)
    )?;
    for candidate in &result.candidates {
        legend(&mut out, candidate, width)?;
    }
    if result.analysis.summaries.is_empty() {
        partial_timing(&mut out, result, width)?;
    } else {
        timing(&mut out, result, &runs, options, width)?;
        verdicts(&mut out, result, options, width)?;
    }
    resources(&mut out, &runs, options, width)?;
    status(&mut out, result, options, width)?;
    if options.details {
        details(&mut out, result, width)?;
    }
    out.flush()
}

/// One command as a shell would read it, wrapped between arguments.
fn legend(out: &mut impl io::Write, candidate: &Candidate, width: usize) -> io::Result<()> {
    let prefix = format!("{}  ", candidate.id);
    write!(out, "{prefix}")?;
    let mut used = prefix.width();
    for (index, argument) in candidate.argv.iter().enumerate() {
        let word = shell_word(argument, index == 0);
        if index > 0 {
            if used.saturating_add(1).saturating_add(word.width()) <= width {
                write!(out, " ")?;
                used = used.saturating_add(1);
            } else {
                write!(out, "\n{}", " ".repeat(prefix.width()))?;
                used = prefix.width();
            }
        }
        if index == 0 {
            write!(out, "{PROGRAM}{word}{PROGRAM:#}")?;
        } else {
            write!(out, "{word}")?;
        }
        used = used.saturating_add(word.width());
    }
    writeln!(out)
}

fn timing(
    out: &mut impl io::Write,
    result: &Result,
    runs: &[Vec<&Invocation>],
    options: &Cli,
    width: usize,
) -> io::Result<()> {
    let summaries = &result.analysis.summaries;
    let single = result.analysis.runs < 2;
    let means: Vec<_> = summaries.iter().map(|s| duration(s.mean_ns)).collect();
    let deviations: Vec<_> = summaries
        .iter()
        .map(|s| {
            if single {
                "—".to_owned()
            } else {
                duration(s.sd_ns)
            }
        })
        .collect();
    let column = |title: &str, cells: &[String]| {
        cells
            .iter()
            .map(|cell| cell.width())
            .fold(title.width(), usize::max)
    };
    let ids = (0..summaries.len())
        .map(|index| id(index).width())
        .fold("Time".width(), usize::max);
    let [mean_width, sd_width] = [column("Mean", &means), column("SD", &deviations)];
    let used = ids
        .saturating_add(mean_width)
        .saturating_add(sd_width)
        .saturating_add(7);
    let cells = if result.analysis.runs >= 3 {
        TIMELINE_CELLS
            .min(result.analysis.runs)
            .min(width.saturating_sub(used))
    } else {
        0
    };
    write!(
        out,
        "\n{HEADING}{:<ids$}  {:>mean_width$}  {:>sd_width$}",
        "Time", "Mean", "SD"
    )?;
    if cells > 0 {
        write!(out, "   Timeline")?;
    }
    writeln!(out, "{HEADING:#}")?;
    for (index, summary) in summaries.iter().enumerate() {
        write!(
            out,
            "{:<ids$}  {:>mean_width$}  {:>sd_width$}",
            id(index),
            means.get(index).expect("mean cell"),
            deviations.get(index).expect("deviation cell")
        )?;
        if cells > 0 {
            let values: Vec<_> = runs
                .iter()
                .filter_map(|run| run.get(index)?.observation.as_ref())
                .map(|observation| observation.elapsed_ns)
                .collect();
            write!(out, "   {}", timeline(&values, cells))?;
        }
        writeln!(out)?;
        if options.details && !single {
            writeln!(
                out,
                "{:ids$}  median {}, range {} to {}",
                "",
                duration(summary.median_ns),
                duration(summary.min_ns as f64),
                duration(summary.max_ns as f64)
            )?;
        }
    }
    Ok(())
}

/// Chronological groups of runs, each relative to the command's mean time. The fixed
/// scale keeps a steady command flat and gives slow outliers more room than fast ones.
fn timeline(values: &[u64], cells: usize) -> String {
    let mean =
        values.iter().map(|&value| u128::from(value)).sum::<u128>() as f64 / values.len() as f64;
    (0..cells)
        .map(|cell| {
            let group = values
                .get(partition(values.len(), cells, cell))
                .expect("partition within values");
            let ratio =
                group.iter().map(|&value| value as f64).sum::<f64>() / group.len() as f64 / mean;
            TIMELINE
                .iter()
                .find(|(limit, _)| ratio < *limit)
                .map_or('█', |&(_, glyph)| glyph)
        })
        .collect()
}

fn verdicts(
    out: &mut impl io::Write,
    result: &Result,
    options: &Cli,
    width: usize,
) -> io::Result<()> {
    writeln!(out)?;
    if let Some(limitation) = &result.analysis.limitation {
        write!(out, "{UNRESOLVED}")?;
        wrapped(out, limitation, width)?;
        return write!(out, "{UNRESOLVED:#}");
    }
    for comparison in &result.analysis.comparisons {
        let Some(denominator) = comparison.denominator else {
            if let Some(interval) = comparison.interval {
                writeln!(
                    out,
                    "A: mean {} (95% CI {} to {})",
                    duration(comparison.estimate),
                    duration(interval.lower),
                    duration(interval.upper)
                )?;
            }
            continue;
        };
        if denominator != 0 && !options.details {
            continue;
        }
        let (style, text) = verdict(comparison, result.analysis.runs, options.margin);
        write!(out, "{style}")?;
        wrapped(out, &text, width)?;
        write!(out, "{style:#}")?;
    }
    if result.candidates.len() > 2 && !options.details {
        overall_conclusion(out, result)?;
    }
    correlation(out, result, options, width)
}

/// One note for every shown comparison whose runs drifted or were correlated. Batching
/// already widens those intervals; strong or long-lived correlation can still defeat it.
fn correlation(
    out: &mut impl io::Write,
    result: &Result,
    options: &Cli,
    width: usize,
) -> io::Result<()> {
    let flagged: Vec<_> = result
        .analysis
        .comparisons
        .iter()
        .filter(|c| c.correlated && (options.details || c.denominator.is_none_or(|d| d == 0)))
        .map(|c| {
            c.denominator.map_or_else(
                || id(c.numerator),
                |denominator| format!("{} vs {}", id(c.numerator), id(denominator)),
            )
        })
        .collect();
    if flagged.is_empty() {
        return Ok(());
    }
    wrapped(
        out,
        &format!(
            "Consecutive runs drifted or were correlated ({}); the interval{} allow{} for this \
             only approximately.",
            flagged.join(", "),
            plural(flagged.len()),
            if flagged.len() == 1 { "s" } else { "" }
        ),
        width,
    )
}

fn verdict(comparison: &Comparison, runs: usize, margin: Option<f64>) -> (Style, String) {
    let numerator = id(comparison.numerator);
    let denominator = comparison.denominator.map_or_else(String::new, id);
    let ratio = comparison.estimate;
    match (comparison.decision, comparison.interval) {
        (Decision::Less | Decision::More, Some(interval)) => {
            let style = if comparison.decision == Decision::Less {
                LESS
            } else {
                MORE
            };
            let text = if ratio >= FACTOR {
                format!(
                    "{numerator} is {}× slower than {denominator} (95% CI {}–{}×)",
                    factor(ratio),
                    factor(interval.lower),
                    factor(interval.upper)
                )
            } else if ratio <= FACTOR.recip() {
                let slowest = factor(interval.upper.recip());
                let range = if interval.lower > 0.0 {
                    format!("{slowest}–{}×", factor(interval.lower.recip()))
                } else {
                    format!("at least {slowest}×")
                };
                format!(
                    "{numerator} is {}× faster than {denominator} (95% CI {range})",
                    factor(ratio.recip())
                )
            } else if ratio >= 1.0 {
                format!(
                    "{numerator} takes {:.1}% more time than {denominator} (95% CI {:.1}–{:.1}%)",
                    100.0 * (ratio - 1.0),
                    100.0 * (interval.lower - 1.0),
                    100.0 * (interval.upper - 1.0)
                )
            } else {
                format!(
                    "{numerator} takes {:.1}% less time than {denominator} (95% CI {:.1}–{:.1}%)",
                    100.0 * (1.0 - ratio),
                    100.0 * (1.0 - interval.upper),
                    100.0 * (1.0 - interval.lower)
                )
            };
            (style, text)
        },
        (Decision::Equivalent, Some(interval)) => (
            LESS,
            format!(
                "{numerator} is within ±{}% of {denominator} (95% CI {} to {})",
                100.0 * margin.unwrap_or(0.0),
                change(interval.lower),
                change(interval.upper)
            ),
        ),
        (Decision::Unresolved, Some(interval)) => (
            UNRESOLVED,
            if interval.lower > 0.0 {
                format!(
                    "{numerator} vs {denominator}: no clear difference (95% CI {} to {})",
                    change(interval.lower),
                    change(interval.upper)
                )
            } else {
                format!(
                    "{numerator} vs {denominator}: no clear difference (95% CI up to {})",
                    change(interval.upper)
                )
            },
        ),
        (Decision::Unresolved, None) => (
            UNRESOLVED,
            format!(
                "{numerator} vs {denominator}: too noisy to bound with {runs} run{}",
                plural(runs)
            ),
        ),
        (
            Decision::Insufficient
            | Decision::NotApplicable
            | Decision::Less
            | Decision::More
            | Decision::Equivalent,
            _,
        ) => (
            UNRESOLVED,
            format!("{numerator} vs {denominator}: no measurable variation, so no interval"),
        ),
    }
}

/// A time ratio in words: a factor beyond 1.5×, otherwise a percentage of time.
fn change(ratio: f64) -> String {
    if ratio >= FACTOR {
        format!("{}× slower", factor(ratio))
    } else if ratio <= FACTOR.recip() {
        format!("{}× faster", factor(ratio.recip()))
    } else if ratio >= 1.0 {
        format!("{:.1}% more", 100.0 * (ratio - 1.0))
    } else {
        format!("{:.1}% less", 100.0 * (1.0 - ratio))
    }
}

/// Three significant digits.
fn factor(value: f64) -> String {
    if value < 10.0 {
        format!("{value:.2}")
    } else if value < 100.0 {
        format!("{value:.1}")
    } else {
        format!("{value:.0}")
    }
}

fn partial_timing(out: &mut impl io::Write, result: &Result, width: usize) -> io::Result<()> {
    writeln!(out)?;
    wrapped(
        out,
        "No run completed for every command; these are the individual observations:",
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
                "{}  run {}: {} ({})",
                id(invocation.candidate),
                invocation.run.wrapping_add(1),
                duration(reading.elapsed_ns as f64),
                if reading.outcome.success() {
                    "succeeded"
                } else {
                    "failed"
                }
            )?;
        }
    }
    Ok(())
}

fn status(
    out: &mut impl io::Write,
    result: &Result,
    options: &Cli,
    width: usize,
) -> io::Result<()> {
    if result.exit_code >= 128 {
        let planned = if result.plan.planned_runs > 0 {
            format!(" of {} planned", result.plan.planned_runs)
        } else {
            String::new()
        };
        writeln!(out)?;
        write!(out, "{UNRESOLVED}")?;
        wrapped(
            out,
            &format!(
                "Interrupted after {} completed run{}{planned}; the report uses only those.",
                result.analysis.runs,
                plural(result.analysis.runs)
            ),
            width,
        )?;
        write!(out, "{UNRESOLVED:#}")?;
    }
    if let Some(margin) = options.check {
        let verdict = match result.exit_code {
            0 => "pass",
            4 => "regression",
            5 => "inconclusive",
            128.. => "interrupted",
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

fn details(out: &mut impl io::Write, result: &Result, width: usize) -> io::Result<()> {
    writeln!(out, "\n{HEADING}Details{HEADING:#}")?;
    let plan = &result.plan;
    let planning = match plan.planning {
        Planning::Fixed => "fixed run count".to_owned(),
        Planning::Automatic {
            target_seconds,
            strict,
        } => format!(
            "automatic, {} {}",
            seconds(target_seconds * 1e9),
            if strict { "budget" } else { "target" }
        ),
    };
    wrapped(
        out,
        &format!(
            "Plan: {planning}; {} warmup, {} pilot ({} discarded as warmup), {} measured runs.",
            plan.warmup_runs, plan.pilot_runs, plan.discarded_runs, result.analysis.runs
        ),
        width,
    )?;
    wrapped(
        out,
        &format!(
            "Intervals: 95% simultaneous Fieller intervals for ratios of mean times, from {} \
             batches of consecutive runs ({}). They treat batch means as independent; drift, \
             load shifts and rare slow runs absent from the measurements can invalidate them.",
            result.analysis.batches, result.analysis.method
        ),
        width,
    )?;
    let legend: Vec<_> = TIMELINE
        .iter()
        .map(|(limit, glyph)| format!("{glyph} <{limit}×"))
        .collect();
    wrapped(
        out,
        &format!(
            "Timeline: groups of consecutive runs relative to the command's mean: {} █ ≥2×.",
            legend.join(" ")
        ),
        width,
    )?;
    wrapped(
        out,
        "Resources: per-run means; ratios pool counts across runs. Resource differences are \
         descriptive. RSS peak/launch is the wait-accounted high-water mark beside the memory \
         inherited at launch, not a simultaneous process-tree peak.",
        width,
    )?;
    if result.protocol.verify.is_none() {
        wrapped(
            out,
            "Correctness: exit status only; output equivalence was not verified.",
            width,
        )?;
    }
    writeln!(
        out,
        "Context: {} Linux {}; allowed CPUs {}; seed {}",
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
    Ok(())
}

/// An argument as a POSIX shell would read it back: bare when only safe characters
/// occur, single-quoted when printable, and `$'...'` with escapes for control
/// characters, invisible formatting characters and bytes that are not UTF-8.
fn shell_word(argument: &Text, program: bool) -> String {
    let bare = |c: char| c.is_alphanumeric() || "%+,-./:=@_".contains(c);
    match argument {
        Text::Utf8(text)
            if !text.is_empty()
                && text.chars().all(bare)
                && !text.starts_with('=')
                && (!program || !text.contains('=')) =>
        {
            text.clone()
        },
        Text::Utf8(text) if text.chars().all(printable) => {
            format!("'{}'", text.replace('\'', r"'\''"))
        },
        Text::Utf8(text) => escaped(text.as_bytes()),
        Text::Bytes(bytes) => escaped(bytes),
    }
}

fn printable(character: char) -> bool {
    matches!(character, '\'' | '"' | '\\')
        || character.escape_debug().eq(std::iter::once(character))
}

fn escaped(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut word = String::from("$'");
    for chunk in bytes.utf8_chunks() {
        for character in chunk.valid().chars() {
            let code = u32::from(character);
            match character {
                '\'' | '\\' => {
                    word.push('\\');
                    word.push(character);
                },
                '\n' => word.push_str(r"\n"),
                '\t' => word.push_str(r"\t"),
                '\r' => word.push_str(r"\r"),
                '\u{1b}' => word.push_str(r"\e"),
                _ if printable(character) => word.push(character),
                _ if code < 0x80 => write!(word, r"\x{code:02x}").expect("formatting a String"),
                _ if code <= 0xFFFF => {
                    write!(word, r"\u{code:04x}").expect("formatting a String");
                },
                _ => write!(word, r"\U{code:08x}").expect("formatting a String"),
            }
        }
        for byte in chunk.invalid() {
            write!(word, r"\x{byte:02x}").expect("formatting a String");
        }
    }
    word.push('\'');
    word
}

pub(crate) const fn plural(count: usize) -> &'static str {
    if count == 1 { "" } else { "s" }
}

/// A wall-clock span: minutes and seconds from one minute, otherwise as `duration`.
pub(crate) fn seconds(ns: f64) -> String {
    let value = ns / 1e9;
    if value >= 60.0 {
        let total = value.round() as u64;
        format!("{}m {:02}s", total / 60, total % 60)
    } else {
        duration(ns)
    }
}

fn duration(ns: f64) -> String {
    let (value, unit) = if ns >= 1e9 {
        (ns / 1e9, "s")
    } else if ns >= 1e6 {
        (ns / 1e6, "ms")
    } else if ns >= 1e3 {
        (ns / 1e3, "µs")
    } else {
        (ns, "ns")
    };
    if value >= 10.0 {
        format!("{value:.1} {unit}")
    } else {
        format!("{value:.2} {unit}")
    }
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
        // An interrupted run is reported once, in the report's status line.
        if let Some(observation) = &invocation.observation
            && !observation.outcome.success()
            && !matches!(observation.outcome, Outcome::Interrupted { .. })
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

/// anstream resolves `Auto` from `NO_COLOR`, `CLICOLOR`, `CLICOLOR_FORCE` and `TERM`, but
/// not `FORCE_COLOR`, which takes the same precedence as `CLICOLOR_FORCE` here.
pub(crate) fn resolve_color(color: Color) -> Color {
    let set = |name| std::env::var_os(name).is_some_and(|value| !value.is_empty());
    match color {
        Color::Auto if set("FORCE_COLOR") && !set("NO_COLOR") => Color::Always,
        Color::Auto | Color::Always | Color::Never => color,
    }
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

/// Breaks lines at spaces with a three-cell continuation indent; only a word wider
/// than the remaining line is split.
fn wrapped(out: &mut impl io::Write, text: &str, width: usize) -> io::Result<()> {
    // Indented lines continue at their own indent; others hang by three cells.
    let indent = match text
        .len()
        .saturating_sub(text.trim_start_matches(' ').len())
    {
        0 => 3,
        indent => indent,
    };
    let continuation = format!("\n{}", " ".repeat(indent));
    let mut used = 0_usize;
    for (index, word) in text.split(' ').enumerate() {
        if index > 0 {
            if used.saturating_add(1).saturating_add(word.width()) <= width {
                write!(out, " ")?;
                used = used.saturating_add(1);
            } else {
                write!(out, "{continuation}")?;
                used = indent;
            }
        }
        for character in word.chars() {
            let cells = character.width().unwrap_or(0);
            if used > indent && used.saturating_add(cells) > width {
                write!(out, "{continuation}")?;
                used = indent;
            }
            write!(out, "{character}")?;
            used = used.saturating_add(cells);
        }
    }
    writeln!(out)
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

#[cfg(test)]
mod tests {
    use super::{change, verdict};
    use crate::statistics::{Comparison, Decision, Interval};

    fn sentence(estimate: f64, bounds: Option<(f64, f64)>, decision: Decision) -> String {
        let comparison = Comparison {
            numerator: 1,
            denominator: Some(0),
            estimate,
            interval: bounds.map(|(lower, upper)| Interval { lower, upper }),
            decision,
            correlated: false,
        };
        verdict(&comparison, 3, Some(0.01)).1
    }

    #[test]
    fn verdicts_state_factors_beyond_one_and_a_half_and_percentages_below() {
        for (estimate, bounds, decision, expected) in [
            (
                3.47,
                Some((3.33, 3.62)),
                Decision::More,
                "B is 3.47× slower than A (95% CI 3.33–3.62×)",
            ),
            (
                0.5,
                Some((0.4, 0.6)),
                Decision::Less,
                "B is 2.00× faster than A (95% CI 1.67–2.50×)",
            ),
            (
                0.3,
                Some((0.0, 0.5)),
                Decision::Less,
                "B is 3.33× faster than A (95% CI at least 2.00×)",
            ),
            (
                1.048,
                Some((1.021, 1.074)),
                Decision::More,
                "B takes 4.8% more time than A (95% CI 2.1–7.4%)",
            ),
            (
                0.952,
                Some((0.93, 0.98)),
                Decision::Less,
                "B takes 4.8% less time than A (95% CI 2.0–7.0%)",
            ),
            (
                1.002,
                Some((0.995, 1.006)),
                Decision::Equivalent,
                "B is within ±1% of A (95% CI 0.5% less to 0.6% more)",
            ),
            (
                1.01,
                Some((0.98, 1.05)),
                Decision::Unresolved,
                "B vs A: no clear difference (95% CI 2.0% less to 5.0% more)",
            ),
            (
                1.1,
                Some((0.0, 2.26)),
                Decision::Unresolved,
                "B vs A: no clear difference (95% CI up to 2.26× slower)",
            ),
            (
                1.1,
                None,
                Decision::Unresolved,
                "B vs A: too noisy to bound with 3 runs",
            ),
            (
                2.0,
                None,
                Decision::Insufficient,
                "B vs A: no measurable variation, so no interval",
            ),
        ] {
            assert_eq!(sentence(estimate, bounds, decision), expected);
        }
    }

    #[test]
    fn changes_switch_to_factors_at_one_and_a_half() {
        assert_eq!(change(1.49), "49.0% more");
        assert_eq!(change(1.5), "1.50× slower");
        assert_eq!(change(0.7), "30.0% less");
        assert_eq!(change(0.6), "1.67× faster");
    }
}
