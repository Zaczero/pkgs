use serde::Serialize;
use statrs::distribution::{ContinuousCDF as _, FisherSnedecor, StudentsT};

/// Error rate shared by every interval of one experiment.
const ALPHA: f64 = 0.05;
/// Error rate shared by the serial-correlation checks of one experiment.
const CORRELATION_ALPHA: f64 = 0.01;
/// Automatic planning measures this many runs when the budget allows: batch means
/// then absorb moderate correlation between consecutive runs.
pub const PREFERRED_RUNS: usize = 100;
/// Under the default target, automatic planning measures at least this many runs;
/// two runs rarely bound an interval.
pub const MINIMUM_RUNS: usize = 3;
const MAXIMUM_RUNS: usize = 10_000;
/// Interval half-width, as a ratio, that the precision estimate plans for.
const PRECISION: f64 = 1.01;
/// Relative standard error below which variation is clock quantization, not evidence.
const RESOLUTION: f64 = 1e-9;

#[derive(Debug, Serialize)]
pub struct Analysis {
    pub method: &'static str,
    pub confidence: f64,
    pub runs: usize,
    pub batches: usize,
    pub summaries: Vec<Summary>,
    pub comparisons: Vec<Comparison>,
    pub limitation: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Summary {
    pub mean_ns: f64,
    pub sd_ns: f64,
    pub median_ns: f64,
    pub min_ns: u64,
    pub max_ns: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    Less,
    More,
    Equivalent,
    Unresolved,
    Insufficient,
    NotApplicable,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Interval {
    pub lower: f64,
    pub upper: f64,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Comparison {
    pub numerator: usize,
    pub denominator: Option<usize>,
    pub estimate: f64,
    /// `None` when the confidence set is unbounded (decision `unresolved`) or the
    /// data cannot support one (decision `insufficient`).
    pub interval: Option<Interval>,
    pub decision: Decision,
    /// Batch means varied more than independent runs explain: drift, load changes,
    /// or runs that influence their successors. The interval may be too narrow.
    pub correlated: bool,
}

impl Analysis {
    pub fn withdraw_inference(&mut self, reason: &str) {
        self.limitation = Some(reason.into());
        for comparison in &mut self.comparisons {
            comparison.interval = None;
            comparison.decision = if comparison.denominator.is_some() {
                Decision::Insufficient
            } else {
                Decision::NotApplicable
            };
        }
    }
}

impl Interval {
    pub fn decision(self, margin: Option<f64>) -> Decision {
        let delta = margin.unwrap_or(0.0);
        if self.upper < 1.0 - delta {
            Decision::Less
        } else if self.lower > 1.0 + delta {
            Decision::More
        } else if margin.is_some() && self.lower >= 1.0 - delta && self.upper <= 1.0 + delta {
            Decision::Equivalent
        } else {
            Decision::Unresolved
        }
    }
}

/// Chronological runs split into contiguous batches whose sizes differ by at most one.
struct Batches {
    runs: usize,
    /// Run count and mean of every column, per batch.
    batches: Vec<(usize, Vec<f64>)>,
    means: Vec<f64>,
}

impl Batches {
    #[expect(
        clippy::indexing_slicing,
        reason = "Rows are complete runs with the first row's column count"
    )]
    fn new(rows: &[Vec<u64>], count: usize) -> Self {
        let runs = rows.len();
        let columns = rows.first().map_or(0, Vec::len);
        let total = |slice: &[Vec<u64>], column: usize| {
            slice
                .iter()
                .map(|row| u128::from(row[column]))
                .sum::<u128>() as f64
        };
        let batches = (0..count)
            .map(|index| {
                let slice = &rows[partition(runs, count, index)];
                let means = (0..columns)
                    .map(|column| total(slice, column) / slice.len() as f64)
                    .collect();
                (slice.len(), means)
            })
            .collect();
        let means = (0..columns)
            .map(|column| total(rows, column) / runs as f64)
            .collect();
        Self {
            runs,
            batches,
            means,
        }
    }

    /// Estimated covariance of the two columns' overall means, from the size-weighted
    /// spread of batch means. For independent normal runs this is exact with
    /// `batches - 1` degrees of freedom, also for unequal batch sizes.
    #[expect(
        clippy::indexing_slicing,
        reason = "Columns index the batch mean vectors built from the same rows"
    )]
    fn covariance(&self, left: usize, right: usize) -> f64 {
        let spread = self
            .batches
            .iter()
            .map(|(size, means)| {
                *size as f64 * (means[left] - self.means[left]) * (means[right] - self.means[right])
            })
            .sum::<f64>();
        spread / self.batches.len().wrapping_sub(1).wrapping_mul(self.runs) as f64
    }

    /// Fieller's confidence set for the ratio of two means, or the t interval of
    /// one mean. Scaling by the denominator's mean keeps the quadratic well conditioned.
    #[expect(
        clippy::indexing_slicing,
        reason = "Pair indices come from the family of this analysis's columns"
    )]
    fn interval(&self, numerator: usize, denominator: Option<usize>, critical: f64) -> Bound {
        let Some(denominator) = denominator else {
            let mean = self.means[numerator];
            let error = self.covariance(numerator, numerator).sqrt();
            if error <= RESOLUTION * mean {
                return Bound::Degenerate;
            }
            return Bound::Interval(Interval {
                lower: critical.mul_add(-error, mean).max(0.0),
                upper: critical.mul_add(error, mean),
            });
        };
        let scale = self.means[denominator];
        let ratio = self.means[numerator] / scale;
        let [vxx, vyy, vxy] = [
            (denominator, denominator),
            (numerator, numerator),
            (numerator, denominator),
        ]
        .map(|(left, right)| self.covariance(left, right) / (scale * scale));
        // Var(Y - R X) at the estimate: exactly zero for proportional columns.
        let residual = ratio.mul_add(ratio * vxx, 2.0_f64.mul_add(-ratio * vxy, vyy));
        if residual <= (RESOLUTION * ratio).powi(2) {
            return Bound::Degenerate;
        }
        // Fieller's set: lead R^2 - 2 middle R + constant <= 0.
        let squared = critical * critical;
        let lead = squared.mul_add(-vxx, 1.0);
        if lead <= 0.0 {
            return Bound::Unbounded;
        }
        let middle = squared.mul_add(-vxy, ratio);
        let constant = ratio.mul_add(ratio, -squared * vyy);
        let root = middle.mul_add(middle, -lead * constant).max(0.0).sqrt();
        // Elapsed times are positive, so the set is intersected with positive ratios;
        // a lower bound of zero means no lower limit.
        Bound::Interval(Interval {
            lower: ((middle - root) / lead).max(0.0),
            upper: (middle + root) / lead,
        })
    }
}

enum Bound {
    Interval(Interval),
    /// The denominator's mean is not distinguishable from zero at this confidence.
    Unbounded,
    /// No variation between batches: quantized clocks, not certainty.
    Degenerate,
}

/// Automatic effort planning. Every decision uses run costs and pilot variation,
/// never a comparison's outcome, so the stopping point cannot favour a result.
#[derive(Clone, Copy, Debug)]
pub struct Planner {
    /// Seconds of total effort, including the pilot and hooks.
    pub target: f64,
    /// Fewest measured runs, even when they exceed the target.
    pub minimum: usize,
    /// Whether the first pilot run may be discarded as a warmup.
    pub warmup: bool,
}

impl Planner {
    /// Seconds per run. Later pilot runs predict the rest better than a cold first run.
    fn cost(costs: &[f64]) -> f64 {
        let steady = costs
            .get(1..)
            .filter(|rest| !rest.is_empty())
            .unwrap_or(costs);
        steady.iter().sum::<f64>() / steady.len() as f64
    }

    /// The pilot ends after eight runs, or when another run would carry the effort
    /// past 20% of the target.
    pub fn pilot_complete(&self, elapsed: f64, costs: &[f64]) -> bool {
        costs.len() >= 8 || elapsed + Self::cost(costs) > 0.2 * self.target
    }

    /// The first run pays for cold caches and lazy loading. It is discarded when
    /// later runs show that one run costs at most 5% of the target, so the loss is small.
    pub fn discard_first(&self, costs: &[f64]) -> bool {
        self.warmup
            && costs
                .get(1..)
                .is_some_and(|rest| !rest.is_empty() && median(rest) <= 0.05 * self.target)
    }

    /// Runs to add after the pilot: those that fit in the remaining target, up to the
    /// larger of the precision estimate and `PREFERRED_RUNS`, and at least enough for
    /// the minimum.
    pub fn additional(&self, elapsed: f64, costs: &[f64], pilot: &[Vec<u64>]) -> usize {
        let kept = pilot
            .get(usize::from(self.discard_first(costs))..)
            .unwrap_or_default();
        let affordable = ((self.target - elapsed).max(0.0) / Self::cost(costs)).floor() as usize;
        let desired = precision_count(kept).clamp(PREFERRED_RUNS, MAXIMUM_RUNS);
        affordable
            .min(desired.saturating_sub(kept.len()))
            .max(self.minimum.saturating_sub(kept.len()))
    }
}

pub fn summarize(values: &[u64]) -> Summary {
    let mean = values.iter().map(|&x| u128::from(x)).sum::<u128>() as f64 / values.len() as f64;
    let variance = if values.len() > 1 {
        values
            .iter()
            .map(|&x| (x as f64 - mean).powi(2))
            .sum::<f64>()
            / values.len().wrapping_sub(1) as f64
    } else {
        0.0
    };
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    Summary {
        mean_ns: mean,
        sd_ns: variance.sqrt(),
        median_ns: median(&sorted.iter().map(|&x| x as f64).collect::<Vec<_>>()),
        min_ns: *sorted.first().expect("nonempty observations"),
        max_ns: *sorted.last().expect("nonempty observations"),
    }
}

fn median(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable_by(f64::total_cmp);
    let (lower, upper) = sorted.split_at(sorted.len() / 2);
    let upper = *upper.first().expect("nonempty values");
    if sorted.len().is_multiple_of(2) {
        f64::midpoint(*lower.last().expect("even count"), upper)
    } else {
        upper
    }
}

/// Every command pair, so all comparisons hold simultaneously; one command has its mean.
fn family(columns: usize) -> Vec<(usize, Option<usize>)> {
    if columns == 1 {
        return vec![(0, None)];
    }
    (0..columns)
        .flat_map(|denominator| {
            (denominator.wrapping_add(1)..columns)
                .map(move |numerator| (numerator, Some(denominator)))
        })
        .collect()
}

/// One batch per run up to ten runs, then ten until the square root of the run
/// count grows past ten, capped at thirty. Batches must be long enough to absorb
/// correlation between consecutive runs; more of them narrow the t quantile.
pub fn batch_count(runs: usize) -> usize {
    runs.min(runs.isqrt().clamp(10, 30))
}

/// The `index`th of `parts` contiguous ranges covering `0..len`, sizes differing by at most one.
pub fn partition(len: usize, parts: usize, index: usize) -> std::ops::Range<usize> {
    let bound = |part: usize| part.wrapping_mul(len).div_euclid(parts);
    bound(index)..bound(index.wrapping_add(1))
}

/// Two-sided Student t quantile at the Bonferroni level for `family` intervals.
fn quantile(degrees: usize, family: usize) -> f64 {
    StudentsT::new(0.0, 1.0, degrees as f64)
        .expect("positive degrees of freedom")
        .inverse_cdf(1.0 - ALPHA / (2.0 * family as f64))
}

/// One-way ANOVA F test of the batch partition on per-run values of the contrast.
/// Independent runs give F(b - 1, n - b); drift or correlated runs inflate it.
#[expect(
    clippy::indexing_slicing,
    reason = "Rows are complete runs; pair indices come from their column family"
)]
fn correlated(
    rows: &[Vec<u64>],
    count: usize,
    pair: (usize, Option<usize>),
    ratio: f64,
    family: usize,
) -> bool {
    let runs = rows.len();
    if runs < count.wrapping_mul(2) || count < 2 {
        return false;
    }
    let (numerator, denominator) = pair;
    let values: Vec<_> = rows
        .iter()
        .map(|row| row[numerator] as f64 - denominator.map_or(0.0, |d| ratio * row[d] as f64))
        .collect();
    let mean = values.iter().sum::<f64>() / runs as f64;
    let mut between = 0.0;
    let mut within = 0.0;
    for index in 0..count {
        let batch = &values[partition(runs, count, index)];
        let batch_mean = batch.iter().sum::<f64>() / batch.len() as f64;
        between += batch.len() as f64 * (batch_mean - mean).powi(2);
        within += batch.iter().map(|v| (v - batch_mean).powi(2)).sum::<f64>();
    }
    let (d1, d2) = (
        count.wrapping_sub(1) as f64,
        runs.wrapping_sub(count) as f64,
    );
    if within <= 0.0 {
        return false;
    }
    let statistic = (between / d1) / (within / d2);
    statistic
        > FisherSnedecor::new(d1, d2)
            .expect("positive degrees of freedom")
            .inverse_cdf(1.0 - CORRELATION_ALPHA / family as f64)
}

/// Simultaneous 95% intervals for every command pair's ratio of arithmetic mean
/// elapsed times (one command: its mean), from chronological complete runs.
#[expect(
    clippy::indexing_slicing,
    reason = "Rows are complete runs; family indices come from their column count"
)]
pub fn analyze(rows: &[Vec<u64>], margin: Option<f64>) -> Analysis {
    let runs = rows.len();
    let columns = rows.first().map_or(0, Vec::len);
    let summaries: Vec<_> = (0..columns)
        .map(|column| summarize(&rows.iter().map(|row| row[column]).collect::<Vec<_>>()))
        .collect();
    let count = batch_count(runs);
    let limitation = match runs {
        0 => Some("No run completed."),
        1 => Some("One run cannot show run-to-run variation; intervals need at least two."),
        _ if summaries.iter().any(|s| s.mean_ns <= 0.0) => {
            Some("Clock resolution is insufficient for inference.")
        },
        _ => None,
    };
    let pairs = family(columns);
    let batches = limitation.is_none().then(|| Batches::new(rows, count));
    let critical = limitation
        .is_none()
        .then(|| quantile(count.wrapping_sub(1), pairs.len()));
    let comparisons = pairs
        .iter()
        .map(|&(numerator, denominator)| {
            let estimate =
                summaries[numerator].mean_ns / denominator.map_or(1.0, |d| summaries[d].mean_ns);
            let bound = batches
                .as_ref()
                .zip(critical)
                .map(|(batches, critical)| batches.interval(numerator, denominator, critical));
            let (interval, decision) = match (bound, denominator) {
                (Some(Bound::Interval(interval)), None) => {
                    (Some(interval), Decision::NotApplicable)
                },
                (Some(Bound::Interval(interval)), Some(_)) => {
                    (Some(interval), interval.decision(margin))
                },
                (Some(Bound::Unbounded | Bound::Degenerate) | None, None) => {
                    (None, Decision::NotApplicable)
                },
                (Some(Bound::Unbounded), Some(_)) => (None, Decision::Unresolved),
                (Some(Bound::Degenerate) | None, Some(_)) => (None, Decision::Insufficient),
            };
            Comparison {
                numerator,
                denominator,
                estimate,
                interval,
                decision,
                correlated: limitation.is_none()
                    && correlated(rows, count, (numerator, denominator), estimate, pairs.len()),
            }
        })
        .collect();
    Analysis {
        method: "fieller-batch-means-bonferroni-v4",
        confidence: 1.0 - ALPHA,
        runs,
        batches: if limitation.is_none() { count } else { 0 },
        summaries,
        comparisons,
        limitation: limitation.map(str::to_owned),
    }
}

/// Runs needed for every pair's interval to reach about ±1%, from the pilot's
/// run-to-run variation of each relative contrast, assuming independent runs.
#[expect(
    clippy::indexing_slicing,
    reason = "Pilot rows are complete runs and pairs are constructed from their shared dimension"
)]
pub fn precision_count(rows: &[Vec<u64>]) -> usize {
    if rows.len() < 4 {
        return PREFERRED_RUNS;
    }
    let columns = rows.first().expect("at least four pilot runs").len();
    let pairs = family(columns);
    let means: Vec<_> = (0..columns)
        .map(|column| rows.iter().map(|row| row[column] as f64).sum::<f64>() / rows.len() as f64)
        .collect();
    let variance = pairs
        .iter()
        .map(|&(num, den)| {
            let values: Vec<_> = rows
                .iter()
                .map(|row| {
                    row[num] as f64 / means[num] - den.map_or(1.0, |d| row[d] as f64 / means[d])
                })
                .collect();
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            values
                .iter()
                .map(|value| (value - mean).powi(2))
                .sum::<f64>()
                / values.len().wrapping_sub(1) as f64
        })
        .fold(0.0_f64, f64::max);
    let critical = quantile(batch_count(PREFERRED_RUNS).wrapping_sub(1), pairs.len());
    (critical * critical * variance / PRECISION.ln().powi(2)).ceil() as usize
}

#[cfg(test)]
mod tests {
    use super::{Decision, Interval, Planner, analyze, batch_count, family, quantile};

    #[test]
    fn arithmetic_mean_ratio_is_not_mean_of_ratios() {
        let result = analyze(&[vec![1, 4], vec![9, 6]], None);
        assert_eq!(result.comparisons[0].estimate, 1.0);
    }

    #[test]
    fn constant_observations_do_not_certify_equivalence() {
        let result = analyze(&vec![vec![100, 100]; 100], Some(0.01));
        assert!(result.comparisons[0].interval.is_none());
        assert_eq!(result.comparisons[0].decision, Decision::Insufficient);
    }

    #[test]
    fn proportional_columns_do_not_get_a_zero_width_certificate() {
        let row = [vec![11, 22], vec![23, 46], vec![31, 62], vec![47, 94]];
        let rows: Vec<_> = row.iter().cycle().take(100).cloned().collect();
        let comparison = analyze(&rows, None).comparisons[0];
        assert_eq!(comparison.estimate, 2.0);
        assert!(comparison.interval.is_none());
        assert_eq!(comparison.decision, Decision::Insufficient);
    }

    #[test]
    fn family_includes_nonbaseline_comparisons() {
        assert_eq!(family(4), vec![
            (1, Some(0)),
            (2, Some(0)),
            (3, Some(0)),
            (2, Some(1)),
            (3, Some(1)),
            (3, Some(2))
        ]);
    }

    #[test]
    fn practical_margin_is_distinct_from_direction() {
        let interval = Interval {
            lower: 0.992,
            upper: 0.998,
        };
        assert_eq!(interval.decision(None), Decision::Less);
        assert_eq!(interval.decision(Some(0.01)), Decision::Equivalent);
    }

    #[test]
    fn single_command_mean_has_no_directional_decision() {
        let rows: Vec<_> = (100..200).map(|value| vec![value]).collect();
        let mean = analyze(&rows, None).comparisons[0];
        assert_eq!(mean.denominator, None);
        assert_eq!(mean.decision, Decision::NotApplicable);
        assert!(mean.interval.is_some());
        assert_eq!(mean.estimate, 149.5);
    }

    #[test]
    fn withdrawing_inference_removes_intervals_but_keeps_measurements() {
        let rows: Vec<_> = (100..200).map(|value| vec![value, 400 - value]).collect();
        let mut result = analyze(&rows, None);
        assert!(result.comparisons[0].interval.is_some());
        let estimate = result.comparisons[0].estimate;
        result.withdraw_inference("Failed");
        assert_eq!(result.runs, 100);
        assert_eq!(result.summaries[0].mean_ns, 149.5);
        assert_eq!(result.comparisons[0].estimate, estimate);
        assert!(result.comparisons[0].interval.is_none());
        assert_eq!(result.comparisons[0].decision, Decision::Insufficient);
    }

    #[test]
    fn intervals_match_an_independent_reference_with_uneven_batches() {
        // 13 runs form ten batches of one or two runs. Reference values come from a
        // separate Python implementation of the same weighted batch-means Fieller sets.
        let rows = [
            [1000, 1210, 905],
            [1030, 1190, 930],
            [990, 1250, 890],
            [1010, 1170, 940],
            [1060, 1230, 910],
            [970, 1205, 925],
            [1025, 1260, 880],
            [1005, 1185, 950],
            [995, 1220, 915],
            [1040, 1195, 900],
            [1015, 1240, 935],
            [985, 1215, 920],
            [1020, 1200, 905],
        ]
        .map(Vec::from);
        let result = analyze(&rows, None);
        assert_eq!(result.batches, 10);
        for (comparison, (estimate, lower, upper)) in result.comparisons.iter().zip([
            (
                1.199_695_701_787_751_9,
                1.166_799_267_159_946_5,
                1.233_619_844_037_688_7,
            ),
            (
                0.905_667_554_203_119,
                0.881_522_141_011_495_6,
                0.930_518_726_210_52,
            ),
            (
                0.754_914_394_419_784_4,
                0.731_594_526_042_090_9,
                0.778_951_468_735_880_7,
            ),
        ]) {
            let interval = comparison.interval.expect("bounded interval");
            for (actual, expected) in [
                (comparison.estimate, estimate),
                (interval.lower, lower),
                (interval.upper, upper),
            ] {
                assert!(
                    (actual / expected - 1.0).abs() < 1e-7,
                    "{actual} != {expected}"
                );
            }
        }
        assert_eq!(result.comparisons[0].decision, Decision::More);
        assert_eq!(result.comparisons[1].decision, Decision::Less);
    }

    #[test]
    fn quantiles_hold_at_the_corrected_levels_used_by_small_experiments() {
        // Three commands with two runs: two degrees of freedom, three comparisons.
        assert!((quantile(2, 3) - 7.648_803_937_915).abs() < 1e-6);
        // Eight commands with two runs: one degree of freedom, 28 comparisons, where
        // the quantile has the closed form tan(pi (1/2 - alpha / 56)).
        let exact = (std::f64::consts::PI * (0.5 - 0.05 / 56.0)).tan();
        assert!((quantile(1, 28) / exact - 1.0).abs() < 1e-6);
    }

    #[test]
    fn batch_count_never_decreases_with_more_runs() {
        let counts: Vec<_> = (1..=2000).map(batch_count).collect();
        assert!(counts.is_sorted());
        assert_eq!(
            [1, 2, 10, 11, 99, 100, 400, 900, 10_000].map(batch_count),
            [1, 2, 10, 10, 10, 10, 20, 30, 30]
        );
    }

    #[test]
    fn two_quiet_runs_can_resolve_a_large_difference() {
        let result = analyze(
            &[vec![1_000_000, 1_500_000], vec![1_002_000, 1_501_000]],
            None,
        );
        assert_eq!(result.comparisons[0].decision, Decision::More);
    }

    #[test]
    fn noise_larger_than_the_baseline_mean_leaves_the_ratio_unbounded() {
        let result = analyze(&[vec![1, 1_000], vec![3_000, 1_000], vec![2, 1_000]], None);
        let comparison = result.comparisons[0];
        assert!(comparison.interval.is_none());
        assert_eq!(comparison.decision, Decision::Unresolved);
    }

    #[test]
    fn ratio_intervals_stop_at_zero() {
        let result = analyze(&[vec![1000, 1], vec![1001, 3000], vec![999, 2]], None);
        let interval = result.comparisons[0].interval.expect("bounded set");
        assert_eq!(interval.lower, 0.0);
        assert_eq!(result.comparisons[0].decision, Decision::Unresolved);
    }

    #[test]
    fn drift_between_batches_is_flagged() {
        let steady: Vec<_> = (0..200_u64)
            .map(|run| {
                vec![
                    1000_u64.wrapping_add(run % 7),
                    1000_u64.wrapping_add(run % 5),
                ]
            })
            .collect();
        assert!(!analyze(&steady, None).comparisons[0].correlated);
        let drifting: Vec<_> = (0..200_u64)
            .map(|run| vec![1000, 1000_u64.wrapping_add(run)])
            .collect();
        assert!(analyze(&drifting, None).comparisons[0].correlated);
    }

    #[test]
    fn planning_discards_a_cheap_cold_run_and_keeps_the_minimum() {
        let planner = Planner {
            target: 30.0,
            minimum: 3,
            warmup: true,
        };
        assert!(planner.discard_first(&[5.0, 1.0, 1.0]));
        assert!(!planner.discard_first(&[5.0]));
        assert!(!planner.discard_first(&[20.0, 20.0]));
        let slow = vec![vec![20_000_000_000, 21_000_000_000]];
        assert_eq!(planner.additional(40.0, &[41.0], &slow), 2);
        let strict = Planner {
            minimum: 1,
            ..planner
        };
        assert_eq!(strict.additional(40.0, &[41.0], &slow), 0);
    }
}
