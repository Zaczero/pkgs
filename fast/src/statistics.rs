use rand::{RngExt as _, SeedableRng as _};
use rand_chacha::ChaCha12Rng;
use serde::Serialize;
use statrs::distribution::{ContinuousCDF as _, Normal, StudentsT};

const REPLICATES: usize = 20_000;

#[derive(Debug, Serialize)]
pub struct Analysis {
    pub method: &'static str,
    pub blocks: usize,
    pub replicates: usize,
    pub segment_length: usize,
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
    pub interval: Option<Interval>,
    pub primary_interval: Option<Interval>,
    pub decision: Decision,
    pub sensitivity_stable: bool,
}

struct Prefix {
    columns: Vec<Vec<u128>>,
    rows: usize,
}

impl Analysis {
    pub fn withdraw_inference(&mut self, reason: &str) {
        self.limitation = Some(reason.into());
        for comparison in &mut self.comparisons {
            comparison.interval = None;
            comparison.primary_interval = None;
            comparison.sensitivity_stable = false;
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

impl Prefix {
    #[expect(
        clippy::indexing_slicing,
        reason = "Complete block columns have the first row's dimension"
    )]
    fn new(rows: &[Vec<u64>]) -> Self {
        let columns = (0..rows.first().expect("eligible nonempty blocks").len())
            .map(|column| {
                let mut prefix = Vec::with_capacity(rows.len().wrapping_add(1));
                prefix.push(0);
                let mut sum = 0_u128;
                for row in rows {
                    sum = sum.wrapping_add(u128::from(row[column]));
                    prefix.push(sum);
                }
                prefix
            })
            .collect();
        Self {
            columns,
            rows: rows.len(),
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "Start is below rows and length is bounded by rows; prefixes have rows + 1 entries"
    )]
    fn segment(&self, start: usize, length: usize, sums: &mut [u128]) {
        let first = length.min(self.rows.wrapping_sub(start));
        let rest = length.wrapping_sub(first);
        for (prefix, sum) in self.columns.iter().zip(sums) {
            *sum = sum.wrapping_add(
                prefix[start.wrapping_add(first)]
                    .wrapping_sub(prefix[start])
                    .wrapping_add(prefix[rest]),
            );
        }
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
    let middle = sorted.len() / 2;
    let (left, right) = sorted.split_at(middle);
    let upper = *right.first().expect("nonempty upper median half") as f64;
    let median = if sorted.len().is_multiple_of(2) {
        f64::midpoint(
            *left.last().expect("nonempty lower median half") as f64,
            upper,
        )
    } else {
        upper
    };
    Summary {
        mean_ns: mean,
        sd_ns: variance.sqrt(),
        median_ns: median,
        min_ns: *sorted.first().expect("nonempty observations"),
        max_ns: *sorted.last().expect("nonempty observations"),
    }
}

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

#[expect(
    clippy::indexing_slicing,
    reason = "The scheduler supplies complete rectangular blocks; family indices and sensitivity vectors are constructed from those dimensions"
)]
pub fn analyze(rows: &[Vec<u64>], completed: bool, seed: u64, margin: Option<f64>) -> Analysis {
    let n = rows.len();
    let columns = rows.first().map_or(0, Vec::len);
    let summaries: Vec<_> = (0..columns)
        .map(|column| summarize(&rows.iter().map(|row| row[column]).collect::<Vec<_>>()))
        .collect();
    let segment = n.isqrt().max(2);
    let limitation = if !completed {
        Some("The planned experiment did not complete; comparisons are descriptive only.")
    } else if n < 100 {
        Some("Too few complete measured blocks for timing conclusions (at least 100 required).")
    } else if summaries.iter().any(|s| s.mean_ns <= 0.0) {
        Some("Clock resolution is insufficient for inference.")
    } else {
        None
    }
    .map(str::to_owned);
    let pairs = family(columns);
    let estimates: Vec<_> = pairs
        .iter()
        .map(|&(num, den)| summaries[num].mean_ns / den.map_or(1.0, |d| summaries[d].mean_ns))
        .collect();
    let intervals = if limitation.is_none() {
        bootstrap(rows, &pairs, segment, REPLICATES, seed)
    } else {
        vec![None; pairs.len()]
    };
    let sensitivities: Vec<_> = if limitation.is_none() {
        [segment / 2, segment.wrapping_mul(2).min(n)]
            .into_iter()
            .enumerate()
            .map(|(index, length)| {
                bootstrap(
                    rows,
                    &pairs,
                    length.max(1),
                    REPLICATES,
                    seed.wrapping_add((index as u64).wrapping_add(1)),
                )
            })
            .collect()
    } else {
        Vec::new()
    };
    let comparisons = pairs
        .into_iter()
        .zip(estimates)
        .zip(intervals)
        .enumerate()
        .map(
            |(index, (((numerator, denominator), estimate), interval))| {
                let decide = |interval: Option<Interval>| {
                    if denominator.is_none() {
                        Decision::NotApplicable
                    } else {
                        interval.map_or(Decision::Insufficient, |i| i.decision(margin))
                    }
                };
                let primary = decide(interval);
                let stable = sensitivities
                    .iter()
                    .all(|arm| decide(arm[index]) == primary);
                let sensitivity_envelope = interval.and_then(|initial| {
                    sensitivities.iter().try_fold(initial, |outer, arm| {
                        arm[index].map(|inner| Interval {
                            lower: outer.lower.min(inner.lower),
                            upper: outer.upper.max(inner.upper),
                        })
                    })
                });
                Comparison {
                    numerator,
                    denominator,
                    estimate,
                    interval: sensitivity_envelope,
                    primary_interval: interval,
                    decision: decide(sensitivity_envelope),
                    sensitivity_stable: stable,
                }
            },
        )
        .collect();
    Analysis {
        method: "stationary-bootstrap-log-ratio-max-envelope-v3",
        blocks: n,
        replicates: REPLICATES,
        segment_length: segment,
        summaries,
        comparisons,
        limitation,
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "Contrast pairs are generated from this mean vector's dimension"
)]
fn contrast(means: &[f64], pair: (usize, Option<usize>)) -> f64 {
    let (numerator, denominator) = pair;
    (means[numerator] / denominator.map_or(1.0, |d| means[d])).ln()
}

fn bootstrap(
    rows: &[Vec<u64>],
    pairs: &[(usize, Option<usize>)],
    length: usize,
    replicates: usize,
    seed: u64,
) -> Vec<Option<Interval>> {
    let prefix = Prefix::new(rows);
    let n = rows.len();
    let k = prefix.columns.len();
    let observed: Vec<_> = prefix
        .columns
        .iter()
        .map(|p| *p.last().expect("nonempty prefix") as f64 / n as f64)
        .collect();
    let theta: Vec<_> = pairs
        .iter()
        .map(|&pair| contrast(&observed, pair))
        .collect();
    let mut rng = ChaCha12Rng::seed_from_u64(seed);
    let mut draws = Vec::with_capacity(replicates.wrapping_mul(k));
    let mut sums = vec![0_u128; k];
    let log_continue = (-1.0 / length as f64).ln_1p();
    for _ in 0..replicates {
        sums.fill(0);
        let mut remaining = n;
        while remaining > 0 {
            let start = rng.random_range(0..n);
            let size = if length == 1 {
                1
            } else {
                let uniform = 1.0 - rng.random::<f64>();
                ((uniform.ln() / log_continue).floor() as usize)
                    .saturating_add(1)
                    .min(remaining)
            };
            prefix.segment(start, size, &mut sums);
            remaining = remaining.wrapping_sub(size);
        }
        draws.extend(sums.iter().map(|&sum| sum as f64 / n as f64));
    }
    let mut means = vec![0.0; pairs.len()];
    let mut m2 = vec![0.0; pairs.len()];
    for (iteration, draw) in draws.chunks_exact(k).enumerate() {
        for ((&pair, mean), variance) in pairs.iter().zip(&mut means).zip(&mut m2) {
            let value = contrast(draw, pair);
            let delta = value - *mean;
            *mean += delta / iteration.wrapping_add(1) as f64;
            *variance += delta * (value - *mean);
        }
    }
    let scales: Vec<_> = m2
        .into_iter()
        .map(|v| (v / replicates.wrapping_sub(1) as f64).sqrt())
        .collect();
    let mut maxima = Vec::with_capacity(replicates);
    for draw in draws.chunks_exact(k) {
        let maximum = pairs
            .iter()
            .zip(&theta)
            .zip(&scales)
            .filter(|(_, scale)| **scale > 0.0 && scale.is_finite())
            .map(|((&pair, &center), &scale)| ((contrast(draw, pair) - center) / scale).abs())
            .fold(0.0_f64, f64::max);
        maxima.push(maximum);
    }
    let rank = replicates
        .wrapping_add(1)
        .wrapping_mul(95)
        .div_ceil(100)
        .min(replicates)
        .wrapping_sub(1);
    let (_, q, _) = maxima.select_nth_unstable_by(rank, f64::total_cmp);
    // Effective-segment t inflation and circular-bootstrap finite-sample correction
    // are calibrated engineering safeguards, not a finite-sample coverage theorem.
    let probability = 1.0 - 0.05 / (2.0 * pairs.len() as f64);
    let normal = Normal::new(0.0, 1.0)
        .expect("standard normal")
        .inverse_cdf(probability);
    let degrees = (n as f64 / length as f64 - 1.0).max(1.0);
    let student = StudentsT::new(0.0, 1.0, degrees)
        .expect("positive degrees")
        .inverse_cdf(probability);
    let correction = (n as f64 / (n as f64 - length.wrapping_mul(2).wrapping_sub(1) as f64)).sqrt()
        * student
        / normal;
    let q = *q * correction;
    theta
        .into_iter()
        .zip(scales)
        .map(|(center, scale)| {
            if scale <= 0.0 || !scale.is_finite() {
                return None;
            }
            let lower = (center - q * scale).exp();
            let upper = (center + q * scale).exp();
            (lower.is_finite() && upper.is_finite()).then_some(Interval { lower, upper })
        })
        .collect()
}

#[expect(
    clippy::indexing_slicing,
    reason = "Pilot rows are complete blocks and pairs are constructed from their shared dimension"
)]
pub fn precision_count(rows: &[Vec<u64>]) -> usize {
    if rows.len() < 4 {
        return 100;
    }
    let k = rows.first().expect("at least four pilot blocks").len();
    let pairs = family(k);
    let means: Vec<_> = (0..k)
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
    let q = Normal::new(0.0, 1.0)
        .expect("standard normal")
        .inverse_cdf(1.0 - 0.05 / (2.0 * pairs.len() as f64));
    (2.0 * q * q * variance / 1.01_f64.ln().powi(2)).ceil() as usize
}

#[cfg(test)]
mod tests {
    #[test]
    fn circular_segments_sum_exact_integer_observations() {
        let prefix = super::Prefix::new(&[vec![2, 11], vec![3, 13], vec![5, 17], vec![7, 19]]);
        let mut sums = [0, 0];
        prefix.segment(3, 3, &mut sums);
        assert_eq!(sums, [12, 43]);
    }

    #[test]
    fn arithmetic_mean_ratio_is_not_mean_of_ratios() {
        let result = super::analyze(&[vec![1, 4], vec![9, 6]], true, 7, None);
        assert_eq!(result.comparisons[0].estimate, 1.0);
        assert!(result.comparisons[0].interval.is_none());
    }

    #[test]
    fn constant_observations_do_not_certify_equivalence() {
        let result = super::analyze(&vec![vec![100, 100]; 100], true, 7, Some(0.01));
        assert!(result.comparisons[0].interval.is_none());
        assert_eq!(
            result.comparisons[0].decision,
            super::Decision::Insufficient
        );
    }

    #[test]
    fn family_includes_nonbaseline_comparisons() {
        assert_eq!(super::family(4), vec![
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
        let interval = super::Interval {
            lower: 0.992,
            upper: 0.998,
        };
        assert_eq!(interval.decision(None), super::Decision::Less);
        assert_eq!(interval.decision(Some(0.01)), super::Decision::Equivalent);
    }

    #[test]
    fn single_command_mean_has_no_directional_decision() {
        let rows: Vec<_> = (100..200).map(|value| vec![value]).collect();
        let result = super::analyze(&rows, true, 7, None);
        let mean = result.comparisons[0];
        assert_eq!(mean.denominator, None);
        assert_eq!(mean.decision, super::Decision::NotApplicable);
        assert!(mean.interval.is_some());
        assert_eq!(mean.estimate, 149.5);
    }

    #[test]
    fn withdrawing_inference_removes_intervals_but_keeps_measurements() {
        let rows: Vec<_> = (100..200).map(|value| vec![value, 400 - value]).collect();
        let mut result = super::analyze(&rows, true, 7, None);
        assert!(result.comparisons[0].interval.is_some());
        assert!(result.comparisons[0].primary_interval.is_some());
        let estimate = result.comparisons[0].estimate;
        result.withdraw_inference("Interrupted");
        assert_eq!(result.blocks, 100);
        assert_eq!(result.summaries[0].mean_ns, 149.5);
        assert_eq!(result.comparisons[0].estimate, estimate);
        assert!(result.comparisons[0].interval.is_none());
        assert!(result.comparisons[0].primary_interval.is_none());
        assert_eq!(
            result.comparisons[0].decision,
            super::Decision::Insufficient
        );
        assert!(!result.comparisons[0].sensitivity_stable);
    }

    #[test]
    fn proportional_columns_do_not_get_a_zero_width_certificate() {
        let row = [vec![11, 22], vec![23, 46], vec![31, 62], vec![47, 94]];
        let rows: Vec<_> = row.iter().cycle().take(100).cloned().collect();
        let result = super::analyze(&rows, true, 19, None);
        let comparison = result.comparisons.first().expect("one contrast");
        assert_eq!(comparison.estimate, 2.0);
        assert!(comparison.interval.is_none());
    }

    #[test]
    fn incomplete_plan_never_has_inferential_intervals() {
        let row = [vec![11, 22], vec![23, 41], vec![31, 63], vec![47, 81]];
        let rows: Vec<_> = row.iter().cycle().take(100).cloned().collect();
        let result = super::analyze(&rows, false, 19, None);
        assert!(result.comparisons.iter().all(|c| c.interval.is_none()));
    }

    #[test]
    fn prefixes_keep_values_above_float_integer_precision() {
        let rows = [vec![u64::MAX, 3], vec![u64::MAX, 5], vec![7, 11]];
        let prefix = super::Prefix::new(&rows);
        let mut sums = [0, 0];
        prefix.segment(1, 3, &mut sums);
        assert_eq!(sums, [0x0002_0000_0000_0000_0005, 19]);
    }

    #[test]
    fn segment_accumulation_matches_literal_chronological_rows() {
        let rows = [
            vec![5, 101],
            vec![7, 103],
            vec![11, 107],
            vec![13, 109],
            vec![17, 113],
        ];
        let prefix = super::Prefix::new(&rows);
        for (segments, selected) in [
            (vec![(4, 3), (2, 2)], vec![4, 0, 1, 2, 3]),
            (vec![(1, 1), (1, 1), (0, 3)], vec![1, 1, 0, 1, 2]),
            (vec![(3, 5)], vec![3, 4, 0, 1, 2]),
        ] {
            let mut sums = [0_u128; 2];
            for (start, length) in segments {
                prefix.segment(start, length, &mut sums);
            }
            let mut reference = [0_u128; 2];
            for index in selected {
                for (total, &value) in reference.iter_mut().zip(&rows[index]) {
                    *total = total.wrapping_add(u128::from(value));
                }
            }
            assert_eq!(sums, reference);
        }
    }
}
