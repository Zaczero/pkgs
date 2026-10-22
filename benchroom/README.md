# benchroom

Turn benchmark measurements into clear comparison charts and tables. Use your
existing runner, keep its results, and change the presentation without measuring
again.

[Install](#installation), [make a chart](#compare-measurements),
[browse the API](#api), or [get support](#support).

## Installation

Requires Python 3.11 or newer. From this repository:

```sh
uv pip install ./benchroom
```

## Compare measurements

Each series contains one measurement per independent run, in a shared unit.
For repeated readings within a run, first summarize that run.

```python
from benchroom import Report, Series

# Illustrative measurements, in milliseconds.
report = Report(
    "Encoding short messages",
    (
        Series("encode", "Implementation A", (2.1, 2.3, 2.2)),
        Series("encode", "Implementation B", (1.5, 1.7, 1.6)),
    ),
    unit="milliseconds",
    caption="Short messages; setup excluded.",
)
report.save("comparison.svg")
```

This writes a chart and `comparison.md`, a table of the same values. Use
`dark=True` for dark backgrounds, or a `.png` filename for raster output.
`report.figure()` returns a Matplotlib figure for further customization.

Dots show medians. Lines show 95% bootstrap intervals across runs, not the
range of individual operation times. A single run has no interval. The report
does not declare winners from overlapping or separated lines.

## Import pyperf

Read pyperf JSON and draw charts with the same installation:

```python
from benchroom import Report
from benchroom.pyperf import load

# Requires two result files produced by pyperf with its -o option.
series = load("baseline.json", name="Baseline")
series += load("candidate.json", name="Candidate")
Report("Operation time", series, unit="seconds").save("comparison.svg")
```

The importer averages readings within each worker run and omits warmup-only
runs. It accepts timing benchmarks in seconds.

## Adapt paired collection

Use `benchroom.sampling` when your runner can collect fresh paired process
blocks. Calibration chooses timing exposure; production blocks supply
uncertainty. Repeated timings or requests in one process are not additional
independent blocks.

```python
from benchroom.sampling import assess_ratios

# Illustrative throughput ratios, one candidate/reference pair per fresh block.
decision = assess_ratios(
    [1.2, 1.4, 1.3, 1.5, 1.6, 1.7],
    block_ids=[1, 2, 3, 4, 5, 6],
)
assert decision.complete
assert decision.direction == "above"
```

The default `SamplingPolicy` permits decisions after six and twelve blocks.
Collect six first; stop if the interval resolves the direction or its
upper/lower ratio is at most 1.10. Otherwise collect to twelve when the remaining
budget permits it. At twelve, stop and retain unresolved comparisons as
inconclusive. A narrow interval crossing one meets reporting precision, not
equivalence.

The intervals use exact binomial order statistics for a common population
median. They require independent blocks with a common median; process restarts
do not remove persistent host drift. The two looks allocate error probabilities
of 1/32 and 3/160. Their sum bounds the probability that any reported interval
misses the median by 5%, per comparison. This is not a simultaneous guarantee
across different workloads or competitors. See the
[NIST median confidence-limit construction](https://www.itl.nist.gov/div898/software/dataplot/refman1/auxillar/mediancl.htm).

Decide the policy before production and inspect only at its allowed looks.
Keep calibration observations out of the production ratios and preserve the
selected production exposure. `rates_are_stable` compares adjacent calibration
rates; `next_batch_size` doubles a short timing batch. Neither supplies
statistical confidence. The runner owns startup, correctness, artifact identity,
deadlines, and cleanup.

<a id="choose-a-chart"></a>

## Scaling and distributions

Use a comparison report for “which implementation is faster?”. Use
`benchroom.plot.curves` for how latency changes across percentiles, or how cost
grows with input size. Use `benchroom.plot.distribution` to show the fraction of
observations below a threshold, without smoothing the data.

```python
from benchroom.plot import curves

# Illustrative measurements: input size to elapsed milliseconds.
figure = curves(
    {"Implementation A": {100: 2.1, 1_000: 18.3, 10_000: 175.0}},
    title="Encoding larger inputs",
    xlabel="Input items",
    ylabel="Milliseconds",
)
figure.savefig("scaling.svg")
```

Distribution plots need observations, not a handful of reported percentiles.
Run summaries describe variation between runs; request times describe variation
between requests. Label the population the plot actually shows.

<a id="reference"></a>

## API

- `Series(case, name, values, x=None)` holds finite run measurements; `x` can
  label an input size. Its `median` and `interval` properties expose the analysis.
- `Report(title, series, unit, better="lower", caption="")` groups comparable
  measurements. Use `better="higher"` for throughput. `figure()`, `markdown()`
  and `save()` share the same estimates.
- `Report.results` contains `Summary` records with each result's case,
  implementation name, median, interval, run count, and optional size.
- `benchroom.stats.percentile(values, fraction)` interpolates a percentile.
- `benchroom.stats.median_interval(values, seed=0)` estimates the median's
  95% bootstrap interval.
- `benchroom.stats.bootstrap_interval(size, statistic, seed=0, samples=10000)`
  passes resampled indexes to a statistic. Use the same indexes for both arms
  of a paired comparison.
- `benchroom.stats.median_order_interval(values, error_probability=...)` returns
  an immutable `OrderInterval` with interval endpoints, achieved coverage,
  sample size, and one-based order indices. A sample too small for finite limits
  has `interval=None`; empty or nonfinite observations and invalid error
  probabilities raise `ValueError`.
- `benchroom.sampling.SamplingPolicy(looks=(6, 12),
  error_probabilities=(1/32, 3/160), precision_ratio=1.10)` defines allowed
  cumulative block counts, error allocations, and a multiplicative precision
  goal. Its `confidence` property is the across-look coverage lower bound.
- `benchroom.sampling.assess_ratios(ratios, block_ids=..., policy=...)` returns
  a `SamplingDecision`: ordinary median estimate, `OrderInterval` uncertainty,
  confidence, current error allocation, direction (`above`, `below`, or
  `unresolved`), stop reason, and `next_look`. `complete` means no further blocks
  are requested. Ratios must be finite and positive; IDs must be distinct and
  match the observation count. Invalid inputs or an unlisted look raise
  `ValueError`.
- `benchroom.sampling.rates_are_stable(previous, current, tolerance=0.10)`
  checks relative agreement of finite positive calibration rates.
- `benchroom.sampling.next_batch_size(iterations, elapsed_seconds,
  minimum_seconds=...)` retains a batch meeting the minimum, otherwise doubles
  its positive integer iteration count. The runner bounds calibration attempts.
- `benchroom.sampling.fits_budget(remaining_seconds=..., required_seconds=...,
  optional_seconds=..., reserve_seconds=0)` admits optional work only after
  protecting required collection and finishing costs. Costs must be finite and
  nonnegative; negative remaining time returns false.

Matplotlib loads only when drawing a figure; statistics and Markdown tables
do not import the plotting stack.

## Support

[Issues and questions](https://github.com/Zaczero/pkgs/issues) ·
[Private security reports](https://github.com/Zaczero/pkgs/security/advisories/new) ·
[Premium support](https://monicz.dev/#get-in-touch)
