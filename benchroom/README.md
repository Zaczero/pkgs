# benchroom

Turn benchmark measurements into clear comparison charts and tables. Use your
existing runner, keep its results, and change the presentation without measuring
again.

[Install](#installation), [make a chart](#compare-measurements),
[browse the API](#api), or [get support](#support).

## Installation

Requires Python 3.10 or newer. From this repository:

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

Install `benchroom[pyperf]` to read pyperf JSON:

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

## API

- `Series(case, name, values, x=None)` holds finite run measurements; `x` can
  label an input size. Its `median` and `interval` properties expose the analysis.
- `Report(title, series, unit, better="lower", caption="")` groups comparable
  measurements. Use `better="higher"` for throughput. `figure()`, `markdown()`
  and `save()` share the same estimates.
- `benchroom.stats.percentile(values, fraction)` interpolates a percentile.
- `benchroom.stats.median_interval(values, seed=0)` estimates the median's
  95% bootstrap interval.
- `benchroom.stats.bootstrap_interval(size, statistic, seed=0, samples=10000)`
  passes resampled indexes to a statistic. Use the same indexes for both arms
  of a paired comparison.

Git tracks the benchmark code and its history. Keep measured results with your
project when publishing charts; a brief caption should explain the machine,
workload and measurement boundary.

## Support

[Report issues](https://github.com/Zaczero/pkgs/issues) with the input
measurements and the call that produced the unexpected result.
