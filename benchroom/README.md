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
Statistics are calculated once when the report is created and exposed through
`report.results`; rendering another format or theme reuses them.

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
- `Summary` holds each result's case, implementation name, median, interval,
  run count and optional size. Reports retain summaries rather than raw samples.
- `benchroom.stats.percentile(values, fraction)` interpolates a percentile.
- `benchroom.stats.median_interval(values, seed=0)` estimates the median's
  95% bootstrap interval.
- `benchroom.stats.bootstrap_interval(size, statistic, seed=0, samples=10000)`
  passes resampled indexes to a statistic. Use the same indexes for both arms
  of a paired comparison.

Matplotlib loads only when drawing a figure; statistics and Markdown tables
do not import the plotting stack.

## Support

[Issues and questions](https://github.com/Zaczero/pkgs/issues) ·
[Private security reports](https://github.com/Zaczero/pkgs/security/advisories/new) ·
[Premium support](https://monicz.dev/#get-in-touch)
