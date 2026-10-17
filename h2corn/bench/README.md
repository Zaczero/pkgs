# Reproduce the benchmarks

Compare h2corn, Uvicorn, Hypercorn, and Gunicorn with the same Starlette
application. The [benchmark page](../docs/benchmarks.md) explains the workloads
and how to read the results.

Run from the `h2corn` checkout on Linux or macOS with Python 3.11+.
Install `oha` for HTTP scenarios and `k6` for WebSockets, then:

```bash
uv sync
uv run python bench/bench.py
```

Results are saved under `bench/results/runs/` as raw JSON, SVG charts, and
matching data tables. Each trial verifies the response and worker readiness
before and after load.

## Select a workload

```bash
uv run python bench/bench.py --servers h2corn uvicorn --types h1
```

Use `--duration`, `--warmup-duration`, `--max-trials`, `--scenario-budget`,
and `--suite-budget` to control measurement effort. A short smoke run checks
that the setup works; it is not a performance comparison:

```bash
uv run python bench/bench.py \
  --servers h2corn --types h1 --network-profile loopback \
  --duration 1s --warmup-duration 1s --max-trials 3 \
  --scenario-budget 15 --suite-budget 30
```

The default suite measures loopback, then tries selected streaming and
multiplexing scenarios with 50 ms round-trip latency in a network namespace.
That profile needs `unshare`, `ip`, and `tc`. If unavailable, it is skipped
with a reason; loopback results are not substituted.

## Comparison conditions

- Servers use the same application, asyncio loop, aligned settings, and access
  logging to `/dev/null`.
- Server order is rotated; neither servers nor load generators are CPU-pinned.
- Results report medians, bootstrap confidence intervals, and observed ranges
  across cold starts.
- Sampling uses the configured trial count or stops at the time budget.
  Confidence intervals describe uncertainty without controlling sample size.
- Unsupported competitor scenarios are excluded with their reason.
  An h2corn failure stops the run.
- Peak proportional set size (PSS) is sampled across the supervisor and workers.

See `uv run python bench/bench.py --help` for the full option list.
