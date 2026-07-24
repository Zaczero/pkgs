---
description: gometry benchmark methodology, release profiles, and evidence rules.
---

# Benchmarks

Performance claims must measure the public API users call, compare equivalent
semantics, and report uncertainty. A one-off timing loop is useful for finding a
lead, not for publishing a result.

## Evidence status

The pre-release API and harness have changed since the last exploratory run, so
there is currently **no locked v1.0.0 performance baseline and no published
faster/slower claim**. The release profile must be rerun on the final worktree
before the first public release. This avoids presenting stale measurements as
evidence for a different API or implementation.

## The two profiles

[`benches/support/_bench_registry.py`](https://github.com/Zaczero/pkgs/tree/main/gometry/benches/support/_bench_registry.py)
is the single manifest for both profiles.

| Profile | Purpose | Sampling |
|---|---|---|
| `smoke` | Prove the curated rows import, execute, and validate their outputs. | One debug value per row. |
| `release` | Produce the reproducible comparison artifact used by release notes. | 6 processes per row; competitor pairs split evenly across A/B and B/A lead order. |

There is no standard/full/exhaustive profile matrix. Focused optimization work
uses the interleaved A/B harness; the release driver stays bounded and has one
unambiguous statistical configuration.

## Running the maintained harness

Build the current extension first:

```bash
uv run --no-project --python .venv/bin/python --with maturin==1.14.1 maturin develop --release
```

Check the manifest and dependencies:

```bash
.venv/bin/python benches/drivers/bench.py --profile smoke
```

Inspect the exact release commands without executing them:

```bash
.venv/bin/python benches/drivers/bench.py --profile release --plan-only
```

Then run the release profile on a quiet host and keep the JSON/Markdown outputs:

```bash
.venv/bin/python benches/drivers/bench.py --profile release \
  --output-dir benches/results/baseline
```

Summarize competitor parity from that run's exact manifest (the driver prints
its path):

```bash
.venv/bin/python benches/support/summarize_bench.py \
  benches/results/baseline/<timestamp>-release.json --format md
```

Use the run manifest rather than a directory containing several runs; the
manifest pins the exact A/B and B/A artifact set that belongs together.

The orchestrator always runs the complete profile: gometry kernels, direct
competitor pairs, and real-world pairs when no filter is supplied. `--filter`
accepts exact row names for a focused sampled check, but its output is marked as
a partial manifest, omits the whole-profile resource probes, and is never
release evidence. Exploratory cases that are not release rows belong in
`benches/cases/` and run through `bench_ab.py`.

Release comparisons require a quiet committed worktree, performance governors,
all competitor dependencies, and a kernel-isolated CPU. Each competitive pair
runs immediately in both lead orders with three pyperf processes per pass, so
each row still receives six processes while slow machine drift cannot favor one
library consistently. Four fresh-process probes additionally retain call-level
p50/p99/p99.9 samples and record absolute process peak RSS, post-setup RSS
growth, and Python allocation peaks. The driver checks contention again after
the last probe; a run is marked
publishable only when the full manifest succeeds with clean preflight and
postflight evidence.

## What the release manifest covers

- geometry construction and vectorized predicates;
- WKB ingestion and representative constructive operations;
- H3 and S2 construction;
- CRS transforms, bounds, factors, and ellipsoidal geodesics;
- spatial-index build, query, nearest, and dense pair workloads;
- real-world country GeoJSON parsing, bounds, area, and representative points.

Competitor rows use Shapely, pyproj, h3-py, and s2sphere only where the input,
semantics, and output shape are meaningfully comparable. A candidate-only index
query is not compared with an exact predicate, and planar area is not presented
as equivalent to ellipsoidal area.

## Per-change A/B measurements

For an implementation optimization, compare separate baseline and candidate
environments with the balanced harness:

```bash
.venv/bin/python benches/drivers/bench_ab.py \
  --a /path/to/baseline/bin/python \
  --b .venv/bin/python \
  --case benches/cases/case_import_wkb.py \
  --rounds 9 --warmup 2 --seed 20260709 --cpu 1 \
  --json-out /tmp/gometry-import-wkb-ab.json
```

The harness alternates lead order, pins both children to the same CPU, and
reports the block median, IQR, maximum block time, and bootstrap intervals.
Treat a `NOISE` verdict as no result. Explain any surprising magnitude before
keeping the change. Call-level p99 and p99.9 belong to the release resource
probes, not a nine-block implementation comparison. Every case invocation has
a 300-second hard timeout, so a broken probe cannot hang the A/B run indefinitely.

## Reading results

- Compare steady-state distributions, not one point estimate.
- Publish ties and regressions alongside wins.
- Separate conversion cost from kernel cost.
- Record Python/Rust/dependency versions, hardware, affinity/governor, and host
  contention with the raw artifact.
- Include peak memory/allocation and tail latency when those dimensions matter
  to the real workload.
- Run correctness/oracle tests before trusting a faster result.

The release report and raw JSON are the source of truth. This guide explains the
method; it does not substitute for measurements.
