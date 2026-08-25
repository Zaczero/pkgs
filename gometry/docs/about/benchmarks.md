---
description: Current accepted gometry benchmark run and its evidence method.
---

# Benchmarks

This page reports the current accepted full release run only. It does not
combine runs, publish filtered or smoke measurements, or make a timing claim
when the tracked result is absent.

## Evidence Workflow

Benchmark evidence is measured and analyzed from explicit immutable manifests.
The release measurement command writes only raw artifacts and a run manifest
under `target/bench/results`; it cannot mutate tracked evidence. Analyze one
exact run manifest to produce normalized review files under
`target/bench/results`. Accepted publication consumes a reviewed immutable
analysis manifest, and this page reports only the accepted evidence.

```bash
uv run python benches/drivers/bench.py --profile release
uv run python benches/drivers/bench.py --analyze target/bench/results/<run-id>/run.json
```

`<run-id>` is the directory name printed by the measurement command. The
normalized evidence retains exact paired-block identities and the raw artifacts
used to derive it.

## Current Run

The release matrix contains 37 fully paired operations and 74 timed rows in
five domains: array construction and I/O, geometry, CRS and geodesy, discrete
global grids, and spatial index.

The runner records the exact Python executable and build identity, extension
digest and effective Cargo profile facts, source identity, a required dependency
lock identity for publishable release evidence, and exact module, distribution,
and loaded native-library identities for each implementation. It also records
the selected CPU and governor, CPU/core/socket/SMT and cache topology, NUMA and
cgroup state, allowed-memory policy, and observed memory placement when Linux
exposes them. CPU affinity is not memory placement; unavailable host facts stay
explicitly unavailable and do not become claims about another host. Cargo
exposes custom profiles to build scripts only through their
effective `debug`/`release` family, so the record does not claim to name a custom
profile such as `release-dist`.
The recorded `debug` value describes compiler debug information; this workspace
may retain line tables in an optimized release build. Release suitability is
determined by the release family, optimization level, and debug-assertion state.
Each release block is one fresh process that executes both arms of one operation
pair. Blocks record their actual lead order, monotonic timestamps, typed metric
and unit, equal useful work, passed oracle status with an operation-specific
oracle identity, and producer identity before and after both arms. The matrix
contains six independent blocks per pair, with
three `AB` and three `BA` leads. Analysis bootstraps the paired median ratio
over those block artifacts and classifies it against a predeclared 3%
practical equivalence margin. `Similar` means the full interval is inside
that margin; `faster` and `slower` require the interval to clear it; all
other intervals are `Inconclusive`. Analysis derives the expected operation,
pair, block, and lead identities from the run manifest and rejects missing,
duplicate, swapped, extra, wrong-profile, or cross-run artifacts before
calculating a result. The analysis manifest hashes that run manifest, whose
raw-artifact hashes bind the exact observation matrix. The [full normalized
evidence](https://github.com/Zaczero/pkgs/tree/main/gometry/benches/results/current)
retains exact medians, intervals, block identities, and raw artifacts.

Optional Linux `perf` counters are diagnostic only. Requested events first run
a moving execution positive control and are grouped no wider than the usable
capacity measured on that host. Unsupported, nonmoving, `<not counted>`, or
over-multiplexed events produce a bounded capability null, never a zero counter
or a publishable result.

The generated summary follows.

--8<-- "benches/results/current/summary.md"

## Method Boundaries

Fixtures are built before timing and each paired row has an untimed
fail-closed oracle for row counts, missingness, types, axes, coordinates,
topology, or domain-specific preconditions. Resource probes remain gometry-only
diagnostics and are not comparative release evidence. They record current RSS
and proportional set size (PSS) at setup and operation endpoints, plus the
process lifetime RSS high-water mark as a diagnostic. Signed memory deltas use
the immediately adjacent samples around the isolated call and latency loop;
they do not turn setup-to-later accumulation into operation growth. The probes
do not infer an in-call native peak from before/after samples; Python traced
allocation peak is Python-only. No dashboard, history, or percentile/peak
resource claim is published here.

## See Also

- [Arrays & performance](../guide/arrays.md) — packed construction and batch results.
- [Internals](internals.md) — durable ownership and interchange boundaries.
- [Compatibility](compatibility.md) — supported runtime and integration matrix.
