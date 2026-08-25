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

The release matrix contains 37 unchanged representative operations plus six
owned three-point scaling curves. Two curve endpoints reuse the canonical
representative rows `gometry.union_all/1024_service_areas` and
`gometry.tile_cover.bbox/BR_z10_15340_tiles`; no duplicate scaling aliases are
created. The result is 53 paired logical operations and 106 timed rows in five
domains: array construction and I/O, geometry, CRS and geodesy, discrete
global grids, and spatial index. The scaling curves cover spatial-index
cardinality, selectivity, and candidate-result amplification as separate
curves, n-ary overlay input count, grid-cover output count, and the
prepared-predicate batch boundary. They are sparse one-dimensional curves;
their points are not a Cartesian product.

The curve points are index cardinality `1k/5k/10k`, query selectivity
`1%/10%/50%`, candidate amplification `1/10/100` pairs per query, overlay
inputs `64/256/1024` geometries, grid output `990/3960/15340` tiles, and probe
counts `9999/10000/10001`. The 1024-geometry and 15340-tile points use the
representative rows named above.

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
pair. Release rows declare the `warm` phase: the worker performs two explicit
untimed prewarm calls for each arm before five recorded repetitions. Blocks
record the declared phase and state scope, actual lead order, monotonic
timestamps, typed metric and unit, useful output/work, passed external-oracle
identity, and producer identity before and after both arms. The untimed oracle
runs in its own fresh process; it is not executed in a timing worker, so it
cannot prewarm the timed case. The matrix contains six independent blocks per
pair, with
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

The generated summary retains each curve's three raw paired medians, per-point
competitor-to-gometry ratios, seconds per validated useful-work unit, and
first-to-last endpoint growth. These are descriptive observations; the summary
does not label a noisy three-point wall-time series as proportional or
quadratic. The generated summary follows.

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

The release matrix does not claim a generic `first-use` or operating-system
cache-cold phase. A shared-process pair cannot give both arms their first
operation invocation without the first arm warming state for the second. A
separate fresh process per arm would be a different execution contract, so the
current evidence uses the explicitly prewarmed `warm` phase instead.

## See Also

- [Arrays & performance](../guide/arrays.md) — packed construction and batch results.
- [Internals](internals.md) — durable ownership and interchange boundaries.
- [Compatibility](compatibility.md) — supported runtime and integration matrix.
