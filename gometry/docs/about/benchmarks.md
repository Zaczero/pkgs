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
normalized evidence retains exact process identities and the raw artifacts
used to derive it.

## Current Run

The release matrix contains 37 fully paired operations and 74 timed rows in
five domains: array construction and I/O, geometry, CRS and geodesy, discrete
global grids, and spatial index.

The runner records the exact Python executable, extension digest, effective Cargo
profile family and build facts, source identity, an optional lock hash, exact
package and native-engine versions, selected CPU and governor, and pyperf artifact
identities. Cargo exposes custom profiles to build scripts only through their
effective `debug`/`release` family, so the record does not claim to name a custom
profile such as `release-dist`.
The recorded `debug` value describes compiler debug information; this workspace
may retain line tables in an optimized release build. Release suitability is
determined by the release family, optimization level, and debug-assertion state.
Each pyperf process is one independent observation. The normalized evidence matches
the separate gometry and comparator rows by process ordinal within each AB/BA
artifact, bootstraps the median ratio, and classifies it against a predeclared
3% equivalence/noise band. `Similar` means the full interval is inside that
band; `faster` and `slower` require the interval to clear it; all other
intervals are `Inconclusive`. The [full normalized
evidence](https://github.com/Zaczero/pkgs/tree/main/gometry/benches/results/current)
retains exact medians, intervals, process identities, and raw artifacts.

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
