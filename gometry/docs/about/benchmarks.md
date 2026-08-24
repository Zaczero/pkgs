---
description: Current accepted gometry benchmark run and its evidence method.
---

# Benchmarks

This page reports the current accepted full release run only. It does not
combine runs, publish filtered or smoke measurements, or make a timing claim
when the tracked result is absent.

## Current Run

The release matrix contains 37 fully paired operations and 74 timed rows in
five domains: array construction and I/O, geometry, CRS and geodesy, discrete
global grids, and spatial index.

The runner records the exact Python executable, extension digest and build
profile, source identity, an optional lock hash, exact package and native-engine
versions, selected CPU and governor, and pyperf artifact identities. Each
pyperf process is one independent observation. The normalized evidence matches
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
diagnostics and are not comparative release evidence. No dashboard, history,
or percentile/peak resource claim is published here.

## See Also

- [Arrays & performance](../guide/arrays.md) — packed construction and batch results.
- [Internals](internals.md) — durable ownership and interchange boundaries.
- [Compatibility](compatibility.md) — supported runtime and integration matrix.
