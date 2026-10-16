---
description: Compare gometry with Shapely, pyproj, H3, and other Python libraries across geometry, CRS transforms, grids, and spatial indexes.
hide:
  - navigation
---

# Benchmarks

These benchmarks compare gometry with Shapely, pyproj, h3-py, Mercantile, and
other Python libraries across geometry, coordinate transforms, grids, and
spatial indexes. Timings are medians across six paired runs of each workload,
with fixture setup excluded.

## API comparisons {#all-measured-comparisons}

--8<-- "benches/results/current/summary.md:comparisons"

## Scaling

<span id="scaling-curves"></span>

Call time as input or output size grows. Whiskers show the range across six runs.

<!-- benchmark-scaling -->

## Methodology {#what-the-run-measures}

Each workload runs both implementations in six fresh processes, alternating
which implementation runs first. Each process makes two untimed warm-up calls.
Correctness checks compare row counts, missing values, types, coordinate axes,
coordinates, and topology outside the timer.

The comparison labels use a 95% bootstrap interval for the paired timing ratio
and a 3% practical-equivalence margin:

| Label | Meaning |
| --- | --- |
| Faster / slower | The entire interval is beyond the 3% margin. |
| Similar | The entire interval is within the margin. |
| Inconclusive | The interval crosses a decision boundary. |

--8<-- "benches/results/current/summary.md:environment"

--8<-- "benches/results/current/summary.md:evidence"
