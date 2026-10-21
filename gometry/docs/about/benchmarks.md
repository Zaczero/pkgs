---
description: Compare gometry with Shapely, pyproj, H3, and other Python libraries across geometry, CRS transforms, grids, and spatial indexes.
hide:
  - navigation
---

# Benchmarks

These benchmarks compare gometry with Shapely, pyproj, h3-py, Mercantile, and
other Python libraries across geometry, coordinate transforms, grids, and
spatial indexes.

## API comparisons {#all-measured-comparisons}

--8<-- "benches/results/current/summary.md:comparisons"

## Reference implementations

Ellipsoidal area and length along straight longitude/latitude edges are
compared with a Python/NumPy quadrature reference.

--8<-- "benches/results/current/summary.md:references"

## Scaling

<span id="scaling-curves"></span>

Call time as input or output size grows. Whiskers show the range across six runs.

<!-- benchmark-scaling -->

## Methodology {#what-the-run-measures}

Each workload runs both libraries in six fresh processes, alternating which
runs first. After warm-up, calls are timed in batches of at least 1 ms, and
each displayed time is the median per call. Before timing, both outputs are
checked for equivalent rows, types, coordinates, and topology.

Labels compare the two libraries within each process and use a 95% bootstrap
interval with a 3% equivalence margin:

| Label | Meaning |
| --- | --- |
| Faster / slower | The entire interval is beyond the 3% margin. |
| Similar | The entire interval is within the margin. |
| Inconclusive | The interval crosses a decision boundary. |

For this run, the selected timing CPU was not isolated and used the powersave
governor; host diagnostics also recorded CPU contention. The raw measurements
retain these conditions and the six-block spread.

--8<-- "benches/results/current/summary.md:environment"

--8<-- "benches/results/current/summary.md:evidence"
