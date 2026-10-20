---
description: Compare gometry with Shapely, pyproj, H3, and other Python libraries across geometry, CRS transforms, grids, and spatial indexes.
hide:
  - navigation
---

# Benchmarks

These benchmarks compare gometry with Shapely, pyproj, h3-py, Mercantile, and
other Python libraries across geometry, coordinate transforms, grids, and
spatial indexes. The tables show warm per-call timings on the recorded host,
with fixture setup excluded.

## API comparisons {#all-measured-comparisons}

--8<-- "benches/results/current/summary.md:comparisons"

The rectangle-clipping comparison uses short synthetic lines in crossing,
inside, and outside categories. Its correctness oracle verifies equivalent
outputs for that fixture; the timing does not establish general equivalence
between the libraries' clipping guarantees.

## Reference implementations

These two measurements use a Python/NumPy quadrature reference, rather than
a competing library operation. They measure area and boundary length on the
WGS84 ellipsoid along straight longitude/latitude chart edges, not shortest
geodesic edges. Pyproj supplies the ellipsoid constants; the reference performs
the integration. Their ratios describe this reference implementation only.

--8<-- "benches/results/current/summary.md:references"

## Scaling

<span id="scaling-curves"></span>

Call time as input or output size grows. Whiskers show the range across six runs.

<!-- benchmark-scaling -->

## Methodology {#what-the-run-measures}

Each workload runs both implementations in six fresh processes, alternating
which implementation runs first. Each arm makes two untimed warm-up calls,
then uses bounded doubling to choose a batch targeting at least 1 ms.
Five measured batches use that fixed call count. Each call releases its result
inside the timer, including the last call; output validation uses a separate
untimed call. Per-call samples divide each batch duration by its recorded
iteration count. Imports, fixture construction, calibration, and correctness
checks are outside the measurement windows. Garbage collection keeps the
interpreter's normal policy.

Each displayed time is the median of the six process medians. The paired
ratio is the median of the six within-process ratios of comparator time to
gometry time: a ratio above one favors gometry. Its 95% percentile-bootstrap
confidence interval resamples whole process pairs, not individual calls.
The paired ratio need not equal the quotient of the two displayed times.
These intervals describe repeatability for this workload and host; they do
not capture differences between machines or applications.

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
