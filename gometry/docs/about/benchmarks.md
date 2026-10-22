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

Call time as input or output size grows. Whiskers show the range across paired
process blocks.

<!-- benchmark-scaling -->

## Methodology {#what-the-run-measures}

Every release workload first runs six independent paired process blocks, with
the libraries alternating which runs first. After that first look, an unresolved
comparison may extend to twelve blocks if the run budget still protects all
required resource probes and time to finish. Resolved or sufficiently precise
comparisons stop at six. Each block times calls in batches of at least 1 ms
after warm-up; its result is the median per call. Before timing, both outputs
are checked for equivalent rows, types, coordinates, and topology. The external
oracle and four resource probes run independently of throughput confidence.
One-block smoke runs are diagnostic only.

Labels use the ordinary median of per-block comparator/gometry duration ratios
and an exact median order interval. The first and second looks allocate error
probabilities of 1/32 and 3/160, respectively, for at least 95% coverage across
the permitted looks for each comparison. A 3% practical-equivalence margin
classifies the interval:

| Label | Meaning |
| --- | --- |
| Faster / slower | The entire interval is beyond the 3% margin. |
| Similar | The entire interval is within the margin. |
| Inconclusive | The interval is too wide or crosses a decision boundary. |

Interval precision is a stopping rule, not an equivalence claim; an interval
crossing one is not by itself evidence that the implementations are equal. The
current release run uses the declared six/twelve-block adaptive policy and
records its actual block plan and per-operation inference with the raw
measurements.

For the current run, CPU 31 was selected from the allowed affinity because no
kernel-isolated CPU was available; it used the powersave governor. Host
diagnostics recorded a one-minute load average of 10.12 before timing and 9.04
at run completion on a 32-CPU host, with external CPU contention. The raw
measurements retain these conditions and the observed block spread.

--8<-- "benches/results/current/summary.md:environment"

--8<-- "benches/results/current/summary.md:evidence"
