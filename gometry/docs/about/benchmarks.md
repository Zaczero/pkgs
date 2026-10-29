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

Each workload uses six to twelve paired measurements after warm-up, alternating
which library runs first. Outputs are checked for equivalent rows, types,
coordinates, and topology before timing.

A separate MVT profile compares tile-local encoding and decoding with the
Python `mapbox-vector-tile` package, using point and mixed-geometry layers.
These codec measurements start with prepared tile-local geometry; source
selection, projection, and clipping are outside their timing boundary.
Encoding and decoding compare equivalent values, while their Python result
representations differ.

Ratios summarize paired call times. Labels use confidence intervals with at
least 95% coverage and a 3% practical-equivalence margin:

| Label | Meaning |
| --- | --- |
| Faster / slower | The entire interval is beyond the 3% margin. |
| Similar | The entire interval is within the margin. |
| Inconclusive | The interval is too wide or crosses a decision boundary. |

Measurements used one pinned CPU with the powersave governor and concurrent
host activity.

--8<-- "benches/results/current/summary.md:environment"

--8<-- "benches/results/current/summary.md:evidence"
