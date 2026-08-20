---
description: gometry benchmark methodology, release profiles, and competitive tables.
---

# Benchmarks

Performance claims must measure the public API users call, compare equivalent
semantics against ecosystem competitors (Shapely, pyproj, h3-py, GeoPandas,
Mercantile, …), and report uncertainty. A one-off timing loop is useful for
finding a lead, not for publishing a result. gometry v1.0.0 has no prior public
release, so every comparison is gometry versus a competitor — never gometry
versus an older gometry.

## The two profiles

[`benches/support/_bench_registry.py`](https://github.com/Zaczero/pkgs/tree/main/gometry/benches/support/_bench_registry.py)
is the single ordered manifest (`RELEASE_OPERATIONS`) for both profiles.

| Profile | Purpose | Sampling |
|---|---|---|
| `smoke` | Prove the curated rows import, execute, and pass the untimed oracle. | One debug value per logical operation. |
| `release` | Produce the competitive comparison table for the docs. | Each competitive pair once as A/B and once as B/A, three processes per ordering. |

There is no standard/full/exhaustive profile matrix. Focused optimization work
uses the interleaved A/B harness; the release driver stays bounded and has one
unambiguous statistical configuration.

## Running the maintained harness

Build the current extension first:

```bash
uv run --no-project --python .venv/bin/python --with maturin==1.14.1 maturin develop --release
```

**Two-step flow** (oracle + timing, then presentation):

```bash
# 1) Run the profile (writes a run manifest JSON with embedded public_operations)
.venv/bin/python benches/drivers/bench.py --profile smoke \
  --output-dir target/bench/results
# or, for publishable evidence on a quiet host:
.venv/bin/python benches/drivers/bench.py --profile release \
  --output-dir target/bench/results

# 2) Render the competitive table from that exact run JSON
.venv/bin/python benches/support/summarize_bench.py \
  target/bench/results/<timestamp>-<profile>.json --format md
```

Inspect the exact release commands without executing them:

```bash
.venv/bin/python benches/drivers/bench.py --profile release --plan-only
```

The driver prints the run-manifest path; pass that file to `summarize_bench.py`
rather than a directory that may contain several runs. The summarizer reads the
ordered `public_operations` metadata embedded in the run JSON (domains, labels,
workloads, competitor labels, footnotes) and never re-infers them from raw row
names. Rows stay in the manifest editorial order (hero-first per domain). Smoke,
filtered, failed-oracle, and otherwise nonpublishable runs show a visible banner
and are not release evidence.

The orchestrator always runs the complete profile when no filter is supplied.
`--filter` accepts exact row names for a focused sampled check, but its output
is marked nonpublishable, omits the whole-profile resource probes, and is never
release evidence. Exploratory cases that are not release rows belong in
`benches/cases/` and run through `bench_ab.py`.

Release comparisons require a quiet committed worktree, performance governors,
the competitor packages required by the selected operations, and a
kernel-isolated CPU. Each competitive pair runs immediately in both lead orders
with three pyperf processes per pass, so each row still receives six processes
while slow machine drift cannot favor one library consistently. Four
fresh-process probes additionally retain call-level p50/p99/p99.9 samples and
record absolute process peak RSS, post-setup RSS growth, and Python allocation
peaks. The driver checks contention again after the last probe; a run is marked
publishable only when the full manifest succeeds with clean preflight and
postflight evidence (a quiet host is required for a table worth pasting).

## What the release manifest covers

Six domains (35 logical operations; 34 competitive pairs + one honest S2-only
row):

1. **Array construction & I/O** — mixed EWKB round-trip, GeoArrow ingest
   (including BinaryView WKB), coordinate extraction, NumPy point construction.
2. **Geometry** — irregular polygon intersects, prepared contains XY, dwithin,
   area/length, simplify, buffer, intersection, union_all, coverage_union,
   is_valid, repair.
3. **CRS & geodesy** — masked `to_crs` (in-core path), vector `crs_transform`,
   geodesic distance and destination.
4. **Discrete global grids** — geohash encoding, H3 cover/compact, adaptive S2
   cover (gometry-only), tile bbox cover.
5. **Spatial index** — one-shot join/within, indexed join/within, candidates,
   nearest k=1, tree build.
6. **Real-world workflows** — country GeoJSON parse, ellipsoidal area and
   exterior length.

Competitor rows use Shapely, pyproj, h3-py, GeoPandas, Mercantile, and
pygeohash only where
the input, semantics, and output shape are meaningfully comparable. The S2
adaptive cover row is gometry-only (s2sphere has no equivalent polygon coverer)
and is excluded from every speedup statistic. A candidate-only index query is
not compared with an exact predicate, and planar area is not presented as
equivalent to ellipsoidal area.

Speedup language is ratio = competitor_time / gometry_time: solo → "—",
statistical tie → "≈ parity", ratio ≥ 1 → "{r:.2f}× faster", ratio < 1 →
"{r:.2f}× as fast". Domain headlines use the geometric mean of pair ratios;
the overall headline is the geometric mean of domain geomeans (equal domain
weight, so Geometry's larger row count does not dominate).

## Per-change A/B measurements

For an implementation optimization, compare separate environments with the
balanced harness (this is local engineering evidence, not the published
competitive table):

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

### Recorded focused measurements

These implementation measurements use paired, interleaved trials and report
retired user-mode instructions, not wall-clock time or competitive release-table
results.

- **Prepared point-in-polygon classification:** for prepared-geometry queries
  against a 64-edge regular polygon, the lazy 64×64 conservative grid activates
  at 10,000 probes. Instruction reductions were 82.388% at 10,000 probes,
  81.273% at 12,000, 78.779% at 16,000, and 74.247% at 24,000. Below that
  threshold, the deltas at 1, 4,096, 8,000, 9,000, and 9,999 probes were
  +0.071%, +0.017%, -0.008%, +0.001%, and +0.010%. Grid construction fell from
  11,140,738 to 833,224 instructions, including marking from 10,554,117 to
  246,321. `Maybe = 252/4096`, so about 94% of cells had a certified result and
  avoided exact evaluation.
- **Hierarchical uncompact:** range-ordered expansion reduced instructions by
  79.047% for S2 (16 roots to 4,096 leaves), 88.256% for Tile (16 roots to
  4,096 leaves), and 80.704% for Geohash (8 roots to 8,192 leaves). At 262,144
  leaves, the reductions were 85.294% for S2 (16 roots), 91.693% for Tile (16
  roots), and 85.167% for Geohash (8 roots). Already-canonical input measured
  reductions of 3.196% for S2 (3,072 roots/leaves), 2.081% for Tile (3,072
  roots/leaves), and 3.134% for Geohash (7,936 roots/leaves). H3 is excluded:
  it uses `h3o::CellIndex::uncompact`, not this generic path.

## Reading results

- Compare steady-state distributions, not one point estimate.
- Publish ties alongside wins.
- Separate conversion cost from kernel cost.
- Record Python/Rust/dependency versions, hardware, affinity/governor, and host
  contention with the raw artifact.
- Include peak memory/allocation and tail latency when those dimensions matter
  to the real workload.
- Run the untimed cross-library oracle before trusting a faster result.

The release JSON artifacts and the domain-grouped markdown table are the source
of truth. This guide explains the method; it does not substitute for
measurements.

## Results

!!! note "Release table pending"

    The competitive table is generated by the two-step release flow above
    (`bench.py --profile release` then `summarize_bench.py … --format md`) on a
    quiet, committed, kernel-isolated host and pasted here at release. No
    publishable numbers have been recorded yet — do not invent them.
