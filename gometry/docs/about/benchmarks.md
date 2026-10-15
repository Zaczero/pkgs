---
description: Current release benchmark results for gometry and how to interpret their scope.
---

# Benchmarks

--8<-- "benches/results/current/summary.md"

A published table represents one complete release comparison. Publication
checks that the benchmark harness, native and Rust build inputs, and packaged
Python members still match the bytes recorded for the run and its selected
release receipts. Later documentation and summary commits do not invalidate
unchanged measurement inputs. Smoke, filtered, incomplete, stale, or
unattributed runs do not appear as current results.

Each table links to a content-addressed evidence bundle containing its
`run.json`, every listed raw measurement and oracle artifact, and the selected
producer receipt files. The bundle includes an index connecting receipts to
the paths and SHA-256 digests recorded by the run. It retains the measured
source, native, extension, wheel, and receipt identities; wheel and native
binaries are not copied into the documentation site.

## What the run measures

The matrix pairs each gometry operation against its ecosystem counterpart:
53 paired logical operations across array construction and I/O, geometry, CRS
and geodesy, discrete global grids, and spatial indexes, plus six three-point
scaling curves over index cardinality, query selectivity, candidate
amplification, n-ary overlay input count, grid output count, and the
prepared-predicate batch boundary.

Each pair runs in six independent blocks — three leading with gometry, three
leading with the comparator — each block a fresh process that executes both
arms after two untimed prewarm calls. Analysis bootstraps the paired median
ratio over those blocks and classifies it against a declared 3% practical
equivalence margin: `faster` and `slower` require the whole interval to clear
the margin, `similar` requires it to sit inside, and anything else is
`inconclusive`. Every row carries an untimed fail-closed oracle that checks row
counts, missingness, types, axes, coordinates, and topology, so a wrong answer
cannot be published as a fast one.

Each measured process admits its selected receipt before importing Gometry.
The driver checks the process's ready record before allowing its first timed
operation; an import-path fallback, a different worktree facade, or an extension
whose bytes do not match the receipt's wheel member is rejected. The process
identity binds its interpreter and ABI, build profile, Python package members
and distribution owner, native extension, wheel archive, source commit, and
receipt digest. Per-invocation process IDs identify the observation; they are
not treated as stable producer identity.

The operation timers begin after admission and setup. `bench_ab.py --perf`
attaches user-mode counters to the case's main thread for one interval per
invocation, from permission until the case reports measurement-done. Existing
case repetitions stay within that interval; split event groups use separate
invocations. Counter records are diagnostic, never release latency. An
unavailable event remains explicitly marked unavailable with a null value.
Runs also record the host's CPU, governor, cache topology, and memory policy. A
host fact the kernel does not expose stays recorded as unavailable rather than
becoming a claim about some other machine.

## See Also

- [Arrays & performance](../guide/arrays.md) — packed construction and batch results.
- [Internals](internals.md) — durable ownership and interchange boundaries.
- [Compatibility](compatibility.md) — supported runtime and integration matrix.
