# Measurement and inference

This reference describes `fast`'s invocation boundary, resource scopes, and
approximate timing inference for Linux command comparisons.

## Invocation boundary

The single-threaded collector prepares arguments, environment, input descriptors,
and capture buffers before timing. It then timestamps process creation, attaches
available performance events before releasing the child to exec, and waits for
the root command's completion. Timing ends before residual stderr draining.

The child has its own process group. A subreaper retains ownership of orphaned
descendants without creating cgroups or changing inherited scheduling policy.
Interruption signals owned work, escalates if necessary, and returns a partial
report. This is lifecycle tracking, not containment: an uninterruptible kernel
task cannot be made immediately reapable by userspace.

Stderr capture keeps the first and last 32 KiB while continuing to drain the pipe.
Each draining turn is bounded so continuous output cannot starve completion or
timeouts. Post-completion draining has a 100 ms limit.

## Resource scopes

`wait4` supplies user/system CPU, faults, switches, and maximum RSS.
Linux's [`getrusage` documentation](https://man7.org/linux/man-pages/man2/getrusage.2.html)
defines the high-water and waited-child semantics. Sampled process or thread CPU
is never added to wait-accounted CPU.

Wait-accounted RSS preserves memory inherited across `fork` and `exec`. The child
reports its launch-time RSS high-water mark over the existing release channel,
before command execution. The default report shows mean peak and mean launch
values together. This exposes a parent-memory floor without subtracting one
high-water mark from another: such a subtraction cannot recover workload memory.
It adds no extra command execution, memory scan, or helper process.

Hardware instructions, cycles, and branch misses use separate ungrouped
events with inheritance, user-space-only privilege scope, and enable-on-exec.
The collector attaches fresh events to each child before releasing it to exec.
Each observation retains raw,
enabled-time, and running-time readings. The report uses scaled counts only when
at least 95% of enabled time was scheduled. These are supplemental descriptive
measurements. The [`perf_event_open` interface](https://man7.org/linux/man-pages/man2/perf_event_open.2.html)
documents the inheritance/group-read restrictions this arrangement avoids.

On AMD Family 19h Model 21h (Zen 3 Vermeer), the standard policy also collects
core-to-L2 request misses: raw event `0x64`, unit mask `0x09`. These exclude
L2 prefetch requests. The report labels the cache level
explicitly and normalizes misses by retired instructions; it does not interpret
the count as stall time or a diagnosis of a memory bottleneck. Other processors
retain the generic counters without guessing an equivalent raw event.

Instructions per cycle and branch-miss ratios require full counter scheduling
through every selected invocation. Ratios use summed counts, not an average of
per-run ratios. Instruction variation is the sample standard deviation divided
by the mean; it describes repeatability, not confidence in a timing improvement.
Peak and launch RSS are means of the individual invocation high-water marks.

Task sampling follows the root and discovered child lists, not a scan of every
process on the host. Identity includes invocation, PID namespace, task ID, and
kernel start ticks. The initial cadence is 100 ms, increasing to at least 200
times the preceding sweep's collector CPU cost. Large sweeps yield between
16-task chunks for at least 200 times each chunk's collector CPU cost, so the
initial sweep cannot run as an uninterrupted burst. A sweep still in progress
when the command exits marks task detail incomplete. At most 1,024 task identities and
bounded activity samples are retained per invocation. Task detail survives only
for the first complete measured invocation of each command.

These are sampled accounting changes, not scheduler events or function profiles.
The [`proc` documentation](https://docs.kernel.org/filesystems/proc.html) describes
the underlying process/thread records and their limitations.

## Frozen planning

Explicit counts bypass the pilot. Automatic effort first performs up to eight
complete pilot blocks, continuing only while another block at the mean pilot
cost fits in 20% of the target. Block cost includes hooks and collection.

Production count is the number of blocks at the mean pilot block cost that fit in
the remaining target, bounded by the larger of a pilot influence-function
precision estimate and the 100 blocks timing conclusions require, and by
in-memory observation limits. Effort is a sum over many blocks, so the mean is
its planning cost; a percentile of eight pilot blocks is effectively the slowest
one, which a single load spike can inflate several-fold. A pilot that
underestimates a heavy-tailed cost can overrun the target. Pilot observations
are retained but excluded from production estimates. No interval is inspected to
decide whether to stop, extend a run, or remove a candidate.

## Arithmetic means and simultaneous intervals

For each command pair, the estimand is the ratio of arithmetic mean elapsed
times. The inference engine uses chronological complete block vectors and joint
stationary bootstrap resampling. All commands share every resampled segment.
Integer prefix sums reconstruct segment totals exactly before conversion to
floating point.

The method, `stationary-bootstrap-log-ratio-max-envelope-v3`, uses 20,000
replicates per segment length and primary expected segment length
`max(2, floor(sqrt(block_count)))`.
Bootstrap standard deviations scale log-ratio deviations. The 95th percentile
of their joint maximum defines approximate simultaneous intervals across all
command pairs. A single command uses log arithmetic mean instead. Each radius
is multiplied by
`sqrt(n / (n - (2L - 1))) * t_quantile / normal_quantile`, with
`n/L - 1` degrees of freedom and quantile probability `1 - .05/(2m)` for
family size `m`. This is a calibrated conservative correction, not an exact
finite-sample theorem.

Inference requires a completed plan and at least 100 measured blocks.
Zero means and degenerate scales cannot support a confidence claim.
Prespecified half-length and double-length calculations use independent random
streams. The reported interval is the envelope of all three intervals;
JSON also retains the primary interval. Decisions and nonregression checks use
the envelope, including when the check threshold differs from the display margin.

These intervals assume an adequate stationary, weak-dependence approximation.
They cannot repair arbitrary drift, carryover, a changing workload, or a rare
tail absent from the observations. Launch repetitions describe this binary,
input, and environment; they do not measure variation across builds or machines.

## Calibration evidence and limits

The source-owned `calibration` Cargo benchmark simulates randomized complete
blocks and runs the production analyzer. It reports joint coverage, false
directions, detection, interval width, bias, and one-sided 95% Wilson binomial
bounds. The fixed held-out seed is `d91453c274b8109a`; each cell below contains
10,000 experiments and uses the full 20,000 resamples at each length.

| Model | Blocks | Joint coverage | False direction | Detect 10% reduction |
| --- | ---: | ---: | ---: | ---: |
| Four independent lognormal commands, log SD 0.1 | 100 | 99.85% | 0.15% | — |
| Candidate-specific AR(1), correlation 0.5 | 100 | 99.35% | 0.65% | — |
| Lognormal skew, log SD 0.7 | 100 | 99.80% | 0.20% | — |
| One command takes 10% less time, log SD 0.1 | 100 | 99.85% | 0.05% | 95.01% |
| Four independent lognormal commands | 200 | 98.56% | 1.44% | — |
| Unequal log SDs 0.03, 0.06, 0.09, 0.12 | 200 | 98.59% | 1.41% | — |
| Shared correlated multiplicative noise | 200 | 98.61% | 1.39% | — |
| Candidate-specific AR(1), correlation 0.5 | 200 | 97.80% | 2.20% | — |
| Lognormal skew, log SD 0.7 | 200 | 98.58% | 1.42% | — |
| One command takes 10% less time | 200 | 98.56% | 0.78% | 100.00% |
| Single-command arithmetic mean | 200 | 97.38% | Not applicable | — |
| Eight independent commands | 200 | 99.65% | 0.35% | — |
| Pilot-selected production count | 191.50 mean | 98.61% | 1.39% | — |

Every supported cell has a one-sided 95% lower coverage bound above 94% and
upper false-direction bound below 6%. These conservative intervals trade width
for protection against dependence and selection. The automatic cell includes
pilot costs and the production count rule; it is not a fixed-count proxy.

The limitations matter just as much:

- Strong AR(1) dependence (correlation 0.9) at 200 blocks covered only 88.36%.
  Strongly persistent conditions are outside the validated model.
- A one-in-1,000, 100-times-slower event affecting only one candidate covered
  only 17.56% at 200 blocks. No resampling procedure can infer a tail that the
  observations usually never encounter.
- Linear drift, periodic load, and carryover simulations do not establish
  stationarity. Their coverage concerns their explicit finite-run mean, not a
  promise about future conditions.
- All 10,000 cancelled-plan controls withheld intervals and conclusions.

The original cube-root, uncorrected proposal failed calibration, including an
independent row-by-row NumPy reference; it is not the implemented method.

Reproduce a cell from the repository root:

```sh
cargo bench -p fast --bench calibration -- 10000 200 ar 4 d91453c274b8109a
```

Arguments are experiment count, blocks (or automatic effort scale), model,
worker count, and hexadecimal seed. Run `--help` for model names. Simulations
are development tools; the installed `fast` binary has no simulation mode.

Aggregate counts and interval widths are retained in
[`calibration.csv`](../benches/calibration.csv); the deterministic model source
is [`calibration.rs`](../benches/calibration.rs). Coverage is a result for these
models, not a distribution-free guarantee.

## Observer perturbation

The `observer` example uses the production runner with paired randomized
observer-on/off invocations. The off arm retains command launch, wait accounting,
and diagnostic draining, but omits PMU attachment and task sampling. This
distinguishes observation cost from ordinary process-launch cost.

The four-event Vermeer policy and paced task sampler were measured on an AMD
Ryzen 9 5950X, Linux 7.2.8, nightly Rust `0abfedbc7`, release profile,
with inherited scheduling and background load:

| Workload | Blocks | On/off time ratio | Approximate 95% interval |
| --- | ---: | ---: | --- |
| Native launch fixture | 1,000 | 1.1265 | 1.0334–1.2279 |
| 1 ms sleep | 100 | 1.0289 | 0.7866–1.3458 |
| 10 ms sleep | 100 | 1.0713 | 0.9544–1.2024 |
| 100 ms sleep | 100 | 1.0153 | 0.9912–1.0400 |
| Ten million integer recurrence steps | 100 | 0.9583 | 0.7440–1.2344 |
| Eight threads, ten million steps each | 100 | 1.0408 | 0.7292–1.4856 |
| 1 MiB stderr | 100 | 0.9480 | 0.6336–1.4185 |
| Eight threads sleeping 500 ms | 100 | 1.0066 | 0.9986–1.0147 |
| 1,100 threads sleeping 200 ms | 100 | 1.0374 | 1.0290–1.0459 |

The launch fixture averaged 13.324 ms without observation and 15.009 ms with
observation on this loaded host. The 1,100-thread fixture paid about 9.0 ms more
with observation enabled. These are observed costs in this environment, not
fixed overhead constants. The remaining intervals include parity; several are
wide, and none establishes zero overhead. Short-command results include launch
and measurement cost, which is not subtracted.

All instruction and cycle readings in this matrix were nonzero and available.
The 1,100-thread runs each retained 16 task identities before command completion,
with the unfinished sweep marked incomplete. The median adaptive full-sweep
interval was 596 ms. This deliberately exercises bounded collection rather than
claiming complete visibility into all workers.

Reproduce the launch comparison:

```sh
cargo build -p fast --release --examples
target/release/examples/observer --runs 1000 -- target/release/examples/workload
```

For other rows, pass `--sleep-ms 1`, `--iterations 10000000`, `--threads 8`,
or `--stderr-bytes 1048576` to the workload as appropriate. The observer emits
all chronological observations and its analysis as JSON on stdout.

## Optimization checks

A public `fast` comparison measured paced and unpaced collectors in 100
randomized blocks, each running the 1,100-thread, 200 ms fixture once and emitting
JSON. Builds used the same compiler, source path, and target directory. The
paced/unpaced elapsed ratio was 0.9955, with interval 0.9898–1.0012: an elapsed
improvement was not established. Mean wait-accounted CPU time fell from
166.407 ms to 144.449 ms, an observed 13.2% reduction. Pacing is retained for
that reduction in work and its smaller initial scanning burst, at the cost of
less task detail for short-lived commands with many workers. Nested collectors
multiplexed the PMU events, so their instruction and cycle counts were
ineligible for comparison.

A matched release-build comparison used `fast` itself to measure 100 randomized
blocks of nested ten-run JSON experiments. Adding a 64 KiB JSON output buffer
gave a buffered/unbuffered elapsed ratio of 1.0031, with interval
0.9717–1.0355. The standard stdout writer remains: the extra buffer did not
establish a useful improvement.

Persistent disabled collector-side PMU events were also evaluated as an
alternative to per-child attachment. On Linux 7.2.8, a 2,000-invocation
repeated-fork check stopped receiving instruction and cycle counts after
invocation 235 and produced 3,528 zero readings. Fresh per-child attachment
produced no zero readings for the same commands. The collector therefore gives
each invocation fresh events rather than retaining cross-invocation counter
state.
