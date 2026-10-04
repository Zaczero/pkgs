# fast

`fast` compares Linux commands in randomized order and prints timing, resource
measurements, and the evidence behind the comparison. It is a stateless command
benchmark: results go to stdout, diagnostics go to stderr, and saving belongs to
your shell.

## Install

Install from this workspace with its nightly Rust toolchain:

```sh
cargo install --locked --path fast
```

Linux is required. The initial development target is x86-64 Linux; hardware
counters are optional and permission failures do not prevent command timing.
No Python interpreter, daemon, configuration, writable home directory, or network
connection is needed to run the binary. After dependencies have been fetched,
Cargo accepts `--offline` for builds.

Publication and licensing are intentionally undecided.

## Compare commands

```sh
fast gzip -c input.dat ::: zstd -c input.dat
fast --input data.json zstd ::: gzip
fast ./program --threads 4 'file with spaces'
fast --once ./expensive-job
fast --runs 100 ./old input.dat ::: ./new input.dat
fast --time 2m ./old input.dat ::: ./new input.dat
fast --json ./old input.dat ::: ./new input.dat > result.json
```

Replace the example programs and input files with your workload. Programs run
directly, with their arguments passed unchanged. A standalone `:::` separates
commands. Quote individual arguments when your shell needs it, such as a filename
containing spaces; do not quote the entire command.

Put `fast` options before the first program. After that point, flags such as
`--help`, `--input`, and `--` belong to the command. An optional initial `--`
explicitly ends `fast` options:

```sh
fast --runs 100 -- ./old --threads 4 ::: ./new --threads 4
```

The command separator still applies after that initial `--`. If a command needs
a literal `:::` argument, choose another marker:

```sh
fast --separator +++ printf '%s' ::: +++ printf '%s' other
```

Only a complete argument matching the marker is special; `name:::suffix` is
ordinary data. Quoting `:::` does not escape it because the shell removes those
quotes before calling `fast`. Empty commands and empty separators are errors.
To use pipelines, redirections, shell expansion, or shell builtins, explicitly
invoke the shell, for example `sh -c 'producer | consumer'`. Shell startup is then
part of that command's timing.

Use ordinary command wrappers for other execution settings, such as environment
variables:

```sh
fast env MODE=old ./program ::: env MODE=new ./program
```

Commands run sequentially, with their internal parallelism intact. Each run
executes every command once in randomized order. The first command is the
displayed baseline. `--runs` measures an exact number of runs; `--once` runs each
command exactly once and cannot include warmup.

By default `fast` plans about 30 seconds. Pilot runs measure cost and run-to-run
variation, then the run count is frozen; the pilot runs count toward the result.
Cheap commands get 100 runs, or more when the time allows and ±1% precision needs
them. Slow commands get at least 3 runs, even when that exceeds 30 seconds.
When one run costs at most 5% of the target, the first run is discarded as a
warmup, because cold caches and lazy loading make it unrepresentative.
`--warmup N` replaces that with N explicit warmup runs, and `--warmup 0` keeps
every run.

`--time` sets a strict budget instead: after the first run, no run starts unless
the plan expects it to fit. `--timeout` terminates a single run that exceeds its
deadline. Ctrl-C stops early and still reports the completed runs and their
comparisons. None of these decisions looks at which command is winning.

## Input and correctness

Command stdin and stdout default to `/dev/null`. `--input FILE` reopens an existing
regular file at offset zero for every invocation. Incoming pipes are not spooled
or consumed as workload input. To measure a pipeline, include it in the expression:

```sh
fast sh -c 'producer | consumer'
fast --input input.dat ./old ::: ./new
```

Successful exit status is the default correctness check, not proof that outputs
are equivalent. Use a verifier or a wrapper script when correctness matters:

```sh
fast --runs 40 \
  --prepare 'rm -f output.dat' \
  --verify 'cmp expected.dat output.dat' \
  sh -c './old input.dat > output.dat' ::: \
  sh -c './new input.dat > output.dat'
```

Both hooks apply to every invocation, including pilots and requested warmups.
Hooks run outside the command timer, but their cost enters effort planning.
They receive `FAST_COMMAND_INDEX` (one-based), `FAST_COMMAND_ID` (`A`, `B`, …),
`FAST_PHASE` (`warmup`, `pilot`, `production`), and `FAST_RUN_INDEX` (one-based,
counting every run in order). Hook timeout is 30 seconds unless `--timeout` supplies another
value. A command or verifier failure aborts the experiment; no replacement runs
or discarded slow successes improve the result.

Repeated execution does not mean cold caches or guaranteed steady state. `fast`
does not change affinity, priorities, governors, cgroups, global caches, or other
work running on the machine.

## Read the result

Elapsed time runs from immediately before process creation through the parent's
observation of the initial command's exit. It includes launch, exec, scheduling,
and completion notification. Diagnostic draining after completion is excluded.
Very short commands can mostly measure process launch.

Timing changes compare **arithmetic means**, not fastest runs, medians, or means
of per-run ratios. Comparisons use paired runs to estimate ratios of mean times.
Their intervals target 95% simultaneous coverage under the method's assumptions,
including every command pair even when the compact report shows only comparisons
with A.
A smaller point estimate alone does not establish a win, and unresolved does not
mean equivalent.

Two runs can already bound an interval; three is the default minimum. More runs
usually improve precision. Grouping consecutive runs into batches reduces the
effect of short-lived correlation, but does not eliminate it. A diagnostic can
flag drift or correlation once there are at least 20 runs; an absent flag does
not establish independence. A single run shows its time and resources without
an interval.

`--margin 1%` asks about practical differences outside a symmetric one-percent
margin. `--check 1%` asks whether every alternative can be established as no
more than one percent slower than A. A check can pass, fail, or remain
inconclusive.

The statistical method and its calibration evidence are documented in
[Measurement and inference](docs/measurement.md). Strongly persistent noise is
outside this method's validated model. Rare slow events that the runs never
encounter can also make the result misleading.

### Resources and sampled activity

- CPU time is Linux `wait4` user/system accounting. It can include descendants
  waited by the command.
- Maximum RSS is the Linux wait-accounted high-water quantity. It is **not**
  simultaneous whole-process-tree memory, nor a sum of per-process peaks.
  The report pairs it with the child's launch-time high-water mark: inherited
  benchmark memory can set a floor before the command executes. A peak near
  that floor does not identify the command's own memory requirement.
- Instructions, cycles, and branch misses count user-space events for the root
  and inherited descendants, starting on root exec. Missing or poorly scheduled counters are
  unavailable, not zero.
- Instructions describe executed CPU work, not elapsed performance. The report
  shows their change versus A and run-to-run variation separately from timing.
  Instructions per cycle and branch misses per 1,000 instructions describe
  execution behavior; neither is a universal efficiency score.
- On supported Zen 3 Vermeer CPUs, core-to-L2 request misses add a cache signal.
  The label names L2 explicitly; this is not a generic cache-hit score or a
  measurement of time lost to memory.
- `--details` shows the same measurements with more explanation, including
  sampled task activity from the first measured invocation of each command.
  Large scans are paced in small chunks; unfinished scans are marked incomplete.
  Sampling can miss short-lived processes and threads. Thread CPU percentages
  use 100% for one logical CPU; threads do not have independent RSS.

No additional command runs when `--details` is requested. Newly surviving
background descendants stop further repetitions. Put persistent services outside
the measured command, or wait for the work in a synchronous wrapper.

## Output and automation

Human text and `--json` are alternative stdout formats. Redirection and `tee`
save them without application-managed files:

```sh
fast ./old ::: ./new | tee comparison.txt
fast --details ./old ::: ./new > detailed.txt
```

`--color` controls styling; `auto` follows `NO_COLOR`, `FORCE_COLOR`, `CLICOLOR`
and `CLICOLOR_FORCE`. JSON never contains ANSI. `--quiet` suppresses progress.

The report lists each command as a shell would read it back: arguments are quoted
only where needed, and control characters appear as `$'...'` escapes. Each
command's timeline groups consecutive runs and compares them with its mean, so
cold starts, drift and outliers are visible; a flat line is a steady command.
Verdicts state a factor beyond 1.5× and a percentage of time below that, with the
95% interval. `--details` adds every command pair, medians and ranges, the plan,
and the assumptions behind the intervals.

JSON schema 2 records each candidate as its exact argument vector and uses tagged
UTF-8 or byte-array strings for Unix arguments. Explicit shell commands include
the shell executable and `-c` in that vector. Elapsed
and CPU nanoseconds and PMU counters are decimal strings to avoid JavaScript
integer rounding. Missing measurements are `null` with availability metadata.
Readers should ignore unfamiliar fields. Raw invocations preserve phase,
chronological run index, order, failures, and partial completion. `plan` records
the planning mode, warmup, pilot and discarded runs, and the planned run count;
the analysis uses the kept pilot runs and the production runs.
For a single command, the interval describes its mean elapsed nanoseconds;
the comparison denominator is `null` and its decision is `not_applicable`.
There is no import or reopen interface.

| Status | Meaning |
| --- | --- |
| 0 | Measurement completed, possibly unresolved; or check passed |
| 1 | Workload, verifier, timeout, or surviving-descendant failure |
| 2 | Invalid arguments or workload specification |
| 3 | Collector or output failure |
| 4 | Nonregression check failed |
| 5 | Nonregression check inconclusive |
| 128 + signal | Interrupted, after owned-process cleanup; completed runs are still analyzed |
| 141 | Output consumer closed its pipe |

`fast --help` is the canonical option reference. Bash, Zsh, and Fish completions
and `fast.1` are generated from the same parser into Cargo's build output.
`cargo install` installs the binary; installing those generated shell and manual
files is a separate, optional step.

## Development and support

Use `cargo nextest run -p fast`, `cargo check -p fast --all-targets`, and
`cargo clippy -p fast --all-targets` in this workspace. Tests exercise the built
binary and real processes. Statistical calibration and perturbation measurements
are separate experiments, described with their evidence in the measurement page.

When reporting a problem to the repository maintainer, include `fast --version`,
the command and flags, kernel/architecture, the observed result, and a small
reproducer. Reports and captured diagnostics may contain command arguments or
workload secrets; inspect them before sharing.
