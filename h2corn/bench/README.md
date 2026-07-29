# Bench harness

Contributor notes for measuring h2corn. The public
[Benchmarks](../docs/benchmarks.md) page shows cross-server plots; the tools
here are what you use when deciding whether a *code change* helped.

| Tool | Purpose |
| ---- | ------- |
| `bench.py` | Cross-server scenarios → raw JSON and the published SVGs |
| `compare.py` | Paired A/B of two h2corn builds or configurations |
| `_core.py` | Shared scenarios, server lifetime, load drivers, statistics |

Raw runs land under `results/runs/` and are gitignored. Canonical SVGs are
checked in; `bench.py` replaces them only with an explicit `--publish` run.

## Methodology

**Nothing is pinned.** Servers and load generators run the way a deployment
runs them, on the whole machine, with whatever parallelism they ship. Do not
add CPU affinity here: pinning the harness pins its children too — that is
exactly how a past revision came to benchmark every server on a single core —
and confining a server to a fraction of the box measures the confinement, not
the server.

**Noise is handled in the statistics, not by demanding a quiet host.** Trials
are rotation-balanced cold starts; the reported value is a median with the
observed range as its whisker; A/B comparisons are *paired* per round with
alternating order, so slow host drift cancels instead of registering as a code
change.

**Sampling is time-dynamic.** Every scenario keeps running trials until the
claim it publishes is resolved, then stops. For a paired A/B that is the
delta's confidence interval (1 % half-width). For a cross-server plot it is
what the chart is read for: the **leader is ahead of the runner-up**, and the
leader's own interval is tight (3 %).

Two things are deliberately not required, because neither is reachable and
neither is the claim. *Every server reaching the same precision*: the slowest
is the noisiest in relative terms, so a scenario burns its whole budget without
the published claim improving — one run measured hypercorn at ±20 % after nine
trials while h2corn's number had been settled since the third. *Every adjacent
pair separating*: two rivals can be genuinely tied, and then no amount of
sampling separates them. In the published run gunicorn and uvicorn tie for
second in one cell, uvicorn and hypercorn tie for third in another, and h2corn
clears the field in both.

Trial duration is never shortened to fit a budget. Publication is refused
outright if any cell's **winner** is not established — that would make the
chart wrong, not merely imprecise. A cell whose leader interval is wider than
the 3 % target still publishes, with the shortfall printed and written onto the
plot next to the whiskers that show it, because an approximate bar height is
honest as long as it says so. In the 21-scenario run behind the current plots,
the winner is established everywhere and 13 cells carry that note.

**Every measured cell proves itself.** Before warmup and again after
measurement, the exact status, content type and response body are verified over
the real protocol (HTTP/1, HTTP/2, or a WebSocket echo), and every configured
worker must answer before a trial counts. A cell that cannot prove itself
raises instead of reporting.

If a *competitor* cannot serve a workload — connection errors under HTTP/2
multiplexing, say — it is excluded from that scenario with the reason printed
and recorded, and the rest of the comparison still publishes. If **h2corn**
fails a cell the whole run stops: that is our bug, not a fact about someone
else's server.

**Access logging is on for every server.** That is how these servers are
deployed, and log construction is part of the work they do. Server output goes
to `/dev/null`, so no server pays for a log sink the others avoid — never point
several workers at one shared regular file, which serializes all of them on one
open file description.

## Cross-server publication

```bash
uv run python bench/bench.py            # stage under results/runs/
uv run python bench/bench.py --publish  # also replace the canonical plots
```

`--publish` requires the complete server and scenario suite. The docs site
picks the plots up from `results/plots/` at build time.

Useful flags: `--types h1 h2` to narrow a run, `--servers h2corn` for a quick
smoke, `--duration` / `--warmup-duration`, `--max-trials`, and the
`--scenario-budget` / `--suite-budget` ceilings.

## Paired A/B

```bash
# two builds
uv run python bench/compare.py \
  --control 'main=/path/to/main/.venv/bin/h2corn' \
  --candidate 'head=.venv/bin/h2corn' \
  --scenario h1 --workers 4

# one build, two configurations
uv run python bench/compare.py \
  --control 'log=' --candidate 'nolog=--no-access-log' --scenario h1
```

The result reports the paired delta, its 95 % confidence interval, whether that
interval excludes zero, and the memory both sides cost. An interval that still
spans zero when the budget runs out is reported as inconclusive — that is an
answer, not a failure.

Memory is **PSS**, not summed peak RSS. Peak RSS counts every shared
file-backed page in full in each process that maps it, so four workers sharing
one extension module report it four times, and two builds whose code merely
pages in differently look megabytes apart while costing the same memory.

### Isolating the server from the framework

The published plots run Starlette, because that is what people deploy and it is
identical for every server. But it dominates the measurement: on this host a
plaintext GET costs **~111k instructions through Starlette and ~34k through
`bench/raw_app.py`**, so about two thirds of the published number is framework.
When measuring a change to the server itself, run against the raw app —
otherwise a 1 % server win shows up as 0.3 % and can vanish into the noise:

```bash
uv run python bench/compare.py --control 'a=…' --candidate 'b=…' \
  --app bench.raw_app:app --scenario h1
```

`bench/ws_echo_app.py` plays the same role for WebSockets: a bare echo, so a
measurement sees the frame path rather than a framework's message handling.

RPS variance on a shared host cannot resolve sub-percent changes. For those,
measure the mechanism directly:

```bash
perf stat -p "$(pgrep -f 'h2corn bench.bench_app' | head -1)" -e instructions,cycles
strace -c -f -p <worker-pid>          # syscalls per request
cargo rustc --release --lib -- -Zprint-type-sizes
cargo asm --lib <path::to::function>  # confirm what the compiler emitted
```

## Kernel microbenchmarks

A single hot kernel is measured in place, as an `#[ignore]`d test next to the
code it measures — it gets full access to crate internals, needs no build
machinery, and never ships (it lives behind `#[cfg(test)]`):

```rust
#[test]
#[ignore = "microbenchmark, run explicitly"]
fn bench_encode_field_bytes() { /* Instant + black_box over a fixed workload */ }
```

```bash
cargo test -p h2corn --release --lib bench_encode_field_bytes -- --ignored --nocapture
```

Add one when you are about to change a kernel, keeping the old implementation
beside the new one for the duration so the run reports both. Delete it again
once the change has landed and the mechanism is recorded.
