# Bench Harness

Contributor notes for measuring h2corn. The public
[benchmark page](../docs/benchmarks.md) contains checked-in charts from one
local run. The harness measures the current checkout or a focused code change.

| Tool | Purpose |
| --- | --- |
| `bench.py` | Cross-server scenarios, raw per-scenario JSON, and SVG charts |
| `compare.py` | Paired A/B comparison of two h2corn builds or configurations |
| `instr.py` | Paired `perf stat` instructions-per-request measurement |
| `_core.py` | Shared scenarios, server lifetime, load drivers, and statistics |
| `../tools/type_sizes.py` | Capture and compare Rust type and future layouts |

The HTTP cells need the external `oha` binary and the WebSocket cells need
`k6`; neither is installed by `uv sync`. The full suite needs Python 3.11 or
newer and Linux or macOS. Loopback needs no root privilege. The optional 50 ms
RTT profile additionally needs `unshare`, `ip`, and `tc` with an unprivileged
network namespace.

## Bounded Smoke

The bounded smoke run is short and non-publishing:

```bash
set -eu
uv sync --all-groups
command -v oha
output_dir="$(mktemp -d)"
uv run python bench/bench.py \
  --servers h2corn --types h1 --network-profile loopback \
  --duration 1s --warmup-duration 1s --max-trials 3 \
  --scenario-budget 15 --suite-budget 30 \
  --output-directory "$output_dir"
```

The smoke run checks startup, worker readiness, the exact response contract,
and teardown. It is not a performance result and cannot replace checked-in
charts.

## Cross-Server Run

```bash
command -v oha
command -v k6
uv run python bench/bench.py
```

The run stages full raw per-scenario JSON and SVGs under `results/runs/`.
Servers and load generators are not CPU-pinned;
the default comparison uses the same Starlette app, asyncio loop, access
logging, and aligned settings for every server.

`--publish` accepts a complete run only:

```bash
uv run python bench/bench.py --publish
```

It replaces the checked-in `results/plots/` charts and `results/raw/` records.
The harness publishes only when every scenario's winner is established; smoke,
filtered, and exploratory runs remain non-publishing. The docs build copies the
checked-in charts into the documentation site.

Selection and budget flags include `--types h1 h2`,
`--servers h2corn`, `--duration`, `--warmup-duration`, `--max-trials`,
`--scenario-budget`, and `--suite-budget`.

The default suite runs loopback scenarios and then tries selected streaming and
multiplexing scenarios through an isolated 50 ms RTT network namespace. If the
namespace, `tc`, or RTT check fails, that profile is skipped with its reason and
produces no shaped result; loopback numbers are not substituted.

## Measurement Rules

- Each trial verifies status, content type, body, and worker readiness before and after load.
- Reported values are medians of 3-9 cold starts; the observed range is retained.
- Sampling stops when the leader is separated from the runner-up and its interval reaches the 3% target, or when the time budget is reached.
- A competitor that cannot serve a scenario is excluded with its reason. An h2corn failure stops the run.
- Peak proportional set size (PSS) is sampled across the supervisor and workers.
- Access logging is enabled for every server, with server output sent to `/dev/null`.

Cross-server measurements use the same Starlette application for every server.
Server-only changes use the raw application so framework work does not hide a
small server-side difference.

## Paired A/B

Compare two builds or configurations with paired, rotation-balanced samples:

```bash
uv run python bench/compare.py \
  --control 'main=/path/to/main/.venv/bin/h2corn' \
  --candidate 'head=.venv/bin/h2corn' \
  --scenario h1 --workers 4

uv run python bench/compare.py \
  --control 'log=' --candidate 'nolog=--no-access-log' \
  --scenario h1
```

Use `--app bench.raw_app:app` for a minimal server-path measurement, or
`bench/ws_echo_app:app` for a minimal WebSocket path. Intervals that still span
zero are reported as inconclusive.

For changes where request rate is too noisy, measure worker instructions with
`perf stat`:

```bash
uv run --no-sync python bench/instr.py \
  --control 'main=/path/to/main/.venv/bin/h2corn' \
  --candidate 'head=.venv/bin/h2corn' \
  --workers 4 --requests 200000
```

This uses `bench/raw_app.py`, counts successful requests exactly, and reports
instructions, cycles, branch misses, cache misses, and IPC per request. It
requires Linux `perf` and `oha`.

## Other Focused Tools

Capture and compare Rust layout changes without mixing the capture with the
normal build cache:

```bash
.venv/bin/python tools/type_sizes.py capture /tmp/main.type-sizes
.venv/bin/python tools/type_sizes.py capture /tmp/head.type-sizes
.venv/bin/python tools/type_sizes.py diff /tmp/main.type-sizes /tmp/head.type-sizes
```

Use an ignored in-place Rust test for a single hot kernel when a change needs
instruction-level confirmation:

```bash
cargo test -p h2corn --release --lib bench_encode_field_bytes -- --ignored --nocapture
```
