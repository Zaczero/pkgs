# Bench harness

Contributor notes for measuring h2corn. The public
[Benchmarks](../docs/benchmarks.md) page shows cross-server plots; the tools
here are what you use when deciding whether a *code change* helped.

| Tool | Purpose |
| ---- | ------- |
| `bench.py` | Cross-server scenarios → raw JSON and the published SVGs |
| `compare.py` | Paired A/B of two h2corn builds or configurations |
| `instr.py` | Paired `perf stat` instructions/request for server-side changes |
| `_core.py` | Shared scenarios, server lifetime, load drivers, statistics |
| `../tools/type_sizes.py` | Zero-noise capture/diff of Rust type and future layouts |

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

**Memory is peak PSS.** The harness snapshots the proportional set size of the
supervisor and every worker at the start, once per second, and at the end of the
measured window; the chart carries the largest snapshot from all trials as
**peak memory (PSS)**. A
`smaps_rollup` read walks every mapping, so once per second keeps the observer
out of the request path while still giving a ten-second trial roughly eleven
high-water samples. PSS divides shared pages among their mappings, unlike RSS,
which would count one extension module in full in every worker.

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

The default suite stages its loopback primary profile and a separate 50 ms RTT
profile. The latter runs inside `unshare -rn`: it brings up loopback, installs
`tc qdisc replace dev lo root netem delay 25ms rate 1gbit`, then measures a
real TCP request/response RTT and refuses to report the shaped cells unless it
falls within 50 ±15 ms. If the namespace, `tc`, or RTT check fails, the profile
is skipped with the reason printed and written to its raw records; it never
falls back to unshaped loopback numbers. Only streaming POSTs, streaming
downloads, an 8 MiB HTTP/2 upload, and the multiplexed HTTP/2 case get this
profile. Small requests remain loopback-only because at 50 ms their rate mostly
measures generator concurrency divided by RTT.

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

Memory is **peak PSS**, not summed RSS. RSS counts every shared
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
uv run --no-sync python bench/instr.py \
  --control 'main=/path/to/main/.venv/bin/h2corn' \
  --candidate 'head=.venv/bin/h2corn'
```

### Instructions per request

`instr.py` is the counter-backed sibling of `compare.py`. It always serves
`bench/raw_app.py`, not Starlette, and attaches `perf stat` to the actual worker
PIDs after the common readiness check. Every measured sample is exactly
`--requests` successful responses from `oha -n`; warmup is also fixed-count and
does not enter the counters. The report divides worker-only user-space
instructions, cycles, branch misses, and cache misses by that exact denominator,
and derives IPC from the two totals.

```bash
uv run --no-sync python bench/instr.py \
  --control 'main=/path/to/main/.venv/bin/h2corn' \
  --candidate 'head=.venv/bin/h2corn' \
  --workers 4 --requests 200000
```

It uses the same cold-start A/B/A/B rotation and paired trimmed-mean bootstrap
interval as `compare.py`. Sampling stops only at scheduled checks after at least
four rounds, once the instructions/request interval reaches a ±0.3 percentage
point half-width; otherwise the time or round budget produces **INCONCLUSIVE
within the budget**. Lower instructions, cycles, branch misses, and cache misses
are better; higher IPC is better. The default `h1` scenario is the raw plaintext
GET. `h1_uds` and `h2` are available when the change belongs to those paths.

For a fast mechanical smoke that avoids TCP-port contention, use
`--scenario h1_uds --requests 100 --warmup-requests 10 --max-rounds 4`; it is
not evidence for a performance claim.

### Allocation counts

The extension statically links mimalloc without its statistics API exported, so
there is no outside process that can reset and read its counters. On this host,
however, `ltrace` can trace the local allocator symbols directly. Launch the
server under it (the console-script needs its interpreter), warm it first, then
drive a large exact-count raw-app run and count `mi_malloc_aligned` records from
that interval. `mi_free` is included to make ownership imbalance visible too.

```bash
ltrace -f -L -ttt -o /tmp/h2corn-alloc.trace \
  -x mi_malloc_aligned -x mi_free \
  .venv/bin/python .venv/bin/h2corn bench.raw_app:app -b 127.0.0.1:8000 -w 1

env -u NO_COLOR oha -n 100000 -c 100 --output-format json \
  --http-version 1.1 http://127.0.0.1:8000/
```

The trace is intentionally an allocation oracle, not a throughput benchmark:
the tracing overhead is large. Keep the trace timestamps around the fixed `oha`
run, count the matching call entries in that interval, and divide by 100,000.
Run the control and candidate independently with the same request count; an
integer allocation removed from a request path is not subject to host-load
noise.

### Type-size oracle

`-Zprint-type-sizes` is likewise independent of host load. Capture each checkout
with the helper, then diff the captures:

```bash
.venv/bin/python tools/type_sizes.py capture /tmp/main.type-sizes
.venv/bin/python tools/type_sizes.py capture /tmp/head.type-sizes
.venv/bin/python tools/type_sizes.py diff /tmp/main.type-sizes /tmp/head.type-sizes
```

The capture command runs `RUSTFLAGS="-Zprint-type-sizes" cargo build --release
--lib` with `CARGO_TARGET_DIR=target/type-sizes`, separate from the normal build
cache. It prints every changed type layout and always lists async state machines
and futures as well, so a smaller struct cannot hide a larger owning future.

### Assembly

Use the package name `h2corn` and the library crate name `_lib` when inspecting
a named function. This is the x86-64 invocation:

```bash
env -u RUSTC_WRAPPER cargo asm -p h2corn --lib \
  _lib::runtime::start_app_call
```

### ARM assembly

h2corn ships Apple Silicon and ARM Linux wheels, and x86 hides two things that
matter: its cache line is 64 bytes where Apple Silicon's is 128, and its memory
model is strong enough to mask an under-ordered atomic that aarch64 exposes.
Read the ARM code before concluding anything about cache-line packing or
memory ordering.

**The whole crate does not cross-compile on this host**, and the blocker is not
Rust: `aws-lc-sys` (rustls's crypto backend) hands ARM `.S` files to the host
x86 `gcc`, which needs a cross C toolchain we do not have. Adding
`--target aarch64-unknown-linux-gnu` to the invocation above therefore fails.

It is also unnecessary. Every kernel where ARM codegen is in question —
`websocket::codec::mask`, `http::header_value`, `hpack::huffman` — is pure
`std`, with no C and no PyO3. Build just those, out of tree:

```bash
D=/tmp/h2corn-armkernel H=$PWD/..
mkdir -p "$D/src"
printf '[toolchain]\nchannel = "nightly"\n' > "$D/rust-toolchain.toml"
cat > "$D/Cargo.toml" <<'EOF'
[package]
name = "armkernel"
version = "0.0.0"
edition = "2024"
[profile.release]
lto = false          # fat LTO leaves an rlib holding only bitcode
codegen-units = 1
EOF
cat > "$D/src/lib.rs" <<EOF
#![feature(portable_simd)]
#![allow(dead_code, unused)]
pub mod codec {
    #[path = "$H/src/websocket/codec/mask.rs"]
    pub mod mask;
}
#[path = "$H/src/http/header_value.rs"]
pub mod header_value;

// A codegen anchor per kernel: without \`no_mangle\` + \`extern "C"\` an unused
// pub fn is never emitted and \`cargo asm\` reports no functions.
#[unsafe(no_mangle)]
pub extern "C" fn anchor_header_value_is_valid(p: *const u8, n: usize) -> bool {
    header_value::header_value_is_valid(unsafe { core::slice::from_raw_parts(p, n) })
}
EOF
cd "$D" && env -u RUSTC_WRAPPER CARGO_TARGET_DIR="$D/target" \
  cargo asm --lib --target aarch64-unknown-linux-gnu --simplify \
  anchor_header_value_is_valid
```

Three details are load-bearing and each one silently produces an empty or
failed result if missed: the toolchain file (the *default* toolchain has no
aarch64 `std`, only the pinned nightly does), `lto = false`, and the anchor.

Read the vector width off the result. `ldp q5, q4, [x0], #32` means the
`u8x32` was split into two 128-bit NEON registers — the same paired-128-bit
shape it takes under `x86-64-v2`, which is worth knowing before retuning a lane
count for one architecture.

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
