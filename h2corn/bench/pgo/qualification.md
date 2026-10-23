# Local PGO qualification — October 23, 2026

PGO reduced worker instructions across all 16 existing raw HTTP scenarios and
improved several framework and gRPC throughput measurements. The held-out
FastAPI application showed a smaller benefit, with inconclusive HTTP/1
throughput. This is evidence for enabling the selected distribution build, not
a claim that every application or latency percentile improves.

The local build, collection, profile-use, and installed-wheel paths were
exercised. The actual hosted manylinux/cibuildwheel execution remains unverified;
this qualification did not push a branch or publish packages.

## Build and measurement conditions

- CPython 3.15.0 with the GIL; Rust `1a2c4797ef2570bd76ca5dee34449169bc7dc457`,
  LLVM 23.1.4; `release-dist`, explicit `x86_64-unknown-linux-gnu`, x86-64-v2.
- AMD Ryzen 9 5950X, Linux 7.2.9, shared loaded host, no CPU pinning.
- Baseline and PGO include the same application-failure publication fix.
  Its correctness benefit is not attributed to PGO.
- One worker, standard asyncio, two native runtime threads, and no access
  logging unless a row specifies four workers. Training separately covers
  uvloop, multiple workers, TLS, and enabled logging.
- Fresh server processes, alternating baseline/candidate order. Results below
  use all observations: the geometric mean of paired candidate/baseline ratios.
  Nominal 95% intervals resample adjacent blocks of two pairs, 10,000 times.
  They cannot account for persistent host drift.

Two independently collected profiles of the same finite corpus were measured:

| Artifact | SHA-256 |
| --- | --- |
| Baseline wheel | `b477514b5cb73986d2ee9ff60a0df13d49682c04fb9dd62ce4ce2bc6436b1640` |
| Broad-comparison profile | `429f98a48a82745e1f8aff9b585385c8c4027e43f8cd45b62721afa869fce397` |
| Broad-comparison wheel | `921dd27a4aae9facc50998fffbca098b3214c6e56e15dbef5b4b1b65fc2cb71d` |
| Final bound profile | `498dd9669eb5bb597903fec3dec7cbddda4bfc79126b0a1930f136a5247027fa` |
| Final wheel | `eeff4dd403b6ac98716600395de56b600e5e5a994e881da8b8b5164a564fee69` |

The broad campaign uses the first profile. The final wheel uses a fresh
collection with the complete source/compiler/target/Python binding and is
checked separately below. CI collects a fresh profile for each selected build.

## Existing raw HTTP scenarios

Worker user-space instructions per completed request, broad-comparison wheel:

| Scenario | Change |
| --- | ---: |
| HTTP/1 | −7.7% |
| HTTP/1 Unix socket | −6.4% |
| HTTP/2 | −10.6% |
| HTTP/1 download | −4.5% |
| HTTP/2 download | −5.0% |
| HTTP/1 paced streaming | −3.5% |
| HTTP/2 paced streaming | −6.5% |
| HTTP/1 file | −3.2% |
| HTTP/2 file | −5.7% |
| HTTP/2 large upload | −7.6% |
| HTTP/1 fast upload | −5.6% |
| HTTP/2 fast upload | −9.5% |
| HTTP/1 streamed response | −9.2% |
| HTTP/2 streamed response | −9.8% |
| HTTP/1 content-length response | −9.3% |
| HTTP/2 content-length response | −9.6% |

HTTP/1 and HTTP/2 use 12 pairs; the other scenarios use six. Most cycle
measurements improved, but the first large-upload comparison measured +4.1%
cycles despite fewer instructions. A 12-pair final-wheel follow-up measured
−7.0% instructions and −4.0% cycles, with a cycle interval of −7.4% to −0.5%.
Its same-wheel control measured +0.3% cycles, interval −5.5% to +7.5%.
The observations do not establish a repeatable upload-cycle regression.

The final wheel independently reproduced the small-response instruction gains:
**−7.8% for HTTP/1 and −10.8% for HTTP/2**, six pairs each.

## Framework, WebSocket, and gRPC throughput

Starlette HTTP/1 measured +10.1% with one worker and +10.3% with four.
Four-worker HTTP/2 measured +8.2%.

Single-worker HTTP/2 timing was inconsistent: the initial comparison measured
−7.7%, while a later 12-pair comparison measured +8.9%. Crucially, the
same-wheel control measured −8.6%, demonstrating drift large enough to
invalidate a simple throughput attribution. The corresponding framework
counter comparison measured approximately −5% instructions and −13% cycles.

The initial WebSocket run was also highly variable. The larger comparison
measured +6.4% throughput, nominal interval +4.6% to +8.9%; its same-wheel
control was −2.6%, interval −11.8% to +4.7%. Retain that control uncertainty
when interpreting the positive comparison.

The existing Connect gRPC client and service workloads completed six pairs
per case, with four-second measurements after one-second warmup. Both arms
used the same installed RPC dependencies. Responses, metadata, message
counts, worker identities, and handler settlement were checked.

| gRPC case | Throughput change | Nominal 95% interval |
| --- | ---: | ---: |
| Zero-payload unary | +7.9% | +0.1% to +15.7% |
| 32-byte unary | +4.6% | +2.5% to +8.7% |
| 64 KiB unary | +5.1% | −1.0% to +13.0% |
| Upload | −1.4% | −5.6% to +1.7% |
| Download | +19.8% | −1.5% to +67.5% |
| Duplex | +7.1% | +4.0% to +9.3% |
| TLS unary | +3.1% | −2.6% to +10.0% |
| TLS duplex | +7.0% | −1.8% to +14.2% |

Several cases remain inconclusive; in particular, the large download point
estimate is not a reliable speedup claim.

## Held-out FastAPI and startup

The final wheel was evaluated on a FastAPI application absent from training:
a typed JSON response with an asynchronous dependency. Each protocol used six
pairs, with 100,000 requests per counter sample and five-second throughput
windows after warmup.

| Metric | HTTP/1 | HTTP/2 |
| --- | ---: | ---: |
| Worker instructions/request | +2.2% | −1.2% |
| Worker cycles/request | −1.7% | −6.2% |
| Throughput | +1.3% | +2.5% |
| Throughput nominal interval | −4.5% to +9.4% | +0.5% to +4.7% |

HTTP/1 is not a generalization win on every metric: its instruction count
increased, while throughput and cycles were inconclusive.

Across 20 paired CLI launches of that application, mean time to the
supervisor's readiness notification was 248 ms for baseline and 237 ms for
PGO. This run showed no startup penalty.

### Arrival-rate latency

The FastAPI application was also driven with k6's constant-arrival-rate
executor, six paired eight-second windows per rate. At 10,000 and 25,000
scheduled requests/second, the end-to-end test could not sustain the offered
rate and dropped scheduled iterations. All issued responses passed status and
body checks, but their conditional latency percentiles do not describe every
offered request.

A lower-rate 3,000 requests/second comparison and same-wheel control were
therefore retained. The PGO comparison's per-run p99 change was −0.7%, nominal
interval −36.3% to +40.1%; p99.9 was +32.7%, interval −20.9% to +72.4%.
The same-wheel control itself reported a spurious −63% p99 change. One
same-wheel window dropped 615 scheduled iterations, and one PGO window dropped
52; the other windows dropped none.

**Tail-latency effects remain inconclusive on this host.** These results do not
support either a tail improvement claim or a consistent PGO tail regression.
The dropped arrivals and slow observations remain in the evidence.

## Footprint and correctness

| Artifact size | Baseline | Final PGO |
| --- | ---: | ---: |
| Compressed Linux wheel | 3,089,543 B | 3,433,062 B |
| Native extension | 7,920,800 B | 8,207,560 B |
| Executable `.text` | 5,197,010 B | 5,401,170 B |

The final wheel adds about 204 KB of executable code and 344 KB of compressed
wheel size. Its 13 Python/stub members exactly match the baseline. Collection
exports, the supervisor overlay, and LLVM instrumentation sections are absent.

The exact final installation passed **2,350 tests**, with 46 explicit
platform/runtime skips, plus the embedded application check. The separate
benchmark-tooling selection passed 82 tests. Profile-admission tests reject
changed digests, compilers, source/dependencies, targets, Python builds,
overriding environment flags, and inconsistent collection plans.
Whole-package Pyright and the affected Ruff, actionlint, and zizmor checks
completed cleanly.

## Evidence

The local evidence root is `/tmp/opencode/h2corn-pgo`:

- `fixed-baseline`, `fixed-use`, `final-generate`, `final-use`: exact wheels
  and build receipts/logs; `final-use/tests` contains the final wheel test
  report and installed-byte inventory.
- `fixed-training-v2`, `final-collection`: finite plans, source snapshots,
  dependency versions, worker receipts, raw profiles, and merge diagnostics.
- `full-*.json`, `framework-*.json`, `grpc-comparison-2`: broad raw paired
  observations and service/client evidence.
- `heldout`: final-wheel counter, throughput, arrival-load, and startup
  observations, including wheel hashes in `artifacts.json`.
- `arrival-controls`: lower-rate baseline/baseline and baseline/PGO arrival
  observations, with scheduled-iteration drops and per-run tail percentiles.
- `summary.json`, `heldout/summary.json`, `final-footprint.json`:
  untrimmed summaries and artifact sizes.

Indexed profiles remain under `h2corn/.pgo/<digest>/profile.profdata` in the
qualification worktree. The release workflow retains equivalent inputs and
evidence in Actions artifacts using default retention; no profile binary is
committed to source control.
