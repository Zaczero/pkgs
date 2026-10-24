# PGO qualification

The [48-workload corpus](README.md) reduces worker instructions for small raw
HTTP responses by **7.2% over HTTP/1 and 10.3% over HTTP/2** on the measured
host. Benefits are smaller for the held-out FastAPI application. Startup is
essentially unchanged; arrival-rate tail latency remains inconclusive.

These measurements compare otherwise matched ordinary and profile-guided
`release-dist` wheels. They support the selected optimization, not a universal
application speedup or a tail-latency guarantee. The
[cross-server benchmarks](../../docs/benchmarks.md) measure the final PGO wheel
against other server stacks, which is a different comparison.

## Build and workload

- CPython 3.15.0 with the GIL; Rust
  `1a2c4797ef2570bd76ca5dee34449169bc7dc457`, LLVM 23.1.4.
- `release-dist`, explicit `x86_64-unknown-linux-gnu`, x86-64-v2, identical
  non-PGO compilation inputs.
- AMD Ryzen 9 5950X, Linux 7.2.9, shared loaded host, no CPU pinning.
- One worker, standard asyncio, two native runtime threads, no access logging.
  The training corpus separately includes multiple workers, uvloop, logging,
  proxy deployments, direct TLS, mutual TLS, WebSockets, transfers, and gRPC.
- Fresh processes and alternating baseline/candidate order. Raw HTTP uses
  50,000 requests per counter sample after 5,000 warmup requests. Held-out
  FastAPI uses 20,000 after 2,000, plus two-second throughput windows after
  0.25-second warmup. Each comparison has six paired samples.

The FastAPI application returns typed JSON through an asynchronous dependency
and is absent from training. All measured responses are checked.

| Artifact | SHA-256 |
| --- | --- |
| Ordinary wheel | `ffce35c1e78fa0e0d1d1960ebd623a53690465861a319d0b7d9f53675b7c1843` |
| Accepted profile | `ed786709d8ca264aa92afa2517c6a561195e33a94a2a8df99e7a5202ecf9322c` |
| PGO wheel | `4ac63268091932f888c92dd9d8b8c312ce7f3387d86c84e4790229dbd2142e83` |
| PGO native extension | `030572c52a62e314b52a5ad8aba31d0e50aed47d6f32c2a6968ac3d1b383985a` |

## Work and throughput

Changes below use every observation: the geometric mean of paired
candidate/baseline ratios. Nominal 95% intervals resample adjacent blocks of two
pairs 10,000 times. Six pairs provide limited independent information, and these
intervals do not account for persistent host drift.

| Workload and metric | Change | Nominal 95% interval |
| --- | ---: | ---: |
| Raw HTTP/1 instructions/request | −7.20% | −7.50% to −7.02% |
| Raw HTTP/2 instructions/request | −10.30% | −10.43% to −10.12% |
| FastAPI HTTP/1 instructions/request | −0.99% | −2.53% to +0.55% |
| FastAPI HTTP/2 instructions/request | −1.16% | −1.20% to −1.13% |
| FastAPI HTTP/1 cycles/request | −6.75% | −7.56% to −5.55% |
| FastAPI HTTP/2 cycles/request | −2.31% | −5.27% to +2.70% |
| FastAPI HTTP/1 throughput | +3.86% | +1.92% to +6.36% |
| FastAPI HTTP/2 throughput | +8.93% | +6.20% to +14.17% |

Same-wheel throughput controls measured −0.52% for HTTP/1 and −1.50% for
HTTP/2. The HTTP/2 control interval excluded zero despite identical code,
demonstrating residual variation that the nominal intervals do not fully
capture. The instruction reductions are stronger evidence than a precise
throughput attribution on this host.

## Startup and arrival-rate latency

Ten paired CLI launches of the FastAPI application averaged **247.0 ms**
to supervisor readiness for the ordinary wheel and **246.7 ms** for PGO.
The paired change was −0.13%, with a nominal interval of −1.69% to +2.15%.

Arrival tests schedule 3,000 requests/second in eight-second k6 windows after
warmup. A six-pair series and a twelve-block series are retained; each block in
the latter interleaves a same-wheel control and a baseline/PGO pair, rotating
both comparison order and arm order.

| Series | PGO p99 change | PGO p99.9 change | Dropped arrivals, baseline / PGO |
| --- | ---: | ---: | ---: |
| Six pairs | +100.8% (−6.5% to +229.6%) | +193.6% (+70.8% to +323.4%) | 8 / 1,146 |
| Twelve interleaved blocks | +97.2% (−7.2% to +335.1%) | +39.8% (−35.6% to +213.4%) | 968 / 2,828 |

The unfavorable observations are not discarded. Same-wheel controls also
varied substantially: the six-pair control's p99 changed −78.1%, while the
interleaved control changed +105.4% for p99 and +67.8% for p99.9, with intervals
crossing zero in the interleaved series. Its two identical-wheel arms dropped
2,455 and 3,734 arrivals.

**The measurements do not establish tail equivalence, improvement, or a
repeatable PGO regression.** All issued responses passed validation, but dropped
arrivals mean the conditional latency percentiles do not describe every
scheduled request. These results cannot support a production tail-latency claim.

## Footprint and correctness

| Artifact size | Ordinary | PGO |
| --- | ---: | ---: |
| Compressed Linux wheel | 3,105,411 B | 3,629,775 B |
| Native extension | 7,913,320 B | 8,506,048 B |
| Executable `.text` | 5,191,890 B | 5,672,082 B |

PGO adds 480,192 bytes of executable code and 524,364 bytes to the compressed
wheel. All 13 Python/stub members match the ordinary wheel. Collection exports,
the collection-only supervisor overlay, and LLVM instrumentation sections are
absent from the final installation.

The exact PGO installation passed 2,350 runtime tests, with 46 explicit
platform/free-threaded skips, and the embedded application request check.
The local build, profile admission, collection, and installed-wheel paths were
exercised. Hosted manylinux/cibuildwheel execution is not covered by this evidence.

## Evidence

[Qualification data](qualification.json) contains raw paired observations,
arrival distributions and dropped counts from both series, execution order,
artifact identities, profile weights and compiler identity, workload sources,
and the evaluation and summary drivers. No observations are trimmed.
The local build logs, wheels, raw profiles, and test reports are retained under
`/tmp/opencode/task824`.

The [HTTP](../results/raw) and [gRPC](../results/grpc/raw) records retain the
complete cross-server measurements and link to their trial evidence, including
comparator failures and inconclusive comparisons.
