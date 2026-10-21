---
description: How h2corn cross-server benchmarks are measured and what workloads they cover.
hide:
  - navigation
---

# Benchmarks

**h2corn delivered 11–29× higher throughput** in the one-worker HTTP/1.1 GET
comparison, using standard asyncio and pure-Python parsers for Uvicorn and Gunicorn.

These benchmarks compare h2corn, Uvicorn, Hypercorn, and Gunicorn serving the
same Starlette application, over loopback and a 50 ms round-trip network profile.
Results cover throughput, latency, and memory.

**Connect on h2corn delivered 1.1–2.8× the throughput of grpc.aio** in seven
of eight gRPC workloads; client-streaming upload was a statistical tie.
These results compare Connect hosted by h2corn and Hypercorn, grpc.aio, and a
selected grpclib subset, using common Python handlers and protobuf codecs over
direct loopback HTTP/2. They measure these complete stacks, not the host
server in isolation.

The figures and raw records currently shown are the previous published
collection. The adaptive sampling procedure below describes the current
benchmark runner; those measurements will be replaced only after a complete
combined run passes its publication checks.

--8<-- "bench/results/_generated/benchmark-results.md"

## HTTP methodology {#measurement-method}

Connections go directly to each server, without a reverse proxy or TLS.
Each trial checks status, content type, response body, and worker readiness
before and after load, including a second HTTP/1.1 request on a persistent
connection when the server keeps it open. Failed response checks produce a
`failed` cell, with diagnostics retained in the raw evidence.
All servers use standard asyncio. HTTP/1.1 uses Uvicorn's h11 parser and
Gunicorn's Python HTTP parser. HTTP/2 uses cleartext prior knowledge.
Requested limits and effective peer SETTINGS are retained in the raw evidence.
TCP listeners are shared by inheritance and have `TCP_NODELAY` enabled for
every server. Hypercorn's per-connection request-retirement limit is raised
beyond the trial workload.

Each time-window scenario is calibrated in one verified serving process per
server, using successive 0.5, 1, 2, and 4 second windows. Calibration stops
when adjacent rates agree within 10%; every server in that scenario then uses
the longest selected duration. A scenario that remains unstable at four
seconds is marked calibration-limited, and its comparisons are inconclusive.
The 8 MiB upload retains its fixed complete-request batch.

Two fresh-process pilot blocks estimate paired log-ratio scatter and cell
cost. They are excluded from results. Before production, the runner freezes six
paired blocks per scenario, adding three blocks when the pilot's paired
log-ratio scatter exceeds `log(1.1)` and the forecast stays within the
30-minute target. Production does not stop early based on observed results.
Fresh server processes, rotated server order, and unpinned CPU placement are
used for every block.

Comparisons use paired production-block throughput ratios and
distribution-free order-statistic intervals for the common median of
independent paired-block ratios. Six blocks use the minimum and maximum ratios
(96.875% coverage); nine use the second and eighth ratios (96.09375% coverage).
Each comparison records its method and achieved coverage. An interval
containing one is inconclusive. Rates are medians; dots show individual blocks
and whiskers show their observed range. Latencies are medians of per-trial
percentiles. Clients use closed-loop load.

One monotonic deadline covers the combined HTTP and gRPC collection. Thirty
minutes is the target and 45 minutes is the hard total. If the complete
six-block plan cannot fit the hard limit, the run retains diagnostic evidence,
exits unsuccessfully, and does not publish.

The 8 MiB network upload uses a fixed batch of sixteen completed requests on
eight connections, after eight warmup requests. Throughput counts successful
completions over actual elapsed time.

Peak proportional set size (PSS) is the maximum sampled simultaneous sum
across unique supervisor and worker processes. PSS shares mapped-page cost
between processes. Sampling includes initial and final readings plus
one-second readings during measured load; short peaks can fall between samples.

The [raw scenario records](https://github.com/Zaczero/pkgs/tree/main/h2corn/bench/results/raw)
and [per-trial evidence](https://github.com/Zaczero/pkgs/tree/main/h2corn/bench/results/evidence)
retain the commands, observations, and identities behind the figures.

### Workloads {#workloads-and-limits}

- HTTP/1.1 and HTTP/2 GETs, including a multiplexed HTTP/2 case with ten
  concurrent streams on one connection.
- GET over Unix domain sockets.
- A 128 KiB Starlette `FileResponse`, using the ASGI
  [`http.response.pathsend`](asgi.md#sending-a-file) extension where supported.
- Streaming downloads that yield eight 16 KiB chunks.
- POSTs that upload 1 KiB, then receive three response chunks with 20 ms of
  application delay.
- An 8 MiB HTTP/2 upload on the shaped network, with the complete upload
  verified before and after load.
- HTTP/1.1 WebSocket sessions that connect, echo one text message, and
  disconnect. They measure session turnover, not sustained message throughput.

## gRPC methodology {#grpc-measurement-method}

The gRPC family uses the same adaptive calibration, excluded pilot, frozen
production plan, and paired-median intervals described in the HTTP
methodology. Its 27 supported cells use one serving process and one standard
asyncio loop per server, with one HTTP/2 client connection per trial. Five
grpclib cells are intentionally not selected. Compression is identity. TLS
rows connect directly to each server; all measured TLS connections negotiated
TLS 1.3 with TLS_AES_128_GCM_SHA256.

Calibration reuses the verified serving process across windows. Pilot and
production trials start fresh processes, open a new client connection, and
complete a control RPC before measurement. Closed-loop load verifies every
response, message count, and final gRPC status; invalid or unfinished work is
retained as a failure rather than credited as throughput.

The Go client uses a common `GOMAXPROCS=16` for every stack and row.
Download sensitivity controls found that a budget of two constrained the
generator. Increasing eight to sixteen gave paired median throughput ratios
of 1.010 for h2corn and 1.043 for grpc.aio; these three-index controls are
diagnostic, not publication uncertainty. The published collection uses only
the sixteen-processor budget. This removes the observed low-budget
constraint without establishing unconstrained server capacity.

Bars show median rates; dots and whiskers show individual production blocks
and their observed range. These intervals describe this host and workload,
not all deployments. Latency curves show medians of per-trial histogram
percentile upper bounds, with one-percent buckets. PSS is the maximum
one-second sampled simultaneous process sum and can miss shorter peaks.

Unary and Upload rows report complete RPCs per second. Download and Duplex
report verified response messages per second. Their detailed tables also
report complete RPC rates and the actual median completed RPC count per
trial, which can be fractional when an even number of blocks is used.

### gRPC workloads {#grpc-workloads}

| Row | Payload bytes per message | Messages per RPC | Concurrent RPCs |
| --- | --- | --- | --- |
| U0 | 0 | 1 | 1 |
| U32 | 32 | 1 | 64 |
| U64K | 65,536 | 1 | 16 |
| Upload | 65,536 | 16 requests, 1 response | 8 |
| Download | 65,536 | 1 request, 16 responses | 8 |
| Duplex | 256 | 64 requests and responses | 8 |
| TLS-U32 | 32 | 1 | 64 |
| TLS-Duplex | 256 | 64 requests and responses | 8 |

Upload's payload size applies to request chunks; its response acknowledges
the received chunk and byte counts. Download's payload size applies to
response messages.

### gRPC qualification and evidence {#grpc-qualification}

All 27 selected positive-work cells passed qualification. The separate
suspended-handler cancellation check passed for h2corn and grpc.aio but
failed to settle within its bound on Hypercorn. Released Connect also
failed the reserved-flag, truncated-suffix, and expired-deadline witnesses
shown in the qualification table. Positive-work throughput does not imply
that these error paths conform.

The [gRPC records, plots, and evidence](https://github.com/Zaczero/pkgs/tree/main/h2corn/bench/results/grpc)
retain artifact identities, trial commands, verified counts, process
measurements, qualification outcomes, and client-budget controls.
