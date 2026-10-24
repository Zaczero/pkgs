---
description: h2corn throughput, latency, and memory compared with other Python server stacks.
hide:
  - navigation
---

# Benchmarks

**h2corn delivered 9.7–30× the throughput of the compared servers** in the
one-worker HTTP/1.1 GET comparison. All serve the same Starlette application
over loopback; h2corn uses its profile-guided release wheel.

**Connect on h2corn delivered 1.5–3.0× the throughput of grpc.aio** in six
of eight gRPC workloads. Upload and download remained inconclusive. These
compare complete stacks using common Python handlers and protobuf codecs,
not the host server in isolation.

--8<-- "bench/results/_generated/benchmark-results.md"

## Reading the results {#measurement-method}

Rates are medians; dots show individual trials and whiskers their observed
range. Headline ratios use paired trials. Each scenario uses six fresh-start
blocks, extended to twelve for gRPC upload and download, with server order
rotated on an unpinned, shared host. Paired comparisons report 95% intervals;
host drift still limits their interpretation. “Inconclusive” does not mean equal.

Latency curves show medians of per-trial percentiles under closed-loop load,
not production arrival-rate guarantees. Memory is sampled peak proportional
set size (PSS) across the supervisor and workers; short peaks can be missed.
Responses are checked, and failed or unstable comparisons are identified
in the detailed results.

HTTP tests use standard asyncio, direct connections without TLS or a proxy,
Uvicorn's h11 parser, and Gunicorn's Python HTTP parser. HTTP/2 uses cleartext
prior knowledge.

The [benchmark source](https://github.com/Zaczero/pkgs/tree/main/h2corn/bench),
[raw results](https://github.com/Zaczero/pkgs/tree/main/h2corn/bench/results/raw),
and [trial evidence](https://github.com/Zaczero/pkgs/tree/main/h2corn/bench/results/evidence)
provide exact commands, calibration rules, configurations, and observations.

### HTTP workload notes {#workloads-and-limits}

- Multiplexed HTTP/2 uses ten concurrent streams on one connection.
- Static files are 128 KiB Starlette `FileResponse` objects, using
  [`http.response.pathsend`](asgi.md#sending-a-file) where supported.
  Gunicorn 26.2.0 fails the reused-connection check, so its file results are
  reported as failed.
- Streaming downloads yield eight 16 KiB chunks. Streaming POSTs upload 1 KiB
  and receive three chunks with 20 ms of application delay.
- WebSocket trials connect, echo one text message, and disconnect: they measure
  session turnover, not sustained message throughput.

## gRPC setup {#grpc-measurement-method}

Each stack uses one serving process, standard asyncio, one HTTP/2 client
connection, and no message compression. TLS rows terminate TLS 1.3 directly
at each server. The same Go client drives every stack.

Unary and Upload report complete RPCs/second; Download and Duplex report
response messages/second. Latency percentiles use histogram upper bounds with
one-percent buckets.

### gRPC workload notes {#grpc-workloads}

U0, U32, and U64K are unary calls with 0-byte, 32-byte, and 64 KiB payloads.
Upload and Download transfer sixteen 64 KiB messages per RPC; Duplex exchanges
64 messages of 256 bytes in each direction. TLS rows repeat U32 and Duplex.
The grpclib comparison covers U32, U64K, and Duplex.

### Known gRPC failures {#grpc-qualification}

Valid-request checks passed for all measured workloads. Cancellation passed
for h2corn and grpc.aio but timed out on Hypercorn.
Released Connect failed the reserved-flag, truncated-suffix, and expired-deadline
checks. Throughput results do not imply that these error paths conform.

The [gRPC records and evidence](https://github.com/Zaczero/pkgs/tree/main/h2corn/bench/results/grpc)
retain exact configurations, verified counts, and failure details.
