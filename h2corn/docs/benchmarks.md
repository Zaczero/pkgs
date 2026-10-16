---
description: How h2corn cross-server benchmarks are measured and what workloads they cover.
---

# Benchmarks

The benchmarks compare the same Starlette application across ASGI servers,
with correctness checks before and after load.

In the one-worker HTTP/1.1 GET case, **h2corn delivered 95,389 requests/s
with 86–97% lower p50–p99 latency** than the compared servers.

![HTTP/1.1 GET latency: h2corn stays below 2 ms from p50 through p99; the other servers range from about 9 to 39 ms.](assets/benchmarks/latency-loopback.svg)

| Server | Requests/s | p50 | p99 |
| --- | ---: | ---: | ---: |
| h2corn | 95,389 | 0.995 ms | 1.671 ms |
| Gunicorn | 10,280 | 9.458 ms | 12.253 ms |
| Uvicorn | 7,312 | 13.367 ms | 20.721 ms |
| Hypercorn | 3,267 | 30.003 ms | 38.665 ms |

These are medians across four fresh starts on a Ryzen 9 5950X with
CPython 3.14.8 and Linux 7.2.9. The complete results below include the
network-limited 50 ms RTT cases, where several servers have similar throughput.

## Measurement method

The harness runs the same Starlette application through `h2corn`, Uvicorn,
Hypercorn, and Gunicorn, directly over loopback without a reverse proxy or TLS
terminator. Each trial checks status, content type, response body, and worker
readiness before and after load. A competitor that cannot serve a scenario is
excluded with its reason; an h2corn failure stops the run.
All servers use standard asyncio; Uvicorn uses h11 and Gunicorn its Python HTTP parser.

Trials use two seconds of warmup followed by ten seconds of load, with four
fresh server starts, rotated server order, and no CPU pinning. Published rate
ranges are the observed minimum and maximum across trials; they are not
confidence intervals. Comparisons use 95% bootstrap intervals to distinguish
the highest median from the runner-up. Peak proportional set size (PSS) is the maximum
sampled across the supervisor and workers.

## Workloads and limits

- HTTP/1.1 and HTTP/2 GETs, including a multiplexed HTTP/2 case with ten
  concurrent streams on one connection.
- GET over Unix domain sockets.
- A 128 KiB Starlette `FileResponse`, using the ASGI
  [`http.response.pathsend`](asgi.md#sending-a-file) extension where supported.
- Streaming downloads that yield eight 16 KiB chunks.
- Streaming POSTs that upload a 1 KiB body and receive a chunked response.
- HTTP/1.1 WebSocket echo sessions.

These workloads do not measure reverse proxies, TLS, HTTP/3, browser behavior,
cold-start latency, failure recovery, or applications other than the tested
Starlette app. They do not establish framework-wide drop-in compatibility.

--8<-- "bench/results/_generated/benchmark-results.md"
