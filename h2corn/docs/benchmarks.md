---
description: How h2corn cross-server benchmarks are measured and what workloads they cover.
hide:
  - navigation
---

# Benchmarks

These benchmarks compare h2corn, Uvicorn, Hypercorn, and Gunicorn serving the
same Starlette application, over loopback and a 50 ms round-trip network profile.
Results are medians across four fresh server starts on a Ryzen 9 5950X with
CPython 3.14.8 and Linux 7.2.9.

In the one-worker HTTP/1.1 GET case, **h2corn was 9.3–29.2× faster,
with 86–97% lower p50–p99 latency** than the compared servers.
The speed ratio compares median throughput: 95,389 requests/s for h2corn
against 3,267–10,280 requests/s for the other servers. Several servers have
similar throughput in the network-limited cases.

--8<-- "bench/results/_generated/benchmark-results.md"

## Methodology {#measurement-method}

Connections go directly to each server, without a reverse proxy or TLS.
Each trial checks status, content type, response body, and worker readiness
before and after load. A server that cannot serve a scenario is excluded with
its reason.
All servers use standard asyncio; Uvicorn uses h11 and Gunicorn its Python HTTP parser.

Trials use two seconds of warmup followed by ten seconds of load, with four
fresh server starts, rotated server order, and no CPU pinning. Published rate
ranges are the observed minimum and maximum across trials; they are not
confidence intervals. Comparisons use 95% bootstrap intervals to distinguish
the highest median from the runner-up. Peak proportional set size (PSS) is the maximum
sampled across the supervisor and workers.

### Workloads {#workloads-and-limits}

- HTTP/1.1 and HTTP/2 GETs, including a multiplexed HTTP/2 case with ten
  concurrent streams on one connection.
- GET over Unix domain sockets.
- A 128 KiB Starlette `FileResponse`, using the ASGI
  [`http.response.pathsend`](asgi.md#sending-a-file) extension where supported.
- Streaming downloads that yield eight 16 KiB chunks.
- Streaming POSTs that upload a 1 KiB body and receive a chunked response.
- HTTP/1.1 WebSocket echo sessions.
