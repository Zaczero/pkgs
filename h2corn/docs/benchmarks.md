---
description: How h2corn cross-server benchmarks are measured and what workloads they cover.
---

# Benchmarks

The benchmarks compare the same Starlette application across ASGI servers,
with correctness checks before and after load.

## Measurement method

The harness runs the same Starlette application through `h2corn`, Uvicorn,
Hypercorn, and Gunicorn, directly over loopback without a reverse proxy or TLS
terminator. Each trial checks status, content type, response body, and worker
readiness before and after load. A competitor that cannot serve a scenario is
excluded with its reason; an h2corn failure stops the run.

Trials use two seconds of warmup followed by ten seconds of load, with three to
nine cold starts, rotated server order, and no CPU pinning. Published rate
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
