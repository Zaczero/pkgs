---
description: How h2corn cross-server benchmarks are measured and what workloads they cover.
---

# Benchmarks

This page describes h2corn's cross-server benchmark method and workload limits.
It publishes a chart only with its matching aggregate record and generated data
table from the same accepted run. No current comparison figures are published
because the aggregate records needed to audit the comparison are unavailable.

## Measurement method

The harness runs the same Starlette application through `h2corn`, Uvicorn,
Hypercorn, and Gunicorn, directly over loopback without a reverse proxy or TLS
terminator. Each trial checks status, content type, response body, and worker
readiness before and after load. A competitor that cannot serve a scenario is
excluded with its reason; an h2corn failure stops the run.

Trials use two seconds of warmup followed by ten seconds of load, with three to
nine cold starts, rotated server order, and no CPU pinning. Published rate
ranges are the observed minimum and maximum across trials; they are not
confidence intervals. When a comparator is present, the generated conclusion
states whether the 95% bootstrap intervals separate the highest median from the
runner-up; for a separated leader it also reports the interval's relative
half-width, not its endpoints. Peak proportional set size (PSS) is the maximum
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

## Current results

--8<-- "bench/results/_generated/benchmark-results.md"
