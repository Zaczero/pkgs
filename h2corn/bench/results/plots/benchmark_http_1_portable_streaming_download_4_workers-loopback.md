| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 47,693 | 6 | 1.978 | 3.842 | 119.4 | - |
| uvicorn | 10,285 | 6 | 9.772 | 15.700 | 122.4 | slower |
| hypercorn | 6,646 | 6 | 13.853 | 25.826 | 135.0 | slower |
| gunicorn | 14,525 | 6 | 6.525 | 11.189 | 89.1 | slower |
