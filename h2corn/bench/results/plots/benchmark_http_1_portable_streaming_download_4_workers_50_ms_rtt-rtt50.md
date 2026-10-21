| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 618 | 6 | 128.550 | 286.948 | 110.8 | - |
| uvicorn | 600 | 6 | 130.351 | 281.879 | 119.1 | inconclusive |
| hypercorn | 615 | 6 | 127.862 | 268.523 | 132.0 | inconclusive |
| gunicorn | 600 | 6 | 130.280 | 292.664 | 86.4 | slower |
