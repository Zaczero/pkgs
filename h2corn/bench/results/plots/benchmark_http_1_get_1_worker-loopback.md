| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 94,412 | 6 | 1.015 | 1.842 | 60.8 | - |
| uvicorn | 7,042 | 6 | 13.887 | 18.291 | 27.2 | slower |
| hypercorn | 3,092 | 6 | 31.046 | 52.393 | 51.3 | slower |
| gunicorn | 9,750 | 6 | 9.888 | 13.931 | 40.8 | slower |

Collection: direction-resolved.
