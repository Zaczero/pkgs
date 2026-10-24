| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 10,890 | 6 | 8.474 | 25.185 | 66.6 | - |
| uvicorn | 1,591 | 6 | 59.134 | 121.444 | 33.2 | slower |
| hypercorn | 1,291 | 6 | 73.978 | 96.682 | 57.6 | slower |
| gunicorn | 2,631 | 6 | 35.302 | 89.520 | 46.5 | slower |

Collection: direction-resolved.
