| Server | Requests/s | Blocks | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| h2corn | 20,767 | 6 | 4.337 | 10.392 | 61.0 | - |
| uvicorn | unsupported | - | - | - | - | - |
| hypercorn | 1,153 | 6 | 80.271 | 104.961 | 66.1 | slower |
| gunicorn | unsupported | - | - | - | - | - |
