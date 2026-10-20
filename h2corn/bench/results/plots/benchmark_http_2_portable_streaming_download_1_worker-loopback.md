| Server | Requests/s | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- |
| h2corn | 20,670 | 4.650 | 10.609 | 62.4 | - |
| uvicorn | unsupported | - | - | - | - |
| hypercorn | 1,149 | 84.730 | 110.943 | 65.6 | slower |
| gunicorn | unsupported | - | - | - | - |
