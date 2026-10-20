| Server | Requests/s | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- |
| h2corn | 755 | 130.109 | 213.761 | 111.3 | - |
| uvicorn | 751 | 131.016 | 229.987 | 121.6 | inconclusive |
| hypercorn | 748 | 131.773 | 212.814 | 132.1 | inconclusive |
| gunicorn | 750 | 130.689 | 211.111 | 84.6 | inconclusive |
