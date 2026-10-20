| Server | Median RPC/s | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs Connect / h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 6,312 | 63,135.5 | 2.487 | 3.834 | 65.8 | - |
| Connect / Hypercorn | 1,216 | 12,165 | 12.779 | 18.376 | 41.7 | slower |
| grpc.aio | 3,895 | 38,964.5 | 4.029 | 5.485 | 70.6 | slower |
| grpclib | 2,025 | 20,263 | 7.969 | 13.367 | 34.7 | slower |
