| Server | Median RPC/s | Blocks | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 17,414 | 6 | 17,464 | 3.474 | 10.526 | 68.3 | - |
| Connect / Hypercorn | 3,165 | 6 | 3,200 | 19.701 | 27.772 | 39.4 | slower |
| grpc.aio | 6,283 | 6 | 6,351.5 | 10.165 | 13.431 | 57.0 | slower |
| grpclib | not selected | - | - | - | - | - | - |
