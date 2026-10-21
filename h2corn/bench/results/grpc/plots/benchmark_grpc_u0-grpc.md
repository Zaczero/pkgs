| Server | Median RPC/s | Blocks | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 5,855 | 6 | 5,855.5 | 0.167 | 0.261 | 60.3 | - |
| Connect / Hypercorn | 2,200 | 6 | 2,200.5 | 0.442 | 0.692 | 36.1 | slower |
| grpc.aio | 3,754 | 6 | 3,755 | 0.260 | 0.387 | 52.3 | slower |
| grpclib | not selected | - | - | - | - | - | - |
