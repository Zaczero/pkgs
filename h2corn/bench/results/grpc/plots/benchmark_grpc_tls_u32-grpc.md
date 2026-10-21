| Server | Median RPC/s | Blocks | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs Connect / h2corn |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 17,305 | 12 | 173,105 | 3.576 | 8.089 | 69.2 | - |
| Connect / Hypercorn | 2,988 | 12 | 29,924 | 20.913 | 30.675 | 44.8 | slower |
| grpc.aio | 6,085 | 12 | 60,916.5 | 10.473 | 13.701 | 60.0 | slower |
| grpclib | not selected | - | - | - | - | - | - |
