| Server | Median RPC/s | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs Connect / h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 5,113 | 51,132 | 0.184 | 0.368 | 61.8 | - |
| Connect / Hypercorn | 2,077 | 20,774.5 | 0.458 | 0.859 | 40.9 | slower |
| grpc.aio | 3,573 | 35,735 | 0.269 | 0.421 | 54.1 | slower |
| grpclib | not selected | - | - | - | - | - |
