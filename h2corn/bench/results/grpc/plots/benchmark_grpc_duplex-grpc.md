| Server | Median messages/s | Blocks | Median complete RPC/s | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 70,859 | 6 | 1,107 | 1,111.5 | 7.034 | 9.866 | 62.5 | - |
| Connect / Hypercorn | 17,758 | 6 | 277 | 283.5 | 29.773 | 39.744 | 36.3 | slower |
| grpc.aio | 33,589 | 6 | 525 | 530 | 15.060 | 19.506 | 55.0 | slower |
| grpclib | 20,030 | 6 | 313 | 316 | 25.266 | 30.071 | 29.2 | slower |
