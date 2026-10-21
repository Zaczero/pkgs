# BlurHash image benchmarks

AMD Ryzen 9 5950X 16-Core Processor · CPython 3.15.0 (64-bit) · Pillow 13.0.0 · NumPy 2.5.4

Median time per image; lower is faster.

## Encode Pillow images

| Workload | blurhash-rs 2.0.0 | blurhash 1.1.5 | blurhash-python 1.2.2 |
| --- | ---: | ---: | ---: |
| 32 x 32 square · RGB · 4 x 3 components | 4.38 µs | 5.26 ms | 561 µs |
| 64 x 64 square · RGB · 4 x 3 components | 7.79 µs | 21.3 ms | 2.18 ms |
| 128 x 128 square · RGB · 4 x 3 components | 22.5 µs | 85.6 ms | 8.46 ms |
| 96 x 54 landscape · RGB · 4 x 3 components | 9.16 µs | 26.9 ms | 2.68 ms |
| 54 x 96 portrait · RGB · 3 x 4 components | 10.1 µs | 26.7 ms | 3.16 ms |
| 32 x 32 square · RGB · 1 x 1 component | 4.84 µs | 1.34 ms | 159 µs |
| 32 x 32 square · RGB · 9 x 9 components | 6.74 µs | 30.5 ms | 3.02 ms |
| 64 x 32 landscape · RGBA input · 4 x 3 components | 8.67 µs | 10.5 ms | 1.1 ms |
| 32 x 32 square · grayscale input · 4 x 3 components | 6.37 µs | 5.21 ms | 561 µs |

## Decode RGB Pillow images

| Workload | blurhash-rs 2.0.0 | blurhash 1.1.5 | blurhash-python 1.2.2 |
| --- | ---: | ---: | ---: |
| 32 x 32 square · RGB · 4 x 3 components | 11 µs | 5.48 ms | 221 µs |
| 64 x 64 square · RGB · 4 x 3 components | 38.9 µs | 25.5 ms | 862 µs |
| 128 x 128 square · RGB · 4 x 3 components | 119 µs | 91.3 ms | 3.88 ms |
| 96 x 54 landscape · RGB · 4 x 3 components | 41.1 µs | 27.2 ms | 1.12 ms |
| 54 x 96 portrait · RGB · 3 x 4 components | 40.4 µs | 28.1 ms | 1.11 ms |
| 32 x 32 square · RGB · 1 x 1 component | 11.2 µs | 1.24 ms | 53.6 µs |
| 32 x 32 square · RGB · 9 x 9 components | 13.5 µs | 35.4 ms | 1.53 ms |
