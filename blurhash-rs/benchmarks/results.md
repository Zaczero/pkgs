# BlurHash image processing

AMD Ryzen 9 5950X 16-Core Processor · CPython 3.14.8 (64-bit)
RGB Pillow images · 4x3 components · input preserved · RGB image output

Values in seconds; lower is better.

| Case | Implementation | Size | Median | 95% interval | Runs |
| --- | --- | ---: | ---: | --- | ---: |
| encode 32x32 (4x3) | blurhash-rs 1.1.0 | — | 4.167e-06 | 4.024e-06 to 4.271e-06 | 12 |
| encode 64x64 (4x3) | blurhash-rs 1.1.0 | — | 7.838e-06 | 7.733e-06 to 7.902e-06 | 12 |
| encode 128x128 (4x3) | blurhash-rs 1.1.0 | — | 2.341e-05 | 2.293e-05 to 2.376e-05 | 12 |
| decode 32x32 (4x3) | blurhash-rs 1.1.0 | — | 1.038e-05 | 1.032e-05 to 1.048e-05 | 12 |
| decode 64x64 (4x3) | blurhash-rs 1.1.0 | — | 3.152e-05 | 3.076e-05 to 3.247e-05 | 12 |
| decode 128x128 (4x3) | blurhash-rs 1.1.0 | — | 0.0001116 | 0.0001107 to 0.0001128 | 12 |
| encode 32x32 (4x3) | blurhash-python 1.2.2 | — | 0.000494 | 0.0004846 to 0.0005002 | 12 |
| encode 64x64 (4x3) | blurhash-python 1.2.2 | — | 0.001938 | 0.001905 to 0.001969 | 12 |
| encode 128x128 (4x3) | blurhash-python 1.2.2 | — | 0.007762 | 0.007702 to 0.007927 | 12 |
| decode 32x32 (4x3) | blurhash-python 1.2.2 | — | 0.0001619 | 0.0001572 to 0.0001666 | 12 |
| decode 64x64 (4x3) | blurhash-python 1.2.2 | — | 0.0006354 | 0.0006217 to 0.000651 | 12 |
| decode 128x128 (4x3) | blurhash-python 1.2.2 | — | 0.002552 | 0.002494 to 0.0026 | 12 |
| encode 32x32 (4x3) | blurhash 1.1.5 | — | 0.004332 | 0.004276 to 0.004443 | 12 |
| encode 64x64 (4x3) | blurhash 1.1.5 | — | 0.01719 | 0.0169 to 0.01753 | 12 |
| encode 128x128 (4x3) | blurhash 1.1.5 | — | 0.06754 | 0.06675 to 0.06816 | 12 |
| decode 32x32 (4x3) | blurhash 1.1.5 | — | 0.004753 | 0.004697 to 0.004881 | 12 |
| decode 64x64 (4x3) | blurhash 1.1.5 | — | 0.01891 | 0.01869 to 0.01922 | 12 |
| decode 128x128 (4x3) | blurhash 1.1.5 | — | 0.07509 | 0.0745 to 0.07531 | 12 |

Intervals use a percentile bootstrap over independent runs. A single run has no interval.
