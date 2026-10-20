# blurhash-rs

Encode images as compact [BlurHash](https://blurha.sh) placeholders and decode
them into Pillow previews. Written in Rust, for Python 3.11+.

## Install

```sh
pip install blurhash-rs
```

## Encode and decode an image

```py
from PIL import Image
from blurhash_rs import blurhash_encode, blurhash_decode

image = Image.new("RGB", (24, 16), (40, 100, 180))
blurhash = blurhash_encode(image)
preview = blurhash_decode(blurhash, 32, 24)

print(blurhash)
```

`preview` is a 32 × 24 RGB image.

## Encode an image file

```py
from PIL import Image
from blurhash_rs import blurhash_encode

# Use your image's path.
with Image.open("photo.jpg") as opened:
    image = opened.copy()

image.thumbnail((100, 100))
blurhash = blurhash_encode(image, x_components=4, y_components=3)
```

You can also pass a filename directly to `blurhash_encode`. Images you supply
remain open and unchanged. Alpha is discarded, so composite transparent images
onto your chosen background first.

## Benchmarks

These Pillow-image benchmarks compare `blurhash-rs` with the current
`blurhash` and `blurhash-python` releases. Square-image charts show scaling;
the workload charts compare image shapes, component counts, and input modes.
On this workstation, blurhash-rs had the lowest measured median in all nine
encoding and seven decoding cases. Lower time is faster; the full table reports
per-case medians and worker-process intervals.

For square RGB images using 4 by 3 components at 32, 64, and 128 pixels,
blurhash-python took 127–378 times as long as blurhash-rs to encode, and
20.5–29.1 times as long to decode, by the median worker-process means on this
workstation.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="benchmarks/encode-dark.svg">
  <img src="benchmarks/encode.svg" alt="Median time to encode square RGB Pillow images at 32, 64, and 128 pixels with 4 by 3 components. blurhash-rs has the lowest measured median at each size compared with blurhash and blurhash-python; whiskers show 95% intervals across worker-process means.">
</picture>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="benchmarks/encode-workloads-dark.svg">
  <img src="benchmarks/encode-workloads.svg" alt="Median encoding time for landscape and portrait images, 1 by 1 and 9 by 9 component counts, and RGBA or grayscale inputs. blurhash-rs has the lowest measured median versus blurhash and blurhash-python in these workload variants.">
</picture>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="benchmarks/decode-dark.svg">
  <img src="benchmarks/decode.svg" alt="Median time to decode BlurHash values into 32, 64, and 128 pixel square RGB Pillow images using 4 by 3 components. blurhash-rs has the lowest measured median at each size compared with blurhash and blurhash-python; whiskers show 95% intervals across worker-process means.">
</picture>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="benchmarks/decode-workloads-dark.svg">
  <img src="benchmarks/decode-workloads.svg" alt="Median decoding time for landscape and portrait output sizes and 1 by 1 or 9 by 9 component counts. blurhash-rs has the lowest measured median versus blurhash and blurhash-python in these workload variants.">
</picture>

Encoding preserves caller-owned Pillow images. The benchmark includes the
Pillow-to-NumPy conversion used by `blurhash`, the RGB image copy required by
`blurhash-python`, and output-image construction where those interfaces require
them. All three libraries produced identical hashes; decoded channels differed
by at most one 8-bit level.

The measured versions are blurhash-rs 1.1.0, [`blurhash` 1.1.5](https://pypi.org/project/blurhash/1.1.5/),
and [`blurhash-python` 1.2.2](https://pypi.org/project/blurhash-python/1.2.2/),
with Pillow 13.0.0, NumPy 2.5.4, and CFFI 2.1.1 for blurhash-python. pyperf
2.10.0 collected the measurements; benchroom 0.1.0 and Matplotlib 3.11.2
generated the reports. Runs used CPython 3.15.0 (Clang 23.1.3) on a shared
Ryzen 9 5950X workstation (Linux 7.2.9, microcode `0xa201213`). Each workload
has twelve independent worker-process means from three calibrated readings of
at least 0.1 seconds each after one warmup. Six-worker batches ran blurhash-rs,
blurhash, blurhash-python and then the reverse order. The table reports medians
and 95% percentile bootstrap intervals from 10,000 resamples of
worker-process means. Warmup-only runs remain in the raw JSON but are excluded
from the reported values and intervals.
Native wheels used release builds with rustc 1.101.0-nightly
(`4ebd25825`, LLVM 23.1.4) targeting x86-64-v2.

[Full table and intervals](benchmarks/results.md), raw results
([blurhash-rs](benchmarks/blurhash-rs.json),
[blurhash-python](benchmarks/blurhash-python.json),
[blurhash](benchmarks/blurhash.json)), and the [benchmark](benchmark.py)
and [report generator](tools/benchmark_report.py) retain the measurement
details.

## API reference

### `blurhash_encode`

Pass a Pillow `Image` or a source accepted by `Image.open`. The x- and
y-component counts, each from 1 through 9, control the hash's detail.

### `blurhash_decode`

Pass a hash and positive output dimensions to get an RGB Pillow `Image`.
Use finite, nonnegative `punch` values to control contrast: zero shows the
constant DC color, values between zero and one reduce contrast, and values
above one increase it.

### `BlurhashDecodeError`

This exception reports an invalid hash, image dimension, or contrast scale
from `blurhash_decode`. Its `blurhash` attribute contains the hash passed to
the decoder.

[Full reference](python/blurhash_rs/__init__.py) ·
[Issues and questions](https://github.com/Zaczero/pkgs/issues) ·
[Private security reports](https://github.com/Zaczero/pkgs/security/advisories/new) ·
[Premium support](https://monicz.dev/#get-in-touch)
