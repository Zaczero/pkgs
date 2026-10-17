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

**About 120–330× faster encoding and 16–23× faster decoding** than
`blurhash-python` by median runtime in these Pillow-image benchmarks.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="benchmarks/encode-dark.svg">
  <img src="benchmarks/encode.svg" alt="Encoding 32, 64, and 128 pixel square images takes blurhash-rs 4.2, 7.8, and 23.4 microseconds, compared with 494, 1,938, and 7,762 microseconds for blurhash-python.">
</picture>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="benchmarks/decode-dark.svg">
  <img src="benchmarks/decode.svg" alt="Decoding to 32, 64, and 128 pixel square images takes blurhash-rs 10.4, 31.5, and 111.6 microseconds, compared with 162, 635, and 2,552 microseconds for blurhash-python.">
</picture>

The workload preserves caller-owned RGB Pillow images, uses 4 × 3 components,
and returns RGB images on decode. Required copies and Pillow/list conversions
are included. All three libraries produced identical hashes; decoded channels
differed by at most one 8-bit level.

Measured on a Ryzen 9 5950X shared workstation with CPython 3.14.8,
Pillow 12.3.0, and Linux 7.2.9. Each case uses twelve worker processes and
three readings after warmup per process. Six-process batches run in
A–B–C–C–B–A order.
[Full table and intervals](benchmarks/results.md), raw results
([blurhash-rs](benchmarks/blurhash-rs.json),
[blurhash-python](benchmarks/blurhash-python.json),
[blurhash](benchmarks/blurhash.json)), and the [benchmark](benchmark.py)
retain the measurement details.

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
