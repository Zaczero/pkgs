# blurhash-rs

Encode images as compact [BlurHash](https://blurha.sh) placeholders and decode
them into Pillow previews. Written in Rust, for Python 3.10+.

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

## API reference

### `blurhash_encode`

Pass a Pillow `Image` or a source accepted by `Image.open`. The x- and
y-component counts, each from 1 through 9, control the hash's detail.

### `blurhash_decode`

Pass a hash and positive output dimensions to get an RGB Pillow `Image`.
Use `punch` above 1 to increase contrast.

### `BlurhashDecodeError`

This exception reports a hash or image-dimension error from `blurhash_decode`. Its `blurhash` attribute contains the hash passed to the decoder.

[Full reference](python/blurhash_rs/__init__.py) ·
[Issues](https://github.com/Zaczero/pkgs/issues)
