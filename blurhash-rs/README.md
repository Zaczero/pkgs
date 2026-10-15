# blurhash-rs

Use this Python package to encode Pillow images as [BlurHash](https://blurha.sh) strings and decode them as RGB preview images.

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

assert isinstance(blurhash, str)
assert preview.mode == "RGB"
assert preview.size == (32, 24)
print(blurhash)
```

The decoded preview is a lossy reconstruction; its pixels are not expected to match the input image.

## Encode an image file

```py
# Template: replace "photo.jpg" with the path to your image.
from PIL import Image
from blurhash_rs import blurhash_encode

with Image.open("photo.jpg") as opened:
    image = opened.copy()

# thumbnail() changes the caller-owned copy in place.
image.thumbnail((100, 100))
blurhash = blurhash_encode(image, x_components=4, y_components=3)
```

`blurhash_encode("photo.jpg")` also accepts a source supported by `PIL.Image.open`; the facade closes images it opens from a filename or path. A file-like object supplied by the caller remains open. A caller-supplied Pillow `Image` remains open and unchanged. The encoder converts non-RGB modes to RGB. For images with alpha, this discards the alpha channel rather than compositing against a background. Choose and apply any desired background before encoding. See Pillow's [file-handling rules](https://pillow.readthedocs.io/en/stable/reference/open_files.html) for its image and file lifecycle.

## API reference

### `blurhash_encode`

Pass a Pillow `Image` or an image source accepted by `Image.open`. The optional x- and y-component counts must each be from 1 through 9 and control the hash's detail. The result is a hash string. Invalid component counts or image dimensions raise `ValueError`; argument extraction can raise `TypeError` or `OverflowError`, and Pillow's image-reading errors propagate.

### `blurhash_decode`

Pass a hash and positive output dimensions; the result is an RGB Pillow `Image`. Invalid hashes, nonpositive dimensions, and dimensions outside the supported storage range raise `BlurhashDecodeError`, which retains the input in its `blurhash` attribute. Python argument-type errors and output-allocation failures are not converted to this exception.

The decoder allocates `width × height × 3` bytes for RGB pixel data. Its representability checks do not impose a memory budget; choose output dimensions with that allocation in mind.

`punch` scales the decoded AC components. Finite values below 1 do not reduce contrast; finite values at least 1 are used as the scale. NaN and negative infinity also behave as 1, while positive infinity is accepted as an infinite scale and can produce degenerate or clipped RGB output.

### `BlurhashDecodeError`

This exception reports a hash or image-dimension error from `blurhash_decode`. Its `blurhash` attribute contains the hash passed to the decoder.
