from contextlib import nullcontext

from PIL import Image

from ._lib import decode_rgb, encode_rgb

TYPE_CHECKING = False
if TYPE_CHECKING:
    from os import PathLike
    from typing import IO

__all__ = [
    'BlurhashDecodeError',
    'blurhash_decode',
    'blurhash_encode',
]


class BlurhashDecodeError(Exception):
    def __init__(self, blurhash: str):
        self.blurhash = blurhash


def blurhash_encode(
    image: 'Image.Image | str | bytes | PathLike[str] | PathLike[bytes] | IO[bytes]',
    x_components: int = 4,
    y_components: int = 3,
) -> str:
    with (
        nullcontext(image)
        if isinstance(image, Image.Image)
        else Image.open(image) as img
    ):
        if img.mode != 'RGB':
            img = img.convert('RGB')
        width, height = img.size
        data = img.tobytes()
        return encode_rgb(data, width, height, x_components, y_components)


def blurhash_decode(
    blurhash: str,
    width: int,
    height: int,
    *,
    punch: float = 1,
) -> Image.Image:
    try:
        data = decode_rgb(blurhash, width, height, punch)
    except ValueError as e:
        raise BlurhashDecodeError(blurhash) from e
    else:
        return Image.frombuffer('RGB', (width, height), data, 'raw', 'RGB', 0, 1)
