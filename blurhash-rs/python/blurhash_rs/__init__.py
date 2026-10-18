from __future__ import annotations

from contextlib import nullcontext

from PIL import Image

from blurhash_rs._lib import decode_rgb, encode_rgb

TYPE_CHECKING = False
if TYPE_CHECKING:
    from os import PathLike
    from typing import IO

__all__ = [
    'BlurhashDecodeError',
    'blurhash_decode',
    'blurhash_encode',
]


class BlurhashDecodeError(ValueError):
    """A hash or output-dimension error raised by :func:`blurhash_decode`.

    This is a :class:`ValueError`. The ``blurhash`` attribute retains the input
    passed to the decoder.
    """

    def __init__(self, blurhash: str, message: str | None = None):
        super().__init__(message or f'invalid blurhash: {blurhash!r}')
        self.blurhash = blurhash


def blurhash_encode(
    image: Image.Image | str | bytes | PathLike[str] | PathLike[bytes] | IO[bytes],
    x_components: int = 4,
    y_components: int = 3,
) -> str:
    """Encode a Pillow image or image source as a BlurHash string.

    Parameters
    ----------
    image
        A Pillow image, or an image source accepted by :func:`PIL.Image.open`.
    x_components, y_components
        Component counts from 1 through 9.

    Returns
    -------
    str
        The encoded BlurHash.

    Notes
    -----
    A caller-supplied Pillow image remains open and unchanged. Images opened by
    this function from a filename or path are closed after encoding; a
    caller-supplied file-like object remains open. Non-RGB images are converted
    to RGB; alpha is discarded rather than composited against a background.

    Raises
    ------
    ValueError
        If the image dimensions or component counts are invalid.
    TypeError, OverflowError
        If an argument cannot be converted to the native input type.
    OSError
        If Pillow cannot read the image source.
    """
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
    """Decode a BlurHash into an RGB Pillow image.

    Parameters
    ----------
    blurhash
        The encoded BlurHash string.
    width, height
        Positive output dimensions in pixels.
    punch
        Finite, nonnegative AC-component contrast scale. Zero removes contrast,
        values between zero and one reduce it, and values above one increase
        it. The DC color is preserved; output RGB channels are clipped to their
        displayable range.

    Returns
    -------
    PIL.Image.Image
        The decoded image in RGB mode.

    Notes
    -----
    The decoder allocates ``width * height * 3`` bytes for RGB pixel data.
    Representability checks do not impose a memory budget.

    Raises
    ------
    BlurhashDecodeError
        If the hash is malformed or the dimensions are nonpositive or outside
        the supported storage range, or ``punch`` is negative or nonfinite.
        The exception retains ``blurhash``.
    TypeError, OverflowError
        If an argument cannot be converted to the native input type.
    MemoryError
        If Pillow or Python cannot allocate the output image.
    """
    if width <= 0 or height <= 0:
        raise BlurhashDecodeError(
            blurhash,
            f'width and height must be positive, got {width}x{height}',
        )
    try:
        data = decode_rgb(blurhash, width, height, punch)
    except (OverflowError, ValueError) as e:
        raise BlurhashDecodeError(blurhash, str(e)) from e
    else:
        return Image.frombuffer('RGB', (width, height), data, 'raw', 'RGB', 0, 1)
