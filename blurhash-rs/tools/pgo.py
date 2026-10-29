"""Train native and Pillow BlurHash paths across component and image regimes."""

from __future__ import annotations

import random
import sys
from pathlib import Path

import pgo_process

GENERATE_FLAGS = ()
PACKAGE = Path(__file__).resolve().parents[1]
OPERATIONS = 4_093
stage_generator = pgo_process.stage_generator
verify_runtime = pgo_process.verify_runtime
known_profile_diagnostic = pgo_process.known_profile_diagnostic


def collect(output: Path) -> Path:
    from blurhash_rs import _lib

    return pgo_process.collect(
        output,
        Path(__file__),
        Path(_lib.__file__),
        units=OPERATIONS,
        functions=['encode_rgb', 'decode_rgb', 'cos_axis_simd_cached', 'render_pixels'],
    )


def test(output: Path) -> None:
    pgo_process.run_tests(output, PACKAGE)


def train() -> None:
    from blurhash_rs import (
        BlurhashDecodeError,
        _lib,
        blurhash_decode,
        blurhash_encode,
        decode_rgb,
        encode_rgb,
    )
    from PIL import Image

    rng = random.Random(327851)
    units = 0
    for width, height, nx, ny, mode, pattern in (
        (16, 16, 1, 1, 'RGB', 'solid'),
        (24, 32, 3, 4, 'L', 'gradient'),
        (64, 40, 4, 3, 'RGB', 'noise'),
        (48, 64, 5, 4, 'RGBA', 'checker'),
        (96, 64, 8, 3, 'RGB', 'gradient'),
        (64, 96, 9, 5, 'RGB', 'noise'),
        (160, 120, 4, 3, 'RGB', 'gradient'),
        (96, 96, 9, 9, 'RGB', 'checker'),
    ):
        if pattern == 'solid':
            data = bytes((40, 120, 210)) * (width * height)
        elif pattern == 'gradient':
            data = bytes(
                channel
                for y in range(height)
                for x in range(width)
                for channel in (
                    x * 255 // (width - 1),
                    y * 255 // (height - 1),
                    (x + y) * 255 // (width + height - 2),
                )
            )
        elif pattern == 'checker':
            data = bytes(
                (240 if (x // 4 + y // 4) % 2 else 15)
                for y in range(height)
                for x in range(width)
                for _ in range(3)
            )
        else:
            data = rng.randbytes(width * height * 3)
        image = Image.frombytes('RGB', (width, height), data).convert(mode)
        data = image.convert('RGB').tobytes()
        value = encode_rgb(data, width, height, nx, ny)
        units += 1
        for index in range(100):
            punch = (1.0, 1.0, 1.0, 0.65, 1.8)[index % 5]
            if (
                encode_rgb(data, width, height, nx, ny) != value
                or blurhash_encode(image, nx, ny) != value
            ):
                raise RuntimeError('BlurHash training encoding changed')
            if (
                len(decode_rgb(value, width, height, punch)) != width * height * 3
                or blurhash_decode(value, width, height, punch=punch).size
                != (width, height)
                or len(decode_rgb(value, width, height, 0.0)) != width * height * 3
            ):
                raise RuntimeError('BlurHash training rendering failed')
            units += 5
    # Varying image sizes exercise cosine-table construction and cache eviction,
    # alongside the repeated-size thumbnail sessions above.
    for width in range(17, 58):
        data = rng.randbytes(width * 11 * 3)
        value = encode_rgb(data, width, 11, 4, 3)
        if len(decode_rgb(value, width, 11, 1.0)) != width * 11 * 3:
            raise RuntimeError('BlurHash training size variation failed')
        units += 2
    for value in ('', 'bad', '0'):
        try:
            blurhash_decode(value, 32, 32)
        except BlurhashDecodeError:
            units += 1
        else:
            raise RuntimeError('BlurHash training accepted a malformed hash')
    pgo_process.completed(units, Path(_lib.__file__))


if __name__ == '__main__':
    if sys.argv[1:] != ['train']:
        raise SystemExit('expected train')
    train()
