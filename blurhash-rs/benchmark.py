"""Pillow workflows measured with pyperf, with image setup excluded.

The default dev environment supports blurhash-rs and blurhash. For
blurhash-python, use ``uv run --no-dev --group compare-cffi`` in a separate
environment: both comparator packages provide the ``blurhash`` import.
Save runs with ``-o`` and compare matching cases with
``python -m pyperf compare_to baseline.json candidate.json``.
"""

import argparse
import hashlib
import sys
from importlib.metadata import version
from pathlib import Path

import pyperf
from PIL import Image


def _pil_rgb_image_to_3d_array(img: Image.Image) -> list[list[list[int]]]:
    data = img.tobytes()
    pixels: list[list[list[int]]] = []
    p = 0
    for _y in range(img.height):
        row: list[list[int]] = []
        for _x in range(img.width):
            r, g, b = data[p : p + 3]
            row.append([r, g, b])
            p += 3
        pixels.append(row)
    return pixels


def _make_gradient_image(width: int, height: int):
    buf = bytearray(width * height * 3)
    denom_x = max(1, width - 1)
    denom_y = max(1, height - 1)
    denom_xy = max(1, (width - 1) + (height - 1))

    p = 0
    for y in range(height):
        gy = (255 * y) // denom_y
        for x in range(width):
            rx = (255 * x) // denom_x
            bx = (255 * (x + y)) // denom_xy
            buf[p + 0] = rx
            buf[p + 1] = gy
            buf[p + 2] = bx
            p += 3

    return Image.frombytes('RGB', (width, height), bytes(buf))


def _run_suite(impl: str, pyperf_args: list[str]) -> int:
    if impl == 'blurhash-python':
        from blurhash import decode as blurhash_decode
        from blurhash import encode as blurhash_encode

        if blurhash_encode.__module__ != 'blurhash':
            raise SystemExit(
                'Expected blurhash-python for --impl blurhash-python, but imported a different "blurhash" package.'
            )

    elif impl == 'blurhash':
        from blurhash import decode as blurhash_decode
        from blurhash import encode as blurhash_encode

        if blurhash_encode.__module__ != 'blurhash.blurhash':
            raise SystemExit(
                'Expected blurhash for --impl blurhash, but imported a different "blurhash" package.'
            )

    else:
        from blurhash_rs import blurhash_decode, blurhash_encode

    from blurhash_rs import _lib
    from blurhash_rs import blurhash_decode as decode_reference
    from blurhash_rs import blurhash_encode as encode_reference

    cases = [
        (32, 32, 'RGB', 4, 3),
        (64, 64, 'RGB', 4, 3),
        (128, 128, 'RGB', 4, 3),
        (96, 54, 'RGB', 4, 3),
        (54, 96, 'RGB', 3, 4),
        (32, 32, 'RGB', 1, 1),
        (32, 32, 'RGB', 9, 9),
        (64, 32, 'RGBA', 4, 3),
        (32, 32, 'L', 4, 3),
    ]

    # pyperf spawns worker processes by re-running the "program". Since we
    # parse custom CLI args ourselves, we must include them in program_args so
    # workers can parse them too.
    runner = pyperf.Runner(program_args=(sys.argv[0], '--impl', impl))
    runner.metadata['implementation'] = f'{impl} {version(impl)}'
    runner.metadata['benchmark_sha256'] = hashlib.sha256(
        Path(__file__).read_bytes()
    ).hexdigest()
    runner.metadata['native_extension_sha256'] = hashlib.sha256(
        Path(_lib.__file__).read_bytes()
    ).hexdigest()
    runner.metadata['workload'] = (
        'Square and thumbnail RGB/RGBA/grayscale Pillow images; '
        '1x1, 3x4, 4x3 and 9x9 components; preserve input; return RGB images'
    )
    runner.parse_args(pyperf_args)

    def encode_image(image, x_components, y_components):
        if impl == 'blurhash-python':
            return blurhash_encode(
                image.copy() if image.mode == 'RGB' else image,
                x_components,
                y_components,
            )
        if impl == 'blurhash':
            if image.mode != 'RGB':
                image = image.convert('RGB')
            return blurhash_encode(
                _pil_rgb_image_to_3d_array(image),
                x_components,
                y_components,
            )
        return blurhash_encode(image, x_components, y_components)

    def decode_image(encoded, width, height):
        decoded = blurhash_decode(encoded, width, height)
        if impl == 'blurhash':
            data = bytes(
                channel for row in decoded for pixel in row for channel in pixel
            )
            return Image.frombytes('RGB', (width, height), data)
        return decoded

    # Encode benchmarks first (so pyperf compare_to shows them grouped).
    for width, height, mode, x_components, y_components in cases:
        img = _make_gradient_image(width, height)
        if mode != 'RGB':
            img = img.convert(mode)

        original = img.tobytes()
        assert encode_image(img, x_components, y_components) == encode_reference(
            img, x_components, y_components
        )
        assert img.tobytes() == original
        runner.bench_func(
            f'encode {width}x{height} ({x_components}x{y_components})'
            + (f' {mode}' if mode != 'RGB' else ''),
            encode_image,
            img,
            x_components,
            y_components,
        )

    # Decode benchmarks second.
    for width, height, mode, x_components, y_components in cases:
        if mode != 'RGB':
            continue
        img = _make_gradient_image(width, height)
        h = encode_reference(img, x_components, y_components)
        decoded = decode_image(h, width, height)
        assert decoded.mode == 'RGB'
        assert decoded.size == (width, height)
        expected = decode_reference(h, width, height)
        assert all(
            abs(actual - reference) <= 1
            for actual, reference in zip(
                decoded.tobytes(), expected.tobytes(), strict=True
            )
        )
        expected.close()
        decoded.close()

        runner.bench_func(
            f'decode {width}x{height} ({x_components}x{y_components})',
            decode_image,
            h,
            width,
            height,
        )

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description='BlurHash benchmark suite. Writes pyperf JSON via -o/--output.'
    )
    parser.add_argument(
        '--impl', required=True, choices=['blurhash-python', 'blurhash', 'blurhash-rs']
    )
    args, pyperf_args = parser.parse_known_args()
    return _run_suite(args.impl, pyperf_args)


if __name__ == '__main__':
    main()
