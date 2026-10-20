"""Pillow workflows measured with pyperf, with image setup excluded.

The default dev environment supports blurhash-rs and blurhash. For
blurhash-python, use ``uv run --no-dev --group compare-cffi`` in a separate
environment: both comparator packages provide the ``blurhash`` import.
Save runs with ``-o`` and compare matching cases with
``python -m pyperf compare_to baseline.json candidate.json``.
"""

import argparse
import hashlib
import json
import subprocess
import sys
from importlib.metadata import version
from pathlib import Path

import numpy as np
import pyperf
from PIL import Image


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


CASE_DEFINITIONS = (
    {
        'name': 'encode 32x32 (4x3)',
        'operation': 'encode',
        'group': 'square-size',
        'width': 32,
        'height': 32,
        'mode': 'RGB',
        'x_components': 4,
        'y_components': 3,
        'label': 'Encode · 32 x 32 square · RGB · 4 x 3 components',
        'x': 32,
    },
    {
        'name': 'encode 64x64 (4x3)',
        'operation': 'encode',
        'group': 'square-size',
        'width': 64,
        'height': 64,
        'mode': 'RGB',
        'x_components': 4,
        'y_components': 3,
        'label': 'Encode · 64 x 64 square · RGB · 4 x 3 components',
        'x': 64,
    },
    {
        'name': 'encode 128x128 (4x3)',
        'operation': 'encode',
        'group': 'square-size',
        'width': 128,
        'height': 128,
        'mode': 'RGB',
        'x_components': 4,
        'y_components': 3,
        'label': 'Encode · 128 x 128 square · RGB · 4 x 3 components',
        'x': 128,
    },
    {
        'name': 'encode 96x54 (4x3)',
        'operation': 'encode',
        'group': 'aspect-ratio',
        'width': 96,
        'height': 54,
        'mode': 'RGB',
        'x_components': 4,
        'y_components': 3,
        'label': 'Encode · 96 x 54 landscape · RGB · 4 x 3 components',
    },
    {
        'name': 'encode 54x96 (3x4)',
        'operation': 'encode',
        'group': 'aspect-ratio',
        'width': 54,
        'height': 96,
        'mode': 'RGB',
        'x_components': 3,
        'y_components': 4,
        'label': 'Encode · 54 x 96 portrait · RGB · 3 x 4 components',
    },
    {
        'name': 'encode 32x32 (1x1)',
        'operation': 'encode',
        'group': 'component-count',
        'width': 32,
        'height': 32,
        'mode': 'RGB',
        'x_components': 1,
        'y_components': 1,
        'label': 'Encode · 32 x 32 square · RGB · 1 x 1 component',
    },
    {
        'name': 'encode 32x32 (9x9)',
        'operation': 'encode',
        'group': 'component-count',
        'width': 32,
        'height': 32,
        'mode': 'RGB',
        'x_components': 9,
        'y_components': 9,
        'label': 'Encode · 32 x 32 square · RGB · 9 x 9 components',
    },
    {
        'name': 'encode 64x32 (4x3) RGBA',
        'operation': 'encode',
        'group': 'input-mode',
        'width': 64,
        'height': 32,
        'mode': 'RGBA',
        'x_components': 4,
        'y_components': 3,
        'label': 'Encode · 64 x 32 landscape · RGBA input · 4 x 3 components',
    },
    {
        'name': 'encode 32x32 (4x3) L',
        'operation': 'encode',
        'group': 'input-mode',
        'width': 32,
        'height': 32,
        'mode': 'L',
        'x_components': 4,
        'y_components': 3,
        'label': 'Encode · 32 x 32 square · grayscale input · 4 x 3 components',
    },
    {
        'name': 'decode 32x32 (4x3)',
        'operation': 'decode',
        'group': 'square-size',
        'width': 32,
        'height': 32,
        'mode': 'RGB',
        'x_components': 4,
        'y_components': 3,
        'label': 'Decode · 32 x 32 square · RGB · 4 x 3 components',
        'x': 32,
    },
    {
        'name': 'decode 64x64 (4x3)',
        'operation': 'decode',
        'group': 'square-size',
        'width': 64,
        'height': 64,
        'mode': 'RGB',
        'x_components': 4,
        'y_components': 3,
        'label': 'Decode · 64 x 64 square · RGB · 4 x 3 components',
        'x': 64,
    },
    {
        'name': 'decode 128x128 (4x3)',
        'operation': 'decode',
        'group': 'square-size',
        'width': 128,
        'height': 128,
        'mode': 'RGB',
        'x_components': 4,
        'y_components': 3,
        'label': 'Decode · 128 x 128 square · RGB · 4 x 3 components',
        'x': 128,
    },
    {
        'name': 'decode 96x54 (4x3)',
        'operation': 'decode',
        'group': 'aspect-ratio',
        'width': 96,
        'height': 54,
        'mode': 'RGB',
        'x_components': 4,
        'y_components': 3,
        'label': 'Decode · 96 x 54 landscape · RGB · 4 x 3 components',
    },
    {
        'name': 'decode 54x96 (3x4)',
        'operation': 'decode',
        'group': 'aspect-ratio',
        'width': 54,
        'height': 96,
        'mode': 'RGB',
        'x_components': 3,
        'y_components': 4,
        'label': 'Decode · 54 x 96 portrait · RGB · 3 x 4 components',
    },
    {
        'name': 'decode 32x32 (1x1)',
        'operation': 'decode',
        'group': 'component-count',
        'width': 32,
        'height': 32,
        'mode': 'RGB',
        'x_components': 1,
        'y_components': 1,
        'label': 'Decode · 32 x 32 square · RGB · 1 x 1 component',
    },
    {
        'name': 'decode 32x32 (9x9)',
        'operation': 'decode',
        'group': 'component-count',
        'width': 32,
        'height': 32,
        'mode': 'RGB',
        'x_components': 9,
        'y_components': 9,
        'label': 'Decode · 32 x 32 square · RGB · 9 x 9 components',
    },
)


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
    runner.metadata['native_package_version'] = version('blurhash-rs')
    runner.metadata['numpy_version'] = np.__version__
    runner.metadata['pillow_version'] = version('Pillow')
    runner.metadata['pyperf_version'] = pyperf.__version__
    dependencies = {
        'Pillow': version('Pillow'),
        'NumPy': np.__version__,
        'pyperf': pyperf.__version__,
    }
    if impl == 'blurhash-python':
        dependencies['cffi'] = version('cffi')
        dependencies['six'] = version('six')
    runner.metadata['implementation_dependencies'] = json.dumps(
        dependencies, sort_keys=True
    )
    runner.metadata['source_revision'] = subprocess.check_output(
        ['git', 'rev-parse', 'HEAD'], text=True
    ).strip()
    runner.metadata['report_sha256'] = hashlib.sha256(
        Path(__file__).with_name('tools').joinpath('benchmark_report.py').read_bytes()
    ).hexdigest()
    runner.metadata['case_definitions'] = json.dumps(
        CASE_DEFINITIONS, separators=(',', ':'), sort_keys=True
    )
    runner.metadata['workload'] = (
        'Pillow images; encode RGB square-size, landscape/portrait, component-count, '
        'RGBA and grayscale cases; decode RGB square-size, aspect-ratio and '
        'component-count cases. The blurhash adapter uses NumPy arrays; '
        'blurhash-python receives a copied Pillow image because its encoder closes '
        'its input. Encoding preserves caller images and decoding returns RGB '
        'Pillow images.'
    )
    runner.parse_args(pyperf_args)

    def encode_image(image, x_components, y_components):
        if impl == 'blurhash-python':
            return blurhash_encode(
                image.copy(),
                x_components,
                y_components,
            )
        if impl == 'blurhash':
            rgb = image if image.mode == 'RGB' else image.convert('RGB')
            return blurhash_encode(
                np.asarray(rgb),
                x_components,
                y_components,
            )
        return blurhash_encode(image, x_components, y_components)

    def decode_image(encoded, width, height):
        decoded = blurhash_decode(encoded, width, height)
        if impl == 'blurhash':
            return Image.fromarray(np.asarray(decoded, dtype=np.uint8))
        return decoded

    # Encode benchmarks first (so pyperf compare_to shows them grouped).
    for case in CASE_DEFINITIONS:
        if case['operation'] != 'encode':
            continue
        width = case['width']
        height = case['height']
        mode = case['mode']
        x_components = case['x_components']
        y_components = case['y_components']
        img = _make_gradient_image(width, height)
        if mode != 'RGB':
            img = img.convert(mode)

        original = img.tobytes()
        assert encode_image(img, x_components, y_components) == encode_reference(
            img, x_components, y_components
        )
        assert img.tobytes() == original
        runner.bench_func(
            case['name'],
            encode_image,
            img,
            x_components,
            y_components,
        )

    # Decode benchmarks second.
    for case in CASE_DEFINITIONS:
        if case['operation'] != 'decode':
            continue
        width = case['width']
        height = case['height']
        x_components = case['x_components']
        y_components = case['y_components']
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
            case['name'],
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
