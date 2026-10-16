import argparse
import sys
from importlib.metadata import version

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

    from blurhash_rs import blurhash_decode as decode_reference
    from blurhash_rs import blurhash_encode as encode_reference

    sizes = [32, 64, 128]
    x_components = 4
    y_components = 3

    # pyperf spawns worker processes by re-running the "program". Since we
    # parse custom CLI args ourselves, we must include them in program_args so
    # workers can parse them too.
    runner = pyperf.Runner(program_args=(sys.argv[0], '--impl', impl))
    runner.metadata['implementation'] = f'{impl} {version(impl)}'
    runner.metadata['workload'] = (
        'RGB Pillow images; 4x3 components; preserve input; return image'
    )
    runner.parse_args(pyperf_args)

    def encode_image(image):
        if impl == 'blurhash-python':
            return blurhash_encode(image.copy(), x_components, y_components)
        if impl == 'blurhash':
            return blurhash_encode(
                _pil_rgb_image_to_3d_array(image), x_components, y_components
            )
        return blurhash_encode(image, x_components, y_components)

    def decode_image(encoded, size):
        decoded = blurhash_decode(encoded, size, size)
        if impl == 'blurhash':
            data = bytes(
                channel for row in decoded for pixel in row for channel in pixel
            )
            return Image.frombytes('RGB', (size, size), data)
        return decoded

    # Encode benchmarks first (so pyperf compare_to shows them grouped).
    for size in sizes:
        img = _make_gradient_image(size, size)

        original = img.tobytes()
        assert encode_image(img) == encode_reference(img, x_components, y_components)
        assert img.tobytes() == original
        runner.bench_func(
            f'encode {size}x{size} ({x_components}x{y_components})',
            encode_image,
            img,
        )

    # Decode benchmarks second.
    for size in sizes:
        img = _make_gradient_image(size, size)
        h = encode_reference(img, x_components, y_components)
        decoded = decode_image(h, size)
        assert decoded.mode == 'RGB'
        assert decoded.size == (size, size)
        expected = decode_reference(h, size, size)
        assert all(
            abs(actual - reference) <= 1
            for actual, reference in zip(
                decoded.tobytes(), expected.tobytes(), strict=True
            )
        )
        expected.close()
        decoded.close()

        runner.bench_func(
            f'decode {size}x{size} ({x_components}x{y_components})',
            decode_image,
            h,
            size,
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
