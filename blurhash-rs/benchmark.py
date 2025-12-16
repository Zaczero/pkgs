import argparse
import sys

import pyperf
from PIL import Image


def _pil_rgb_image_to_3d_array(img: Image.Image) -> list[list[list[int]]]:
    data = list(img.getdata())  # type: ignore
    pixels: list[list[list[int]]] = []
    p = 0
    for _y in range(img.height):
        row: list[list[int]] = []
        for _x in range(img.width):
            r, g, b = data[p]
            row.append([r, g, b])
            p += 1
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

    sizes = [32, 64, 128]
    x_components = 4
    y_components = 3

    # pyperf spawns worker processes by re-running the "program". Since we
    # parse custom CLI args ourselves, we must include them in program_args so
    # workers can parse them too.
    runner = pyperf.Runner(program_args=(sys.argv[0], '--impl', impl))
    runner.parse_args(pyperf_args)

    # Encode benchmarks first (so pyperf compare_to shows them grouped).
    for size in sizes:
        img = _make_gradient_image(size, size)

        if impl == 'blurhash-python':
            # blurhash-python closes the image it is given; avoid breaking the
            # benchmark loop by making close() a no-op.
            img.close = lambda: None  # type: ignore[method-assign]
        elif impl == 'blurhash':
            img = _pil_rgb_image_to_3d_array(img)

        runner.bench_func(
            f'encode {size}x{size} ({x_components}x{y_components})',
            blurhash_encode,
            img,
            x_components,
            y_components,
        )

    # Decode benchmarks second.
    from blurhash_rs import blurhash_encode

    for size in sizes:
        img = _make_gradient_image(size, size)
        h = blurhash_encode(img, x_components, y_components)

        runner.bench_func(
            f'decode {size}x{size} ({x_components}x{y_components})',
            blurhash_decode,
            h,
            size,
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
