from pathlib import Path

import pytest
from blurhash_rs import BlurhashDecodeError, blurhash_decode, blurhash_encode
from PIL import Image


def _assert_bytes_close(
    actual: bytes,
    expected: bytes,
    *,
    max_abs_diff: int,
    max_mean_abs_diff: float,
    max_different_bytes: int,
) -> None:
    assert len(actual) == len(expected)
    diffs = [abs(a - b) for a, b in zip(actual, expected)]
    assert max(diffs) <= max_abs_diff
    assert (sum(diffs) / len(diffs)) <= max_mean_abs_diff
    assert sum(1 for d in diffs if d) <= max_different_bytes


_DATA_DIR = Path(__file__).resolve().parent / 'data'


def _load_rgbhex(filename: str, *, width: int, height: int):
    path = _DATA_DIR / filename
    rgb: list[int] = []

    for raw_line in path.read_text(encoding='utf-8').splitlines():
        line = raw_line.split('#', 1)[0].strip()
        if not line:
            continue

        for token in line.split():
            token = token.strip()
            token = token.removeprefix('0x')

            if len(token) != 6:
                raise AssertionError(f'{path}: expected RRGGBB token, got {token!r}')

            try:
                r = int(token[0:2], 16)
                g = int(token[2:4], 16)
                b = int(token[4:6], 16)
            except ValueError as e:
                raise AssertionError(
                    f'{path}: expected RRGGBB token, got {token!r}'
                ) from e

            rgb.extend((r, g, b))

    expected_len = width * height * 3
    assert len(rgb) == expected_len, (
        f'{path}: expected {expected_len} bytes, got {len(rgb)}'
    )
    return bytes(rgb)


_SMALL_PATTERN_RGB = _load_rgbhex('small_pattern_4x3.rgbhex', width=4, height=3)


def test_encode_1x1_red_matches_reference() -> None:
    img = Image.new('RGB', (1, 1), (255, 0, 0))
    assert blurhash_encode(img, 1, 1) == '00TI:j'


def test_encode_small_pattern_matches_reference() -> None:
    img = Image.frombytes('RGB', (4, 3), _SMALL_PATTERN_RGB)
    assert blurhash_encode(img, 4, 3) == 'LzJkWT?SSs?F~q$um-krE0vfmJ5t'


def test_encode_accepts_rgba_input_ignores_alpha_matches_rgb() -> None:
    rgb = _SMALL_PATTERN_RGB
    rgba = bytearray()
    for i in range(0, len(rgb), 3):
        rgba.extend((
            rgb[i],
            rgb[i + 1],
            rgb[i + 2],
            0 if (i // 3) % 2 == 0 else 255,
        ))

    img = Image.frombytes('RGBA', (4, 3), rgba)
    assert blurhash_encode(img, 4, 3) == 'LzJkWT?SSs?F~q$um-krE0vfmJ5t'


def test_decode_known_hash_matches_reference_bytes() -> None:
    hash_ = 'LEHV6nWB2yk8pyo0adR*.7kCMdnj'
    img = blurhash_decode(hash_, 8, 8)
    assert img.mode == 'RGB'
    assert img.size == (8, 8)

    reference = _load_rgbhex('decode_known_hash_8x8.rgbhex', width=8, height=8)
    _assert_bytes_close(
        img.tobytes(),
        reference,
        max_abs_diff=2,
        max_mean_abs_diff=0.05,
        max_different_bytes=32,
    )


def test_decode_invalid_blurhash_raises() -> None:
    with pytest.raises(BlurhashDecodeError) as excinfo:
        blurhash_decode('not a blurhash', 8, 8)
    assert excinfo.value.blurhash == 'not a blurhash'


def test_decode_whitespace_suffix_raises() -> None:
    with pytest.raises(BlurhashDecodeError):
        blurhash_decode('LEHV6nWB2yk8pyo0adR*.7kCMdnj ', 8, 8)
