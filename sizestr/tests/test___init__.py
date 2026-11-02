import pytest

from sizestr import sizestr


@pytest.mark.parametrize(
    ('size', 'expected'),
    [
        (10000, '9.77 KiB'),
        (1024, '1.00 KiB'),
        (1023, '1023 B'),
        (1, '1 B'),
        (1e-308, '0 B'),
        (0.0, '0 B'),
        (0, '0 B'),
        (-1, '-1 B'),
        (-1023, '-1023 B'),
        (float('inf'), '(inf)'),
        (float('-inf'), '(-inf)'),
        (float('NaN'), '(nan)'),
        (1e24, '847.0 ZiB'),
        (1e25, '(too large to display)'),
        (1024**5 - 1, '1024.0 TiB'),
        (1024**5, '1.00 PiB'),
        (1024**8 - 1, '1024.0 ZiB'),
        (1024**8, '(too large to display)'),
        (float.fromhex('0x1.fffffffffffffp+79'), '1024.0 ZiB'),
        pytest.param(10**400, '(too large to display)', id='huge-positive-integer'),
        pytest.param(-(10**400), '(too large to display)', id='huge-negative-integer'),
    ],
)
def test_sizestr(size, expected):
    assert sizestr(size) == expected
