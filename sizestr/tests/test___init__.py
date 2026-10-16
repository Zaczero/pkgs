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
        pytest.param(10**400, '(too large to display)', id='huge-positive-integer'),
        pytest.param(-(10**400), '(too large to display)', id='huge-negative-integer'),
    ],
)
def test_sizestr(size, expected):
    assert sizestr(size) == expected
