from math import isclose, sqrt

import pytest
from osm_shortlink import shortlink_decode, shortlink_encode


@pytest.mark.parametrize(
    'input',
    [
        (0, 0, 5),
        (156, 45, 17),
        (1.23456789, 2.34567891, 20),
        (-1.23456789, -2.34567891, 20),
        (119.99999999, 39.99999999, 21),
        (15.545454, 45.454545, 13),
    ],
)
def test_encode_decode(input: tuple[float, float, int]):
    encoded = shortlink_encode(*input)
    decoded = shortlink_decode(encoded)
    assert input[2] == decoded[2]  # zoom must be equal
    distance = sqrt((input[0] - decoded[0]) ** 2 + (input[1] - decoded[1]) ** 2)
    max_distance = 360 / (2 ** (input[2] + 8)) * 0.5 * sqrt(5)
    assert max_distance > distance


def test_encode_wrapping():
    assert shortlink_encode(720, 0, 5) == shortlink_encode(0, 0, 5)


@pytest.mark.parametrize(
    'lat',
    [-91, -90, 90, 91],
)
def test_encode_lat(lat):
    shortlink_encode(0, lat, 5)


@pytest.mark.parametrize(
    ('lon', 'lat', 'expected_lon', 'expected_lat'),
    [
        (0, 100, 180, 80),
        (0, -100, 180, -80),
        (10, 180, 190, 0),
        (10, -180, 190, 0),
    ],
)
def test_encode_lat_wrapping(lon, lat, expected_lon, expected_lat):
    assert shortlink_encode(lon, lat, 5) == shortlink_encode(
        expected_lon, expected_lat, 5
    )


@pytest.mark.parametrize(
    ('input', 'expected'),
    [
        ('0EEQjE--', (0.0550, 51.5110, 9)),
        ('0OP4tXGMB', (19.57922, 51.87695, 19)),
        ('ecetE--', (-31.113, 64.130, 6)),
    ],
)
def test_decode(input, expected):
    decoded = shortlink_decode(input)
    for a, b in zip(expected, decoded):
        assert isclose(a, b, abs_tol=0.01)


@pytest.mark.parametrize(
    ('new', 'old'),
    [
        ('--~v2juONc', '@v2juONc=-'),
        ('as3I3GpG~-', 'as3I3GpG@='),
        ('D~hV--', 'D@hV--'),
        ('CO0O-~m8-', 'CO0O@m8--'),
    ],
)
def test_decode_deprecated(new, old):
    decoded1 = shortlink_decode(new)
    decoded2 = shortlink_decode(old)
    for a, b in zip(decoded1, decoded2):
        assert isclose(a, b)
