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


def test_encode_supports_max_zoom_without_panicking():
    encoded = shortlink_encode(0, 0, 22)
    assert encoded == 'wAAAAAAAAA'
    assert shortlink_decode(encoded) == (0.0, 0.0, 22)


def test_encode_rejects_unsupported_zoom():
    with pytest.raises(
        ValueError,
        match='zoom must be between 0 and 22, got 23',
    ):
        shortlink_encode(0, 0, 23)


@pytest.mark.parametrize('zoom', [-1, 256])
def test_encode_rejects_zoom_outside_unsigned_byte(zoom):
    with pytest.raises(OverflowError):
        shortlink_encode(0.0, 0.0, zoom)


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
    for a, b in zip(expected, decoded, strict=True):
        assert isclose(a, b, abs_tol=0.01)


@pytest.mark.parametrize(
    ('code', 'expected'),
    [
        ('0EEQjE--', (0.054931640625, 51.510772705078125, 9)),
        ('0OP4tXGMB', (19.579224586486816, 51.87694922089577, 19)),
        ('ecetE--', (-31.11328125, 64.127197265625, 6)),
    ],
)
def test_decode_returns_independently_specified_cell_coordinates(code, expected):
    assert shortlink_decode(code) == expected


# Literal tokens pin boundary cells instead of relying on an encoder/decoder
# round trip, including the north pole's saturated final latitude row.
@pytest.mark.parametrize(
    ('latitude', 'expected_code'),
    [
        (-91, 'AAEUU-'),
        (-90, 'gAAAA-'),
        (90, 'VVVVV-'),
        (91, 'VVRBB-'),
    ],
)
def test_encode_latitude_boundaries_to_known_cells(latitude, expected_code):
    assert shortlink_encode(0.0, latitude, 5) == expected_code


def test_encode_known_openstreetmap_shortlink_vector():
    assert shortlink_encode(0.055, 51.511, 9) == '0EEQjE--'


@pytest.mark.parametrize(
    ('lon', 'lat', 'coordinate'),
    [
        (float('nan'), 0.0, 'longitude'),
        (float('inf'), 0.0, 'longitude'),
        (0.0, float('nan'), 'latitude'),
        (0.0, float('-inf'), 'latitude'),
    ],
)
def test_encode_rejects_nonfinite_coordinates(lon, lat, coordinate):
    with pytest.raises(ValueError, match=f'{coordinate} must be finite'):
        shortlink_encode(lon, lat, 5)


def test_zoom_error_precedes_nonfinite_coordinate_error():
    with pytest.raises(ValueError, match='zoom must be'):
        shortlink_encode(float('nan'), 0.0, 23)


@pytest.mark.parametrize(
    'input',
    [
        'X',
        'A' * 11,
        '---',
    ],
)
def test_decode_rejects_malformed_input(input):
    with pytest.raises(ValueError, match='shortlink is too'):
        shortlink_decode(input)


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
    for a, b in zip(decoded1, decoded2, strict=True):
        assert isclose(a, b)
