from math import ulp

import pytest
from polyline_rs import decode_latlon, decode_lonlat, encode_latlon, encode_lonlat


@pytest.mark.parametrize(
    ('coords', 'precision', 'expected'),
    [
        (
            [(38.5, -120.2), (40.7, -120.95), (43.252, -126.453)],
            5,
            '_p~iF~ps|U_ulLnnqC_mqNvxq`@',
        ),
        (
            [
                (51.77263, 18.08583),
                (51.77261, 18.08586),
                (51.77261, 18.08589),
                (51.77263, 18.08596),
                (51.77269, 18.08604),
                (51.77301, 18.08661),
                (51.77302, 18.08677),
                (51.77302, 18.08702),
            ],
            5,
            '}y~zHmkkmBBE?ECMKO_AqBA_@?q@',
        ),
        (
            [
                (51.772627, 18.085833),
                (51.772611, 18.085859),
                (51.772606, 18.085893),
                (51.772613, 18.085927),
                (51.772628, 18.085957),
                (51.772685, 18.086038),
                (51.773014, 18.086607),
                (51.773023, 18.086775),
                (51.773022, 18.08694),
                (51.77302, 18.08702),
            ],
            6,
            'el}vaBq{zna@^s@HcAMcA]{@qBaDqSqb@QoI@iIB_D',
        ),
    ],
)
def test_encode_decode(coords, precision, expected):
    coords_iter = ((v for v in c) for c in coords)
    coords_lonlat = tuple((x, y) for y, x in coords)
    assert encode_latlon(coords, precision) == expected
    assert encode_latlon(coords_iter, precision) == expected
    assert encode_lonlat(coords_lonlat, precision) == expected
    # These golden coordinates lie on the encoded grid; decoding may differ
    # from the Python literal by one binary64 ulp, never by a whole grid unit.
    assert all(
        actual == pytest.approx(expected_value, abs=ulp(expected_value))
        for decoded, expected_coord in zip(
            decode_latlon(expected, precision), coords, strict=True
        )
        for actual, expected_value in zip(decoded, expected_coord, strict=True)
    )
    assert all(
        actual == pytest.approx(expected_value, abs=ulp(expected_value))
        for decoded, expected_coord in zip(
            decode_lonlat(expected, precision), coords_lonlat, strict=True
        )
        for actual, expected_value in zip(decoded, expected_coord, strict=True)
    )


@pytest.mark.parametrize(
    'polyline',
    [
        '\x01',
        '\x7f',
        '~' * 8,
        '_',
        '?',
    ],
)
def test_decode_rejects_malformed_input(polyline):
    with pytest.raises(ValueError, match='Invalid polyline'):
        decode_latlon(polyline)
    with pytest.raises(ValueError, match='Invalid polyline'):
        decode_lonlat(polyline)


def test_encode_rounds_scaled_values_per_spec():
    # 0.000006 * 1e5 = 0.6 must round to 1 (the reference implementations
    # round; truncation would encode 0 and bias toward zero).
    assert encode_latlon([(0.000006, 0.000002)], 5) == 'A?'
    assert encode_latlon([(-0.000006, -0.000002)], 5) == '@?'


@pytest.mark.parametrize(
    ('encoder', 'coordinates', 'precision', 'message'),
    [
        pytest.param(
            encode_latlon,
            [(30.0, 0.0)],
            8,
            'coordinate at index 0: latitude',
            id='precision-8-latitude-30',
        ),
        pytest.param(
            encode_lonlat,
            [(0.0, 30.0)],
            8,
            'coordinate at index 0: latitude',
            id='precision-8-lonlat-order',
        ),
        pytest.param(
            encode_latlon,
            [(0.0, 180.0), (0.0, -180.0)],
            7,
            'coordinate at index 1: longitude delta',
            id='longitude-delta-exceeds-i32',
        ),
        pytest.param(
            encode_lonlat,
            [(180.0, 0.0), (-180.0, 0.0)],
            7,
            'coordinate at index 1: longitude delta',
            id='lonlat-large-delta',
        ),
    ],
)
def test_encode_rejects_unrepresentable_coordinates_and_deltas(
    encoder, coordinates, precision, message
):
    with pytest.raises(ValueError, match=message):
        encoder(coordinates, precision)


@pytest.mark.parametrize(
    ('coordinates', 'precision', 'expected'),
    [
        ([(21.47483647, 0.0)], 7, (21.47483647, 0.0)),
        ([(-21.47483648, 0.0)], 7, (-21.47483648, 0.0)),
    ],
)
def test_encode_accepts_signed_i32_coordinate_endpoints(
    coordinates, precision, expected
):
    encoded = encode_latlon(coordinates, precision)
    decoded = decode_latlon(encoded, precision)[0]
    assert decoded == pytest.approx(expected, abs=1e-7)


@pytest.mark.parametrize(
    ('coordinates', 'axis'),
    [
        ([(float('nan'), 0.0)], 'latitude'),
        ([(float('inf'), 0.0)], 'latitude'),
        ([(0.0, float('nan'))], 'longitude'),
        ([(0.0, float('-inf'))], 'longitude'),
    ],
)
def test_encode_rejects_nonfinite_coordinates(coordinates, axis):
    with pytest.raises(ValueError, match=f'coordinate at index 0: {axis}'):
        encode_latlon(coordinates, 5)


@pytest.mark.parametrize('precision', [-(2**31), -299, 309, 2**31 - 1])
def test_encode_and_decode_reject_unusable_precision_scales(precision):
    for encoder in (encode_latlon, encode_lonlat):
        with pytest.raises(ValueError, match=f'Invalid precision {precision}'):
            encoder([], precision)
    for decoder in (decode_latlon, decode_lonlat):
        with pytest.raises(ValueError, match=f'Invalid precision {precision}'):
            decoder('', precision)


@pytest.mark.parametrize('precision', [-298, 308])
def test_encode_and_decode_accept_finite_precision_scale_boundaries(precision):
    assert encode_latlon([], precision) == ''
    assert encode_lonlat([], precision) == ''
    assert decode_latlon('', precision) == []
    assert decode_lonlat('', precision) == []
