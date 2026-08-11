"""GeoJSON full numeric fidelity (R15-G).

Guarantees:
  B1. Finite floats text↔geometry↔text are bit-identical (no silent 1 ULP drift).
  B2. Integer coordinates are admitted only when exactly representable as f64;
      non-exact integers raise ParseError (never silently rounded).
  B3. TEXT (`from_geojson` str/bytes) and MAPPING (dict / __geo_interface__) admit
      the same coordinate values and produce bit-identical geometry.

Assert on float.hex() / raw bits — never formatted-text equality alone.
No timing assertions.

Reversion proofs (manual):
  - Drop `float_roundtrip` from serde_json features in Cargo.toml →
    ``test_geojson_hostile_float_round_trip_bit_identity`` (and the long-decimal
    cases) go RED on 1 ULP mismatches.
  - Restore ``reject`` of every |n| > 2^53 → exact large-int tests go RED.
  - Revert mapping to ``extract::<i64>()`` only → large exact PyLong / path
    equivalence tests go RED.
"""

from __future__ import annotations

import math
import struct
import subprocess
import sys

import gometry as gm
import pytest


def _bits(value: float) -> int:
    return struct.unpack('>Q', struct.pack('>d', value))[0]


def _assert_bits_equal(left: float, right: float, *, label: str = '') -> None:
    assert _bits(left) == _bits(right), (
        f'{label}: {left.hex()} != {right.hex()} (bits {_bits(left)} vs {_bits(right)})'
    )


# Hostile finite doubles (within WGS84 lon domain so default/CRS-free parse works).
_HOSTILE_FLOATS: list[float] = [
    0.1,
    0.3,
    1.0000000000000002,  # needs 17 significant digits
    1e-323,  # subnormal
    5e-324,  # min positive subnormal
    -0.0,
    2.2250738585072014e-308,  # min normal
    math.nextafter(1.0, 2.0),
    1.2345678901234567,
    179.99999999999997,
    -179.99999999999997,
    math.nextafter(0.0, 1.0),
    1e-15,
    1e-16,
    1e-17,
    # Long expansion that default serde_json mis-rounded by 1 ULP before float_roundtrip:
    float('1.23991866549017345278116408735513687133789062500000e+02'),
]


# Long decimal literals (correctly-rounded target = Python float()).
_LONG_DECIMALS: list[str] = [
    '0.1000000000000000055511151231257827021181583404541015625',
    '0.3000000000000000444089209850062616169452667236328125',
    '1.0000000000000002220446049250313080847263336181640625',
    '1.23456789012345678901234567890',
    '0.123456789012345678901234567890',
    '1.23991866549017345278116408735513687133789062500000e+02',
    '9.999999999999998e-1',
    '1.234567890123456789e-10',
]


# Exactly-representable integers (use Z so domain does not reject magnitude).
_EXACT_INTS: list[int] = [
    0,
    1,
    -1,
    2**53,
    2**53 + 2,  # first even integer past 2^53 — exact, was wrongly rejected
    2**54,
    2**60,
    10**18,  # exact in binary64
    -(2**63),  # i64::MIN = -2^63 — exact power of two
    2**100,  # beyond u64; exact power of two
]

# Not exactly representable as f64 (reject on integer token / Python int).
# Includes cast-saturation edges: i64::MAX / u64::MAX round to 2^63 / 2^64 as
# f64; a Rust `as i64`/`as u64` check false-passes them via saturation.
_INEXACT_INTS: list[int] = [
    2**53 + 1,
    2**53 + 3,
    9007199254740993,  # 2^53+1
    2**63 - 1,  # i64::MAX
    2**64 - 1,  # u64::MAX
    2**100 + 1,
]


def test_geojson_hostile_float_round_trip_bit_identity() -> None:
    for value in _HOSTILE_FLOATS:
        geom = gm.Point(value, 0.0)
        text = geom.to_geojson()
        back = gm.from_geojson(text, crs=None)
        _assert_bits_equal(back.x, value, label=f'write→read {value!r}')
        _assert_bits_equal(back.x, geom.x, label=f'identity {value!r}')
        # Second emission must match the first (stable shortest form).
        assert back.to_geojson() == text


def test_geojson_long_decimal_matches_correctly_rounded_float() -> None:
    for literal in _LONG_DECIMALS:
        expected = float(literal)
        if not (-180.0 <= expected <= 180.0):
            continue
        text = f'{{"type":"Point","coordinates":[{literal},0]}}'
        geom = gm.from_geojson(text, crs=None)
        _assert_bits_equal(geom.x, expected, label=f'long decimal {literal[:40]}')


def test_geojson_exact_integers_admitted_on_z() -> None:
    for n in _EXACT_INTS:
        assert float(n) == n  # fixture must be exact
        expected = float(n)
        text = f'{{"type":"Point","coordinates":[0,0,{n}]}}'
        from_text = gm.from_geojson(text, crs=None)
        from_map = gm.from_geojson(
            {'type': 'Point', 'coordinates': [0, 0, n]}, crs=None
        )
        _assert_bits_equal(from_text.z, expected, label=f'TEXT exact int {n}')
        _assert_bits_equal(from_map.z, expected, label=f'MAP exact int {n}')
        _assert_bits_equal(from_text.z, from_map.z, label=f'TEXT/MAP exact int {n}')


def test_geojson_inexact_integers_rejected() -> None:
    for n in _INEXACT_INTS:
        assert float(n) != n  # fixture must be inexact
        text = f'{{"type":"Point","coordinates":[0,0,{n}]}}'
        mapping = {'type': 'Point', 'coordinates': [0, 0, n]}
        # Mapping always sees a true Python int → reject non-exact.
        with pytest.raises(gm.ParseError, match='not exactly representable as f64'):
            gm.from_geojson(mapping, crs=None)
        # Text: integer tokens that fit in i64/u64 also reject. Tokens that only
        # fit as JSON floats (magnitude above u64) follow float semantics — see
        # docs/about/compatibility.md — so only assert reject inside u64 range.
        if 0 <= n < 2**64:
            with pytest.raises(gm.ParseError, match='not exactly representable as f64'):
                gm.from_geojson(text, crs=None)
        # from_features must match mapping admission (B3).
        with pytest.raises(gm.ParseError, match='not exactly representable as f64'):
            gm.from_features(
                {
                    'type': 'FeatureCollection',
                    'features': [
                        {
                            'type': 'Feature',
                            'properties': {},
                            'geometry': mapping,
                        }
                    ],
                },
                crs=None,
            )


def test_feature_side_data_keeps_arbitrary_json_integers_exact_on_every_lane() -> None:
    """Properties/ids are opaque Python values, never binary64 staging values."""
    value = 2**100 + 1
    literal = str(value)
    text = (
        '{"type":"FeatureCollection","features":[{'
        '"type":"Feature","geometry":{"type":"Point","coordinates":[1,2]},'
        f'"properties":{{"n":{literal},"nested":[{literal},1.0000000000000002]}},'
        f'"id":{literal}'
        '}]}'
    )
    mapping = {
        'type': 'FeatureCollection',
        'features': [
            {
                'type': 'Feature',
                'geometry': {'type': 'Point', 'coordinates': [1, 2]},
                'properties': {'n': value, 'nested': [value, 1.0000000000000002]},
                'id': value,
            }
        ],
    }
    for lane, source in (
        ('TEXT', text),
        ('BUFFER', bytearray(text, 'utf-8')),
        ('MAPPING', mapping),
    ):
        features = gm.from_features(source)
        properties = features.properties[0]
        assert isinstance(properties['n'], int), lane
        assert properties['n'] == value, lane
        assert isinstance(properties['nested'][0], int), lane
        assert properties['nested'][0] == value, lane
        _assert_bits_equal(properties['nested'][1], 1.0000000000000002, label=lane)
        assert isinstance(features.ids[0], int), lane
        assert features.ids[0] == value, lane


def test_feature_side_data_depth_limit_rejects_cleanly_before_native_recursion() -> (
    None
):
    """A 20k property nest must return ParseError, never fault the process."""
    code = """
import gometry as gm
depth = 20_000
text = ("{\\"type\\":\\"Feature\\",\\"geometry\\":{\\"type\\":\\"Point\\",\\"coordinates\\":[0,0]},\\"properties\\":"
        + "{\\"nested\\":" + "[" * depth + "0" + "]" * depth + "}}")
try:
    gm.from_features(text)
except gm.ParseError as exc:
    assert "nesting depth 128" in str(exc), exc
    print("REJECTED", flush=True)
else:
    raise SystemExit("deep Feature properties were accepted")
"""
    result = subprocess.run(
        [sys.executable, '-c', code],
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == 'REJECTED'


def test_feature_side_data_at_accepted_depth_keeps_one_large_payload_once() -> None:
    """The accepted-depth RawValue path admits one opaque payload without limits."""
    payload = 'x' * (1 << 20)
    depth = 125
    text = (
        '{"type":"Feature","geometry":{"type":"Point","coordinates":[0,0]},'
        '"properties":{"nested":'
        + '[' * depth
        + repr(payload).replace("'", '"')
        + ']' * depth
        + '}}'
    )
    nested = gm.from_features(text).properties[0]['nested']
    for _ in range(depth):
        assert isinstance(nested, list) and len(nested) == 1
        nested = nested[0]
    assert nested == payload


def test_geojson_cast_saturation_edges_rejected_all_paths() -> None:
    """i64::MAX / u64::MAX must reject on TEXT, MAP, and from_features.

    These are the values where `(v as f64) as int == v` false-passes in Rust
    because float→int casts saturate. A green suite without these fixtures
    does not prove B2/B3.
    """
    for n, label in (
        (2**63 - 1, 'i64::MAX'),
        (2**64 - 1, 'u64::MAX'),
    ):
        assert float(n) != n, label
        text = f'{{"type":"Point","coordinates":[0,0,{n}]}}'
        mapping = {'type': 'Point', 'coordinates': [0, 0, n]}
        for _path, call in (
            ('TEXT', lambda t=text: gm.from_geojson(t, crs=None)),
            ('MAP', lambda m=mapping: gm.from_geojson(m, crs=None)),
            (
                'FEATURES',
                lambda m=mapping: gm.from_features(
                    {
                        'type': 'FeatureCollection',
                        'features': [
                            {
                                'type': 'Feature',
                                'properties': {},
                                'geometry': m,
                            }
                        ],
                    },
                    crs=None,
                ),
            ),
        ):
            with pytest.raises(
                gm.ParseError,
                match='not exactly representable as f64',
            ):
                call()


def test_geojson_text_mapping_float_equivalence() -> None:
    for value in _HOSTILE_FLOATS:
        # Full-precision decimal so the text path does not depend on repr().
        literal = format(value, '.24e')
        text_geom = gm.from_geojson(
            f'{{"type":"Point","coordinates":[{literal},0]}}', crs=None
        )
        map_geom = gm.from_geojson(
            {'type': 'Point', 'coordinates': [value, 0.0]}, crs=None
        )
        _assert_bits_equal(text_geom.x, map_geom.x, label=f'TEXT/MAP float {value!r}')
        _assert_bits_equal(text_geom.x, value, label=f'value {value!r}')


def test_geojson_text_mapping_exact_int_equivalence() -> None:
    for n in _EXACT_INTS:
        text_geom = gm.from_geojson(
            f'{{"type":"Point","coordinates":[0,0,{n}]}}', crs=None
        )
        map_geom = gm.from_geojson(
            {'type': 'Point', 'coordinates': [0, 0, n]}, crs=None
        )
        _assert_bits_equal(text_geom.z, map_geom.z, label=f'TEXT/MAP int {n}')
        # from_features mapping path (geometry only via Feature).
        feats = gm.from_features(
            {
                'type': 'FeatureCollection',
                'features': [
                    {
                        'type': 'Feature',
                        'properties': {},
                        'geometry': {'type': 'Point', 'coordinates': [0, 0, n]},
                    }
                ],
            },
            crs=None,
        )
        _assert_bits_equal(feats.geometries[0].z, map_geom.z, label=f'features int {n}')


def test_geojson_negative_zero_preserved() -> None:
    geom = gm.from_geojson('{"type":"Point","coordinates":[-0.0,0.0]}', crs=None)
    assert math.copysign(1.0, geom.x) < 0.0
    _assert_bits_equal(geom.x, -0.0)
    map_geom = gm.from_geojson({'type': 'Point', 'coordinates': [-0.0, 0.0]}, crs=None)
    _assert_bits_equal(map_geom.x, -0.0)
    _assert_bits_equal(map_geom.x, geom.x)


def test_geojson_rejects_inexact_integer_on_xy() -> None:
    """Lon/lat inexact integers fail admission before (or instead of) domain."""
    with pytest.raises(gm.ParseError, match='not exactly representable as f64'):
        gm.from_geojson({'type': 'Point', 'coordinates': [9007199254740993, 0]})
    with pytest.raises(gm.ParseError, match='not exactly representable as f64'):
        gm.from_geojson('{"type":"Point","coordinates":[9007199254740993,0]}')
