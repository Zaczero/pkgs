"""Geometry collection nesting is bounded at the Shape admission owner."""

from __future__ import annotations

import pickle
import struct
import subprocess
import sys

import gometry as gm
import pytest


def nested_constructor(depth: int) -> gm.Geometry:
    shape: gm.Geometry = gm.Point(0, 0)
    for _ in range(depth):
        shape = gm.GeometryCollection([shape])
    return shape


def nested_wkt(depth: int) -> str:
    return 'GEOMETRYCOLLECTION (' * depth + 'POINT (0 0)' + ')' * depth


def nested_wkb(depth: int) -> bytes:
    value = struct.pack('<BIdd', 1, 1, 0.0, 0.0)
    for _ in range(depth):
        value = struct.pack('<BII', 1, 7, 1) + value
    return value


def test_constructor_accepts_127_and_128_and_rejects_129() -> None:
    assert nested_constructor(127).geometry_type == 'GeometryCollection'
    assert nested_constructor(128).geometry_type == 'GeometryCollection'
    with pytest.raises(gm.InvalidGeometryError, match='nesting exceeds'):
        nested_constructor(129)


@pytest.mark.parametrize('depth', [127, 128])
def test_wkt_accepts_boundary(depth: int) -> None:
    assert gm.from_wkt(nested_wkt(depth)).geometry_type == 'GeometryCollection'


def test_wkt_rejects_129() -> None:
    with pytest.raises(gm.ParseError, match='nesting exceeds'):
        gm.from_wkt(nested_wkt(129))


@pytest.mark.parametrize('depth', [127, 128])
def test_wkb_accepts_boundary(depth: int) -> None:
    assert gm.from_wkb(nested_wkb(depth)).geometry_type == 'GeometryCollection'


def test_wkb_rejects_129() -> None:
    with pytest.raises(gm.ParseError, match='nesting exceeds'):
        gm.from_wkb(nested_wkb(129))


@pytest.mark.parametrize('depth', [127, 128])
def test_pickle_accepts_boundary(depth: int) -> None:
    shape = nested_constructor(depth)
    restored = pickle.loads(pickle.dumps(shape))
    assert restored.to_wkt() == shape.to_wkt()


def test_pickle_reconstruction_rejects_129() -> None:
    import gometry._lib as native

    with pytest.raises(gm.ParseError, match='nesting exceeds'):
        native._unpickle_geometry(nested_wkb(129), None, None)


def test_constructor_rejects_50000_without_process_crash() -> None:
    script = """
import gometry as gm

shape = gm.Point(0, 0)
try:
    for _ in range(50_000):
        shape = gm.GeometryCollection([shape])
except gm.InvalidGeometryError as error:
    assert 'nesting exceeds' in str(error)
    print('typed nesting error')
else:
    raise AssertionError('50,000 nested constructors were accepted')
"""
    result = subprocess.run(
        [sys.executable, '-c', script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == 'typed nesting error'
