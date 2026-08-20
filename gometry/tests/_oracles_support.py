from __future__ import annotations

import math
from typing import Any

import gometry as gm
import numpy as np
import pytest

FLOAT_RTOL = 1e-09
FLOAT_ATOL = 1e-12
HAUSDORFF_ATOL = 1e-09


def normalize_geometry_wkt(geom: gm.Geometry) -> str:
    """Canonical geometry presentation for snapshot compare/capture."""
    return ((geom).normalize()).to_wkt()


def error_snapshot(exc: BaseException) -> dict[str, str]:
    """Stable error lane for golden capture/compare."""
    message = str(exc).strip().lower()
    key = message.split(',')[0].split(';')[0][:80]
    key = '_'.join(key.split())
    return {'kind': 'error', 'class': type(exc).__name__, 'message_key': key}


def round_float(value: float) -> float:
    if not math.isfinite(value):
        return value
    return float(f'{value:.12g}')


def geometries_match(left: gm.Geometry, right: gm.Geometry) -> bool:
    """Normalized WKT equality with a Hausdorff fallback."""
    if left.is_empty and right.is_empty:
        return left.geometry_type == right.geometry_type
    if normalize_geometry_wkt(left) == normalize_geometry_wkt(right):
        return True
    try:
        return gm.hausdorff_distance(left, right) <= HAUSDORFF_ATOL
    except Exception:
        return False


def floats_match(expected: Any, actual: Any) -> None:
    if isinstance(expected, list):
        assert isinstance(actual, list)
        assert len(expected) == len(actual)
        for left, right in zip(expected, actual, strict=True):
            floats_match(left, right)
        return
    if expected is None or actual is None:
        assert expected is None and actual is None
        return
    if isinstance(expected, float) and math.isnan(expected):
        assert isinstance(actual, float) and math.isnan(actual)
        return
    assert isinstance(actual, (int, float))
    assert float(actual) == pytest.approx(
        float(expected), rel=FLOAT_RTOL, abs=FLOAT_ATOL
    )


def snapshot_values_match(expected: Any, actual: Any) -> None:
    """Compare serialized snapshot payloads with kernel-parity tolerances."""
    if isinstance(expected, dict) and expected.get('kind') == 'error':
        assert isinstance(actual, dict) and actual.get('kind') == 'error'
        assert actual['class'] == expected['class']
        assert actual['message_key'] == expected['message_key']
        return
    if isinstance(expected, dict) and expected.get('kind') == 'geometry':
        assert isinstance(actual, dict) and actual.get('kind') == 'geometry'
        left = gm.from_wkt(expected['wkt'])
        right = gm.from_wkt(actual['wkt'])
        assert geometries_match(left, right)
        return
    if isinstance(expected, dict) and expected.get('kind') == 'geometry_list':
        assert isinstance(actual, dict) and actual.get('kind') == 'geometry_list'
        assert len(expected['wkt']) == len(actual['wkt'])
        for left_wkt, right_wkt in zip(expected['wkt'], actual['wkt'], strict=True):
            assert geometries_match(gm.from_wkt(left_wkt), gm.from_wkt(right_wkt))
        return
    if isinstance(expected, dict) and expected.get('kind') == 'float':
        floats_match(expected['value'], actual['value'])
        return
    if isinstance(expected, dict) and expected.get('kind') == 'bool':
        assert actual == {'kind': 'bool', 'value': expected['value']}
        return
    if isinstance(expected, dict) and expected.get('kind') == 'str':
        assert actual == expected
        return
    if isinstance(expected, dict) and expected.get('kind') == 'bytes':
        assert actual == expected
        return
    if isinstance(expected, dict) and expected.get('kind') == 'json':
        assert actual == expected
        return
    if isinstance(expected, dict) and expected.get('kind') == 'cell':
        assert actual == expected
        return
    if isinstance(expected, dict) and expected.get('kind') == 'cell_list':
        assert actual == expected
        return
    if isinstance(expected, dict) and expected.get('kind') == 'coords':
        assert actual == expected
        return
    if isinstance(expected, dict) and expected.get('kind') == 'validation':
        assert actual == expected
        return
    if isinstance(expected, dict) and expected.get('kind') == 'groups':
        assert actual == expected
        return
    raise AssertionError(f'unsupported snapshot kind: {expected!r}')


def serialize_snapshot_value(value: object) -> object:
    """Deterministic JSON-friendly snapshot encoding."""
    if isinstance(value, gm.Geometry):
        return {'kind': 'geometry', 'wkt': normalize_geometry_wkt(value)}
    if isinstance(value, gm.GeometryArray):
        return {
            'kind': 'geometry_list',
            'wkt': [normalize_geometry_wkt(geom) for geom in value],
        }
    if isinstance(value, gm.ValidationReport):
        return {'kind': 'validation', 'valid': value.valid, 'reason': value.reason}
    if isinstance(value, (gm.H3Cell, gm.S2Cell, gm.Tile)):
        return {'kind': 'cell', 'system': type(value).__name__, 'id': int(value.id)}
    if isinstance(value, gm.GeohashCell):
        return {'kind': 'cell', 'system': 'GeohashCell', 'id': value.token}
    if isinstance(value, gm.CellArray):
        if len(value) == 0:
            return {'kind': 'cell_list', 'cells': [], 'system': 'empty'}
        first = value[0]
        system = type(first).__name__
        cells: list[int | str] = [
            cell.token if isinstance(cell, gm.GeohashCell) else int(cell.id)
            for cell in value
        ]
        return {'kind': 'cell_list', 'cells': cells, 'system': system}
    if (
        isinstance(value, list)
        and value
        and isinstance(value[0], (gm.H3Cell, gm.S2Cell, gm.Tile, gm.GeohashCell))
    ):
        system = type(value[0]).__name__
        cells: list[int | str] = [
            cell.token if isinstance(cell, gm.GeohashCell) else int(cell.id)
            for cell in value
        ]
        return {'kind': 'cell_list', 'cells': cells, 'system': system}
    if isinstance(
        value, (gm.CellArray, gm.Groups)
    ):
        cells = value
        if not cells:
            return {'kind': 'cell_list', 'cells': [], 'system': 'empty'}
        return serialize_snapshot_value(cells)
    if isinstance(value, gm.Coordinates):
        return {'kind': 'coords', 'nested': value.to_nested()}
    if isinstance(value, gm.Groups):
        raw = value.values.tolist()
        offsets = value.offsets.tolist()
        return {'kind': 'groups', 'values': raw, 'offsets': offsets}
    if isinstance(value, bool):
        return {'kind': 'bool', 'value': value}
    if isinstance(value, (int, np.integer)):
        return {'kind': 'float', 'value': round_float(float(value))}
    if isinstance(value, (float, np.floating)):
        return {'kind': 'float', 'value': round_float(float(value))}
    if isinstance(value, str):
        return {'kind': 'str', 'value': value}
    if isinstance(value, (bytes, bytearray)):
        return {'kind': 'bytes', 'hex': bytes(value).hex()}
    if isinstance(value, dict):
        return {'kind': 'json', 'value': value}
    if (
        isinstance(value, list)
        and value
        and all(isinstance(item, str) for item in value)
    ):
        return {'kind': 'json', 'value': value}
    if isinstance(value, list) and (
        not value
        or all(isinstance(item, (bool, int, float)) for item in value)
        or all(isinstance(item, (int, float)) for item in value)
    ):
        return {
            'kind': 'json',
            'value': [
                round_float(float(item))
                if isinstance(item, (float, np.floating))
                else item
                for item in value
            ],
        }
    if isinstance(value, np.ndarray):
        if value.dtype == np.bool_:
            return {'kind': 'json', 'value': value.tolist()}
        if np.issubdtype(value.dtype, np.floating):
            return {
                'kind': 'json',
                'value': [round_float(float(item)) for item in value.tolist()],
            }
        return {'kind': 'json', 'value': value.tolist()}
    if isinstance(value, tuple):
        return serialize_snapshot_value(list(value))
    raise TypeError(f'unsupported snapshot value type: {type(value)!r}')
