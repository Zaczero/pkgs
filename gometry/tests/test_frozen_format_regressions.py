"""Regression coverage for v1 on-disk and cross-process formats."""

from __future__ import annotations

import inspect
import pickle

import gometry as gm
from gometry import _lib


def test_nullable_homogeneous_ingress_stays_geoarrow_native() -> None:
    """D2: missing rows scatter after, rather than before, packed inference."""
    line = gm.LineString([(0, 0), (1, 1)])
    geojson_line = {'type': 'LineString', 'coordinates': [[0, 0], [1, 1]]}
    feature_collection = {
        'type': 'FeatureCollection',
        'features': [
            {'type': 'Feature', 'geometry': geojson_line, 'properties': {}},
            {'type': 'Feature', 'geometry': None, 'properties': {}},
        ],
    }
    arrays = (
        gm.GeometryArray([line, None]),
        gm.from_wkt([line.to_wkt(), None]),
        gm.from_wkb([line.to_wkb(), None]),
        gm.from_geojson([geojson_line, None]),
        gm.from_geojson(feature_collection),
        gm.from_features(feature_collection).geometries,
    )
    for values in arrays:
        assert values.is_missing.tolist() == [False, True]
        assert values.to_arrow().type.extension_name == 'geoarrow.linestring'


def test_masked_packed_pickle_uses_typed_columns() -> None:
    """D3: packed reducers preserve native lanes; distinct sides survive missing."""
    # Distinct values on both sides of the missing row — WKT equality alone
    # would lose ordering/value sensitivity if both present sides were identical.
    cases = (
        (
            gm.GeometryArray([gm.Point(0, 0), None, gm.Point(1, 1)]),
            'point',
            'point',
        ),
        (
            gm.GeometryArray([
                gm.LineString([(0, 0), (1, 1)]),
                None,
                gm.LineString([(2, 2), (3, 3)]),
            ]),
            'line',
            'linestring',
        ),
        (
            gm.GeometryArray([
                gm.Polygon([(0, 0), (1, 0), (1, 1), (0, 0)]),
                None,
                gm.box(0, 0, 1, 1),
            ]),
            'polygon',
            'polygon',
        ),
    )
    for values, reducer_kind, arrow_kind in cases:
        callable_, args = values.__reduce_ex__(5)
        assert callable_.__name__ == f'_unpickle_{reducer_kind}_array'
        assert args[-1] == b'\x00\x01\x00'
        restored = pickle.loads(pickle.dumps(values))
        assert restored.is_missing.tolist() == values.is_missing.tolist()
        assert restored.is_missing.tolist() == [False, True, False]
        assert restored.to_wkt() == values.to_wkt()
        # Distinct present-side values (not both the same geometry).
        present = [w for w, m in zip(restored.to_wkt(), restored.is_missing, strict=True) if not m]
        assert len(set(present)) == 2, present
        assert restored.to_arrow().type.extension_name == f'geoarrow.{arrow_kind}'


def test_geometry_array_unpickler_requires_its_missing_payload() -> None:
    """D4: no v1 reducer leaves a shorter private pickle arity valid."""
    parameter = inspect.signature(_lib._unpickle_geometry_array).parameters['missing']
    assert parameter.default is inspect.Parameter.empty
