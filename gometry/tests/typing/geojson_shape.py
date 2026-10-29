"""GeoJSON return types follow the input's semantic shape."""

from typing import assert_type

import gometry as gm
from gometry._types import GeoJsonFeature, GeoJsonFeatureCollection, GeoJsonGeometry

point: GeoJsonGeometry = {'type': 'Point', 'coordinates': [1.0, 2.0]}
feature: GeoJsonFeature = {'type': 'Feature', 'geometry': None}
collection: GeoJsonFeatureCollection = {
    'type': 'FeatureCollection',
    'features': [feature],
}

assert_type(gm.from_geojson(point), gm.Geometry)
assert_type(gm.from_geojson(feature), gm.Feature)
assert_type(gm.from_geojson(collection), gm.Features)
assert_type(gm.from_geojson([point, None]), gm.GeometryArray[gm.Geometry | None])
assert_type(
    gm.from_geojson([feature, None]), gm.GeometryArray[gm.Geometry | None] | gm.Features
)
assert_type(gm.from_geojson([point, None], as_features=True), gm.Features)
assert_type(
    gm.from_geojson('{}'),
    gm.Geometry | gm.GeometryArray[gm.Geometry | None] | gm.Feature | gm.Features,
)
