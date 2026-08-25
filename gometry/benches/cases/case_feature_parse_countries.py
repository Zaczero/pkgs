"""Print-only A/B case: text Feature side-data capture on countries."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import time

import gometry as gm
from _observation import emit
from _validation import emit_validation


TEXT = (
    Path(__file__).resolve().parents[2] / 'fixtures' / 'osm_countries_0_1.geojson'
).read_text(encoding='utf-8')


def parse(text: str = TEXT) -> object:
    features = gm.from_features(text)
    assert len(features.geometries) == len(features.properties) == len(features.ids)
    return features


def main() -> None:
    features = parse()
    count = len(features.geometries)
    started = time.perf_counter()
    for _ in range(12):
        parse()
    emit(
        metric='duration',
        raw_values=[time.perf_counter() - started],
        useful_work={'count': 12 * count, 'unit': 'features-parsed'},
        oracle_identity='case_feature_parse_countries:feature-metadata',
        phase='steady',
        calibration_iterations=12,
    )


def validate_case() -> None:
    text = json.dumps({
        'type': 'FeatureCollection',
        'features': [{
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [1.0, 2.0]},
            'properties': {'id': 1},
            'id': 'one',
        }],
    })
    features = parse(text)
    assert len(features.geometries) == 1 and list(features.ids) == ['one']
    emit_validation(
        'case_feature_parse_countries',
        useful_output={'count': 1, 'unit': 'features-parsed'},
    )


if __name__ == '__main__':
    validate_case() if '--validate' in sys.argv else main()
