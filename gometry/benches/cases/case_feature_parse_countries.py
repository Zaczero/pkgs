"""Print-only A/B case: text Feature side-data capture on the countries fixture."""

from pathlib import Path
import time

import gometry as gm


text = (
    Path(__file__).resolve().parents[2] / 'fixtures' / 'osm_countries_0_1.geojson'
).read_text(encoding='utf-8')


def parse() -> None:
    features = gm.from_features(text)
    assert len(features.geometries) == 217
    assert len(features.properties) == len(features.ids) == 217


parse()
t0 = time.perf_counter()
for _ in range(12):
    parse()
print(time.perf_counter() - t0)
