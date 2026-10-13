"""Public known answers for geographic Q publication invariants."""

from __future__ import annotations

import hashlib
import itertools
import json
from pathlib import Path

import gometry as gm

_COUNTRY_FIXTURE = Path(__file__).parents[1] / 'fixtures/osm_countries_0_1.geojson'


def _countries(codes: tuple[str, ...]) -> dict[str, gm.Geometry]:
    features = json.loads(_COUNTRY_FIXTURE.read_text(encoding='utf-8'))['features']
    selected = {
        feature['properties']['tags']['ISO3166-1:alpha2']: gm.from_geojson(
            feature['geometry'], crs=4326
        )
        for feature in features
        if feature['properties']['tags'].get('ISO3166-1:alpha2') in codes
    }
    assert set(selected) == set(codes)
    return selected


def test_au_pg_union_keeps_its_known_public_wkb_digest() -> None:
    countries = _countries(('AU', 'PG'))
    expected_digest = (
        'e5cd6a92943e8e13db66c64da5c8b9bd0d621ecd6bf1baa72aa5f393899bc6c0'
    )

    for order in itertools.permutations(('AU', 'PG')):
        result = gm.union(*(countries[code] for code in order))
        assert result.is_valid, f'invalid public result for operand order {order!r}'
        assert hashlib.sha256(result.to_wkb()).hexdigest() == expected_digest, (
            f'public AU/PG digest changed for operand order {order!r}'
        )


def test_tv_ar_uy_nary_symmetric_difference_is_valid_in_all_six_orders() -> None:
    countries = _countries(('TV', 'AR', 'UY'))
    expected = None

    for order in itertools.permutations(('TV', 'AR', 'UY')):
        result = gm.symmetric_difference_all([countries[code] for code in order])
        assert result.is_valid, f'invalid public result for operand order {order!r}'
        wkb = result.to_wkb()
        if expected is None:
            expected = wkb
        else:
            assert wkb == expected, (
                f'public TV/AR/UY result changed for operand order {order!r}'
            )
