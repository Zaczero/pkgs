"""Reusable source admission is independent of later tile selection."""

import gometry as gm
import pytest


def test_source_rejects_unsupported_property_outside_any_requested_tile() -> None:
    features = gm.Features(
        [gm.Point(0, 0, crs=3857), gm.Point(10_000_000, 10_000_000, crs=3857)],
        properties=[{'name': 'ok'}, {'unsupported': [1, 2]}],
    )
    with pytest.raises(TypeError, match='unsupported'):
        gm.MVTSource(features)
