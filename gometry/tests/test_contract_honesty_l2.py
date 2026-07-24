import inspect
import json
import pickle
import re

import gometry as gm
import pytest


def test_s2_cover_defaults_are_truthful_and_pickle_preserves_custom_target() -> None:
    area = gm.box(0, 80, 10, 85, crs=4326)
    signature = str(inspect.signature(gm.s2_cover))
    assert 'max_cells=1000000' in signature
    assert 'target_cells=8' in signature

    fixed = gm.s2_cover(area, level=10)
    fixed_default = gm.s2_cover(area, level=10, max_cells=1_000_000)
    assert fixed.cells == fixed_default.cells
    assert fixed.max_cells == fixed_default.max_cells == 1_000_000

    adaptive = gm.s2_cover(area)
    adaptive_default = gm.s2_cover(
        area, max_cells=1_000_000, target_cells=8
    )
    assert adaptive.cells == adaptive_default.cells
    assert adaptive.max_cells == adaptive_default.max_cells == 1_000_000
    assert adaptive.target_cells == adaptive_default.target_cells == 8

    custom = gm.s2_cover(area, min_level=2, max_level=12, target_cells=24)
    restored = pickle.loads(pickle.dumps(custom))
    assert restored == custom
    assert restored.target_cells == 24
    assert 'target_cells=24' in repr(restored)

    with pytest.raises(TypeError):
        gm.s2_cover(area, target_cells=None)


@pytest.mark.parametrize(
    "value",
    [
        gm.Point(0, 0, crs=4326, epoch=2020.0),
        gm.GeometryArray([gm.Point(0, 0, crs=4326, epoch=2020.0)]),
    ],
)
@pytest.mark.parametrize("serializer", ["to_wkb", "to_wkt", "to_geojson"])
def test_lossy_core_serializers_require_epoch_drop_acknowledgement(value, serializer) -> None:
    method = getattr(value, serializer)
    with pytest.raises(gm.GeometryError, match="drop_epoch=True"):
        method()
    assert method(drop_epoch=True) is not None


def test_arrow_preserves_epoch_while_lossy_serializers_require_acknowledgement() -> None:
    values = gm.GeometryArray([gm.Point(0, 0, crs=4326, epoch=2020.0)])
    restored = gm.from_arrow(values.to_arrow())
    assert restored.epoch == 2020.0


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        (
            {"version": "WKT2_2018"},
            "did you mean 'WKT2_2019'? expected 'WKT2_2019', 'WKT2_2019_SIMPLIFIED', 'WKT2_2015', 'WKT2_2015_SIMPLIFIED', 'WKT1_GDAL', or 'WKT1_ESRI'",
        ),
        (
            {"version": "WKT2_2018_SIMPLIFIED"},
            "did you mean 'WKT2_2019_SIMPLIFIED'? expected 'WKT2_2019', 'WKT2_2019_SIMPLIFIED', 'WKT2_2015', 'WKT2_2015_SIMPLIFIED', 'WKT1_GDAL', or 'WKT1_ESRI'",
        ),
        ({"output_axis": "true"}, "expected 'auto', 'yes', or 'no'"),
        ({"output_axis": "false"}, "expected 'auto', 'yes', or 'no'"),
    ],
)
def test_crs_export_tokens_reject_hidden_aliases(kwargs, expected) -> None:
    with pytest.raises(gm.GeometryError, match=re.escape(expected)):
        gm.CRS(4326).to_wkt(**kwargs)


def test_crs_export_defaults_are_literals_and_match_omission() -> None:
    crs = gm.CRS(4326)
    assert str(inspect.signature(crs.to_wkt)) == (
        "(*, version='WKT2_2019', pretty=False, output_axis='auto', strict=True, "
        "indentation_width=4)"
    )
    assert str(inspect.signature(crs.to_projjson)) == "(*, pretty=False, indentation_width=2)"
    assert crs.to_wkt() == crs.to_wkt(output_axis="auto")
    assert crs.to_wkt(pretty=True) == crs.to_wkt(pretty=True, indentation_width=4)
    assert crs.to_projjson(pretty=True) == crs.to_projjson(
        pretty=True, indentation_width=2
    )
    with pytest.raises(TypeError):
        crs.to_wkt(output_axis=None)
    with pytest.raises(TypeError):
        crs.to_wkt(indentation_width=None)
    with pytest.raises(TypeError):
        crs.to_projjson(indentation_width=None)


def test_from_features_resolves_frame_before_all_input_lanes() -> None:
    feature = {"type": "Feature", "properties": {}}
    for value in (feature, json.dumps(feature), json.dumps(feature).encode()):
        with pytest.raises(gm.CRSError) as raised:
            gm.from_features(value, crs="not-a-crs")
        assert type(raised.value) is gm.CRSError
