"""Packed polygon take / filter / concat — storage-twin parity."""

from __future__ import annotations

from tests._support import canon, polygon_storage_twins


def test_packed_polygons_take_matches_mixed_storage() -> None:
    packed, mixed = polygon_storage_twins()
    assert canon(packed[[1, 0]]) == canon(mixed[[1, 0]])


def test_packed_polygons_filter_matches_mixed_storage() -> None:
    packed, mixed = polygon_storage_twins()
    mask = [True, False]
    assert canon(packed[mask]) == canon(mixed[mask])


def test_packed_polygons_concat_matches_mixed_storage() -> None:
    packed, mixed = polygon_storage_twins()
    assert canon(packed.concat(packed)) == canon(mixed.concat(mixed))


def test_packed_polygons_take_preserves_geoarrow_layout() -> None:
    packed, _ = polygon_storage_twins()
    out = packed[[0]]
    assert out.to_arrow().type.extension_name == 'geoarrow.polygon'
