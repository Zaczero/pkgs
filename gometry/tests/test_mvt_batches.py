"""One-shot MVT batches preserve sparse values, IDs, and input order."""

import gometry as gm
import pytest


def test_one_shot_batches_encode_once_with_aligned_sparse_attributes() -> None:
    consumed = []

    def batches():
        consumed.append(1)
        yield gm.Features([gm.Point(1, 1)], properties=[{'a': 2}], ids=[0])
        consumed.append(2)
        yield gm.Features([gm.Point(3, 3)], properties=[{'b': 'two'}], ids=[9])

    payload = gm.to_mvt(
        batches(),
        layer='items',
        frame=gm.TileFrame((0, 0, 4, 4), crs=None),
        extent=4,
        buffer=0,
    )
    decoded = gm.from_mvt(payload, layer='items').features
    assert consumed == [1, 2]
    assert decoded.ids == (0, 9)
    assert decoded[0].properties == {'a': 2}
    assert decoded[1].properties == {'b': 'two'}


def test_prepared_batches_keep_null_and_sparse_properties_through_edits() -> None:
    batches = iter([
        gm.Features(
            [gm.Point(1, 1), gm.Point(2, 1)], properties=[None, {'a': 5}], ids=[0, 1]
        ),
        gm.Features([gm.Point(3, 1)], properties=[{'b': 'x'}], ids=[2]),
    ])
    frame = gm.TileFrame((0, 0, 4, 4), crs=None)
    features = gm.mvt_prepare(batches, frame=frame, extent=4, buffer=0).features
    assert features.properties[:] == (None, {'a': 5}, {'b': 'x'})
    assert features[1:].column('a')[:] == (5, None)
    edited = features.with_columns({'rank': [8, 9, 10]})
    assert edited.properties[:] == (
        {'rank': 8},
        {'a': 5, 'rank': 9},
        {'b': 'x', 'rank': 10},
    )
    assert edited.materialize().properties == edited.properties


def test_source_batches_preserve_order_and_validate_each_batch() -> None:
    batches = [
        gm.Features([gm.Point(1, 1)], properties=[{'a': 5}], ids=[0]),
        gm.Features([gm.Point(2, 1)], properties=[{'b': 7}], ids=[2]),
    ]
    source = gm.MVTSource(iter(batches), crs=None, extent=4, buffer=0)
    frame = gm.TileFrame((0, 0, 4, 4), crs=None)
    features = gm.mvt_prepare(source, frame=frame).features
    assert features.ids == (0, 2)
    assert features.properties[:] == ({'a': 5}, {'b': 7})
    with pytest.raises(TypeError):
        gm.MVTSource(iter([batches[0], object()]), crs=None)
