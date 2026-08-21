"""Runtime documentation coverage for public Python re-exports."""

from __future__ import annotations


def test_public_toplevel_documented() -> None:
    """Top-level package re-exports must carry runtime docs (beyond `_lib`)."""
    import gometry as gm

    undocumented = [
        n
        for n in gm.__all__
        if callable(getattr(gm, n, None))
        and not isinstance(getattr(gm, n), type)
        and not (getattr(gm, n).__doc__ or '').strip()
    ]
    assert not undocumented, f'undocumented top-level functions: {undocumented}'


def test_documented_array_axes_contract() -> None:
    """Array axes are row-aligned; the common layout is a separate fact."""
    import gometry as gm

    values = gm.GeometryArray([
        gm.from_wkt('POINT Z (1 2 3)'),
        None,
        gm.from_wkt('POINT M (1 2 9)'),
    ])
    assert values.coordinate_axes == ['XYZ', None, 'XYM']
    assert values.common_coordinate_axes is None


def test_documented_arrow_array_contract() -> None:
    """The Arrow C array exporter returns schema and array capsules together."""
    import gometry as gm

    schema, array = gm.points([1.0], [2.0]).__arrow_c_array__()
    assert 'arrow_schema' in repr(schema)
    assert 'arrow_array' in repr(array)


def test_documented_geographic_segmentize_contract() -> None:
    """Geographic max_length is measured in metres, unlike planar override."""
    import gometry as gm

    line = gm.LineString([(0.0, 0.0), (1.0, 0.0)], crs=4326)
    assert len(line.segmentize(20_000).coords) == 7
    assert len(line.segmentize(0.25, unit='planar').coords) == 5
