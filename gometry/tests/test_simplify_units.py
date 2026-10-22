"""CRS-natural simplification tolerance and packed/scalar parity."""

import gometry as gm
import pytest


@pytest.mark.parametrize('method', ['dp', 'vw'])
def test_simplify_geographic_tolerance_is_physical_with_planar_escape(
    method: str,
) -> None:
    line = gm.LineString([(0, 0), (1, 0.0001), (2, 0)], crs=4326)
    assert len(line.simplify(1, method=method).coords) == 3
    assert len(line.simplify(20_000, method=method).coords) == 2
    assert len(line.simplify(1, method=method, unit='planar').coords) == 2
    assert (
        line.simplify(1, method=method).to_wkt()
        == line.simplify(
            1,
            method=method,
            unit='meters',
        ).to_wkt()
    )


def test_simplify_projected_metre_override_converts_native_feet() -> None:
    line = gm.LineString([(0, 0), (1, 0.4), (2, 0)], crs=2263)
    assert len(line.simplify(0.5, method='dp').coords) == 2
    assert len(line.simplify(0.1, method='dp', unit='meters').coords) == 3
    rows = gm.GeometryArray([line, line])
    result = rows.simplify(0.1, method='dp', preserve_topology=False, unit='meters')
    assert result.to_wkt() == [line.to_wkt()] * 2


def test_physical_simplification_admits_geographic_coordinates_in_both_lanes() -> None:
    line = gm.LineString([(0, 100), (1, 101), (2, 100)], crs=4326)
    with pytest.raises(gm.InvalidGeometryError, match='latitude'):
        line.simplify(1.0)
    rows = gm.GeometryArray([line, line])
    with pytest.raises(gm.InvalidGeometryError, match='latitude'):
        rows.simplify(1.0, preserve_topology=False)
    assert len(line.simplify(1.0, unit='planar').coords) >= 2


@pytest.mark.parametrize('method', ['dp', 'vw'])
@pytest.mark.parametrize('unit', [None, 'meters', 'planar'])
def test_simplify_packed_units_match_scalar_and_per_row_tolerances(
    method: str,
    unit: str | None,
) -> None:
    line = gm.LineString(
        [(0, 0), (1, 0.0001), (2, 0)],
        z=[1, 2, 3],
        m=[11, 12, 13],
        crs=4326,
    )
    rows = gm.GeometryArray([line, line])
    expected = line.simplify(1.0, method=method, preserve_topology=False, unit=unit)
    scalar_tolerance = rows.simplify(
        1.0,
        method=method,
        preserve_topology=False,
        unit=unit,
    )
    per_row = rows.simplify(
        [1.0, 1.0],
        method=method,
        preserve_topology=False,
        unit=unit,
    )
    assert scalar_tolerance.to_wkt() == [expected.to_wkt()] * 2
    assert per_row.to_wkt() == scalar_tolerance.to_wkt()
