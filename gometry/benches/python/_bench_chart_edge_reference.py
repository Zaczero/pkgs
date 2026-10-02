"""Independent quadrature for geographic chart-edge benchmark oracles."""

from __future__ import annotations

import math
from itertools import pairwise
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from collections.abc import Iterable

_NODES, _WEIGHTS = np.polynomial.legendre.leggauss(32)
_T = (_NODES + 1.0) / 2.0
_W = _WEIGHTS / 2.0


def _longitude_step(start: float, end: float) -> float:
    step = math.remainder(end - start, 360.0)
    if abs(step) == 180.0:
        raise ValueError('chart-edge reference encountered an ambiguous half turn')
    return step


def _authalic_q(latitude: np.ndarray, eccentricity_squared: float) -> np.ndarray:
    sine = np.sin(latitude)
    if eccentricity_squared == 0.0:
        return 2.0 * sine
    if eccentricity_squared > 0.0:
        eccentricity = math.sqrt(eccentricity_squared)
        inverse = np.arctanh(eccentricity * sine) / eccentricity
    else:
        eccentricity = math.sqrt(-eccentricity_squared)
        inverse = np.arctan(eccentricity * sine) / eccentricity
    return (1.0 - eccentricity_squared) * (
        sine / (1.0 - eccentricity_squared * sine * sine) + inverse
    )


def _ring_area(
    coordinates: Iterable[tuple[float, float]],
    semi_major: float,
    flattening: float,
) -> float:
    points = list(coordinates)
    edges = [
        (x0, y0, x1, y1, _longitude_step(x0, x1))
        for (x0, y0), (x1, y1) in pairwise(points)
    ]
    longitude_turn = round(math.fsum(edge[4] for edge in edges) / 360.0)
    full_turn = longitude_turn != 0
    latitudes = [point[1] for point in points]
    north = (
        latitudes[0] >= 0.0
        if not full_turn
        else min(latitudes) > 0.0 or max(latitudes) >= 0.0
    )
    direction = 1.0 if north else -1.0

    eccentricity_squared = flattening * (2.0 - flattening)
    if eccentricity_squared > 0.0:
        eccentricity = math.sqrt(eccentricity_squared)
        polar_q = (1.0 - eccentricity_squared) * (
            1.0 / (1.0 - eccentricity_squared) + math.atanh(eccentricity) / eccentricity
        )
    elif eccentricity_squared < 0.0:
        eccentricity = math.sqrt(-eccentricity_squared)
        polar_q = (1.0 - eccentricity_squared) * (
            1.0 / (1.0 - eccentricity_squared) + math.atan(eccentricity) / eccentricity
        )
    else:
        polar_q = 2.0

    contributions = []
    for _, latitude0, _, latitude1, longitude_step in edges:
        edge_latitudes = np.radians(latitude0 + (latitude1 - latitude0) * _T)
        mean_q = math.fsum(
            (_W * _authalic_q(edge_latitudes, eccentricity_squared)).tolist()
        )
        contributions.append(-direction * math.radians(longitude_step) * mean_q)
    if full_turn:
        contributions.append(longitude_turn * 2.0 * math.pi * polar_q)

    area = semi_major * semi_major / 2.0 * abs(math.fsum(contributions))
    surface_area = 2.0 * math.pi * semi_major * semi_major * polar_q
    if full_turn and area > surface_area / 2.0:
        area = surface_area - area
    return area


def _polygon_area(polygon, semi_major: float, flattening: float) -> float:
    shell = _ring_area(polygon.exterior.coords, semi_major, flattening)
    holes = math.fsum(
        _ring_area(ring.coords, semi_major, flattening) for ring in polygon.interiors
    )
    return shell - holes


def _geometry_area(geometry, semi_major: float, flattening: float) -> float:
    if geometry.geom_type == 'Polygon':
        return _polygon_area(geometry, semi_major, flattening)
    if geometry.geom_type == 'MultiPolygon':
        return math.fsum(
            _polygon_area(polygon, semi_major, flattening) for polygon in geometry.geoms
        )
    raise TypeError(f'expected polygonal country geometry, got {geometry.geom_type}')


def polygonal_chart_areas(geometries, geod) -> np.ndarray:
    """Return independent ellipsoidal areas for polygons with straight chart edges."""
    return np.fromiter(
        (_geometry_area(geometry, geod.a, geod.f) for geometry in geometries),
        dtype=np.float64,
    )


def _edge_length(
    x0: float,
    y0: float,
    x1: float,
    y1: float,
    semi_major: float,
    eccentricity_squared: float,
) -> float:
    longitude_step = math.radians(_longitude_step(x0, x1))
    latitude_step = math.radians(y1 - y0)
    latitude = np.radians(y0 + (y1 - y0) * _T)
    sine = np.sin(latitude)
    denominator = 1.0 - eccentricity_squared * sine * sine
    meridional_radius = semi_major * (1.0 - eccentricity_squared) / denominator**1.5
    prime_vertical_radius = semi_major / np.sqrt(denominator)
    speed = np.sqrt(
        (meridional_radius * latitude_step) ** 2
        + (prime_vertical_radius * np.cos(latitude) * longitude_step) ** 2
    )
    return math.fsum((_W * speed).tolist())


def country_exterior_chart_length(lines, geod) -> float:
    """Return total ellipsoidal length of the supplied straight chart edges."""
    eccentricity_squared = geod.f * (2.0 - geod.f)
    lengths = []
    for line in lines:
        coordinates = list(line.coords)
        lengths.append(
            math.fsum(
                _edge_length(
                    x0,
                    y0,
                    x1,
                    y1,
                    geod.a,
                    eccentricity_squared,
                )
                for (x0, y0), (x1, y1) in pairwise(coordinates)
            )
        )
    return math.fsum(lengths)
