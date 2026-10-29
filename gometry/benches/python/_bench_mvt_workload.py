"""Shared MVT workload parameters."""

import math
from dataclasses import dataclass

EXTENT = 4096
BUFFER = 64
ZOOM = 8
TILE_X = 128
TILE_Y = 128
X_STEP = 137
Y_STEP = 397
FRACTION = 0.2
UNIT = math.pi * 6_378_137 / 128 / EXTENT


@dataclass(frozen=True, slots=True)
class Workload:
    layer: str
    points: int
    lines: int
    polygons: int
    period: int
    offset: int
    alternate_x: int
    select_every: int

    @property
    def rows(self) -> int:
        return self.points + self.lines + self.polygons


POINTS = Workload('points', 5_000, 0, 0, 8192, -2048, 0, 1)
MIXED = Workload('mixed', 5_000, 1_000, 1_000, 3800, 128, 8192, 4)
