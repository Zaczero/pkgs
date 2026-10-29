"""Tile iterators release their own references and leave inputs caller-owned."""

from __future__ import annotations

import gc
import weakref

import gometry as gm
import pytest


class Addresses:
    def __init__(self) -> None:
        self.advances = 0
        self.closed = False

    def __iter__(self) -> Addresses:
        return self

    def __next__(self) -> gm.Tile:
        self.advances += 1
        raise StopIteration

    def close(self) -> None:
        self.closed = True


def test_closing_unadvanced_tile_iterator_releases_its_address_iterator() -> None:
    addresses = Addresses()
    reference = weakref.ref(addresses)
    tiles = gm.mvt_tiles({}, tiles=addresses)
    assert addresses.advances == 0
    del addresses
    assert reference() is not None
    tiles.close()
    gc.collect()
    assert reference() is None


@pytest.mark.parametrize('automatic', [False, True], ids=['explicit', 'automatic'])
@pytest.mark.parametrize('advanced', [False, True], ids=['unadvanced', 'advanced'])
def test_closing_tile_iterator_releases_sources(
    *, automatic: bool, advanced: bool
) -> None:
    class WeakSource(gm.MVTSource):
        __slots__ = ('__weakref__',)

    source = WeakSource(gm.Features(gm.points([0], [0], crs=3857)))
    reference = weakref.ref(source)
    if automatic:
        tiles = gm.mvt_tiles({'points': source}, min_zoom=0, max_zoom=0)
    else:
        tiles = gm.mvt_tiles({'points': source}, tiles=[gm.Tile(x=0, y=0, zoom=0)])
    if advanced:
        next(tiles)
    del source
    assert reference() is not None
    tiles.close()
    gc.collect()
    assert reference() is None


def test_closing_tile_iterator_does_not_close_caller_address_iterator() -> None:
    addresses = Addresses()
    tiles = gm.mvt_tiles({}, tiles=addresses)
    list(tiles)
    tiles.close()
    assert not addresses.closed
