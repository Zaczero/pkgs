"""Tile iterators release their own references and leave inputs caller-owned."""

from __future__ import annotations

import gc
import weakref

import gometry as gm


class Addresses:
    def __init__(self) -> None:
        self.closed = False

    def __iter__(self) -> Addresses:
        return self

    def __next__(self) -> gm.Tile:
        raise StopIteration

    def close(self) -> None:
        self.closed = True


def test_closing_unadvanced_tile_iterator_releases_its_address_iterator() -> None:
    addresses = Addresses()
    reference = weakref.ref(addresses)
    tiles = gm.mvt_tiles({}, tiles=addresses)
    del addresses
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
