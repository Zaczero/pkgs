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
