"""Shared figure helpers for the docs site's ``markdown-exec`` blocks.

Guide pages render geometry as inline SVG via ``geom._repr_svg_()``. This module
is the single place that wraps that SVG in a consistent
``<figure class="gometry-figure">`` (styled by ``docs/stylesheets/extra.css``),
so every visual across the site shares one layout, caption style, and palette
instead of hand-built ``<figure>`` strings. Figures live on guide pages (not the
API reference).

``markdown-exec`` blocks import it directly::

    ```python exec="on" html="true"
    from _figures import before_after
    import gometry as gm
    line = gm.LineString([(0, 0), (5, 0), (5, 5)])
    print(before_after(line, (line).buffer(1.0), after_caption="buffer(1.0)"))
    ```

``examples/`` is added to ``sys.path`` by ``tools/docs/gen_api_pages.py`` (which
the ``gen-files`` plugin runs before any page is rendered), so the bare
``from _figures import ...`` resolves during the build.
"""

from __future__ import annotations

from html import escape
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable


def _svg(obj: Any) -> str:
    """The SVG for a geometry/array, an overlaid list of them, or a raw string."""
    if isinstance(obj, str):
        return obj
    import gometry as gm

    if isinstance(obj, (list, tuple)):
        # Overlay several geometries in one frame (e.g. result over its input).
        obj = gm.GeometryCollection(list(obj))
    repr_svg = getattr(obj, '_repr_svg_', None)
    if repr_svg is not None:
        return repr_svg()
    # A GeometryArray has no _repr_svg_: render every element in one frame.
    if isinstance(obj, gm.GeometryArray):
        return gm.GeometryCollection(list(obj))._repr_svg_()
    raise TypeError(f'cannot render {type(obj).__name__} as SVG')


def figure(
    obj: Any,
    caption: str | None = None,
) -> str:
    """One captioned SVG panel.

    ``obj`` is a geometry/array, a raw SVG string, or a list of geometries to
    overlay in a single frame. ``caption`` renders as a ``<figcaption>`` and
    names the SVG for assistive technology.
    """
    if not caption:
        raise ValueError('a figure needs a non-empty caption')
    svg = _svg(obj).replace(
        '<svg', f'<svg role="img" aria-label="{escape(caption, quote=True)}"', 1
    )
    cap = f'<figcaption><code>{escape(caption)}</code></figcaption>' if caption else ''
    return f'<figure class="gometry-figure">{cap}{svg}</figure>'


def panels(items: Iterable[Any]) -> str:
    """A responsive row of captioned panels.

    Each item is a ``(caption, obj)`` pair. ``obj`` may be a geometry, an SVG
    string, or a list of geometries to overlay. Captions are required so every
    panel has useful equivalent text.
    """
    figs = []
    for item in items:
        if (
            isinstance(item, tuple)
            and len(item) == 2
            and isinstance(item[0], (str, type(None)))
        ):
            caption, obj = item
        else:
            caption, obj = None, item
        if not caption:
            raise ValueError('each panel needs a non-empty caption')
        figs.append(figure(obj, caption))
    return (
        '<div class="gometry-panels" '
        'style="display:flex;gap:1.5rem;align-items:flex-start;flex-wrap:wrap">'
        f'{"".join(figs)}</div>'
    )


def before_after(
    before: Any,
    after: Any,
    *,
    before_caption: str = 'input',
    after_caption: str = 'result',
) -> str:
    """Two side-by-side panels — the highest-value geospatial comparison."""
    return panels([(before_caption, before), (after_caption, after)])


def with_vertices(geom: Any) -> list[Any]:
    """The geometry overlaid with its own vertices as points.

    Returns a ``[geom, MultiPoint(geom.coords)]`` overlay list (the form
    :func:`figure`/:func:`panels`/:func:`before_after` accept directly), so the
    individual coordinates are visible as dots on top of the shape. This is what
    makes vertex-changing operations legible — ``simplify`` dropping points,
    ``repair`` splitting a ring, ``snap``/``densify`` moving or adding them —
    where a bare fill or outline would look unchanged.
    """
    import gometry as gm

    pts = gm.MultiPoint(geom.coords, crs=getattr(geom, 'crs', None))
    return [geom, pts]


def result_over_input(
    before: Any,
    after: Any,
    *,
    before_caption: str = 'input',
    after_caption: str = 'result',
) -> str:
    """Before/after where the result panel overlays the result on the input.

    Unlike :func:`before_after` (two independent panels), the second panel draws
    ``after`` on top of ``before`` in one frame, so the reader sees exactly which
    region the operation kept, added, or removed relative to the input.
    """
    return panels([(before_caption, before), (after_caption, [before, after])])


def curve_through(
    points: Any,
    *,
    curve: str = 'hilbert',
    level: int = 16,
    bounds: Any = None,
) -> list[Any]:
    """A path and points in space-filling-curve order.

    The returned ``[LineString, MultiPoint]`` overlay threads the geometries'
    centroids in Hilbert or Morton order, making locality visible as path shape.
    """
    import gometry as gm

    arr = (
        points
        if isinstance(points, gm.GeometryArray)
        else gm.GeometryArray(list(points))
    )
    ordered = (
        arr.sort_by_spatial_key(curve='hilbert', level=level, bounds=bounds)
        if curve == 'hilbert'
        else arr.sort_by_spatial_key(curve='morton', level=level, bounds=bounds)
    )
    xy = [(g.centroid().x, g.centroid().y) for g in ordered]
    crs = getattr(arr, 'crs', None)
    return [gm.LineString(xy, crs=crs), gm.MultiPoint(xy, crs=crs)]


def cell_polys(cells: Any) -> list[Any]:
    """List of per-cell polygons from a CellArray.

    Use the row-aligned ``.polygon`` array, not the aggregate ``to_polygon()``
    coverage dissolve.
    """
    return list(cells.polygon)


def _world_frame(geom: Any) -> list[Any]:
    """A geometry overlaid on a lon/lat world rectangle for geographic context.

    The rectangle's left and right edges sit on the ±180° antimeridian, so a
    geometry split there reads as pieces hugging opposite edges of the world.
    """
    import gometry as gm

    crs = getattr(geom, 'crs', None)
    world = gm.LineString(
        [(-180, -90), (180, -90), (180, 90), (-180, 90), (-180, -90)], crs=crs
    )
    return [world, geom]


def antimeridian_before_after(
    before: Any,
    after: Any,
    *,
    before_caption: str = 'input (crosses ±180°)',
    after_caption: str = 'split into two pieces',
) -> str:
    """Before/after for ``split_antimeridian`` with a lon/lat world frame.

    Both panels are drawn inside the lon/lat world rectangle (-180..180 by
    -90..90) so the antimeridian seam is visible: the input line spans the map,
    and the split result is two separate pieces against the left and right edges
    (the date line).
    """
    return panels([
        (before_caption, _world_frame(before)),
        (after_caption, _world_frame(after)),
    ])
