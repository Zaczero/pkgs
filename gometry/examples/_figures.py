"""Shared figure rendering for the docs site's ``markdown-exec`` blocks.

Every authored visual on the site is drawn here, from one vocabulary of
geometry *roles*, into one themed SVG. A figure names what each shape is for —
the input, the geometry being tested, a coarse candidate, the result — instead
of painting them all in one colour and leaving the caption to explain which is
which.

The SVG carries classes only, never a colour: ``docs/stylesheets/extra.css``
owns the palette, so figures follow the light and dark schemes. The product's
own ``_repr_svg_`` is a REPL convenience that colours by validity and paints
every member of an overlay identically; documentation renders here instead.

``markdown-exec`` blocks import it directly::

    ```python exec="on" html="true"
    from _figures import figure
    import gometry as gm
    line = gm.LineString([(0, 0), (5, 0), (5, 5)])
    print(figure(
        {'source': line, 'result': line.buffer(1.0)},
        'buffer(1.0) widens the route into a corridor',
        alt='The corridor surrounds both sides and both ends of the route.',
    ))
    ```

``examples/`` is added to ``sys.path`` by ``tools/docs/gen_api_pages.py`` (which
the ``gen-files`` plugin runs before any page is rendered), so the bare
``from _figures import ...`` resolves during the build.
"""

from __future__ import annotations

from collections.abc import Mapping
from html import escape
from math import cos, isfinite, radians
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

# The role vocabulary. A figure's meaning lives here and in its legend, never in
# colour alone: each role also carries its own stroke weight and dash pattern.
ROLE_LABELS = {
    'source': 'input',
    'query': 'query',
    'candidate': 'candidates',
    'result': 'result',
    'excluded': 'excluded',
    'anchor': 'anchor',
}

# Paint context first and the answer last, so a result stays visible over the
# inputs and candidates it was derived from. The legend keeps the caller's
# reading order instead of this one.
_PAINT_ORDER = {
    'candidate': 0,
    'source': 1,
    'query': 2,
    'excluded': 3,
    'result': 4,
    'anchor': 5,
}

# The drawing frame. Height is fixed and width follows the content's own aspect
# between these bounds, so a square subject gets a square figure and a wide one
# is not letterboxed. Strokes are non-scaling, so weight reads the same at every
# figure width.
_VIEW_HEIGHT = 340.0
_MIN_VIEW_WIDTH = 200.0
_MAX_VIEW_WIDTH = 680.0
_PAD = 14.0
_MARKER_RADIUS = 5.0

# A legend swatch. Strokes do not scale, so a swatch drawn at this size carries
# exactly the weight and dash the same role carries in the figure above it.
_KEY_WIDTH = 26.0
_KEY_HEIGHT = 14.0

# An alt text that names the subject without stating what the reader should
# conclude from it carries no information a caption does not already carry.
_GENERIC_ALT = {
    'image',
    'figure',
    'geometry',
    'map',
    'input',
    'result',
    'point',
    'polygon',
    'linestring',
}

# A drawing frame: the extent to fit, and the aspect to fit it at.
Frame = tuple[float, float, float, float, float]


def figure(
    layers: Any,
    caption: str,
    *,
    alt: str | None = None,
    labels: Mapping[str, str] | None = None,
    aspect: str = 'auto',
    frame: Frame | None = None,
    legend: bool = True,
) -> str:
    """One captioned figure.

    ``layers`` is a geometry, a sequence of geometries (all drawn as
    ``source``), or a mapping of role name to either. ``labels`` overrides a
    role's legend text, and the legend appears once a figure carries more than
    one role. ``alt`` states the visual conclusion for assistive technology;
    without one the caption is the accessible name.

    ``aspect='raw'`` draws coordinate units 1:1 — use it when the figure is
    *about* how a frame's own units distort. ``frame`` forces a shared drawing
    frame and ``legend=False`` defers the key to the caller; :func:`panels` uses
    both to make a row of figures read as one comparison.
    """
    if not caption or not caption.strip():
        raise ValueError('a figure needs a non-empty caption')
    description = caption if alt is None else _conclusion_alt(alt)
    drawn, geographic = _layers(layers)
    svg = _svg(
        drawn,
        frame if frame is not None else _extent(drawn, geographic, aspect),
        description,
    )
    key = _legend(drawn, labels) if legend and len(drawn) > 1 else ''
    return (
        '<figure class="gometry-figure">'
        f'<figcaption>{escape(caption)}</figcaption>'
        f'{svg}{key}'
        '</figure>'
    )


def panels(
    items: Iterable[Any],
    *,
    caption: str | None = None,
    alt: str | None = None,
    labels: Mapping[str, str] | None = None,
    aspect: str = 'auto',
    share_frame: bool = True,
) -> str:
    """A responsive row of captioned figures.

    Each item is a ``(caption, layers)`` pair or a ``(caption, layers, alt)``
    triple. Panels share one drawing frame by default, so a shape is the same
    size in every panel and the comparison is about the geometry rather than
    about each panel's own zoom. Pass ``share_frame=False`` when the panels hold
    deliberately different coordinate frames, where a shared extent would shrink
    every panel but one to a dot.

    A group ``caption`` needs an ``alt``: together they name the conclusion the
    row of panels exists to support.
    """
    entries = []
    for item in items:
        if not isinstance(item, tuple) or len(item) not in {2, 3}:
            raise TypeError('each panel needs a (caption, layers[, alt]) tuple')
        panel_caption, panel_layers, *panel_alt = item
        entries.append((
            panel_caption,
            panel_layers,
            panel_alt[0] if panel_alt else None,
        ))
    if not entries:
        raise ValueError('a panel group needs at least one panel')
    if (caption is None) != (alt is None):
        raise ValueError('comparison panels need both caption and alt')
    if caption is not None and not caption.strip():
        raise ValueError('comparison panels need a non-empty caption')
    drawn = [_layers(spec) for _, spec, _ in entries]
    shared = (
        _union([_extent(shapes, geographic, aspect) for shapes, geographic in drawn])
        if share_frame
        else None
    )
    # Panels that all carry the same roles are read against one key. Repeating
    # it under every panel is noise the reader has to check for differences.
    common = {tuple(role for role, _ in shapes) for shapes, _ in drawn}
    one_key = len(common) == 1 and len(drawn[0][0]) > 1
    row = ''.join(
        figure(
            panel_layers,
            panel_caption,
            alt=panel_alt,
            labels=labels,
            aspect=aspect,
            frame=shared,
            legend=not one_key,
        )
        for panel_caption, panel_layers, panel_alt in entries
    )
    # The key and the group caption sit outside the panel row: inside it they
    # would occupy grid tracks and hold open columns the panels do not need.
    body = f'<div class="gometry-panels__row">{row}</div>'
    if one_key:
        body += _legend(drawn[0][0], labels)
    if caption is None:
        return f'<div class="gometry-panels">{body}</div>'
    conclusion = _conclusion_alt(alt) if alt is not None else ''
    return (
        f'<div class="gometry-panels" role="group" '
        f'aria-label="{escape(conclusion, quote=True)}">{body}'
        f'<p class="gometry-panels__caption">{escape(caption)}</p></div>'
    )


def before_after(
    before: Any,
    after: Any,
    *,
    before_caption: str = 'input',
    after_caption: str = 'result',
    before_alt: str | None = None,
    after_alt: str | None = None,
) -> str:
    """Two panels in one frame — the operation's input, then its result.

    The result panel keeps the input beneath it, so the reader sees what the
    operation added, kept, or moved rather than two unrelated pictures.
    """
    return panels([
        (before_caption, {'source': before}, before_alt),
        (after_caption, {'source': before, 'result': after}, after_alt),
    ])


def with_vertices(geom: Any) -> list[Any]:
    """A geometry together with its own vertices as markers.

    Vertex-changing operations — ``simplify`` dropping points, ``segmentize``
    adding them, ``snap`` moving them — leave the outline looking untouched, so
    only the vertices show what happened. Both entries share one role, so the
    markers are drawn in that role's ink.
    """
    import gometry as gm

    return [geom, gm.MultiPoint(geom.coords, crs=getattr(geom, 'crs', None))]


def curve_through(
    points: Any,
    *,
    curve: str = 'hilbert',
    level: int = 16,
    bounds: Any = None,
) -> list[Any]:
    """A path threading geometries in space-filling-curve order.

    The path visits each row's centroid in the order
    ``sort_by_spatial_key`` produces, so locality reads as path shape.
    """
    import gometry as gm

    array = (
        points
        if isinstance(points, gm.GeometryArray)
        else gm.GeometryArray(list(points))
    )
    ordered = array.sort_by_spatial_key(curve=curve, level=level, bounds=bounds)
    xy = [(row.centroid().x, row.centroid().y) for row in ordered]
    crs = getattr(array, 'crs', None)
    return [gm.LineString(xy, crs=crs), gm.MultiPoint(xy, crs=crs)]


def cell_polys(cells: Any) -> list[Any]:
    """Per-cell polygons from a cell or cell array.

    Uses the row-aligned ``.polygon`` array, not the aggregate ``to_polygon()``
    coverage dissolve, so each cell stays a separate outline.
    """
    return list(cells.polygon)


def antimeridian_before_after(
    before: Any,
    after: Any,
    *,
    before_caption: str = 'input crossing ±180°',
    after_caption: str = 'split_antimeridian()',
    before_alt: str | None = None,
    after_alt: str | None = None,
) -> str:
    """Before/after for ``split_antimeridian`` inside the lon/lat world frame.

    The frame's left and right edges sit on the ±180° antimeridian, so a
    geometry split there reads as pieces hugging opposite edges of the world.
    """
    import gometry as gm

    world = gm.LineString(
        [(-180, -90), (180, -90), (180, 90), (-180, 90), (-180, -90)],
        crs=getattr(before, 'crs', None),
    )
    return panels(
        [
            (before_caption, {'source': world, 'query': before}, before_alt),
            (after_caption, {'source': world, 'result': after}, after_alt),
        ],
        labels={'source': 'lon/lat world frame'},
    )


def _extent(drawn: Sequence[Any], geographic: bool, aspect: str) -> Frame:
    """The drawing frame for shapes already laid out: what to fit, at what aspect."""
    points = [
        point
        for _role, shapes in drawn
        for _kind, rings in shapes
        for ring in rings
        for point in ring
    ]
    min_y, max_y = min(y for _, y in points), max(y for _, y in points)
    return (
        min(x for x, _ in points),
        min_y,
        max(x for x, _ in points),
        max_y,
        _aspect(min_y, max_y, geographic, aspect),
    )


def _aspect(min_y: float, max_y: float, geographic: bool, mode: str) -> float:
    """How much to compress the x axis when drawing this frame.

    A degree of longitude shortens toward the poles, so a lon/lat frame drawn
    with equal axes flattens a metric buffer into an ellipse. This corrects the
    drawing only; no coordinate is transformed. ``mode='raw'`` keeps coordinate
    units 1:1 for the figures that are *about* that distortion.
    """
    if mode not in {'auto', 'raw'}:
        raise ValueError(f"figure aspect must be 'auto' or 'raw', not {mode!r}")
    if mode == 'raw' or not geographic:
        return 1.0
    return max(cos(radians((min_y + max_y) / 2)), 0.05)


def _union(frames: Sequence[Frame]) -> Frame:
    """One frame covering every panel, at the narrowest panel's aspect."""
    return (
        min(frame[0] for frame in frames),
        min(frame[1] for frame in frames),
        max(frame[2] for frame in frames),
        max(frame[3] for frame in frames),
        min(frame[4] for frame in frames),
    )


def _conclusion_alt(alt: str) -> str:
    if not alt or not alt.strip() or alt.strip().lower() in _GENERIC_ALT:
        raise ValueError('an explicit figure alt must state the visual conclusion')
    return alt.strip()


def _walk(layers: Any) -> list[tuple[str, list[Any]]]:
    """``layers`` normalized to ``(role, geometries)`` in caller order."""
    import gometry as gm

    entries = layers.items() if isinstance(layers, Mapping) else [('source', layers)]
    out = []
    for role, value in entries:
        if role not in ROLE_LABELS:
            raise ValueError(f'unknown figure role: {role!r}')
        # A scalar geometry is one drawing; an array or a list is many, and a
        # missing array row draws nothing.
        geometries = (
            [value]
            if isinstance(value, gm.Geometry)
            else [row for row in value if row is not None]
        )
        if geometries:
            out.append((role, geometries))
    return out


def _layers(
    layers: Any,
) -> tuple[list[tuple[str, list[tuple[str, list[list[Any]]]]]], bool]:
    """``layers`` laid out as ``(role, shapes)``, and whether the frame is lon/lat."""
    drawn = []
    geographic = True
    for role, geometries in _walk(layers):
        shapes = []
        for geometry in geometries:
            crs = getattr(geometry, 'crs', None)
            geographic = geographic and crs is not None and crs.is_geographic
            shapes.extend(_shapes(geometry))
        if shapes:
            drawn.append((role, shapes))
    if not drawn:
        raise ValueError('a figure needs at least one drawable geometry')
    return drawn, geographic


def _shapes(geometry: Any) -> list[tuple[str, list[list[Any]]]]:
    """One geometry as ``(kind, rings)`` primitives in drawing terms.

    Read through the structural accessors rather than ``__geo_interface__``:
    GeoJSON is a WGS 84 interchange boundary that rejects a projected CRS and
    an M ordinate outright, and figures draw both.
    """
    kind = getattr(geometry, 'geometry_type', None)
    if kind is None:
        raise TypeError(f'{type(geometry).__name__} is not a drawable geometry')
    # An empty geometry is a legitimate result; it simply draws nothing.
    if geometry.is_empty:
        return []
    if kind in {'MultiPolygon', 'MultiLineString', 'GeometryCollection'}:
        return [shape for part in geometry.parts for shape in _shapes(part)]
    if kind == 'Point':
        return [('marker', [[_xy(geometry.coords[0])]])]
    if kind == 'MultiPoint':
        return [('marker', [[_xy(position)] for position in geometry.coords])]
    if kind == 'LineString':
        return [('line', [[_xy(position) for position in geometry.coords]])]
    if kind == 'Polygon':
        return [
            (
                'area',
                [
                    [_xy(position) for position in ring.coords]
                    for ring in [geometry.exterior, *geometry.interiors]
                ],
            )
        ]
    raise TypeError(f'unsupported figure geometry type: {kind!r}')


def _xy(position: Any) -> tuple[float, float]:
    x, y = position[0], position[1]
    if not isfinite(x) or not isfinite(y):
        raise ValueError('figure positions need finite x and y coordinates')
    return float(x), float(y)


def _svg(
    drawn: Sequence[tuple[str, list[tuple[str, list[list[Any]]]]]],
    frame: Frame,
    description: str,
) -> str:
    min_x, min_y, max_x, max_y, aspect = frame
    width = (max_x - min_x) * aspect
    height = max_y - min_y
    ratio = width / height if width > 0 and height > 0 else 1.0
    view_width = min(max(_VIEW_HEIGHT * ratio, _MIN_VIEW_WIDTH), _MAX_VIEW_WIDTH)
    scale = min(
        (view_width - 2 * _PAD) / width if width > 0 else float('inf'),
        (_VIEW_HEIGHT - 2 * _PAD) / height if height > 0 else float('inf'),
    )
    if not isfinite(scale) or scale <= 0:
        scale = 1.0
    left = (view_width - width * scale) / 2
    bottom = _VIEW_HEIGHT - (_VIEW_HEIGHT - height * scale) / 2

    def screen(point: tuple[float, float]) -> tuple[float, float]:
        x, y = point
        return left + (x - min_x) * aspect * scale, bottom - (y - min_y) * scale

    body = []
    for role, shapes in sorted(drawn, key=lambda layer: _PAINT_ORDER[layer[0]]):
        cls = f'gometry-role gometry-role--{role}'
        for kind, rings in shapes:
            if kind == 'marker':
                for ring in rings:
                    x, y = screen(ring[0])
                    body.append(
                        f'<circle class="{cls} gometry-mark" '
                        f'cx="{x:.2f}" cy="{y:.2f}" r="{_MARKER_RADIUS}"/>'
                    )
                continue
            path = ' '.join(
                'M '
                + ' L '.join(f'{x:.2f} {y:.2f}' for x, y in map(screen, ring))
                + (' Z' if kind == 'area' else '')
                for ring in rings
            )
            fill = ' fill-rule="evenodd"' if kind == 'area' else ''
            body.append(f'<path class="{cls} gometry-{kind}" d="{path}"{fill}/>')
    # An intrinsic width and height make the figure size itself: Material sizes a
    # <figure> to fit its content, and a viewBox-only SVG reports a 300px
    # default width, which collapses every diagram to a thumbnail.
    return (
        f'<svg class="gometry-canvas" width="{view_width:.0f}" height="{_VIEW_HEIGHT:.0f}" '
        f'viewBox="0 0 {view_width:.0f} {_VIEW_HEIGHT:.0f}" '
        f'preserveAspectRatio="xMidYMid meet" role="img" '
        f'aria-label="{escape(description, quote=True)}">'
        f'<desc>{escape(description)}</desc>{"".join(body)}</svg>'
    )


def _legend(drawn: Sequence[tuple[str, Any]], labels: Mapping[str, str] | None) -> str:
    labels = labels or {}
    for role in labels:
        if role not in ROLE_LABELS:
            raise ValueError(f'unknown figure role: {role!r}')
    items = ''.join(
        f'<li>{_key(role, shapes)}'
        f'<span>{escape(labels.get(role, ROLE_LABELS[role]))}</span></li>'
        for role, shapes in drawn
    )
    return f'<ul class="gometry-legend">{items}</ul>'


def _key(role: str, shapes: Sequence[tuple[str, Any]]) -> str:
    """One legend swatch: the role drawn the way the figure draws it.

    The swatch is a drawing carrying the figure's own role classes, not a
    styled box beside it. Ink, fill, stroke weight, and dash pattern are then
    declared once, and a key cannot end up heavier or differently dashed than
    the thing it is a key to. It also takes its shape from what the role
    actually draws, so a role of points is keyed by a dot rather than a box.
    """
    kinds = {kind for kind, _rings in shapes}
    cls = f'gometry-role gometry-role--{role}'
    middle = _KEY_HEIGHT / 2
    if 'area' in kinds:
        shape = (
            f'<rect class="{cls} gometry-area" x="1.5" y="2" '
            f'width="{_KEY_WIDTH - 3:.0f}" height="{_KEY_HEIGHT - 4:.0f}"/>'
        )
    elif 'line' in kinds:
        shape = (
            f'<path class="{cls} gometry-line" '
            f'd="M 1.5 {middle:.0f} L {_KEY_WIDTH - 1.5:.1f} {middle:.0f}"/>'
        )
    else:
        shape = (
            f'<circle class="{cls} gometry-mark" cx="{_KEY_WIDTH / 2:.0f}" '
            f'cy="{middle:.0f}" r="{_MARKER_RADIUS}"/>'
        )
    return (
        f'<svg class="gometry-key" width="{_KEY_WIDTH:.0f}" '
        f'height="{_KEY_HEIGHT:.0f}" '
        f'viewBox="0 0 {_KEY_WIDTH:.0f} {_KEY_HEIGHT:.0f}" '
        f'aria-hidden="true">{shape}</svg>'
    )
