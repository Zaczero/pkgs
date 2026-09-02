"""Runtime documentation coverage for public Python re-exports."""

from __future__ import annotations

import importlib.util
import math
import re

import pytest

from tests._support import GOMETRY_ROOT


def _figures():
    spec = importlib.util.spec_from_file_location(
        '_figures_test', GOMETRY_ROOT / 'examples/_figures.py'
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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


def test_docs_figures_carry_no_inline_color() -> None:
    """The stylesheet owns the palette, so figures can follow the page scheme.

    A colour baked into the markup cannot answer to the dark scheme, to print,
    or to forced colors, so the renderer emits role classes and nothing else.
    """
    import gometry as gm

    figures = _figures()
    html = figures.figure(
        {
            'source': gm.box(0, 0, 4, 4),
            'query': gm.LineString([(0, 0), (4, 4)]),
            'candidate': gm.box(1, 1, 2, 2),
            'result': gm.box(0, 0, 2, 2),
            'excluded': gm.box(3, 3, 4, 4),
            'anchor': gm.Point(2, 2),
        },
        'Every canonical role',
        alt='Each canonical role is drawn with its own outline pattern, not colour alone.',
    )
    for banned in ('fill="#', 'stroke="#', 'style=', 'rgb(', 'rgba('):
        assert banned not in html, f'figure markup carries inline colour: {banned}'

    emitted = set(re.findall(r'gometry-role--([a-z]+)', html))
    assert emitted == set(figures.ROLE_LABELS)

    # A legend swatch is drawn from the figure's own role classes, so weight,
    # dash, ink, and fill are declared once and a key cannot drift from the
    # thing it is a key to. It also takes the shape that role actually draws.
    keys = dict(
        re.findall(
            r'<svg class="gometry-key".*?gometry-role--([a-z]+) gometry-([a-z]+)"', html
        )
    )
    assert set(keys) == set(figures.ROLE_LABELS)
    assert keys['source'] == 'area'
    assert keys['query'] == 'line'
    assert keys['anchor'] == 'mark'
    assert 'gometry-key--' not in html, 'the legend restates the role encoding'


def test_docs_figure_role_order_and_legend() -> None:
    """Context paints under the answer; the legend keeps the caller's order."""
    import gometry as gm

    figures = _figures()
    html = figures.figure(
        {'result': gm.box(0, 0, 2, 2), 'candidate': gm.box(1, 1, 3, 3)},
        'Refinement over its candidates',
        alt='The exact result is drawn over the candidate envelope it was refined from.',
        labels={'candidate': 'bbox candidates', 'result': 'exact rows'},
    )
    body = html[: html.index('</svg>')]
    assert body.index('gometry-role--candidate') < body.index('gometry-role--result')
    legend = html[html.index('<ul class="gometry-legend">') :]
    assert legend.index('exact rows') < legend.index('bbox candidates')

    # One role needs no key to read against.
    assert 'gometry-legend' not in figures.figure(gm.Point(0, 0), 'One role')

    with pytest.raises(ValueError, match='unknown figure role'):
        figures.figure({'exact': gm.Point(0, 0)}, 'x')
    with pytest.raises(ValueError, match='non-empty caption'):
        figures.figure(gm.Point(0, 0), ' ')


def test_docs_figure_frame_and_aspect() -> None:
    """A figure's drawing frame is a stated choice, not an accident of zoom."""
    import gometry as gm

    figures = _figures()

    def viewbox(html: str) -> str:
        match = re.search(r'viewBox="([^"]+)"', html)
        assert match
        return match.group(1)

    # Shared by default: a panel showing less of the same scene keeps its scale.
    shared = figures.panels([
        ('whole', gm.box(0, 0, 10, 10)),
        ('corner', gm.box(0, 0, 1, 1)),
    ])
    boxes = re.findall(r'viewBox="([^"]+)"', shared)
    assert boxes[0] == boxes[1]
    paths = re.findall(r'<path class="[^"]+" d="([^"]+)"', shared)
    assert paths[0] != paths[1]

    # Frames in different units are not comparable, and say so explicitly.
    separate = figures.panels(
        [('degrees', gm.box(0, 0, 1, 2)), ('metres', gm.box(0, 0, 200_000, 100_000))],
        share_frame=False,
    )
    separate_boxes = re.findall(r'viewBox="([^"]+)"', separate)
    assert separate_boxes[0] != separate_boxes[1]

    # A lon/lat frame is drawn at its latitude's aspect unless asked otherwise.
    box = gm.box(0, 60, 2, 61, crs=4326)
    assert viewbox(figures.figure(box, 'corrected')) != viewbox(
        figures.figure(box, 'raw degrees', aspect='raw')
    )
    assert viewbox(figures.figure(gm.box(0, 0, 2, 1), 'planar')) == viewbox(
        figures.figure(gm.box(0, 0, 2, 1), 'planar raw', aspect='raw')
    )
    with pytest.raises(ValueError, match="'auto' or 'raw'"):
        figures.figure(box, 'bad aspect', aspect='mercator')


def test_docs_figure_accepts_every_drawable_public_shape() -> None:
    """Figures draw the public geometry surface, including arrays and empties."""
    import gometry as gm

    figures = _figures()
    drawable = [
        gm.Point(0, 0),
        gm.MultiPoint([(0, 0), (1, 1)]),
        gm.LineString([(0, 0), (1, 1)]),
        gm.MultiLineString([[(0, 0), (1, 1)], [(1, 0), (2, 1)]]),
        gm.Polygon([(0, 0), (2, 0), (2, 2)], holes=[[(0.5, 0.5), (1, 0.5), (1, 1)]]),
        gm.from_wkt('MULTIPOLYGON (((0 0, 2 0, 2 2, 0 0)), ((3 3, 4 3, 4 4, 3 3)))'),
        gm.GeometryCollection([gm.Point(0, 0), gm.LineString([(0, 0), (1, 1)])]),
        gm.GeometryArray([gm.Point(0, 0), gm.LineString([(0, 0), (1, 1)])]),
    ]
    for geometry in drawable:
        assert '<svg' in figures.figure(geometry, f'{type(geometry).__name__} figure')

    # An empty result is a legitimate answer; it simply draws nothing.
    html = figures.figure(
        {'source': gm.box(0, 0, 1, 1), 'result': gm.from_wkt('POLYGON EMPTY')},
        'An empty overlay result',
        alt='The overlay produced no result, so only the input outline is drawn.',
    )
    assert 'gometry-role--source' in html
    assert 'gometry-role--result' not in html


def test_projection_lens_public_contract() -> None:
    """The homepage projection conclusions agree with public CRS operations."""
    import gometry as gm

    pole_x, pole_y = gm.crs_transform(4326, 'ESRI:102035', 0, 90)
    equator_x, equator_y = gm.crs_transform(4326, 'ESRI:102035', 0, 0)
    assert math.hypot(pole_x, pole_y) < 1e-6
    assert math.isclose(math.hypot(equator_x, equator_y), 6_378_137, abs_tol=1e-6)
    mollweide = gm.CRS('ESRI:54009')
    for latitude in (0, 60):
        assert math.isclose(
            mollweide.factors(0, latitude)['areal_scale'],
            1,
            rel_tol=0,
            abs_tol=2e-10,
        )


def test_comparison_panels_accessible_conclusion() -> None:
    """Comparison panels expose identity and their conclusion without color."""
    import gometry as gm

    figures = _figures()
    html = figures.panels(
        [('First projection', gm.Point(0, 0)), ('Second projection', gm.Point(1, 1))],
        caption='Same coordinates; different planar frames.',
        alt='Two labelled projections change the shape of the same coordinates.',
    )
    assert (
        'role="group" aria-label="Two labelled projections change the shape of the same coordinates."'
        in html
    )
    assert '<figcaption>First projection</figcaption>' in html
    assert '<figcaption>Second projection</figcaption>' in html
    assert (
        '<p class="gometry-panels__caption">Same coordinates; different planar frames.</p>'
        in html
    )

    with pytest.raises(ValueError, match='both caption and alt'):
        figures.panels([('Projection', gm.Point(0, 0))], caption='Conclusion')

    # Panels carrying the same roles are read against one key, not a repeated
    # one the reader has to compare panel by panel.
    same_roles = figures.panels([
        (caption, {'source': gm.box(0, 0, 2, 2), 'result': gm.box(0, 0, size, size)})
        for caption, size in (('grown once', 3), ('grown twice', 4))
    ])
    assert same_roles.count('class="gometry-legend"') == 1

    mixed_roles = figures.panels([
        ('input alone', {'source': gm.box(0, 0, 2, 2)}),
        ('with result', {'source': gm.box(0, 0, 2, 2), 'result': gm.box(0, 0, 3, 3)}),
    ])
    assert mixed_roles.count('class="gometry-legend"') == 1
    assert mixed_roles.index('class="gometry-legend"') > mixed_roles.index(
        'input alone'
    )


def test_docs_figure_separates_caption_and_conclusion() -> None:
    """Figures escape their caption and expose an explicit visual conclusion."""
    import gometry as gm

    figures = _figures()
    html = figures.figure(
        gm.LineString([(0, 0), (1, 1)]),
        'Visible <route>',
        alt='The "route" rises from left to right & retains both endpoints.',
    )
    assert '<figcaption>Visible &lt;route&gt;</figcaption>' in html
    assert (
        'aria-label="The &quot;route&quot; rises from left to right &amp; retains both endpoints."'
        in html
    )
    assert (
        '<desc>The &quot;route&quot; rises from left to right &amp; retains both endpoints.</desc>'
        in html
    )
    assert html.index('<desc>') < html.index('</svg>')

    panel_html = figures.panels([
        ('before', gm.Point(0, 0), 'The point starts at the origin.'),
        ('after', gm.Point(1, 1), 'The point moves up and to the right.'),
    ])
    assert panel_html.count('<desc>') == 2
    assert 'The point moves up and to the right.' in panel_html

    for generic in ('', ' ', 'input', 'result', 'Point', 'geometry'):
        with pytest.raises(ValueError, match='state the visual conclusion'):
            figures.figure(gm.Point(0, 0), 'visible caption', alt=generic)
