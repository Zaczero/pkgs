use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::{
    Bounds, BufferCapStyle, BufferJoinStyle, BufferSide, DefaultedF64Input, DefaultedI64Input,
    DistanceUnit, F64Param, GeometryArrayStorage, GridOrigin, NavigationPath, OriginSpec,
    PyGeometry, PyGeometryArray, QUADRANT_SEGMENTS_DEFAULT_I64, SimplifyMethod, SmoothMethod,
    angle_radians, finite_f64_required, parse_affine_matrix, parse_grid_size, parse_precision,
    validate_buffer_miter_limit, validate_buffer_quadrant_segments, validate_densify_fraction,
    validate_max_segment_length, validate_smooth_iterations,
};

method_doc! {
    doc_buffer;
    pre: r"Buffer a geometry by ``distance``, returning the offset region (measured for
the CRS).

On a geographic CRS, stored source-chart edges are refined when their projected
curves cannot be certified within the local-frame input budget before the planar
kernel runs. This input-fidelity budget is separate from the 0.1% frame-fit
limit and from round-arc resolution, which ``quadrant_segments`` controls; it
does not promise an end-to-end ellipsoidal distance error.
For single-sided buffers, chord directions determine endpoint normals, so the
input-fidelity budget does not bound output endpoint displacement.

Parameters
----------";
    scalar: (r"
distance : float
    Buffer radius; negative shrinks areal geometries. CRS-aware: geodesic
    meters on a geographic CRS, native linear units on a projected CRS, raw
    coordinate units when CRS-free. Must be finite.
", r"

Returns
-------
Polygon or MultiPolygon
    The offset region.

");
    array: (r"
distance : float or sequence of float
    Buffer radius; negative shrinks areal geometries. CRS-aware: geodesic
    meters on a geographic CRS, native linear units on a projected CRS, raw
    coordinate units when CRS-free — a scalar applies to every geometry, or pass one value
    per geometry. Every distance must be finite.
", r"

Returns
-------
GeometryArray
    One offset region per row.

");
    post: concat!(r"cap_style : {'round', 'flat', 'square'}, default 'round'
    End-cap shape at the original first and last vertices of open lines. Closed lines have no end caps.
    Point inputs always buffer to round discs, and MultiPoint buffers are
    unions of round discs; `cap_style` does not affect either. Sided buffers
    always use flat ends, so this parameter applies to ``side='both'`` only.

join_style : {'round', 'miter', 'bevel'}, default 'round'
    Interior corner style for both two-sided and sided line buffers, including
    180-degree reversals.

quadrant_segments : int, default 8
    Segments used to approximate a quarter-circle join in either buffer mode.

miter_limit : float, default 5.0
    With ``join_style='miter'``: how far a mitered corner may reach, in
    multiples of ``distance``; sharper corners are clipped flat at that reach.
    Applies to both two-sided and sided buffers; must be positive and finite.

side : {'both', 'left', 'right'}, default 'both'
    Which side(s) of lineal input to buffer. ``'left'``/``'right'`` build
    the one-sided strip between the line and its offset curve, with flat ends.
    These sides honor the join parameters: ``join_style``,
    ``quadrant_segments``, and ``miter_limit``. The distance must be
    non-negative.

    Buffer boundaries are synthesized and therefore returned in XY; Z/M are
    not fabricated.

", unit_parameter_doc!());
    tail: r"
Raises
------
InvalidGeometryError
    If coordinates are non-finite or buffer arithmetic produces a non-finite
    derived coordinate.
GeometryError
    If ``distance``/``quadrant_segments``/style parameters are invalid, or
    ``unit=meters`` is requested for a CRS-free geometry.
GeometryTypeError
    If ``side`` is ``'left'``/``'right'`` and the geometry is not lineal.

See Also
--------
offset_curve : One-sided raw parallel curve of a line.

Examples
--------
>>> import gometry as gm
>>> disc = gm.Point(0, 0).buffer(10)
>>> round(disc.area)  # ~ pi * 10²
312";
}

method_doc! {
    doc_offset_curve;
    pre: r"Compute a line offset to one side of a linestring by ``distance`` (measured
for the CRS).

Geographic input uses the same local-frame admission and source-chart edge
refinement as ``buffer``. Its input-fidelity budget is separate from the 0.1%
frame-fit limit and round-join resolution, which ``quadrant_segments`` controls.
Chord directions determine endpoint normals, so the input-fidelity budget does
not bound output endpoint displacement.

Parameters
----------";
    scalar: (r"
distance : float
    Offset; sign selects the side. CRS-aware: geodesic meters on a geographic
    CRS, native linear units on a projected CRS, raw coordinate units when
    CRS-free. The result is the RAW parallel
    curve: where the input folds back within ``distance`` the curve can
    self-intersect; ``buffer(side=...)`` gives the trimmed area instead. Must
    be finite.
", r"

Returns
-------
LineString, MultiLineString, or GeometryCollection
    The raw offset curve, of the input's kind.

");
    array: (r"
distance : float or sequence of float
    Offset; sign selects the side. CRS-aware: geodesic meters on a geographic
    CRS, native linear units on a projected CRS, raw coordinate units when
    CRS-free — a scalar applies to every
    geometry, or pass one value per geometry. The result is the RAW
    parallel curve: where the input folds back within ``distance`` the
    curve can self-intersect; ``buffer(side=...)`` gives the trimmed area
    instead. Every distance must be finite.
", r"

Returns
-------
GeometryArray
    One offset curve per row.

");
    post: concat!(r"join_style : {'round', 'miter', 'bevel'}, default 'round'
    Corner treatment at outside turns: ``'round'`` inscribes fillet arcs,
    ``'miter'`` extends the carriers to their crossing (clipped at
    ``miter_limit``), ``'bevel'`` connects the offsets directly.

quadrant_segments : int, default 8
    Segments per quarter circle of every round join.

miter_limit : float, default 5.0
    With ``join_style='miter'``: how far a mitered corner may reach, in
    multiples of ``distance``, before it is clipped flat.

", unit_parameter_doc!());
    tail: r"
Raises
------
GeometryError
    If ``distance``/``quadrant_segments``/style parameters are invalid.

See Also
--------
buffer : Offset region (optionally one-sided via ``side``).

Examples
--------
>>> import gometry as gm
>>> path = gm.LineString([(0, 0), (4, 0)])
>>> (path.offset_curve(1)).to_wkt()
'LINESTRING (0 1, 4 1)'";
}

method_doc! {
    doc_simplify;
    pre: r"Simplify a geometry, dropping vertices below a tolerance (planar). Two
algorithms, selected by ``method``, both reading ``tolerance`` on the
same distance scale:

- ``'vw'`` (Visvalingam-Whyatt, the default) removes the least visually
  significant vertices first — the smallest effective triangle spanned
  with its two neighbors — for a smoother, more natural cartographic
  result. The effective-area threshold is ``tolerance**2 / 2``.
- ``'dp'`` (Douglas-Peucker) compares exact distances to the closed retained
  chord against ``tolerance``. Equal maximum distances select the earliest
  source vertex.

Parameters
----------";
    scalar: (r"
tolerance : float
    Distance scale of removable detail in coordinate space: native angular
    units for geographic CRSs, native linear units for projected CRSs, and
    raw coordinate units when CRS-free.
", r"

Returns
-------
Geometry
    The simplified geometry (same kind as the input).

");
    array: (r"
tolerance : float or sequence of float
    Distance scale of removable detail in coordinate space — native angular
    units for geographic CRSs, native linear units for projected CRSs, and raw
    coordinate units when CRS-free. A scalar applies to every geometry, or
    pass one value per geometry.
", r"

Returns
-------
GeometryArray
    One simplified geometry per row (kinds preserved).

");
    post: r"method : {'vw', 'dp'}, default 'vw'
    ``'vw'`` is area-based (Visvalingam-Whyatt); ``'dp'`` is distance-
    based (Douglas-Peucker).

preserve_topology : bool, default True
    Guarantee the output keeps the input's topology: a polygon stays valid
    and non-collapsed, a simple line stays simple. The raw algorithm runs
    first (the fast path); a guarded pass only kicks in when it broke
    something. ``False`` is the raw algorithm.";
    tail: r"
Raises
------
GeometryError
    If ``tolerance`` is negative or non-finite.

See Also
--------
coverage_simplify : Topology-preserving simplification across a polygonal coverage.

Examples
--------
>>> import gometry as gm
>>> wiggly = gm.LineString([(0, 0), (1, 0.1), (2, -0.1), (3, 0)])
>>> (wiggly.simplify(1.0)).to_wkt()
'LINESTRING (0 0, 3 0)'";
}

method_doc! {
    doc_smooth;
    pre: r"Smooth line and polygon boundary linework (planar).
Two algorithms, selected by ``method``:

- ``'chaikin'`` (the default) applies corner-cutting quadratic B-spline
  refinement. Each iteration replaces every edge with points at
  one-quarter and three-quarters along it (~doubling vertices). Open
  lines honor ``keep_endpoints``; polygon rings are always treated as
  cyclic.
- ``'catmull_rom'`` subdivides each segment with a centripetal Catmull-Rom
  cubic that passes through every original vertex. ``iterations`` sets the
  per-segment sample count as ``2**iterations`` (``0`` is identity).
  It always interpolates endpoints, so ``keep_endpoints=False`` is rejected.

Parameters
----------";
    scalar: (r"
iterations : int
    Smoothing strength; ``0`` returns the input unchanged.
", r"

Returns
-------
Geometry
    The smoothed geometry (same kind as the input).

");
    array: (r"
iterations : int or sequence of int
    Smoothing strength — a scalar applies to every geometry, or pass one
    value per geometry. ``0`` returns the input unchanged.
", r"

Returns
-------
GeometryArray
    One smoothed geometry per row (kinds preserved).

");
    post: r"method : {'chaikin', 'catmull_rom'}, default 'chaikin'
    ``'chaikin'`` corner-cuts every edge; ``'catmull_rom'`` subdivides
    with a centripetal interpolating cubic.

keep_endpoints : bool, default True
    For open lines under ``'chaikin'``, hold the first and last vertices
    fixed. Rings are cyclic; Catmull-Rom requires ``True``.";
    tail: r"
Raises
------
GeometryError
    If ``iterations`` is negative, or if it would smooth the geometry to
    more coordinates than the output budget allows (a tiny input with a
    very large ``iterations``).

Examples
--------
>>> import gometry as gm
>>> square = gm.box(0, 0, 1, 1)
>>> square.smooth(iterations=1).area < square.area
True";
}

method_doc! {
    doc_snap_to_grid;
    body: r"Snap every coordinate onto a regular grid and clean the result. X/Y move to
the nearest ``origin + k * grid_size`` grid node; consecutive duplicate vertices
collapse, and parts that degenerate below their minimum become empty (the
PostGIS ``ST_SnapToGrid`` shape — output may be non-simple). Z/M ride on
surviving vertices. quantize is the decimal-rounding, vertex-preserving
sibling.

Parameters
----------
grid_size : float or tuple of float
    Positive finite coordinate-space grid spacing — native angular units for
    geographic CRSs, native linear units for projected CRSs, and raw
    coordinate units when CRS-free. Pass one value for a square grid or
    ``(sx, sy)``.
    Zero is invalid here; ``coverage_clean(grid_size=0)`` instead disables
    snapping.

origin : tuple of float, default (0.0, 0.0)
    A grid node anchoring the lattice.

repair : bool, default False
    If ``True``, guarantee a valid result: snap, linework-repair, and re-
    snap to a fixpoint. The geometry kind may change (a ``Polygon`` whose
    snapped shell pinches splits into a ``MultiPolygon``). Geographic
    ``repair=True`` validates the exact quotient result; projected and
    CRS-free geometry remains planar.";
    scalar: r"

Returns
-------
Geometry
    The snapped geometry; parts below their minimum degenerate to empty.

";
    array: r"

Returns
-------
GeometryArray
    One snapped geometry per row.

";
    tail: r"
Raises
------
GeometryError
    If ``grid_size`` or ``origin`` is invalid, or the grid is too fine for the
    coordinate magnitude.
InvalidGeometryError
    If ``repair=True`` and repair would have to invent Z/M ordinates, or
    the snap-repair loop cannot converge.

Examples
--------
>>> import gometry as gm
>>> jittery = gm.LineString([(0.12, 0.88), (2.49, 1.51)])
>>> jittery.snap_to_grid(grid_size=0.5).to_wkt()
'LINESTRING (0 1, 2.5 1.5)'";
}

method_doc! {
    doc_remove_repeated_points;
    pre: r"Drop consecutive duplicate coordinates within ``tolerance``.

Parameters
----------";
    scalar: (r"
tolerance : float, default 0.0
    Finite non-negative coordinate-space tolerance: native angular units for
    geographic CRSs, native linear units for projected CRSs, and raw coordinate
    units when CRS-free.
", r"

Returns
-------
Geometry
    The deduplicated geometry (same kind as the input).

");
    array: (r"
tolerance : float or sequence of float, default 0.0
    Finite non-negative coordinate-space tolerance — native angular units for
    geographic CRSs, native linear units for projected CRSs, and raw coordinate
    units when CRS-free. A scalar applies to every geometry, or pass one value
    per geometry.
", r"

Returns
-------
GeometryArray
    One deduplicated geometry per row (kinds preserved).

");
    post: r"";
    tail: r"
Raises
------
GeometryError
    If ``tolerance`` is negative or non-finite.

Examples
--------
>>> import gometry as gm
>>> stuttery = gm.LineString([(0, 0), (0, 0), (1, 1)])
>>> stuttery.remove_repeated_points().to_wkt()
'LINESTRING (0 0, 1 1)'";
}

method_doc! {
    doc_segmentize;
    pre: r"Densify linework by inserting vertices so no segment exceeds
``max_length`` (or a fraction of its length).

By default, each geographic edge keeps its stored shape: the straight line
between its longitude/latitude vertices. ``max_length`` is measured along that
edge on the ellipsoid. ``path='geodesic'`` or ``path='rhumb'`` instead places
new vertices along the shortest ellipsoidal route or the constant-bearing route
between each pair of endpoints. The result remains a polyline with straight
longitude/latitude edges, so its length approaches the selected route length as
the pieces get shorter. Projected geometries use native linear units, and
CRS-free geometries use raw coordinate units. Every original vertex survives
unchanged — this operation only inserts. Consecutive equal-X/Y vertices with
distinct active Z/M ordinates remain in their stored order; their zero-length
segments are not subdivided.

Parameters
----------";
    scalar: (r"
max_length : float, optional
    Positive maximum segment length, interpreted for the CRS (see ``unit``).
    Pass this
    positional argument or use ``fraction``, but not both.

fraction : float, optional
    Fraction in ``(0, 1]`` of each source segment. Keyword-only; use when
    the subdivision is naturally relative rather than expressed in units.
path : {'geodesic', 'rhumb'}, optional
    Route for inserted geographic vertices. ``None`` keeps each stored edge;
    ``'geodesic'`` follows the shortest ellipsoidal route and is planar for
    projected or CRS-free inputs; ``'rhumb'`` follows a constant-bearing route
    and requires a geographic CRS. ``fraction`` is measured along the selected
    route.
", r"

Returns
-------
Geometry
    The segmentized geometry (same kind as the input).

");
    array: (r"
max_length : float or sequence of float, optional
    Positive maximum segment length, interpreted for the CRS (see ``unit``) —
    a scalar applies to every geometry, or pass one value per geometry. Pass this
    positional argument or use ``fraction``, but not both.

fraction : float or sequence of float, optional
    Fraction in ``(0, 1]`` of each source segment. Keyword-only; a scalar
    applies to every geometry, or pass one value per geometry.
path : {'geodesic', 'rhumb'}, optional
    Route for inserted geographic vertices. ``None`` keeps each stored edge;
    ``'geodesic'`` follows the shortest ellipsoidal route and is planar for
    projected or CRS-free inputs; ``'rhumb'`` follows a constant-bearing route
    and requires a geographic CRS. ``fraction`` is measured along the selected
    route.
", r"

Returns
-------
GeometryArray
    One segmentized geometry per row (kinds preserved).

");
    post: concat!(
        unit_parameter_doc!(),
        r"    The override applies only to ``max_length``; ``fraction`` has no unit
    and cannot be combined with ``unit``. ``path`` cannot be combined with
    ``unit``; geographic route distances are meters."
    );
    tail: r"
Raises
------
CRSError
    If ``unit='meters'`` is requested and the CRS lacks linear axis units, or
    ``path='rhumb'`` is requested without a geographic CRS, or the selected
    geodesic route has multiple shortest paths.
GeometryError
    If neither or both constraints are supplied, ``max_length`` is not a
    positive finite number, ``fraction`` is outside ``(0, 1]``, ``unit`` is
    combined with ``fraction``, ``path`` is combined with ``unit``,
    ``unit='meters'`` is requested for a CRS-free geometry, or the
    endpoint-preserving binary64 output cannot satisfy ``max_length``.

Examples
--------
>>> import gometry as gm
>>> gm.LineString([(0, 0), (4, 0)]).segmentize(2).to_wkt()
'LINESTRING (0 0, 2 0, 4 0)'
>>> # On a geographic CRS the bound is meters along the ellipsoid.
>>> line = gm.LineString([(0, 0), (1, 0)], crs=4326)
>>> len(list(line.segmentize(20_000).coords))
7";
}

method_doc! {
    doc_clip_by_rect;
    body: r"Return the intersection with the closed rectangle
``(minx, miny, maxx, maxy)``. Its WKB is identical to
``intersection(self, box(minx, miny, maxx, maxy))`` when the box carries the
same CRS and coordinate epoch. For a geographic interval crossing the
antimeridian, use ``wrap='split'`` with a matching CRS. Point-only and edge-only
contacts are retained; equal ``minx``/``maxx`` or ``miny``/``maxy`` select a
closed line or point.

When the closed rectangle contains the subject's raw bounds, the subject is
returned verbatim for Cartesian inputs and canonical seam-free geographic bounds.

Source ordinates are carried where meaningful; synthesized clip vertices use
the operation's natural XY result.

In a geographic CRS, ``minx``/``maxx`` are the west/east longitudes and must be
canonical endpoints in ``[-180, 180]``. ``west <= east`` selects the ordinary
closed interval; ``west > east`` selects the closed interval crossing the
antimeridian. ``west=-180`` and ``east=180`` select a full longitude band, and
equal endpoints select a zero-width meridian. ``miny``/``maxy`` are the
south/north latitudes and must be finite with ``south <= north``.

Geographic inputs use frame-aware overlay so longitude aliases, seam crossings,
and pole vertices follow the CRS topology. Geographic rectangle horizontal
edges follow parallels and use the same construction as :func:`box` when that
box uses the same CRS and coordinate epoch.

Parameters
----------
minx, miny, maxx, maxy : float
    The Cartesian bounds, or the geographic west, south, east, and north
    endpoints respectively.";
    scalar: r"

Returns
-------
Geometry
    The clipped geometry (kind may change; empty when fully outside).

";
    array: r"

Returns
-------
GeometryArray
    One clipped geometry per row.

";
    tail: r"
Raises
------
GeometryError
    If the rectangle coordinates are non-finite, south exceeds north, or a
    geographic west/east endpoint is outside ``[-180, 180]``, or no adjacent
    binary64 assignment preserves the exact clipped result or the bounded
    embedding search is exhausted.
InvalidGeometryError
    If the input contains coordinates outside its geographic CRS domain.

Examples
--------
>>> import gometry as gm
>>> gm.LineString([(0, 0), (10, 0)]).clip_by_rect(2, -1, 5, 1).to_wkt()
'LINESTRING (2 0, 5 0)'";
}

method_doc! {
    doc_normalize;
    body: r"Return a geometry in canonical (normalized) form. The canonical form is the
lexicographically smallest equivalent presentation: parts sort ascending,
lines take their smaller direction (closed lines the smallest rotation), and
polygon rings lead with their minimum vertex under RFC 7946 winding
(exterior counter-clockwise, holes clockwise).";
    scalar: r"

Returns
-------
Geometry
    The canonical form (same kind as the input).

";
    array: r"

Returns
-------
GeometryArray
    The canonical form of every element (kinds preserved).

";
    tail: r"Examples
--------
>>> import gometry as gm
>>> messy = gm.from_wkt('MULTIPOINT ((1 1), (0 0))')
>>> messy.normalize().to_wkt()
'MULTIPOINT ((0 0), (1 1))'";
}

method_doc! {
    doc_reverse;
    body: r"Reverse the vertex order of a geometry. Linework retains every stored
vertex and its active Z/M ordinates, including equal-X/Y vertices, in reverse
order.";
    scalar: r"

Returns
-------
Geometry
    The geometry with vertex order reversed (same kind as the input).

";
    array: r"

Returns
-------
GeometryArray
    One reversed geometry per row (kinds preserved).

";
    tail: r"Examples
--------
>>> import gometry as gm
>>> line = gm.LineString([(0, 0), (1, 1), (2, 2)])
>>> line.reverse().to_wkt()
'LINESTRING (2 2, 1 1, 0 0)'";
}

method_doc! {
    doc_orient_polygons;
    body: r"Orient polygon rings to a consistent winding.

Parameters
----------
ccw : bool, default True
    ``True`` (default): exterior CCW, holes CW; ``False`` flips.";
    scalar: r"

Returns
-------
Geometry
    The geometry with ring winding normalized (same kind as the input).

";
    array: r"

Returns
-------
GeometryArray
    One reoriented geometry per row (kinds preserved).

";
    tail: r"Examples
--------
>>> import gometry as gm
>>> cw = gm.Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])
>>> cw.orient_polygons().exterior.is_ccw
True";
}

method_doc! {
    doc_swap_xy;
    body: r"Swap the X and Y ordinate of every coordinate (Z/M untouched). The axis-
order repair for data delivered latitude-first: gometry is always ``(x, y)``
= ``(lon, lat)``, so latitude-ordered input swaps once on ingest.";
    scalar: r"

Returns
-------
Geometry
    The geometry with X and Y swapped (same kind as the input).

";
    array: r"

Returns
-------
GeometryArray
    One swapped geometry per row (kinds preserved).

";
    tail: r"Examples
--------
>>> import gometry as gm
>>> latitude_first = gm.Point(52.5, 13.4)
>>> latitude_first.swap_xy().to_wkt()
'POINT (13.4 52.5)'";
}

method_doc! {
    doc_quantize;
    body: r"Round coordinates to a fixed number of decimal places.

Parameters
----------
precision : int
    Decimal places to keep (``0``-``15``).";
    scalar: r"

Returns
-------
Geometry
    The rounded geometry (same kind as the input; vertices preserved).

";
    array: r"

Returns
-------
GeometryArray
    One rounded geometry per row (kinds preserved).

";
    tail: r"
Raises
------
GeometryError
    If ``precision`` is outside ``0``-``15``.

Examples
--------
>>> import gometry as gm
>>> gm.Point(2.3479218, 48.8589321).quantize(3).to_wkt()
'POINT (2.348 48.859)'";
}

method_doc! {
    doc_subdivide;
    body: r"Split geometry into parts of bounded complexity. Recursively clips
across the longer axis until every part has at most ``max_vertices``
coordinates (the PostGIS ``ST_Subdivide`` shape). Linework with no
representable rectangular cut is divided between source vertices. Parts cover
the input up to binary64 rounding at constructed cut vertices. Source
ordinates are carried where meaningful; synthesized clip vertices use the
operation's natural XY result.

Parameters
----------
max_vertices : int, default 256
    Maximum coordinates per part (at least ``8``).";
    scalar: r"

Returns
-------
GeometryArray
    The parts of the input geometry, in input order.

";
    array: r"

Returns
-------
Groups of GeometryArray
    One ragged group of parts per input geometry, in input order (row ``i``
    is ``self[i].subdivide(...)``).

";
    tail: r"
Raises
------
GeometryError
    If ``max_vertices`` is below ``8``, exact partitioning cannot produce
    parts within the requested bound, no adjacent binary64 assignment
    preserves the exact subdivision, or the bounded embedding search is
    exhausted.

Examples
--------
>>> import gometry as gm
>>> parts = gm.LineString([(i, 0) for i in range(20)]).subdivide(max_vertices=8)
>>> len(parts)
4";
}

method_doc! {
    doc_affine_transform;
    body: r"Apply a 2D affine transform ``(a, b, d, e, xoff, yoff)`` to a geometry.

Parameters
----------
matrix : sequence of float
    The six affine coefficients (a, b, d, e, xoff, yoff).";
    scalar: r"

Returns
-------
Geometry
    The transformed geometry (same kind as the input).

";
    array: r"

Returns
-------
GeometryArray
    One transformed geometry per row (kinds preserved).

";
    tail: r"
Raises
------
GeometryError
    If ``matrix`` is not 6 finite numbers.

Examples
--------
>>> import gometry as gm
>>> gm.LineString([(0, 0), (1, 1)]).affine_transform([2, 0, 0, 2, 1, 1]).to_wkt()
'LINESTRING (1 1, 3 3)'";
}

method_doc! {
    doc_translate;
    body: r"Translate a geometry by ``(x_offset, y_offset)``.

Parameters
----------
x_offset, y_offset : float
    Offsets along the X and Y axes.";
    scalar: r"

Returns
-------
Geometry
    The transformed geometry (same kind as the input).

";
    array: r"

Returns
-------
GeometryArray
    One transformed geometry per row (kinds preserved).

";
    tail: r"Examples
--------
>>> import gometry as gm
>>> gm.Point(1, 2).translate(10, 20).to_wkt()
'POINT (11 22)'";
}

method_doc! {
    doc_rotate;
    body: r"Rotate a geometry about an origin.

Parameters
----------
angle : float
    Rotation angle (degrees by default; radians if ``radians=True``).

origin : str or sequence of float, optional
    Transform origin: ``'centroid'`` (default), ``'center'``, or an ``(x,
    y)`` point.

radians : bool, optional
    Interpret ``angle`` in radians instead of degrees.";
    scalar: r"

Returns
-------
Geometry
    The transformed geometry (same kind as the input).

";
    array: r"

Returns
-------
GeometryArray
    One transformed geometry per row (kinds preserved).

";
    tail: r"Examples
--------
>>> import gometry as gm
>>> gm.Point(1, 0).rotate(90, origin=(0, 0)).to_wkt(precision=6)
'POINT (0 1)'";
}

method_doc! {
    doc_scale;
    body: r"Scale a geometry about an origin.

Parameters
----------
x_factor, y_factor : float
    Scale factors along the X and Y axes; ``y_factor`` defaults to ``x_factor``.

origin : str or sequence of float, optional
    Transform origin: ``'centroid'`` (default), ``'center'``, or an ``(x,
    y)`` point.";
    scalar: r"

Returns
-------
Geometry
    The transformed geometry (same kind as the input).

";
    array: r"

Returns
-------
GeometryArray
    One transformed geometry per row (kinds preserved).

";
    tail: r"Examples
--------
>>> import gometry as gm
>>> gm.box(0, 0, 1, 1).scale(2, 2, origin=(0, 0)).bounds
(0.0, 0.0, 2.0, 2.0)";
}

method_doc! {
    doc_skew;
    body: r"Skew (shear) a geometry about an origin.

Parameters
----------
x_angle, y_angle : float, default 0.0
    Shear angles along the X and Y axes (degrees by default; radians if
    ``radians=True``).

origin : str or sequence of float, optional
    Transform origin: ``'centroid'`` (default), ``'center'``, or an ``(x,
    y)`` point.

radians : bool, optional
    Interpret ``x_angle``/``y_angle`` in radians instead of degrees.";
    scalar: r"

Returns
-------
Geometry
    The transformed geometry (same kind as the input).

";
    array: r"

Returns
-------
GeometryArray
    One transformed geometry per row (kinds preserved).

";
    tail: r"Examples
--------
>>> import gometry as gm
>>> gm.Point(0, 2).skew(x_angle=45, origin=(0, 0)).to_wkt(precision=6)
'POINT (2 2)'";
}

#[path = "unary_transform_affine.rs"]
mod unary_transform_affine;
#[path = "unary_transform_offset.rs"]
mod unary_transform_offset;
