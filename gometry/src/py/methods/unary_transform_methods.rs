use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::geometry::affine_about;
use crate::{
    Bounds, BufferCapStyle, BufferJoinStyle, BufferSide, DefaultedF64Input, DefaultedI64Input,
    DistanceUnit, F64Param, GeometryArrayStorage, GridOrigin, OriginSpec, PyGeometry,
    PyGeometryArray, QUADRANT_SEGMENTS_DEFAULT_I64, SimplifyMethod, SmoothMethod, angle_radians,
    finite_f64_required, parse_affine_matrix, parse_grid_size, parse_precision,
    validate_buffer_miter_limit, validate_buffer_quadrant_segments, validate_densify_fraction,
    validate_max_segment_length, validate_smooth_iterations,
};

macro_rules! doc_buffer {
    (scalar) => {
        concat!(doc_buffer!(@pre), r"
distance : float
    Buffer radius; negative shrinks areal geometries. CRS-aware: geodesic
    meters on a geographic CRS, native units on a projected one, coordinate
    units otherwise.
", doc_buffer!(@post), r"

Returns
-------
Polygon or MultiPolygon
    The offset region.

", doc_buffer!(@tail))
    };
    (array) => {
        concat!(doc_buffer!(@pre), r"
distance : float or sequence of float
    Buffer radius; negative shrinks areal geometries. CRS-aware: geodesic
    meters on a geographic CRS, native units on a projected one, coordinate
    units otherwise — a scalar applies to every geometry, or pass one value
    per geometry.
", doc_buffer!(@post), r"

Returns
-------
GeometryArray
    One offset region per row.

", doc_buffer!(@tail))
    };
    (@pre) => {
        r"Buffer a geometry by ``distance``, returning the offset region (measured for
the CRS).

Parameters
----------"
    };
    (@post) => {
        r"cap_style : {'round', 'flat', 'square'}, default 'round'
    End-cap shape for open ends.

join_style : {'round', 'miter', 'bevel'}, default 'round'
    Corner join shape.

quadrant_segments : int, default 8
    Segments used to approximate a quarter circle.

miter_limit : float, default 5.0
    With ``join_style='miter'``: how far a mitered corner may reach, in
    multiples of ``distance``; sharper corners are clipped flat at that
    reach. Must be positive and finite.

side : {'both', 'left', 'right'}, default 'both'
    Which side(s) of lineal input to buffer. ``'left'``/``'right'`` build
    the one-sided strip between the line and its offset curve (flat ends,
    miter joins — ``offset_curve(join_style='miter')`` closed into a
    polygon); the style parameters apply to ``'both'`` only, and sided
    buffers take a non-negative distance.

    Buffer boundaries are synthesized and therefore returned in XY; Z/M are
    not fabricated.

unit : {'planar', 'meters'}, default None
    Omitted follows the CRS: geodesic meters on a geographic CRS, native units on a projected one, coordinate units without a CRS. ``planar``
    forces raw coordinate units (degrees-as-Cartesian on a geographic CRS
    — only for deliberate coordinate-space math); ``meters`` forces the
    CRS metric and raises without a CRS."
    };
    (@tail) => {
        r"
Raises
------
InvalidGeometryError
    If coordinates are non-finite.
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
>>> round(disc.area)  # ~ pi * 10^2
312"
    };
}

macro_rules! doc_offset_curve {
    (scalar) => {
        concat!(doc_offset_curve!(@pre), r"
distance : float
    Offset; sign selects the side. CRS-aware: geodesic meters on a geographic
    CRS, native units on a projected one, coordinate units otherwise. The result is the RAW parallel
    curve: where the input folds back within ``distance`` the curve can
    self-intersect (GEOS trims such curls away); ``buffer(side=...)``
    gives the trimmed area instead.
", doc_offset_curve!(@post), r"

Returns
-------
LineString or MultiLineString
    The raw offset curve.

", doc_offset_curve!(@tail))
    };
    (array) => {
        concat!(doc_offset_curve!(@pre), r"
distance : float or sequence of float
    Offset; sign selects the side. CRS-aware: geodesic meters on a geographic
    CRS, native units on a projected one, coordinate units otherwise — a scalar applies to every
    geometry, or pass one value per geometry. The result is the RAW
    parallel curve: where the input folds back within ``distance`` the
    curve can self-intersect (GEOS trims such curls away);
    ``buffer(side=...)`` gives the trimmed area instead.
", doc_offset_curve!(@post), r"

Returns
-------
GeometryArray
    One offset curve per row.

", doc_offset_curve!(@tail))
    };
    (@pre) => {
        r"Compute a line offset to one side of a linestring by ``distance`` (measured
for the CRS).

Parameters
----------"
    };
    (@post) => {
        r"join_style : {'round', 'miter', 'bevel'}, default 'round'
    Corner treatment at outside turns: ``'round'`` inscribes fillet arcs,
    ``'miter'`` extends the carriers to their crossing (clipped at
    ``miter_limit``), ``'bevel'`` connects the offsets directly.

quadrant_segments : int, default 8
    Segments per quarter circle of every round join.

miter_limit : float, default 5.0
    With ``join_style='miter'``: how far a mitered corner may reach, in
    multiples of ``distance``, before it is clipped flat.

unit : {'planar', 'meters'}, default None
    Omitted follows the CRS: geodesic meters on a geographic CRS, native units on a projected one, coordinate units without a CRS. ``planar``
    forces raw coordinate units (degrees-as-Cartesian on a geographic CRS
    — only for deliberate coordinate-space math); ``meters`` forces the
    CRS metric and raises without a CRS."
    };
    (@tail) => {
        r"
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
'LINESTRING (0 1, 4 1)'"
    };
}

macro_rules! doc_simplify {
    (scalar) => {
        concat!(doc_simplify!(@pre), r"
tolerance : float
    Distance scale of removable detail, in coordinate units.
", doc_simplify!(@post), r"

Returns
-------
Geometry
    The simplified geometry (same kind as the input).

", doc_simplify!(@tail))
    };
    (array) => {
        concat!(doc_simplify!(@pre), r"
tolerance : float or sequence of float
    Distance scale of removable detail, in coordinate units — a scalar
    applies to every geometry, or pass one value per geometry.
", doc_simplify!(@post), r"

Returns
-------
GeometryArray
    One simplified geometry per row (kinds preserved).

", doc_simplify!(@tail))
    };
    (@pre) => {
        r"Simplify a geometry, dropping vertices below a tolerance (planar). Two
algorithms, selected by ``method``, both reading ``tolerance`` on the
same distance scale:

- ``'vw'`` (Visvalingam-Whyatt, the default) removes the least visually
  significant vertices first — the smallest effective triangle spanned
  with its two neighbors — for a smoother, more natural cartographic
  result. The effective-area threshold is ``tolerance**2 / 2``.
- ``'dp'`` (Douglas-Peucker) removes vertices whose perpendicular distance
  from the retained chord is within ``tolerance``.

Parameters
----------"
    };
    (@post) => {
        r"method : {'vw', 'dp'}, default 'vw'
    ``'vw'`` is area-based (Visvalingam-Whyatt); ``'dp'`` is distance-
    based (Douglas-Peucker).

preserve_topology : bool, default True
    Guarantee the output keeps the input's topology: a polygon stays valid
    and non-collapsed, a simple line stays simple. The raw algorithm runs
    first (the fast path); a guarded pass only kicks in when it broke
    something. ``False`` is the raw algorithm."
    };
    (@tail) => {
        r"
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
'LINESTRING (0 0, 3 0)'"
    };
}

macro_rules! doc_smooth {
    (scalar) => {
        concat!(doc_smooth!(@pre), r"
iterations : int
    Smoothing strength; ``0`` returns the input unchanged.
", doc_smooth!(@post), r"

Returns
-------
Geometry
    The smoothed geometry (same kind as the input).

", doc_smooth!(@tail))
    };
    (array) => {
        concat!(doc_smooth!(@pre), r"
iterations : int or sequence of int
    Smoothing strength — a scalar applies to every geometry, or pass one
    value per geometry. ``0`` returns the input unchanged.
", doc_smooth!(@post), r"

Returns
-------
GeometryArray
    One smoothed geometry per row (kinds preserved).

", doc_smooth!(@tail))
    };
    (@pre) => {
        r"Smooth line and polygon boundary linework (planar).
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
----------"
    };
    (@post) => {
        r"method : {'chaikin', 'catmull_rom'}, default 'chaikin'
    ``'chaikin'`` corner-cuts every edge; ``'catmull_rom'`` subdivides
    with a centripetal interpolating cubic.

keep_endpoints : bool, default True
    For open lines under ``'chaikin'``, hold the first and last vertices
    fixed. Rings are cyclic; Catmull-Rom requires ``True``."
    };
    (@tail) => {
        r"
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
True"
    };
}

macro_rules! doc_snap_to_grid {
    (scalar) => {
        concat!(doc_snap_to_grid!(@body), r"

Returns
-------
Geometry
    The snapped geometry; parts below their minimum degenerate to empty.

", doc_snap_to_grid!(@tail))
    };
    (array) => {
        concat!(doc_snap_to_grid!(@body), r"

Returns
-------
GeometryArray
    One snapped geometry per row.

", doc_snap_to_grid!(@tail))
    };
    (@body) => {
        r"Snap every coordinate onto a regular grid and clean the result. X/Y move to
the nearest ``origin + k * size`` grid node; consecutive duplicate vertices
collapse, and parts that degenerate below their minimum become empty (the
PostGIS ``ST_SnapToGrid`` shape — output may be non-simple). Z/M ride on
surviving vertices. quantize is the decimal-rounding, vertex-preserving
sibling.

Parameters
----------
size : float or tuple of float
    Grid spacing — one value for a square grid or ``(sx, sy)`` (positive
    finite).

origin : tuple of float, default (0.0, 0.0)
    A grid node anchoring the lattice.

repair : bool, default False
    If ``True``, guarantee a valid result: snap, linework-repair, and re-
    snap to a fixpoint. The geometry kind may change (a ``Polygon`` whose
    snapped shell pinches splits into a ``MultiPolygon``). Geographic
    antimeridian crossings use normalized validity; projected and CRS-free
    geometry remains planar."
    };
    (@tail) => {
        r"
Raises
------
GeometryError
    If ``size`` or ``origin`` is invalid, or the grid is too fine for the
    coordinate magnitude.
InvalidGeometryError
    If ``repair=True`` and repair would have to invent Z/M ordinates, or
    the snap-repair loop cannot converge.

Examples
--------
>>> import gometry as gm
>>> jittery = gm.LineString([(0.12, 0.88), (2.49, 1.51)])
>>> (jittery.snap_to_grid(0.5)).to_wkt()
'LINESTRING (0 1, 2.5 1.5)'"
    };
}

macro_rules! doc_remove_repeated_points {
    (scalar) => {
        concat!(doc_remove_repeated_points!(@pre), r"
tolerance : float, default 0.0
    Tolerance in coordinate units (non-negative).
", doc_remove_repeated_points!(@post), r"

Returns
-------
Geometry
    The deduplicated geometry (same kind as the input).

", doc_remove_repeated_points!(@tail))
    };
    (array) => {
        concat!(doc_remove_repeated_points!(@pre), r"
tolerance : float or sequence of float, default 0.0
    Tolerance in coordinate units (non-negative) — a scalar applies to
    every geometry, or pass one value per geometry.
", doc_remove_repeated_points!(@post), r"

Returns
-------
GeometryArray
    One deduplicated geometry per row (kinds preserved).

", doc_remove_repeated_points!(@tail))
    };
    (@pre) => {
        r"Drop consecutive duplicate coordinates within ``tolerance``.

Parameters
----------"
    };
    (@post) => {
        r""
    };
    (@tail) => {
        r"
Raises
------
GeometryError
    If ``tolerance`` is negative or non-finite.

Examples
--------
>>> import gometry as gm
>>> stuttery = gm.LineString([(0, 0), (0, 0), (1, 1)])
>>> stuttery.remove_repeated_points().to_wkt()
'LINESTRING (0 0, 1 1)'"
    };
}

macro_rules! doc_segmentize {
    (scalar) => {
        concat!(doc_segmentize!(@pre), r"
max_length : float, optional
    Maximum segment length in coordinate units (positive). Pass this
    positional argument or use ``fraction``, but not both.

fraction : float, optional
    Fraction in ``(0, 1]`` of each source segment. Keyword-only; use when
    the subdivision is naturally relative rather than expressed in units.
", doc_segmentize!(@post), r"

Returns
-------
Geometry
    The segmentized geometry (same kind as the input).

", doc_segmentize!(@tail))
    };
    (array) => {
        concat!(doc_segmentize!(@pre), r"
max_length : float or sequence of float, optional
    Maximum segment length in coordinate units (positive) — a scalar
    applies to every geometry, or pass one value per geometry. Pass this
    positional argument or use ``fraction``, but not both.

fraction : float or sequence of float, optional
    Fraction in ``(0, 1]`` of each source segment. Keyword-only; a scalar
    applies to every geometry, or pass one value per geometry.
", doc_segmentize!(@post), r"

Returns
-------
GeometryArray
    One segmentized geometry per row (kinds preserved).

", doc_segmentize!(@tail))
    };
    (@pre) => {
        r"Densify linework by inserting vertices so no segment exceeds
``max_length`` (or a fraction of its length).

``max_length`` is a real-world distance measured for the CRS, exactly like
``length``: a geographic CRS subdivides along the ellipsoid in meters, a
projected CRS uses its native linear units, and a CRS-free geometry uses
coordinate units. Every original vertex survives unchanged — this operation
only inserts.

Parameters
----------"
    };
    (@post) => {
        r"
unit : {'planar', 'meters'} or None, default None
    ``None`` follows the CRS. ``'planar'`` forces raw coordinate units
    (degrees-as-Cartesian on a geographic CRS — only for deliberate
    coordinate-space math); ``'meters'`` forces the CRS metric and raises
    without a CRS. Cannot be combined with ``fraction``, which is already
    relative to each segment."
    };
    (@tail) => {
        r"
Raises
------
CRSError
    If ``unit='meters'`` is requested and the CRS lacks linear axis units.
GeometryError
    If neither or both constraints are supplied, ``max_length`` is not a
    positive finite number, ``fraction`` is outside ``(0, 1]``, ``unit`` is
    combined with ``fraction``, or ``unit='meters'`` is requested for a
    CRS-free geometry.

Examples
--------
>>> import gometry as gm
>>> gm.LineString([(0, 0), (4, 0)]).segmentize(2).to_wkt()
'LINESTRING (0 0, 2 0, 4 0)'
>>> # On a geographic CRS the bound is meters along the ellipsoid.
>>> line = gm.LineString([(0, 0), (1, 0)], crs=4326)
>>> len(list(line.segmentize(20_000).coords))
7"
    };
}

macro_rules! doc_clip_by_rect {
    (scalar) => {
        concat!(doc_clip_by_rect!(@body), r"

Returns
-------
Geometry
    The clipped geometry (kind may change; empty when fully outside).

", doc_clip_by_rect!(@tail))
    };
    (array) => {
        concat!(doc_clip_by_rect!(@body), r"

Returns
-------
GeometryArray
    One clipped geometry per row.

", doc_clip_by_rect!(@tail))
    };
    (@body) => {
        r"Clip a geometry to a rectangle ``(minx, miny, maxx, maxy)``.

Source ordinates are carried where meaningful; synthesized clip vertices use
the operation's natural XY result.

Parameters
----------
minx, miny, maxx, maxy : float
    The clip rectangle bounds."
    };
    (@tail) => {
        r"
Raises
------
GeometryError
    If the rectangle bounds are non-finite or unordered.

Examples
--------
>>> import gometry as gm
>>> gm.LineString([(0, 0), (10, 0)]).clip_by_rect(2, -1, 5, 1).to_wkt()
'LINESTRING (2 0, 5 0)'"
    };
}

macro_rules! doc_normalize {
    (scalar) => {
        concat!(doc_normalize!(@body), r"

Returns
-------
Geometry
    The canonical form (same kind as the input).

", doc_normalize!(@tail))
    };
    (array) => {
        concat!(doc_normalize!(@body), r"

Returns
-------
GeometryArray
    The canonical form of every element (kinds preserved).

", doc_normalize!(@tail))
    };
    (@body) => {
        r"Return a geometry in canonical (normalized) form. The canonical form is the
lexicographically smallest equivalent presentation: parts sort ascending,
lines take their smaller direction (closed lines the smallest rotation), and
polygon rings lead with their minimum vertex under RFC 7946 winding
(exterior counter-clockwise, holes clockwise)."
    };
    (@tail) => {
        r"Examples
--------
>>> import gometry as gm
>>> messy = gm.from_wkt('MULTIPOINT ((1 1), (0 0))')
>>> messy.normalize().to_wkt()
'MULTIPOINT ((0 0), (1 1))'"
    };
}

macro_rules! doc_reverse {
    (scalar) => {
        concat!(doc_reverse!(@body), r"

Returns
-------
Geometry
    The geometry with vertex order reversed (same kind as the input).

", doc_reverse!(@tail))
    };
    (array) => {
        concat!(doc_reverse!(@body), r"

Returns
-------
GeometryArray
    One reversed geometry per row (kinds preserved).

", doc_reverse!(@tail))
    };
    (@body) => {
        r"Reverse the vertex order of a geometry."
    };
    (@tail) => {
        r"Examples
--------
>>> import gometry as gm
>>> line = gm.LineString([(0, 0), (1, 1), (2, 2)])
>>> line.reverse().to_wkt()
'LINESTRING (2 2, 1 1, 0 0)'"
    };
}

macro_rules! doc_orient_polygons {
    (scalar) => {
        concat!(doc_orient_polygons!(@body), r"

Returns
-------
Geometry
    The geometry with ring winding normalized (same kind as the input).

", doc_orient_polygons!(@tail))
    };
    (array) => {
        concat!(doc_orient_polygons!(@body), r"

Returns
-------
GeometryArray
    One reoriented geometry per row (kinds preserved).

", doc_orient_polygons!(@tail))
    };
    (@body) => {
        r"Orient polygon rings to a consistent winding.

Parameters
----------
ccw : bool, default True
    ``True`` (default): exterior CCW, holes CW; ``False`` flips."
    };
    (@tail) => {
        r"Examples
--------
>>> import gometry as gm
>>> cw = gm.Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])
>>> cw.orient_polygons().exterior.is_ccw
True"
    };
}

macro_rules! doc_swap_xy {
    (scalar) => {
        concat!(doc_swap_xy!(@body), r"

Returns
-------
Geometry
    The geometry with X and Y swapped (same kind as the input).

", doc_swap_xy!(@tail))
    };
    (array) => {
        concat!(doc_swap_xy!(@body), r"

Returns
-------
GeometryArray
    One swapped geometry per row (kinds preserved).

", doc_swap_xy!(@tail))
    };
    (@body) => {
        r"Swap the X and Y ordinate of every coordinate (Z/M untouched). The axis-
order repair for data delivered latitude-first: gometry is always ``(x, y)``
= ``(lon, lat)``, so latitude-ordered input swaps once on ingest."
    };
    (@tail) => {
        r"Examples
--------
>>> import gometry as gm
>>> latitude_first = gm.Point(52.5, 13.4)
>>> latitude_first.swap_xy().to_wkt()
'POINT (13.4 52.5)'"
    };
}

macro_rules! doc_quantize {
    (scalar) => {
        concat!(doc_quantize!(@body), r"

Returns
-------
Geometry
    The rounded geometry (same kind as the input; vertices preserved).

", doc_quantize!(@tail))
    };
    (array) => {
        concat!(doc_quantize!(@body), r"

Returns
-------
GeometryArray
    One rounded geometry per row (kinds preserved).

", doc_quantize!(@tail))
    };
    (@body) => {
        r"Round coordinates to a fixed number of decimal places.

Parameters
----------
precision : int
    Decimal places to keep (``0``-``15``)."
    };
    (@tail) => {
        r"
Raises
------
GeometryError
    If ``precision`` is outside ``0``-``15``.

Examples
--------
>>> import gometry as gm
>>> gm.Point(2.3479218, 48.8589321).quantize(3).to_wkt()
'POINT (2.348 48.859)'"
    };
}

macro_rules! doc_subdivide {
    (scalar) => {
        concat!(doc_subdivide!(@body), r"

Returns
-------
GeometryArray
    The parts of the input geometry, in input order.

", doc_subdivide!(@tail))
    };
    (array) => {
        concat!(doc_subdivide!(@body), r"

Returns
-------
Groups of GeometryArray
    One ragged group of parts per input geometry, in input order (row ``i``
    is ``self[i].subdivide(...)``).

", doc_subdivide!(@tail))
    };
    (@body) => {
        r"Split geometry into parts of bounded complexity. Recursively halves each
geometry's bounds across the longer axis and clips until every part has at
most ``max_vertices`` coordinates (the PostGIS ``ST_Subdivide`` shape).
Parts cover the input exactly. Source ordinates are carried where meaningful;
synthesized clip vertices use the operation's natural XY result.

Parameters
----------
max_vertices : int, default 256
    Maximum coordinates per part (at least ``8``)."
    };
    (@tail) => {
        r"
Raises
------
GeometryError
    If ``max_vertices`` is below ``8``.

Examples
--------
>>> import gometry as gm
>>> parts = gm.LineString([(i, 0) for i in range(20)]).subdivide(max_vertices=8)
>>> len(parts)
4"
    };
}

macro_rules! doc_affine_transform {
    (scalar) => {
        concat!(doc_affine_transform!(@body), r"

Returns
-------
Geometry
    The transformed geometry (same kind as the input).

", doc_affine_transform!(@tail))
    };
    (array) => {
        concat!(doc_affine_transform!(@body), r"

Returns
-------
GeometryArray
    One transformed geometry per row (kinds preserved).

", doc_affine_transform!(@tail))
    };
    (@body) => {
        r"Apply a 2D affine transform ``(a, b, d, e, xoff, yoff)`` to a geometry.

Parameters
----------
matrix : sequence of float
    The six affine coefficients (a, b, d, e, xoff, yoff)."
    };
    (@tail) => {
        r"
Raises
------
GeometryError
    If ``matrix`` is not 6 finite numbers.

Examples
--------
>>> import gometry as gm
>>> gm.LineString([(0, 0), (1, 1)]).affine_transform([2, 0, 0, 2, 1, 1]).to_wkt()
'LINESTRING (1 1, 3 3)'"
    };
}

macro_rules! doc_translate {
    (scalar) => {
        concat!(doc_translate!(@body), r"

Returns
-------
Geometry
    The transformed geometry (same kind as the input).

", doc_translate!(@tail))
    };
    (array) => {
        concat!(doc_translate!(@body), r"

Returns
-------
GeometryArray
    One transformed geometry per row (kinds preserved).

", doc_translate!(@tail))
    };
    (@body) => {
        r"Translate a geometry by ``(x_offset, y_offset)``.

Parameters
----------
x_offset, y_offset : float
    Offsets along the X and Y axes."
    };
    (@tail) => {
        r"Examples
--------
>>> import gometry as gm
>>> gm.Point(1, 2).translate(10, 20).to_wkt()
'POINT (11 22)'"
    };
}

macro_rules! doc_rotate {
    (scalar) => {
        concat!(doc_rotate!(@body), r"

Returns
-------
Geometry
    The transformed geometry (same kind as the input).

", doc_rotate!(@tail))
    };
    (array) => {
        concat!(doc_rotate!(@body), r"

Returns
-------
GeometryArray
    One transformed geometry per row (kinds preserved).

", doc_rotate!(@tail))
    };
    (@body) => {
        r"Rotate a geometry about an origin.

Parameters
----------
angle : float
    Rotation angle (degrees by default; radians if ``radians=True``).

origin : str or sequence of float, optional
    Transform origin: ``'centroid'`` (default), ``'center'``, or an ``(x,
    y)`` point.

radians : bool, optional
    Interpret ``angle`` in radians instead of degrees."
    };
    (@tail) => {
        r"Examples
--------
>>> import gometry as gm
>>> gm.Point(1, 0).rotate(90, origin=(0, 0)).to_wkt(precision=6)
'POINT (0 1)'"
    };
}

macro_rules! doc_scale {
    (scalar) => {
        concat!(doc_scale!(@body), r"

Returns
-------
Geometry
    The transformed geometry (same kind as the input).

", doc_scale!(@tail))
    };
    (array) => {
        concat!(doc_scale!(@body), r"

Returns
-------
GeometryArray
    One transformed geometry per row (kinds preserved).

", doc_scale!(@tail))
    };
    (@body) => {
        r"Scale a geometry about an origin.

Parameters
----------
x_factor, y_factor : float
    Scale factors along the X and Y axes; ``y_factor`` defaults to ``x_factor``.

origin : str or sequence of float, optional
    Transform origin: ``'centroid'`` (default), ``'center'``, or an ``(x,
    y)`` point."
    };
    (@tail) => {
        r"Examples
--------
>>> import gometry as gm
>>> gm.box(0, 0, 1, 1).scale(2, 2, origin=(0, 0)).bounds
(0.0, 0.0, 2.0, 2.0)"
    };
}

macro_rules! doc_skew {
    (scalar) => {
        concat!(doc_skew!(@body), r"

Returns
-------
Geometry
    The transformed geometry (same kind as the input).

", doc_skew!(@tail))
    };
    (array) => {
        concat!(doc_skew!(@body), r"

Returns
-------
GeometryArray
    One transformed geometry per row (kinds preserved).

", doc_skew!(@tail))
    };
    (@body) => {
        r"Skew (shear) a geometry about an origin.

Parameters
----------
x_angle, y_angle : float, default 0.0
    Shear angles along the X and Y axes (degrees by default; radians if
    ``radians=True``).

origin : str or sequence of float, optional
    Transform origin: ``'centroid'`` (default), ``'center'``, or an ``(x,
    y)`` point.

radians : bool, optional
    Interpret ``x_angle``/``y_angle`` in radians instead of degrees."
    };
    (@tail) => {
        r"Examples
--------
>>> import gometry as gm
>>> gm.Point(0, 2).skew(x_angle=45, origin=(0, 0)).to_wkt(precision=6)
'POINT (2 2)'"
    };
}

#[path = "unary_transform_affine.rs"]
mod unary_transform_affine;
#[path = "unary_transform_offset.rs"]
mod unary_transform_offset;
