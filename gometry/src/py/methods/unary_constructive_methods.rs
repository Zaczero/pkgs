use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::{DefaultedF64Input, DistanceUnit, F64Param, PyGeometry, PyGeometryArray};

// Numpydoc text lives in `doc_<op>!()` string macros; methods attach it via `#[doc = ...]`.

method_doc! {
    doc_boundary;
    body: r"Return the topological boundary of the geometry's occupied support.";
    scalar: r"

Returns
-------
MultiPoint, LineString, MultiLineString, or GeometryCollection
    Boundary-location cells, one dimension below the effective dimension of
    the occupied support. A Polygon or MultiPolygon is evaluated as one union
    of polygonal support, so internal edges, collapsed interior spans, and
    ring edges outside that support are omitted. A GeometryCollection is
    processed member by member; non-empty member occurrences are retained.

Notes
-----
In a geographic CRS, seam aliases and all longitudes at a pole identify the
same vertex. Edges introduced only by a chart cut or pole closure are not
boundaries, although a seam vertex may appear where the geographic line
emitter splits a run. The CRS and coordinate epoch are preserved; coordinates
are not transformed. Source Z/M values are kept where emitted vertices have
unambiguous ordinates; an axis is dropped when an emitted vertex has no value.
Typed empty inputs keep their dimension-appropriate empty boundary.

Raises
------
CRSError
    If the declared CRS cannot be resolved to a topology frame.
InvalidGeometryError
    If a geographic coordinate read by boundary is outside its CRS domain.
ResourceLimitError
    If exact work exceeds the generated-item or embedding-search budget.
OverflowError
    If emitted cells cannot preserve the exact topology in binary64.

";
    array: r"

Returns
-------
GeometryArray
    One boundary per row, retaining missing rows.

Notes
-----
Each row is processed independently. Non-missing outputs retain their row's
CRS and coordinate epoch. Output coordinate axes reflect ordinates available
at every emitted vertex.

Raises
------
CRSError
    If the declared CRS cannot be resolved to a topology frame.
InvalidGeometryError
    If a geographic coordinate read by boundary is outside its CRS domain.
ResourceLimitError
    If exact work exceeds the generated-item or embedding-search budget.
OverflowError
    If emitted cells cannot preserve the exact topology in binary64.

";
    tail: r"Examples
--------
>>> import gometry as gm
>>> gm.box(0, 0, 2, 2).boundary().to_wkt()
'LINESTRING (0 0, 2 0, 2 2, 0 2, 0 0)'";
}

method_doc! {
    doc_build_area;
    body: r"Assemble linework into one areal geometry. Input ordinates are carried
where vertices can be sourced; otherwise the mathematically planar result is XY.
When returned polygons retain different coordinate axes, they remain separate
members of a GeometryCollection rather than being promoted or flattened. The
GeometryArray method processes rows independently; to assemble linework from
several geometries as one area, combine them into one GeometryCollection first.";
    scalar: r"

Returns
-------
Polygon, MultiPolygon, or GeometryCollection
    The maximal areal geometry covered by the input. A GeometryCollection is
    returned only when multiple output polygons retain different coordinate axes;
    each polygon member preserves its own ordinates.

";
    array: r"

Returns
-------
GeometryArray
    One areal geometry per input row; missing rows remain missing.

";
    tail: r"
Raises
------
InvalidGeometryError
    If the area cannot be assembled from the input linework.

Examples
--------
>>> import gometry as gm
>>> edges = [[(0,0),(2,0)],[(2,0),(0,2)],[(0,2),(0,0)]]
>>> gm.MultiLineString(edges).build_area().to_wkt()
'POLYGON ((0 0, 2 0, 0 2, 0 0))'";
}

method_doc! {
    doc_centroid;
    body: r"Area/length-weighted center of the geometry. Geographic (lon/lat) input
crossing the antimeridian is auto-split-normalized; no manual
``split_antimeridian`` is required. The computed center is an XY point.";
    scalar: r"

Returns
-------
Point
    Area/length-weighted center; may lie outside the geometry.

";
    array: r"

Returns
-------
GeometryArray[Point]
    One centroid per row (area/length-weighted; may lie outside).

";
    tail: r"See Also
--------
point_on_surface : A representative point guaranteed on the geometry.
polylabel : Pole of inaccessibility (best label anchor).

Raises
------
InvalidGeometryError
    If a finite centroid cannot be computed.

Examples
--------
>>> import gometry as gm
>>> (gm.box(0, 0, 2, 4).centroid()).to_wkt()
'POINT (1 2)'";
}

method_doc! {
    doc_concave_hull;
    pre: r"Compute the concave hull of the geometry. CRS-aware via local projection
(approximate) and does NOT auto-split antimeridian-crossing geographic input;
call ``split_antimeridian`` first. Uses gometry's chi-shape kernel: Delaunay
boundary triangles are peeled from longest edge to shortest. The geometric XY
hull is independent of input point order. Hull vertices are copies of input
vertices, so X/Y/Z/M ordinates are preserved exactly. When several input vertices
share an XY position, the first in storage order is the survivor.

Parameters
----------";
    scalar: (r"
concavity : float, default 2.0
    Sets the peel distance guard: a candidate triangle's opposite vertex must
    lie within ``edge_length / concavity`` of its boundary edge. ``0`` disables
    this guard. Must be finite and non-negative. Higher values typically keep
    more area, but the greedy peel does not guarantee area monotonicity as
    ``concavity`` increases.
length_threshold : float, default 0.0
    Boundary edges at or below this length are kept; must be finite and
    non-negative. For fixed input and
    ``concavity``, a larger threshold always gives a hull that contains the
    smaller-threshold hull, so area never decreases. Interpreted for the CRS
    (see ``unit``). On a geographic CRS the threshold is evaluated in a local
    projection, while the output vertices are emitted from the original input
    coordinates.
", r"

Returns
-------
Point, LineString, or Polygon
    The concave hull; degenerate inputs reduce dimension.

");
    array: (r"
concavity : float or sequence of float, default 2.0
    Sets the peel distance guard: a candidate triangle's opposite vertex must
    lie within ``edge_length / concavity`` of its boundary edge. ``0`` disables
    this guard. Higher values typically keep more area, but the greedy peel
    does not guarantee area monotonicity as ``concavity`` increases — a scalar
    applies to every geometry, or pass one value per geometry. Each value must
    be finite and non-negative.
length_threshold : float or sequence of float, default 0.0
    Boundary edges at or below this length are kept; each value must be finite
    and non-negative. For fixed input and
    ``concavity``, a larger threshold always gives a hull that contains the
    smaller-threshold hull, so area never decreases. Interpreted for the CRS
    (see ``unit``). On a geographic CRS the threshold is evaluated in a local
    projection, while the output vertices are emitted from the original input
    coordinates — a scalar applies to every geometry, or pass one value per
    geometry.
", r"

Returns
-------
GeometryArray
    One concave hull per row; degenerate inputs reduce dimension.

");
    post: unit_parameter_doc!();
    tail: r"
Raises
------
GeometryError
    If parameters are non-finite or negative, or selected hull vertices have different
    coordinate axis layouts.

See Also
--------
convex_hull : Smallest convex set containing the geometry (planar).

Examples
--------
>>> import gometry as gm
>>> mp = gm.MultiPoint([(0, 0), (2, 0), (2, 2), (0, 2), (1, 0.2)])
>>> hull = mp.concave_hull(concavity=1.0)
>>> (hull.geometry_type, round(hull.area, 2))
('Polygon', 3.0)";
}

method_doc! {
    doc_convex_hull;
    body: r"Compute the convex hull of the geometry. Operates in planar lon/lat space and does NOT
auto-split antimeridian-crossing geographic input; call
``split_antimeridian`` first. Source vertices retained by the hull keep their
ordinates. Geographic hull construction can add chart-support vertices on
producer root edges; see the geometry guide's ``Z/M under operations`` section
for how their ordinates are carried. When several input vertices share an XY
position, the first in storage order is the copied survivor.";
    scalar: r"

Returns
-------
Point, LineString, or Polygon
    The convex hull; degenerate inputs reduce dimension.

";
    array: r"

Returns
-------
GeometryArray
    One convex hull per row.

";
    tail: r"Raises
------
GeometryError
    If selected hull vertices have different coordinate axis layouts.

See Also
--------
concave_hull : Concave hull that can follow non-convex outlines.

Examples
--------
>>> import gometry as gm
>>> pts = gm.MultiPoint([(0, 0), (2, 0), (1, 1), (0, 2), (2, 2)])
>>> pts.convex_hull().to_wkt()
'POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'";
}

method_doc! {
    doc_envelope;
    body: r"Axis-aligned bounding-box polygon of the geometry, returned in XY.";
    scalar: r"

Returns
-------
Point, LineString, or Polygon
    The axis-aligned bounding shape (degenerate inputs reduce dimension).

";
    array: r"

Returns
-------
GeometryArray
    One envelope per row.

";
    tail: r"
Raises
------
InvalidGeometryError
    If coordinates are non-finite.

Examples
--------
>>> import gometry as gm
>>> gm.LineString([(0, 0), (3, 1)]).envelope().to_wkt()
'POLYGON ((0 0, 3 0, 3 1, 0 1, 0 0))'";
}

method_doc! {
    doc_extremes;
    body: r"Return the west, south, east, and north extreme vertices of the
geometry (numeric X/Y; ties keep the first vertex in storage order).";
    scalar: r"

Returns
-------
Extremes or None
    The ``(west, south, east, north)`` named tuple, or ``None`` for an empty
    geometry — the same contract as ``bounds`` and the other extent
    accessors.

";
    array: r"

Returns
-------
Extremes
    Four row-aligned ``Point`` arrays as ``(west, south, east, north)``.
    Missing rows stay missing in every column; empty rows degrade to an
    empty point per column.

";
    tail: r"
Examples
--------
>>> import gometry as gm
>>> extremes = gm.box(0, 0, 2, 4).extremes()
>>> assert extremes is not None  # None only for an empty geometry
>>> (extremes.west.to_wkt(), extremes.north.to_wkt())
('POINT (0 0)', 'POINT (2 4)')";
}

method_doc! {
    doc_line_merge;
    body: r"Merge connected LineString parts into longer LineStrings.";
    scalar: r"

Returns
-------
LineString or MultiLineString
    The merged linework.

";
    array: r"

Returns
-------
GeometryArray
    One merged linework per row.

";
    tail: r"
Raises
------
GeometryTypeError
    If the geometry is not lineal.

Examples
--------
>>> import gometry as gm
>>> a, b = [(0, 0), (1, 1)], [(1, 1), (2, 2)]
>>> (gm.MultiLineString([a, b]).line_merge()).to_wkt()
'LINESTRING (0 0, 1 1, 2 2)'";
}

method_doc! {
    doc_maximum_inscribed_circle;
    pre: r"Solve the largest circle inscribed in a polygonal geometry.
The returned Circle retains its center and radius independently of polygon
resolution. Read ``circle.center`` and ``circle.radius`` directly, or call
``circle.to_polygon(quadrant_segments=16)`` for a contained polygonal realization.
Planar/projected input certifies the global inradius gap against ``tolerance``;
the radius is the downward-rounded clearance at the retained representable center.
Geographic metric input solves in one retained local projection: the guarantee
applies there, rather than to the ellipsoid or restored source-coordinate polygon.
The center is returned in the source CRS and retains the source coordinate epoch.
Empty input has an empty center and NaN radius. A nonempty polygon with no
filled interior has its stored-bounds midpoint and radius zero.

Parameters
----------";
    scalar: (r"
tolerance : float, optional
    Positive absolute bound on the planar global-inradius gap, in the selected
    units. Omitted selects a scale-aware tolerance from the working geometry.
", r"

Returns
-------
Circle
    Retained circle solution; polygon construction is deferred until requested.

");
    array: (r"
tolerance : float or sequence of float, optional
    Positive absolute bound on the planar global-inradius gap, either shared
    or one value per row. Omitted selects a scale-aware tolerance per row.
", r"

Returns
-------
CircleArray
    Retained circle solutions with vectorized center and radius projections.
    Missing rows remain missing; empty and missing radii are NaN.

");
    post: unit_parameter_doc!();
    tail: r"
Raises
------
GeometryTypeError
    If the input is not polygonal.
GeometryError
    If tolerance is not positive and finite, or meters are requested without a CRS.
OverflowError
    If the requested inradius accuracy has no representable center/radius result.
CRSError
    If a geographic working frame cannot be prepared.

See Also
--------
minimum_bounding_circle : Smallest enclosing circle solution.
polylabel : Approximate label anchor from a distinct floating-point search.

Examples
--------
>>> import gometry as gm
>>> circle = gm.box(0, 0, 2, 2).maximum_inscribed_circle()
>>> (circle.center.to_wkt(), circle.radius)
('POINT (1 1)', 1.0)
>>> circle.to_polygon().geometry_type
'Polygon'";
}

method_doc! {
    doc_minimum_bounding_circle;
    body: concat!(r"Solve the smallest circle enclosing the geometry.
Read ``circle.center`` and ``circle.radius`` without constructing a polygon.
``circle.to_polygon(quadrant_segments=16)`` creates a circumscribed polygon
whose edges enclose the exact working-frame disk; a zero-radius disk yields
a Point and empty input yields POLYGON EMPTY.

For planar/projected input, the exact support disk is retained. Its center and
radius are rounded independently for numerical access; projected unit conversion
is applied before rounding. The polygon is certified against the exact disk,
not a reconstruction from those rounded values.

Geographic point sets with two distinct sites retain the midpoint of a shortest
ellipsoidal geodesic and half its inverse distance; a single site has radius zero.
This numerical solution remains available across global extents. Its polygon
uses the geographic Point.buffer local-projection approximation and may raise
CRSError when that realization exceeds the projection's accuracy domain.
Other geographic input uses one retained local-projection disk, an approximation
rather than an ellipsoidal optimum. Source-chart edges may be refined before
the projected support calculation. Geographic radii are in meters; centers and
polygons are in the source CRS.
The center retains the source coordinate epoch.

Parameters
----------
", unit_parameter_doc!());
    scalar: r"

Returns
-------
Circle
    Retained circle solution. Empty input has an empty center and NaN radius.

";
    array: r"

Returns
-------
CircleArray
    Retained circle solutions. Missing rows remain missing; numeric projections
    use NaN for empty and missing rows.

";
    tail: r"
Raises
------
GeometryError
    If meters are requested without a CRS.
CRSError
    If a geographic local-projection solution cannot fit its accuracy domain.
    One-/two-point ellipsoidal solutions do not require a local frame.
OverflowError
    If input coordinates cannot support a finite-coordinate circle calculation.

Examples
--------
>>> import gometry as gm
>>> circle = gm.MultiPoint([(0, 0), (4, 0)]).minimum_bounding_circle()
>>> (circle.center.to_wkt(), circle.radius)
('POINT (2 0)', 2.0)";
}

method_doc! {
    doc_minimum_width;
    body: concat!(r"Width of the narrowest parallel strip enclosing the geometry.
Uses the complete convex hull, including all disconnected components. Holes
do not reduce the width. This is distinct from minimum clearance and from
the shorter side of the minimum-area rotated rectangle. A point or collinear
support has width zero; empty input has width NaN.

Planar/projected input selects antipodal hull supports using exact squared-width
comparisons and rounds the final measurement after unit conversion. Geographic
metric input uses the prepared geometry in one local projection, an approximation
on the ellipsoid. Source-chart edge refinement precedes the hull calculation.

Parameters
----------
", unit_parameter_doc!());
    scalar: r"

Returns
-------
float
    Minimum strip width in the selected units.

";
    array: r"

Returns
-------
numpy.ndarray of float
    Minimum strip widths; NaN for empty and missing rows.

";
    tail: r"
Raises
------
GeometryError
    If meters are requested without a CRS.
CRSError
    If a geographic working frame cannot fit its accuracy domain.
OverflowError
    If non-finite coordinates prevent the support calculation.

Examples
--------
>>> import gometry as gm
>>> gm.box(0, 0, 4, 2).minimum_width()
2.0";
}

method_doc! {
    doc_minimum_clearance;
    body: concat!(r"Smallest distance by which a vertex could move to invalidate the geometry.
On a geographic CRS, the witness is selected in the geometry's best local
projection and then measured geodesically in source coordinates; this is a
local-projection approximation, not an exact ellipsoidal clearance search.
Original vertices remain clearance sites. Refined chart-edge samples improve
boundary support without becoming new sites; an on-edge witness is interpolated
on its original source segment.
Projected CRS and CRS-free geometries measure in the active planar units.

Parameters
----------
", unit_parameter_doc!());
    scalar: r"

Returns
-------
float
    The minimum clearance distance.

";
    array: r"

Returns
-------
numpy.ndarray
    One clearance distance per row.

";
    tail: r"Examples
--------
>>> import gometry as gm
>>> gm.box(0, 0, 3, 2).minimum_clearance()
2.0";
}

method_doc! {
    doc_minimum_clearance_line;
    body: concat!(r"Two-point line realizing `minimum_clearance`. The metric matches
minimum_clearance: on a geographic CRS, the witness is selected in the
geometry's best local projection and returned in source coordinates, so it is a
local-projection approximation rather than an exact ellipsoidal clearance
search. Original vertices remain clearance sites; inserted chart-edge support
samples do not become new sites, and an on-edge witness is interpolated on the
original source segment.
``LINESTRING EMPTY`` when the clearance is infinite (fewer than two distinct
vertices).

Parameters
----------
", unit_parameter_doc!());
    scalar: r"

Returns
-------
LineString
    The two-point line realizing the minimum clearance.

";
    array: r"

Returns
-------
GeometryArray
    One realizing line per row.

";
    tail: r"See Also
--------
minimum_clearance : The clearance distance itself.

Examples
--------
>>> import gometry as gm
>>> (gm.box(0, 0, 3, 2).minimum_clearance_line()).to_wkt()
'LINESTRING (0 0, 0 2)'";
}

method_doc! {
    doc_minimum_rotated_rectangle;
    body: r"Minimum-area rotated bounding rectangle. A computed areal rectangle is
returned in XY. A degenerate geographic result that follows source-edge support
can retain resolvable Z/M by the original edge's ellipsoidal arclength; see the
geometry guide's ``Z/M under operations`` section.";
    scalar: r"

Returns
-------
Point, LineString, or Polygon
    The minimum rotated rectangle (degenerate inputs reduce dimension).

";
    array: r"

Returns
-------
GeometryArray
    One rotated rectangle per row.

";
    tail: r"
Raises
------
InvalidGeometryError
    If the rotated rectangle cannot be computed.

Examples
--------
>>> import gometry as gm
>>> rect = gm.box(0, 0, 2, 2).minimum_rotated_rectangle()
>>> gm.equals(rect, gm.box(0, 0, 2, 2))
True";
}

method_doc! {
    doc_node;
    body: r"Node linework by splitting every edge at all intersections. Input
ordinates are carried where possible; unsourceable seam vertices yield XY.";
    scalar: r"

Returns
-------
MultiLineString
    The noded linework.

";
    array: r"

Returns
-------
GeometryArray
    Noded linework per row.

";
    tail: r"
Raises
------
OverflowError
    If no adjacent binary64 assignment preserves the exact noded linework.
ResourceLimitError
    If the bounded embedding search is exhausted.
InvalidGeometryError
    If noding fails on the input linework.

Examples
--------
>>> import gometry as gm
>>> lines = gm.MultiLineString([[(0,0),(2,0)],[(1,-1),(1,1)]])
>>> noded = lines.node()
>>> (noded.num_geometries, noded.length)
(4, 4.0)";
}

method_doc! {
    doc_point_on_surface;
    body: r"Representative point guaranteed to lie on the geometry. Geographic (lon/lat)
input crossing the antimeridian is auto-split-normalized; no manual
``split_antimeridian`` is required. The point lies inside or on the geometry,
unlike centroid. Polygonal scanline candidates are checked after rounding; an
uncovered candidate falls back to a source boundary point. The representative
is computed in XY and does not imply a source Z/M. On planar polygonal input,
the first polygon component wins when representative chords have equal width.

See Also
--------
centroid : Area/length-weighted center (may fall outside).
polylabel : Pole of inaccessibility (best label anchor).
";
    scalar: r"

Returns
-------
Point
    A point guaranteed to lie on the geometry.

";
    array: r"

Returns
-------
GeometryArray[Point]
    One point inside or on each row's geometry.

";
    tail: r"
Raises
------
InvalidGeometryError
    If a finite representative point cannot be computed.

Examples
--------
>>> import gometry as gm
>>> square = gm.box(0, 0, 2, 2)
>>> gm.within(square.point_on_surface(), square)
True";
}

method_doc! {
    doc_polygonize;
    body: r"Build polygons from a geometry's own linework, automatically noding
crossings and overlaps. Each geometry is polygonized in isolation; to reconstruct
polygons from a pile of edges pooled across many geometries, use free function
``polygonize_all`` on an iterable of
values. Input ordinates are carried where possible; unsourceable noding seams
yield XY.";
    scalar: r"

Returns
-------
GeometryArray
    The polygons built from the input linework.

";
    array: r"

Returns
-------
Groups of GeometryArray
    One ragged group of polygons per input geometry, in input order; missing
    rows yield ``None`` groups and present empty rows yield empty groups. Each
    input's OWN linework is polygonized independently (row ``i`` is
    ``self[i].polygonize()``). To pool ALL rows' edges into one graph so a
    ring can close across inputs, use the free function ``polygonize_all``.

";
    tail: r"
See Also
--------
polygonize_all : Pool linework across all supplied geometries.

Raises
------
OverflowError
    If no adjacent binary64 assignment preserves the exact polygonized output.
ResourceLimitError
    If the bounded embedding search is exhausted.
InvalidGeometryError
    If polygons cannot be assembled from the linework.

Examples
--------
>>> import gometry as gm
>>> a, b = [(0, 0), (1, 0)], [(1, 0), (1, 1)]
>>> edges = gm.MultiLineString([a, b, [(1, 1), (0, 0)]])
>>> edges.polygonize().to_wkt()[0]
'POLYGON ((0 0, 1 0, 1 1, 0 0))'";
}

method_doc! {
    doc_polylabel;
pre: r"Pole of inaccessibility: an approximate most-distant interior point
for label placement. Uses a floating-point cell search with ``tolerance`` as
its absolute stopping target. For a certified planar global-inradius gap,
use ``maximum_inscribed_circle(...).center`` instead.
Geographic line and ring boundaries may be refined in the local projection
before the search; ``tolerance`` controls the search, not the separate
input-refinement budget.
For a nonempty polygon with no filled interior, returns the midpoint of its
stored bounds with radius zero, including when its rings cancel the entire
support.
Empty geometries return ``POINT EMPTY``.

See Also
--------
maximum_inscribed_circle : Certified circle solution with direct center access.
centroid : Area/length-weighted center (may fall outside).
point_on_surface : A representative point guaranteed on the geometry.

Parameters
----------";
    scalar: (r"
tolerance : float, optional
    Positive absolute stopping target for the approximate label search,
    interpreted for the CRS (see ``unit``).
    Omitted selects a scale-aware tolerance from the geometry's extent.
", r"

Returns
-------
Point
    The pole of inaccessibility.

");
    array: (r"
tolerance : float or sequence of float, optional
    Positive absolute stopping target for the approximate label search,
    interpreted for the CRS (see ``unit``) — a scalar applies to every geometry, or pass
    one value per geometry.
    Omitted selects a scale-aware tolerance independently for each geometry.
", r"

Returns
-------
GeometryArray[Point]
    One point per row.

");
    post: unit_parameter_doc!();
    tail: r"
Raises
------
GeometryError
    If an explicit ``tolerance`` is not positive and finite.
InvalidGeometryError
    If the pole of inaccessibility cannot be computed.

Examples
--------
>>> import gometry as gm
>>> gm.box(0, 0, 2, 2).polylabel().to_wkt()
'POINT (1 1)'";
}

macro_rules! doc_sample_points {
    (scalar) => {
        concat!(doc_sample_points!(@pre), r"
count : int
    Number of points to draw (``>= 0``).
seed : int, numpy.random.Generator, or None, default None
    An integer reproduces the sample stream. A Generator advances once to
    seed the native stream. ``None`` uses fresh entropy.
unit : {'meters', 'planar'} or None, default None
    ``None`` samples physical ellipsoid area or length on geographic input,
    and coordinate area or length otherwise. ``'planar'`` samples the native
    chart instead. ``'meters'`` requires a CRS and uses its metric measure.
", r"

Returns
-------
GeometryArray
    The ``count`` sampled points.

", doc_sample_points!(@tail))
    };
    (array) => {
        concat!(doc_sample_points!(@pre), r"
count : int or iterable of int
    Number of points to draw per row (``>= 0``). A scalar broadcasts;
    otherwise pass one count per row.
seed : int, iterable of int, numpy.random.Generator, or None, default None
    Seed for the deterministic sample stream. A scalar derives a distinct
    stream for every row; otherwise pass one explicit seed per row. A Generator
    advances once per call. ``None`` uses fresh entropy.
unit : {'meters', 'planar'} or None, default None
    ``None`` uses ellipsoidal area/length on geographic input and coordinate
    measures otherwise. ``'planar'`` uses chart measures. ``'meters'`` requires
    a CRS.
", r"

Returns
-------
Groups
    One ``GeometryArray`` of ``count`` sampled points per row.

", doc_sample_points!(@tail))
    };
    (@pre) => {
        r"Random points on the geometry. The sample space is its highest
dimension: area weighted for areal input, length
weighted for lineal input, and equal weight across point atoms. Geographic
lines follow each retained longitude branch, while geographic areas are split
into seam- and pole-aware chart parts before sampling. Geographic defaults are
uniform in ellipsoid surface area and arclength on the straight source-chart
edges, not in a local projection or along geodesic replacement edges.
``unit='planar'`` instead uses native chart measure; for example, a
degree-based 170° to -170° line follows its retained +20° path, contributing
20° rather than the raw 340° chord. Seam-crossing polygons use their seam-split
chart area, not the complementary raw-coordinate span. Each logical
member contributes separately, so overlapping members retain their sampling
multiplicity. A zero-area polygon falls back to its linework, and zero-length
linework falls back to vertices, like centroid. An integer ``seed`` reproduces
the same points for the same input and measure. Array rows draw distinct streams derived
from ``seed`` and the row index, and a scalar geometry IS row 0 — so
        ``arr.sample_points(n, seed=s)[0]`` and
        ``arr[0].sample_points(n, seed=s)`` agree. An empty geometry has nothing to
        sample and yields no points. Results retain the input CRS and coordinate
        epoch. Sampled points are new XY locations, so they have no source Z/M.

Parameters
----------"
    };
    (@tail) => {
        r"
Raises
------
GeometryError
    If ``count`` or ``seed`` is invalid.
ResourceLimitError
    If ``count`` exceeds the generated-output budget.

Examples
--------
>>> import gometry as gm
>>> square = gm.box(0, 0, 10, 10)
>>> pts = square.sample_points(5, seed=42)
>>> (len(pts), all(p is not None and gm.within(p, square) for p in pts))
(5, True)"
    };
}

method_doc! {
    doc_self_intersections;
    body: r"Return points where the geometry coincides with itself. Reports proper linework
self-crossings, non-adjacent touches, the endpoints of collinear overlaps
(spikes and backtracks), contact between distinct parts, and duplicate point
coordinates; legal adjacent shared vertices, ring closures, and removable
repeated consecutive vertices are not nodes. For point/lineal input the
result is non-empty exactly when is_simple is ``False``; areal input
diagnoses its rings' linework, and collections are diagnosed recursively.
Geographic diagnostics follow the CRS longitude quotient; projected and
CRS-free geometry remains planar. Points are XY only.";
    scalar: r"

Returns
-------
GeometryArray
    The distinct self-intersection points.

";
    array: r"

Returns
-------
Groups
    One ``GeometryArray`` of distinct self-intersection points per
    element; missing rows yield ``None`` groups.

";
    tail: r"Examples
--------
>>> import gometry as gm
>>> cross = gm.from_wkt('LINESTRING (0 0, 1 1, 1 0, 0 1)')
>>> cross.self_intersections().to_wkt()
['POINT (0.5 0.5)']";
}

method_doc! {
    doc_split_antimeridian;
    body: r"Split at the antimeridian. CRS-free input is treated as CRS84; a tagged
geographic CRS supplies its own longitude frame and ellipsoid. Parts that cross
come back as multiple parts
whose edges follow the seam — each side keeping its own seam sign — so the
result remains a coordinate-planar longitude/latitude chart for topology.
Inserted seam vertices keep the affine chart crossing in X/Y; eligible Z/M
follow ellipsoidal arclength along the retained source edge. An authored
full-period edge is retained with quarter witnesses before its longitudes are
canonicalized. The two seam aliases and a displacement of multiple periods do
not imply a full-period edge. When a seam split needs polar closure, it follows
the ring's enclosed pole; a pole-free band does not acquire a polar cap.
Geometries that do not cross are returned unchanged. A split
``LineString`` becomes a ``MultiLineString`` and a split ``Polygon`` a
``MultiPolygon``, like repair. On a geographic CRS, the split subdivides
existing chart edges, so ellipsoidal area and perimeter are conserved to
rounding.";
    scalar: r"

Returns
-------
Geometry
    The seam-split geometry.

";
    array: r"

Returns
-------
GeometryArray
    One seam-split geometry per row.

";
    tail: r"
Raises
------
CRSError
    If the CRS is projected (CRS-free lon/lat and geographic CRS are
    accepted), or a coordinate is outside the longitude/latitude domain.
InvalidGeometryError
    If stitching fails, or pole closure would have to invent Z/M
    ordinates.

Examples
--------
>>> import gometry as gm
>>> line = gm.LineString([(170, 0), (-170, 0)], crs=4326)
>>> split = line.split_antimeridian()
>>> (split.geometry_type, len(split.parts))
('MultiLineString', 2)";
}

method_doc! {
    doc_unique_points;
    body: r"Distinct vertices in first-occurrence order. Vertices compare by exact
structural identity (every active ordinate by bit pattern, the
``equals_identical`` notion), so XYZ points that differ only in Z stay
distinct.";
    scalar: r"

Returns
-------
MultiPoint or GeometryCollection
    The distinct vertices. Homogeneous surviving vertices use a ``MultiPoint``;
    when surviving vertices carry different axis layouts, a ``GeometryCollection``
    of ordered ``Point`` members preserves each layout without promotion or loss.
    An empty input returns a typed ``MULTIPOINT EMPTY``.

";
    array: r"

Returns
-------
GeometryArray
    One distinct-vertex result per row: a ``MultiPoint`` for homogeneous rows or
    a ``GeometryCollection`` of ordered ``Point`` members for heterogeneous rows.

";
    tail: r"Notes
-----
A heterogeneous result follows ordinary ``GeometryCollection`` semantics:
``is_simple`` is false by convention and structural equality remains type-sensitive.
Mixed-axis and collection exports use WKB for Arrow interchange; see
[Arrow & storage](../../ecosystem/arrow.md).

See Also
--------
remove_repeated_points : Collapse consecutive duplicate vertices in place, keeping the geometry kind.

Examples
--------
>>> import gometry as gm
>>> loop = gm.LineString([(0, 0), (1, 1), (0, 0), (2, 2)])
>>> loop.unique_points().to_wkt()
'MULTIPOINT ((0 0), (1 1), (2 2))'";
}

method_doc! {
    doc_voronoi;
    body: r"Construct a Voronoi diagram from the geometry's vertices. Geographic
sites and a supplied clip polygon use a CRS-native chart for seam crossings;
publication retains chart cuts and polar-cap edge roles without a preliminary
``split_antimeridian``. The construction is planar in longitude/latitude
coordinates, not a geodesic or spherical Voronoi diagram. Collinear vertices
divide the clip region into strips at their
perpendicular bisectors, and a lone vertex owns the whole clip polygon; the
padded and envelope frames of a lone vertex, or the envelope of a horizontal
or vertical row, bound no cells.

Parameters
----------
output : {'polygons', 'edges'}
    Return clipped Voronoi cells or the internal Voronoi edges. The polygon
    form omits ``output``; edge output requires ``output='edges'`` and excludes
    the clip polygon's exterior boundary.
tolerance : float, default 0.0
    Finite non-negative coordinate-space tolerance: native angular units for
    geographic CRSs, native linear units for projected CRSs, and raw coordinate
    units when CRS-free.
clip : {'padded', 'envelope'} or Polygon, default 'padded'
    How to bound the unbounded outer cells: a padded box, the input
    envelope, or a `Polygon` to clip the diagram to.
    Diagram vertices are synthesized and returned in XY. Rounded clip
    incidences need not lie on the source polygon under overlay membership.";
    scalar: r"

Returns
-------
GeometryArray[Polygon] or GeometryArray[LineString]
    The selected output for the scalar geometry's vertices.
";
    array: r"

Returns
-------
Groups[GeometryArray[Polygon]] or Groups[GeometryArray[LineString]]
    One ragged group of the selected output per input geometry, in input order.
";
    tail: r"
Raises
------
OverflowError
    If the output cannot be represented in binary64. ``operation`` is
    ``'voronoi_cells'`` for polygon output and ``'voronoi_edge_lines'`` for edges.
ResourceLimitError
    If generated output or the embedding search exceeds its budget.
InvalidGeometryError
    If the clip polygon is invalid or exact Voronoi construction fails.

Examples
--------
>>> import gometry as gm
>>> sites = gm.MultiPoint([(0, 0), (2, 0), (1, 2)])
>>> len(sites.voronoi())
3
>>> len(sites.voronoi(output='edges'))
3";
}

macro_rules! doc_geographic_triangulation {
    () => {
        r"Geographic Delaunay and constrained triangulation use a CRS-native chart
for seam-crossing input without a preliminary split. Delaunay triangulates point
sites in that coordinate chart; it is not a spherical mesh. Constrained
triangulation preserves polygon boundaries and the pole selected by a polar
cap. Earcut follows polygon XY rings; call ``split_antimeridian`` first when a
geographic polygon crosses the seam or encloses a pole.

"
    };
}

macro_rules! doc_triangulate {
    (scalar) => {
        concat!(
            r"Triangulate geometry with an explicit algorithm.

",
            doc_geographic_triangulation!(),
            r"Parameters
----------
method : {'earcut', 'delaunay', 'constrained'}
    Required algorithm choice: ``earcut`` triangulates polygon interiors,
    ``delaunay`` triangulates input vertices, and ``constrained`` preserves
    polygon boundaries. Delaunay methods resolve exact cocircular ties with
    a canonical rule keyed by lexicographic XY coordinates, independent of
    insertion order.
min_angle : float, optional
    Minimum triangle angle in degrees; positive finite values up to 30 are
    valid with ``method='constrained'``.

max_area : float, optional
    Positive finite maximum triangle area in coordinate space. Geographic
    CRSs use squared native angular units, projected CRSs use squared native
    linear units, and CRS-free inputs use squared raw coordinate units. Valid
    with ``method='constrained'``. Setting either option enables
    refinement;
    without either option, duplicate XY sites use their first source
    occurrence and triangle corners preserve the selected source ordinates.
    Refinement inserts Steiner vertices and therefore returns uniform XY.
    Refinement processes the worst triangle first: angle violations by
    circumradius-to-shortest-edge ratio, then by area when ``max_area`` is
    also set; triangles meeting the angle target follow in descending area.
    Equal priorities use sorted numeric XY corners. Refinement is independent
    of insertion order and input ring rotation or reversal.
    Each boundary Steiner vertex is the nearest binary64 coordinate pair to
    its exact position on the source edge, so the triangles cover the source
    polygon up to that rounding.

Returns
-------
GeometryArray[Polygon]
    The generated triangles. Delaunay methods return counter-clockwise
    triangles starting at their smallest XY corner, sorted by corner coordinates.

Raises
------
GeometryError
    If a quality constraint is invalid or method-specific options are used
    with the wrong algorithm.
OverflowError
    If a boundary Steiner point cannot be represented at float resolution;
    ``operation`` is ``'triangulate'``.
ResourceLimitError
    If refinement exceeds the generated-output budget.
InvalidGeometryError
    If the input polygon is invalid or the triangulation cannot be constructed.

Examples
--------
>>> import gometry as gm
>>> sites = gm.MultiPoint([(0, 0), (2, 0), (1, 2), (1, 0.5)])
>>> tris = sites.triangulate(method='delaunay')
>>> (len(tris), tris.geometry_type[0])
(3, 'Polygon')"
        )
    };
    (array) => {
        concat!(
            r"Triangulate each geometry with an explicit algorithm.

",
            doc_geographic_triangulation!(),
            r"Parameters
----------
method : {'earcut', 'delaunay', 'constrained'}
    Required triangulation algorithm. Delaunay methods resolve exact
    cocircular ties with a canonical rule keyed by lexicographic XY
    coordinates, independent of insertion order.
min_angle : float or sequence of float, optional
    Minimum triangle angle in degrees; each value must be positive, finite,
    and at most 30. Valid only with ``method='constrained'``.

max_area : float or sequence of float, optional
    Maximum triangle area in coordinate space; each value must be positive
    and finite. Geographic CRSs use squared native angular units, projected
    CRSs use squared native linear units, and CRS-free inputs use squared raw
    coordinate units. Valid only with ``method='constrained'``. Setting
    either option enables refinement;
    inserted Steiner vertices return uniform XY. Scalar and array rows use
    quality-first refinement: angle violations by circumradius-to-shortest-edge
    ratio, then area when both options are set; triangles meeting the angle
    target follow in descending area. Equal priorities use sorted numeric XY
    corners. Refinement is independent of insertion order and input ring
    rotation or reversal. Scalar and array rows use
    the same first-occurrence source-site and ordinate policy. Each boundary
    Steiner vertex is the nearest binary64 coordinate pair to its exact
    position on the source edge, so the triangles cover the source polygon
    up to that rounding.

Returns
-------
Groups of GeometryArray[Polygon]
    One ragged triangle group per input geometry. Delaunay methods return
    counter-clockwise triangles starting at their smallest XY corner,
    sorted by corner coordinates within each group.

Raises
------
GeometryError
    If a quality constraint is invalid or method-specific options are used
    with the wrong algorithm.
OverflowError
    If a boundary Steiner point cannot be represented at float resolution;
    ``operation`` is ``'triangulate'``.
ResourceLimitError
    If refinement exceeds the generated-output budget.
InvalidGeometryError
    If an input polygon is invalid or its triangulation cannot be constructed.

Examples
--------
>>> import gometry as gm
>>> groups = gm.GeometryArray([gm.box(0, 0, 2, 2)]).triangulate(method='earcut')
>>> groups[0].to_wkt()
['POLYGON ((0 2, 0 0, 2 0, 0 2))', 'POLYGON ((2 2, 0 2, 2 0, 2 2))']"
        )
    };
}

#[path = "unary_constructive_hulls.rs"]
mod unary_constructive_hulls;
#[path = "unary_constructive_mesh.rs"]
mod unary_constructive_mesh;
#[path = "unary_constructive_noding.rs"]
mod unary_constructive_noding;
