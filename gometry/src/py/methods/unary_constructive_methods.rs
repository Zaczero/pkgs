use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::{DefaultedF64Input, DistanceUnit, F64Param, PyGeometry, PyGeometryArray};

// Numpydoc text lives in `doc_<op>!()` string macros; methods attach it via `#[doc = ...]`.

method_doc! {
    doc_boundary;
    body: r"Return the topological boundary of the geometry.";
    scalar: r"

Returns
-------
MultiPoint, LineString, MultiLineString, or GeometryCollection
    The topological boundary, one dimension below the input.

";
    array: r"

Returns
-------
GeometryArray
    One boundary per row.

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
members of a GeometryCollection rather than being promoted or flattened.";
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
    One areal geometry per row.

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
point_on_surface : A guaranteed-interior representative point.
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
    Higher values are looser: fewer edges are peeled and area grows toward
    the convex hull. ``0`` disables the distance guard.
length_threshold : float, default 0.0
    Boundary edges at or below this length are kept, so higher values also
    make the hull looser; interpreted for the CRS (see ``unit``). On a
    geographic CRS the threshold is evaluated in a local projection, while the
    output vertices are emitted from the original input coordinates.
", r"

Returns
-------
Point, LineString, or Polygon
    The concave hull; degenerate inputs reduce dimension.

");
    array: (r"
concavity : float or sequence of float, default 2.0
    Higher values are looser: fewer edges are peeled and area grows toward
    the convex hull. ``0`` disables the distance guard — a scalar applies
    to every geometry, or pass one value per geometry.
length_threshold : float or sequence of float, default 0.0
    Boundary edges at or below this length are kept, so higher values also
    make the hull looser; interpreted for the CRS (see ``unit``). On a
    geographic CRS the threshold is evaluated in a local projection, while the
    output vertices are emitted from the original input coordinates — a scalar
    applies to every geometry, or pass one value per geometry.
", r"

Returns
-------
GeometryArray
    One concave hull per row; degenerate inputs reduce dimension.

");
    post: r"unit : {'planar', 'meters'}, default None
    Omitted follows the CRS: geodesic meters on a geographic CRS, native units on a projected one, coordinate units without a CRS. ``planar``
    forces raw coordinate units (native angular units as Cartesian coordinates on a geographic CRS
    — only for deliberate coordinate-space math); ``meters`` forces the
    CRS metric and raises without a CRS.";
    tail: r"
Raises
------
GeometryError
    If parameters are non-finite or selected hull vertices have different
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
``split_antimeridian`` first. Hull vertices are copies of input vertices, so Z/M
ordinates are preserved. When several input vertices share an XY position, the
first in storage order is the survivor.";
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
Raises
------
InvalidGeometryError
    If the geometry is empty.

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
    pre: r"Largest circle inscribed in a polygonal geometry, as a contained filled disk.
Planar/projected input uses one deterministic representable center and
certifies the global inradius against the requested absolute tolerance.
Geographic input retains the documented local-projection approximation.
Mirrors minimum_bounding_circle; the radius alone is
maximum_inscribed_radius.

Parameters
----------";
    scalar: (r"
tolerance : float, optional
    Positive absolute bound on the planar global-inradius gap. Omitted
    selects the existing scale-aware tolerance from the geometry's extent.
", r"

Returns
-------
Point or Polygon
    The fixed-resolution filled polygonal approximation of the inscribed
    circle. A degenerate (zero-area) polygon, or a certified subnormal-radius
    result, returns its center `Point`.

");
    array: (r"
tolerance : float or sequence of float, optional
    Positive absolute bound on the planar global-inradius gap — a scalar
    applies to every geometry, or pass one value per geometry. Omitted
    selects a scale-aware tolerance independently for each geometry.
", r"

Returns
-------
GeometryArray
    The filled inscribed circle `Polygon` (or center `Point` when
    degenerate) per row.

");
    post: r"unit : {'planar', 'meters'}, default None
    Omitted follows the CRS: geodesic meters on a geographic CRS, native
    units on a projected one, coordinate units without a CRS. ``planar`` forces
    raw coordinate units; ``meters`` forces the CRS metric and raises without a CRS.";
    tail: r"See Also
--------
 polylabel : Approximate pole-of-inaccessibility label point.
maximum_inscribed_radius : The radius alone.
point_on_surface : A guaranteed-interior representative point.
centroid : Area/length-weighted center (may fall outside).

Raises
------
GeometryError
    If a finite planar/projected result cannot be represented as a contained
    polygon without changing its center or radius.
InvalidGeometryError
    If invalid polygonal structure prevents the operation from completing.

Examples
--------
>>> import gometry as gm
>>> disk = gm.box(0, 0, 2, 2).maximum_inscribed_circle()
>>> (disk.geometry_type, round(disk.area, 2))
('Polygon', 3.14)";
}

method_doc! {
    doc_maximum_inscribed_radius;
    pre: r"Radius of the largest inscribed circle. For planar/projected input,
one deterministic representable center is retained and the returned
downward-rounded boundary clearance of that underlying circle certifies
the global inradius against the requested absolute tolerance. Geographic
input uses the documented local-projection approximation. The numeric twin
of minimum_bounding_radius; the polygonal approximation is
maximum_inscribed_circle.

Parameters
----------";
    scalar: (r"
tolerance : float, optional
    Positive absolute bound on the planar global-inradius gap. Omitted
    selects the existing scale-aware tolerance from the geometry's extent.
", r"

Returns
-------
float
    The inscribed radius in the requested/CRS metric units (see ``unit``); ``NaN`` for empty input.

");
    array: (r"
tolerance : float or sequence of float, optional
    Positive absolute bound on the planar global-inradius gap — a scalar
    applies to every geometry, or pass one value per geometry. Omitted
    selects a scale-aware tolerance independently for each geometry.
", r"

Returns
-------
numpy.ndarray of float
    The inscribed radius per row; ``NaN`` where empty.

");
    post: r"unit : {'planar', 'meters'}, default None
    Omitted follows the CRS: geodesic meters on a geographic CRS, native
    units on a projected one, coordinate units without a CRS. ``planar`` forces
    raw coordinate units; ``meters`` forces the CRS metric and raises without a CRS.";
    tail: r"
See Also
--------
maximum_inscribed_circle : The filled inscribed circle (center and radius).

Examples
--------
>>> import gometry as gm
>>> gm.box(0, 0, 2, 2).maximum_inscribed_radius()
1.0";
}

method_doc! {
    doc_minimum_bounding_circle;
    body: r"Smallest circle enclosing the geometry, as a polygon. The standard shape:
for planar/projected input, ``minimum_bounding_radius`` is the correctly rounded
mathematical minimum radius and this method realizes that disk as a certified
circumscribed 64-gon. Every edge encloses the disk and every vertex lies outside
it. A single distinct vertex returns itself; empty input returns
``POLYGON EMPTY``. CRS-aware via local projection (approximate) on a
geographic CRS; projected CRS distances default to the CRS's native unit
and scale through the CRS linear unit only with ``unit='meters'``.

Parameters
----------
unit : {'planar', 'meters'}, default None
    Omitted follows the CRS: geodesic meters on a geographic CRS, native
    units on a projected one, coordinate units without a CRS. ``planar`` forces
    raw coordinate units; ``meters`` forces the CRS metric and raises without a CRS.";
    scalar: r"

Returns
-------
Point or Polygon
    The smallest enclosing circle, or the source point for a zero-radius result.

";
    array: r"

Returns
-------
GeometryArray
    The smallest enclosing circle per row.

";
    tail: r"
Raises
------
GeometryError
    If the correctly rounded circle cannot be emitted as a finite, valid,
    source-enclosing polygon without changing its mathematical center or scalar
    radius, or if the existing realization work budget is exhausted while
    finding representable vertices.
InvalidGeometryError
    If the bounding circle cannot be computed.

Examples
--------
>>> import gometry as gm
>>> pts = gm.MultiPoint([(0, 0), (4, 0)])
>>> (pts.minimum_bounding_circle().geometry_type, pts.minimum_bounding_radius())
('Polygon', 2.0)";
}

method_doc! {
    doc_minimum_bounding_radius;
    body: r"Radius of the smallest circle enclosing the geometry. For planar/projected
input it is rounded from the exact source minimum; this is the numeric twin of
minimum_bounding_circle, and both use the same exact support kernel. Empty
input yields ``NaN``; a single distinct vertex yields ``0``. Geographic point
sets with three or more distinct vertices use the planar center and
ellipsoidal support measurement (an approximation); two-point geographic
inputs are the exact geodesic half-distance. For a projected
``unit='meters'`` result, the exact squared radius is scaled before the one
binary64 rounding.

Parameters
----------
unit : {'planar', 'meters'}, default None
    Omitted follows the CRS: geodesic meters on a geographic CRS, native
    units on a projected one, coordinate units without a CRS. ``planar`` forces
    raw coordinate units; ``meters`` forces the CRS metric and raises without a CRS.";
    scalar: r"

Returns
-------
float
    The enclosing circle radius.

";
    array: r"

Returns
-------
numpy.ndarray
    One enclosing circle radius per row.

";
    tail: r"Examples
--------
>>> import gometry as gm
>>> gm.MultiPoint([(0, 0), (4, 0)]).minimum_bounding_radius()
2.0";
}

method_doc! {
    doc_minimum_clearance;
    body: r"Smallest distance by which a vertex could move to invalidate the geometry.
On a geographic CRS, the witness is selected in the geometry's best local
projection and then measured geodesically in source coordinates; this is a
local-projection approximation, not an exact ellipsoidal clearance search.
Projected CRS and CRS-free geometries measure in the active planar units.

Parameters
----------
unit : {'planar', 'meters'}, default None
    Omitted follows the CRS: geodesic meters on a geographic CRS, native units on a projected one, coordinate units without a CRS. ``planar``
    forces raw coordinate units (native angular units as Cartesian coordinates on a geographic CRS
    — only for deliberate coordinate-space math); ``meters`` forces the
    CRS metric and raises without a CRS.";
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
    body: r"Two-point line realizing `minimum_clearance`. The metric matches
minimum_clearance: on a geographic CRS, the witness is selected in the
geometry's best local projection and returned in source coordinates, so it is a
local-projection approximation rather than an exact ellipsoidal clearance
search.
``LINESTRING EMPTY`` when the clearance is infinite (fewer than two distinct
vertices).

Parameters
----------
unit : {'planar', 'meters'}, default None
    Omitted follows the CRS: geodesic meters on a geographic CRS, native units on a projected one, coordinate units without a CRS. ``planar``
    forces raw coordinate units (native angular units as Cartesian coordinates on a geographic CRS
    — only for deliberate coordinate-space math); ``meters`` forces the
    CRS metric and raises without a CRS.";
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
    body: r"Minimum-area rotated bounding rectangle, returned in XY.";
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
``split_antimeridian`` is required. Always inside (or on) the geometry,
unlike centroid. The representative point is computed in XY and does not
imply a source Z/M.

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
    One interior point per row.

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
``polygonize`` on an iterable of
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
    One ragged group of polygons per input geometry, in input order — each
    input's OWN linework is polygonized independently (row ``i`` is
    ``self[i].polygonize()``). To pool ALL rows' edges into one graph so a
    ring can close across inputs, use the free function ``polygonize``.

";
    tail: r"
Raises
------
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
for label placement. Its CRS metric matches maximum_inscribed_circle, but the
certified planar circle search may retain a different representable center.

See Also
--------
maximum_inscribed_circle : Certified filled disk from its own center search.
centroid : Area/length-weighted center (may fall outside).
point_on_surface : A guaranteed-interior representative point.

Parameters
----------";
    scalar: (r"
tolerance : float, optional
    Precision of the search, interpreted for the CRS (see ``unit``).
    Omitted selects a scale-aware tolerance from the geometry's extent.
", r"

Returns
-------
Point
    The pole of inaccessibility.

");
    array: (r"
tolerance : float or sequence of float, optional
    Precision of the search, interpreted for the CRS (see ``unit``) — a
    scalar applies to every geometry, or pass one value per geometry. Omitted
    selects a scale-aware tolerance independently for each geometry.
", r"

Returns
-------
GeometryArray[Point]
    One point per row.

");
    post: r"unit : {'planar', 'meters'}, default None
    Omitted follows the CRS: geodesic meters on a geographic CRS, native units on a projected one, coordinate units without a CRS. ``planar``
    forces raw coordinate units (native angular units as Cartesian coordinates on a geographic CRS
    — only for deliberate coordinate-space math); ``meters`` forces the
    CRS metric and raises without a CRS.";
    tail: r"
Raises
------
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
seed : int
    Seed for the deterministic sample stream.
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
seed : int or iterable of int
    Seed for the deterministic sample stream. A scalar derives a distinct
    stream for every row; otherwise pass one explicit seed per row.
", r"

Returns
-------
Groups
    One ``GeometryArray`` of ``count`` sampled points per row.

", doc_sample_points!(@tail))
    };
    (@pre) => {
        r"Random points on the geometry. The sample space is the geometry's highest
dimension: uniform over area for areal input, along length for lineal input,
and across the member points of a point set — falling back a dimension when
the higher one is degenerate (a zero-area polygon samples its boundary),
like centroid. Deterministic: the same input and ``seed`` always produce
the same points (an explicit seed is required — no hidden global RNG). Array
rows draw distinct deterministic streams derived from ``seed`` and the row
index, and a scalar geometry IS row 0 — so ``arr.sample_points(n, seed=s)[0]``
and ``arr[0].sample_points(n, seed=s)`` agree. An empty row yields an empty
group rather than failing the batch; an empty SCALAR raises. Sampled points are invented interior points, so they cannot carry the
source geometry's Z/M and are returned in XY.

Parameters
----------"
    };
    (@tail) => {
        r"
Raises
------
InvalidGeometryError
    If ``count > 0`` and a geometry is empty.
GeometryError
    If ``count`` or ``seed`` is invalid.

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
Geographic antimeridian crossings use normalized topology; projected and
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
    element; missing rows yield an empty group.

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
    body: r"Split at the antimeridian. Parts that cross come back as multiple parts
whose edges follow the seam — each side keeping its own seam sign — so the
result remains a coordinate-planar longitude/latitude chart for topology.
Crossings are affine subdivisions of unwrapped straight chart edges; a ring
running off the seam closes over its pole automatically. Geometries that do
not cross are returned unchanged. A split ``LineString`` becomes a
``MultiLineString`` and a split ``Polygon`` a ``MultiPolygon``, like repair.
Seam vertices interpolate Z/M at the same chart fraction. On a geographic CRS,
geodesic metrics still interpret each stored edge from its endpoints on the
CRS ellipsoid, so this operation does not promise geodesic area or length
conservation.";
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
    doc_voronoi_edges;
    body: r"Voronoi diagram edges of the geometry's vertices. Operates in planar lon/lat
space and does NOT auto-split antimeridian-crossing geographic input; call
``split_antimeridian`` first.

Parameters
----------
tolerance : float, default 0.0
    Tolerance in coordinate units (non-negative).
clip : {'padded', 'envelope'} or Polygon, default 'padded'
    How to bound the unbounded outer cells: a padded box, the input
    envelope, or a `Polygon` to clip the diagram to.
    Diagram vertices are synthesized and returned in XY. Rounded clip
    incidences need not lie on the source polygon under overlay membership.";
    scalar: r"

Returns
-------
GeometryArray
    The Voronoi edges of the input geometry.

";
    array: r"

Returns
-------
Groups of GeometryArray
    One ragged group of Voronoi edges per input geometry, in input order.

";
    tail: r"
Raises
------
InvalidGeometryError
    If the Voronoi diagram cannot be constructed.

Examples
--------
>>> import gometry as gm
>>> sites = gm.MultiPoint([(0, 0), (2, 0), (1, 2)])
>>> len(sites.voronoi_edges())
3";
}

method_doc! {
    doc_voronoi_polygons;
    body: r"Voronoi diagram polygons of the geometry's vertices. Operates in planar
lon/lat space and does NOT auto-split antimeridian-crossing geographic
input; call ``split_antimeridian`` first.

Parameters
----------
tolerance : float, default 0.0
    Tolerance in coordinate units (non-negative).
clip : {'padded', 'envelope'} or Polygon, default 'padded'
    How to bound the unbounded outer cells: a padded box, the input
    envelope, or a `Polygon` to clip the diagram to.
    Diagram vertices are synthesized and returned in XY. Rounded clip
    incidences need not lie on the source polygon under overlay membership.";
    scalar: r"

Returns
-------
GeometryArray
    The Voronoi cells of the input geometry.

";
    array: r"

Returns
-------
Groups of GeometryArray
    One ragged group of Voronoi cells per input geometry, in input order.

";
    tail: r"
Raises
------
InvalidGeometryError
    If the Voronoi diagram cannot be constructed.

Examples
--------
>>> import gometry as gm
>>> sites = gm.MultiPoint([(0, 0), (2, 0), (1, 2)])
>>> len(sites.voronoi_polygons())
3";
}

macro_rules! doc_triangulate {
    (scalar) => {
        r"Triangulate geometry with an explicit algorithm.

Parameters
----------
method : {'earcut', 'delaunay', 'constrained'}
    Required algorithm choice: ``earcut`` triangulates polygon interiors,
    ``delaunay`` triangulates input vertices, and ``constrained`` preserves
    polygon boundaries.
min_angle : float, optional
    Minimum triangle angle in degrees; valid only with
    ``method='constrained'``.

max_area : float, optional
    Maximum triangle area in square coordinate units; valid only with
    ``method='constrained'``. Setting either option enables refinement;
    without either option, duplicate XY sites use their first source
    occurrence and triangle corners preserve the selected source ordinates.
    Refinement inserts Steiner vertices and therefore returns uniform XY.

Returns
-------
GeometryArray[Polygon]
    The generated triangles.

Raises
------
GeometryError
    If method-specific options are used with the wrong algorithm.
InvalidGeometryError
    If the triangulation cannot be constructed.

Examples
--------
>>> import gometry as gm
>>> sites = gm.MultiPoint([(0, 0), (2, 0), (1, 2), (1, 0.5)])
>>> tris = sites.triangulate(method='delaunay')
>>> (len(tris), tris.geometry_type[0])
(3, 'Polygon')"
    };
    (array) => {
        r"Triangulate each geometry with an explicit algorithm.

Parameters
----------
method : {'earcut', 'delaunay', 'constrained'}
    Required triangulation algorithm.
min_angle : float or sequence of float, optional
    Minimum triangle angle in degrees; valid only with
    ``method='constrained'``.

max_area : float or sequence of float, optional
    Maximum triangle area in square coordinate units; valid only with
    ``method='constrained'``. Setting either option enables refinement;
    inserted Steiner vertices return uniform XY. Scalar and array rows use
    the same first-occurrence source-site and ordinate policy.

Returns
-------
Groups of GeometryArray[Polygon]
    One ragged triangle group per input geometry.

Raises
------
GeometryError
    If method-specific options are used with the wrong algorithm.

Examples
--------
>>> import gometry as gm
>>> groups = gm.GeometryArray([gm.box(0, 0, 2, 2)]).triangulate(method='earcut')
>>> groups[0].to_wkt()
['POLYGON ((0 2, 0 0, 2 0, 0 2))', 'POLYGON ((2 0, 2 2, 0 2, 2 0))']"
    };
}

#[path = "unary_constructive_hulls.rs"]
mod unary_constructive_hulls;
#[path = "unary_constructive_mesh.rs"]
mod unary_constructive_mesh;
#[path = "unary_constructive_noding.rs"]
mod unary_constructive_noding;
