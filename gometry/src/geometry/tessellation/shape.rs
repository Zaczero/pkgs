use ahash::HashSetExt as _;

use crate::NonNegative;
use crate::error::Result;
use crate::geometry::derived::monotone_chain_hull;
use crate::geometry::tessellation::{
    CdtRefinement, Site, build_area_lines, certified_delaunay, collect_sample_triangles,
    collect_xy_chains, constrained_triangle_vertices, earcut_polygon_with, line_boundary,
    multiline_boundary, polygonize_full, polygonize_lines, rings_to_boundary, sample_weighted,
    snap_sites, triangle_shape, uniform_f64, voronoi_dcel, weight_scale,
};
use crate::geometry::{
    AxisFrame, CoordSeq, Dimension, EmptyKind, ExpansionBudget, GeometryErrorKind, HashSet, Point,
    PointKey, Polygon, PolygonizeFull, Ring, Segment, Shape, VoronoiBoundary, carry_each,
    empty_geometry, line_segments,
};

/// The ear-cut emitter owns the admission-before-allocation invariant: every
/// closed triangle ring is charged before its `Shape` is built.
pub(super) struct BudgetedTriangleSink<'budget> {
    budget: &'budget mut ExpansionBudget,
    shapes: Vec<Shape>,
}

// The cap must be admitted before this local output vector is reserved. Keep
// the ordering observable in a test build; it is compiled out of production.
#[cfg(test)]
thread_local! {
    static DELAUNAY_OUTPUT_RESERVES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn note_delaunay_output_reserve() {
    DELAUNAY_OUTPUT_RESERVES.with(|count| count.set(count.get() + 1));
}

#[cfg(test)]
pub(super) fn take_delaunay_output_reserves() -> usize {
    DELAUNAY_OUTPUT_RESERVES.with(|count| count.replace(0))
}

impl<'budget> BudgetedTriangleSink<'budget> {
    pub(super) const fn new(budget: &'budget mut ExpansionBudget) -> Self {
        Self {
            budget,
            shapes: Vec::new(),
        }
    }

    pub(super) fn emit(&mut self, a: Point, b: Point, c: Point) -> Result<()> {
        self.budget.add(4)?;
        self.shapes.push(triangle_shape(a, b, c));
        Ok(())
    }

    pub(super) fn into_shapes(self) -> Vec<Shape> {
        self.shapes
    }
}

impl Shape {
    pub fn delaunay_triangles(&self) -> Result<Vec<Self>> {
        let mut budget = crate::geometry::ExpansionBudget::new("triangulate", "method");
        self.delaunay_triangles_budgeted(&mut budget)
    }

    pub(crate) fn delaunay_triangles_budgeted(
        &self,
        budget: &mut crate::geometry::ExpansionBudget,
    ) -> Result<Vec<Self>> {
        let shapes = self
            .delaunay_triangle_vertices_budgeted(budget)?
            .as_chunks::<4>()
            .0
            .iter()
            .map(|ring| {
                let ring = ring.map(Point::to_xy);
                Self::Polygon(Polygon::new(
                    Ring::from_trusted_closed(CoordSeq::from_points(&ring)),
                    Vec::new(),
                ))
            })
            .collect::<Vec<_>>();
        // Triangle vertices are input vertices: Z/M always resolve.
        carry_each(shapes, &[self], "delaunay_triangles", false)
    }

    /// The Delaunay triangulation as one flat CLOSED-ring vertex stream — four
    /// points `[a, c, b, a]` per triangle, CCW (gometry's shell canon). Lets the
    /// array surface build a packed `Polygons` layout in ONE pass instead of a
    /// `Polygon`/`CoordSeq` per triangle that is then re-packed.
    pub(crate) fn delaunay_triangle_vertices(&self) -> Result<Vec<Point>> {
        let mut budget = crate::geometry::ExpansionBudget::new("triangulate", "method");
        self.delaunay_triangle_vertices_budgeted(&mut budget)
    }

    fn delaunay_points(&self) -> Vec<Point> {
        // XY-unique vertices keeping FULL ordinates (first-seen Z/M): triangle
        // corners are input vertices, so carrying their Z/M here preserves it
        // directly — no post-hoc XY re-lookup (the old `carry_each` step).
        let capacity = self.coord_count();
        let mut seen = HashSet::with_capacity(capacity);
        let mut points: Vec<Point> = Vec::with_capacity(capacity);
        self.for_each_point(|point| {
            if seen.insert(PointKey::new(point)) {
                points.push(point);
            }
        });
        if points.len() < 3 {
            return Vec::new();
        }
        points.sort_unstable_by(|left, right| {
            left.x
                .total_cmp(&right.x)
                .then_with(|| left.y.total_cmp(&right.y))
        });
        // Sweep-circle triangulation (delaunator: robust-predicate hull walk,
        // the mapbox algorithm) straight over the unique vertices -- no geo
        // conversion, no incremental-insertion engine.
        points
    }

    pub(crate) fn delaunay_triangle_vertices_budgeted(
        &self,
        budget: &mut crate::geometry::ExpansionBudget,
    ) -> Result<Vec<Point>> {
        let points = self.delaunay_points();
        if points.len() < 3 {
            return Ok(Vec::new());
        }
        // The triangulation's indexed faces are input-sized working state; the
        // closed-ring vertices below are the generated output. Charge their
        // exact count before reserving or emitting any of them.
        let sites: Vec<_> = points
            .iter()
            .copied()
            .enumerate()
            .map(|(id, point)| Site { id, point })
            .collect();
        let triangulation = certified_delaunay(&sites)?;
        let capacity = triangulation
            .triangles()
            .len()
            .checked_mul(4)
            .ok_or_else(|| GeometryErrorKind::triangulation("triangle output size overflow"))?;
        budget.add(capacity)?;
        let mut vertices = Vec::new();
        #[cfg(test)]
        note_delaunay_output_reserve();
        vertices.try_reserve_exact(capacity).map_err(|_| {
            GeometryErrorKind::triangulation(format!(
                "could not allocate {capacity} Delaunay-triangulation coordinates"
            ))
        })?;
        let mut triangles = triangulation.triangles().to_vec();
        triangles.sort_unstable_by_key(|triangle| {
            let mut key = *triangle;
            key.sort_unstable();
            key
        });
        for triangle in triangles {
            vertices.extend_from_slice(&[
                points[triangle[0]],
                points[triangle[1]],
                points[triangle[2]],
                points[triangle[0]],
            ]);
        }
        Ok(vertices)
    }

    pub(crate) fn constrained_delaunay_triangles(
        &self,
        refinement: CdtRefinement,
    ) -> Result<Vec<Self>> {
        let mut budget = crate::geometry::ExpansionBudget::new("triangulate", "min_angle/max_area");
        self.constrained_delaunay_triangles_budgeted(refinement, &mut budget)
    }

    pub(crate) fn constrained_delaunay_triangles_budgeted(
        &self,
        refinement: CdtRefinement,
        budget: &mut crate::geometry::ExpansionBudget,
    ) -> Result<Vec<Self>> {
        // The "is this face inside the polygon?" filter runs once per triangle
        // (~one per vertex), so build the hierarchical `PointBatchTester` ONCE
        // and reuse it: an uncached `contains_point` per face is an
        // O(faces x edges) brute sweep that dominated large inputs (8x slower
        // than GEOS at 800 verts).
        let vertices = self.constrained_delaunay_vertices_budgeted(refinement, budget)?;
        let shapes = vertices
            .as_chunks::<4>()
            .0
            .iter()
            .map(|ring| {
                let ring = ring.map(Point::to_xy);
                Self::Polygon(Polygon::new(
                    Ring::from_trusted_closed(CoordSeq::from_points(&ring)),
                    Vec::new(),
                ))
            })
            .collect();
        // Triangle vertices are input vertices when refinement is inactive;
        // resolve their Z/M only after the shared budget admitted the output.
        if refinement.active() {
            Ok(shapes)
        } else {
            carry_each(shapes, &[self], "triangulate", false)
        }
    }

    /// Constrained-Delaunay interior as a flat `[a, b, c, a]`-per-triangle
    /// vertex stream (XY) — the packed `Polygons` builder's input. Z/M callers
    /// use [`Self::constrained_delaunay_triangles`] (which resolves ordinates);
    /// this 2D form is exact for XY operands.
    pub(crate) fn constrained_delaunay_vertices(
        &self,
        refinement: CdtRefinement,
    ) -> Result<Vec<Point>> {
        let mut budget = crate::geometry::ExpansionBudget::new("triangulate", "min_angle/max_area");
        self.constrained_delaunay_vertices_budgeted(refinement, &mut budget)
    }

    pub(crate) fn constrained_delaunay_vertices_budgeted(
        &self,
        refinement: CdtRefinement,
        budget: &mut crate::geometry::ExpansionBudget,
    ) -> Result<Vec<Point>> {
        match self {
            Self::Polygon(polygon) => {
                constrained_triangle_vertices(polygon.rings(), self.area(), refinement, budget)
            },
            Self::MultiPolygon(polygons) => constrained_triangle_vertices(
                polygons.iter().flat_map(Polygon::rings),
                self.area(),
                refinement,
                budget,
            ),
            Self::Empty(EmptyKind::Polygon | EmptyKind::MultiPolygon, _) => Ok(Vec::new()),
            _ => Err(GeometryErrorKind::PolygonRequired.into()),
        }
    }

    pub fn polygon_triangles(&self) -> Result<Vec<Self>> {
        let mut budget = crate::geometry::ExpansionBudget::new("triangulate", "method");
        self.polygon_triangles_budgeted(&mut budget)
    }

    pub(crate) fn polygon_triangles_budgeted(
        &self,
        budget: &mut crate::geometry::ExpansionBudget,
    ) -> Result<Vec<Self>> {
        let mut sink = BudgetedTriangleSink::new(budget);
        match self {
            Self::Polygon(polygon) => {
                earcut_polygon_with(polygon, &mut |a, b, c| sink.emit(a, b, c))
            },
            Self::MultiPolygon(polygons) => {
                for polygon in polygons {
                    earcut_polygon_with(polygon, &mut |a, b, c| sink.emit(a, b, c))?;
                }
                Ok(())
            },
            Self::Empty(EmptyKind::Polygon | EmptyKind::MultiPolygon, _) => Ok(()),
            _ => Err(GeometryErrorKind::PolygonRequired.into()),
        }?;
        let shapes = carry_each(sink.into_shapes(), &[self], "polygon_triangles", false)?;
        Ok(shapes)
    }

    /// `count` planar-uniform random points on the geometry, deterministic
    /// from `seed` (a splitmix64 stream). The sample space is the
    /// geometry's highest-dimension support — uniform over area (areal
    /// input: triangulate, area-weighted pick, square-root barycentric
    /// warp), along length (lineal input: length-weighted segment pick), or
    /// across point atoms (equal weight) — with the centroid rule's
    /// degenerate fallback: zero-area input samples its linework, zero
    /// length its vertices. Output points are XY (new locations have no
    /// meaningful Z/M); CRS rides at the surface layer.
    pub fn sample_points(&self, count: usize, seed: u64) -> Result<Vec<Point>> {
        ExpansionBudget::check("sample_points", "count", count)?;
        if count == 0 {
            return Ok(Vec::new());
        }
        if self.topological_dimension() >= Dimension::Surface {
            let mut triangles: Vec<[Point; 3]> = Vec::new();
            collect_sample_triangles(self, &mut triangles)?;
            // Every triangle uses one shared *per-axis* frame. Relative areas
            // retain the common `sx*sy` factor, while reciprocal spans remain
            // nonzero instead of collapsing to the lower-dimensional fallback.
            let vertices: Vec<Point> = triangles.iter().flatten().copied().collect();
            let frame = AxisFrame::from_points(&vertices);
            let placed = sample_weighted(
                count,
                seed,
                &triangles,
                |[a, b, c]| {
                    let Some(frame) = frame else {
                        return 0.0;
                    };
                    let a = frame.frame_point(*a);
                    let b = frame.frame_point(*b);
                    let c = frame.frame_point(*c);
                    ((b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x)).abs()
                },
                |[base, mid, apex], state| {
                    // P = (1 - s) A + s (1 - t) B + s t C with s = sqrt(u):
                    // uniform over the triangle.
                    let spread = uniform_f64(state).sqrt();
                    let blend = uniform_f64(state);
                    let x =
                        (1.0 - spread) * base.x + spread * ((1.0 - blend) * mid.x + blend * apex.x);
                    let y =
                        (1.0 - spread) * base.y + spread * ((1.0 - blend) * mid.y + blend * apex.y);
                    Point::new(x, y)
                },
            )?;
            if let Some(points) = placed {
                return Ok(points);
            }
        }
        if self.topological_dimension() >= Dimension::Curve {
            let mut segments: Vec<Segment> = Vec::new();
            self.for_each_segment_chain(|chain| segments.extend(line_segments(chain)));
            let scale = weight_scale(
                segments
                    .iter()
                    .flat_map(|segment| [segment.start, segment.end])
                    .flat_map(|point| [point.x.abs(), point.y.abs()])
                    .fold(0.0_f64, f64::max),
            );
            let placed = sample_weighted(
                count,
                seed,
                &segments,
                |segment| {
                    let dx = (segment.end.x - segment.start.x) * scale;
                    let dy = (segment.end.y - segment.start.y) * scale;
                    (dx * dx + dy * dy).sqrt()
                },
                |segment, state| {
                    let along = uniform_f64(state);
                    let x = (1.0 - along) * segment.start.x + along * segment.end.x;
                    let y = (1.0 - along) * segment.start.y + along * segment.end.y;
                    Point::new(x, y)
                },
            )?;
            if let Some(points) = placed {
                return Ok(points);
            }
        }
        let mut atoms: Vec<Point> = Vec::new();
        self.for_each_point(|point| atoms.push(point));
        sample_weighted(
            count,
            seed,
            &atoms,
            |_| 1.0,
            |atom, _| Point::new(atom.x, atom.y),
        )?
        .ok_or_else(|| GeometryErrorKind::EmptySampleSource.into())
    }

    pub fn voronoi_polygons(
        &self,
        tolerance: f64,
        boundary: VoronoiBoundary<'_>,
    ) -> Result<Vec<Self>> {
        let mut budget = ExpansionBudget::new("voronoi_polygons", "sites/clip topology");
        self.voronoi_polygons_budgeted(tolerance, boundary, &mut budget)
    }

    pub(crate) fn voronoi_polygons_budgeted(
        &self,
        tolerance: f64,
        boundary: VoronoiBoundary<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<Vec<Self>> {
        let tolerance = NonNegative::try_new("tolerance", tolerance)?.get();
        let canonical = canonical_voronoi_sites(self.unique_xy_points(), tolerance);
        if canonical.len() < 2 {
            return Ok(Vec::new());
        }
        let points: Vec<_> = canonical.iter().map(|site| site.point).collect();
        if monotone_chain_hull(&points).len() < 3 {
            return Err(GeometryErrorKind::voronoi(
                "input points are collinear; Voronoi cells cannot be computed",
            ));
        }
        Ok(voronoi_dcel::build(&canonical, boundary, budget, true)?
            .into_polygons()
            .into_iter()
            .map(Self::Polygon)
            .collect())
    }

    pub fn voronoi_edges(
        &self,
        tolerance: f64,
        boundary: VoronoiBoundary<'_>,
    ) -> Result<Vec<Self>> {
        let mut budget = ExpansionBudget::new("voronoi_edges", "sites/clip topology");
        self.voronoi_edges_budgeted(tolerance, boundary, &mut budget)
    }

    pub(crate) fn voronoi_edges_budgeted(
        &self,
        tolerance: f64,
        boundary: VoronoiBoundary<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<Vec<Self>> {
        let tolerance = NonNegative::try_new("tolerance", tolerance)?.get();
        let canonical = canonical_voronoi_sites(self.unique_xy_points(), tolerance);
        if canonical.len() < 2 {
            return Ok(Vec::new());
        }
        let points: Vec<_> = canonical.iter().map(|site| site.point).collect();
        if monotone_chain_hull(&points).len() >= 3 {
            return Ok(voronoi_dcel::build(&canonical, boundary, budget, false)?
                .into_edges()
                .into_iter()
                .map(Self::LineString)
                .collect());
        }
        Ok(
            voronoi_dcel::build_collinear_edges(&canonical, boundary, budget)?
                .into_iter()
                .map(Self::LineString)
                .collect(),
        )
    }

    pub fn boundary(&self) -> Self {
        match self {
            // Points have no boundary; a typed `POLYGON EMPTY` still boundaries
            // to linework (`MULTILINESTRING EMPTY`), per the polygon contract.
            Self::Point(_) | Self::MultiPoint(_) => empty_geometry(),
            // Dimensional empties keep their declared axes on the boundary
            // sentinel, matching `exterior` (a Z empty polygon's boundary is
            // MULTILINESTRING Z EMPTY, not an XY collapse).
            Self::Empty(EmptyKind::Point | EmptyKind::GeometryCollection, axes) => {
                Self::typed_empty(EmptyKind::GeometryCollection, *axes)
            },
            Self::Empty(EmptyKind::Polygon | EmptyKind::MultiPolygon, axes) => {
                Self::typed_empty(EmptyKind::MultiLineString, *axes)
            },
            Self::Empty(EmptyKind::MultiLineString, axes) => {
                Self::MultiPoint(CoordSeq::empty(*axes))
            },
            Self::LineString(points) => line_boundary(points),
            Self::MultiLineString(lines) => multiline_boundary(lines),
            // A polygon's boundary IS its rings as linework: clone the ring
            // coordinate columns straight into the result (one `LineString`, or
            // a `MultiLineString` when there are holes / parts) — no per-vertex
            // `Point` round-trip through `to_vec` + column rebuild.
            Self::Polygon(polygon) => rings_to_boundary(polygon.rings().cloned().collect()),
            Self::MultiPolygon(polygons) => {
                rings_to_boundary(polygons.iter().flat_map(Polygon::rings).cloned().collect())
            },
            Self::GeometryCollection(geometries) => {
                let boundaries = geometries
                    .iter()
                    .map(Self::boundary)
                    .filter(|boundary| !boundary.is_empty())
                    .collect();
                Self::GeometryCollection(boundaries)
            },
        }
    }

    pub fn polygonize(&self, strict: bool) -> Result<Vec<Self>> {
        Self::polygonize_all(&[self], strict)
    }

    pub fn polygonize_full(&self, strict: bool) -> Result<PolygonizeFull> {
        Self::polygonize_full_all(&[self], strict)
    }

    /// Polygonize many shapes as one noding universe: every line part from
    /// every input participates in a single assembly (the array form — rings
    /// that close across elements still close). Output rings reuse input
    /// vertices, so Z/M is carried by position; `strict` (the `'strict'`
    /// while the default raises (the `'error'` policy).
    pub fn polygonize_all(shapes: &[&Self], strict: bool) -> Result<Vec<Self>> {
        let lines = collect_xy_chains(shapes);
        let polygons = polygonize_lines(&lines)
            .into_iter()
            .map(Self::Polygon)
            .collect();
        carry_each(polygons, shapes, "polygonize", strict)
    }

    /// Assemble noded linework into areal geometry with GEOS `BuildArea`
    /// semantics: nested rings alternate solid/hole (even-odd fill), so a ring
    /// enclosing a hole becomes ONE holed polygon — unlike [`polygonize_all`],
    /// which emits every minimal face. This is the right model for dissolving a
    /// cell-grid coverage into its outline (a coverage hole is a hole, not a
    /// separate region).
    ///
    /// [`polygonize_all`]: Self::polygonize_all
    pub fn build_area_all(shapes: &[&Self], strict: bool) -> Result<Vec<Self>> {
        let lines = collect_xy_chains(shapes);
        let polygons = build_area_lines(&lines)
            .into_iter()
            .map(Self::Polygon)
            .collect();
        carry_each(polygons, shapes, "build_area", strict)
    }

    /// Assemble this geometry's linework into one areal geometry (GEOS
    /// `BuildArea`): nested rings alternate solid/hole by even-odd fill, so a
    /// ring enclosing a hole becomes one holed polygon. Returns `POLYGON EMPTY`
    /// when no ring closes, a `Polygon` for one face, or a `MultiPolygon`.
    pub fn build_area(&self, strict: bool) -> Result<Self> {
        let polygons: Vec<Polygon> = Self::build_area_all(&[self], strict)?
            .into_iter()
            .map(|shape| match shape {
                Self::Polygon(polygon) => polygon,
                _ => unreachable!("build_area_all yields only polygons"),
            })
            .collect();
        Ok(match polygons.len() {
            0 => Self::empty_polygon(),
            1 => Self::Polygon(polygons.into_iter().next().expect("one polygon")),
            _ => Self::MultiPolygon(polygons),
        })
    }

    /// [`Shape::polygonize_all`] with diagnostics: one combined universe for
    /// polygons, cut edges, dangles, and invalid rings alike.
    pub fn polygonize_full_all(shapes: &[&Self], strict: bool) -> Result<PolygonizeFull> {
        let lines = collect_xy_chains(shapes);
        let full = polygonize_full(&lines);
        Ok(PolygonizeFull {
            polygons: carry_each(full.polygons, shapes, "polygonize_full", strict)?,
            cuts: carry_each(full.cuts, shapes, "polygonize_full", strict)?,
            dangles: carry_each(full.dangles, shapes, "polygonize_full", strict)?,
            invalid_rings: carry_each(full.invalid_rings, shapes, "polygonize_full", strict)?,
        })
    }
}

/// Snap once at the public boundary, then remove coincident representatives.
/// The downstream tessellators always receive a true site set, never the
/// duplicate aliases that snapping intentionally creates.
pub(super) fn canonical_voronoi_sites(mut points: Vec<Point>, tolerance: f64) -> Vec<Site> {
    for point in &mut points {
        point.x += 0.0;
        point.y += 0.0;
    }
    points.sort_unstable_by(|left, right| {
        left.x
            .total_cmp(&right.x)
            .then_with(|| left.y.total_cmp(&right.y))
    });
    points.dedup_by(|left, right| PointKey::new(*left) == PointKey::new(*right));
    let sites: Vec<_> = points
        .into_iter()
        .enumerate()
        .map(|(id, point)| Site { id, point })
        .collect();
    if tolerance == 0.0 {
        return sites;
    }
    let mut snapped = snap_sites(&sites, tolerance);
    let mut seen = HashSet::with_capacity(snapped.len());
    snapped.retain(|site| seen.insert(PointKey::new(site.point)));
    for (id, site) in snapped.iter_mut().enumerate() {
        site.id = id;
    }
    snapped
}
