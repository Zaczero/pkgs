use crate::NonNegative;
use crate::error::Result;
use crate::geometry::overlay::ExactSourceInventory;
use crate::geometry::tessellation::{
    CdtRefinement, CertifiedDelaunay, Site, certified_delaunay, collect_sample_triangles,
    constrained_triangle_vertices, earcut_polygon_with, line_boundary, multiline_boundary,
    rings_to_boundary, sample_weighted, selected_source_sites, selected_triangle_points,
    snap_sites, try_triangle_shape, uniform_f64, voronoi_dcel, weight_scale,
};
use crate::geometry::{
    AxisFrame, CoordSeq, CoordinateAxes, CsrOffsetColumn, Dimension, ExpansionBudget,
    GeometryErrorKind, GeometryKind, Point, PointKey, Polygon, Polygonal, PolygonizeFull, Ring,
    Segment, Shape, VoronoiBoundary, axis_extent, carry_each, empty_geometry, line_segments,
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
        self.budget.check_additional(4)?;
        let shape = try_triangle_shape(a, b, c)?;
        self.shapes.push(shape);
        self.budget.add(4).map(|_| ())
    }

    pub(super) fn into_shapes(self) -> Vec<Shape> {
        self.shapes
    }
}

/// Packed `[a, b, c, a]` emitter: charge four vertices, then grow the stream.
pub(super) struct BudgetedPackedTriangleSink<'budget> {
    budget: &'budget mut ExpansionBudget,
    vertices: Vec<Point>,
}

impl<'budget> BudgetedPackedTriangleSink<'budget> {
    pub(super) const fn new(budget: &'budget mut ExpansionBudget) -> Self {
        Self {
            budget,
            vertices: Vec::new(),
        }
    }

    pub(super) fn emit(&mut self, a: Point, b: Point, c: Point) -> Result<()> {
        self.budget.check_additional(4)?;
        self.vertices.reserve(4);
        self.vertices.extend_from_slice(&[a, b, c, a]);
        self.budget.add(4).map(|_| ())
    }

    pub(super) fn into_vertices(self) -> Vec<Point> {
        self.vertices
    }
}

impl Shape {
    #[cfg(test)]
    pub(crate) fn delaunay_triangles(&self) -> Result<Vec<Self>> {
        let mut budget = crate::geometry::ExpansionBudget::new("triangulate", "method");
        self.delaunay_triangles_budgeted(&mut budget)
    }

    pub(crate) fn delaunay_triangles_budgeted(
        &self,
        budget: &mut crate::geometry::ExpansionBudget,
    ) -> Result<Vec<Self>> {
        let vertices = self.delaunay_triangle_vertices_budgeted(budget)?;
        Self::triangle_shapes_from_vertices(&vertices)
    }

    pub(crate) fn triangle_shapes_from_vertices(vertices: &[Point]) -> Result<Vec<Self>> {
        if !vertices.len().is_multiple_of(4) {
            return Err(GeometryErrorKind::Invalid(
                "triangle vertex stream is not a multiple of four".into(),
            )
            .into());
        }
        let mut shapes = Vec::with_capacity(vertices.len() / 4);
        for ring in vertices.as_chunks::<4>().0 {
            shapes.push(Self::Polygon(Polygon::new(
                Ring::try_from_closed(CoordSeq::try_from_points(ring)?)?,
                Vec::new(),
            )));
        }
        Ok(shapes)
    }

    /// The Delaunay triangulation as one flat CLOSED-ring vertex stream — four
    /// points `[a, c, b, a]` per triangle, CCW (gometry's shell canon). Lets the
    /// array surface build a packed `Polygons` layout in ONE pass instead of a
    /// `Polygon`/`CoordSeq` per triangle that is then re-packed.
    pub(crate) fn delaunay_triangle_vertices(&self) -> Result<Vec<Point>> {
        let mut budget = crate::geometry::ExpansionBudget::new("triangulate", "method");
        self.delaunay_triangle_vertices_budgeted(&mut budget)
    }

    fn delaunay_points(&self) -> Result<Vec<Point>> {
        // XY-unique vertices keeping FULL ordinates (first-seen Z/M): triangle
        // corners are input vertices, so carrying their Z/M here preserves it
        // directly — no post-hoc XY re-lookup (the old `carry_each` step).
        let source_points = self.points_vec();
        let (points, _) = selected_source_sites(source_points)?;
        if points.len() < 3 {
            return Ok(Vec::new());
        }
        // The exact engine takes the unique vertices as they are: no
        // conversion, no proposal path, no repair afterwards.
        Ok(points)
    }

    pub(crate) fn delaunay_triangle_vertices_budgeted(
        &self,
        budget: &mut crate::geometry::ExpansionBudget,
    ) -> Result<Vec<Point>> {
        let points = self.delaunay_points()?;
        if points.len() < 3 {
            return Ok(Vec::new());
        }
        // The triangulation's indexed faces are input-sized working state; the
        // closed-ring vertices below are the generated output. Charge their
        // exact count before reserving or emitting any of them.
        let mut sites = Vec::with_capacity(points.len());
        for (id, &point) in points.iter().enumerate() {
            sites.push(Site { id, point });
        }
        let triangulation = certified_delaunay(&sites)?;
        let capacity = triangulation
            .triangles()
            .len()
            .checked_mul(4)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        budget.check_additional(capacity)?;
        #[cfg(test)]
        note_delaunay_output_reserve();
        let mut vertices = Vec::with_capacity(capacity);
        let mut triangles = triangulation.triangles().to_vec();
        triangles.sort_unstable_by_key(|triangle| {
            let mut key = *triangle;
            key.sort_unstable();
            key
        });
        for triangle in triangles {
            vertices.extend_from_slice(&selected_triangle_points(&points, triangle, false));
        }
        budget.add(capacity)?;
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
        // Build and publish one constrained mesh plan; packed and per-shape
        // callers consume the same selected source sites and face IDs.
        let vertices = self.constrained_delaunay_vertices_budgeted(refinement, budget)?;
        Self::triangle_shapes_from_vertices(&vertices)
    }

    /// Constrained-Delaunay interior as a flat `[a, b, c, a]`-per-triangle
    /// vertex stream. Each ring is emitted from the shared mesh plan: inactive
    /// refinement preserves selected source-site axes, while active refinement
    /// emits uniform XY.
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
        match self.polygonal().ok_or(GeometryErrorKind::PolygonRequired)? {
            Polygonal::Polygon(polygon) => constrained_triangle_vertices(
                polygon.rings().map(Ring::coords),
                self.area(),
                refinement,
                budget,
            ),
            Polygonal::MultiPolygon(polygons) => constrained_triangle_vertices(
                polygons.iter().flat_map(Polygon::rings).map(Ring::coords),
                self.area(),
                refinement,
                budget,
            ),
            Polygonal::Empty => Ok(Vec::new()),
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
        self.earcut_with(&mut |a, b, c| sink.emit(a, b, c))?;
        Ok(sink.into_shapes())
    }

    /// Axes every contributing ring shares, or `None` when rings disagree,
    /// the operand is empty, or it is not areal. Packed triangle columns need
    /// this before clip; `Polygon::axes` / `MultiPolygon::axes` are unions.
    pub(crate) fn earcut_packed_axes(&self) -> Option<CoordinateAxes> {
        fn rings_axes<'a>(rings: impl IntoIterator<Item = &'a Ring>) -> Option<CoordinateAxes> {
            let mut axes = None;
            for ring in rings {
                let member = ring.axes();
                match axes {
                    None => axes = Some(member),
                    Some(prev) if prev == member => {},
                    Some(_) => return None,
                }
            }
            axes
        }
        match self {
            Self::Polygon(polygon) => rings_axes(polygon.rings()),
            Self::MultiPolygon(polygons) => {
                let mut axes = None;
                for polygon in polygons.iter() {
                    let part = rings_axes(polygon.rings())?;
                    match axes {
                        None => axes = Some(part),
                        Some(prev) if prev == part => {},
                        Some(_) => return None,
                    }
                }
                axes
            },
            _ => None,
        }
    }

    pub(crate) fn polygon_triangle_vertices(&self) -> Result<Vec<Point>> {
        let mut budget = crate::geometry::ExpansionBudget::new("triangulate", "method");
        self.polygon_triangle_vertices_budgeted(&mut budget)
    }

    pub(crate) fn polygon_triangle_vertices_budgeted(
        &self,
        budget: &mut crate::geometry::ExpansionBudget,
    ) -> Result<Vec<Point>> {
        let mut sink = BudgetedPackedTriangleSink::new(budget);
        self.earcut_with(&mut |a, b, c| sink.emit(a, b, c))?;
        Ok(sink.into_vertices())
    }

    fn earcut_with(&self, emit: &mut impl FnMut(Point, Point, Point) -> Result<()>) -> Result<()> {
        match self.polygonal().ok_or(GeometryErrorKind::PolygonRequired)? {
            Polygonal::Polygon(polygon) => earcut_polygon_with(polygon, emit),
            Polygonal::MultiPolygon(polygons) => {
                for polygon in polygons {
                    earcut_polygon_with(polygon, emit)?;
                }
                Ok(())
            },
            Polygonal::Empty => Ok(()),
        }
    }

    /// `count` planar-uniform random points on the geometry, deterministic
    /// from `seed` (a splitmix64 stream). The sample space is the
    /// geometry's highest-dimension support — uniform over area (areal
    /// input: triangulate, area-weighted pick, square-root barycentric
    /// warp), along length (lineal input: length-weighted segment pick), or
    /// across point atoms (equal weight) — with the centroid rule's
    /// degenerate fallback: zero-area input samples its linework, zero
    /// length its vertices. Output points are XY (new locations have no
    /// meaningful Z/M); CRS rides at the surface layer. An empty geometry
    /// samples nothing.
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
            let frame = triangles.first().and_then(|[origin, ..]| {
                let extent_x =
                    axis_extent(triangles.iter().flatten().map(|point| point.x), origin.x)?;
                let extent_y =
                    axis_extent(triangles.iter().flatten().map(|point| point.y), origin.y)?;
                AxisFrame::from_origin_extents(*origin, extent_x, extent_y)
            });
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
            let mut segments: Vec<Segment> = Vec::with_capacity(self.segment_count());
            self.for_each_segment_chain(|chain| {
                segments.extend(line_segments(chain));
            });
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
        let atoms = self.points_vec();
        Ok(sample_weighted(
            count,
            seed,
            &atoms,
            |_| 1.0,
            |atom, _| Point::new(atom.x, atom.y),
        )?
        .unwrap_or_default())
    }

    #[cfg(test)]
    pub(crate) fn voronoi_polygons(
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
        let (canonical, mesh) = self.prepare_voronoi(tolerance)?;
        let Some(mesh) = mesh else {
            // A lone site's cell is the whole clip region; the envelope and
            // padded frames of one point have no area.
            return Ok(match (canonical.len(), boundary) {
                (1, VoronoiBoundary::Polygon(polygon)) => vec![Self::Polygon(polygon.clone())],
                _ => Vec::new(),
            });
        };
        let cells = if mesh.collinear_order().is_some() {
            voronoi_dcel::build_collinear_polygons(&canonical, boundary, budget, &mesh)?
        } else {
            voronoi_dcel::build_product(
                &canonical,
                boundary,
                budget,
                true,
                &mesh,
                voronoi_dcel::VoronoiProduct::Polygons,
            )?
            .into_polygons()
        };
        Ok(cells.into_iter().map(Self::Polygon).collect())
    }

    /// The edges as one packed line column: what an array result stores, so
    /// the Python lane never builds a line object per edge.
    pub(crate) fn voronoi_edge_column_budgeted(
        &self,
        tolerance: f64,
        boundary: VoronoiBoundary<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<(CoordSeq, CsrOffsetColumn)> {
        self.voronoi_edges_raw(tolerance, boundary, budget)?
            .into_line_column()
    }

    fn voronoi_edges_raw(
        &self,
        tolerance: f64,
        boundary: VoronoiBoundary<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<voronoi_dcel::EmbeddedEdges> {
        let tolerance = NonNegative::try_new("tolerance", tolerance)?.get();
        let (canonical, Some(mesh)) = self.prepare_voronoi(tolerance)? else {
            return voronoi_dcel::empty_packed_edges();
        };
        if mesh.collinear_order().is_none() {
            return voronoi_dcel::build_edges(&canonical, boundary, budget, true, &mesh);
        }
        voronoi_dcel::build_collinear_edges(&canonical, boundary, budget, &mesh)
    }

    pub fn boundary(&self) -> Result<Self> {
        Ok(match self {
            // Points have no boundary; a typed `POLYGON EMPTY` still boundaries
            // to linework (`MULTILINESTRING EMPTY`), per the polygon contract.
            Self::Point(_) | Self::MultiPoint(_) => empty_geometry(),
            // Dimensional empties keep their declared axes on the boundary
            // sentinel, matching `exterior` (a Z empty polygon's boundary is
            // MULTILINESTRING Z EMPTY, not an XY collapse).
            Self::Empty(
                GeometryKind::Point | GeometryKind::GeometryCollection | GeometryKind::MultiPoint,
                axes,
            ) => Self::typed_empty(GeometryKind::GeometryCollection, *axes),
            Self::Empty(GeometryKind::LineString | GeometryKind::MultiLineString, axes) => {
                Self::typed_empty(GeometryKind::MultiPoint, *axes)
            },
            Self::Empty(GeometryKind::Polygon | GeometryKind::MultiPolygon, axes) => {
                Self::typed_empty(GeometryKind::MultiLineString, *axes)
            },
            Self::LineString(points) => line_boundary(points, points.axes())?,
            Self::MultiLineString(lines) => multiline_boundary(lines, lines.axes())?,
            // A polygon's boundary IS its rings as linework: clone the ring
            // coordinate columns straight into the result (one `LineString`, or
            // a `MultiLineString` when there are holes / parts) — no per-vertex
            // `Point` round-trip through `to_vec` + column rebuild.
            Self::Polygon(polygon) => {
                rings_to_boundary(polygon.rings().map(Ring::as_line).collect(), polygon.axes())
            },
            Self::MultiPolygon(polygons) => rings_to_boundary(
                polygons
                    .iter()
                    .flat_map(Polygon::rings)
                    .map(Ring::as_line)
                    .collect(),
                polygons.axes(),
            ),
            Self::GeometryCollection(geometries) => {
                let mut boundaries = Vec::new();
                for geometry in geometries {
                    let boundary = geometry.boundary()?;
                    if !boundary.is_empty() {
                        boundaries.push(boundary);
                    }
                }
                if boundaries.is_empty() {
                    Self::typed_empty(GeometryKind::GeometryCollection, self.axes())
                } else {
                    Self::GeometryCollection(geometries.try_rebuild_members(boundaries)?)
                }
            },
        })
    }

    pub fn polygonize(&self, strict: bool) -> Result<Vec<Self>> {
        Self::polygonize_all(&[self], strict)
    }

    /// Polygonize many shapes as one noding universe: every line part from
    /// every input participates in a single assembly (the array form — rings
    /// that close across elements still close). Output rings reuse input
    /// vertices and interpolate noding vertices from source edges, so Z/M is
    /// carried by provenance; `strict` selects strict ordinate verification
    /// while the default uses the lenient degradation policy.
    pub fn polygonize_all(shapes: &[&Self], strict: bool) -> Result<Vec<Self>> {
        let inventory = ExactSourceInventory::from_shapes(shapes)?;
        let mut budget = ExpansionBudget::new("polygonize", "exact partition");
        let mut partition = inventory.partition(&mut budget)?;
        let polygons = inventory
            .linework_polygons(&mut partition, false)?
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
        let inventory = ExactSourceInventory::from_shapes(shapes)?;
        let mut budget = ExpansionBudget::new("build_area", "exact partition");
        let mut partition = inventory.partition(&mut budget)?;
        let polygons = inventory
            .linework_polygons(&mut partition, true)?
            .into_iter()
            .map(Self::Polygon)
            .collect();
        carry_each(polygons, shapes, "build_area", strict)
    }

    /// Assemble this geometry's linework into one areal geometry (GEOS
    /// `BuildArea`): nested rings alternate solid/hole by even-odd fill, so a
    /// ring enclosing a hole becomes one holed polygon. Returns `POLYGON EMPTY`
    /// when no ring closes, a `Polygon` for one face, a `MultiPolygon` when all
    /// returned polygons share axes, or a `GeometryCollection` when their axes
    /// differ.
    pub fn build_area(&self, strict: bool) -> Result<Self> {
        let polygons: Vec<Polygon> = Self::build_area_all(&[self], strict)?
            .into_iter()
            .map(|shape| match shape {
                Self::Polygon(polygon) => polygon,
                _ => unreachable!("build_area_all yields only polygons"),
            })
            .collect();
        match polygons.len() {
            0 => Ok(Self::empty_polygon()),
            1 => Ok(Self::Polygon(
                polygons.into_iter().next().expect("one polygon"),
            )),
            _ => {
                let axes = polygons[0].axes();
                if polygons.iter().all(|polygon| polygon.axes() == axes) {
                    let members = polygons
                        .into_iter()
                        .map(crate::geometry::MultiMember::Value)
                        .collect();
                    Ok(Self::MultiPolygon(crate::geometry::MultiPolygon::try_new(
                        axes, members,
                    )?))
                } else {
                    Self::geometry_collection(polygons.into_iter().map(Self::Polygon).collect())
                }
            },
        }
    }

    /// [`Shape::polygonize_all`] with diagnostics: one combined universe for
    /// polygons, cut edges, dangles, and invalid rings alike.
    pub fn polygonize_full_all(shapes: &[&Self], strict: bool) -> Result<PolygonizeFull> {
        let inventory = ExactSourceInventory::from_shapes(shapes)?;
        let mut budget = ExpansionBudget::new("polygonize_full", "exact partition");
        let mut partition = inventory.partition(&mut budget)?;
        let exact_polygons = inventory.linework_polygons(&mut partition, false)?;
        let mut full = inventory.polygonize_diagnostics(&partition)?;
        full.polygons = exact_polygons.into_iter().map(Shape::Polygon).collect();
        let carry_diagnostic_rows = |rows: Vec<Vec<Self>>| {
            rows.into_iter()
                .enumerate()
                .map(|(row, outputs)| {
                    carry_each(outputs, &[shapes[row]], "polygonize_full", strict)
                })
                .collect::<Result<Vec<_>>>()
        };
        Ok(PolygonizeFull {
            polygons: carry_each(full.polygons, shapes, "polygonize_full", strict)?,
            cuts: carry_diagnostic_rows(full.cuts)?,
            dangles: carry_diagnostic_rows(full.dangles)?,
            invalid_rings: carry_diagnostic_rows(full.invalid_rings)?,
        })
    }
}

impl Shape {
    fn voronoi_sites(&self, tolerance: f64) -> Result<Vec<Site>> {
        let mut points = Vec::with_capacity(self.coord_count());
        self.for_each_point(|point| points.push(point.to_xy()));
        canonical_voronoi_sites(points, tolerance)
    }

    fn prepare_voronoi(&self, tolerance: f64) -> Result<(Vec<Site>, Option<CertifiedDelaunay>)> {
        let sites = self.voronoi_sites(tolerance)?;
        if let Some(last) = sites.len().checked_sub(1) {
            u32::try_from(last).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        }
        let mesh = if sites.len() >= 2 {
            Some(certified_delaunay(&sites)?)
        } else {
            None
        };
        Ok((sites, mesh))
    }
}

/// Canonical sites: signed zeros normalized, coincident points merged, and
/// snapping applied once at the public boundary, so the tessellators always
/// receive a true site set in x order with dense ids.
pub(super) fn canonical_voronoi_sites(mut points: Vec<Point>, tolerance: f64) -> Result<Vec<Site>> {
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
    let sites = points
        .into_iter()
        .enumerate()
        .map(|(id, point)| Site { id, point })
        .collect();
    if tolerance == 0.0 {
        return Ok(sites);
    }
    let mut survivors = snap_sites(&sites, tolerance)?;
    for (id, site) in survivors.iter_mut().enumerate() {
        site.id = id;
    }
    Ok(survivors)
}
