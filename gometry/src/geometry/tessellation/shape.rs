use super::*;
use crate::NonNegative;
use crate::error::Result;
impl Shape {
    pub fn delaunay_triangles(&self) -> Result<Vec<Self>> {
        let shapes = self
            .delaunay_triangle_vertices()
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
    pub fn delaunay_triangle_vertices(&self) -> Vec<Point> {
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
        // Sweep-circle triangulation (delaunator: robust-predicate hull walk,
        // the mapbox algorithm) straight over the unique vertices -- no geo
        // conversion, no incremental-insertion engine.
        let triangulation = delaunay_triangulation(&points);
        let mut vertices = Vec::with_capacity(triangulation.triangles.len() / 3 * 4);
        for triangle in triangulation.triangles.as_chunks::<3>().0 {
            // Delaunator emits triangles CLOCKWISE in y-up coordinates; shells
            // are CCW in gometry's canon, so reverse the walk: a, c, b, a.
            vertices.extend_from_slice(&[
                points[triangle[0]],
                points[triangle[2]],
                points[triangle[1]],
                points[triangle[0]],
            ]);
        }
        vertices
    }

    pub(crate) fn constrained_delaunay_triangles(
        &self,
        refinement: CdtRefinement,
    ) -> Result<Vec<Self>> {
        // The "is this face inside the polygon?" filter runs once per triangle
        // (~one per vertex), so build the banded raycaster ONCE and reuse it:
        // an uncached `contains_point` per face is an O(faces x edges) brute
        // sweep that dominated large inputs (8x slower than GEOS at 800 verts).
        let shapes = match self {
            Self::Polygon(polygon) => {
                let tester = PointBatchTester::new(self);
                constrained_triangles(
                    polygon.rings(),
                    |centroid| tester.contains_point(centroid),
                    refinement,
                )
            },
            Self::MultiPolygon(polygons) => {
                let tester = PointBatchTester::new(self);
                constrained_triangles(
                    polygons.iter().flat_map(Polygon::rings),
                    |centroid| tester.contains_point(centroid),
                    refinement,
                )
            },
            Self::Empty(EmptyKind::Polygon | EmptyKind::MultiPolygon, _) => Ok(Vec::new()),
            _ => Err(GeometryErrorKind::PolygonRequired.into()),
        }?;
        if refinement.active() {
            return Ok(shapes);
        }
        carry_each(shapes, &[self], "triangulate", false)
    }

    /// Constrained-Delaunay interior as a flat `[a, b, c, a]`-per-triangle
    /// vertex stream (XY) — the packed `Polygons` builder's input. Z/M callers
    /// use [`Self::constrained_delaunay_triangles`] (which resolves ordinates);
    /// this 2D form is exact for XY operands.
    pub(crate) fn constrained_delaunay_vertices(
        &self,
        refinement: CdtRefinement,
    ) -> Result<Vec<Point>> {
        match self {
            Self::Polygon(polygon) => {
                let tester = PointBatchTester::new(self);
                constrained_triangle_vertices(
                    polygon.rings(),
                    |centroid| tester.contains_point(centroid),
                    refinement,
                )
            },
            Self::MultiPolygon(polygons) => {
                let tester = PointBatchTester::new(self);
                constrained_triangle_vertices(
                    polygons.iter().flat_map(Polygon::rings),
                    |centroid| tester.contains_point(centroid),
                    refinement,
                )
            },
            Self::Empty(EmptyKind::Polygon | EmptyKind::MultiPolygon, _) => Ok(Vec::new()),
            _ => Err(GeometryErrorKind::PolygonRequired.into()),
        }
    }

    pub fn polygon_triangles(&self) -> Result<Vec<Self>> {
        let shapes = match self {
            Self::Polygon(polygon) => polygon_triangles(polygon),
            Self::MultiPolygon(polygons) => {
                let mut triangles = Vec::new();
                for polygon in polygons {
                    triangles.extend(polygon_triangles(polygon)?);
                }
                Ok(triangles)
            },
            Self::Empty(EmptyKind::Polygon | EmptyKind::MultiPolygon, _) => Ok(Vec::new()),
            _ => Err(GeometryErrorKind::PolygonRequired.into()),
        }?;
        carry_each(shapes, &[self], "polygon_triangles", false)
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
            // One common power-of-two scale keeps the cross products finite
            // for huge-but-valid coordinates; weights only need to be
            // relatively consistent.
            let scale = weight_scale(
                triangles
                    .iter()
                    .flatten()
                    .flat_map(|point| [point.x.abs(), point.y.abs()])
                    .fold(0.0_f64, f64::max),
            );
            let placed = sample_weighted(
                count,
                seed,
                &triangles,
                |[a, b, c]| {
                    (((b.x - a.x) * scale) * ((c.y - a.y) * scale)
                        - ((b.y - a.y) * scale) * ((c.x - a.x) * scale))
                        .abs()
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
        let tolerance = NonNegative::try_new("tolerance", tolerance)?.get();
        let points = self.unique_xy_points();
        if points.len() < 2 {
            return Ok(Vec::new());
        }
        // Native dual lane for the rect-bounded modes: every cell is the
        // boundary rect successively half-plane-clipped by its Delaunay
        // neighbors' bisectors — hull cells need no infinite-ray handling
        // at all. The OTHER modes keep the geo engine on MEASURED grounds:
        // tolerance>0 snapping is 0.95x shapely (parity), and routing
        // polygon clips through per-cell native overlay measured SLOWER
        // than geo's integrated clip (13.8 vs 12.5ms at 5k sites) — an
        // arrangement setup per tiny convex cell outweighs the native
        // cell construction. Collinear-degenerate site sets also stay geo.
        if tolerance == 0.0
            && matches!(
                boundary,
                VoronoiBoundary::Padded | VoronoiBoundary::Envelope
            )
            && let Some(cells) = native_voronoi_cells(&points, &boundary)
        {
            return Ok(cells.into_iter().map(Self::Polygon).collect());
        }
        // Spade-direct fallback (the engine geo wrapped): build raw cells from
        // the Delaunay dual — circumcenters plus extended hull rays — then clip
        // each to the boundary rectangle or polygon.
        let (raw_cells, base) = build_raw_voronoi_cells(&points, tolerance)?;
        let cells = clip_voronoi_cells(raw_cells, base, boundary);
        Ok(cells.into_iter().map(Self::Polygon).collect())
    }

    pub fn voronoi_edges(
        &self,
        tolerance: f64,
        boundary: VoronoiBoundary<'_>,
    ) -> Result<Vec<Self>> {
        let tolerance = NonNegative::try_new("tolerance", tolerance)?.get();
        let _ = boundary; // edges always clip to the 50%-padded bounds (PostGIS).
        let points = self.unique_xy_points();
        if points.len() < 2 {
            return Ok(Vec::new());
        }
        let lines = voronoi_edge_segments(&points, tolerance)?;
        lines
            .into_iter()
            .map(|[start, end]| {
                Ok(Self::LineString(LineSeq::try_new(CoordSeq::from(vec![
                    Point::new(start.x, start.y)?,
                    Point::new(end.x, end.y)?,
                ]))?))
            })
            .collect()
    }

    pub fn minimum_rotated_rectangle(&self) -> Result<Self> {
        // Empty input has no rectangle: `POLYGON EMPTY` (the op's areal output
        // type, matching Shapely), not the untyped collection the hull yields.
        if self.is_empty() {
            return Ok(Self::empty_polygon());
        }
        // Fused monotone-chain hull + rotating calipers — O(n log n + h), no
        // intermediate polygon hull or ordinate carry on the hot path.
        let points = self.points_vec();
        let open = monotone_chain_hull(&points);
        if open.len() < 3 {
            return carry_ordinates(
                shape_from_open_hull(&open, Self::empty_polygon),
                &[self],
                "minimum_rotated_rectangle",
                false,
            );
        }
        Ok(Polygon {
            shell: Ring::from_trusted_closed(minimum_area_rectangle(&open)),
            holes: Vec::new().into(),
        }
        .normalized_degenerate())
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
