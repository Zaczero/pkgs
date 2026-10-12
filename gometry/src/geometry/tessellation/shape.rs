use std::borrow::Borrow;

use super::delaunay::certified_delaunay_for_retained_points;
use crate::NonNegative;
use crate::error::Result;
use crate::geographic::{ChartPointSite, GeographicFrame, PointSelectionChart};
use crate::geometry::overlay::ExactSourceInventory;
use crate::geometry::tessellation::{
    CdtRefinement, CertifiedDelaunay, RingOwner, Site, certified_delaunay,
    collect_sample_triangles, constrained_triangle_vertices, earcut_polygon_with, sample_weighted,
    selected_triangle_points, snap_sites, try_triangle_shape, uniform_f64, voronoi_dcel,
    weight_scale,
};
use crate::geometry::{
    AxisFrame, CoordSeq, CoordinateAxes, Coordinates as _, Dimension, ExpansionBudget,
    GeometryErrorKind, MultiMember, MultiPolygon, Point, PointKey, Polygon, Polygonal,
    PolygonizeFull, Ring, Segment, Shape, ShapeCountQuery, SourceArcContext, TopologyDomain,
    VoronoiBoundary, axis_extent, line_segments,
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
    DELAUNAY_OUTPUT_RESERVES.with(|count| {
        let reserves = count
            .get()
            .checked_add(1)
            .expect("Delaunay output reserve test counter overflow");
        count.set(reserves);
    });
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

#[derive(Clone, Copy)]
struct GeographicSampleSegment {
    start: Point,
    end: Point,
    longitude: crate::geographic::RetainedChartDisplacement,
    delta_x: f64,
    delta_y: f64,
    raw_compatible: bool,
}

impl GeographicSampleSegment {
    fn sample(self, along: f64, frame: crate::geographic::GeographicFrame) -> Result<Point> {
        let x = if self.raw_compatible {
            (1.0 - along) * self.start.x + along * self.end.x
        } else {
            let start = self.longitude.endpoints()[0].get();
            frame
                .longitude()
                .canonicalize(start + self.delta_x * along)
                .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))?
                .get()
        };
        Point::new(x, (1.0 - along) * self.start.y + along * self.end.y)
    }
}

#[expect(
    clippy::float_cmp,
    reason = "raw interpolation requires equal deltas; signed zero remains compatible"
)]
fn raw_delta_matches_retained(raw_delta: f64, retained_delta: f64) -> bool {
    raw_delta == retained_delta
}

fn geographic_sample_segments(
    shape: &Shape,
    frame: crate::geographic::GeographicFrame,
) -> Result<Vec<GeographicSampleSegment>> {
    let mut segments = Vec::with_capacity(ShapeCountQuery::default().segments(shape)?);
    shape.try_for_each_segment_chain(|chain| -> Result<()> {
        let mut points = chain.iter_coords();
        let Some(mut start) = points.next() else {
            return Ok(());
        };
        for end in points {
            let connection = frame
                .longitude()
                .chart_connection(start.x, end.x)
                .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))?;
            let longitude = connection.retained();
            let raw_delta = end.x - start.x;
            let retained_delta = longitude.delta_native(frame.longitude());
            let raw_compatible = raw_delta_matches_retained(raw_delta, retained_delta);
            segments.push(GeographicSampleSegment {
                start,
                end,
                longitude,
                delta_x: if raw_compatible {
                    raw_delta
                } else {
                    retained_delta
                },
                delta_y: end.y - start.y,
                raw_compatible,
            });
            start = end;
        }
        Ok(())
    })?;
    Ok(segments)
}

fn geographic_sample_requires_preparation(
    shape: &Shape,
    frame: crate::geographic::GeographicFrame,
) -> Result<bool> {
    let axis = frame.longitude();
    let mut raw_compatible = true;
    shape.try_for_each_segment_chain(|chain| -> Result<()> {
        let mut points = chain.iter_coords();
        let Some(mut start) = points.next() else {
            return Ok(());
        };
        for end in points {
            let connection = axis
                .chart_connection(start.x, end.x)
                .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))?;
            if connection.step.is_circle()
                || !raw_delta_matches_retained(
                    end.x - start.x,
                    connection.retained().delta_native(axis),
                )
            {
                raw_compatible = false;
                return Ok(());
            }
            start = end;
        }
        Ok(())
    })?;
    Ok(!raw_compatible || crate::geometry::crosses_antimeridian_in_frame(shape, frame))
}

fn prepare_geographic_sample_leaf(
    shape: &Shape,
    frame: crate::geographic::GeographicFrame,
) -> Result<Shape> {
    if geographic_sample_requires_preparation(shape, frame)? {
        shape.force_2d()?.split_antimeridian_in_frame(frame)
    } else {
        Ok(shape.clone())
    }
}

fn geographic_areal_sample_support(
    shape: &Shape,
    frame: crate::geographic::GeographicFrame,
) -> Result<Shape> {
    match shape {
        Shape::Polygon(_) => prepare_geographic_sample_leaf(shape, frame),
        Shape::MultiPolygon(polygons) => {
            let mut members = Vec::with_capacity(polygons.member_count());
            for member in polygons.members_with_values() {
                match member {
                    MultiMember::Empty => members.push(MultiMember::Empty),
                    MultiMember::Value(polygon) => {
                        let polygon = Shape::Polygon(polygon.clone());
                        match prepare_geographic_sample_leaf(&polygon, frame)?.force_2d()? {
                            Shape::Polygon(polygon) => members.push(MultiMember::Value(polygon)),
                            Shape::MultiPolygon(split) => {
                                members.extend(split.members_with_values().map(
                                    |member| match member {
                                        MultiMember::Empty => MultiMember::Empty,
                                        MultiMember::Value(polygon) => {
                                            MultiMember::Value(polygon.clone())
                                        },
                                    },
                                ));
                            },
                            Shape::Point(_)
                            | Shape::MultiPoint(_)
                            | Shape::LineString(_)
                            | Shape::MultiLineString(_)
                            | Shape::GeometryCollection(_)
                            | Shape::Empty(..) => {
                                unreachable!("splitting areal support preserves surface dimension")
                            },
                        }
                    },
                }
            }
            Ok(Shape::MultiPolygon(MultiPolygon::try_new(
                CoordinateAxes::XY,
                members,
            )?))
        },
        Shape::GeometryCollection(collection) => Ok(Shape::GeometryCollection(
            collection.try_map_members(|member| geographic_areal_sample_support(member, frame))?,
        )),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => Ok(shape.clone()),
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
        // Sites are an XY set, but each one carries the first full source Point
        // so triangle corners retain its Z/M without a post-hoc lookup.
        let mut points = self.first_seen_xy_vertices()?;
        points.sort_unstable_by(|left, right| {
            left.x
                .total_cmp(&right.x)
                .then_with(|| left.y.total_cmp(&right.y))
        });
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
            sites.push(Site::raw(id, point, id));
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
        let xy = |site| super::delaunay::exact_delaunay::site_xy(&points, site);
        super::delaunay::exact_delaunay::canonicalize_triangles(&xy, &mut triangles);
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
                polygon
                    .rings()
                    .enumerate()
                    .map(|(ring, value)| (RingOwner::new(0, ring), value.coords())),
                self.area(),
                refinement,
                budget,
            ),
            Polygonal::MultiPolygon(polygons) => constrained_triangle_vertices(
                polygons.iter().enumerate().flat_map(|(polygon, value)| {
                    value
                        .rings()
                        .enumerate()
                        .map(move |(ring, value)| (RingOwner::new(polygon, ring), value.coords()))
                }),
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
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::GeometryCollection(_)
            | Self::Empty(..) => None,
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

    /// `count` coordinate-uniform random points on this XY support, deterministic
    /// from `seed` (a splitmix64 stream). The sample space is the
    /// geometry's highest-dimension support — uniform over area (areal
    /// input: triangulate, area-weighted pick, square-root barycentric
    /// warp), along length (lineal input: length-weighted segment pick), or
    /// across point atoms (equal weight) — with the centroid rule's
    /// degenerate fallback: zero-area input samples its linework, zero
    /// length its vertices. Output points are XY (new locations have no
    /// meaningful Z/M); CRS rides at the surface layer. Geographic entry
    /// points prepare retained support before calling this coordinate sampler.
    /// An empty geometry samples nothing.
    pub fn sample_points(&self, count: usize, seed: u64) -> Result<Vec<Point>> {
        ExpansionBudget::check("sample_points", "count", count)?;
        if count == 0 {
            return Ok(Vec::new());
        }
        self.sample_points_from_support(self, count, seed, None)
    }

    pub(crate) fn sample_points_in_domain(
        &self,
        count: usize,
        seed: u64,
        domain: TopologyDomain,
    ) -> Result<Vec<Point>> {
        ExpansionBudget::check("sample_points", "count", count)?;
        if count == 0 {
            return Ok(Vec::new());
        }
        let TopologyDomain::Geographic(frame) = domain else {
            return self.sample_points_from_support(self, count, seed, None);
        };
        if self.topological_dimension() < Dimension::Curve {
            return self.sample_points_from_support(self, count, seed, None);
        }
        crate::geographic::ScalarGeographicView::try_new(frame, self)
            .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))?;
        if !geographic_sample_requires_preparation(self, frame)? {
            return self.sample_points_from_support(self, count, seed, None);
        }
        let area_support = geographic_areal_sample_support(self, frame)?;
        let segments = geographic_sample_segments(self, frame)?;
        area_support.sample_points_from_support(self, count, seed, Some((&segments, &frame)))
    }

    fn sample_points_from_support(
        &self,
        point_source: &Self,
        count: usize,
        seed: u64,
        geographic_segments: Option<(
            &[GeographicSampleSegment],
            &crate::geographic::GeographicFrame,
        )>,
    ) -> Result<Vec<Point>> {
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
            let placed = if let Some((segments, frame)) = geographic_segments {
                let scale = weight_scale(
                    segments
                        .iter()
                        .flat_map(|segment| [segment.start, segment.end])
                        .flat_map(|point| [point.x.abs(), point.y.abs()])
                        .fold(0.0_f64, f64::max),
                );
                sample_weighted(
                    count,
                    seed,
                    segments,
                    |segment| {
                        let dx = segment.delta_x * scale;
                        let dy = segment.delta_y * scale;
                        (dx * dx + dy * dy).sqrt()
                    },
                    |segment, state| segment.sample(uniform_f64(state), *frame),
                )?
            } else {
                let segment_count = ShapeCountQuery::default().segments(self)?;
                let mut segments: Vec<Segment> = Vec::with_capacity(segment_count);
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
                sample_weighted(
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
                )?
            };
            if let Some(points) = placed {
                return Ok(points);
            }
        }
        let atoms = point_source.points_vec()?;
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
    pub(crate) fn voronoi_cells(
        &self,
        tolerance: f64,
        boundary: VoronoiBoundary<'_>,
    ) -> Result<Vec<Self>> {
        let mut budget = ExpansionBudget::new("voronoi_cells", "sites/clip topology");
        self.voronoi_cells_budgeted(tolerance, boundary, &mut budget)
    }

    #[cfg(test)]
    pub(crate) fn voronoi_cells_budgeted(
        &self,
        tolerance: f64,
        boundary: VoronoiBoundary<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<Vec<Self>> {
        self.voronoi_polygon_outputs_budgeted(tolerance, boundary, budget)
            .map(|outputs| {
                outputs
                    .into_iter()
                    .map(|output| Self::Polygon(output.polygon))
                    .collect()
            })
    }

    #[cfg(test)]
    pub(crate) fn voronoi_polygon_outputs_budgeted(
        &self,
        tolerance: f64,
        boundary: VoronoiBoundary<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<crate::geometry::VoronoiPolygonOutputs> {
        self.voronoi_polygon_outputs_in_frame_budgeted(tolerance, boundary, None, budget)
    }

    pub(crate) fn voronoi_polygon_outputs_in_frame_budgeted(
        &self,
        tolerance: f64,
        boundary: VoronoiBoundary<'_>,
        frame: Option<GeographicFrame>,
        budget: &mut ExpansionBudget,
    ) -> Result<crate::geometry::VoronoiPolygonOutputs> {
        let tolerance = NonNegative::try_new("tolerance", tolerance)?.get();
        let (canonical, mesh, chart) = self.prepare_voronoi(tolerance, frame, boundary)?;
        let Some(mesh) = mesh else {
            // A lone site's cell is the whole clip region; the envelope and
            // padded frames of one point have no area.
            return match (canonical.len(), boundary) {
                (1, VoronoiBoundary::Polygon(polygon)) => {
                    voronoi_dcel::lone_site_polygon_outputs(&canonical, polygon, chart)
                },
                (1, _) => voronoi_dcel::lone_site_empty_polygon_outputs(&canonical),
                _ => voronoi_dcel::empty_polygon_outputs(&canonical),
            };
        };
        let embedded = if mesh.collinear_order().is_some() {
            voronoi_dcel::build_collinear_polygons_with_chart(
                &canonical, boundary, budget, &mesh, chart,
            )?
        } else {
            voronoi_dcel::build_product_with_chart(
                &canonical,
                boundary,
                budget,
                true,
                &mesh,
                voronoi_dcel::VoronoiProduct::Polygons,
                chart,
            )?
        };
        let (cells, receipt) = embedded.into_polygon_outputs();
        let outputs = cells
            .into_iter()
            .map(|output| crate::geometry::VoronoiPolygonOutput {
                polygon: output.polygon,
                owner: output.owner,
                edge_roles: output.edge_roles,
            })
            .collect();
        Ok(crate::geometry::VoronoiPolygonOutputs { outputs, receipt })
    }

    /// The edges as one packed line column: what an array result stores, so
    /// the Python lane never builds a line object per edge.
    #[cfg(test)]
    pub(crate) fn voronoi_edge_column_budgeted(
        &self,
        tolerance: f64,
        boundary: VoronoiBoundary<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<(CoordSeq, crate::geometry::CsrOffsetColumn)> {
        self.voronoi_edge_lines_raw(tolerance, boundary, budget)?
            .into_line_column()
    }

    #[cfg(test)]
    fn voronoi_edge_lines_raw(
        &self,
        tolerance: f64,
        boundary: VoronoiBoundary<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<voronoi_dcel::EmbeddedEdges> {
        self.voronoi_edge_lines_in_frame_raw(tolerance, boundary, None, budget)
    }

    pub(crate) fn voronoi_edge_column_in_frame_budgeted(
        &self,
        tolerance: f64,
        boundary: VoronoiBoundary<'_>,
        frame: Option<GeographicFrame>,
        budget: &mut ExpansionBudget,
    ) -> Result<crate::geometry::VoronoiEdgeColumn> {
        let (coordinates, offsets, receipt) = self
            .voronoi_edge_lines_in_frame_raw(tolerance, boundary, frame, budget)?
            .into_line_column_with_receipt()?;
        Ok(crate::geometry::VoronoiEdgeColumn {
            coordinates,
            offsets,
            receipt,
        })
    }

    fn voronoi_edge_lines_in_frame_raw(
        &self,
        tolerance: f64,
        boundary: VoronoiBoundary<'_>,
        frame: Option<GeographicFrame>,
        budget: &mut ExpansionBudget,
    ) -> Result<voronoi_dcel::EmbeddedEdges> {
        let tolerance = NonNegative::try_new("tolerance", tolerance)?.get();
        let (canonical, mesh, chart) = self.prepare_voronoi(tolerance, frame, boundary)?;
        let Some(mesh) = mesh else {
            return if canonical.len() == 1 {
                voronoi_dcel::lone_site_empty_edges(&canonical, boundary, chart)
            } else {
                voronoi_dcel::empty_packed_edges()
            };
        };
        if mesh.collinear_order().is_none() {
            return voronoi_dcel::build_edges_with_chart(
                &canonical, boundary, budget, true, &mesh, chart,
            );
        }
        voronoi_dcel::build_collinear_edges_with_chart(&canonical, boundary, budget, &mesh, chart)
    }

    pub fn polygonize(&self, strict: bool) -> Result<Vec<Self>> {
        Self::polygonize_all(&[self], strict)
    }

    pub(crate) fn polygonize_with_context(
        &self,
        strict: bool,
        context: &SourceArcContext<'_, '_>,
    ) -> Result<Vec<Self>> {
        Self::polygonize_all_with_context(&[self], strict, context)
    }

    /// Polygonize many shapes as one noding universe: every line part from
    /// every input participates in a single assembly (the array form — rings
    /// that close across elements still close). Output rings reuse input
    /// vertices and interpolate noding vertices from source edges, so Z/M is
    /// carried by provenance; `strict` selects strict ordinate verification
    /// while the default uses the lenient degradation policy.
    pub fn polygonize_all<S: Borrow<Self>>(shapes: &[S], strict: bool) -> Result<Vec<Self>> {
        Self::polygonize_all_with_context(
            shapes,
            strict,
            &crate::geometry::SourceArcContext::cartesian(),
        )
    }

    pub(crate) fn polygonize_all_with_context<S: Borrow<Self>>(
        shapes: &[S],
        strict: bool,
        context: &SourceArcContext<'_, '_>,
    ) -> Result<Vec<Self>> {
        let declared_axes = shapes.iter().fold(CoordinateAxes::XY, |axes, shape| {
            axes.union(Borrow::<Self>::borrow(shape).axes())
        });
        let inventory = ExactSourceInventory::from_shapes(shapes)?;
        let mut budget = ExpansionBudget::new("polygonize", "exact partition");
        let mut partition = inventory.partition(&mut budget)?;
        inventory.linework_polygons(
            &mut partition,
            false,
            "polygonize",
            declared_axes,
            strict,
            context,
        )
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
        Self::build_area_all_with_context(
            shapes,
            strict,
            &crate::geometry::SourceArcContext::cartesian(),
        )
    }

    pub(crate) fn build_area_all_with_context(
        shapes: &[&Self],
        strict: bool,
        context: &SourceArcContext<'_, '_>,
    ) -> Result<Vec<Self>> {
        let declared_axes = shapes
            .iter()
            .fold(CoordinateAxes::XY, |axes, shape| axes.union(shape.axes()));
        let inventory = ExactSourceInventory::from_shapes(shapes)?;
        let mut budget = ExpansionBudget::new("build_area", "exact partition");
        let mut partition = inventory.partition(&mut budget)?;
        inventory.linework_polygons(
            &mut partition,
            true,
            "build_area",
            declared_axes,
            strict,
            context,
        )
    }

    /// Assemble this geometry's linework into one areal geometry (GEOS
    /// `BuildArea`): nested rings alternate solid/hole by even-odd fill, so a
    /// ring enclosing a hole becomes one holed polygon. Returns `POLYGON EMPTY`
    /// when no ring closes, a `Polygon` for one face, a `MultiPolygon` when all
    /// returned polygons share axes, or a `GeometryCollection` when their axes
    /// differ.
    pub fn build_area(&self, strict: bool) -> Result<Self> {
        self.build_area_with_context(strict, &crate::geometry::SourceArcContext::cartesian())
    }

    pub(crate) fn build_area_with_context(
        &self,
        strict: bool,
        context: &SourceArcContext<'_, '_>,
    ) -> Result<Self> {
        let polygons: Vec<Polygon> = Self::build_area_all_with_context(&[self], strict, context)?
            .into_iter()
            .map(|shape| {
                let Self::Polygon(polygon) = shape else {
                    unreachable!("build_area_all yields only polygons")
                };
                polygon
            })
            .collect();
        match polygons.len() {
            0 => Ok(Self::empty_polygon()),
            1 => Ok(Self::Polygon(
                polygons.into_iter().next().expect("one polygon"),
            )),
            _ => {
                #[expect(
                    clippy::indexing_slicing,
                    reason = "this arm handles at least two polygons"
                )]
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
    pub fn polygonize_full_all<S: Borrow<Self>>(
        shapes: &[S],
        strict: bool,
    ) -> Result<PolygonizeFull> {
        Self::polygonize_full_all_with_context(
            shapes,
            strict,
            &crate::geometry::SourceArcContext::cartesian(),
        )
    }

    pub(crate) fn polygonize_full_all_with_context<S: Borrow<Self>>(
        shapes: &[S],
        strict: bool,
        context: &SourceArcContext<'_, '_>,
    ) -> Result<PolygonizeFull> {
        let declared_axes = shapes.iter().fold(CoordinateAxes::XY, |axes, shape| {
            axes.union(Borrow::<Self>::borrow(shape).axes())
        });
        let row_axes = shapes
            .iter()
            .map(|shape| Borrow::<Self>::borrow(shape).axes())
            .collect::<Vec<_>>();
        let inventory = ExactSourceInventory::from_shapes_with_occurrences(shapes)?;
        let mut budget = ExpansionBudget::new("polygonize_full", "exact partition");
        let mut partition = inventory.partition(&mut budget)?;
        inventory.polygonize_full(&mut partition, declared_axes, &row_axes, strict, context)
    }
}

impl Shape {
    fn voronoi_sites(
        &self,
        tolerance: f64,
        frame: Option<GeographicFrame>,
        boundary: VoronoiBoundary<'_>,
    ) -> Result<(Vec<Site>, Option<PointSelectionChart>)> {
        let mut points = Vec::new();
        self.try_for_each_point(|point| -> Result<()> {
            points.push(point);
            Ok(())
        })?;
        let chart = if let Some(frame) = frame {
            let mut support = points.clone();
            if let VoronoiBoundary::Polygon(polygon) = boundary {
                support.extend(
                    std::iter::once(&polygon.shell)
                        .chain(polygon.holes.iter())
                        .flat_map(|ring| ring.iter()),
                );
            }
            PointSelectionChart::try_from_points(frame, &support)?
        } else {
            None
        };
        let Some(chart) = chart else {
            return Ok((canonical_voronoi_sites(points, tolerance)?, None));
        };

        let frame = chart.frame();
        let normalize = |value: f64| {
            if value == 0.0 {
                0.0_f64.to_bits()
            } else {
                value.to_bits()
            }
        };
        let mut seen: crate::collections::HashMap<_, usize> =
            crate::collections::try_hash_map_with_capacity(points.len())?;
        let mut retained: Vec<(ChartPointSite, Vec<usize>)> = Vec::new();
        for (source_ordinal, &point) in points.iter().enumerate() {
            let chart_site = chart.site(point, source_ordinal)?;
            let longitude = chart_site.longitude();
            let key = (
                longitude.periods(),
                normalize(longitude.native().get()),
                normalize(chart_site.source_y()),
            );
            if let Some(&site) = seen.get(&key) {
                retained
                    .get_mut(site)
                    .expect("chart site key points into the retained site table")
                    .1
                    .push(source_ordinal);
            } else {
                let site = retained.len();
                retained.push((chart_site, Vec::new()));
                let _ = seen.insert(key, site);
            }
        }
        retained.sort_unstable_by(|(left, _), (right, _)| left.compare_chart_xy(*right));
        let mut sites = Vec::with_capacity(retained.len());
        for (chart_site, quotient_aliases) in retained {
            let source_ordinal = chart_site.source_ordinal();
            let point = *points.get(source_ordinal).ok_or_else(|| {
                GeometryErrorKind::Invalid("retained Voronoi source is unknown".into())
            })?;
            let mut site = Site::retained(sites.len(), point, source_ordinal, frame, chart_site)?;
            site.quotient_aliases = quotient_aliases;
            sites.push(site);
        }
        let mut sites = if tolerance == 0.0 {
            sites
        } else {
            snap_sites(&sites, tolerance)?
        };
        for (id, site) in sites.iter_mut().enumerate() {
            site.id = id;
        }
        Ok((sites, Some(chart)))
    }

    fn prepare_voronoi(
        &self,
        tolerance: f64,
        frame: Option<GeographicFrame>,
        boundary: VoronoiBoundary<'_>,
    ) -> Result<(
        Vec<Site>,
        Option<CertifiedDelaunay>,
        Option<PointSelectionChart>,
    )> {
        let (sites, chart) = self.voronoi_sites(tolerance, frame, boundary)?;
        if let Some(last) = sites.len().checked_sub(1) {
            u32::try_from(last).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        }
        let mesh = if sites.len() >= 2 {
            Some(if let Some(frame) = chart.map(|chart| chart.frame()) {
                let retained = sites
                    .iter()
                    .map(|site| -> Result<crate::geographic::ChartPointSite> {
                        let retained = site.retained_chart_site.ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "retained Voronoi site lost its chart identity".into(),
                            )
                        })?;
                        if retained.source_ordinal() != site.source_ordinal {
                            return Err(GeometryErrorKind::Invalid(
                                "retained Voronoi site lost its source occurrence".into(),
                            )
                            .into());
                        }
                        Ok(retained)
                    })
                    .collect::<Result<Vec<_>>>()?;
                certified_delaunay_for_retained_points(frame, &retained)?
            } else {
                certified_delaunay(&sites)?
            })
        } else {
            None
        };
        Ok((sites, mesh, chart))
    }
}

/// Canonical sites: signed zeros normalized, coincident points merged, and
/// snapping applied once at the public boundary, so the tessellators always
/// receive a true site set in x order with dense ids.
pub(super) fn canonical_voronoi_sites(mut points: Vec<Point>, tolerance: f64) -> Result<Vec<Site>> {
    let mut rows = points.drain(..).enumerate().collect::<Vec<_>>();
    for (_, point) in &mut rows {
        point.x += 0.0;
        point.y += 0.0;
    }
    let mut seen: crate::collections::HashMap<_, usize> =
        crate::collections::try_hash_map_with_capacity(rows.len())?;
    let mut unique: Vec<(usize, Point, Vec<usize>)> = Vec::with_capacity(rows.len());
    for (source_ordinal, point) in rows {
        let key = PointKey::new(point);
        if let Some(&site) = seen.get(&key) {
            unique
                .get_mut(site)
                .expect("raw site key points into the unique source table")
                .2
                .push(source_ordinal);
        } else {
            let site = unique.len();
            unique.push((source_ordinal, point, Vec::new()));
            let _ = seen.insert(key, site);
        }
    }
    unique.sort_unstable_by(|(left_ordinal, left, _), (right_ordinal, right, _)| {
        left.x
            .total_cmp(&right.x)
            .then_with(|| left.y.total_cmp(&right.y))
            .then_with(|| left_ordinal.cmp(right_ordinal))
    });
    let sites: Vec<Site> = unique
        .into_iter()
        .enumerate()
        .map(|(id, (source_ordinal, point, quotient_aliases))| {
            let mut site = Site::raw(id, point, source_ordinal);
            site.quotient_aliases = quotient_aliases;
            site
        })
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

#[cfg(test)]
mod geographic_sample_tests {
    use super::geographic_sample_segments;
    use crate::geographic::GeographicFrame;
    use crate::geometry::{
        CoordSeq, CoordinateAxes, LineSeq, MultiLineString, MultiMember, MultiPolygon, Point,
        Polygon, Ring, Shape, TopologyDomain, row_sample_seed,
    };

    fn line_seq(points: &[(f64, f64)]) -> LineSeq {
        let points = points
            .iter()
            .map(|&(x, y)| Point::new(x, y).unwrap())
            .collect::<Vec<_>>();
        LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap()
    }

    fn line(points: &[(f64, f64)]) -> Shape {
        Shape::LineString(line_seq(points))
    }

    fn polygon(points: &[(f64, f64)]) -> Polygon {
        let (xs, ys): (Vec<_>, Vec<_>) = points.iter().copied().unzip();
        Polygon::new(
            Ring::closed_coordseq(CoordSeq::try_from_vecs(xs, ys, None, None).unwrap()).unwrap(),
            Vec::new(),
        )
    }

    fn xy_bits(points: &[Point]) -> Vec<(u64, u64)> {
        points
            .iter()
            .map(|point| (point.x.to_bits(), point.y.to_bits()))
            .collect()
    }

    #[test]
    fn retained_line_midpoint_is_the_antimeridian() {
        let frame = GeographicFrame::crs84();
        let source = line(&[(170.0, 0.0), (-170.0, 0.0)]);
        let segments = geographic_sample_segments(&source, frame).unwrap();

        assert_eq!(segments.len(), 1);
        let midpoint = segments[0].sample(0.5, frame).unwrap();
        assert_eq!(midpoint.x.abs(), 180.0);
        assert_eq!(midpoint.y, 0.0);
    }

    #[test]
    fn periodic_alias_edges_sample_only_their_quotient_vertices() {
        let frame = GeographicFrame::crs84();
        for (coordinates, aliases) in [
            ([(-180.0, 0.0), (180.0, 0.0)], [-180.0, 180.0]),
            ([(0.0, 0.0), (720.0, 0.0)], [0.0, 720.0]),
        ] {
            let source = line(&coordinates);
            let points = source
                .sample_points_in_domain(32, 5, TopologyDomain::Geographic(frame))
                .unwrap();
            assert_eq!(points.len(), 32);
            assert!(
                points
                    .iter()
                    .all(|point| { point.y == 0.0 && aliases.contains(&point.x) }),
                "input={coordinates:?}, samples={points:?}"
            );
        }
    }

    #[test]
    fn zero_geographic_sample_count_skips_support_admission() {
        let invalid = line(&[(0.0, 100.0), (1.0, 101.0)]);
        let domain = TopologyDomain::Geographic(GeographicFrame::crs84());

        assert!(
            invalid
                .sample_points_in_domain(0, 1, domain)
                .unwrap()
                .is_empty()
        );
        invalid.sample_points_in_domain(1, 1, domain).unwrap_err();
    }

    #[test]
    fn retained_line_weight_and_seeded_draw_use_the_twenty_degree_edge() {
        let frame = GeographicFrame::crs84();
        let lines = Shape::MultiLineString(
            MultiLineString::try_new(CoordinateAxes::XY, vec![
                MultiMember::Value(line_seq(&[(170.0, 0.0), (-170.0, 0.0)])),
                MultiMember::Value(line_seq(&[(0.0, 0.0), (100.0, 0.0)])),
            ])
            .unwrap(),
        );
        let segments = geographic_sample_segments(&lines, frame).unwrap();

        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].delta_x, 20.0);
        assert_eq!(segments[1].delta_x, 100.0);
        assert_eq!(segments[1].delta_x / segments[0].delta_x, 5.0);

        let sample = lines
            .sample_points_in_domain(1, row_sample_seed(1, 0), TopologyDomain::Geographic(frame))
            .unwrap();
        assert_eq!(sample.len(), 1);
        assert!((sample[0].x - 94.356_423_086_485_44).abs() <= 1.0e-12);
        assert_eq!(sample[0].y, 0.0);
    }

    #[test]
    fn duplicate_crossing_polygons_keep_their_area_weight_multiplicity() {
        let frame = GeographicFrame::crs84();
        let crossing = polygon(&[
            (170.0, 0.0),
            (-170.0, 0.0),
            (-170.0, 10.0),
            (170.0, 10.0),
            (170.0, 0.0),
        ]);
        let ordinary = polygon(&[
            (0.0, 0.0),
            (20.0, 0.0),
            (20.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]);
        let source = Shape::MultiPolygon(
            MultiPolygon::try_new(CoordinateAxes::XY, vec![
                MultiMember::Value(crossing.clone()),
                MultiMember::Value(crossing),
                MultiMember::Value(ordinary),
            ])
            .unwrap(),
        );
        let sample = source
            .sample_points_in_domain(1, 1, TopologyDomain::Geographic(frame))
            .unwrap();

        assert_eq!(sample.len(), 1);
        assert!(sample[0].x.abs() > 170.0, "sample was {sample:?}");
        assert!((0.0..=10.0).contains(&sample[0].y));
    }

    #[test]
    fn raw_compatible_geographic_and_cartesian_samples_keep_the_base_bits() {
        let ordinary_geographic = [
            line(&[(190.0, 10.0), (200.0, 20.0)]),
            Shape::Polygon(polygon(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ])),
        ];
        let frame = GeographicFrame::crs84();
        for source in ordinary_geographic {
            let base = source.sample_points(5, 7).unwrap();
            let geographic = source
                .sample_points_in_domain(5, 7, TopologyDomain::Geographic(frame))
                .unwrap();
            assert_eq!(xy_bits(&geographic), xy_bits(&base));
        }

        let cartesian = line(&[(0.0, 0.0), (1.0, 1.0)]);
        assert_eq!(
            xy_bits(
                &cartesian
                    .sample_points_in_domain(5, 7, TopologyDomain::Cartesian)
                    .unwrap()
            ),
            xy_bits(&cartesian.sample_points(5, 7).unwrap())
        );
    }
}
