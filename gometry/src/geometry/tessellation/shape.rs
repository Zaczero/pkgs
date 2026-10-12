use std::borrow::Borrow;

use super::delaunay::certified_delaunay_for_retained_points;
use crate::NonNegative;
use crate::error::Result;
use crate::geographic::{ChartPointSite, GeographicFrame, PointSelectionChart};
use crate::geometry::overlay::ExactSourceInventory;
use crate::geometry::tessellation::{
    CdtRefinement, CertifiedDelaunay, GeographicSampleSegment, PreparedSampling, RingOwner,
    SampleOccurrenceId, SampleRecipe, SampleSupport, Site, certified_delaunay,
    collect_sample_atoms, collect_sample_triangles, earcut_polygon_with, sample_weighted,
    snap_sites, try_triangle_shape, uniform_f64, voronoi_dcel, weight_scale,
};
use crate::geometry::{
    AxisFrame, CoordSeq, CoordinateAxes, Coordinates as _, CsrOffsetColumn, Dimension,
    ExpansionBudget, GeometryErrorKind, MultiMember, MultiPolygon, Point, PointKey, Polygon,
    Polygonal, PolygonizeFull, Ring, Segment, Shape, ShapeCountQuery, SourceArcContext,
    TopologyDomain, VoronoiBoundary, axis_extent, line_segments,
};

/// The ear-cut emitter owns the admission-before-allocation invariant: every
/// closed triangle ring is charged before its `Shape` is built.
pub(super) struct BudgetedTriangleSink<'budget> {
    budget: &'budget mut ExpansionBudget,
    shapes: Vec<Shape>,
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
) -> Result<Vec<SampleSupport>> {
    let mut segments = Vec::with_capacity(ShapeCountQuery::default().segments(shape)?);
    let mut component = 0_usize;
    shape.try_for_each_segment_chain(|chain| -> Result<()> {
        let mut points = chain.iter_coords();
        let Some(mut start) = points.next() else {
            return Ok(());
        };
        for (occurrence, end) in points.enumerate() {
            let connection = frame
                .longitude()
                .chart_connection(start.x, end.x)
                .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))?;
            let longitude = connection.retained();
            let raw_delta = end.x - start.x;
            let retained_delta = longitude.delta_native(frame.longitude());
            let raw_compatible = raw_delta_matches_retained(raw_delta, retained_delta);
            segments.push(SampleSupport::GeographicSegment {
                segment: GeographicSampleSegment {
                    occurrence: SampleOccurrenceId {
                        component,
                        occurrence,
                    },
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
                },
            });
            start = end;
        }
        component = component
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
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

fn sample_triangle_vertices(support: &SampleSupport) -> &[Point; 3] {
    let SampleSupport::AreaTriangle { vertices, .. } = support else {
        unreachable!("surface support contains only Earcut triangles")
    };
    vertices
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
            shapes.push(Self::triangle_shape_from_ring(ring)?);
        }
        Ok(shapes)
    }

    pub(crate) fn triangle_shapes_from_published_rings(
        vertices: &[Point],
        ring_ends: &[usize],
    ) -> Result<Vec<Self>> {
        let mut shapes = Vec::with_capacity(ring_ends.len());
        let mut start = 0;
        for &end in ring_ends {
            if end > vertices.len() || end.checked_sub(start).is_none_or(|len| len < 4) {
                return Err(GeometryErrorKind::Invalid(
                    "triangle ring offsets do not match the vertex stream".into(),
                )
                .into());
            }
            let ring = vertices.get(start..end).ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "triangle ring offsets do not match the vertex stream".into(),
                )
            })?;
            shapes.push(Self::triangle_shape_from_ring(ring)?);
            start = end;
        }
        if start != vertices.len() {
            return Err(GeometryErrorKind::Invalid(
                "triangle ring offsets do not match the vertex stream".into(),
            )
            .into());
        }
        Ok(shapes)
    }

    fn triangle_shape_from_ring(vertices: &[Point]) -> Result<Self> {
        Ok(Self::Polygon(Polygon::new(
            Ring::try_from_closed(CoordSeq::try_from_points(vertices)?)?,
            Vec::new(),
        )))
    }

    pub(crate) fn delaunay_triangle_vertices_budgeted(
        &self,
        budget: &mut crate::geometry::ExpansionBudget,
    ) -> Result<Vec<Point>> {
        Ok(self
            .delaunay_mesh_publication_in_domain_budgeted(
                crate::geometry::TopologyDomain::Cartesian,
                budget,
            )?
            .into_points())
    }

    #[cfg(test)]
    pub(crate) fn delaunay_mesh_publication_in_domain(
        &self,
        domain: crate::geometry::TopologyDomain,
    ) -> Result<super::PreparedMeshPublication> {
        let mut budget = crate::geometry::ExpansionBudget::new("triangulate", "method");
        self.delaunay_mesh_publication_in_domain_budgeted(domain, &mut budget)
    }

    pub(crate) fn delaunay_mesh_publication_in_domain_budgeted(
        &self,
        domain: crate::geometry::TopologyDomain,
        budget: &mut crate::geometry::ExpansionBudget,
    ) -> Result<super::PreparedMeshPublication> {
        let source_points = self.first_seen_xy_vertices()?;
        let publication = match domain {
            crate::geometry::TopologyDomain::Cartesian => {
                super::delaunay::prepare_raw_delaunay_publication(&source_points, budget)?
            },
            crate::geometry::TopologyDomain::Geographic(frame) => {
                super::delaunay::prepare_retained_delaunay_publication(
                    frame,
                    &source_points,
                    budget,
                )?
            },
        };
        Ok(publication)
    }

    pub(crate) fn constrained_delaunay_triangles_in_domain(
        &self,
        refinement: CdtRefinement,
        domain: TopologyDomain,
    ) -> Result<Vec<Self>> {
        let mut budget = crate::geometry::ExpansionBudget::new("triangulate", "min_angle/max_area");
        self.constrained_delaunay_triangles_in_domain_budgeted(refinement, domain, &mut budget)
    }

    pub(crate) fn constrained_delaunay_triangles_in_domain_budgeted(
        &self,
        refinement: CdtRefinement,
        domain: TopologyDomain,
        budget: &mut crate::geometry::ExpansionBudget,
    ) -> Result<Vec<Self>> {
        let vertices =
            self.constrained_delaunay_vertices_in_domain_budgeted(refinement, domain, budget)?;
        Self::triangle_shapes_from_vertices(&vertices)
    }

    /// Constrained-Delaunay interior as a flat `[a, b, c, a]`-per-triangle
    /// vertex stream. Each ring is emitted from the shared mesh plan: inactive
    /// refinement preserves selected source-site axes, while active refinement
    /// emits uniform XY.
    #[cfg(test)]
    pub(crate) fn constrained_delaunay_vertices(
        &self,
        refinement: CdtRefinement,
    ) -> Result<Vec<Point>> {
        let mut budget = crate::geometry::ExpansionBudget::new("triangulate", "min_angle/max_area");
        self.constrained_delaunay_vertices_budgeted(refinement, &mut budget)
    }

    pub(crate) fn constrained_delaunay_vertices_in_domain(
        &self,
        refinement: CdtRefinement,
        domain: TopologyDomain,
    ) -> Result<Vec<Point>> {
        let mut budget = crate::geometry::ExpansionBudget::new("triangulate", "min_angle/max_area");
        self.constrained_delaunay_vertices_in_domain_budgeted(refinement, domain, &mut budget)
    }

    #[cfg(test)]
    pub(crate) fn constrained_delaunay_vertices_budgeted(
        &self,
        refinement: CdtRefinement,
        budget: &mut crate::geometry::ExpansionBudget,
    ) -> Result<Vec<Point>> {
        self.constrained_delaunay_vertices_in_domain_budgeted(
            refinement,
            TopologyDomain::Cartesian,
            budget,
        )
    }

    pub(crate) fn constrained_delaunay_vertices_in_domain_budgeted(
        &self,
        refinement: CdtRefinement,
        domain: TopologyDomain,
        budget: &mut crate::geometry::ExpansionBudget,
    ) -> Result<Vec<Point>> {
        let Some(plan) = self.prepare_constrained_delaunay_plan_in_domain(refinement, domain)?
        else {
            return Ok(Vec::new());
        };
        super::delaunay::publish_constrained_plan(&plan, budget)
    }

    pub(crate) fn prepare_constrained_delaunay_plan_in_domain(
        &self,
        refinement: CdtRefinement,
        domain: TopologyDomain,
    ) -> Result<Option<super::delaunay::ConstrainedMeshPlan>> {
        let polygonal = self.polygonal().ok_or(GeometryErrorKind::PolygonRequired)?;
        let chart = match domain {
            TopologyDomain::Cartesian => None,
            TopologyDomain::Geographic(frame) => {
                PointSelectionChart::try_from_shape(frame, self, self.bounds())?
            },
        };
        match polygonal {
            Polygonal::Polygon(polygon) => {
                Ok(Some(super::delaunay::prepare_constrained_triangle_plan(
                    polygon
                        .rings()
                        .enumerate()
                        .map(|(ring, value)| (RingOwner::new(0, ring), value)),
                    self.area(),
                    refinement,
                    domain,
                    chart.as_ref(),
                )?))
            },
            Polygonal::MultiPolygon(polygons) => {
                Ok(Some(super::delaunay::prepare_constrained_triangle_plan(
                    polygons.iter().enumerate().flat_map(|(polygon, value)| {
                        value
                            .rings()
                            .enumerate()
                            .map(move |(ring, value)| (RingOwner::new(polygon, ring), value))
                    }),
                    self.area(),
                    refinement,
                    domain,
                    chart.as_ref(),
                )?))
            },
            Polygonal::Empty => Ok(None),
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
        self.sample_point_receipts_from_support(
            self,
            count,
            seed,
            0,
            TopologyDomain::Cartesian,
            None,
        )
        .map(PreparedSampling::into_points)
    }

    #[cfg(test)]
    pub(crate) fn sample_points_in_domain(
        &self,
        count: usize,
        seed: u64,
        domain: TopologyDomain,
    ) -> Result<Vec<Point>> {
        self.sample_point_receipts_in_domain(count, seed, 0, domain)
            .map(PreparedSampling::into_points)
    }

    pub(crate) fn sample_point_receipts_in_domain(
        &self,
        count: usize,
        seed: u64,
        logical_row: usize,
        domain: TopologyDomain,
    ) -> Result<PreparedSampling> {
        ExpansionBudget::check("sample_points", "count", count)?;
        if count == 0 {
            return Ok(PreparedSampling::empty(logical_row, seed, domain));
        }
        let TopologyDomain::Geographic(frame) = domain else {
            return self.sample_point_receipts_from_support(
                self,
                count,
                seed,
                logical_row,
                domain,
                None,
            );
        };
        if self.topological_dimension() < Dimension::Curve {
            return self.sample_point_receipts_from_support(
                self,
                count,
                seed,
                logical_row,
                domain,
                None,
            );
        }
        crate::geographic::ScalarGeographicView::try_new(frame, self)
            .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))?;
        if !geographic_sample_requires_preparation(self, frame)? {
            return self.sample_point_receipts_from_support(
                self,
                count,
                seed,
                logical_row,
                domain,
                None,
            );
        }
        let area_support = geographic_areal_sample_support(self, frame)?;
        let segments = geographic_sample_segments(self, frame)?;
        area_support.sample_point_receipts_from_support(
            self,
            count,
            seed,
            logical_row,
            domain,
            Some(segments),
        )
    }

    fn sample_point_receipts_from_support(
        &self,
        point_source: &Self,
        count: usize,
        seed: u64,
        logical_row: usize,
        domain: TopologyDomain,
        mut geographic_segments: Option<Vec<SampleSupport>>,
    ) -> Result<PreparedSampling> {
        if self.topological_dimension() >= Dimension::Surface {
            let mut supports = Vec::new();
            collect_sample_triangles(self, &mut supports)?;
            // Every triangle uses one shared *per-axis* frame. Relative areas
            // retain the common `sx*sy` factor, while reciprocal spans remain
            // nonzero instead of collapsing to the lower-dimensional fallback.
            let frame = supports.first().and_then(|support| {
                let origin = *sample_triangle_vertices(support)
                    .first()
                    .expect("an area triangle has a first vertex");
                let extent_x = axis_extent(
                    supports.iter().flat_map(|support| {
                        sample_triangle_vertices(support)
                            .iter()
                            .map(|point| point.x)
                    }),
                    origin.x,
                )?;
                let extent_y = axis_extent(
                    supports.iter().flat_map(|support| {
                        sample_triangle_vertices(support)
                            .iter()
                            .map(|point| point.y)
                    }),
                    origin.y,
                )?;
                AxisFrame::from_origin_extents(origin, extent_x, extent_y)
            });
            let placed = sample_weighted(
                count,
                seed,
                &supports,
                |support| {
                    let Some(frame) = frame else {
                        return 0.0;
                    };
                    let [a, b, c] = sample_triangle_vertices(support);
                    let a = frame.frame_point(*a);
                    let b = frame.frame_point(*b);
                    let c = frame.frame_point(*c);
                    ((b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x)).abs()
                },
                |support, state| {
                    let [base, mid, apex] = sample_triangle_vertices(support);
                    // P = (1 - s) A + s (1 - t) B + s t C with s = sqrt(u):
                    // uniform over the triangle.
                    let spread = uniform_f64(state).sqrt();
                    let blend = uniform_f64(state);
                    let x =
                        (1.0 - spread) * base.x + spread * ((1.0 - blend) * mid.x + blend * apex.x);
                    let y =
                        (1.0 - spread) * base.y + spread * ((1.0 - blend) * mid.y + blend * apex.y);
                    Ok((Point::new(x, y)?, SampleRecipe::Triangle { spread, blend }))
                },
            )?;
            if let Some(draws) = placed {
                return Ok(PreparedSampling::from_draws(
                    logical_row,
                    seed,
                    domain,
                    supports,
                    draws,
                ));
            }
        }
        if self.topological_dimension() >= Dimension::Curve {
            let placed = if let Some(supports) = geographic_segments.take() {
                let TopologyDomain::Geographic(frame) = domain else {
                    unreachable!("retained sample segments require a geographic domain")
                };
                let scale = weight_scale(supports.iter().fold(0.0_f64, |largest, support| {
                    let SampleSupport::GeographicSegment { segment } = support else {
                        unreachable!("geographic support contains retained segments")
                    };
                    [segment.start, segment.end]
                        .into_iter()
                        .flat_map(|point| [point.x.abs(), point.y.abs()])
                        .fold(largest, f64::max)
                }));
                sample_weighted(
                    count,
                    seed,
                    &supports,
                    |support| {
                        let SampleSupport::GeographicSegment { segment } = support else {
                            unreachable!("geographic support contains retained segments")
                        };
                        let dx = segment.delta_x * scale;
                        let dy = segment.delta_y * scale;
                        (dx * dx + dy * dy).sqrt()
                    },
                    |support, state| {
                        let SampleSupport::GeographicSegment { segment } = support else {
                            unreachable!("geographic support contains retained segments")
                        };
                        let along = uniform_f64(state);
                        Ok((segment.sample(along, frame)?, SampleRecipe::Segment {
                            along,
                        }))
                    },
                )?
                .map(|draws| (supports, draws))
            } else {
                let segment_count = ShapeCountQuery::default().segments(self)?;
                let mut segments: Vec<SampleSupport> = Vec::with_capacity(segment_count);
                let mut component = 0_usize;
                self.try_for_each_segment_chain(|chain| -> Result<()> {
                    for (occurrence, segment) in line_segments(chain).enumerate() {
                        segments.push(SampleSupport::Segment {
                            occurrence: SampleOccurrenceId {
                                component,
                                occurrence,
                            },
                            segment,
                        });
                    }
                    component = component
                        .checked_add(1)
                        .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    Ok(())
                })?;
                let scale = weight_scale(
                    segments
                        .iter()
                        .flat_map(|support| {
                            let SampleSupport::Segment { segment, .. } = support else {
                                unreachable!("Cartesian support contains line segments")
                            };
                            [
                                segment.start.x.abs(),
                                segment.start.y.abs(),
                                segment.end.x.abs(),
                                segment.end.y.abs(),
                            ]
                        })
                        .fold(0.0_f64, f64::max),
                );
                sample_weighted(
                    count,
                    seed,
                    &segments,
                    |support| {
                        let SampleSupport::Segment { segment, .. } = support else {
                            unreachable!("Cartesian support contains line segments")
                        };
                        let dx = (segment.end.x - segment.start.x) * scale;
                        let dy = (segment.end.y - segment.start.y) * scale;
                        (dx * dx + dy * dy).sqrt()
                    },
                    |support, state| {
                        let SampleSupport::Segment { segment, .. } = support else {
                            unreachable!("Cartesian support contains line segments")
                        };
                        let along = uniform_f64(state);
                        let x = (1.0 - along) * segment.start.x + along * segment.end.x;
                        let y = (1.0 - along) * segment.start.y + along * segment.end.y;
                        Ok((Point::new(x, y)?, SampleRecipe::Segment { along }))
                    },
                )?
                .map(|draws| (segments, draws))
            };
            if let Some((supports, draws)) = placed {
                return Ok(PreparedSampling::from_draws(
                    logical_row,
                    seed,
                    domain,
                    supports,
                    draws,
                ));
            }
        }
        let atom_count = ShapeCountQuery::default().coordinates(point_source)?;
        let mut supports = Vec::with_capacity(atom_count);
        collect_sample_atoms(point_source, &mut supports)?;
        let draws = sample_weighted(
            count,
            seed,
            &supports,
            |_| 1.0,
            |support, _| {
                let SampleSupport::PointAtom { point, .. } = support else {
                    unreachable!("atomic support contains points")
                };
                Ok((Point::new(point.x, point.y)?, SampleRecipe::Atom))
            },
        )?;
        Ok(draws.map_or_else(
            || PreparedSampling::empty(logical_row, seed, domain),
            |draws| PreparedSampling::from_draws(logical_row, seed, domain, supports, draws),
        ))
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
                    voronoi_dcel::lone_site_polygon_outputs(&canonical, polygon, chart.as_ref())
                },
                (1, _) => voronoi_dcel::lone_site_empty_polygon_outputs(&canonical),
                _ => voronoi_dcel::empty_polygon_outputs(&canonical),
            };
        };
        let embedded = if mesh.collinear_order().is_some() {
            voronoi_dcel::build_collinear_polygons_with_chart(
                &canonical,
                boundary,
                budget,
                &mesh,
                chart.as_ref(),
            )?
        } else {
            voronoi_dcel::build_product_with_chart(
                &canonical,
                boundary,
                budget,
                true,
                &mesh,
                voronoi_dcel::VoronoiProduct::Polygons,
                chart.as_ref(),
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
                voronoi_dcel::lone_site_empty_edges(&canonical, boundary, chart.as_ref())
            } else {
                voronoi_dcel::empty_packed_edges()
            };
        };
        if mesh.collinear_order().is_none() {
            return voronoi_dcel::build_edges_with_chart(
                &canonical,
                boundary,
                budget,
                true,
                &mesh,
                chart.as_ref(),
            );
        }
        voronoi_dcel::build_collinear_edges_with_chart(
            &canonical,
            boundary,
            budget,
            &mesh,
            chart.as_ref(),
        )
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
pub(super) fn canonical_voronoi_sites(points: Vec<Point>, tolerance: f64) -> Result<Vec<Site>> {
    let mut rows = points.into_iter().enumerate().collect::<Vec<_>>();
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
    use crate::geometry::tessellation::{
        GeographicSampleSegment, PreparedSampling, SampleRecipe, SampleSupport, sample_weighted,
    };
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

    fn retained_segment(support: &SampleSupport) -> GeographicSampleSegment {
        let SampleSupport::GeographicSegment { segment } = support else {
            panic!("retained geographic segment support")
        };
        *segment
    }

    #[test]
    fn retained_line_midpoint_is_the_antimeridian() {
        let frame = GeographicFrame::crs84();
        let source = line(&[(170.0, 0.0), (-170.0, 0.0)]);
        let segments = geographic_sample_segments(&source, frame).unwrap();

        assert_eq!(segments.len(), 1);
        let midpoint = retained_segment(segments.first().expect("one retained segment"))
            .sample(0.5, frame)
            .unwrap();
        assert_eq!(midpoint.x.abs(), 180.0);
        assert_eq!(midpoint.y, 0.0);

        let support = segments;
        let draws = sample_weighted(
            1,
            1,
            &support,
            |_| 1.0,
            |support, _| {
                let segment = retained_segment(support);
                let along = 0.5;
                Ok((segment.sample(along, frame)?, SampleRecipe::Segment {
                    along,
                }))
            },
        )
        .unwrap()
        .unwrap();
        let product =
            PreparedSampling::from_draws(4, 1, TopologyDomain::Geographic(frame), support, draws);
        assert_eq!(product.logical_row, 4);
        assert_eq!(product.resolved_seed, 1);
        assert_eq!(product.domain, TopologyDomain::Geographic(frame));
        let [draw] = product.draws.as_slice() else {
            panic!("one midpoint draw")
        };
        assert_eq!(draw.draw_ordinal, 0);
        assert_eq!(draw.selected_element, 0);
        let segment = retained_segment(
            product
                .support_elements
                .get(draw.selected_element)
                .expect("draw selects a prepared support element"),
        );
        assert_eq!(segment.occurrence.component, 0);
        assert_eq!(segment.occurrence.occurrence, 0);
        assert_eq!(segment.longitude.delta_native(frame.longitude()), 20.0);
        assert_eq!(draw.recipe, SampleRecipe::Segment { along: 0.5 });
        assert_eq!(draw.point.x.abs(), 180.0);
        assert_eq!(draw.point.y, 0.0);
    }

    #[test]
    fn line_receipt_keeps_selected_segment_and_along_parameter() {
        let source = line(&[(0.0, 0.0), (1.0, 0.0), (2.0, 0.0)]);
        let product = source
            .sample_point_receipts_in_domain(1, 1, 12, TopologyDomain::Cartesian)
            .unwrap();
        let [draw] = product.draws.as_slice() else {
            panic!("one line sample")
        };

        assert_eq!(product.logical_row, 12);
        assert_eq!(product.resolved_seed, 1);
        assert_eq!(draw.draw_ordinal, 0);
        assert_eq!(draw.selected_element, 1);
        let SampleSupport::Segment {
            occurrence,
            segment,
        } = *product
            .support_elements
            .get(draw.selected_element)
            .expect("draw selects a prepared support element")
        else {
            panic!("Cartesian segment receipt")
        };
        assert_eq!(occurrence.component, 0);
        assert_eq!(occurrence.occurrence, 1);
        assert_eq!((segment.start.x, segment.start.y), (1.0, 0.0));
        assert_eq!((segment.end.x, segment.end.y), (2.0, 0.0));
        assert_eq!(draw.recipe, SampleRecipe::Segment {
            along: 0.745_781_757_262_701_1,
        });
        assert_eq!(draw.point.x, 1.745_781_757_262_701);
        assert_eq!(draw.point.y, 0.0);
    }

    #[test]
    fn coincident_atom_draws_keep_each_selected_occurrence() {
        let point = Point::new(3.0, 4.0).unwrap();
        let source = Shape::multi_point(
            CoordinateAxes::XY,
            CoordSeq::try_from_points(&[point, point, point]).unwrap(),
        );
        let product = source
            .sample_point_receipts_in_domain(2, 1, 5, TopologyDomain::Cartesian)
            .unwrap();

        assert_eq!(product.logical_row, 5);
        assert_eq!(product.resolved_seed, 1);
        assert_eq!(product.support_elements.len(), 3);
        assert_eq!(product.draws.len(), 2);
        for (draw_ordinal, draw) in product.draws.iter().enumerate() {
            assert_eq!(draw.draw_ordinal, draw_ordinal);
            assert_eq!(draw.point, point);
            assert_eq!(draw.recipe, SampleRecipe::Atom);
            let SampleSupport::PointAtom {
                occurrence,
                point: support,
            } = *product
                .support_elements
                .get(draw.selected_element)
                .expect("draw selects a prepared support element")
            else {
                panic!("point atom receipt")
            };
            assert_eq!(occurrence.component, 0);
            assert_eq!(occurrence.occurrence, draw.selected_element);
            assert_eq!(support, point);
        }
        assert_eq!(
            product
                .draws
                .iter()
                .map(|draw| draw.selected_element)
                .collect::<Vec<_>>(),
            [1, 2]
        );
    }

    #[test]
    fn area_receipt_keeps_the_selected_earcut_triangle_and_parameters() {
        let source_vertices = [
            Point::new(0.0, 0.0).unwrap(),
            Point::new(10.0, 0.0).unwrap(),
            Point::new(10.0, 10.0).unwrap(),
            Point::new(0.0, 10.0).unwrap(),
        ];
        let source = Shape::Polygon(polygon(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
        ]));
        let product = source
            .sample_point_receipts_in_domain(1, 1, 9, TopologyDomain::Cartesian)
            .unwrap();

        assert_eq!(product.logical_row, 9);
        assert_eq!(product.resolved_seed, 1);
        assert_eq!(product.support_elements.len(), 2);
        let [draw] = product.draws.as_slice() else {
            panic!("one weighted area draw")
        };
        assert_eq!(draw.draw_ordinal, 0);
        assert_eq!(draw.selected_element, 1);
        let SampleSupport::AreaTriangle {
            occurrence,
            vertices,
            corners,
        } = *product
            .support_elements
            .get(draw.selected_element)
            .expect("draw selects a prepared support element")
        else {
            panic!("area triangle receipt")
        };
        assert_eq!(occurrence.component, 0);
        assert_eq!(occurrence.occurrence, draw.selected_element);
        for (point, corner) in vertices.iter().zip(corners) {
            assert_eq!(corner.site.ring, 0);
            assert_eq!(
                *source_vertices
                    .get(corner.site.vertex)
                    .expect("Earcut site ID points to a source corner"),
                *point
            );
        }
        assert!(
            corners
                .iter()
                .all(|corner| corner.occurrence.0 < source_vertices.len())
        );
        assert_eq!(draw.recipe, SampleRecipe::Triangle {
            spread: 0.863_586_566_166_184_1,
            blend: 0.971_002_753_586_796_2,
        });
        assert!(draw.point.x.is_finite() && draw.point.y.is_finite());
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

        let [first, second] = segments.as_slice() else {
            panic!("two retained source segments")
        };
        let first = retained_segment(first);
        let second = retained_segment(second);
        assert_eq!(first.delta_x, 20.0);
        assert_eq!(second.delta_x, 100.0);
        assert_eq!(second.delta_x / first.delta_x, 5.0);

        let resolved_seed = row_sample_seed(1, 0);
        let product = lines
            .sample_point_receipts_in_domain(1, resolved_seed, 0, TopologyDomain::Geographic(frame))
            .unwrap();
        assert_eq!(product.logical_row, 0);
        assert_eq!(product.resolved_seed, resolved_seed);
        let [draw] = product.draws.as_slice() else {
            panic!("one retained-line draw")
        };
        assert_eq!(draw.selected_element, 1);
        let segment = retained_segment(
            product
                .support_elements
                .get(draw.selected_element)
                .expect("draw selects a prepared support element"),
        );
        assert_eq!(segment.occurrence.component, 1);
        assert_eq!(segment.occurrence.occurrence, 0);
        assert_eq!(segment.delta_x, 100.0);
        assert_eq!(draw.recipe, SampleRecipe::Segment {
            along: 0.943_564_230_864_854_4,
        });
        assert!((draw.point.x - 94.356_423_086_485_44).abs() <= 1.0e-12);
        assert_eq!(draw.point.y, 0.0);
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
