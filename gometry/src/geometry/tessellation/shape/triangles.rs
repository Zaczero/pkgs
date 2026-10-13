use super::{
    CdtRefinement, CoordSeq, CoordinateAxes, ExpansionBudget, GeometryErrorKind, Point,
    PointSelectionChart, Polygon, Polygonal, Result, Ring, RingOwner, Shape, TopologyDomain,
    earcut_polygon_with, try_triangle_shape,
};

/// The ear-cut emitter owns the admission-before-allocation invariant: every
/// closed triangle ring is charged before its `Shape` is built.
pub(in crate::geometry::tessellation) struct BudgetedTriangleSink<'budget> {
    budget: &'budget mut ExpansionBudget,
    shapes: Vec<Shape>,
}

impl<'budget> BudgetedTriangleSink<'budget> {
    pub(in crate::geometry::tessellation) const fn new(
        budget: &'budget mut ExpansionBudget,
    ) -> Self {
        Self {
            budget,
            shapes: Vec::new(),
        }
    }

    pub(in crate::geometry::tessellation) fn emit(
        &mut self,
        a: Point,
        b: Point,
        c: Point,
    ) -> Result<()> {
        self.budget.check_additional(4)?;
        let shape = try_triangle_shape(a, b, c)?;
        self.shapes.push(shape);
        self.budget.add(4).map(|_| ())
    }

    pub(in crate::geometry::tessellation) fn into_shapes(self) -> Vec<Shape> {
        self.shapes
    }
}

/// Packed `[a, b, c, a]` emitter: charge four vertices, then grow the stream.
pub(in crate::geometry::tessellation) struct BudgetedPackedTriangleSink<'budget> {
    budget: &'budget mut ExpansionBudget,
    vertices: Vec<Point>,
}

impl<'budget> BudgetedPackedTriangleSink<'budget> {
    pub(in crate::geometry::tessellation) const fn new(
        budget: &'budget mut ExpansionBudget,
    ) -> Self {
        Self {
            budget,
            vertices: Vec::new(),
        }
    }

    pub(in crate::geometry::tessellation) fn emit(
        &mut self,
        a: Point,
        b: Point,
        c: Point,
    ) -> Result<()> {
        self.budget.check_additional(4)?;
        self.vertices.reserve(4);
        self.vertices.extend_from_slice(&[a, b, c, a]);
        self.budget.add(4).map(|_| ())
    }

    pub(in crate::geometry::tessellation) fn into_vertices(self) -> Vec<Point> {
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
    ) -> Result<super::super::PreparedMeshPublication> {
        let mut budget = crate::geometry::ExpansionBudget::new("triangulate", "method");
        self.delaunay_mesh_publication_in_domain_budgeted(domain, &mut budget)
    }

    pub(crate) fn delaunay_mesh_publication_in_domain_budgeted(
        &self,
        domain: crate::geometry::TopologyDomain,
        budget: &mut crate::geometry::ExpansionBudget,
    ) -> Result<super::super::PreparedMeshPublication> {
        let source_points = self.first_seen_xy_vertices()?;
        let publication = match domain {
            crate::geometry::TopologyDomain::Cartesian => {
                super::super::delaunay::prepare_raw_delaunay_publication(&source_points, budget)?
            },
            crate::geometry::TopologyDomain::Geographic(frame) => {
                super::super::delaunay::prepare_retained_delaunay_publication(
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
        super::super::delaunay::publish_constrained_plan(&plan, budget)
    }

    pub(crate) fn prepare_constrained_delaunay_plan_in_domain(
        &self,
        refinement: CdtRefinement,
        domain: TopologyDomain,
    ) -> Result<Option<super::super::delaunay::ConstrainedMeshPlan>> {
        let polygonal = self.polygonal().ok_or(GeometryErrorKind::PolygonRequired)?;
        let chart = match domain {
            TopologyDomain::Cartesian => None,
            TopologyDomain::Geographic(frame) => {
                PointSelectionChart::try_from_shape(frame, self, self.bounds())?
            },
        };
        match polygonal {
            Polygonal::Polygon(polygon) => Ok(Some(
                super::super::delaunay::prepare_constrained_triangle_plan(
                    polygon
                        .rings()
                        .enumerate()
                        .map(|(ring, value)| (RingOwner::new(0, ring), value)),
                    self.area(),
                    refinement,
                    domain,
                    chart.as_ref(),
                )?,
            )),
            Polygonal::MultiPolygon(polygons) => Ok(Some(
                super::super::delaunay::prepare_constrained_triangle_plan(
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
                )?,
            )),
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

    pub(crate) fn earcut_mesh_publication_budgeted(
        &self,
        budget: &mut crate::geometry::ExpansionBudget,
    ) -> Result<super::super::PreparedMeshPublication> {
        super::super::mesh_publication::prepare_earcut_publication(self, budget)
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
}
