use super::{
    CoordSeq, CoordinateAxes, ExactLineSource, ExactPartition, ExactSourceInventory,
    ExpansionBudget, GeometryErrorKind, GeometryKind, LineSeq, NodeId, NodeParts, Point, Polygon,
    ResolvedNodePoints, ResourceError, Result, Ring, Segment, Shape, XY, embedding,
};

#[derive(Debug)]
pub(in crate::geometry) struct ExactLineEmission<'p> {
    pub(super) embedding: embedding::Embedding<'p>,
    pub(super) items: Vec<[NodeId; 2]>,
    pub(super) owners: Vec<ExactLineSource>,
    pub(super) offsets: Vec<u32>,
}

impl ExactLineEmission<'_> {
    pub(in crate::geometry) const fn len(&self) -> usize {
        self.items.len()
    }

    pub(in crate::geometry) fn segments_with_nodes(
        &self,
    ) -> impl ExactSizeIterator<Item = ([NodeId; 2], Segment)> + '_ {
        let coordinates = self.embedding.coordinates();
        self.items
            .iter()
            .copied()
            .map(move |nodes| (nodes, embedding::Embedding::segment_at(coordinates, nodes)))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the span ordinal is admitted by ExactPartition and offsets has one checked terminal entry"
    )]
    pub(in crate::geometry) fn sources(&self, span: usize) -> &[ExactLineSource] {
        let start = self.offsets[span] as usize;
        let end = self.offsets[span.wrapping_add(1)] as usize;
        &self.owners[start..end]
    }
}

impl ExactSourceInventory {
    pub(in crate::geometry) fn line_emission<'p>(
        &self,
        partition: &'p ExactPartition<'_>,
        operation: &'static str,
    ) -> Result<ExactLineEmission<'p>> {
        partition.emit_lines(&self.source_operands, operation)
    }

    pub(super) fn coordinates_from_nodes(
        &self,
        nodes: &[NodeId],
        by_node: &ResolvedNodePoints,
        axes: CoordinateAxes,
    ) -> Result<CoordSeq> {
        let mut xs = Vec::with_capacity(nodes.len());
        let mut ys = Vec::with_capacity(nodes.len());
        let mut zs = axes.has_z().then(|| Vec::with_capacity(nodes.len()));
        let mut ms = axes.has_m().then(|| Vec::with_capacity(nodes.len()));
        for &node in nodes {
            let point = by_node.point(
                node,
                embedding::Embedding::xy_at(by_node.coordinates(), node),
            );
            xs.push(point.x);
            ys.push(point.y);
            if let Some(zs) = &mut zs {
                zs.push(point.z().expect("retained Z has a resolved ordinate"));
            }
            if let Some(ms) = &mut ms {
                ms.push(point.m().expect("retained M has a resolved ordinate"));
            }
        }
        CoordSeq::try_from_vecs(xs, ys, zs, ms)
    }

    pub(super) fn node_lines_to_shape_resolved(
        &self,
        lines: Vec<Vec<NodeId>>,
        by_node: &ResolvedNodePoints,
        axes: CoordinateAxes,
    ) -> Result<Option<Shape>> {
        if lines.is_empty() {
            return Ok(None);
        }
        let mut output = Vec::with_capacity(lines.len());
        for line in lines {
            output.push(LineSeq::from_proven(
                self.coordinates_from_nodes(&line, by_node, axes)?,
            ));
        }
        Ok(Some(Shape::narrow_lines(axes, output)))
    }

    pub(super) fn boundary_node_parts_to_shape_resolved(
        &self,
        parts: NodeParts,
        by_node: &ResolvedNodePoints,
        declared_axes: CoordinateAxes,
        has_area: bool,
        has_curve_support: bool,
        has_point_support: bool,
    ) -> Result<Shape> {
        let point_axes = bucket_axes(
            parts.points.iter().copied(),
            by_node,
            declared_axes,
            false,
            "boundary",
        )?;
        let line_axes = bucket_axes(
            parts.lines.iter().flatten().copied(),
            by_node,
            declared_axes,
            false,
            "boundary",
        )?;
        let points = if parts.points.is_empty() {
            None
        } else {
            Some(Shape::multi_point(
                point_axes,
                self.coordinates_from_nodes(&parts.points, by_node, point_axes)?,
            ))
        };
        let lines = self.node_lines_to_shape_resolved(parts.lines, by_node, line_axes)?;

        if has_area {
            let lines = lines.unwrap_or_else(|| {
                Shape::typed_empty(GeometryKind::MultiLineString, declared_axes)
            });
            return match points {
                Some(points) => Shape::geometry_collection(vec![lines, points]),
                None => Ok(lines),
            };
        }
        if has_curve_support {
            return Ok(points
                .unwrap_or_else(|| Shape::typed_empty(GeometryKind::MultiPoint, declared_axes)));
        }
        if has_point_support {
            return Ok(Shape::typed_empty(
                GeometryKind::GeometryCollection,
                declared_axes,
            ));
        }
        Ok(Shape::typed_empty(
            GeometryKind::MultiLineString,
            declared_axes,
        ))
    }

    pub(super) fn node_parts_to_shape_resolved(
        &self,
        parts: NodeParts,
        embedding: &embedding::Embedding<'_>,
        by_node: &ResolvedNodePoints,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
        operation: &str,
        polygon_publication: Option<(
            crate::geometry::PolygonPartEdgeRoles,
            crate::geographic::GeographicFrame,
            &mut ExpansionBudget,
        )>,
    ) -> Result<Shape> {
        let mut polygon_publication = polygon_publication;
        if !declared_axes.has_z() && !declared_axes.has_m() {
            let xy = embedding.coordinates();
            let mut points = Vec::with_capacity(parts.points.len());
            for node in parts.points {
                let point = embedding::Embedding::xy_at(xy, node);
                points.push(Point::new_raw_xy(point.x, point.y));
            }
            let mut lines = Vec::with_capacity(parts.lines.len());
            for line in parts.lines {
                lines.push(xy_sequence_for_nodes(&line, xy)?);
            }
            let mut polygons = polygons_from_xy_nodes(parts.polygons, xy)?;
            if let Some((roles, frame, budget)) = polygon_publication.take() {
                polygons = crate::geometry::publish_polygon_parts_with_edge_roles(
                    polygons, &roles, frame, budget,
                )?;
            }
            return assemble_node_parts(
                points,
                lines,
                polygons,
                crate::geometry::CoordinateAxes::XY,
                crate::geometry::CoordinateAxes::XY,
                crate::geometry::CoordinateAxes::XY,
            );
        }

        let point_axes = bucket_axes(
            parts.points.iter().copied(),
            by_node,
            declared_axes,
            strict,
            operation,
        )?;
        let line_axes = bucket_axes(
            parts.lines.iter().flatten().copied(),
            by_node,
            declared_axes,
            strict,
            operation,
        )?;
        let mut polygon_axes = bucket_axes(
            parts
                .polygons
                .iter()
                .flat_map(|polygon| polygon.iter().flatten().copied()),
            by_node,
            declared_axes,
            strict,
            operation,
        )?;

        let xy = by_node.coordinates();
        let mut points = Vec::with_capacity(parts.points.len());
        for node in parts.points {
            points.push(with_axes(
                by_node.point(node, embedding::Embedding::xy_at(xy, node)),
                point_axes,
            ));
        }
        let mut lines = Vec::with_capacity(parts.lines.len());
        for line in parts.lines {
            lines.push(self.coordinates_from_nodes(&line, by_node, line_axes)?);
        }
        let mut polygons = Vec::with_capacity(parts.polygons.len());
        for polygon in parts.polygons {
            let mut rings = Vec::with_capacity(polygon.len());
            for ring in polygon {
                rings.push(Ring::try_from_closed(self.coordinates_from_nodes(
                    &ring,
                    by_node,
                    polygon_axes,
                )?)?);
            }
            let mut ring_iter = rings.into_iter();
            let shell = ring_iter.next().expect("polygon has a shell");
            let mut holes = Vec::with_capacity(ring_iter.size_hint().0);
            holes.extend(ring_iter);
            polygons.push(Polygon::new(shell, holes));
        }
        if let Some((roles, frame, budget)) = polygon_publication.take() {
            (polygons, polygon_axes) = publish_polygon_parts_with_roles(
                polygons,
                &roles,
                frame,
                budget,
                polygon_axes,
                strict,
                operation,
            )?;
        }
        assemble_node_parts(points, lines, polygons, point_axes, line_axes, polygon_axes)
    }
}

fn publish_polygon_parts_with_roles(
    polygons: Vec<Polygon>,
    roles: &[Vec<Vec<bool>>],
    frame: crate::geographic::GeographicFrame,
    budget: &mut ExpansionBudget,
    axes: crate::geometry::CoordinateAxes,
    strict: bool,
    operation: &str,
) -> Result<(Vec<Polygon>, crate::geometry::CoordinateAxes)> {
    let mut candidate_budget = budget.clone();
    match crate::geometry::publish_polygon_parts_with_edge_roles(
        polygons.clone(),
        roles,
        frame,
        &mut candidate_budget,
    ) {
        Ok(polygons) => {
            *budget = candidate_budget;
            Ok((polygons, axes))
        },
        Err(error)
            if matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(GeometryErrorKind::OrdinateDropped(_))
            ) =>
        {
            if strict {
                return Err(GeometryErrorKind::ordinate_dropped(operation));
            }
            let polygons = polygons
                .into_iter()
                .map(polygon_xy)
                .collect::<Result<Vec<_>>>()?;
            let mut fallback_budget = budget.clone();
            let polygons = crate::geometry::publish_polygon_parts_with_edge_roles(
                polygons,
                roles,
                frame,
                &mut fallback_budget,
            )?;
            *budget = fallback_budget;
            Ok((polygons, crate::geometry::CoordinateAxes::XY))
        },
        Err(error) => Err(error),
    }
}

fn polygon_xy(polygon: Polygon) -> Result<Polygon> {
    let mut rings = polygon.rings().map(|ring| {
        let coordinates = ring.coords().try_map_points(|point| {
            Ok::<_, crate::error::Error>(Point::new_raw_xy(point.x, point.y))
        })?;
        Ring::try_from_closed(coordinates)
    });
    let shell = rings
        .next()
        .ok_or_else(|| GeometryErrorKind::message("a polygon has an exterior ring"))??;
    Ok(Polygon::new(shell, rings.collect::<Result<Vec<_>>>()?))
}

pub(super) fn bucket_axes<I: IntoIterator<Item = NodeId>>(
    nodes: I,
    points: &ResolvedNodePoints,
    declared: crate::geometry::CoordinateAxes,
    strict: bool,
    operation: &str,
) -> Result<crate::geometry::CoordinateAxes> {
    if !declared.has_z() && !declared.has_m() {
        return Ok(crate::geometry::CoordinateAxes::XY);
    }
    let mut nodes = nodes.into_iter();
    let Some(first) = nodes.next() else {
        return Ok(crate::geometry::CoordinateAxes::XY);
    };
    let mut has_z = declared.has_z();
    let mut has_m = declared.has_m();
    for node in std::iter::once(first).chain(nodes) {
        let (z, m) = points.ordinates(node).ok_or_else(|| {
            GeometryErrorKind::Invalid("exact overlay output lost a live node".into())
        })?;
        has_z &= z.0.is_some();
        has_m &= m.0.is_some();
    }
    let axes = crate::geometry::CoordinateAxes::new(
        crate::geometry::HasZ(has_z),
        crate::geometry::HasM(has_m),
    );
    if strict && axes != declared {
        return Err(GeometryErrorKind::ordinate_dropped(operation));
    }
    Ok(axes)
}

pub(super) fn xy_sequence_for_nodes(nodes: &[NodeId], coordinates: &[XY]) -> Result<CoordSeq> {
    let mut xs = Vec::with_capacity(nodes.len());
    let mut ys = Vec::with_capacity(nodes.len());
    for &node in nodes {
        let point = embedding::Embedding::xy_at(coordinates, node);
        xs.push(point.x);
        ys.push(point.y);
    }
    CoordSeq::try_from_vecs(xs, ys, None, None)
}

pub(super) fn polygons_from_xy_nodes(
    source: Vec<Vec<Vec<NodeId>>>,
    coordinates: &[XY],
) -> Result<Vec<Polygon>> {
    let mut polygons = Vec::with_capacity(source.len());
    for polygon in source {
        let mut rings = Vec::with_capacity(polygon.len());
        for ring in polygon {
            rings.push(Ring::try_from_closed(xy_sequence_for_nodes(
                &ring,
                coordinates,
            )?)?);
        }
        let mut rings = rings.into_iter();
        let shell = rings.next().expect("polygon has a shell");
        let mut holes = Vec::with_capacity(rings.size_hint().0);
        holes.extend(rings);
        polygons.push(Polygon::new(shell, holes));
    }
    Ok(polygons)
}

pub(super) fn assemble_node_parts(
    points: Vec<Point>,
    lines: Vec<CoordSeq>,
    polygons: Vec<Polygon>,
    point_axes: crate::geometry::CoordinateAxes,
    line_axes: crate::geometry::CoordinateAxes,
    polygon_axes: crate::geometry::CoordinateAxes,
) -> Result<Shape> {
    let point_part = (!points.is_empty())
        .then(|| Shape::narrow_points(point_axes, points))
        .transpose()?;
    let line_part = (!lines.is_empty()).then(|| {
        let mut proven = Vec::with_capacity(lines.len());
        for line in lines {
            proven.push(LineSeq::from_proven(line));
        }
        Shape::narrow_lines(line_axes, proven)
    });
    let polygon_part =
        (!polygons.is_empty()).then(|| Shape::narrow_polygons(polygon_axes, polygons));
    let mut output = Vec::with_capacity(3);
    output.extend([point_part, line_part, polygon_part].into_iter().flatten());
    match <[Shape; 1]>::try_from(output) {
        Ok([single]) => Ok(single),
        Err(parts) => Shape::geometry_collection(parts),
    }
}

pub(super) fn with_axes(point: Point, axes: crate::geometry::CoordinateAxes) -> Point {
    Point::new_raw_axes(
        point.x,
        point.y,
        crate::geometry::ZOrdinate(axes.has_z().then(|| point.z().expect("retained Z"))),
        crate::geometry::MOrdinate(axes.has_m().then(|| point.m().expect("retained M"))),
    )
}

impl ExactPartition<'_> {
    #[expect(
        clippy::indexing_slicing,
        reason = "span IDs and source operands match the admitted span and provenance rows"
    )]
    pub(super) fn emit_lines(
        &self,
        source_operands: &[u32],
        operation: &'static str,
    ) -> Result<ExactLineEmission<'_>> {
        let spans = (0..self.spans.len()).map(|span| span as u32);
        let emission = self.span_emission(spans);
        let emissions = [&emission];
        let embedding = self.embed(&emissions, operation)?;

        let mut offsets = Vec::new();
        let offset_capacity = self
            .spans
            .len()
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        offsets.reserve(offset_capacity);
        offsets.push(0);
        let mut owners = Vec::new();
        for span_index in 0..self.spans.len() {
            for &(source, reversed) in self.owners(span_index) {
                let operand = source_operands[source as usize];
                owners.push(ExactLineSource { operand, reversed });
            }
            offsets.push(u32::try_from(owners.len()).map_err(|_| ResourceError::SizeOverflow)?);
        }
        Ok(ExactLineEmission {
            embedding,
            items: emission.items,
            owners,
            offsets,
        })
    }
}
