use super::{
    Bounds, ChartMetric, CoordSeq, CoordinateAxes, ExactPartition, ExactSourceInventory,
    ExpansionBudget, GeometryErrorKind, GeometryKind, IndexedTopology, KeptNodes, LineSeq,
    OverlayPolygonPartWithSources, OverlaySourceLocation, Point, Polygon, PolygonizeFull,
    ResourceError, Result, Shape, SourceArcContext, assemble_node_parts, bucket_axes, embedding,
    emission, polygonize_full_emission, with_axes, xy_sequence_for_nodes,
};
use crate::geometry::graph::GlobalFaces;

pub(in crate::geometry) fn clip_non_areal_rectangle(
    subject: &Shape,
    bounds: Bounds,
    strict: bool,
) -> Result<Shape> {
    let (inventory, intervals) = ExactSourceInventory::from_non_areal_rect_clip(subject, bounds)?;
    let mut budget = ExpansionBudget::new("clip_by_rect", "rectangle-local exact partition");
    let partition = inventory.partition_in_window(&intervals, bounds, &mut budget)?;
    let (topology, edges) = partition.local_topology()?;
    let plan =
        emission::BoundaryPlan::from_non_areal_partition(&partition, &topology, &edges, bounds)?;
    let result = inventory.emit_overlay_plan::<crate::crs::GeodesicMetric>(
        &partition,
        &topology,
        &edges,
        &plan,
        crate::geometry::OverlayOp::Intersection,
        subject.axes(),
        strict,
        &SourceArcContext::cartesian(),
    )?;
    if result.is_empty() {
        Ok(super::super::nary::empty_shape_for_dimension_with_axes(
            subject.topological_dimension(),
            subject.axes(),
        ))
    } else {
        result.normalize()
    }
}

pub(in crate::geometry) fn clip_polygonal_rectangle<P: AsRef<Polygon>>(
    polygons: &[P],
    rectangle: &Polygon,
    bounds: Bounds,
    axes: CoordinateAxes,
    strict: bool,
    rectangle_first: bool,
) -> Result<Option<Shape>> {
    if polygons.is_empty() {
        return Ok(Some(Shape::typed_empty(GeometryKind::Polygon, axes)));
    }
    let Some((inventory, intervals)) =
        ExactSourceInventory::from_polygon_rect_clip(polygons, rectangle, bounds, rectangle_first)?
    else {
        return Ok(None);
    };
    let mut budget = ExpansionBudget::new("clip_by_rect", "rectangle-local exact partition");
    let mut partition = inventory.partition_in_window(&intervals, bounds, &mut budget)?;
    let (topology, edges) = partition.local_topology()?;
    let global = partition.global_faces(&topology, &edges)?;
    partition.winding_seed =
        inventory.rectangle_winding_seed(&partition, &topology, &edges, &global, bounds)?;
    let result = inventory.overlay_shape_with_faces::<crate::crs::GeodesicMetric>(
        &partition,
        &topology,
        &edges,
        &global,
        crate::geometry::OverlayOp::Intersection,
        axes,
        strict,
        &crate::geometry::SourceArcContext::cartesian(),
    )?;
    Ok(Some(if result.is_empty() {
        Shape::typed_empty(GeometryKind::Polygon, axes)
    } else {
        result.normalize()?
    }))
}

impl ExactSourceInventory {
    pub(in crate::geometry) fn node_shape<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
        context: &SourceArcContext<'_, '_, M>,
    ) -> Result<Shape> {
        if partition.spans.is_empty() {
            return Ok(Shape::multi_line_string(declared_axes, Vec::new()));
        }
        let (embedding, items) = {
            let spans = (0..partition.spans.len()).map(|span| span as u32);
            let emission = partition.span_emission(spans);
            let emissions = [&emission];
            let embedding = partition.embed(&emissions, "node")?;
            (embedding, emission.items)
        };
        if !declared_axes.has_z() && !declared_axes.has_m() {
            let coordinates = embedding.coordinates();
            let mut lines = Vec::with_capacity(items.len());
            for item in items {
                lines.push(LineSeq::from_proven(xy_sequence_for_nodes(
                    &item,
                    coordinates,
                )?));
            }
            return Ok(Shape::multi_line_string(
                crate::geometry::CoordinateAxes::XY,
                lines,
            ));
        }
        let by_node = self.resolve_node_points(
            partition,
            &embedding,
            items.iter().flatten().copied(),
            declared_axes,
            context,
            None,
        )?;
        let axes = bucket_axes(
            items.iter().flatten().copied(),
            &by_node,
            declared_axes,
            strict,
            "node",
        )?;
        let coordinates = by_node.coordinates();
        let mut lines = Vec::with_capacity(items.len());
        for item in items {
            let points = vec![
                with_axes(
                    by_node.point(item[0], embedding::Embedding::xy_at(coordinates, item[0])),
                    axes,
                ),
                with_axes(
                    by_node.point(item[1], embedding::Embedding::xy_at(coordinates, item[1])),
                    axes,
                ),
            ];
            lines.push(LineSeq::from_proven(CoordSeq::try_from_points(&points)?));
        }
        Ok(Shape::multi_line_string(axes, lines))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "span, node, dart, face, and operand IDs come from the checked partition and its aligned diagnostic rows"
    )]
    pub(in crate::geometry) fn polygonize_full<M: ChartMetric>(
        &self,
        partition: &mut ExactPartition<'_>,
        declared_axes: CoordinateAxes,
        row_axes: &[CoordinateAxes],
        strict: bool,
        context: &SourceArcContext<'_, '_, M>,
    ) -> Result<PolygonizeFull> {
        if row_axes.len() != self.operand_count() {
            return Err(GeometryErrorKind::Invalid(
                "polygonize diagnostic axes do not match source rows".into(),
            )
            .into());
        }
        partition.kept_nodes = KeptNodes::All;
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let mut same_face = vec![false; partition.spans.len()];
        for (slot, &span) in edges.iter().enumerate() {
            let twin = topology.twins[slot] as usize;
            if slot > twin {
                continue;
            }
            let span = span as usize;
            let face = global.walk_faces[topology.face_of[slot] as usize];
            let other_face = global.walk_faces[topology.face_of[twin] as usize];
            same_face[span] = face == other_face;
        }

        let mut degree: Vec<usize> = topology
            .starts
            .array_windows::<2>()
            .map(|&[start, end]| end.wrapping_sub(start) as usize)
            .collect();
        let mut pending: Vec<usize> = degree
            .iter()
            .enumerate()
            .filter_map(|(node, &degree)| (degree == 1).then_some(node))
            .collect();
        let mut dangle_spans = vec![false; partition.spans.len()];
        while let Some(node) = pending.pop() {
            if degree[node] != 1 {
                continue;
            }
            let start = topology.starts[node] as usize;
            let end = topology.starts[node.wrapping_add(1)] as usize;
            let span = edges[start..end]
                .iter()
                .map(|&span| span as usize)
                .find(|&span| !dangle_spans[span])
                .expect("a degree-one node has one remaining incident span");
            dangle_spans[span] = true;

            let span = partition.spans[span];
            let other = if span.start.0 as usize == node {
                span.end.0 as usize
            } else {
                span.start.0 as usize
            };
            degree[node] = 0;
            degree[other] = degree[other].wrapping_sub(1);
            if degree[other] == 1 {
                pending.push(other);
            }
        }
        drop(pending);
        drop(degree);

        let mut cuts = Vec::new();
        let mut dangles = Vec::new();
        let mut invalid_rings = Vec::new();
        for rows in [&mut cuts, &mut dangles, &mut invalid_rings] {
            rows.reserve(self.polygon_rings.len());
            rows.resize_with(self.polygon_rings.len(), Vec::new);
        }
        let mut diagnostic_spans = vec![false; partition.spans.len()];
        for index in 0..partition.spans.len() {
            let owners = partition.owners(index);
            let mut rows = Vec::with_capacity(owners.len());
            for &(source, _) in owners {
                let row = self.source_operands[source as usize];
                if !rows.contains(&row) {
                    rows.push(row);
                }
            }
            for row in rows {
                let row = row as usize;
                if owners.len() > 1 {
                    invalid_rings[row].push((index, true));
                } else if dangle_spans[index] {
                    dangles[row].push((index, false));
                } else if same_face[index] {
                    cuts[row].push((index, false));
                } else {
                    continue;
                }
                diagnostic_spans[index] = true;
            }
        }

        let emission =
            polygonize_full_emission(partition, &topology, &edges, &global, &diagnostic_spans)?;
        let embedding = partition.embed(&[&emission], "polygonize_full")?;
        let groups = emission::polygonize_node_groups(partition, &topology, &global, &embedding)?;
        let polygons = self.polygon_shapes_from_node_groups(
            partition,
            &embedding,
            groups,
            declared_axes,
            strict,
            "polygonize_full",
            context,
            None,
        )?;
        let cuts = self
            .diagnostic_shapes_for_rows(partition, &embedding, cuts, row_axes, strict, context)?;
        let dangles = self.diagnostic_shapes_for_rows(
            partition, &embedding, dangles, row_axes, strict, context,
        )?;
        let invalid_rings = self.diagnostic_shapes_for_rows(
            partition,
            &embedding,
            invalid_rings,
            row_axes,
            strict,
            context,
        )?;
        Ok(PolygonizeFull {
            polygons,
            cuts,
            dangles,
            invalid_rings,
        })
    }

    /// Publish the Boundary-location cells of one polygonal operand. Occupied
    /// area cycles remain closed linework; only uncovered residual boundary
    /// nodes become points. Lower-dimensional spans are never emitted.
    #[expect(
        clippy::indexing_slicing,
        reason = "source darts and node IDs are admitted by this partition's plan and topology"
    )]
    pub(in crate::geometry) fn occupied_boundary_shape<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        declared_axes: CoordinateAxes,
        context: &SourceArcContext<'_, '_, M>,
    ) -> Result<Shape> {
        debug_assert_eq!(self.operand_count(), 1);
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let selection = self.boundary_cells(partition, &topology, &edges, &global)?;
        let has_area = selection.cells.faces.iter().any(|&selected| selected);
        let ordinate_protected = self.ordinate_protected_nodes(partition, declared_axes)?;
        let mut plan = global.boundary_plan_with_protected(
            &[],
            &topology,
            &edges,
            partition,
            &selection.cells.faces,
            ordinate_protected.as_deref(),
            None,
        )?;

        let mut covered_by_area_walk = vec![false; partition.node_count()];
        for chord in plan.walks.iter().flatten() {
            for &dart in &plan.source_darts[chord.sources.clone()] {
                covered_by_area_walk[topology.owners[dart as usize] as usize] = true;
                covered_by_area_walk[topology.targets[dart as usize] as usize] = true;
            }
        }
        plan.points.reserve(selection.boundary_nodes.len());
        for node in selection.boundary_nodes {
            if covered_by_area_walk[node.0 as usize] {
                continue;
            }
            let host = global.node_host_face(&topology, node);
            plan.points.push((node, plan.face_quotient[host as usize]));
        }

        let emission = plan.emission(&topology, &edges);
        let embedding = partition.embed(&[&emission], "boundary")?;
        let parts = plan.emit_boundary_node_parts(partition, &emission)?;
        let by_node = self.resolve_node_points(
            partition,
            &embedding,
            parts
                .points
                .iter()
                .copied()
                .chain(parts.lines.iter().flatten().copied()),
            declared_axes,
            context,
            None,
        )?;
        self.boundary_node_parts_to_shape_resolved(
            parts,
            &by_node,
            declared_axes,
            has_area,
            selection.has_curve_support,
            selection.has_point_support,
        )
    }

    pub(super) const fn operand_count(&self) -> usize {
        self.polygon_rings.len()
    }

    /// Select and serialize all relatively open cells for one Cartesian
    /// operation. The exact partition is the only incidence owner; this
    /// method never reconstructs a graph from rounded coordinates.
    #[cfg(test)]
    pub(in crate::geometry) fn overlay_parts(
        &self,
        partition: &ExactPartition<'_>,
        operation: crate::geometry::OverlayOp,
    ) -> Result<(Vec<Point>, Vec<CoordSeq>, Vec<Polygon>)> {
        self.overlay_parts_with_fill(partition, operation, operation.name())
    }

    pub(in crate::geometry) fn overlay_shape<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        operation: crate::geometry::OverlayOp,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
        context: &SourceArcContext<'_, '_, M>,
    ) -> Result<Shape> {
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        self.overlay_shape_with_faces(
            partition,
            &topology,
            &edges,
            &global,
            operation,
            declared_axes,
            strict,
            context,
        )
    }

    pub(in crate::geometry) fn overlay_polygon_parts_with_preserved_operand(
        &self,
        operation: crate::geometry::OverlayOp,
        preserved_operand: u32,
    ) -> Result<Vec<OverlayPolygonPartWithSources>> {
        let partition = self.partition_without_budget()?;
        self.overlay_polygon_parts_from_partition(&partition, operation, preserved_operand)
    }

    pub(in crate::geometry) fn overlay_polygon_parts_with_source_roles(
        &self,
        operation: crate::geometry::OverlayOp,
        preserved_operand: u32,
        frame: crate::geographic::GeographicFrame,
    ) -> Result<Vec<OverlayPolygonPartWithSources>> {
        let mut budget = ExpansionBudget::new(operation.name(), "exact partition");
        let mut partition = self.partition(&mut budget)?;
        partition.set_geographic_frame(frame);
        self.overlay_polygon_parts_from_partition(&partition, operation, preserved_operand)
    }

    fn overlay_polygon_parts_from_partition(
        &self,
        partition: &ExactPartition<'_>,
        operation: crate::geometry::OverlayOp,
        preserved_operand: u32,
    ) -> Result<Vec<OverlayPolygonPartWithSources>> {
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let cells = self.select_cells(partition, &topology, &edges, &global, operation, false)?;
        let mut preserved_spans = Vec::with_capacity(partition.spans.len());
        for span in 0..partition.spans.len() {
            let preserved = partition
                .provenance
                .owners(span)
                .iter()
                .any(|&(source, _)| {
                    self.source_operands.get(source as usize).copied() == Some(preserved_operand)
                });
            preserved_spans.push(preserved);
        }
        let mut ring_local = vec![0_u32; self.ring_count];
        for range in &self.polygon_ring_ranges {
            for ring in range.clone() {
                let local_index = ring.checked_sub(range.start).ok_or_else(|| {
                    GeometryErrorKind::Invalid("polygon ring range starts after its ring".into())
                })?;
                let local = u32::try_from(local_index).map_err(|_| ResourceError::SizeOverflow)?;
                *ring_local.get_mut(ring).ok_or_else(|| {
                    GeometryErrorKind::Invalid("polygon source ring ordinal is unknown".into())
                })? = local;
            }
        }
        let mut ring_next_edge = vec![0_u32; self.ring_count];
        let mut source_locations = Vec::with_capacity(self.source_segments.len());
        for (source, maybe_ring) in self.source_rings.iter().copied().enumerate() {
            let ring = maybe_ring.ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "polygon edge provenance contains a non-ring source".into(),
                )
            })? as usize;
            let edge = *ring_next_edge.get(ring).ok_or_else(|| {
                GeometryErrorKind::Invalid("polygon source ring is unknown".into())
            })?;
            let next = edge.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            *ring_next_edge.get_mut(ring).ok_or_else(|| {
                GeometryErrorKind::Invalid("polygon source ring is unknown".into())
            })? = next;
            let operand = *self.source_operands.get(source).ok_or_else(|| {
                GeometryErrorKind::Invalid("polygon source operand is unknown".into())
            })?;
            source_locations.push(OverlaySourceLocation {
                operand,
                ring: *ring_local.get(ring).ok_or_else(|| {
                    GeometryErrorKind::Invalid("polygon source ring ordinal is unknown".into())
                })?,
                edge,
            });
        }
        let plan = cells.emission_plan_with_protected(
            partition,
            &topology,
            &edges,
            &global,
            None,
            Some(&preserved_spans),
        )?;
        plan.emit_xy_polygon_parts_with_source_roles(
            partition,
            &topology,
            &edges,
            operation.name(),
            &preserved_spans,
            &source_locations,
            &self.source_segments,
        )
    }

    pub(super) fn overlay_shape_with_faces<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        operation: crate::geometry::OverlayOp,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
        context: &SourceArcContext<'_, '_, M>,
    ) -> Result<Shape> {
        let cells = self.select_cells(partition, topology, edges, global, operation, false)?;
        let ordinate_protected = self.ordinate_protected_nodes(partition, declared_axes)?;
        let plan = cells.emission_plan_with_protected(
            partition,
            topology,
            edges,
            global,
            ordinate_protected.as_deref(),
            None,
        )?;
        self.emit_overlay_plan(
            partition,
            topology,
            edges,
            &plan,
            operation,
            declared_axes,
            strict,
            context,
        )
    }

    pub(super) fn emit_overlay_plan<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        plan: &emission::BoundaryPlan,
        operation: crate::geometry::OverlayOp,
        declared_axes: CoordinateAxes,
        strict: bool,
        context: &SourceArcContext<'_, '_, M>,
    ) -> Result<Shape> {
        if declared_axes == CoordinateAxes::XY && plan.points.is_empty() && plan.lines.is_empty() {
            let (_, _, polygons) =
                plan.emit_xy_parts(partition, topology, edges, operation.name())?;
            return Ok(Shape::narrow_polygons(CoordinateAxes::XY, polygons));
        }
        if !declared_axes.has_z() && !declared_axes.has_m() {
            let (points, lines, polygons) =
                plan.emit_xy_parts(partition, topology, edges, operation.name())?;
            return assemble_node_parts(
                points,
                lines,
                polygons,
                crate::geometry::CoordinateAxes::XY,
                crate::geometry::CoordinateAxes::XY,
                crate::geometry::CoordinateAxes::XY,
            );
        }
        let emission = plan.emission(topology, edges);
        let embedding = partition.embed(&[&emission], operation.name())?;
        let parts = plan.build_node_parts(partition, topology, &emission)?;
        let by_node = self.resolve_node_points(
            partition,
            &embedding,
            parts
                .points
                .iter()
                .copied()
                .chain(parts.lines.iter().flatten().copied())
                .chain(
                    parts
                        .polygons
                        .iter()
                        .flat_map(|polygon| polygon.iter().flatten().copied()),
                ),
            declared_axes,
            context,
            None,
        )?;
        self.node_parts_to_shape_resolved(
            parts,
            &embedding,
            &by_node,
            declared_axes,
            strict,
            operation.name(),
            None,
        )
    }

    pub(super) fn overlay_parts_with_fill(
        &self,
        partition: &ExactPartition<'_>,
        operation: crate::geometry::OverlayOp,
        embedding_operation: &'static str,
    ) -> Result<(Vec<Point>, Vec<CoordSeq>, Vec<Polygon>)> {
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let cells = self.select_cells(partition, &topology, &edges, &global, operation, false)?;
        let plan = cells.emission_plan(partition, &topology, &edges, &global)?;
        plan.emit_xy_parts(partition, &topology, &edges, embedding_operation)
    }
}
