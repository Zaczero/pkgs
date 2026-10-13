use super::*;

impl ExactSourceInventory {
    pub(in crate::geometry) fn repair_polygons<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        method: RepairMethod,
        declared_axes: CoordinateAxes,
        strict: bool,
        context: &SourceArcContext<'_, '_, M>,
    ) -> Result<Vec<Shape>> {
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let cells = match method {
            RepairMethod::Linework => {
                let winding_rings = self.winding_source_rings();
                self.select_cells_with_winding_rings(
                    partition,
                    &topology,
                    &edges,
                    &global,
                    crate::geometry::OverlayOp::Union,
                    true,
                    &winding_rings,
                )?
            },
            RepairMethod::Structure => self.select_cells(
                partition,
                &topology,
                &edges,
                &global,
                crate::geometry::OverlayOp::Union,
                false,
            )?,
        };
        let protected = self.ordinate_protected_nodes(partition, declared_axes)?;
        let plan = cells.emission_plan_with_protected(
            partition,
            &topology,
            &edges,
            &global,
            protected.as_deref(),
            None,
        )?;
        let emission = plan.emission(&topology, &edges);
        let embedding = partition.embed(&[&emission], "repair")?;
        let parts = plan.build_node_parts(partition, &topology, &emission)?;
        let mut groups = Vec::with_capacity(parts.polygons.len());
        for rings in parts.polygons {
            let mut ring_iter = rings.into_iter();
            let shell = ring_iter.next().expect("repair polygon has a shell");
            let Some(shell) = remove_node_antennas(shell) else {
                continue;
            };
            let mut polygon = Vec::with_capacity(ring_iter.size_hint().0.wrapping_add(1));
            polygon.push(shell);
            for hole in ring_iter {
                if let Some(hole) = remove_node_antennas(hole) {
                    polygon.push(hole);
                }
            }
            groups.push(polygon);
        }
        self.polygon_shapes_from_node_groups(
            partition,
            &embedding,
            groups,
            declared_axes,
            strict,
            "repair",
            context,
            None,
        )
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "face and dart IDs are produced by the checked local/global topology built from this partition"
    )]
    pub(in crate::geometry) fn linework_polygons<M: ChartMetric>(
        &self,
        partition: &mut ExactPartition<'_>,
        build_area: bool,
        operation: &'static str,
        declared_axes: CoordinateAxes,
        strict: bool,
        context: &SourceArcContext<'_, '_, M>,
    ) -> Result<Vec<Shape>> {
        partition.kept_nodes = KeptNodes::All;
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        if !build_area {
            let emission = polygonize_emission(partition, &topology, &edges, &global)?;
            let embedding = partition.embed(&[&emission], operation)?;
            let groups =
                emission::polygonize_node_groups(partition, &topology, &global, &embedding)?;
            return self.polygon_shapes_from_node_groups(
                partition,
                &embedding,
                groups,
                declared_axes,
                strict,
                operation,
                context,
                None,
            );
        }
        let mut cycles: Vec<Vec<Vec<NodeId>>> = Vec::with_capacity(global.count as usize);
        cycles.resize_with(global.count as usize, Vec::new);
        for (walk, &[start, end]) in topology.face_starts.array_windows::<2>().enumerate() {
            let (start, end) = (start as usize, end as usize);
            let mut cycle = Vec::with_capacity(end.saturating_sub(start));
            for &slot in &topology.face_slots[start..end] {
                cycle.push(NodeId(topology.owners[slot as usize]));
            }
            let face = global.walk_faces[walk] as usize;
            cycles[face].push(cycle);
        }
        let mut samples = Vec::with_capacity(global.count as usize);
        samples.resize(global.count as usize, None);
        for (face_cycles, sample_slot) in cycles.iter().zip(&mut samples).skip(1) {
            for &node in face_cycles.iter().flatten() {
                let earlier = match *sample_slot {
                    None => true,
                    Some(prior) => partition.compare_nodes(node, prior)?.is_lt(),
                };
                if earlier {
                    *sample_slot = Some(node);
                }
            }
        }
        // A face's nesting depth counts the other faces whose cycles contain
        // its sample. Rounding is monotone, so a cycle can contain the sample
        // only if the sample's rounded coordinates lie within the rounded
        // bounds of the cycle's nodes; samples sorted by x visit only those.
        let mut by_x = Vec::with_capacity(global.count as usize);
        for (face, sample) in samples.iter().enumerate() {
            if let Some(sample) = sample {
                by_x.push((partition.nearest_xy(*sample), face, *sample));
            }
        }
        by_x.sort_unstable_by(|left, right| left.0.x.total_cmp(&right.0.x));
        let mut depths = vec![0_u32; global.count as usize];
        for (other, other_cycles) in cycles.iter().enumerate().skip(1) {
            let Some([min_x, min_y, max_x, max_y]) = other_cycles
                .iter()
                .flatten()
                .map(|&node| partition.nearest_xy(node))
                .map(|xy| [xy.x, xy.y, xy.x, xy.y])
                .reduce(|[a, b, c, d], [x, y, z, w]| [a.min(x), b.min(y), c.max(z), d.max(w)])
            else {
                continue;
            };
            let first = by_x.partition_point(|(xy, ..)| xy.x < min_x);
            for &(xy, face, sample) in by_x.iter().skip(first) {
                if xy.x > max_x {
                    break;
                }
                if face == other || xy.y < min_y || xy.y > max_y {
                    continue;
                }
                for cycle in other_cycles {
                    if cycle_contains_node(partition, cycle, sample)? {
                        depths[face] = depths[face].wrapping_add(1);
                        break;
                    }
                }
            }
        }
        let mut selected = Vec::with_capacity(global.count as usize);
        selected.extend(
            samples
                .iter()
                .zip(&depths)
                .enumerate()
                .map(|(face, (sample, &depth))| {
                    face != 0 && sample.is_some() && depth.is_multiple_of(2)
                }),
        );
        let cells = ArealCells {
            faces: selected,
            spans: {
                let mut values = Vec::with_capacity(partition.spans.len());
                values.resize(partition.spans.len(), false);
                values
            },
            nodes: {
                let mut values = Vec::with_capacity(partition.node_count());
                values.resize(partition.node_count(), false);
                values
            },
        };
        let plan = cells.emission_plan(partition, &topology, &edges, &global)?;
        let emission = plan.emission(&topology, &edges);
        let embedding = partition.embed(&[&emission], operation)?;
        let parts = plan.build_node_parts(partition, &topology, &emission)?;
        debug_assert!(parts.points.is_empty() && parts.lines.is_empty());
        self.polygon_shapes_from_node_groups(
            partition,
            &embedding,
            parts.polygons,
            declared_axes,
            strict,
            operation,
            context,
            None,
        )
    }

    pub(super) fn polygon_shapes_from_node_groups<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        embedding: &embedding::Embedding<'_>,
        groups: Vec<Vec<Vec<NodeId>>>,
        declared_axes: CoordinateAxes,
        strict: bool,
        operation: &'static str,
        context: &SourceArcContext<'_, '_, M>,
        source_scope: Option<u32>,
    ) -> Result<Vec<Shape>> {
        let by_node = self.resolve_node_points(
            partition,
            embedding,
            groups
                .iter()
                .flat_map(|polygon| polygon.iter().flatten().copied()),
            declared_axes,
            context,
            source_scope,
        )?;
        let mut output = Vec::with_capacity(groups.len());
        for polygon in groups {
            let axes = bucket_axes(
                polygon.iter().flatten().copied(),
                &by_node,
                declared_axes,
                strict,
                operation,
            )?;
            let mut rings = Vec::with_capacity(polygon.len());
            for mut nodes in polygon {
                if let Some(&first) = nodes.first()
                    && nodes.last() != Some(&first)
                {
                    nodes.push(first);
                }
                rings.push(Ring::try_from_closed(
                    self.coordinates_from_nodes(&nodes, &by_node, axes)?,
                )?);
            }
            let mut rings = rings.into_iter();
            let shell = rings.next().expect("polygon node group has a shell");
            let mut holes = Vec::with_capacity(rings.size_hint().0);
            holes.extend(rings);
            output.push(Shape::Polygon(Polygon::new(shell, holes)));
        }
        Ok(output)
    }

    pub(super) fn diagnostic_shapes_for_rows<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        embedding: &embedding::Embedding<'_>,
        rows: Vec<Vec<(usize, bool)>>,
        row_axes: &[CoordinateAxes],
        strict: bool,
        context: &SourceArcContext<'_, '_, M>,
    ) -> Result<Vec<Vec<Shape>>> {
        let mut output = Vec::with_capacity(rows.len());
        for (row, spans) in rows.into_iter().enumerate() {
            let declared_axes = *row_axes.get(row).ok_or_else(|| {
                GeometryErrorKind::Invalid("polygonize diagnostic row has no source axes".into())
            })?;
            let mut lines = Vec::with_capacity(spans.len());
            for (span, closed) in spans {
                let span = partition.spans.get(span).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "polygonize diagnostic references an unknown source span".into(),
                    )
                })?;
                let mut line_nodes = Vec::with_capacity(if closed { 3 } else { 2 });
                line_nodes.extend([span.start, span.end]);
                if closed {
                    line_nodes.push(span.start);
                }
                lines.push(line_nodes);
            }
            let source_scope = Some(u32::try_from(row).map_err(|_| ResourceError::SizeOverflow)?);
            let by_node = self.resolve_node_points(
                partition,
                embedding,
                lines.iter().flatten().copied(),
                declared_axes,
                context,
                source_scope,
            )?;
            let mut diagnostics = Vec::with_capacity(lines.len());
            for line_nodes in lines {
                let axes = bucket_axes(
                    line_nodes.iter().copied(),
                    &by_node,
                    declared_axes,
                    strict,
                    "polygonize_full",
                )?;
                diagnostics.push(Shape::LineString(LineSeq::from_proven(
                    self.coordinates_from_nodes(&line_nodes, &by_node, axes)?,
                )));
            }
            diagnostics.sort_unstable_by(compare_diagnostic_lines);
            output.push(diagnostics);
        }
        Ok(output)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "face IDs yielded by visit_face_windings index the selected vector sized from GlobalFaces"
    )]
    pub(in crate::geometry) fn winding_polygons(
        &self,
        partition: &ExactPartition<'_>,
        keep: impl Fn(i64) -> bool,
    ) -> Result<Vec<Polygon>> {
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let winding_rings = self.winding_source_rings();
        let mut selected = Vec::with_capacity(global.count as usize);
        selected.resize(global.count as usize, false);
        partition.visit_face_windings(
            &topology,
            &edges,
            &global,
            &winding_rings,
            self.ring_count,
            |face, winding| {
                selected[face as usize] = keep(winding.iter().sum());
                Ok(())
            },
        )?;
        self.emit_selected_winding_polygons(
            partition, &topology, &edges, &global, selected, "buffer",
        )
    }

    /// Select the union of independently wound polygon groups from one exact
    /// source partition. Each range contains one polygon's canonical shell
    /// followed by its canonical holes; a face belongs when any group's
    /// signed winding is negative.
    #[expect(
        clippy::indexing_slicing,
        reason = "face IDs yielded by visit_face_windings index the selected vector sized from GlobalFaces"
    )]
    pub(in crate::geometry) fn winding_group_union_polygons(
        &self,
        partition: &ExactPartition<'_>,
        groups: &[Range<usize>],
    ) -> Result<Vec<Polygon>> {
        let mut cursor = 0_usize;
        for group in groups {
            if group.start != cursor || group.start >= group.end || group.end > self.ring_count {
                return Err(GeometryErrorKind::Invalid(
                    "exact winding polygon groups are not contiguous".into(),
                )
                .into());
            }
            cursor = group.end;
        }
        if cursor != self.ring_count {
            return Err(GeometryErrorKind::Invalid(
                "exact winding polygon groups do not cover all rings".into(),
            )
            .into());
        }
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let winding_rings = self.winding_source_rings();
        let mut selected = Vec::with_capacity(global.count as usize);
        selected.resize(global.count as usize, false);
        partition.visit_face_windings(
            &topology,
            &edges,
            &global,
            &winding_rings,
            self.ring_count,
            |face, winding| {
                let mut occupied = false;
                for group in groups {
                    let mut sum = 0_i64;
                    for index in group.clone() {
                        let value = *winding.get(index).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "exact winding group references an unknown ring".into(),
                            )
                        })?;
                        sum = sum.checked_add(value).ok_or(ResourceError::SizeOverflow)?;
                    }
                    occupied |= sum <= -1;
                    if occupied {
                        break;
                    }
                }
                selected[face as usize] = occupied;
                Ok(())
            },
        )?;
        self.emit_selected_winding_polygons(
            partition, &topology, &edges, &global, selected, "buffer",
        )
    }

    pub(super) fn emit_selected_winding_polygons(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        selected: Vec<bool>,
        operation: &'static str,
    ) -> Result<Vec<Polygon>> {
        let cells = ArealCells {
            faces: selected,
            spans: {
                let mut values = Vec::with_capacity(partition.spans.len());
                values.resize(partition.spans.len(), false);
                values
            },
            nodes: {
                let mut values = Vec::with_capacity(partition.node_count());
                values.resize(partition.node_count(), false);
                values
            },
        };
        let plan = cells.emission_plan(partition, topology, edges, global)?;
        let (_, _, polygons) = plan.emit_xy_parts(partition, topology, edges, operation)?;
        Ok(polygons)
    }
}

pub(super) fn compare_diagnostic_lines(left: &Shape, right: &Shape) -> Ordering {
    let key = |shape: &Shape| {
        let Shape::LineString(line) = shape else {
            unreachable!("polygonize diagnostics are line strings")
        };
        (
            PointKey::new(line.first().expect("diagnostic line has a start")),
            PointKey::new(line.point_at(1)),
        )
    };
    key(left).cmp(&key(right))
}
