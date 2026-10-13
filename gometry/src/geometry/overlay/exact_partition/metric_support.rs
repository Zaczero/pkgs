use super::*;

impl ExactSourceInventory {
    /// Exact closed support for scalar metrics, before coordinate publication.
    ///
    /// Face walks carry filled membership; selected spans retain their original
    /// carriers and exact parameter windows.
    pub(in crate::geometry) fn metric_support(
        &self,
        partition: &ExactPartition<'_>,
        area_location: AreaLocationStrategy,
    ) -> Result<ExactMetricSupport> {
        self.metric_support_impl(partition, false, area_location)
    }

    /// Exact closed support plus the areal boundary incidence needed by
    /// directed filled-set metrics. Boundary flags follow global-face
    /// adjacency without constructing an emission plan.
    pub(in crate::geometry) fn metric_support_with_boundary(
        &self,
        partition: &ExactPartition<'_>,
        area_location: AreaLocationStrategy,
    ) -> Result<ExactMetricSupport> {
        self.metric_support_impl(partition, true, area_location)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "node, span, walk, and dart IDs address the checked partition topology; boundary flags remain parallel to partition spans"
    )]
    pub(super) fn metric_support_impl(
        &self,
        partition: &ExactPartition<'_>,
        with_boundary: bool,
        area_location: AreaLocationStrategy,
    ) -> Result<ExactMetricSupport> {
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let cells = self.select_cells(
            partition,
            &topology,
            &edges,
            &global,
            crate::geometry::OverlayOp::Union,
            false,
        )?;
        let exact_points = partition.freeze_location_points()?;
        let has_selected_area = cells.faces.iter().any(|&selected| selected);
        let area_location_cells = match (has_selected_area, area_location) {
            (false, _) | (true, AreaLocationStrategy::BoundedScan) => None,
            (true, AreaLocationStrategy::PersistentSlabs) => Some(self.area_location_cells(
                partition,
                &topology,
                &edges,
                &global,
                &cells.faces,
                Arc::clone(&exact_points),
            )?),
        };
        let has_selected_area = with_boundary && has_selected_area;
        let selected_boundary_spans = with_boundary.then(|| {
            if has_selected_area {
                cells.selected_boundary_spans(&topology, &edges, &global)
            } else {
                cells.spans.clone()
            }
        });
        let mut spans = Vec::new();
        let mut boundary_spans = with_boundary.then(Vec::new);
        spans.reserve(cells.spans.iter().filter(|&&value| value).count());
        if let Some(boundary_spans) = boundary_spans.as_mut() {
            boundary_spans.reserve(cells.spans.iter().filter(|&&value| value).count());
        }
        let mut covered = Vec::with_capacity(partition.node_count());
        covered.resize(partition.node_count(), false);
        for (index, (span, &selected)) in partition.spans.iter().zip(&cells.spans).enumerate() {
            if !selected {
                continue;
            }
            let source = partition.unique[span.unique as usize];
            let start_parameter = parameter_along(source, &exact_points[span.start.0 as usize])?;
            let end_parameter = parameter_along(source, &exact_points[span.end.0 as usize])?;
            covered[span.start.0 as usize] = true;
            covered[span.end.0 as usize] = true;
            spans.push(ExactMetricSpan {
                start: ExactMetricNode {
                    points: Arc::clone(&exact_points),
                    node: span.start.0,
                },
                end: ExactMetricNode {
                    points: Arc::clone(&exact_points),
                    node: span.end.0,
                },
                start_parameter,
                end_parameter,
                source: partition.unique[span.unique as usize],
            });
            if let Some(boundary_spans) = boundary_spans.as_mut() {
                boundary_spans.push(
                    selected_boundary_spans
                        .as_ref()
                        .expect("boundary incidence was requested")[index],
                );
            }
        }
        let mut points = Vec::new();
        let mut boundary_points = with_boundary.then(Vec::new);
        points.reserve(cells.nodes.iter().filter(|&&value| value).count());
        for (index, (&selected, &is_covered)) in cells.nodes.iter().zip(&covered).enumerate() {
            if selected && !is_covered {
                points.push(ExactMetricNode {
                    points: Arc::clone(&exact_points),
                    node: index as u32,
                });
                if let Some(boundary_points) = boundary_points.as_mut() {
                    boundary_points.push(!has_selected_area);
                }
            }
        }
        let mut faces = Vec::with_capacity(global.count as usize);
        faces.resize_with(global.count as usize, Vec::new);
        for (walk, &[start, end]) in topology.face_starts.array_windows::<2>().enumerate() {
            let face = global.walk_faces[walk] as usize;
            if face == 0 || !cells.faces[face] {
                continue;
            }
            let (start, end) = (start as usize, end as usize);
            let mut cycle = Vec::with_capacity(end.saturating_sub(start));
            for &slot in &topology.face_slots[start..end] {
                let node = NodeId(topology.owners[slot as usize]);
                cycle.push(ExactMetricNode {
                    points: Arc::clone(&exact_points),
                    node: node.0,
                });
            }
            faces[face].push(cycle);
        }
        faces.retain(|cycles| !cycles.is_empty());
        Ok(ExactMetricSupport {
            exact_points,
            spans,
            points,
            faces,
            area_location_cells,
            boundary_spans,
            boundary_points,
        })
    }
}
