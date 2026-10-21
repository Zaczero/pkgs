use super::{
    BoundaryPlan, CoordinateAxes, ExactPartition, ExactSourceInventory, ExactSpan, ExpansionBudget,
    Loc, NodeId, Result, Shape, SourceArcContext, XY, homogeneous_midpoint,
};
use crate::geometry::graph::RayAxis as Axis;
use crate::geometry::segments::contact::point_on_segment;
use crate::geometry::source_support::ExactProbe;
use crate::geometry::{OverlayOp, PreparedPlan, PreparedPointQueries, same_topological_coordinate};

#[derive(Clone, Copy, Default)]
struct LinePolygonNode {
    line_source: bool,
    probe_required: bool,
    covered: bool,
}

impl ExactSourceInventory {
    pub(in crate::geometry) fn overlay_line_polygon(
        &self,
        polygon: &Shape,
        line_operand: u32,
        operation: OverlayOp,
        axes: CoordinateAxes,
        strict: bool,
        budget: Option<&mut ExpansionBudget>,
    ) -> Result<Shape> {
        debug_assert!(matches!(
            operation,
            OverlayOp::Intersection | OverlayOp::Difference
        ));
        let partition = self.partition_with_budget(budget)?;
        let (spans, points) =
            self.select_line_polygon_cells(&partition, polygon, line_operand, operation)?;
        let (topology, edges) = partition.local_topology()?;
        let protected = self.ordinate_protected_nodes(&partition, axes)?;
        let plan = BoundaryPlan::from_non_areal_selection(
            &partition,
            &topology,
            &edges,
            &spans,
            &points,
            protected.as_deref(),
        )?;
        self.emit_overlay_plan::<crate::crs::GeodesicMetric>(
            &partition,
            &topology,
            &edges,
            &plan,
            operation,
            axes,
            strict,
            &SourceArcContext::cartesian(),
        )
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "node and source IDs come from this inventory's admitted partition and provenance"
    )]
    fn select_line_polygon_cells(
        &self,
        partition: &ExactPartition<'_>,
        polygon: &Shape,
        line_operand: u32,
        operation: OverlayOp,
    ) -> Result<(Vec<u32>, Vec<NodeId>)> {
        let line_source = |source: u32| self.source_operands[source as usize] == line_operand;
        let mut nodes = vec![LinePolygonNode::default(); partition.node_count()];
        let mut line_spans = Vec::new();
        for (index, span) in partition.spans.iter().enumerate() {
            let mut has_line = false;
            let mut has_polygon = false;
            for &(source, _) in partition.owners(index) {
                if line_source(source) {
                    has_line = true;
                } else {
                    has_polygon = true;
                }
            }
            for node in [span.start, span.end] {
                nodes[node.0 as usize].line_source |= has_line;
                nodes[node.0 as usize].probe_required |= has_polygon;
            }
            if has_line {
                line_spans.push(index);
            }
        }
        let plan = PreparedPlan::transient(polygon);
        let queries = plan.prepare_point_queries(line_spans.len())?;
        let intersection = operation == OverlayOp::Intersection;
        let keep = |location| {
            if intersection {
                location != Loc::Exterior
            } else {
                location == Loc::Exterior
            }
        };
        let mut spans = Vec::with_capacity(line_spans.len());
        for index in line_spans {
            let span = partition.spans[index];
            if keep(span_polygon_location(partition, span, &queries)?) {
                spans.push(index as u32);
                // Downward closure retains a selected span's endpoints even
                // when pointwise difference excludes their polygon boundary.
                nodes[span.start.0 as usize].covered = true;
                nodes[span.end.0 as usize].covered = true;
            }
        }
        for &(node, source) in &partition.point_sources {
            nodes[node.0 as usize].line_source |= line_source(source);
            nodes[node.0 as usize].probe_required = true;
        }
        for (node, _, operand) in self.isolated_point_occurrences(partition) {
            nodes[node.0 as usize].line_source |= operand == line_operand;
            nodes[node.0 as usize].probe_required = true;
        }
        let mut points = Vec::new();
        for (index, source) in nodes.iter().enumerate() {
            // Without polygon incidence or isolated point support, an
            // uncovered line node has its adjacent rejected atom's membership.
            // Raw polygon incidence only nominates a probe, not occupied support.
            if source.line_source && !source.covered && source.probe_required {
                let node = NodeId(index as u32);
                let exact = partition.exact(node)?;
                if keep(queries.locate_probe(&ExactProbe::new(&exact))?.location) {
                    points.push(node);
                }
            }
        }
        Ok((spans, points))
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "the span's unique ID was admitted by this exact partition"
)]
fn span_polygon_location(
    partition: &ExactPartition<'_>,
    span: ExactSpan,
    queries: &PreparedPointQueries<'_>,
) -> Result<Loc> {
    let start = partition.nearest_xy(span.start);
    let end = partition.nearest_xy(span.end);
    let midpoint = XY::new(f64::midpoint(start.x, end.x), f64::midpoint(start.y, end.y));
    let source = partition.unique[span.unique as usize];
    let axis = if same_topological_coordinate(source.start.x, source.end.x) {
        Axis::Y
    } else {
        Axis::X
    };
    if point_on_segment(midpoint.point(), source.start, source.end) {
        let from_start = partition.compare_axis_to(span.start, axis, axis.of(midpoint))?;
        let from_end = partition.compare_axis_to(span.end, axis, axis.of(midpoint))?;
        // A stored probe is usable only on the original carrier and strictly
        // inside the exact atom, not merely on its rounded drawing.
        if (from_start.is_lt() && from_end.is_gt()) || (from_start.is_gt() && from_end.is_lt()) {
            return queries
                .locate_stored(midpoint.point())
                .map(|location| location.location);
        }
    }
    let start = partition.exact(span.start)?;
    let end = partition.exact(span.end)?;
    let midpoint = homogeneous_midpoint(&start, &end)?;
    queries
        .locate_probe(&ExactProbe::new(&midpoint))
        .map(|location| location.location)
}
