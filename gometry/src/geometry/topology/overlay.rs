//! Exact n-ary geographic set selection and output emission over global Q.

use std::ops::Range;

use super::exact::{
    ExactChartPiece, ExactSourceEdge, ExactSourceRange, ExactSourceRing, SourceEdgeId,
    SourceEdgeKind, SourceRingId, SourceRowId, exact_source_piece_point,
};
use super::q::{ExactGeographicTopology, GlobalFaceId, QDartId, QNodeId, QPortId};
use crate::error::{ResourceError, Result};
use crate::geographic::{GeographicRing, Latitude, RingSummary, SeamIdentity};
use crate::geometry::ordinates::AxisValue;
use crate::geometry::overlay::{OverlayOp, build_certified_overlay_shape_with_axes};
use crate::geometry::segments::try_sort_by;
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{
    CoordSeq, CoordinateAxes, Coordinates as _, GeometryErrorKind, HasM, HasZ, MOrdinate, Point,
    Polygon, Ring, RingWinding, Shape, XY, ZOrdinate, interpolate_optional_ordinate,
    open_point_cycle_winding, ring_winding,
};

#[derive(Clone, Copy)]
struct LineStep {
    span: usize,
    from_endpoint: SpanEndpoint,
    owner: usize,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum SpanEndpoint {
    Start,
    End,
}

impl SpanEndpoint {
    const fn opposite(self) -> Self {
        match self {
            Self::Start => Self::End,
            Self::End => Self::Start,
        }
    }
}

struct BoundaryCycle {
    darts: Vec<QDartId>,
    region: usize,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum EmittedNodeKey {
    Coordinate(u64, u64),
    NorthPole,
    SouthPole,
}

#[derive(Clone, Copy)]
struct EmittedVertex {
    node: QNodeId,
    port: QPortId,
    point: Point,
}

#[derive(Clone, Copy, Default)]
struct NodeOrdinates {
    z: AxisValue,
    m: AxisValue,
}

/// Select all dimensions from the one exact source chart and its global Q.
/// Rounding happens only after face, span, and node selection is complete.
pub(in crate::geometry) fn exact_geographic_overlay(
    topology: &ExactGeographicTopology,
    operation: OverlayOp,
    declared_axes: CoordinateAxes,
    strict: bool,
) -> Result<Shape> {
    let q = &topology.q;
    let row_count = topology.sources.row_count;
    let faces = select_faces(q, row_count, operation)?;
    let whole_sphere = !faces.is_empty() && faces.iter().all(|&selected| selected);
    let mut face_regions = face_regions(q, &faces)?;
    let line_spans = select_line_spans(topology, &faces, row_count, operation)?;
    let cycles = selected_area_boundaries(q, &faces, &mut face_regions)?;
    let point_nodes = select_points(topology, &faces, &line_spans, row_count, operation)?;
    let piece_ranges = source_piece_ranges(topology)?;
    let ordinates = node_ordinates(topology)?;

    let mut live_node_keys = Vec::new();
    let mut polygons = emit_polygons(
        topology,
        &piece_ranges,
        &ordinates,
        &faces,
        operation,
        &cycles,
        &mut live_node_keys,
    )?;
    let lines = emit_lines(
        topology,
        &piece_ranges,
        &ordinates,
        &line_spans,
        &mut live_node_keys,
    )?;
    let points = emit_points(topology, &point_nodes, &mut live_node_keys)?;
    certify_node_injectivity(&mut live_node_keys)?;

    if points.is_empty() && lines.is_empty() && polygons.is_empty() {
        return build_certified_overlay_shape_with_axes(points, lines, polygons, declared_axes);
    }

    let mut has_z = declared_axes.has_z();
    let mut has_m = declared_axes.has_m();
    for (_, node) in &live_node_keys {
        let ordinates = ordinates
            .get(node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted an unknown node".into()))?;
        has_z &= ordinates.z.value().is_some();
        has_m &= ordinates.m.value().is_some();
    }
    let axes = CoordinateAxes::new(HasZ(has_z), HasM(has_m));
    if strict && axes != declared_axes {
        return Err(GeometryErrorKind::ordinate_dropped(operation.name()));
    }
    let mut points = points;
    let mut lines = lines;
    apply_axes(
        topology.sources.frame,
        &ordinates,
        &live_node_keys,
        axes,
        &mut points,
        &mut lines,
        &mut polygons,
    )?;
    if whole_sphere {
        return match polygons.len() {
            0 => Err(GeometryErrorKind::Invalid(
                "Q selected the whole sphere without source area polygons".into(),
            )
            .into()),
            1 => {
                Ok(Shape::Polygon(polygons.pop().expect(
                    "a one-polygon whole-sphere output has its polygon",
                )))
            },
            _ => Shape::geometry_collection(polygons.into_iter().map(Shape::Polygon).collect()),
        };
    }
    build_certified_overlay_shape_with_axes(points, lines, polygons, axes)
}

const fn selects_support(
    count: usize,
    first: bool,
    row_count: usize,
    operation: OverlayOp,
) -> bool {
    match operation {
        OverlayOp::Union => count != 0,
        OverlayOp::Intersection => count == row_count,
        OverlayOp::Difference => first && count == 1,
        OverlayOp::SymmetricDifference => !count.is_multiple_of(2),
    }
}

/// Incident faces can differ only on rows owning an incident source edge.
/// Count their union from one face and those rows, without expanding ancestors.
#[derive(Default)]
struct AreaSupport {
    faces: Vec<GlobalFaceId>,
    rows: Vec<SourceRowId>,
}

impl AreaSupport {
    fn clear(&mut self) {
        self.faces.clear();
        self.rows.clear();
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "span indices come from this topology's span table or admitted darts"
    )]
    fn append_span(&mut self, topology: &ExactGeographicTopology, span: usize) -> Result<()> {
        for owner in &topology.q.spans[span].owners {
            let edge = topology
                .sources
                .edges
                .get(owner.source_edge.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q support references an unknown source edge".into())
                })?;
            self.rows.push(edge.row);
        }
        Ok(())
    }

    fn contains(&self, q: &super::q::ExactGeographicQ, row: SourceRowId) -> bool {
        self.faces
            .iter()
            .any(|&face| q.face_rows.contains(face, row))
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "the union counts distinct admitted source rows"
    )]
    fn count(&mut self, q: &super::q::ExactGeographicQ) -> usize {
        let Some(&first) = self.faces.first() else {
            return 0;
        };
        self.rows.sort_unstable();
        self.rows.dedup();
        q.face_rows.count(first)
            + self
                .rows
                .iter()
                .filter(|&&row| !q.face_rows.contains(first, row) && self.contains(q, row))
                .count()
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "sorted unique support rows add only rows absent from the area union"
    )]
    fn with_rows(&mut self, q: &super::q::ExactGeographicQ, rows: &[SourceRowId]) -> (usize, bool) {
        let count = self.count(q) + rows.iter().filter(|&&row| !self.contains(q, row)).count();
        let first = rows.binary_search(&SourceRowId(0)).is_ok() || self.contains(q, SourceRowId(0));
        (count, first)
    }
}

fn append_unique_rows(output: &mut Vec<SourceRowId>, left: &[SourceRowId], right: &[SourceRowId]) {
    output.clear();
    output.extend_from_slice(left);
    output.extend_from_slice(right);
    output.sort_unstable();
    output.dedup();
}

fn select_faces(
    q: &super::q::ExactGeographicQ,
    row_count: usize,
    operation: OverlayOp,
) -> Result<Vec<bool>> {
    let mut selected = Vec::with_capacity(q.global_face_count);
    for face in 0..q.global_face_count {
        let face = GlobalFaceId(u32::try_from(face).map_err(|_| ResourceError::SizeOverflow)?);
        selected.push(selects_support(
            q.face_rows.count(face),
            q.face_rows.contains(face, SourceRowId(0)),
            row_count,
            operation,
        ));
    }
    Ok(selected)
}

struct FaceUnion {
    parents: Vec<usize>,
    selected: Vec<bool>,
}

impl FaceUnion {
    fn new(selected: &[bool]) -> Self {
        Self {
            parents: (0..selected.len()).collect(),
            selected: selected.to_vec(),
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "Face IDs enter the union only from its selected-face table"
    )]
    fn find(&mut self, mut face: usize) -> usize {
        while self.parents[face] != face {
            self.parents[face] = self.parents[self.parents[face]];
            face = self.parents[face];
        }
        face
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "Both joined face IDs were admitted against the selected-face table"
    )]
    fn join(&mut self, left: usize, right: usize) {
        let (left, right) = (self.find(left), self.find(right));
        if left != right {
            self.parents[left.max(right)] = left.min(right);
        }
    }

    fn selected_root(&mut self, face: GlobalFaceId) -> Result<usize> {
        let index = face.0 as usize;
        let selected = self.selected.get(index).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q boundary references an unknown global face".into())
        })?;
        if !selected {
            return Err(GeometryErrorKind::Invalid(
                "Q boundary references an unselected global face".into(),
            )
            .into());
        }
        Ok(self.find(index))
    }
}

fn face_regions(q: &super::q::ExactGeographicQ, selected: &[bool]) -> Result<FaceUnion> {
    if selected.len() != q.global_face_count {
        return Err(GeometryErrorKind::Invalid(
            "Q face selection does not match its global face table".into(),
        )
        .into());
    }
    let mut regions = FaceUnion::new(selected);
    for faces in &q.span_global_faces {
        let Some([left, right]) = faces else {
            continue;
        };
        let left = left.0 as usize;
        let right = right.0 as usize;
        let left_selected = selected.get(left).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q span references an unknown global face".into())
        })?;
        let right_selected = selected.get(right).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q span references an unknown global face".into())
        })?;
        if left_selected && right_selected {
            regions.join(left, right);
        }
    }
    Ok(regions)
}

#[expect(
    clippy::indexing_slicing,
    reason = "global face indices and the selected-face table belong to the same admitted Q"
)]
fn select_line_spans(
    topology: &ExactGeographicTopology,
    selected_faces: &[bool],
    row_count: usize,
    operation: OverlayOp,
) -> Result<Vec<bool>> {
    let q = &topology.q;
    let mut selected = Vec::with_capacity(q.spans.len());
    let mut area = AreaSupport::default();
    let mut curve_rows = Vec::new();
    for (span_index, faces) in q.span_global_faces.iter().enumerate() {
        if q.span_is_collapsed_pole(span_index)? {
            selected.push(false);
            continue;
        }
        area.clear();
        if let Some([left, right]) = faces {
            if selected_faces[left.0 as usize] || selected_faces[right.0 as usize] {
                selected.push(false);
                continue;
            }
            area.faces.extend([*left, *right]);
            area.append_span(topology, span_index)?;
        }
        append_unique_rows(
            &mut curve_rows,
            q.line_rows_for_span(span_index)?,
            q.collapsed_rows_for_span(span_index)?,
        );
        let curve_selected = match operation {
            OverlayOp::Union => !curve_rows.is_empty(),
            OverlayOp::Intersection => area.with_rows(q, &curve_rows).0 == row_count,
            OverlayOp::Difference => {
                curve_rows.binary_search(&SourceRowId(0)).is_ok()
                    && area.with_rows(q, &curve_rows).0 == 1
            },
            OverlayOp::SymmetricDifference => !curve_rows.len().is_multiple_of(2),
        };
        selected.push(curve_selected);
    }
    Ok(selected)
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "node, dart, local and global face indices and fan offsets are admitted by this Q"
)]
fn select_points(
    topology: &ExactGeographicTopology,
    selected_faces: &[bool],
    selected_spans: &[bool],
    row_count: usize,
    operation: OverlayOp,
) -> Result<Vec<QNodeId>> {
    let q = &topology.q;
    let mut line_nodes = vec![false; q.nodes.len()];
    for (span, &selected) in q.spans.iter().zip(selected_spans) {
        if selected {
            for node in span.nodes {
                *line_nodes.get_mut(node.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q span references an unknown node".into())
                })? = true;
            }
        }
    }

    let mut output = Vec::new();
    let mut area = AreaSupport::default();
    let mut support_rows = Vec::new();
    for (node_index, node) in q.nodes.iter().enumerate() {
        if line_nodes[node_index] {
            continue;
        }
        area.clear();
        support_rows.clear();
        let fan = &q.outgoing[q.outgoing_offsets[node_index]..q.outgoing_offsets[node_index + 1]];
        if fan.is_empty() {
            let face = q.source_node_faces[node_index].ok_or_else(|| {
                GeometryErrorKind::Invalid("an isolated Q node has no host face".into())
            })?;
            area.faces.push(face);
        } else {
            for &dart in fan {
                let span_index = q.darts[dart.0 as usize].span.0 as usize;
                support_rows.extend_from_slice(q.line_rows_for_span(span_index)?);
                support_rows.extend_from_slice(q.collapsed_rows_for_span(span_index)?);
                let local = q.dart_faces[dart.0 as usize];
                area.faces.push(q.global_face_of_local[local.0 as usize]);
                area.append_span(topology, span_index)?;
            }
        }
        if area
            .faces
            .iter()
            .any(|face| selected_faces[face.0 as usize])
        {
            continue;
        }
        support_rows.extend_from_slice(q.point_rows_for_node(node_index)?);
        support_rows.sort_unstable();
        support_rows.dedup();
        let (count, first) = area.with_rows(q, &support_rows);
        if selects_support(count, first, row_count, operation) {
            output.push(node.id);
        }
    }
    Ok(output)
}

fn selected_area_boundaries(
    q: &super::q::ExactGeographicQ,
    selected_faces: &[bool],
    regions: &mut FaceUnion,
) -> Result<Vec<BoundaryCycle>> {
    let mut boundary = vec![false; q.darts.len()];
    for (span_index, faces) in q.span_global_faces.iter().enumerate() {
        let Some([left, right]) = faces else {
            continue;
        };
        let left_selected = selected_faces
            .get(left.0 as usize)
            .copied()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q span has an unknown left face".into()))?;
        let right_selected = selected_faces
            .get(right.0 as usize)
            .copied()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q span has an unknown right face".into()))?;
        if left_selected == right_selected {
            continue;
        }
        let forward = q
            .span_darts
            .get(span_index)
            .copied()
            .flatten()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("a selected Q face boundary has no dart".into())
            })?;
        let dart = if left_selected {
            forward
        } else {
            QDartId(forward.0 ^ 1)
        };
        let slot = boundary.get_mut(dart.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("a Q face boundary dart is unknown".into())
        })?;
        *slot = true;
    }

    let mut visited = vec![false; q.darts.len()];
    let mut positions = vec![None; q.nodes.len()];
    let mut walks = Vec::new();
    for (seed, &is_boundary) in boundary.iter().enumerate() {
        let was_visited = visited.get(seed).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q overlay boundary has no visited state".into())
        })?;
        if !is_boundary || was_visited {
            continue;
        }
        let start = QDartId(u32::try_from(seed).map_err(|_| ResourceError::SizeOverflow)?);
        let mut current = start;
        let mut walk = Vec::new();
        loop {
            let current_index = current.0 as usize;
            if visited.get(current_index).copied().unwrap_or(false) {
                if current == start {
                    break;
                }
                return Err(GeometryErrorKind::Invalid(
                    "Q overlay boundary walks intersect before closing".into(),
                )
                .into());
            }
            *visited.get_mut(current_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q overlay boundary dart is unknown".into())
            })? = true;
            let dart = *q.darts.get(current_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q overlay boundary dart is unknown".into())
            })?;
            *positions
                .get_mut(dart.from_node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q boundary starts at an unknown node".into())
                })? = Some(walk.len());
            walk.push(current);

            if let Some(begin) = positions.get(dart.to_node.0 as usize).copied().flatten() {
                let mut cycle = Vec::with_capacity(walk.len().saturating_sub(begin));
                for dart in walk.drain(begin..) {
                    let node = q
                        .darts
                        .get(dart.0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q boundary cycle has an unknown dart".into(),
                            )
                        })?
                        .from_node;
                    *positions.get_mut(node.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q boundary cycle has an unknown node".into())
                    })? = None;
                    cycle.push(dart);
                }
                if !cycle.is_empty() {
                    walks.push(cycle);
                }
            }
            current = next_boundary_dart(q, &boundary, current)?;
            if current == start {
                break;
            }
        }
        if !walk.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "Q overlay boundary did not decompose into closed cycles".into(),
            )
            .into());
        }
    }
    if boundary
        .iter()
        .zip(visited)
        .any(|(&selected, visited)| selected && !visited)
    {
        return Err(GeometryErrorKind::Invalid(
            "Q overlay left a selected boundary dart unvisited".into(),
        )
        .into());
    }

    let mut cycles = Vec::with_capacity(walks.len());
    for darts in walks {
        let first = *darts.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q overlay emitted an empty boundary cycle".into())
        })?;
        let local = *q.dart_faces.get(first.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q boundary cycle has no left face".into())
        })?;
        let face = *q
            .global_face_of_local
            .get(local.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q boundary cycle left face has no global ID".into())
            })?;
        let region = regions.selected_root(face)?;
        cycles.push(BoundaryCycle { darts, region });
    }
    Ok(cycles)
}

fn next_boundary_dart(
    q: &super::q::ExactGeographicQ,
    selected: &[bool],
    dart_id: QDartId,
) -> Result<QDartId> {
    let dart = q
        .darts
        .get(dart_id.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary dart is unknown".into()))?;
    let twin = QDartId(dart_id.0 ^ 1);
    let twin_position = *q
        .dart_positions
        .get(twin.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary twin has no fan position".into()))?;
    let start = *q
        .outgoing_offsets
        .get(dart.to_node.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary node has no fan start".into()))?;
    let end = *q
        .outgoing_offsets
        .get(dart.to_node.0 as usize + 1)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary node has no fan end".into()))?;
    let length = end.checked_sub(start).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q boundary node has reversed fan offsets".into())
    })?;
    if length == 0 || twin_position >= length {
        return Err(
            GeometryErrorKind::Invalid("Q boundary twin is outside its node fan".into()).into(),
        );
    }
    let fan = q
        .outgoing
        .get(start..end)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q node fan is incomplete".into()))?;
    let (before_twin, twin_and_after) = fan.split_at(twin_position);
    let (twin, after_twin) = twin_and_after.split_first().ok_or_else(|| {
        GeometryErrorKind::Invalid("Q boundary twin is outside its node fan".into())
    })?;
    for &candidate in std::iter::once(twin)
        .chain(before_twin.iter().rev())
        .chain(after_twin.iter().rev())
    {
        if selected.get(candidate.0 as usize).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q boundary fan has an unknown dart".into())
        })? {
            return Ok(candidate);
        }
    }
    Err(GeometryErrorKind::Invalid("a selected Q boundary dart has no successor".into()).into())
}

fn source_piece_ranges(topology: &ExactGeographicTopology) -> Result<Vec<Range<usize>>> {
    let mut ranges = vec![0..0; topology.sources.edges.len()];
    for (piece_index, piece) in topology.sources.pieces.iter().enumerate() {
        let range = ranges
            .get_mut(piece.source_edge.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q chart piece has an unknown source edge".into())
            })?;
        if range.start == range.end {
            *range = piece_index..piece_index.saturating_add(1);
        } else if range.end == piece_index {
            range.end = piece_index.saturating_add(1);
        } else {
            return Err(GeometryErrorKind::Invalid(
                "Q source chart pieces are not grouped by source edge".into(),
            )
            .into());
        }
    }
    Ok(ranges)
}

fn source_piece_for_owner<'a>(
    topology: &'a ExactGeographicTopology,
    piece_ranges: &[Range<usize>],
    source_edge: SourceEdgeId,
    source_range: &ExactSourceRange,
) -> Result<&'a ExactChartPiece> {
    let range = piece_ranges
        .get(source_edge.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q owner has an unknown source edge".into()))?;
    for piece_index in range.clone() {
        let piece =
            topology.sources.pieces.get(piece_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source chart piece is unknown".into())
            })?;
        if piece
            .source_range
            .start
            .try_cmp(&source_range.start)?
            .is_le()
            && piece.source_range.end.try_cmp(&source_range.end)?.is_ge()
        {
            return Ok(piece);
        }
    }
    Err(
        GeometryErrorKind::Invalid("Q span owner crosses an original source-chart split".into())
            .into(),
    )
}

fn source_parameter_at_port<'a>(
    span: &super::q::ChartSpan,
    owner: &'a super::q::QSpanOwner,
    port: QPortId,
) -> Result<&'a ExactRational> {
    let endpoint = span
        .ports
        .iter()
        .position(|&candidate| candidate == port)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q port is not on its owned span".into()))?;
    let at_source_start = (endpoint == 0) != owner.reversed;
    Ok(if at_source_start {
        &owner.source_range.start
    } else {
        &owner.source_range.end
    })
}

fn source_edge_at(
    topology: &ExactGeographicTopology,
    source_edge: SourceEdgeId,
) -> Result<&ExactSourceEdge> {
    topology
        .sources
        .edges
        .get(source_edge.0 as usize)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("Q owner has an unknown source edge".into()).into()
        })
}

fn line_owner_index(topology: &ExactGeographicTopology, span_index: usize) -> Result<usize> {
    let q = &topology.q;
    let span = q
        .spans
        .get(span_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q line span is unknown".into()))?;
    let line_rows = q.line_rows_for_span(span_index)?;
    let collapsed_rows = q.collapsed_rows_for_span(span_index)?;
    for (owner_index, owner) in span.owners.iter().enumerate() {
        let edge = source_edge_at(topology, owner.source_edge)?;
        let supported = match edge.kind {
            SourceEdgeKind::Line => line_rows.binary_search(&edge.row).is_ok(),
            SourceEdgeKind::Ring {
                hole: false,
                kind: crate::geometry::RingKind::Collapsed,
                ..
            } => collapsed_rows.binary_search(&edge.row).is_ok(),
            SourceEdgeKind::Ring { .. } => false,
        };
        if supported {
            return Ok(owner_index);
        }
    }
    Err(
        GeometryErrorKind::Invalid("selected Q curve span has no source interval owner".into())
            .into(),
    )
}

fn area_owner_index(topology: &ExactGeographicTopology, span_index: usize) -> Result<usize> {
    let span = topology
        .q
        .spans
        .get(span_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q area span is unknown".into()))?;
    for (owner_index, owner) in span.owners.iter().enumerate() {
        if matches!(
            source_edge_at(topology, owner.source_edge)?.kind,
            SourceEdgeKind::Ring { .. }
        ) {
            return Ok(owner_index);
        }
    }
    Err(GeometryErrorKind::Invalid("selected Q area boundary has no ring owner".into()).into())
}

fn line_steps(topology: &ExactGeographicTopology, selected: &[bool]) -> Result<Vec<Vec<LineStep>>> {
    let q = &topology.q;
    let node_count = q.nodes.len();
    let mut degree = vec![0_usize; node_count];
    let mut incidences = Vec::new();
    let edge_count = selected.iter().filter(|&&keep| keep).count();
    incidences.reserve(
        edge_count
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?,
    );
    for (span_index, (span, &keep)) in q.spans.iter().zip(selected).enumerate() {
        if !keep {
            continue;
        }
        let [start_node, end_node] = span.nodes;
        for (endpoint, node_id) in [
            (SpanEndpoint::Start, start_node),
            (SpanEndpoint::End, end_node),
        ] {
            let node = node_id.0 as usize;
            let next = degree.get_mut(node).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line span has an unknown node".into())
            })?;
            *next = next.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            incidences.push((node, span_index, endpoint));
        }
    }
    incidences.sort_unstable();
    let offset_count = node_count
        .checked_add(1)
        .ok_or(ResourceError::SizeOverflow)?;
    let mut offsets = Vec::with_capacity(offset_count);
    offsets.push(0_usize);
    for count in &degree {
        offsets.push(
            offsets
                .last()
                .copied()
                .expect("line incidence offsets start at zero")
                .checked_add(*count)
                .ok_or(ResourceError::SizeOverflow)?,
        );
    }
    if offsets.last().copied() != Some(incidences.len()) {
        return Err(GeometryErrorKind::Invalid(
            "Q line incidence offsets do not match their entries".into(),
        )
        .into());
    }
    let mut visited = vec![false; q.spans.len()];
    let mut paths = Vec::new();
    for (node, &node_degree) in degree.iter().enumerate() {
        if node_degree == 2 {
            continue;
        }
        let [start, end] = offsets
            .get(node..node.saturating_add(2))
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line node has invalid offsets".into()))?
        else {
            return Err(
                GeometryErrorKind::Invalid("Q line node has invalid offsets".into()).into(),
            );
        };
        let incident = incidences.get(*start..*end).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q line node has an invalid incidence range".into())
        })?;
        for &(_, span, endpoint) in incident {
            let keep = selected.get(span).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line incidence references an unknown span".into())
            })?;
            let already_visited = visited.get(span).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line incidence has no visited state".into())
            })?;
            if keep && !already_visited {
                paths.push(walk_line_steps(
                    topology,
                    &degree,
                    &offsets,
                    &incidences,
                    &mut visited,
                    span,
                    endpoint,
                )?);
            }
        }
    }
    for (span, &keep) in selected.iter().enumerate() {
        let already_visited = visited
            .get(span)
            .copied()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line span has no visited state".into()))?;
        if keep && !already_visited {
            paths.push(walk_line_steps(
                topology,
                &degree,
                &offsets,
                &incidences,
                &mut visited,
                span,
                SpanEndpoint::Start,
            )?);
        }
    }
    Ok(paths)
}

fn walk_line_steps(
    topology: &ExactGeographicTopology,
    degree: &[usize],
    offsets: &[usize],
    incidences: &[(usize, usize, SpanEndpoint)],
    visited: &mut [bool],
    start_span: usize,
    start_endpoint: SpanEndpoint,
) -> Result<Vec<LineStep>> {
    let q = &topology.q;
    let mut steps = Vec::new();
    let (mut span_index, mut from_endpoint) = (start_span, start_endpoint);
    loop {
        let already_visited = visited
            .get_mut(span_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line span is unknown".into()))?;
        if *already_visited {
            if span_index == start_span && from_endpoint == start_endpoint {
                break;
            }
            return Err(GeometryErrorKind::Invalid(
                "Q line traversal entered an earlier span".into(),
            )
            .into());
        }
        *already_visited = true;
        steps.push(LineStep {
            span: span_index,
            from_endpoint,
            owner: line_owner_index(topology, span_index)?,
        });
        let span = q
            .spans
            .get(span_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line span is unknown".into()))?;
        let arrived_endpoint = from_endpoint.opposite();
        let [start_node, end_node] = span.nodes;
        let node = match arrived_endpoint {
            SpanEndpoint::Start => start_node,
            SpanEndpoint::End => end_node,
        }
        .0 as usize;
        if degree.get(node).copied() != Some(2) {
            break;
        }
        let start = *offsets.get(node).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q line node has no incidence start".into())
        })?;
        let end = *offsets
            .get(node + 1)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line node has no incidence end".into()))?;
        let next = incidences
            .get(start..end)
            .and_then(|incident| {
                incident.iter().find(|&&(_, candidate, endpoint)| {
                    candidate != span_index || endpoint != arrived_endpoint
                })
            })
            .copied()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line node has no continuation".into()))?;
        if visited.get(next.1).copied().unwrap_or(false) {
            if next.1 == start_span && next.2 == start_endpoint {
                break;
            }
            return Err(GeometryErrorKind::Invalid(
                "Q line cycle closes on a different used span".into(),
            )
            .into());
        }
        span_index = next.1;
        from_endpoint = next.2;
    }
    if steps.is_empty() {
        return Err(GeometryErrorKind::Invalid("Q line traversal emitted no spans".into()).into());
    }
    Ok(steps)
}

fn source_ordinate_at(
    start: Option<f64>,
    end: Option<f64>,
    parameter: &ExactRational,
) -> Result<Option<f64>> {
    if parameter.is_zero() {
        return Ok(start);
    }
    if parameter.is_one() {
        return Ok(end);
    }
    Ok(interpolate_optional_ordinate(
        start,
        end,
        parameter.round()?,
    ))
}

fn node_ordinates(topology: &ExactGeographicTopology) -> Result<Vec<NodeOrdinates>> {
    let q = &topology.q;
    if q.source_edge_ports.len() != topology.sources.edges.len() {
        return Err(GeometryErrorKind::Invalid(
            "Q source-edge ports do not match their source intervals".into(),
        )
        .into());
    }
    let mut output = vec![NodeOrdinates::default(); q.nodes.len()];
    for (edge, ports) in topology
        .sources
        .edges
        .iter()
        .zip(q.source_edge_ports.iter())
    {
        for (point, &port) in edge.source.iter().zip(ports) {
            let node = q
                .ports
                .get(port.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q source endpoint port is unknown".into())
                })?
                .node;
            let values = output.get_mut(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source endpoint node is unknown".into())
            })?;
            values.z.observe(point.z());
            values.m.observe(point.m());
        }
    }
    for span in &q.spans {
        for owner in &span.owners {
            let edge = source_edge_at(topology, owner.source_edge)?;
            let start_parameter = if owner.reversed {
                &owner.source_range.end
            } else {
                &owner.source_range.start
            };
            let end_parameter = if owner.reversed {
                &owner.source_range.start
            } else {
                &owner.source_range.end
            };
            let start_values = (
                source_ordinate_at(edge.source[0].z(), edge.source[1].z(), start_parameter)?,
                source_ordinate_at(edge.source[0].m(), edge.source[1].m(), start_parameter)?,
            );
            let end_values = (
                source_ordinate_at(edge.source[0].z(), edge.source[1].z(), end_parameter)?,
                source_ordinate_at(edge.source[0].m(), edge.source[1].m(), end_parameter)?,
            );
            let start = output
                .get_mut(span.nodes[0].0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q span start node is unknown".into()))?;
            start.z.observe(start_values.0);
            start.m.observe(start_values.1);
            let end = output
                .get_mut(span.nodes[1].0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q span end node is unknown".into()))?;
            end.z.observe(end_values.0);
            end.m.observe(end_values.1);
        }
    }
    if q.source_point_ports.len() != topology.sources.points.len() {
        return Err(GeometryErrorKind::Invalid(
            "Q source-point ports do not match their source points".into(),
        )
        .into());
    }
    for (source, &port) in topology
        .sources
        .points
        .iter()
        .zip(q.source_point_ports.iter())
    {
        let node = q
            .ports
            .get(port.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q source point port is unknown".into()))?
            .node;
        let values = output
            .get_mut(node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q source point node is unknown".into()))?;
        values.z.observe(source.point.z());
        values.m.observe(source.point.m());
    }
    Ok(output)
}

fn emitted_point(
    topology: &ExactGeographicTopology,
    piece_ranges: &[Range<usize>],
    ordinates: &[NodeOrdinates],
    span_index: usize,
    owner_index: usize,
    port: QPortId,
) -> Result<EmittedVertex> {
    let q = &topology.q;
    let span = q
        .spans
        .get(span_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted span is unknown".into()))?;
    let owner = span
        .owners
        .get(owner_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted owner is unknown".into()))?;
    let parameter = source_parameter_at_port(span, owner, port)?;
    let piece = source_piece_for_owner(
        topology,
        piece_ranges,
        owner.source_edge,
        &owner.source_range,
    )?;
    let exact = exact_source_piece_point(piece, parameter)?;
    let longitude = round_canonical_longitude(topology.sources.frame, &exact.x)?;
    let latitude = Latitude::try_new(topology.sources.frame, exact.y.round()?)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
        .get();
    let node = q
        .ports
        .get(port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted port is unknown".into()))?
        .node;
    let _ = ordinates
        .get(node.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted node has no ordinates".into()))?;
    Ok(EmittedVertex {
        node,
        port,
        point: Point::new_raw_xy(longitude, latitude),
    })
}

fn round_canonical_longitude(
    frame: crate::geographic::GeographicFrame,
    longitude: &ExactRational,
) -> Result<f64> {
    let axis = frame.longitude();
    let half = ExactRational::from_f64(axis.half_period());
    let period = ExactRational::from_f64(axis.period());
    let negative_half = ExactRational::zero().try_subtract(&half)?;
    let canonical = if longitude.try_cmp(&half)?.is_ge() {
        longitude.try_subtract(&period)?
    } else if longitude.try_cmp(&negative_half)?.is_lt() {
        longitude.try_add(&period)?
    } else {
        longitude.clone()
    };
    // A rounded chart alias such as `122.427194 + 360` loses source low bits
    // before floating remainder can canonicalize it.
    axis.canonicalize(canonical.round()?)
        .map(|longitude| longitude.get())
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()).into())
}

fn push_line_part(output: &mut Vec<CoordSeq>, points: &mut Vec<Point>) -> Result<()> {
    if points.len() >= 2 {
        output.push(CoordSeq::try_from_points(points)?);
    }
    points.clear();
    Ok(())
}

fn emit_polygons(
    topology: &ExactGeographicTopology,
    piece_ranges: &[Range<usize>],
    ordinates: &[NodeOrdinates],
    selected_faces: &[bool],
    operation: OverlayOp,
    cycles: &[BoundaryCycle],
    live_node_keys: &mut Vec<(EmittedNodeKey, QNodeId)>,
) -> Result<Vec<Polygon>> {
    let q = &topology.q;
    let face_count = q.global_face_count;
    if !selected_faces.is_empty() && selected_faces.iter().all(|&selected| selected) {
        // A whole-sphere selection has no exterior boundary to trace. Its
        // closed source polygon union is the representable output boundary.
        let mut output = Vec::new();
        for source_polygon in &topology.sources.polygons {
            if !source_polygon_contributes_to_whole_sphere(source_polygon, operation) {
                continue;
            }
            let has_areal_shell = source_polygon
                .rings
                .clone()
                .filter_map(|ring| topology.sources.rings.get(ring))
                .any(|ring| !ring.hole && ring.has_areal_winding());
            if has_areal_shell {
                output.push(emit_source_polygon(
                    topology,
                    source_polygon,
                    live_node_keys,
                )?);
            }
        }
        if output.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "Q selected the whole sphere without source area polygons".into(),
            )
            .into());
        }
        return Ok(output);
    }

    let mut shells = Vec::with_capacity(face_count);
    let mut holes = Vec::with_capacity(face_count);
    shells.resize_with(face_count, Vec::<Ring>::new);
    holes.resize_with(face_count, Vec::<Ring>::new);

    let mut emitted_cycles = vec![false; cycles.len()];
    for source_ring in &topology.sources.rings {
        if !source_ring.summary.exact_axis_band {
            continue;
        }
        let mut source_cycles = Vec::new();
        for (cycle_index, cycle) in cycles.iter().enumerate() {
            if cycle_belongs_to_source_ring(topology, cycle, source_ring.id)? {
                source_cycles.push(cycle_index);
            }
        }
        if source_cycles.len() != 2 {
            continue;
        }
        let Some(&first_cycle) = source_cycles.first() else {
            continue;
        };
        let region = cycles
            .get(first_cycle)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q axis-band cycle is unknown".into()))?
            .region;
        let mut cycles_are_consistent = true;
        for &cycle_index in &source_cycles {
            let cycle = cycles
                .get(cycle_index)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q axis-band cycle is unknown".into()))?;
            let emitted = emitted_cycles.get(cycle_index).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q axis-band cycle has no emission state".into())
            })?;
            if cycle.region != region || emitted {
                cycles_are_consistent = false;
                break;
            }
        }
        if !cycles_are_consistent
            || !source_axis_band_cycles_cover_boundary(
                topology,
                source_ring,
                cycles,
                &source_cycles,
            )?
        {
            continue;
        }
        let Some(follows_source) =
            source_axis_band_cycles_follow_source(topology, source_ring, cycles, &source_cycles)?
        else {
            continue;
        };
        let mut ring = emit_source_ring(topology, source_ring, live_node_keys)?;
        if !follows_source {
            ring = ring.reversed()?;
        }
        let is_hole = chart_ring_is_hole(topology.sources.frame, &ring)?;
        let destination = if is_hole {
            holes.get_mut(region)
        } else {
            shells.get_mut(region)
        }
        .ok_or_else(|| GeometryErrorKind::Invalid("Q axis-band region is unknown".into()))?;
        destination.push(ring);
        for cycle in source_cycles {
            *emitted_cycles.get_mut(cycle).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q axis-band cycle has no emission state".into())
            })? = true;
        }
    }

    for (cycle, &emitted) in cycles.iter().zip(&emitted_cycles) {
        if emitted {
            continue;
        }
        let (ring, is_hole) = emit_ring(topology, piece_ranges, ordinates, cycle, live_node_keys)?;
        let destination = if is_hole {
            holes.get_mut(cycle.region)
        } else {
            shells.get_mut(cycle.region)
        }
        .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary region is unknown".into()))?;
        destination.push(ring);
    }

    let mut output = Vec::new();
    for (region, (mut region_shells, region_holes)) in shells.into_iter().zip(holes).enumerate() {
        if region_shells.is_empty() && region_holes.is_empty() {
            continue;
        }
        if region_shells.len() != 1 {
            return Err(GeometryErrorKind::Invalid(
                format!(
                    "selected Q region {region} has {} shells",
                    region_shells.len(),
                )
                .into(),
            )
            .into());
        }
        let shell = region_shells
            .pop()
            .expect("the selected region has one shell");
        output.push(Polygon::new(shell, region_holes));
    }
    Ok(output)
}

const fn source_polygon_contributes_to_whole_sphere(
    source_polygon: &super::exact::ExactSourcePolygon,
    operation: OverlayOp,
) -> bool {
    match operation {
        OverlayOp::Intersection | OverlayOp::Difference => source_polygon.row.0 == 0,
        OverlayOp::Union | OverlayOp::SymmetricDifference => true,
    }
}

fn cycle_belongs_to_source_ring(
    topology: &ExactGeographicTopology,
    cycle: &BoundaryCycle,
    source_ring: SourceRingId,
) -> Result<bool> {
    let q = &topology.q;
    for &dart_id in &cycle.darts {
        let dart = q
            .darts
            .get(dart_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary dart is unknown".into()))?;
        let span = q
            .spans
            .get(dart.span.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary span is unknown".into()))?;
        let mut owned = false;
        for owner in &span.owners {
            let edge = source_edge_at(topology, owner.source_edge)?;
            if matches!(
                edge.kind,
                SourceEdgeKind::Ring { ring, .. } if ring == source_ring
            ) {
                owned = true;
                break;
            }
        }
        if !owned {
            return Ok(false);
        }
    }
    Ok(true)
}

fn source_axis_band_cycles_follow_source(
    topology: &ExactGeographicTopology,
    source_ring: &ExactSourceRing,
    all_cycles: &[BoundaryCycle],
    cycle_indices: &[usize],
) -> Result<Option<bool>> {
    let q = &topology.q;
    let mut follows_source = None;
    for &cycle_index in cycle_indices {
        let cycle = all_cycles
            .get(cycle_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q axis-band cycle is unknown".into()))?;
        for &dart_id in &cycle.darts {
            let dart = q
                .darts
                .get(dart_id.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary dart is unknown".into()))?;
            let span = q
                .spans
                .get(dart.span.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary span is unknown".into()))?;
            let mut span_direction = None;
            for owner in &span.owners {
                let edge = source_edge_at(topology, owner.source_edge)?;
                if !matches!(
                    edge.kind,
                    SourceEdgeKind::Ring { ring, .. } if ring == source_ring.id
                ) {
                    continue;
                }
                let forward = (dart_id.0 & 1 == 0) != owner.reversed;
                if span_direction.is_some_and(|previous| previous != forward) {
                    return Ok(None);
                }
                span_direction = Some(forward);
            }
            let Some(span_direction) = span_direction else {
                return Ok(None);
            };
            if follows_source.is_some_and(|previous| previous != span_direction) {
                return Ok(None);
            }
            follows_source = Some(span_direction);
        }
    }
    Ok(follows_source)
}

fn source_axis_band_cycles_cover_boundary(
    topology: &ExactGeographicTopology,
    source_ring: &super::exact::ExactSourceRing,
    all_cycles: &[BoundaryCycle],
    cycle_indices: &[usize],
) -> Result<bool> {
    let frame = topology.sources.frame;
    let axis = frame.longitude();
    let south = canonical_bits(source_ring.summary.south.get());
    let north = canonical_bits(source_ring.summary.north.get());
    let mut parallel_edges = Vec::new();
    let mut parallel_mask = 0_u8;
    for edge_index in source_ring.source_edges.clone() {
        let edge =
            topology.sources.edges.get(edge_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source ring edge is unknown".into())
            })?;
        let latitude = canonical_bits(edge.source[0].y);
        if latitude != canonical_bits(edge.source[1].y) {
            continue;
        }
        let parallel = if latitude == south {
            1
        } else if latitude == north {
            2
        } else {
            continue;
        };
        let full_turn = axis
            .full_turn_delta(edge.source[0].x, edge.source[1].x)
            .is_some();
        if !full_turn {
            let start = axis
                .canonicalize(edge.source[0].x)
                .expect("source longitude was admitted")
                .get();
            let end = axis
                .canonicalize(edge.source[1].x)
                .expect("source longitude was admitted")
                .get();
            if start.to_bits() == end.to_bits() {
                continue;
            }
        }
        parallel_mask |= parallel;
        parallel_edges.push(edge.id);
    }
    if parallel_mask != 0b11 {
        return Ok(false);
    }

    for edge_id in parallel_edges {
        let mut ranges = Vec::new();
        for &cycle_index in cycle_indices {
            let cycle = all_cycles
                .get(cycle_index)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q axis-band cycle is unknown".into()))?;
            for &dart_id in &cycle.darts {
                let dart = topology.q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q axis-band dart is unknown".into())
                })?;
                let span = topology.q.spans.get(dart.span.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q axis-band span is unknown".into())
                })?;
                ranges.extend(
                    span.owners
                        .iter()
                        .filter(|owner| owner.source_edge == edge_id)
                        .map(|owner| owner.source_range.clone()),
                );
            }
        }
        if !source_ranges_cover_unit_interval(&mut ranges)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn source_ranges_cover_unit_interval(ranges: &mut [ExactSourceRange]) -> Result<bool> {
    try_sort_by(ranges, |left, right| left.start.try_cmp(&right.start))?;
    let mut covered = ExactRational::zero();
    let one = ExactRational::one();
    for range in ranges {
        if range.start.try_cmp(&covered)?.is_gt() {
            return Ok(false);
        }
        if range.end.try_cmp(&covered)?.is_gt() {
            covered = range.end.clone();
        }
    }
    Ok(covered.try_cmp(&one)?.is_ge())
}

fn emit_source_ring(
    topology: &ExactGeographicTopology,
    source_ring: &super::exact::ExactSourceRing,
    live_node_keys: &mut Vec<(EmittedNodeKey, QNodeId)>,
) -> Result<Ring> {
    let q = &topology.q;
    let mut points = Vec::with_capacity(source_ring.source_edges.len().saturating_add(1));
    for edge_index in source_ring.source_edges.clone() {
        let edge =
            topology.sources.edges.get(edge_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source ring edge is unknown".into())
            })?;
        let ports = q
            .source_edge_ports
            .get(edge.id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q source ring ports are unknown".into()))?;
        for (endpoint, &port_id) in edge.source.iter().zip(ports) {
            let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source ring port is unknown".into())
            })?;
            record_emitted_node(topology.sources.frame, port.node, *endpoint, live_node_keys)?;
        }
        points.push(edge.source[0].with_xy_raw(
            edge.source[0].x,
            Latitude::from_admitted(topology.sources.frame, edge.source[0].y).get(),
        ));
    }
    let last_edge = source_ring
        .source_edges
        .end
        .checked_sub(1)
        .and_then(|index| topology.sources.edges.get(index))
        .ok_or_else(|| GeometryErrorKind::Invalid("Q source ring has no edges".into()))?;
    points.push(last_edge.source[1].with_xy_raw(
        last_edge.source[1].x,
        Latitude::from_admitted(topology.sources.frame, last_edge.source[1].y).get(),
    ));
    Ring::try_from_closed(CoordSeq::try_from_points(&points)?)
}

fn emit_source_polygon(
    topology: &ExactGeographicTopology,
    source_polygon: &super::exact::ExactSourcePolygon,
    live_node_keys: &mut Vec<(EmittedNodeKey, QNodeId)>,
) -> Result<Polygon> {
    let mut shell = None;
    let mut holes = Vec::new();
    for ring_index in source_polygon.rings.clone() {
        let source_ring =
            topology.sources.rings.get(ring_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source polygon ring is unknown".into())
            })?;
        let ring = emit_source_ring(topology, source_ring, live_node_keys)?;
        if source_ring.hole {
            holes.push(ring);
        } else if shell.replace(ring).is_some() {
            return Err(GeometryErrorKind::Invalid(
                "Q source polygon has more than one shell".into(),
            )
            .into());
        }
    }
    Ok(Polygon::new(
        shell.ok_or_else(|| GeometryErrorKind::Invalid("Q source polygon has no shell".into()))?,
        holes,
    ))
}

fn emit_ring(
    topology: &ExactGeographicTopology,
    piece_ranges: &[Range<usize>],
    ordinates: &[NodeOrdinates],
    cycle: &BoundaryCycle,
    live_node_keys: &mut Vec<(EmittedNodeKey, QNodeId)>,
) -> Result<(Ring, bool)> {
    let q = &topology.q;
    let mut pivot = 0;
    for (index, &dart_id) in cycle.darts.iter().enumerate() {
        let dart = q
            .darts
            .get(dart_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary dart is unknown".into()))?;
        let port = q
            .ports
            .get(dart.from_port.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary port is unknown".into()))?;
        if !port.cut {
            pivot = index;
            break;
        }
    }
    let (before_pivot, from_pivot) = cycle.darts.split_at(pivot);
    let mut vertices = Vec::new();
    for &dart_id in from_pivot.iter().chain(before_pivot) {
        let dart = *q
            .darts
            .get(dart_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary dart is unknown".into()))?;
        let owner_index = area_owner_index(topology, dart.span.0 as usize)?;
        for port in [dart.from_port, dart.to_port] {
            let emitted = emitted_point(
                topology,
                piece_ranges,
                ordinates,
                dart.span.0 as usize,
                owner_index,
                port,
            )?;
            if q.ports
                .get(port.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary port is unknown".into()))?
                .cut
            {
                continue;
            }
            if vertices
                .last()
                .is_none_or(|previous: &EmittedVertex| previous.port != emitted.port)
            {
                vertices.push(emitted);
            }
        }
    }
    if vertices.len() < 3 {
        return Err(GeometryErrorKind::Invalid(
            "selected Q area boundary has fewer than three emitted vertices".into(),
        )
        .into());
    }
    if vertices
        .first()
        .is_some_and(|first| vertices.last().is_some_and(|last| last.port != first.port))
    {
        let first = *vertices.first().expect("the boundary has vertices");
        vertices.push(first);
    }
    let mut points = vertices
        .iter()
        .map(|vertex| vertex.point)
        .collect::<Vec<_>>();
    canonicalize_ring_vertices(topology.sources.frame, &vertices, &mut points)?;
    let is_hole = {
        match open_point_cycle_winding(&points) {
            RingWinding::CounterClockwise => false,
            RingWinding::Clockwise => true,
            RingWinding::Degenerate => {
                let chart_ring = Ring::try_from_closed(CoordSeq::try_from_points(&points)?)?;
                chart_ring_is_hole(topology.sources.frame, &chart_ring)?
            },
        }
    };
    for (vertex, point) in vertices.iter().zip(&mut points) {
        *point = vertex.point;
    }
    let first = *points
        .first()
        .expect("selected Q area boundary has at least three vertices");
    *points
        .last_mut()
        .expect("selected Q area boundary has at least three vertices") = first;
    for (vertex, &point) in vertices.iter().zip(&points) {
        record_emitted_node(topology.sources.frame, vertex.node, point, live_node_keys)?;
    }
    Ok((
        Ring::try_from_closed(CoordSeq::try_from_points(&points)?)?,
        is_hole,
    ))
}

fn chart_ring_is_hole(frame: crate::geographic::GeographicFrame, ring: &Ring) -> Result<bool> {
    // Overlay can create cycles with no single source-ring role; classify the emitted chart.
    match ring_winding(ring) {
        RingWinding::CounterClockwise => Ok(false),
        RingWinding::Clockwise => Ok(true),
        RingWinding::Degenerate => {
            let summary = RingSummary::try_new(frame, &GeographicRing::from_ring(ring))
                .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
            if summary.seam_transits == 0 {
                return Err(GeometryErrorKind::Invalid(
                    "selected Q area boundary has no chart winding".into(),
                )
                .into());
            }
            // A polar ring can have zero planar shoelace area; its signed
            // longitude winding places the selected chart side at a pole.
            Ok(if summary.north.get() > 0.0 {
                summary.seam_transits < 0
            } else {
                summary.seam_transits > 0
            })
        },
    }
}

fn emit_lines(
    topology: &ExactGeographicTopology,
    piece_ranges: &[Range<usize>],
    ordinates: &[NodeOrdinates],
    selected: &[bool],
    live_node_keys: &mut Vec<(EmittedNodeKey, QNodeId)>,
) -> Result<Vec<CoordSeq>> {
    let q = &topology.q;
    let mut output = Vec::new();
    for mut path in line_steps(topology, selected)? {
        let mut start = None;
        for (index, step) in path.iter().enumerate() {
            let span = q
                .spans
                .get(step.span)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q line span is unknown".into()))?;
            let [start_port, end_port] = span.ports;
            let from_port = match step.from_endpoint {
                SpanEndpoint::Start => start_port,
                SpanEndpoint::End => end_port,
            };
            let port = q.ports.get(from_port.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line span has an unknown port".into())
            })?;
            if !port.cut {
                start = Some(index);
                break;
            }
        }
        if let Some(start) = start {
            path.rotate_left(start);
        }
        let mut points = Vec::new();
        let mut previous: Option<EmittedVertex> = None;
        for step in path {
            let span = q
                .spans
                .get(step.span)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q line span is unknown".into()))?;
            let [start_port, end_port] = span.ports;
            let (from_port, to_port) = match step.from_endpoint {
                SpanEndpoint::Start => (start_port, end_port),
                SpanEndpoint::End => (end_port, start_port),
            };
            let from = emitted_point(
                topology,
                piece_ranges,
                ordinates,
                step.span,
                step.owner,
                from_port,
            )?;
            let to = emitted_point(
                topology,
                piece_ranges,
                ordinates,
                step.span,
                step.owner,
                to_port,
            )?;
            let from_cut = q
                .ports
                .get(from_port.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q line port is unknown".into()))?
                .cut;
            let to_cut = q
                .ports
                .get(to_port.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q line port is unknown".into()))?
                .cut;

            if !from_cut {
                if let Some(previous) = previous {
                    if previous.node != from.node {
                        return Err(GeometryErrorKind::Invalid(
                            "Q line path changed quotient nodes between spans".into(),
                        )
                        .into());
                    }
                    if previous.point != from.point {
                        let previous_key =
                            emitted_node_key(topology.sources.frame, previous.point.xy())?;
                        let from_key = emitted_node_key(topology.sources.frame, from.point.xy())?;
                        if previous_key != from_key {
                            return Err(GeometryErrorKind::ExactNodeCollision {
                                first: previous.node.0,
                                second: from.node.0,
                            }
                            .into());
                        }
                        let is_seam = matches!(
                            topology
                                .sources
                                .frame
                                .longitude()
                                .canonicalize(from.point.x)
                                .map_err(|error| {
                                    GeometryErrorKind::Invalid(error.to_string().into())
                                })?
                                .identity(topology.sources.frame.longitude()),
                            SeamIdentity::Seam
                        );
                        let is_pole = matches!(
                            from_key,
                            EmittedNodeKey::NorthPole | EmittedNodeKey::SouthPole
                        );
                        if is_seam || is_pole {
                            push_line_part(&mut output, &mut points)?;
                        } else if points.last().is_some_and(|last| *last != previous.point) {
                            return Err(GeometryErrorKind::Invalid(
                                "Q line endpoint aliases disagree away from a quotient singularity"
                                    .into(),
                            )
                            .into());
                        }
                    }
                }
                if points.last().is_none_or(|last| *last != from.point) {
                    points.push(from.point);
                }
                record_emitted_node(
                    topology.sources.frame,
                    from.node,
                    from.point,
                    live_node_keys,
                )?;
            }
            if !to_cut {
                points.push(to.point);
                record_emitted_node(topology.sources.frame, to.node, to.point, live_node_keys)?;
            }
            previous = Some(to);
        }
        push_line_part(&mut output, &mut points)?;
    }
    Ok(output)
}

fn emit_points(
    topology: &ExactGeographicTopology,
    nodes: &[QNodeId],
    live_node_keys: &mut Vec<(EmittedNodeKey, QNodeId)>,
) -> Result<Vec<Point>> {
    let q = &topology.q;
    let mut source_points = vec![None; q.nodes.len()];
    for (source, &port) in topology.sources.points.iter().zip(&q.source_point_ports) {
        let node = q
            .ports
            .get(port.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q source point port is unknown".into()))?
            .node;
        source_points
            .get_mut(node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q source point has an unknown node".into()))?
            .get_or_insert(source.point);
    }
    let mut output = Vec::with_capacity(nodes.len());
    for &node in nodes {
        let point = if let Some(source) = source_points.get(node.0 as usize).copied().flatten() {
            Point::new_raw_xy(
                source.x,
                Latitude::from_admitted(topology.sources.frame, source.y).get(),
            )
        } else {
            let node_data = q
                .nodes
                .get(node.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q point node is unknown".into()))?;
            let port_id = *q.node_ports.get(node_data.ports.start).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q point node has no first port".into())
            })?;
            let port = q
                .ports
                .get(port_id.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q point node has no port".into()))?;
            let longitude = topology
                .sources
                .frame
                .longitude()
                .canonicalize(port.point.x.round()?)
                .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
                .get();
            let latitude = Latitude::try_new(topology.sources.frame, port.point.y.round()?)
                .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
                .get();
            Point::new_raw_xy(longitude, latitude)
        };
        record_emitted_node(topology.sources.frame, node, point, live_node_keys)?;
        output.push(point);
    }
    Ok(output)
}

fn record_emitted_node(
    frame: crate::geographic::GeographicFrame,
    node: QNodeId,
    point: Point,
    live: &mut Vec<(EmittedNodeKey, QNodeId)>,
) -> Result<()> {
    live.push((emitted_node_key(frame, point.xy())?, node));
    Ok(())
}

fn emitted_node_key(
    frame: crate::geographic::GeographicFrame,
    point: XY,
) -> Result<EmittedNodeKey> {
    let longitude = frame
        .longitude()
        .canonicalize(point.x)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    let latitude = Latitude::try_new(frame, point.y)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    match latitude.identity(frame) {
        SeamIdentity::Pole if latitude.get().is_sign_positive() => Ok(EmittedNodeKey::NorthPole),
        SeamIdentity::Pole => Ok(EmittedNodeKey::SouthPole),
        SeamIdentity::Ordinary | SeamIdentity::Seam => Ok(EmittedNodeKey::Coordinate(
            canonical_bits(longitude.get()),
            canonical_bits(latitude.get()),
        )),
    }
}

const fn canonical_bits(value: f64) -> u64 {
    if value == 0.0 {
        0.0_f64.to_bits()
    } else {
        value.to_bits()
    }
}

fn certify_node_injectivity(live: &mut Vec<(EmittedNodeKey, QNodeId)>) -> Result<()> {
    live.sort_unstable_by_key(|&(key, node)| (node, key));
    for pair in live.windows(2) {
        let [(left_key, left_node), (right_key, right_node)] = pair else {
            unreachable!("windows(2) yields adjacent emitted nodes");
        };
        if left_node == right_node && left_key != right_key {
            return Err(GeometryErrorKind::ExactNodeCollision {
                first: left_node.0,
                second: right_node.0,
            }
            .into());
        }
    }
    live.sort_unstable();
    let mut unique: Vec<(EmittedNodeKey, QNodeId)> = Vec::with_capacity(live.len());
    for &(key, node) in live.iter() {
        if let Some(&(previous_key, previous_node)) = unique.last() {
            if previous_key == key && previous_node != node {
                return Err(GeometryErrorKind::ExactNodeCollision {
                    first: previous_node.0,
                    second: node.0,
                }
                .into());
            }
            if previous_key == key && previous_node == node {
                continue;
            }
        }
        unique.push((key, node));
    }
    *live = unique;
    Ok(())
}

fn with_axes(point: Point, ordinates: NodeOrdinates, axes: CoordinateAxes) -> Result<Point> {
    let z = if axes.has_z() {
        Some(ordinates.z.value().ok_or_else(|| {
            GeometryErrorKind::Invalid("retained Q output Z has no source interval".into())
        })?)
    } else {
        None
    };
    let m = if axes.has_m() {
        Some(ordinates.m.value().ok_or_else(|| {
            GeometryErrorKind::Invalid("retained Q output M has no source interval".into())
        })?)
    } else {
        None
    };
    Ok(Point::new_raw_axes(
        point.x,
        point.y,
        ZOrdinate(z),
        MOrdinate(m),
    ))
}

fn output_node_for_point(
    frame: crate::geographic::GeographicFrame,
    live: &[(EmittedNodeKey, QNodeId)],
    point: Point,
) -> Result<QNodeId> {
    let key = emitted_node_key(frame, point.xy())?;
    let index = live
        .binary_search_by_key(&key, |(candidate, _)| *candidate)
        .map_err(|_| {
            GeometryErrorKind::Invalid("Q output coordinate has no exact node identity".into())
        })?;
    live.get(index).map(|(_, node)| *node).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q output coordinate has no exact node identity".into()).into()
    })
}

fn apply_axes(
    frame: crate::geographic::GeographicFrame,
    ordinates: &[NodeOrdinates],
    live: &[(EmittedNodeKey, QNodeId)],
    axes: CoordinateAxes,
    points: &mut [Point],
    lines: &mut [CoordSeq],
    polygons: &mut [Polygon],
) -> Result<()> {
    for point in points {
        let node = output_node_for_point(frame, live, *point)?;
        let source = ordinates
            .get(node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q output point has no ordinates".into()))?;
        *point = with_axes(*point, *source, axes)?;
    }
    for line in lines {
        let mut coordinates = Vec::with_capacity(line.coord_count());
        for point in line.iter_coords() {
            let node = output_node_for_point(frame, live, point)?;
            let source = ordinates.get(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q output line has no ordinates".into())
            })?;
            coordinates.push(with_axes(point, *source, axes)?);
        }
        *line = CoordSeq::try_from_points(&coordinates)?;
    }
    for polygon in polygons {
        let shell = carry_ring_axes(frame, &polygon.shell, ordinates, live, axes)?;
        let mut holes = Vec::with_capacity(polygon.holes.len());
        for hole in polygon.holes.iter() {
            holes.push(carry_ring_axes(frame, hole, ordinates, live, axes)?);
        }
        *polygon = Polygon::new(shell, holes);
    }
    Ok(())
}

fn carry_ring_axes(
    frame: crate::geographic::GeographicFrame,
    ring: &Ring,
    ordinates: &[NodeOrdinates],
    live: &[(EmittedNodeKey, QNodeId)],
    axes: CoordinateAxes,
) -> Result<Ring> {
    let mut coordinates = Vec::with_capacity(ring.coord_count());
    for point in ring.coords().iter_coords() {
        let node = output_node_for_point(frame, live, point)?;
        let source = ordinates
            .get(node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q output ring has no ordinates".into()))?;
        coordinates.push(with_axes(point, *source, axes)?);
    }
    Ring::try_from_closed(CoordSeq::try_from_points(&coordinates)?)
}

fn canonicalize_ring_vertices(
    frame: crate::geographic::GeographicFrame,
    vertices: &[EmittedVertex],
    chart_points: &mut [Point],
) -> Result<()> {
    if vertices.len() != chart_points.len() {
        return Err(GeometryErrorKind::Invalid(
            "Q chart ring vertices do not match their emitted nodes".into(),
        )
        .into());
    }
    let first_vertex = vertices
        .first()
        .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted an empty polygon ring".into()))?;
    let first_point = chart_points
        .first_mut()
        .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted an empty polygon ring".into()))?;
    let latitude = Latitude::try_new(frame, first_vertex.point.y)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    first_point.x = first_vertex.point.x;
    first_point.y = latitude.get();
    let mut reference = first_vertex.point.x;
    for (vertex, point) in vertices.iter().zip(chart_points.iter_mut()).skip(1) {
        let latitude = Latitude::try_new(frame, vertex.point.y)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        point.x = nearest_longitude_alias(frame, vertex.point.x, reference)?;
        point.y = latitude.get();
        reference = point.x;
    }
    let first_vertex = vertices
        .first()
        .expect("an admitted ring has a first vertex");
    let last_vertex = vertices.last().expect("an admitted ring has a last vertex");
    if first_vertex.node != last_vertex.node {
        return Err(GeometryErrorKind::Invalid(
            "Q emitted a polygon ring with distinct endpoint nodes".into(),
        )
        .into());
    }
    let first_point = *chart_points
        .first()
        .expect("an admitted ring has a first chart point");
    *chart_points
        .last_mut()
        .expect("an admitted ring has a last chart point") = first_point;
    Ok(())
}

fn nearest_longitude_alias(
    frame: crate::geographic::GeographicFrame,
    raw: f64,
    reference: f64,
) -> Result<f64> {
    let axis = frame.longitude();
    let canonical = axis
        .canonicalize(raw)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
        .get();
    let period = axis.period();
    [canonical - period, canonical, canonical + period]
        .into_iter()
        .filter(|longitude| longitude.is_finite())
        .min_by(|left, right| {
            (left - reference)
                .abs()
                .total_cmp(&(right - reference).abs())
                .then_with(|| {
                    // At a half-period tie, preserve the alias chosen by Q's lifted chart.
                    (left - raw)
                        .abs()
                        .total_cmp(&(right - raw).abs())
                        .then_with(|| left.total_cmp(right))
                })
        })
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("Q output longitude has no finite chart alias".into()).into()
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geographic::GeographicFrame;

    #[test]
    fn emitted_longitude_reduces_chart_period_before_rounding() {
        let frame = GeographicFrame::crs84();
        let source = 122.427194_f64;
        let period = ExactRational::from_f64(frame.longitude().period());
        let shifted = ExactRational::from_f64(source).try_add(&period).unwrap();
        let rounded_chart_alias = source + frame.longitude().period();

        assert_eq!(
            frame
                .longitude()
                .canonicalize(rounded_chart_alias)
                .unwrap()
                .get()
                .to_bits(),
            source.next_down().to_bits()
        );
        assert_eq!(
            round_canonical_longitude(frame, &shifted)
                .unwrap()
                .to_bits(),
            source.to_bits()
        );
    }
}
