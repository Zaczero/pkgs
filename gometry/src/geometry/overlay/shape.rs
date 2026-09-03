#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use ahash::{HashMapExt as _, HashSetExt as _};

use crate::error::Result;
use crate::geometry::overlay::{
    OverlayOp, Strictness, empty_overlay_shape, merge_chains, overlay_all,
};
use crate::geometry::{
    CoordSeq, CoordinateAxes, GeometryErrorKind, GeometryKind, HashMap, HashSet, LineSeq, PointKey,
    Segment, Shape, TopologyDomain, TopologyGraph, carry_ordinates, segment_projection,
    undirected_segment_edge_key,
};
impl Shape {
    pub(crate) fn intersection(&self, other: &Self, strictness: Strictness) -> Result<Self> {
        self.overlay(other, OverlayOp::Intersection, strictness)
    }

    pub(crate) fn union(&self, other: &Self, strictness: Strictness) -> Result<Self> {
        self.overlay(other, OverlayOp::Union, strictness)
    }

    pub(crate) fn difference(&self, other: &Self, strictness: Strictness) -> Result<Self> {
        self.overlay(other, OverlayOp::Difference, strictness)
    }

    pub(crate) fn symmetric_difference(
        &self,
        other: &Self,
        strictness: Strictness,
    ) -> Result<Self> {
        self.overlay(other, OverlayOp::SymmetricDifference, strictness)
    }

    /// Dispatch an overlay op, then normalize an empty result to the op's
    /// dimension-typed empty (see [`empty_overlay_shape`]). A vanishing
    /// intersection/difference/symdiff/union participates in gometry's
    /// typed-empty model — `POLYGON EMPTY` for a polygon−polygon difference
    /// that cancels, not an untyped `GEOMETRYCOLLECTION EMPTY` stray among
    /// typed siblings in an array result.
    pub(crate) fn overlay(
        &self,
        other: &Self,
        op: OverlayOp,
        strictness: Strictness,
    ) -> Result<Self> {
        let result = self.overlay_inner(other, op, strictness)?;
        Ok(if result.is_empty() {
            empty_overlay_shape(self, other, op)
        } else {
            result
        })
    }

    /// Mixed-dimension set overlay over X/Y, restoring Z/M afterwards via the
    /// provenance resolver (or dropping it when `strict`). Binary and n-ary
    /// entry points use the same all-input arrangement and assembly.
    pub(crate) fn overlay_inner(
        &self,
        other: &Self,
        op: OverlayOp,
        strictness: Strictness,
    ) -> Result<Self> {
        overlay_all(&[self, other], TopologyDomain::Cartesian, op, strictness)
    }

    /// Dissolve every geometry into one mixed-dimension union.
    ///
    /// All inputs share one topology graph and arrangement; points and lines
    /// survive only where the selected set operation does not absorb them.
    /// Z/M is restored afterwards unless `strict`.
    ///
    /// Planar-only internal path (no frame). Prefer
    /// [`Self::union_all_topo`] when a geographic frame is known so
    /// antimeridian-crossing operands are split first (same gate as binary
    /// overlay).
    pub(crate) fn union_all<S: std::borrow::Borrow<Self>>(
        geometries: &[S],
        strictness: Strictness,
    ) -> Result<Self> {
        Self::union_all_topo(geometries, TopologyDomain::Cartesian, strictness)
    }

    /// Frame-aware n-ary union. All operands are assembled in one arrangement.
    pub(crate) fn union_all_topo<S: std::borrow::Borrow<Self>>(
        geometries: &[S],
        domain: TopologyDomain,
        strictness: Strictness,
    ) -> Result<Self> {
        if geometries.is_empty() {
            return Err(GeometryErrorKind::EmptyGeometrySequence {
                operation: "union_all",
            }
            .into());
        }
        overlay_all(geometries, domain, OverlayOp::Union, strictness)
    }

    /// Frame-aware n-ary intersection. All operands are assembled in one
    /// arrangement.
    pub(crate) fn intersection_all_topo<S: std::borrow::Borrow<Self>>(
        geometries: &[S],
        domain: TopologyDomain,
        strictness: Strictness,
    ) -> Result<Self> {
        if geometries.is_empty() {
            return Err(GeometryErrorKind::EmptyGeometrySequence {
                operation: "intersection_all",
            }
            .into());
        }
        overlay_all(geometries, domain, OverlayOp::Intersection, strictness)
    }

    /// Frame-aware n-ary symmetric difference. All operands are assembled in
    /// one arrangement.
    pub(crate) fn symmetric_difference_all_topo<S: std::borrow::Borrow<Self>>(
        geometries: &[S],
        domain: TopologyDomain,
        strictness: Strictness,
    ) -> Result<Self> {
        if geometries.is_empty() {
            return Err(GeometryErrorKind::EmptyGeometrySequence {
                operation: "symmetric_difference_all",
            }
            .into());
        }
        overlay_all(
            geometries,
            domain,
            OverlayOp::SymmetricDifference,
            strictness,
        )
    }

    /// Node this geometry's linework: split every edge at all intersections
    /// (crossings and shared points) and return the noded edges as a
    /// `MultiLineString` (GEOS/PostGIS `ST_Node`). Polygon boundaries are
    /// included; point-only input yields `MULTILINESTRING EMPTY`. Z/M is
    pub(crate) fn node(&self, strictness: Strictness) -> Result<Self> {
        let graph = TopologyGraph::cartesian(self);
        let noded = graph
            .edges
            .iter()
            .map(|edge| CoordSeq::from(vec![edge.segment.start.point(), edge.segment.end.point()]))
            .map(|line| LineSeq::try_new(line).expect("noded segment has two vertices"))
            .collect::<Vec<_>>();
        let noded = Self::multi_line_string(self.axes(), noded);
        carry_ordinates(noded, &[self], "node", strictness.is_strict())
    }

    pub fn line_merge(&self) -> Result<Self> {
        let merged = match self {
            Self::LineString(_) => self.clone(),
            Self::MultiLineString(lines) => {
                // Dissolve the component lines into maximal chains at degree-2
                // nodes, splitting every Y/T/X junction (degree `>= 3`) — the
                // JTS LineMerger contract, shared verbatim with the overlay line
                // sink via [`merge_chains`].
                let mut chains =
                    merge_chains(lines.iter().map(|line| line.as_coords().clone()).collect());
                match chains.len() {
                    0 => Self::typed_empty(GeometryKind::MultiLineString, CoordinateAxes::XY),
                    1 => Self::LineString(
                        LineSeq::try_new(chains.pop().expect("len == 1"))
                            .expect("merged chain is lineal"),
                    ),
                    _ => Self::multi_line_string(
                        lines.axes(),
                        chains
                            .into_iter()
                            .map(|line| LineSeq::try_new(line).expect("merged chain is lineal"))
                            .collect(),
                    ),
                }
            },
            _ => return Err(GeometryErrorKind::LinealRequired.into()),
        };
        carry_ordinates(merged, &[self], "line_merge", false)
    }

    pub fn shared_paths(&self, other: &Self) -> Result<Self> {
        self.linework()
            .map_err(|_| GeometryErrorKind::LinealRequired)?;
        other
            .linework()
            .map_err(|_| GeometryErrorKind::LinealRequired)?;
        let graph = TopologyGraph::cartesian_many(&[self, other]);
        let (same, opposite) = shared_paths_from_graph(&graph);
        let same = same
            .into_iter()
            .map(|line| LineSeq::try_new(line).expect("shared path is lineal"))
            .collect::<Vec<_>>();
        let opposite = opposite
            .into_iter()
            .map(|line| LineSeq::try_new(line).expect("shared path is lineal"))
            .collect::<Vec<_>>();
        let result = Self::geometry_collection(vec![
            Self::multi_line_string(CoordinateAxes::XY, same),
            Self::multi_line_string(CoordinateAxes::XY, opposite),
        ])?;
        carry_ordinates(result, &[self, other], "shared_paths", false)
    }
}

fn shared_paths_from_graph(graph: &TopologyGraph) -> (Vec<CoordSeq>, Vec<CoordSeq>) {
    let mut same = Vec::new();
    let mut opposite = Vec::new();
    let mut same_path_direction = HashMap::new();
    let mut opposite_path_direction = HashMap::new();
    let mut same_keys = HashSet::new();
    let mut opposite_keys = HashSet::new();

    for edge in &graph.edges {
        let mut left_direction = None;
        let mut left_mask = 0_u8;
        let mut right_mask = 0_u8;
        for &source_index in &edge.sources {
            let source = &graph.source_edges[source_index as usize];
            let direction = source_direction(edge.segment, source.segment);
            match source.source_row {
                0 => {
                    left_direction.get_or_insert(direction);
                    left_mask |= if direction { 1 } else { 2 };
                },
                1 => {
                    right_mask |= if direction { 1 } else { 2 };
                },
                _ => {},
            }
        }
        let Some(left_direction) = left_direction else {
            continue;
        };
        let has_same_direction = left_mask & right_mask != 0;
        let has_opposite_direction = (left_mask & 1 != 0 && right_mask & 2 != 0)
            || (left_mask & 2 != 0 && right_mask & 1 != 0);
        if !has_same_direction && !has_opposite_direction {
            continue;
        }
        let key = undirected_segment_edge_key(edge.segment);
        let segment = if left_direction {
            edge.segment
        } else {
            Segment {
                start: edge.segment.end,
                end: edge.segment.start,
            }
        };
        let line = CoordSeq::from(vec![segment.start.point(), segment.end.point()]);
        if has_opposite_direction {
            opposite_keys.insert(key);
            opposite_path_direction.insert(key, segment);
            opposite.push(line);
        } else {
            same_keys.insert(key);
            same_path_direction.insert(key, segment);
            same.push(line);
        }
    }

    same.retain(|line| {
        let start = line.first().expect("shared path atom has a start");
        let end = line.point_at(1);
        !opposite_keys.contains(&undirected_segment_edge_key(Segment {
            start: start.xy(),
            end: end.xy(),
        }))
    });
    same_keys.retain(|key| !opposite_keys.contains(key));
    same_path_direction.retain(|key, _| same_keys.contains(key));

    (
        orient_shared_paths(merge_chains(same), &same_path_direction),
        orient_shared_paths(merge_chains(opposite), &opposite_path_direction),
    )
}

fn source_direction(piece: Segment, source: Segment) -> bool {
    let start = segment_projection(piece.start, source);
    let end = segment_projection(piece.end, source);
    end.cmp_along(&start).is_ge()
}

fn orient_shared_paths(
    paths: Vec<CoordSeq>,
    directions: &HashMap<(PointKey, PointKey), Segment>,
) -> Vec<CoordSeq> {
    paths
        .into_iter()
        .map(|path| {
            let start = path.first().expect("shared path has a start").xy();
            let end = path.point_at(1).xy();
            let key = undirected_segment_edge_key(Segment { start, end });
            let Some(expected) = directions.get(&key) else {
                return path;
            };
            if start == expected.start && end == expected.end {
                path
            } else {
                path.iter().rev().collect()
            }
        })
        .collect()
}
