use crate::error::Result;
use crate::geometry::overlay::{
    ExactLineEmission, ExactSourceInventory, OverlayOp, Strictness, merge_chains, overlay_all,
};
use crate::geometry::{
    CoordSeq, CoordinateAxes, GeometryErrorKind, GeometryKind, HashMap, HashSet, LineSeq, Lineal,
    PointKey, Segment, Shape, TopologyDomain, carry_ordinates, undirected_segment_edge_key,
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

    /// Dispatch an overlay op. `overlay_all` owns the operation's
    /// dimension-typed empty normalization for binary and n-ary inputs, so a
    /// vanishing result remains `POLYGON EMPTY` for a canceled polygon
    /// difference rather than an untyped collection among array siblings.
    pub(crate) fn overlay(
        &self,
        other: &Self,
        op: OverlayOp,
        strictness: Strictness,
    ) -> Result<Self> {
        self.overlay_inner(other, op, strictness)
    }

    /// Mixed-dimension set overlay over X/Y. Binary puntal operations,
    /// line/polygon intersections, and line-minus-polygon differences use
    /// dimension-specific lanes; other inputs use the exact partition. New
    /// vertices restore Z/M from source provenance, or drop an unavailable axis
    /// when `strict` is false.
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
    /// Cartesian inputs use the exact partition except for binary puntal unions,
    /// which can be resolved directly. Points and lines survive only where the
    /// selected operation does not absorb them. Z/M is restored afterwards
    /// unless `strict`.
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

    /// Frame-aware union. Geographic inputs use their frame-aware overlay
    /// route; Cartesian inputs use the exact partition except for binary
    /// puntal unions, which can be resolved directly.
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

    /// Frame-aware n-ary intersection. Geographic inputs use their frame-aware
    /// overlay route; Cartesian inputs use the exact partition except for
    /// binary puntal and line/polygon combinations, which can be resolved
    /// directly.
    #[cfg(test)]
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

    /// Frame-aware n-ary symmetric difference. Geographic inputs use their
    /// frame-aware overlay route; binary Cartesian puntal combinations can be
    /// resolved directly.
    #[cfg(test)]
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
    pub(crate) fn node_with_metric<M: crate::geometry::ChartMetric>(
        &self,
        domain: TopologyDomain,
        strictness: Strictness,
        metric_access: Option<&crate::geometry::SourceArcMetricAccess<'_, M>>,
    ) -> Result<Self> {
        let inventory = ExactSourceInventory::from_shapes(&[self])?;
        let mut budget = crate::geometry::ExpansionBudget::new("node", "exact partition");
        let mut partition = inventory.partition(&mut budget)?;
        if let TopologyDomain::Geographic(frame) = domain {
            partition.set_geographic_frame(frame);
        }
        inventory.node_shape(
            &partition,
            self.axes(),
            strictness.is_strict(),
            metric_access,
        )
    }

    pub fn line_merge(&self) -> Result<Self> {
        let merged = match self.lineal().ok_or(GeometryErrorKind::LinealRequired)? {
            Lineal::LineString(_) | Lineal::Empty => self.clone(),
            Lineal::MultiLineString(lines) => {
                // Dissolve the component lines into maximal chains at degree-2
                // nodes, splitting every Y/T/X junction (degree `>= 3`). The
                // overlay line sink uses the same [`merge_chains`] rule.
                let chains = merge_chains(lines)?;
                // A merge that produced nothing keeps the lineal type
                // (`MULTILINESTRING EMPTY`), unlike the general narrowing.
                if chains.is_empty() {
                    Self::typed_empty(GeometryKind::MultiLineString, CoordinateAxes::XY)
                } else {
                    let mut proven = Vec::with_capacity(chains.len());
                    for line in chains {
                        proven.push(LineSeq::from_proven(line));
                    }
                    Self::narrow_lines(lines.axes(), proven)
                }
            },
        };
        carry_ordinates(merged, &[self], "line_merge", false)
    }

    pub fn shared_paths(&self, other: &Self) -> Result<Self> {
        self.linework()
            .map_err(|_| GeometryErrorKind::LinealRequired)?;
        other
            .linework()
            .map_err(|_| GeometryErrorKind::LinealRequired)?;
        let inventory = ExactSourceInventory::from_shapes(&[self, other])?;
        let mut budget = crate::geometry::ExpansionBudget::new("shared_paths", "exact partition");
        let partition = inventory.partition(&mut budget)?;
        let emission = inventory.line_emission(&partition, "shared_paths")?;
        let (same, opposite) = shared_paths_from_exact(&emission)?;
        let mut same_lines = Vec::with_capacity(same.len());
        for line in same {
            same_lines.push(LineSeq::from_proven(line));
        }
        let mut opposite_lines = Vec::with_capacity(opposite.len());
        for line in opposite {
            opposite_lines.push(LineSeq::from_proven(line));
        }
        let result_parts = vec![
            Self::multi_line_string(CoordinateAxes::XY, same_lines),
            Self::multi_line_string(CoordinateAxes::XY, opposite_lines),
        ];
        let result = Self::geometry_collection(result_parts)?;
        carry_ordinates(result, &[self, other], "shared_paths", false)
    }
}

fn shared_paths_from_exact(
    emission: &ExactLineEmission<'_>,
) -> Result<(Vec<CoordSeq>, Vec<CoordSeq>)> {
    let mut same = Vec::new();
    let mut opposite = Vec::new();
    let mut same_path_direction =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    let mut opposite_path_direction =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    let mut same_keys = HashSet::with_hasher(crate::collections::collection_hash_state()?.clone());
    let mut opposite_keys =
        HashSet::with_hasher(crate::collections::collection_hash_state()?.clone());
    same.reserve(emission.len());
    opposite.reserve(emission.len());
    for map in [&mut same_path_direction, &mut opposite_path_direction] {
        map.reserve(emission.len());
    }
    for set in [&mut same_keys, &mut opposite_keys] {
        set.reserve(emission.len());
    }

    for (index, segment) in emission.segments().enumerate() {
        let mut left_direction = None;
        let mut left_mask = 0_u8;
        let mut right_mask = 0_u8;
        for source in emission.sources(index) {
            let direction = !source.reversed;
            match source.operand {
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
        let key = undirected_segment_edge_key(segment);
        let segment = if left_direction {
            segment
        } else {
            Segment {
                start: segment.end,
                end: segment.start,
            }
        };
        let line = CoordSeq::try_from_points(&[segment.start.point(), segment.end.point()])?;
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

    Ok((
        orient_shared_paths(merge_chains(&same)?, &same_path_direction)?,
        orient_shared_paths(merge_chains(&opposite)?, &opposite_path_direction)?,
    ))
}

fn orient_shared_paths(
    paths: Vec<CoordSeq>,
    directions: &HashMap<(PointKey, PointKey), Segment>,
) -> Result<Vec<CoordSeq>> {
    let mut oriented = Vec::with_capacity(paths.len());
    for path in paths {
        let start = path.first().expect("shared path has a start").xy();
        let end = path.point_at(1).xy();
        let key = undirected_segment_edge_key(Segment { start, end });
        let Some(expected) = directions.get(&key) else {
            oriented.push(path);
            continue;
        };
        if start == expected.start && end == expected.end {
            oriented.push(path);
        } else {
            let mut points = Vec::with_capacity(path.len());
            points.extend(path.iter().rev());
            oriented.push(CoordSeq::try_from_points(&points)?);
        }
    }
    Ok(oriented)
}
