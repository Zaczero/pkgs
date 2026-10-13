use super::{
    Bounds, CanonicalQNodeKey, De9im, Dimension, ExactChartPoint, ExactGeographicPointLocation,
    ExactGeographicQ, ExactGeographicRectangleLocation, ExactGeographicTopology, ExactRational,
    GeographicFrame, GeometryErrorKind, GlobalFaceId, Loc, Point, QNodeId, QNodeKey, QRowLocations,
    ResourceError, Result, Shape, SourceRowId, ValidationIssue, validity,
};

impl QRowLocations<'_> {
    pub(in crate::geometry::topology) fn face(&self, face: GlobalFaceId) -> Loc {
        location_in_global_face(&self.topology.q, face, self.row).geometry
    }

    pub(in crate::geometry::topology) fn span(&self, span_index: usize) -> Result<Option<Loc>> {
        let span = self.topology.q.spans.get(span_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q row location has an unknown span".into())
        })?;
        if span.nodes[0] == span.nodes[1] {
            return Ok(None);
        }
        Ok(Some(
            locate_q_span(&self.topology.q, span_index, self.row).geometry,
        ))
    }

    pub(in crate::geometry::topology) fn node(&self, node: QNodeId) -> Result<Loc> {
        Ok(locate_q_node(&self.topology.q, node, self.row)?.geometry)
    }
}

impl ExactGeographicTopology {
    pub(in crate::geometry::topology) fn canonical_node_key(
        &self,
        node: QNodeId,
    ) -> Result<CanonicalQNodeKey<'_>> {
        let q_node = self.q.nodes.get(node.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("canonical Q coordinate references an unknown node".into())
        })?;
        let QNodeKey::Source(source) = &q_node.key else {
            let QNodeKey::Cut(latitude) = &q_node.key else {
                unreachable!("QNodeKey has exactly Source and Cut variants");
            };
            return Ok(CanonicalQNodeKey::Cut {
                longitude: &self.q.cut,
                latitude,
            });
        };
        self.chart
            .nodes
            .get(source.0 as usize)
            .map(|chart_node| CanonicalQNodeKey::Chart(&chart_node.key))
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "canonical Q coordinate references an unknown chart node".into(),
                )
                .into()
            })
    }

    pub(crate) fn validity_issue(&self, shape: &Shape) -> Result<Option<ValidationIssue>> {
        validity::issue(self, shape)
    }

    pub(crate) fn self_intersection_points(&self, shape: &Shape) -> Result<Vec<Point>> {
        validity::self_intersection_points(self, shape)
    }

    pub(crate) fn relate_pair(&self) -> Result<De9im> {
        exact_pair_relation(self)
    }

    pub(crate) fn locate_point(&self, point: Point) -> Result<ExactGeographicPointLocation> {
        if self.sources.row_count != 1 {
            return Err(GeometryErrorKind::Invalid(
                "an exact geographic point query requires one source row".into(),
            )
            .into());
        }
        let frame = self.sources.frame;
        frame
            .longitude()
            .canonicalize(point.x)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        let latitude = crate::geographic::Latitude::try_new(frame, point.y)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        let point = Point::new_raw_xy(point.x, latitude.get());
        let point = super::super::exact::source_chart_point(frame, point);
        self.q.locate_point(self, &point, SourceRowId(0))
    }

    pub(crate) fn classify_rectangle(
        &self,
        bounds: Bounds,
    ) -> Result<Option<ExactGeographicRectangleLocation>> {
        if self.sources.row_count != 1 {
            return Err(GeometryErrorKind::Invalid(
                "an exact geographic rectangle query requires one source row".into(),
            )
            .into());
        }
        self.q.classify_rectangle(self, bounds, SourceRowId(0))
    }
}

pub(in crate::geometry::topology::q) fn lift_query_point(
    frame: GeographicFrame,
    cut: &ExactRational,
    source_point: &ExactChartPoint,
) -> Result<ExactChartPoint> {
    let period = ExactRational::from_f64(frame.longitude().period());
    let cut_east = cut.try_add(&period)?;
    let mut point = source_point.clone();
    if point.x.try_cmp(cut)?.is_lt() {
        point.x = point.x.try_add(&period)?;
    } else if point.x.try_cmp(&cut_east)?.is_gt() {
        point.x = point.x.try_subtract(&period)?;
    }
    if point.x.try_cmp(cut)?.is_lt() || point.x.try_cmp(&cut_east)?.is_gt() {
        return Err(GeometryErrorKind::Invalid(
            "exact geographic query point is outside the Q chart".into(),
        )
        .into());
    }
    Ok(point)
}

pub(in crate::geometry::topology::q) fn q_face_has_row(
    q: &ExactGeographicQ,
    face: GlobalFaceId,
    row: SourceRowId,
) -> bool {
    q.face_rows.contains(face, row)
}

pub(in crate::geometry::topology::q) const fn area_location(any: bool, all: bool) -> Loc {
    if !any {
        Loc::Exterior
    } else if all {
        Loc::Interior
    } else {
        Loc::Boundary
    }
}

pub(in crate::geometry::topology::q) struct CurveSupport {
    pub(in crate::geometry::topology::q) has_curve: bool,
    pub(in crate::geometry::topology::q) line_boundary: bool,
    pub(in crate::geometry::topology::q) collapsed_degree: bool,
}

pub(in crate::geometry::topology::q) struct PointSupport {
    pub(in crate::geometry::topology::q) area: Loc,
    pub(in crate::geometry::topology::q) curve: CurveSupport,
    pub(in crate::geometry::topology::q) point: bool,
}

pub(in crate::geometry::topology::q) fn point_location_from_support(
    support: &PointSupport,
) -> ExactGeographicPointLocation {
    let geometry = if support.area != Loc::Exterior {
        support.area
    } else if support.curve.has_curve {
        if support.curve.line_boundary ^ support.curve.collapsed_degree {
            Loc::Boundary
        } else {
            Loc::Interior
        }
    } else if support.point {
        Loc::Interior
    } else {
        Loc::Exterior
    };
    ExactGeographicPointLocation {
        geometry,
        area: support.area,
    }
}

pub(in crate::geometry::topology::q) const fn location_in_area_face(
    inside: bool,
) -> ExactGeographicPointLocation {
    let location = if inside { Loc::Interior } else { Loc::Exterior };
    ExactGeographicPointLocation {
        geometry: location,
        area: location,
    }
}

pub(in crate::geometry::topology::q) fn location_in_global_face(
    q: &ExactGeographicQ,
    face: GlobalFaceId,
    row: SourceRowId,
) -> ExactGeographicPointLocation {
    location_in_area_face(q_face_has_row(q, face, row))
}

pub(in crate::geometry::topology::q) fn q_node_fan(
    q: &ExactGeographicQ,
    node_index: usize,
) -> Result<(usize, usize)> {
    let offsets = q
        .outgoing_offsets
        .get(node_index..)
        .and_then(|tail| tail.get(..2))
        .ok_or_else(|| GeometryErrorKind::Invalid("Q node has no outgoing fan".into()))?;
    let [start, end] = offsets else {
        return Err(GeometryErrorKind::Invalid("Q node has an invalid outgoing fan".into()).into());
    };
    Ok((*start, *end))
}

pub(in crate::geometry::topology::q) fn locate_q_node(
    q: &ExactGeographicQ,
    node: QNodeId,
    row: SourceRowId,
) -> Result<ExactGeographicPointLocation> {
    let node_index = node.0 as usize;
    let (start, end) = q_node_fan(q, node_index)?;
    let (area_any, area_all, curve, collapsed_degree) = if start == end {
        let face = q
            .source_node_faces
            .get(node_index)
            .copied()
            .flatten()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("an isolated Q node has no host face".into())
            })?;
        let inside = q_face_has_row(q, face, row);
        (inside, inside, false, false)
    } else {
        let fan = q.outgoing.get(start..end).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q node has an invalid outgoing fan".into())
        })?;
        let (mut area_any, mut area_all) = (false, true);
        let (mut curve, mut collapsed_degree) = (false, false);
        for &dart_id in fan {
            let local = *q.dart_faces.get(dart_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q node fan has an unknown dart face".into())
            })?;
            let global = *q
                .global_face_of_local
                .get(local.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q node dart face has no global identity".into())
                })?;
            let inside = q_face_has_row(q, global, row);
            area_any |= inside;
            area_all &= inside;

            let dart = q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q node fan has an unknown dart".into())
            })?;
            let span_index = dart.span.0 as usize;
            let span = q.spans.get(span_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q node fan has an unknown span".into())
            })?;
            if span.nodes[0] == span.nodes[1] {
                continue;
            }
            let line = q.span_line_rows.contains(span_index, row);
            let collapsed = q.span_collapsed_rows.contains(span_index, row);
            curve |= line || collapsed;
            collapsed_degree ^= collapsed && !line;
        }
        (area_any, area_all, curve, collapsed_degree)
    };
    let line_boundary = q.node_line_boundary_rows.contains(node_index, row);
    let point = q.node_point_rows.contains(node_index, row);
    Ok(point_location_from_support(&PointSupport {
        area: area_location(area_any, area_all),
        curve: CurveSupport {
            has_curve: curve,
            line_boundary,
            collapsed_degree,
        },
        point,
    }))
}

pub(in crate::geometry::topology::q) fn locate_q_span(
    q: &ExactGeographicQ,
    span_index: usize,
    row: SourceRowId,
) -> ExactGeographicPointLocation {
    let Some([left, right]) = q.span_global_faces.get(span_index).copied().flatten() else {
        let curve = q.span_line_rows.contains(span_index, row)
            || q.span_collapsed_rows.contains(span_index, row);
        let location = if curve { Loc::Interior } else { Loc::Exterior };
        return ExactGeographicPointLocation {
            geometry: location,
            area: Loc::Exterior,
        };
    };
    let left_area = q_face_has_row(q, left, row);
    let right_area = q_face_has_row(q, right, row);
    let line = q.span_line_rows.contains(span_index, row);
    let collapsed = q.span_collapsed_rows.contains(span_index, row);
    point_location_from_support(&PointSupport {
        area: area_location(left_area || right_area, left_area && right_area),
        curve: CurveSupport {
            has_curve: line || collapsed,
            line_boundary: false,
            collapsed_degree: false,
        },
        point: false,
    })
}

pub(in crate::geometry::topology) fn row_locations(
    topology: &ExactGeographicTopology,
    row: SourceRowId,
) -> Result<QRowLocations<'_>> {
    if row.0 as usize >= topology.sources.row_count {
        return Err(GeometryErrorKind::Invalid(
            "Q row location references an unknown source row".into(),
        )
        .into());
    }
    Ok(QRowLocations { topology, row })
}

pub(in crate::geometry::topology::q) fn span_crosses_north_ray(
    q: &ExactGeographicQ,
    span_index: usize,
    point: &ExactChartPoint,
    row: SourceRowId,
) -> Result<bool> {
    let Some([left, right]) = q.span_global_faces.get(span_index).copied().flatten() else {
        return Ok(false);
    };
    if q_face_has_row(q, left, row) == q_face_has_row(q, right, row) {
        return Ok(false);
    }
    let span = q
        .spans
        .get(span_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q ray references an unknown span".into()))?;
    let start = q
        .ports
        .get(span.ports[0].0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q ray has no start port".into()))?;
    let end = q
        .ports
        .get(span.ports[1].0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q ray has no end port".into()))?;
    let order = start.point.y.try_cmp(&end.point.y)?;
    if order.is_eq() {
        return Ok(false);
    }
    // The north-side germ gives vertices a half-open crossing rule; only the
    // parity of row occupancy changes, so crossings need no global ordering.
    let (low, high) = if order.is_lt() {
        (&start.point, &end.point)
    } else {
        (&end.point, &start.point)
    };
    if point.y.try_cmp(&low.y)?.is_lt() || !point.y.try_cmp(&high.y)?.is_lt() {
        return Ok(false);
    }
    let delta_y = high.y.try_subtract(&low.y)?;
    let slope = high.x.try_subtract(&low.x)?.try_quotient(&delta_y)?;
    let hit_x = low
        .x
        .try_add(&point.y.try_subtract(&low.y)?.try_product(&slope)?)?;
    Ok(hit_x.try_cmp(&q.cut)?.is_gt() && hit_x.try_cmp(&point.x)?.is_lt())
}

pub(in crate::geometry::topology::q) fn exact_pair_relation(
    topology: &ExactGeographicTopology,
) -> Result<De9im> {
    if topology.sources.row_count != 2 {
        return Err(GeometryErrorKind::Invalid(
            "an exact geographic DE-9IM relation requires two source rows".into(),
        )
        .into());
    }
    let q = &topology.q;
    let left = row_locations(topology, SourceRowId(0))?;
    let right = row_locations(topology, SourceRowId(1))?;
    let mut matrix = De9im::empty_disjoint();

    for face in 0..q.global_face_count {
        let face = GlobalFaceId(u32::try_from(face).map_err(|_| ResourceError::SizeOverflow)?);
        matrix.set_at_least(left.face(face), right.face(face), Dimension::Surface);
    }

    for span_index in 0..q.spans.len() {
        let (Some(left_location), Some(right_location)) =
            (left.span(span_index)?, right.span(span_index)?)
        else {
            continue;
        };
        if left_location != Loc::Exterior || right_location != Loc::Exterior {
            matrix.set_at_least(left_location, right_location, Dimension::Curve);
        }
    }

    for node in &q.nodes {
        let left_location = left.node(node.id)?;
        let right_location = right.node(node.id)?;
        if left_location != Loc::Exterior || right_location != Loc::Exterior {
            matrix.set_at_least(left_location, right_location, Dimension::Point);
        }
    }
    Ok(matrix)
}
