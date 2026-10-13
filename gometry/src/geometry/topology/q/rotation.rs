use super::*;

impl QuotientFaces {
    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "Q IDs and outgoing CSR offsets are created together from checked source spans"
    )]
    pub(in crate::geometry::topology) fn build(
        frame: GeographicFrame,
        chart_data: CutChart,
    ) -> Result<Self> {
        let mut union_find = UnionFind::new(chart_data.nodes.len());
        let mut degree = vec![0_usize; chart_data.nodes.len()];
        let mut face_span = vec![false; chart_data.spans.len()];
        for span in &chart_data.spans {
            if is_collapsed_pole_span(frame, &chart_data, span)? {
                continue;
            }
            face_span[span.id.0 as usize] = true;
            for node in span.nodes {
                let index = node.0 as usize;
                degree[index] = degree[index]
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
            }
            union_find.union(span.nodes[0].0 as usize, span.nodes[1].0 as usize);
        }

        let mut root_components = vec![None; chart_data.nodes.len()];
        let mut node_components = Vec::with_capacity(chart_data.nodes.len());
        let mut component_count = 0_usize;
        for node in 0..chart_data.nodes.len() {
            let root = union_find.find(node);
            let component = if let Some(component) = root_components[root] {
                component
            } else {
                let component = QComponentId(
                    u32::try_from(component_count).map_err(|_| ResourceError::SizeOverflow)?,
                );
                component_count = component_count
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
                root_components[root] = Some(component);
                component
            };
            node_components.push(component);
        }

        let face_span_count = face_span.iter().filter(|&&keep| keep).count();
        let dart_count = face_span_count
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?;
        u32::try_from(dart_count).map_err(|_| ResourceError::SizeOverflow)?;
        let mut darts = Vec::with_capacity(dart_count);
        let mut span_darts = vec![None; chart_data.spans.len()];
        for span in &chart_data.spans {
            if !face_span[span.id.0 as usize] {
                continue;
            }
            let forward =
                QDartId(u32::try_from(darts.len()).map_err(|_| ResourceError::SizeOverflow)?);
            darts.push(QDart {
                span: span.id,
                from_port: span.ports[0],
                to_port: span.ports[1],
                from_node: span.nodes[0],
                to_node: span.nodes[1],
            });
            darts.push(QDart {
                span: span.id,
                from_port: span.ports[1],
                to_port: span.ports[0],
                from_node: span.nodes[1],
                to_node: span.nodes[0],
            });
            span_darts[span.id.0 as usize] = Some(forward);
        }

        let offset_count = chart_data
            .nodes
            .len()
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        let mut outgoing_offsets = Vec::with_capacity(offset_count);
        outgoing_offsets.push(0_usize);
        for count in &degree {
            let next = outgoing_offsets
                .last()
                .copied()
                .expect("the offsets begin with zero")
                .checked_add(*count)
                .ok_or(ResourceError::SizeOverflow)?;
            outgoing_offsets.push(next);
        }
        if outgoing_offsets.last().copied() != Some(darts.len()) {
            return Err(GeometryErrorKind::Invalid(
                "Q fan degrees do not match the dart total".into(),
            )
            .into());
        }
        let mut outgoing = vec![QDartId(0); darts.len()];
        let mut cursor = outgoing_offsets[..chart_data.nodes.len()].to_vec();
        for (index, dart) in darts.iter().enumerate() {
            let node = dart.from_node.0 as usize;
            let position = cursor[node];
            outgoing[position] =
                QDartId(u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?);
            cursor[node] = position.wrapping_add(1);
        }

        for node_index in 0..chart_data.nodes.len() {
            let start = outgoing_offsets[node_index];
            let end = outgoing_offsets[node_index + 1];
            let row = &mut outgoing[start..end];
            try_sort_by(row, |left, right| {
                compare_q_darts(chart_data.face_chart(), &darts, *left, *right)
            })?;
        }
        let mut dart_positions = vec![0_usize; darts.len()];
        for node_index in 0..chart_data.nodes.len() {
            for (position, dart) in outgoing
                [outgoing_offsets[node_index]..outgoing_offsets[node_index + 1]]
                .iter()
                .enumerate()
            {
                dart_positions[dart.0 as usize] = position;
            }
        }

        let mut dart_faces = vec![None; darts.len()];
        let mut local_faces = Vec::new();
        for seed_index in 0..darts.len() {
            if dart_faces[seed_index].is_some() {
                continue;
            }
            let face = QLocalFaceId(
                u32::try_from(local_faces.len()).map_err(|_| ResourceError::SizeOverflow)?,
            );
            let seed = QDartId(u32::try_from(seed_index).map_err(|_| ResourceError::SizeOverflow)?);
            let component = node_components[darts[seed_index].from_node.0 as usize];
            let mut current = seed;
            loop {
                let current_index = current.0 as usize;
                if let Some(previous) = dart_faces[current_index] {
                    if current == seed && previous == face {
                        break;
                    }
                    return Err(GeometryErrorKind::Invalid(
                        "an exact Q face walk reached a dart from another walk".into(),
                    )
                    .into());
                }
                dart_faces[current_index] = Some(face);
                let dart = darts[current_index];
                let twin = QDartId(current.0 ^ 1);
                let twin_position = dart_positions[twin.0 as usize];
                let target = dart.to_node.0 as usize;
                let row_start = outgoing_offsets[target];
                let row_end = outgoing_offsets[target + 1];
                let row_len = row_end - row_start;
                if row_len == 0 {
                    return Err(GeometryErrorKind::Invalid(
                        "an exact Q dart reaches a node with an empty fan".into(),
                    )
                    .into());
                }
                let next_position = twin_position.wrapping_add(row_len).wrapping_sub(1) % row_len;
                current = outgoing[row_start + next_position];
                if current == seed {
                    if dart_faces[current.0 as usize] != Some(face) {
                        return Err(GeometryErrorKind::Invalid(
                            "an exact Q face walk did not close at its seed".into(),
                        )
                        .into());
                    }
                    break;
                }
            }
            local_faces.push(QLocalFace { component });
        }
        let dart_faces = dart_faces
            .into_iter()
            .map(|face| {
                face.ok_or_else(|| {
                    GeometryErrorKind::Invalid("an exact Q dart has no local face".into()).into()
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let mut span_local_faces = vec![None; chart_data.spans.len()];
        for (span, dart) in chart_data.spans.iter().zip(&span_darts) {
            if let Some(dart) = dart {
                span_local_faces[span.id.0 as usize] = Some([
                    dart_faces[dart.0 as usize],
                    dart_faces[(dart.0 ^ 1) as usize],
                ]);
            }
        }
        validate_spherical_components(
            component_count,
            &chart_data.spans,
            &face_span,
            &node_components,
            &degree,
            &local_faces,
        )?;
        let cut_index = QCutIndex::build(
            frame,
            chart_data.face_chart(),
            &node_components,
            component_count,
            &darts,
            &outgoing,
            &outgoing_offsets,
            &dart_faces,
        )?;

        let cut = chart_data.cut;
        let cut_east = chart_data.cut_east;
        Ok(Self {
            cut,
            cut_east,
            ports: chart_data.ports,
            nodes: chart_data.nodes,
            node_ports: chart_data.node_ports,
            spans: chart_data.spans,
            darts,
            span_darts,
            outgoing,
            outgoing_offsets,
            dart_positions,
            dart_faces,
            local_faces,
            node_components,
            span_local_faces,
            source_edge_ports: chart_data.source_edge_ports,
            source_point_ports: chart_data.source_point_ports,
            cut_index,
        })
    }
}

pub(in crate::geometry::topology::q) struct UnionFind {
    pub(in crate::geometry::topology::q) parent: Vec<usize>,
    pub(in crate::geometry::topology::q) size: Vec<usize>,
}

impl UnionFind {
    pub(in crate::geometry::topology::q) fn new(count: usize) -> Self {
        Self {
            parent: (0..count).collect(),
            size: vec![1; count],
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "union-find IDs are admitted from the exact component and local-face counts"
    )]
    pub(in crate::geometry::topology::q) fn find(&mut self, mut node: usize) -> usize {
        while self.parent[node] != node {
            self.parent[node] = self.parent[self.parent[node]];
            node = self.parent[node];
        }
        node
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "union-find roots are produced by find over the same admitted arrays"
    )]
    pub(in crate::geometry::topology::q) fn union(&mut self, left: usize, right: usize) {
        let (mut left, mut right) = (self.find(left), self.find(right));
        if left == right {
            return;
        }
        if self.size[left] < self.size[right] {
            std::mem::swap(&mut left, &mut right);
        }
        self.parent[right] = left;
        self.size[left] = self.size[left].wrapping_add(self.size[right]);
    }
}

pub(in crate::geometry::topology::q) fn is_collapsed_pole_span(
    frame: GeographicFrame,
    chart: &CutChart,
    span: &ChartSpan,
) -> Result<bool> {
    if span.nodes[0] != span.nodes[1] {
        return Ok(false);
    }
    let node = chart
        .nodes
        .get(span.nodes[0].0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q span has an unknown node".into()))?;
    let Some(pole) = node.pole else {
        return Ok(false);
    };
    let latitude = ExactRational::from_f64(frame.chart_pole_latitude(pole));
    let start = chart
        .ports
        .get(span.ports[0].0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q span has an unknown start port".into()))?;
    let end = chart
        .ports
        .get(span.ports[1].0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q span has an unknown end port".into()))?;
    Ok(start.point.y.try_cmp(&latitude)?.is_eq() && end.point.y.try_cmp(&latitude)?.is_eq())
}

fn compare_q_darts(
    chart: QFaceChart<'_>,
    darts: &[QDart],
    left: QDartId,
    right: QDartId,
) -> Result<Ordering> {
    let order = compare_q_darts_with_points(chart, darts, left, right, |port_id| {
        chart
            .ports
            .get(port_id.0 as usize)
            .map(|port| &port.point)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q dart has an unknown port".into()).into())
    })?;
    if !order.is_eq() {
        return Ok(order);
    }
    let left = darts
        .get(left.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q fan references an unknown dart".into()))?;
    let right = darts
        .get(right.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q fan references an unknown dart".into()))?;
    let left_origin = chart
        .ports
        .get(left.from_port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q dart has an unknown origin port".into()))?;
    let left_target = chart
        .ports
        .get(left.to_port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q dart has an unknown target port".into()))?;
    let right_origin = chart
        .ports
        .get(right.from_port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q dart has an unknown origin port".into()))?;
    let right_target = chart
        .ports
        .get(right.to_port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q dart has an unknown target port".into()))?;
    let node = chart
        .nodes
        .get(left.from_node.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q fan has an unknown node".into()))?;
    let (left_ray, right_ray) = if node.pole.is_some() {
        (
            pole_tangent_ray(&left_origin.point, &left_target.point)?,
            pole_tangent_ray(&right_origin.point, &right_target.point)?,
        )
    } else {
        (
            vector_difference(&left_origin.point, &left_target.point)?,
            vector_difference(&right_origin.point, &right_target.point)?,
        )
    };
    Err(coincident_q_ray_error(
        left.from_node,
        left.span,
        right.span,
        &left_ray,
        &right_ray,
    ))
}

pub(in crate::geometry::topology) fn compare_q_darts_with_points<'point>(
    chart: QFaceChart<'_>,
    darts: &[QDart],
    left: QDartId,
    right: QDartId,
    mut point_for_port: impl FnMut(QPortId) -> Result<&'point ExactChartPoint>,
) -> Result<Ordering> {
    let left = darts
        .get(left.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q fan references an unknown dart".into()))?;
    let right = darts
        .get(right.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q fan references an unknown dart".into()))?;
    if left.from_node != right.from_node {
        return Err(GeometryErrorKind::Invalid(
            "Q fan comparator received rays from different nodes".into(),
        )
        .into());
    }
    let node = chart
        .nodes
        .get(left.from_node.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q fan has an unknown node".into()))?;
    let left_origin = point_for_port(left.from_port)?;
    let left_target = point_for_port(left.to_port)?;
    let right_origin = point_for_port(right.from_port)?;
    let right_target = point_for_port(right.to_port)?;

    if let Some(pole) = node.pole {
        let canonical_longitude = |longitude: &ExactRational| -> Result<ExactRational> {
            if longitude.try_cmp(chart.cut_east)?.is_eq() {
                Ok(chart.cut.clone())
            } else {
                Ok(longitude.clone())
            }
        };
        let left_longitude = canonical_longitude(&left_origin.x)?;
        let right_longitude = canonical_longitude(&right_origin.x)?;
        let longitude_order = compare_pole_meridians(pole, &left_longitude, &right_longitude)?;
        if !longitude_order.is_eq() {
            return Ok(longitude_order);
        }
        let left_ray = pole_tangent_ray(left_origin, left_target)?;
        let right_ray = pole_tangent_ray(right_origin, right_target)?;
        return compare_pole_tangent_rays(pole, &left_ray, &right_ray);
    }

    let left_ray = vector_difference(left_origin, left_target)?;
    let right_ray = vector_difference(right_origin, right_target)?;
    compare_exact_rays_allow_equal(&left_ray, &right_ray)
}

pub(in crate::geometry::topology::q) fn coincident_q_ray_error(
    node: QNodeId,
    left_span: ChartSpanId,
    right_span: ChartSpanId,
    left_ray: &(ExactRational, ExactRational),
    right_ray: &(ExactRational, ExactRational),
) -> crate::error::Error {
    GeometryErrorKind::Invalid(
        format!(
            "Q spans {left_span:?} and {right_span:?} leave node {node:?} with coincident tangent rays {left_ray:?} and {right_ray:?}"
        )
        .into(),
    )
    .into()
}

pub(in crate::geometry::topology::q) fn compare_exact_rays(
    left: &(ExactRational, ExactRational),
    right: &(ExactRational, ExactRational),
) -> Result<Ordering> {
    let order = compare_exact_rays_allow_equal(left, right)?;
    if order.is_eq() {
        return Err(GeometryErrorKind::Invalid(
            "two exact Q spans leave a quotient port in the same tangent direction".into(),
        )
        .into());
    }
    Ok(order)
}

pub(in crate::geometry::topology::q) fn compare_exact_rays_allow_equal(
    left: &(ExactRational, ExactRational),
    right: &(ExactRational, ExactRational),
) -> Result<Ordering> {
    let half_order = exact_ray_half(left).cmp(&exact_ray_half(right));
    if !half_order.is_eq() {
        return Ok(half_order);
    }
    match vector_cross(left, right)?.sign() {
        ExactSign::Positive => Ok(Ordering::Less),
        ExactSign::Negative => Ok(Ordering::Greater),
        ExactSign::Zero => Ok(Ordering::Equal),
    }
}

pub(in crate::geometry::topology::q) fn exact_ray_half(ray: &(ExactRational, ExactRational)) -> u8 {
    u8::from(
        !(ray.1.sign() == ExactSign::Positive
            || (ray.1.is_zero() && ray.0.sign() != ExactSign::Negative)),
    )
}

pub(in crate::geometry::topology::q) fn pole_tangent_ray(
    origin: &ExactChartPoint,
    target: &ExactChartPoint,
) -> Result<(ExactRational, ExactRational)> {
    let dx = target.x.try_subtract(&origin.x)?;
    let latitude_delta = target.y.try_subtract(&origin.y)?;
    let absolute_latitude_delta = if latitude_delta.sign() == ExactSign::Negative {
        ExactRational::zero().try_subtract(&latitude_delta)?
    } else {
        latitude_delta
    };
    let dy = ExactRational::zero().try_subtract(&absolute_latitude_delta)?;
    Ok((dx, dy))
}

pub(in crate::geometry::topology::q) fn compare_q_dart_to_direction(
    chart: QFaceChart<'_>,
    darts: &[QDart],
    dart: QDartId,
    direction: &(ExactRational, ExactRational),
) -> Result<Ordering> {
    let dart = darts.get(dart.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q direction references an unknown dart".into())
    })?;
    let node = chart.nodes.get(dart.from_node.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q direction references an unknown node".into())
    })?;
    if node.pole.is_some() {
        return Err(GeometryErrorKind::Invalid(
            "a planar direction cannot order a pole fan".into(),
        )
        .into());
    }
    let origin = chart
        .ports
        .get(dart.from_port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q direction has no origin port".into()))?;
    let target = chart
        .ports
        .get(dart.to_port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q direction has no target port".into()))?;
    compare_exact_rays(&vector_difference(&origin.point, &target.point)?, direction)
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "the Q outgoing range is the admitted CSR fan for this dense node ID"
)]
pub(in crate::geometry::topology::q) fn face_in_direction(
    chart: QFaceChart<'_>,
    darts: &[QDart],
    outgoing: &[QDartId],
    offsets: &[usize],
    dart_faces: &[QLocalFaceId],
    node: QNodeId,
    direction: &(ExactRational, ExactRational),
) -> Result<QLocalFaceId> {
    let node_index = node.0 as usize;
    let start = *offsets.get(node_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q face query references an unknown node".into())
    })?;
    let end = *offsets.get(node_index + 1).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q face query references an incomplete fan".into())
    })?;
    if start == end {
        return Err(
            GeometryErrorKind::Invalid("Q face query reached an isolated node".into()).into(),
        );
    }
    let mut low = start;
    let mut high = end;
    while low < high {
        let middle = low + (high - low) / 2;
        let dart = outgoing[middle];
        match compare_q_dart_to_direction(chart, darts, dart, direction)? {
            Ordering::Less => low = middle + 1,
            Ordering::Equal => {
                return Err(GeometryErrorKind::Invalid(
                    "Q face germ overlaps an exact source span".into(),
                )
                .into());
            },
            Ordering::Greater => high = middle,
        }
    }
    let preceding = if low == start { end - 1 } else { low - 1 };
    let dart = outgoing[preceding];
    dart_faces
        .get(dart.0 as usize)
        .copied()
        .ok_or_else(|| GeometryErrorKind::Invalid("Q face query found no dart face".into()).into())
}

pub(in crate::geometry::topology::q) fn compare_pole_dart_to_longitude(
    chart: QFaceChart<'_>,
    darts: &[QDart],
    dart: QDartId,
) -> Result<Ordering> {
    let dart = darts
        .get(dart.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("pole fan references an unknown dart".into()))?;
    let node = chart
        .nodes
        .get(dart.from_node.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("pole fan references an unknown node".into()))?;
    let pole = node.pole.ok_or_else(|| {
        GeometryErrorKind::Invalid("pole fan query reached an ordinary node".into())
    })?;
    let origin = chart
        .ports
        .get(dart.from_port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("pole dart has no origin port".into()))?;
    let target = chart
        .ports
        .get(dart.to_port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("pole dart has no target port".into()))?;
    let longitude = if origin.point.x.try_cmp(chart.cut_east)?.is_eq() {
        chart.cut.clone()
    } else {
        origin.point.x.clone()
    };
    let order = compare_pole_meridians(pole, &longitude, chart.cut)?;
    if !order.is_eq() {
        return Ok(order);
    }
    let cut_germ = (ExactRational::zero(), ExactRational::from_f64(-1.0));
    compare_pole_tangent_rays(
        pole,
        &pole_tangent_ray(&origin.point, &target.point)?,
        &cut_germ,
    )
}

pub(in crate::geometry::topology::q) fn compare_pole_meridians(
    pole: ChartPole,
    left: &ExactRational,
    right: &ExactRational,
) -> Result<Ordering> {
    let order = left.try_cmp(right)?;
    Ok(if pole == ChartPole::South {
        order.reverse()
    } else {
        order
    })
}

/// The south-pole quotient reverses the complete circular order, including
/// the tangent tie within one meridian.
pub(in crate::geometry::topology::q) fn compare_pole_tangent_rays(
    pole: ChartPole,
    left: &(ExactRational, ExactRational),
    right: &(ExactRational, ExactRational),
) -> Result<Ordering> {
    let order = compare_exact_rays_allow_equal(left, right)?;
    Ok(if pole == ChartPole::South {
        order.reverse()
    } else {
        order
    })
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "the Q outgoing range is the admitted CSR fan for this dense pole node ID"
)]
pub(in crate::geometry::topology::q) fn face_at_pole_longitude(
    chart: QFaceChart<'_>,
    darts: &[QDart],
    outgoing: &[QDartId],
    offsets: &[usize],
    dart_faces: &[QLocalFaceId],
    node: QNodeId,
) -> Result<QLocalFaceId> {
    let node_index = node.0 as usize;
    let start = *offsets.get(node_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("pole query references an unknown node".into())
    })?;
    let end = *offsets.get(node_index + 1).ok_or_else(|| {
        GeometryErrorKind::Invalid("pole query references an incomplete fan".into())
    })?;
    if start == end {
        return Err(
            GeometryErrorKind::Invalid("pole query reached an isolated pole".into()).into(),
        );
    }
    let mut low = start;
    let mut high = end;
    while low < high {
        let middle = low + (high - low) / 2;
        match compare_pole_dart_to_longitude(chart, darts, outgoing[middle])? {
            Ordering::Less => low = middle + 1,
            Ordering::Equal => {
                return Err(GeometryErrorKind::Invalid(
                    "the selected cut overlaps a pole tangent germ".into(),
                )
                .into());
            },
            Ordering::Greater => high = middle,
        }
    }
    let preceding = if low == start { end - 1 } else { low - 1 };
    let dart = outgoing[preceding];
    dart_faces.get(dart.0 as usize).copied().ok_or_else(|| {
        GeometryErrorKind::Invalid("pole query found no incident face".into()).into()
    })
}

#[expect(
    clippy::indexing_slicing,
    reason = "component IDs are dense ordinals used to index the parallel Euler counts"
)]
pub(in crate::geometry::topology::q) fn validate_spherical_components(
    component_count: usize,
    spans: &[ChartSpan],
    face_spans: &[bool],
    node_components: &[QComponentId],
    degrees: &[usize],
    faces: &[QLocalFace],
) -> Result<()> {
    let mut vertices = vec![0_usize; component_count];
    let mut edges = vec![0_usize; component_count];
    let mut face_counts = vec![0_usize; component_count];
    for (node, &degree) in degrees.iter().enumerate() {
        if degree != 0 {
            let component = node_components[node].0 as usize;
            vertices[component] = vertices[component]
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
    }
    for (span, &is_face_span) in spans.iter().zip(face_spans) {
        if is_face_span {
            let component = node_components[span.nodes[0].0 as usize].0 as usize;
            edges[component] = edges[component]
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
    }
    for face in faces {
        let component = face.component.0 as usize;
        face_counts[component] = face_counts[component]
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
    }
    for component in 0..component_count {
        if edges[component] == 0 {
            continue;
        }
        let euler = vertices[component]
            .checked_add(face_counts[component])
            .and_then(|count| count.checked_sub(edges[component]));
        if euler != Some(2) {
            return Err(GeometryErrorKind::Invalid(
                "an exact geographic quotient component is not spherical".into(),
            )
            .into());
        }
    }
    Ok(())
}
