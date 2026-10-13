use super::{
    ChartPole, ChartSpan, CutIntervalId, ExactGeographicChartInventory, ExactRational,
    GeographicFrame, GeometryErrorKind, GlobalFaceId, HeapSize, Ordering, QComponentCutRuns,
    QComponentId, QCutEvent, QCutIndex, QCutInterval, QDart, QDartId, QFaceChart, QLocalFaceId,
    QNodeId, QNodeKey, QPortId, QSpanOwner, QuotientFaces, ResourceError, Result, RingTransition,
    SourceEdgeId, SourceEdgeKind, face_at_pole_longitude, face_in_direction, rational_heap_bytes,
    try_sort_by,
};

impl QCutIndex {
    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "Q node, component, event, and CSR ordinals are admitted from the exact graph columns"
    )]
    pub(in crate::geometry::topology::q) fn build(
        frame: GeographicFrame,
        chart: QFaceChart<'_>,
        node_components: &[QComponentId],
        component_count: usize,
        darts: &[QDart],
        outgoing: &[QDartId],
        outgoing_offsets: &[usize],
        dart_faces: &[QLocalFaceId],
    ) -> Result<Self> {
        let mut events = Vec::new();
        let mut south_pole_face = None;
        let mut north_pole_face = None;
        for node in chart.nodes {
            let start = outgoing_offsets[node.id.0 as usize];
            let end = outgoing_offsets[node.id.0 as usize + 1];
            if start == end {
                continue;
            }
            if let Some(pole) = node.pole {
                let face = face_at_pole_longitude(
                    chart,
                    darts,
                    outgoing,
                    outgoing_offsets,
                    dart_faces,
                    node.id,
                )?;
                let value = (node_components[node.id.0 as usize], face);
                let slot = match pole {
                    ChartPole::South => &mut south_pole_face,
                    ChartPole::North => &mut north_pole_face,
                };
                if slot.replace(value).is_some() {
                    return Err(GeometryErrorKind::Invalid(
                        "Q contains more than one quotient node at a pole".into(),
                    )
                    .into());
                }
                continue;
            }
            if let QNodeKey::Cut(y) = &node.key {
                events.push(QCutEvent {
                    y: y.clone(),
                    node: node.id,
                    component: node_components[node.id.0 as usize],
                    south_face: face_in_direction(
                        chart,
                        darts,
                        outgoing,
                        outgoing_offsets,
                        dart_faces,
                        node.id,
                        &(ExactRational::zero(), ExactRational::from_f64(-1.0)),
                    )?,
                    north_face: face_in_direction(
                        chart,
                        darts,
                        outgoing,
                        outgoing_offsets,
                        dart_faces,
                        node.id,
                        &(ExactRational::zero(), ExactRational::one()),
                    )?,
                });
            }
        }
        try_sort_by(&mut events, |left, right| left.y.try_cmp(&right.y))?;
        for pair in events.windows(2) {
            let [left, right] = pair else {
                unreachable!("windows(2) yields adjacent Q cut events");
            };
            if left.y.try_cmp(&right.y)?.is_eq() {
                return Err(GeometryErrorKind::Invalid(
                    "two exact Q cut events have the same quotient latitude".into(),
                )
                .into());
            }
        }

        let mut local_event_indices = (0..events.len()).collect::<Vec<_>>();
        try_sort_by(&mut local_event_indices, |left, right| {
            let left = &events[*left];
            let right = &events[*right];
            let component = left.component.cmp(&right.component);
            Ok(if component.is_eq() {
                left.y.try_cmp(&right.y)?
            } else {
                component
            })
        })?;
        let mut component_event_counts = vec![0_usize; component_count];
        for &event in &local_event_indices {
            let component = events[event].component.0 as usize;
            component_event_counts[component] = component_event_counts[component]
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
        let mut component_event_offsets = Vec::with_capacity(
            component_count
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?,
        );
        component_event_offsets.push(0_usize);
        for count in component_event_counts {
            component_event_offsets.push(
                component_event_offsets
                    .last()
                    .copied()
                    .expect("component offsets start at zero")
                    .checked_add(count)
                    .ok_or(ResourceError::SizeOverflow)?,
            );
        }

        let mut root_ports = vec![None; component_count];
        for port in chart.ports {
            if chart.nodes[port.node.0 as usize].pole.is_some()
                || outgoing_offsets[port.node.0 as usize]
                    == outgoing_offsets[port.node.0 as usize + 1]
            {
                continue;
            }
            let component = node_components[port.node.0 as usize].0 as usize;
            if root_ports[component].is_none() {
                root_ports[component] = Some(port.id);
            }
        }
        let west = (ExactRational::from_f64(-1.0), ExactRational::zero());
        let mut component_runs = Vec::with_capacity(component_count);
        let mut run_faces = Vec::with_capacity(
            events
                .len()
                .checked_add(component_count)
                .ok_or(ResourceError::SizeOverflow)?,
        );
        for component in 0..component_count {
            let event_range =
                component_event_offsets[component]..component_event_offsets[component + 1];
            let local_events = &local_event_indices[event_range.clone()];
            let face_start = run_faces.len();
            if local_events.is_empty() {
                let component_id = QComponentId(
                    u32::try_from(component).map_err(|_| ResourceError::SizeOverflow)?,
                );
                let south = south_pole_face
                    .filter(|(owner, _)| *owner == component_id)
                    .map(|(_, face)| face);
                let north = north_pole_face
                    .filter(|(owner, _)| *owner == component_id)
                    .map(|(_, face)| face);
                if let (Some(south), Some(north)) = (south, north)
                    && south != north
                {
                    return Err(GeometryErrorKind::Invalid(
                        "a Q component changes its cut face without a cut event".into(),
                    )
                    .into());
                }
                let face = if let Some(face) = south.or(north) {
                    Some(face)
                } else if let Some(port) = root_ports[component] {
                    let node = chart.ports[port.0 as usize].node;
                    Some(face_in_direction(
                        chart,
                        darts,
                        outgoing,
                        outgoing_offsets,
                        dart_faces,
                        node,
                        &west,
                    )?)
                } else {
                    None
                };
                if let Some(face) = face {
                    run_faces.push(face);
                }
            } else {
                let first = &events[local_events[0]];
                let component_id = first.component;
                let south_pole = south_pole_face
                    .filter(|(owner, _)| *owner == component_id)
                    .map(|(_, face)| face);
                let mut current = south_pole.unwrap_or(first.south_face);
                if current != first.south_face {
                    let pole_node = chart
                        .nodes
                        .iter()
                        .find(|node| {
                            node.pole == Some(ChartPole::South)
                                && node_components.get(node.id.0 as usize) == Some(&component_id)
                        })
                        .map(|node| node.id);
                    return Err(GeometryErrorKind::Invalid(
                        format!(
                            "south-pole cut wedge {south_pole:?} at node {pole_node:?} disagrees with component {component_id:?} cut event {:?} at latitude {:?} below-face {:?}",
                            first.node, first.y, first.south_face,
                        )
                        .into(),
                    )
                    .into());
                }
                run_faces.push(current);
                for &event_id in local_events {
                    let event = &events[event_id];
                    if event.south_face != current {
                        return Err(GeometryErrorKind::Invalid(
                            "a Q cut run changed face without an event".into(),
                        )
                        .into());
                    }
                    current = event.north_face;
                    run_faces.push(current);
                }
                let north_pole = north_pole_face
                    .filter(|(owner, _)| *owner == component_id)
                    .map(|(_, face)| face);
                if north_pole.is_some_and(|face| face != current) {
                    return Err(GeometryErrorKind::Invalid(
                        "the last Q cut run disagrees with the north-pole wedge".into(),
                    )
                    .into());
                }
            }
            component_runs.push(QComponentCutRuns {
                event_range,
                face_range: face_start..run_faces.len(),
            });
        }

        let south_y = ExactRational::from_f64(frame.chart_pole_latitude(ChartPole::South));
        let north_y = ExactRational::from_f64(frame.chart_pole_latitude(ChartPole::North));
        let interval_count = events
            .len()
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        let mut intervals = Vec::with_capacity(interval_count);
        for index in 0..interval_count {
            let low = if index == 0 {
                south_y.clone()
            } else {
                events[index - 1].y.clone()
            };
            let high = if index == events.len() {
                north_y.clone()
            } else {
                events[index].y.clone()
            };
            if !low.try_cmp(&high)?.is_lt() {
                return Err(GeometryErrorKind::Invalid(
                    "Q cut intervals are not strictly ordered".into(),
                )
                .into());
            }
            let low_event = index.checked_sub(1);
            let high_event = (index < events.len()).then_some(index);
            let (low_face, low_component) = low_event
                .map_or(south_pole_face, |event| {
                    Some((events[event].component, events[event].north_face))
                })
                .map_or((None, None), |(component, face)| {
                    (Some(face), Some(component))
                });
            let (high_face, high_component) = high_event
                .map_or(north_pole_face, |event| {
                    Some((events[event].component, events[event].south_face))
                })
                .map_or((None, None), |(component, face)| {
                    (Some(face), Some(component))
                });
            intervals.push(QCutInterval {
                id: CutIntervalId(u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?),
                low,
                high,
                low_face,
                low_component,
                high_face,
                high_component,
            });
        }
        Ok(Self {
            events: events.into_boxed_slice(),
            intervals: intervals.into_boxed_slice(),
            local_event_indices: local_event_indices.into_boxed_slice(),
            component_runs: component_runs.into_boxed_slice(),
            run_faces: run_faces.into_boxed_slice(),
            root_ports: root_ports.into_boxed_slice(),
        })
    }

    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "Q component and event IDs index the dense component-local cut runs built above"
    )]
    pub(in crate::geometry::topology::q) fn local_face_at(
        &self,
        component: QComponentId,
        latitude: &ExactRational,
        side: i8,
    ) -> Result<QLocalFaceId> {
        let runs = self
            .component_runs
            .get(component.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q cut query references an unknown component".into())
            })?;
        if runs.face_range.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "an isolated Q component has no local cut face".into(),
            )
            .into());
        }
        let (mut low, mut high) = (runs.event_range.start, runs.event_range.end);
        while low < high {
            let middle = low + (high - low) / 2;
            let event_id = self.local_event_indices[middle];
            let order = self.events[event_id].y.try_cmp(latitude)?;
            if order.is_lt() || (order.is_eq() && side > 0) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        let face = runs.face_range.start + (low - runs.event_range.start);
        self.run_faces.get(face).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q cut run has no face for the selected interval".into())
                .into()
        })
    }

    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "the binary search maintains low <= high <= the sorted exact event count"
    )]
    pub(in crate::geometry::topology::q) fn interval_at(
        &self,
        latitude: &ExactRational,
        side: i8,
    ) -> Result<&QCutInterval> {
        let mut low = 0_usize;
        let mut high = self.events.len();
        while low < high {
            let middle = low + (high - low) / 2;
            let order = self.events[middle].y.try_cmp(latitude)?;
            if order.is_lt() || (order.is_eq() && side > 0) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        self.intervals.get(low).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q cut query is outside the latitude domain".into()).into()
        })
    }

    pub(in crate::geometry::topology) fn interval_boundary_nodes(
        &self,
    ) -> Vec<[Option<QNodeId>; 2]> {
        self.intervals
            .iter()
            .enumerate()
            .map(|(index, _)| {
                let low = index
                    .checked_sub(1)
                    .and_then(|event| self.events.get(event))
                    .map(|event| event.node);
                let high = self.events.get(index).map(|event| event.node);
                [low, high]
            })
            .collect()
    }

    pub(in crate::geometry::topology::q) fn pole_interval_id(
        &self,
        pole: ChartPole,
    ) -> Result<CutIntervalId> {
        let interval = match pole {
            ChartPole::South => self.intervals.first(),
            ChartPole::North => self.intervals.last(),
        }
        .ok_or_else(|| GeometryErrorKind::Invalid("Q has no pole cut interval".into()))?;
        Ok(interval.id)
    }

    pub(in crate::geometry::topology::q) fn host_face_on_cut(
        &self,
        component: QComponentId,
        reference: QComponentId,
        latitude: &ExactRational,
        side: i8,
    ) -> Result<QLocalFaceId> {
        let interval = self.interval_at(latitude, side)?;
        if interval
            .low_component
            .is_some_and(|owner| owner != component)
            && let Some(face) = interval.low_face
        {
            return Ok(face);
        }
        if interval
            .high_component
            .is_some_and(|owner| owner != component)
            && let Some(face) = interval.high_face
        {
            return Ok(face);
        }
        self.local_face_at(reference, latitude, side)
    }

    pub(in crate::geometry::topology::q) fn validate_global_intervals(
        &self,
        local_to_global: &[GlobalFaceId],
        reference: QComponentId,
    ) -> Result<Vec<GlobalFaceId>> {
        let mut global_faces = Vec::with_capacity(self.intervals.len());
        for (interval_index, interval) in self.intervals.iter().enumerate() {
            let mut global_face = None;
            for local in [interval.low_face, interval.high_face]
                .into_iter()
                .flatten()
            {
                let global = *local_to_global.get(local.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Q cut wedge references an unknown local face".into(),
                    )
                })?;
                if let Some((previous, previous_local)) = global_face
                    && previous != global
                {
                    return Err(GeometryErrorKind::Invalid(
                        format!(
                            "paired Q cut interval {interval_index} [{:?}, {:?}] maps local faces {previous_local:?} and {local:?} to different global faces {previous:?} and {global:?}",
                            interval.low, interval.high
                        )
                        .into(),
                    )
                    .into());
                }
                global_face = Some((global, local));
            }
            let global_face = if let Some((global_face, _)) = global_face {
                global_face
            } else {
                let midpoint = interval.low.try_midpoint(&interval.high)?;
                let local = self.local_face_at(reference, &midpoint, 0)?;
                *local_to_global.get(local.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Q cut interval references an unknown local face".into(),
                    )
                })?
            };
            global_faces.push(global_face);
        }
        Ok(global_faces)
    }
}

pub(in crate::geometry::topology::q) fn apply_cut_event_winding(
    topology: &QuotientFaces,
    node_id: QNodeId,
    winding: &mut [i64],
) -> Result<()> {
    let node = topology
        .nodes
        .get(node_id.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q cut event has an unknown node".into()))?;
    let ports = topology
        .node_ports
        .get(node.ports.clone())
        .ok_or_else(|| GeometryErrorKind::Invalid("Q cut event has an invalid port run".into()))?;
    let mut east_port = None;
    for &port_id in ports {
        let port = topology
            .ports
            .get(port_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q cut event has an unknown port".into()))?;
        if port.cut && port.point.x.try_cmp(&topology.cut_east)?.is_eq() {
            east_port = Some(port.id);
            break;
        }
    }
    let east_port = east_port.ok_or_else(|| {
        GeometryErrorKind::Invalid("Q cut event has no east-side chart port".into())
    })?;
    let start = *topology
        .outgoing_offsets
        .get(node_id.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q cut event has no fan start".into()))?;
    let end = *topology
        .outgoing_offsets
        .get(node_id.0 as usize + 1)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q cut event has no fan end".into()))?;
    let fan = topology
        .outgoing
        .get(start..end)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q cut event has an invalid fan".into()))?;
    for &dart_id in fan {
        let dart = topology
            .darts
            .get(dart_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q cut fan has an unknown dart".into()))?;
        if dart.from_port != east_port {
            continue;
        }
        let span = topology
            .spans
            .get(dart.span.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q cut fan has an unknown span".into()))?;
        for_each_span_ring_transition(&span.owners, &mut |transition| {
            apply_ring_transition(winding, transition, 1)
        })?;
    }
    Ok(())
}

pub(in crate::geometry::topology::q) fn apply_ring_transition(
    winding: &mut [i64],
    transition: RingTransition,
    multiplier: i64,
) -> Result<()> {
    let value = winding
        .get_mut(transition.ring.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q transition has an unknown ring".into()))?;
    let delta = transition
        .delta
        .checked_mul(multiplier)
        .ok_or(ResourceError::SizeOverflow)?;
    *value = value
        .checked_add(delta)
        .ok_or(ResourceError::SizeOverflow)?;
    Ok(())
}

pub(in crate::geometry::topology::q) fn for_each_span_ring_transition(
    owners: &[QSpanOwner],
    visit: &mut impl FnMut(RingTransition) -> Result<()>,
) -> Result<()> {
    // The cut chart keeps source-edge order, and each ring's source edges are
    // contiguous in the inventory.
    let mut current: Option<RingTransition> = None;
    for owner in owners {
        let Some(transition) = owner.ring_transition else {
            continue;
        };
        if let Some(previous) = current {
            match transition.ring.cmp(&previous.ring) {
                Ordering::Less => {
                    return Err(GeometryErrorKind::Invalid(
                        "Q span source owners are not ordered by ring".into(),
                    )
                    .into());
                },
                Ordering::Equal => {
                    current = Some(RingTransition {
                        delta: previous
                            .delta
                            .checked_add(transition.delta)
                            .ok_or(ResourceError::SizeOverflow)?,
                        ..previous
                    });
                    continue;
                },
                Ordering::Greater => {
                    if previous.delta != 0 {
                        visit(previous)?;
                    }
                },
            }
        }
        current = Some(transition);
    }
    if let Some(transition) = current
        && transition.delta != 0
    {
        visit(transition)?;
    }
    Ok(())
}

pub(in crate::geometry::topology::q) fn span_has_ring_transition(span: &ChartSpan) -> Result<bool> {
    let mut found = false;
    for_each_span_ring_transition(&span.owners, &mut |_| {
        found = true;
        Ok(())
    })?;
    Ok(found)
}

pub(in crate::geometry::topology::q) fn source_ring_transition(
    sources: &ExactGeographicChartInventory,
    source_edge: SourceEdgeId,
    reversed: bool,
) -> Result<Option<RingTransition>> {
    let edge = sources
        .edges
        .get(source_edge.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q span has an unknown source edge".into()))?;
    let SourceEdgeKind::Ring { ring, .. } = edge.kind else {
        return Ok(None);
    };
    let source_ring = sources
        .rings
        .get(ring.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q span has an unknown source ring".into()))?;
    if !source_ring.has_areal_winding() {
        return Ok(None);
    }
    Ok(Some(RingTransition {
        ring,
        delta: if reversed { 1 } else { -1 },
    }))
}

impl HeapSize for QCutIndex {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "HeapSize sums the already-owned cut-index buffers as an estimate"
    )]
    fn heap_bytes(&self) -> usize {
        self.events.len() * std::mem::size_of::<QCutEvent>()
            + self
                .events
                .iter()
                .map(|event| rational_heap_bytes(&event.y))
                .sum::<usize>()
            + self.intervals.len() * std::mem::size_of::<QCutInterval>()
            + self
                .intervals
                .iter()
                .map(|interval| {
                    rational_heap_bytes(&interval.low) + rational_heap_bytes(&interval.high)
                })
                .sum::<usize>()
            + self.local_event_indices.len() * std::mem::size_of::<usize>()
            + self.component_runs.len() * std::mem::size_of::<QComponentCutRuns>()
            + self.run_faces.len() * std::mem::size_of::<QLocalFaceId>()
            + self.root_ports.len() * std::mem::size_of::<Option<QPortId>>()
    }
}
impl QuotientFaces {
    pub(in crate::geometry::topology) fn cut_interval_boundary_nodes(
        &self,
    ) -> Vec<[Option<QNodeId>; 2]> {
        self.cut_index.interval_boundary_nodes()
    }
}
