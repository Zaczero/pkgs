use super::*;

impl ExactGeographicQ {
    pub(in crate::geometry::topology) fn build(
        frame: GeographicFrame,
        sources: &ExactGeographicChartInventory,
        chart: &NodedExactChart,
    ) -> Result<Self> {
        if sources.frame != frame {
            return Err(GeometryErrorKind::Invalid(
                "exact geographic Q frame differs from its source inventory".into(),
            )
            .into());
        }
        let cut = select_cut(frame, chart)?;
        let cut_chart = make_cut_chart(frame, sources, chart, &cut)?;
        let topology = QuotientFaces::build(frame, cut_chart)?;
        let global = build_global_face_topology(frame, sources, &topology)?;
        let GlobalFaceRowProducts {
            face_rows,
            span_collapsed_rows,
            node_point_rows,
            face_validity,
            seam_owner_sides,
        } = GlobalFaceRowOccupancy::build(global.count, sources, &topology, &global)?;
        let span_line_rows = build_span_line_rows(sources, &topology.spans)?;
        let node_line_boundary_rows = build_node_line_boundary_rows(sources, &topology)?;
        let spatial_index = QSpatialIndex::build(
            &topology.ports,
            &topology.nodes,
            &topology.spans,
            &node_point_rows,
        )?;
        Ok(Self {
            cut,
            ports: topology.ports.into_boxed_slice(),
            nodes: topology.nodes.into_boxed_slice(),
            node_ports: topology.node_ports.into_boxed_slice(),
            spans: topology.spans.into_boxed_slice(),
            darts: topology.darts.into_boxed_slice(),
            span_darts: topology.span_darts.into_boxed_slice(),
            outgoing: topology.outgoing.into_boxed_slice(),
            outgoing_offsets: topology.outgoing_offsets.into_boxed_slice(),
            dart_positions: topology.dart_positions.into_boxed_slice(),
            dart_faces: topology.dart_faces.into_boxed_slice(),
            local_faces: topology.local_faces.into_boxed_slice(),
            node_components: topology.node_components.into_boxed_slice(),
            span_local_faces: topology.span_local_faces.into_boxed_slice(),
            source_edge_ports: topology.source_edge_ports.into_boxed_slice(),
            source_point_ports: topology.source_point_ports.into_boxed_slice(),
            cut_index: topology.cut_index,
            component_hosts: global.component_hosts.into_boxed_slice(),
            global_face_of_local: global.local_to_global.into_boxed_slice(),
            span_global_faces: global.span_global_faces.into_boxed_slice(),
            cut_interval_faces: global.cut_interval_faces.into_boxed_slice(),
            source_node_faces: global.source_node_faces.into_boxed_slice(),
            source_point_faces: global.source_point_faces.into_boxed_slice(),
            global_face_count: global.count,
            face_rows,
            face_validity,
            seam_owner_sides,
            span_line_rows,
            span_collapsed_rows,
            node_point_rows,
            node_line_boundary_rows,
            spatial_index,
            #[cfg(test)]
            host_status_comparisons: global.host_status_comparisons,
            #[cfg(test)]
            host_status_max_height: global.host_status_max_height,
            #[cfg(test)]
            seed_face: global.seed_face,
            #[cfg(test)]
            seed_winding: global.seed_winding.into_boxed_slice(),
        })
    }

    pub(in crate::geometry::topology) fn line_rows_for_span(
        &self,
        span_index: usize,
    ) -> Result<&[SourceRowId]> {
        self.span_line_rows.row(span_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q line support rows are incomplete".into()).into()
        })
    }

    pub(in crate::geometry::topology) fn collapsed_rows_for_span(
        &self,
        span_index: usize,
    ) -> Result<&[SourceRowId]> {
        self.span_collapsed_rows.row(span_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q collapsed support rows are incomplete".into()).into()
        })
    }

    pub(in crate::geometry::topology) fn point_rows_for_node(
        &self,
        node_index: usize,
    ) -> Result<&[SourceRowId]> {
        self.node_point_rows.row(node_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q point support rows are incomplete".into()).into()
        })
    }

    pub(in crate::geometry::topology) fn span_is_collapsed_pole(
        &self,
        span_index: usize,
    ) -> Result<bool> {
        let span = self
            .spans
            .get(span_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q span index is out of range".into()))?;
        if span.nodes[0] != span.nodes[1] {
            return Ok(false);
        }
        let node = self
            .nodes
            .get(span.nodes[0].0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q span has an unknown node".into()))?;
        Ok(node.pole.is_some())
    }

    pub(in crate::geometry::topology) fn for_each_spatial_candidate_with_work<W: Work>(
        &self,
        start: &ExactChartPoint,
        end: &ExactChartPoint,
        work: &mut W,
        mut visit: impl FnMut(QSpatialItem, &mut W) -> Result<bool>,
    ) -> Result<bool> {
        let mut failure = None;
        let mut stopped = false;
        let result =
            self.spatial_index
                .for_each_candidate(outward_segment_proxy(start, end)?, |item| {
                    match visit(item, work) {
                        Ok(true) => std::ops::ControlFlow::Continue(()),
                        Ok(false) => {
                            stopped = true;
                            std::ops::ControlFlow::Break(())
                        },
                        Err(error) => {
                            failure = Some(error);
                            std::ops::ControlFlow::Break(())
                        },
                    }
                })?;
        failure.map_or_else(|| Ok(!stopped && result.is_continue()), Err)
    }

    pub(in crate::geometry) fn for_each_pole_sector(
        &self,
        pole: ChartPole,
        work: &mut impl Work,
        mut visit: impl FnMut(PoleSector) -> Result<()>,
    ) -> Result<()> {
        self.for_each_pole_sector_with_work(pole, work, |sector, _work| visit(sector))
    }

    pub(in crate::geometry) fn for_each_pole_sector_with_work<W: Work + ?Sized>(
        &self,
        pole: ChartPole,
        work: &mut W,
        mut visit: impl FnMut(PoleSector, &mut W) -> Result<()>,
    ) -> Result<()> {
        let mut found_pole_node = false;
        for node in &self.nodes {
            work.charge_into_error(1)?;
            if node.pole != Some(pole) {
                continue;
            }
            found_pole_node = true;
            let node_index = node.id.0 as usize;
            let start = *self.outgoing_offsets.get(node_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q pole has no outgoing-fan start".into())
            })?;
            let end = *self
                .outgoing_offsets
                .get(node_index.wrapping_add(1))
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q pole has no outgoing-fan end".into())
                })?;
            if start == end {
                let face = self
                    .source_node_faces
                    .get(node_index)
                    .copied()
                    .flatten()
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("isolated Q pole has no host face".into())
                    })?;
                work.charge_into_error(1)?;
                visit(
                    PoleSector::Isolated {
                        pole,
                        node: node.id,
                        face,
                    },
                    work,
                )?;
                continue;
            }
            let fan = self.outgoing.get(start..end).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q pole outgoing fan is incomplete".into())
            })?;
            for (index, &clockwise) in fan.iter().enumerate() {
                work.charge_into_error(1)?;
                let next_index = index.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                let counterclockwise_index = if next_index == fan.len() {
                    0
                } else {
                    next_index
                };
                let counterclockwise = *fan.get(counterclockwise_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q pole fan sector is incomplete".into())
                })?;
                let local_face = *self.dart_faces.get(clockwise.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q pole dart has no incident sector".into())
                })?;
                let face = *self
                    .global_face_of_local
                    .get(local_face.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q pole sector has no global face".into())
                    })?;
                visit(
                    PoleSector::Fan {
                        pole,
                        clockwise,
                        counterclockwise,
                        face,
                    },
                    work,
                )?;
            }
        }
        if !found_pole_node {
            let interval = self.cut_index.pole_interval_id(pole)?;
            let face = *self
                .cut_interval_faces
                .get(interval.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q pole cut interval has no global host".into())
                })?;
            work.charge_into_error(1)?;
            visit(PoleSector::Terminal { pole, face }, work)?;
        }
        Ok(())
    }

    pub(in crate::geometry::topology::q) fn locate_point(
        &self,
        topology: &ExactGeographicTopology,
        source_point: &ExactChartPoint,
        row: SourceRowId,
    ) -> Result<ExactGeographicPointLocation> {
        let point = lift_query_point(topology.sources.frame, &self.cut, source_point)?;
        let frame = topology.sources.frame;
        if point
            .y
            .try_cmp(&ExactRational::from_f64(
                frame.chart_pole_latitude(ChartPole::North),
            ))?
            .is_eq()
        {
            if let Some(node) = self
                .nodes
                .iter()
                .find(|node| node.pole == Some(ChartPole::North))
            {
                return locate_q_node(self, node.id, row);
            }
            let interval = self.cut_index.pole_interval_id(ChartPole::North)?;
            let face = *self
                .cut_interval_faces
                .get(interval.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q north-pole interval has no global face".into())
                })?;
            return Ok(location_in_global_face(self, face, row));
        }
        if point
            .y
            .try_cmp(&ExactRational::from_f64(
                frame.chart_pole_latitude(ChartPole::South),
            ))?
            .is_eq()
        {
            if let Some(node) = self
                .nodes
                .iter()
                .find(|node| node.pole == Some(ChartPole::South))
            {
                return locate_q_node(self, node.id, row);
            }
            let interval = self.cut_index.pole_interval_id(ChartPole::South)?;
            let face = *self
                .cut_interval_faces
                .get(interval.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q south-pole interval has no global face".into())
                })?;
            return Ok(location_in_global_face(self, face, row));
        }

        let cut_east = self
            .cut
            .try_add(&ExactRational::from_f64(frame.longitude().period()))?;
        let mut matched_node = q_port_node(&self.ports, &point)?;
        if matched_node.is_none() && point.x.try_cmp(&self.cut)?.is_eq() {
            let east_alias = ExactChartPoint::from_rationals(cut_east.clone(), point.y.clone());
            matched_node = q_port_node(&self.ports, &east_alias)?;
        } else if matched_node.is_none() && point.x.try_cmp(&cut_east)?.is_eq() {
            let west_alias = ExactChartPoint::from_rationals(self.cut.clone(), point.y.clone());
            matched_node = q_port_node(&self.ports, &west_alias)?;
        }
        if let Some(node) = matched_node {
            return locate_q_node(self, node, row);
        }
        if point.x.try_cmp(&self.cut)?.is_eq() || point.x.try_cmp(&cut_east)?.is_eq() {
            return self.location_at_cut(&point.y, row);
        }

        let query = outward_segment_proxy(&point, &point)?;
        let mut boundary_span = None;
        let mut boundary_node = None;
        let mut failure = None;
        let candidates = self
            .spatial_index
            .for_each_candidate(query, |item| match item {
                QSpatialItem::Span(span_index) => {
                    let Some(span) = self.spans.get(span_index) else {
                        failure = Some(
                            GeometryErrorKind::Invalid("Q point index has an unknown span".into())
                                .into(),
                        );
                        return std::ops::ControlFlow::Break(());
                    };
                    let Some(start) = self.ports.get(span.ports[0].0 as usize) else {
                        failure = Some(
                            GeometryErrorKind::Invalid("Q point span has no start port".into())
                                .into(),
                        );
                        return std::ops::ControlFlow::Break(());
                    };
                    let Some(end) = self.ports.get(span.ports[1].0 as usize) else {
                        failure = Some(
                            GeometryErrorKind::Invalid("Q point span has no end port".into())
                                .into(),
                        );
                        return std::ops::ControlFlow::Break(());
                    };
                    match point_on_exact_segment(&point, &start.point, &end.point) {
                        Ok(true) => {
                            if boundary_span.is_some_and(|previous| previous != span_index) {
                                failure = Some(
                                    GeometryErrorKind::Invalid(
                                        "an exact Q query meets un-noded spans".into(),
                                    )
                                    .into(),
                                );
                                std::ops::ControlFlow::Break(())
                            } else {
                                boundary_span = Some(span_index);
                                std::ops::ControlFlow::Break(())
                            }
                        },
                        Ok(false) => std::ops::ControlFlow::Continue(()),
                        Err(error) => {
                            failure = Some(error);
                            std::ops::ControlFlow::Break(())
                        },
                    }
                },
                QSpatialItem::Point(port_id) => {
                    let Some(port) = self.ports.get(port_id.0 as usize) else {
                        failure = Some(
                            GeometryErrorKind::Invalid("Q point index has an unknown port".into())
                                .into(),
                        );
                        return std::ops::ControlFlow::Break(());
                    };
                    match port.point.same_position(&point) {
                        Ok(true) => {
                            boundary_node = Some(port.node);
                            std::ops::ControlFlow::Break(())
                        },
                        Ok(false) => std::ops::ControlFlow::Continue(()),
                        Err(error) => {
                            failure = Some(error);
                            std::ops::ControlFlow::Break(())
                        },
                    }
                },
            })?;
        if let Some(error) = failure {
            return Err(error);
        }
        if let Some(node) = boundary_node {
            return locate_q_node(self, node, row);
        }
        if let Some(span) = boundary_span {
            return Ok(locate_q_span(self, span, row));
        }
        if candidates.is_break() {
            return Err(GeometryErrorKind::Invalid(
                "Q point index stopped without an exact contact".into(),
            )
            .into());
        }
        if topology.sources.polygons.is_empty() {
            return Ok(location_in_area_face(false));
        }
        self.location_from_cut_ray(&point, row)
    }

    pub(in crate::geometry::topology::q) fn location_at_cut(
        &self,
        latitude: &ExactRational,
        row: SourceRowId,
    ) -> Result<ExactGeographicPointLocation> {
        let interval = self.cut_index.interval_at(latitude, 1)?;
        let face = *self
            .cut_interval_faces
            .get(interval.id.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q cut interval has no global face".into())
            })?;
        Ok(location_in_global_face(self, face, row))
    }

    pub(in crate::geometry::topology::q) fn location_from_cut_ray(
        &self,
        point: &ExactChartPoint,
        row: SourceRowId,
    ) -> Result<ExactGeographicPointLocation> {
        let interval = self.cut_index.interval_at(&point.y, 1)?;
        let face = *self
            .cut_interval_faces
            .get(interval.id.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q cut interval has no global face".into())
            })?;
        let mut area_inside = q_face_has_row(self, face, row);
        let cut_point = ExactChartPoint::from_rationals(self.cut.clone(), point.y.clone());
        let query = outward_segment_proxy(&cut_point, point)?;
        let mut failure = None;
        let candidates = self.spatial_index.for_each_candidate(query, |item| {
            let QSpatialItem::Span(span_index) = item else {
                return std::ops::ControlFlow::Continue(());
            };
            match span_crosses_north_ray(self, span_index, point, row) {
                Ok(crosses) => {
                    if crosses {
                        area_inside = !area_inside;
                    }
                    std::ops::ControlFlow::Continue(())
                },
                Err(error) => {
                    failure = Some(error);
                    std::ops::ControlFlow::Break(())
                },
            }
        })?;
        if let Some(error) = failure {
            return Err(error);
        }
        if candidates.is_break() {
            return Err(GeometryErrorKind::Invalid(
                "Q point ray stopped without an exact query result".into(),
            )
            .into());
        }
        Ok(location_in_area_face(area_inside))
    }

    pub(in crate::geometry::topology::q) fn classify_rectangle(
        &self,
        topology: &ExactGeographicTopology,
        bounds: Bounds,
        row: SourceRowId,
    ) -> Result<Option<ExactGeographicRectangleLocation>> {
        let Some(windows) = ExactQRectangleWindows::new(topology.sources.frame, &self.cut, bounds)?
        else {
            return Ok(None);
        };
        let corners = [
            Point::new_raw_xy(bounds.minx(), bounds.miny()),
            Point::new_raw_xy(bounds.maxx(), bounds.miny()),
            Point::new_raw_xy(bounds.maxx(), bounds.maxy()),
            Point::new_raw_xy(bounds.minx(), bounds.maxy()),
        ];
        let mut corners_covered = true;
        let mut intersects = false;
        for corner in corners {
            let location = topology.locate_point(corner)?;
            corners_covered &= location.area != Loc::Exterior;
            intersects |= location.geometry != Loc::Exterior;
        }
        let center = Point::new_raw_xy(
            f64::midpoint(bounds.minx(), bounds.maxx()),
            f64::midpoint(bounds.miny(), bounds.maxy()),
        );
        let center_location = topology.locate_point(center)?;
        let covered = corners_covered && center_location.area == Loc::Interior;
        intersects |= center_location.geometry != Loc::Exterior;
        if intersects && !covered {
            return Ok(Some(ExactGeographicRectangleLocation::Boundary));
        }

        let mut area_boundary_enters = false;
        let mut support_intersects = false;
        for rectangle in windows.iter() {
            let query = rectangle.outward_query_segment()?;
            let mut failure: Option<crate::error::Error> = None;
            let candidates = self.spatial_index.for_each_candidate(query, |item| {
                match item {
                    QSpatialItem::Span(span_index) => {
                        let Some(span) = self.spans.get(span_index) else {
                            failure = Some(
                                GeometryErrorKind::Invalid(
                                    "Q rectangle index has an unknown span".into(),
                                )
                                .into(),
                            );
                            return std::ops::ControlFlow::Break(());
                        };
                        let Some(start) = self.ports.get(span.ports[0].0 as usize) else {
                            failure = Some(
                                GeometryErrorKind::Invalid(
                                    "Q rectangle span has no start port".into(),
                                )
                                .into(),
                            );
                            return std::ops::ControlFlow::Break(());
                        };
                        let Some(end) = self.ports.get(span.ports[1].0 as usize) else {
                            failure = Some(
                                GeometryErrorKind::Invalid(
                                    "Q rectangle span has no end port".into(),
                                )
                                .into(),
                            );
                            return std::ops::ControlFlow::Break(());
                        };
                        let faces = self.span_global_faces.get(span_index).copied().flatten();
                        let (area_left, area_right) =
                            faces.map_or((false, false), |[left, right]| {
                                (
                                    q_face_has_row(self, left, row),
                                    q_face_has_row(self, right, row),
                                )
                            });
                        let line = self.span_line_rows.contains(span_index, row);
                        let collapsed = self.span_collapsed_rows.contains(span_index, row);
                        let support = area_left || area_right || line || collapsed;
                        match rectangle.segment_intersects_closed(&start.point, &end.point) {
                            Ok(true) if support => support_intersects = true,
                            Ok(_) => {},
                            Err(error) => {
                                failure = Some(error);
                                return std::ops::ControlFlow::Break(());
                            },
                        }
                        if area_left != area_right {
                            match rectangle.segment_intersects_open(&start.point, &end.point) {
                                Ok(true) => area_boundary_enters = true,
                                Ok(false) => {},
                                Err(error) => {
                                    failure = Some(error);
                                    return std::ops::ControlFlow::Break(());
                                },
                            }
                        }
                    },
                    QSpatialItem::Point(port_id) => {
                        let Some(port) = self.ports.get(port_id.0 as usize) else {
                            failure = Some(
                                GeometryErrorKind::Invalid(
                                    "Q rectangle index has an unknown point port".into(),
                                )
                                .into(),
                            );
                            return std::ops::ControlFlow::Break(());
                        };
                        let node_index = port.node.0 as usize;
                        if self.node_point_rows.contains(node_index, row) {
                            match rectangle.contains_closed(&port.point) {
                                Ok(true) => support_intersects = true,
                                Ok(false) => {},
                                Err(error) => {
                                    failure = Some(error);
                                    return std::ops::ControlFlow::Break(());
                                },
                            }
                        }
                    },
                }
                if (covered && area_boundary_enters) || (!covered && support_intersects) {
                    std::ops::ControlFlow::Break(())
                } else {
                    std::ops::ControlFlow::Continue(())
                }
            })?;
            if let Some(error) = failure {
                return Err(error);
            }
            if candidates.is_break() {
                break;
            }
        }
        if covered && !area_boundary_enters {
            Ok(Some(ExactGeographicRectangleLocation::Interior))
        } else if intersects || support_intersects || area_boundary_enters {
            Ok(Some(ExactGeographicRectangleLocation::Boundary))
        } else {
            Ok(Some(ExactGeographicRectangleLocation::Outside))
        }
    }
}

impl HeapSize for ExactGeographicQ {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "HeapSize sums the already-owned Q buffers as an estimate"
    )]
    fn heap_bytes(&self) -> usize {
        self.ports.len() * std::mem::size_of::<QPort>()
            + self
                .ports
                .iter()
                .map(|port| point_heap_bytes(&port.point))
                .sum::<usize>()
            + self.nodes.len() * std::mem::size_of::<QNode>()
            + self
                .nodes
                .iter()
                .map(|node| match &node.key {
                    QNodeKey::Source(_) => 0,
                    QNodeKey::Cut(latitude) => rational_heap_bytes(latitude),
                })
                .sum::<usize>()
            + self.node_ports.len() * std::mem::size_of::<QPortId>()
            + self.spans.len() * std::mem::size_of::<ChartSpan>()
            + self
                .spans
                .iter()
                .map(|span| {
                    span.owners.len() * std::mem::size_of::<QSpanOwner>()
                        + span
                            .owners
                            .iter()
                            .map(|owner| source_range_heap_bytes(&owner.source_range))
                            .sum::<usize>()
                })
                .sum::<usize>()
            + self.darts.len() * std::mem::size_of::<QDart>()
            + self.span_darts.len() * std::mem::size_of::<Option<QDartId>>()
            + self.outgoing.len() * std::mem::size_of::<QDartId>()
            + self.outgoing_offsets.len() * std::mem::size_of::<usize>()
            + self.dart_positions.len() * std::mem::size_of::<usize>()
            + self.dart_faces.len() * std::mem::size_of::<QLocalFaceId>()
            + self.local_faces.len() * std::mem::size_of::<QLocalFace>()
            + self.node_components.len() * std::mem::size_of::<QComponentId>()
            + self.span_local_faces.len() * std::mem::size_of::<Option<[QLocalFaceId; 2]>>()
            + self.source_edge_ports.len() * std::mem::size_of::<[QPortId; 2]>()
            + self.source_point_ports.len() * std::mem::size_of::<QPortId>()
            + self.cut_index.heap_bytes()
            + self.component_hosts.len() * std::mem::size_of::<Option<GlobalFaceId>>()
            + self.global_face_of_local.len() * std::mem::size_of::<GlobalFaceId>()
            + self.span_global_faces.len() * std::mem::size_of::<Option<[GlobalFaceId; 2]>>()
            + self.cut_interval_faces.len() * std::mem::size_of::<GlobalFaceId>()
            + self.source_node_faces.len() * std::mem::size_of::<Option<GlobalFaceId>>()
            + self.source_point_faces.len() * std::mem::size_of::<Option<GlobalFaceId>>()
            + self.face_rows.heap_bytes()
            + self.face_validity.len() * std::mem::size_of::<QFaceValidity>()
            + self.seam_owner_sides.len() * std::mem::size_of::<SeamOwnerSide>()
            + self.span_line_rows.heap_bytes()
            + self.span_collapsed_rows.heap_bytes()
            + self.node_point_rows.heap_bytes()
            + self.node_line_boundary_rows.heap_bytes()
            + self.spatial_index.heap_bytes()
            + {
                #[cfg(test)]
                {
                    self.seed_winding.len() * std::mem::size_of::<i64>()
                }
                #[cfg(not(test))]
                {
                    0
                }
            }
    }
}

impl RootCatalog for ExactGeographicTopology {
    fn resolve<M: ChartMetric, W: Work>(
        &self,
        root: RootId,
        parameter: &ExactRational,
        axes: crate::geometry::CoordinateAxes,
        metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
        state: &mut RootMetricState,
        fraction: &mut Option<f64>,
        work: &mut W,
    ) -> Result<NodeOrdinates> {
        let edge = self.sources.edges.get(root.index()).ok_or_else(|| {
            GeometryErrorKind::Invalid("source trace references an unknown source edge".into())
        })?;
        resolve_source_root(
            self.sources.frame,
            &edge.source,
            edge.retained_longitude(),
            parameter,
            axes,
            metric_access,
            state,
            fraction,
            work,
        )
    }
}
impl ExactGeographicTopology {
    pub(crate) fn from_views<'a>(
        frame: GeographicFrame,
        views: impl ExactSizeIterator<Item = crate::geographic::ScalarGeographicView<'a>>,
    ) -> Result<Self> {
        let mut budget =
            crate::geometry::ExpansionBudget::new("geographic topology", "exact chart noding");
        let mut work = crate::geometry::Unmetered;
        Self::from_views_with_work_and_expansion_budget(frame, views, &mut budget, &mut work)
    }

    pub(crate) fn from_views_with_work_and_expansion_budget<'a>(
        frame: GeographicFrame,
        views: impl ExactSizeIterator<Item = crate::geographic::ScalarGeographicView<'a>>,
        budget: &mut ExpansionBudget,
        work: &mut impl Work,
    ) -> Result<Self> {
        let sources = ExactGeographicChartInventory::collect_views_with_work(frame, views, work)?;
        Self::build(frame, sources, budget, work)
    }

    pub(crate) fn from_views_as_set<'a>(
        frame: GeographicFrame,
        views: impl ExactSizeIterator<Item = crate::geographic::ScalarGeographicView<'a>>,
    ) -> Result<Self> {
        Self::from_views_as_set_with_publication_frame(frame, crate::Frame::None, views)
    }

    pub(in crate::geometry) fn from_views_as_set_with_publication_frame<'a>(
        frame: GeographicFrame,
        publication_frame: crate::Frame,
        views: impl ExactSizeIterator<Item = crate::geographic::ScalarGeographicView<'a>>,
    ) -> Result<Self> {
        let mut budget =
            crate::geometry::ExpansionBudget::new("geographic topology", "exact chart noding");
        let mut work = crate::geometry::Unmetered;
        let sources =
            ExactGeographicChartInventory::collect_views_as_set_with_work(frame, views, &mut work)?;
        Self::build_with_publication_frame(
            frame,
            sources,
            publication_frame,
            &mut budget,
            &mut work,
        )
    }

    pub(in crate::geometry::topology::q) fn build(
        frame: GeographicFrame,
        sources: ExactGeographicChartInventory,
        budget: &mut ExpansionBudget,
        work: &mut impl Work,
    ) -> Result<Self> {
        Self::build_with_publication_frame(frame, sources, crate::Frame::None, budget, work)
    }

    pub(in crate::geometry::topology::q) fn build_with_publication_frame(
        frame: GeographicFrame,
        sources: ExactGeographicChartInventory,
        publication_frame: crate::Frame,
        budget: &mut ExpansionBudget,
        work: &mut impl Work,
    ) -> Result<Self> {
        let chart = sources.node_with_work(budget, work)?;
        let q = ExactGeographicQ::build(frame, &sources, &chart)?;
        Ok(Self {
            sources,
            chart,
            q,
            publication_frame,
        })
    }

    pub(in crate::geometry) const fn publication_frame(&self) -> &crate::Frame {
        &self.publication_frame
    }
}

impl HeapSize for ExactGeographicTopology {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "HeapSize sums the retained source inventory, exact chart, global Q, and publication-frame estimates"
    )]
    fn heap_bytes(&self) -> usize {
        self.sources.heap_bytes()
            + self.chart.heap_bytes()
            + self.q.heap_bytes()
            + std::mem::size_of_val(&self.publication_frame)
    }
}
