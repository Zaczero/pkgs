use super::super::*;

impl<'topology, 'emission, 'access, 'metric, M: ChartMetric>
    QEmbeddingProblem<'topology, 'emission, 'access, 'metric, M>
{
    pub(in crate::geometry::topology::q_embedding) fn local_candidate(
        &self,
        node: QNodeId,
        class: usize,
        assigned: &[Option<QCandidate>],
    ) -> Result<QCandidate> {
        match self.node_location.get(node.0 as usize).copied().flatten() {
            Some((owner, rank)) if owner == class => {
                assigned.get(rank).copied().flatten().ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Q local check reached an unassigned dependency".into(),
                    )
                    .into()
                })
            },
            _ => {
                let index = node.0 as usize;
                Ok(QCandidate {
                    native: self.nearest.xy(node)?,
                    periods: *self.nearest.longitude_periods.get(index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q node has no retained longitude lift".into())
                    })?,
                })
            },
        }
    }
    pub(in crate::geometry::topology::q_embedding) fn candidate_cut_latitude(
        &self,
        cut_index: usize,
        class: usize,
        assigned: &[Option<QCandidate>],
        budget: &mut impl Work,
    ) -> Result<Option<ExactRational>> {
        let cut_row = self.cut_rows.get(cut_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q cut certificate index is unknown".into())
        })?;
        let cut =
            self.emission.cut_nodes.get(cut_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q cut certificate row is unknown".into())
            })?;
        if cut.node != cut_row.node {
            return Err(
                GeometryErrorKind::Invalid("Q cut certificate rows are misaligned".into()).into(),
            );
        }
        let frame = self.topology.sources.frame;
        let q = &self.topology.q;
        let period = ExactRational::from_f64(frame.longitude().period());
        let mut latitude: Option<ExactRational> = None;
        for chord in &cut.chords {
            budget.charge_into_error(1)?;
            let west_port = q.ports.get(chord.west_far_port.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q west cut endpoint is unknown".into())
            })?;
            let east_port = q.ports.get(chord.east_far_port.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q east cut endpoint is unknown".into())
            })?;
            if west_port.cut || east_port.cut {
                return Ok(None);
            }
            let west_nearest = self.nearest.xy(west_port.node)?;
            let east_nearest = self.nearest.xy(east_port.node)?;
            let west_candidate = self.local_candidate(west_port.node, class, assigned)?;
            let east_candidate = self.local_candidate(east_port.node, class, assigned)?;
            let west_longitude = self
                .nearest
                .pole_port_longitude
                .get(chord.west_far_port.0 as usize)
                .copied()
                .flatten();
            let east_longitude = self
                .nearest
                .pole_port_longitude
                .get(chord.east_far_port.0 as usize)
                .copied()
                .flatten();
            let west_periods = self
                .nearest
                .pole_port_periods
                .get(chord.west_far_port.0 as usize)
                .copied()
                .flatten();
            let east_periods = self
                .nearest
                .pole_port_periods
                .get(chord.east_far_port.0 as usize)
                .copied()
                .flatten();
            let Some(west_point) = assigned_port_point(
                frame,
                q,
                chord.west_far_port,
                west_nearest,
                west_candidate,
                west_longitude,
                west_periods,
                &self.cut_east,
                budget,
            )?
            else {
                return Ok(None);
            };
            let Some(east_point) = assigned_port_point(
                frame,
                q,
                chord.east_far_port,
                east_nearest,
                east_candidate,
                east_longitude,
                east_periods,
                &self.cut_east,
                budget,
            )?
            else {
                return Ok(None);
            };
            let Some(candidate_y) =
                cut_intersection_y(q, &period, &west_point, &east_point, budget)?
            else {
                return Ok(None);
            };
            if let Some(previous) = &latitude {
                if !previous.try_cmp_with_work(&candidate_y, budget)?.is_eq() {
                    return Ok(None);
                }
            } else {
                latitude = Some(candidate_y);
            }
        }
        let Some(latitude) = latitude else {
            return Ok(None);
        };
        let node = q
            .nodes
            .get(cut.node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q cut node is unknown".into()))?;
        if node.pole.is_some() {
            let exact = q
                .ports
                .get(cut.west.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q pole cut has no west port".into()))?;
            if !latitude.try_cmp_with_work(&exact.point.y, budget)?.is_eq() {
                return Ok(None);
            }
        }
        Ok(Some(latitude))
    }
    pub(in crate::geometry::topology::q_embedding) fn candidate_port_point(
        &self,
        port_id: QPortId,
        class: usize,
        assigned: &[Option<QCandidate>],
        cut_cache: &mut BTreeMap<QNodeId, ExactRational>,
        budget: &mut impl Work,
    ) -> Result<Option<ExactChartPoint>> {
        budget.charge_into_error(1)?;
        let frame = self.topology.sources.frame;
        let q = &self.topology.q;
        let port = q
            .ports
            .get(port_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q candidate port is unknown".into()))?;
        if port.cut {
            let latitude = if let Some(latitude) = cut_cache.get(&port.node) {
                latitude.clone()
            } else {
                let Some(cut_index) = self
                    .cut_index_by_node
                    .get(port.node.0 as usize)
                    .copied()
                    .flatten()
                else {
                    return Ok(None);
                };
                let Some(latitude) =
                    self.candidate_cut_latitude(cut_index, class, assigned, budget)?
                else {
                    return Ok(None);
                };
                cut_cache.insert(port.node, latitude.clone());
                latitude
            };
            return Ok(Some(ExactChartPoint::from_rationals(
                port.point.x.clone(),
                latitude,
            )));
        }
        let nearest_xy = self.nearest.xy(port.node)?;
        let candidate = self.local_candidate(port.node, class, assigned)?;
        let pole_longitude = self
            .nearest
            .pole_port_longitude
            .get(port_id.0 as usize)
            .copied()
            .flatten();
        assigned_port_point(
            frame,
            q,
            port_id,
            nearest_xy,
            candidate,
            pole_longitude,
            self.nearest
                .pole_port_periods
                .get(port_id.0 as usize)
                .copied()
                .flatten(),
            &self.cut_east,
            budget,
        )
    }
    pub(in crate::geometry::topology::q_embedding) fn local_publication_endpoint(
        &self,
        endpoint: &PublishedEndpoint,
        reference: &ExactChartPoint,
        reference_offset: &ExactRational,
        class: usize,
        assigned: &[Option<QCandidate>],
        cut_cache: &mut BTreeMap<QNodeId, ExactRational>,
        budget: &mut impl Work,
    ) -> Result<Option<ExactChartPoint>> {
        budget.charge_into_error(1)?;
        let assigned_point = match endpoint {
            PublishedEndpoint::Source { port, .. } => {
                self.candidate_port_point(*port, class, assigned, cut_cache, budget)?
            },
            PublishedEndpoint::Published { node, sample } => {
                if self.publication_sample_nodes.get(*sample) != Some(node) {
                    return Err(GeometryErrorKind::Invalid(
                        "local publication occurrence changed its node authority".into(),
                    )
                    .into());
                }
                let candidate = match self
                    .publication_location
                    .get(node.0 as usize)
                    .copied()
                    .flatten()
                {
                    Some((owner, rank)) if owner == class => assigned.get(rank).copied().flatten(),
                    Some(_) => {
                        return Err(GeometryErrorKind::Invalid(
                            "publication predicate crosses variable classes".into(),
                        )
                        .into());
                    },
                    None => self
                        .publication_nearest
                        .get(node.0 as usize)
                        .copied()
                        .flatten(),
                };
                candidate
                    .map(|candidate| {
                        candidate_chart_point_with_work(
                            self.topology.sources.frame,
                            candidate,
                            budget,
                        )
                    })
                    .transpose()?
            },
        };
        assigned_point
            .as_ref()
            .map(|assigned| {
                self.assigned_publication_endpoint_point(
                    endpoint,
                    reference,
                    reference_offset,
                    assigned,
                    budget,
                )
            })
            .transpose()
    }
    pub(in crate::geometry::topology::q_embedding) fn local_publication_chord(
        &self,
        chord_id: QPublicationChordId,
        class: usize,
        assigned: &[Option<QCandidate>],
        cut_cache: &mut BTreeMap<QNodeId, ExactRational>,
        budget: &mut impl Work,
    ) -> Result<Option<LocalPublicationChord>> {
        budget.charge_into_error(1)?;
        match chord_id {
            QPublicationChordId::Source(index) => {
                let chord = self.chords.get(index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication source chord is unknown".into())
                })?;
                let [start_port, end_port] = chord.ports;
                let exact = [
                    self.exact_port_point(start_port)?.clone(),
                    self.exact_port_point(end_port)?.clone(),
                ];
                let [Some(start), Some(end)] = [
                    self.candidate_port_point(start_port, class, assigned, cut_cache, budget)?,
                    self.candidate_port_point(end_port, class, assigned, cut_cache, budget)?,
                ] else {
                    return Ok(None);
                };
                Ok(Some(LocalPublicationChord {
                    nodes: chord.nodes.map(QGraphNode::Source),
                    exact,
                    candidate: [start, end],
                }))
            },
            QPublicationChordId::Published(index) => {
                let chord = self.publication_chords.get(index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication chord is unknown".into())
                })?;
                let [Some(start), Some(end)] = [
                    self.local_publication_endpoint(
                        &chord.endpoints[0],
                        &chord.exact[0],
                        &chord.reference_offsets[0],
                        class,
                        assigned,
                        cut_cache,
                        budget,
                    )?,
                    self.local_publication_endpoint(
                        &chord.endpoints[1],
                        &chord.exact[1],
                        &chord.reference_offsets[1],
                        class,
                        assigned,
                        cut_cache,
                        budget,
                    )?,
                ] else {
                    return Ok(None);
                };
                Ok(Some(LocalPublicationChord {
                    nodes: chord.nodes,
                    exact: chord.exact.clone(),
                    candidate: [start, end],
                }))
            },
        }
    }
    pub(in crate::geometry::topology::q_embedding) fn publication_pair_matches_local(
        &self,
        pair: QPublicationPair,
        class: usize,
        assigned: &[Option<QCandidate>],
        cut_cache: &mut BTreeMap<QNodeId, ExactRational>,
        budget: &mut impl Work,
    ) -> Result<bool> {
        budget.charge_into_error(1)?;
        let (Some(left), Some(right)) = (
            self.local_publication_chord(pair.left, class, assigned, cut_cache, budget)?,
            self.local_publication_chord(pair.right, class, assigned, cut_cache, budget)?,
        ) else {
            return Ok(false);
        };
        let period = ExactRational::from_f64(self.topology.sources.frame.longitude().period());
        let right_exact = [
            translated_exact_point_with_work(&right.exact[0], pair.right_shift, &period, budget)?,
            translated_exact_point_with_work(&right.exact[1], pair.right_shift, &period, budget)?,
        ];
        let right_candidate = [
            translated_exact_point_with_work(
                &right.candidate[0],
                pair.right_shift,
                &period,
                budget,
            )?,
            translated_exact_point_with_work(
                &right.candidate[1],
                pair.right_shift,
                &period,
                budget,
            )?,
        ];
        pair_preserves_incidence_with_work(
            left.nodes,
            &left.exact,
            &left.candidate,
            right.nodes,
            &right_exact,
            &right_candidate,
            budget,
        )
    }
    pub(in crate::geometry::topology::q_embedding) fn exact_port_point(
        &self,
        port_id: QPortId,
    ) -> Result<&ExactChartPoint> {
        self.topology
            .q
            .ports
            .get(port_id.0 as usize)
            .map(|port| &port.point)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q exact port is unknown".into()).into())
    }
}
