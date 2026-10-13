use super::super::*;

impl<'topology, 'emission, 'access, 'metric, M: ChartMetric>
    QEmbeddingProblem<'topology, 'emission, 'access, 'metric, M>
{
    pub(in crate::geometry::topology::q_embedding) fn fan_matches(
        &self,
        fan: &QFanRow,
        class: usize,
        assigned: &[Option<QCandidate>],
        cut_cache: &mut BTreeMap<QNodeId, ExactRational>,
        budget: &mut impl Work,
    ) -> Result<bool> {
        let q = &self.topology.q;
        budget.charge_into_error(
            u64::try_from(fan.darts.len())
                .map_err(|_| ResourceError::SizeOverflow)?
                .checked_mul(2)
                .ok_or(ResourceError::SizeOverflow)?,
        )?;
        let mut ports = Vec::with_capacity(fan.darts.len().saturating_mul(2));
        for &dart_id in &fan.darts {
            budget.charge_into_error(1)?;
            let dart = q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q fan certificate dart is unknown".into())
            })?;
            ports.extend([dart.from_port, dart.to_port]);
        }
        try_sort_by_with_work(&mut ports, budget, |left, right, _work| Ok(left.cmp(right)))?;
        budget.charge_into_error(
            u64::try_from(ports.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        ports.dedup();
        let mut points = Vec::<(QPortId, ExactChartPoint)>::with_capacity(ports.len());
        for port_id in ports {
            budget.charge_into_error(1)?;
            let Some(point) =
                self.candidate_port_point(port_id, class, assigned, cut_cache, budget)?
            else {
                return Ok(false);
            };
            points.push((port_id, point));
        }
        let chart = QFaceChart {
            cut: &q.cut,
            cut_east: &self.cut_east,
            ports: &q.ports,
            nodes: &q.nodes,
        };
        budget.charge_into_error(
            u64::try_from(fan.darts.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut candidate_order = fan.darts.clone();
        try_sort_by_with_work(&mut candidate_order, budget, |left, right, work| {
            work.charge_into_error(1)?;
            compare_q_darts_with_points(chart, &q.darts, *left, *right, |port| {
                work.charge_into_error(1)?;
                let index = points
                    .binary_search_by_key(&port, |(candidate, _)| *candidate)
                    .map_err(|_| {
                        GeometryErrorKind::Invalid("Q fan point cache is incomplete".into())
                    })?;
                points.get(index).map(|(_, point)| point).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q fan point cache index is unknown".into()).into()
                })
            })
        })?;
        for adjacent in candidate_order.windows(2) {
            budget.charge_into_error(1)?;
            let [left, right] = adjacent else {
                unreachable!("windows(2) contains two Q fan darts");
            };
            if compare_q_darts_with_points(chart, &q.darts, *left, *right, |port| {
                budget.charge_into_error(1)?;
                let index = points
                    .binary_search_by_key(&port, |(candidate, _)| *candidate)
                    .map_err(|_| {
                        GeometryErrorKind::Invalid("Q fan point cache is incomplete".into())
                    })?;
                points.get(index).map(|(_, point)| point).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q fan point cache index is unknown".into()).into()
                })
            })?
            .is_eq()
            {
                return Ok(false);
            }
        }
        if let (Some(&first), Some(&last)) = (candidate_order.first(), candidate_order.last())
            && compare_q_darts_with_points(chart, &q.darts, last, first, |port| {
                budget.charge_into_error(1)?;
                let index = points
                    .binary_search_by_key(&port, |(candidate, _)| *candidate)
                    .map_err(|_| {
                        GeometryErrorKind::Invalid("Q fan point cache is incomplete".into())
                    })?;
                points.get(index).map(|(_, point)| point).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q fan point cache index is unknown".into()).into()
                })
            })?
            .is_eq()
        {
            return Ok(false);
        }
        budget.charge_into_error(
            u64::try_from(fan.darts.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        Ok(rotation_row_matches(&fan.darts, &candidate_order))
    }
    pub(in crate::geometry::topology::q_embedding) fn source_variable_location(
        &self,
        node: QNodeId,
        budget: &mut impl Work,
    ) -> Result<Option<QNodeLocation>> {
        budget.charge_into_error(1)?;
        let index = node.0 as usize;
        let location = self
            .node_location
            .get(index)
            .copied()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q dependency node is unknown".into()))?;
        let exact = self
            .exact_positions
            .get(index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q dependency domain is unknown".into()))?;
        let movable = match exact {
            Some(exact) => inexact_axes_with_work(exact, self.nearest.xy(node)?, budget)?
                .into_iter()
                .any(|axis| axis),
            None => false,
        };
        if location.is_some() != movable {
            return Err(GeometryErrorKind::Invalid(
                "Q dependency is movable but absent from the frozen source inventory".into(),
            )
            .into());
        }
        Ok(location)
    }
    pub(in crate::geometry::topology::q_embedding) fn publication_variable_location(
        &self,
        node: EmissionNodeId,
        budget: &mut impl Work,
    ) -> Result<Option<QNodeLocation>> {
        budget.charge_into_error(1)?;
        let index = node.0 as usize;
        let location = self
            .publication_location
            .get(index)
            .copied()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication dependency is unknown".into())
            })?;
        let authority = &self
            .publication_nodes
            .nodes()
            .get(index)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication dependency authority is unknown".into())
            })?
            .authority;
        let nearest = self
            .publication_nearest
            .get(index)
            .copied()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication dependency has no nearest row".into())
            })?;
        let domain = self.publication_domains.get(index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q publication dependency has no domain".into())
        })?;
        let is_computed = matches!(authority, NodeAuthority::Computed(_));
        if is_computed != nearest.is_some() {
            return Err(GeometryErrorKind::Invalid(
                "Q publication authority and nearest inventory disagree".into(),
            )
            .into());
        }
        let movable = if let Some(nearest) = nearest {
            if domain.is_empty() {
                return Err(GeometryErrorKind::Invalid(
                    "Q publication dependency has an invalid admitted domain".into(),
                )
                .into());
            }
            let mut movable = false;
            let mut contains_nearest = false;
            for &candidate in domain {
                budget.charge_into_error(1)?;
                contains_nearest |= candidate == nearest;
                movable |= candidate != nearest;
            }
            if !contains_nearest {
                return Err(GeometryErrorKind::Invalid(
                    "Q publication dependency has an invalid admitted domain".into(),
                )
                .into());
            }
            movable
        } else {
            if !domain.is_empty() {
                return Err(GeometryErrorKind::Invalid(
                    "fixed Q publication dependency has an admitted domain".into(),
                )
                .into());
            }
            false
        };
        if location.is_some() != movable {
            return Err(GeometryErrorKind::Invalid(
                "Q publication dependency is movable but absent from the frozen inventory".into(),
            )
            .into());
        }
        Ok(location)
    }
    pub(in crate::geometry::topology::q_embedding) fn classes_for_nodes(
        &self,
        nodes: impl IntoIterator<Item = QNodeId>,
        budget: &mut impl Work,
    ) -> Result<Vec<usize>> {
        budget.charge_into_error(
            u64::try_from(self.node_location.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut seen = vec![false; self.node_location.len()];
        let mut pending = Vec::new();
        for node in nodes {
            budget.charge_into_error(1)?;
            pending.push(node);
        }
        let mut classes = Vec::new();
        let mut next = 0;
        while let Some(node) = pending.get(next).copied() {
            next = next.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            budget.charge_into_error(1)?;
            let seen_node = seen.get_mut(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q rejection references an unknown node".into())
            })?;
            if std::mem::replace(seen_node, true) {
                continue;
            }
            if let Some((class, _)) = self.source_variable_location(node, budget)? {
                budget.charge_into_error(1)?;
                classes.push(class);
            }
            let cuts = self
                .dependency_index
                .cuts
                .get(node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q rejection node has no cut dependency row".into())
                })?;
            for &cut_index in cuts {
                budget.charge_into_error(1)?;
                let cut = self.cut_rows.get(cut_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q cut dependency row is unknown".into())
                })?;
                for &dependency in &cut.dependencies {
                    budget.charge_into_error(1)?;
                    pending.push(dependency);
                }
            }
            let fans = self
                .dependency_index
                .fans
                .get(node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q rejection node has no fan dependency row".into())
                })?;
            for &fan_index in fans {
                budget.charge_into_error(1)?;
                let fan = self.fan_rows.get(fan_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q fan dependency row is unknown".into())
                })?;
                for &dependency in &fan.dependencies {
                    budget.charge_into_error(1)?;
                    pending.push(dependency);
                }
            }
            let pairs = self
                .dependency_index
                .pairs
                .get(node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q rejection node has no pair dependency row".into())
                })?;
            for &pair_index in pairs {
                budget.charge_into_error(1)?;
                let pair = self.proximity_pairs.get(pair_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q proximity dependency pair is unknown".into())
                })?;
                for chord_index in [pair.left, pair.right] {
                    let chord = self.chords.get(chord_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q proximity dependency chord is unknown".into())
                    })?;
                    for &dependency in &chord.dependencies {
                        budget.charge_into_error(1)?;
                        pending.push(dependency);
                    }
                }
            }
        }
        try_sort_by_with_work(&mut classes, budget, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        budget.charge_into_error(
            u64::try_from(classes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        classes.dedup();
        Ok(classes)
    }
    pub(in crate::geometry::topology::q_embedding) fn classes_for_variables(
        &self,
        variables: impl IntoIterator<Item = QVariableNode>,
        budget: &mut impl Work,
    ) -> Result<Vec<usize>> {
        let mut source_nodes = Vec::new();
        let mut classes = Vec::new();
        for variable in variables {
            budget.charge_into_error(1)?;
            match variable {
                QVariableNode::Source(node) => source_nodes.push(node),
                QVariableNode::Published(node) => {
                    if let Some((class, _)) = self.publication_variable_location(node, budget)? {
                        classes.push(class);
                    }
                },
            }
        }
        for class in self.classes_for_nodes(source_nodes, budget)? {
            budget.charge_into_error(1)?;
            classes.push(class);
        }
        try_sort_by_with_work(&mut classes, budget, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        budget.charge_into_error(
            u64::try_from(classes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        classes.dedup();
        Ok(classes)
    }
}
