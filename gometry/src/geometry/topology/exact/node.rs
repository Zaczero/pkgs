use super::events::compare_cut_events;
use super::keys::find_chart_port_with_work;
use super::{
    AtomOccurrence, ChartPort, ChartPortId, CutEvent, ExactChartAtom, ExactChartPoint,
    ExactGeographicChartInventory, ExactQuotientKey, ExactSpanOwner, FallibleAvl, NodedExactChart,
    QuotientNode, QuotientNodeId, compare_quotient_keys, quotient_key, source_chart_point,
};
use crate::error::{ResourceError, Result};
use crate::geometry::segments::ordering::try_sort_by_with_work;
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{ExpansionBudget, for_each_candidate_pair};
#[cfg(test)]
use crate::work::Unmetered;
use crate::work::Work;

impl ExactGeographicChartInventory {
    #[cfg(test)]
    pub(in crate::geometry::topology) fn node(
        &self,
        budget: &mut ExpansionBudget,
    ) -> Result<NodedExactChart> {
        let mut work = Unmetered;
        self.node_with_work(budget, &mut work)
    }

    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "piece, port, quotient-node, and atom IDs index dense columns emitted by this noder"
    )]
    pub(in crate::geometry::topology) fn node_with_work<W: Work>(
        &self,
        budget: &mut ExpansionBudget,
        work: &mut W,
    ) -> Result<NodedExactChart> {
        let cut_capacity = self
            .pieces
            .len()
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?;
        work.charge_into_error(
            u64::try_from(cut_capacity).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut cuts = FallibleAvl::with_capacity(cut_capacity);
        let mut compare_cuts = |left: &CutEvent, right: &CutEvent, work: &mut W| {
            work.charge_into_error(1)?;
            compare_cut_events(left, right)
        };
        for piece in &self.pieces {
            work.charge_into_error(1)?;
            cuts.insert_with_work(
                CutEvent {
                    piece: piece.id,
                    parameter: ExactRational::zero(),
                    point: piece.start.clone(),
                },
                work,
                &mut compare_cuts,
            )?;
            cuts.insert_with_work(
                CutEvent {
                    piece: piece.id,
                    parameter: ExactRational::one(),
                    point: piece.end.clone(),
                },
                work,
                &mut compare_cuts,
            )?;
        }
        let source_keys = self.source_quotient_keys_with_work(work)?;
        let node_capacity = source_keys
            .len()
            .checked_add(self.pieces.len())
            .ok_or(ResourceError::SizeOverflow)?;
        work.charge_into_error(
            u64::try_from(node_capacity).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut constructed_nodes = FallibleAvl::with_capacity(node_capacity);
        let mut compare_keys = |left: &ExactQuotientKey, right: &ExactQuotientKey, work: &mut W| {
            work.charge_into_error(1)?;
            compare_quotient_keys(left, right)
        };
        for key in source_keys {
            constructed_nodes.insert_with_work(key, work, &mut compare_keys)?;
        }
        for piece in &self.pieces {
            work.charge_into_error(1)?;
            if !piece.source_range.start.is_zero() {
                let key = quotient_key(self.frame, &piece.start)?;
                if constructed_nodes.insert_with_work(key, work, &mut compare_keys)? {
                    budget.add(1)?;
                }
            }
            if !piece.source_range.end.is_one() {
                let key = quotient_key(self.frame, &piece.end)?;
                if constructed_nodes.insert_with_work(key, work, &mut compare_keys)? {
                    budget.add(1)?;
                }
            }
        }
        let mut proxy_piece_ids = Vec::with_capacity(self.pieces.len());
        let mut proxies = Vec::with_capacity(self.pieces.len());
        let mut proxyable = Vec::with_capacity(self.pieces.len());
        for piece in &self.pieces {
            work.charge_into_error(1)?;
            if let Some(proxy) = piece.outward_proxy()? {
                proxy_piece_ids.push(piece.id.0 as usize);
                proxies.push(proxy);
                proxyable.push(true);
            } else {
                proxyable.push(false);
            }
        }
        u32::try_from(proxies.len()).map_err(|_| ResourceError::SizeOverflow)?;

        let mut failure = None;
        let candidates = for_each_candidate_pair(
            &proxies,
            |index| index as u32,
            |left, right| {
                if let Err(error) = work.charge_into_error(1) {
                    failure = Some(error);
                    return std::ops::ControlFlow::Break(());
                }
                let left_piece = proxy_piece_ids[left];
                let right_piece = proxy_piece_ids[right];
                match self.refine_piece_pair(
                    [left_piece, right_piece],
                    None,
                    &mut cuts,
                    &mut constructed_nodes,
                    &mut compare_cuts,
                    &mut compare_keys,
                    budget,
                    work,
                ) {
                    Ok(()) => std::ops::ControlFlow::Continue(()),
                    Err(error) => {
                        failure = Some(error);
                        std::ops::ControlFlow::Break(())
                    },
                }
            },
        )?;
        if candidates.is_break() {
            return Err(failure.expect("a failed exact pair stores its first error"));
        }

        // Proxy-declined pieces have no conservative broadphase interval.
        // Enumerate every pair involving one, without imposing a candidate cap.
        for left in 0..self.pieces.len() {
            work.charge_into_error(1)?;
            if proxyable[left] {
                continue;
            }
            for (right, &is_proxyable) in proxyable.iter().enumerate().take(left) {
                work.charge_into_error(1)?;
                if is_proxyable {
                    self.refine_piece_pair(
                        [right, left],
                        None,
                        &mut cuts,
                        &mut constructed_nodes,
                        &mut compare_cuts,
                        &mut compare_keys,
                        budget,
                        work,
                    )?;
                }
            }
            for right in left + 1..self.pieces.len() {
                work.charge_into_error(1)?;
                self.refine_piece_pair(
                    [left, right],
                    None,
                    &mut cuts,
                    &mut constructed_nodes,
                    &mut compare_cuts,
                    &mut compare_keys,
                    budget,
                    work,
                )?;
            }
        }
        self.refine_seam_aliases(
            &mut cuts,
            &mut constructed_nodes,
            &mut compare_cuts,
            &mut compare_keys,
            budget,
            work,
        )?;
        self.refine_source_points(
            &mut cuts,
            &mut compare_cuts,
            &proxy_piece_ids,
            &proxies,
            &proxyable,
            work,
        )?;

        let cut_order = cuts.in_order_indices_with_work(work)?;
        work.charge_into_error(
            u64::try_from(self.pieces.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut occurrences = Vec::with_capacity(self.pieces.len());
        let mut previous_piece = None;
        let mut previous_cut: Option<usize> = None;
        for cut_index in cut_order {
            work.charge_into_error(1)?;
            let cut = &cuts.nodes[cut_index].value;
            if previous_piece != Some(cut.piece) {
                previous_piece = Some(cut.piece);
                previous_cut = None;
            }
            if let Some(start_index) = previous_cut {
                let start = &cuts.nodes[start_index].value;
                work.charge_into_error(1)?;
                if !start.point.same_position(&cut.point)? {
                    let piece = &self.pieces[cut.piece.0 as usize];
                    work.charge_into_error(1)?;
                    occurrences.push(AtomOccurrence {
                        start: start.point.clone(),
                        end: cut.point.clone(),
                        source_edge: piece.source_edge,
                        source_range: piece.source_range.compose_with_work(
                            &start.parameter,
                            &cut.parameter,
                            work,
                        )?,
                        lift: piece.lift,
                    });
                }
            }
            previous_cut = Some(cut_index);
        }

        let port_capacity = occurrences
            .len()
            .checked_mul(2)
            .and_then(|count| self.edges.len().checked_mul(2)?.checked_add(count))
            .and_then(|count| count.checked_add(self.points.len()))
            .ok_or(ResourceError::SizeOverflow)?;
        work.charge_into_error(
            u64::try_from(port_capacity).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut port_points = Vec::with_capacity(port_capacity);
        for occurrence in &occurrences {
            work.charge_into_error(2)?;
            port_points.extend([occurrence.start.clone(), occurrence.end.clone()]);
        }
        for edge in &self.edges {
            for point in edge.source {
                work.charge_into_error(1)?;
                port_points.push(source_chart_point(self.frame, point));
            }
        }
        for source in &self.points {
            work.charge_into_error(1)?;
            port_points.push(source_chart_point(self.frame, source.point));
        }
        try_sort_by_with_work(&mut port_points, work, |left, right, work| {
            work.charge_into_error(1)?;
            left.compare_lex(right)
        })?;
        work.charge_into_error(
            u64::try_from(port_points.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut unique_ports: Vec<ExactChartPoint> = Vec::with_capacity(port_points.len());
        for point in port_points {
            work.charge_into_error(1)?;
            let duplicate = if let Some(previous) = unique_ports.last() {
                work.charge_into_error(1)?;
                previous.compare_lex(&point)?.is_eq()
            } else {
                false
            };
            if duplicate {
                continue;
            }
            unique_ports.push(point);
        }
        work.charge_into_error(
            u64::try_from(unique_ports.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut ports = Vec::with_capacity(unique_ports.len());
        for (index, point) in unique_ports.into_iter().enumerate() {
            work.charge_into_error(1)?;
            ports.push(ChartPort {
                id: ChartPortId(u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?),
                point,
                node: QuotientNodeId(0),
            });
        }

        work.charge_into_error(
            u64::try_from(self.edges.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut source_edge_ports = Vec::with_capacity(self.edges.len());
        for edge in &self.edges {
            work.charge_into_error(1)?;
            source_edge_ports.push([
                find_chart_port_with_work(
                    &ports,
                    &source_chart_point(self.frame, edge.source[0]),
                    work,
                )?,
                find_chart_port_with_work(
                    &ports,
                    &source_chart_point(self.frame, edge.source[1]),
                    work,
                )?,
            ]);
        }
        work.charge_into_error(
            u64::try_from(self.points.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut source_point_ports = Vec::with_capacity(self.points.len());
        for point in &self.points {
            work.charge_into_error(1)?;
            source_point_ports.push(find_chart_port_with_work(
                &ports,
                &source_chart_point(self.frame, point.point),
                work,
            )?);
        }

        work.charge_into_error(
            u64::try_from(ports.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut quotient_keys = Vec::with_capacity(ports.len());
        for port in &ports {
            work.charge_into_error(1)?;
            quotient_keys.push((quotient_key(self.frame, &port.point)?, port.id));
        }
        try_sort_by_with_work(&mut quotient_keys, work, |left, right, work| {
            work.charge_into_error(1)?;
            compare_quotient_keys(&left.0, &right.0)
                .map(|order| order.then_with(|| left.1.cmp(&right.1)))
        })?;
        work.charge_into_error(
            u64::try_from(quotient_keys.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut nodes = Vec::with_capacity(quotient_keys.len());
        work.charge_into_error(
            u64::try_from(ports.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut node_ports = Vec::with_capacity(ports.len());
        let mut port_cursor = 0;
        while port_cursor < quotient_keys.len() {
            work.charge_into_error(1)?;
            let node_id = QuotientNodeId(
                u32::try_from(nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
            );
            let start = node_ports.len();
            let key = quotient_keys[port_cursor].0.clone();
            while port_cursor < quotient_keys.len() {
                work.charge_into_error(1)?;
                if !compare_quotient_keys(&key, &quotient_keys[port_cursor].0)?.is_eq() {
                    break;
                }
                let port = quotient_keys[port_cursor].1;
                ports[port.0 as usize].node = node_id;
                node_ports.push(port);
                port_cursor += 1;
            }
            nodes.push(QuotientNode {
                id: node_id,
                ports: start..node_ports.len(),
                key,
            });
        }

        work.charge_into_error(
            u64::try_from(occurrences.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut atom_occurrences = Vec::with_capacity(occurrences.len());
        for occurrence in occurrences {
            work.charge_into_error(1)?;
            let start = find_chart_port_with_work(&ports, &occurrence.start, work)?;
            let end = find_chart_port_with_work(&ports, &occurrence.end, work)?;
            let (ports, reversed) = if start <= end {
                ([start, end], false)
            } else {
                ([end, start], true)
            };
            atom_occurrences.push((
                ports,
                occurrence.source_edge,
                occurrence.source_range,
                occurrence.lift,
                reversed,
            ));
        }
        try_sort_by_with_work(&mut atom_occurrences, work, |left, right, work| {
            work.charge_into_error(1)?;
            let endpoints = left.0.cmp(&right.0);
            if !endpoints.is_eq() {
                return Ok(endpoints);
            }
            let source = left.1.cmp(&right.1);
            if !source.is_eq() {
                return Ok(source);
            }
            let start = left.2.start.try_cmp(&right.2.start)?;
            if !start.is_eq() {
                return Ok(start);
            }
            let end = left.2.end.try_cmp(&right.2.end)?;
            Ok(if end.is_eq() {
                left.3.cmp(&right.3).then_with(|| left.4.cmp(&right.4))
            } else {
                end
            })
        })?;
        work.charge_into_error(
            u64::try_from(atom_occurrences.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut atoms = Vec::with_capacity(atom_occurrences.len());
        let mut cursor = 0;
        while cursor < atom_occurrences.len() {
            work.charge_into_error(1)?;
            let endpoints = atom_occurrences[cursor].0;
            let mut owners = Vec::new();
            while cursor < atom_occurrences.len() && atom_occurrences[cursor].0 == endpoints {
                work.charge_into_error(1)?;
                let (_, source_edge, source_range, lift, reversed) = &atom_occurrences[cursor];
                owners.push(ExactSpanOwner {
                    source_edge: *source_edge,
                    source_range: source_range.clone(),
                    reversed: *reversed,
                    lift: *lift,
                });
                cursor += 1;
            }
            atoms.push(ExactChartAtom {
                ports: endpoints,
                owners: owners.into_boxed_slice(),
            });
        }

        let distinct_sources = self.distinct_source_piece_count_with_work(work)?;
        let net_atom_growth = atoms.len().saturating_sub(distinct_sources);
        budget.add(net_atom_growth)?;

        Ok(NodedExactChart {
            ports: ports.into_boxed_slice(),
            nodes: nodes.into_boxed_slice(),
            node_ports: node_ports.into_boxed_slice(),
            atoms: atoms.into_boxed_slice(),
            source_edge_ports: source_edge_ports.into_boxed_slice(),
            source_point_ports: source_point_ports.into_boxed_slice(),
        })
    }

    fn source_quotient_keys_with_work<W: Work>(
        &self,
        work: &mut W,
    ) -> Result<Vec<ExactQuotientKey>> {
        let source_endpoints = self
            .edges
            .len()
            .checked_mul(2)
            .and_then(|count| count.checked_add(self.points.len()))
            .ok_or(ResourceError::SizeOverflow)?;
        work.charge_into_error(
            u64::try_from(source_endpoints).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut keys = Vec::with_capacity(source_endpoints);
        for edge in &self.edges {
            for point in edge.source {
                work.charge_into_error(1)?;
                keys.push(quotient_key(
                    self.frame,
                    &source_chart_point(self.frame, point),
                )?);
            }
        }
        for point in &self.points {
            work.charge_into_error(1)?;
            keys.push(quotient_key(
                self.frame,
                &source_chart_point(self.frame, point.point),
            )?);
        }
        try_sort_by_with_work(&mut keys, work, |left, right, work| {
            work.charge_into_error(1)?;
            compare_quotient_keys(left, right)
        })?;
        work.charge_into_error(
            u64::try_from(keys.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut unique = Vec::with_capacity(keys.len());
        for key in keys {
            work.charge_into_error(1)?;
            let duplicate = if let Some(previous) = unique.last() {
                work.charge_into_error(1)?;
                compare_quotient_keys(previous, &key)?.is_eq()
            } else {
                false
            };
            if duplicate {
                continue;
            }
            unique.push(key);
        }
        Ok(unique)
    }

    fn distinct_source_piece_count_with_work<W: Work>(&self, work: &mut W) -> Result<usize> {
        work.charge_into_error(
            u64::try_from(self.pieces.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut endpoints = Vec::with_capacity(self.pieces.len());
        for piece in &self.pieces {
            work.charge_into_error(1)?;
            let (start, end) = if piece.start.compare_lex(&piece.end)?.is_le() {
                (piece.start.clone(), piece.end.clone())
            } else {
                (piece.end.clone(), piece.start.clone())
            };
            endpoints.push((start, end));
        }
        try_sort_by_with_work(&mut endpoints, work, |left, right, work| {
            work.charge_into_error(1)?;
            let start = left.0.compare_lex(&right.0)?;
            Ok(if start.is_eq() {
                left.1.compare_lex(&right.1)?
            } else {
                start
            })
        })?;
        let mut unique = 0_usize;
        let mut previous: Option<(ExactChartPoint, ExactChartPoint)> = None;
        for piece in endpoints {
            work.charge_into_error(1)?;
            let same = if let Some(previous) = &previous {
                previous.0.same_position(&piece.0)? && previous.1.same_position(&piece.1)?
            } else {
                false
            };
            if same {
                continue;
            }
            unique = unique.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            previous = Some(piece);
        }
        Ok(unique)
    }
}
