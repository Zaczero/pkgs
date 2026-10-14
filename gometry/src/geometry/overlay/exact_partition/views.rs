use super::{
    Bounds, Cow, ExactChainView, ExactChainViews, ExactDyadic, ExactPartition, ExactRational,
    ExactSourceInventory, GeometryErrorKind, KeptNodes, NodeId, Ordering, PointKey, ResourceError,
    Result, Segment, SegmentSource, XY, embedding, parameter_along, walk_exact_chain,
};
#[cfg(test)]
use super::{SOURCE_SPAN_VIEW_ORDERED, SOURCE_SPAN_VIEW_SORTS};
use crate::geometry::graph::{RayAxis as Axis, RayPoints as _};
use crate::geometry::numerics::product::ExactSign;

impl ExactPartition<'_> {
    pub(in crate::geometry) const fn node_count(&self) -> usize {
        self.points.len()
    }

    pub(in crate::geometry) fn preserve_source_vertices(&mut self) {
        self.kept_nodes = KeptNodes::All;
    }

    pub(in crate::geometry) fn set_geographic_frame(
        &mut self,
        frame: crate::geographic::GeographicFrame,
    ) {
        self.kept_nodes = KeptNodes::All;
        self.embedding_domain = embedding::EmbeddingDomain::Geographic(frame);
    }

    /// A node's nearest-even binary64 point.
    #[expect(
        clippy::indexing_slicing,
        reason = "NodeId is admitted by this partition's PointArena"
    )]
    pub(in crate::geometry) fn nearest_xy(&self, node: NodeId) -> XY {
        self.points.rounded[node.0 as usize]
    }

    pub(super) fn is_stored(&self, node: NodeId) -> bool {
        self.points.is_stored(node.0)
    }

    pub(super) fn stored_key(&self, node: usize) -> Option<PointKey> {
        self.points.stored_key(node as u32)
    }

    /// A node's exact homogeneous point: built in place for a stored node,
    /// constructed once and kept for a crossing.
    pub(in crate::geometry) fn exact(&self, node: NodeId) -> Result<Cow<'_, [ExactDyadic; 3]>> {
        self.points.exact(node.0)
    }

    pub(in crate::geometry) fn parameter_on_source_segment(
        &self,
        segment: Segment,
        node: NodeId,
    ) -> Result<ExactRational> {
        let point = self.exact(node)?;
        let [numerator, denominator] = parameter_along(segment, &point)?;
        ExactRational::new(numerator, denominator)
    }

    pub(super) fn exact_owned(&self, node: NodeId) -> Result<[ExactDyadic; 3]> {
        match self.exact(node)? {
            Cow::Owned(point) => Ok(point),
            Cow::Borrowed(point) => Ok(point.clone()),
        }
    }

    pub(super) fn compare_axis(&self, left: NodeId, right: NodeId, axis: Axis) -> Result<Ordering> {
        self.points.compare_axis(left.0, right.0, axis)
    }

    pub(super) fn compare_axis_to(&self, node: NodeId, axis: Axis, value: f64) -> Result<Ordering> {
        self.points.compare_axis_to(node.0, axis, value)
    }

    pub(super) fn orientation(&self, a: NodeId, b: NodeId, c: NodeId) -> Result<ExactSign> {
        self.points.orientation(a.0, b.0, c.0)
    }

    pub(super) fn source_span_view_is_ordered(
        &self,
        segment: Segment,
        view: &[(u32, bool)],
    ) -> Result<bool> {
        let entry = |(span_id, reversed): (u32, bool)| -> Result<NodeId> {
            let span = self.spans.get(span_id as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("source span view references an unknown span".into())
            })?;
            Ok(if reversed { span.end } else { span.start })
        };
        let exit = |(span_id, reversed): (u32, bool)| -> Result<NodeId> {
            let span = self.spans.get(span_id as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("source span view references an unknown span".into())
            })?;
            Ok(if reversed { span.start } else { span.end })
        };
        let first = *view.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("source span view has no first span".into())
        })?;
        let last = *view.last().ok_or_else(|| {
            GeometryErrorKind::Invalid("source span view has no last span".into())
        })?;
        if self.stored_key(entry(first)?.0 as usize) != Some(PointKey::new(segment.start))
            || self.stored_key(exit(last)?.0 as usize) != Some(PointKey::new(segment.end))
        {
            return Ok(false);
        }
        for pair in view.windows(2) {
            let [left, right] = pair else {
                continue;
            };
            if exit(*left)? != entry(*right)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Return each source segment's exact atomic span run in source direction.
    ///
    /// The partition provenance is the only source-to-atom relation here.
    /// Ordering the inherited atoms from the stored source endpoint gives
    /// callers a view of the source spelling without rebuilding a coordinate
    /// graph.
    #[expect(
        clippy::indexing_slicing,
        reason = "span IDs are assigned from self.spans; each zipped source segment owns one pre-sized view"
    )]
    pub(in crate::geometry) fn source_span_views(
        &self,
        source_segments: &[Segment],
    ) -> Result<Vec<Vec<(u32, bool)>>> {
        let mut views = Vec::with_capacity(source_segments.len());
        views.resize_with(source_segments.len(), Vec::new);
        for (span, owners) in self.provenance.iter().enumerate() {
            for &(source, reversed) in owners {
                let view = views.get_mut(source as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "exact partition provenance references an unknown source".into(),
                    )
                })?;
                view.push((span as u32, reversed));
            }
        }
        for (segment, view) in source_segments.iter().zip(&mut views) {
            if view.len() < 2 {
                continue;
            }
            // Provenance enumeration often already forms a contiguous source
            // run; only genuinely unordered rows need the exact sweep sort.
            if self.source_span_view_is_ordered(*segment, view)? {
                #[cfg(test)]
                SOURCE_SPAN_VIEW_ORDERED.with(|count| count.set(count.get().saturating_add(1)));
                continue;
            }
            #[cfg(test)]
            SOURCE_SPAN_VIEW_SORTS.with(|count| count.set(count.get().saturating_add(1)));
            // Each atom's entry is its node nearest the source start; ordered
            // along the source, every exit is the next atom's entry.
            let entry = |(span, reversed): (u32, bool)| {
                let span = self.spans[span as usize];
                if reversed { span.end.0 } else { span.start.0 }
            };
            let exit = |(span, reversed): (u32, bool)| {
                let span = self.spans[span as usize];
                if reversed { span.start } else { span.end }
            };
            let mut ordered = std::mem::take(view);
            self.points
                .order_along(*segment, &mut ordered, entry, |_, _| {
                    Err(GeometryErrorKind::Invalid(
                        "exact partition source atoms share an entry node".into(),
                    )
                    .into())
                })?;
            let first = NodeId(entry(ordered[0]));
            let last = exit(ordered[ordered.len().wrapping_sub(1)]);
            if self.stored_key(first.0 as usize) != Some(PointKey::new(segment.start))
                || self.stored_key(last.0 as usize) != Some(PointKey::new(segment.end))
            {
                return Err(GeometryErrorKind::Invalid(
                    "exact partition source atoms do not reach their endpoint".into(),
                )
                .into());
            }
            if ordered
                .array_windows::<2>()
                .any(|&[left, right]| exit(left).0 != entry(right))
            {
                return Err(GeometryErrorKind::Invalid(
                    "exact partition source atoms are not a contiguous run".into(),
                )
                .into());
            }
            *view = ordered;
        }
        Ok(views)
    }

    /// Build maximal chain views directly from exact span incidence.
    ///
    /// `source_operands` maps the partition's source ordinals to coverage row
    /// identities. An atom is shared only when its exact provenance names two
    /// distinct rows. A chain ends at every non-degree-two node and wherever
    /// that shared/exterior class changes.
    #[expect(
        clippy::indexing_slicing,
        reason = "span and node IDs come from this partition; degree-two continuation rows are built from its checked topology"
    )]
    pub(in crate::geometry) fn coverage_chain_views(
        &self,
        source_operands: &[u32],
    ) -> Result<ExactChainViews> {
        let (topology, edge_of_slot) = self.local_topology()?;
        let mut shared_by_span = Vec::with_capacity(self.spans.len());
        for owners in self.provenance.iter() {
            let mut first = None;
            let mut second = None;
            let mut count = 0_usize;
            for &(source, _) in owners {
                let operand = *source_operands.get(source as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "exact partition provenance references an unknown source".into(),
                    )
                })?;
                if Some(operand) == first || Some(operand) == second {
                    continue;
                }
                if first.is_none() {
                    first = Some(operand);
                } else if second.is_none() {
                    second = Some(operand);
                }
                count = count.wrapping_add(1);
            }
            shared_by_span.push(count == 2);
        }

        let mut node_pin = Vec::with_capacity(self.node_count());
        node_pin.resize(self.node_count(), false);
        for (node, &[start, end]) in topology.starts.array_windows::<2>().enumerate() {
            let slots = start as usize..end as usize;
            node_pin[node] = slots.len() != 2
                || shared_by_span[edge_of_slot[slots.start] as usize]
                    != shared_by_span[edge_of_slot[slots.start.wrapping_add(1)] as usize];
        }

        let mut visited = Vec::with_capacity(self.spans.len());
        visited.resize(self.spans.len(), false);
        let mut chain_of_span = Vec::with_capacity(self.spans.len());
        chain_of_span.resize(self.spans.len(), u32::MAX);
        let mut chains = Vec::with_capacity(self.spans.len());

        let append_chain = |seed: usize,
                            origin: NodeId,
                            cycle: bool,
                            visited: &mut [bool],
                            chain_of_span: &mut [u32],
                            chains: &mut Vec<ExactChainView>|
         -> Result<()> {
            let (mut nodes, spans) = walk_exact_chain(
                &topology,
                &edge_of_slot,
                &self.spans,
                &node_pin,
                visited,
                origin,
                seed as u32,
            )?;
            debug_assert_eq!(cycle, !node_pin[origin.0 as usize]);
            let mut spans = spans;
            if cycle {
                let count = spans.len();
                let mut minimum = 0;
                for index in 1..count {
                    if self.compare_nodes(nodes[index], nodes[minimum])? == Ordering::Less {
                        minimum = index;
                    }
                }
                nodes[..count].rotate_left(minimum);
                nodes[count] = nodes[0];
                spans.rotate_left(minimum);
                if self.compare_nodes(nodes[1], nodes[count.wrapping_sub(1)])? == Ordering::Greater
                {
                    nodes[1..count].reverse();
                    spans.reverse();
                }
            }
            let shared = shared_by_span[seed];
            debug_assert!(
                spans
                    .iter()
                    .all(|&span| shared_by_span[span as usize] == shared)
            );
            let chain = u32::try_from(chains.len()).map_err(|_| ResourceError::SizeOverflow)?;
            for &span in &spans {
                chain_of_span[span as usize] = chain;
            }
            chains.push(ExactChainView {
                nodes,
                spans,
                shared,
            });
            Ok(())
        };

        // Open chains start at pinned junctions. Consume them before an
        // unpinned interior span can split one walk and leave a visited suffix.
        for seed in 0..self.spans.len() {
            if visited[seed] {
                continue;
            }
            let span = self.spans[seed];
            let start_pinned = node_pin[span.start.0 as usize];
            let end_pinned = node_pin[span.end.0 as usize];
            if !start_pinned && !end_pinned {
                continue;
            }
            let origin = if start_pinned {
                if node_pin[span.end.0 as usize]
                    && self.compare_nodes(span.end, span.start)? == Ordering::Less
                {
                    span.end
                } else {
                    span.start
                }
            } else {
                span.end
            };
            append_chain(
                seed,
                origin,
                false,
                &mut visited,
                &mut chain_of_span,
                &mut chains,
            )?;
        }
        // No unvisited span touches a pin now. Every remaining component has
        // degree two at every node and is therefore a closed cycle.
        for seed in 0..self.spans.len() {
            if visited[seed] {
                continue;
            }
            let span = self.spans[seed];
            debug_assert!(!node_pin[span.start.0 as usize]);
            debug_assert!(!node_pin[span.end.0 as usize]);
            append_chain(
                seed,
                span.start,
                true,
                &mut visited,
                &mut chain_of_span,
                &mut chains,
            )?;
        }
        Ok(ExactChainViews {
            chains,
            chain_of_span,
        })
    }

    pub(in crate::geometry) fn owners(&self, span: usize) -> &[SegmentSource] {
        self.provenance.owners(span)
    }

    pub(super) fn node_in_bounds(&self, node: NodeId, bounds: Bounds) -> Result<bool> {
        Ok(
            self.compare_axis_to(node, Axis::X, bounds.minx())? != Ordering::Less
                && self.compare_axis_to(node, Axis::X, bounds.maxx())? != Ordering::Greater
                && self.compare_axis_to(node, Axis::Y, bounds.miny())? != Ordering::Less
                && self.compare_axis_to(node, Axis::Y, bounds.maxy())? != Ordering::Greater,
        )
    }

    #[cfg(test)]
    pub(super) fn round_nodes(&self, nodes: &[NodeId]) -> Vec<XY> {
        nodes.iter().map(|&node| self.nearest_xy(node)).collect()
    }
}

impl ExactSourceInventory {
    /// Return the admitted source-segment ordinal range for each input ring.
    ///
    /// Ring membership is recorded while the inventory stages each occurrence;
    /// callers do not need to recover it from endpoint coordinates.
    pub(in crate::geometry) fn source_ring_segment_ranges(
        &self,
    ) -> Result<Vec<std::ops::Range<usize>>> {
        let mut ranges = Vec::with_capacity(self.ring_count);
        ranges.resize_with(self.ring_count, || None);
        for (ordinal, ring) in self.source_rings.iter().copied().enumerate() {
            let Some(ring) = ring else {
                continue;
            };
            let ring = usize::try_from(ring).map_err(|_| ResourceError::SizeOverflow)?;
            let end = ordinal.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            let range = ranges.get_mut(ring).ok_or_else(|| {
                GeometryErrorKind::Invalid("source segment references an unknown input ring".into())
            })?;
            match range {
                None => *range = Some(ordinal..end),
                Some(range) if range.end == ordinal => range.end = end,
                Some(_) => {
                    return Err(GeometryErrorKind::Invalid(
                        "source ring segments are not contiguous in the inventory".into(),
                    )
                    .into());
                },
            }
        }
        ranges
            .into_iter()
            .enumerate()
            .map(|(ring, range)| {
                range.ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        format!("source ring {ring} has no admitted segments").into(),
                    )
                    .into()
                })
            })
            .collect()
    }
}
