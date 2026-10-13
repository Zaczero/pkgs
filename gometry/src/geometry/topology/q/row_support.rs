use super::*;

pub(in crate::geometry::topology::q) fn build_span_line_rows(
    sources: &ExactGeographicChartInventory,
    spans: &[ChartSpan],
) -> Result<GlobalSpanRowSupport> {
    let mut memberships = Vec::with_capacity(sources.edges.len());
    for (span_index, span) in spans.iter().enumerate() {
        for owner in &span.owners {
            let edge = sources
                .edges
                .get(owner.source_edge.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q span has an unknown source edge".into())
                })?;
            if matches!(edge.kind, SourceEdgeKind::Line { .. }) {
                memberships.push((span_index, edge.row));
            }
        }
    }
    GlobalSpanRowSupport::build(spans.len(), &mut memberships)
}

pub(in crate::geometry::topology::q) fn build_node_line_boundary_rows(
    sources: &ExactGeographicChartInventory,
    topology: &QuotientFaces,
) -> Result<GlobalNodeRowSupport> {
    let mut memberships = Vec::with_capacity(sources.line_boundaries.len());
    for boundary in &sources.line_boundaries {
        let endpoints = topology
            .source_edge_ports
            .get(boundary.edge.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line boundary has no source edge ports".into())
            })?;
        let port_id = if boundary.at_end {
            endpoints.last().copied()
        } else {
            endpoints.first().copied()
        }
        .ok_or_else(|| GeometryErrorKind::Invalid("Q line boundary has no chart port".into()))?;
        let node = topology
            .ports
            .get(port_id.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "Q line boundary references an unknown chart port".into(),
                )
            })?
            .node;
        memberships.push((node.0 as usize, boundary.row));
    }
    memberships.sort_unstable();
    let mut odd_memberships = Vec::with_capacity(memberships.len());
    for run in memberships.chunk_by(|left, right| left == right) {
        if !run.len().is_multiple_of(2)
            && let Some(&membership) = run.first()
        {
            odd_memberships.push(membership);
        }
    }
    GlobalNodeRowSupport::build(topology.nodes.len(), &mut odd_memberships)
}

#[derive(Debug)]
pub(in crate::geometry::topology::q) struct QRowCsr<T> {
    pub(in crate::geometry::topology::q) offsets: Box<[usize]>,
    pub(in crate::geometry::topology::q) rows: Box<[T]>,
}

impl<T: Copy + Ord> QRowCsr<T> {
    pub(in crate::geometry::topology::q) fn build(
        row_count: usize,
        memberships: &mut Vec<(usize, T)>,
    ) -> Result<Self> {
        memberships.sort_unstable();
        memberships.dedup();
        let offset_count = row_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        let mut offsets = vec![0_usize; offset_count];
        for &(row, _) in memberships.iter() {
            let offset = row.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            let count = offsets.get_mut(offset).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q row CSR references an unknown item".into())
            })?;
            *count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
        let mut total = 0_usize;
        for offset in offsets.iter_mut().skip(1) {
            total = total
                .checked_add(*offset)
                .ok_or(ResourceError::SizeOverflow)?;
            *offset = total;
        }
        if total != memberships.len() {
            return Err(GeometryErrorKind::Invalid(
                "Q row CSR offsets do not match their membership count".into(),
            )
            .into());
        }
        let rows = memberships
            .iter()
            .map(|&(_, value)| value)
            .collect::<Vec<_>>();
        Ok(Self {
            offsets: offsets.into_boxed_slice(),
            rows: rows.into_boxed_slice(),
        })
    }

    pub(in crate::geometry::topology::q) fn row(&self, index: usize) -> Option<&[T]> {
        let next = index.checked_add(1)?;
        let start = *self.offsets.get(index)?;
        let end = *self.offsets.get(next)?;
        self.rows.get(start..end)
    }

    pub(in crate::geometry::topology::q) fn contains(&self, index: usize, value: T) -> bool {
        let Some(row) = self.row(index) else {
            return false;
        };
        row.binary_search(&value).is_ok()
    }
}

impl<T> QRowCsr<T> {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "HeapSize sums the published CSR buffers as an estimate"
    )]
    pub(in crate::geometry::topology::q) fn heap_bytes(&self) -> usize {
        self.offsets.len() * std::mem::size_of::<usize>()
            + self.rows.len() * std::mem::size_of::<T>()
    }
}

pub(in crate::geometry::topology::q) type GlobalSpanRowSupport = QRowCsr<SourceRowId>;
pub(in crate::geometry::topology::q) type GlobalNodeRowSupport = QRowCsr<SourceRowId>;
pub(in crate::geometry::topology::q) type CollapsedPointFaces = QRowCsr<usize>;
