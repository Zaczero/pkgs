use super::*;

#[derive(Clone, Copy, Debug)]
pub(in crate::geometry::topology) enum QSpatialItem {
    Span(usize),
    Point(QPortId),
}

/// Candidate-only R-tree: every indexed envelope is outward-rounded from Q
/// coordinates, and query hits are refined against their exact source.
pub(in crate::geometry::topology::q) struct QSpatialIndex {
    pub(in crate::geometry::topology::q) index: SegmentIndex,
    pub(in crate::geometry::topology::q) indexed_items: Box<[QSpatialItem]>,
    pub(in crate::geometry::topology::q) unindexed_items: Box<[QSpatialItem]>,
    #[cfg(test)]
    pub(in crate::geometry::topology::q) candidate_visits: std::sync::atomic::AtomicUsize,
}

impl QSpatialIndex {
    pub(in crate::geometry::topology::q) fn build(
        ports: &[QPort],
        nodes: &[QNode],
        spans: &[ChartSpan],
        node_point_rows: &GlobalNodeRowSupport,
    ) -> Result<Self> {
        let mut point_port_count = 0_usize;
        for (node_index, node) in nodes.iter().enumerate() {
            let point_rows = node_point_rows.row(node_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q node point rows are incomplete".into())
            })?;
            if !point_rows.is_empty() {
                point_port_count = point_port_count
                    .checked_add(node.ports.len())
                    .ok_or(ResourceError::SizeOverflow)?;
            }
        }
        let item_capacity = spans
            .len()
            .checked_add(point_port_count)
            .ok_or(ResourceError::SizeOverflow)?;
        let mut proxies = Vec::with_capacity(item_capacity);
        let mut indexed_items = Vec::with_capacity(item_capacity);
        let mut unindexed_items = Vec::new();
        for (span_index, span) in spans.iter().enumerate() {
            let start = ports
                .get(span.ports[0].0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q span has no start port".into()))?;
            let end = ports
                .get(span.ports[1].0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q span has no end port".into()))?;
            let item = QSpatialItem::Span(span_index);
            if let Some(proxy) = outward_segment_proxy(&start.point, &end.point)? {
                proxies.push(proxy);
                indexed_items.push(item);
            } else {
                unindexed_items.push(item);
            }
        }
        for (node_index, node) in nodes.iter().enumerate() {
            let rows = node_point_rows.row(node_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q node point rows are incomplete".into())
            })?;
            if rows.is_empty() {
                continue;
            }
            let node_ports = ports.get(node.ports.clone()).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q point support has an invalid port range".into())
            })?;
            for port in node_ports {
                let item = QSpatialItem::Point(port.id);
                if let Some(proxy) = outward_segment_proxy(&port.point, &port.point)? {
                    proxies.push(proxy);
                    indexed_items.push(item);
                } else {
                    unindexed_items.push(item);
                }
            }
        }
        if proxies.len() != indexed_items.len() {
            return Err(
                GeometryErrorKind::Invalid("Q spatial index columns diverged".into()).into(),
            );
        }
        Ok(Self {
            index: SegmentIndex::build_from_iter(proxies),
            indexed_items: indexed_items.into_boxed_slice(),
            unindexed_items: unindexed_items.into_boxed_slice(),
            #[cfg(test)]
            candidate_visits: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    pub(in crate::geometry::topology::q) fn for_each_candidate<B>(
        &self,
        query: Option<Segment>,
        mut visit: impl FnMut(QSpatialItem) -> std::ops::ControlFlow<B>,
    ) -> Result<std::ops::ControlFlow<B>> {
        if let Some(query) = query {
            for entry in self.index.intersecting_candidates(query) {
                let item = self
                    .indexed_items
                    .get(entry.ordinal)
                    .copied()
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q spatial index has an unknown item".into())
                    })?;
                #[cfg(test)]
                self.candidate_visits
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if let std::ops::ControlFlow::Break(value) = visit(item) {
                    return Ok(std::ops::ControlFlow::Break(value));
                }
            }
        } else {
            for &item in &self.indexed_items {
                #[cfg(test)]
                self.candidate_visits
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if let std::ops::ControlFlow::Break(value) = visit(item) {
                    return Ok(std::ops::ControlFlow::Break(value));
                }
            }
        }
        for &item in &self.unindexed_items {
            #[cfg(test)]
            self.candidate_visits
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if let std::ops::ControlFlow::Break(value) = visit(item) {
                return Ok(std::ops::ControlFlow::Break(value));
            }
        }
        Ok(std::ops::ControlFlow::Continue(()))
    }

    #[cfg(test)]
    pub(in crate::geometry::topology::q) fn candidate_visits(&self) -> usize {
        self.candidate_visits
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(in crate::geometry::topology::q) fn reset_candidate_visits(&self) {
        self.candidate_visits
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }
}

impl HeapSize for QSpatialIndex {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "HeapSize estimates the retained spatial-index and ID buffers"
    )]
    fn heap_bytes(&self) -> usize {
        self.index.heap_bytes()
            + self.indexed_items.len() * std::mem::size_of::<QSpatialItem>()
            + self.unindexed_items.len() * std::mem::size_of::<QSpatialItem>()
    }
}

impl std::fmt::Debug for QSpatialIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QSpatialIndex")
            .field("indexed_items", &self.indexed_items.len())
            .field("unindexed_items", &self.unindexed_items.len())
            .finish_non_exhaustive()
    }
}

pub(in crate::geometry::topology::q) fn outward_segment_proxy(
    start: &ExactChartPoint,
    end: &ExactChartPoint,
) -> Result<Option<Segment>> {
    let [start_min_x, start_min_y, start_max_x, start_max_y] = start.outward_bounds()?;
    let [end_min_x, end_min_y, end_max_x, end_max_y] = end.outward_bounds()?;
    let bounds = [
        start_min_x.min(end_min_x),
        start_min_y.min(end_min_y),
        start_max_x.max(end_max_x),
        start_max_y.max(end_max_y),
    ];
    if !bounds.into_iter().all(f64::is_finite) {
        return Ok(None);
    }
    let [min_x, min_y, max_x, max_y] = bounds;
    Ok(Some(Segment {
        start: crate::geometry::XY::new(min_x, min_y),
        end: crate::geometry::XY::new(max_x, max_y),
    }))
}
