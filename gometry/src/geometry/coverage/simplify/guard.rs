use super::*;

/// The topology guard behind chain simplification: indexes every original
/// segment and vertex once; a removal is safe when the shortcut crosses no
/// other segment and no foreign vertex lies inside the swept triangle.
/// Checking against the ORIGINAL linework is conservative (a segment already
/// simplified away can still veto), which can under-simplify but can never
/// produce a crossing.
pub(in crate::geometry) struct TopologyGuard<'a> {
    segments: &'a [Segment],
    // Both trees build LAZILY on the first probe: exact-collinear removals
    // (the dominant case on digitized/gridded inputs) are accepted without
    // probing, so an all-collinear pass never pays either bulk load.
    segment_index: std::cell::OnceCell<ChordIndex>,
    vertex_cloud: std::cell::Cell<Vec<[f64; 2]>>,
    vertex_tree: std::cell::OnceCell<BulkRTree<[f64; 2]>>,
}

impl<'a> TopologyGuard<'a> {
    /// Wrap the original segments and the vertex cloud they span.
    ///
    /// `vertex_cloud` is the guard's required XY index payload, gathered by
    /// walking original carriers once. Construction does not allocate.
    pub(in crate::geometry) const fn new(
        segments: &'a [Segment],
        vertex_cloud: Vec<[f64; 2]>,
    ) -> Self {
        Self {
            segments,
            segment_index: std::cell::OnceCell::new(),
            vertex_cloud: std::cell::Cell::new(vertex_cloud),
            vertex_tree: std::cell::OnceCell::new(),
        }
    }

    fn segment_index(&self) -> &ChordIndex {
        self.segment_index
            .get_or_init(|| ChordIndex::build(self.segments))
    }

    fn vertex_tree(&self) -> &BulkRTree<[f64; 2]> {
        self.vertex_tree
            .get_or_init(|| BulkRTree::bulk_load_with_params(self.vertex_cloud.take()))
    }

    pub(in crate::geometry) fn removal_is_safe(
        &self,
        prev: XY,
        vertex: XY,
        next: XY,
        chain_segments: &[Segment],
    ) -> bool {
        // Exact-collinear interior vertex: the shortcut occupies precisely
        // the union of the two original segments — the linework's point set
        // is unchanged, so both probes are vacuous. This keeps gridded /
        // densified inputs from ever touching the trees.
        if orientation(prev, vertex, next) == Orientation::Collinear
            && vertex.x >= prev.x.min(next.x)
            && vertex.x <= prev.x.max(next.x)
            && vertex.y >= prev.y.min(next.y)
            && vertex.y <= prev.y.max(next.y)
        {
            return true;
        }
        // The contact guard and the swept-region guard below are separate:
        // the first preserves the embedding against linework contacts, while
        // the second preserves the ownership of vertices inside the region
        // between the old two-edge path and this shortcut.
        let shortcut = Segment {
            start: prev,
            end: next,
        };
        let no_contact = self.segment_index().all_candidates(shortcut, |entry| {
            let chain_segment = if chain_segments.is_empty() {
                // The public scalar simplifier has no shared-chain
                // provenance. Preserve its established local-endpoint
                // exemption; coverage callers always provide the exact
                // source segment view above.
                [entry.segment.start, entry.segment.end]
                    .into_iter()
                    .any(|point| {
                        same_point(point, prev)
                            || same_point(point, vertex)
                            || same_point(point, next)
                    })
            } else {
                chain_segments.iter().any(|segment| {
                    undirected_segment_edge_key(*segment)
                        == undirected_segment_edge_key(entry.segment)
                })
            };
            if chain_segment {
                return true;
            }
            match segment_contact_exact(shortcut, entry.segment) {
                crate::geometry::Contact::Disjoint => true,
                crate::geometry::Contact::Cross => false,
                crate::geometry::Contact::Touch(endpoint) => {
                    let point = endpoint.point(shortcut, entry.segment);
                    same_point(point, prev) || same_point(point, next)
                },
                contact @ crate::geometry::Contact::Collinear => {
                    let span = contact
                        .shared_span(shortcut, entry.segment)
                        .expect("collinear contact has a shared span");
                    same_point(span.start, span.end)
                        && (same_point(span.start, prev) || same_point(span.start, next))
                },
            }
        });
        if !no_contact {
            return false;
        }
        // No foreign vertex inside or on the swept triangle. On the
        // original valid linework, a segment entering this region would have
        // to cross one of its two original sides (already an invalid source
        // contact) or have an endpoint in the region. Thus these two guards
        // together certify the replacement without a dynamic spatial graph.
        let (min_x, max_x) = (
            prev.x.min(vertex.x).min(next.x),
            prev.x.max(vertex.x).max(next.x),
        );
        let (min_y, max_y) = (
            prev.y.min(vertex.y).min(next.y),
            prev.y.max(vertex.y).max(next.y),
        );
        let inside = self
            .vertex_tree()
            .locate_in_envelope_intersecting(AABB::from_corners([min_x, min_y], [max_x, max_y]))
            .any(|coords| {
                let point = XY::new(coords[0], coords[1]);
                !(same_point(point, prev) || same_point(point, vertex) || same_point(point, next))
                    && point_in_triangle(point, prev, vertex, next)
            });
        !inside
    }
}

fn point_in_triangle(point: XY, a: XY, b: XY, c: XY) -> bool {
    let oab = orientation(a, b, point);
    let obc = orientation(b, c, point);
    let oca = orientation(c, a, point);
    let clockwise = [oab, obc, oca]
        .iter()
        .all(|&o| o != Orientation::CounterClockwise);
    let counter = [oab, obc, oca].iter().all(|&o| o != Orientation::Clockwise);
    clockwise || counter
}
