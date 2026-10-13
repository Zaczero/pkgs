//! Source-ordered segment ranges retain directional bounds for chord contacts.

use std::ops::Range;

use rstar::{AABB, Envelope as _};

use super::StoredHull;
use super::index::{SegmentEntry, SegmentIndex, segment_envelope};
use crate::geometry::{Orientation, Segment, XY, orientation};

const LEAF: usize = 8;

#[cfg(test)]
std::thread_local! {
    static CONTACT_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(in crate::geometry) fn take_contact_visits() -> usize {
    CONTACT_VISITS.with(|visits| visits.replace(0))
}

struct Node {
    range: Range<usize>,
    children: Option<[usize; 2]>,
    envelope: AABB<[f64; 2]>,
    hull: StoredHull<XY>,
}

struct HullSegments {
    entries: Vec<SegmentEntry>,
    nodes: Vec<Node>,
}

enum Storage {
    Envelope(SegmentIndex),
    Hull(Box<HullSegments>),
}

pub(in crate::geometry) struct ChordIndex {
    storage: Storage,
}

impl ChordIndex {
    pub(in crate::geometry) fn build(segments: &[Segment]) -> Self {
        if segments.len() <= 1024 {
            return Self {
                storage: Storage::Envelope(SegmentIndex::build(segments)),
            };
        }
        let mut index = HullSegments {
            entries: segments
                .iter()
                .copied()
                .enumerate()
                .map(|(ordinal, segment)| SegmentEntry::new(segment, ordinal))
                .collect(),
            nodes: Vec::with_capacity(segments.len().wrapping_mul(2) / LEAF),
        };
        index.build(0..segments.len());
        Self {
            storage: Storage::Hull(Box::new(index)),
        }
    }

    pub(in crate::geometry) fn all_candidates(
        &self,
        query: Segment,
        mut check: impl FnMut(&SegmentEntry) -> bool,
    ) -> bool {
        let mut check = |entry: &SegmentEntry| {
            #[cfg(test)]
            CONTACT_VISITS.with(|visits| visits.set(visits.get().wrapping_add(1)));
            check(entry)
        };
        match &self.storage {
            Storage::Envelope(index) => index.intersecting_candidates(query).all(check),
            Storage::Hull(index) => index.query(
                index.nodes.len().wrapping_sub(1),
                query,
                &segment_envelope(query),
                &mut { check },
            ),
        }
    }
}

impl HullSegments {
    fn build(&mut self, range: Range<usize>) -> usize {
        let children = (range.len() > LEAF).then(|| {
            let middle = range.start.wrapping_add(range.len() / 2);
            [
                self.build(range.start..middle),
                self.build(middle..range.end),
            ]
        });
        let mut points = if let Some([left, right]) = children {
            self.nodes
                .get(left)
                .expect("left child precedes its parent")
                .hull
                .merged_sites(
                    &self
                        .nodes
                        .get(right)
                        .expect("right child precedes its parent")
                        .hull,
                    |point| point,
                )
        } else {
            self.entries
                .get(range.clone())
                .expect("leaf range belongs to segment entries")
                .iter()
                .flat_map(|entry| [entry.segment.start, entry.segment.end])
                .collect()
        };
        if children.is_none() {
            points.sort_unstable_by(|a, b| {
                a.x.partial_cmp(&b.x)
                    .expect("finite hull X")
                    .then(a.y.partial_cmp(&b.y).expect("finite hull Y"))
            });
        }
        let hull = StoredHull::new(&points, |point| point);
        let mut lower = [f64::INFINITY; 2];
        let mut upper = [f64::NEG_INFINITY; 2];
        for point in &hull.vertices {
            lower[0] = lower[0].min(point.x);
            lower[1] = lower[1].min(point.y);
            upper[0] = upper[0].max(point.x);
            upper[1] = upper[1].max(point.y);
        }
        let index = self.nodes.len();
        self.nodes.push(Node {
            range,
            children,
            envelope: AABB::from_bounds(lower, upper),
            hull,
        });
        index
    }

    fn query(
        &self,
        ordinal: usize,
        query: Segment,
        envelope: &AABB<[f64; 2]>,
        check: &mut impl FnMut(&SegmentEntry) -> bool,
    ) -> bool {
        let node = self
            .nodes
            .get(ordinal)
            .expect("query node belongs to the built hull tree");
        if !node.envelope.intersects(envelope) {
            return true;
        }
        let maximum = *node
            .hull
            .vertices
            .get(node.hull.support::<false>(query, false, |point| point))
            .expect("hull maximum support belongs to its vertices");
        if orientation(query.start, query.end, maximum) == Orientation::Clockwise {
            return true;
        }
        let minimum = *node
            .hull
            .vertices
            .get(node.hull.support::<false>(query, true, |point| point))
            .expect("hull minimum support belongs to its vertices");
        if orientation(query.start, query.end, minimum) == Orientation::CounterClockwise {
            return true;
        }
        if let Some([left, right]) = node.children {
            self.query(left, query, envelope, check) && self.query(right, query, envelope, check)
        } else {
            self.entries
                .get(node.range.clone())
                .expect("leaf range belongs to segment entries")
                .iter()
                .filter(|entry| segment_envelope(entry.segment).intersects(envelope))
                .all(check)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::segments::contact::segments_intersect;

    #[test]
    fn hull_contact_candidates_retain_exact_contacts_across_scales() {
        for exponent in [-500, 0, 500] {
            let scale = 2.0_f64.powi(exponent);
            let segments: Vec<_> = (0..2048)
                .map(|index| {
                    let x = f64::from(index % 64) - 32.0;
                    let y = f64::from(index / 64) - 16.0;
                    Segment {
                        start: XY::new(x * scale, y * scale),
                        end: XY::new((x + 0.75) * scale, (y + 0.25) * scale),
                    }
                })
                .collect();
            let index = ChordIndex::build(&segments);
            for (a, b) in [
                ((-32.0, -16.0), (32.0, 16.0)),
                ((0.0, -16.0), (0.0, 16.0)),
                ((-32.0, 0.0), (32.0, 0.0)),
                ((0.0, 0.0), (0.0, 0.0)),
                ((-32.0, -16.0), (-31.25, -15.75)),
            ] {
                let query = Segment {
                    start: XY::new(a.0 * scale, a.1 * scale),
                    end: XY::new(b.0 * scale, b.1 * scale),
                };
                let mut candidates = vec![false; segments.len()];
                assert!(index.all_candidates(query, |entry| {
                    candidates[entry.ordinal] = true;
                    true
                }));
                for (ordinal, segment) in segments.iter().enumerate() {
                    assert!(
                        !segments_intersect(query, *segment) || candidates[ordinal],
                        "lost contact {ordinal} at exponent {exponent}: {query:?}/{segment:?}",
                    );
                }
            }
        }
    }
}
