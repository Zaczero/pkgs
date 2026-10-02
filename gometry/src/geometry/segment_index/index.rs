use rstar::{AABB, RTreeObject};

use crate::HeapSize;
#[cfg(test)]
use crate::geometry::point_on_segment;
use crate::geometry::{BulkRTree, Point, Segment, same_point};

/// One segment as an R-tree leaf, carrying its position in the build slice
/// (so pair-once sweeps can keep `ordinal > i` candidates).
pub(crate) struct SegmentEntry {
    pub segment: Segment,
    pub ordinal: usize,
    envelope: AABB<[f64; 2]>,
}

impl SegmentEntry {
    pub(super) fn new(segment: Segment, ordinal: usize) -> Self {
        Self {
            segment,
            ordinal,
            envelope: segment_envelope(segment),
        }
    }
}

impl RTreeObject for SegmentEntry {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.envelope
    }
}

pub(crate) fn segment_envelope(segment: Segment) -> AABB<[f64; 2]> {
    // Corners are ordered here — skip from_corners' second min/max pass.
    AABB::from_bounds(
        [
            segment.start.x.min(segment.end.x),
            segment.start.y.min(segment.end.y),
        ],
        [
            segment.start.x.max(segment.end.x),
            segment.start.y.max(segment.end.y),
        ],
    )
}

/// An R-tree over a segment set.
pub(crate) struct SegmentIndex {
    tree: BulkRTree<SegmentEntry>,
}

impl HeapSize for SegmentIndex {
    fn heap_bytes(&self) -> usize {
        super::bulk_rtree_heap_bytes(&self.tree)
    }
}

impl SegmentIndex {
    pub(crate) fn build(segments: &[Segment]) -> Self {
        Self::build_from_iter(segments.iter().copied())
    }

    /// Build straight from a segment iterator — entries are gathered once for
    /// the bulk load, with no caller-side `Vec<Segment>` staging.
    pub(crate) fn build_from_iter(segments: impl IntoIterator<Item = Segment>) -> Self {
        let entries = segments
            .into_iter()
            .enumerate()
            .map(|(ordinal, segment)| SegmentEntry::new(segment, ordinal))
            .collect();
        Self {
            tree: BulkRTree::bulk_load_with_params(entries),
        }
    }

    /// Indexed entries whose envelope intersects `query`'s — the noding /
    /// pair-sweep candidate set (each entry carries its build ordinal).
    pub(crate) fn intersecting_candidates(
        &self,
        query: Segment,
    ) -> impl Iterator<Item = &SegmentEntry> + '_ {
        self.tree
            .locate_in_envelope_intersecting(segment_envelope(query))
    }

    /// Whether a point lies on any indexed segment. The envelope query keeps
    /// the exact segment predicate on the small candidate set, so repeated
    /// point probes do not rescan the full linework.
    #[cfg(test)]
    pub(crate) fn covers_point(&self, point: Point) -> bool {
        let query = Segment {
            start: point.xy(),
            end: point.xy(),
        };
        self.intersecting_candidates(query)
            .any(|entry| point_on_segment(point, entry.segment.start, entry.segment.end))
    }
}

/// An R-tree over a point set (Hausdorff refinement, clearance candidates).
pub(crate) struct PointSetIndex {
    tree: BulkRTree<[f64; 2]>,
}

impl PointSetIndex {
    pub(crate) fn build(points: impl IntoIterator<Item = Point>) -> Self {
        let coords = points.into_iter().map(|point| [point.x, point.y]).collect();
        Self {
            tree: BulkRTree::bulk_load_with_params(coords),
        }
    }

    /// The nearest indexed point (INCLUDING a coincident one, distance 0) with
    /// its squared distance — the nearest-points/shortest-line query. `None`
    /// only for an empty set.
    pub(crate) fn nearest(&self, point: Point) -> Option<(Point, f64)> {
        self.tree
            .nearest_neighbor_with_distance_2([point.x, point.y])
            .map(|(coords, distance)| (Point::new_raw_xy(coords[0], coords[1]), distance))
    }

    /// The nearest indexed point that is not bit-identical to `point`, with
    /// its squared distance (`None` when none exists) — the
    /// minimum-clearance "nearest other vertex" query.
    ///
    /// Skips only **true** coordinate identity. A squared residual of 0 from
    /// underflow (distinct points whose `dx²+dy²` collapses) is still a
    /// clearance candidate — the finisher recomputes in distance space.
    pub(crate) fn nearest_other(&self, point: Point) -> Option<(Point, f64)> {
        self.tree
            .nearest_neighbor_iter_with_distance_2([point.x, point.y])
            .find(|&(coords, _distance)| {
                !same_point(point, Point::new_raw_xy(coords[0], coords[1]))
            })
            .map(|(coords, distance)| (Point::new_raw_xy(coords[0], coords[1]), distance))
    }
}

impl HeapSize for PointSetIndex {
    fn heap_bytes(&self) -> usize {
        super::bulk_rtree_heap_bytes(&self.tree)
    }
}

#[cfg(test)]
mod query_tests {
    use super::*;

    #[test]
    fn segment_index_covers_segment_interiors() {
        let segments = [
            Segment {
                start: crate::geometry::XY::new(0.0, 0.0),
                end: crate::geometry::XY::new(1.0, 0.0),
            },
            Segment {
                start: crate::geometry::XY::new(1.0, 0.0),
                end: crate::geometry::XY::new(1.0, 1.0),
            },
        ];
        let index = SegmentIndex::build(&segments);
        assert!(index.covers_point(Point::new_raw_xy(0.5, 0.0)));
    }

    #[test]
    fn chord_query_prunes_diagonal_envelope_amplification() {
        use crate::geometry::XY;
        use crate::geometry::segment_index::ChordIndex;

        let segments: Vec<_> = (0..4096)
            .map(|index| {
                let coordinate = f64::from(index);
                Segment {
                    start: XY::new(coordinate, coordinate + 32.0),
                    end: XY::new(coordinate + 0.25, coordinate + 32.0),
                }
            })
            .collect();
        let index = ChordIndex::build(&segments);
        let chord = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(8192.0, 8192.0),
        };
        assert_eq!(
            SegmentIndex::build(&segments)
                .intersecting_candidates(chord)
                .count(),
            4096
        );
        let mut checked = 0;
        assert!(index.all_candidates(chord, |_| {
            checked += 1;
            true
        }));
        assert_eq!(checked, 0);
    }

    #[test]
    fn chord_query_retains_contacts_and_uncertain_boxes() {
        use crate::geometry::segment_index::ChordIndex;
        use crate::geometry::{XY, segments_intersect};

        let cases = [
            (
                Segment {
                    start: XY::new(0.0, 0.0),
                    end: XY::new(4.0, 4.0),
                },
                [
                    Segment {
                        start: XY::new(1.0, 1.0),
                        end: XY::new(3.0, 3.0),
                    },
                    Segment {
                        start: XY::new(0.0, 1.0),
                        end: XY::new(1.0, 0.0),
                    },
                    Segment {
                        start: XY::new(4.0, 4.0),
                        end: XY::new(5.0, 3.0),
                    },
                ],
            ),
            (
                Segment {
                    start: XY::new(-1e308, -1e308),
                    end: XY::new(1e308, 1e308),
                },
                [
                    Segment {
                        start: XY::new(-1.0, 1.0),
                        end: XY::new(1.0, -1.0),
                    },
                    Segment {
                        start: XY::new(0.0, 0.0),
                        end: XY::new(0.0, 0.0),
                    },
                    Segment {
                        start: XY::new(1e308, 1e308),
                        end: XY::new(1e308, 0.0),
                    },
                ],
            ),
            (
                Segment {
                    start: XY::new(0.0, 0.0),
                    end: XY::new(f64::from_bits(4), f64::from_bits(4)),
                },
                [
                    Segment {
                        start: XY::new(f64::from_bits(2), 0.0),
                        end: XY::new(f64::from_bits(2), f64::from_bits(4)),
                    },
                    Segment {
                        start: XY::new(0.0, 0.0),
                        end: XY::new(0.0, 0.0),
                    },
                    Segment {
                        start: XY::new(f64::from_bits(4), f64::from_bits(4)),
                        end: XY::new(f64::from_bits(4), 0.0),
                    },
                ],
            ),
        ];
        for (chord, segments) in cases {
            let segments: Vec<_> = segments.into_iter().cycle().take(2048).collect();
            let index = ChordIndex::build(&segments);
            let mut retained = vec![false; segments.len()];
            assert!(index.all_candidates(chord, |entry| {
                retained[entry.ordinal] = true;
                true
            }));
            for (ordinal, &segment) in segments.iter().enumerate() {
                assert!(segments_intersect(chord, segment));
                assert!(retained[ordinal], "{chord:?} against {segment:?}");
            }
        }
    }

    #[test]
    fn point_set_index_nearest_other_excludes_the_query_point() {
        let points = [
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1.0, 0.0),
            Point::new_raw_xy(0.0, 1.0),
        ];
        let index = PointSetIndex::build(points.iter().copied());
        let (other, _) = index.nearest_other(points[0]).unwrap();
        assert!(!same_point(other, points[0]));
    }
}

// Prepared polygonal point membership lives in `geometry::point_location`
// (hierarchical Y-stabbing). This module keeps bipartite segment/point R-trees
// and the candidate-pair sweep.
