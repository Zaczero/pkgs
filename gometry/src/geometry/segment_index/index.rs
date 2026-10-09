use ahash::HashSetExt as _;
use rstar::{AABB, PointDistance, RTreeObject};

use crate::HeapSize;
use crate::collections::HashSet;
#[cfg(test)]
use crate::geometry::point_on_segment;
use crate::geometry::{
    BulkRTree, Point, PointKey, Segment, SquaredDistanceKey, XY, point_pair_distance_key,
    same_point,
};

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

/// An R-tree over source point entries used by nearest-point and clearance queries.
pub(crate) struct PointSetIndex {
    tree: BulkRTree<PointEntry>,
}

/// A point-index leaf retains its position in the full-ordinate source.
pub(crate) struct PointEntry {
    coords: [f64; 2],
    ordinal: usize,
}

impl RTreeObject for PointEntry {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point(self.coords)
    }
}

impl PointDistance for PointEntry {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        self.coords.distance_2(point)
    }
}

fn consider_clearance_neighbor(
    best: &mut (usize, SquaredDistanceKey),
    point: Point,
    entry: &PointEntry,
) -> crate::error::Result<()> {
    let target = Point::new_raw_xy(entry.coords[0], entry.coords[1]);
    if same_point(point, target) {
        return Ok(());
    }
    let key = point_pair_distance_key(point.xy(), target.xy());
    let order = key.cmp_distance(&best.1)?;
    if order.is_lt() || order.is_eq() && entry.ordinal < best.0 {
        *best = (entry.ordinal, key);
    }
    Ok(())
}

impl PointSetIndex {
    /// Build the clearance tree with the first source ordinal for each XY.
    pub(crate) fn build_clearance_with_ordinals(points: &[Point]) -> Self {
        let mut seen = HashSet::with_capacity(points.len());
        let entries: Vec<_> = points
            .iter()
            .copied()
            .enumerate()
            .filter_map(|(ordinal, point)| {
                let unique =
                    point.x.is_nan() || point.y.is_nan() || seen.insert(PointKey::new(point));
                unique.then_some(PointEntry {
                    coords: [point.x, point.y],
                    ordinal,
                })
            })
            .collect();
        drop(seen);
        Self {
            tree: BulkRTree::bulk_load_with_params(entries),
        }
    }

    pub(crate) fn build_with_ordinals(points: impl IntoIterator<Item = Point>) -> Self {
        let entries = points
            .into_iter()
            .enumerate()
            .map(|(ordinal, point)| PointEntry {
                coords: [point.x, point.y],
                ordinal,
            })
            .collect();
        Self {
            tree: BulkRTree::bulk_load_with_params(entries),
        }
    }

    pub(crate) fn nearest_ordinal(&self, point: Point) -> crate::error::Result<Option<usize>> {
        let mut best = None;
        let mut limit = f64::INFINITY;
        for (entry, distance) in self
            .tree
            .nearest_neighbor_iter_with_distance_2([point.x, point.y])
        {
            if distance.is_normal() && distance > limit {
                break;
            }
            let target = crate::geometry::XY::new(entry.coords[0], entry.coords[1]);
            let key = point_pair_distance_key(point.xy(), target);
            let replace = match best.as_ref() {
                None => true,
                Some((ordinal, incumbent)) => {
                    let order = key.cmp(incumbent)?;
                    order.is_lt() || order.is_eq() && entry.ordinal < *ordinal
                },
            };
            if replace {
                limit = key.upper_bound() * (1.0 + 4.0 * f64::EPSILON);
                best = Some((entry.ordinal, key));
            }
        }
        Ok(best.map(|(ordinal, _)| ordinal))
    }

    /// Nearest distinct XY source that can improve `incumbent`, ordered by
    /// exact source distance and then by source ordinal. With no incumbent,
    /// returns the nearest distinct source. The clearance tree stores only
    /// the first source ordinal for each XY alias. Its first neighbor and the
    /// incumbent supply conservative coordinate-envelope bounds; uncertain
    /// bounds use a full leaf scan if the radius cannot be certified or the
    /// coordinate envelope is unbounded.
    pub(crate) fn nearest_clearance_neighbor(
        &self,
        point: Point,
        incumbent: Option<&SquaredDistanceKey>,
    ) -> crate::error::Result<Option<(usize, SquaredDistanceKey)>> {
        let Some((seed, _)) = self
            .tree
            .nearest_neighbor_iter_with_distance_2([point.x, point.y])
            .find(|&(entry, _)| {
                !same_point(point, Point::new_raw_xy(entry.coords[0], entry.coords[1]))
            })
        else {
            return Ok(None);
        };
        let mut best = (
            seed.ordinal,
            point_pair_distance_key(point.xy(), XY::new(seed.coords[0], seed.coords[1])),
        );
        let seed_upper = best.1.upper_bound();
        let upper = incumbent.map_or_else(|| seed_upper, |key| seed_upper.min(key.upper_bound()));

        if upper.is_finite() {
            let search_key = if let Some(incumbent) = incumbent {
                if best.1.cmp_distance(incumbent)?.is_gt() {
                    incumbent
                } else {
                    &best.1
                }
            } else {
                &best.1
            };
            let radius = upper.sqrt().next_up();
            let radius_covers_search = if radius.is_finite() {
                let radius_key = point_pair_distance_key(XY::new(0.0, 0.0), XY::new(radius, 0.0));
                !radius_key.cmp_distance(search_key)?.is_lt()
            } else {
                false
            };
            let lower = [
                (point.x - radius).next_down(),
                (point.y - radius).next_down(),
            ];
            let upper = [(point.x + radius).next_up(), (point.y + radius).next_up()];
            if radius_covers_search
                && lower
                    .iter()
                    .chain(&upper)
                    .all(|coordinate| coordinate.is_finite())
            {
                let envelope = AABB::from_corners(lower, upper);
                for entry in self.tree.locate_in_envelope_intersecting(envelope) {
                    consider_clearance_neighbor(&mut best, point, entry)?;
                }
            } else {
                for entry in &self.tree {
                    consider_clearance_neighbor(&mut best, point, entry)?;
                }
            }
        } else {
            for entry in &self.tree {
                consider_clearance_neighbor(&mut best, point, entry)?;
            }
        }
        if let Some(incumbent) = incumbent
            && !best.1.cmp_distance(incumbent)?.is_lt()
        {
            return Ok(None);
        }
        Ok(Some(best))
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
    fn clearance_point_query_matches_exact_exhaustive_source_order() {
        let cases = [
            vec![
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(1.0, 2.0_f64.powi(-28)),
                Point::new_raw_xy(1.0, 0.0),
                Point::new_raw_xy(-0.0, 0.0),
                Point::new_raw_xy(20.0, 20.0),
            ],
            vec![
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(2.0_f64.powi(-700), 0.0),
                Point::new_raw_xy(3.0 * 2.0_f64.powi(-700), 0.0),
                Point::new_raw_xy(100.0, 100.0),
            ],
            vec![
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(2.0_f64.powi(600), 0.0),
                Point::new_raw_xy(3.0 * 2.0_f64.powi(600), 0.0),
                Point::new_raw_xy(-(2.0_f64.powi(600)), 2.0_f64.powi(600)),
            ],
        ];
        for points in cases {
            let index = PointSetIndex::build_clearance_with_ordinals(&points);
            for (probe_ordinal, probe) in points.iter().copied().enumerate() {
                let mut expected: Option<(usize, SquaredDistanceKey)> = None;
                for (ordinal, target) in points.iter().copied().enumerate() {
                    if same_point(probe, target) {
                        continue;
                    }
                    let key = point_pair_distance_key(probe.xy(), target.xy());
                    let replace = match expected.as_ref() {
                        None => true,
                        Some((best_ordinal, best)) => {
                            let order = key
                                .cmp_distance(best)
                                .expect("finite test coordinates have comparable exact keys");
                            order.is_lt() || order.is_eq() && ordinal < *best_ordinal
                        },
                    };
                    if replace {
                        expected = Some((ordinal, key));
                    }
                }
                let actual = index.nearest_clearance_neighbor(probe, None).unwrap();
                assert_eq!(
                    actual.as_ref().map(|(ordinal, _)| *ordinal),
                    expected.as_ref().map(|(ordinal, _)| *ordinal),
                    "probe source ordinal {probe_ordinal}",
                );
                if let (Some((_, actual)), Some((_, expected))) = (actual, expected) {
                    assert!(
                        actual.cmp_distance(&expected).unwrap().is_eq(),
                        "probe source ordinal {probe_ordinal}",
                    );
                }
            }
        }
    }
}

// Prepared polygonal point membership lives in `geometry::point_location`
// (hierarchical Y-stabbing). This module keeps bipartite segment/point R-trees
// and the candidate-pair sweep.
