use std::cmp::Ordering;

use crate::geometry::exact_arith::ExactSign;
use crate::geometry::segments::stored_cross_sign;
use crate::geometry::{Orientation, Segment, XY, orientation};

pub(in crate::geometry) struct StoredHull<Site> {
    pub(in crate::geometry) vertices: Vec<Site>,
    rightmost: usize,
}

impl<Site: Copy> StoredHull<Site> {
    pub(in crate::geometry) fn new(sorted: &[Site], point: impl Fn(Site) -> XY) -> Self {
        let mut vertices = Vec::with_capacity(sorted.len().wrapping_mul(2));
        for &site in sorted {
            while vertices.len() >= 2 {
                let a = vertices[vertices.len() - 2];
                let b = vertices[vertices.len() - 1];
                if orientation(point(a), point(b), point(site)) == Orientation::CounterClockwise {
                    break;
                }
                vertices.pop();
            }
            vertices.push(site);
        }
        let rightmost = vertices.len().wrapping_sub(1);
        let lower_count = vertices.len();
        for &site in sorted.iter().rev().skip(1) {
            while vertices.len() > lower_count {
                let a = vertices[vertices.len() - 2];
                let b = vertices[vertices.len() - 1];
                if orientation(point(a), point(b), point(site)) == Orientation::CounterClockwise {
                    break;
                }
                vertices.pop();
            }
            vertices.push(site);
        }
        if vertices.len() > 1 {
            vertices.pop();
        }
        Self {
            vertices,
            rightmost,
        }
    }

    pub(in crate::geometry) fn support<const DOT: bool>(
        &self,
        chord: Segment,
        negate: bool,
        point: impl Fn(Site) -> XY,
    ) -> usize {
        let y_sign = if DOT {
            chord.end.y.partial_cmp(&chord.start.y)
        } else {
            chord.end.x.partial_cmp(&chord.start.x)
        }
        .expect("finite source direction");
        let y_sign = if negate { y_sign.reverse() } else { y_sign };
        if y_sign == Ordering::Equal {
            let x_sign = if DOT {
                chord.end.x.partial_cmp(&chord.start.x)
            } else {
                chord.start.y.partial_cmp(&chord.end.y)
            }
            .expect("finite source direction");
            return if (if negate { x_sign.reverse() } else { x_sign }).is_gt() {
                self.rightmost
            } else {
                0
            };
        }
        let upper = y_sign.is_gt();
        let edge_count = if upper {
            self.vertices.len().wrapping_sub(self.rightmost)
        } else {
            self.rightmost
        };
        let position = |step: usize| {
            if upper && step != 0 {
                self.vertices.len().wrapping_sub(step)
            } else {
                step
            }
        };
        let (mut low, mut high) = (0_usize, edge_count);
        while low < high {
            let middle = low.wrapping_add(high.wrapping_sub(low) / 2);
            let a = point(self.vertices[position(middle)]);
            let b = point(self.vertices[position(middle.wrapping_add(1))]);
            let (a, b) = if DOT {
                (XY::new(-a.y, a.x), XY::new(-b.y, b.x))
            } else {
                (a, b)
            };
            let sign = stored_cross_sign(chord.start, chord.end, a, b);
            let rises = if negate {
                sign == ExactSign::Negative
            } else {
                sign == ExactSign::Positive
            };
            if rises {
                low = middle.wrapping_add(1);
            } else {
                high = middle;
            }
        }
        position(low)
    }

    pub(in crate::geometry) fn sorted_sites(
        &self,
        point: impl Fn(Site) -> XY,
    ) -> impl Iterator<Item = Site> {
        let (mut lower, mut upper) = (0_usize, self.vertices.len().wrapping_sub(1));
        std::iter::from_fn(move || {
            let left = (lower <= self.rightmost).then(|| self.vertices[lower]);
            let right = (upper > self.rightmost).then(|| self.vertices[upper]);
            let take_lower = match (left, right) {
                (Some(left), Some(right)) => {
                    let (left, right) = (point(left), point(right));
                    left.x
                        .partial_cmp(&right.x)
                        .expect("finite hull X")
                        .then(left.y.partial_cmp(&right.y).expect("finite hull Y"))
                        .is_le()
                },
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => return None,
            };
            if take_lower {
                lower = lower.wrapping_add(1);
                left
            } else {
                upper = upper.wrapping_sub(1);
                right
            }
        })
    }

    pub(in crate::geometry) fn merged_sites(
        &self,
        other: &Self,
        point: impl Fn(Site) -> XY,
    ) -> Vec<Site> {
        let mut left = self.sorted_sites(&point).peekable();
        let mut right = other.sorted_sites(&point).peekable();
        let mut sites = Vec::with_capacity(self.vertices.len().wrapping_add(other.vertices.len()));
        while let (Some(&a), Some(&b)) = (left.peek(), right.peek()) {
            let (a, b) = (point(a), point(b));
            if a.x
                .partial_cmp(&b.x)
                .expect("finite hull X")
                .then(a.y.partial_cmp(&b.y).expect("finite hull Y"))
                .is_le()
            {
                sites.push(left.next().expect("nonempty lower merge head"));
            } else {
                sites.push(right.next().expect("nonempty upper merge head"));
            }
        }
        sites.extend(left);
        sites.extend(right);
        sites
    }
}
