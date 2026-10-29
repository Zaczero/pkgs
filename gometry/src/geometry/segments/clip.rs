//! Exact clipping of source segments to rectangular bounds.

use super::ExactDyadic;
use super::contact::{PairOrientations, collinear_shared_span_exact, touch_endpoint};
use crate::error::Result;
use crate::geometry::numerics::product::ExactSign;
use crate::geometry::tessellation::exact::{compare_ratios, round_ratio};
use crate::geometry::{Bounds, Ordering, Segment, XY, same_point, same_topological_coordinate};

/// Exact endpoints of a segment's closed intersection with a rectangle.
///
/// `exact` is absent only for an original stored endpoint. Constructed
/// crossings retain their exact homogeneous coordinate; consumers can node the
/// local span without treating its rounded endpoints as new source geometry.
#[derive(Clone, Debug)]
pub(in crate::geometry) struct SegmentClipPoint {
    pub(in crate::geometry) point: XY,
    pub(in crate::geometry) exact: Option<Box<[ExactDyadic; 3]>>,
}

#[derive(Clone, Debug)]
pub(in crate::geometry) struct SegmentClipInterval {
    pub(in crate::geometry) start: SegmentClipPoint,
    pub(in crate::geometry) end: SegmentClipPoint,
}

/// Clip a stored source carrier to a closed rectangle without replacing that
/// carrier by its rounded endpoint chord.
#[expect(
    clippy::indexing_slicing,
    reason = "hit indices are bounded by the fixed six-slot buffer populated from the segment and four rectangle sides"
)]
pub(in crate::geometry) fn clip_segment_rect_interval(
    segment: Segment,
    rect: Bounds,
) -> Result<Option<SegmentClipInterval>> {
    let (start, end) = (segment.start, segment.end);
    if same_point(start, end) {
        return Ok(rect.contains_point(start).then_some(SegmentClipInterval {
            start: SegmentClipPoint {
                point: start,
                exact: None,
            },
            end: SegmentClipPoint {
                point: start,
                exact: None,
            },
        }));
    }
    if rect.contains_point(start) && rect.contains_point(end) {
        return Ok(Some(SegmentClipInterval {
            start: SegmentClipPoint {
                point: start,
                exact: None,
            },
            end: SegmentClipPoint {
                point: end,
                exact: None,
            },
        }));
    }
    let corners = [
        XY::new(rect.minx(), rect.miny()),
        XY::new(rect.maxx(), rect.miny()),
        XY::new(rect.maxx(), rect.maxy()),
        XY::new(rect.minx(), rect.maxy()),
    ];
    let edges = [
        (corners[0], corners[1]),
        (corners[1], corners[2]),
        (corners[2], corners[3]),
        (corners[3], corners[0]),
    ];
    let segment = Segment { start, end };
    let zero = ExactDyadic::from_f64(0.0);
    let one = ExactDyadic::from_f64(1.0);
    let mut hits: [Option<ClipHit>; 6] = [const { None }; 6];
    let mut n_hits = 0_usize;
    let point_in = |p: XY| {
        p.x >= rect.minx() && p.x <= rect.maxx() && p.y >= rect.miny() && p.y <= rect.maxy()
    };
    if point_in(start) {
        push_clip_hit(
            ClipHit {
                parameter: SegmentParameter::Start,
                point: start,
                exact: None,
            },
            &mut hits,
            &mut n_hits,
        )?;
    }
    if point_in(end) {
        push_clip_hit(
            ClipHit {
                parameter: SegmentParameter::End,
                point: end,
                exact: None,
            },
            &mut hits,
            &mut n_hits,
        )?;
    }
    for (edge_start, edge_end) in edges {
        let edge = Segment {
            start: edge_start,
            end: edge_end,
        };
        let orientations = PairOrientations::new(segment, edge);
        if orientations.all_collinear() {
            if let Some(span) = collinear_shared_span_exact(segment, edge) {
                push_clip_hit(
                    ClipHit {
                        parameter: clip_segment_parameter(segment, span.start)?,
                        point: span.start,
                        exact: None,
                    },
                    &mut hits,
                    &mut n_hits,
                )?;
                push_clip_hit(
                    ClipHit {
                        parameter: clip_segment_parameter(segment, span.end)?,
                        point: span.end,
                        exact: None,
                    },
                    &mut hits,
                    &mut n_hits,
                )?;
            }
        } else if orientations.straddles_or_touches() {
            if orientations.has_collinear() {
                if let Some(point) = touch_endpoint(segment, edge, orientations) {
                    push_clip_hit(
                        ClipHit {
                            parameter: clip_segment_parameter(segment, point)?,
                            point,
                            exact: None,
                        },
                        &mut hits,
                        &mut n_hits,
                    )?;
                }
            } else {
                push_clip_hit(
                    clip_segment_edge_cross_exact(segment, edge_start, edge_end)?,
                    &mut hits,
                    &mut n_hits,
                )?;
            }
        }
    }
    if n_hits == 0 {
        return Ok(None);
    }
    let mut i_min = 0_usize;
    let mut i_max = 0_usize;
    for i in 1..n_hits {
        let left = hits[i].as_ref().expect("stored clip hit");
        let min = hits[i_min].as_ref().expect("stored clip hit");
        if left.parameter.cmp(&min.parameter, &zero, &one)?.is_lt() {
            i_min = i;
        }
        let max = hits[i_max].as_ref().expect("stored clip hit");
        if left.parameter.cmp(&max.parameter, &zero, &one)?.is_gt() {
            i_max = i;
        }
    }
    let first = hits[i_min].take().expect("stored clip hit");
    let last = if i_min == i_max {
        first.clone()
    } else {
        hits[i_max].take().expect("stored clip hit")
    };
    Ok(Some(SegmentClipInterval {
        start: SegmentClipPoint {
            point: first.point,
            exact: first.exact.map(Box::new),
        },
        end: SegmentClipPoint {
            point: last.point,
            exact: last.exact.map(Box::new),
        },
    }))
}

#[derive(Clone)]
struct ClipHit {
    parameter: SegmentParameter,
    point: XY,
    exact: Option<[ExactDyadic; 3]>,
}

/// A position along a segment's carrier: an endpoint, or the exact ratio
/// `num / den` of the way from start to end, possibly outside the segment.
#[derive(Clone, Debug)]
pub(in crate::geometry) enum SegmentParameter {
    Start,
    Ratio { num: ExactDyadic, den: ExactDyadic },
    End,
}

impl SegmentParameter {
    pub(in crate::geometry) fn cmp(
        &self,
        other: &Self,
        zero: &ExactDyadic,
        one: &ExactDyadic,
    ) -> Result<Ordering> {
        match (self, other) {
            (Self::Start, Self::Start) | (Self::End, Self::End) => Ok(Ordering::Equal),
            (Self::Start, Self::End) => Ok(Ordering::Less),
            (Self::End, Self::Start) => Ok(Ordering::Greater),
            (Self::Start, Self::Ratio { num, den }) => compare_ratios(zero, one, num, den),
            (Self::End, Self::Ratio { num, den }) => compare_ratios(one, one, num, den),
            (Self::Ratio { num, den }, Self::Start) => compare_ratios(num, den, zero, one),
            (Self::Ratio { num, den }, Self::End) => compare_ratios(num, den, one, one),
            (
                Self::Ratio {
                    num: left_num,
                    den: left_den,
                },
                Self::Ratio {
                    num: right_num,
                    den: right_den,
                },
            ) => compare_ratios(left_num, left_den, right_num, right_den),
        }
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "hit count is bounded by distinct endpoints of one segment's convex rectangle intersection"
)]
fn push_clip_hit(hit: ClipHit, output: &mut [Option<ClipHit>; 6], count: &mut usize) -> Result<()> {
    let (zero, one) = (ExactDyadic::zero(), ExactDyadic::one());
    for existing in output[..*count].iter().flatten() {
        if existing.parameter.cmp(&hit.parameter, &zero, &one)? == Ordering::Equal {
            return Ok(());
        }
    }
    output[*count] = Some(hit);
    *count = (*count).wrapping_add(1);
    Ok(())
}

pub(in crate::geometry) fn clip_segment_parameter(
    segment: Segment,
    point: XY,
) -> Result<SegmentParameter> {
    if same_point(segment.start, point) {
        return Ok(SegmentParameter::Start);
    }
    if same_point(segment.end, point) {
        return Ok(SegmentParameter::End);
    }
    let use_x = !same_topological_coordinate(segment.start.x, segment.end.x);
    let (point, start, end) = if use_x {
        (point.x, segment.start.x, segment.end.x)
    } else {
        (point.y, segment.start.y, segment.end.y)
    };
    let numerator = ExactDyadic::from_f64(point).try_subtract(ExactDyadic::from_f64(start))?;
    let denominator = ExactDyadic::from_f64(end).try_subtract(ExactDyadic::from_f64(start))?;
    Ok(SegmentParameter::Ratio {
        num: numerator,
        den: denominator,
    })
}

fn clip_segment_edge_cross_exact(
    segment: Segment,
    edge_start: XY,
    edge_end: XY,
) -> Result<ClipHit> {
    debug_assert!(
        same_topological_coordinate(edge_start.x, edge_end.x)
            || same_topological_coordinate(edge_start.y, edge_end.y),
        "rectangle edge must be axis aligned"
    );
    let start_x = ExactDyadic::from_f64(segment.start.x);
    let end_x = ExactDyadic::from_f64(segment.end.x);
    let start_y = ExactDyadic::from_f64(segment.start.y);
    let end_y = ExactDyadic::from_f64(segment.end.y);
    let delta_x = end_x.try_subtract(start_x.clone())?;
    let delta_y = end_y.try_subtract(start_y.clone())?;
    if same_topological_coordinate(edge_start.x, edge_end.x) {
        let edge_x = ExactDyadic::from_f64(edge_start.x);
        let offset = edge_x.clone().try_subtract(start_x)?;
        let mut denominator = delta_x.clone();
        let mut numerator = start_y
            .try_product(&delta_x)?
            .try_add(delta_y.try_product(&offset)?)?;
        if denominator.sign() == ExactSign::Negative {
            denominator = denominator.neg();
            numerator = numerator.neg();
        }
        let x = edge_x.try_product(&denominator)?;
        let free = round_ratio(&numerator, &denominator)?;
        Ok(ClipHit {
            parameter: SegmentParameter::Ratio {
                num: offset,
                den: delta_x,
            },
            point: XY::new(edge_start.x, free),
            exact: Some([x, numerator, denominator]),
        })
    } else {
        let edge_y = ExactDyadic::from_f64(edge_start.y);
        let offset = edge_y.clone().try_subtract(start_y)?;
        let mut denominator = delta_y.clone();
        let mut numerator = start_x
            .try_product(&delta_y)?
            .try_add(delta_x.try_product(&offset)?)?;
        if denominator.sign() == ExactSign::Negative {
            denominator = denominator.neg();
            numerator = numerator.neg();
        }
        let y = edge_y.try_product(&denominator)?;
        let free = round_ratio(&numerator, &denominator)?;
        Ok(ClipHit {
            parameter: SegmentParameter::Ratio {
                num: offset,
                den: delta_y,
            },
            point: XY::new(free, edge_start.y),
            exact: Some([numerator, y, denominator]),
        })
    }
}

#[cfg(test)]
#[path = "clip/tests.rs"]
mod tests;
