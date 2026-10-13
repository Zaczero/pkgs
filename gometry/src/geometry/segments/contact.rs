//! Exact classification and witnesses for segment contacts.

use super::{Orientation, crossing_point, orientation};
use crate::error::Result;
use crate::geometry::numerics::product::ExactSign;
use crate::geometry::{Bounds, Coordinates, PointKey, Segment, XY, same_point};

/// Return a binary64 witness when the stored segments contact, or `None` when
/// they are disjoint. Contact classification is exact; touches return stored
/// endpoints and collinear overlaps return a stored endpoint of the shared
/// interval. Proper crossings are rounded to nearest-even, so the emitted
/// coordinates need not lie exactly on either carrier.
pub(in crate::geometry) fn segment_contact_point(
    left: Segment,
    right: Segment,
) -> Result<Option<XY>> {
    let contact = segment_contact_exact(left, right);
    Ok(match contact {
        Contact::Cross => Some(crossing_point(left, right)?.point),
        Contact::Touch(endpoint) => Some(endpoint.point(left, right)),
        // A positive-length overlap has no single crossing; its near end is a
        // real point of both segments and is the conventional witness.
        Contact::Collinear => contact.shared_span(left, right).map(|span| span.start),
        Contact::Disjoint => None,
    })
}

/// Transversal crossing point of two segments, or `None` when they do not
/// meet (or are collinear — overlaps are the endpoint-overlap path's job).
///
/// **Existence is decided exactly** by the robust orientation predicates, so
/// floating-point conditioning can never drop a true crossing (nor invent
/// one). A touching endpoint is returned bit-exactly and a proper crossing as
/// [`crossing_point`], the nearest-even rounding of the exact point. The
/// returned point is the plain X/Y crossing; Z/M is restored later.
pub(in crate::geometry) fn segment_cross_point(a: Segment, b: Segment) -> Result<Option<XY>> {
    Ok(match segment_contact_exact(a, b) {
        Contact::Cross => Some(crossing_point(a, b)?.point),
        Contact::Touch(endpoint) => Some(endpoint.point(a, b)),
        Contact::Disjoint | Contact::Collinear => None,
    })
}

/// The endpoint at which a pair of segments touches without crossing:
/// a straight segment meets the other's carrier once, so an endpoint on it is
/// the contact, bit-exactly. `None` when neither straddles on an endpoint.
pub(super) fn touch_endpoint(a: Segment, b: Segment, orientations: PairOrientations) -> Option<XY> {
    if orientations.right_start == Orientation::Collinear {
        return Some(b.start);
    }
    if orientations.right_end == Orientation::Collinear {
        return Some(b.end);
    }
    if orientations.left_start == Orientation::Collinear {
        return Some(a.start);
    }
    if orientations.left_end == Orientation::Collinear {
        return Some(a.end);
    }
    None
}

pub(crate) fn point_on_segment(
    point: impl Into<XY>,
    start: impl Into<XY>,
    end: impl Into<XY>,
) -> bool {
    let (point, start, end) = (point.into(), start.into(), end.into());
    // Cheap closed-envelope reject first: orientation() is the robust adaptive
    // determinant and the compiler cannot reorder it past the && short-circuit.
    if point.x < start.x.min(end.x)
        || point.x > start.x.max(end.x)
        || point.y < start.y.min(end.y)
        || point.y > start.y.max(end.y)
    {
        return false;
    }
    orientation(start, end, point) == Orientation::Collinear
}

pub(crate) fn line_segments<C: Coordinates + ?Sized>(
    points: &C,
) -> impl Iterator<Item = Segment> + '_ {
    let mut columns = points
        .xy_columns()
        .map(|(xs, ys)| (xs.array_windows::<2>(), ys.array_windows::<2>()));
    let mut pairs = points.segment_pairs();
    std::iter::from_fn(move || {
        if let Some((xs, ys)) = &mut columns {
            let x = xs.next()?;
            let y = ys.next()?;
            return Some(Segment {
                start: XY::new(x[0], y[0]),
                end: XY::new(x[1], y[1]),
            });
        }
        pairs.next().map(|[start, end]| Segment {
            start: start.into(),
            end: end.into(),
        })
    })
}

/// Orientation-independent undirected key for two segment endpoints.
pub(in crate::geometry) fn undirected_edge_key(
    left: PointKey,
    right: PointKey,
) -> (PointKey, PointKey) {
    if (left.x, left.y) <= (right.x, right.y) {
        (left, right)
    } else {
        (right, left)
    }
}

/// Orientation-independent undirected key for an atomic segment.
pub(in crate::geometry) fn undirected_segment_edge_key(segment: Segment) -> (PointKey, PointKey) {
    undirected_edge_key(PointKey::new(segment.start), PointKey::new(segment.end))
}

pub(in crate::geometry) fn segments_intersect(left: Segment, right: Segment) -> bool {
    segment_contact(left, right) != SegmentContact::None
}

/// How a segment meets a rectangle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RectContact {
    /// Enters the rectangle's interior.
    Open,
    /// Touches the closed rectangle (edge/corner contact only).
    Closed,
    /// No contact.
    None,
}

/// Tolerance for deciding an output vertex lies on an input segment when
/// restoring Z/M (relative to the segment length; see `segment_ordinate_at`).
pub(in crate::geometry) const ON_SEGMENT_EPSILON: f64 = 1e-9;

/// Exact segment-vs-rectangle contact via the separating-axis test on robust
/// orientations. The closed contact result is kept distinct from entering the
/// open rectangle so a source boundary coincident with a covered cell edge
/// does not demote that cell to a partial result.
pub(crate) fn segment_vs_rect(segment: Segment, rect: Bounds) -> RectContact {
    let (sx, ex) = (segment.start.x, segment.end.x);
    let (sy, ey) = (segment.start.y, segment.end.y);
    let (lo_x, hi_x) = (sx.min(ex), sx.max(ex));
    let (lo_y, hi_y) = (sy.min(ey), sy.max(ey));
    if hi_x < rect.minx() || lo_x > rect.maxx() || hi_y < rect.miny() || lo_y > rect.maxy() {
        return RectContact::None;
    }
    #[expect(
        clippy::float_cmp,
        reason = "exact coordinate identity is required to classify a repeated segment endpoint"
    )]
    let degenerate = sx == ex && sy == ey;
    if degenerate {
        return if sx > rect.minx() && sx < rect.maxx() && sy > rect.miny() && sy < rect.maxy() {
            RectContact::Open
        } else {
            RectContact::Closed
        };
    }
    let corners = [
        XY::new(rect.minx(), rect.miny()),
        XY::new(rect.maxx(), rect.miny()),
        XY::new(rect.maxx(), rect.maxy()),
        XY::new(rect.minx(), rect.maxy()),
    ];
    let mut strict_positive = false;
    let mut strict_negative = false;
    let mut collinear = false;
    for corner in corners {
        match orientation(segment.start, segment.end, corner) {
            Orientation::CounterClockwise => strict_positive = true,
            Orientation::Clockwise => strict_negative = true,
            Orientation::Collinear => collinear = true,
        }
    }
    if !(strict_positive && strict_negative) {
        return if collinear {
            RectContact::Closed
        } else {
            RectContact::None
        };
    }
    if hi_x > rect.minx() && lo_x < rect.maxx() && hi_y > rect.miny() && lo_y < rect.maxy() {
        RectContact::Open
    } else {
        RectContact::Closed
    }
}

/// Closed-envelope disjointness — the cheap reject before the robust
/// orientation work in brute-force pair scans (the indexed paths already
/// prune by envelope).
pub(in crate::geometry) fn segment_envelopes_disjoint(left: Segment, right: Segment) -> bool {
    left.start.x.max(left.end.x) < right.start.x.min(right.end.x)
        || right.start.x.max(right.end.x) < left.start.x.min(left.end.x)
        || left.start.y.max(left.end.y) < right.start.y.min(right.end.y)
        || right.start.y.max(right.end.y) < left.start.y.min(left.end.y)
}

/// How a segment pair meets, strongest last: `Cross` is a TRANSVERSAL
/// crossing strictly interior to both segments (every endpoint strictly off
/// the other's carrier, both straddling), `Touch` is any other intersection
/// (endpoint contact or collinear overlap). The split is exact: with any
/// endpoint on the other's carrier, the carrier meets that segment only at
/// the endpoint (or the segments are fully collinear), so the intersection
/// — if it exists — involves an endpoint and the boundary-inclusive
/// membership checks decide it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SegmentContact {
    None,
    Touch,
    Cross,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PairOrientations {
    pub(crate) right_start: Orientation,
    pub(crate) right_end: Orientation,
    pub(crate) left_start: Orientation,
    pub(crate) left_end: Orientation,
}

impl PairOrientations {
    pub(crate) fn new(left: Segment, right: Segment) -> Self {
        Self {
            right_start: orientation(left.start, left.end, right.start),
            right_end: orientation(left.start, left.end, right.end),
            left_start: orientation(right.start, right.end, left.start),
            left_end: orientation(right.start, right.end, left.end),
        }
    }

    pub(super) fn all_collinear(self) -> bool {
        self.right_start == Orientation::Collinear
            && self.right_end == Orientation::Collinear
            && self.left_start == Orientation::Collinear
            && self.left_end == Orientation::Collinear
    }

    pub(super) fn has_collinear(self) -> bool {
        self.right_start == Orientation::Collinear
            || self.right_end == Orientation::Collinear
            || self.left_start == Orientation::Collinear
            || self.left_end == Orientation::Collinear
    }

    pub(super) fn straddles_or_touches(self) -> bool {
        self.right_start != self.right_end && self.left_start != self.left_end
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(in crate::geometry) struct SharedSpan {
    pub same_direction: bool,
    pub start: XY,
    pub end: XY,
}

/// Exact touches are stored endpoints; defer copying their coordinate until a
/// consumer needs it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::geometry) enum TouchEndpoint {
    LeftStart,
    RightStart,
    RightEnd,
}

impl TouchEndpoint {
    pub(in crate::geometry) const fn point(self, left: Segment, right: Segment) -> XY {
        match self {
            Self::LeftStart => left.start,
            Self::RightStart => right.start,
            Self::RightEnd => right.end,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::geometry) enum SegmentEndpoint {
    Start,
    End,
}

impl SegmentEndpoint {
    const fn point(self, segment: Segment) -> XY {
        match self {
            Self::Start => segment.start,
            Self::End => segment.end,
        }
    }
}

/// Which segment contains a touch in its interior, or which stored endpoints
/// are shared.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::geometry) enum TouchLocation {
    Shared(TouchEndpoint),
    LeftInterior(SegmentEndpoint),
    RightInterior(SegmentEndpoint),
}

impl TouchLocation {
    pub(in crate::geometry) const fn point(self, left: Segment, right: Segment) -> XY {
        match self {
            Self::Shared(endpoint) => endpoint.point(left, right),
            Self::LeftInterior(endpoint) => endpoint.point(right),
            Self::RightInterior(endpoint) => endpoint.point(left),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::geometry) enum Contact {
    Disjoint,
    /// A proper crossing, interior to both segments; [`crossing_point`]
    /// places it.
    Cross,
    Touch(TouchLocation),
    /// The carriers coincide; consumers that need run endpoints ask for them.
    Collinear,
}

impl Contact {
    /// Construct the shared run only for consumers that need its endpoints.
    pub(in crate::geometry) fn shared_span(
        self,
        left: Segment,
        right: Segment,
    ) -> Option<SharedSpan> {
        match self {
            Self::Collinear => collinear_shared_span_exact(left, right),
            Self::Disjoint | Self::Cross | Self::Touch(_) => None,
        }
    }
}

// The partition expands the same exact decisions as ordinary callers.
macro_rules! segment_contact_exact_impl {
    (@collinear_cuts $left:expr, $right:expr; |$location:ident| $effect:block) => {{
        use $crate::geometry::segments::contact::{
            SegmentEndpoint, TouchLocation, strictly_inside,
        };

        let left = $left;
        let right = $right;
        if strictly_inside(left, right.start) {
            let $location = TouchLocation::LeftInterior(SegmentEndpoint::Start);
            $effect
        }
        if strictly_inside(left, right.end) {
            let $location = TouchLocation::LeftInterior(SegmentEndpoint::End);
            $effect
        }
        if strictly_inside(right, left.start) {
            let $location = TouchLocation::RightInterior(SegmentEndpoint::Start);
            $effect
        }
        if strictly_inside(right, left.end) {
            let $location = TouchLocation::RightInterior(SegmentEndpoint::End);
            $effect
        }
    }};
    ($left:expr, $right:expr) => {{
        use $crate::geometry::numerics::product::ExactSign;
        use $crate::geometry::same_point;
        use $crate::geometry::segments::contact::{
            Contact, SegmentEndpoint, TouchEndpoint, TouchLocation, strictly_inside,
        };
        use $crate::geometry::segments::stored_orientation_sign;

        let left = $left;
        let right = $right;
        'contact: {
            'decide: {
                if same_point(left.start, left.end) {
                    if same_point(right.start, right.end) {
                        if same_point(left.start, right.start) {
                            break 'contact Contact::Touch(TouchLocation::Shared(
                                TouchEndpoint::LeftStart,
                            ));
                        }
                        break 'contact Contact::Disjoint;
                    }
                    if !$crate::geometry::segments::contact::point_in_segment_envelope(
                        left.start, right,
                    ) {
                        break 'contact Contact::Disjoint;
                    }
                    if stored_orientation_sign(
                        right.start.x,
                        right.start.y,
                        right.end.x,
                        right.end.y,
                        left.start.x,
                        left.start.y,
                    ) != ExactSign::Zero
                    {
                        break 'contact Contact::Disjoint;
                    }
                    if same_point(left.start, right.start) || same_point(left.start, right.end) {
                        break 'contact Contact::Touch(TouchLocation::Shared(
                            TouchEndpoint::LeftStart,
                        ));
                    }
                    break 'contact Contact::Touch(TouchLocation::RightInterior(
                        SegmentEndpoint::Start,
                    ));
                }
                if same_point(right.start, right.end) {
                    if !$crate::geometry::segments::contact::point_in_segment_envelope(
                        right.start,
                        left,
                    ) {
                        break 'contact Contact::Disjoint;
                    }
                    if stored_orientation_sign(
                        left.start.x,
                        left.start.y,
                        left.end.x,
                        left.end.y,
                        right.start.x,
                        right.start.y,
                    ) != ExactSign::Zero
                    {
                        break 'contact Contact::Disjoint;
                    }
                    if same_point(right.start, left.start) || same_point(right.start, left.end) {
                        break 'contact Contact::Touch(TouchLocation::Shared(
                            TouchEndpoint::RightStart,
                        ));
                    }
                    break 'contact Contact::Touch(TouchLocation::LeftInterior(
                        SegmentEndpoint::Start,
                    ));
                }

                if $crate::geometry::segments::contact::segment_envelopes_disjoint(left, right) {
                    break 'contact Contact::Disjoint;
                }

                let right_start_shared =
                    same_point(right.start, left.start) || same_point(right.start, left.end);
                let right_end_shared =
                    same_point(right.end, left.start) || same_point(right.end, left.end);
                let shared = match (right_start_shared, right_end_shared) {
                    (true, true) => break 'decide,
                    (true, false) => Some((TouchEndpoint::RightStart, right.end)),
                    (false, true) => Some((TouchEndpoint::RightEnd, right.start)),
                    (false, false) => None,
                };
                if let Some((shared, other)) = shared {
                    if stored_orientation_sign(
                        left.start.x,
                        left.start.y,
                        left.end.x,
                        left.end.y,
                        other.x,
                        other.y,
                    ) == ExactSign::Zero
                    {
                        break 'decide;
                    }
                    break 'contact Contact::Touch(TouchLocation::Shared(shared));
                }

                let right_start = stored_orientation_sign(
                    left.start.x,
                    left.start.y,
                    left.end.x,
                    left.end.y,
                    right.start.x,
                    right.start.y,
                );
                let right_end = stored_orientation_sign(
                    left.start.x,
                    left.start.y,
                    left.end.x,
                    left.end.y,
                    right.end.x,
                    right.end.y,
                );
                if $crate::geometry::segments::contact::strictly_same_side(right_start, right_end) {
                    break 'contact Contact::Disjoint;
                }
                if right_start == ExactSign::Zero && right_end == ExactSign::Zero {
                    break 'decide;
                }
                if right_start == ExactSign::Zero {
                    if strictly_inside(left, right.start) {
                        break 'contact Contact::Touch(TouchLocation::LeftInterior(
                            SegmentEndpoint::Start,
                        ));
                    }
                    break 'contact Contact::Disjoint;
                }
                if right_end == ExactSign::Zero {
                    if strictly_inside(left, right.end) {
                        break 'contact Contact::Touch(TouchLocation::LeftInterior(
                            SegmentEndpoint::End,
                        ));
                    }
                    break 'contact Contact::Disjoint;
                }

                let left_start = stored_orientation_sign(
                    right.start.x,
                    right.start.y,
                    right.end.x,
                    right.end.y,
                    left.start.x,
                    left.start.y,
                );
                let left_end = stored_orientation_sign(
                    right.start.x,
                    right.start.y,
                    right.end.x,
                    right.end.y,
                    left.end.x,
                    left.end.y,
                );
                if $crate::geometry::segments::contact::strictly_same_side(left_start, left_end) {
                    break 'contact Contact::Disjoint;
                }
                if left_start == ExactSign::Zero && left_end == ExactSign::Zero {
                    break 'decide;
                }
                if left_start == ExactSign::Zero {
                    if strictly_inside(right, left.start) {
                        break 'contact Contact::Touch(TouchLocation::RightInterior(
                            SegmentEndpoint::Start,
                        ));
                    }
                    break 'contact Contact::Disjoint;
                }
                if left_end == ExactSign::Zero {
                    if strictly_inside(right, left.end) {
                        break 'contact Contact::Touch(TouchLocation::RightInterior(
                            SegmentEndpoint::End,
                        ));
                    }
                    break 'contact Contact::Disjoint;
                }

                if left_start != left_end {
                    break 'contact Contact::Cross;
                }
                break 'contact Contact::Disjoint;
            }

            Contact::Collinear
        }
    }};
}

pub(in crate::geometry) use segment_contact_exact_impl;

pub(in crate::geometry) fn segment_contact_exact(left: Segment, right: Segment) -> Contact {
    segment_contact_exact_impl!(left, right)
}

pub(in crate::geometry) fn strictly_same_side(left: ExactSign, right: ExactSign) -> bool {
    left == right && left != ExactSign::Zero
}

pub(in crate::geometry) fn segment_contact(left: Segment, right: Segment) -> SegmentContact {
    match segment_contact_exact(left, right) {
        Contact::Disjoint => SegmentContact::None,
        Contact::Cross => SegmentContact::Cross,
        Contact::Touch(_) | Contact::Collinear => SegmentContact::Touch,
    }
}

pub(super) fn collinear_shared_span_exact(left: Segment, right: Segment) -> Option<SharedSpan> {
    let left_degenerate = same_point(left.start, left.end);
    let right_degenerate = same_point(right.start, right.end);

    if left_degenerate && right_degenerate {
        // A zero-length span carries no meaningful direction; the endpoint
        // deltas are exactly zero, so the dot product is a well-defined `0.0`
        // here (no underflow) — reuse it for the degenerate agreement flag.
        let same_direction = segment_direction_dot(left, right) > 0.0;
        return same_point(left.start, right.start).then_some(SharedSpan {
            same_direction,
            start: left.start,
            end: left.start,
        });
    }
    if left_degenerate {
        let same_direction = segment_direction_dot(left, right) > 0.0;
        return point_in_segment_envelope(left.start, right).then_some(SharedSpan {
            same_direction,
            start: left.start,
            end: left.start,
        });
    }
    if right_degenerate {
        let same_direction = segment_direction_dot(left, right) > 0.0;
        return point_in_segment_envelope(right.start, left).then_some(SharedSpan {
            same_direction,
            start: right.start,
            end: right.start,
        });
    }

    let axis = if (left.end.x - left.start.x).abs() >= (left.end.y - left.start.y).abs() {
        Axis::X
    } else {
        Axis::Y
    };
    let left_forward = axis.value(left.start) <= axis.value(left.end);
    // Shared direction from endpoint ORDER on the dominant axis, NOT a
    // direction-vector dot product: for tiny (~1e-162) collinear segments the
    // products underflow to `0.0`, misreading two identical segments as
    // opposite-direction. Both segments are non-degenerate and collinear here,
    // so each has a strictly nonzero extent on the larger-extent axis and the
    // ordering comparison is exact.
    let same_direction = left_forward == (axis.value(right.start) <= axis.value(right.end));
    let along_left = |point: XY| {
        let value = axis.value(point);
        if left_forward { value } else { -value }
    };

    let left_start = along_left(left.start);
    let left_end = along_left(left.end);
    let right_start = along_left(right.start);
    let right_end = along_left(right.end);
    let right_min = right_start.min(right_end);
    let right_max = right_start.max(right_end);
    let overlap_start = left_start.max(right_min);
    let overlap_end = left_end.min(right_max);
    if overlap_start > overlap_end {
        return None;
    }

    Some(SharedSpan {
        same_direction,
        start: endpoint_at_collinear_order(left, right, along_left, overlap_start),
        end: endpoint_at_collinear_order(left, right, along_left, overlap_end),
    })
}

#[derive(Clone, Copy)]
enum Axis {
    X,
    Y,
}

impl Axis {
    const fn value(self, point: XY) -> f64 {
        match self {
            Self::X => point.x,
            Self::Y => point.y,
        }
    }
}

/// Whether `point`, already known to lie on the carrier of this segment, is
/// strictly between its endpoints.
pub(in crate::geometry) fn strictly_inside(segment: Segment, point: XY) -> bool {
    let axis = if (segment.end.x - segment.start.x).abs() >= (segment.end.y - segment.start.y).abs()
    {
        Axis::X
    } else {
        Axis::Y
    };
    let start = axis.value(segment.start);
    let end = axis.value(segment.end);
    let value = axis.value(point);
    start.min(end) < value && value < start.max(end)
}

fn endpoint_at_collinear_order(
    left: Segment,
    right: Segment,
    along_left: impl Fn(XY) -> f64,
    target: f64,
) -> XY {
    [left.start, left.end, right.start, right.end]
        .into_iter()
        .find(|point| endpoint_order_key_matches(along_left(*point), target))
        .expect("collinear overlap boundary is one of the input endpoints")
}

#[expect(
    clippy::float_cmp,
    reason = "target is selected from the same exact endpoint keys"
)]
fn endpoint_order_key_matches(candidate: f64, target: f64) -> bool {
    // Exact key match: the target was selected from these endpoint keys.
    candidate == target
}

pub(crate) fn point_in_segment_envelope(point: XY, segment: Segment) -> bool {
    point.x >= segment.start.x.min(segment.end.x)
        && point.x <= segment.start.x.max(segment.end.x)
        && point.y >= segment.start.y.min(segment.end.y)
        && point.y <= segment.start.y.max(segment.end.y)
}

fn segment_direction_dot(left: Segment, right: Segment) -> f64 {
    let left_dx = left.end.x - left.start.x;
    let left_dy = left.end.y - left.start.y;
    let right_dx = right.end.x - right.start.x;
    let right_dy = right.end.y - right.start.y;
    left_dx * right_dx + left_dy * right_dy
}

/// Return the positive-length collinear overlap in source coordinates.
pub(in crate::geometry) fn shared_segment_part(
    left: Segment,
    right: Segment,
) -> Option<SharedSpan> {
    // The fused kernel already computes the EXACT collinear overlap span (its
    // endpoints are actual input coordinates, ordered along the segment). Reuse
    // it directly — a positive-length shared run is exactly an overlap whose two
    // endpoints differ by coordinate identity. No scale-dependent `1e-12` cutoff
    // (which misgraded a tiny-but-real shared edge as a point on large
    // coordinates) and no interpolation jitter.
    let span = segment_contact_exact(left, right).shared_span(left, right)?;
    if same_point(span.start, span.end) {
        return None;
    }
    Some(span)
}

#[cfg(test)]
#[path = "contact/tests.rs"]
mod tests;
