use crate::array::{PackedRow, ShapeRow};
use crate::error::Result;
use crate::geometry::distance::{bounds_distance_squared, bounds_squared_safe};
use crate::geometry::facet_bvh::{FACET_SEGMENTS, facet_point_distance_columns};
use crate::geometry::source_support::RowPointQuery;
use crate::geometry::{
    Bounds, GeometryKind, Point, Segment, Shape, XY, finish_planar_squared_min, point_distance,
    point_distance_squared, point_segment_distance, point_segment_distance_squared, points_dwithin,
    same_point, segment_envelopes_disjoint, segments_intersect,
};

const DIRECT_PAIR_MAX_SEGMENTS: usize = 64;

#[derive(Default)]
struct Boundary<'a> {
    chains: Vec<ChainView<'a>>,
    point: Option<Point>,
}

struct ChainView<'a> {
    xs: &'a [f64],
    ys: &'a [f64],
    closure: Option<Segment>,
}

impl<'a> ChainView<'a> {
    fn new(xs: &'a [f64], ys: &'a [f64], closed: bool) -> Self {
        let closure = closed
            .then(|| {
                let (&ax, &ay) = xs.first().zip(ys.first())?;
                let (&bx, &by) = xs.last().zip(ys.last())?;
                (!same_point(XY::new(ax, ay), XY::new(bx, by))).then_some(Segment {
                    start: XY::new(bx, by),
                    end: XY::new(ax, ay),
                })
            })
            .flatten();
        Self { xs, ys, closure }
    }

    fn segments(&self) -> impl Iterator<Item = Segment> + '_ {
        self.xs
            .array_windows::<2>()
            .zip(self.ys.array_windows::<2>())
            .map(|([ax, bx], [ay, by])| Segment {
                start: XY::new(*ax, *ay),
                end: XY::new(*bx, *by),
            })
            .chain(self.closure)
    }

    fn segment_count(&self) -> usize {
        self.xs
            .len()
            .saturating_sub(1)
            .saturating_add(usize::from(self.closure.is_some()))
    }
}

impl<'a> Boundary<'a> {
    fn chain(&mut self, xs: &'a [f64], ys: &'a [f64], closed: bool) {
        self.chains.push(ChainView::new(xs, ys, closed));
    }

    fn points(&self) -> impl Iterator<Item = Point> + '_ {
        self.point
            .into_iter()
            .chain(self.chains.iter().flat_map(|chain| {
                chain
                    .xs
                    .iter()
                    .zip(chain.ys)
                    .map(|(&x, &y)| Point::new_raw_xy(x, y))
            }))
    }

    fn segments(&self) -> impl Iterator<Item = Segment> + '_ {
        self.chains.iter().flat_map(ChainView::segments)
    }

    fn segment_count(&self) -> usize {
        self.chains.iter().map(ChainView::segment_count).sum()
    }

    fn load(&mut self, row: ShapeRow<'a>) -> bool {
        self.chains.clear();
        self.point = None;
        if let Some(point) = row.point() {
            self.point = Some(point);
            return true;
        }
        match row {
            ShapeRow::Packed(packed @ PackedRow::Lines { coords, .. }) => {
                if packed.coord_count() > DIRECT_PAIR_MAX_SEGMENTS + 1 {
                    return false;
                }
                let window = packed
                    .vertex_window()
                    .expect("line row has a vertex window");
                self.chain(
                    coords
                        .xs()
                        .get(window.clone())
                        .expect("admitted line x window"),
                    coords.ys().get(window).expect("admitted line y window"),
                    false,
                );
            },
            ShapeRow::Packed(packed @ PackedRow::Polygons { coords, .. }) => {
                if packed.coord_count() > DIRECT_PAIR_MAX_SEGMENTS + 1 {
                    return false;
                }
                for ring in packed.polygon_rings().expect("polygon row has rings") {
                    let window = packed
                        .polygon_ring_window(ring)
                        .expect("admitted polygon ring");
                    self.chain(
                        coords
                            .xs()
                            .get(window.clone())
                            .expect("admitted ring x window"),
                        coords.ys().get(window).expect("admitted ring y window"),
                        true,
                    );
                }
            },
            ShapeRow::Handle(plan) => return self.load(ShapeRow::Shape(plan.shape())),
            ShapeRow::Shape(shape) => match shape {
                Shape::LineString(line) if line.len() <= DIRECT_PAIR_MAX_SEGMENTS + 1 => {
                    self.chain(line.xs(), line.ys(), false);
                },
                Shape::Polygon(polygon) => {
                    for ring in polygon.rings() {
                        if self.segment_count().saturating_add(ring.len())
                            > DIRECT_PAIR_MAX_SEGMENTS
                        {
                            return false;
                        }
                        self.chain(ring.xs(), ring.ys(), true);
                    }
                },
                Shape::Empty(..) => {},
                Shape::Point(_)
                | Shape::MultiPoint(_)
                | Shape::LineString(_)
                | Shape::MultiLineString(_)
                | Shape::MultiPolygon(_)
                | Shape::GeometryCollection(_) => return false,
            },
            ShapeRow::Point(_) | ShapeRow::Packed(PackedRow::Points { .. }) => unreachable!(),
        }
        self.segment_count() <= DIRECT_PAIR_MAX_SEGMENTS
    }

    fn sweep<const SQUARED: bool, const SIMD: bool>(
        &self,
        target: &Self,
        target_bounds: Option<Bounds>,
        mut best: f64,
    ) -> Result<f64> {
        for point in self.points() {
            if SQUARED
                && target_bounds.is_some_and(|bounds| {
                    bounds_distance_squared(Bounds::from_point(point), bounds) >= best
                })
            {
                continue;
            }
            if target.chains.is_empty() {
                for other in target.points() {
                    best = best.min(if SQUARED {
                        point_distance_squared(point, other)
                    } else {
                        point_distance(point, other)
                    });
                }
            } else {
                for chain in &target.chains {
                    for first in (0..chain.xs.len().saturating_sub(1)).step_by(FACET_SEGMENTS) {
                        let end = first
                            .saturating_add(FACET_SEGMENTS)
                            .min(chain.xs.len().saturating_sub(1));
                        best = best.min(facet_point_distance_columns!(
                            SQUARED,
                            SIMD,
                            chain.xs.get(first..=end).expect("admitted facet x window"),
                            chain.ys.get(first..=end).expect("admitted facet y window"),
                            point,
                            best,
                        )?);
                    }
                    if let Some(segment) = chain.closure {
                        best = best.min(if SQUARED {
                            point_segment_distance_squared(point, segment)?
                        } else {
                            point_segment_distance(point, segment)?
                        });
                    }
                }
            }
        }
        Ok(best)
    }
}

/// Small pairs use the same exact contact and projection primitives as prepared
/// linework, without allocating shape handles or indexes for a one-use row.
pub(crate) struct RowMetricPair<'a> {
    left: Boundary<'a>,
    right: Boundary<'a>,
    query: RowPointQuery<'a>,
}

impl<'a> RowMetricPair<'a> {
    pub(crate) fn new() -> Self {
        Self {
            left: Boundary::default(),
            right: Boundary::default(),
            query: RowPointQuery::new(),
        }
    }

    fn admit(&mut self, left: ShapeRow<'a>, right: ShapeRow<'a>) -> bool {
        self.left.load(left)
            && self.right.load(right)
            && self
                .left
                .segment_count()
                .saturating_add(self.right.segment_count())
                <= DIRECT_PAIR_MAX_SEGMENTS
    }

    fn contacts(
        &mut self,
        left: ShapeRow<'a>,
        right: ShapeRow<'a>,
        bounds: Option<&(Bounds, Bounds)>,
    ) -> bool {
        if !bounds.is_some_and(|(left, right)| left.intersects(*right)) {
            return false;
        }
        if self.left.segments().any(|left| {
            self.right.segments().any(|right| {
                !segment_envelopes_disjoint(left, right) && segments_intersect(left, right)
            })
        }) {
            return true;
        }
        if right.kind() == GeometryKind::Polygon
            && let Some(point) = self.left.points().next()
            && self.query.area_covers(right, point)
        {
            return true;
        }
        if left.kind() == GeometryKind::Polygon
            && let Some(point) = self.right.points().next()
            && self.query.area_covers(left, point)
        {
            return true;
        }
        false
    }

    fn minimum<const SQUARED: bool, const SIMD: bool>(
        &self,
        bounds: Option<&(Bounds, Bounds)>,
    ) -> Result<f64> {
        let best = self.left.sweep::<SQUARED, SIMD>(
            &self.right,
            bounds.map(|(_, right)| *right),
            f64::INFINITY,
        )?;
        self.right
            .sweep::<SQUARED, SIMD>(&self.left, bounds.map(|(left, _)| *left), best)
    }

    fn magnitude(&self, bounds: Option<&(Bounds, Bounds)>) -> Result<f64> {
        if bounds
            .is_some_and(|(left, right)| bounds_squared_safe(*left) && bounds_squared_safe(*right))
        {
            let squared = if self.left.point.is_none() && self.right.point.is_none() {
                self.minimum::<true, true>(bounds)?
            } else {
                self.minimum::<true, false>(bounds)?
            };
            finish_planar_squared_min(squared, || self.minimum::<false, false>(bounds))
        } else {
            self.minimum::<false, false>(bounds)
        }
    }

    pub(crate) fn distance(
        &mut self,
        left: ShapeRow<'a>,
        right: ShapeRow<'a>,
        left_bounds: Option<Bounds>,
        right_bounds: Option<Bounds>,
    ) -> Result<Option<f64>> {
        if let (Some(left), Some(right)) = (left.point(), right.point()) {
            return Ok(Some(point_distance(left, right)));
        }
        if !self.admit(left, right) {
            return Ok(None);
        }
        let bounds = left_bounds.zip(right_bounds);
        Ok(Some(if self.contacts(left, right, bounds.as_ref()) {
            0.0
        } else {
            self.magnitude(bounds.as_ref())?
        }))
    }

    pub(crate) fn dwithin(
        &mut self,
        left: ShapeRow<'a>,
        right: ShapeRow<'a>,
        left_bounds: Option<Bounds>,
        right_bounds: Option<Bounds>,
        distance: f64,
    ) -> Result<Option<bool>> {
        if distance == f64::INFINITY {
            return Ok(Some(!left.is_empty() && !right.is_empty()));
        }
        if let (Some(left), Some(right)) = (left.point(), right.point()) {
            return Ok(Some(points_dwithin(left, right, distance)));
        }
        let bounds = left_bounds.zip(right_bounds);
        let limit = distance * distance;
        if limit.is_finite()
            && bounds.is_some_and(|(left, right)| bounds_distance_squared(left, right) > limit)
        {
            return Ok(Some(false));
        }
        if !self.admit(left, right) {
            return Ok(None);
        }
        if self.contacts(left, right, bounds.as_ref()) {
            return Ok(Some(true));
        }
        let use_sq = limit.is_normal() && distance > 0.0;
        if use_sq && self.left.point.is_none() && self.right.point.is_none() {
            let squared = if bounds.as_ref().is_some_and(|(left, right)| {
                bounds_squared_safe(*left) && bounds_squared_safe(*right)
            }) {
                self.minimum::<true, true>(bounds.as_ref())?
            } else {
                self.minimum::<true, false>(bounds.as_ref())?
            };
            return Ok(Some(squared <= limit));
        }
        Ok(Some(self.magnitude(bounds.as_ref())? <= distance))
    }
}
