use super::{
    GeographicRdpGuard, GeometryKind, KeepScratch, NonNegative, PeriodicAxis, PolygonKeep,
    PolygonalSimplificationPlan, RawSimplify, Result, Segment, Shape, XY, keep_multi_polygons,
    keep_polygon, point_segment_distance, rdp_keep_with_edge_guard, triangle_area,
    vw_area_tolerance,
};
use crate::geometry::constructive::vw_keep_geographic;
use crate::geometry::{GeometryErrorKind, SimplifyMethod};

impl Shape {
    /// Simplify a polygonal geographic shape and retain the source indices
    /// selected for each output ring.
    pub(crate) fn simplify_geographic_with_plan(
        &self,
        tolerance: f64,
        method: SimplifyMethod,
        axis: PeriodicAxis,
    ) -> Result<(Self, PolygonalSimplificationPlan)> {
        let tolerance = NonNegative::try_new("tolerance", tolerance)?.get();
        let mut scratch = KeepScratch::default();
        let mut keep_mask = |xs: &[f64], ys: &[f64], scratch: &mut KeepScratch| match method {
            SimplifyMethod::Dp => rdp_keep_with_edge_guard(
                xs,
                ys,
                tolerance,
                GeographicRdpGuard(&axis),
                &mut scratch.keep,
                &mut scratch.stack,
            ),
            SimplifyMethod::Vw => Ok(vw_keep_geographic(
                xs,
                ys,
                tolerance,
                axis,
                &mut scratch.keep,
            )),
        };
        let (raw, plan) = match self {
            Self::Polygon(polygon) => {
                let (keep, plan) = keep_polygon(polygon, &mut scratch, &mut keep_mask, true)?;
                let raw = match keep {
                    PolygonKeep::Same => RawSimplify::Unchanged,
                    PolygonKeep::Empty => RawSimplify::Changed(Self::typed_empty(
                        GeometryKind::Polygon,
                        polygon.axes(),
                    )),
                    PolygonKeep::New(polygon) => RawSimplify::Changed(Self::Polygon(polygon)),
                };
                let polygons = plan.into_iter().collect();
                (raw, PolygonalSimplificationPlan { polygons })
            },
            Self::MultiPolygon(polygons) => {
                let (raw, polygons) =
                    keep_multi_polygons(polygons, &mut scratch, &mut keep_mask, true)?;
                (raw, PolygonalSimplificationPlan { polygons })
            },
            Self::Empty(..) => {
                return Ok((self.clone(), PolygonalSimplificationPlan {
                    polygons: Vec::new(),
                }));
            },
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::GeometryCollection(_) => {
                return Err(GeometryErrorKind::message(
                    "geographic coverage simplification requires polygonal input",
                ));
            },
        };

        match raw {
            RawSimplify::Unchanged => Ok((self.clone(), plan)),
            RawSimplify::Changed(shape) if self.simplify_kept_topology(&shape)? => {
                Ok((shape, plan))
            },
            RawSimplify::Changed(_) => {
                let area_tolerance = vw_area_tolerance(tolerance);
                let candidate = |previous: XY, vertex: XY, next: XY| match method {
                    SimplifyMethod::Dp => {
                        let deviation = point_segment_distance(vertex, Segment {
                            start: previous,
                            end: next,
                        })?;
                        Ok((deviation <= tolerance).then_some(deviation))
                    },
                    SimplifyMethod::Vw => {
                        let area = triangle_area(previous, vertex, next);
                        Ok((area < area_tolerance).then_some(area))
                    },
                };
                let (shape, ring_plans) =
                    self.simplify_guarded_with_capture::<true>(axis, candidate, true)?;
                let ring_plans = ring_plans.expect("guarded simplification captured its plan");
                let polygons = group_polygon_ring_plans(self, ring_plans)?;
                Ok((shape, PolygonalSimplificationPlan { polygons }))
            },
        }
    }
}

fn group_polygon_ring_plans(
    source: &Shape,
    ring_plans: Vec<Vec<usize>>,
) -> Result<Vec<Vec<Vec<usize>>>> {
    let ring_counts: Vec<usize> = match source {
        Shape::Polygon(polygon) => vec![polygon.rings().count()],
        Shape::MultiPolygon(polygons) => polygons
            .as_slice()
            .iter()
            .map(|polygon| polygon.rings().count())
            .collect(),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => {
            return Err(GeometryErrorKind::message(
                "guarded geographic simplification lost its polygonal source",
            ));
        },
    };
    let mut ring_plans = ring_plans.into_iter();
    let polygons = ring_counts
        .into_iter()
        .map(|ring_count| {
            std::iter::repeat_with(|| {
                ring_plans.next().ok_or_else(|| {
                    GeometryErrorKind::message(
                        "guarded geographic simplification lost a source ring plan",
                    )
                })
            })
            .take(ring_count)
            .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    if ring_plans.next().is_some() {
        return Err(GeometryErrorKind::message(
            "guarded geographic simplification has unused source ring plans",
        ));
    }
    Ok(polygons)
}
