use crate::error::Result;
use crate::geometry::{
    Coordinates as _, FrameDependentCaches, GeodesicMetric, GeodesicParts, GeodesicPartsKey, Point,
    PreparedPlan, RunIdentity, Shape, SharedShapeOwners,
};

/// Validate linework operands used by continuous geodesic Hausdorff.
pub(crate) fn ensure_geodesic_linework(shape: &Shape, metric: &impl GeodesicMetric) -> Result<()> {
    ensure_geodesic_linework_inner(shape, metric, &mut SharedShapeOwners::for_shape(shape))
}

fn ensure_geodesic_linework_inner(
    shape: &Shape,
    metric: &impl GeodesicMetric,
    owners: &mut Option<SharedShapeOwners>,
) -> Result<()> {
    match shape {
        Shape::Polygon(_) | Shape::MultiPolygon(_) => Err(crate::crs::CrsError::message(
            "geodesic distance for polygon interiors is unsupported",
        )),
        Shape::GeometryCollection(geometries) => {
            if owners.as_mut().is_some_and(|owners| {
                !owners.first_collection(geometries.as_slice(), geometries.members_are_shared())
            }) {
                return Ok(());
            }
            for geometry in geometries {
                ensure_geodesic_linework_inner(geometry, metric, owners)?;
            }
            Ok(())
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => metric.ensure_shape(shape),
    }
}

pub(crate) fn geodesic_cap_streaming(
    shape: &Shape,
    metric: &impl GeodesicMetric,
) -> Result<Option<(Point, f64)>> {
    ensure_geodesic_linework(shape, metric)?;
    let anchor = match shape.try_for_each_first_point_run(std::ops::ControlFlow::Break) {
        std::ops::ControlFlow::Break(point) => Some(point),
        std::ops::ControlFlow::Continue(()) => None,
    };
    let Some(anchor) = anchor else {
        return Ok(None);
    };
    let mut reach = 0.0_f64;
    shape.try_for_each_first_segment_chain(RunIdentity::Full, |chain| {
        for [start, end] in chain.segment_pairs() {
            let length = metric.make_segment(start, end)?.length;
            let from_anchor = metric.segment_length(anchor, start);
            reach = reach.max(from_anchor + length);
        }
        Ok::<_, crate::error::Error>(())
    })?;
    shape.for_each_first_point_only(RunIdentity::Full, |point| {
        reach = reach.max(metric.segment_length(anchor, point));
    });
    Ok(Some((anchor, reach)))
}

pub(crate) fn geodesic_cap_from_parts(
    parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
) -> Option<(Point, f64)> {
    let anchor = *parts.points.first()?;
    let mut reach = 0.0_f64;
    for segment in &parts.segments {
        let from_anchor = metric.segment_length(anchor, segment.start);
        reach = reach.max(from_anchor + segment.length);
    }
    for &point in &parts.point_only {
        reach = reach.max(metric.segment_length(anchor, point));
    }
    Some((anchor, reach))
}

impl PreparedPlan {
    /// Auxiliary-sphere cap reusing cached geodesic parts when present.
    pub(crate) fn geodesic_cap_cached(
        &self,
        frame_cache: &FrameDependentCaches,
        crs: &str,
        semi_major: f64,
        flattening: f64,
        metric: &impl GeodesicMetric,
        state: &crate::crs::RuntimeState,
    ) -> Result<Option<(Point, f64)>> {
        let key =
            GeodesicPartsKey::new_at_generation(crs, semi_major, flattening, state.generation);
        if let Some(parts) = self.cached_geodesic_parts(frame_cache, &key) {
            return Ok(geodesic_cap_from_parts(&parts, metric));
        }
        self.shape().geodesic_cap(metric)
    }
}
