use crate::error::Result;
use crate::geometry::distance::{
    collect_point_only_into, geodesic_capped_dwithin_with_parts, geodesic_capped_sweep_with_parts,
    geodesic_scratch, geodesic_witness_sweep_with_parts,
};
use crate::geometry::{
    FrameDependentCaches, GeodesicFacetBvh, GeodesicMetric, GeodesicParts, GeodesicPartsKey, Point,
    PreparedPlan, Shape, distance,
};

/// Validate the geodesic linework domain and reject filled areal operands.
/// Filled polygon interiors need a geodesic point-in-polygon certificate;
/// using planar containment here would manufacture a false zero distance.
pub(crate) fn ensure_geodesic_linework(shape: &Shape, metric: &impl GeodesicMetric) -> Result<()> {
    match shape {
        Shape::Polygon(_) | Shape::MultiPolygon(_) => Err(crate::crs::CrsError::message(
            "geodesic distance for polygon interiors is unsupported",
        )),
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                ensure_geodesic_linework(geometry, metric)?;
            }
            Ok(())
        },
        _ => metric.ensure_shape(shape),
    }
}
pub(crate) fn geodesic_cap_streaming(
    shape: &Shape,
    metric: &impl GeodesicMetric,
) -> Result<Option<(Point, f64)>> {
    ensure_geodesic_linework(shape, metric)?;
    let mut anchor = None;
    shape.for_each_point(|point| {
        if anchor.is_none() {
            anchor = Some(point);
        }
    });
    let Some(anchor) = anchor else {
        return Ok(None);
    };
    let mut reach = 0.0_f64;
    let mut error = None;
    shape.for_each_vertex_pair(|start, end| {
        let length = match metric.make_segment(start, end) {
            Ok(segment) => segment.length,
            Err(value) => {
                error = Some(value);
                return;
            },
        };
        let from_anchor = metric.segment_length(anchor, start);
        reach = reach.max(from_anchor + length);
    });
    if let Some(error) = error {
        return Err(error);
    }
    let mut point_only = Vec::new();
    collect_point_only_into(shape, &mut point_only);
    for point in point_only {
        reach = reach.max(metric.segment_length(anchor, point));
    }
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
    if !parts.point_only.is_empty() {
        for &point in &parts.point_only {
            reach = reach.max(metric.segment_length(anchor, point));
        }
    }
    Some((anchor, reach))
}

impl PreparedPlan {
    /// Auxiliary-sphere cap reusing a cached [`GeodesicParts`] when present.
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

pub(crate) fn geodesic_segments_cross_streaming(
    left: &Shape,
    right: &Shape,
    metric: &impl GeodesicMetric,
) -> Result<bool> {
    let mut cross = false;
    let mut error = None;
    left.for_each_vertex_pair(|l_start, l_end| {
        if cross || error.is_some() {
            return;
        }
        right.for_each_vertex_pair(|r_start, r_end| {
            if cross || error.is_some() {
                return;
            }
            match metric.segments_cross(l_start, l_end, r_start, r_end) {
                Ok(true) => cross = true,
                Ok(false) => {},
                Err(value) => error = Some(value),
            }
        });
    });
    if let Some(error) = error {
        return Err(error);
    }
    Ok(cross)
}

pub(crate) fn geodesic_segments_cross_parts(
    left: &GeodesicParts,
    right: &GeodesicParts,
    metric: &impl GeodesicMetric,
) -> Result<bool> {
    for segment in &left.segments {
        for other in &right.segments {
            if metric.segments_cross(segment.start, segment.end, other.start, other.end)? {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub(crate) fn geodesic_distance_with_parts(
    left: &Shape,
    left_parts: &GeodesicParts,
    right: &Shape,
    right_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
) -> Result<f64> {
    if geodesic_segments_cross_parts(left_parts, right_parts, metric)? {
        return Ok(0.0);
    }
    if matches!(left, Shape::Point(_)) {
        return geodesic_point_distance_with_parts(left_parts.points[0], right_parts, metric, None);
    }
    if matches!(right, Shape::Point(_)) {
        return geodesic_point_distance_with_parts(right_parts.points[0], left_parts, metric, None);
    }
    let mut best = f64::INFINITY;
    best = geodesic_sweep_with_parts(&left_parts.points, right_parts, metric, best)?;
    geodesic_sweep_with_parts(&right_parts.points, left_parts, metric, best)
}

pub(crate) fn geodesic_dwithin_with_parts(
    left: &Shape,
    left_parts: &GeodesicParts,
    right: &Shape,
    right_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
    distance: f64,
) -> Result<bool> {
    if geodesic_segments_cross_parts(left_parts, right_parts, metric)? {
        return Ok(true);
    }
    if matches!(left, Shape::Point(_)) {
        return geodesic_point_dwithin_with_parts(
            left_parts.points[0],
            right_parts,
            metric,
            distance,
            None,
        );
    }
    if matches!(right, Shape::Point(_)) {
        return geodesic_point_dwithin_with_parts(
            right_parts.points[0],
            left_parts,
            metric,
            distance,
            None,
        );
    }
    Ok(
        geodesic_dwithin_sweep_with_parts(&left_parts.points, right_parts, metric, distance)?
            || geodesic_dwithin_sweep_with_parts(
                &right_parts.points,
                left_parts,
                metric,
                distance,
            )?,
    )
}

pub(crate) fn geodesic_nearest_points_with_parts(
    left: &Shape,
    left_parts: &GeodesicParts,
    right: &Shape,
    right_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
) -> Result<Option<(Point, Point)>> {
    if left_parts.points.is_empty() || right_parts.points.is_empty() {
        return Ok(None);
    }
    for segment in &left_parts.segments {
        for other in &right_parts.segments {
            if let Some(witness) =
                metric.segment_intersection(segment.start, segment.end, other.start, other.end)?
            {
                return Ok(Some(witness));
            }
        }
    }
    if matches!(left, Shape::Point(_)) {
        return Ok(geodesic_witness_sweep_with_parts(
            &left_parts.points,
            right_parts,
            metric,
            None,
            0,
            false,
        )?
        .map(|candidate| (candidate.probe, candidate.target)));
    }
    if matches!(right, Shape::Point(_)) {
        return Ok(geodesic_witness_sweep_with_parts(
            &right_parts.points,
            left_parts,
            metric,
            None,
            0,
            true,
        )?
        .map(|candidate| (candidate.target, candidate.probe)));
    }
    let reverse_order_offset =
        left_parts.points.len() * (right_parts.segments.len() + right_parts.point_only.len());
    let best =
        geodesic_witness_sweep_with_parts(&left_parts.points, right_parts, metric, None, 0, false)?;
    let best = geodesic_witness_sweep_with_parts(
        &right_parts.points,
        left_parts,
        metric,
        best,
        reverse_order_offset,
        true,
    )?;
    Ok(best.map(|candidate| {
        if candidate.swapped {
            (candidate.target, candidate.probe)
        } else {
            (candidate.probe, candidate.target)
        }
    }))
}

pub(crate) fn geodesic_sweep_with_parts(
    vertices: &[Point],
    target_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
    mut best: f64,
) -> Result<f64> {
    let mut scratch = geodesic_scratch();
    let scratch = &mut *scratch;
    if let Some(bvh) = target_parts.geodesic_bvh(metric)? {
        for &vertex in vertices {
            best =
                bvh.min_point_distance(vertex, target_parts, metric, &mut scratch.stack, best)?;
        }
        Ok(best)
    } else {
        geodesic_capped_sweep_with_parts(vertices, target_parts, metric, best, &mut scratch.rows)
    }
}

pub(crate) fn geodesic_point_distance_with_parts(
    point: Point,
    target_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
    stack: Option<&mut Vec<u32>>,
) -> Result<f64> {
    let mut scratch = geodesic_scratch();
    let bvh = target_parts.geodesic_bvh(metric)?;
    geodesic_point_distance_with_parts_and_scratch(
        point,
        target_parts,
        metric,
        bvh,
        &mut scratch,
        stack,
    )
}

pub(crate) fn geodesic_point_distance_with_parts_and_scratch(
    point: Point,
    target_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
    bvh: Option<&GeodesicFacetBvh>,
    scratch: &mut distance::GeodesicScratch,
    stack: Option<&mut Vec<u32>>,
) -> Result<f64> {
    if let Some(bvh) = bvh {
        let stack = stack.unwrap_or(&mut scratch.stack);
        bvh.min_point_distance(point, target_parts, metric, stack, f64::INFINITY)
    } else {
        geodesic_capped_sweep_with_parts(
            std::slice::from_ref(&point),
            target_parts,
            metric,
            f64::INFINITY,
            &mut scratch.rows,
        )
    }
}

pub(crate) fn geodesic_dwithin_sweep_with_parts(
    vertices: &[Point],
    target_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
    distance: f64,
) -> Result<bool> {
    let mut scratch = geodesic_scratch();
    let scratch = &mut *scratch;
    if let Some(bvh) = target_parts.geodesic_bvh(metric)? {
        vertices.iter().try_fold(false, |found, &vertex| {
            if found {
                return Ok(true);
            }
            bvh.dwithin_point(vertex, target_parts, metric, &mut scratch.stack, distance)
        })
    } else {
        geodesic_capped_dwithin_with_parts(
            vertices,
            target_parts,
            metric,
            distance,
            &mut scratch.rows,
        )
    }
}

pub(crate) fn geodesic_point_dwithin_with_parts(
    point: Point,
    target_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
    distance: f64,
    stack: Option<&mut Vec<u32>>,
) -> Result<bool> {
    let mut scratch = geodesic_scratch();
    let bvh = target_parts.geodesic_bvh(metric)?;
    geodesic_point_dwithin_with_parts_and_scratch(
        point,
        target_parts,
        metric,
        distance,
        bvh,
        &mut scratch,
        stack,
    )
}

pub(crate) fn geodesic_point_dwithin_with_parts_and_scratch(
    point: Point,
    target_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
    distance: f64,
    bvh: Option<&GeodesicFacetBvh>,
    scratch: &mut distance::GeodesicScratch,
    stack: Option<&mut Vec<u32>>,
) -> Result<bool> {
    if let Some(bvh) = bvh {
        let stack = stack.unwrap_or(&mut scratch.stack);
        bvh.dwithin_point(point, target_parts, metric, stack, distance)
    } else {
        geodesic_capped_dwithin_with_parts(
            std::slice::from_ref(&point),
            target_parts,
            metric,
            distance,
            &mut scratch.rows,
        )
    }
}
