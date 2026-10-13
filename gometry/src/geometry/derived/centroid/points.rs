use super::*;

/// Equal-weight stable mean of 2-D points. Ordinary mid-range inputs keep the
/// flat `column_mean2` bit path. Large absolute coordinates or detected
/// cancellation scale first then Neumaier-sum so every permutation of
/// `[1e16, 1, -1e16]` yields exact `1/3`.
pub(super) fn stable_point_mean(xs: &[f64], ys: &[f64]) -> Option<(f64, f64)> {
    if xs.is_empty() {
        return None;
    }
    debug_assert_eq!(xs.len(), ys.len());
    let mut max_abs_x = 0.0_f64;
    let mut max_abs_y = 0.0_f64;
    for (&x, &y) in xs.iter().zip(ys.iter()) {
        max_abs_x = max_abs_x.max(x.abs());
        max_abs_y = max_abs_y.max(y.abs());
    }
    Some(stable_point_mean_from_magnitudes(
        xs, ys, max_abs_x, max_abs_y,
    ))
}

pub(super) fn stable_point_mean_from_magnitudes(
    xs: &[f64],
    ys: &[f64],
    max_abs_x: f64,
    max_abs_y: f64,
) -> (f64, f64) {
    debug_assert!(!xs.is_empty());
    debug_assert_eq!(xs.len(), ys.len());
    let n = xs.len();
    let max_abs = max_abs_x.max(max_abs_y);
    if max_abs <= AREAL_EXACT_MAX_COORD
        && let Some((mx, my)) = column_mean2(xs, ys)
        && mx.is_finite()
        && my.is_finite()
        && (max_abs == 0.0 || mx.abs().max(my.abs()) >= max_abs * f64::EPSILON * 32.0)
    {
        return (mx, my);
    }
    let scale_x = axis_pow2_scale(max_abs_x.max(1.0));
    let scale_y = axis_pow2_scale(max_abs_y.max(1.0));
    let n_f = n as f64;
    let sx = neumaier_sum(xs.iter().map(|&x| x * scale_x));
    let sy = neumaier_sum(ys.iter().map(|&y| y * scale_y));
    (
        canonicalize_zero((sx / n_f) / scale_x),
        canonicalize_zero((sy / n_f) / scale_y),
    )
}

pub(crate) enum PointSetCenter {
    Ordinary((f64, f64)),
    Chart(Option<Bounds>),
}

pub(super) fn point_set_center_columns(
    xs: &[f64],
    ys: &[f64],
    frame: crate::geographic::GeographicFrame,
) -> Option<PointSetCenter> {
    if xs.is_empty() {
        return None;
    }
    debug_assert_eq!(xs.len(), ys.len());
    let Some([minx, miny, maxx, maxy]) = xy_finite_bounds_columns(xs, ys) else {
        return Some(PointSetCenter::Chart(None));
    };
    let raw_extrema = Bounds::new_raw(minx, miny, maxx, maxy);
    if frame.bounds_certify_ordinary_support(raw_extrema) {
        let max_abs_x = minx.abs().max(maxx.abs());
        let max_abs_y = miny.abs().max(maxy.abs());
        return Some(PointSetCenter::Ordinary(stable_point_mean_from_magnitudes(
            xs, ys, max_abs_x, max_abs_y,
        )));
    }
    Some(PointSetCenter::Chart(Some(raw_extrema)))
}

/// Route point-set centers; column means follow finite actual-bounds admission.
pub(crate) fn point_set_center_summary(
    shape: &Shape,
    frame: crate::geographic::GeographicFrame,
) -> Option<PointSetCenter> {
    debug_assert!(shape.point_set_only());
    if matches!(shape, Shape::Empty(..)) {
        return None;
    }
    if let Shape::Point(point) = shape {
        let bounds = (point.x.is_finite() && point.y.is_finite())
            .then(|| Bounds::new_raw(point.x, point.y, point.x, point.y));
        if bounds.is_some_and(|bounds| frame.bounds_certify_ordinary_support(bounds)) {
            return Some(PointSetCenter::Ordinary((point.x, point.y)));
        }
        return Some(PointSetCenter::Chart(bounds));
    }
    if let Shape::MultiPoint(seq) = shape {
        return point_set_center_columns(seq.xs(), seq.ys(), frame);
    }

    let mut xs = Vec::new();
    let mut ys = Vec::new();
    let mut admission = None;
    match visit_fallback_points(shape, &mut admission, &mut |x, y| {
        xs.push(x);
        ys.push(y);
    }) {
        CentroidAttempt::ExactAtRoot(_) => {
            drop(xs);
            drop(ys);
            (!centroid_is_empty(shape)).then_some(PointSetCenter::Chart(None))
        },
        CentroidAttempt::Ready(()) => point_set_center_columns(&xs, &ys, frame),
    }
}

/// Dimension-0 centroid: the average of the geometry's points — actual
/// `Point`/`MultiPoint` vertices plus the first vertex of each (necessarily
/// zero-length, since we only get here when total length is zero) lineal run.
/// `None` for a fully empty geometry.
pub(crate) fn point_centroid(shape: &Shape) -> CoreResult<Option<(f64, f64)>> {
    if let Shape::Point(point) = shape {
        return Ok(Some((point.x, point.y)));
    }
    if let Shape::MultiPoint(seq) = shape {
        return Ok(stable_point_mean(seq.xs(), seq.ys()));
    }
    let mut xs = Vec::new();
    let mut ys = Vec::new();
    if matches!(shape, Shape::GeometryCollection(_)) {
        let mut admission = None;
        match visit_fallback_points(shape, &mut admission, &mut |x, y| {
            xs.push(x);
            ys.push(y);
        }) {
            CentroidAttempt::ExactAtRoot(_) => {
                drop(xs);
                drop(ys);
                exact_owner_centroid(shape, CentroidDimension::Puntal)
            },
            CentroidAttempt::Ready(()) => Ok(stable_point_mean(&xs, &ys)),
        }
    } else {
        for_each_fallback_point(shape, &mut |x, y| {
            xs.push(x);
            ys.push(y);
        });
        Ok(stable_point_mean(&xs, &ys))
    }
}

pub(super) fn polygon_rings(polygon: &Polygon) -> impl Iterator<Item = (&Ring, bool)> {
    std::iter::once((&polygon.shell, false)).chain(polygon.holes.iter().map(|hole| (hole, true)))
}

pub(super) fn anchor_vertex(xs: &[f64], ys: &[f64], visit: &mut impl FnMut(f64, f64)) {
    if let Some((&x, &y)) = xs.first().zip(ys.first()) {
        visit(x, y);
    }
}

pub(super) fn for_each_fallback_point(shape: &Shape, visit: &mut impl FnMut(f64, f64)) {
    shape.for_each_component(&mut |component| match component {
        Component::Point(point) => visit(point.x, point.y),
        Component::LineString(line) => anchor_vertex(line.xs(), line.ys(), visit),
        Component::Polygon(polygon) => {
            for (ring, _) in polygon_rings(polygon) {
                if let Some((xs, ys)) = ring.xy_columns() {
                    anchor_vertex(xs, ys, visit);
                }
            }
        },
    });
}

pub(super) fn try_anchor_vertex(
    xs: &[f64],
    ys: &[f64],
    visit: &mut impl FnMut(f64, f64) -> bool,
) -> bool {
    xs.first()
        .zip(ys.first())
        .is_some_and(|(&x, &y)| visit(x, y))
}

pub(super) fn try_for_each_fallback_point(
    shape: &Shape,
    visit: &mut impl FnMut(f64, f64) -> bool,
) -> bool {
    shape
        .try_for_each_component(&mut |component| {
            let stopped = match component {
                Component::Point(point) => visit(point.x, point.y),
                Component::LineString(line) => try_anchor_vertex(line.xs(), line.ys(), visit),
                Component::Polygon(polygon) => polygon_rings(polygon).any(|(ring, _)| {
                    ring.xy_columns()
                        .is_some_and(|(xs, ys)| try_anchor_vertex(xs, ys, visit))
                }),
            };
            if stopped { Err(()) } else { Ok(()) }
        })
        .is_err()
}

/// Visit point-tier occurrences at collection boundaries. Non-collection
/// children use the baseline point, line and ring traversal without owner checks.
pub(super) fn visit_fallback_points<'owner>(
    shape: &'owner Shape,
    admission: &mut Option<OwnerAdmission<'owner>>,
    visit: &mut impl FnMut(f64, f64),
) -> CentroidAttempt<()> {
    visit_fallback_points_impl::<false>(shape, admission, visit)
}

pub(super) fn visit_fallback_point_leaf(
    shape: &Shape,
    admission: &mut Option<OwnerAdmission<'_>>,
    visit: &mut impl FnMut(f64, f64),
) -> CentroidAttempt<()> {
    let repeated = admission
        .as_ref()
        .is_some_and(|admission| admission.repeated_owner);
    if !repeated {
        for_each_fallback_point(shape, visit);
        return CentroidAttempt::Ready(());
    }

    let stopped = try_for_each_fallback_point(shape, &mut |x, y| {
        if charge_repeated_work(admission, 1) {
            true
        } else {
            visit(x, y);
            false
        }
    });
    if stopped {
        CentroidAttempt::ExactAtRoot(ExactAtRoot)
    } else {
        CentroidAttempt::Ready(())
    }
}

pub(super) fn visit_fallback_points_impl<'owner, const TRACK_OWNERS: bool>(
    shape: &'owner Shape,
    admission: &mut Option<OwnerAdmission<'owner>>,
    visit: &mut impl FnMut(f64, f64),
) -> CentroidAttempt<()> {
    let Shape::GeometryCollection(collection) = shape else {
        return visit_fallback_point_leaf(shape, admission, visit);
    };
    if TRACK_OWNERS && observe_collection_owner(shape, admission) {
        return CentroidAttempt::ExactAtRoot(ExactAtRoot);
    }
    for child in collection {
        if admission
            .as_ref()
            .is_some_and(|admission| admission.repeated_owner)
            && charge_repeated_work(admission, 1)
        {
            return CentroidAttempt::ExactAtRoot(ExactAtRoot);
        }
        let outcome = if let Shape::GeometryCollection(child_collection) = child {
            if child_collection.members_are_shared() {
                visit_fallback_points_impl::<true>(child, admission, visit)
            } else {
                visit_fallback_points_impl::<false>(child, admission, visit)
            }
        } else {
            visit_fallback_point_leaf(child, admission, visit)
        };
        if let CentroidAttempt::ExactAtRoot(reason) = outcome {
            return CentroidAttempt::ExactAtRoot(reason);
        }
    }
    CentroidAttempt::Ready(())
}
