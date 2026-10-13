use super::*;

pub(super) fn polygon_ring_columns<'a>(
    xs: &'a [f64],
    ys: &'a [f64],
    ring_offsets: &'a [i32],
    rings: std::ops::Range<usize>,
) -> impl Iterator<Item = (bool, &'a [f64], &'a [f64])> + 'a {
    let offsets = ring_offsets
        .get(rings.start..=rings.end)
        .expect("ring offsets cover the admitted ring range");
    offsets
        .array_windows::<2>()
        .enumerate()
        .map(move |(index, &[start, end])| {
            let start = usize::try_from(start).expect("ring offsets are nonnegative");
            let end = usize::try_from(end).expect("ring offsets are nonnegative");
            let xs = xs
                .get(start..end)
                .expect("ring offsets fit the admitted x column");
            let ys = ys
                .get(start..end)
                .expect("ring offsets fit the admitted y column");
            (index != 0, xs, ys)
        })
}

pub(super) fn accumulate_ring_centroid(
    xs: &[f64],
    ys: &[f64],
    hole: bool,
    base: &mut Option<(f64, f64)>,
    area_sum2: &mut f64,
    cg3: &mut (f64, f64),
) {
    let pairs = xs.len().saturating_sub(1);
    if pairs == 0 {
        return;
    }
    // One GLOBAL base point (the first ring vertex seen) keeps every
    // triangle small. The SIMD `centroid_ring_sums` twin of the shoelace
    // fold sums the ring's SIGNED triangle-fan contributions (cross-product
    // areas about the anchor); shells and holes receive opposite signs based
    // on their declared role and winding.
    let Some((&x, &y)) = xs.first().zip(ys.first()) else {
        return;
    };
    let anchor = *base.get_or_insert((x, y));
    let ccw = closed_columns_winding(xs, ys, pairs).is_ccw();
    let sign = if hole == ccw { 1.0 } else { -1.0 };
    let (area2, cgx, cgy) = centroid_ring_sums(xs, ys, pairs, anchor);
    *area_sum2 += sign * area2;
    cg3.0 += sign * cgx;
    cg3.1 += sign * cgy;
}

pub(super) fn finish_areal_centroid(area_sum2: f64, cg3: (f64, f64)) -> Option<(f64, f64, f64)> {
    // Signed triangle-fan sums can cancel to -0.0 on a symmetric polygon; the
    // lineal/puntal paths happen to land on +0.0, so normalize here to keep the
    // +0.0 invariant uniform across centroid of every geometry type.
    // Reject non-finite so a moment overflow cannot reach `Point::new`.
    // Reject subnormal |area|: cg/area collapses toward the origin even when
    // the ring is a positive square (e.g. [0,2e-162]²) — leave None for the
    // exact-areal / lineal cascade.
    let area_abs = area_sum2.abs();
    if area_abs > 0.0 && !area_sum2.is_normal() {
        return None;
    }
    (area_abs > 0.0)
        .then(|| {
            (
                area_abs / 2.0,
                canonicalize_zero(cg3.0 / 3.0 / area_sum2),
                canonicalize_zero(cg3.1 / 3.0 / area_sum2),
            )
        })
        .filter(|(_, x, y)| x.is_finite() && y.is_finite())
}

/// Exact areal centroid over original stored coordinates.  This cold fallback
/// avoids both shared-frame cancellation and a rounded ring-orientation gate.
pub(super) fn exact_areal_centroid(
    visit: impl Fn(&mut dyn FnMut(&[f64], &[f64], bool)),
) -> Option<(f64, f64, f64)> {
    let mut sums = FixedExactCentroidSums::zero();
    visit(&mut |xs, ys, hole| {
        let mut ring = FixedExactCentroidSums::zero();
        for (&[x0, x1], &[y0, y1]) in xs.array_windows::<2>().zip(ys.array_windows::<2>()) {
            ring.add_edge(x0, y0, x1, y1);
        }
        sums.merge_ring(&ring, hole);
    });
    sums.finish_with_area()
}

/// Column kernel for polygonal centroid (dimension-2 pass) over one polygon's
/// ring window range.
/// Use the exact original-coordinate centroid when world-space moments lose digits:
/// tiny extents (subnormal / near-subnormal area) or huge coordinates.
const AREAL_EXACT_MIN_EXTENT: f64 = 1e-8;

pub(super) const AREAL_EXACT_MAX_COORD: f64 = 1e12;

pub(super) fn centroid_polygon_columns(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
) -> ArealCentroidSummary {
    let exact = || {
        exact_areal_centroid(|visit| {
            for (hole, ring_xs, ring_ys) in
                polygon_ring_columns(xs, ys, ring_offsets, rings.clone())
            {
                visit(ring_xs, ring_ys, hole);
            }
        })
    };
    // Classical fan + extent in one ring walk (no separate pre-scan).
    let mut base = None;
    let mut area_sum2 = 0.0_f64;
    let mut cg3 = (0.0_f64, 0.0_f64);
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;
    for (hole, rxs, rys) in polygon_ring_columns(xs, ys, ring_offsets, rings.clone()) {
        for (&x, &y) in rxs.iter().zip(rys.iter()) {
            min_x = min_x.min(x);
            max_x = max_x.max(x);
            min_y = min_y.min(y);
            max_y = max_y.max(y);
        }
        accumulate_ring_centroid(rxs, rys, hole, &mut base, &mut area_sum2, &mut cg3);
    }
    let extreme = if min_x.is_finite() {
        let extent = (max_x - min_x).max(max_y - min_y);
        let max_abs = max_x
            .abs()
            .max(min_x.abs())
            .max(max_y.abs())
            .max(min_y.abs());
        extent < AREAL_EXACT_MIN_EXTENT || max_abs > AREAL_EXACT_MAX_COORD
    } else {
        false
    };
    let centroid = if extreme {
        exact()
    } else {
        // Ordinary coordinates retain the single-pass triangle fan.
        finish_areal_centroid(area_sum2, cg3).or_else(exact)
    };
    let bounds = (min_x.is_finite()
        && max_x.is_finite()
        && min_y.is_finite()
        && max_y.is_finite()
        && area_sum2.is_finite()
        && cg3.0.is_finite()
        && cg3.1.is_finite())
    .then_some(Bounds::new_raw(min_x, min_y, max_x, max_y));
    ArealCentroidSummary { centroid, bounds }
}

/// Packed-polygon row kernel: areal → lineal boundary → first-shell-vertex
/// cascade.
pub(crate) fn centroid_polygon_row_columns(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
) -> Option<(f64, f64)> {
    centroid_polygon_row_columns_with_bounds(xs, ys, ring_offsets, rings).0
}

pub(crate) fn centroid_polygon_row_columns_with_bounds(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
) -> (Option<(f64, f64)>, Option<Bounds>) {
    let summary = centroid_polygon_columns(xs, ys, ring_offsets, rings.clone());
    let centroid = summary
        .centroid
        .map(|(_, x, y)| (x, y))
        .or_else(|| centroid_polygon_lineal_columns(xs, ys, ring_offsets, rings.clone()))
        .or_else(|| {
            let start = usize::try_from(
                *ring_offsets
                    .get(rings.start)
                    .expect("polygon row has an admitted ring start"),
            )
            .expect("ring offsets are nonnegative");
            xs.get(start).zip(ys.get(start)).map(|(&x, &y)| (x, y))
        });
    (centroid, summary.bounds)
}

/// Single-polygon areal centroid (shell + holes). One triangle fan also tracks
/// extent; exact accumulation runs only when the fan rejects or the extent is
/// extreme. Ordinary mid-range polygons pay a single vertex pass (no separate
/// pre-scan).
#[derive(Clone, Copy, Default)]
pub(super) struct ArealCentroidSummary {
    centroid: Option<(f64, f64, f64)>,
    bounds: Option<Bounds>,
}

pub(super) fn areal_centroid_polygon(polygon: &Polygon) -> ArealCentroidSummary {
    let exact = || {
        exact_areal_centroid(|visit| {
            for (ring, hole) in polygon_rings(polygon) {
                let Some((xs, ys)) = ring.xy_columns() else {
                    continue;
                };
                visit(xs, ys, hole);
            }
        })
    };
    let mut base = None;
    let mut area_sum2 = 0.0_f64;
    let mut cg3 = (0.0_f64, 0.0_f64);
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;
    for (ring, hole) in polygon_rings(polygon) {
        let Some((xs, ys)) = ring.xy_columns() else {
            continue;
        };
        for (&x, &y) in xs.iter().zip(ys.iter()) {
            min_x = min_x.min(x);
            max_x = max_x.max(x);
            min_y = min_y.min(y);
            max_y = max_y.max(y);
        }
        accumulate_ring_centroid(xs, ys, hole, &mut base, &mut area_sum2, &mut cg3);
    }
    let extreme = if min_x.is_finite() {
        let extent = (max_x - min_x).max(max_y - min_y);
        let max_abs = max_x
            .abs()
            .max(min_x.abs())
            .max(max_y.abs())
            .max(min_y.abs());
        extent < AREAL_EXACT_MIN_EXTENT || max_abs > AREAL_EXACT_MAX_COORD
    } else {
        false
    };
    // Extreme extents: prefer exact accumulation (classical moments overflow / lose digits).
    let centroid = if extreme {
        exact()
    } else {
        finish_areal_centroid(area_sum2, cg3).or_else(exact)
    };
    let bounds = (min_x.is_finite()
        && max_x.is_finite()
        && min_y.is_finite()
        && max_y.is_finite()
        && area_sum2.is_finite()
        && cg3.0.is_finite()
        && cg3.1.is_finite())
    .then_some(Bounds::new_raw(min_x, min_y, max_x, max_y));
    ArealCentroidSummary { centroid, bounds }
}

pub(super) fn exact_areal_centroid_shape(shape: &Shape) -> Option<(f64, f64, f64)> {
    exact_areal_centroid(|visit| {
        shape.for_each_component(&mut |component| {
            if let Component::Polygon(polygon) = component {
                for (ring, hole) in polygon_rings(polygon) {
                    if let Some((xs, ys)) = ring.xy_columns() {
                        visit(xs, ys, hole);
                    }
                }
            }
        });
    })
}

pub(super) fn areal_child_weight_requires_exact_replay(area: f64) -> bool {
    // Exact zero area finishes as None; a present zero weight is positive
    // exact area that rounded to zero.
    area.is_infinite() || area == 0.0
}

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "Only polygon variants contribute; keep the measured non-areal fast path"
)]
pub(super) fn areal_centroid_summary_direct_with_bounds(shape: &Shape) -> ArealCentroidSummary {
    match shape {
        Shape::Polygon(polygon) => areal_centroid_polygon(polygon),
        Shape::MultiPolygon(polygons) => {
            if polygons.len() == 1 {
                return polygons
                    .first()
                    .map_or_else(ArealCentroidSummary::default, areal_centroid_polygon);
            }
            let mut parts = Vec::with_capacity(polygons.len());
            let mut area = 0.0_f64;
            let mut bounds: Option<Bounds> = None;
            let mut complete_bounds = true;
            for polygon in polygons {
                let summary = areal_centroid_polygon(polygon);
                if let Some(part_bounds) = summary.bounds {
                    match &mut bounds {
                        Some(current) => current.include_bounds(part_bounds),
                        None => bounds = Some(part_bounds),
                    }
                } else {
                    complete_bounds = false;
                }
                if let Some(part) = summary.centroid {
                    if areal_child_weight_requires_exact_replay(part.0) {
                        return ArealCentroidSummary {
                            centroid: exact_areal_centroid_shape(shape),
                            bounds: None,
                        };
                    }
                    if part.0 > 0.0 && part.0.is_finite() {
                        area += part.0;
                        parts.push(part);
                    }
                }
            }
            let merged = weighted_centroid_merge::<true, false>(&parts);
            if merged.fallback
                == WeightedCentroidMergeFallback::PositiveWeightNormalizationUnderflow
            {
                return ArealCentroidSummary {
                    centroid: exact_areal_centroid_shape(shape),
                    bounds: bounds.filter(|_| complete_bounds),
                };
            }
            ArealCentroidSummary {
                centroid: merged.centroid.map(|(x, y)| (area, x, y)),
                bounds: bounds.filter(|_| complete_bounds),
            }
        },
        _ => ArealCentroidSummary::default(),
    }
}

pub(super) fn areal_centroid_summary_direct(shape: &Shape) -> Option<(f64, f64, f64)> {
    areal_centroid_summary_direct_with_bounds(shape).centroid
}

pub(crate) fn areal_centroid_with_bounds(shape: &Shape) -> (Option<(f64, f64)>, Option<Bounds>) {
    if !matches!(shape, Shape::Polygon(_) | Shape::MultiPolygon(_)) {
        return (None, None);
    }
    let summary = areal_centroid_summary_direct_with_bounds(shape);
    (summary.centroid.map(|(_, x, y)| (x, y)), summary.bounds)
}

/// Aggregate areal members only at GeometryCollection boundaries. Other shape
/// variants take their direct kernel and never construct owner-admission state.
pub(super) fn areal_centroid_summary<'owner>(
    shape: &'owner Shape,
    admission: &mut Option<OwnerAdmission<'owner>>,
) -> CentroidAttempt<Option<(f64, f64, f64)>> {
    let repeated_work = if let Shape::GeometryCollection(collection) = shape {
        collection.depth() > 1
            || admission
                .as_ref()
                .is_some_and(|admission| admission.repeated_owner)
    } else {
        false
    };
    if repeated_work {
        areal_centroid_summary_impl::<false, true>(shape, admission)
    } else {
        areal_centroid_summary_impl::<false, false>(shape, admission)
    }
}

pub(super) fn areal_centroid_summary_impl<
    'owner,
    const TRACK_OWNERS: bool,
    const TRACK_REPEAT_WORK: bool,
>(
    shape: &'owner Shape,
    admission: &mut Option<OwnerAdmission<'owner>>,
) -> CentroidAttempt<Option<(f64, f64, f64)>> {
    let Shape::GeometryCollection(collection) = shape else {
        return CentroidAttempt::Ready(areal_centroid_summary_direct(shape));
    };
    if collection.topological_dimension() < Dimension::Surface {
        return CentroidAttempt::Ready(None);
    }
    if TRACK_OWNERS && observe_collection_owner(shape, admission) {
        return CentroidAttempt::ExactAtRoot(ExactAtRoot);
    }
    let mut parts = Vec::new();
    let mut area = 0.0_f64;
    for geometry in collection {
        if TRACK_REPEAT_WORK
            && admission
                .as_ref()
                .is_some_and(|admission| admission.repeated_owner)
            && charge_repeated_work(admission, 1)
        {
            return CentroidAttempt::ExactAtRoot(ExactAtRoot);
        }
        let part = if let Shape::GeometryCollection(child) = geometry {
            let track_owners = child.members_are_shared()
                || (TRACK_OWNERS
                    && admission
                        .as_ref()
                        .is_some_and(|admission| admission.repeated_owner));
            if track_owners {
                areal_centroid_summary_impl::<true, TRACK_REPEAT_WORK>(geometry, admission)
            } else {
                areal_centroid_summary_impl::<false, TRACK_REPEAT_WORK>(geometry, admission)
            }
        } else {
            CentroidAttempt::Ready(areal_centroid_summary_direct(geometry))
        };
        let part = match part {
            CentroidAttempt::Ready(part) => part,
            CentroidAttempt::ExactAtRoot(reason) => {
                return CentroidAttempt::ExactAtRoot(reason);
            },
        };
        if let Some(part) = part {
            if areal_child_weight_requires_exact_replay(part.0) {
                return CentroidAttempt::ExactAtRoot(ExactAtRoot);
            }
            if part.0 > 0.0 && part.0.is_finite() {
                area += part.0;
                parts.push(part);
            }
        }
    }
    let merged = weighted_centroid_merge::<true, false>(&parts);
    if merged.fallback == WeightedCentroidMergeFallback::PositiveWeightNormalizationUnderflow {
        return CentroidAttempt::ExactAtRoot(ExactAtRoot);
    }
    CentroidAttempt::Ready(merged.centroid.map(|(x, y)| (area, x, y)))
}

pub(crate) fn areal_centroid(shape: &Shape) -> CoreResult<Option<(f64, f64)>> {
    if !matches!(shape, Shape::GeometryCollection(_)) {
        return Ok(areal_centroid_summary_direct(shape).map(|(_, x, y)| (x, y)));
    }
    if shape.topological_dimension() < Dimension::Surface {
        return Ok(None);
    }
    let mut admission = None;
    match areal_centroid_summary(shape, &mut admission) {
        CentroidAttempt::Ready(summary) => Ok(summary.map(|(_, x, y)| (x, y))),
        CentroidAttempt::ExactAtRoot(_) => exact_owner_centroid(shape, CentroidDimension::Areal),
    }
}
