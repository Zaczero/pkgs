use super::*;

pub(crate) fn same_storage_similarity_metric_zeros(storage: &GeometryArrayStorage) -> Vec<f64> {
    let mut values = Vec::with_capacity(storage.len());
    match storage {
        GeometryArrayStorage::Packed(PackedStorage::Lines {
            offsets, row_map, ..
        }) => {
            let map = row_map.as_deref();
            let rows = storage::line_logical_len(offsets.as_slice(), map);
            for logical in 0..rows {
                values.push(if map.csr_window(offsets.as_slice(), logical).is_empty() {
                    f64::INFINITY
                } else {
                    0.0
                });
            }
        },
        GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
            let rows = storage::point_logical_len(coords, row_map.as_deref());
            values.resize(rows, 0.0);
        },
        GeometryArrayStorage::Packed(PackedStorage::Polygons {
            ring_offsets,
            polygon_offsets,
            row_map,
            ..
        }) => {
            let map = row_map.as_deref();
            let rows = storage::polygon_logical_len(polygon_offsets.as_slice(), map);
            for logical in 0..rows {
                let rings = map.csr_window(polygon_offsets.as_slice(), logical);
                values.push(
                    if !rings.is_empty()
                        && (ring_offsets[rings.start] as usize) < (ring_offsets[rings.end] as usize)
                    {
                        0.0
                    } else {
                        f64::INFINITY
                    },
                );
            }
        },
        GeometryArrayStorage::Mixed(shapes) => {
            for shape in shapes {
                values.push(if shape.is_empty() { f64::INFINITY } else { 0.0 });
            }
        },
    }
    values
}

/// CRS-aware continuous Hausdorff distance: native linear units on projected
/// frames, raw coordinate units for CRS-free frames, coordinate-space units
/// under `unit='planar'`, explicit SI meters under `unit='meters'`, or true
/// geodesic meters on a geographic CRS.
pub(crate) fn metric_hausdorff(
    model: &crs::MetricModel,
    a: &Shape,
    b: &Shape,
    max_error: Option<f64>,
) -> crate::error::Result<f64> {
    // Domain validation BEFORE the identity exit: equal operands with
    // out-of-domain latitude must still raise on a geographic model.
    if let crs::MetricModel::Geodesic { frame, .. } = model {
        ensure_hausdorff_geodesic_shape(*frame, a)?;
        ensure_hausdorff_geodesic_shape(*frame, b)?;
    }
    // Like `distance`, a supremum of infima over an empty operand is infinite.
    if a.is_empty() || b.is_empty() {
        return Ok(f64::INFINITY);
    }
    if a == b && !a.has_area_parts() {
        return Ok(0.0);
    }
    dispatch_metric_model(
        model,
        |to_metre| crate::geometry::hausdorff_distance(a, b, max_error, to_metre),
        |crs, _, state| crs::geodesic_hausdorff_at(crs, a, b, state),
    )
}

#[cfg(test)]
mod same_storage_metric_tests {
    use super::*;

    #[test]
    fn same_storage_hausdorff_returns_one_zero_per_row() {
        let storage = GeometryArrayStorage::Mixed(vec![
            Shape::Point(Point::new_raw_xy(0.0, 0.0)),
            Shape::Point(Point::new_raw_xy(1.0, 0.0)),
        ]);
        assert_eq!(same_storage_similarity_metric_zeros(&storage), [0.0, 0.0]);
    }
}

pub(crate) fn metric_frechet_with_scratch(
    model: &crs::MetricModel,
    a: &Shape,
    b: &Shape,
    scratch: &mut crate::geometry::FrechetDpScratch,
) -> crate::error::Result<f64> {
    // Domain validation BEFORE the identity exit (same contract as Hausdorff).
    if let crs::MetricModel::Geodesic { frame, .. } = model {
        crs::ensure_geographic_domain_in_frame(*frame, a)?;
        crs::ensure_geographic_domain_in_frame(*frame, b)?;
    }
    // Emptiness is TOTAL and is decided here, once, for every lane below —
    // planar, geodesic and identity alike. Deciding it per kernel is what let
    // the geodesic arm keep raising after the planar one was fixed, and let an
    // identical pair of empties raise (via `single_linework`'s `EmptyLinework`,
    // a DATA condition) while every non-identical empty pair returned the
    // sentinel. Domain validation stays above it: that is a real error, and an
    // empty shape has no coordinates to validate anyway.
    if a.is_empty() || b.is_empty() {
        return Ok(f64::INFINITY);
    }
    if a == b {
        // Kind check before the identity shortcut — equal polygons must still
        // raise `GeometryTypeError`, not return 0.
        a.single_linework()?;
        return Ok(0.0);
    }
    match model {
        crs::MetricModel::Planar { to_metre } => {
            Ok(a.frechet_distance_with_scratch(b, scratch)? * to_metre.get())
        },
        crs::MetricModel::Geodesic { crs, state, .. } => {
            crs::geodesic_frechet_at_with_scratch(crs, a, b, state, scratch)
        },
    }
}

/// The discrete Fréchet DP is `n · m` cells, so `densify` amplifies
/// quadratically: on a pair of two-vertex lines, `1e-5` is 10¹⁰ cells and
/// `1e-6` is 10¹². Charge the product so an unreachable answer is a typed
/// error naming the parameter to raise, rather than an uninterruptible wait.
/// Undensified operands are input-sized and stay uncharged.
fn charge_frechet_dp(a: &Shape, b: &Shape) -> crate::error::Result<()> {
    let mut counts = ShapeCountQuery::default();
    crate::geometry::ExpansionBudget::pairwise(
        "frechet_distance",
        "densify",
        counts.coordinates(a)?,
        counts.coordinates(b)?,
    )
}

pub(crate) fn metric_frechet_densified_with_scratch(
    model: &crs::MetricModel,
    a: &Shape,
    b: &Shape,
    densify: Option<f64>,
    scratch: &mut crate::geometry::FrechetDpScratch,
) -> crate::error::Result<f64> {
    match densify {
        None => {
            charge_frechet_dp(a, b)?;
            metric_frechet_with_scratch(model, a, b, scratch)
        },
        Some(fraction) => {
            let (a, b) = match model {
                crs::MetricModel::Planar { .. } => {
                    let densified = |shape: &Shape| {
                        shape.densified_budgeted(
                            fraction,
                            &mut crate::geometry::ExpansionBudget::new(
                                "frechet_distance",
                                "densify",
                            ),
                        )
                    };
                    (densified(a)?, densified(b)?)
                },
                crs::MetricModel::Geodesic { crs, frame, state } => {
                    crs::ensure_geographic_domain_in_frame(*frame, a)?;
                    crs::ensure_geographic_domain_in_frame(*frame, b)?;
                    // Copy the cached ellipsoid out before running the kernel: the
                    // geodesic metric kernel may acquire the same thread-local
                    // cache, and holding its RefCell borrow across that call would
                    // panic on the similarity path.
                    let geodesic = crs::with_geodesic_cache_at(
                        crs,
                        state,
                        |error| error,
                        |geodesic| Ok(*geodesic),
                    )?;
                    let mut budget =
                        crate::geometry::ExpansionBudget::new("frechet_distance", "densify");
                    (
                        a.densified_budgeted_with_placement(
                            fraction,
                            &crate::geometry::SegmentPlacement::Chart(&geodesic, *frame),
                            &mut budget,
                        )?,
                        b.densified_budgeted_with_placement(
                            fraction,
                            &crate::geometry::SegmentPlacement::Chart(&geodesic, *frame),
                            &mut budget,
                        )?,
                    )
                },
            };
            charge_frechet_dp(&a, &b)?;
            metric_frechet_with_scratch(model, &a, &b, scratch)
        },
    }
}
