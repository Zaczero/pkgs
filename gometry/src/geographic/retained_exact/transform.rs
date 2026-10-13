//! Retained geographic transform arithmetic.

use super::edges::retained_edge_displacement;
use super::sites::exact_integer_i64;
use crate::geometry::ExactDyadic;
#[cfg(test)]
use crate::geometry::ExactRational;

/// Select the target lift that exactly minimizes the difference between the
/// source connection and the transformed endpoint connection.
///
/// The source branch comes from its admitted endpoint pair. Source and target
/// longitude-axis orientations are applied before comparing displacements;
/// ties retain a source Circle's longer branch, otherwise prefer the shorter
/// target displacement, then follow the authentic canonical target difference.
/// This selects an endpoint-defined target chart chord, not an interior source
/// edge transport.
pub(crate) fn select_transformed_chart_displacement(
    source_frame: crate::geographic::GeographicFrame,
    source_endpoints: [f64; 2],
    target_frame: crate::geographic::GeographicFrame,
    target_endpoints: [f64; 2],
) -> crate::error::Result<crate::geographic::RetainedChartDisplacement> {
    use std::cmp::Ordering;

    let source_axis = source_frame.longitude();
    let target_axis = target_frame.longitude();
    let source_connection = source_axis
        .chart_connection(source_endpoints[0], source_endpoints[1])
        .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))?;
    let target_endpoints = [
        target_axis
            .canonicalize(target_endpoints[0])
            .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))?,
        target_axis
            .canonicalize(target_endpoints[1])
            .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))?,
    ];

    let source_displacement =
        retained_edge_displacement(source_connection.retained(), source_axis)?;
    let target_period = ExactDyadic::from_f64(target_axis.period());
    let source_period = ExactDyadic::from_f64(source_axis.period());
    let source_in_target_orientation = source_displacement.try_product(&target_period)?;
    let source_in_target_orientation = if source_frame.orientation().longitude_east()
        == target_frame.orientation().longitude_east()
    {
        source_in_target_orientation
    } else {
        source_in_target_orientation.neg()
    };
    let target_difference = ExactDyadic::from_f64(target_endpoints[1].get())
        .try_subtract(ExactDyadic::from_f64(target_endpoints[0].get()))?;
    let target_difference_scaled = target_difference.try_product(&source_period)?;
    let numerator = source_in_target_orientation.try_subtract(target_difference_scaled)?;
    let denominator = source_period.try_product(&target_period)?;
    // A source chart branch spans at most one source period, while the
    // canonical target difference is strictly less than one target period.
    // Thus the selected K is relative and bounded by two, never an absolute
    // longitude sheet.
    let center = numerator.nearest_integer_quotient_ties_to_zero(&denominator)?;
    // The quotient is already the exact nearest lift; only an exact half
    // residual has a second minimizer.
    let residual = numerator.try_subtract(denominator.try_product(&center)?)?;
    if residual.cmp_magnitude_scaled_with_work(&denominator, -1, &mut crate::work::Unmetered)?
        != Ordering::Equal
    {
        return Ok(
            crate::geographic::RetainedChartDisplacement::from_native_endpoints(
                target_endpoints,
                exact_integer_i64(&center)?,
            ),
        );
    }

    let zero = ExactDyadic::zero();
    let residual_direction = residual.cmp(&zero);
    debug_assert_ne!(
        residual_direction,
        Ordering::Equal,
        "an exact half residual is nonzero"
    );
    let competitor = if residual_direction == Ordering::Greater {
        ExactDyadic::try_add_refs(&center, &ExactDyadic::one())?
    } else {
        ExactDyadic::try_subtract_refs(&center, &ExactDyadic::one())?
    };
    let center_period = target_period.try_product(&center)?;
    let center_displacement = ExactDyadic::try_add_refs(&target_difference, &center_period)?;
    let competitor_displacement = if residual_direction == Ordering::Greater {
        ExactDyadic::try_add_refs(&center_displacement, &target_period)?
    } else {
        ExactDyadic::try_subtract_refs(&center_displacement, &target_period)?
    };
    let source_is_circle = matches!(
        source_connection.step,
        crate::geographic::ChartStep::Circle(_)
    );
    let lift = match center_displacement.cmp_magnitude_scaled_with_work(
        &competitor_displacement,
        0,
        &mut crate::work::Unmetered,
    )? {
        Ordering::Greater if source_is_circle => center,
        Ordering::Greater => competitor,
        Ordering::Less if source_is_circle => competitor,
        Ordering::Less => center,
        Ordering::Equal => {
            let target_direction = target_difference.cmp(&zero);
            let center_direction = center_displacement.cmp(&zero);
            let competitor_direction = competitor_displacement.cmp(&zero);
            let center_follows = center_direction == target_direction;
            let competitor_follows = competitor_direction == target_direction;
            debug_assert!(
                target_direction != Ordering::Equal
                    && center_direction != Ordering::Equal
                    && competitor_direction != Ordering::Equal
                    && center_follows != competitor_follows,
                "equal-magnitude target lifts are antipodal with a directed endpoint pair"
            );
            if center_follows { center } else { competitor }
        },
    };

    Ok(
        crate::geographic::RetainedChartDisplacement::from_native_endpoints(
            target_endpoints,
            exact_integer_i64(&lift)?,
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geographic::retained_exact::sites::{
        ExactNativeLongitude, retained_exact_native_site, round_native_longitude,
        split_rational_native_site,
    };
    use crate::geographic::{
        AngularFactor, GeographicAxisOrientation, GeographicFrame, RetainedLongitudeSite,
    };

    fn connector_frame(period: f64, longitude_east: bool) -> GeographicFrame {
        let unit = AngularFactor::try_new("longitude", std::f64::consts::TAU / period).unwrap();
        GeographicFrame::try_new_with_orientation(
            unit,
            unit,
            0.0,
            GeographicAxisOrientation::from_axis_directions(longitude_east, true),
        )
        .unwrap()
    }
    #[test]
    fn transformed_circle_ties_keep_the_long_branch_and_reverse_with_traversal() {
        let degrees = connector_frame(360.0, true);
        let forward =
            select_transformed_chart_displacement(degrees, [10.0, 370.0], degrees, [0.0, -180.0])
                .unwrap();
        let reverse =
            select_transformed_chart_displacement(degrees, [370.0, 10.0], degrees, [-180.0, 0.0])
                .unwrap();

        assert_eq!(forward.periods(), 2);
        assert_eq!(forward.delta_native(degrees.longitude()), 540.0);
        assert_eq!(reverse.periods(), -2);
        assert_eq!(reverse.delta_native(degrees.longitude()), -540.0);
        assert_eq!(forward.endpoints().map(|longitude| longitude.get()), [
            0.0, -180.0
        ]);
        assert_eq!(reverse.endpoints().map(|longitude| longitude.get()), [
            -180.0, 0.0
        ]);
    }
    #[test]
    fn transformed_shortest_ties_do_not_add_a_turn_to_a_half_edge() {
        let degrees = connector_frame(360.0, true);
        let edge =
            select_transformed_chart_displacement(degrees, [0.0, 180.0], degrees, [12.0, 12.0])
                .unwrap();

        assert_eq!(edge.periods(), 0);
        assert_eq!(edge.delta_native(degrees.longitude()), 0.0);
        assert_eq!(edge.endpoints().map(|longitude| longitude.get()), [
            12.0, 12.0
        ]);
    }
    #[test]
    fn transformed_connector_compares_different_native_periods_exactly() {
        let grads = connector_frame(400.0, true);
        let degrees = connector_frame(360.0, true);
        let edge =
            select_transformed_chart_displacement(grads, [10.0, 410.0], degrees, [0.0, -180.0])
                .unwrap();

        assert_eq!(edge.periods(), 2);
        assert_eq!(edge.delta_native(degrees.longitude()), 540.0);
    }
    #[test]
    fn transformed_connector_uses_the_exact_side_of_a_rounded_half_period() {
        let degrees = connector_frame(360.0, true);
        let target: [f64; 2] = [-0.1, 179.9];
        assert_eq!((target[1] - target[0]).to_bits(), 180.0_f64.to_bits());

        let edge =
            select_transformed_chart_displacement(degrees, [0.0, 0.0], degrees, target).unwrap();

        assert_eq!(edge.periods(), -1);
        assert_eq!(
            edge.endpoints().map(|longitude| longitude.get().to_bits()),
            target.map(f64::to_bits)
        );
    }
    #[test]
    fn transformed_zero_ties_follow_the_canonical_target_difference() {
        let degrees = connector_frame(360.0, true);
        let negative =
            select_transformed_chart_displacement(degrees, [0.0, 0.0], degrees, [0.0, -180.0])
                .unwrap();
        let positive =
            select_transformed_chart_displacement(degrees, [0.0, 0.0], degrees, [-180.0, 0.0])
                .unwrap();

        assert_eq!(negative.periods(), 0);
        assert_eq!(negative.delta_native(degrees.longitude()), -180.0);
        assert_eq!(positive.periods(), 0);
        assert_eq!(positive.delta_native(degrees.longitude()), 180.0);
    }
    #[test]
    fn transformed_west_east_period_lift_preserves_forward_endpoint_bits() {
        let west = connector_frame(360.0, false);
        let east = connector_frame(360.0, true);
        let source = [0.0, 176.4];
        let target = [0.0, -176.4];
        let edge = select_transformed_chart_displacement(west, source, east, target).unwrap();

        assert_eq!(edge.periods(), 0);
        assert_eq!(
            edge.endpoints().map(|longitude| longitude.get().to_bits()),
            target.map(f64::to_bits)
        );
    }
    #[test]
    fn transformed_west_east_period_lift_preserves_reversed_endpoint_bits() {
        let west = connector_frame(360.0, false);
        let east = connector_frame(360.0, true);
        let source = [176.4, 0.0];
        let target = [-176.4, 0.0];
        let edge = select_transformed_chart_displacement(west, source, east, target).unwrap();

        assert_eq!(edge.periods(), 0);
        assert_eq!(
            edge.endpoints().map(|longitude| longitude.get().to_bits()),
            target.map(f64::to_bits)
        );
    }
    #[test]
    fn transformed_connector_retains_authentic_target_endpoints() {
        let degrees = connector_frame(360.0, true);
        let target = [-0.1, 0.2];
        let edge =
            select_transformed_chart_displacement(degrees, [0.0, 0.3], degrees, target).unwrap();

        assert_eq!(edge.periods(), 0);
        assert_eq!(
            edge.endpoints().map(|longitude| longitude.get().to_bits()),
            target.map(f64::to_bits)
        );
    }
    #[test]
    fn scalar_and_retained_projection_share_exact_rounding_without_lift_narrowing() {
        let degree = GeographicFrame::crs84().longitude();
        let grad = GeographicFrame::try_new(
            crate::geographic::AngularFactor::try_new("longitude", std::f64::consts::PI / 200.0)
                .unwrap(),
            crate::geographic::AngularFactor::try_new("latitude", std::f64::consts::PI / 200.0)
                .unwrap(),
            0.0,
        )
        .unwrap()
        .longitude();
        let radian = GeographicFrame::try_new(
            crate::geographic::AngularFactor::try_new("longitude", 1.0).unwrap(),
            crate::geographic::AngularFactor::try_new("latitude", 1.0).unwrap(),
            0.0,
        )
        .unwrap()
        .longitude();
        let third = ExactRational::new(ExactDyadic::one(), ExactDyadic::from_i64(3)).unwrap();
        let gap =
            ExactRational::new(ExactDyadic::one(), ExactDyadic::from_i64(1_i64 << 47)).unwrap();
        for (axis, exact, expected) in [
            (degree, third.clone(), 1.0 / 3.0),
            (degree, ExactRational::from_f64(-0.0), 0.0),
            (degree, ExactRational::from_f64(180.0), -180.0),
            (grad, ExactRational::from_f64(200.0), -200.0),
            (
                radian,
                ExactRational::from_f64(std::f64::consts::PI),
                -std::f64::consts::PI,
            ),
            (
                degree,
                ExactRational::from_f64(180.0).try_subtract(&gap).unwrap(),
                -180.0,
            ),
            (
                degree,
                ExactRational::from_f64(-180.0).try_subtract(&gap).unwrap(),
                -180.0,
            ),
            (degree, ExactRational::from_f64(540.0), -180.0),
            (degree, ExactRational::from_f64(-540.0), -180.0),
        ] {
            let scalar = round_native_longitude(axis, &exact).unwrap();
            let retained = retained_exact_native_site(axis, &exact).unwrap();
            let rounded = ExactNativeLongitude::round_site(axis, &retained).unwrap();
            assert_eq!(scalar.to_bits(), expected.to_bits());
            assert_eq!(rounded.native().get().to_bits(), expected.to_bits());
            assert!((-axis.half_period()..axis.half_period()).contains(&scalar));
        }
        let (borrowed, lift) = split_rational_native_site(&third, 360.0, 180.0).unwrap();
        assert!(
            matches!(borrowed, std::borrow::Cow::Borrowed(value) if std::ptr::eq(value, &raw const third))
        );
        assert!(lift.is_zero());
        for axis in [degree, grad, radian] {
            let huge = ExactRational::from_f64(2.0_f64.powi(80))
                .try_product(&ExactRational::from_f64(axis.period()))
                .unwrap()
                .try_add(&third)
                .unwrap();
            assert_eq!(
                round_native_longitude(axis, &huge).unwrap().to_bits(),
                (1.0_f64 / 3.0).to_bits()
            );
            drop(retained_exact_native_site(axis, &huge).unwrap_err());
        }
        let almost_half = ExactRational::from_f64(180.0).try_subtract(&gap).unwrap();
        let overflow_site = RetainedLongitudeSite::new(
            ExactNativeLongitude::from_rational(degree, almost_half.clone()).unwrap(),
            i64::MAX,
        );
        drop(ExactNativeLongitude::round_site(degree, &overflow_site).unwrap_err());
        assert_eq!(
            round_native_longitude(degree, &almost_half).unwrap(),
            -180.0
        );
    }
}
