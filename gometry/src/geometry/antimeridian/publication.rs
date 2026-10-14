//! Captured literal layout and ancestry for antimeridian output.

mod capture;
mod drawing;
mod restrictions;

pub(super) use capture::SplitCapture;
pub(crate) use drawing::{SplitArrayPublisher, SplitDrawing};
pub(super) use restrictions::restriction_for_edge;

#[cfg(test)]
mod tests {
    use crate::Frame;
    use crate::geometry::embedding_completion::storage::FrozenLiteralBuffers;
    use crate::geometry::publication::{
        ProducerDrawingReceipt, ProducerReceipt, PublicationCall, PublicationOutcome,
        PublicationRejection, StorageTarget, finish_geographic,
    };
    use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
    use crate::geometry::{
        ChartMetric as _, CoordSeq, CoordinateAxes, MOrdinate, Point, Polygon, Ring, Shape,
        ZOrdinate,
    };
    use crate::work::Unmetered;

    fn independent_oblique_edge_fraction(
        parameter: f64,
        start: (f64, f64),
        end: (f64, f64),
        radians_per_unit: f64,
        semi_major: f64,
        flattening: f64,
    ) -> f64 {
        fn integrate(
            limit: f64,
            start: (f64, f64),
            end: (f64, f64),
            radians_per_unit: f64,
            semi_major: f64,
            flattening: f64,
        ) -> f64 {
            // 2^14 Simpson intervals keep quadrature error below the Z tolerance.
            const INTERVALS: usize = 0x4000;
            let step = limit / INTERVALS as f64;
            let longitude_rate = (end.0 - start.0) * radians_per_unit;
            let latitude_rate = (end.1 - start.1) * radians_per_unit;
            let eccentricity_squared = flattening * (2.0 - flattening);
            let mut sum = 0.0;
            for index in 0..=INTERVALS {
                let parameter = index as f64 * step;
                let latitude = (start.1 + (end.1 - start.1) * parameter) * radians_per_unit;
                let sine = latitude.sin();
                let denominator = 1.0 - eccentricity_squared * sine * sine;
                let meridional_radius =
                    semi_major * (1.0 - eccentricity_squared) / denominator.powf(1.5);
                let prime_vertical_radius = semi_major / denominator.sqrt();
                let northing = meridional_radius * latitude_rate;
                let easting = prime_vertical_radius * latitude.cos() * longitude_rate;
                let weight = if index == 0 || index == INTERVALS {
                    1.0
                } else if index % 2 == 0 {
                    2.0
                } else {
                    4.0
                };
                sum += weight * northing.hypot(easting);
            }
            step * sum / 3.0
        }

        integrate(
            parameter,
            start,
            end,
            radians_per_unit,
            semi_major,
            flattening,
        ) / integrate(1.0, start, end, radians_per_unit, semi_major, flattening)
    }

    #[test]
    #[expect(
        clippy::float_cmp,
        reason = "the fixture checks exact seam coordinates from the split chart"
    )]
    fn split_hole_overlay_keeps_the_original_root_for_a_late_coowned_sample() {
        const CRS: &str = r#"GEOGCS["Custom grad",DATUM["Custom datum",SPHEROID["Custom",7000000,150]],PRIMEM["Greenwich",0],UNIT["grad",0.015707963267948967],AXIS["Longitude",EAST],AXIS["Latitude",NORTH]]"#;
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let frame = crate::crs::info_with_state(CRS, &state)
            .unwrap()
            .geographic_frame()
            .unwrap();
        let source_start =
            Point::new_axes(160.0, -40.0, ZOrdinate(Some(20.0)), MOrdinate(Some(200.0))).unwrap();
        let source_end =
            Point::new_axes(240.0, 60.0, ZOrdinate(Some(120.0)), MOrdinate(Some(1200.0))).unwrap();
        let parameter =
            ExactRational::new(ExactDyadic::from_i64(1), ExactDyadic::from_i64(2)).unwrap();
        let (fixture_z, fixture_m) =
            crate::crs::with_ellipsoid_metric_at(CRS, &[], &state, |metric| {
                let edge = metric.chart_edge(source_start, source_end)?;
                let fraction =
                    metric.chart_edge_arclength_fraction(&edge, parameter.round()?, None)?;
                Ok(crate::geometry::source_ordinate_values_at_fraction(
                    source_start,
                    source_end,
                    CoordinateAxes::XYZM,
                    fraction,
                ))
            })
            .unwrap();
        let expected_fraction = independent_oblique_edge_fraction(
            0.5,
            (160.0, -40.0),
            (240.0, 60.0),
            0.015_707_963_267_948_967,
            7_000_000.0,
            1.0 / 150.0,
        );
        let expected_z = 20.0 + (120.0 - 20.0) * expected_fraction;
        let expected_m = 200.0 + (1200.0 - 200.0) * expected_fraction;
        assert!(
            (expected_fraction - 0.5).abs() > 1.0e-4,
            "the oblique custom-ellipsoid oracle must distinguish arclength from chart t"
        );
        let fixture_z = fixture_z.0.expect("source edge has Z");
        let fixture_m = fixture_m.0.expect("source edge has M");

        let ring = |records: &[(f64, f64, f64, f64)]| {
            let points = records
                .iter()
                .map(|&(x, y, z, m)| {
                    Point::new_axes(x, y, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap()
                })
                .collect::<Vec<_>>();
            Ring::closed_coordseq(CoordSeq::try_from_points(&points).unwrap()).unwrap()
        };
        let shell = ring(&[
            (150.0, -80.0, fixture_z, fixture_m),
            (-150.0, -80.0, fixture_z, fixture_m),
            (-150.0, 80.0, fixture_z, fixture_m),
            (150.0, 80.0, fixture_z, fixture_m),
            (150.0, -80.0, fixture_z, fixture_m),
        ]);
        let hole = ring(&[
            (160.0, -50.0, fixture_z, fixture_m),
            (160.0, -40.0, 20.0, 200.0),
            (240.0, 60.0, 120.0, 1200.0),
            (240.0, -50.0, fixture_z, fixture_m),
            (160.0, -50.0, fixture_z, fixture_m),
        ]);
        // The split hole's seam chord partially co-owns the shell's cut chord
        // in reverse. The point member shares the sample's full record, but is
        // a separate source occurrence.
        let point = Shape::Point(
            Point::new_axes(
                200.0,
                10.0,
                ZOrdinate(Some(fixture_z)),
                MOrdinate(Some(fixture_m)),
            )
            .unwrap(),
        );
        let source = Shape::geometry_collection(vec![
            Shape::Polygon(Polygon::new(shell, vec![hole])),
            point,
        ])
        .unwrap();
        let drawing = crate::crs::with_ellipsoid_metric_at(CRS, &[&source], &state, |metric| {
            let metric_access = || Ok(metric);
            source.split_antimeridian_captured_in_frame_with_metric(frame, Some(&metric_access))
        })
        .unwrap()
        .orient_geojson()
        .unwrap();
        let row = drawing.into_row(0, 0).unwrap();
        let sample = row
            .literals
            .iter()
            .find(|literal| literal.record.x.abs() == 200.0 && literal.record.y == 10.0)
            .expect("the hole overlay retains its oblique seam sample");
        assert!(
            (sample.record.z().unwrap() - expected_z).abs() <= 2.0e-8,
            "the later sample must use original-edge ellipsoidal Z: {:?}, expected {expected_z}",
            sample.record
        );
        assert!(
            (sample.record.m().unwrap() - expected_m).abs() <= 2.0e-7,
            "the later sample must use original-edge ellipsoidal M: {:?}, expected {expected_m}",
            sample.record
        );

        let buffers = FrozenLiteralBuffers::shapes(row.shape, row.layout.layout);
        let receipt = ProducerDrawingReceipt::new(buffers, row.literals);
        let mut work = Unmetered;
        let outcome = finish_geographic(
            PublicationCall::new(
                ProducerReceipt::ProducerDrawing(receipt),
                Frame::None,
                &mut work,
            ),
            StorageTarget::Scalar,
        )
        .unwrap();
        let PublicationOutcome::Pass(finished) = outcome else {
            panic!("the final scalar storage accepts the coowned split receipt");
        };
        let (published, _) = finished.into_scalar().unwrap();
        let mut retained = false;
        published.for_each_point(|point| {
            retained |= point.x.abs() == 200.0
                && point.y == 10.0
                && point.z().is_some_and(|z| (z - expected_z).abs() <= 2.0e-8)
                && point.m().is_some_and(|m| (m - expected_m).abs() <= 2.0e-7);
        });
        assert!(
            retained,
            "the final storage readback retains the late sample"
        );
    }

    #[test]
    fn captured_pole_cap_publishes_valid_output() {
        let shell = Ring::closed_coordseq(
            CoordSeq::try_from_vecs(vec![170.0, -70.0, 50.0, 170.0], vec![80.0; 4], None, None)
                .unwrap(),
        )
        .unwrap();
        let source = Shape::Polygon(Polygon::new(shell, Vec::new()));
        let drawing = source.split_antimeridian_captured_in_crs84().unwrap();
        let mut pole_cap = false;
        drawing.shape.for_each_point(|point| {
            pole_cap |= point.y.to_bits() == 90.0_f64.to_bits();
        });
        assert!(pole_cap, "the fixture must publish a north-pole cap");

        let finished = drawing
            .finish_scalar(Frame::None)
            .expect("the pole-cap output passes final storage readback");
        let (published, _) = finished.into_scalar().unwrap();
        assert_eq!(published.validate().unwrap(), None, "{published:?}");
    }

    #[test]
    #[expect(
        clippy::float_cmp,
        reason = "this operation classifies exact antimeridian or pole coordinates before chart mapping"
    )]
    fn omitted_synthetic_pole_literal_is_rejected_by_final_readback() {
        let shell = Ring::closed_coordseq(
            CoordSeq::try_from_vecs(
                vec![-45.0, 45.0, 135.0, -135.0, -45.0],
                vec![40.0, 40.0, 40.0, 40.0, 40.0],
                None,
                None,
            )
            .unwrap(),
        )
        .unwrap();
        let source = Shape::Polygon(Polygon::new(shell, Vec::new()));
        let mut row = source
            .split_antimeridian_captured_in_crs84()
            .unwrap()
            .into_row(0, 0)
            .unwrap();
        let omitted_index = row
            .literals
            .iter()
            .position(|literal| literal.record.y.abs() == 90.0)
            .expect("the XY polar split emits a synthetic pole literal");
        let omitted = row.literals.remove(omitted_index);
        let buffers = FrozenLiteralBuffers::shapes(row.shape, row.layout.layout);
        let receipt = ProducerDrawingReceipt::new(buffers, row.literals);
        let mut work = Unmetered;
        let outcome = finish_geographic(
            PublicationCall::new(
                ProducerReceipt::ProducerDrawing(receipt),
                Frame::None,
                &mut work,
            ),
            StorageTarget::Scalar,
        )
        .expect("an omitted receipt record is a readback rejection");
        let PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) = outcome
        else {
            panic!("the shared readback rejects a missing producer literal");
        };
        assert_eq!(images, vec![omitted.image]);
    }
}
