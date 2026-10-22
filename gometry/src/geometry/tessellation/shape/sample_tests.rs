use super::{area_density, area_density_bound, geographic_sample_segments};
use crate::geographic::GeographicFrame;
use crate::geometry::tessellation::{
    GeographicSampleSegment, PreparedSampling, SampleRecipe, SampleSupport, sample_weighted,
};
use crate::geometry::{
    CoordSeq, CoordinateAxes, LineSeq, MultiLineString, MultiMember, MultiPolygon, Point, Polygon,
    Ring, Shape, TopologyDomain, row_sample_seed,
};

fn line_seq(points: &[(f64, f64)]) -> LineSeq {
    let points = points
        .iter()
        .map(|&(x, y)| Point::new(x, y).unwrap())
        .collect::<Vec<_>>();
    LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap()
}

fn line(points: &[(f64, f64)]) -> Shape {
    Shape::LineString(line_seq(points))
}

fn polygon(points: &[(f64, f64)]) -> Polygon {
    let (xs, ys): (Vec<_>, Vec<_>) = points.iter().copied().unzip();
    Polygon::new(
        Ring::closed_coordseq(CoordSeq::try_from_vecs(xs, ys, None, None).unwrap()).unwrap(),
        Vec::new(),
    )
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn prolate_area_sampling_envelope_dominates_equatorial_density() {
    let model =
        crate::crs::metric_model(Some("+proj=longlat +a=6371000 +b=6434710 +type=crs")).unwrap();
    let metric = crate::crs::GeodesicMetric::from_model(&model).unwrap();
    let triangle = [
        Point::new(0.0, 0.0).unwrap(),
        Point::new(1.0, 0.0).unwrap(),
        Point::new(0.0, 80.0).unwrap(),
    ];
    let bound = area_density_bound(&metric, &triangle);
    assert!(metric.geodesic.f < 0.0);
    assert!(bound >= 1.0);
    for latitude in [0.0, 15.0, 45.0, 80.0] {
        assert!(bound >= area_density(&metric, latitude));
    }
}

fn xy_bits(points: &[Point]) -> Vec<(u64, u64)> {
    points
        .iter()
        .map(|point| (point.x.to_bits(), point.y.to_bits()))
        .collect()
}

fn retained_segment(support: &SampleSupport) -> GeographicSampleSegment {
    let SampleSupport::GeographicSegment { segment } = support else {
        panic!("retained geographic segment support")
    };
    *segment
}

#[test]
fn retained_line_midpoint_is_the_antimeridian() {
    let frame = GeographicFrame::crs84();
    let source = line(&[(170.0, 0.0), (-170.0, 0.0)]);
    let segments = geographic_sample_segments(&source, frame).unwrap();

    assert_eq!(segments.len(), 1);
    let midpoint = retained_segment(segments.first().expect("one retained segment"))
        .sample(0.5, frame)
        .unwrap();
    assert_eq!(midpoint.x.abs(), 180.0);
    assert_eq!(midpoint.y, 0.0);

    let support = segments;
    let draws = sample_weighted(
        1,
        1,
        &support,
        |_| 1.0,
        |support, _| {
            let segment = retained_segment(support);
            let along = 0.5;
            Ok((segment.sample(along, frame)?, SampleRecipe::Segment {
                along,
            }))
        },
    )
    .unwrap()
    .unwrap();
    let product =
        PreparedSampling::from_draws(4, 1, TopologyDomain::Geographic(frame), support, draws);
    assert_eq!(product.logical_row, 4);
    assert_eq!(product.resolved_seed, 1);
    assert_eq!(product.domain, TopologyDomain::Geographic(frame));
    let [draw] = product.draws.as_slice() else {
        panic!("one midpoint draw")
    };
    assert_eq!(draw.draw_ordinal, 0);
    assert_eq!(draw.selected_element, 0);
    let segment = retained_segment(
        product
            .support_elements
            .get(draw.selected_element)
            .expect("draw selects a prepared support element"),
    );
    assert_eq!(segment.occurrence.component, 0);
    assert_eq!(segment.occurrence.occurrence, 0);
    assert_eq!(segment.longitude.delta_native(frame.longitude()), 20.0);
    assert_eq!(draw.recipe, SampleRecipe::Segment { along: 0.5 });
    assert_eq!(draw.point.x.abs(), 180.0);
    assert_eq!(draw.point.y, 0.0);
}

#[test]
fn line_receipt_keeps_selected_segment_and_along_parameter() {
    let source = line(&[(0.0, 0.0), (1.0, 0.0), (2.0, 0.0)]);
    let product = source
        .sample_point_receipts_in_domain(1, 1, 12, TopologyDomain::Cartesian)
        .unwrap();
    let [draw] = product.draws.as_slice() else {
        panic!("one line sample")
    };

    assert_eq!(product.logical_row, 12);
    assert_eq!(product.resolved_seed, 1);
    assert_eq!(draw.draw_ordinal, 0);
    assert_eq!(draw.selected_element, 1);
    let SampleSupport::Segment {
        occurrence,
        segment,
    } = *product
        .support_elements
        .get(draw.selected_element)
        .expect("draw selects a prepared support element")
    else {
        panic!("Cartesian segment receipt")
    };
    assert_eq!(occurrence.component, 0);
    assert_eq!(occurrence.occurrence, 1);
    assert_eq!((segment.start.x, segment.start.y), (1.0, 0.0));
    assert_eq!((segment.end.x, segment.end.y), (2.0, 0.0));
    assert_eq!(draw.recipe, SampleRecipe::Segment {
        along: 0.745_781_757_262_701_1,
    });
    assert_eq!(draw.point.x, 1.745_781_757_262_701);
    assert_eq!(draw.point.y, 0.0);
}

#[test]
fn coincident_atom_draws_keep_each_selected_occurrence() {
    let point = Point::new(3.0, 4.0).unwrap();
    let source = Shape::multi_point(
        CoordinateAxes::XY,
        CoordSeq::try_from_points(&[point, point, point]).unwrap(),
    );
    let product = source
        .sample_point_receipts_in_domain(2, 1, 5, TopologyDomain::Cartesian)
        .unwrap();

    assert_eq!(product.logical_row, 5);
    assert_eq!(product.resolved_seed, 1);
    assert_eq!(product.support_elements.len(), 3);
    assert_eq!(product.draws.len(), 2);
    for (draw_ordinal, draw) in product.draws.iter().enumerate() {
        assert_eq!(draw.draw_ordinal, draw_ordinal);
        assert_eq!(draw.point, point);
        assert_eq!(draw.recipe, SampleRecipe::Atom);
        let SampleSupport::PointAtom {
            occurrence,
            point: support,
        } = *product
            .support_elements
            .get(draw.selected_element)
            .expect("draw selects a prepared support element")
        else {
            panic!("point atom receipt")
        };
        assert_eq!(occurrence.component, 0);
        assert_eq!(occurrence.occurrence, draw.selected_element);
        assert_eq!(support, point);
    }
    assert_eq!(
        product
            .draws
            .iter()
            .map(|draw| draw.selected_element)
            .collect::<Vec<_>>(),
        [1, 2]
    );
}

#[test]
fn area_receipt_keeps_the_selected_earcut_triangle_and_parameters() {
    let source_vertices = [
        Point::new(0.0, 0.0).unwrap(),
        Point::new(10.0, 0.0).unwrap(),
        Point::new(10.0, 10.0).unwrap(),
        Point::new(0.0, 10.0).unwrap(),
    ];
    let source = Shape::Polygon(polygon(&[
        (0.0, 0.0),
        (10.0, 0.0),
        (10.0, 10.0),
        (0.0, 10.0),
    ]));
    let product = source
        .sample_point_receipts_in_domain(1, 1, 9, TopologyDomain::Cartesian)
        .unwrap();

    assert_eq!(product.logical_row, 9);
    assert_eq!(product.resolved_seed, 1);
    assert_eq!(product.support_elements.len(), 2);
    let [draw] = product.draws.as_slice() else {
        panic!("one weighted area draw")
    };
    assert_eq!(draw.draw_ordinal, 0);
    assert_eq!(draw.selected_element, 1);
    let SampleSupport::AreaTriangle {
        occurrence,
        vertices,
        corners,
    } = *product
        .support_elements
        .get(draw.selected_element)
        .expect("draw selects a prepared support element")
    else {
        panic!("area triangle receipt")
    };
    assert_eq!(occurrence.component, 0);
    assert_eq!(occurrence.occurrence, draw.selected_element);
    for (point, corner) in vertices.iter().zip(corners) {
        assert_eq!(corner.site.ring, 0);
        assert_eq!(
            *source_vertices
                .get(corner.site.vertex)
                .expect("Earcut site ID points to a source corner"),
            *point
        );
    }
    assert!(
        corners
            .iter()
            .all(|corner| corner.occurrence.0 < source_vertices.len())
    );
    assert_eq!(draw.recipe, SampleRecipe::Triangle {
        spread: 0.863_586_566_166_184_1,
        blend: 0.971_002_753_586_796_2,
    });
    assert!(draw.point.x.is_finite() && draw.point.y.is_finite());
}

#[rstest::rstest]
#[case::coordinates_180_0_0_0_180_0_0_0([(-180.0, 0.0), (180.0, 0.0)], [-180.0, 180.0])]
#[case::coordinates_0_0_0_0_720_0_0_0([(0.0, 0.0), (720.0, 0.0)], [0.0, 720.0])]
fn periodic_alias_edges_sample_only_their_quotient_vertices(
    #[case] coordinates: [(f64, f64); 2],
    #[case] aliases: [f64; 2],
) {
    let frame = GeographicFrame::crs84();

    let source = line(&coordinates);
    let points = source
        .sample_points_in_domain(32, 5, TopologyDomain::Geographic(frame))
        .unwrap();
    assert_eq!(points.len(), 32);
    assert!(
        points
            .iter()
            .all(|point| { point.y == 0.0 && aliases.contains(&point.x) }),
        "input={coordinates:?}, samples={points:?}"
    );
}

#[test]
fn zero_geographic_sample_count_skips_support_admission() {
    let invalid = line(&[(0.0, 100.0), (1.0, 101.0)]);
    let domain = TopologyDomain::Geographic(GeographicFrame::crs84());

    assert!(
        invalid
            .sample_points_in_domain(0, 1, domain)
            .unwrap()
            .is_empty()
    );
    invalid.sample_points_in_domain(1, 1, domain).unwrap_err();
}

#[test]
fn retained_line_weight_and_seeded_draw_use_the_twenty_degree_edge() {
    let frame = GeographicFrame::crs84();
    let lines = Shape::MultiLineString(
        MultiLineString::try_new(CoordinateAxes::XY, vec![
            MultiMember::Value(line_seq(&[(170.0, 0.0), (-170.0, 0.0)])),
            MultiMember::Value(line_seq(&[(0.0, 0.0), (100.0, 0.0)])),
        ])
        .unwrap(),
    );
    let segments = geographic_sample_segments(&lines, frame).unwrap();

    let [first, second] = segments.as_slice() else {
        panic!("two retained source segments")
    };
    let first = retained_segment(first);
    let second = retained_segment(second);
    assert_eq!(first.delta_x, 20.0);
    assert_eq!(second.delta_x, 100.0);
    assert_eq!(second.delta_x / first.delta_x, 5.0);

    let resolved_seed = row_sample_seed(1, 0);
    let product = lines
        .sample_point_receipts_in_domain(1, resolved_seed, 0, TopologyDomain::Geographic(frame))
        .unwrap();
    assert_eq!(product.logical_row, 0);
    assert_eq!(product.resolved_seed, resolved_seed);
    let [draw] = product.draws.as_slice() else {
        panic!("one retained-line draw")
    };
    assert_eq!(draw.selected_element, 1);
    let segment = retained_segment(
        product
            .support_elements
            .get(draw.selected_element)
            .expect("draw selects a prepared support element"),
    );
    assert_eq!(segment.occurrence.component, 1);
    assert_eq!(segment.occurrence.occurrence, 0);
    assert_eq!(segment.delta_x, 100.0);
    assert_eq!(draw.recipe, SampleRecipe::Segment {
        along: 0.943_564_230_864_854_4,
    });
    assert!((draw.point.x - 94.356_423_086_485_44).abs() <= 1.0e-12);
    assert_eq!(draw.point.y, 0.0);
}

#[test]
fn duplicate_crossing_polygons_keep_their_area_weight_multiplicity() {
    let frame = GeographicFrame::crs84();
    let crossing = polygon(&[
        (170.0, 0.0),
        (-170.0, 0.0),
        (-170.0, 10.0),
        (170.0, 10.0),
        (170.0, 0.0),
    ]);
    let ordinary = polygon(&[
        (0.0, 0.0),
        (20.0, 0.0),
        (20.0, 10.0),
        (0.0, 10.0),
        (0.0, 0.0),
    ]);
    let source = Shape::MultiPolygon(
        MultiPolygon::try_new(CoordinateAxes::XY, vec![
            MultiMember::Value(crossing.clone()),
            MultiMember::Value(crossing),
            MultiMember::Value(ordinary),
        ])
        .unwrap(),
    );
    let sample = source
        .sample_points_in_domain(1, 1, TopologyDomain::Geographic(frame))
        .unwrap();

    assert_eq!(sample.len(), 1);
    assert!(sample[0].x.abs() > 170.0, "sample was {sample:?}");
    assert!((0.0..=10.0).contains(&sample[0].y));
}

#[test]
fn raw_compatible_geographic_and_cartesian_samples_keep_the_base_bits() {
    let ordinary_geographic = [
        line(&[(190.0, 10.0), (200.0, 20.0)]),
        Shape::Polygon(polygon(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ])),
    ];
    let frame = GeographicFrame::crs84();
    for source in ordinary_geographic {
        let base = source.sample_points(5, 7).unwrap();
        let geographic = source
            .sample_points_in_domain(5, 7, TopologyDomain::Geographic(frame))
            .unwrap();
        assert_eq!(xy_bits(&geographic), xy_bits(&base));
    }

    let cartesian = line(&[(0.0, 0.0), (1.0, 1.0)]);
    assert_eq!(
        xy_bits(
            &cartesian
                .sample_points_in_domain(5, 7, TopologyDomain::Cartesian)
                .unwrap()
        ),
        xy_bits(&cartesian.sample_points(5, 7).unwrap())
    );
}
