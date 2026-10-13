use super::*;

pub(super) fn metric_with_flattening_and_frame(
    flattening: f64,
    frame: GeographicFrame,
) -> GeodesicMetric {
    metric_with_semimajor_and_frame(6_378_137.0, flattening, frame)
}

pub(super) fn metric_with_semimajor_and_frame(
    semi_major: f64,
    flattening: f64,
    frame: GeographicFrame,
) -> GeodesicMetric {
    let geodesic = Geodesic::new(semi_major, flattening);
    let authalic_series = AuthalicSeries::new(&geodesic);
    GeodesicMetric {
        crs: Arc::new("EPSG:4326".to_owned()),
        geodesic: Box::new(geodesic),
        authalic_series,
        frame,
        state: crate::crs::runtime_state_snapshot().unwrap(),
    }
}

pub(super) fn metric_with_flattening(flattening: f64) -> GeodesicMetric {
    metric_with_flattening_and_frame(flattening, GeographicFrame::crs84())
}

pub(super) fn oriented_degree_frame(longitude_east: bool, latitude_north: bool) -> GeographicFrame {
    let unit = AngularFactor::try_new("degree", std::f64::consts::PI / 180.0).unwrap();
    GeographicFrame::try_new_with_orientation(
        unit,
        unit,
        0.0,
        GeographicAxisOrientation::from_axis_directions(longitude_east, latitude_north),
    )
    .unwrap()
}

pub(super) fn metric() -> GeodesicMetric {
    metric_with_flattening(1.0 / 298.257_223_563)
}

pub(super) fn ring(points: &[(f64, f64)]) -> Ring {
    Ring::closed(
        points
            .iter()
            .map(|&(x, y)| Point::new_raw_xy(x, y))
            .collect(),
    )
    .unwrap()
}

pub(super) fn polygon(shell: &[(f64, f64)], holes: &[&[(f64, f64)]]) -> Polygon {
    Polygon::new(ring(shell), holes.iter().map(|hole| ring(hole)).collect())
}

pub(super) fn chart_polygon_area(
    metric: &impl ChartMetric,
    polygon: &Polygon,
    frame: GeographicFrame,
) -> crate::error::Result<f64> {
    crate::geometry::geographic_polygon_area(metric, polygon, frame)
}

pub(super) fn perimeter(metric: &impl ChartMetric, shape: &Shape, frame: GeographicFrame) -> f64 {
    crate::geometry::geographic_polygonal_perimeter(metric, shape, frame, None).unwrap()
}

pub(super) fn analytic_parallel_circumference(metric: &GeodesicMetric, latitude: f64) -> f64 {
    let geodesic = metric.geodesic.as_ref();
    let latitude = latitude.to_radians();
    let eccentricity_squared = geodesic.f * (2.0 - geodesic.f);
    std::f64::consts::TAU * geodesic.a * latitude.cos()
        / (1.0 - eccentricity_squared * latitude.sin().powi(2)).sqrt()
}

pub(super) fn raw_stored_perimeter(metric: &impl ChartMetric, shape: &Shape) -> f64 {
    let polygons: Vec<&Polygon> = match shape {
        Shape::Polygon(polygon) => vec![polygon],
        Shape::MultiPolygon(polygons) => polygons.iter().collect(),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => panic!("raw perimeter fixture is polygonal"),
    };
    let mut sum = 0.0;
    let mut correction = 0.0;
    for polygon in polygons {
        for ring in polygon.rings() {
            for [start, end] in ring.segment_pairs() {
                let edge = metric.chart_edge(start, end).unwrap();
                let adjusted = metric.chart_edge_length(&edge).unwrap() - correction;
                let next = sum + adjusted;
                correction = (next - sum) - adjusted;
                sum = next;
            }
        }
    }
    sum
}

pub(super) fn multi_polygon(polygons: Vec<Polygon>) -> Shape {
    Shape::MultiPolygon(MultiPolygon::from_dense(polygons).unwrap())
}

pub(super) fn north_closure(longitude: f64, latitude: f64, hop: bool) -> Vec<(f64, f64)> {
    let mut points = (0..=4)
        .map(|index| (longitude + 90.0 * f64::from(index), latitude))
        .collect::<Vec<_>>();
    points.push((longitude + 360.0, 90.0));
    if hop {
        points.push((longitude, 90.0));
    } else {
        points.extend(
            (0..4)
                .rev()
                .map(|index| (longitude + 90.0 * f64::from(index), 90.0)),
        );
    }
    points
}

pub(super) fn south_closure(longitude: f64, latitude: f64, hop: bool) -> Vec<(f64, f64)> {
    north_closure(longitude, latitude, hop)
        .into_iter()
        .map(|(longitude, latitude)| (longitude, -latitude))
        .collect()
}

pub(super) fn oblique_north_closure(longitude: f64, latitude: f64, noded: bool) -> Vec<(f64, f64)> {
    let mut points = vec![
        (longitude, latitude),
        (longitude + 90.0, latitude),
        (longitude + 180.0, latitude),
        (longitude + 270.0, latitude),
        (longitude + 360.0, latitude),
        (longitude + 450.0, 90.0),
        (longitude + 360.0, 90.0),
        (longitude + 270.0, 90.0),
        (longitude + 180.0, 90.0),
        (longitude + 90.0, 90.0),
    ];
    if noded {
        points.push((longitude + 45.0, f64::midpoint(latitude, 90.0)));
    }
    points
}

pub(super) fn full_longitude_band(longitude: f64, south: f64, north: f64) -> Vec<(f64, f64)> {
    let mut points = (0..=4)
        .map(|index| (longitude + 90.0 * f64::from(index), south))
        .collect::<Vec<_>>();
    points.extend(
        (0..=4)
            .rev()
            .map(|index| (longitude + 90.0 * f64::from(index), north)),
    );
    points
}

pub(super) fn arbitrary_cut_closure(noded: bool) -> Vec<(f64, f64)> {
    let ascent = [(360.0, 60.0), (400.0, 70.0), (380.0, 80.0), (400.0, 90.0)];
    let mut points = (0..=4)
        .map(|index| (90.0 * f64::from(index), 60.0))
        .collect::<Vec<_>>();
    points.extend(ascent.into_iter().skip(1));
    points.extend([(310.0, 90.0), (220.0, 90.0), (130.0, 90.0), (40.0, 90.0)]);
    points.extend([(20.0, 80.0), (40.0, 70.0)]);
    if noded {
        let insertion = points
            .len()
            .checked_sub(1)
            .expect("the descending cut has a final edge");
        points.insert(insertion, (30.0, 75.0));
    }
    points
}

pub(super) fn box_polygon(x0: f64, y0: f64, x1: f64, y1: f64) -> Polygon {
    polygon(&[(x0, y0), (x1, y0), (x1, y1), (x0, y1)], &[])
}
