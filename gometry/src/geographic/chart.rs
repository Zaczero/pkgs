use std::cmp::Ordering;

use super::{
    CanonicalLongitude, CircularInterval, ComputedEdgeIntent, ComputedVertexIntent,
    GeographicFrame, GeographicSupportProducts, RetainedChartDisplacement, RetainedLongitudeSite,
    publish_computed_chain_with_vertex_intents,
};
use crate::error::Result;
use crate::geometry::{
    Bounds, CoordSeq, ExpansionBudget, LineSeq, Point, PointKey, Polygon, Ring, Shape, exact_arith,
};

/// A point-site identity retained separately from its rounded working X.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ChartPointSite {
    longitude: RetainedLongitudeSite,
    source_y: f64,
    source_ordinal: usize,
}

impl ChartPointSite {
    pub(crate) const fn longitude(self) -> RetainedLongitudeSite {
        self.longitude
    }

    pub(crate) const fn source_ordinal(self) -> usize {
        self.source_ordinal
    }

    pub(crate) const fn source_y(self) -> f64 {
        self.source_y
    }

    /// The west cut assigns only zero or one period; the canonical seam assigns zero.
    pub(crate) fn displacement_to(self, end: Self) -> RetainedChartDisplacement {
        RetainedChartDisplacement::from_native_endpoints(
            [*self.longitude.native(), *end.longitude.native()],
            end.longitude
                .periods()
                .wrapping_sub(self.longitude.periods()),
        )
    }

    pub(crate) fn compare_chart_xy(self, other: Self) -> Ordering {
        self.longitude
            .periods()
            .cmp(&other.longitude.periods())
            .then_with(|| {
                self.longitude
                    .native()
                    .get()
                    .total_cmp(&other.longitude.native().get())
            })
            .then_with(|| self.source_y.total_cmp(&other.source_y))
    }
}

#[derive(Clone, Copy, Debug)]
enum SelectionCut {
    West(CanonicalLongitude),
    CanonicalSeam,
}

/// One operation-local lift for an admitted point-set geometry.
///
/// The support owner chooses the cut; every site keeps its native longitude
/// and integer lift so rounded chart coordinates never become site identity.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PointSelectionChart {
    frame: GeographicFrame,
    cut: SelectionCut,
}

/// Selected source chain before 303 canonical writing.
///
/// The edge inventory deliberately retains authentic endpoints and integral
/// lifts; it is not a parallel publisher or a cache.
enum ChartedPointChain {
    SourcePoint(Point),
    Chain {
        coordinates: CoordSeq,
        edges: Vec<RetainedChartDisplacement>,
        closed_ring: bool,
    },
}

enum ChartedPointRectangle {
    Source(ChartedPointChain),
    ComputedRing { coordinates: CoordSeq },
}

impl PointSelectionChart {
    pub(crate) fn accepts_point_set(shape: &Shape) -> bool {
        shape.point_set_only()
    }

    pub(crate) fn try_from_shape(
        frame: GeographicFrame,
        shape: &Shape,
        raw_extrema: Option<Bounds>,
    ) -> Result<Option<Self>> {
        if raw_extrema.is_some_and(|bounds| frame.bounds_certify_ordinary_support(bounds)) {
            return Ok(None);
        }
        let products =
            GeographicSupportProducts::try_from_shape_with_bounds(frame, shape, raw_extrema)?;
        Ok(Self::from_products(frame, products))
    }

    pub(crate) fn try_from_points(
        frame: GeographicFrame,
        points: &[Point],
    ) -> Result<Option<Self>> {
        let products = GeographicSupportProducts::try_from_points(frame, points.iter().copied())?;
        Ok(Self::from_products(frame, products))
    }

    fn from_products(frame: GeographicFrame, products: GeographicSupportProducts) -> Option<Self> {
        let cut = match products.support.longitude {
            CircularInterval::Empty => return None,
            CircularInterval::Arc { start, .. } => SelectionCut::West(start),
            CircularInterval::Full => SelectionCut::CanonicalSeam,
        };
        Some(Self { frame, cut })
    }

    pub(crate) const fn frame(self) -> GeographicFrame {
        self.frame
    }

    pub(crate) fn site(self, source: Point, source_ordinal: usize) -> Result<ChartPointSite> {
        Ok(ChartPointSite {
            longitude: self.longitude_site(source.x)?,
            source_y: source.y,
            source_ordinal,
        })
    }

    pub(crate) fn longitude_site(self, source_x: f64) -> Result<RetainedLongitudeSite> {
        let axis = self.frame.longitude();
        let native = axis.canonicalize(source_x).map_err(|error| {
            crate::geometry::GeometryErrorKind::Invalid(error.to_string().into())
        })?;
        let periods = match self.cut {
            SelectionCut::West(west) => i64::from(native.get() < west.get()),
            SelectionCut::CanonicalSeam => 0,
        };
        Ok(RetainedLongitudeSite::new(native, periods))
    }

    pub(crate) fn point_cloud(self, shape: &Shape) -> Result<(Vec<Point>, Vec<ChartPointSite>)> {
        let points = shape.first_seen_xy_vertices()?;
        let sites = self.sites_from_points(&points)?;
        Ok((points, sites))
    }

    pub(crate) fn sites_from_points(self, points: &[Point]) -> Result<Vec<ChartPointSite>> {
        let mut sites = Vec::with_capacity(points.len());
        let mut seen = crate::collections::try_hash_set_with_capacity(points.len())?;
        for (source_ordinal, &point) in points.iter().enumerate() {
            let site = self.site(point, source_ordinal)?;
            let key = PointKey::new(Point::new_raw_xy(site.longitude.native().get(), point.y));
            if seen.insert(key) {
                sites.push(site);
            }
        }
        Ok(sites)
    }
}

pub(crate) fn point_set_centroid(shape: &Shape, frame: GeographicFrame) -> Result<Option<Shape>> {
    if matches!(shape, Shape::Point(_) | Shape::Empty(..))
        || !PointSelectionChart::accepts_point_set(shape)
    {
        return Ok(None);
    }
    let (source_mean, raw_extrema) = crate::geometry::point_set_centroid_with_bounds(shape)?;
    let Some(source_mean) = source_mean else {
        return Ok(None);
    };
    if raw_extrema.is_some_and(|bounds| frame.bounds_certify_ordinary_support(bounds)) {
        let point = frame
            .canonical_computed_point(Point::new_raw_xy(source_mean.0, source_mean.1))
            .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))?;
        return Ok(Some(Shape::Point(point)));
    }
    let Some(chart) = PointSelectionChart::try_from_shape(frame, shape, raw_extrema)? else {
        return Ok(None);
    };
    let Some((x, y)) = crate::geometry::exact_point_chart_centroid(shape, chart)? else {
        return Ok(None);
    };
    let point = frame
        .canonical_computed_point(Point::new_raw_xy(x, y))
        .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))?;
    Ok(Some(Shape::Point(point)))
}

pub(crate) fn point_set_surface(shape: &Shape, frame: GeographicFrame) -> Result<Option<Shape>> {
    if matches!(shape, Shape::Point(_) | Shape::Empty(..))
        || !PointSelectionChart::accepts_point_set(shape)
    {
        return Ok(None);
    }
    let (source_mean, raw_extrema) = crate::geometry::point_set_centroid_with_bounds(shape)?;
    let Some(source_mean) = source_mean else {
        return Ok(None);
    };
    if raw_extrema.is_some_and(|bounds| frame.bounds_certify_ordinary_support(bounds)) {
        let center = Point::new_raw_xy(source_mean.0, source_mean.1);
        return Ok(crate::geometry::point_set_surface_point_at(shape, center).map(Shape::Point));
    }
    let Some(chart) = PointSelectionChart::try_from_shape(frame, shape, raw_extrema)? else {
        return Ok(None);
    };
    let Some((center_x, center_y)) = crate::geometry::exact_point_chart_centroid(shape, chart)?
    else {
        return Ok(None);
    };
    let center = Point::new_raw_xy(center_x, center_y);
    let (points, sites) = chart.point_cloud(shape)?;
    let Some(mut best) = sites.first().copied() else {
        return Ok(None);
    };
    for &candidate in sites.iter().skip(1) {
        let order = exact_arith::retained_site_distance_order(
            frame.longitude(),
            (
                candidate.longitude(),
                points
                    .get(candidate.source_ordinal())
                    .expect("site ordinal refers to its source point")
                    .y,
            ),
            (
                best.longitude(),
                points
                    .get(best.source_ordinal())
                    .expect("site ordinal refers to its source point")
                    .y,
            ),
            center,
        )?;
        if order.is_lt() {
            best = candidate;
        }
    }
    let source = *points
        .get(best.source_ordinal())
        .expect("selected site ordinal refers to its source point");
    Ok(Some(Shape::Point(Point::new_raw_xy(source.x, source.y))))
}

fn point_set_convex_hull(
    shape: &Shape,
    frame: GeographicFrame,
    raw_extrema: Option<Bounds>,
) -> Result<Option<ChartedPointChain>> {
    if matches!(shape, Shape::Point(_) | Shape::Empty(..))
        || !PointSelectionChart::accepts_point_set(shape)
    {
        return Ok(None);
    }
    if raw_extrema.is_some_and(|bounds| frame.bounds_certify_ordinary_support(bounds)) {
        return Ok(None);
    }
    let points = shape.first_seen_xy_vertices()?;
    if points.is_empty() {
        return Ok(None);
    }
    let Some(chart) = PointSelectionChart::try_from_points(frame, &points)? else {
        return Ok(None);
    };
    let sites = chart.sites_from_points(&points)?;
    let hull = crate::geometry::monotone_chain_chart_hull(&sites, frame)?;
    charted_point_chain(&points, &sites, &hull).map(Some)
}

fn point_set_minimum_rotated_rectangle(
    shape: &Shape,
    frame: GeographicFrame,
    raw_extrema: Option<Bounds>,
) -> Result<Option<ChartedPointRectangle>> {
    if matches!(shape, Shape::Point(_) | Shape::Empty(..))
        || !PointSelectionChart::accepts_point_set(shape)
    {
        return Ok(None);
    }
    if raw_extrema.is_some_and(|bounds| frame.bounds_certify_ordinary_support(bounds)) {
        return Ok(None);
    }
    let points = shape.first_seen_xy_vertices()?;
    if points.is_empty() {
        return Ok(None);
    }
    let Some(chart) = PointSelectionChart::try_from_points(frame, &points)? else {
        return Ok(None);
    };
    let sites = chart.sites_from_points(&points)?;
    let hull = crate::geometry::monotone_chain_chart_hull(&sites, frame)?;
    if hull.len() < 3 {
        return Ok(Some(ChartedPointRectangle::Source(charted_point_chain(
            &points, &sites, &hull,
        )?)));
    }
    let hull_sites = hull
        .iter()
        .map(|&index| {
            *sites
                .get(index)
                .expect("hull ordinal belongs to chart sites")
        })
        .collect::<Vec<_>>();
    let coordinates = crate::geometry::minimum_area_rectangle_chart(&hull_sites, frame)?;
    Ok(Some(ChartedPointRectangle::ComputedRing { coordinates }))
}

pub(crate) fn point_set_convex_hull_shape(
    shape: &Shape,
    frame: GeographicFrame,
    raw_extrema: Option<Bounds>,
) -> Result<Option<Shape>> {
    let Some(chain) = point_set_convex_hull(shape, frame, raw_extrema)? else {
        return Ok(None);
    };
    Ok(Some(publish_charted_point_chain(
        chain,
        frame,
        "convex_hull",
    )?))
}

pub(crate) fn point_set_minimum_rotated_rectangle_shape(
    shape: &Shape,
    frame: GeographicFrame,
    raw_extrema: Option<Bounds>,
) -> Result<Option<Shape>> {
    let Some(rectangle) = point_set_minimum_rotated_rectangle(shape, frame, raw_extrema)? else {
        return Ok(None);
    };
    let shape = match rectangle {
        ChartedPointRectangle::Source(chain) => {
            publish_charted_point_chain(chain, frame, "minimum_rotated_rectangle")?
        },
        // `minimum_area_rectangle_chart` has already used the shared publisher
        // and certified its actual emitted boundary.
        ChartedPointRectangle::ComputedRing { coordinates } => Shape::Polygon(Polygon::new(
            Ring::try_from_closed(coordinates)?,
            Vec::new(),
        )),
    };
    Ok(Some(shape))
}

fn publish_charted_point_chain(
    chain: ChartedPointChain,
    frame: GeographicFrame,
    operation: &'static str,
) -> Result<Shape> {
    match chain {
        ChartedPointChain::SourcePoint(point) => Ok(Shape::Point(point)),
        ChartedPointChain::Chain {
            coordinates,
            edges,
            closed_ring,
        } => {
            debug_assert_eq!(edges.len(), coordinates.len().saturating_sub(1));
            let mut budget = ExpansionBudget::new(operation, "computed edges");
            let published = publish_computed_chain_with_vertex_intents(
                frame,
                &coordinates,
                closed_ring,
                &mut budget,
                |index, _, _| {
                    Ok(ComputedEdgeIntent::retained_native(
                        *edges
                            .get(index)
                            .expect("each selected source edge has one retained relation"),
                    ))
                },
                |_, _| ComputedVertexIntent::Canonical,
            )?;
            let coordinates = match published {
                Some(points) => CoordSeq::try_from_points(&points)?,
                None => coordinates,
            };
            if closed_ring {
                Ok(Shape::Polygon(Polygon::new(
                    Ring::try_from_closed(coordinates)?,
                    Vec::new(),
                )))
            } else {
                Ok(Shape::LineString(LineSeq::from_proven(coordinates)))
            }
        },
    }
}

fn charted_point_chain(
    source_points: &[Point],
    sites: &[ChartPointSite],
    indices: &[usize],
) -> Result<ChartedPointChain> {
    match indices {
        [] => Ok(ChartedPointChain::Chain {
            coordinates: CoordSeq::try_from_points(&[])?,
            edges: Vec::new(),
            closed_ring: false,
        }),
        [index] => {
            let site = sites
                .get(*index)
                .expect("point hull ordinal belongs to sites");
            Ok(ChartedPointChain::SourcePoint(
                *source_points
                    .get(site.source_ordinal())
                    .expect("site ordinal belongs to source points"),
            ))
        },
        _ => {
            let closed_ring = indices.len() >= 3;
            let mut source_chain =
                Vec::with_capacity(indices.len().saturating_add(usize::from(closed_ring)));
            for &index in indices {
                let site = sites
                    .get(index)
                    .expect("hull ordinal belongs to chart sites");
                source_chain.push(
                    *source_points
                        .get(site.source_ordinal())
                        .expect("site ordinal belongs to source points"),
                );
            }
            if closed_ring {
                source_chain.push(*source_chain.first().expect("closed hull has a first site"));
            }
            let coordinates = CoordSeq::try_from_points(&source_chain)?;
            let mut edges = Vec::with_capacity(source_chain.len().saturating_sub(1));
            for pair in indices.windows(2) {
                let [start, end] = pair else {
                    unreachable!("windows(2) yields two hull ordinals");
                };
                let start = *sites
                    .get(*start)
                    .expect("hull start belongs to chart sites");
                let end = *sites.get(*end).expect("hull end belongs to chart sites");
                edges.push(start.displacement_to(end));
            }
            if closed_ring {
                let start = *sites
                    .get(*indices.last().expect("closed hull has a last site"))
                    .expect("hull closure start belongs to chart sites");
                let end = *sites
                    .get(*indices.first().expect("closed hull has a first site"))
                    .expect("hull closure end belongs to chart sites");
                edges.push(start.displacement_to(end));
            }
            Ok(ChartedPointChain::Chain {
                coordinates,
                edges,
                closed_ring,
            })
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ChartedPointChain, ChartedPointRectangle, PointSelectionChart, point_set_centroid,
        point_set_convex_hull, point_set_minimum_rotated_rectangle, point_set_surface,
    };
    use crate::geographic::GeographicFrame;
    use crate::geometry::{
        CoordSeq, CoordinateAxes, Coordinates as _, MOrdinate, Point, Polygon, Ring, Shape,
        ZOrdinate,
    };

    fn point(x: f64, y: f64) -> Point {
        Point::new_raw_xy(x, y)
    }

    fn multipoint(points: &[Point], axes: CoordinateAxes) -> Shape {
        Shape::multi_point(
            axes,
            CoordSeq::try_from_points(points).expect("point sequence"),
        )
    }

    fn chart_sites(
        shape: &Shape,
        frame: GeographicFrame,
    ) -> (Vec<Point>, PointSelectionChart, Vec<super::ChartPointSite>) {
        let bounds = shape.bounds();
        let points = shape.first_seen_xy_vertices().expect("source point sites");
        let chart = PointSelectionChart::try_from_shape(frame, shape, bounds)
            .expect("chart support")
            .expect("nonempty exceptional point support");
        let sites = chart
            .sites_from_points(&points)
            .expect("retained point sites");
        (points, chart, sites)
    }

    #[test]
    fn longitude_aliases_share_one_centroid_and_first_source_representative() {
        let shape = multipoint(&[point(10.0, 0.0), point(370.0, 0.0)], CoordinateAxes::XY);
        let frame = GeographicFrame::crs84();

        let Some(Shape::Point(centroid)) =
            point_set_centroid(&shape, frame).expect("point-set centroid")
        else {
            panic!("aggregate point centroid is a point");
        };
        assert_eq!((centroid.x, centroid.y), (10.0, 0.0));

        let Some(Shape::Point(representative)) =
            point_set_surface(&shape, frame).expect("point-set representative")
        else {
            panic!("point-set representative is a point");
        };
        assert_eq!((representative.x, representative.y), (10.0, 0.0));
    }

    #[test]
    fn alias_collapsed_hull_keeps_the_first_source_record() {
        let first = Point::new_axes(10.0, 0.0, ZOrdinate(Some(7.0)), MOrdinate(None))
            .expect("first XYZ point");
        let alias = Point::new_axes(370.0, 0.0, ZOrdinate(Some(11.0)), MOrdinate(None))
            .expect("aliased XYZ point");
        let shape = multipoint(&[first, alias], CoordinateAxes::XYZ);
        let result = point_set_convex_hull(&shape, GeographicFrame::crs84(), shape.bounds())
            .expect("point-set hull")
            .expect("exceptional point chart");

        let ChartedPointChain::SourcePoint(point) = result else {
            panic!("quotient-identical sites reduce to their first source point");
        };
        assert_eq!((point.x, point.y), (10.0, 0.0));
        assert_eq!(point.z(), Some(7.0));
    }

    #[test]
    fn alias_collapsed_rectangle_keeps_the_first_source_record() {
        let first = Point::new_axes(10.0, 0.0, ZOrdinate(Some(7.0)), MOrdinate(None))
            .expect("first XYZ point");
        let alias = Point::new_axes(370.0, 0.0, ZOrdinate(Some(11.0)), MOrdinate(None))
            .expect("aliased XYZ point");
        let shape = multipoint(&[first, alias], CoordinateAxes::XYZ);
        let result =
            point_set_minimum_rotated_rectangle(&shape, GeographicFrame::crs84(), shape.bounds())
                .expect("point-set rectangle")
                .expect("exceptional point chart");

        let ChartedPointRectangle::Source(ChartedPointChain::SourcePoint(point)) = result else {
            panic!("quotient-identical sites reduce to their first source point");
        };
        assert_eq!((point.x, point.y), (10.0, 0.0));
        assert_eq!(point.z(), Some(7.0));
    }

    #[test]
    fn crossing_hull_retains_the_directed_source_site_displacement() {
        let shape = multipoint(
            &[point(170.0, 0.0), point(-170.0, 10.0)],
            CoordinateAxes::XY,
        );
        let frame = GeographicFrame::crs84();
        let Some(Shape::Point(centroid)) =
            point_set_centroid(&shape, frame).expect("point-set centroid")
        else {
            panic!("aggregate point centroid is a point");
        };
        assert_eq!((centroid.x, centroid.y), (-180.0, 5.0));

        let Some(Shape::Point(representative)) =
            point_set_surface(&shape, frame).expect("point-set representative")
        else {
            panic!("point-set representative is a point");
        };
        assert_eq!((representative.x, representative.y), (170.0, 0.0));

        let result = point_set_convex_hull(&shape, frame, shape.bounds())
            .expect("point-set hull")
            .expect("chart");
        let ChartedPointChain::Chain {
            coordinates,
            edges,
            closed_ring,
        } = result
        else {
            panic!("two distinct source sites form a line");
        };
        assert_eq!(coordinates.iter_coords().collect::<Vec<_>>(), [
            point(170.0, 0.0),
            point(-170.0, 10.0)
        ]);
        assert!(!closed_ring);
        assert_eq!(edges.len(), 1);
        let [start, end] = *edges[0].endpoints();
        assert_eq!((start.get(), end.get()), (170.0, -170.0));
        assert_eq!(edges[0].periods(), 1);
    }

    #[test]
    fn adjacent_native_sites_remain_distinct_after_period_translation() {
        let points = [
            point(-170.0, 1.0),
            point(-100.0, 1.0),
            point(0.2, 1.0),
            point(0.20000000000000004, 1.0),
            point(170.0, 1.0),
        ];
        let shape = multipoint(&points, CoordinateAxes::XY);
        let frame = GeographicFrame::crs84();
        let (_, _chart, sites) = chart_sites(&shape, frame);
        let axis = frame.longitude();
        let first_lift = axis
            .lifted_site_longitude(&sites[2].longitude())
            .expect("first translated site");
        let second_lift = axis
            .lifted_site_longitude(&sites[3].longitude())
            .expect("adjacent translated site");
        assert_eq!(first_lift.to_bits(), second_lift.to_bits());
        assert_eq!(
            sites[2].compare_chart_xy(sites[3]),
            std::cmp::Ordering::Less
        );
        assert_eq!(sites[2].source_ordinal(), 2);
        assert_eq!(sites[3].source_ordinal(), 3);

        let result = point_set_convex_hull(&shape, frame, shape.bounds())
            .expect("point-set hull")
            .expect("chart");
        let ChartedPointChain::Chain { coordinates, .. } = result else {
            panic!("distinct collinear extremes form a line");
        };
        let selected = coordinates.iter_coords().collect::<Vec<_>>();
        assert_eq!(selected, [
            point(170.0, 1.0),
            point(0.20000000000000004, 1.0)
        ]);
    }

    #[test]
    fn primitive_support_of_collection_points_uses_one_covering_chart() {
        let first_row = multipoint(
            &[point(0.0, 0.0), point(120.0, 0.0), point(240.0, 0.0)],
            CoordinateAxes::XY,
        );
        let shape = Shape::geometry_collection(vec![first_row, Shape::Point(point(180.0, 0.0))])
            .expect("point-only collection");
        let frame = GeographicFrame::crs84();
        let (_, _, sites) = chart_sites(&shape, frame);
        let mut ordered = (0..sites.len()).collect::<Vec<_>>();
        ordered.sort_unstable_by(|&left, &right| sites[left].compare_chart_xy(sites[right]));
        let first = sites[*ordered.first().expect("nonempty sites")];
        let last = sites[*ordered.last().expect("nonempty sites")];
        let axis = frame.longitude();
        assert_eq!(
            axis.lifted_site_longitude(&first.longitude())
                .expect("chart west"),
            0.0
        );
        assert_eq!(
            axis.lifted_site_longitude(&last.longitude())
                .expect("chart east"),
            240.0
        );
    }

    #[test]
    fn chart_rectangle_publishes_canonical_coordinates_with_its_seam_lift() {
        let shape = multipoint(
            &[point(170.0, 0.0), point(-170.0, 0.0), point(180.0, 10.0)],
            CoordinateAxes::XY,
        );
        let result =
            point_set_minimum_rotated_rectangle(&shape, GeographicFrame::crs84(), shape.bounds())
                .expect("point-set rectangle")
                .expect("exceptional point chart");
        let ChartedPointRectangle::ComputedRing { coordinates } = result else {
            panic!("three noncollinear chart sites produce a rectangle ring");
        };
        let corners = coordinates.iter_coords().collect::<Vec<_>>();
        assert_eq!(corners.len(), 5);
        assert_eq!(corners, [
            point(170.0, 0.0),
            point(-170.0, 0.0),
            point(-170.0, 10.0),
            point(170.0, 10.0),
            point(170.0, 0.0),
        ]);
        let axis = GeographicFrame::crs84().longitude();
        assert_eq!(
            axis.chart_step(corners[0].x, corners[1].x)
                .unwrap()
                .delta(axis),
            20.0
        );
    }

    #[test]
    fn shared_point_collection_occurrences_change_mass_not_site_selection() {
        let repeated_point = Shape::Point(point(170.0, 0.0));
        let mut repeated = repeated_point;
        for _ in 0..127 {
            repeated = Shape::geometry_collection(vec![repeated.clone(), repeated])
                .expect("shared point collection");
        }
        let shape = Shape::geometry_collection(vec![repeated, Shape::Point(point(-170.0, 10.0))])
            .expect("point-only collection");
        let Some(Shape::Point(centroid)) =
            point_set_centroid(&shape, GeographicFrame::crs84()).expect("point-set centroid")
        else {
            panic!("shared point collection centroid is a point");
        };
        assert_eq!(centroid.x, 170.0);
        assert_eq!(centroid.y.to_bits(), (10.0 / 2.0_f64.powi(127)).to_bits());

        let result = point_set_convex_hull(&shape, GeographicFrame::crs84(), shape.bounds())
            .expect("point-set hull")
            .expect("exceptional point chart");
        let ChartedPointChain::Chain {
            coordinates,
            edges,
            closed_ring,
        } = result
        else {
            panic!("the distinct support sites form a line");
        };
        assert_eq!(coordinates.len(), 2);
        assert_eq!(edges.len(), 1);
        assert!(!closed_ring);
    }

    #[test]
    fn empty_members_do_not_reclassify_point_collections_but_degenerate_areas_do() {
        let point_with_empty_area = Shape::geometry_collection(vec![
            Shape::Point(point(10.0, 0.0)),
            Shape::empty_polygon(),
        ])
        .expect("point-only collection with an empty polygon");
        assert!(PointSelectionChart::accepts_point_set(
            &point_with_empty_area
        ));

        let Some(Shape::Point(centroid)) =
            point_set_centroid(&point_with_empty_area, GeographicFrame::crs84())
                .expect("point-set centroid")
        else {
            panic!("the nonempty point member supplies the centroid");
        };
        assert_eq!((centroid.x, centroid.y), (10.0, 0.0));

        let degenerate_area = Shape::Polygon(Polygon::new(
            Ring::try_from_closed(
                CoordSeq::try_from_points(&[
                    point(0.0, 0.0),
                    point(1.0, 0.0),
                    point(2.0, 0.0),
                    point(0.0, 0.0),
                ])
                .expect("degenerate shell coordinates"),
            )
            .expect("closed degenerate shell"),
            Vec::new(),
        ));
        let point_with_degenerate_area =
            Shape::geometry_collection(vec![Shape::Point(point(10.0, 0.0)), degenerate_area])
                .expect("point and degenerate-area collection");
        assert!(!PointSelectionChart::accepts_point_set(
            &point_with_degenerate_area
        ));
    }
}
