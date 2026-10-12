//! Raw and retained coordinate identity for constrained triangulation.

use std::cmp::Ordering;

use rstar::AABB;

use super::super::exact_delaunay::MeshCoordinates;
use super::{CdtCoordinateModel, CdtCoordinates, CdtKey, CdtPosition, CdtQuery};
use crate::error::Result;
use crate::geographic::{ChartPointSite, GeographicFrame};
use crate::geometry::exact_arith::{ExactSign, retained_site_longitude};
use crate::geometry::tessellation::exact;
use crate::geometry::{GeometryErrorKind, Point, XY};

impl CdtPosition {
    fn raw(point: Point) -> Self {
        Self {
            exact: [
                exact::ExactDyadic::from_f64(point.x),
                exact::ExactDyadic::from_f64(point.y),
            ],
            proposal: point.xy(),
            retained: None,
        }
    }

    fn retained(frame: GeographicFrame, site: ChartPointSite) -> Result<Self> {
        let exact_x = retained_site_longitude(frame.longitude(), site.longitude())?;
        let native_x = site.longitude().native().get();
        let chart_x = native_x + frame.longitude().period() * site.longitude().periods() as f64;
        let proposal_x = if chart_x.is_finite() {
            chart_x
        } else {
            native_x
        };
        Ok(Self {
            exact: [exact_x, exact::ExactDyadic::from_f64(site.source_y())],
            // The proposal may round distinct retained sites together; predicates
            // and site identity use `exact`, while overflow only degrades locality.
            proposal: XY::new(proposal_x, site.source_y()),
            retained: Some(site),
        })
    }

    fn from_point(point: XY) -> Self {
        Self {
            exact: [
                exact::ExactDyadic::from_f64(point.x),
                exact::ExactDyadic::from_f64(point.y),
            ],
            proposal: point,
            retained: None,
        }
    }
}

impl CdtCoordinates {
    pub(in crate::geometry::tessellation::delaunay) fn raw(
        sites: &[Point],
        source_occurrences: &[Point],
    ) -> Self {
        Self {
            sites: sites.iter().copied().map(CdtPosition::raw).collect(),
            source_occurrences: source_occurrences
                .iter()
                .copied()
                .map(CdtPosition::raw)
                .collect(),
            model: CdtCoordinateModel::Raw,
        }
    }

    pub(in crate::geometry::tessellation::delaunay) fn retained(
        frame: GeographicFrame,
        sites: &[ChartPointSite],
        source_occurrences: &[ChartPointSite],
    ) -> Result<Self> {
        Ok(Self {
            sites: sites
                .iter()
                .copied()
                .map(|site| CdtPosition::retained(frame, site))
                .collect::<Result<_>>()?,
            source_occurrences: source_occurrences
                .iter()
                .copied()
                .map(|site| CdtPosition::retained(frame, site))
                .collect::<Result<_>>()?,
            model: CdtCoordinateModel::Retained,
        })
    }

    pub(in crate::geometry::tessellation::delaunay) fn push_generated(
        &mut self,
        point: XY,
    ) -> usize {
        let site = self.sites.len();
        self.sites.push(CdtPosition::from_point(point));
        site
    }

    pub(super) fn reserve_generated(&mut self) {
        self.sites.reserve(1);
    }

    pub(super) const fn point_query(&self, point: XY) -> CdtQuery {
        CdtQuery::Point(point)
    }

    pub(super) const fn source_query(&self, occurrence: usize) -> CdtQuery {
        CdtQuery::Source(occurrence)
    }

    pub(super) const fn site_query(&self, site: usize) -> CdtQuery {
        CdtQuery::Site(site)
    }

    pub(in crate::geometry::tessellation::delaunay) fn site_xy(&self, site: usize) -> XY {
        self.sites
            .get(site)
            .expect("CDT site belongs to its coordinate table")
            .proposal
    }

    pub(in crate::geometry::tessellation::delaunay) fn source_xy(&self, occurrence: usize) -> XY {
        self.source_occurrences
            .get(occurrence)
            .expect("CDT source occurrence belongs to its coordinate table")
            .proposal
    }

    pub(super) fn retained_site_location(&self, site: usize) -> Option<ChartPointSite> {
        self.sites
            .get(site)
            .expect("CDT site belongs to its coordinate table")
            .retained
    }

    pub(super) fn retained_source_location(&self, occurrence: usize) -> Option<ChartPointSite> {
        self.source_occurrences
            .get(occurrence)
            .expect("CDT source occurrence belongs to its coordinate table")
            .retained
    }

    pub(super) fn proposal_xy(&self, query: CdtQuery) -> XY {
        match query {
            CdtQuery::Site(site) => self.site_xy(site),
            CdtQuery::Source(occurrence) => self.source_xy(occurrence),
            CdtQuery::Point(point) => point,
        }
    }

    pub(super) fn between(&self, start: CdtQuery, end: CdtQuery, point: CdtQuery) -> bool {
        let [start_x, start_y] = self.exact_xy(start);
        let [end_x, end_y] = self.exact_xy(end);
        let [point_x, point_y] = self.exact_xy(point);
        let between_axis =
            |start: &exact::ExactDyadic, end: &exact::ExactDyadic, point: &exact::ExactDyadic| {
                let (low, high) = if start.cmp(end).is_le() {
                    (start, end)
                } else {
                    (end, start)
                };
                low.cmp(point).is_le() && point.cmp(high).is_le()
            };
        between_axis(&start_x, &end_x, &point_x)
            && between_axis(&start_y, &end_y, &point_y)
            && (start_x != point_x || start_y != point_y)
            && (end_x != point_x || end_y != point_y)
    }

    pub(super) fn on_segment(
        &self,
        start: CdtQuery,
        end: CdtQuery,
        point: CdtQuery,
    ) -> Result<bool> {
        Ok(self.orient([start, end, point])? == ExactSign::Zero
            && (self.exact_xy(start) == self.exact_xy(point)
                || self.exact_xy(end) == self.exact_xy(point)
                || self.between(start, end, point)))
    }

    pub(super) fn segments_properly_cross(&self, [a, b, c, d]: [CdtQuery; 4]) -> Result<bool> {
        let first_a = self.orient([a, b, c])?;
        let first_b = self.orient([a, b, d])?;
        let second_c = self.orient([c, d, a])?;
        let second_d = self.orient([c, d, b])?;
        Ok(opposite_nonzero(first_a, first_b) && opposite_nonzero(second_c, second_d))
    }

    pub(super) fn interpolate_source_parameter(
        &self,
        lo: &exact::ExactRational,
        hi: &exact::ExactRational,
        start: CdtQuery,
        end: CdtQuery,
        point: CdtQuery,
    ) -> Result<exact::ExactRational> {
        let [start_x, start_y] = self.exact_xy(start);
        let [end_x, end_y] = self.exact_xy(end);
        let [point_x, point_y] = self.exact_xy(point);
        let dx = end_x.try_subtract(start_x.clone())?;
        let (numerator, denominator) = if dx.is_zero() {
            (
                point_y.try_subtract(start_y.clone())?,
                end_y.try_subtract(start_y)?,
            )
        } else {
            (point_x.try_subtract(start_x)?, dx)
        };
        if denominator.is_zero() {
            return Err(GeometryErrorKind::triangulation(
                "source constraint has coincident chart endpoints",
            ));
        }
        let local = exact::ExactRational::new(numerator, denominator)?;
        lo.try_add(&hi.try_subtract(lo)?.try_product(&local)?)
    }

    pub(super) fn exact_xy(&self, query: CdtQuery) -> [exact::ExactDyadic; 2] {
        match query {
            CdtQuery::Site(site) => self
                .sites
                .get(site)
                .expect("CDT mesh site belongs to its coordinate table")
                .exact
                .clone(),
            CdtQuery::Source(occurrence) => self
                .source_occurrences
                .get(occurrence)
                .expect("CDT source query belongs to its occurrence table")
                .exact
                .clone(),
            CdtQuery::Point(point) => [
                exact::ExactDyadic::from_f64(point.x),
                exact::ExactDyadic::from_f64(point.y),
            ],
        }
    }

    pub(super) fn compare_query_x(&self, left: CdtQuery, right: CdtQuery) -> std::cmp::Ordering {
        let [left_x, _] = self.exact_xy(left);
        let [right_x, _] = self.exact_xy(right);
        left_x.cmp(&right_x)
    }

    pub(super) fn compare_query_y(&self, left: CdtQuery, right: CdtQuery) -> std::cmp::Ordering {
        let [_, left_y] = self.exact_xy(left);
        let [_, right_y] = self.exact_xy(right);
        left_y.cmp(&right_y)
    }

    pub(super) fn metric_frame(&self, [a, b, c]: [usize; 3]) -> Result<[XY; 3]> {
        if self.model == CdtCoordinateModel::Raw {
            return Ok([self.site_xy(a), self.site_xy(b), self.site_xy(c)]);
        }
        let [origin_x, origin_y] = self.exact_xy(self.site_query(a));
        let relative = |site| -> Result<XY> {
            let [point_x, point_y] = self.exact_xy(self.site_query(site));
            let x = point_x.try_subtract(origin_x.clone())?;
            let y = point_y.try_subtract(origin_y.clone())?;
            Ok(XY::new(
                exact::ExactRational::from_dyadic(x).round()?,
                exact::ExactRational::from_dyadic(y).round()?,
            ))
        };
        Ok([XY::new(0.0, 0.0), relative(b)?, relative(c)?])
    }

    pub(super) fn rounded_midpoint(&self, start: usize, end: usize) -> Result<XY> {
        if self.model == CdtCoordinateModel::Raw {
            let start = self.site_xy(start);
            let end = self.site_xy(end);
            return Ok(XY::new(
                f64::midpoint(start.x, end.x),
                f64::midpoint(start.y, end.y),
            ));
        }
        let [start_x, start_y] = self.exact_xy(self.site_query(start));
        let [end_x, end_y] = self.exact_xy(self.site_query(end));
        let x = start_x
            .try_add(end_x)?
            .try_product(&exact::ExactDyadic::from_f64(0.5))?;
        let y = start_y
            .try_add(end_y)?
            .try_product(&exact::ExactDyadic::from_f64(0.5))?;
        Ok(XY::new(
            exact::ExactRational::from_dyadic(x).round()?,
            exact::ExactRational::from_dyadic(y).round()?,
        ))
    }

    pub(super) fn circumcenter(&self, triangle: [usize; 3]) -> Result<Option<XY>> {
        if self.model == CdtCoordinateModel::Raw {
            let [a, b, c] = triangle.map(|site| self.site_xy(site));
            return exact::circumcenter(a, b, c)?.finite_rounding();
        }
        let [a, b, c] = triangle.map(|site| self.exact_xy(self.site_query(site)));
        exact::circumcenter_dyadic_points(&a, &b, &c)
    }

    pub(super) fn axis_bounds(&self, query: CdtQuery) -> Result<[(f64, f64); 2]> {
        let [x, y] = self.exact_xy(query);
        let (x_low, x_high) = exact::ExactRational::from_dyadic(x).outward_bounds()?;
        let (y_low, y_high) = exact::ExactRational::from_dyadic(y).outward_bounds()?;
        Ok([(x_low, x_high), (y_low, y_high)])
    }

    pub(super) fn envelope(&self, queries: &[CdtQuery]) -> Result<AABB<[f64; 2]>> {
        let Some((&first, rest)) = queries.split_first() else {
            return Err(GeometryErrorKind::triangulation(
                "refinement envelope has no coordinates",
            ));
        };
        let [(mut min_x, mut max_x), (mut min_y, mut max_y)] = self.axis_bounds(first)?;
        for &query in rest {
            let [(low_x, high_x), (low_y, high_y)] = self.axis_bounds(query)?;
            min_x = min_x.min(low_x);
            max_x = max_x.max(high_x);
            min_y = min_y.min(low_y);
            max_y = max_y.max(high_y);
        }
        Ok(AABB::from_corners(
            [min_x.next_down(), min_y.next_down()],
            [max_x.next_up(), max_y.next_up()],
        ))
    }

    pub(super) fn in_diametral_circle(
        &self,
        start: usize,
        end: usize,
        point: CdtQuery,
    ) -> Result<bool> {
        if self.model == CdtCoordinateModel::Raw {
            let start = self.site_xy(start);
            let end = self.site_xy(end);
            let point = self.proposal_xy(point);
            return Ok((start.x - point.x) * (end.x - point.x)
                + (start.y - point.y) * (end.y - point.y)
                < 0.0);
        }
        let [point_x, point_y] = self.exact_xy(point);
        let [start_x, start_y] = self.exact_xy(self.site_query(start));
        let [end_x, end_y] = self.exact_xy(self.site_query(end));
        let left_x = start_x.try_subtract(point_x.clone())?;
        let left_y = start_y.try_subtract(point_y.clone())?;
        let right_x = end_x.try_subtract(point_x)?;
        let right_y = end_y.try_subtract(point_y)?;
        Ok(left_x
            .try_product(&right_x)?
            .try_add(left_y.try_product(&right_y)?)?
            .sign()
            == ExactSign::Negative)
    }

    pub(super) fn triangle_twice_area(&self, [a, b, c]: [usize; 3]) -> Result<exact::ExactDyadic> {
        let [a_x, a_y] = self.exact_xy(self.site_query(a));
        let [b_x, b_y] = self.exact_xy(self.site_query(b));
        let [c_x, c_y] = self.exact_xy(self.site_query(c));
        let ab_x = b_x.try_subtract(a_x.clone())?;
        let ab_y = b_y.try_subtract(a_y.clone())?;
        let ac_x = c_x.try_subtract(a_x)?;
        let ac_y = c_y.try_subtract(a_y)?;
        Ok(ab_x
            .try_product(&ac_y)?
            .try_subtract(ab_y.try_product(&ac_x)?)?
            .abs())
    }

    pub(super) fn triangle_exceeds_area(&self, sites: [usize; 3], max_area: f64) -> Result<bool> {
        if self.model == CdtCoordinateModel::Raw {
            let [a, b, c] = sites.map(|site| self.site_xy(site));
            return Ok(open_xy_cycle_magnitude(&[a, b, c]).get() > max_area);
        }
        let maximum_twice_area = exact::ExactDyadic::from_f64(max_area)
            .try_product(&exact::ExactDyadic::from_f64(2.0))?;
        Ok(self
            .triangle_twice_area(sites)?
            .cmp(&maximum_twice_area)
            .is_gt())
    }
}

impl MeshCoordinates for CdtCoordinates {
    type Query = CdtQuery;
    type Key = CdtKey;

    fn query(&self, site: usize) -> Self::Query {
        CdtQuery::Site(site)
    }

    fn key(&self, query: Self::Query) -> Self::Key {
        CdtKey(self.exact_xy(query))
    }

    fn compare(&self, left: Self::Query, right: Self::Query) -> std::cmp::Ordering {
        if self.model == CdtCoordinateModel::Raw {
            return super::exact_delaunay::compare_xy(
                self.proposal_xy(left),
                self.proposal_xy(right),
            );
        }
        self.compare_query_x(left, right)
            .then_with(|| self.compare_query_y(left, right))
    }

    fn compare_x(&self, left: Self::Query, right: Self::Query) -> std::cmp::Ordering {
        if self.model == CdtCoordinateModel::Raw {
            return super::exact_delaunay::compare_f64(
                self.proposal_xy(left).x,
                self.proposal_xy(right).x,
            );
        }
        self.compare_query_x(left, right)
    }

    fn compare_y(&self, left: Self::Query, right: Self::Query) -> std::cmp::Ordering {
        if self.model == CdtCoordinateModel::Raw {
            return super::exact_delaunay::compare_f64(
                self.proposal_xy(left).y,
                self.proposal_xy(right).y,
            );
        }
        self.compare_query_y(left, right)
    }

    fn orient(&self, [a, b, c]: [Self::Query; 3]) -> Result<ExactSign> {
        if self.model == CdtCoordinateModel::Raw {
            return Ok(exact::orient2d(
                self.proposal_xy(a),
                self.proposal_xy(b),
                self.proposal_xy(c),
            ));
        }
        exact::orient2d_dyadic_points(self.exact_xy(a), self.exact_xy(b), self.exact_xy(c))
    }

    fn incircle(&self, [a, b, c, d]: [Self::Query; 4]) -> Result<ExactSign> {
        if self.model == CdtCoordinateModel::Raw {
            return exact::incircle(
                self.proposal_xy(a),
                self.proposal_xy(b),
                self.proposal_xy(c),
                self.proposal_xy(d),
            );
        }
        exact::incircle_dyadic_points(
            self.exact_xy(a),
            self.exact_xy(b),
            self.exact_xy(c),
            self.exact_xy(d),
        )
    }

    fn compare_axis_spans(
        &self,
        [min_x, max_x]: [Self::Query; 2],
        [min_y, max_y]: [Self::Query; 2],
    ) -> Result<std::cmp::Ordering> {
        if self.model == CdtCoordinateModel::Raw {
            let min_x = self.proposal_xy(min_x);
            let max_x = self.proposal_xy(max_x);
            let min_y = self.proposal_xy(min_y);
            let max_y = self.proposal_xy(max_y);
            return Ok(super::exact_delaunay::compare_f64(
                max_x.x - min_x.x,
                max_y.y - min_y.y,
            ));
        }
        let [max_x, _] = self.exact_xy(max_x);
        let [min_x, _] = self.exact_xy(min_x);
        let [_, max_y] = self.exact_xy(max_y);
        let [_, min_y] = self.exact_xy(min_y);
        let x = max_x.try_subtract(min_x)?.abs();
        let y = max_y.try_subtract(min_y)?.abs();
        Ok(x.cmp(&y))
    }

    fn locality_xy(&self, site: usize) -> XY {
        self.site_xy(site)
    }
}
