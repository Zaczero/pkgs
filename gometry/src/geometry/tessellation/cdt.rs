//! Constrained Delaunay triangulation and bounded Ruppert refinement on the
//! exact mesh.
//!
//! A constraint is forced into a Delaunay mesh by flipping the edges its
//! segment crosses until none remain, every crossing decided by exact
//! `orient2d`; once present it is never flipped again, and a cavity search
//! never crosses it, so later insertions keep it. Faces inside the polygon
//! are found by the same parity flood the old engine used: from the hull
//! inwards, crossing a constraint toggles inside and outside. Refinement is
//! Ruppert's: a retained face that misses the angle or area target has its
//! circumcenter inserted, unless that center encroaches a constraint's
//! diametral circle, in which case the constraint is split at its midpoint
//! instead. A constrained split evaluates the original source segment at an
//! exact parameter and certifies the nearest binary64 point within half an ulp
//! per axis. A center outside binary64 uses the explicit longest-edge midpoint
//! construction; resource and invariant failures remain errors. The vertex
//! budget bounds the loop, and a mesh the budget could not finish is rejected
//! rather than returned as if it met the request.

#![expect(
    clippy::many_single_char_names,
    reason = "a, b, c, d name the vertices of a quad and e, f, t, u its slots, the geometry kernel convention"
)]

use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};

use rstar::{AABB, RTree, RTreeObject};

use super::super::RingOwner;
use super::exact_delaunay::{
    EMPTY, FaceId, FaceSide, HalfedgeId, Located, LocationOutcome, Mesh, MeshCoordinates,
};
use crate::collections::{HashMap, HashSet};
use crate::error::{ErrorKind, Result};
use crate::geographic::{ChartPointSite, GeographicFrame, RetainedLongitudeSite};
use crate::geometry::exact_arith::{ExactSign, retained_site_longitude};
use crate::geometry::publication::{
    ChartRestriction, DirectedChartRestrictionUse, SyntheticChartVertex,
};
use crate::geometry::tessellation::exact;
use crate::geometry::{GeometryErrorKind, Point, XY, open_xy_cycle_magnitude};

#[derive(Clone, Copy, Debug)]
pub(super) enum CdtQuery {
    Site(usize),
    Source(usize),
    Point(XY),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CdtPointLocation {
    Outside,
    Boundary,
    Inside,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::geometry::tessellation) struct CdtKey([exact::ExactDyadic; 2]);

#[derive(Clone, Debug)]
struct CdtPosition {
    exact: [exact::ExactDyadic; 2],
    proposal: XY,
    retained: Option<ChartPointSite>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CdtCoordinateModel {
    Raw,
    Retained,
}

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

    fn retained_generated(
        frame: GeographicFrame,
        longitude: RetainedLongitudeSite,
        latitude: f64,
    ) -> Result<Self> {
        let exact_x = retained_site_longitude(frame.longitude(), longitude)?;
        let native_x = longitude.native().get();
        let chart_x = native_x + frame.longitude().period() * longitude.periods() as f64;
        Ok(Self {
            exact: [exact_x, exact::ExactDyadic::from_f64(latitude)],
            proposal: XY::new(
                if chart_x.is_finite() {
                    chart_x
                } else {
                    native_x
                },
                latitude,
            ),
            retained: None,
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

#[derive(Debug)]
pub(super) struct CdtCoordinates {
    sites: Vec<CdtPosition>,
    source_occurrences: Vec<CdtPosition>,
    model: CdtCoordinateModel,
}

impl CdtCoordinates {
    pub(super) fn raw(sites: &[Point], source_occurrences: &[Point]) -> Self {
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

    #[cfg(test)]
    pub(super) fn retained(
        frame: GeographicFrame,
        sites: &[ChartPointSite],
        source_occurrences: &[ChartPointSite],
    ) -> Result<Self> {
        Self::retained_with_generated(frame, sites, source_occurrences, &[])
    }

    pub(super) fn retained_with_generated(
        frame: GeographicFrame,
        sites: &[ChartPointSite],
        source_occurrences: &[ChartPointSite],
        generated: &[(RetainedLongitudeSite, f64)],
    ) -> Result<Self> {
        let mut site_positions = sites
            .iter()
            .copied()
            .map(|site| CdtPosition::retained(frame, site))
            .collect::<Result<Vec<_>>>()?;
        site_positions.reserve(generated.len());
        for &(longitude, latitude) in generated {
            site_positions.push(CdtPosition::retained_generated(frame, longitude, latitude)?);
        }
        Ok(Self {
            sites: site_positions,
            source_occurrences: source_occurrences
                .iter()
                .copied()
                .map(|site| CdtPosition::retained(frame, site))
                .collect::<Result<_>>()?,
            model: CdtCoordinateModel::Retained,
        })
    }

    pub(super) fn push_generated(&mut self, point: XY) -> usize {
        let site = self.sites.len();
        self.sites.push(CdtPosition::from_point(point));
        site
    }

    fn reserve_generated(&mut self) {
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

    pub(super) fn site_xy(&self, site: usize) -> XY {
        self.sites
            .get(site)
            .expect("CDT site belongs to its coordinate table")
            .proposal
    }

    pub(super) fn source_xy(&self, occurrence: usize) -> XY {
        self.source_occurrences
            .get(occurrence)
            .expect("CDT source occurrence belongs to its coordinate table")
            .proposal
    }

    fn retained_site_location(&self, site: usize) -> Option<ChartPointSite> {
        self.sites
            .get(site)
            .expect("CDT site belongs to its coordinate table")
            .retained
    }

    fn retained_source_location(&self, occurrence: usize) -> Option<ChartPointSite> {
        self.source_occurrences
            .get(occurrence)
            .expect("CDT source occurrence belongs to its coordinate table")
            .retained
    }

    fn proposal_xy(&self, query: CdtQuery) -> XY {
        match query {
            CdtQuery::Site(site) => self.site_xy(site),
            CdtQuery::Source(occurrence) => self.source_xy(occurrence),
            CdtQuery::Point(point) => point,
        }
    }

    fn between(&self, start: CdtQuery, end: CdtQuery, point: CdtQuery) -> bool {
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

    fn on_segment(&self, start: CdtQuery, end: CdtQuery, point: CdtQuery) -> Result<bool> {
        Ok(self.orient([start, end, point])? == ExactSign::Zero
            && (self.exact_xy(start) == self.exact_xy(point)
                || self.exact_xy(end) == self.exact_xy(point)
                || self.between(start, end, point)))
    }

    fn segments_properly_cross(&self, [a, b, c, d]: [CdtQuery; 4]) -> Result<bool> {
        let first_a = self.orient([a, b, c])?;
        let first_b = self.orient([a, b, d])?;
        let second_c = self.orient([c, d, a])?;
        let second_d = self.orient([c, d, b])?;
        Ok(opposite_nonzero(first_a, first_b) && opposite_nonzero(second_c, second_d))
    }

    fn interpolate_source_parameter(
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

    fn exact_xy(&self, query: CdtQuery) -> [exact::ExactDyadic; 2] {
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

    fn compare_query_x(&self, left: CdtQuery, right: CdtQuery) -> std::cmp::Ordering {
        let [left_x, _] = self.exact_xy(left);
        let [right_x, _] = self.exact_xy(right);
        left_x.cmp(&right_x)
    }

    fn compare_query_y(&self, left: CdtQuery, right: CdtQuery) -> std::cmp::Ordering {
        let [_, left_y] = self.exact_xy(left);
        let [_, right_y] = self.exact_xy(right);
        left_y.cmp(&right_y)
    }

    fn metric_frame(&self, [a, b, c]: [usize; 3]) -> Result<[XY; 3]> {
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

    fn rounded_midpoint(&self, start: usize, end: usize) -> Result<XY> {
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

    fn circumcenter(&self, triangle: [usize; 3]) -> Result<Option<XY>> {
        if self.model == CdtCoordinateModel::Raw {
            let [a, b, c] = triangle.map(|site| self.site_xy(site));
            return exact::circumcenter(a, b, c)?.finite_rounding();
        }
        let [a, b, c] = triangle.map(|site| self.exact_xy(self.site_query(site)));
        exact::circumcenter_dyadic_points(&a, &b, &c)
    }

    fn axis_bounds(&self, query: CdtQuery) -> Result<[(f64, f64); 2]> {
        let [x, y] = self.exact_xy(query);
        let (x_low, x_high) = exact::ExactRational::from_dyadic(x).outward_bounds()?;
        let (y_low, y_high) = exact::ExactRational::from_dyadic(y).outward_bounds()?;
        Ok([(x_low, x_high), (y_low, y_high)])
    }

    fn envelope(&self, queries: &[CdtQuery]) -> Result<AABB<[f64; 2]>> {
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

    fn in_diametral_circle(&self, start: usize, end: usize, point: CdtQuery) -> Result<bool> {
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

    fn triangle_twice_area(&self, [a, b, c]: [usize; 3]) -> Result<exact::ExactDyadic> {
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

    fn triangle_exceeds_area(&self, sites: [usize; 3], max_area: f64) -> Result<bool> {
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

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct RefinementWorkCounters {
    pub(super) inside_label_passes: usize,
    pub(super) bad_face_scans: usize,
    pub(super) constraint_reconstruction_passes: usize,
    pub(super) quality_checks: usize,
    pub(super) source_piece_visits: usize,
}

#[cfg(test)]
thread_local! {
    static REFINEMENT_WORK: std::cell::Cell<RefinementWorkCounters> =
        const { std::cell::Cell::new(RefinementWorkCounters {
            inside_label_passes: 0,
            bad_face_scans: 0,
            constraint_reconstruction_passes: 0,
            quality_checks: 0,
            source_piece_visits: 0,
        }) };
}

#[cfg(test)]
fn update_work(update: impl FnOnce(&mut RefinementWorkCounters)) {
    REFINEMENT_WORK.with(|cell| {
        let mut counters = cell.get();
        update(&mut counters);
        cell.set(counters);
    });
}

#[cfg(test)]
pub(super) fn take_refinement_work_counters() -> RefinementWorkCounters {
    REFINEMENT_WORK.with(|cell| cell.replace(RefinementWorkCounters::default()))
}

#[derive(Clone, Debug)]
pub(crate) struct SourceConstraint {
    pub(crate) id: usize,
    pub(crate) start: Point,
    pub(crate) end: Point,
    pub(crate) start_site: usize,
    pub(crate) end_site: usize,
    pub(crate) start_occurrence: usize,
    pub(crate) end_occurrence: usize,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct SourceRingStart {
    first_source: usize,
    owner: RingOwner,
}

impl SourceRingStart {
    pub(super) const fn new(first_source: usize, owner: RingOwner) -> Self {
        Self {
            first_source,
            owner,
        }
    }
}

pub(super) fn source_ring_owner(
    source: usize,
    source_rings: &[SourceRingStart],
) -> Option<RingOwner> {
    let index = source_rings
        .partition_point(|ring| ring.first_source <= source)
        .checked_sub(1)?;
    source_rings.get(index).map(|ring| ring.owner)
}

#[derive(Clone, Debug)]
pub(crate) struct SourcePiece {
    pub(crate) source: usize,
    pub(crate) start_site: usize,
    pub(crate) end_site: usize,
    pub(crate) lo: exact::ExactRational,
    pub(crate) hi: exact::ExactRational,
}

#[derive(Clone, Debug)]
pub(crate) struct RestrictionConstraint {
    pub(crate) id: usize,
    pub(crate) owner: RingOwner,
    pub(crate) start_site: usize,
    pub(crate) end_site: usize,
    pub(crate) use_: DirectedChartRestrictionUse,
}

#[derive(Clone, Debug)]
pub(crate) struct RestrictionPiece {
    pub(crate) restriction: usize,
    pub(crate) owner: RingOwner,
    pub(crate) start_site: usize,
    pub(crate) end_site: usize,
    pub(crate) start: exact::ExactRational,
    pub(crate) end: exact::ExactRational,
    pub(crate) use_: DirectedChartRestrictionUse,
}

#[derive(Clone, Debug)]
struct ConstraintPiece {
    constraint: usize,
    start_site: usize,
    end_site: usize,
    lo: exact::ExactRational,
    hi: exact::ExactRational,
}

/// A rounded boundary point whose root parameter and half-ULP error bound
/// were checked before the point entered the mesh.
#[derive(Clone, Debug)]
struct CertifiedBoundarySplit {
    constraint: usize,
    parameter: exact::ExactRational,
    point: XY,
    half_ulp: [exact::ExactDyadic; 2],
}

#[derive(Debug)]
pub(crate) enum NodeProvenance {
    /// A vertex in the source point slice supplied to the mesh builder.
    SourceSite,
    /// A producer-generated cut or pole vertex with no source record.
    ChartRestrictionSite,
    ConstraintSplit {
        certificate: usize,
    },
    InteriorRefinement {
        parent_face: usize,
        parent_sites: [usize; 3],
        construction: RefinementConstruction,
    },
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum RefinementConstruction {
    Circumcenter,
    EdgeMidpoint([usize; 2]),
    LongestEdgeMidpoint([usize; 2]),
}

#[derive(Clone, Debug)]
pub(crate) struct CdtSourceUse {
    pub(crate) source: usize,
    pub(crate) owner: RingOwner,
    pub(crate) start: exact::ExactRational,
    pub(crate) end: exact::ExactRational,
    pub(crate) forward: bool,
}

#[derive(Clone, Debug)]
pub(crate) enum CdtSideBoundaryUse {
    Source(usize),
    /// Index into the plan's directed chart-restriction piece table.
    Restriction(usize),
}

#[derive(Clone, Debug)]
pub(crate) struct CdtSideUse {
    pub(crate) halfedge: usize,
    pub(crate) start_site: usize,
    pub(crate) end_site: usize,
    pub(crate) twin: Option<usize>,
    pub(crate) twin_selected: bool,
    pub(crate) boundary: Option<CdtSideBoundaryUse>,
}

#[derive(Clone, Debug)]
pub(crate) struct CdtFaceUse {
    pub(crate) live_face: usize,
    pub(crate) selected_sites: [usize; 3],
    pub(crate) output_to_selected: [u8; 3],
    pub(crate) sides: Vec<CdtSideUse>,
}

/// One constrained mesh and its checked publication inputs.
///
/// Selected faces, authored source intervals, chart-restriction uses, and
/// generated-node provenance stay together so publishers cannot reconstruct a
/// different mesh result from exported points.
#[derive(Debug)]
pub(crate) struct ConstrainedMeshPlan {
    pub(crate) vertices: Vec<Point>,
    pub(crate) faces: Vec<[usize; 3]>,
    pub(crate) face_uses: Vec<CdtFaceUse>,
    coordinates: CdtCoordinates,
    face_slot_count: usize,
    pub(crate) provenance: Vec<NodeProvenance>,
    pub(crate) source_constraints: Vec<SourceConstraint>,
    pub(crate) source_uses: Vec<CdtSourceUse>,
    pub(crate) restrictions: Vec<RestrictionConstraint>,
    source_rings: Vec<SourceRingStart>,
    pub(crate) constraint_pieces: Vec<SourcePiece>,
    pub(crate) restriction_pieces: Vec<RestrictionPiece>,
    split_certificates: Vec<CertifiedBoundarySplit>,
    pub(crate) refinement: super::CdtRefinement,
    pub(crate) force_xy: bool,
}

impl ConstrainedMeshPlan {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the parent-owned final writer binds retained site locations to stored corner records"
        )
    )]
    pub(crate) fn site_chart_location(&self, site: usize) -> Option<ChartPointSite> {
        self.coordinates.retained_site_location(site)
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the parent-owned final writer resolves source-occurrence coordinates for boundary output"
        )
    )]
    pub(crate) fn source_chart_location(&self, occurrence: usize) -> Option<ChartPointSite> {
        self.coordinates.retained_source_location(occurrence)
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the parent-owned final writer emits realized generated chart coordinates"
        )
    )]
    pub(crate) fn chart_proposal(&self, site: usize) -> XY {
        self.coordinates.site_xy(site)
    }

    pub(super) fn triangle_orientation(&self, sites: [usize; 3]) -> Result<ExactSign> {
        self.coordinates
            .orient(sites.map(|site| self.coordinates.site_query(site)))
    }

    pub(super) fn validate_refined_triangle(
        &self,
        sites: [usize; 3],
        refinement: super::CdtRefinement,
    ) -> Result<()> {
        let super::CdtRefinement::On {
            min_angle,
            max_area,
        } = refinement
        else {
            return Ok(());
        };
        let [a, b, c] = self.coordinates.metric_frame(sites)?;
        if let Some(limit) = min_angle
            && triangle_min_angle_degrees(a, b, c) + 1.0e-9 < limit.get()
        {
            return Err(GeometryErrorKind::triangulation(format!(
                "constrained refinement did not satisfy min_angle={} degrees",
                limit.get()
            )));
        }
        if let Some(limit) = max_area
            && self.coordinates.triangle_exceeds_area(sites, limit.get())?
        {
            return Err(GeometryErrorKind::triangulation(format!(
                "constrained refinement did not satisfy max_area={}",
                limit.get()
            )));
        }
        Ok(())
    }

    /// Consume the retained source certificate immediately before publication.
    ///
    /// The topology builder performs these checks while the live mesh is
    /// available. Rechecking the detached product prevents a publisher from
    /// silently dropping source intervals or split proofs.
    pub(crate) fn validate_certificate(&self) -> Result<()> {
        if self.provenance.len() != self.vertices.len()
            || self.coordinates.sites.len() != self.vertices.len()
            || self.face_uses.len() != self.faces.len()
        {
            return Err(GeometryErrorKind::triangulation(
                "constrained mesh certificate has inconsistent node or edge state",
            ));
        }
        let zero = exact::ExactRational::zero();
        let one = exact::ExactRational::one();
        for (site, provenance) in self.provenance.iter().enumerate() {
            match provenance {
                NodeProvenance::SourceSite | NodeProvenance::InteriorRefinement { .. } => {},
                NodeProvenance::ChartRestrictionSite => {
                    if self.coordinates.retained_site_location(site).is_some()
                        || !self.restrictions.iter().any(|restriction| {
                            restriction.start_site == site || restriction.end_site == site
                        })
                    {
                        return Err(GeometryErrorKind::triangulation(
                            "synthetic restriction site has no matching source-less chart use",
                        ));
                    }
                },
                NodeProvenance::ConstraintSplit { certificate } => {
                    let Some(certificate) = self.split_certificates.get(*certificate) else {
                        return Err(GeometryErrorKind::triangulation(
                            "constrained mesh certificate has a missing split proof",
                        ));
                    };
                    let constraint_count = self
                        .source_constraints
                        .len()
                        .checked_add(self.restrictions.len())
                        .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    if certificate.constraint >= constraint_count
                        || certificate.parameter.try_cmp(&zero)? != std::cmp::Ordering::Greater
                        || certificate.parameter.try_cmp(&one)? != std::cmp::Ordering::Less
                    {
                        return Err(GeometryErrorKind::triangulation(
                            "constrained mesh certificate has an invalid split provenance",
                        ));
                    }
                    let point = self.vertices.get(site).ok_or_else(|| {
                        GeometryErrorKind::triangulation(
                            "constrained mesh certificate has a missing split vertex",
                        )
                    })?;
                    let expected_half_ulp = split_half_ulp_bounds(point.xy())?;
                    if point.xy() != certificate.point
                        || expected_half_ulp != certificate.half_ulp
                        || self.coordinates.site_xy(site) != certificate.point
                    {
                        return Err(GeometryErrorKind::triangulation(
                            "constrained mesh certificate has a mismatched split proof",
                        ));
                    }
                },
            }
            if let NodeProvenance::InteriorRefinement {
                parent_face,
                parent_sites,
                construction,
            } = provenance
            {
                if *parent_face >= self.face_slot_count
                    || parent_sites.iter().any(|&parent| parent >= site)
                {
                    return Err(GeometryErrorKind::triangulation(
                        "generated mesh site has an invalid parent recipe",
                    ));
                }
                let valid_construction = match construction {
                    RefinementConstruction::Circumcenter => true,
                    RefinementConstruction::EdgeMidpoint(edge)
                    | RefinementConstruction::LongestEdgeMidpoint(edge) => {
                        let [edge_start, edge_end] = edge;
                        edge_start != edge_end
                            && parent_sites.contains(edge_start)
                            && parent_sites.contains(edge_end)
                    },
                };
                if !valid_construction
                    || self
                        .vertices
                        .get(site)
                        .is_none_or(|point| point.xy() != self.coordinates.site_xy(site))
                {
                    return Err(GeometryErrorKind::triangulation(
                        "generated mesh site has an invalid construction recipe",
                    ));
                }
            }
        }
        for (id, source) in self.source_constraints.iter().enumerate() {
            if source.start_occurrence >= self.coordinates.source_occurrences.len()
                || source.end_occurrence >= self.coordinates.source_occurrences.len()
            {
                return Err(GeometryErrorKind::triangulation(
                    "constrained mesh certificate has an absent source occurrence",
                ));
            }
            let source_start = self.coordinates.source_query(source.start_occurrence);
            let source_end = self.coordinates.source_query(source.end_occurrence);
            if source.id != id
                || source.start_site == source.end_site
                || !source.start.xy().x.is_finite()
                || !source.start.xy().y.is_finite()
                || !source.end.xy().x.is_finite()
                || !source.end.xy().y.is_finite()
                || self.vertices.get(source.start_site).is_none()
                || self.vertices.get(source.end_site).is_none()
                || self.coordinates.key(source_start)
                    != self
                        .coordinates
                        .key(self.coordinates.site_query(source.start_site))
                || self.coordinates.key(source_end)
                    != self
                        .coordinates
                        .key(self.coordinates.site_query(source.end_site))
            {
                return Err(GeometryErrorKind::triangulation(
                    "constrained mesh certificate has an invalid source site",
                ));
            }
        }
        for (id, restriction) in self.restrictions.iter().enumerate() {
            let valid_role = match restriction.use_.restriction {
                ChartRestriction::CutChord => matches!(
                    (restriction.use_.start, restriction.use_.end),
                    (
                        SyntheticChartVertex::Cut {
                            side: start_side, ..
                        },
                        SyntheticChartVertex::Cut {
                            side: end_side, ..
                        }
                    ) if start_side == end_side
                ),
                ChartRestriction::PolarCap { pole } => matches!(
                    (restriction.use_.start, restriction.use_.end),
                    (
                        SyntheticChartVertex::Pole {
                            pole: start_pole, ..
                        },
                        SyntheticChartVertex::Pole {
                            pole: end_pole, ..
                        }
                    ) if start_pole == pole && end_pole == pole
                ),
            };
            if restriction.id != id
                || restriction.start_site == restriction.end_site
                || self.vertices.get(restriction.start_site).is_none()
                || self.vertices.get(restriction.end_site).is_none()
                || !valid_role
            {
                return Err(GeometryErrorKind::triangulation(
                    "constrained mesh certificate has an invalid chart restriction",
                ));
            }
        }
        let mut source_id = 0;
        let mut cursor = exact::ExactRational::zero();
        for piece in &self.constraint_pieces {
            if self.source_constraints.get(piece.source).is_none()
                || self.vertices.get(piece.start_site).is_none()
                || self.vertices.get(piece.end_site).is_none()
            {
                return Err(GeometryErrorKind::triangulation(
                    "constrained mesh certificate has an invalid source piece",
                ));
            }
            if piece.start_site == piece.end_site
                || piece.lo.try_cmp(&piece.hi)? != std::cmp::Ordering::Less
            {
                return Err(GeometryErrorKind::triangulation(
                    "constrained mesh certificate has an invalid source piece",
                ));
            }
            if piece.source != source_id {
                if source_id.checked_add(1) != Some(piece.source)
                    || cursor.try_cmp(&one)? != std::cmp::Ordering::Equal
                {
                    return Err(GeometryErrorKind::triangulation(
                        "constrained mesh certificate has a discontinuous source chain",
                    ));
                }
                source_id = piece.source;
                cursor = exact::ExactRational::zero();
            }
            if piece.lo.try_cmp(&cursor)? != std::cmp::Ordering::Equal {
                return Err(GeometryErrorKind::triangulation(
                    "constrained mesh certificate has an unordered source chain",
                ));
            }
            cursor = piece.hi.clone();
        }
        let source_count_matches = if self.constraint_pieces.is_empty() {
            self.source_constraints.is_empty()
        } else {
            source_id.checked_add(1) == Some(self.source_constraints.len())
        };
        if !source_count_matches
            || (!self.constraint_pieces.is_empty()
                && cursor.try_cmp(&one)? != std::cmp::Ordering::Equal)
        {
            return Err(GeometryErrorKind::triangulation(
                "constrained mesh certificate does not cover all sources",
            ));
        }
        let mut remaining_restrictions = self.restriction_pieces.as_slice();
        for restriction in &self.restrictions {
            let count = remaining_restrictions
                .iter()
                .take_while(|piece| piece.restriction == restriction.id)
                .count();
            let (pieces, rest) = remaining_restrictions.split_at(count);
            remaining_restrictions = rest;
            if pieces.is_empty() {
                return Err(GeometryErrorKind::triangulation(
                    "chart restriction has no retained directed pieces",
                ));
            }
            let mut cursor = exact::ExactRational::zero();
            let mut previous_vertex = restriction.use_.start;
            for piece in pieces {
                if self.vertices.get(piece.start_site).is_none()
                    || self.vertices.get(piece.end_site).is_none()
                    || piece.start_site == piece.end_site
                    || piece.start.try_cmp(&cursor)? != std::cmp::Ordering::Equal
                    || piece.start.try_cmp(&piece.end)? != std::cmp::Ordering::Less
                    || piece.use_.restriction != restriction.use_.restriction
                    || piece.use_.start != previous_vertex
                {
                    return Err(GeometryErrorKind::triangulation(
                        "constrained mesh certificate has a broken directed restriction chain",
                    ));
                }
                let valid_role = match piece.use_.restriction {
                    ChartRestriction::CutChord => matches!(
                        (piece.use_.start, piece.use_.end),
                        (
                            SyntheticChartVertex::Cut { side: start, .. },
                            SyntheticChartVertex::Cut { side: end, .. }
                        ) if start == end
                    ),
                    ChartRestriction::PolarCap { pole } => matches!(
                        (piece.use_.start, piece.use_.end),
                        (
                            SyntheticChartVertex::Pole { pole: start, .. },
                            SyntheticChartVertex::Pole { pole: end, .. }
                        ) if start == pole && end == pole
                    ),
                };
                if !valid_role {
                    return Err(GeometryErrorKind::triangulation(
                        "restriction piece has incompatible synthetic endpoint roles",
                    ));
                }
                cursor = piece.end.clone();
                previous_vertex = piece.use_.end;
            }
            if cursor.try_cmp(&one)? != std::cmp::Ordering::Equal
                || previous_vertex != restriction.use_.end
            {
                return Err(GeometryErrorKind::triangulation(
                    "constrained mesh certificate does not cover a chart restriction",
                ));
            }
        }
        if !remaining_restrictions.is_empty() {
            return Err(GeometryErrorKind::triangulation(
                "constrained mesh certificate contains an unknown chart restriction",
            ));
        }
        let mut constrained_piece_use_count =
            crate::collections::try_hash_map_with_capacity(self.constraint_pieces.len())?;
        let mut source_use_count = vec![0_usize; self.source_uses.len()];
        let mut restriction_piece_use_count =
            crate::collections::try_hash_map_with_capacity(self.restriction_pieces.len())?;
        for (sites, uses) in self.faces.iter().zip(&self.face_uses) {
            if uses.sides.len() != 3 {
                return Err(GeometryErrorKind::triangulation(
                    "selected constrained face has an incomplete side-use table",
                ));
            }
            for output_vertex in 0..3 {
                let selected_vertex = uses
                    .output_to_selected
                    .get(output_vertex)
                    .copied()
                    .map(usize::from);
                let output_site = sites.get(output_vertex).copied();
                if selected_vertex.and_then(|selected| uses.selected_sites.get(selected).copied())
                    != output_site
                {
                    return Err(GeometryErrorKind::triangulation(
                        "canonical constrained face lost its selected-site map",
                    ));
                }
            }
            for output_side in 0..3 {
                let Some(side) = uses.sides.get(output_side) else {
                    return Err(GeometryErrorKind::triangulation(
                        "selected constrained face has a missing directed side",
                    ));
                };
                let start = sites.get(output_side).copied();
                let end = sites.get(next_triangle_side(output_side)).copied();
                let live_edge = HalfedgeId::from_index(side.halfedge);
                let twin_in_range = side
                    .twin
                    .is_none_or(|twin| twin < self.face_slot_count.saturating_mul(3));
                if start != Some(side.start_site)
                    || end != Some(side.end_site)
                    || uses.live_face >= self.face_slot_count
                    || live_edge.face().index() != uses.live_face
                    || side.twin_selected && side.twin.is_none()
                    || !twin_in_range
                {
                    return Err(GeometryErrorKind::triangulation(
                        "selected constrained side has an invalid live-use receipt",
                    ));
                }
                match &side.boundary {
                    Some(CdtSideBoundaryUse::Source(source_use_index)) => {
                        let Some(source_use) = self.source_uses.get(*source_use_index) else {
                            return Err(GeometryErrorKind::triangulation(
                                "constrained side references an absent source-use record",
                            ));
                        };
                        let Some(source) = self.source_constraints.get(source_use.source) else {
                            return Err(GeometryErrorKind::triangulation(
                                "constrained side references an absent source root",
                            ));
                        };
                        if source_ring_owner(source.id, &self.source_rings)
                            != Some(source_use.owner)
                        {
                            return Err(GeometryErrorKind::triangulation(
                                "constrained side has a mismatched polygon-ring owner",
                            ));
                        }
                        let matched_piece =
                            self.constraint_pieces
                                .iter()
                                .enumerate()
                                .find(|(_, piece)| {
                                    piece.source == source_use.source
                                        && ordered(piece.start_site, piece.end_site)
                                            == ordered(side.start_site, side.end_site)
                                });
                        let Some((piece_index, piece)) = matched_piece else {
                            return Err(GeometryErrorKind::triangulation(
                                "constrained side has no matching directed source interval",
                            ));
                        };
                        let (expected_start, expected_end) = if source_use.forward {
                            (&piece.lo, &piece.hi)
                        } else {
                            (&piece.hi, &piece.lo)
                        };
                        if source_use.forward
                            != ((piece.start_site, piece.end_site)
                                == (side.start_site, side.end_site))
                            || source_use.start.try_cmp(expected_start)? != Ordering::Equal
                            || source_use.end.try_cmp(expected_end)? != Ordering::Equal
                        {
                            return Err(GeometryErrorKind::triangulation(
                                "constrained side has a reversed or crosswired source interval",
                            ));
                        }
                        let count = constrained_piece_use_count
                            .entry(piece_index)
                            .or_insert(0_usize);
                        *count = count
                            .checked_add(1)
                            .ok_or(crate::error::ResourceError::SizeOverflow)?;
                        let count =
                            source_use_count.get_mut(*source_use_index).ok_or_else(|| {
                                GeometryErrorKind::triangulation(
                                    "source-use index is outside its prepared table",
                                )
                            })?;
                        *count = count
                            .checked_add(1)
                            .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    },
                    Some(CdtSideBoundaryUse::Restriction(piece_index)) => {
                        let Some(piece) = self.restriction_pieces.get(*piece_index) else {
                            return Err(GeometryErrorKind::triangulation(
                                "constrained side references an absent chart-restriction piece",
                            ));
                        };
                        let Some(restriction) = self.restrictions.get(piece.restriction) else {
                            return Err(GeometryErrorKind::triangulation(
                                "chart-restriction piece references an absent root",
                            ));
                        };
                        if piece.owner != restriction.owner
                            || ordered(piece.start_site, piece.end_site)
                                != ordered(side.start_site, side.end_site)
                            || piece.use_.restriction != restriction.use_.restriction
                        {
                            return Err(GeometryErrorKind::triangulation(
                                "constrained side has a crosswired chart-restriction use",
                            ));
                        }
                        let count = restriction_piece_use_count
                            .entry(*piece_index)
                            .or_insert(0_usize);
                        *count = count
                            .checked_add(1)
                            .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    },
                    None => {},
                }
            }
        }
        if constrained_piece_use_count.len() != self.constraint_pieces.len()
            || constrained_piece_use_count
                .values()
                .any(|&count| count != 1)
        {
            return Err(GeometryErrorKind::triangulation(
                "source constraint piece does not have exactly one selected face use",
            ));
        }
        if source_use_count.len() != self.constraint_pieces.len()
            || source_use_count.iter().any(|&count| count != 1)
        {
            return Err(GeometryErrorKind::triangulation(
                "source-use record does not have exactly one selected face use",
            ));
        }
        if restriction_piece_use_count.len() != self.restriction_pieces.len()
            || restriction_piece_use_count
                .values()
                .any(|&count| count != 1)
        {
            return Err(GeometryErrorKind::triangulation(
                "chart restriction piece does not have exactly one selected face use",
            ));
        }
        let live_edge_count = self
            .face_slot_count
            .checked_mul(3)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let side_capacity = self
            .face_uses
            .len()
            .checked_mul(3)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let mut selected_uses = crate::collections::try_hash_map_with_capacity(side_capacity)?;
        for (face_index, uses) in self.face_uses.iter().enumerate() {
            for (side_index, side) in uses.sides.iter().enumerate() {
                if side.halfedge >= live_edge_count
                    || selected_uses
                        .insert(side.halfedge, (face_index, side_index))
                        .is_some()
                {
                    return Err(GeometryErrorKind::triangulation(
                        "selected constrained face repeats or misidentifies a live halfedge",
                    ));
                }
            }
        }
        for uses in &self.face_uses {
            for side in &uses.sides {
                let expected_twin_selected = side
                    .twin
                    .is_some_and(|twin| selected_uses.contains_key(&twin));
                if side.twin_selected != expected_twin_selected {
                    return Err(GeometryErrorKind::triangulation(
                        "selected constrained side has a mismatched twin-selection fact",
                    ));
                }
                if side.twin_selected {
                    let twin = side.twin.expect("selected twins were checked above");
                    let reciprocal = selected_uses
                        .get(&twin)
                        .and_then(|&(face_index, side_index)| {
                            self.face_uses
                                .get(face_index)
                                .and_then(|face| face.sides.get(side_index))
                        })
                        .is_some_and(|other| {
                            other.twin == Some(side.halfedge)
                                && other.start_site == side.end_site
                                && other.end_site == side.start_site
                                && other.twin_selected
                        });
                    if !reciprocal {
                        return Err(GeometryErrorKind::triangulation(
                            "selected twin uses do not identify each other",
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

fn exact_source_coordinate(
    start: &exact::ExactDyadic,
    end: &exact::ExactDyadic,
    parameter: &exact::ExactRational,
) -> Result<exact::ExactRational> {
    let start = exact::ExactRational::from_dyadic(start.clone());
    let delta = exact::ExactRational::from_dyadic(end.clone()).try_subtract(&start)?;
    start.try_add(&delta.try_product(parameter)?)
}

fn round_source_coordinate(value: &exact::ExactRational) -> Result<f64> {
    match value.round() {
        Ok(value) => Ok(value),
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::Geometry(GeometryErrorKind::ExactCoordinateOutOfRange)
            ) =>
        {
            Err(GeometryErrorKind::unrepresentable_output("triangulate"))
        },
        Err(error) => Err(error),
    }
}

fn split_half_ulp(value: f64) -> Result<exact::ExactDyadic> {
    if !value.is_finite() {
        return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
    }
    let next = value.next_up();
    let previous = value.next_down();
    let up_gap = if next.is_finite() { next - value } else { 0.0 };
    let down_gap = if previous.is_finite() {
        value - previous
    } else {
        0.0
    };
    let gap = up_gap.max(down_gap);
    exact::ExactDyadic::from_f64(gap).try_product(&exact::ExactDyadic::from_f64(0.5))
}

fn split_half_ulp_bounds(point: XY) -> Result<[exact::ExactDyadic; 2]> {
    Ok([split_half_ulp(point.x)?, split_half_ulp(point.y)?])
}

fn restriction_use_for_piece(
    restriction: &RestrictionConstraint,
    start_site: usize,
    end_site: usize,
    coordinates: &CdtCoordinates,
) -> Result<DirectedChartRestrictionUse> {
    let synthetic_vertex = |site| -> Result<SyntheticChartVertex> {
        if site == restriction.start_site {
            return Ok(restriction.use_.start);
        }
        if site == restriction.end_site {
            return Ok(restriction.use_.end);
        }
        Ok(match restriction.use_.restriction {
            ChartRestriction::CutChord => {
                let side = match (restriction.use_.start, restriction.use_.end) {
                    (
                        SyntheticChartVertex::Cut { side: start, .. },
                        SyntheticChartVertex::Cut { side: end, .. },
                    ) if start == end => start,
                    _ => {
                        return Err(GeometryErrorKind::triangulation(
                            "cut restriction has incompatible endpoint aliases",
                        ));
                    },
                };
                SyntheticChartVertex::Cut {
                    node: u32::try_from(site)
                        .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                    side,
                }
            },
            ChartRestriction::PolarCap { pole } => SyntheticChartVertex::Pole {
                node: u32::try_from(site).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                pole,
                native_longitude_bits: coordinates.site_xy(site).x.to_bits(),
            },
        })
    };
    Ok(DirectedChartRestrictionUse {
        restriction: restriction.use_.restriction,
        start: synthetic_vertex(start_site)?,
        end: synthetic_vertex(end_site)?,
    })
}

fn split_deviation_within(
    coordinates: &CdtCoordinates,
    start: CdtQuery,
    end: CdtQuery,
    parameter: &exact::ExactRational,
    point: CdtQuery,
    half_ulp: &[exact::ExactDyadic; 2],
) -> Result<bool> {
    let [half_ulp_x, half_ulp_y] = half_ulp;
    let [start_x, start_y] = coordinates.exact_xy(start);
    let [end_x, end_y] = coordinates.exact_xy(end);
    let [actual_x, actual_y] = coordinates.exact_xy(point);
    let exact_x = exact_source_coordinate(&start_x, &end_x, parameter)?;
    let exact_y = exact_source_coordinate(&start_y, &end_y, parameter)?;
    let error_x = exact::ExactRational::from_dyadic(actual_x).try_subtract(&exact_x)?;
    let error_y = exact::ExactRational::from_dyadic(actual_y).try_subtract(&exact_y)?;
    let zero = exact::ExactRational::zero();
    let abs_x = if error_x.sign() == ExactSign::Negative {
        zero.try_subtract(&error_x)?
    } else {
        error_x
    };
    let abs_y = if error_y.sign() == ExactSign::Negative {
        zero.try_subtract(&error_y)?
    } else {
        error_y
    };
    Ok(
        abs_x.try_cmp(&exact::ExactRational::from_dyadic(half_ulp_x.clone()))?
            != std::cmp::Ordering::Greater
            && abs_y.try_cmp(&exact::ExactRational::from_dyadic(half_ulp_y.clone()))?
                != std::cmp::Ordering::Greater,
    )
}

fn validate_piece_endpoint(
    coordinates: &CdtCoordinates,
    constraint: usize,
    start_site: usize,
    end_site: usize,
    parameter: &exact::ExactRational,
    point: CdtQuery,
    site: usize,
    provenance: &[NodeProvenance],
    certificates: &[CertifiedBoundarySplit],
) -> Result<()> {
    let point_xy = coordinates.proposal_xy(point);
    match provenance.get(site) {
        Some(NodeProvenance::ConstraintSplit { certificate }) => {
            let Some(certificate) = certificates.get(*certificate) else {
                return Err(GeometryErrorKind::triangulation(
                    "constraint references a missing rounded split proof",
                ));
            };
            if certificate.constraint != constraint
                || certificate.parameter.try_cmp(parameter)? != std::cmp::Ordering::Equal
                || certificate.point != point_xy
            {
                return Err(GeometryErrorKind::triangulation(
                    "constraint interval does not match its rounded split",
                ));
            }
        },
        Some(
            NodeProvenance::SourceSite
            | NodeProvenance::ChartRestrictionSite
            | NodeProvenance::InteriorRefinement { .. },
        ) => {
            let start = coordinates.site_query(start_site);
            let end = coordinates.site_query(end_site);
            if coordinates.orient([start, end, point])? != ExactSign::Zero {
                return Err(GeometryErrorKind::triangulation(
                    "constraint has an off-line emitted vertex",
                ));
            }
            let actual = coordinates.interpolate_source_parameter(
                &exact::ExactRational::zero(),
                &exact::ExactRational::one(),
                start,
                end,
                point,
            )?;
            if actual.try_cmp(parameter)? != std::cmp::Ordering::Equal {
                return Err(GeometryErrorKind::triangulation(
                    "constraint interval does not match its emitted vertex",
                ));
            }
        },
        None => {
            return Err(GeometryErrorKind::triangulation(
                "constraint references missing provenance",
            ));
        },
    }
    Ok(())
}

struct PreparedSplit {
    candidate: XY,
    located: Located,
    constraint: Option<PreparedConstraintSplit>,
    construction: Option<RefinementConstruction>,
}

struct PreparedConstraintSplit {
    piece: ConstraintPiece,
    certificate: CertifiedBoundarySplit,
}

struct PieceNode {
    piece: Option<ConstraintPiece>,
    next: Option<usize>,
}

enum PieceCursor {
    Replace(usize),
    After(usize),
}

// Constraint IDs own the first slots. A split replaces its existing slot so
// predecessor links remain valid when the edge-index key changes.
struct ConstraintPieces {
    edges: HashMap<(usize, usize), usize>,
    nodes: Vec<PieceNode>,
}

impl ConstraintPieces {
    fn new(constraint_count: usize) -> Result<Self> {
        Ok(Self {
            edges: crate::collections::try_hash_map_with_capacity(constraint_count)?,
            nodes: std::iter::repeat_with(|| PieceNode {
                piece: None,
                next: None,
            })
            .take(constraint_count)
            .collect(),
        })
    }

    fn piece(&self, index: usize) -> &ConstraintPiece {
        self.nodes
            .get(index)
            .and_then(|node| node.piece.as_ref())
            .expect("the edge index names a recorded constraint piece")
    }

    fn get(&self, edge: (usize, usize)) -> Option<&ConstraintPiece> {
        self.edges.get(&edge).map(|&index| self.piece(index))
    }

    fn values(&self) -> impl Iterator<Item = &ConstraintPiece> {
        self.edges.values().map(|&index| self.piece(index))
    }

    fn reserve(&mut self, additional: usize) {
        self.edges.reserve(additional);
        self.nodes.reserve(additional);
    }

    fn remove(&mut self, edge: (usize, usize)) -> Option<PieceCursor> {
        self.edges.remove(&edge).map(PieceCursor::Replace)
    }

    fn record(&mut self, piece: ConstraintPiece, cursor: &mut PieceCursor) {
        let entry = self.edges.entry(ordered(piece.start_site, piece.end_site));
        if let std::collections::hash_map::Entry::Vacant(entry) = entry {
            let index = match *cursor {
                PieceCursor::Replace(index) => {
                    self.nodes
                        .get_mut(index)
                        .expect("replacement uses an admitted constraint slot")
                        .piece = Some(piece);
                    index
                },
                PieceCursor::After(previous) => {
                    let next = self
                        .nodes
                        .get(previous)
                        .expect("the insertion cursor names an admitted constraint slot")
                        .next;
                    let index = self.nodes.len();
                    self.nodes.push(PieceNode {
                        piece: Some(piece),
                        next,
                    });
                    self.nodes
                        .get_mut(previous)
                        .expect("the insertion cursor names an admitted constraint slot")
                        .next = Some(index);
                    index
                },
            };
            entry.insert(index);
            *cursor = PieceCursor::After(index);
        }
    }

    #[cfg_attr(
        test,
        expect(
            clippy::panic_in_result_fn,
            reason = "test-only assertions characterize source-chain order against the sorted oracle"
        )
    )]
    fn take_ordered(
        &mut self,
        sources: &[SourceConstraint],
        restrictions: &[RestrictionConstraint],
        source_rings: &[SourceRingStart],
        coordinates: &CdtCoordinates,
    ) -> Result<Vec<ConstraintPiece>> {
        let root_count = sources
            .len()
            .checked_add(restrictions.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        #[cfg(test)]
        let expected = {
            let mut pieces: Vec<_> = self.values().cloned().collect();
            crate::geometry::try_sort_by(&mut pieces, compare_constraint_pieces)?;
            pieces
        };
        let mut pieces = Vec::with_capacity(self.edges.len());
        for constraint in 0..root_count {
            let mut at = Some(constraint);
            while let Some(index) = at {
                let node = self
                    .nodes
                    .get_mut(index)
                    .expect("constraint successors name admitted piece slots");
                let Some(piece) = node.piece.take() else {
                    if constraint >= sources.len() {
                        let restriction = constraint
                            .checked_sub(sources.len())
                            .ok_or(crate::error::ResourceError::SizeOverflow)?;
                        return Err(GeometryErrorKind::triangulation(format!(
                            "chart restriction {restriction} has no emitted boundary chain"
                        )));
                    }
                    let missing = sources.get(constraint).expect("source IDs are ordered");
                    let mut overlap = None;
                    for other in sources {
                        if other.id != missing.id
                            && source_constraints_overlap(coordinates, missing, other)?
                        {
                            overlap = Some(other);
                            break;
                        }
                    }
                    if let Some(overlap) = overlap {
                        let left = source_ring_owner(missing.id, source_rings)
                            .expect("a source constraint belongs to a source ring");
                        let right = source_ring_owner(overlap.id, source_rings)
                            .expect("an overlapping source constraint belongs to a source ring");
                        return Err(super::super::overlapping_rings_error(left, right));
                    }
                    return Err(GeometryErrorKind::triangulation(format!(
                        "source constraint {constraint} has no emitted boundary chain"
                    )));
                };
                pieces.push(piece);
                at = node.next;
            }
        }
        debug_assert_eq!(pieces.len(), self.edges.len());
        #[cfg(test)]
        {
            assert_eq!(pieces.len(), expected.len());
            for (actual, expected) in pieces.iter().zip(&expected) {
                assert_eq!(
                    compare_constraint_pieces(actual, expected)?,
                    std::cmp::Ordering::Equal
                );
            }
        }
        self.edges.clear();
        self.nodes.clear();
        Ok(pieces)
    }
}

/// The mesh with its constraint flags, one per halfedge slot.
pub(super) struct ConstrainedMesh {
    pub(super) mesh: Mesh,
    pub(super) constrained: Vec<bool>,
    coordinates: CdtCoordinates,
    source_constraints: Vec<SourceConstraint>,
    restrictions: Vec<RestrictionConstraint>,
    source_rings: Vec<SourceRingStart>,
    constraint_pieces: ConstraintPieces,
    provenance: Vec<NodeProvenance>,
    split_certificates: Vec<CertifiedBoundarySplit>,
}

struct FaceLabels(Vec<Option<bool>>);

#[derive(Clone, Copy)]
struct BadTriangle {
    face: FaceId,
    triangle: [usize; 3],
    corners: [XY; 3],
    angle: f64,
    area: f64,
}

impl PartialEq for BadTriangle {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for BadTriangle {}

impl PartialOrd for BadTriangle {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BadTriangle {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.angle
            .total_cmp(&other.angle)
            .then_with(|| self.area.total_cmp(&other.area))
            .then_with(|| {
                self.corners
                    .iter()
                    .zip(other.corners)
                    .map(|(&left, right)| super::exact_delaunay::compare_xy(right, left))
                    .find(|order| !order.is_eq())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| self.triangle.cmp(&other.triangle))
    }
}

impl BadTriangle {
    fn new(
        face: FaceId,
        triangle: [usize; 3],
        coordinates: &CdtCoordinates,
        min_angle: Option<f64>,
        max_area: Option<f64>,
    ) -> Result<Option<Self>> {
        #[cfg(test)]
        update_work(|work| {
            work.quality_checks = work.quality_checks.saturating_add(1);
        });
        let mut corners = coordinates.metric_frame(triangle)?;
        if max_area.is_some() {
            corners
                .sort_unstable_by(|&left, &right| super::exact_delaunay::compare_xy(left, right));
        }
        let [a, b, c] = corners;
        let angle_bad = min_angle.is_some_and(|limit| triangle_min_angle_degrees(a, b, c) < limit);
        let area = max_area.map_or(0.0, |_| open_xy_cycle_magnitude(&[a, b, c]).get());
        let area_bad = if let Some(limit) = max_area {
            coordinates.triangle_exceeds_area(triangle, limit)?
        } else {
            false
        };
        if !angle_bad && !area_bad {
            return Ok(None);
        }
        if max_area.is_none() {
            corners
                .sort_unstable_by(|&left, &right| super::exact_delaunay::compare_xy(left, right));
        }
        let [a, b, c] = corners;
        Ok(Some(Self {
            face,
            triangle,
            corners,
            angle: if angle_bad {
                radius_edge_ratio(a, b, c)
            } else {
                0.0
            },
            area,
        }))
    }
}

fn radius_edge_ratio(a: XY, b: XY, c: XY) -> f64 {
    let scale = [a.x, a.y, b.x, b.y, c.x, c.y]
        .into_iter()
        .map(f64::abs)
        .fold(0.0_f64, f64::max);
    let normalized = |point: XY| XY::new(point.x / scale, point.y / scale);
    let (a, b, c) = (normalized(a), normalized(b), normalized(c));
    let length = |left: XY, right: XY| (left.x - right.x).hypot(left.y - right.y);
    let mut lengths = [length(a, b), length(b, c), length(c, a)];
    lengths.sort_unstable_by(f64::total_cmp);
    let [_, middle, longest] = lengths;
    let cross = ((b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x)).abs();
    if cross == 0.0 {
        f64::INFINITY
    } else {
        middle * longest / (2.0 * cross)
    }
}

impl FaceLabels {
    fn from_inside(mesh: &Mesh, inside: &[bool]) -> Self {
        let mut faces = Vec::with_capacity(mesh.triangle_count());
        for (face, &inside) in mesh.face_ids().zip(inside) {
            faces.push(mesh.alive(face).then_some(inside));
        }
        Self(faces)
    }

    fn label(&mut self, face: FaceId, value: bool) -> Result<bool> {
        let Some(inside) = self.0.get_mut(face.index()) else {
            return Err(GeometryErrorKind::triangulation(
                "refinement label references an absent face",
            ));
        };
        if let Some(previous) = inside {
            if *previous != value {
                return Err(GeometryErrorKind::triangulation(
                    "refinement boundary has inconsistent inside labels",
                ));
            }
            return Ok(false);
        }
        *inside = Some(value);
        Ok(true)
    }

    fn refresh(
        &mut self,
        mesh: &Mesh,
        constrained: &[bool],
        changed: &mut Vec<FaceId>,
    ) -> Result<()> {
        changed.sort_unstable_by_key(|face| face.index());
        changed.dedup();
        self.0.resize(mesh.triangle_count(), None);
        for &face in changed.iter() {
            let previous = self.0.get_mut(face.index()).ok_or_else(|| {
                GeometryErrorKind::triangulation("refinement changed a missing face")
            })?;
            *previous = None;
        }
        let mut queue = VecDeque::new();
        for &face in changed.iter() {
            if !mesh.alive(face) {
                continue;
            }
            for side in FaceSide::ALL {
                let edge = HalfedgeId::in_face(face, side);
                let twin = mesh.twin(edge);
                let across = if twin == EMPTY {
                    Some(false)
                } else {
                    self.get(HalfedgeId::from_index(twin).face())
                };
                if let Some(across) = across {
                    let label = across ^ Mesh::side_value(constrained, edge);
                    if self.label(face, label)? {
                        queue.push_back((face, label));
                    }
                }
            }
        }
        while let Some((face, inside)) = queue.pop_front() {
            for side in FaceSide::ALL {
                let edge = HalfedgeId::in_face(face, side);
                let twin = mesh.twin(edge);
                if twin == EMPTY {
                    continue;
                }
                let neighbor = HalfedgeId::from_index(twin).face();
                if self.get(neighbor).is_none() {
                    let label = inside ^ Mesh::side_value(constrained, edge);
                    if self.label(neighbor, label)? {
                        queue.push_back((neighbor, label));
                    }
                }
            }
        }
        for &face in changed.iter() {
            if mesh.alive(face) && self.get(face).is_none() {
                return Err(GeometryErrorKind::triangulation(
                    "refinement could not label a changed face",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq)]
struct ConstraintCircle {
    sites: (usize, usize),
    corners: [XY; 2],
    envelope: AABB<[f64; 2]>,
}

impl RTreeObject for ConstraintCircle {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.envelope
    }
}

impl ConstraintCircle {
    fn new(coordinates: &CdtCoordinates, start: usize, end: usize) -> Result<Self> {
        let sites = ordered(start, end);
        let [start_x, start_y] = coordinates.exact_xy(coordinates.site_query(sites.0));
        let [end_x, end_y] = coordinates.exact_xy(coordinates.site_query(sites.1));
        let dx = end_x.clone().try_subtract(start_x.clone())?.abs();
        let dy = end_y.clone().try_subtract(start_y.clone())?.abs();
        // `(|dx| + |dy|) / 2` is a conservative upper bound for the exact
        // diametral-circle radius, so the tree can only add candidates.
        let radius = dx
            .try_add(dy)?
            .try_product(&exact::ExactDyadic::from_f64(0.5))?;
        let (low_x, high_x) = if start_x.cmp(&end_x).is_le() {
            (start_x, end_x)
        } else {
            (end_x, start_x)
        };
        let (low_y, high_y) = if start_y.cmp(&end_y).is_le() {
            (start_y, end_y)
        } else {
            (end_y, start_y)
        };
        let min_x = exact::ExactRational::from_dyadic(low_x.try_subtract(radius.clone())?)
            .outward_bounds()?;
        let max_x =
            exact::ExactRational::from_dyadic(high_x.try_add(radius.clone())?).outward_bounds()?;
        let min_y = exact::ExactRational::from_dyadic(low_y.try_subtract(radius.clone())?)
            .outward_bounds()?;
        let max_y = exact::ExactRational::from_dyadic(high_y.try_add(radius)?).outward_bounds()?;
        let envelope = AABB::from_corners([min_x.0.next_down(), min_y.0.next_down()], [
            max_x.1.next_up(),
            max_y.1.next_up(),
        ]);
        let mut corners = [coordinates.site_xy(sites.0), coordinates.site_xy(sites.1)];
        if coordinates
            .compare(
                coordinates.site_query(sites.1),
                coordinates.site_query(sites.0),
            )
            .is_lt()
        {
            corners.swap(0, 1);
        }
        Ok(Self {
            sites,
            corners,
            envelope,
        })
    }
}

struct ConstraintIndex {
    tree: RTree<ConstraintCircle>,
}

impl ConstraintIndex {
    fn new(pieces: &ConstraintPieces, coordinates: &CdtCoordinates) -> Result<Self> {
        let circles = pieces
            .values()
            .map(|piece| ConstraintCircle::new(coordinates, piece.start_site, piece.end_site))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            tree: RTree::bulk_load(circles),
        })
    }

    fn encroached(
        &self,
        mesh: &Mesh,
        coordinates: &CdtCoordinates,
        center: XY,
    ) -> Result<Option<HalfedgeId>> {
        let query = coordinates.point_query(center);
        let bounds = coordinates.axis_bounds(query)?;
        let [(min_x, max_x), (min_y, max_y)] = bounds;
        let envelope = AABB::from_corners([min_x, min_y], [max_x, max_y]);
        let mut selected = None;
        for circle in self.tree.locate_in_envelope_intersecting(envelope) {
            if !coordinates.in_diametral_circle(circle.sites.0, circle.sites.1, query)? {
                continue;
            }
            let replace = selected.is_none_or(|previous: &ConstraintCircle| {
                compare_constraint_order(coordinates, circle.sites, previous.sites).is_lt()
            });
            if replace {
                selected = Some(circle);
            }
        }
        selected
            .map(|circle| {
                mesh.edge_slot(circle.sites.0, circle.sites.1)
                    .ok_or_else(|| {
                        GeometryErrorKind::triangulation(
                            "refinement constraint index references an absent edge",
                        )
                    })
            })
            .transpose()
    }

    fn remove(&mut self, coordinates: &CdtCoordinates, piece: &ConstraintPiece) -> Result<()> {
        let circle = ConstraintCircle::new(coordinates, piece.start_site, piece.end_site)?;
        self.tree.remove(&circle);
        Ok(())
    }

    fn insert(&mut self, coordinates: &CdtCoordinates, start: usize, end: usize) -> Result<()> {
        self.tree
            .insert(ConstraintCircle::new(coordinates, start, end)?);
        Ok(())
    }
}

fn compare_constraint_order(
    coordinates: &CdtCoordinates,
    left: (usize, usize),
    right: (usize, usize),
) -> Ordering {
    let canonical = |(start, end)| {
        if coordinates
            .compare(coordinates.site_query(start), coordinates.site_query(end))
            .is_le()
        {
            (start, end)
        } else {
            (end, start)
        }
    };
    let (left_start, left_end) = canonical(left);
    let (right_start, right_end) = canonical(right);
    coordinates
        .compare(
            coordinates.site_query(left_start),
            coordinates.site_query(right_start),
        )
        .then_with(|| {
            coordinates.compare(
                coordinates.site_query(left_end),
                coordinates.site_query(right_end),
            )
        })
}

impl FaceLabels {
    fn new(count: usize) -> Self {
        Self(vec![None; count])
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "FaceId is enumerated from the same live mesh whose face count sizes this label table"
    )]
    fn get(&self, face: FaceId) -> Option<bool> {
        self.0[face.index()]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "FaceId is enumerated from the same live mesh whose face count sizes this label table"
    )]
    fn set(&mut self, face: FaceId, value: bool) {
        self.0[face.index()] = Some(value);
    }

    fn into_inside(self) -> Vec<bool> {
        self.0
            .into_iter()
            .map(|value| value.unwrap_or(false))
            .collect()
    }
}

impl ConstrainedMesh {
    #[expect(
        clippy::indexing_slicing,
        reason = "HalfedgeId comes from this mesh and the constraint column is admitted to its edge count"
    )]
    fn is_constrained(&self, edge: HalfedgeId) -> bool {
        self.constrained[edge.index()]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "HalfedgeId comes from this mesh and the constraint column is admitted to its edge count"
    )]
    const fn set_constrained(flags: &mut [bool], edge: HalfedgeId, value: bool) {
        flags[edge.index()] = value;
    }

    /// Delaunay-triangulate `points`, then force every `edges` pair in.
    #[cfg(test)]
    pub(super) fn build(points: &[Point], edges: &[[usize; 2]]) -> Option<Self> {
        let sources: Vec<_> = edges
            .iter()
            .enumerate()
            .map(|(id, &[start_site, end_site])| SourceConstraint {
                id,
                start: points[start_site],
                end: points[end_site],
                start_site,
                end_site,
                start_occurrence: start_site,
                end_occurrence: end_site,
            })
            .collect();
        Self::build_with_sources(
            points,
            &sources,
            vec![SourceRingStart::new(0, RingOwner::new(0, 0))],
            CdtCoordinates::raw(points, points),
        )
        .ok()
        .flatten()
    }

    #[cfg(test)]
    pub(super) fn build_with_sources(
        points: &[Point],
        sources: &[SourceConstraint],
        source_rings: Vec<SourceRingStart>,
        coordinates: CdtCoordinates,
    ) -> Result<Option<Self>> {
        Self::build_with_restrictions(points, sources, &[], source_rings, coordinates)
    }

    pub(super) fn build_with_restrictions(
        points: &[Point],
        sources: &[SourceConstraint],
        restrictions: &[RestrictionConstraint],
        source_rings: Vec<SourceRingStart>,
        coordinates: CdtCoordinates,
    ) -> Result<Option<Self>> {
        let Some(mesh) = Mesh::try_delaunay(&coordinates, points.len())? else {
            return Ok(None);
        };
        let constrained = vec![false; mesh.edge_count()];
        let source_constraints = sources.to_vec();
        let retained_restrictions = restrictions.to_vec();
        let constraint_count = sources
            .len()
            .checked_add(retained_restrictions.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let constraint_pieces = ConstraintPieces::new(constraint_count)?;
        let mut provenance = points
            .iter()
            .map(|_| NodeProvenance::SourceSite)
            .collect::<Vec<_>>();
        for restriction in restrictions {
            for site in [restriction.start_site, restriction.end_site] {
                if coordinates.retained_site_location(site).is_none() {
                    let provenance = provenance.get_mut(site).ok_or_else(|| {
                        GeometryErrorKind::triangulation(
                            "chart restriction endpoint is outside the initial mesh sites",
                        )
                    })?;
                    *provenance = NodeProvenance::ChartRestrictionSite;
                }
            }
        }
        let mut this = Self {
            mesh,
            constrained,
            coordinates,
            source_constraints,
            restrictions: retained_restrictions,
            source_rings,
            constraint_pieces,
            provenance,
            split_certificates: Vec::new(),
        };
        for source in sources {
            this.insert_constraint(
                ConstraintPiece {
                    constraint: source.id,
                    start_site: source.start_site,
                    end_site: source.end_site,
                    lo: exact::ExactRational::zero(),
                    hi: exact::ExactRational::one(),
                },
                &mut PieceCursor::Replace(source.id),
            )?;
        }
        let restriction_base = sources.len();
        for restriction in restrictions {
            let slot = restriction_base
                .checked_add(restriction.id)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            this.insert_constraint(
                ConstraintPiece {
                    constraint: slot,
                    start_site: restriction.start_site,
                    end_site: restriction.end_site,
                    lo: exact::ExactRational::zero(),
                    hi: exact::ExactRational::one(),
                },
                &mut PieceCursor::Replace(slot),
            )?;
        }
        Ok(Some(this))
    }

    /// Force the segment `u -> v` to be an edge, splitting it at any vertex it
    /// passes through exactly, and flipping every edge it crosses.
    fn insert_constraint(
        &mut self,
        piece: ConstraintPiece,
        cursor: &mut PieceCursor,
    ) -> Result<Vec<FaceId>> {
        let mut legalization = Vec::new();
        self.recover_constraint(piece, cursor, &mut legalization)?;
        if legalization.is_empty() {
            return Ok(Vec::new());
        }
        let mut changed: Vec<_> = legalization.iter().map(|edge| edge.face()).collect();
        let coordinates = &self.coordinates;
        changed.extend(self.mesh.try_legalize_constrained_from(
            coordinates,
            &mut self.constrained,
            legalization,
        )?);
        Ok(changed)
    }

    fn recover_constraint(
        &mut self,
        piece: ConstraintPiece,
        cursor: &mut PieceCursor,
        legalization: &mut Vec<HalfedgeId>,
    ) -> Result<()> {
        let ConstraintPiece {
            constraint,
            start_site: u,
            end_site: v,
            lo,
            hi,
        } = piece;
        if u == v {
            return Ok(());
        }
        if let Some(slot) = self.mesh.edge_slot(u, v) {
            self.constraint_pieces.record(
                ConstraintPiece {
                    constraint,
                    start_site: u,
                    end_site: v,
                    lo,
                    hi,
                },
                cursor,
            );
            self.mark(slot);
            return Ok(());
        }
        // Walk the fan around `u` to find the triangle the segment leaves
        // through, or a vertex it passes through exactly.
        let (pu, pv) = (
            self.coordinates.site_query(u),
            self.coordinates.site_query(v),
        );
        let mut crossing = Vec::new();
        let mut start = None;
        for slot in self.mesh.outgoing(u) {
            let a = self.mesh.edge_end(slot);
            let b = self.mesh.edge_apex(slot);
            // Segment u->v exits triangle (u, a, b) through edge a->b when v
            // is left of u->a and right of u->b (strictly), or through a
            // vertex when collinear.
            let qa = self.coordinates.site_query(a);
            let qb = self.coordinates.site_query(b);
            let sa = self.coordinates.orient([pu, qa, pv])?;
            let sb = self.coordinates.orient([pu, qb, pv])?;
            if sa == ExactSign::Zero && self.coordinates.between(pu, pv, qa) {
                let split = self
                    .coordinates
                    .interpolate_source_parameter(&lo, &hi, pu, pv, qa)?;
                self.certify_constraint_boundary_point(constraint, &split, qa)?;
                self.recover_constraint(
                    ConstraintPiece {
                        constraint,
                        start_site: u,
                        end_site: a,
                        lo,
                        hi: split.clone(),
                    },
                    cursor,
                    legalization,
                )?;
                self.recover_constraint(
                    ConstraintPiece {
                        constraint,
                        start_site: a,
                        end_site: v,
                        lo: split,
                        hi,
                    },
                    cursor,
                    legalization,
                )?;
                return Ok(());
            }
            if sb == ExactSign::Zero && self.coordinates.between(pu, pv, qb) {
                let split = self
                    .coordinates
                    .interpolate_source_parameter(&lo, &hi, pu, pv, qb)?;
                self.certify_constraint_boundary_point(constraint, &split, qb)?;
                self.recover_constraint(
                    ConstraintPiece {
                        constraint,
                        start_site: u,
                        end_site: b,
                        lo,
                        hi: split.clone(),
                    },
                    cursor,
                    legalization,
                )?;
                self.recover_constraint(
                    ConstraintPiece {
                        constraint,
                        start_site: b,
                        end_site: v,
                        lo: split,
                        hi,
                    },
                    cursor,
                    legalization,
                )?;
                return Ok(());
            }
            if sa == ExactSign::Positive && sb == ExactSign::Negative {
                start = Some(slot.next()); // the edge a -> b
                break;
            }
        }
        let Some(mut edge) = start else {
            return Err(GeometryErrorKind::triangulation(
                "constraint walk could not leave its start vertex",
            ));
        };
        // March across triangles: `edge` is the crossed edge of the current
        // triangle, running from its endpoint right of `u -> v` to the one on
        // the left; enter its twin's triangle and find the next exit. A
        // straight segment enters each triangle at most once.
        loop {
            if self.is_constrained(edge) {
                return Err(GeometryErrorKind::triangulation(
                    "constrained triangulation boundaries cross",
                ));
            }
            let twin = self.mesh.twin(edge);
            if twin == EMPTY || crossing.len() == self.mesh.triangle_count() {
                return Err(GeometryErrorKind::triangulation(
                    "constraint walk left the triangulation",
                ));
            }
            let (a, b) = self.mesh.edge_vertices(edge);
            crossing.push((edge, ordered(a, b)));
            let twin = HalfedgeId::from_index(twin);
            let apex = self.mesh.edge_apex(twin);
            if apex == v {
                break;
            }
            let pa = self.coordinates.site_query(apex);
            let s = self.coordinates.orient([pu, pv, pa])?;
            if s == ExactSign::Zero {
                if !self.coordinates.between(pu, pv, pa) {
                    return Err(GeometryErrorKind::triangulation(
                        "constraint walk met a collinear vertex outside the constraint",
                    ));
                }
                let split = self
                    .coordinates
                    .interpolate_source_parameter(&lo, &hi, pu, pv, pa)?;
                self.certify_constraint_boundary_point(constraint, &split, pa)?;
                // Passes exactly through `apex`: constrain u->apex by flipping
                // what we have so far, then continue from apex.
                self.flip_until_clear(u, apex, crossing, legalization)?;
                self.recover_constraint(
                    ConstraintPiece {
                        constraint,
                        start_site: u,
                        end_site: apex,
                        lo,
                        hi: split.clone(),
                    },
                    cursor,
                    legalization,
                )?;
                self.recover_constraint(
                    ConstraintPiece {
                        constraint,
                        start_site: apex,
                        end_site: v,
                        lo: split,
                        hi,
                    },
                    cursor,
                    legalization,
                )?;
                return Ok(());
            }
            // The twin runs left to right, so its next edge leaves the right
            // endpoint and its previous edge enters the left one. An apex on
            // the left makes the next edge the crossed one; on the right, the
            // previous.
            edge = if s == ExactSign::Positive {
                twin.next()
            } else {
                twin.prev()
            };
        }
        self.flip_until_clear(u, v, crossing, legalization)?;
        self.constraint_pieces.record(
            ConstraintPiece {
                constraint,
                start_site: u,
                end_site: v,
                lo,
                hi,
            },
            cursor,
        );
        Ok(())
    }

    fn constraint_sites(&self, constraint: usize) -> Result<(usize, usize)> {
        let source_count = self.source_constraints.len();
        if let Some(source) = self.source_constraints.get(constraint) {
            return Ok((source.start_site, source.end_site));
        }
        self.restrictions
            .get(constraint.checked_sub(source_count).ok_or_else(|| {
                GeometryErrorKind::triangulation("constraint ID is outside its root table")
            })?)
            .map(|restriction| (restriction.start_site, restriction.end_site))
            .ok_or_else(|| {
                GeometryErrorKind::triangulation("constraint ID is outside its root table")
            })
    }

    fn certify_constraint_boundary_point(
        &self,
        constraint: usize,
        parameter: &exact::ExactRational,
        point: CdtQuery,
    ) -> Result<()> {
        let (start_site, end_site) = self.constraint_sites(constraint)?;
        let start = self.coordinates.site_query(start_site);
        let end = self.coordinates.site_query(end_site);
        if self.coordinates.orient([start, end, point])? == ExactSign::Zero {
            return Ok(());
        }
        let point_xy = self.coordinates.proposal_xy(point);
        let half_ulp = split_half_ulp_bounds(point_xy)?;
        if !split_deviation_within(&self.coordinates, start, end, parameter, point, &half_ulp)? {
            return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
        }
        Ok(())
    }

    /// Flip the crossed edges until none cross `u -> v`, then mark the edge.
    fn flip_until_clear(
        &mut self,
        u: usize,
        v: usize,
        mut crossing: Vec<(HalfedgeId, (usize, usize))>,
        legalization: &mut Vec<HalfedgeId>,
    ) -> Result<()> {
        let (pu, pv) = (
            self.coordinates.site_query(u),
            self.coordinates.site_query(v),
        );
        let mut steps = 0_usize;
        while let Some((candidate, pair)) = crossing.pop() {
            steps = steps.saturating_add(1);
            if steps >= 1_000_000 {
                return Err(GeometryErrorKind::triangulation(
                    "constraint insertion did not converge",
                ));
            }
            let (start, end) = self.mesh.edge_vertices(candidate);
            let edge = if ordered(start, end) == pair {
                Some(candidate)
            } else {
                self.mesh
                    .edge_slot(pair.0, pair.1)
                    .or_else(|| self.mesh.edge_slot(pair.1, pair.0))
            };
            let Some(edge) = edge else {
                // A flip removed this diagonal; its replacement was queued
                // only if it still crosses the constraint.
                continue;
            };
            let (a, b) = self.mesh.edge_vertices(edge);
            if self.is_constrained(edge) {
                return Err(GeometryErrorKind::triangulation(
                    "constrained triangulation boundaries cross",
                ));
            }
            let twin = self.mesh.twin(edge);
            if twin == EMPTY {
                return Err(GeometryErrorKind::triangulation(
                    "constraint walk left the triangulation",
                ));
            }
            let twin = HalfedgeId::from_index(twin);
            let c = self.mesh.edge_apex(edge);
            let d = self.mesh.edge_apex(twin);
            let first_a = self.coordinates.orient([
                self.coordinates.site_query(c),
                self.coordinates.site_query(d),
                self.coordinates.site_query(a),
            ])?;
            let first_b = self.coordinates.orient([
                self.coordinates.site_query(c),
                self.coordinates.site_query(d),
                self.coordinates.site_query(b),
            ])?;
            let second_c = self.coordinates.orient([
                self.coordinates.site_query(a),
                self.coordinates.site_query(b),
                self.coordinates.site_query(c),
            ])?;
            let second_d = self.coordinates.orient([
                self.coordinates.site_query(a),
                self.coordinates.site_query(b),
                self.coordinates.site_query(d),
            ])?;
            if !opposite_nonzero(first_a, first_b) || !opposite_nonzero(second_c, second_d) {
                crossing.insert(0, (edge, pair));
                continue;
            }
            let new_edge = self.mesh.flip_carrying(edge, &mut self.constrained, false);
            let twin = HalfedgeId::from_index(self.mesh.twin(new_edge));
            // Recovery can make any of the six affected slots illegal. Defer
            // Lawson flips until the complete source chain has been constrained.
            legalization.extend([
                new_edge,
                new_edge.next(),
                new_edge.prev(),
                twin,
                twin.next(),
                twin.prev(),
            ]);
            let (new_start, new_end) = self.mesh.edge_vertices(new_edge);
            if self.coordinates.segments_properly_cross([
                pu,
                pv,
                self.coordinates.site_query(new_start),
                self.coordinates.site_query(new_end),
            ])? {
                crossing.insert(0, (new_edge, ordered(new_start, new_end)));
            }
        }
        let Some(slot) = self.mesh.edge_slot(u, v) else {
            return Err(GeometryErrorKind::triangulation(
                "constraint insertion did not produce an edge",
            ));
        };
        self.mark(slot);
        Ok(())
    }

    fn mark(&mut self, slot: HalfedgeId) {
        Self::set_constrained(&mut self.constrained, slot, true);
        let twin = self.mesh.twin(slot);
        if twin != EMPTY {
            Self::set_constrained(&mut self.constrained, HalfedgeId::from_index(twin), true);
        }
    }

    /// Faces inside the constraint polygon: the hull faces are outside, and
    /// crossing a constraint toggles, crossing anything else does not.
    pub(super) fn inside_faces(&self) -> Vec<bool> {
        #[cfg(test)]
        update_work(|counters| {
            counters.inside_label_passes = counters.inside_label_passes.saturating_add(1);
        });
        let count = self.mesh.triangle_count();
        let mut labels = FaceLabels::new(count);
        let mut queue = VecDeque::with_capacity(count);
        // Seed from the hull: beyond a hull edge is the outside, so a face is
        // outside unless that hull edge is itself a constraint, in which case
        // the polygon's boundary runs along the hull and the face is inside.
        for edge in self.mesh.edge_ids() {
            let face = edge.face();
            if self.mesh.twin(edge) == EMPTY && self.mesh.alive(face) && labels.get(face).is_none()
            {
                let inside = self.is_constrained(edge);
                labels.set(face, inside);
                queue.push_back((face, inside));
            }
        }
        while let Some((face, inside)) = queue.pop_front() {
            for side in FaceSide::ALL {
                let edge = HalfedgeId::in_face(face, side);
                let twin = self.mesh.twin(edge);
                if twin == EMPTY {
                    continue;
                }
                let neighbor = HalfedgeId::from_index(twin).face();
                if labels.get(neighbor).is_none() {
                    let inside = if self.is_constrained(edge) {
                        !inside
                    } else {
                        inside
                    };
                    labels.set(neighbor, inside);
                    queue.push_back((neighbor, inside));
                }
            }
        }
        labels.into_inside()
    }

    /// Ruppert refinement of the inside faces to `min_angle` degrees and/or
    /// `max_area`, inserting at most `limit` vertices. `Ok(false)` means the
    /// budget or a topological placement ran out before the mesh met the
    /// request; exact construction failures are returned as `Err`.
    pub(super) fn refine(
        &mut self,
        points: &mut Vec<Point>,
        min_angle: Option<f64>,
        max_area: Option<f64>,
        limit: usize,
    ) -> Result<bool> {
        let inside = self.inside_faces();
        let mut bad = BinaryHeap::new();
        #[cfg(test)]
        update_work(|counters| {
            counters.bad_face_scans = counters.bad_face_scans.saturating_add(1);
        });
        for (face, &kept) in self.mesh.face_ids().zip(&inside) {
            if kept
                && self.mesh.alive(face)
                && let Some(entry) = BadTriangle::new(
                    face,
                    self.mesh.triangle(face),
                    &self.coordinates,
                    min_angle,
                    max_area,
                )?
            {
                bad.push(entry);
            }
        }
        if bad.is_empty() {
            return Ok(true);
        }
        let mut faces = FaceLabels::from_inside(&self.mesh, &inside);
        let mut circles = ConstraintIndex::new(&self.constraint_pieces, &self.coordinates)?;
        let mut inserted = 0_usize;
        while let Some(entry) = bad.pop() {
            let face = entry.face;
            if !self.mesh.alive(face)
                || self.mesh.triangle(face) != entry.triangle
                || faces.get(face) != Some(true)
            {
                continue;
            }
            if inserted >= limit {
                return Ok(false);
            }
            // No finite binary64 can represent this center. Ruppert can
            // still make progress with the explicitly chosen longest-edge
            // midpoint; this is not a recovery for construction errors.
            let (center, center_construction) =
                if let Some(center) = self.coordinates.circumcenter(entry.triangle)? {
                    (center, RefinementConstruction::Circumcenter)
                } else {
                    let (midpoint, edge) =
                        longest_edge_midpoint_for(&self.coordinates, entry.triangle)?;
                    (midpoint, RefinementConstruction::LongestEdgeMidpoint(edge))
                };
            // A constrained edge or the hull stops this center's location walk.
            // Rounded source splits carry a half-ULP incidence certificate.
            let encroached = circles.encroached(&self.mesh, &self.coordinates, center)?;
            let center_query = self.coordinates.point_query(center);
            let placed = if let Some(edge) = encroached {
                self.prepare_split(&circles, edge)?
            } else {
                match self.mesh.locate_bounded(
                    &self.coordinates,
                    face,
                    center_query,
                    &self.constrained,
                )? {
                    LocationOutcome::Located(Located::OnEdge(edge))
                        if self.is_constrained(edge) =>
                    {
                        self.prepare_split(&circles, edge)?
                    },
                    LocationOutcome::Located(located) => Some(PreparedSplit {
                        candidate: center,
                        located,
                        constraint: None,
                        construction: Some(center_construction),
                    }),
                    LocationOutcome::Blocked(edge) => self.prepare_split(&circles, edge)?,
                }
            };
            let Some(PreparedSplit {
                candidate,
                located,
                constraint,
                construction,
            }) = placed
            else {
                return Ok(false);
            };
            if matches!(located, Located::OnVertex) {
                return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
            }
            let site = points.len();
            if self.coordinates.sites.len() != site {
                return Err(GeometryErrorKind::triangulation(
                    "refinement coordinate table lost its mesh site alignment",
                ));
            }
            if let Some(split) = &constraint {
                let edge = self
                    .mesh
                    .edge_slot(split.piece.start_site, split.piece.end_site)
                    .ok_or_else(|| {
                        GeometryErrorKind::triangulation(
                            "constrained edge disappeared before splitting",
                        )
                    })?;
                Self::set_constrained(&mut self.constrained, edge, false);
                let twin = self.mesh.twin(edge);
                if twin != EMPTY {
                    Self::set_constrained(
                        &mut self.constrained,
                        HalfedgeId::from_index(twin),
                        false,
                    );
                }
                circles.remove(&self.coordinates, &split.piece)?;
                self.constraint_pieces.reserve(2);
                self.split_certificates.reserve(1);
            }
            points.reserve(1);
            self.coordinates.reserve_generated();
            self.provenance.reserve(1);
            let admitted_site = self.coordinates.push_generated(candidate);
            debug_assert_eq!(admitted_site, site);
            points.push(Point::new_raw_xy(candidate.x, candidate.y));
            self.mesh.admit_vertex(site);
            let constraint = if let Some(split) = constraint {
                let certificate = self.split_certificates.len();
                let parameter = split.certificate.parameter.clone();
                self.split_certificates.push(split.certificate);
                Some((split.piece, parameter, certificate))
            } else {
                None
            };
            let provenance = if let Some((_, _, certificate)) = &constraint {
                NodeProvenance::ConstraintSplit {
                    certificate: *certificate,
                }
            } else {
                let Some(construction) = construction else {
                    return Err(GeometryErrorKind::triangulation(
                        "refinement candidate has no construction recipe",
                    ));
                };
                NodeProvenance::InteriorRefinement {
                    parent_face: face.index(),
                    parent_sites: entry.triangle,
                    construction,
                }
            };
            self.provenance.push(provenance);
            let mut changed = if let Located::Outside(visible_edge) = located {
                let insertion = self.mesh.insert_outside(
                    &self.coordinates,
                    site,
                    visible_edge,
                    &mut self.constrained,
                )?;
                let mut changed = insertion.changed;
                changed.extend(self.mesh.try_legalize_constrained_from(
                    &self.coordinates,
                    &mut self.constrained,
                    insertion.legalization,
                )?);
                changed
            } else {
                self.mesh.insert_constrained(
                    &self.coordinates,
                    site,
                    located,
                    &mut self.constrained,
                )?
            };
            if let Some((piece, parameter, _)) = constraint {
                let mut cursor = self
                    .constraint_pieces
                    .remove(ordered(piece.start_site, piece.end_site))
                    .expect("the split owns an indexed constraint piece");
                changed.extend(self.insert_constraint(
                    ConstraintPiece {
                        constraint: piece.constraint,
                        start_site: piece.start_site,
                        end_site: site,
                        lo: piece.lo.clone(),
                        hi: parameter.clone(),
                    },
                    &mut cursor,
                )?);
                changed.extend(self.insert_constraint(
                    ConstraintPiece {
                        constraint: piece.constraint,
                        start_site: site,
                        end_site: piece.end_site,
                        lo: parameter,
                        hi: piece.hi,
                    },
                    &mut cursor,
                )?);
                circles.insert(&self.coordinates, piece.start_site, site)?;
                circles.insert(&self.coordinates, site, piece.end_site)?;
            }
            faces.refresh(&self.mesh, &self.constrained, &mut changed)?;
            for &face in &changed {
                if self.mesh.alive(face)
                    && faces.get(face) == Some(true)
                    && let Some(entry) = BadTriangle::new(
                        face,
                        self.mesh.triangle(face),
                        &self.coordinates,
                        min_angle,
                        max_area,
                    )?
                {
                    bad.push(entry);
                }
            }
            if !changed.contains(&entry.face)
                && self.mesh.alive(entry.face)
                && self.mesh.triangle(entry.face) == entry.triangle
                && faces.get(entry.face) == Some(true)
            {
                bad.push(entry);
            }
            inserted = inserted.wrapping_add(1);
        }
        Ok(true)
    }

    fn prepare_split(
        &self,
        circles: &ConstraintIndex,
        e: HalfedgeId,
    ) -> Result<Option<PreparedSplit>> {
        let (start, end) = self.mesh.edge_vertices(e);
        if self.is_constrained(e) {
            let Some(piece) = self.constraint_pieces.get(ordered(start, end)) else {
                return Err(GeometryErrorKind::triangulation(
                    "constrained edge has no retained root interval",
                ));
            };
            let requested = piece.lo.try_midpoint(&piece.hi)?;
            let certificate = self.certify_constraint_split(piece, &requested)?;
            let candidate = certificate.point;
            let start = self.coordinates.site_query(piece.start_site);
            let end = self.coordinates.site_query(piece.end_site);
            let candidate_query = self.coordinates.point_query(candidate);
            // Splitting the current edge in its interior preserves the mesh
            // embedding; bent rounded segments require the incidence query.
            if self.coordinates.orient([start, end, candidate_query])? != ExactSign::Zero
                || !self.coordinates.between(start, end, candidate_query)
            {
                self.certify_split_incidence(circles, piece, candidate)?;
            }
            let located = self.locate_boundary_split(e, candidate)?;
            return Ok(Some(PreparedSplit {
                candidate,
                located,
                constraint: Some(PreparedConstraintSplit {
                    piece: piece.clone(),
                    certificate,
                }),
                construction: None,
            }));
        }
        let midpoint = self.coordinates.rounded_midpoint(start, end)?;
        let located = match self.mesh.locate_bounded(
            &self.coordinates,
            e.face(),
            self.coordinates.point_query(midpoint),
            &self.constrained,
        )? {
            LocationOutcome::Located(Located::OnEdge(slot))
                if slot == e || slot.index() == self.mesh.twin(e) =>
            {
                Located::OnEdge(e)
            },
            LocationOutcome::Located(Located::Inside(face)) => Located::Inside(face),
            LocationOutcome::Located(
                Located::OnVertex | Located::OnEdge(_) | Located::Outside(_),
            )
            | LocationOutcome::Blocked(_) => {
                return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
            },
        };
        Ok(Some(PreparedSplit {
            candidate: midpoint,
            located,
            constraint: None,
            construction: Some(RefinementConstruction::EdgeMidpoint([start, end])),
        }))
    }

    fn certify_constraint_split(
        &self,
        piece: &ConstraintPiece,
        requested: &exact::ExactRational,
    ) -> Result<CertifiedBoundarySplit> {
        if requested.try_cmp(&piece.lo)? != std::cmp::Ordering::Greater
            || requested.try_cmp(&piece.hi)? != std::cmp::Ordering::Less
        {
            return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
        }
        let (root_start, root_end) = self.constraint_sites(piece.constraint)?;
        let [start_x, start_y] = self
            .coordinates
            .exact_xy(self.coordinates.site_query(root_start));
        let [end_x, end_y] = self
            .coordinates
            .exact_xy(self.coordinates.site_query(root_end));
        let candidate = XY::new(
            round_source_coordinate(&exact_source_coordinate(&start_x, &end_x, requested)?)?,
            round_source_coordinate(&exact_source_coordinate(&start_y, &end_y, requested)?)?,
        );
        let half_ulp = split_half_ulp_bounds(candidate)?;
        let candidate_query = self.coordinates.point_query(candidate);
        if self.coordinates.orient([
            self.coordinates.site_query(root_start),
            self.coordinates.site_query(root_end),
            candidate_query,
        ])? != ExactSign::Zero
            && !split_deviation_within(
                &self.coordinates,
                self.coordinates.site_query(root_start),
                self.coordinates.site_query(root_end),
                requested,
                candidate_query,
                &half_ulp,
            )?
        {
            return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
        }
        Ok(CertifiedBoundarySplit {
            constraint: piece.constraint,
            parameter: requested.clone(),
            point: candidate,
            half_ulp,
        })
    }

    fn certify_split_incidence(
        &self,
        circles: &ConstraintIndex,
        piece: &ConstraintPiece,
        candidate: XY,
    ) -> Result<()> {
        let start = self.coordinates.site_query(piece.start_site);
        let end = self.coordinates.site_query(piece.end_site);
        let candidate_query = self.coordinates.point_query(candidate);
        let old_pair = ordered(piece.start_site, piece.end_site);
        let subsegments = [
            (start, candidate_query, piece.start_site),
            (candidate_query, end, piece.end_site),
        ];
        let envelope = self.coordinates.envelope(&[start, candidate_query, end])?;
        for circle in circles.tree.locate_in_envelope_intersecting(envelope) {
            let (other_start, other_end) = circle.sites;
            if ordered(other_start, other_end) == old_pair {
                continue;
            }
            let (a, b) = (
                self.coordinates.site_query(other_start),
                self.coordinates.site_query(other_end),
            );
            if self.coordinates.on_segment(a, b, candidate_query)? {
                return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
            }
            for (left, right, shared_site) in subsegments {
                if self
                    .coordinates
                    .segments_properly_cross([left, right, a, b])?
                {
                    return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
                }
                if (other_start != shared_site && self.coordinates.on_segment(left, right, a)?)
                    || (other_end != shared_site && self.coordinates.on_segment(left, right, b)?)
                {
                    return Err(GeometryErrorKind::unrepresentable_output("triangulate"));
                }
            }
        }
        Ok(())
    }

    /// Locate a rounded source split while permitting traversal across only the
    /// source edge it replaces.
    fn locate_boundary_split(&self, e: HalfedgeId, point: XY) -> Result<Located> {
        let twin = self.mesh.twin(e);
        let twin = (twin != EMPTY).then(|| HalfedgeId::from_index(twin));
        let point = self.coordinates.point_query(point);
        match self
            .mesh
            .locate_bounded(&self.coordinates, e.face(), point, &[])?
        {
            LocationOutcome::Located(Located::OnVertex | Located::Outside(_)) => {
                Err(GeometryErrorKind::unrepresentable_output("triangulate"))
            },
            LocationOutcome::Located(Located::Inside(face)) => Ok(Located::Inside(face)),
            LocationOutcome::Located(Located::OnEdge(slot)) if slot == e || twin == Some(slot) => {
                Ok(Located::OnEdge(e))
            },
            LocationOutcome::Located(Located::OnEdge(slot)) if self.is_constrained(slot) => {
                Err(GeometryErrorKind::unrepresentable_output("triangulate"))
            },
            LocationOutcome::Located(Located::OnEdge(slot)) => Ok(Located::OnEdge(slot)),
            LocationOutcome::Blocked(edge) => {
                if self.mesh.twin(edge) == EMPTY {
                    Ok(Located::Outside(edge))
                } else if let Some(edge) = self.mesh.outside_edge(&self.coordinates, point)? {
                    Ok(Located::Outside(edge))
                } else {
                    Err(GeometryErrorKind::unrepresentable_output("triangulate"))
                }
            },
        }
    }

    fn constrained_pairs(&self) -> Result<HashSet<(usize, usize)>> {
        #[cfg(test)]
        update_work(|counters| {
            counters.constraint_reconstruction_passes =
                counters.constraint_reconstruction_passes.saturating_add(1);
        });
        let mut pairs = crate::collections::try_hash_set_with_capacity(self.mesh.edge_count())?;
        for edge in self.mesh.edge_ids() {
            if self.is_constrained(edge) && self.mesh.alive(edge.face()) {
                let (start, end) = self.mesh.edge_vertices(edge);
                pairs.insert(ordered(start, end));
            }
        }
        Ok(pairs)
    }

    fn validate_constraint_chains(
        &self,
        points: &[Point],
        pieces: &[ConstraintPiece],
    ) -> Result<()> {
        let one = exact::ExactRational::one();
        let constrained = self.constrained_pairs()?;
        let mut remaining = pieces;
        let root_count = self
            .source_constraints
            .len()
            .checked_add(self.restrictions.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        for constraint in 0..root_count {
            let count = remaining
                .iter()
                .take_while(|piece| {
                    #[cfg(test)]
                    update_work(|work| {
                        work.source_piece_visits = work.source_piece_visits.saturating_add(1);
                    });
                    piece.constraint == constraint
                })
                .count();
            let (pieces, rest) = remaining.split_at(count);
            remaining = rest;
            if pieces.is_empty() {
                let label = if constraint < self.source_constraints.len() {
                    format!("source constraint {constraint}")
                } else {
                    let restriction = constraint
                        .checked_sub(self.source_constraints.len())
                        .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    format!("chart restriction {restriction}")
                };
                return Err(GeometryErrorKind::triangulation(format!(
                    "{label} has no ordered boundary pieces"
                )));
            }
            let (root_start, root_end) = self.constraint_sites(constraint)?;
            let mut cursor = exact::ExactRational::zero();
            for piece in pieces {
                if piece.lo.try_cmp(&cursor)? != std::cmp::Ordering::Equal
                    || piece.lo.try_cmp(&piece.hi)? != std::cmp::Ordering::Less
                {
                    return Err(GeometryErrorKind::triangulation(format!(
                        "constraint root {constraint} has a non-ordered parameter chain"
                    )));
                }
                if points.get(piece.start_site).is_none() {
                    return Err(GeometryErrorKind::triangulation(
                        "constraint references a missing start site",
                    ));
                }
                if points.get(piece.end_site).is_none() {
                    return Err(GeometryErrorKind::triangulation(
                        "constraint references a missing end site",
                    ));
                }
                validate_piece_endpoint(
                    &self.coordinates,
                    constraint,
                    root_start,
                    root_end,
                    &piece.lo,
                    self.coordinates.site_query(piece.start_site),
                    piece.start_site,
                    &self.provenance,
                    &self.split_certificates,
                )?;
                validate_piece_endpoint(
                    &self.coordinates,
                    constraint,
                    root_start,
                    root_end,
                    &piece.hi,
                    self.coordinates.site_query(piece.end_site),
                    piece.end_site,
                    &self.provenance,
                    &self.split_certificates,
                )?;
                if !constrained.contains(&ordered(piece.start_site, piece.end_site)) {
                    return Err(GeometryErrorKind::triangulation(format!(
                        "constraint root {constraint} does not match its emitted edge"
                    )));
                }
                cursor = piece.hi.clone();
            }
            if cursor.try_cmp(&one)? != std::cmp::Ordering::Equal {
                return Err(GeometryErrorKind::triangulation(format!(
                    "constraint root {constraint} does not cover its parameter interval"
                )));
            }
        }
        if !remaining.is_empty() {
            return Err(GeometryErrorKind::triangulation(
                "constraint pieces contain an unknown root",
            ));
        }
        Ok(())
    }

    pub(super) fn finish_plan(
        mut self,
        points: Vec<Point>,
        refinement: super::CdtRefinement,
        force_xy: bool,
    ) -> Result<ConstrainedMeshPlan> {
        let constraint_pieces = self.constraint_pieces.take_ordered(
            &self.source_constraints,
            &self.restrictions,
            &self.source_rings,
            &self.coordinates,
        )?;
        self.validate_constraint_chains(&points, &constraint_pieces)?;
        let inside = self.inside_faces();
        let face_slot_count = self.mesh.triangle_count();
        let mut selected = Vec::with_capacity(self.mesh.triangle_count());
        for (face, &kept) in self.mesh.face_ids().zip(&inside) {
            if kept && self.mesh.alive(face) {
                selected.push((face, self.mesh.triangle(face)));
            }
        }
        let triangles = selected.iter().map(|(_, sites)| *sites).collect::<Vec<_>>();
        let canonical =
            super::exact_delaunay::canonical_triangle_order(&self.coordinates, &triangles);
        let mut faces = Vec::with_capacity(canonical.len());
        let mut face_uses = Vec::with_capacity(canonical.len());
        let mut source_pieces = Vec::with_capacity(self.source_constraints.len());
        let mut source_uses = Vec::with_capacity(self.source_constraints.len());
        let mut restriction_pieces = Vec::with_capacity(self.restrictions.len());
        let mut piece_by_edge =
            crate::collections::try_hash_map_with_capacity(constraint_pieces.len())?;
        for (piece_index, piece) in constraint_pieces.iter().enumerate() {
            if piece_by_edge
                .insert(ordered(piece.start_site, piece.end_site), piece_index)
                .is_some()
            {
                return Err(GeometryErrorKind::triangulation(
                    "two constraint roots claim one mesh edge",
                ));
            }
            if piece.constraint < self.source_constraints.len() {
                source_pieces.push(SourcePiece {
                    source: piece.constraint,
                    start_site: piece.start_site,
                    end_site: piece.end_site,
                    lo: piece.lo.clone(),
                    hi: piece.hi.clone(),
                });
            } else {
                let restriction_id = piece
                    .constraint
                    .checked_sub(self.source_constraints.len())
                    .ok_or_else(|| {
                        GeometryErrorKind::triangulation(
                            "constraint piece has an invalid restriction root",
                        )
                    })?;
                let restriction = self.restrictions.get(restriction_id).ok_or_else(|| {
                    GeometryErrorKind::triangulation(
                        "constraint piece references a missing chart restriction",
                    )
                })?;
                restriction_pieces.push(RestrictionPiece {
                    restriction: restriction_id,
                    owner: restriction.owner,
                    start_site: piece.start_site,
                    end_site: piece.end_site,
                    start: piece.lo.clone(),
                    end: piece.hi.clone(),
                    use_: restriction_use_for_piece(
                        restriction,
                        piece.start_site,
                        piece.end_site,
                        &self.coordinates,
                    )?,
                });
            }
        }
        let source_piece_count = source_pieces.len();
        for canonical_face in canonical {
            let (live_face, selected_sites) =
                *selected.get(canonical_face.selected_face).ok_or_else(|| {
                    GeometryErrorKind::triangulation(
                        "canonical constrained face lost its live mesh identity",
                    )
                })?;
            let output_sites = canonical_face.output_sites;
            let mut sides = Vec::with_capacity(3);
            for output_side in 0..3 {
                let selected_site_index = canonical_face
                    .output_to_selected
                    .get(output_side)
                    .copied()
                    .ok_or_else(|| {
                        GeometryErrorKind::triangulation(
                            "canonical constrained side has no source map",
                        )
                    })?;
                let selected_side = usize::from(selected_site_index);
                let side = *FaceSide::ALL.get(selected_side).ok_or_else(|| {
                    GeometryErrorKind::triangulation(
                        "canonical constrained side has an invalid source map",
                    )
                })?;
                let halfedge = HalfedgeId::in_face(live_face, side);
                let (start_site, end_site) = self.mesh.edge_vertices(halfedge);
                let expected_start = output_sites.get(output_side).copied().ok_or_else(|| {
                    GeometryErrorKind::triangulation(
                        "canonical constrained face has a missing output site",
                    )
                })?;
                let expected_end = output_sites
                    .get(next_triangle_side(output_side))
                    .copied()
                    .ok_or_else(|| {
                        GeometryErrorKind::triangulation(
                            "canonical constrained face has a missing output side",
                        )
                    })?;
                if (start_site, end_site) != (expected_start, expected_end) {
                    return Err(GeometryErrorKind::triangulation(
                        "canonical constrained side does not match its live halfedge",
                    ));
                }
                let twin_index = self.mesh.twin(halfedge);
                let twin = (twin_index != EMPTY).then(|| HalfedgeId::from_index(twin_index));
                let twin_selected = twin.is_some_and(|twin| {
                    self.mesh.alive(twin.face())
                        && inside.get(twin.face().index()).copied().unwrap_or(false)
                });
                let constrained = self.is_constrained(halfedge);
                let constraint_piece_index =
                    piece_by_edge.get(&ordered(start_site, end_site)).copied();
                let boundary = if let Some(piece_index) = constraint_piece_index {
                    let piece = constraint_pieces.get(piece_index).ok_or_else(|| {
                        GeometryErrorKind::triangulation(
                            "live constrained side references a missing root piece",
                        )
                    })?;
                    if piece.constraint < self.source_constraints.len() {
                        let source = piece.constraint;
                        let forward = (piece.start_site, piece.end_site) == (start_site, end_site);
                        let owner =
                            source_ring_owner(source, &self.source_rings).ok_or_else(|| {
                                GeometryErrorKind::triangulation(
                                    "source piece has no polygon-ring owner",
                                )
                            })?;
                        let (start, end) = if forward {
                            (piece.lo.clone(), piece.hi.clone())
                        } else {
                            (piece.hi.clone(), piece.lo.clone())
                        };
                        let source_use_index = source_uses.len();
                        source_uses.push(CdtSourceUse {
                            source,
                            owner,
                            start,
                            end,
                            forward,
                        });
                        Some(CdtSideBoundaryUse::Source(source_use_index))
                    } else {
                        let restriction_piece = piece_index
                            .checked_sub(source_piece_count)
                            .filter(|&index| index < restriction_pieces.len())
                            .ok_or_else(|| {
                                GeometryErrorKind::triangulation(
                                    "restricted side has no prepared directed use",
                                )
                            })?;
                        Some(CdtSideBoundaryUse::Restriction(restriction_piece))
                    }
                } else {
                    None
                };
                if constrained != boundary.is_some() {
                    return Err(GeometryErrorKind::triangulation(
                        "live constrained side has no matching source or chart-restriction use",
                    ));
                }
                sides.push(CdtSideUse {
                    halfedge: halfedge.index(),
                    start_site,
                    end_site,
                    twin: twin.map(HalfedgeId::index),
                    twin_selected,
                    boundary,
                });
            }
            faces.push(output_sites);
            face_uses.push(CdtFaceUse {
                live_face: live_face.index(),
                selected_sites,
                output_to_selected: canonical_face.output_to_selected,
                sides,
            });
        }
        Ok(ConstrainedMeshPlan {
            vertices: points,
            faces,
            face_uses,
            coordinates: self.coordinates,
            face_slot_count,
            provenance: self.provenance,
            source_constraints: self.source_constraints,
            source_uses,
            restrictions: self.restrictions,
            source_rings: self.source_rings,
            constraint_pieces: source_pieces,
            restriction_pieces,
            split_certificates: self.split_certificates,
            refinement,
            force_xy,
        })
    }
}

const fn ordered(a: usize, b: usize) -> (usize, usize) {
    if a < b { (a, b) } else { (b, a) }
}

pub(super) const fn next_triangle_side(side: usize) -> usize {
    match side {
        0 => 1,
        1 => 2,
        _ => 0,
    }
}

#[cfg(test)]
fn compare_constraint_pieces(
    left: &ConstraintPiece,
    right: &ConstraintPiece,
) -> Result<std::cmp::Ordering> {
    let constraint = left.constraint.cmp(&right.constraint);
    if constraint.is_ne() {
        return Ok(constraint);
    }
    let lo = left.lo.try_cmp(&right.lo)?;
    if lo.is_ne() {
        return Ok(lo);
    }
    let hi = left.hi.try_cmp(&right.hi)?;
    if hi.is_ne() {
        return Ok(hi);
    }
    Ok(left
        .start_site
        .cmp(&right.start_site)
        .then_with(|| left.end_site.cmp(&right.end_site)))
}

fn source_constraints_overlap(
    coordinates: &CdtCoordinates,
    left: &SourceConstraint,
    right: &SourceConstraint,
) -> Result<bool> {
    let (a, b, c, d) = (
        coordinates.source_query(left.start_occurrence),
        coordinates.source_query(left.end_occurrence),
        coordinates.source_query(right.start_occurrence),
        coordinates.source_query(right.end_occurrence),
    );
    Ok(coordinates.orient([a, b, c])? == ExactSign::Zero
        && coordinates.orient([a, b, d])? == ExactSign::Zero
        && (coordinates.between(a, b, c)
            || coordinates.between(a, b, d)
            || coordinates.between(c, d, a)
            || coordinates.between(c, d, b)))
}

fn opposite_nonzero(a: ExactSign, b: ExactSign) -> bool {
    a != ExactSign::Zero && b != ExactSign::Zero && a != b
}

pub(super) fn triangle_min_angle_degrees(a: XY, b: XY, c: XY) -> f64 {
    let side = |p: XY, q: XY| ((p.x - q.x).powi(2) + (p.y - q.y).powi(2)).sqrt();
    let (ab, bc, ca) = (side(a, b), side(b, c), side(c, a));
    let angle = |opposite: f64, s1: f64, s2: f64| {
        (((s1 * s1 + s2 * s2 - opposite * opposite) / (2.0 * s1 * s2)).clamp(-1.0, 1.0)).acos()
    };
    [angle(bc, ab, ca), angle(ca, ab, bc), angle(ab, bc, ca)]
        .into_iter()
        .fold(f64::INFINITY, f64::min)
        .to_degrees()
}

/// The exact circumcenter rounded to nearest. `None` means that the exact
/// center is outside the finite binary64 range; the refinement caller makes
/// the deliberate midpoint choice for that numerical case. Every other error
/// remains an error, including resource failures and mesh invariants.
#[cfg(test)]
fn circumcenter(a: XY, b: XY, c: XY) -> Result<Option<XY>> {
    exact::circumcenter(a, b, c)?.finite_rounding()
}

#[cfg(test)]
fn longest_edge_midpoint(a: XY, b: XY, c: XY) -> XY {
    let length = |p: XY, q: XY| (p.x - q.x).hypot(p.y - q.y);
    let (p, q) = if length(a, b) >= length(b, c) && length(a, b) >= length(c, a) {
        (a, b)
    } else if length(b, c) >= length(c, a) {
        (b, c)
    } else {
        (c, a)
    };
    XY::new(f64::midpoint(p.x, q.x), f64::midpoint(p.y, q.y))
}

fn longest_edge_midpoint_for(
    coordinates: &CdtCoordinates,
    [a, b, c]: [usize; 3],
) -> Result<(XY, [usize; 2])> {
    let [pa, pb, pc] = coordinates.metric_frame([a, b, c])?;
    let length = |left: XY, right: XY| (left.x - right.x).hypot(left.y - right.y);
    let edge = if length(pa, pb) >= length(pb, pc) && length(pa, pb) >= length(pc, pa) {
        (a, b)
    } else if length(pb, pc) >= length(pc, pa) {
        (b, c)
    } else {
        (c, a)
    };
    let edge_sites: [usize; 2] = edge.into();
    let [start, end] = edge_sites;
    Ok((coordinates.rounded_midpoint(start, end)?, edge_sites))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_cdt_refinement_uses_exact_sites_after_chart_rounding() {
        let frame = GeographicFrame::crs84();
        let next = 0.2_f64.next_up();
        let source_points = [
            Point::new_raw_xy(0.2, 0.0),
            Point::new_raw_xy(next, 0.0),
            Point::new_raw_xy(0.2, 1.0),
        ];
        let sites = source_points
            .iter()
            .enumerate()
            .map(|(ordinal, &point)| {
                ChartPointSite::from_lifted_source(frame, point, ordinal, 1).unwrap()
            })
            .collect::<Vec<_>>();
        let coordinates = CdtCoordinates::retained(frame, &sites, &sites).unwrap();

        assert_eq!(
            coordinates.site_xy(0).x,
            coordinates.site_xy(1).x,
            "adjacent retained chart X values have one rounded proposal"
        );
        let triangle = [0, 1, 2];
        let queries = triangle.map(|site| coordinates.site_query(site));
        assert_eq!(coordinates.orient(queries).unwrap(), ExactSign::Positive);
        assert!(
            coordinates
                .triangle_exceeds_area(triangle, 1.0e-18)
                .unwrap()
        );
        assert!(coordinates.circumcenter(triangle).unwrap().is_some());
    }

    #[test]
    fn public_constrained_source_chain_work_is_linear() {
        use pyo3::prelude::*;
        use pyo3::types::{PyDict, PyModule};

        crate::test_support::initialize_python();
        Python::attach(|py| {
            let module = PyModule::new(py, "gometry._lib").unwrap();
            crate::lib(&module).unwrap();
            let options = PyDict::new(py);
            options.set_item("method", "constrained").unwrap();
            for count in [128_u32, 512] {
                let points = (0..count)
                    .map(|index| {
                        let angle = f64::from(index) * std::f64::consts::TAU / f64::from(count);
                        format!("{} {}", angle.cos(), angle.sin())
                    })
                    .collect::<Vec<_>>();
                let ring = points
                    .iter()
                    .chain(points.first())
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(",");
                let polygon = module
                    .getattr("from_wkt")
                    .unwrap()
                    .call1((format!("POLYGON(({ring}))"),))
                    .unwrap();
                let _ = take_refinement_work_counters();
                let triangles = polygon
                    .call_method("triangulate", (), Some(&options))
                    .unwrap();
                assert_eq!(triangles.len().unwrap(), count as usize - 2);
                let work = take_refinement_work_counters();
                assert!(
                    work.source_piece_visits <= count as usize * 2,
                    "{count} source edges inspected {} pieces",
                    work.source_piece_visits
                );
            }
        });
    }

    #[test]
    fn public_refinement_quality_work_scales_with_output() {
        use pyo3::prelude::*;
        use pyo3::types::{PyDict, PyModule};

        crate::test_support::initialize_python();
        Python::attach(|py| {
            let module = PyModule::new(py, "gometry._lib").unwrap();
            crate::lib(&module).unwrap();
            let polygon = module
                .getattr("from_wkt")
                .unwrap()
                .call1(("POLYGON((0 0,2 0,2 2,0 2,0 0))",))
                .unwrap();
            let mut observations = Vec::new();
            for count in [128_u32, 512] {
                let options = PyDict::new(py);
                options.set_item("method", "constrained").unwrap();
                options
                    .set_item("max_area", 4.0 / f64::from(count))
                    .unwrap();
                let _ = take_refinement_work_counters();
                let triangles = polygon
                    .call_method("triangulate", (), Some(&options))
                    .unwrap();
                let output = triangles.len().unwrap();
                assert!(output >= count as usize);
                let work = take_refinement_work_counters();
                assert_eq!(work.bad_face_scans, 1);
                assert_eq!(work.constraint_reconstruction_passes, 1);
                assert!(work.quality_checks <= output * 16, "{work:?}");
                observations.push(work.quality_checks);
            }
            assert!(observations[1] <= observations[0] * 5, "{observations:?}");
        });
    }

    fn points(xy: &[(f64, f64)]) -> Vec<Point> {
        xy.iter().map(|&(x, y)| Point::new_raw_xy(x, y)).collect()
    }

    #[test]
    fn a_constraint_becomes_an_edge_and_survives() {
        // Four corners of a square plus a point off the diagonal that makes
        // the Delaunay diagonal the "wrong" one for the constraint.
        let pts = points(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (2.0, 8.0),
        ]);
        let mesh = ConstrainedMesh::build(&pts, &[[0, 2]]).unwrap();
        assert!(
            mesh.mesh.edge_slot(0, 2).is_some(),
            "the constraint 0-2 is an edge"
        );
        let slot = mesh.mesh.edge_slot(0, 2).unwrap();
        assert!(mesh.constrained[slot.index()]);
    }

    /// The constraint flags mark exactly the constrained vertex pairs, on both
    /// halves of each edge.
    fn assert_flags_match(mesh: &ConstrainedMesh, edges: &[[usize; 2]]) {
        for edge in mesh.mesh.edge_ids() {
            if !mesh.mesh.alive(edge.face()) {
                continue;
            }
            let (start, end) = mesh.mesh.edge_vertices(edge);
            let pair = ordered(start, end);
            let expected = edges.iter().any(|&[u, v]| ordered(u, v) == pair);
            assert_eq!(
                mesh.constrained[edge.index()],
                expected,
                "slot {} pair {pair:?}",
                edge.index()
            );
        }
    }

    #[test]
    fn a_constraint_crossing_many_edges_becomes_an_edge() {
        // Points alternate above and below the axis, so the Delaunay edges
        // between them cross the constraint 0-1 many times.
        let mut xy = vec![(0.0, 0.0), (12.0, 0.0)];
        for step in 1..12 {
            let side = if step % 2 == 0 { 1.0 } else { -1.0 };
            xy.push((f64::from(step), side * (0.5 + 0.1 * f64::from(step % 3))));
        }
        let pts = points(&xy);
        let mesh = ConstrainedMesh::build(&pts, &[[0, 1]]).unwrap();
        assert_flags_match(&mesh, &[[0, 1]]);
    }

    #[test]
    fn flips_carry_earlier_constraint_flags() {
        let pts = points(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (2.0, 8.0),
        ]);
        let edges = [[0, 1], [1, 2], [2, 3], [3, 0], [0, 2]];
        let mesh = ConstrainedMesh::build(&pts, &edges).unwrap();
        assert_flags_match(&mesh, &edges);
    }

    #[test]
    fn crossing_constraints_are_an_error() {
        let pts = points(&[
            (0.0, 0.0),
            (10.0, 10.0),
            (10.0, 0.0),
            (0.0, 10.0),
            (5.0, 1.0),
        ]);
        let result = ConstrainedMesh::build_with_sources(
            &pts,
            &[[0, 1], [2, 3]]
                .iter()
                .enumerate()
                .map(|(id, &[start_site, end_site])| SourceConstraint {
                    id,
                    start: pts[start_site],
                    end: pts[end_site],
                    start_site,
                    end_site,
                    start_occurrence: start_site,
                    end_occurrence: end_site,
                })
                .collect::<Vec<_>>(),
            vec![SourceRingStart::new(0, RingOwner::new(0, 0))],
            CdtCoordinates::raw(&pts, &pts),
        );
        assert!(matches!(
            result.as_ref(),
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::Geometry(GeometryErrorKind::Triangulation(message))
                        if message.as_ref() == "constrained triangulation boundaries cross"
                )
        ));
    }

    #[test]
    fn parity_flood_finds_the_inside_of_a_square_with_a_hole() {
        let outer = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)];
        let hole = [(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0)];
        let pts = points(&outer.iter().chain(hole.iter()).copied().collect::<Vec<_>>());
        let ring = |base: usize| {
            (0..4)
                .map(|i| [base + i, base + (i + 1) % 4])
                .collect::<Vec<_>>()
        };
        let edges: Vec<[usize; 2]> = ring(0).into_iter().chain(ring(4)).collect();
        let mesh = ConstrainedMesh::build(&pts, &edges).unwrap();
        let inside = mesh.inside_faces();
        let xy = |s: usize| pts[s].xy();
        for (face, &kept) in mesh.mesh.face_ids().zip(&inside) {
            if !mesh.mesh.alive(face) {
                continue;
            }
            let [a, b, c] = mesh.mesh.triangle(face);
            let cx = (xy(a).x + xy(b).x + xy(c).x) / 3.0;
            let cy = (xy(a).y + xy(b).y + xy(c).y) / 3.0;
            let in_hole = (4.0..6.0).contains(&cx) && (4.0..6.0).contains(&cy);
            assert_eq!(
                kept,
                !in_hole,
                "face {} centroid ({cx}, {cy})",
                face.index()
            );
        }
    }

    #[test]
    fn refinement_meets_an_area_target_within_budget_and_reports_when_it_cannot() {
        let mut pts = points(&[(0.0, 0.0), (20.0, 0.0), (20.0, 20.0), (0.0, 20.0)]);
        let edges: Vec<[usize; 2]> = (0..4).map(|i| [i, (i + 1) % 4]).collect();
        let mut mesh = ConstrainedMesh::build(&pts, &edges).unwrap();
        assert!(
            mesh.refine(&mut pts, None, Some(25.0), 10_000).unwrap(),
            "a generous budget completes"
        );
        let inside = mesh.inside_faces();
        for (face, &kept) in mesh.mesh.face_ids().zip(&inside) {
            if kept && mesh.mesh.alive(face) {
                let [a, b, c] = mesh.mesh.triangle(face);
                assert!(
                    open_xy_cycle_magnitude(&[pts[a].xy(), pts[b].xy(), pts[c].xy()]).get() <= 25.0
                );
            }
        }
        let mut pts2 = points(&[(0.0, 0.0), (20.0, 0.0), (20.0, 20.0), (0.0, 20.0)]);
        let mut mesh2 = ConstrainedMesh::build(&pts2, &edges).unwrap();
        assert!(
            !mesh2.refine(&mut pts2, None, Some(0.5), 3).unwrap(),
            "three vertices cannot reach 0.5 area"
        );
    }

    #[test]
    fn refinement_keeps_construction_recipes_and_exact_boundary_roots() {
        let mut points = points(&[(0.0, 0.0), (20.0, 0.0), (20.0, 20.0), (0.0, 20.0)]);
        let edges: Vec<[usize; 2]> = (0..4).map(|index| [index, (index + 1) % 4]).collect();
        let mut mesh = ConstrainedMesh::build(&points, &edges).unwrap();
        let refinement = super::super::CdtRefinement::On {
            min_angle: None,
            max_area: Some(crate::Positive::try_new("max_area", 25.0).unwrap()),
        };
        assert!(mesh.refine(&mut points, None, Some(25.0), 10_000).unwrap());

        assert!(mesh.provenance.iter().any(|origin| {
            matches!(origin, NodeProvenance::InteriorRefinement {
                construction: RefinementConstruction::Circumcenter,
                ..
            })
        }));
        let half = exact::ExactRational::zero()
            .try_midpoint(&exact::ExactRational::one())
            .unwrap();
        assert!(mesh.split_certificates.iter().any(|certificate| {
            certificate
                .parameter
                .try_cmp(&half)
                .is_ok_and(|order| order.is_eq())
        }));

        let plan = mesh.finish_plan(points, refinement, true).unwrap();
        assert!(
            plan.provenance
                .iter()
                .any(|origin| { matches!(origin, NodeProvenance::ConstraintSplit { .. }) })
        );
        assert!(
            plan.constraint_pieces
                .iter()
                .any(|piece| { piece.lo.try_cmp(&half).is_ok_and(|order| order.is_eq()) })
        );
        assert!(
            plan.constraint_pieces
                .iter()
                .any(|piece| { piece.hi.try_cmp(&half).is_ok_and(|order| order.is_eq()) })
        );
        plan.validate_certificate().unwrap();
    }

    #[test]
    fn constraint_recovery_composes_exact_root_parameters_through_a_site() {
        let frame = GeographicFrame::crs84();
        let points = points(&[
            (170.0, 0.0),
            (-170.0, 0.0),
            (177.0, 0.0),
            (190.0, 4.0),
            (170.0, 4.0),
        ]);
        let chart_sites = points
            .iter()
            .enumerate()
            .map(|(ordinal, &point)| {
                ChartPointSite::from_lifted_source(
                    frame,
                    point,
                    ordinal,
                    i64::from(matches!(ordinal, 1 | 3)),
                )
            })
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let start = *points.first().expect("fixture has its first source site");
        let end = *points.get(1).expect("fixture has its second source site");
        let source = SourceConstraint {
            id: 0,
            start,
            end,
            start_site: 0,
            end_site: 1,
            start_occurrence: 0,
            end_occurrence: 1,
        };
        let sources = [source];
        let source_rings = [SourceRingStart::new(0, RingOwner::new(0, 0))];
        let source_locations = chart_sites.iter().copied().take(2).collect::<Vec<_>>();
        let coordinates = CdtCoordinates::retained(frame, &chart_sites, &source_locations).unwrap();
        let mut mesh = ConstrainedMesh::build_with_sources(
            &points,
            &sources,
            source_rings.to_vec(),
            coordinates,
        )
        .unwrap()
        .expect("the five source sites have a non-collinear mesh");
        let pieces = mesh
            .constraint_pieces
            .take_ordered(&sources, &[], &source_rings, &mesh.coordinates)
            .unwrap();
        let third = exact::ExactRational::new(
            exact::ExactDyadic::from_f64(7.0),
            exact::ExactDyadic::from_f64(20.0),
        )
        .unwrap();
        assert_eq!(pieces.len(), 2);
        let first_piece = pieces
            .first()
            .expect("source recovery emits the first interval");
        let second_piece = pieces
            .get(1)
            .expect("source recovery emits the second interval");
        assert_eq!(
            first_piece
                .lo
                .try_cmp(&exact::ExactRational::zero())
                .unwrap(),
            Ordering::Equal
        );
        assert_eq!(first_piece.hi.try_cmp(&third).unwrap(), Ordering::Equal);
        assert_eq!(second_piece.lo.try_cmp(&third).unwrap(), Ordering::Equal);
        assert_eq!(
            second_piece
                .hi
                .try_cmp(&exact::ExactRational::one())
                .unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            (
                first_piece.start_site,
                first_piece.end_site,
                second_piece.end_site,
            ),
            (0, 2, 1)
        );
    }

    #[test]
    fn refinement_initializes_global_state_once() {
        let _ = take_refinement_work_counters();
        let mut pts = points(&[(0.0, 0.0), (20.0, 0.0), (20.0, 20.0), (0.0, 20.0)]);
        let edges: Vec<[usize; 2]> = (0..4).map(|i| [i, (i + 1) % 4]).collect();
        let mut mesh = ConstrainedMesh::build(&pts, &edges).unwrap();
        assert!(mesh.refine(&mut pts, None, Some(100.0), 100).unwrap());
        let counters = take_refinement_work_counters();
        assert_eq!(counters.inside_label_passes, 1);
        assert_eq!(counters.bad_face_scans, 1);
        assert_eq!(counters.constraint_reconstruction_passes, 0);
        assert!(counters.quality_checks <= mesh.mesh.triangle_count() * 8);
    }

    #[test]
    fn rounded_source_midpoint_is_rejected_before_mesh_mutation() {
        let next = 1.0_f64.next_up();
        let mut pts = points(&[(1.0, 0.0), (next, 1.0), (1.0, 0.5)]);
        let before = pts.clone();
        let edges = vec![[0, 1], [1, 2], [2, 0]];
        let mut mesh = ConstrainedMesh::build(&pts, &edges).expect("non-collinear source");
        let error = mesh
            .refine(&mut pts, None, Some(1.0e-30), 32)
            .expect_err("the rounded midpoint collides with an existing source vertex");
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::UnrepresentableOutput {
                operation: "triangulate",
                ..
            })
        ));
        assert_eq!(pts, before, "rejected source split does not append a site");
    }

    #[test]
    fn rounded_point_inside_the_adjacent_face_is_a_boundary_split_location() {
        let next = 1.0_f64.next_up();
        let pts = points(&[(1.0, 0.0), (next, 1.0), (0.0, 0.5)]);
        let mesh = ConstrainedMesh::build(&pts, &[[0, 1], [1, 2], [2, 0]]).unwrap();
        let edge = mesh.mesh.edge_slot(0, 1).unwrap();
        assert!(matches!(
            mesh.locate_boundary_split(edge, XY::new(1.0, 0.5)),
            Ok(Located::Inside(_)),
        ));
    }

    #[test]
    fn unrepresentable_circumcenter_is_an_explicit_midpoint_case() {
        let a = XY::new(0.0, 0.0);
        let b = XY::new(1.0, 0.0);
        let c = XY::new(0.5, f64::from_bits(1));

        assert_eq!(circumcenter(a, b, c).unwrap(), None);
        assert_eq!(longest_edge_midpoint(a, b, c), XY::new(0.5, 0.0));
    }

    #[test]
    fn collinear_circumcenter_failure_is_not_a_midpoint() {
        let error =
            circumcenter(XY::new(0.0, 0.0), XY::new(1.0, 1.0), XY::new(2.0, 2.0)).unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(crate::geometry::GeometryErrorKind::Voronoi(_))
        ));
    }
}

/// Inputs the public suite once broke this engine with: a clockwise ring
/// whose constraints lie on the hull, a sliver whose circumcenters leave the
/// hull, and reciprocal extents whose float circumcenter cancels.
#[cfg(test)]
mod regressions {
    use super::*;

    fn pts(xy: &[(f64, f64)]) -> Vec<Point> {
        xy.iter().map(|&(x, y)| Point::new_raw_xy(x, y)).collect()
    }
    fn ring_edges(n: usize) -> Vec<[usize; 2]> {
        (0..n)
            .map(|i| [i, i.wrapping_add(1).rem_euclid(n)])
            .collect()
    }
    fn inside_count(cdt: &ConstrainedMesh) -> usize {
        let inside = cdt.inside_faces();
        cdt.mesh
            .face_ids()
            .zip(inside)
            .filter(|(face, kept)| cdt.mesh.alive(*face) && *kept)
            .count()
    }

    #[test]
    fn reciprocal_pentagon_swapped_axes_is_three_faces() {
        for exponent in [22_i32, 154, 162] {
            let (large, tiny) = (10_f64.powi(exponent), 10_f64.powi(-exponent));
            let base = [
                (-large, 0.0),
                (-0.5 * large, -tiny),
                (0.5 * large, -tiny),
                (large, 0.0),
                (0.0, tiny),
            ];
            let swapped: Vec<(f64, f64)> = base.iter().map(|&(x, y)| (y, x)).collect();
            let points = pts(&swapped);
            let cdt = ConstrainedMesh::build(&points, &ring_edges(5)).expect("five corners");
            assert_eq!(inside_count(&cdt), 3, "exponent {exponent}");
        }
    }

    #[test]
    fn sliver_refines_to_the_angle_without_leaving_the_hull() {
        let mut points = pts(&[(0.0, 0.0), (20.0, 0.0), (20.0, 0.25), (0.125, 0.0625)]);
        let mut cdt = ConstrainedMesh::build(&points, &ring_edges(4)).unwrap();
        let before = inside_count(&cdt);
        assert!(
            cdt.refine(&mut points, Some(25.0), None, 100_000).unwrap(),
            "refinement completes"
        );
        assert!(inside_count(&cdt) > before);
    }

    #[test]
    fn reciprocal_rectangle_max_area_splits_into_four() {
        for exponent in [77_i32, 159, 200, 300] {
            for swap in [false, true] {
                let (large, tiny) = (10_f64.powi(exponent), 10_f64.powi(-exponent));
                let mut base = vec![(0.0, 0.0), (large, 0.0), (large, tiny), (0.0, tiny)];
                if swap {
                    base = base.iter().map(|&(x, y)| (y, x)).collect();
                }
                let mut points = pts(&base);
                let mut cdt = ConstrainedMesh::build(&points, &ring_edges(4)).unwrap();
                assert!(
                    cdt.refine(&mut points, None, Some(0.4), 100_000).unwrap(),
                    "exponent {exponent} swap {swap}"
                );
                assert_eq!(inside_count(&cdt), 4, "exponent {exponent} swap {swap}");
            }
        }
    }
}
