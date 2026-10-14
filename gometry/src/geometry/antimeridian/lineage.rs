use crate::error::{ResourceError, Result};
use crate::geographic::{GeographicFrame, RetainedChartDisplacement};
use crate::geometry::ordinates::{
    NodeOrdinates, RequestRowInput, RootCatalog, RootEvaluationContext, RootEvaluationStates,
    RootId, RootRange, RootRequest, RootSourceData, SourceArcMetricAccess,
};
use crate::geometry::publication::ChartRestriction;
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{
    ChartMetric, CoordinateAxes, GeometryErrorKind, MultiMember, Point, Polygon, Shape,
};
use crate::work::{Unmetered, Work};

enum SplitRootSource {
    Point { member: MemberId, point: Point },
    Edge(Box<SplitEdgeRoot>),
}

struct SplitEdgeRoot {
    member: MemberId,
    points: [Point; 2],
    longitude: RetainedChartDisplacement,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct MemberId(u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct EventId(u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct EdgeUseId(u32);

pub(super) struct SplitEvent {
    pub(super) member: MemberId,
    pub(super) roots: Vec<RootRange>,
}

pub(super) struct SplitEdgeUse {
    pub(super) member: MemberId,
    pub(super) roots: Vec<RootRange>,
    pub(super) restriction: Option<ChartRestriction>,
}

pub(super) struct LineageCatalog {
    frame: GeographicFrame,
    roots: Vec<SplitRootSource>,
    evaluation_states: RootEvaluationStates,
    next_member: u32,
    events: Vec<SplitEvent>,
    edge_uses: Vec<SplitEdgeUse>,
}

pub(super) struct LineageRootCatalog<'a> {
    frame: GeographicFrame,
    roots: &'a [SplitRootSource],
}

impl RootCatalog for LineageRootCatalog<'_> {
    fn frame(&self) -> GeographicFrame {
        self.frame
    }

    fn source_root(&self, root: RootId) -> Option<RootSourceData<'_>> {
        match self.roots.get(root.index())? {
            SplitRootSource::Point { point, .. } => Some(RootSourceData::Point(*point)),
            SplitRootSource::Edge(edge) => Some(RootSourceData::Edge {
                points: &edge.points,
                longitude: Some(edge.longitude),
            }),
        }
    }
}

impl LineageCatalog {
    pub(super) fn new(frame: GeographicFrame) -> Self {
        Self {
            frame,
            roots: Vec::new(),
            evaluation_states: RootEvaluationStates::default(),
            next_member: 0,
            events: Vec::new(),
            edge_uses: Vec::new(),
        }
    }

    pub(super) const fn frame(&self) -> GeographicFrame {
        self.frame
    }

    fn member_of_root(&self, root: RootId) -> Result<MemberId> {
        self.roots
            .get(root.index())
            .map(|source| match source {
                SplitRootSource::Point { member, .. } => *member,
                SplitRootSource::Edge(edge) => edge.member,
            })
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("split lineage references an unknown source root".into())
                    .into()
            })
    }

    pub(super) fn member(&mut self) -> Result<MemberId> {
        let member = MemberId(self.next_member);
        self.next_member = self
            .next_member
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        Ok(member)
    }

    pub(super) fn point_root(&mut self, member: MemberId, point: Point) -> Result<RootId> {
        let index = u32::try_from(self.roots.len()).map_err(|_| ResourceError::SizeOverflow)?;
        self.roots.push(SplitRootSource::Point { member, point });
        Ok(RootId::from_index(index))
    }

    pub(super) fn edge_root(
        &mut self,
        member: MemberId,
        points: &[Point; 2],
        longitude: RetainedChartDisplacement,
    ) -> Result<RootId> {
        let index = u32::try_from(self.roots.len()).map_err(|_| ResourceError::SizeOverflow)?;
        self.roots
            .push(SplitRootSource::Edge(Box::new(SplitEdgeRoot {
                member,
                points: *points,
                longitude,
            })));
        Ok(RootId::from_index(index))
    }

    pub(super) fn resolve_root<M: ChartMetric>(
        &mut self,
        root: RootId,
        parameter: &ExactRational,
        axes: CoordinateAxes,
        metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    ) -> Result<NodeOrdinates> {
        if !axes.has_z() && !axes.has_m() {
            return Ok(NodeOrdinates::default());
        }
        let mut work = Unmetered;
        let request =
            RootRequest::new_with_work(root, parameter.try_clone_with_work(&mut work)?, &mut work)?;
        self.resolve_requests(vec![request], axes, metric_access, &mut work)
    }

    pub(super) fn resolve_vertex_ordinates<M: ChartMetric>(
        &mut self,
        events: &[EventId],
        axes: CoordinateAxes,
        metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    ) -> Result<NodeOrdinates> {
        if !axes.has_z() && !axes.has_m() {
            return Ok(NodeOrdinates::default());
        }
        let mut work = Unmetered;
        let request_count = events.iter().try_fold(0_usize, |count, &event| {
            count
                .checked_add(self.event_uses(event).len())
                .ok_or(ResourceError::SizeOverflow)
        })?;
        let mut requests = Vec::with_capacity(request_count);
        for &event in events {
            for range in self.event_uses(event) {
                requests.push(RootRequest::new_with_work(
                    range.root(),
                    range.source_start().try_clone_with_work(&mut work)?,
                    &mut work,
                )?);
            }
        }
        self.resolve_requests(requests, axes, metric_access, &mut work)
    }

    fn resolve_requests<M: ChartMetric>(
        &mut self,
        requests: Vec<RootRequest>,
        axes: CoordinateAxes,
        metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
        work: &mut impl Work,
    ) -> Result<NodeOrdinates> {
        let rows = [RequestRowInput { axes, requests }];
        let roots = LineageRootCatalog {
            frame: self.frame,
            roots: &self.roots,
        };
        let mut context =
            RootEvaluationContext::new(&roots, metric_access, &mut self.evaluation_states);
        context
            .resolve_rows(&rows, work)?
            .into_iter()
            .next()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("split source request row was not resolved".into())
                    .into()
            })
    }

    pub(super) fn event(&mut self, member: MemberId, roots: Vec<RootRange>) -> Result<EventId> {
        for range in &roots {
            if self.member_of_root(range.root())? != member {
                return Err(GeometryErrorKind::Invalid(
                    "split event borrows a root from a different member".into(),
                )
                .into());
            }
        }
        let id =
            EventId(u32::try_from(self.events.len()).map_err(|_| ResourceError::SizeOverflow)?);
        self.events.push(SplitEvent { member, roots });
        Ok(id)
    }

    pub(super) fn push_edge_use(
        &mut self,
        member: MemberId,
        roots: Vec<RootRange>,
        restriction: Option<ChartRestriction>,
    ) -> Result<EdgeUseId> {
        for range in &roots {
            if self.member_of_root(range.root())? != member {
                return Err(GeometryErrorKind::Invalid(
                    "split edge borrows a root from a different member".into(),
                )
                .into());
            }
        }
        let id = EdgeUseId(
            u32::try_from(self.edge_uses.len()).map_err(|_| ResourceError::SizeOverflow)?,
        );
        self.edge_uses.push(SplitEdgeUse {
            member,
            roots,
            restriction,
        });
        Ok(id)
    }

    pub(super) fn event_uses(&self, event: EventId) -> &[RootRange] {
        &self
            .events
            .get(event.0 as usize)
            .expect("split event IDs come from this catalog")
            .roots
    }

    pub(super) fn event_member(&self, event: EventId) -> MemberId {
        self.events
            .get(event.0 as usize)
            .expect("split event IDs come from this catalog")
            .member
    }

    pub(super) fn vertex_roots(
        &self,
        events: &[EventId],
        work: &mut impl Work,
    ) -> Result<Vec<RootRange>> {
        let mut roots: Vec<RootRange> = Vec::new();
        for &event in events {
            for range in self.event_uses(event) {
                let mut found = false;
                for existing in &roots {
                    if existing.try_eq_with_work(range, work)? {
                        found = true;
                        break;
                    }
                }
                if !found {
                    roots.push(range.try_clone_with_work(work)?);
                }
            }
        }
        Ok(roots)
    }

    pub(super) fn combine_events(
        &mut self,
        member: MemberId,
        events: &[EventId],
    ) -> Result<EventId> {
        if events
            .iter()
            .any(|&event| self.event_member(event) != member)
        {
            return Err(GeometryErrorKind::Invalid(
                "combined split events belong to different members".into(),
            )
            .into());
        }
        let mut work = Unmetered;
        let roots = self.vertex_roots(events, &mut work)?;
        self.event(member, roots)
    }

    pub(super) fn edge_use_at(&self, edge: EdgeUseId) -> &SplitEdgeUse {
        self.edge_uses
            .get(edge.0 as usize)
            .expect("split edge-use IDs come from this catalog")
    }

    pub(super) fn event_from_edge_parameters<'a>(
        &mut self,
        member: MemberId,
        uses: impl IntoIterator<Item = (EdgeUseId, &'a ExactRational)>,
    ) -> Result<EventId> {
        let mut work = Unmetered;
        let mut roots: Vec<RootRange> = Vec::new();
        for (edge, parameter) in uses {
            let source = self.edge_use_at(edge);
            if source.member != member {
                return Err(GeometryErrorKind::Invalid(
                    "split event uses an edge from a different member".into(),
                )
                .into());
            }
            for range in &source.roots {
                let Some(source_parameter) = range.parameter_at_with_work(parameter, &mut work)?
                else {
                    continue;
                };
                let candidate = RootRange::point_with_work(
                    range.root(),
                    source_parameter,
                    ExactRational::zero(),
                    &mut work,
                )?;
                let mut found = false;
                for existing in &roots {
                    if existing.try_eq_with_work(&candidate, &mut work)? {
                        found = true;
                        break;
                    }
                }
                if !found {
                    roots.push(candidate);
                }
            }
        }
        self.event(member, roots)
    }

    pub(super) fn reverse_sequence(
        &mut self,
        lineage: &LineageSequence,
    ) -> Result<LineageSequence> {
        if lineage.edges.len() != lineage.vertices.len().saturating_sub(1) {
            return Err(GeometryErrorKind::Invalid(
                "split sequence coordinates and edge lineage disagree".into(),
            )
            .into());
        }
        let one = ExactRational::one();
        let zero = ExactRational::zero();
        let mut edges = Vec::with_capacity(lineage.edges.len());
        for &edge in lineage.edges.iter().rev() {
            edges.push(self.compose_edge_uses(lineage.member, [(edge, &one, &zero)], None)?);
        }
        Ok(LineageSequence {
            member: lineage.member,
            vertices: lineage.vertices.iter().rev().cloned().collect(),
            edges,
        })
    }

    pub(super) fn compose_edge_uses<'a>(
        &mut self,
        member: MemberId,
        source_uses: impl IntoIterator<Item = (EdgeUseId, &'a ExactRational, &'a ExactRational)>,
        mut restriction: Option<ChartRestriction>,
    ) -> Result<EdgeUseId> {
        let mut work = Unmetered;
        let mut roots: Vec<RootRange> = Vec::new();
        for (edge, local_start, local_end) in source_uses {
            let source = self.edge_use_at(edge);
            if source.member != member {
                return Err(GeometryErrorKind::Invalid(
                    "split edge use belongs to a different member".into(),
                )
                .into());
            }
            if let Some(inherited) = source.restriction {
                if restriction.is_some_and(|current| current != inherited) {
                    return Err(GeometryErrorKind::Invalid(
                        "split edge composition combines incompatible chart restrictions".into(),
                    )
                    .into());
                }
                restriction = Some(inherited);
            }
            for range in &source.roots {
                let Some(child) = range.compose_with_work(local_start, local_end, &mut work)?
                else {
                    continue;
                };
                let mut found = false;
                for existing in &roots {
                    if existing.try_eq_with_work(&child, &mut work)? {
                        found = true;
                        break;
                    }
                }
                if !found {
                    roots.push(child);
                }
            }
        }
        self.push_edge_use(member, roots, restriction)
    }

    pub(super) fn identity_shape(&mut self, shape: &Shape) -> Result<ShapeLineage> {
        let sequence = |catalog: &mut Self,
                        member: MemberId,
                        coordinate_count: usize|
         -> Result<LineageSequence> {
            let edge_count = coordinate_count.saturating_sub(1);
            let mut edges = Vec::with_capacity(edge_count);
            for _ in 0..edge_count {
                edges.push(catalog.push_edge_use(member, Vec::new(), None)?);
            }
            Ok(LineageSequence {
                member,
                vertices: vec![Vec::new(); coordinate_count],
                edges,
            })
        };

        match shape {
            Shape::Point(_) => Ok(ShapeLineage::Point {
                member: self.member()?,
                events: Vec::new(),
            }),
            Shape::MultiPoint(points) => Ok(ShapeLineage::MultiPoint {
                member: self.member()?,
                events: vec![Vec::new(); points.len()],
            }),
            Shape::LineString(line) => {
                let member = self.member()?;
                Ok(ShapeLineage::LineString(sequence(
                    self,
                    member,
                    line.len(),
                )?))
            },
            Shape::MultiLineString(lines) => {
                let mut members = Vec::with_capacity(lines.member_count());
                for value in lines.members_with_values() {
                    members.push(match value {
                        MultiMember::Empty => None,
                        MultiMember::Value(line) => {
                            let member = self.member()?;
                            Some(sequence(self, member, line.len())?)
                        },
                    });
                }
                Ok(ShapeLineage::MultiLineString { members })
            },
            Shape::Polygon(polygon) => {
                let member = self.member()?;
                let shell = sequence(self, member, polygon.shell.coords().len())?;
                let mut holes = Vec::with_capacity(polygon.holes.len());
                for hole in polygon.holes.iter() {
                    holes.push(sequence(self, member, hole.coords().len())?);
                }
                Ok(ShapeLineage::Polygon(LineagePolygon { shell, holes }))
            },
            Shape::MultiPolygon(polygons) => {
                let mut members = Vec::with_capacity(polygons.member_count());
                for value in polygons.members_with_values() {
                    members.push(match value {
                        MultiMember::Empty => None,
                        MultiMember::Value(polygon) => {
                            let member = self.member()?;
                            let shell = sequence(self, member, polygon.shell.coords().len())?;
                            let mut holes = Vec::with_capacity(polygon.holes.len());
                            for hole in polygon.holes.iter() {
                                holes.push(sequence(self, member, hole.coords().len())?);
                            }
                            Some(LineagePolygon { shell, holes })
                        },
                    });
                }
                Ok(ShapeLineage::MultiPolygon { members })
            },
            Shape::GeometryCollection(collection) => {
                let mut members = Vec::with_capacity(collection.len());
                for member in collection {
                    members.push(self.identity_shape(member)?);
                }
                Ok(ShapeLineage::GeometryCollection(members))
            },
            Shape::Empty(..) => Ok(ShapeLineage::Empty),
        }
    }
}

#[derive(Clone)]
pub(super) struct LineageSequence {
    pub(super) member: MemberId,
    pub(super) vertices: Vec<Vec<EventId>>,
    pub(super) edges: Vec<EdgeUseId>,
}

#[derive(Clone)]
pub(super) struct LineagePolygon {
    pub(super) shell: LineageSequence,
    pub(super) holes: Vec<LineageSequence>,
}

pub(super) struct PolygonWithLineage {
    pub(super) polygon: Polygon,
    pub(super) lineage: LineagePolygon,
}

#[derive(Clone)]
pub(super) enum ShapeLineage {
    Point {
        member: MemberId,
        events: Vec<EventId>,
    },
    MultiPoint {
        member: MemberId,
        events: Vec<Vec<EventId>>,
    },
    LineString(LineageSequence),
    MultiLineString {
        members: Vec<Option<LineageSequence>>,
    },
    Polygon(LineagePolygon),
    MultiPolygon {
        members: Vec<Option<LineagePolygon>>,
    },
    GeometryCollection(Vec<Self>),
    Empty,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geographic::ChartPole;
    use crate::geometry::{MOrdinate, ZOrdinate};

    fn edge_root(
        catalog: &mut LineageCatalog,
        member: MemberId,
        frame: GeographicFrame,
        points: &[Point; 2],
    ) -> RootId {
        let longitude = frame
            .longitude()
            .chart_connection(points[0].x, points[1].x)
            .expect("test edge longitudes are admitted")
            .retained();
        catalog
            .edge_root(member, points, longitude)
            .expect("test source root fits the catalog")
    }

    fn source_point(x: f64, y: f64, z: f64, m: f64) -> Point {
        Point::new_axes(x, y, ZOrdinate(Some(z)), MOrdinate(Some(m)))
            .expect("test source point is finite")
    }

    fn mapped_partial_range(root: RootId) -> RootRange {
        RootRange::from_mapped(
            root,
            ExactRational::zero(),
            ExactRational::one(),
            ExactRational::from_f64(0.25),
            ExactRational::from_f64(0.75),
        )
        .expect("test root range is inside the unit interval")
    }

    fn catalog_edge() -> (LineageCatalog, MemberId, RootId) {
        let frame = GeographicFrame::crs84();
        let mut catalog = LineageCatalog::new(frame);
        let member = catalog.member().expect("test member fits the catalog");
        let points = [Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(10.0, 20.0)];
        let root = edge_root(&mut catalog, member, frame, &points);
        (catalog, member, root)
    }

    #[test]
    fn edge_composition_retains_partial_root_support() {
        let (mut catalog, member, root) = catalog_edge();
        let parent = catalog
            .push_edge_use(member, vec![mapped_partial_range(root)], None)
            .expect("parent edge use is valid");

        let child = catalog
            .compose_edge_uses(
                member,
                [(parent, &ExactRational::zero(), &ExactRational::one())],
                None,
            )
            .expect("full child edge composes");
        let roots = &catalog.edge_use_at(child).roots;

        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].source_start(), &ExactRational::zero());
        assert_eq!(roots[0].source_end(), &ExactRational::one());
        assert_eq!(roots[0].producer_start(), &ExactRational::from_f64(0.25));
        assert_eq!(roots[0].producer_end(), &ExactRational::from_f64(0.75));
    }

    #[test]
    fn reversed_edge_composition_retains_partial_support_and_direction() {
        let (mut catalog, member, root) = catalog_edge();
        let parent = catalog
            .push_edge_use(member, vec![mapped_partial_range(root)], None)
            .expect("parent edge use is valid");

        let child = catalog
            .compose_edge_uses(
                member,
                [(parent, &ExactRational::one(), &ExactRational::zero())],
                None,
            )
            .expect("reversed child edge composes");
        let roots = &catalog.edge_use_at(child).roots;

        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].source_start(), &ExactRational::one());
        assert_eq!(roots[0].source_end(), &ExactRational::zero());
        assert_eq!(roots[0].producer_start(), &ExactRational::from_f64(0.25));
        assert_eq!(roots[0].producer_end(), &ExactRational::from_f64(0.75));
    }

    #[test]
    fn edge_composition_retains_endpoint_only_root_support() {
        let (mut catalog, member, root) = catalog_edge();
        let parent = catalog
            .push_edge_use(
                member,
                vec![
                    RootRange::point(
                        root,
                        ExactRational::from_f64(0.5),
                        ExactRational::from_f64(0.5),
                    )
                    .expect("endpoint-only root support is valid"),
                ],
                None,
            )
            .expect("parent edge use is valid");

        let child = catalog
            .compose_edge_uses(
                member,
                [(parent, &ExactRational::zero(), &ExactRational::one())],
                None,
            )
            .expect("child edge retains endpoint-only support");
        let roots = &catalog.edge_use_at(child).roots;

        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].source_start(), &ExactRational::from_f64(0.5));
        assert_eq!(roots[0].source_end(), &ExactRational::from_f64(0.5));
        assert_eq!(roots[0].producer_start(), &ExactRational::from_f64(0.5));
        assert_eq!(roots[0].producer_end(), &ExactRational::from_f64(0.5));
    }

    #[test]
    fn edge_composition_rejects_incompatible_inherited_restrictions() {
        let (mut catalog, member, root) = catalog_edge();
        let points = [Point::new_raw_xy(20.0, 30.0), Point::new_raw_xy(30.0, 40.0)];
        let second_root = edge_root(&mut catalog, member, GeographicFrame::crs84(), &points);
        let first = catalog
            .push_edge_use(
                member,
                vec![RootRange::whole(root)],
                Some(ChartRestriction::CutChord),
            )
            .expect("first restricted edge use is valid");
        let second = catalog
            .push_edge_use(
                member,
                vec![RootRange::whole(second_root)],
                Some(ChartRestriction::PolarCap {
                    pole: ChartPole::North,
                }),
            )
            .expect("second restricted edge use is valid");

        let result = catalog.compose_edge_uses(
            member,
            [
                (first, &ExactRational::zero(), &ExactRational::one()),
                (second, &ExactRational::zero(), &ExactRational::one()),
            ],
            None,
        );

        let _error = result.expect_err("incompatible chart restrictions must be rejected");
    }

    #[test]
    fn repeated_lineage_contributors_share_one_root_request() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let metric = crs84_metric();
        let metric_access = || Ok(&metric);
        let mut catalog = LineageCatalog::new(frame);
        let member = catalog.member()?;
        let points = [
            source_point(0.0, 0.0, 1.0, 11.0),
            source_point(10.0, 10.0, 9.0, 99.0),
        ];
        let root = edge_root(&mut catalog, member, frame, &points);
        let half = ExactRational::from_f64(0.5);
        let start = ExactRational::zero();
        let first = catalog.event(member, vec![RootRange::point(
            root,
            half.clone(),
            start.clone(),
        )?])?;
        let second = catalog.event(member, vec![RootRange::point(root, half, start)?])?;

        crate::geometry::ordinates::reset_root_range_mapping_counts();
        crate::geometry::ordinates::reset_root_work_counters();
        let resolved = catalog.resolve_vertex_ordinates(
            &[first, second],
            CoordinateAxes::XYZM,
            Some(&metric_access),
        )?;

        assert_eq!(crate::geometry::ordinates::root_request_table_builds(), 1);
        assert_eq!(
            crate::geometry::ordinates::root_work_counters().root_catalog_resolutions,
            1
        );
        assert!(resolved.z.value().is_some());
        assert!(resolved.m.value().is_some());
        Ok(())
    }

    fn crs84_metric() -> crate::crs::GeodesicMetric {
        let model =
            crate::crs::metric_model(Some("OGC:CRS84")).expect("CRS84 metric model is available");
        crate::crs::GeodesicMetric::from_model(&model).expect("CRS84 metric is valid")
    }

    #[test]
    fn zero_length_root_conflicts_are_axis_local_and_direction_independent() {
        let frame = GeographicFrame::crs84();
        let metric = crs84_metric();
        let metric_access = || Ok(&metric);
        let mut catalog = LineageCatalog::new(frame);
        let member = catalog.member().expect("test member fits the catalog");
        let start = source_point(12.0, 5.0, 10.0, 7.0);
        let end = source_point(12.0, 5.0, 20.0, 7.0);
        let forward = edge_root(&mut catalog, member, frame, &[start, end]);
        let reverse = edge_root(&mut catalog, member, frame, &[end, start]);
        let half = ExactRational::from_f64(0.5);
        let axes = CoordinateAxes::XYZM;
        let metric_access: &SourceArcMetricAccess<'_, '_, crate::crs::GeodesicMetric> =
            &metric_access;

        for root in [forward, reverse] {
            let middle = catalog
                .resolve_root(root, &half, axes, Some(metric_access))
                .expect("zero-length root resolves without choosing an endpoint");
            assert_eq!(middle.z.value(), None);
            assert_eq!(middle.m.value(), Some(7.0));
        }

        let no_metric: Option<&SourceArcMetricAccess<'_, '_, crate::crs::GeodesicMetric>> = None;
        let first = catalog
            .resolve_root(forward, &ExactRational::zero(), axes, no_metric)
            .expect("authentic start endpoint needs no metric");
        let last = catalog
            .resolve_root(forward, &ExactRational::one(), axes, no_metric)
            .expect("authentic end endpoint needs no metric");
        assert_eq!(first.z.value(), Some(10.0));
        assert_eq!(last.z.value(), Some(20.0));
    }

    #[test]
    fn pole_alias_zero_length_root_keeps_equal_bits_and_conflicts_on_m() {
        let frame = GeographicFrame::crs84();
        let metric = crs84_metric();
        let metric_access = || Ok(&metric);
        let mut catalog = LineageCatalog::new(frame);
        let member = catalog.member().expect("test member fits the catalog");
        let start = source_point(0.0, 90.0, -0.0, 10.0);
        let end = source_point(90.0, 90.0, -0.0, 20.0);
        let root = edge_root(&mut catalog, member, frame, &[start, end]);
        let reverse = edge_root(&mut catalog, member, frame, &[end, start]);
        let half = ExactRational::from_f64(0.5);
        let metric_access: &SourceArcMetricAccess<'_, '_, crate::crs::GeodesicMetric> =
            &metric_access;

        for root in [root, reverse] {
            let middle = catalog
                .resolve_root(root, &half, CoordinateAxes::XYZM, Some(metric_access))
                .expect("pole-alias zero-length root resolves symmetrically");

            assert_eq!(
                middle
                    .z
                    .value()
                    .expect("equal endpoint Z bits remain available")
                    .to_bits(),
                (-0.0_f64).to_bits()
            );
            assert_eq!(middle.m.value(), None);
        }
    }

    #[test]
    fn numerical_zero_length_underflow_is_an_error() {
        let frame = GeographicFrame::crs84();
        let metric = crs84_metric();
        let metric_access = || Ok(&metric);
        let mut catalog = LineageCatalog::new(frame);
        let member = catalog.member().expect("test member fits the catalog");
        let start = source_point(0.0, 0.0, 10.0, 20.0);
        let end = source_point(f64::from_bits(1), 0.0, 30.0, 40.0);
        let root = edge_root(&mut catalog, member, frame, &[start, end]);
        let half = ExactRational::from_f64(0.5);
        let metric_access: &SourceArcMetricAccess<'_, '_, crate::crs::GeodesicMetric> =
            &metric_access;

        let result = catalog.resolve_root(root, &half, CoordinateAxes::XYZM, Some(metric_access));

        let Err(error) = result else {
            panic!("a nonzero edge whose metric underflows must fail");
        };
        assert!(error.to_string().contains("underflow"), "{error}");
    }
}
