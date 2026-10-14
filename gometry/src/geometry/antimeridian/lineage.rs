use std::cell::Cell;

use crate::error::{ResourceError, Result};
use crate::geographic::{GeographicFrame, RetainedChartDisplacement};
use crate::geometry::ordinates::{
    AxisValue, NodeOrdinates, RootId, RootRange, SourceArcMetricAccess, resolve_source_root,
};
use crate::geometry::publication::ChartRestriction;
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{
    ChartMetric, CoordinateAxes, GeometryErrorKind, MultiMember, Point, Polygon, RootMetricState,
    Shape,
};
use crate::work::{Unmetered, Work};

enum SplitRootSource {
    Point {
        member: MemberId,
        point: Point,
    },
    Edge {
        member: MemberId,
        points: [Point; 2],
        longitude: RetainedChartDisplacement,
        metric: Cell<RootMetricState>,
    },
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

#[derive(Default)]
pub(super) struct LineageCatalog {
    roots: Vec<SplitRootSource>,
    next_member: u32,
    events: Vec<SplitEvent>,
    edge_uses: Vec<SplitEdgeUse>,
}

impl LineageCatalog {
    fn member_of_root(&self, root: RootId) -> Result<MemberId> {
        self.roots
            .get(root.index())
            .map(|source| match source {
                SplitRootSource::Point { member, .. } | SplitRootSource::Edge { member, .. } => {
                    *member
                },
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
        self.roots.push(SplitRootSource::Edge {
            member,
            points: *points,
            longitude,
            metric: Cell::new(RootMetricState::default()),
        });
        Ok(RootId::from_index(index))
    }

    pub(super) fn resolve_root<M: ChartMetric>(
        &self,
        root: RootId,
        parameter: &ExactRational,
        axes: CoordinateAxes,
        frame: GeographicFrame,
        metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    ) -> Result<NodeOrdinates> {
        let source = self.roots.get(root.index()).ok_or_else(|| {
            GeometryErrorKind::Invalid("split lineage references an unknown source root".into())
        })?;
        match source {
            SplitRootSource::Point { point, .. } => Ok(NodeOrdinates {
                z: {
                    let mut value = AxisValue::default();
                    if axes.has_z() {
                        value.observe(point.z());
                    }
                    value
                },
                m: {
                    let mut value = AxisValue::default();
                    if axes.has_m() {
                        value.observe(point.m());
                    }
                    value
                },
            }),
            SplitRootSource::Edge {
                points,
                longitude,
                metric,
                ..
            } => {
                let mut fraction = None;
                let mut work = Unmetered;
                let mut metric_state = metric.get();
                let ordinates = resolve_source_root(
                    frame,
                    points,
                    *longitude,
                    parameter,
                    axes,
                    metric_access,
                    &mut metric_state,
                    &mut fraction,
                    &mut work,
                )?;
                metric.set(metric_state);
                Ok(ordinates)
            },
        }
    }

    pub(super) fn resolve_vertex_ordinates<M: ChartMetric>(
        &self,
        events: &[EventId],
        axes: CoordinateAxes,
        frame: GeographicFrame,
        metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    ) -> Result<NodeOrdinates> {
        let mut result = NodeOrdinates::default();
        for &event in events {
            for range in self.event_uses(event) {
                result.merge(self.resolve_root(
                    range.root(),
                    range.source_start(),
                    axes,
                    frame,
                    metric_access,
                )?);
            }
        }
        Ok(result)
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
            if restriction.is_none() {
                restriction = source.restriction;
            }
            for range in &source.roots {
                let Some(source_start) = range.parameter_at_with_work(local_start, &mut work)?
                else {
                    continue;
                };
                let Some(source_end) = range.parameter_at_with_work(local_end, &mut work)? else {
                    continue;
                };
                if source_start.try_eq_with_work(&source_end, &mut work)? {
                    continue;
                }
                let child = RootRange::from_mapped_with_work(
                    range.root(),
                    source_start,
                    source_end,
                    ExactRational::zero(),
                    ExactRational::one(),
                    &mut work,
                )?;
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
                Ok(ShapeLineage::MultiLineString {
                    axes: lines.axes(),
                    members,
                })
            },
            Shape::Polygon(polygon) => {
                let member = self.member()?;
                let shell = sequence(self, member, polygon.shell.coords().len())?;
                let mut holes = Vec::with_capacity(polygon.holes.len());
                for hole in polygon.holes.iter() {
                    holes.push(sequence(self, member, hole.coords().len())?);
                }
                Ok(ShapeLineage::Polygon(LineagePolygon {
                    member,
                    shell,
                    holes,
                }))
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
                            Some(LineagePolygon {
                                member,
                                shell,
                                holes,
                            })
                        },
                    });
                }
                Ok(ShapeLineage::MultiPolygon {
                    axes: polygons.axes(),
                    members,
                })
            },
            Shape::GeometryCollection(collection) => {
                let mut members = Vec::with_capacity(collection.len());
                for member in collection {
                    members.push(Some(self.identity_shape(member)?));
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
    pub(super) member: MemberId,
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
        axes: CoordinateAxes,
        members: Vec<Option<LineageSequence>>,
    },
    Polygon(LineagePolygon),
    MultiPolygon {
        axes: CoordinateAxes,
        members: Vec<Option<LineagePolygon>>,
    },
    GeometryCollection(Vec<Option<Self>>),
    Empty,
}
