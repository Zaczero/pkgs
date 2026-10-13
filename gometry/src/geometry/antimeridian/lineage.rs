use crate::geometry::ordinates::{RootId, RootRange};
use crate::geometry::publication::ChartRestriction;

enum SplitRootSource {
    Point {
        member: MemberId,
        point: crate::geometry::Point,
    },
    Edge {
        member: MemberId,
        points: [crate::geometry::Point; 2],
        longitude: crate::geographic::RetainedChartDisplacement,
        metric: std::cell::Cell<crate::geometry::RootMetricState>,
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
    fn member_of_root(&self, root: RootId) -> crate::error::Result<MemberId> {
        self.roots
            .get(root.index())
            .map(|source| match source {
                SplitRootSource::Point { member, .. } | SplitRootSource::Edge { member, .. } => {
                    *member
                },
            })
            .ok_or_else(|| {
                crate::geometry::GeometryErrorKind::Invalid(
                    "split lineage references an unknown source root".into(),
                )
                .into()
            })
    }

    pub(super) fn member(&mut self) -> crate::error::Result<MemberId> {
        let member = MemberId(self.next_member);
        self.next_member = self
            .next_member
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        Ok(member)
    }

    pub(super) fn point_root(
        &mut self,
        member: MemberId,
        point: crate::geometry::Point,
    ) -> crate::error::Result<RootId> {
        let index = u32::try_from(self.roots.len())
            .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        self.roots.push(SplitRootSource::Point { member, point });
        Ok(RootId::from_index(index))
    }

    pub(super) fn edge_root(
        &mut self,
        member: MemberId,
        points: [crate::geometry::Point; 2],
        longitude: crate::geographic::RetainedChartDisplacement,
    ) -> crate::error::Result<RootId> {
        let index = u32::try_from(self.roots.len())
            .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        self.roots.push(SplitRootSource::Edge {
            member,
            points,
            longitude,
            metric: std::cell::Cell::new(crate::geometry::RootMetricState::default()),
        });
        Ok(RootId::from_index(index))
    }

    pub(super) fn resolve_root<M: crate::geometry::ChartMetric>(
        &self,
        root: RootId,
        parameter: &crate::geometry::tessellation::exact::ExactRational,
        axes: crate::geometry::CoordinateAxes,
        frame: crate::geographic::GeographicFrame,
        metric_access: Option<&crate::geometry::ordinates::SourceArcMetricAccess<'_, '_, M>>,
    ) -> crate::error::Result<crate::geometry::ordinates::NodeOrdinates> {
        use crate::geometry::ordinates::{SourceArcRequest, source_arc_ordinates};

        let source = self.roots.get(root.index()).ok_or_else(|| {
            crate::geometry::GeometryErrorKind::Invalid(
                "split lineage references an unknown source root".into(),
            )
        })?;
        match source {
            SplitRootSource::Point { point, .. } => Ok(crate::geometry::ordinates::NodeOrdinates {
                z: {
                    let mut value = crate::geometry::ordinates::AxisValue::default();
                    if axes.has_z() {
                        value.observe(point.z());
                    }
                    value
                },
                m: {
                    let mut value = crate::geometry::ordinates::AxisValue::default();
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
                let request =
                    SourceArcRequest::new(frame, points, axes, parameter, || Ok(*longitude))?;
                let mut fraction = None;
                let mut work = crate::geometry::Unmetered;
                let mut metric_state = metric.get();
                let ordinates = source_arc_ordinates(
                    &request,
                    || {
                        let access = metric_access.ok_or_else(|| {
                            crate::crs::CrsError::invalid(
                                "antimeridian lineage resolution requires the source CRS metric",
                            )
                        })?;
                        access()
                    },
                    &mut metric_state,
                    &mut fraction,
                    &mut work,
                )?;
                metric.set(metric_state);
                Ok(ordinates)
            },
        }
    }

    pub(super) fn resolve_vertex_ordinates<M: crate::geometry::ChartMetric>(
        &self,
        events: &[EventId],
        axes: crate::geometry::CoordinateAxes,
        frame: crate::geographic::GeographicFrame,
        metric_access: Option<&crate::geometry::ordinates::SourceArcMetricAccess<'_, '_, M>>,
    ) -> crate::error::Result<crate::geometry::ordinates::NodeOrdinates> {
        let mut result = crate::geometry::ordinates::NodeOrdinates::default();
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

    pub(super) fn event(
        &mut self,
        member: MemberId,
        roots: Vec<RootRange>,
    ) -> crate::error::Result<EventId> {
        for range in &roots {
            if self.member_of_root(range.root())? != member {
                return Err(crate::geometry::GeometryErrorKind::Invalid(
                    "split event borrows a root from a different member".into(),
                )
                .into());
            }
        }
        let id = EventId(
            u32::try_from(self.events.len())
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        );
        self.events.push(SplitEvent { member, roots });
        Ok(id)
    }

    pub(super) fn push_edge_use(
        &mut self,
        member: MemberId,
        roots: Vec<RootRange>,
        restriction: Option<ChartRestriction>,
    ) -> crate::error::Result<EdgeUseId> {
        for range in &roots {
            if self.member_of_root(range.root())? != member {
                return Err(crate::geometry::GeometryErrorKind::Invalid(
                    "split edge borrows a root from a different member".into(),
                )
                .into());
            }
        }
        let id = EdgeUseId(
            u32::try_from(self.edge_uses.len())
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
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

    pub(super) fn edge_use_at(&self, edge: EdgeUseId) -> &SplitEdgeUse {
        self.edge_uses
            .get(edge.0 as usize)
            .expect("split edge-use IDs come from this catalog")
    }

    pub(super) fn event_on_edge(
        &mut self,
        member: MemberId,
        edge: EdgeUseId,
        parameter: &crate::geometry::tessellation::exact::ExactRational,
    ) -> crate::error::Result<EventId> {
        let mut work = crate::geometry::Unmetered;
        let mut roots = Vec::new();
        if self.edge_use_at(edge).member != member {
            return Err(crate::geometry::GeometryErrorKind::Invalid(
                "split event uses an edge from a different member".into(),
            )
            .into());
        }
        for range in &self.edge_use_at(edge).roots {
            if let Some(source_parameter) = range.parameter_at_with_work(parameter, &mut work)? {
                roots.push(RootRange::point_with_work(
                    range.root(),
                    source_parameter,
                    crate::geometry::tessellation::exact::ExactRational::zero(),
                    &mut work,
                )?);
            }
        }
        self.event(member, roots)
    }

    pub(super) fn event_from_edge_parameters(
        &mut self,
        member: MemberId,
        uses: &[(
            EdgeUseId,
            crate::geometry::tessellation::exact::ExactRational,
        )],
    ) -> crate::error::Result<EventId> {
        let mut work = crate::geometry::Unmetered;
        let mut roots: Vec<RootRange> = Vec::new();
        for (edge, parameter) in uses {
            let source = self.edge_use_at(*edge);
            if source.member != member {
                return Err(crate::geometry::GeometryErrorKind::Invalid(
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
                    crate::geometry::tessellation::exact::ExactRational::zero(),
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

    pub(super) fn split_edge_use(
        &mut self,
        member: MemberId,
        edge: EdgeUseId,
        local_start: &crate::geometry::tessellation::exact::ExactRational,
        local_end: &crate::geometry::tessellation::exact::ExactRational,
    ) -> crate::error::Result<EdgeUseId> {
        let mut work = crate::geometry::Unmetered;
        let mut roots = Vec::new();
        if self.edge_use_at(edge).member != member {
            return Err(crate::geometry::GeometryErrorKind::Invalid(
                "reversed split edge belongs to a different member".into(),
            )
            .into());
        }
        for range in &self.edge_use_at(edge).roots {
            let Some(source_start) = range.parameter_at_with_work(local_start, &mut work)? else {
                continue;
            };
            let Some(source_end) = range.parameter_at_with_work(local_end, &mut work)? else {
                continue;
            };
            if source_start.try_eq_with_work(&source_end, &mut work)? {
                continue;
            }
            roots.push(RootRange::from_mapped_with_work(
                range.root(),
                source_start,
                source_end,
                crate::geometry::tessellation::exact::ExactRational::zero(),
                crate::geometry::tessellation::exact::ExactRational::one(),
                &mut work,
            )?);
        }
        let restriction = self.edge_use_at(edge).restriction;
        self.push_edge_use(member, roots, restriction)
    }

    pub(super) fn compose_edge_uses(
        &mut self,
        member: MemberId,
        source_uses: &[(
            EdgeUseId,
            crate::geometry::tessellation::exact::ExactRational,
            crate::geometry::tessellation::exact::ExactRational,
        )],
        mut restriction: Option<ChartRestriction>,
    ) -> crate::error::Result<EdgeUseId> {
        let mut work = crate::geometry::Unmetered;
        let mut roots: Vec<RootRange> = Vec::new();
        for (edge, local_start, local_end) in source_uses {
            let source = self.edge_use_at(*edge);
            if source.member != member {
                return Err(crate::geometry::GeometryErrorKind::Invalid(
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
                    crate::geometry::tessellation::exact::ExactRational::zero(),
                    crate::geometry::tessellation::exact::ExactRational::one(),
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

    pub(super) fn identity_shape(
        &mut self,
        shape: &crate::geometry::Shape,
    ) -> crate::error::Result<ShapeLineage> {
        use crate::geometry::{MultiMember, Shape};

        let sequence = |catalog: &mut Self,
                        member: MemberId,
                        coordinate_count: usize|
         -> crate::error::Result<LineageSequence> {
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
    pub(super) polygon: crate::geometry::Polygon,
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
        axes: crate::geometry::CoordinateAxes,
        members: Vec<Option<LineageSequence>>,
    },
    Polygon(LineagePolygon),
    MultiPolygon {
        axes: crate::geometry::CoordinateAxes,
        members: Vec<Option<LineagePolygon>>,
    },
    GeometryCollection(Vec<Option<ShapeLineage>>),
    Empty,
}
