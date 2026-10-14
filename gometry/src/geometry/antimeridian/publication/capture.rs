use super::super::lineage::{EventId, LineageCatalog, MemberId, ShapeLineage};
use super::drawing::SplitDrawing;
use crate::error::Result;
use crate::geographic::GeographicFrame;
use crate::geometry::ordinates::{RootId, RootRange};
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{GeometryErrorKind, Point, Shape};
use crate::work::Unmetered;

pub(in crate::geometry::antimeridian) struct SplitCapture {
    lineage: LineageCatalog,
}

impl SplitCapture {
    pub(in crate::geometry::antimeridian) fn enabled() -> Self {
        Self {
            lineage: LineageCatalog::default(),
        }
    }

    pub(in crate::geometry::antimeridian) fn mark_identity(&mut self) {
        self.lineage = LineageCatalog::default();
    }

    pub(in crate::geometry::antimeridian) fn next_member(&mut self) -> Result<MemberId> {
        self.lineage.member()
    }

    pub(in crate::geometry::antimeridian) fn edge_root(
        &mut self,
        member: MemberId,
        points: &[Point; 2],
        frame: GeographicFrame,
    ) -> Result<RootId> {
        let longitude = frame
            .longitude()
            .chart_connection(points[0].x, points[1].x)
            .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))?
            .retained();
        self.lineage.edge_root(member, points, longitude)
    }

    pub(in crate::geometry::antimeridian) fn identity_lineage(
        &mut self,
        shape: &Shape,
    ) -> Result<ShapeLineage> {
        self.lineage.identity_shape(shape)
    }

    pub(in crate::geometry::antimeridian) const fn lineage_mut(&mut self) -> &mut LineageCatalog {
        &mut self.lineage
    }

    pub(in crate::geometry::antimeridian) fn source_edge_use(
        &mut self,
        member: MemberId,
        root: RootId,
        source_start: &ExactRational,
        source_end: &ExactRational,
    ) -> Result<super::super::lineage::EdgeUseId> {
        let mut work = Unmetered;
        let range = RootRange::from_mapped_with_work(
            root,
            source_start.try_clone_with_work(&mut work)?,
            source_end.try_clone_with_work(&mut work)?,
            ExactRational::zero(),
            ExactRational::one(),
            &mut work,
        )?;
        self.lineage.push_edge_use(member, vec![range], None)
    }

    pub(in crate::geometry::antimeridian) fn record_root_point(
        &mut self,
        root: RootId,
        member: MemberId,
        source_parameter: &ExactRational,
    ) -> Result<EventId> {
        let mut work = Unmetered;
        let range = RootRange::point_with_work(
            root,
            source_parameter.try_clone_with_work(&mut work)?,
            ExactRational::zero(),
            &mut work,
        )?;
        self.lineage.event(member, vec![range])
    }

    pub(in crate::geometry::antimeridian) fn into_drawing(self, shape: Shape) -> SplitDrawing {
        drop(self);
        SplitDrawing { shape }
    }
}
