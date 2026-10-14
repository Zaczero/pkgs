use super::super::lineage::{
    EventId, LineageCatalog, LineagePolygon, LineageSequence, MemberId, ShapeLineage,
};
use super::drawing::SplitDrawing;
use crate::error::Result;
use crate::geographic::GeographicFrame;
use crate::geometry::ordinates::{RootId, RootRange};
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{GeometryErrorKind, MultiMember, Point, Polygon, Ring, Shape, ring_winding};
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

    pub(in crate::geometry::antimeridian) fn point_root(
        &mut self,
        member: MemberId,
        point: Point,
    ) -> Result<RootId> {
        self.lineage.point_root(member, point)
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

    pub(in crate::geometry::antimeridian) fn into_drawing(
        self,
        shape: Shape,
        frame: GeographicFrame,
        longitude_scale: f64,
        latitude_scale: f64,
        lineage: ShapeLineage,
    ) -> SplitDrawing {
        SplitDrawing {
            shape,
            frame,
            longitude_scale,
            latitude_scale,
            lineage: self.lineage,
            shape_lineage: lineage,
        }
    }
}

fn orient_lineage_sequence(
    ring: &Ring,
    lineage: &LineageSequence,
    clockwise: bool,
    catalog: &mut LineageCatalog,
) -> Result<LineageSequence> {
    if ring.coords().len() != lineage.vertices.len() {
        return Err(GeometryErrorKind::Invalid(
            "antimeridian ring coordinates and vertex lineage disagree".into(),
        )
        .into());
    }
    if ring_winding(ring).reverse_for_clockwise(clockwise) {
        catalog.reverse_sequence(lineage)
    } else {
        Ok(lineage.clone())
    }
}

fn orient_lineage_polygon(
    polygon: &Polygon,
    lineage: &LineagePolygon,
    catalog: &mut LineageCatalog,
) -> Result<LineagePolygon> {
    if polygon.holes.len() != lineage.holes.len() {
        return Err(GeometryErrorKind::Invalid(
            "antimeridian polygon holes and lineage disagree".into(),
        )
        .into());
    }
    let shell = orient_lineage_sequence(&polygon.shell, &lineage.shell, false, catalog)?;
    let mut holes = Vec::with_capacity(polygon.holes.len());
    for (ring, ring_lineage) in polygon.holes.iter().zip(&lineage.holes) {
        holes.push(orient_lineage_sequence(ring, ring_lineage, true, catalog)?);
    }
    Ok(LineagePolygon {
        member: lineage.member,
        shell,
        holes,
    })
}

pub(super) fn orient_shape_lineage(
    shape: &Shape,
    lineage: &mut ShapeLineage,
    catalog: &mut LineageCatalog,
) -> Result<()> {
    match (shape, lineage) {
        (Shape::Polygon(polygon), ShapeLineage::Polygon(polygon_lineage)) => {
            *polygon_lineage = orient_lineage_polygon(polygon, polygon_lineage, catalog)?;
        },
        (
            Shape::MultiPolygon(polygons),
            ShapeLineage::MultiPolygon {
                members: lineages, ..
            },
        ) => {
            if polygons.member_count() != lineages.len() {
                return Err(GeometryErrorKind::Invalid(
                    "antimeridian multipolygon members and lineage disagree".into(),
                )
                .into());
            }
            for (polygon, lineage) in polygons.members_with_values().zip(lineages) {
                match (polygon, lineage) {
                    (MultiMember::Empty, None) => {},
                    (MultiMember::Value(polygon), Some(lineage)) => {
                        *lineage = orient_lineage_polygon(polygon, lineage, catalog)?;
                    },
                    _ => {
                        return Err(GeometryErrorKind::Invalid(
                            "antimeridian multipolygon members and lineage disagree".into(),
                        )
                        .into());
                    },
                }
            }
        },
        (Shape::GeometryCollection(collection), ShapeLineage::GeometryCollection(lineages)) => {
            if collection.len() != lineages.len() {
                return Err(GeometryErrorKind::Invalid(
                    "antimeridian collection members and lineage disagree".into(),
                )
                .into());
            }
            for (member, lineage) in collection.iter().zip(lineages) {
                orient_shape_lineage(member, lineage, catalog)?;
            }
        },
        (Shape::Point(_), ShapeLineage::Point { .. })
        | (Shape::MultiPoint(_), ShapeLineage::MultiPoint { .. })
        | (Shape::LineString(_), ShapeLineage::LineString(_))
        | (Shape::MultiLineString(_), ShapeLineage::MultiLineString { .. })
        | (Shape::Empty(..), ShapeLineage::Empty) => {},
        _ => {
            return Err(GeometryErrorKind::Invalid(
                "antimeridian shape and lineage disagree during orientation".into(),
            )
            .into());
        },
    }
    Ok(())
}
