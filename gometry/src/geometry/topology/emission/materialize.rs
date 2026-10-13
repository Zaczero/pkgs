use super::{
    AssignedSite, CoordSeq, CoordinateAxes, EmissionPortId, ExpandedEmissionPlan, FinishedGeometry,
    FrozenLiteralBuffers, GeometryErrorKind, LiteralPoint, LiteralPolygon, LiteralSequence,
    NodeOrdinates, Point, Polygon, QPortId, ResourceError, Result, Ring, Shape, ShapeLiteralLayout,
    SourceEdgeId, native_chord_points, native_port_point,
};
use crate::work::Work;
pub(in crate::geometry::topology) struct CompletedNativeDrawing {
    pub(in crate::geometry::topology) sealed: FinishedGeometry,
}

pub(in crate::geometry::topology) struct NativeFrozenDrawing {
    pub(in crate::geometry::topology) shape: Shape,
    pub(in crate::geometry::topology) literal_uses: ShapeLiteralLayout<EmissionPortId>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[expect(
    private_interfaces,
    reason = "the common sink carries Q image blame opaquely while the Q adapter alone interprets its topology-private IDs"
)]
pub(in crate::geometry) enum QSourceImage {
    /// A computed or respelled port, authenticated by its checked chart image.
    Port(QPortId),
    /// Retained source records. Their locator lets the final sink recheck the
    /// writer's XY bit law without deriving an expectation from that sink.
    SourcePoint(u32),
    SourceRingStart(SourceEdgeId),
    SourceRingEnd(SourceEdgeId),
    CollapsedStart(SourceEdgeId),
    CollapsedEnd(SourceEdgeId),
    PublicationSample(u32),
}

pub(in crate::geometry::topology) struct SourceFrozenDrawing {
    pub(in crate::geometry::topology) buffers: FrozenLiteralBuffers<QSourceImage>,
}

#[cfg(test)]
pub(in crate::geometry::topology) type NativePolygonLiteralUses = LiteralPolygon<EmissionPortId>;

pub(in crate::geometry::topology) type NativeLiteralUses = ShapeLiteralLayout<EmissionPortId>;

pub(in crate::geometry::topology) struct NativeMaterializedParts {
    pub(in crate::geometry::topology) axes: CoordinateAxes,
    pub(in crate::geometry::topology) points: Vec<Point>,
    pub(in crate::geometry::topology) lines: Vec<CoordSeq>,
    pub(in crate::geometry::topology) polygons: Vec<Polygon>,
    pub(in crate::geometry::topology) literal_uses: NativeLiteralUses,
}

impl NativeMaterializedParts {
    #[expect(
        clippy::suspicious_operation_groupings,
        reason = "LiteralSequence::images is the one-to-one source map for each ring's coordinates"
    )]
    pub(in crate::geometry::topology) fn validate_literal_uses(
        &self,
        work: &mut impl Work,
    ) -> Result<()> {
        if self.literal_uses.points.len() != self.points.len()
            || self.literal_uses.lines.len() != self.lines.len()
            || self.literal_uses.polygons.len() != self.polygons.len()
        {
            return Err(GeometryErrorKind::Invalid(
                "native literal-use rows do not match their output parts".into(),
            )
            .into());
        }
        for index in 0..self.points.len() {
            work.charge_into_error(1)?;
            let point = self.points.get(index).expect("point row count was checked");
            let uses = self
                .literal_uses
                .points
                .get(index)
                .expect("point-use row count was checked");
            if CoordinateAxes::from_point(*point) != uses.axes {
                return Err(GeometryErrorKind::Invalid(
                    "native point axes differ from its logical output policy".into(),
                )
                .into());
            }
        }
        for index in 0..self.lines.len() {
            work.charge_into_error(1)?;
            let line = self.lines.get(index).expect("line row count was checked");
            let uses = self
                .literal_uses
                .lines
                .get(index)
                .expect("line-use row count was checked");
            if line.len() != uses.images.len() || line.axes() != uses.axes {
                return Err(GeometryErrorKind::Invalid(
                    "native line literal-use rows do not match their coordinates".into(),
                )
                .into());
            }
        }
        for index in 0..self.polygons.len() {
            work.charge_into_error(1)?;
            let polygon = self
                .polygons
                .get(index)
                .expect("polygon row count was checked");
            let uses = self
                .literal_uses
                .polygons
                .get(index)
                .expect("polygon-use row count was checked");
            if polygon.shell.coords().len() != uses.shell.images.len()
                || polygon.holes.len() != uses.holes.len()
                || polygon.shell.coords().axes() != uses.axes
                || uses.shell.axes != uses.axes
            {
                return Err(GeometryErrorKind::Invalid(
                    "native polygon literal-use rows do not match their rings".into(),
                )
                .into());
            }
            for hole_index in 0..polygon.holes.len() {
                work.charge_into_error(1)?;
                let hole = polygon
                    .holes
                    .get(hole_index)
                    .expect("hole row count was checked");
                let hole_uses = uses
                    .holes
                    .get(hole_index)
                    .expect("hole-use row count was checked");
                if hole.coords().len() != hole_uses.images.len() || hole_uses.axes != uses.axes {
                    return Err(GeometryErrorKind::Invalid(
                        "native hole literal-use rows do not match their coordinates".into(),
                    )
                    .into());
                }
            }
        }
        Ok(())
    }
}

pub(super) struct NativeChordPoints {
    pub(super) points: Vec<Point>,
    pub(super) ports: Vec<EmissionPortId>,
}

pub(in crate::geometry::topology) fn materialize_native_parts(
    plan: &ExpandedEmissionPlan<'_>,
    sites: &[AssignedSite],
    ordinates: &[NodeOrdinates],
    axes: CoordinateAxes,
    budget: &mut impl Work,
) -> Result<NativeMaterializedParts> {
    budget.charge_into_error(
        u64::try_from(plan.points.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut points = Vec::with_capacity(plan.points.len());
    budget.charge_into_error(
        u64::try_from(plan.points.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut point_uses = Vec::with_capacity(plan.points.len());
    for (index, &port) in plan.points.iter().enumerate() {
        budget.charge_into_error(1)?;
        points.push(native_port_point(plan, sites, ordinates, port, axes)?);
        point_uses.push(LiteralPoint {
            output: u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?,
            axes,
            image: port,
        });
    }
    budget.charge_into_error(
        u64::try_from(plan.lines.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut lines = Vec::with_capacity(plan.lines.len());
    budget.charge_into_error(
        u64::try_from(plan.lines.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut line_uses = Vec::with_capacity(plan.lines.len());
    for (index, chords) in plan.lines.iter().enumerate() {
        let walk = native_chord_points(plan, sites, ordinates, chords, axes, false, budget)?;
        budget.charge_into_error(
            u64::try_from(walk.points.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        lines.push(CoordSeq::try_from_points_with_work(&walk.points, budget)?);
        line_uses.push(LiteralSequence {
            output: u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?,
            axes,
            images: walk.ports,
        });
    }
    budget.charge_into_error(
        u64::try_from(plan.parts.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut polygons = Vec::with_capacity(plan.parts.len());
    budget.charge_into_error(
        u64::try_from(plan.parts.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut polygon_uses = Vec::with_capacity(plan.parts.len());
    for (part_index, part) in plan.parts.iter().enumerate() {
        let output = u32::try_from(part_index).map_err(|_| ResourceError::SizeOverflow)?;
        let shell_walk =
            native_chord_points(plan, sites, ordinates, &part.shell, axes, true, budget)?;
        budget.charge_into_error(
            u64::try_from(shell_walk.points.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let shell = Ring::try_from_closed(CoordSeq::try_from_points_with_work(
            &shell_walk.points,
            budget,
        )?)?;
        budget.charge_into_error(
            u64::try_from(part.holes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut holes = Vec::with_capacity(part.holes.len());
        budget.charge_into_error(
            u64::try_from(part.holes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut hole_uses = Vec::with_capacity(part.holes.len());
        for hole in &part.holes {
            let walk = native_chord_points(plan, sites, ordinates, hole, axes, true, budget)?;
            budget.charge_into_error(
                u64::try_from(walk.points.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            holes.push(Ring::try_from_closed(CoordSeq::try_from_points_with_work(
                &walk.points,
                budget,
            )?)?);
            hole_uses.push(LiteralSequence {
                output,
                axes,
                images: walk.ports,
            });
        }
        polygons.push(Polygon::new(shell, holes));
        polygon_uses.push(LiteralPolygon {
            output,
            axes,
            shell: LiteralSequence {
                output,
                axes,
                images: shell_walk.ports,
            },
            holes: hole_uses,
        });
    }
    Ok(NativeMaterializedParts {
        axes,
        points,
        lines,
        polygons,
        literal_uses: NativeLiteralUses {
            points: point_uses,
            lines: line_uses,
            polygons: polygon_uses,
        },
    })
}

pub(in crate::geometry::topology) fn write_completed_native(
    drawing: CompletedNativeDrawing,
) -> Result<Shape> {
    Ok(drawing.sealed.into_scalar()?.0)
}
