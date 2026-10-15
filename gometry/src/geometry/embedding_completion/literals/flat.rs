use std::ops::Range;

use super::structures::{CapturedStructure, StructureHeaders};
use super::{
    LiteralAddress, LiteralPoint, LiteralRowLayout, ShapeLiteralLayout, ShapeStructure,
    validate_dense_output_ids,
};
use crate::error::{ResourceError, Result};
use crate::geometry::storage::row::CoordRow;
use crate::geometry::storage::{PackedRow, ShapeRow};
use crate::geometry::{
    CoordinateAxes, Coordinates, GeometryErrorKind, GeometryKind, Point, Polygon, Shape,
};
use crate::work::Work;

pub(in crate::geometry) enum FlatRowBinding {
    Checked(usize),
    OtherProducer,
}

pub(in crate::geometry) struct FlatRowLayout {
    pub(in crate::geometry) address: LiteralAddress,
    structure: CapturedStructure,
    points: usize,
    lines: usize,
    polygons: usize,
}

pub(in crate::geometry) struct FlatLiteralLayout<I> {
    pub(in crate::geometry) rows: Vec<Option<FlatRowBinding>>,
    pub(in crate::geometry) checked_rows: Vec<FlatRowLayout>,
    headers: StructureHeaders,
    points: Vec<LiteralPoint<I>>,
    lines: Vec<FlatSequence>,
    polygons: Vec<FlatPolygon>,
    rings: Vec<FlatSequence>,
    images: Vec<I>,
}

struct FlatSequence {
    output: u32,
    axes: CoordinateAxes,
    images: Range<usize>,
}

struct FlatPolygon {
    output: u32,
    axes: CoordinateAxes,
    rings: Range<usize>,
}

impl<I> FlatLiteralLayout<I> {
    pub(in crate::geometry) fn with_capacity(rows: usize) -> Self {
        Self {
            rows: Vec::with_capacity(rows),
            checked_rows: Vec::with_capacity(rows),
            headers: StructureHeaders::default(),
            points: Vec::new(),
            lines: Vec::new(),
            polygons: Vec::new(),
            rings: Vec::new(),
            images: Vec::new(),
        }
    }

    pub(in crate::geometry) fn push_layout(&mut self, row: LiteralRowLayout<I>) -> Result<()> {
        let ShapeLiteralLayout {
            structure,
            points,
            lines,
            polygons,
        } = row.layout;
        let structure = self.headers.adopt(
            structure.ok_or_else(|| invalid("final publication has no accepted structure"))?,
        );
        self.points.extend(points);
        for line in lines {
            let start = self.images.len();
            self.images.extend(line.images);
            self.lines.push(FlatSequence {
                output: line.output,
                axes: line.axes,
                images: start..self.images.len(),
            });
        }
        for polygon in polygons {
            let start = self.rings.len();
            for ring in std::iter::once(polygon.shell).chain(polygon.holes) {
                let image_start = self.images.len();
                self.images.extend(ring.images);
                self.rings.push(FlatSequence {
                    output: ring.output,
                    axes: ring.axes,
                    images: image_start..self.images.len(),
                });
            }
            self.polygons.push(FlatPolygon {
                output: polygon.output,
                axes: polygon.axes,
                rings: start..self.rings.len(),
            });
        }
        let index = self.checked_rows.len();
        self.checked_rows.push(FlatRowLayout {
            address: LiteralAddress {
                operation: row.operation,
                logical_row: row.logical_row,
                group_row: row.group_row,
                value_row: row.value_row,
                output: 0,
            },
            structure,
            points: self.points.len(),
            lines: self.lines.len(),
            polygons: self.polygons.len(),
        });
        self.rows.push(Some(FlatRowBinding::Checked(index)));
        Ok(())
    }

    pub(in crate::geometry) fn capture_shape(
        &mut self,
        shape: &Shape,
        address: LiteralAddress,
        emit: &mut impl FnMut(LiteralAddress, Point) -> Result<I>,
    ) -> Result<()> {
        let mut row = FlatRowLayout {
            address,
            structure: self.headers.capture(shape),
            points: self.points.len(),
            lines: self.lines.len(),
            polygons: self.polygons.len(),
        };
        self.capture_parts(shape, &row, emit)?;
        row.points = self.points.len();
        row.lines = self.lines.len();
        row.polygons = self.polygons.len();
        let index = self.checked_rows.len();
        self.checked_rows.push(row);
        self.rows.push(Some(FlatRowBinding::Checked(index)));
        Ok(())
    }

    pub(in crate::geometry) fn capture_line(
        &mut self,
        coordinates: &(impl Coordinates + ?Sized),
        axes: CoordinateAxes,
        address: LiteralAddress,
        emit: &mut impl FnMut(LiteralAddress, Point) -> Result<I>,
    ) -> Result<()> {
        let sequence = self.capture_sequence(coordinates, axes, address, 0, emit)?;
        self.lines.push(sequence);
        self.push_primitive(
            GeometryKind::LineString,
            axes,
            coordinates.coord_count() == 0,
            address,
        );
        Ok(())
    }

    pub(in crate::geometry) fn capture_polygon_ring(
        &mut self,
        coordinates: &(impl Coordinates + ?Sized),
        axes: CoordinateAxes,
        address: LiteralAddress,
        emit: &mut impl FnMut(LiteralAddress, Point) -> Result<I>,
    ) -> Result<()> {
        let start = self.rings.len();
        let ring = self.capture_sequence(coordinates, axes, address, 0, emit)?;
        self.rings.push(ring);
        self.polygons.push(FlatPolygon {
            output: 0,
            axes,
            rings: start..self.rings.len(),
        });
        self.push_primitive(
            GeometryKind::Polygon,
            axes,
            coordinates.coord_count() == 0,
            address,
        );
        Ok(())
    }

    fn push_primitive(
        &mut self,
        kind: GeometryKind,
        axes: CoordinateAxes,
        empty: bool,
        address: LiteralAddress,
    ) {
        let index = self.checked_rows.len();
        self.checked_rows.push(FlatRowLayout {
            address,
            structure: self
                .headers
                .adopt(ShapeStructure::primitive(kind, axes, empty)),
            points: self.points.len(),
            lines: self.lines.len(),
            polygons: self.polygons.len(),
        });
        self.rows.push(Some(FlatRowBinding::Checked(index)));
    }

    fn capture_parts(
        &mut self,
        shape: &Shape,
        row: &FlatRowLayout,
        emit: &mut impl FnMut(LiteralAddress, Point) -> Result<I>,
    ) -> Result<()> {
        match shape {
            Shape::Point(point) => self.capture_point(*point, point.axes, row, emit)?,
            Shape::MultiPoint(points) => {
                for point in points.points() {
                    self.capture_point(point, points.axes(), row, emit)?;
                }
            },
            Shape::LineString(line) => {
                let output = output_id(
                    self.lines
                        .len()
                        .checked_sub(row.lines)
                        .ok_or(ResourceError::SizeOverflow)?,
                )?;
                let sequence = self.capture_sequence(
                    line.as_coords(),
                    line.as_coords().axes(),
                    row.address,
                    output,
                    emit,
                )?;
                self.lines.push(sequence);
            },
            Shape::MultiLineString(lines) => {
                for line in lines.as_slice() {
                    let output = output_id(
                        self.lines
                            .len()
                            .checked_sub(row.lines)
                            .ok_or(ResourceError::SizeOverflow)?,
                    )?;
                    let sequence = self.capture_sequence(
                        line.as_coords(),
                        line.as_coords().axes(),
                        row.address,
                        output,
                        emit,
                    )?;
                    self.lines.push(sequence);
                }
            },
            Shape::Polygon(polygon) => self.capture_polygon(polygon, row, emit)?,
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons.as_slice() {
                    self.capture_polygon(polygon, row, emit)?;
                }
            },
            Shape::GeometryCollection(parts) => {
                for part in parts {
                    self.capture_parts(part, row, emit)?;
                }
            },
            Shape::Empty(..) => {},
        }
        Ok(())
    }

    fn capture_point(
        &mut self,
        point: Point,
        axes: CoordinateAxes,
        row: &FlatRowLayout,
        emit: &mut impl FnMut(LiteralAddress, Point) -> Result<I>,
    ) -> Result<()> {
        let output = output_id(
            self.points
                .len()
                .checked_sub(row.points)
                .ok_or(ResourceError::SizeOverflow)?,
        )?;
        let image = emit(
            LiteralAddress {
                output,
                ..row.address
            },
            point,
        )?;
        if self.points.is_empty() {
            self.points.reserve_exact(self.rows.capacity());
        }
        self.points.push(LiteralPoint {
            output,
            axes,
            image,
        });
        Ok(())
    }

    fn capture_sequence(
        &mut self,
        coordinates: &(impl Coordinates + ?Sized),
        axes: CoordinateAxes,
        address: LiteralAddress,
        output: u32,
        emit: &mut impl FnMut(LiteralAddress, Point) -> Result<I>,
    ) -> Result<FlatSequence> {
        let start = self.images.len();
        self.images.reserve(coordinates.coord_count());
        for point in coordinates.iter_coords() {
            self.images
                .push(emit(LiteralAddress { output, ..address }, point)?);
        }
        Ok(FlatSequence {
            output,
            axes,
            images: start..self.images.len(),
        })
    }

    fn capture_polygon(
        &mut self,
        polygon: &Polygon,
        row: &FlatRowLayout,
        emit: &mut impl FnMut(LiteralAddress, Point) -> Result<I>,
    ) -> Result<()> {
        let output = output_id(
            self.polygons
                .len()
                .checked_sub(row.polygons)
                .ok_or(ResourceError::SizeOverflow)?,
        )?;
        let start = self.rings.len();
        for ring in std::iter::once(&polygon.shell).chain(polygon.holes.iter()) {
            let sequence = self.capture_sequence(
                ring.coords(),
                ring.coords().axes(),
                row.address,
                output,
                emit,
            )?;
            self.rings.push(sequence);
        }
        self.polygons.push(FlatPolygon {
            output,
            axes: polygon.axes(),
            rings: start..self.rings.len(),
        });
        Ok(())
    }
}

impl<I: Copy> FlatLiteralLayout<I> {
    pub(in crate::geometry) fn visit_row<W: Work>(
        &self,
        actual: ShapeRow<'_>,
        index: usize,
        work: &mut W,
        visit: &mut impl FnMut(
            LiteralAddress,
            I,
            Point,
            CoordinateAxes,
            CoordinateAxes,
            &mut W,
        ) -> Result<()>,
    ) -> Result<()> {
        let row = self
            .checked_rows
            .get(index)
            .ok_or_else(|| invalid("row has no checked layout"))?;
        let previous = index
            .checked_sub(1)
            .and_then(|previous| self.checked_rows.get(previous));
        if !self.headers.matches(&row.structure, actual) {
            return Err(invalid(
                "final geometry kind, axes, empty state, or member structure changed",
            ));
        }
        let points = self
            .points
            .get(previous.map_or(0, |row| row.points)..row.points)
            .ok_or_else(|| invalid("point range is outside its publication"))?;
        let lines = self
            .lines
            .get(previous.map_or(0, |row| row.lines)..row.lines)
            .ok_or_else(|| invalid("line range is outside its publication"))?;
        let polygons = self
            .polygons
            .get(previous.map_or(0, |row| row.polygons)..row.polygons)
            .ok_or_else(|| invalid("polygon range is outside its publication"))?;
        validate_dense_output_ids(points.iter().map(|point| point.output), points.len(), work)?;
        validate_dense_output_ids(lines.iter().map(|line| line.output), lines.len(), work)?;
        validate_dense_output_ids(
            polygons.iter().map(|polygon| polygon.output),
            polygons.len(),
            work,
        )?;
        for polygon in polygons {
            work.charge_into_error(1)?;
            let rings = self
                .rings
                .get(polygon.rings.clone())
                .ok_or_else(|| invalid("polygon ring range is outside its publication"))?;
            if rings.is_empty() || rings.iter().any(|ring| ring.output != polygon.output) {
                return Err(invalid(
                    "polygon ring rows lost their logical output identity",
                ));
            }
        }
        let mut walker = FlatRowWalker {
            layout: self,
            points: points.iter(),
            lines: lines.iter(),
            polygons: polygons.iter(),
            address: row.address,
            work,
            visit,
        };
        walker.row(actual)?;
        if walker.points.next().is_some()
            || walker.lines.next().is_some()
            || walker.polygons.next().is_some()
        {
            return Err(invalid("sealed output left physical literal rows unused"));
        }
        Ok(())
    }
}

struct FlatRowWalker<'a, 'w, I, W, F> {
    layout: &'a FlatLiteralLayout<I>,
    points: std::slice::Iter<'a, LiteralPoint<I>>,
    lines: std::slice::Iter<'a, FlatSequence>,
    polygons: std::slice::Iter<'a, FlatPolygon>,
    address: LiteralAddress,
    work: &'w mut W,
    visit: &'w mut F,
}

impl<I: Copy, W: Work, F> FlatRowWalker<'_, '_, I, W, F>
where
    F: FnMut(LiteralAddress, I, Point, CoordinateAxes, CoordinateAxes, &mut W) -> Result<()>,
{
    fn point(&mut self, point: Point, axes: CoordinateAxes) -> Result<()> {
        let literal = self
            .points
            .next()
            .ok_or_else(|| invalid("sealed point has no physical literal row"))?;
        self.work.charge_into_error(1)?;
        (self.visit)(
            LiteralAddress {
                output: literal.output,
                ..self.address
            },
            literal.image,
            point,
            literal.axes,
            axes,
            self.work,
        )
    }

    fn sequence(
        &mut self,
        coordinates: &(impl Coordinates + ?Sized),
        axes: CoordinateAxes,
        sequence: &FlatSequence,
    ) -> Result<()> {
        let images = self
            .layout
            .images
            .get(sequence.images.clone())
            .ok_or_else(|| invalid("sequence image range is outside its publication"))?;
        if coordinates.coord_count() != images.len() || axes != sequence.axes {
            return Err(invalid(
                "sealed row coordinates differ from their physical literal map",
            ));
        }
        for (&image, point) in images.iter().zip(coordinates.iter_coords()) {
            self.work.charge_into_error(1)?;
            (self.visit)(
                LiteralAddress {
                    output: sequence.output,
                    ..self.address
                },
                image,
                point,
                sequence.axes,
                axes,
                self.work,
            )?;
        }
        Ok(())
    }

    fn line(
        &mut self,
        coordinates: &(impl Coordinates + ?Sized),
        axes: CoordinateAxes,
    ) -> Result<()> {
        let sequence = self
            .lines
            .next()
            .ok_or_else(|| invalid("sealed line has no physical literal row"))?;
        self.sequence(coordinates, axes, sequence)
    }

    fn polygon<'row>(
        &mut self,
        axes: CoordinateAxes,
        mut actual: impl Iterator<Item = CoordRow<'row>>,
    ) -> Result<()> {
        let polygon = self
            .polygons
            .next()
            .ok_or_else(|| invalid("sealed polygon has no physical literal row"))?;
        let expected = self
            .layout
            .rings
            .get(polygon.rings.clone())
            .ok_or_else(|| invalid("polygon ring range is outside its publication"))?;
        if expected.iter().any(|ring| ring.axes != polygon.axes) {
            return Err(invalid(
                "sealed polygon rings lost their logical output identity",
            ));
        }
        for ring in expected {
            let coordinates = actual
                .next()
                .ok_or_else(|| invalid("sealed polygon has a missing ring window"))?;
            self.sequence(&coordinates, axes, ring)?;
        }
        if actual.next().is_some() {
            return Err(invalid("sealed polygon has an unmapped ring window"));
        }
        Ok(())
    }

    fn polygon_shape(&mut self, polygon: &Polygon) -> Result<()> {
        let rings = std::iter::once(&polygon.shell)
            .chain(polygon.holes.iter())
            .map(|ring| CoordRow::new(ring.coords(), 0..ring.len()));
        self.polygon(polygon.axes(), rings)
    }

    fn row(&mut self, row: ShapeRow<'_>) -> Result<()> {
        match row {
            ShapeRow::Handle(handle) => self.shape(handle.shape()),
            ShapeRow::Shape(shape) => self.shape(shape),
            ShapeRow::Point(point) => self.point(point, point.axes),
            ShapeRow::Packed(packed @ PackedRow::Points { .. }) => self.point(
                packed
                    .point_value()
                    .ok_or_else(|| invalid("packed point has no coordinate"))?,
                packed.axes(),
            ),
            ShapeRow::Packed(PackedRow::Lines { .. }) => {
                let coordinates = row
                    .line_coords()
                    .ok_or_else(|| invalid("packed line has no coordinate window"))?;
                self.line(&coordinates, row.axes())
            },
            ShapeRow::Packed(PackedRow::Polygons { .. }) => self.polygon(
                row.axes(),
                row.polygon_rings()
                    .ok_or_else(|| invalid("sealed polygon has no ring windows"))?,
            ),
        }
    }

    fn shape(&mut self, shape: &Shape) -> Result<()> {
        match shape {
            Shape::Point(point) => self.point(*point, point.axes),
            Shape::MultiPoint(points) => {
                for point in points.points() {
                    self.point(point, points.axes())?;
                }
                Ok(())
            },
            Shape::LineString(line) => self.line(line.as_coords(), line.axes()),
            Shape::MultiLineString(lines) => {
                for line in lines.as_slice() {
                    self.line(line.as_coords(), line.axes())?;
                }
                Ok(())
            },
            Shape::Polygon(polygon) => self.polygon_shape(polygon),
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons.as_slice() {
                    self.polygon_shape(polygon)?;
                }
                Ok(())
            },
            Shape::GeometryCollection(parts) => {
                for part in parts {
                    self.shape(part)?;
                }
                Ok(())
            },
            Shape::Empty(..) => Ok(()),
        }
    }
}

fn output_id(index: usize) -> Result<u32> {
    let output = u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?;
    output.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
    Ok(output)
}

fn invalid(message: &'static str) -> crate::error::Error {
    GeometryErrorKind::Invalid(message.into()).into()
}
