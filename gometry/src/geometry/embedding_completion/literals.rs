use super::{ResourceError, Result, Work};
use crate::geometry::output::CertifiedShapeOrder;
use crate::geometry::segments::ordering::try_sort_by_with_work;
use crate::geometry::storage::ShapeRow;
use crate::geometry::{CoordinateAxes, GeometryErrorKind, Point, Shape};

mod flat;
mod structures;

pub(in crate::geometry) use flat::{FlatLiteralLayout, FlatRowBinding, FlatRowLayout};

#[derive(Debug, Eq, PartialEq)]
pub(in crate::geometry) struct LiteralPoint<I> {
    pub(in crate::geometry) output: u32,
    pub(in crate::geometry) axes: CoordinateAxes,
    pub(in crate::geometry) image: I,
}

#[derive(Debug, Eq, PartialEq)]
pub(in crate::geometry) struct LiteralSequence<I> {
    pub(in crate::geometry) output: u32,
    pub(in crate::geometry) axes: CoordinateAxes,
    pub(in crate::geometry) images: Vec<I>,
}

#[derive(Debug, Eq, PartialEq)]
pub(in crate::geometry) struct LiteralPolygon<I> {
    pub(in crate::geometry) output: u32,
    pub(in crate::geometry) axes: CoordinateAxes,
    pub(in crate::geometry) shell: LiteralSequence<I>,
    pub(in crate::geometry) holes: Vec<LiteralSequence<I>>,
}

pub(in crate::geometry) struct ShapeLiteralLayout<I> {
    pub(in crate::geometry) structure: Option<ShapeStructure>,
    pub(in crate::geometry) points: Vec<LiteralPoint<I>>,
    pub(in crate::geometry) lines: Vec<LiteralSequence<I>>,
    pub(in crate::geometry) polygons: Vec<LiteralPolygon<I>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::geometry) struct ShapeStructure {
    kind: crate::geometry::GeometryKind,
    axes: CoordinateAxes,
    empty: bool,
    members: ShapeMembers,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ShapeMembers {
    Primitive,
    Multipart { count: usize, empty: Vec<usize> },
    Collection(Vec<ShapeStructure>),
}

impl ShapeMembers {
    fn multipart(
        members: impl ExactSizeIterator<Item = crate::geometry::MultiMember<usize>>,
    ) -> Self {
        Self::Multipart {
            count: members.len(),
            empty: members
                .enumerate()
                .filter_map(|(index, member)| {
                    matches!(member, crate::geometry::MultiMember::Empty).then_some(index)
                })
                .collect(),
        }
    }
}

impl ShapeStructure {
    pub(in crate::geometry) fn capture(shape: &Shape) -> Self {
        let row = ShapeRow::Shape(shape);
        let mut header = Self::primitive(row.kind(), row.axes(), row.is_empty());
        match shape {
            Shape::GeometryCollection(parts) => {
                header.members =
                    ShapeMembers::Collection(parts.iter().map(Self::capture).collect());
            },
            Shape::MultiPoint(parts) => {
                header.members = ShapeMembers::multipart(parts.members());
            },
            Shape::MultiLineString(parts) => {
                header.members = ShapeMembers::multipart(parts.members());
            },
            Shape::MultiPolygon(parts) => {
                header.members = ShapeMembers::multipart(parts.members());
            },
            Shape::Point(_) | Shape::LineString(_) | Shape::Polygon(_) | Shape::Empty(..) => {},
        }
        header
    }

    pub(in crate::geometry) const fn primitive(
        kind: crate::geometry::GeometryKind,
        axes: CoordinateAxes,
        empty: bool,
    ) -> Self {
        Self {
            kind,
            axes,
            empty,
            members: ShapeMembers::Primitive,
        }
    }

    fn matches(&self, row: ShapeRow<'_>) -> bool {
        if !matches_shape_header(self.kind, self.axes, self.empty, row) {
            return false;
        }
        let shape = match row {
            ShapeRow::Handle(handle) => handle.shape(),
            ShapeRow::Shape(shape) => shape,
            ShapeRow::Point(_) | ShapeRow::Packed(_) => {
                return matches!(self.members, ShapeMembers::Primitive);
            },
        };
        match shape {
            Shape::GeometryCollection(parts) => match &self.members {
                ShapeMembers::Collection(expected) => {
                    expected.len() == parts.len()
                        && expected
                            .iter()
                            .zip(parts.iter())
                            .all(|(expected, actual)| expected.matches(ShapeRow::Shape(actual)))
                },
                ShapeMembers::Primitive | ShapeMembers::Multipart { .. } => false,
            },
            Shape::MultiPoint(parts) => self.matches_members(parts.members()),
            Shape::MultiLineString(parts) => self.matches_members(parts.members()),
            Shape::MultiPolygon(parts) => self.matches_members(parts.members()),
            Shape::Point(_) | Shape::LineString(_) | Shape::Polygon(_) | Shape::Empty(..) => {
                matches!(self.members, ShapeMembers::Primitive)
            },
        }
    }

    fn matches_members(
        &self,
        members: impl ExactSizeIterator<Item = crate::geometry::MultiMember<usize>>,
    ) -> bool {
        match &self.members {
            ShapeMembers::Multipart { count, empty } => {
                matches_multipart_members(*count, empty.iter().copied(), members)
            },
            ShapeMembers::Primitive | ShapeMembers::Collection(_) => false,
        }
    }
}

fn matches_shape_header(
    kind: crate::geometry::GeometryKind,
    axes: CoordinateAxes,
    empty: bool,
    row: ShapeRow<'_>,
) -> bool {
    kind == row.kind() && axes == row.axes() && empty == row.is_empty()
}

fn matches_multipart_members(
    count: usize,
    empty: impl Iterator<Item = usize>,
    members: impl ExactSizeIterator<Item = crate::geometry::MultiMember<usize>>,
) -> bool {
    count == members.len()
        && empty.eq(members.enumerate().filter_map(|(index, member)| {
            matches!(member, crate::geometry::MultiMember::Empty).then_some(index)
        }))
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry) struct LiteralAddress {
    pub(in crate::geometry) operation: u32,
    pub(in crate::geometry) logical_row: usize,
    pub(in crate::geometry) group_row: Option<usize>,
    pub(in crate::geometry) value_row: Option<usize>,
    pub(in crate::geometry) output: u32,
}

#[derive(Clone, Copy)]
pub(in crate::geometry) struct LiteralRowAddress {
    pub(super) operation: u32,
    pub(super) logical_row: usize,
    pub(super) group_row: Option<usize>,
    pub(super) value_row: Option<usize>,
}

impl LiteralRowAddress {
    pub(in crate::geometry) const fn scalar() -> Self {
        Self {
            operation: 0,
            logical_row: 0,
            group_row: None,
            value_row: None,
        }
    }

    const fn output(self, output: u32) -> LiteralAddress {
        LiteralAddress {
            operation: self.operation,
            logical_row: self.logical_row,
            group_row: self.group_row,
            value_row: self.value_row,
            output,
        }
    }
}

pub(in crate::geometry) struct LiteralRowLayout<I> {
    pub(in crate::geometry) operation: u32,
    pub(in crate::geometry) logical_row: usize,
    pub(in crate::geometry) group_row: Option<usize>,
    pub(in crate::geometry) value_row: Option<usize>,
    pub(in crate::geometry) layout: ShapeLiteralLayout<I>,
}

impl<I> ShapeLiteralLayout<I> {
    pub(in crate::geometry) fn with_structure(mut self, shape: &Shape) -> Self {
        self.structure = Some(ShapeStructure::capture(shape));
        self
    }

    fn validate_structure(&self, row: ShapeRow<'_>) -> Result<()> {
        if !self
            .structure
            .as_ref()
            .is_some_and(|expected| expected.matches(row))
        {
            return Err(GeometryErrorKind::Invalid(
                "final geometry kind, axes, empty state, or member structure changed".into(),
            )
            .into());
        }
        Ok(())
    }
    pub(in crate::geometry) fn in_output_order(
        self,
        order: &CertifiedShapeOrder,
        work: &mut impl Work,
    ) -> Result<Self> {
        Ok(Self {
            structure: self.structure,
            points: reorder_rows(self.points, &order.points, work)?,
            lines: reorder_rows(self.lines, &order.lines, work)?,
            polygons: reorder_rows(self.polygons, &order.polygons, work)?,
        })
    }
}

pub(in crate::geometry) fn reorder_rows<T>(
    rows: Vec<T>,
    order: &[usize],
    work: &mut impl Work,
) -> Result<Vec<T>> {
    if rows.len() != order.len() {
        return Err(
            GeometryErrorKind::Invalid("literal output order omits a physical row".into()).into(),
        );
    }
    work.charge_into_error(u64::try_from(rows.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut rows = rows.into_iter().map(Some).collect::<Vec<_>>();
    work.charge_into_error(u64::try_from(order.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut output = Vec::with_capacity(order.len());
    for &index in order {
        work.charge_into_error(1)?;
        let row = rows.get_mut(index).ok_or_else(|| {
            GeometryErrorKind::Invalid("literal output order references an unknown row".into())
        })?;
        output.push(row.take().ok_or_else(|| {
            GeometryErrorKind::Invalid("literal output order repeats a row".into())
        })?);
    }
    Ok(output)
}

fn validate_dense_output_ids(
    values: impl ExactSizeIterator<Item = u32>,
    count: usize,
    work: &mut impl Work,
) -> Result<()> {
    work.charge_into_error(u64::try_from(count).map_err(|_| ResourceError::SizeOverflow)?)?;
    if values.len() != count {
        return Err(GeometryErrorKind::Invalid(
            "literal logical output IDs do not cover their rows".into(),
        )
        .into());
    }

    let mut unordered_ids: Option<Vec<u32>> = None;
    for (index, id) in values.enumerate() {
        if let Some(ids) = &mut unordered_ids {
            ids.push(id);
        } else if usize::try_from(id).ok() != Some(index) {
            let mut ids = Vec::with_capacity(count);
            ids.extend(
                (0..index)
                    .map(|value| u32::try_from(value).expect("the ordered ID prefix fits in u32")),
            );
            ids.push(id);
            unordered_ids = Some(ids);
        }
    }

    let Some(mut ids) = unordered_ids else {
        return Ok(());
    };
    try_sort_by_with_work(&mut ids, work, |left, right, work| {
        work.charge_into_error(1)?;
        Ok(left.cmp(right))
    })?;
    if ids
        .iter()
        .enumerate()
        .any(|(index, &id)| usize::try_from(id).ok() != Some(index))
    {
        return Err(GeometryErrorKind::Invalid(
            "literal logical output IDs are missing or repeated".into(),
        )
        .into());
    }
    Ok(())
}

pub(super) fn validate_shape_layout<I>(
    layout: &ShapeLiteralLayout<I>,
    work: &mut impl Work,
) -> Result<()> {
    validate_dense_output_ids(
        layout.points.iter().map(|row| row.output),
        layout.points.len(),
        work,
    )?;
    validate_dense_output_ids(
        layout.lines.iter().map(|row| row.output),
        layout.lines.len(),
        work,
    )?;
    validate_dense_output_ids(
        layout.polygons.iter().map(|row| row.output),
        layout.polygons.len(),
        work,
    )?;
    for polygon in &layout.polygons {
        work.charge_into_error(1)?;
        if polygon.shell.output != polygon.output
            || polygon
                .holes
                .iter()
                .any(|hole| hole.output != polygon.output)
        {
            return Err(GeometryErrorKind::Invalid(
                "polygon ring rows lost their logical output identity".into(),
            )
            .into());
        }
    }
    Ok(())
}

pub(in crate::geometry) fn visit_shape_literals<I: Copy, W: Work>(
    shape: &Shape,
    layout: &ShapeLiteralLayout<I>,
    row_address: LiteralRowAddress,
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
    fn visit_sequence<I: Copy, W: Work>(
        sequence: &crate::geometry::CoordSeq,
        locations: &LiteralSequence<I>,
        row_address: LiteralRowAddress,
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
        if sequence.len() != locations.images.len() {
            return Err(GeometryErrorKind::Invalid(
                "sealed coordinate sequence differs from its physical literal map".into(),
            )
            .into());
        }
        let axes = sequence.axes();
        if axes != locations.axes {
            return Err(GeometryErrorKind::Invalid(
                "sealed sequence axes differ from its logical output policy".into(),
            )
            .into());
        }
        for (&image, point) in locations.images.iter().zip(sequence.points()) {
            work.charge_into_error(1)?;
            visit(
                row_address.output(locations.output),
                image,
                point,
                locations.axes,
                axes,
                work,
            )?;
        }
        Ok(())
    }

    fn visit_polygon<I: Copy, W: Work>(
        polygon: &crate::geometry::Polygon,
        locations: &LiteralPolygon<I>,
        row_address: LiteralRowAddress,
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
        visit_sequence(
            polygon.shell.coords(),
            &locations.shell,
            row_address,
            work,
            visit,
        )?;
        if polygon.holes.len() != locations.holes.len()
            || locations.shell.axes != locations.axes
            || locations
                .holes
                .iter()
                .any(|hole| hole.axes != locations.axes)
        {
            return Err(GeometryErrorKind::Invalid(
                "sealed polygon holes differ from their physical literal map".into(),
            )
            .into());
        }
        for (ring, locations) in polygon.holes.iter().zip(&locations.holes) {
            visit_sequence(ring.coords(), locations, row_address, work, visit)?;
        }
        Ok(())
    }

    fn visit_shape<I: Copy, W: Work>(
        shape: &Shape,
        layout: &ShapeLiteralLayout<I>,
        row_address: LiteralRowAddress,
        point_row: &mut usize,
        line_row: &mut usize,
        polygon_row: &mut usize,
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
        match shape {
            Shape::Point(point) => {
                let location = layout.points.get(*point_row).ok_or_else(|| {
                    GeometryErrorKind::Invalid("sealed point has no physical literal row".into())
                })?;
                *point_row = point_row
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
                work.charge_into_error(1)?;
                visit(
                    row_address.output(location.output),
                    location.image,
                    *point,
                    location.axes,
                    CoordinateAxes::from_point(*point),
                    work,
                )
            },
            Shape::MultiPoint(points) => {
                let end = point_row
                    .checked_add(points.len())
                    .ok_or(ResourceError::SizeOverflow)?;
                let locations = layout.points.get(*point_row..end).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "sealed multipoint has incomplete physical literal rows".into(),
                    )
                })?;
                *point_row = end;
                for (location, point) in locations.iter().zip(points.points()) {
                    work.charge_into_error(1)?;
                    visit(
                        row_address.output(location.output),
                        location.image,
                        point,
                        location.axes,
                        points.axes(),
                        work,
                    )?;
                }
                Ok(())
            },
            Shape::LineString(line) => {
                let locations = layout.lines.get(*line_row).ok_or_else(|| {
                    GeometryErrorKind::Invalid("sealed line has no physical literal row".into())
                })?;
                *line_row = line_row.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                visit_sequence(line.as_coords(), locations, row_address, work, visit)
            },
            Shape::MultiLineString(lines) => {
                for line in lines.as_slice() {
                    let locations = layout.lines.get(*line_row).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "sealed multiline has incomplete physical literal rows".into(),
                        )
                    })?;
                    *line_row = line_row.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                    visit_sequence(line.as_coords(), locations, row_address, work, visit)?;
                }
                Ok(())
            },
            Shape::Polygon(polygon) => {
                let locations = layout.polygons.get(*polygon_row).ok_or_else(|| {
                    GeometryErrorKind::Invalid("sealed polygon has no physical literal row".into())
                })?;
                *polygon_row = polygon_row
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
                visit_polygon(polygon, locations, row_address, work, visit)
            },
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons.as_slice() {
                    let locations = layout.polygons.get(*polygon_row).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "sealed multipolygon has incomplete physical literal rows".into(),
                        )
                    })?;
                    *polygon_row = polygon_row
                        .checked_add(1)
                        .ok_or(ResourceError::SizeOverflow)?;
                    visit_polygon(polygon, locations, row_address, work, visit)?;
                }
                Ok(())
            },
            Shape::GeometryCollection(collection) => {
                for part in collection {
                    visit_shape(
                        part,
                        layout,
                        row_address,
                        point_row,
                        line_row,
                        polygon_row,
                        work,
                        visit,
                    )?;
                }
                Ok(())
            },
            Shape::Empty(..) => Ok(()),
        }
    }

    layout.validate_structure(ShapeRow::Shape(shape))?;
    let mut point_row = 0_usize;
    let mut line_row = 0_usize;
    let mut polygon_row = 0_usize;
    visit_shape(
        shape,
        layout,
        row_address,
        &mut point_row,
        &mut line_row,
        &mut polygon_row,
        work,
        visit,
    )?;
    if point_row != layout.points.len()
        || line_row != layout.lines.len()
        || polygon_row != layout.polygons.len()
    {
        return Err(GeometryErrorKind::Invalid(
            "sealed output left physical literal rows unused".into(),
        )
        .into());
    }
    Ok(())
}
