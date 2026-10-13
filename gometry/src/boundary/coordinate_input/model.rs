use super::*;

pub(crate) struct CoordinateInput {
    pub values: Vec<f64>,
    pub scalar: bool,
}

#[derive(Clone, Copy)]
pub(crate) enum CoordSeqShape {
    Empty,
    One([usize; 1]),
    Two([usize; 2]),
    Three([usize; 3]),
    Four([usize; 4]),
}

impl CoordSeqShape {
    pub(super) const fn from_slice(dimensions: &[usize]) -> Option<Self> {
        match dimensions {
            [] => Some(Self::Empty),
            [first] => Some(Self::One([*first])),
            [first, second] => Some(Self::Two([*first, *second])),
            [first, second, third] => Some(Self::Three([*first, *second, *third])),
            [first, second, third, fourth] => Some(Self::Four([*first, *second, *third, *fourth])),
            _ => None,
        }
    }

    pub(crate) const fn as_slice(&self) -> &[usize] {
        match self {
            Self::Empty => &[],
            Self::One(dimensions) => dimensions.as_slice(),
            Self::Two(dimensions) => dimensions.as_slice(),
            Self::Three(dimensions) => dimensions.as_slice(),
            Self::Four(dimensions) => dimensions.as_slice(),
        }
    }
}

pub(crate) struct CoordSeqBuffer {
    pub coordinates: CoordSeq,
    pub shape: CoordSeqShape,
}

impl CoordSeqBuffer {
    pub(crate) fn row(&self, index: usize) -> crate::error::Result<Self> {
        let shape = self.shape.as_slice();
        let Some((&rows, row_shape)) = shape.split_first() else {
            return Err(crate::error::ResourceError::SizeOverflow.into());
        };
        if index >= rows {
            return Err(crate::error::ResourceError::SizeOverflow.into());
        }
        let row_coordinates = row_shape
            .iter()
            .try_fold(1_usize, |total, &length| total.checked_mul(length));
        let Some(row_coordinates) = row_coordinates else {
            return Err(crate::error::ResourceError::SizeOverflow.into());
        };
        let start = index
            .checked_mul(row_coordinates)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let end = start
            .checked_add(row_coordinates)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let window = CoordWindow::checked(start..end, self.coordinates.len())?;
        let Some(shape) = CoordSeqShape::from_slice(row_shape) else {
            return Err(crate::error::ResourceError::SizeOverflow.into());
        };
        Ok(Self {
            coordinates: self.coordinates.view(window),
            shape,
        })
    }
}

#[derive(Clone, Copy)]
pub(crate) enum CoordSeqStructure {
    Sequence,
    Polygon,
    MultiLineString,
    MultiPolygon,
}

#[derive(Clone, Copy)]
pub(crate) enum CoordSeqBufferMode {
    Geometry(CoordSeqStructure),
    GeometryArray(CoordSeqStructure),
}

impl CoordSeqBufferMode {
    pub(super) const fn accepts_rank(self, rank: usize) -> bool {
        let (structure, geometry_rank) = match self {
            Self::Geometry(structure) => (structure, Some(rank)),
            Self::GeometryArray(structure) => (structure, rank.checked_sub(1)),
        };
        let Some(geometry_rank) = geometry_rank else {
            return false;
        };
        match structure {
            CoordSeqStructure::Sequence => geometry_rank == 2,
            CoordSeqStructure::Polygon => geometry_rank == 2 || geometry_rank == 3,
            CoordSeqStructure::MultiLineString => geometry_rank == 3,
            CoordSeqStructure::MultiPolygon => geometry_rank == 4,
        }
    }
}
