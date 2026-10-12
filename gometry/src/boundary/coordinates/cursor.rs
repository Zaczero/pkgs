use std::sync::Arc;

use super::{
    CoordSeq, CoordinateFrame, CoordinateOwner, CoordinatePointCursor, CoordinateStep,
    CoordinateView, Point, Shape,
};
use crate::geometry::CoordWindow;
use crate::geometry::storage::{
    self, GeometryArrayStorage, PackedStorage, ValidityMask, csr_value_window,
};

impl CoordinatePointCursor {
    pub(super) fn new(view: CoordinateView, reverse: bool) -> Self {
        let mut frames = Vec::new();
        match &view.owner {
            CoordinateOwner::Shape(shape) => {
                frames.push(CoordinateFrame::Shape(shape.shape().clone()));
            },
            CoordinateOwner::Array(rows) => {
                let (row_count, compact_point_rank) = match rows.storage() {
                    GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
                        let point_rows = storage::point_logical_len(coords, row_map.as_deref());
                        let compact = rows
                            .is_compact_nullable()
                            .then(|| rows.missing().expect("compact rows carry validity"));
                        (
                            compact.map_or(point_rows, ValidityMask::len),
                            compact.map(|mask| if reverse { mask.present_count() } else { 0 }),
                        )
                    },
                    GeometryArrayStorage::Mixed(_)
                    | GeometryArrayStorage::Packed(
                        PackedStorage::Lines { .. } | PackedStorage::Polygons { .. },
                    ) => (rows.len(), None),
                };
                frames.push(CoordinateFrame::ArrayRows {
                    rows: rows.clone(),
                    remaining: 0..row_count,
                    compact_point_rank,
                });
            },
        }
        Self {
            view,
            frames,
            reverse,
        }
    }

    pub(crate) fn next_point(&mut self) -> Option<Point> {
        loop {
            if matches!(self.frames.last(), Some(CoordinateFrame::Shape(_))) {
                let Some(CoordinateFrame::Shape(shape)) = self.frames.pop() else {
                    unreachable!("shape frame checked above");
                };
                match shape {
                    Shape::Point(point) => return Some(point),
                    Shape::MultiPoint(points) => {
                        let coordinates = CoordSeq::clone(&points);
                        self.frames.push(CoordinateFrame::Sequence {
                            remaining: 0..coordinates.len(),
                            coordinates,
                        });
                    },
                    Shape::LineString(line) => {
                        let coordinates = line.as_coords().clone();
                        self.frames.push(CoordinateFrame::Sequence {
                            remaining: 0..coordinates.len(),
                            coordinates,
                        });
                    },
                    Shape::MultiLineString(lines) => {
                        let len = lines.as_slice().len();
                        self.frames.push(CoordinateFrame::MultiLines {
                            shape: Shape::MultiLineString(lines),
                            remaining: 0..len,
                        });
                    },
                    Shape::Polygon(polygon) => {
                        let rings = polygon.holes.len().wrapping_add(1);
                        self.frames.push(CoordinateFrame::PolygonRings {
                            polygon,
                            remaining: 0..rings,
                        });
                    },
                    Shape::MultiPolygon(polygons) => {
                        let len = polygons.as_slice().len();
                        self.frames.push(CoordinateFrame::MultiPolygons {
                            shape: Shape::MultiPolygon(polygons),
                            remaining: 0..len,
                        });
                    },
                    Shape::GeometryCollection(collection) => {
                        let len = collection.len();
                        self.frames.push(CoordinateFrame::Collection {
                            shape: Shape::GeometryCollection(collection),
                            remaining: 0..len,
                        });
                    },
                    Shape::Empty(..) => {},
                }
                continue;
            }

            let step = self
                .frames
                .last_mut()
                .map_or(CoordinateStep::Pop, |frame| frame.advance(self.reverse));
            match step {
                CoordinateStep::Continue => {},
                CoordinateStep::Point(point) => return Some(point),
                CoordinateStep::Push(frame) => self.frames.push(frame),
                CoordinateStep::Pop => {
                    self.frames.pop()?;
                },
            }
        }
    }

    pub(crate) fn logical_heap_bytes(&self) -> usize {
        self.view.logical_heap_bytes().saturating_add(
            self.frames
                .capacity()
                .saturating_mul(std::mem::size_of::<CoordinateFrame>()),
        )
    }
}

impl CoordinateFrame {
    fn advance(&mut self, reverse: bool) -> CoordinateStep {
        match self {
            Self::Sequence {
                coordinates,
                remaining,
            } => {
                let index = next_index(remaining, reverse);
                index.map_or(CoordinateStep::Pop, |index| {
                    CoordinateStep::Point(coordinates.point_at(index))
                })
            },
            Self::Collection { shape, remaining } => {
                let index = next_index(remaining, reverse);
                index.map_or(CoordinateStep::Pop, |index| {
                    let Shape::GeometryCollection(collection) = shape else {
                        unreachable!("collection frame owns a geometry collection");
                    };
                    CoordinateStep::Push(Self::Shape(
                        collection
                            .as_slice()
                            .get(index)
                            .expect("collection frame index is within its admitted member range")
                            .clone(),
                    ))
                })
            },
            Self::MultiLines { shape, remaining } => {
                let index = next_index(remaining, reverse);
                index.map_or(CoordinateStep::Pop, |index| {
                    let Shape::MultiLineString(lines) = shape else {
                        unreachable!("line frame owns a multi-line string");
                    };
                    let coordinates = lines
                        .as_slice()
                        .get(index)
                        .expect("multi-line frame index is within its admitted member range")
                        .as_coords()
                        .clone();
                    CoordinateStep::Push(Self::Sequence {
                        remaining: 0..coordinates.len(),
                        coordinates,
                    })
                })
            },
            Self::MultiPolygons { shape, remaining } => {
                let index = next_index(remaining, reverse);
                index.map_or(CoordinateStep::Pop, |index| {
                    let Shape::MultiPolygon(polygons) = shape else {
                        unreachable!("polygon frame owns a multi-polygon");
                    };
                    CoordinateStep::Push(Self::Shape(Shape::Polygon(
                        polygons
                            .as_slice()
                            .get(index)
                            .expect("multi-polygon frame index is within its admitted member range")
                            .clone(),
                    )))
                })
            },
            Self::PolygonRings { polygon, remaining } => {
                let index = next_index(remaining, reverse);
                index.map_or(CoordinateStep::Pop, |index| {
                    let coordinates = if index == 0 {
                        polygon.shell.coords()
                    } else {
                        polygon
                            .holes
                            .get(index.wrapping_sub(1))
                            .expect("polygon ring index follows its shell")
                            .coords()
                    }
                    .clone();
                    CoordinateStep::Push(Self::Sequence {
                        remaining: 0..coordinates.len(),
                        coordinates,
                    })
                })
            },
            Self::ArrayRows {
                rows,
                remaining,
                compact_point_rank,
            } => {
                let Some(row) = next_index(remaining, reverse) else {
                    return CoordinateStep::Pop;
                };
                match rows.storage() {
                    GeometryArrayStorage::Mixed(shapes) => {
                        if rows.missing().is_some_and(|mask| mask.is_missing(row)) {
                            CoordinateStep::Continue
                        } else {
                            CoordinateStep::Push(Self::Shape(
                                shapes
                                    .get(row)
                                    .expect(
                                        "array row frame index is within its admitted row range",
                                    )
                                    .clone(),
                            ))
                        }
                    },
                    GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
                        let map = row_map.as_deref();
                        let physical_row = if let Some(mask) = rows.missing()
                            && compact_point_rank.is_some()
                        {
                            if mask.is_missing(row) {
                                return CoordinateStep::Continue;
                            }
                            let rank = compact_point_rank
                                .as_mut()
                                .expect("compact point rank accompanies a mismatched mask");
                            if reverse {
                                *rank = rank
                                    .checked_sub(1)
                                    .expect("a present compact row has a stored rank");
                                *rank
                            } else {
                                let present = *rank;
                                *rank = rank
                                    .checked_add(1)
                                    .expect("present point rank fits the stored point count");
                                present
                            }
                        } else {
                            if rows.missing().is_some_and(|mask| mask.is_missing(row)) {
                                return CoordinateStep::Continue;
                            }
                            row
                        };
                        CoordinateStep::Point(
                            coords.point_at(storage::physical_row(map, physical_row)),
                        )
                    },
                    GeometryArrayStorage::Packed(PackedStorage::Lines {
                        coords,
                        offsets,
                        row_map,
                    }) => {
                        if rows.missing().is_some_and(|mask| mask.is_missing(row)) {
                            return CoordinateStep::Continue;
                        }
                        let window = row_map.as_deref().csr_window(offsets.as_slice(), row);
                        let coordinates =
                            coords.view(CoordWindow::from_range(window, coords.len()));
                        CoordinateStep::Push(Self::Sequence {
                            remaining: 0..coordinates.len(),
                            coordinates,
                        })
                    },
                    GeometryArrayStorage::Packed(PackedStorage::Polygons {
                        coords,
                        ring_offsets,
                        polygon_offsets,
                        row_map,
                    }) => {
                        if rows.missing().is_some_and(|mask| mask.is_missing(row)) {
                            return CoordinateStep::Continue;
                        }
                        let rings = row_map
                            .as_deref()
                            .csr_window(polygon_offsets.as_slice(), row);
                        if rings.is_empty() {
                            return CoordinateStep::Continue;
                        }
                        CoordinateStep::Push(Self::PackedRings {
                            coordinates: Arc::clone(coords),
                            offsets: ring_offsets.clone(),
                            remaining: rings,
                        })
                    },
                }
            },
            Self::PackedRings {
                coordinates,
                offsets,
                remaining,
            } => {
                let Some(ring) = next_index(remaining, reverse) else {
                    return CoordinateStep::Pop;
                };
                let window = csr_value_window(offsets.as_slice(), ring..ring.wrapping_add(1));
                let coordinates =
                    coordinates.view(CoordWindow::from_range(window, coordinates.len()));
                CoordinateStep::Push(Self::Sequence {
                    remaining: 0..coordinates.len(),
                    coordinates,
                })
            },
            Self::Shape(_) => unreachable!("shape frames are expanded before advancing"),
        }
    }
}

fn next_index(range: &mut std::ops::Range<usize>, reverse: bool) -> Option<usize> {
    if reverse {
        range.next_back()
    } else {
        range.next()
    }
}
