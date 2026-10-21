use std::collections::hash_map::Entry;

use ahash::{HashMap, HashMapExt as _};

use crate::geographic::GeographicFrame;
use crate::geometry::{CoordSeq, LineSeq, Polygon, Ring, XY};
use crate::grid::cell_set;
use crate::py::cells::coverage_ops::{SortedCells, multipolygon_from_cell_shapes};
use crate::py::cells::grid_outline::{coordseq_crosses_lon_seam, polygonize_outline};
use crate::py::cells::h3::{CellIndex, DirectedEdgeIndex, LatLng, VertexIndex};
use crate::py::cells::{CellArrayError, Point, Shape, Typed, coverage_to_polygon};

pub(crate) fn h3_dissolve(cells: Vec<CellIndex>) -> Result<Typed, CellArrayError> {
    // A cell SET dissolves to the same outline regardless of order or repeats,
    // so canonicalize to a set — the user shouldn't have to pre-deduplicate
    // (matching the set semantics of the cell-algebra ops).
    let resolution = cells
        .iter()
        .map(|cell| cell.resolution())
        .max()
        .ok_or_else(|| {
            CellArrayError::Geometry("to_polygon requires at least one cell".to_owned())
        })?;
    let cells = SortedCells::new(cells);
    // A coarse boundary is not the union of its children's boundaries. Retain
    // the finest original resolution before hierarchical set admission.
    let cells = if cells
        .as_slice()
        .iter()
        .any(|cell| cell.resolution() != resolution)
    {
        let expanded = cell_set::uncompact(cells.as_slice(), resolution.into())?;
        SortedCells::new(expanded.into_public_vec())
    } else {
        cells
    };
    let contains_pole = [90.0, -90.0].into_iter().any(|latitude| {
        let pole = LatLng::new(latitude, 0.0)
            .expect("a geographic pole is valid")
            .to_cell(resolution);
        cells.as_slice().binary_search(&pole).is_ok()
    });
    if contains_pole {
        // A closed exterior can bound a hole rather than the occupied region.
        // The planar polygonizer cannot choose the occupied side of a pole.
        return coverage_to_polygon(&canonical_cell_shapes(cells.as_slice())?);
    }
    let any_adjacent = cells.as_slice().iter().any(|cell| {
        cell.edges()
            .any(|edge| cells.as_slice().binary_search(&edge.destination()).is_ok())
    });
    if !any_adjacent {
        return multipolygon_from_cell_shapes(canonical_cell_shapes(cells.as_slice())?);
    }
    if let Some(outline) = exterior_outline(&cells)?
        && let Some(output) = polygonize_outline(outline)?
    {
        return Ok(output);
    }
    coverage_to_polygon(&canonical_cell_shapes(cells.as_slice())?)
}

fn exterior_outline(
    cells: &SortedCells<CellIndex>,
) -> Result<Option<Vec<CoordSeq>>, CellArrayError> {
    let mut coordinates = CanonicalCoordinates::new(0);
    let mut outline = Vec::with_capacity(cells.as_slice().len().saturating_mul(2));
    for &cell in cells.as_slice() {
        let exterior: Vec<_> = cell
            .edges()
            .filter(|edge| cells.as_slice().binary_search(&edge.destination()).is_err())
            .collect();
        if exterior.is_empty() {
            continue;
        }
        for (index, vertices) in cell.boundary_edges(|index| exterior.contains(&index)) {
            let mut points = Vec::with_capacity(4);
            DissolveEdge { index, vertices }.append(&mut points, &mut coordinates)?;
            let segment = CoordSeq::try_from_xy(&points)?;
            if coordseq_crosses_lon_seam(&segment) {
                return Ok(None);
            }
            outline.push(segment);
        }
    }
    Ok(Some(outline))
}

fn canonical_cell_shapes(cells: &[CellIndex]) -> Result<Vec<Shape>, CellArrayError> {
    let mut coordinates = CanonicalCoordinates::new(cells.len().saturating_mul(2));
    let mut shapes = Vec::with_capacity(cells.len());
    for &cell in cells {
        shapes.push(canonical_cell_shape(cell, &mut coordinates)?);
    }
    Ok(shapes)
}

fn canonical_cell_shape(
    cell: CellIndex,
    coordinates: &mut CanonicalCoordinates,
) -> Result<Shape, CellArrayError> {
    let mut points = Vec::with_capacity(11);
    for (index, vertices) in cell.boundary_edges(|_| true) {
        DissolveEdge { index, vertices }.append(&mut points, coordinates)?;
        points
            .pop()
            .expect("the following edge owns the shared vertex");
    }
    points.push(*points.first().expect("a cell has boundary vertices"));
    let ordinary = points.iter().all(|point| point.y.abs() < 90.0)
        && points
            .array_windows::<2>()
            .all(|[a, b]| (a.x - b.x).abs() < 180.0);
    let boundary = Shape::Polygon(Polygon::new(
        Ring::try_from_closed(CoordSeq::try_from_xy(&points)?)?,
        Vec::new(),
    ));
    // Admitted canonical nodes and ordinary chords already read literally in
    // the longitude chart. Only seam and pole cells need geographic rewriting.
    if ordinary {
        return Ok(boundary);
    }
    let boundary = crate::geometry::publish_stored_computed_shape(
        &boundary,
        GeographicFrame::crs84(),
        "H3 dissolve boundary",
    )?;
    Ok(boundary.split_antimeridian()?)
}

struct CanonicalCoordinates(HashMap<VertexIndex, XY>);

impl CanonicalCoordinates {
    fn new(capacity: usize) -> Self {
        Self(HashMap::with_capacity(capacity))
    }

    fn vertex(&mut self, vertex: VertexIndex) -> crate::error::Result<XY> {
        match self.0.entry(vertex) {
            Entry::Occupied(entry) => Ok(*entry.get()),
            Entry::Vacant(entry) => Ok(*entry.insert(canonical_xy(LatLng::from(vertex))?)),
        }
    }
}

fn canonical_xy(coordinate: LatLng) -> crate::error::Result<XY> {
    GeographicFrame::crs84()
        .canonical_computed_point(Point::new_raw_xy(coordinate.lng(), coordinate.lat()))
        .map(XY::from)
        .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))
}

#[derive(Clone, Copy)]
struct DissolveEdge {
    index: DirectedEdgeIndex,
    vertices: [VertexIndex; 2],
}

impl DissolveEdge {
    fn append(
        self,
        output: &mut Vec<XY>,
        coordinates: &mut CanonicalCoordinates,
    ) -> crate::error::Result<()> {
        let [first, last] = self.vertices;
        let start = coordinates.vertex(first)?;
        let end = coordinates.vertex(last)?;
        // A shared corner uses its vertex owner; a face-crossing insertion uses
        // the lower cell's directed edge. Independent cell projections can
        // otherwise leave real gaps between neighboring polygons.
        let (middle, count) = if self.index.origin().resolution().is_class3() {
            let (origin, destination) = self.index.cells();
            let index = if origin > destination {
                destination.edge(origin).expect("an edge has a reverse")
            } else {
                self.index
            };
            let boundary = index.boundary();
            debug_assert!((2..=3).contains(&boundary.len()));
            let middle = if boundary.len() == 3 {
                let coordinate = boundary.get(1).expect("distortion vertex");
                canonical_xy(*coordinate)?
            } else {
                end
            };
            (middle, boundary.len())
        } else {
            (end, 2)
        };
        let points = [start, middle, end];
        let points = points
            .get(..count)
            .expect("a directed edge has at most one distortion vertex");
        if points
            .array_windows::<2>()
            .any(|[a, b]| (a.x - b.x).abs() > 180.0)
        {
            let (origin, destination) = self.index.cells();
            let reverse = origin > destination;
            let canonical = if reverse {
                [end, if count == 3 { middle } else { start }, start]
            } else {
                [start, middle, end]
            };
            let points = canonical.get(..count).expect("edge coordinate count");
            // Use the same cut direction for both cells. Splitting opposite
            // per-cell spellings can round the cut latitude differently.
            let edge = Shape::LineString(LineSeq::try_new(CoordSeq::try_from_xy(points)?)?);
            let edge = crate::geometry::publish_stored_computed_shape(
                &edge,
                GeographicFrame::crs84(),
                "H3 dissolve edge",
            )?
            .split_antimeridian()?;
            let mut cut = Vec::with_capacity(6);
            let mut append = |line: &LineSeq| {
                for point in line {
                    #[expect(
                        clippy::float_cmp,
                        reason = "the splitter emits exact opposite antimeridian aliases"
                    )]
                    let longitude = if point.x == 180.0 { -180.0 } else { point.x };
                    let point = XY::new(longitude, point.y);
                    if cut.last() != Some(&point) {
                        cut.push(point);
                    }
                }
            };
            if let Shape::LineString(line) = &edge {
                append(line);
            } else if let Shape::MultiLineString(lines) = &edge {
                for line in lines {
                    append(line);
                }
            } else {
                unreachable!("splitting an edge retains its line components")
            }
            if reverse {
                output.extend(cut.into_iter().rev());
            } else {
                output.extend(cut);
            }
        } else {
            output.extend_from_slice(points);
        }
        Ok(())
    }
}
