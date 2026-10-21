use crate::geographic::GeographicFrame;
use crate::geometry::{CoordSeq, LineSeq, Polygon, Ring};
use crate::grid::cell_set;
use crate::py::cells::coverage_ops::{GridDissolver, SortedCells, dissolve_grid_cells};
use crate::py::cells::grid_outline::coordseq_crosses_lon_seam;
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
        let shapes = cells
            .as_slice()
            .iter()
            .copied()
            .map(H3Dissolver::fallback_shape)
            .collect::<Result<Vec<_>, _>>()?;
        return coverage_to_polygon(&shapes);
    }
    dissolve_grid_cells::<H3Dissolver>(cells)
}

struct H3Dissolver;

impl GridDissolver for H3Dissolver {
    type Cell = CellIndex;

    fn fast_path_cells(
        cells: &SortedCells<Self::Cell>,
    ) -> Result<Option<SortedCells<Self::Cell>>, CellArrayError> {
        Ok(Some(cells.clone()))
    }

    fn adjacency_neighbors(cell: Self::Cell) -> impl Iterator<Item = Self::Cell> {
        cell.edges().map(h3o::DirectedEdgeIndex::destination)
    }

    /// Exterior directed-edge segments as lon/lat polylines.
    ///
    /// These are **planar chord proxies** of H3's spherical edge geometry —
    /// exact region algebra should use cell set ops, not dissolved polygons.
    fn exterior_edge_segments(
        cell: Self::Cell,
        is_member: &dyn Fn(Self::Cell) -> bool,
    ) -> Result<Vec<CoordSeq>, CellArrayError> {
        let mut segments = Vec::with_capacity(6);
        let mut vertices = None;
        for edge in cell.edges() {
            if is_member(edge.destination()) {
                continue;
            }
            let vertices = vertices.get_or_insert_with(|| cell.vertexes().collect::<Vec<_>>());
            let (_, edge) = DissolveEdge::new(edge, vertices);
            let mut points = Vec::with_capacity(4);
            edge.append(&mut points)?;
            segments.push(CoordSeq::try_from_points(&points)?);
        }
        Ok(segments)
    }

    fn crosses_seam(segment: &CoordSeq) -> bool {
        coordseq_crosses_lon_seam(segment)
    }

    fn fallback_shape(cell: Self::Cell) -> Result<Shape, CellArrayError> {
        let vertices = cell.vertexes().collect::<Vec<_>>();
        let mut edges = [None; 6];
        for edge in cell.edges() {
            let (start, edge) = DissolveEdge::new(edge, &vertices);
            *edges.get_mut(start).expect("cell vertex indexes its edge") = Some(edge);
        }
        let mut points = Vec::with_capacity(11);
        for edge in edges.into_iter().flatten() {
            edge.append(&mut points)?;
            points
                .pop()
                .expect("the following edge owns the shared vertex");
        }
        points.push(*points.first().expect("a cell has boundary vertices"));
        let boundary = Shape::Polygon(Polygon::new(Ring::closed(points)?, Vec::new()));
        let boundary = crate::geometry::publish_stored_computed_shape(
            &boundary,
            GeographicFrame::crs84(),
            "H3 dissolve boundary",
        )?;
        Ok(boundary.split_antimeridian()?)
    }
}

#[derive(Clone, Copy)]
struct DissolveEdge {
    index: DirectedEdgeIndex,
    vertices: [VertexIndex; 2],
}

impl DissolveEdge {
    fn new(index: DirectedEdgeIndex, vertices: &[VertexIndex]) -> (usize, Self) {
        let mut shared = index
            .destination()
            .vertexes()
            .filter(|vertex| vertices.contains(vertex));
        let first = shared.next().expect("neighboring cells share two vertices");
        let second = shared.next().expect("neighboring cells share two vertices");
        let first_index = vertices
            .iter()
            .position(|&vertex| vertex == first)
            .expect("shared vertex belongs to the origin");
        let second_index = vertices
            .iter()
            .position(|&vertex| vertex == second)
            .expect("shared vertex belongs to the origin");
        let (start, vertices) = if first_index.checked_add(1) == Some(second_index)
            || (second_index == 0 && first_index.checked_add(1) == Some(vertices.len()))
        {
            (first_index, [first, second])
        } else {
            (second_index, [second, first])
        };
        (start, Self { index, vertices })
    }

    fn append(self, output: &mut Vec<Point>) -> crate::error::Result<()> {
        let (origin, destination) = self.index.cells();
        let reverse = origin > destination;
        let (index, vertices) = if reverse {
            (destination.edge(origin).expect("an edge has a reverse"), [
                self.vertices[1],
                self.vertices[0],
            ])
        } else {
            (self.index, self.vertices)
        };
        let [start, end] = vertices.map(|vertex| {
            let coordinate = LatLng::from(vertex);
            Point::new_raw_xy(coordinate.lng(), coordinate.lat())
        });
        // A shared corner uses its vertex owner; a face-crossing insertion uses
        // the lower cell's directed edge. Independent cell projections can
        // otherwise leave real gaps between neighboring polygons.
        let boundary = index.boundary();
        debug_assert!((2..=3).contains(&boundary.len()));
        let middle = if boundary.len() == 3 {
            let coordinate = boundary.get(1).expect("distortion vertex");
            Point::new_raw_xy(coordinate.lng(), coordinate.lat())
        } else {
            end
        };
        let points = [start, middle, end];
        let points = points
            .get(..boundary.len())
            .expect("a directed edge has at most one distortion vertex");
        if points
            .array_windows::<2>()
            .any(|[a, b]| (a.x - b.x).abs() > 180.0)
        {
            // Use the same cut direction for both cells. Splitting opposite
            // per-cell spellings can round the cut latitude differently.
            let edge = Shape::LineString(LineSeq::try_new(CoordSeq::try_from_points(points)?)?);
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
                    let point = Point::new_raw_xy(longitude, point.y);
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
        } else if reverse {
            output.extend(points.iter().rev().copied());
        } else {
            output.extend_from_slice(points);
        }
        Ok(())
    }
}
