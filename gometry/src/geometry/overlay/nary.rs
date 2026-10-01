use std::borrow::Borrow;
use std::ops::ControlFlow;

use crate::boundary::geographic::invalid_lonlat_error;
use crate::error::Result;
use crate::geographic::{GeographicInputs, GeographicViewError};
use crate::geometry::overlay::{
    ExactSourceInventory, OverlayOp, Strictness, build_certified_overlay_shape,
    overlay_declared_axes,
};
use crate::geometry::topology::overlay::exact_geographic_overlay;
use crate::geometry::{
    Bounds, CoordSeq, CoordinateAxes, Dimension, GeographicTopologyRoute, GeometryErrorKind,
    GeometryKind, Shape, TopologyDomain,
};

/// Evaluate one or more operands in one canonical overlay arrangement.
///
/// The source order is normalized before noding. Unless disjoint bounds settle
/// the operation, all operands participate in the same exact partition or
/// geographic quotient, and every output bucket is assembled once.
pub(crate) fn overlay_all<S: Borrow<Shape>>(
    geometries: &[S],
    domain: TopologyDomain,
    op: OverlayOp,
    strictness: Strictness,
) -> Result<Shape> {
    overlay_all_with_bounds_certificates(geometries, domain, op, strictness, true)
}

/// Overlay already-charted geographic split parts with exact planar topology
/// while retaining vertices that still define geographic metric edges.
pub(crate) fn overlay_all_geographic_chart<S: Borrow<Shape>>(
    geometries: &[S],
    op: OverlayOp,
    strictness: Strictness,
) -> Result<Shape> {
    if geometries.is_empty() {
        return Err(GeometryErrorKind::EmptyGeometrySequence {
            operation: op.name(),
        }
        .into());
    }
    let mut inputs = Vec::with_capacity(geometries.len());
    inputs.extend(geometries.iter().map(std::borrow::Borrow::borrow));
    exact_partition_overlay_with_bounds_certificates(
        inputs,
        op,
        strictness,
        true,
        true,
        ExactPartitionBudget::Charge,
    )
}

#[cfg(test)]
pub(super) fn overlay_all_without_bounds_certificates<S: Borrow<Shape>>(
    geometries: &[S],
    domain: TopologyDomain,
    op: OverlayOp,
    strictness: Strictness,
) -> Result<Shape> {
    overlay_all_with_bounds_certificates(geometries, domain, op, strictness, false)
}

pub(super) fn overlay_all_with_bounds_certificates<S: Borrow<Shape>>(
    geometries: &[S],
    domain: TopologyDomain,
    op: OverlayOp,
    strictness: Strictness,
    bounds_certificates: bool,
) -> Result<Shape> {
    if geometries.is_empty() {
        return Err(GeometryErrorKind::EmptyGeometrySequence {
            operation: op.name(),
        }
        .into());
    }

    match domain {
        TopologyDomain::Cartesian => {
            let mut refs = Vec::with_capacity(geometries.len());
            refs.extend(geometries.iter().map(std::borrow::Borrow::borrow));
            exact_cartesian_overlay(
                refs,
                op,
                strictness,
                bounds_certificates,
                ExactPartitionBudget::Charge,
            )
        },
        TopologyDomain::Geographic(frame) => {
            let originals = GeographicInputs::try_from_shapes(
                frame,
                geometries.iter().map(std::borrow::Borrow::borrow),
            )
            .map_err(|error| overlay_view_error(&error))?;
            overlay_all_geographic_from(&originals, op, strictness, bounds_certificates)
        },
    }
}

#[derive(Clone, Copy)]
enum ExactPartitionBudget {
    Charge,
    Ignore,
}

#[derive(Clone, Copy)]
pub(in crate::geometry) enum VoronoiIntermediateOverlay {
    Intersection,
    Difference,
}

impl VoronoiIntermediateOverlay {
    const fn operation(self) -> OverlayOp {
        match self {
            Self::Intersection => OverlayOp::Intersection,
            Self::Difference => OverlayOp::Difference,
        }
    }
}

/// The Cartesian overlay route. Source inventory, exact incidence, cell
/// selection, dimensional absorption, and checked emission belong to
/// `ExactPartition`; disjoint bounds only partition independent inputs.
fn exact_cartesian_overlay(
    inputs: Vec<&Shape>,
    operation: OverlayOp,
    strictness: Strictness,
    bounds_certificates: bool,
    partition_budget: ExactPartitionBudget,
) -> Result<Shape> {
    if inputs.len() == 1 && operation == OverlayOp::Intersection {
        return Ok(inputs[0].clone());
    }
    exact_partition_overlay_with_bounds_certificates(
        inputs,
        operation,
        strictness,
        bounds_certificates,
        false,
        partition_budget,
    )
}

fn exact_partition_overlay_with_bounds_certificates(
    mut inputs: Vec<&Shape>,
    operation: OverlayOp,
    strictness: Strictness,
    bounds_certificates: bool,
    preserve_geographic_vertices: bool,
    partition_budget: ExactPartitionBudget,
) -> Result<Shape> {
    if operation == OverlayOp::Difference {
        inputs[1..].sort_unstable_by(|left, right| crate::geometry::compare_shapes(left, right));
    } else {
        inputs.sort_unstable_by(|left, right| crate::geometry::compare_shapes(left, right));
    }
    let inputs = inputs.as_slice();
    let declared_axes = overlay_declared_axes(inputs, operation);
    let strict = strictness.is_strict();
    let components = if bounds_certificates && inputs.len() > 1 {
        let mut bounds = Vec::with_capacity(inputs.len());
        for shape in inputs {
            bounds.push(shape.bounds());
        }
        if operation == OverlayOp::Intersection && intersection_bounds_empty(&bounds) {
            return Ok(empty_nary_overlay_shape(inputs, operation));
        }
        interacting_components(&bounds, operation)?
    } else {
        None
    };
    let shape = match components {
        None => exact_partition_overlay(
            inputs,
            operation,
            declared_axes,
            strict,
            preserve_geographic_vertices,
            partition_budget,
        )?,
        Some(components) => {
            let mut shapes = Vec::with_capacity(components.len());
            let mut subset = Vec::new();
            for component in components {
                subset.clear();
                subset.reserve(component.len());
                subset.extend(component.iter().map(|&index| inputs[index]));
                shapes.push(exact_partition_overlay(
                    &subset,
                    operation,
                    declared_axes,
                    strict,
                    preserve_geographic_vertices,
                    partition_budget,
                )?);
            }
            join_components(&shapes)?
        },
    };
    Ok(if shape.is_empty() {
        empty_nary_overlay_shape(inputs, operation)
    } else {
        shape.normalize()?
    })
}

/// Independently partitioned results retain one common ordinate axis per
/// dimension, just as a single exact partition does.
fn join_components(shapes: &[Shape]) -> Result<Shape> {
    let mut points = Vec::new();
    let mut lines = Vec::new();
    let mut polygons = Vec::new();
    for shape in shapes {
        append_overlay_parts(shape, &mut points, &mut lines, &mut polygons)?;
    }
    let common = |axes: &mut dyn Iterator<Item = CoordinateAxes>| {
        axes.reduce(CoordinateAxes::intersection)
            .unwrap_or(CoordinateAxes::XY)
    };
    let restricted = |coords: &CoordSeq, axes: CoordinateAxes| -> Result<CoordSeq> {
        let coords = if axes.has_z() {
            coords.clone()
        } else {
            coords.set_z(None, false)?
        };
        if axes.has_m() {
            Ok(coords)
        } else {
            coords.set_m(None, false)
        }
    };
    let point_axes = common(&mut points.iter().map(|point| point.axes()));
    for point in &mut points {
        *point = crate::geometry::Point::new_raw_axes(
            point.x,
            point.y,
            crate::geometry::ZOrdinate(point.z().filter(|_| point_axes.has_z())),
            crate::geometry::MOrdinate(point.m().filter(|_| point_axes.has_m())),
        );
    }
    let line_axes = common(&mut lines.iter().map(CoordSeq::axes));
    for line in &mut lines {
        if line.axes() != line_axes {
            *line = restricted(line, line_axes)?;
        }
    }
    let polygon_axes = common(&mut polygons.iter().map(crate::geometry::Polygon::axes));
    for polygon in &mut polygons {
        if polygon.axes() != polygon_axes {
            let mut holes = Vec::with_capacity(polygon.holes.len());
            for hole in polygon.holes.iter() {
                holes.push(crate::geometry::Ring::try_from_closed(restricted(
                    hole.coords(),
                    polygon_axes,
                )?)?);
            }
            let shell = crate::geometry::Ring::try_from_closed(restricted(
                polygon.shell.coords(),
                polygon_axes,
            )?)?;
            *polygon = crate::geometry::Polygon::new(shell, holes);
        }
    }
    build_certified_overlay_shape(points, lines, polygons)
}

fn append_overlay_parts(
    shape: &Shape,
    points: &mut Vec<crate::geometry::Point>,
    lines: &mut Vec<CoordSeq>,
    polygons: &mut Vec<crate::geometry::Polygon>,
) -> Result<()> {
    shape.try_for_each_component(&mut |component| {
        match component {
            crate::geometry::Component::Point(point) => points.push(point),
            crate::geometry::Component::LineString(line) => {
                lines.push(line.as_coords().clone());
            },
            crate::geometry::Component::Polygon(polygon) => polygons.push(polygon.clone()),
        }
        Ok(())
    })
}

/// One exact Cartesian partition over the already canonical-ordered inputs.
fn exact_partition_overlay(
    inputs: &[&Shape],
    operation: OverlayOp,
    declared_axes: CoordinateAxes,
    strict: bool,
    preserve_geographic_vertices: bool,
    partition_budget: ExactPartitionBudget,
) -> Result<Shape> {
    let union = if operation == OverlayOp::Union {
        ExactSourceInventory::from_polygonal_union(inputs)?
    } else {
        None
    };
    let inventory = match union {
        Some(inventory) => inventory,
        None => ExactSourceInventory::from_shapes(inputs)?,
    };
    let mut partition = match partition_budget {
        ExactPartitionBudget::Charge => {
            let mut budget =
                crate::geometry::ExpansionBudget::new(operation.name(), "exact partition");
            inventory.partition(&mut budget)?
        },
        ExactPartitionBudget::Ignore => inventory.partition_without_budget()?,
    };
    if preserve_geographic_vertices {
        // Chart-collinear vertices still define separate geographic metric
        // edges, so exact planar simplification must not merge them.
        partition.keep_all_nodes();
    }
    inventory.overlay_shape(&partition, operation, declared_axes, strict)
}

/// Voronoi owns the published-output budget for these intermediate clips.
pub(in crate::geometry) fn binary_cartesian_overlay_without_partition_budget(
    left: &Shape,
    right: &Shape,
    operation: VoronoiIntermediateOverlay,
    strictness: Strictness,
) -> Result<Shape> {
    exact_cartesian_overlay(
        vec![left, right],
        operation.operation(),
        strictness,
        true,
        ExactPartitionBudget::Ignore,
    )
}

/// Geographic overlays share one chart classifier. Ordinary charts use the
/// exact planar partition; seam and pole cases use one global Q over the
/// original rows so source intervals still own checked Z/M emission.
pub(in crate::geometry) fn overlay_all_geographic_from(
    originals: &GeographicInputs<'_>,
    op: OverlayOp,
    strictness: Strictness,
    bounds_certificates: bool,
) -> Result<Shape> {
    let mut original_refs = Vec::with_capacity(originals.views().len());
    original_refs.extend(originals.views().iter().map(|view| view.shape()));
    if crate::geometry::geographic_topology_route(originals.views().iter().copied())
        == GeographicTopologyRoute::ExactPartition
    {
        return exact_partition_overlay_with_bounds_certificates(
            original_refs,
            op,
            strictness,
            bounds_certificates,
            true,
            ExactPartitionBudget::Charge,
        );
    }
    exact_geographic_q_overlay(
        originals,
        &original_refs,
        op,
        strictness,
        bounds_certificates,
    )
}

#[cfg(test)]
pub(crate) fn overlay_all_geographic_q_from(
    originals: &GeographicInputs<'_>,
    op: OverlayOp,
    strictness: Strictness,
    bounds_certificates: bool,
) -> Result<Shape> {
    let mut original_refs = Vec::with_capacity(originals.views().len());
    original_refs.extend(originals.views().iter().map(|view| view.shape()));
    exact_geographic_q_overlay(
        originals,
        &original_refs,
        op,
        strictness,
        bounds_certificates,
    )
}

fn exact_geographic_q_overlay(
    originals: &GeographicInputs<'_>,
    original_refs: &[&Shape],
    op: OverlayOp,
    strictness: Strictness,
    bounds_certificates: bool,
) -> Result<Shape> {
    let frame = originals.frame();
    let declared_axes = overlay_declared_axes(original_refs, op);

    if bounds_certificates && op == OverlayOp::Intersection && originals.views().len() > 1 {
        let facts = overlay_bounds_facts_geographic(originals);
        if facts.raw_coordinate_bounds_sound && intersection_bounds_empty(&facts.bounds) {
            return Ok(empty_nary_overlay_shape(original_refs, op));
        }
    }

    // Preserve the left row for difference; commutative operations use a
    // canonical row order so Q ownership is independent of input permutation.
    let mut views = originals.views().to_vec();
    if op == OverlayOp::Difference {
        let (_, rest) = views
            .split_first_mut()
            .expect("geographic overlays have at least one operand");
        rest.sort_unstable_by(|left, right| {
            crate::geometry::compare_shapes(left.shape(), right.shape())
        });
    } else {
        views.sort_unstable_by(|left, right| {
            crate::geometry::compare_shapes(left.shape(), right.shape())
        });
    }
    let topology = crate::geometry::ExactGeographicTopology::from_views(frame, views.into_iter())?;
    let output = exact_geographic_overlay(&topology, op, declared_axes, strictness.is_strict())?;
    Ok(if output.is_empty() {
        empty_nary_overlay_shape(original_refs, op)
    } else {
        output
    })
}

pub(crate) fn occupied_geographic_support(
    shape: &Shape,
    frame: crate::geographic::GeographicFrame,
) -> Result<Shape> {
    let originals = GeographicInputs::try_from_shapes(frame, std::iter::once(shape))
        .map_err(|error| overlay_view_error(&error))?;
    overlay_all_geographic_from(&originals, OverlayOp::Union, Strictness::Lenient, false)
}

struct OverlayBoundsFacts {
    bounds: Vec<Option<Bounds>>,
    raw_coordinate_bounds_sound: bool,
}

pub(in crate::geometry) fn overlay_view_error(error: &GeographicViewError) -> crate::error::Error {
    invalid_lonlat_error(error.native.x, error.native.y)
}

fn overlay_bounds_facts_geographic(originals: &GeographicInputs<'_>) -> OverlayBoundsFacts {
    let mut bounds = Vec::with_capacity(originals.views().len());
    for view in originals.views() {
        bounds.push(view.shape().bounds());
    }
    let raw_coordinate_bounds_sound =
        originals.views().iter().zip(&bounds).all(|(view, bounds)| {
            bounds.is_some_and(|bounds| {
                crate::geometry::raw_coordinate_bounds_are_sound_in_frame(
                    Some(view.shape()),
                    bounds,
                    view.frame(),
                )
            })
        });
    OverlayBoundsFacts {
        bounds,
        raw_coordinate_bounds_sound,
    }
}

/// Inputs whose certified bounds never meet cannot interact in the Cartesian
/// lane. Geographic Q instead owns every admitted row in one global face table.
fn interacting_components(
    bounds: &[Option<Bounds>],
    op: OverlayOp,
) -> Result<Option<Vec<Vec<usize>>>> {
    const fn root(parent: &mut [usize], mut node: usize) -> usize {
        while parent[node] != node {
            parent[node] = parent[parent[node]];
            node = parent[node];
        }
        node
    }
    if op == OverlayOp::Intersection {
        return Ok(None);
    }
    let count = bounds.len();
    let mut parent = Vec::with_capacity(count);
    parent.extend(0..count);
    let ControlFlow::Continue(()): ControlFlow<std::convert::Infallible> =
        crate::geometry::for_each_indexed_overlapping_bounds_pair(bounds, |left, right| {
            let (a, b) = (root(&mut parent, left), root(&mut parent, right));
            parent[a.max(b)] = a.min(b);
            ControlFlow::Continue(())
        })?;
    for node in 0..count {
        parent[node] = root(&mut parent, node);
    }
    let mut components: Vec<Vec<usize>> = Vec::new();
    if op == OverlayOp::Difference {
        let left = parent.iter().filter(|&&root| root == 0).count();
        if left == count {
            return Ok(None);
        }
        let mut component = Vec::with_capacity(left);
        component.extend((0..count).filter(|&node| parent[node] == 0));
        components.push(component);
        return Ok(Some(components));
    }
    let roots = (0..count).filter(|&node| parent[node] == node).count();
    if roots == 1 {
        return Ok(None);
    }
    let mut slot_of_root = Vec::with_capacity(count);
    slot_of_root.resize(count, usize::MAX);
    components.reserve(roots);
    for (node, &root) in parent.iter().enumerate() {
        if slot_of_root[root] == usize::MAX {
            slot_of_root[root] = components.len();
            components.push(Vec::new());
        }
        components[slot_of_root[root]].push(node);
    }
    Ok(Some(components))
}

fn intersection_bounds_empty(bounds: &[Option<Bounds>]) -> bool {
    let Some((first, rest)) = bounds.split_first() else {
        return false;
    };
    let Some(mut bounds) = *first else {
        return true;
    };
    for next in rest {
        let Some(next) = *next else {
            return true;
        };
        if !bounds.intersects(next) {
            return true;
        }
        bounds = Bounds::new_raw(
            bounds.minx().max(next.minx()),
            bounds.miny().max(next.miny()),
            bounds.maxx().min(next.maxx()),
            bounds.maxy().min(next.maxy()),
        );
    }
    false
}

pub(crate) fn empty_nary_overlay_shape(inputs: &[&Shape], op: OverlayOp) -> Shape {
    debug_assert!(!inputs.is_empty());
    let dimension = match op {
        OverlayOp::Intersection => inputs
            .iter()
            .map(|shape| shape.topological_dimension())
            .min()
            .expect("non-empty inputs"),
        OverlayOp::Difference => inputs[0].topological_dimension(),
        OverlayOp::Union | OverlayOp::SymmetricDifference => inputs
            .iter()
            .map(|shape| shape.topological_dimension())
            .max()
            .expect("non-empty inputs"),
    };
    empty_shape_for_dimension_with_axes(dimension, overlay_declared_axes(inputs, op))
}

pub(crate) const fn empty_shape_for_dimension_with_axes(
    dimension: Dimension,
    axes: CoordinateAxes,
) -> Shape {
    match dimension {
        Dimension::Point => Shape::typed_empty(GeometryKind::Point, axes),
        Dimension::Curve => Shape::typed_empty(GeometryKind::LineString, axes),
        Dimension::Surface => Shape::typed_empty(GeometryKind::Polygon, axes),
    }
}

#[cfg(test)]
mod geographic_chart_route_tests {
    use super::*;
    use crate::geographic::GeographicFrame;
    use crate::geometry::{Point, Polygon, Ring, graph_relate_shapes};

    fn rectangle(west: f64, south: f64, east: f64, north: f64) -> Shape {
        let points = [
            Point::new_raw_xy(west, south),
            Point::new_raw_xy(east, south),
            Point::new_raw_xy(east, north),
            Point::new_raw_xy(west, north),
            Point::new_raw_xy(west, south),
        ];
        Shape::Polygon(Polygon::new(
            Ring::try_from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            Vec::new(),
        ))
    }

    #[test]
    fn ordinary_geographic_overlays_match_the_q_reference() {
        let shell = rectangle(-10.0, -10.0, 10.0, 10.0);
        let overlap = rectangle(0.0, -5.0, 8.0, 5.0);
        let inner = rectangle(-2.0, -2.0, 2.0, 2.0);
        let disconnected = rectangle(20.0, 0.0, 24.0, 4.0);
        let cases = [
            (OverlayOp::Union, vec![&shell, &overlap, &disconnected]),
            (OverlayOp::Intersection, vec![&shell, &overlap]),
            (OverlayOp::Difference, vec![&shell, &inner]),
            (OverlayOp::SymmetricDifference, vec![&shell, &overlap]),
        ];

        for (operation, inputs) in cases {
            let originals =
                GeographicInputs::try_from_shapes(GeographicFrame::crs84(), inputs.iter().copied())
                    .unwrap();
            assert_eq!(
                crate::geometry::geographic_topology_route(originals.views().iter().copied()),
                GeographicTopologyRoute::ExactPartition
            );

            let routed =
                overlay_all_geographic_from(&originals, operation, Strictness::Lenient, true)
                    .unwrap();
            let q = overlay_all_geographic_q_from(&originals, operation, Strictness::Lenient, true)
                .unwrap();
            assert!(
                graph_relate_shapes(&routed, &q).unwrap().is_equal_topo(),
                "{operation:?}: ExactPartition and Q outputs differ"
            );
        }
    }

    #[test]
    fn ordinary_near_ulp_country_overlay_matches_the_q_reference() {
        let argentina = crate::io::parse_wkt(
            "MULTIPOLYGON (((-58.10222180941994 -33.010937543229446, \
             -58.084289 -33.0009615, -58.085477253434156 -32.99, \
             -58.089648910878246 -32.99, -58.0878711 -32.9965279, \
             -58.10222180941994 -33.010937543229446)), \
             ((-58.11 -33.00166081596568, -58.10769305699579 -32.99, \
             -58.11 -32.99, -58.11 -33.00166081596568)))",
        )
        .unwrap();
        let uruguay = crate::io::parse_wkt(
            "POLYGON ((-58.11 -33.02, -58.08 -33.02, -58.08 -32.99, \
             -58.085477253434156 -32.99, -58.084289 -33.0009615, \
             -58.10256757394891 -33.011129892448544, \
             -58.0878711 -32.9965279, -58.089648910878246 -32.99, \
             -58.10765630407339 -32.99, -58.11 -33.001904214459806, \
             -58.11 -33.02))",
        )
        .unwrap();
        let inputs = [&argentina, &uruguay];
        let originals =
            GeographicInputs::try_from_shapes(GeographicFrame::crs84(), inputs.into_iter())
                .unwrap();
        assert_eq!(
            crate::geometry::geographic_topology_route(originals.views().iter().copied()),
            GeographicTopologyRoute::ExactPartition
        );

        let routed =
            overlay_all_geographic_from(&originals, OverlayOp::Union, Strictness::Lenient, true)
                .unwrap();
        let q =
            overlay_all_geographic_q_from(&originals, OverlayOp::Union, Strictness::Lenient, true)
                .unwrap();
        assert!(
            graph_relate_shapes(&routed, &q).unwrap().is_equal_topo(),
            "ExactPartition and Q differ for the near-ULP border union"
        );
    }
}
