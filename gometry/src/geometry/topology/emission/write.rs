use super::super::exact::ExactChartPoint;
use super::super::q::QPortId;
use super::ordinates::{
    source_literal_image, with_assigned_source_vertex_axes, with_publication_axes,
};
use super::plan::{DrawnPolygons, QDiagnosticLine, QEmissionDrawing, QLineOrigin};
use super::sites::EmittedVertex;
use super::source_publication::SourcePublication;
use super::{
    BTreeMap, CoordSeq, CoordinateAxes, GeometryErrorKind, LiteralPoint, LiteralPolygon,
    LiteralSequence, NodeOrdinates, Point, Polygon, QEmissionPlan, QSourceImage, RecipeWalkIndex,
    RecipeWalkOwner, ResourceError, Result, Ring, Shape, ShapeLiteralLayout, next_recipe_walk,
};
use crate::geometry::output::{
    CertifiedShapeOrder, build_certified_shape_with_axes, build_certified_shape_with_axes_and_order,
};
use crate::geometry::{CoordSeqBuilder, GeometryKind, LineSeq};
use crate::work::Work;

pub(in crate::geometry::topology) struct WrittenDrawingParts {
    pub(in crate::geometry::topology) points: Vec<Point>,
    pub(in crate::geometry::topology) lines: Vec<CoordSeq>,
    pub(in crate::geometry::topology) polygons: Vec<Polygon>,
    pub(in crate::geometry::topology) layout: ShapeLiteralLayout<QSourceImage>,
    pub(in crate::geometry::topology) diagnostic_lines: Vec<QDiagnosticLine>,
}

pub(in crate::geometry::topology) struct SourceWrittenPath {
    pub(in crate::geometry::topology) points: Vec<Point>,
    pub(in crate::geometry::topology) locations: LiteralSequence<QSourceImage>,
    pub(in crate::geometry::topology) diagnostic_points: Option<Vec<Point>>,
}
#[derive(Clone, Copy)]
pub(in crate::geometry::topology) struct SourcePathContext<'a, 'walk> {
    pub(in crate::geometry::topology) ordinates: &'a [NodeOrdinates],
    pub(in crate::geometry::topology) source_ports: Option<&'a [Option<ExactChartPoint>]>,
    pub(in crate::geometry::topology) publication: Option<SourcePublication<'a>>,
    pub(in crate::geometry::topology) walks: Option<&'a RecipeWalkIndex<'walk>>,
    pub(in crate::geometry::topology) axes: CoordinateAxes,
    pub(in crate::geometry::topology) diagnostic_axes: Option<CoordinateAxes>,
}
pub(in crate::geometry::topology) fn write_source_path(
    vertices: &[EmittedVertex<'_>],
    output: u32,
    owner: RecipeWalkOwner,
    recipe_rotation: usize,
    context: &SourcePathContext<'_, '_>,
    cursors: &mut BTreeMap<RecipeWalkOwner, usize>,
    work: &mut impl Work,
) -> Result<SourceWrittenPath> {
    let SourcePathContext {
        ordinates,
        source_ports,
        publication,
        walks,
        axes,
        diagnostic_axes,
    } = *context;
    let walk = match (publication, walks) {
        (Some(_), Some(walks)) => Some(next_recipe_walk(walks, cursors, owner, work)?),
        (None, None) => None,
        _ => {
            return Err(GeometryErrorKind::Invalid(
                "source publication inventory and walk map have different lifetimes".into(),
            )
            .into());
        },
    };
    let rotated_fragments = if let Some(walk) = walk
        && recipe_rotation != 0
    {
        if recipe_rotation >= walk.fragments.len() {
            return Err(GeometryErrorKind::Invalid(
                "source ring rotation exceeds its producer walk".into(),
            )
            .into());
        }
        // Keep each frozen publication sample on the producer edge that owns it.
        work.charge_into_error(
            u64::try_from(walk.fragments.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut fragments = walk.fragments.clone();
        fragments.rotate_left(recipe_rotation);
        Some(fragments)
    } else {
        None
    };
    let walk_fragments = match walk {
        Some(walk) => rotated_fragments.as_deref().unwrap_or(&walk.fragments),
        None => &[],
    };
    let edge_count = vertices.len().saturating_sub(1);
    if walk.is_some() && walk_fragments.len() != edge_count {
        return Err(GeometryErrorKind::Invalid(
            "source drawing path and published producer walk have different edge counts".into(),
        )
        .into());
    }
    let mut capacity = vertices.len();
    if let (Some(publication), Some(_walk)) = (publication, walk) {
        for &recipe in walk_fragments {
            let fragment = publication
                .inventory
                .fragments
                .get(recipe.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("source path references an unknown fragment".into())
                })?;
            capacity = capacity
                .checked_add(fragment.sample_range.len())
                .ok_or(ResourceError::SizeOverflow)?;
        }
    }
    work.charge_into_error(u64::try_from(capacity).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut points = Vec::with_capacity(capacity);
    if diagnostic_axes.is_some() {
        work.charge_into_error(u64::try_from(capacity).map_err(|_| ResourceError::SizeOverflow)?)?;
    }
    let mut diagnostic_points = diagnostic_axes.map(|_| Vec::with_capacity(capacity));
    let mut images = Vec::with_capacity(capacity);
    if let Some(&first) = vertices.first() {
        points.push(with_assigned_source_vertex_axes(
            first,
            ordinates,
            source_ports,
            axes,
            work,
        )?);
        if let Some(diagnostic_axes) = diagnostic_axes {
            diagnostic_points
                .as_mut()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "diagnostic source path lost its ordinate buffer".into(),
                    )
                })?
                .push(with_assigned_source_vertex_axes(
                    first,
                    ordinates,
                    source_ports,
                    diagnostic_axes,
                    work,
                )?);
        }
        images.push(source_literal_image(first, source_ports));
    }
    for (edge_index, pair) in vertices.array_windows::<2>().enumerate() {
        let [_, end] = pair;
        work.charge_into_error(1)?;
        if let (Some(publication), Some(_walk)) = (publication, walk) {
            let recipe = *walk_fragments.get(edge_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("source path has no recipe for an output edge".into())
            })?;
            let fragment = publication
                .inventory
                .fragments
                .get(recipe.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "source path recipe has no published fragment".into(),
                    )
                })?;
            let mut previous_vertex = 0_usize;
            let last_vertex = fragment.points.len().checked_sub(1).ok_or_else(|| {
                GeometryErrorKind::Invalid("source published fragment has no endpoint".into())
            })?;
            for sample_index in fragment.sample_range.clone() {
                work.charge_into_error(1)?;
                let sample = publication
                    .inventory
                    .samples
                    .get(sample_index)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("source path sample row is unknown".into())
                    })?;
                if sample.family.fragment_recipe() != Some(recipe)
                    || sample.request_row.index() != sample_index
                    || sample.written_vertex <= previous_vertex
                    || sample.written_vertex >= last_vertex
                    || fragment.points.get(sample.written_vertex) != Some(&sample.point)
                {
                    return Err(GeometryErrorKind::Invalid(
                        "source publication sample is not an ordered interior fragment vertex"
                            .into(),
                    )
                    .into());
                }
                previous_vertex = sample.written_vertex;
                let point = *publication.points.get(sample_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "source publication sample has no assigned coordinate".into(),
                    )
                })?;
                let sample_ordinates =
                    *publication.ordinates.get(sample_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "source publication sample has no resolved ordinate row".into(),
                        )
                    })?;
                points.push(with_publication_axes(point, sample_ordinates, axes)?);
                if let Some(diagnostic_axes) = diagnostic_axes {
                    diagnostic_points
                        .as_mut()
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "diagnostic source path lost its ordinate buffer".into(),
                            )
                        })?
                        .push(with_publication_axes(
                            point,
                            sample_ordinates,
                            diagnostic_axes,
                        )?);
                }
                images.push(QSourceImage::PublicationSample(
                    u32::try_from(sample_index).map_err(|_| ResourceError::SizeOverflow)?,
                ));
            }
        }
        points.push(with_assigned_source_vertex_axes(
            *end,
            ordinates,
            source_ports,
            axes,
            work,
        )?);
        if let Some(diagnostic_axes) = diagnostic_axes {
            diagnostic_points
                .as_mut()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "diagnostic source path lost its ordinate buffer".into(),
                    )
                })?
                .push(with_assigned_source_vertex_axes(
                    *end,
                    ordinates,
                    source_ports,
                    diagnostic_axes,
                    work,
                )?);
        }
        images.push(source_literal_image(*end, source_ports));
    }
    Ok(SourceWrittenPath {
        points,
        locations: LiteralSequence {
            output,
            axes,
            images,
        },
        diagnostic_points,
    })
}
pub(in crate::geometry::topology) fn write_ring(
    vertices: &[EmittedVertex<'_>],
    output: u32,
    owner: RecipeWalkOwner,
    recipe_rotation: usize,
    context: &SourcePathContext<'_, '_>,
    cursors: &mut BTreeMap<RecipeWalkOwner, usize>,
    work: &mut impl Work,
) -> Result<(Ring, LiteralSequence<QSourceImage>)> {
    let path = write_source_path(
        vertices,
        output,
        owner,
        recipe_rotation,
        context,
        cursors,
        work,
    )?;
    Ok((
        Ring::try_from_closed(CoordSeq::try_from_points(&path.points)?)?,
        path.locations,
    ))
}
pub(in crate::geometry::topology) fn rotate_area_ring_vertices<'topology>(
    vertices: &[EmittedVertex<'topology>],
    start_port: Option<QPortId>,
    work: &mut impl Work,
) -> Result<Option<(Vec<EmittedVertex<'topology>>, usize)>> {
    let Some(start_port) = start_port else {
        return Ok(None);
    };
    let edge_count = vertices
        .len()
        .checked_sub(1)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q area ring has no closing vertex".into()))?;
    let start_index = vertices
        .get(..edge_count)
        .and_then(|open| open.iter().position(|vertex| vertex.port.id == start_port))
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("Q area ring lost its native cycle start".into())
        })?;
    if start_index == 0 {
        return Ok(None);
    }
    work.charge_into_error(
        u64::try_from(vertices.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut rotated = Vec::with_capacity(vertices.len());
    let open = vertices.get(..edge_count).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q area ring has an invalid open vertex range".into())
    })?;
    let (prefix, suffix) = open.split_at_checked(start_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q area ring has an invalid native cycle start".into())
    })?;
    rotated.extend_from_slice(suffix);
    rotated.extend_from_slice(prefix);
    let first = *rotated.first().ok_or_else(|| {
        GeometryErrorKind::Invalid("Q area ring rotation removed every vertex".into())
    })?;
    rotated.push(first);
    Ok(Some((rotated, start_index)))
}
pub(in crate::geometry::topology) fn write_drawing(
    drawing: QEmissionDrawing<'_>,
    ring_roles: &[bool],
    ordinates: &[NodeOrdinates],
    publication: Option<SourcePublication<'_>>,
    source_ports: Option<&[Option<ExactChartPoint>]>,
    walks: Option<&RecipeWalkIndex<'_>>,
    axes: CoordinateAxes,
    work: &mut impl Work,
) -> Result<WrittenDrawingParts> {
    let mut walk_cursors = BTreeMap::new();
    let path_context = SourcePathContext {
        ordinates,
        source_ports,
        publication,
        walks,
        axes,
        diagnostic_axes: None,
    };
    let (polygons, polygon_locations) = match drawing.polygons {
        DrawnPolygons::WholeSphere(source_polygons) => {
            work.charge_into_error(
                u64::try_from(source_polygons.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut output = Vec::with_capacity(source_polygons.len());
            let mut output_locations = Vec::with_capacity(source_polygons.len());
            for (output_index, source_rings) in source_polygons.into_iter().enumerate() {
                work.charge_into_error(1)?;
                let output_id =
                    u32::try_from(output_index).map_err(|_| ResourceError::SizeOverflow)?;
                let mut shell = None;
                let mut shell_locations = None;
                let mut holes = Vec::new();
                let mut hole_locations = Vec::new();
                for source_ring in source_rings {
                    work.charge_into_error(1)?;
                    let (ring, locations) = write_ring(
                        &source_ring.vertices,
                        output_id,
                        source_ring.recipe_walk,
                        0,
                        &path_context,
                        &mut walk_cursors,
                        work,
                    )?;
                    if source_ring.hole {
                        holes.push(ring);
                        hole_locations.push(locations);
                    } else if shell.replace(ring).is_some()
                        || shell_locations.replace(locations).is_some()
                    {
                        return Err(GeometryErrorKind::Invalid(
                            "Q source polygon has more than one shell".into(),
                        )
                        .into());
                    }
                }
                let shell = shell.ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q source polygon has no shell".into())
                })?;
                output.push(Polygon::new(shell, holes));
                output_locations.push(LiteralPolygon {
                    output: output_id,
                    axes,
                    shell: shell_locations.ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q source polygon has no shell map".into())
                    })?,
                    holes: hole_locations,
                });
            }
            if output.is_empty() {
                return Err(GeometryErrorKind::Invalid(
                    "Q selected the whole sphere without source area polygons".into(),
                )
                .into());
            }
            (output, output_locations)
        },
        DrawnPolygons::Regions {
            rings,
            region_count,
        } => {
            work.charge_into_error(
                u64::try_from(region_count).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut shells = Vec::with_capacity(region_count);
            let mut holes = Vec::with_capacity(region_count);
            let mut shell_locations = Vec::with_capacity(region_count);
            let mut hole_locations = Vec::with_capacity(region_count);
            shells.resize_with(region_count, Vec::<Ring>::new);
            holes.resize_with(region_count, Vec::<Ring>::new);
            shell_locations.resize_with(region_count, Vec::<LiteralSequence<QSourceImage>>::new);
            hole_locations.resize_with(region_count, Vec::<LiteralSequence<QSourceImage>>::new);
            for (ring_index, drawn) in rings.into_iter().enumerate() {
                work.charge_into_error(1)?;
                let is_hole = *ring_roles.get(ring_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q certified ring roles are incomplete".into())
                })?;
                let region_id =
                    u32::try_from(drawn.region).map_err(|_| ResourceError::SizeOverflow)?;
                let rotated_vertices =
                    rotate_area_ring_vertices(&drawn.vertices, drawn.native_start_port, work)?;
                let (vertices, recipe_rotation) = rotated_vertices
                    .as_ref()
                    .map_or((&drawn.vertices[..], 0), |(vertices, rotation)| {
                        (vertices.as_slice(), *rotation)
                    });
                let (ring, locations) = write_ring(
                    vertices,
                    region_id,
                    drawn.recipe_walk,
                    recipe_rotation,
                    &path_context,
                    &mut walk_cursors,
                    work,
                )?;
                let destination = if is_hole {
                    holes.get_mut(drawn.region)
                } else {
                    shells.get_mut(drawn.region)
                }
                .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary region is unknown".into()))?;
                destination.push(ring);
                let location_destination = if is_hole {
                    hole_locations.get_mut(drawn.region)
                } else {
                    shell_locations.get_mut(drawn.region)
                }
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q boundary region literal map is unknown".into())
                })?;
                location_destination.push(locations);
            }

            let mut output = Vec::new();
            let mut output_locations = Vec::new();
            for (
                region,
                (((mut region_shells, region_holes), mut region_shell_maps), mut region_hole_maps),
            ) in shells
                .into_iter()
                .zip(holes)
                .zip(shell_locations)
                .zip(hole_locations)
                .enumerate()
            {
                work.charge_into_error(1)?;
                if region_shells.is_empty() && region_holes.is_empty() {
                    continue;
                }
                if region_shells.len() != 1 {
                    return Err(GeometryErrorKind::Invalid(
                        format!(
                            "selected Q region {region} has {} shells",
                            region_shells.len(),
                        )
                        .into(),
                    )
                    .into());
                }
                let shell = region_shells
                    .pop()
                    .expect("the selected region has one shell");
                if region_shell_maps.len() != 1 {
                    return Err(GeometryErrorKind::Invalid(
                        "selected Q region has no unique shell literal map".into(),
                    )
                    .into());
                }
                let output_id =
                    u32::try_from(output.len()).map_err(|_| ResourceError::SizeOverflow)?;
                let mut shell_map = region_shell_maps
                    .pop()
                    .expect("the selected region has one shell literal map");
                shell_map.output = output_id;
                for hole_map in &mut region_hole_maps {
                    hole_map.output = output_id;
                }
                output.push(Polygon::new(shell, region_holes));
                output_locations.push(LiteralPolygon {
                    output: output_id,
                    axes,
                    shell: shell_map,
                    holes: region_hole_maps,
                });
            }
            (output, output_locations)
        },
    };
    work.charge_into_error(
        u64::try_from(drawing.lines.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut lines = Vec::with_capacity(drawing.lines.len());
    let mut line_locations = Vec::with_capacity(drawing.lines.len());
    let mut diagnostic_lines = Vec::new();
    let mut diagnostic_tags = drawing.diagnostic_line_tags.iter().peekable();
    for (line_index, line) in drawing.lines.iter().enumerate() {
        let output = u32::try_from(line_index).map_err(|_| ResourceError::SizeOverflow)?;
        let diagnostic_tag = diagnostic_tags
            .next_if(|(tagged_line, _)| *tagged_line == line_index)
            .map(|(_, tag)| *tag);
        if diagnostic_tags
            .peek()
            .is_some_and(|(tagged_line, _)| *tagged_line < line_index)
        {
            return Err(GeometryErrorKind::Invalid(
                "Q diagnostic source tags are not ordered with their emitted lines".into(),
            )
            .into());
        }
        if matches!(line.origin, QLineOrigin::Diagnostic(_)) != diagnostic_tag.is_some() {
            return Err(GeometryErrorKind::Invalid(
                "Q diagnostic source tags do not match their emitted lines".into(),
            )
            .into());
        }
        let diagnostic_axes = diagnostic_tag.and_then(|tag| (tag.axes != axes).then_some(tag.axes));
        let line_path_context = SourcePathContext {
            diagnostic_axes,
            ..path_context
        };
        let path = write_source_path(
            &line.path,
            output,
            line.recipe_walk,
            0,
            &line_path_context,
            &mut walk_cursors,
            work,
        )?;
        let SourceWrittenPath {
            points,
            locations,
            diagnostic_points,
        } = path;
        let coordinates = CoordSeq::try_from_points(&points)?;
        if let Some(tag) = diagnostic_tag {
            work.charge_into_error(1)?;
            let diagnostic_coordinates = match diagnostic_points {
                Some(points) => CoordSeq::try_from_points(&points)?,
                None => coordinates.clone(),
            };
            diagnostic_lines.push(QDiagnosticLine {
                tag,
                coordinates: diagnostic_coordinates,
            });
        }
        lines.push(coordinates);
        line_locations.push(locations);
    }
    if diagnostic_tags.peek().is_some() {
        return Err(GeometryErrorKind::Invalid(
            "Q diagnostic source tags have no emitted line".into(),
        )
        .into());
    }
    work.charge_into_error(
        u64::try_from(drawing.points.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut points = Vec::with_capacity(drawing.points.len());
    let mut point_locations = Vec::with_capacity(drawing.points.len());
    for (point_index, &vertex) in drawing.points.iter().enumerate() {
        work.charge_into_error(1)?;
        points.push(with_assigned_source_vertex_axes(
            vertex,
            ordinates,
            source_ports,
            axes,
            work,
        )?);
        point_locations.push(LiteralPoint {
            output: u32::try_from(point_index).map_err(|_| ResourceError::SizeOverflow)?,
            axes,
            image: source_literal_image(vertex, source_ports),
        });
    }
    if let Some(walks) = walks {
        for (owner, owner_walks) in walks {
            work.charge_into_error(1)?;
            if walk_cursors.get(owner).copied().unwrap_or_default() != owner_walks.len() {
                return Err(GeometryErrorKind::Invalid(
                    "source drawing did not consume each producer walk exactly once".into(),
                )
                .into());
            }
        }
    }
    Ok(WrittenDrawingParts {
        points,
        lines,
        polygons,
        layout: ShapeLiteralLayout {
            structure: None,
            points: point_locations,
            lines: line_locations,
            polygons: polygon_locations,
        },
        diagnostic_lines,
    })
}
pub(in crate::geometry::topology) fn identity_output_order(
    point_count: usize,
    line_count: usize,
    polygon_count: usize,
    work: &mut impl Work,
) -> Result<CertifiedShapeOrder> {
    let count = point_count
        .checked_add(line_count)
        .and_then(|count| count.checked_add(polygon_count))
        .ok_or(ResourceError::SizeOverflow)?;
    work.charge_into_error(u64::try_from(count).map_err(|_| ResourceError::SizeOverflow)?)?;
    Ok(CertifiedShapeOrder {
        points: identity_output_indices(point_count, work)?,
        lines: identity_output_indices(line_count, work)?,
        polygons: identity_output_indices(polygon_count, work)?,
    })
}
pub(in crate::geometry::topology) fn identity_output_indices(
    length: usize,
    work: &mut impl Work,
) -> Result<Vec<usize>> {
    work.charge_into_error(u64::try_from(length).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut order = Vec::with_capacity(length);
    for index in 0..length {
        work.charge_into_error(1)?;
        order.push(index);
    }
    Ok(order)
}
pub(in crate::geometry::topology) fn seal_output_parts(
    plan: &QEmissionPlan,
    points: Vec<Point>,
    lines: Vec<CoordSeq>,
    mut polygons: Vec<Polygon>,
    axes: CoordinateAxes,
    declared_axes: CoordinateAxes,
    track_order: bool,
    work: &mut impl Work,
) -> Result<(Shape, Option<CertifiedShapeOrder>)> {
    let output_count = points
        .len()
        .checked_add(lines.len())
        .and_then(|count| count.checked_add(polygons.len()))
        .ok_or(ResourceError::SizeOverflow)?;
    work.charge_into_error(u64::try_from(output_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let point_count = points.len();
    let line_count = lines.len();

    if let Some(support) = plan.boundary_support {
        let point_part = if points.is_empty() {
            None
        } else {
            work.charge_into_error(
                u64::try_from(points.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut coordinates = CoordSeqBuilder::with_capacity(axes, points.len());
            for point in points {
                work.charge_into_error(1)?;
                coordinates.push(point);
            }
            Some(Shape::multi_point(axes, coordinates.finish()?))
        };
        let line_part = if lines.is_empty() {
            None
        } else {
            work.charge_into_error(
                u64::try_from(lines.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let values = lines.into_iter().map(LineSeq::from_proven).collect();
            Some(Shape::narrow_lines(axes, values))
        };

        if support.has_area {
            let line_part = line_part.unwrap_or_else(|| {
                Shape::typed_empty(GeometryKind::MultiLineString, declared_axes)
            });
            let keep_points = point_part.is_some();
            let keep_lines = line_count != 0;
            let shape = match point_part {
                Some(point_part) => {
                    work.charge_into_error(2)?;
                    Shape::geometry_collection(vec![line_part, point_part])
                },
                None => Ok(line_part),
            }?;
            let order = track_order
                .then(|| {
                    identity_output_order(
                        if keep_points { point_count } else { 0 },
                        if keep_lines { line_count } else { 0 },
                        0,
                        work,
                    )
                })
                .transpose()?;
            return Ok((shape, order));
        }
        if support.has_curve {
            let keep_points = point_part.is_some();
            let shape = point_part
                .unwrap_or_else(|| Shape::typed_empty(GeometryKind::MultiPoint, declared_axes));
            let order = track_order
                .then(|| {
                    identity_output_order(if keep_points { point_count } else { 0 }, 0, 0, work)
                })
                .transpose()?;
            return Ok((shape, order));
        }
        if support.has_point {
            let shape = Shape::typed_empty(GeometryKind::GeometryCollection, declared_axes);
            let order = track_order
                .then(|| identity_output_order(0, 0, 0, work))
                .transpose()?;
            return Ok((shape, order));
        }
        let shape = Shape::typed_empty(GeometryKind::MultiLineString, declared_axes);
        let order = track_order
            .then(|| identity_output_order(0, 0, 0, work))
            .transpose()?;
        return Ok((shape, order));
    }

    if plan.whole_sphere {
        let polygon_count = polygons.len();
        let shape = match polygons.len() {
            0 => Err(GeometryErrorKind::Invalid(
                "Q selected the whole sphere without source area polygons".into(),
            )
            .into()),
            1 => {
                Ok(Shape::Polygon(polygons.pop().expect(
                    "a one-polygon whole-sphere output has its polygon",
                )))
            },
            _ => {
                work.charge_into_error(
                    u64::try_from(polygons.len()).map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                Shape::geometry_collection(polygons.into_iter().map(Shape::Polygon).collect())
            },
        }?;
        let order = track_order
            .then(|| identity_output_order(0, 0, polygon_count, work))
            .transpose()?;
        return Ok((shape, order));
    }

    if track_order {
        let (shape, order) =
            build_certified_shape_with_axes_and_order(points, lines, polygons, axes, work)?;
        Ok((shape, Some(order)))
    } else {
        Ok((
            build_certified_shape_with_axes(points, lines, polygons, axes)?,
            None,
        ))
    }
}
