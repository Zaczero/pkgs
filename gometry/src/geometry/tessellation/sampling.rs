use super::{EarcutCornerId, EarcutTriangle, earcut_polygon_with_ids};
use crate::error::{ResourceError, Result};
use crate::geometry::publication::{
    ProducerDrawingReceipt, StorageTarget, finish_producer_drawing,
};
use crate::geometry::storage::ValidityMask;
use crate::geometry::{MultiMember, Point, Polygon, Segment, Shape, TopologyDomain};
use crate::py::vectors::Groups;
use crate::{Frame, PyGeometryArray};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SampleOccurrenceId {
    pub(crate) component: usize,
    pub(crate) occurrence: usize,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct GeographicSampleSegment {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "The common sampling sink binds the retained source occurrence to stored output"
        )
    )]
    pub(crate) occurrence: SampleOccurrenceId,
    pub(crate) start: Point,
    pub(crate) end: Point,
    pub(crate) longitude: crate::geographic::RetainedChartDisplacement,
    pub(crate) delta_x: f64,
    pub(crate) delta_y: f64,
    pub(crate) raw_compatible: bool,
}

impl GeographicSampleSegment {
    pub(crate) fn sample(
        self,
        along: f64,
        frame: crate::geographic::GeographicFrame,
    ) -> Result<Point> {
        let x = if self.raw_compatible {
            (1.0 - along) * self.start.x + along * self.end.x
        } else {
            let start = self.longitude.endpoints()[0].get();
            frame
                .longitude()
                .canonicalize(start + self.delta_x * along)
                .map_err(|error| {
                    crate::geometry::GeometryErrorKind::antimeridian_split_failed(error.to_string())
                })?
                .get()
        };
        Point::new(x, (1.0 - along) * self.start.y + along * self.end.y)
    }
}

#[derive(Clone, Copy, Debug)]
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "The common sampling sink consumes selected support identities for stored output"
    )
)]
pub(crate) enum SampleSupport {
    AreaTriangle {
        occurrence: SampleOccurrenceId,
        vertices: [Point; 3],
        corners: [EarcutCornerId; 3],
    },
    Segment {
        occurrence: SampleOccurrenceId,
        segment: Segment,
    },
    GeographicSegment {
        segment: GeographicSampleSegment,
    },
    PointAtom {
        occurrence: SampleOccurrenceId,
        point: Point,
    },
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum SampleRecipe {
    Triangle { spread: f64, blend: f64 },
    Segment { along: f64 },
    Atom,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SampleDraw {
    pub(crate) draw_ordinal: usize,
    pub(crate) selected_element: usize,
    pub(crate) recipe: SampleRecipe,
    pub(crate) point: Point,
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "The common sampling sink consumes row, domain, seed, and prepared support authority"
    )
)]
pub(crate) struct PreparedSampling {
    pub(crate) logical_row: usize,
    pub(crate) resolved_seed: u64,
    pub(crate) domain: TopologyDomain,
    pub(crate) support_elements: Vec<SampleSupport>,
    pub(crate) draws: Vec<SampleDraw>,
}

impl PreparedSampling {
    pub(in crate::geometry::tessellation) const fn from_draws(
        logical_row: usize,
        resolved_seed: u64,
        domain: TopologyDomain,
        support_elements: Vec<SampleSupport>,
        draws: Vec<SampleDraw>,
    ) -> Self {
        Self {
            logical_row,
            resolved_seed,
            domain,
            support_elements,
            draws,
        }
    }

    pub(in crate::geometry::tessellation) const fn empty(
        logical_row: usize,
        resolved_seed: u64,
        domain: TopologyDomain,
    ) -> Self {
        Self::from_draws(logical_row, resolved_seed, domain, Vec::new(), Vec::new())
    }

    pub(crate) fn into_points(self) -> Vec<Point> {
        self.draws.into_iter().map(|draw| draw.point).collect()
    }
}

fn validate_prepared_sampling(prepared: &PreparedSampling, logical_row: usize) -> Result<()> {
    if prepared.logical_row != logical_row {
        return Err(crate::geometry::GeometryErrorKind::Invalid(
            "prepared sampling receipt changed its logical source row".into(),
        )
        .into());
    }
    for (ordinal, draw) in prepared.draws.iter().enumerate() {
        let Some(support) = prepared.support_elements.get(draw.selected_element) else {
            return Err(crate::geometry::GeometryErrorKind::Invalid(
                "prepared sampling draw has no selected support element".into(),
            )
            .into());
        };
        let matching_recipe = matches!(
            (support, draw.recipe),
            (
                SampleSupport::AreaTriangle { .. },
                SampleRecipe::Triangle { .. }
            ) | (SampleSupport::Segment { .. }, SampleRecipe::Segment { .. })
                | (
                    SampleSupport::GeographicSegment { .. },
                    SampleRecipe::Segment { .. }
                )
                | (SampleSupport::PointAtom { .. }, SampleRecipe::Atom)
        );
        if draw.draw_ordinal != ordinal || !matching_recipe {
            return Err(crate::geometry::GeometryErrorKind::Invalid(
                "prepared sampling draw lost its selected placement identity".into(),
            )
            .into());
        }
    }
    Ok(())
}

pub(crate) fn publish_sampling_array(
    prepared: PreparedSampling,
    frame: Frame,
) -> Result<PyGeometryArray> {
    use std::sync::Arc;

    use crate::array::StreamingShapes;
    use crate::geometry::embedding_completion::literals::{
        LiteralAddress, LiteralPoint, LiteralRowLayout, ShapeLiteralLayout,
    };
    use crate::geometry::embedding_completion::storage::FrozenLiteralBuffers;
    use crate::geometry::publication::{ProducerImageId, ProducerLiteral};
    use crate::work::Unmetered;

    validate_prepared_sampling(&prepared, prepared.logical_row)?;
    let output_count = prepared.draws.len();
    let mut writer = StreamingShapes::with_capacity(0, output_count);
    let mut row_map = Vec::with_capacity(output_count);
    let mut literals = Vec::with_capacity(output_count);
    for (logical_row, draw) in prepared.draws.into_iter().enumerate() {
        let image = ProducerImageId::new(logical_row);
        let address = LiteralAddress {
            operation: 0,
            logical_row,
            group_row: None,
            value_row: None,
            output: 0,
        };
        writer.try_push(Shape::Point(draw.point))?;
        row_map.push(Some(LiteralRowLayout {
            operation: 0,
            logical_row,
            group_row: None,
            value_row: None,
            layout: ShapeLiteralLayout {
                structure: Some(
                    crate::geometry::embedding_completion::literals::ShapeStructure::capture(
                        &Shape::Point(draw.point),
                    ),
                ),
                points: vec![LiteralPoint {
                    output: 0,
                    axes: crate::geometry::CoordinateAxes::from_point(draw.point),
                    image,
                }],
                lines: Vec::new(),
                polygons: Vec::new(),
            },
        }));
        literals.push(ProducerLiteral::from_point(address, image, draw.point));
    }
    let (storage, missing, layout) = writer.finish_parts()?;
    let mut work = Unmetered;
    let buffers =
        FrozenLiteralBuffers::rows(Arc::new(storage), missing, layout, row_map, &mut work)?;
    let receipt = ProducerDrawingReceipt::new(buffers, literals);
    PyGeometryArray::from_finished(finish_producer_drawing(
        receipt,
        frame,
        StorageTarget::Array,
    )?)
}

pub(crate) fn publish_sampling_groups(
    rows: Vec<Option<PreparedSampling>>,
    frame: Frame,
    missing: Option<ValidityMask>,
) -> Result<Groups> {
    use std::sync::Arc;

    use crate::array::StreamingShapes;
    use crate::geometry::embedding_completion::literals::{
        LiteralAddress, LiteralPoint, LiteralRowLayout, ShapeLiteralLayout,
    };
    use crate::geometry::embedding_completion::storage::FrozenLiteralBuffers;
    use crate::geometry::publication::{ProducerImageId, ProducerLiteral};
    use crate::work::Unmetered;

    let output_count = rows.iter().try_fold(0_usize, |total, row| {
        total
            .checked_add(row.as_ref().map_or(0, |row| row.draws.len()))
            .ok_or(ResourceError::SizeOverflow)
    })?;
    if missing
        .as_ref()
        .is_some_and(|missing| missing.len() != rows.len())
    {
        return Err(crate::geometry::GeometryErrorKind::Invalid(
            "sampling group validity does not match its logical rows".into(),
        )
        .into());
    }

    let mut writer = StreamingShapes::with_capacity(0, output_count);
    let offset_count = rows
        .len()
        .checked_add(1)
        .ok_or(ResourceError::SizeOverflow)?;
    let mut offsets = Vec::with_capacity(offset_count);
    offsets.push(0_i64);
    let mut value_map = Vec::with_capacity(output_count);
    let mut literals = Vec::with_capacity(output_count);
    let mut value_row = 0_usize;
    for (logical_row, prepared) in rows.into_iter().enumerate() {
        let is_missing = missing
            .as_ref()
            .is_some_and(|missing| missing.is_missing(logical_row));
        if is_missing != prepared.is_none() {
            return Err(crate::geometry::GeometryErrorKind::Invalid(
                "sampling group receipt does not match its source-row validity".into(),
            )
            .into());
        }
        if let Some(prepared) = prepared {
            validate_prepared_sampling(&prepared, logical_row)?;
            for draw in prepared.draws {
                let image = ProducerImageId::new(value_row);
                let address = LiteralAddress {
                    operation: 0,
                    logical_row,
                    group_row: Some(logical_row),
                    value_row: Some(value_row),
                    output: 0,
                };
                writer.try_push(Shape::Point(draw.point))?;
                value_map.push(Some(LiteralRowLayout {
                    operation: 0,
                    logical_row,
                    group_row: Some(logical_row),
                    value_row: Some(value_row),
                    layout: ShapeLiteralLayout {
                        structure: Some(crate::geometry::embedding_completion::literals::ShapeStructure::capture(&Shape::Point(draw.point))),
                        points: vec![LiteralPoint {
                            output: 0,
                            axes: crate::geometry::CoordinateAxes::from_point(draw.point),
                            image,
                        }],
                        lines: Vec::new(),
                        polygons: Vec::new(),
                    },
                }));
                literals.push(ProducerLiteral::from_point(address, image, draw.point));
                value_row = value_row
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
            }
        }
        offsets.push(i64::try_from(value_row).map_err(|_| ResourceError::SizeOverflow)?);
    }
    let (storage, value_missing, layout) = writer.finish_parts()?;
    let mut work = Unmetered;
    let buffers = FrozenLiteralBuffers::groups(
        Arc::new(storage),
        value_missing,
        layout,
        value_map,
        offsets,
        missing,
        &mut work,
    )?;
    let receipt = ProducerDrawingReceipt::new(buffers, literals);
    Groups::from_finished(finish_producer_drawing(
        receipt,
        frame,
        StorageTarget::Groups,
    )?)
}

/// A common power-of-two factor that keeps weight products finite for
/// huge-but-valid coordinates (exact in binary floating point; weights
/// only need relative consistency).
pub(crate) fn weight_scale(largest: f64) -> f64 {
    if largest > 2.0_f64.powi(500) {
        // Admitted finite binary64 coordinates have a ceiling exponent at most 1024.
        let exponent = largest.log2().ceil() as i32;
        2.0_f64.powi(500_i32.wrapping_sub(exponent))
    } else {
        1.0
    }
}

/// The weighted-pick engine behind `sample_points`: cumulative weights over
/// `pieces` (zero-weight slivers never selected), then `count` draws of
/// piece-by-prefix-sum followed by the piece's own placement map. `None`
/// when the total weight is not positive finite — the caller falls back to
/// a lower-dimension sample space.
pub(crate) fn sample_weighted<T>(
    count: usize,
    seed: u64,
    pieces: &[T],
    weight: impl Fn(&T) -> f64,
    place: impl Fn(&T, &mut u64) -> Result<(Point, SampleRecipe)>,
) -> Result<Option<Vec<SampleDraw>>> {
    let mut total = 0.0;
    let mut cumulative = Vec::with_capacity(pieces.len());
    for piece in pieces {
        total += weight(piece);
        cumulative.push(total);
    }
    if total <= 0.0 || !total.is_finite() {
        return Ok(None);
    }
    let mut draws = Vec::with_capacity(count);
    let mut state = seed;
    for draw_ordinal in 0..count {
        let pick = uniform_f64(&mut state) * total;
        let index = cumulative.partition_point(|&edge| edge <= pick);
        let element_id = index.min(pieces.len().saturating_sub(1));
        let support = pieces
            .get(element_id)
            .expect("a positive finite total comes from at least one piece");
        let (point, recipe) = place(support, &mut state)?;
        draws.push(SampleDraw {
            draw_ordinal,
            selected_element: element_id,
            recipe,
            point,
        });
    }
    Ok(Some(draws))
}

fn next_sample_component(component: &mut usize) -> Result<usize> {
    let id = *component;
    *component = id
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    Ok(id)
}

fn collect_polygon_sample_triangles(
    polygon: &Polygon,
    component: &mut usize,
    out: &mut Vec<SampleSupport>,
) -> Result<()> {
    let component = next_sample_component(component)?;
    earcut_polygon_with_ids(polygon, &mut |triangle: EarcutTriangle| {
        out.push(SampleSupport::AreaTriangle {
            occurrence: SampleOccurrenceId {
                component,
                occurrence: triangle.face,
            },
            vertices: triangle.points,
            corners: triangle.corners,
        });
        Ok(())
    })
}

fn collect_sample_triangles_inner(
    shape: &Shape,
    component: &mut usize,
    out: &mut Vec<SampleSupport>,
) -> Result<()> {
    match shape {
        Shape::Polygon(polygon) => collect_polygon_sample_triangles(polygon, component, out)?,
        Shape::MultiPolygon(polygons) => {
            for member in polygons.members_with_values() {
                match member {
                    MultiMember::Empty => {
                        next_sample_component(component)?;
                    },
                    MultiMember::Value(polygon) => {
                        collect_polygon_sample_triangles(polygon, component, out)?;
                    },
                }
            }
        },
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                collect_sample_triangles_inner(geometry, component, out)?;
            }
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => {},
    }
    Ok(())
}

/// Retain each emitted Earcut triangle and its source component before any
/// weighted draw can detach it from its support.
pub(crate) fn collect_sample_triangles(shape: &Shape, out: &mut Vec<SampleSupport>) -> Result<()> {
    let mut component = 0;
    collect_sample_triangles_inner(shape, &mut component, out)
}

fn append_sample_atoms(
    points: impl IntoIterator<Item = Point>,
    component: usize,
    out: &mut Vec<SampleSupport>,
) {
    out.extend(points.into_iter().enumerate().map(|(occurrence, point)| {
        SampleSupport::PointAtom {
            occurrence: SampleOccurrenceId {
                component,
                occurrence,
            },
            point,
        }
    }));
}

fn collect_sample_atoms_inner(
    shape: &Shape,
    component: &mut usize,
    out: &mut Vec<SampleSupport>,
) -> Result<()> {
    match shape {
        Shape::Point(point) => {
            let component = next_sample_component(component)?;
            append_sample_atoms([*point], component, out);
        },
        Shape::MultiPoint(points) => {
            let component = next_sample_component(component)?;
            append_sample_atoms(points, component, out);
        },
        Shape::LineString(line) => {
            let component = next_sample_component(component)?;
            append_sample_atoms(line, component, out);
        },
        Shape::MultiLineString(lines) => {
            for member in lines.members_with_values() {
                let component = next_sample_component(component)?;
                if let MultiMember::Value(line) = member {
                    append_sample_atoms(line, component, out);
                }
            }
        },
        Shape::Polygon(polygon) => {
            let component = next_sample_component(component)?;
            append_sample_atoms(polygon.points(), component, out);
        },
        Shape::MultiPolygon(polygons) => {
            for member in polygons.members_with_values() {
                let component = next_sample_component(component)?;
                if let MultiMember::Value(polygon) = member {
                    append_sample_atoms(polygon.points(), component, out);
                }
            }
        },
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                collect_sample_atoms_inner(geometry, component, out)?;
            }
        },
        Shape::Empty(..) => {},
    }
    Ok(())
}

/// Vertices of the dimension-0 support, retaining each collection and
/// multipart component occurrence, including coincident atoms.
pub(crate) fn collect_sample_atoms(shape: &Shape, out: &mut Vec<SampleSupport>) -> Result<()> {
    let mut component = 0;
    collect_sample_atoms_inner(shape, &mut component, out)
}

/// The splitmix64 step — the whole RNG behind `sample_points`: tiny,
/// splittable, and deterministic across platforms (no `rand` dependency).
pub(crate) const fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A uniform `f64` in `[0, 1)` from the top 53 bits of one splitmix64 draw.
pub(crate) fn uniform_f64(state: &mut u64) -> f64 {
    (splitmix64(state) >> 11) as f64 * (1.0 / 9_007_199_254_740_992.0)
}

/// Decorrelated per-row seed for array lanes: same geometry at different
/// rows samples different deterministic streams.
pub(crate) const fn row_sample_seed(seed: u64, row: usize) -> u64 {
    let mut state = seed ^ (row as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    splitmix64(&mut state)
}

#[cfg(test)]
mod tests {
    use super::{SampleRecipe, sample_weighted};
    use crate::geometry::{Point, Segment, XY};

    #[test]
    fn seed1_unit_segment_two_draws_match_splitmix64() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(1.0, 0.0),
        };
        let points = sample_weighted(
            2,
            1,
            &[segment],
            |_| 1.0,
            |segment, state| {
                let along = super::uniform_f64(state);
                Ok((
                    Point::new(
                        (1.0 - along) * segment.start.x + along * segment.end.x,
                        (1.0 - along) * segment.start.y + along * segment.end.y,
                    )?,
                    SampleRecipe::Segment { along },
                ))
            },
        )
        .expect("positive weight")
        .expect("samples");
        assert_eq!(points.len(), 2);
        let [first, second] = points.as_slice() else {
            panic!("two samples were requested");
        };
        assert_eq!(first.selected_element, 0);
        let SampleRecipe::Segment { along } = first.recipe else {
            panic!("segment placement recipe");
        };
        assert_eq!(
            first.point.x.to_bits(),
            0.745_781_757_262_701_1_f64.to_bits()
        );
        assert_eq!(first.point.y.to_bits(), 0.0_f64.to_bits());
        assert_eq!(along.to_bits(), first.point.x.to_bits());
        assert_eq!(second.selected_element, 0);
        assert_eq!(
            second.point.x.to_bits(),
            0.444_359_217_055_772_1_f64.to_bits()
        );
        assert_eq!(second.point.y.to_bits(), 0.0_f64.to_bits());
        let SampleRecipe::Segment { along } = second.recipe else {
            panic!("segment placement recipe");
        };
        assert_eq!(along.to_bits(), second.point.x.to_bits());
    }
}
