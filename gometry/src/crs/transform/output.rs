use super::{
    CoordRun, CoordSeq, CoordWindow, GeometryErrorKind, LineSeq, MColumn, MOrdinate, Point, Range,
    Result, Shape, SharedSlice, TransformFailure, ZOrdinate, types,
};

const TRANSFORM_CHUNK_BYTES: usize = 1024 * 1024;
const XY_CHUNK_VERTICES: usize = TRANSFORM_CHUNK_BYTES / (2 * size_of::<f64>());
const XYZ_CHUNK_VERTICES: usize = TRANSFORM_CHUNK_BYTES / (3 * size_of::<f64>());

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OutputBatch {
    Xy,
    Xyz,
}

/// The [`OutputBatch`] a run needs: XYZ when it carries Z, XY otherwise.
fn source_batch(source: CoordRun<'_>) -> OutputBatch {
    match source {
        CoordRun::Point(point) => point.z().map_or(OutputBatch::Xy, |_| OutputBatch::Xyz),
        CoordRun::Seq(seq) => seq.zs().map_or(OutputBatch::Xy, |_| OutputBatch::Xyz),
    }
}

fn source_len(source: CoordRun<'_>) -> usize {
    match source {
        CoordRun::Point(..) => 1,
        CoordRun::Seq(seq) => seq.len(),
    }
}

fn source_carried_ms(source: CoordRun<'_>) -> Option<MColumn> {
    match source {
        CoordRun::Point(point) => point.m().map(|m| MColumn::Local {
            owner: SharedSlice::from_vec(vec![m]),
            offset: 0,
        }),
        CoordRun::Seq(seq) => seq.carried_ms(),
    }
}

#[derive(Default)]
struct OutputCounts {
    xy_vertices: usize,
    xyz_vertices: usize,
    sequences: usize,
}

impl OutputCounts {
    fn add(&mut self, source: CoordRun<'_>) -> Result<()> {
        self.add_run(source_batch(source), source_len(source))
    }

    fn add_run(&mut self, batch: OutputBatch, vertices: usize) -> Result<()> {
        let total = match batch {
            OutputBatch::Xy => &mut self.xy_vertices,
            OutputBatch::Xyz => &mut self.xyz_vertices,
        };
        let Some(total_vertices) = total.checked_add(vertices) else {
            return Err(GeometryErrorKind::OffsetCapacityExceeded.into());
        };
        types::ensure_coordseq_vertex_capacity(total_vertices)?;
        *total = total_vertices;
        // Sequence count sizes native staging only; it is not a wire count.
        self.sequences = self.sequences.wrapping_add(1);
        Ok(())
    }
}

/// One final contiguous coordinate batch. M is carried independently because
/// it is never transformed; full columns and windows keep their shared owners.
struct OutputColumns {
    xs: Vec<f64>,
    ys: Vec<f64>,
    zs: Option<Vec<f64>>,
}

impl OutputColumns {
    fn with_capacity(capacity: usize, has_z: bool) -> Self {
        Self {
            xs: Vec::with_capacity(capacity),
            ys: Vec::with_capacity(capacity),
            zs: has_z.then(|| Vec::with_capacity(capacity)),
        }
    }

    fn append(&mut self, source: CoordRun<'_>) -> Range<usize> {
        let start = self.xs.len();
        match source {
            CoordRun::Point(point) => {
                debug_assert_eq!(self.zs.is_some(), point.z().is_some());
                self.xs.push(point.x);
                self.ys.push(point.y);
                if let Some(zs) = &mut self.zs {
                    zs.push(point.z().expect("XYZ output source carries Z"));
                }
            },
            CoordRun::Seq(seq) => {
                debug_assert_eq!(self.zs.is_some(), seq.zs().is_some());
                self.xs.extend_from_slice(seq.xs());
                self.ys.extend_from_slice(seq.ys());
                if let (Some(zs), Some(source_zs)) = (&mut self.zs, seq.zs()) {
                    zs.extend_from_slice(source_zs);
                }
            },
        }
        start..self.xs.len()
    }
}

struct OutputCoordSeq {
    batch: OutputBatch,
    range: Range<usize>,
    shape: usize,
    ms: Option<MColumn>,
}

pub(crate) struct TransformOutputPlan {
    xy: OutputColumns,
    xyz: OutputColumns,
    sequences: Vec<OutputCoordSeq>,
}

impl TransformOutputPlan {
    pub(crate) fn try_new(shapes: &[&Shape]) -> Result<Self> {
        let mut counts = OutputCounts::default();
        for shape in shapes {
            shape.for_each_coord_run(&mut |source| counts.add(source))?;
        }
        let mut plan = Self {
            xy: OutputColumns::with_capacity(counts.xy_vertices, false),
            xyz: OutputColumns::with_capacity(counts.xyz_vertices, true),
            sequences: Vec::with_capacity(counts.sequences),
        };
        for (shape_index, shape) in shapes.iter().enumerate() {
            let Ok(()) = shape.for_each_coord_run(&mut |source| {
                plan.push_source(source, shape_index);
                Ok::<_, std::convert::Infallible>(())
            });
        }
        Ok(plan)
    }

    fn push_source(&mut self, source: CoordRun<'_>, shape: usize) {
        let batch = source_batch(source);
        let range = match batch {
            OutputBatch::Xy => self.xy.append(source),
            OutputBatch::Xyz => self.xyz.append(source),
        };
        self.sequences.push(OutputCoordSeq {
            batch,
            range,
            shape,
            ms: source_carried_ms(source),
        });
    }

    pub(crate) fn execute_indexed(
        &mut self,
        mut transform: impl FnMut(
            &mut [f64],
            &mut [f64],
            Option<&mut [f64]>,
        ) -> std::result::Result<(), TransformFailure>,
    ) -> std::result::Result<(), TransformFailure> {
        self.execute_with_capacities_indexed(XY_CHUNK_VERTICES, XYZ_CHUNK_VERTICES, &mut transform)
    }

    fn execute_with_capacities_indexed(
        &mut self,
        xy_capacity: usize,
        xyz_capacity: usize,
        transform: &mut impl FnMut(
            &mut [f64],
            &mut [f64],
            Option<&mut [f64]>,
        ) -> std::result::Result<(), TransformFailure>,
    ) -> std::result::Result<(), TransformFailure> {
        match self.execute_axes_indexed(false, xy_capacity, None, transform) {
            Ok(()) => self.execute_axes_indexed(true, xyz_capacity, None, transform),
            Err(xy_failure) => {
                let Some(xy_shape) = xy_failure.coordinate else {
                    return Err(xy_failure);
                };
                // Only an XYZ failure on an earlier input shape can replace
                // the XY failure. Do not execute same- or later-shape input.
                match self.execute_axes_indexed(true, xyz_capacity, Some(xy_shape), transform) {
                    Err(xyz_failure)
                        if xyz_failure
                            .coordinate
                            .is_some_and(|xyz_shape| xyz_shape < xy_shape) =>
                    {
                        Err(xyz_failure)
                    },
                    _ => Err(xy_failure),
                }
            },
        }
    }

    fn execute_axes_indexed(
        &mut self,
        has_z: bool,
        capacity: usize,
        before_shape: Option<usize>,
        transform: &mut impl FnMut(
            &mut [f64],
            &mut [f64],
            Option<&mut [f64]>,
        ) -> std::result::Result<(), TransformFailure>,
    ) -> std::result::Result<(), TransformFailure> {
        let batch = if has_z {
            OutputBatch::Xyz
        } else {
            OutputBatch::Xy
        };
        let columns = if has_z { &self.xyz } else { &self.xy };
        let coordinate_count = before_shape.map_or(columns.xs.len(), |shape| {
            self.sequences
                .iter()
                .filter(|sequence| sequence.batch == batch && sequence.shape < shape)
                .map(|sequence| sequence.range.end)
                .max()
                .unwrap_or(0)
        });
        debug_assert!(coordinate_count <= columns.xs.len());
        let columns = if has_z { &mut self.xyz } else { &mut self.xy };
        execute_batch_prefix_indexed(columns, coordinate_count, capacity, transform).map_err(
            |failure| {
                let shape = failure.coordinate.and_then(|coordinate| {
                    self.sequences
                        .iter()
                        .find(|sequence| {
                            sequence.batch == batch && sequence.range.contains(&coordinate)
                        })
                        .map(|sequence| sequence.shape)
                });
                debug_assert!(failure.coordinate.is_none_or(|_| shape.is_some()));
                failure.with_coordinate(shape)
            },
        )
    }

    pub(crate) fn finish(self, shapes: &[&Shape]) -> Result<Vec<Shape>> {
        let Self { xy, xyz, sequences } = self;
        let xy_xs = SharedSlice::from_vec(xy.xs);
        let xy_ys = SharedSlice::from_vec(xy.ys);
        let xyz_xs = SharedSlice::from_vec(xyz.xs);
        let xyz_ys = SharedSlice::from_vec(xyz.ys);
        let xyz_zs = SharedSlice::from_vec(xyz.zs.expect("XYZ output columns carry Z"));
        let sequences = sequences
            .into_iter()
            .map(|owner| {
                let (xs, ys, zs) = match owner.batch {
                    OutputBatch::Xy => (&xy_xs, &xy_ys, None),
                    OutputBatch::Xyz => (&xyz_xs, &xyz_ys, Some(&xyz_zs)),
                };
                let window = CoordWindow::from_range(owner.range, xs.len());
                let ms = owner.ms.map(|column| match column {
                    MColumn::Shared(owner) => MColumn::Local { owner, offset: 0 },
                    local @ MColumn::Local { .. } => local,
                });
                CoordSeq::from_shared_columns_with_window(
                    xs.clone(),
                    ys.clone(),
                    zs.cloned(),
                    ms,
                    window,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let mut cursor = TransformCursor {
            sequences: sequences.into_iter(),
        };
        shapes
            .iter()
            .map(|shape| rebuild_shape_columns(shape, &mut cursor))
            .collect::<Result<Vec<_>>>()
    }
}

fn execute_batch_prefix_indexed(
    columns: &mut OutputColumns,
    coordinate_count: usize,
    capacity: usize,
    transform: &mut impl FnMut(
        &mut [f64],
        &mut [f64],
        Option<&mut [f64]>,
    ) -> std::result::Result<(), TransformFailure>,
) -> std::result::Result<(), TransformFailure> {
    let capacity = capacity.max(1);
    let OutputColumns { xs, ys, zs } = columns;
    debug_assert_eq!(xs.len(), ys.len(), "XY output columns stay aligned");
    debug_assert!(
        zs.as_ref().is_none_or(|zs| zs.len() == xs.len()),
        "XYZ output columns stay aligned",
    );
    debug_assert!(coordinate_count <= xs.len());
    let xs = xs
        .get_mut(..coordinate_count)
        .expect("the selected shape prefix fits its X column");
    let ys = ys
        .get_mut(..coordinate_count)
        .expect("the selected shape prefix fits its Y column");
    let mut offset = 0_usize;
    match zs.as_mut() {
        Some(zs) => {
            let zs = zs
                .get_mut(..coordinate_count)
                .expect("the selected shape prefix fits its Z column");
            for ((xs, ys), zs) in xs
                .chunks_mut(capacity)
                .zip(ys.chunks_mut(capacity))
                .zip(zs.chunks_mut(capacity))
            {
                let end = offset.wrapping_add(xs.len());
                transform(xs, ys, Some(zs))
                    .map_err(|failure| failure.offset_coordinate(offset, end))?;
                offset = end;
            }
        },
        None => {
            for (xs, ys) in xs.chunks_mut(capacity).zip(ys.chunks_mut(capacity)) {
                let end = offset.wrapping_add(xs.len());
                transform(xs, ys, None)
                    .map_err(|failure| failure.offset_coordinate(offset, end))?;
                offset = end;
            }
        },
    }
    Ok(())
}

/// Read cursor over the two transformed batches: each shape consumes exactly
/// the slices it contributed, in order, from the XY or XYZ batch matching its
/// axes.
struct TransformCursor {
    sequences: std::vec::IntoIter<CoordSeq>,
}

impl TransformCursor {
    fn take(&mut self) -> CoordSeq {
        self.sequences
            .next()
            .expect("the shape walk consumes its emitted coordinate sequences in order")
    }
}

fn rebuild_point_columns(point: &Point, cursor: &mut TransformCursor) -> Point {
    let transformed = cursor.take().point_at(0);
    Point::new_raw_axes(
        transformed.x,
        transformed.y,
        ZOrdinate(transformed.z()),
        MOrdinate(point.m()),
    )
}

/// Rebuild one shape from the transformed batches — original axes are
/// preserved (2D stays 2D; 3D gets the transformed Z lane). Collection
/// reconstruction remains checked against the source depth.
fn rebuild_shape_columns(shape: &Shape, cursor: &mut TransformCursor) -> Result<Shape> {
    Ok(match shape {
        Shape::Point(point) => Shape::Point(rebuild_point_columns(point, cursor)),
        Shape::MultiPoint(points) => {
            Shape::MultiPoint(points.with_coords(rebuild_seq_columns(points, cursor)))
        },
        Shape::LineString(points) => {
            Shape::LineString(LineSeq::from_proven(rebuild_seq_columns(points, cursor)))
        },
        Shape::MultiLineString(lines) => {
            let mapped = lines.try_map_values(lines.axes(), |line| {
                Ok::<_, crate::error::Error>(LineSeq::from_proven(rebuild_seq_columns(
                    line, cursor,
                )))
            })?;
            Shape::MultiLineString(mapped)
        },
        Shape::Polygon(polygon) => Shape::Polygon(rebuild_polygon_columns(polygon, cursor)?),
        Shape::MultiPolygon(polygons) => {
            let mapped = polygons.try_map_values(polygons.axes(), |polygon| {
                rebuild_polygon_columns(polygon, cursor)
            })?;
            Shape::MultiPolygon(mapped)
        },
        Shape::GeometryCollection(geometries) => Shape::GeometryCollection(
            geometries.try_map_members(|geometry| rebuild_shape_columns(geometry, cursor))?,
        ),
        Shape::Empty(..) => shape.clone(),
    })
}

fn rebuild_polygon_columns(
    polygon: &crate::geometry::Polygon,
    cursor: &mut TransformCursor,
) -> Result<crate::geometry::Polygon> {
    // CRS rebuild only rewrites coordinates, preserving ring structure.
    polygon.map_ring_seqs(|ring| Ok(rebuild_seq_columns(ring, cursor)))
}

fn rebuild_seq_columns(seq: &CoordSeq, cursor: &mut TransformCursor) -> CoordSeq {
    let transformed = cursor.take();
    debug_assert_eq!(transformed.len(), seq.len());
    transformed
}

#[cfg(test)]
mod lane_tests {
    use super::*;
    use crate::error::ErrorKind;
    use crate::geometry::{CoordinateAxes, MultiPoint};

    #[test]
    fn output_counts_reject_one_vertex_past_each_packed_column_limit() {
        let maximum = usize::try_from(i32::MAX).unwrap();
        for batch in [OutputBatch::Xy, OutputBatch::Xyz] {
            let mut counts = OutputCounts::default();
            counts.add_run(batch, maximum).unwrap();

            let error = counts.add_run(batch, 1).unwrap_err();
            assert!(matches!(
                error.kind(),
                ErrorKind::Geometry(GeometryErrorKind::OffsetCapacityExceeded)
            ));
        }
    }

    #[test]
    fn output_counts_reject_wrapping_vertex_total_before_it_can_pass_the_limit() {
        let mut counts = OutputCounts {
            // Seed the invalid pre-wrap total directly instead of constructing
            // billions of coordinates to reach the same checked-add boundary.
            xy_vertices: usize::MAX.wrapping_sub(3),
            ..OutputCounts::default()
        };

        let error = counts
            .add_run(OutputBatch::Xy, usize::try_from(i32::MAX).unwrap())
            .unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::OffsetCapacityExceeded)
        ));
    }

    #[test]
    fn chunks_cross_owners_and_run_xy_before_xyz() {
        let shapes = [
            Shape::Point(Point::new_raw_axes(
                1.0,
                2.0,
                ZOrdinate(None),
                MOrdinate(None),
            )),
            Shape::Point(Point::new_raw_axes(
                3.0,
                4.0,
                ZOrdinate(None),
                MOrdinate(None),
            )),
            Shape::Point(Point::new_raw_axes(
                5.0,
                6.0,
                ZOrdinate(None),
                MOrdinate(None),
            )),
            Shape::Point(Point::new_raw_axes(
                7.0,
                8.0,
                ZOrdinate(Some(9.0)),
                MOrdinate(None),
            )),
            Shape::Point(Point::new_raw_axes(
                10.0,
                11.0,
                ZOrdinate(Some(12.0)),
                MOrdinate(None),
            )),
        ];
        let refs: Vec<_> = shapes.iter().collect();
        let mut plan = TransformOutputPlan::try_new(&refs).unwrap();
        let mut calls = Vec::new();
        plan.execute_with_capacities_indexed(2, 1, &mut |x, y, z| {
            calls.push((z.is_some(), x.len()));
            for value in x.iter_mut().chain(y.iter_mut()) {
                *value += 100.0;
            }
            if let Some(z) = z {
                for value in z {
                    *value += 100.0;
                }
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(calls, [(false, 2), (false, 1), (true, 1), (true, 1)]);
        assert_eq!(plan.xy.xs, [101.0, 103.0, 105.0]);
        assert_eq!(plan.xy.ys, [102.0, 104.0, 106.0]);
        assert_eq!(plan.xyz.xs, [107.0, 110.0]);
        assert_eq!(plan.xyz.ys, [108.0, 111.0]);
        assert_eq!(plan.xyz.zs.as_deref(), Some([109.0, 112.0].as_slice()));
        assert!(plan.xy.xs.capacity() >= 3);
        assert!(plan.xy.ys.capacity() >= 3);
        assert!(plan.xyz.xs.capacity() >= 2);
        assert!(plan.xyz.ys.capacity() >= 2);
        assert!(plan.xyz.zs.as_ref().is_some_and(|zs| zs.capacity() >= 2));
        let output = plan.finish(&refs).unwrap();
        let coordinates = output
            .iter()
            .map(|shape| match shape {
                Shape::Point(point) => (point.x, point.y, point.z()),
                Shape::MultiPoint(_)
                | Shape::LineString(_)
                | Shape::MultiLineString(_)
                | Shape::Polygon(_)
                | Shape::MultiPolygon(_)
                | Shape::GeometryCollection(_)
                | Shape::Empty(..) => unreachable!(),
            })
            .collect::<Vec<_>>();
        assert_eq!(coordinates, [
            (101.0, 102.0, None),
            (103.0, 104.0, None),
            (105.0, 106.0, None),
            (107.0, 108.0, Some(109.0)),
            (110.0, 111.0, Some(112.0)),
        ]);
    }

    #[test]
    fn xy_failure_only_executes_xyz_shapes_before_its_shape() {
        let shapes = [
            Shape::Point(Point::new_raw_xy(1.0, 2.0)),
            Shape::Point(Point::new_raw_axes(
                3.0,
                4.0,
                ZOrdinate(Some(5.0)),
                MOrdinate(None),
            )),
            Shape::Point(Point::new_raw_xy(6.0, 7.0)),
            Shape::Point(Point::new_raw_axes(
                8.0,
                9.0,
                ZOrdinate(Some(10.0)),
                MOrdinate(None),
            )),
        ];
        let refs: Vec<_> = shapes.iter().collect();
        let mut plan = TransformOutputPlan::try_new(&refs).unwrap();
        let mut calls = Vec::new();

        let failure = plan
            .execute_with_capacities_indexed(8, 8, &mut |x, _, z| {
                calls.push((z.is_some(), x.len()));
                if z.is_none() {
                    Err(TransformFailure::at(
                        GeometryErrorKind::projection("XY source admission failed"),
                        1,
                    ))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();

        assert_eq!(failure.coordinate, Some(2));
        assert_eq!(calls, [(false, 2), (true, 1)]);
    }

    #[test]
    fn indexed_execution_prefers_the_earliest_shape_across_axes() {
        let shapes = [
            Shape::Point(Point::new_raw_axes(
                0.0,
                0.0,
                ZOrdinate(Some(1.0)),
                MOrdinate(None),
            )),
            Shape::Point(Point::new_raw_xy(0.0, 0.0)),
        ];
        let refs: Vec<_> = shapes.iter().collect();
        let mut plan = TransformOutputPlan::try_new(&refs).unwrap();
        let failure = plan
            .execute_indexed(|_, _, z| {
                Err(TransformFailure::at(
                    GeometryErrorKind::projection(if z.is_some() {
                        "xyz failed"
                    } else {
                        "xy failed"
                    }),
                    0,
                ))
            })
            .unwrap_err();

        assert_eq!(failure.coordinate, Some(0));
        assert_eq!(failure.error.to_string(), "projection failed: xyz failed");
    }

    #[test]
    fn indexed_execution_rejects_a_backend_ordinal_outside_the_current_chunk() {
        let sequence =
            CoordSeq::try_from_vecs(vec![0.0, 1.0, 2.0], vec![0.0, 1.0, 2.0], None, None).unwrap();
        let shape = Shape::MultiPoint(MultiPoint::from_coordseq(sequence).unwrap());
        let refs = [&shape];
        let mut plan = TransformOutputPlan::try_new(&refs).unwrap();
        let mut calls = 0;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            plan.execute_with_capacities_indexed(2, 2, &mut |x, _, _| {
                calls += 1;
                Err(TransformFailure::at(
                    GeometryErrorKind::projection("invalid chunk ordinal"),
                    x.len(),
                ))
            })
        }));

        result.unwrap_err();
        assert_eq!(calls, 1);
    }

    #[test]
    fn mixed_sequences_share_final_lanes_and_reconstruct_exactly() {
        let xy = CoordSeq::try_from_vecs(vec![1.0, 2.0, 3.0], vec![10.0, 20.0, 30.0], None, None)
            .unwrap();
        let xyz = CoordSeq::try_from_vecs(
            vec![4.0, 5.0, 6.0],
            vec![40.0, 50.0, 60.0],
            Some(vec![400.0, 500.0, 600.0]),
            None,
        )
        .unwrap();
        let shapes = [
            Shape::MultiPoint(MultiPoint::from_coordseq(xy).unwrap()),
            Shape::MultiPoint(
                MultiPoint::from_coordseq(
                    CoordSeq::try_from_vecs(vec![7.0], vec![70.0], None, None).unwrap(),
                )
                .unwrap(),
            ),
            Shape::LineString(LineSeq::try_new(xyz).unwrap()),
            Shape::MultiPoint(
                MultiPoint::from_coordseq(
                    CoordSeq::try_from_vecs(vec![8.0], vec![80.0], Some(vec![800.0]), None)
                        .unwrap(),
                )
                .unwrap(),
            ),
        ];
        let refs: Vec<_> = shapes.iter().collect();
        let mut plan = TransformOutputPlan::try_new(&refs).unwrap();
        let mut calls = Vec::new();
        plan.execute_with_capacities_indexed(2, 2, &mut |x, y, z| {
            calls.push((z.is_some(), x.len()));
            for value in x {
                *value += 100.0;
            }
            for value in y {
                *value += 1000.0;
            }
            if let Some(z) = z {
                for value in z {
                    *value += 10_000.0;
                }
            }
            Ok(())
        })
        .unwrap();

        assert_eq!(calls, [(false, 2), (false, 2), (true, 2), (true, 2)]);
        assert_eq!(plan.sequences[0].batch, OutputBatch::Xy);
        assert_eq!(plan.sequences[0].range, 0..3);
        assert_eq!(plan.sequences[1].range, 3..4);
        assert_eq!(plan.sequences[2].batch, OutputBatch::Xyz);
        assert_eq!(plan.sequences[2].range, 0..3);
        assert_eq!(plan.sequences[3].range, 3..4);
        assert_eq!(plan.xy.xs, [101.0, 102.0, 103.0, 107.0]);
        assert_eq!(plan.xy.ys, [1010.0, 1020.0, 1030.0, 1070.0]);
        assert_eq!(plan.xyz.xs, [104.0, 105.0, 106.0, 108.0]);
        assert_eq!(plan.xyz.ys, [1040.0, 1050.0, 1060.0, 1080.0]);
        assert_eq!(
            plan.xyz.zs.as_deref(),
            Some([10_400.0, 10_500.0, 10_600.0, 10_800.0].as_slice())
        );
        assert_eq!(plan.xy.xs.len(), 4);
        assert_eq!(plan.xyz.xs.len(), 4);
        assert!(plan.xy.xs.capacity() >= plan.xy.xs.len());
        assert!(plan.xyz.xs.capacity() >= plan.xyz.xs.len());

        let output = plan.finish(&refs).unwrap();
        let Shape::MultiPoint(output_xy) = &output[0] else {
            unreachable!()
        };
        let Shape::MultiPoint(output_xy_point) = &output[1] else {
            unreachable!()
        };
        let Shape::LineString(output_xyz) = &output[2] else {
            unreachable!()
        };
        let Shape::MultiPoint(output_xyz_point) = &output[3] else {
            unreachable!()
        };
        assert_eq!(output_xy.xs(), [101.0, 102.0, 103.0]);
        assert_eq!(output_xy.ys(), [1010.0, 1020.0, 1030.0]);
        assert_eq!(output_xy_point.xs(), [107.0]);
        assert_eq!(output_xy_point.ys(), [1070.0]);
        assert_eq!(output_xyz.xs(), [104.0, 105.0, 106.0]);
        assert_eq!(output_xyz.ys(), [1040.0, 1050.0, 1060.0]);
        assert_eq!(
            output_xyz.zs(),
            Some([10_400.0, 10_500.0, 10_600.0].as_slice())
        );
        assert_eq!(output_xyz_point.xs(), [108.0]);
        assert_eq!(output_xyz_point.ys(), [1080.0]);
        assert_eq!(output_xyz_point.zs(), Some([10_800.0].as_slice()));
    }

    #[test]
    fn mixed_m_sequences_keep_local_windows_on_shared_coordinates() {
        let first_ms = vec![50.0, 60.0];
        let second_ms = vec![70.0];
        let shapes = [
            Shape::MultiPoint(
                MultiPoint::from_coordseq(
                    CoordSeq::try_from_vecs(vec![1.0, 2.0], vec![10.0, 20.0], None, Some(first_ms))
                        .unwrap(),
                )
                .unwrap(),
            ),
            Shape::MultiPoint(
                MultiPoint::from_coordseq(
                    CoordSeq::try_from_vecs(vec![3.0], vec![30.0], None, Some(second_ms)).unwrap(),
                )
                .unwrap(),
            ),
        ];
        let refs: Vec<_> = shapes.iter().collect();
        let mut plan = TransformOutputPlan::try_new(&refs).unwrap();
        plan.execute_with_capacities_indexed(2, 2, &mut |x, y, _| {
            for value in x.iter_mut().chain(y.iter_mut()) {
                *value += 1.0;
            }
            Ok(())
        })
        .unwrap();
        let output = plan.finish(&refs).unwrap();
        let Shape::MultiPoint(first) = &output[0] else {
            unreachable!()
        };
        let Shape::MultiPoint(second) = &output[1] else {
            unreachable!()
        };
        assert_eq!(first.ms(), Some([50.0, 60.0].as_slice()));
        assert_eq!(second.ms(), Some([70.0].as_slice()));
        assert_eq!(first.column_arcs().window, 0..2);
        assert_eq!(first.column_arcs().m_parts().unwrap().1, 0..2);
        assert_eq!(second.column_arcs().window, 2..3);
        assert_eq!(second.column_arcs().m_parts().unwrap().1, 0..1);
    }

    #[test]
    fn full_window_m_remains_shared() {
        let seq =
            CoordSeq::try_from_vecs(vec![1.0, 2.0], vec![3.0, 4.0], None, Some(vec![5.0, 6.0]))
                .unwrap();
        let shape = Shape::MultiPoint(crate::geometry::MultiPoint::from_coordseq(seq).unwrap());
        let refs = [&shape];
        let mut plan = TransformOutputPlan::try_new(&refs).unwrap();
        plan.execute_with_capacities_indexed(1, 1, &mut |_, _, _| Ok(()))
            .unwrap();
        let Shape::MultiPoint(output) = plan.finish(&refs).unwrap().pop().unwrap() else {
            unreachable!()
        };
        assert_eq!(output.axes(), CoordinateAxes::XYM);
    }

    #[test]
    fn windowed_m_copies_only_visible_values() {
        let seq = CoordSeq::try_from_vecs(
            vec![0.0, 1.0, 2.0, 3.0],
            vec![10.0, 11.0, 12.0, 13.0],
            None,
            Some(vec![4.0, 5.0, 6.0, 7.0]),
        )
        .unwrap()
        .view(crate::geometry::CoordWindow::from_range(1..3, 4));
        let shape = Shape::MultiPoint(crate::geometry::MultiPoint::from_coordseq(seq).unwrap());
        let refs = [&shape];
        let mut plan = TransformOutputPlan::try_new(&refs).unwrap();
        plan.execute_with_capacities_indexed(1, 1, &mut |_, _, _| Ok(()))
            .unwrap();
        assert_eq!(
            plan.sequences[0]
                .ms
                .as_ref()
                .map(|column| column.values(0..2)),
            Some(&[5.0, 6.0][..])
        );
    }
}
