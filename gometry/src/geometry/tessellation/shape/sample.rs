use super::*;

#[expect(
    clippy::float_cmp,
    reason = "raw interpolation requires equal deltas; signed zero remains compatible"
)]
fn raw_delta_matches_retained(raw_delta: f64, retained_delta: f64) -> bool {
    raw_delta == retained_delta
}

fn geographic_sample_segments(
    shape: &Shape,
    frame: crate::geographic::GeographicFrame,
) -> Result<Vec<SampleSupport>> {
    let mut segments = Vec::with_capacity(ShapeCountQuery::default().segments(shape)?);
    let mut component = 0_usize;
    shape.try_for_each_segment_chain(|chain| -> Result<()> {
        let mut points = chain.iter_coords();
        let Some(mut start) = points.next() else {
            return Ok(());
        };
        for (occurrence, end) in points.enumerate() {
            let connection = frame
                .longitude()
                .chart_connection(start.x, end.x)
                .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))?;
            let longitude = connection.retained();
            let raw_delta = end.x - start.x;
            let retained_delta = longitude.delta_native(frame.longitude());
            let raw_compatible = raw_delta_matches_retained(raw_delta, retained_delta);
            segments.push(SampleSupport::GeographicSegment {
                segment: GeographicSampleSegment {
                    occurrence: SampleOccurrenceId {
                        component,
                        occurrence,
                    },
                    start,
                    end,
                    longitude,
                    delta_x: if raw_compatible {
                        raw_delta
                    } else {
                        retained_delta
                    },
                    delta_y: end.y - start.y,
                    raw_compatible,
                },
            });
            start = end;
        }
        component = component
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        Ok(())
    })?;
    Ok(segments)
}

fn geographic_sample_requires_preparation(
    shape: &Shape,
    frame: crate::geographic::GeographicFrame,
) -> Result<bool> {
    let axis = frame.longitude();
    let mut raw_compatible = true;
    shape.try_for_each_segment_chain(|chain| -> Result<()> {
        let mut points = chain.iter_coords();
        let Some(mut start) = points.next() else {
            return Ok(());
        };
        for end in points {
            let connection = axis
                .chart_connection(start.x, end.x)
                .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))?;
            if connection.step.is_circle()
                || !raw_delta_matches_retained(
                    end.x - start.x,
                    connection.retained().delta_native(axis),
                )
            {
                raw_compatible = false;
                return Ok(());
            }
            start = end;
        }
        Ok(())
    })?;
    Ok(!raw_compatible || crate::geometry::crosses_antimeridian_in_frame(shape, frame))
}

fn sample_triangle_vertices(support: &SampleSupport) -> &[Point; 3] {
    let SampleSupport::AreaTriangle { vertices, .. } = support else {
        unreachable!("surface support contains only Earcut triangles")
    };
    vertices
}

fn prepare_geographic_sample_leaf(
    shape: &Shape,
    frame: crate::geographic::GeographicFrame,
) -> Result<Shape> {
    if geographic_sample_requires_preparation(shape, frame)? {
        shape.force_2d()?.split_antimeridian_in_frame(frame)
    } else {
        Ok(shape.clone())
    }
}

fn geographic_areal_sample_support(
    shape: &Shape,
    frame: crate::geographic::GeographicFrame,
) -> Result<Shape> {
    match shape {
        Shape::Polygon(_) => prepare_geographic_sample_leaf(shape, frame),
        Shape::MultiPolygon(polygons) => {
            let mut members = Vec::with_capacity(polygons.member_count());
            for member in polygons.members_with_values() {
                match member {
                    MultiMember::Empty => members.push(MultiMember::Empty),
                    MultiMember::Value(polygon) => {
                        let polygon = Shape::Polygon(polygon.clone());
                        match prepare_geographic_sample_leaf(&polygon, frame)?.force_2d()? {
                            Shape::Polygon(polygon) => members.push(MultiMember::Value(polygon)),
                            Shape::MultiPolygon(split) => {
                                members.extend(split.members_with_values().map(
                                    |member| match member {
                                        MultiMember::Empty => MultiMember::Empty,
                                        MultiMember::Value(polygon) => {
                                            MultiMember::Value(polygon.clone())
                                        },
                                    },
                                ));
                            },
                            Shape::Point(_)
                            | Shape::MultiPoint(_)
                            | Shape::LineString(_)
                            | Shape::MultiLineString(_)
                            | Shape::GeometryCollection(_)
                            | Shape::Empty(..) => {
                                unreachable!("splitting areal support preserves surface dimension")
                            },
                        }
                    },
                }
            }
            Ok(Shape::MultiPolygon(MultiPolygon::try_new(
                CoordinateAxes::XY,
                members,
            )?))
        },
        Shape::GeometryCollection(collection) => Ok(Shape::GeometryCollection(
            collection.try_map_members(|member| geographic_areal_sample_support(member, frame))?,
        )),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => Ok(shape.clone()),
    }
}

impl Shape {
    /// `count` coordinate-uniform random points on this XY support, deterministic
    /// from `seed` (a splitmix64 stream). The sample space is the
    /// geometry's highest-dimension support — uniform over area (areal
    /// input: triangulate, area-weighted pick, square-root barycentric
    /// warp), along length (lineal input: length-weighted segment pick), or
    /// across point atoms (equal weight) — with the centroid rule's
    /// degenerate fallback: zero-area input samples its linework, zero
    /// length its vertices. Output points are XY (new locations have no
    /// meaningful Z/M); CRS rides at the surface layer. Geographic entry
    /// points prepare retained support before calling this coordinate sampler.
    /// An empty geometry samples nothing.
    pub fn sample_points(&self, count: usize, seed: u64) -> Result<Vec<Point>> {
        ExpansionBudget::check("sample_points", "count", count)?;
        if count == 0 {
            return Ok(Vec::new());
        }
        self.sample_point_receipts_from_support(
            self,
            count,
            seed,
            0,
            TopologyDomain::Cartesian,
            None,
        )
        .map(PreparedSampling::into_points)
    }

    #[cfg(test)]
    pub(crate) fn sample_points_in_domain(
        &self,
        count: usize,
        seed: u64,
        domain: TopologyDomain,
    ) -> Result<Vec<Point>> {
        self.sample_point_receipts_in_domain(count, seed, 0, domain)
            .map(PreparedSampling::into_points)
    }

    pub(crate) fn sample_point_receipts_in_domain(
        &self,
        count: usize,
        seed: u64,
        logical_row: usize,
        domain: TopologyDomain,
    ) -> Result<PreparedSampling> {
        ExpansionBudget::check("sample_points", "count", count)?;
        if count == 0 {
            return Ok(PreparedSampling::empty(logical_row, seed, domain));
        }
        let TopologyDomain::Geographic(frame) = domain else {
            return self.sample_point_receipts_from_support(
                self,
                count,
                seed,
                logical_row,
                domain,
                None,
            );
        };
        if self.topological_dimension() < Dimension::Curve {
            return self.sample_point_receipts_from_support(
                self,
                count,
                seed,
                logical_row,
                domain,
                None,
            );
        }
        crate::geographic::ScalarGeographicView::try_new(frame, self)
            .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))?;
        if !geographic_sample_requires_preparation(self, frame)? {
            return self.sample_point_receipts_from_support(
                self,
                count,
                seed,
                logical_row,
                domain,
                None,
            );
        }
        let area_support = geographic_areal_sample_support(self, frame)?;
        let segments = geographic_sample_segments(self, frame)?;
        area_support.sample_point_receipts_from_support(
            self,
            count,
            seed,
            logical_row,
            domain,
            Some(segments),
        )
    }

    fn sample_point_receipts_from_support(
        &self,
        point_source: &Self,
        count: usize,
        seed: u64,
        logical_row: usize,
        domain: TopologyDomain,
        mut geographic_segments: Option<Vec<SampleSupport>>,
    ) -> Result<PreparedSampling> {
        if self.topological_dimension() >= Dimension::Surface {
            let mut supports = Vec::new();
            collect_sample_triangles(self, &mut supports)?;
            // Every triangle uses one shared *per-axis* frame. Relative areas
            // retain the common `sx*sy` factor, while reciprocal spans remain
            // nonzero instead of collapsing to the lower-dimensional fallback.
            let frame = supports.first().and_then(|support| {
                let origin = *sample_triangle_vertices(support)
                    .first()
                    .expect("an area triangle has a first vertex");
                let extent_x = axis_extent(
                    supports.iter().flat_map(|support| {
                        sample_triangle_vertices(support)
                            .iter()
                            .map(|point| point.x)
                    }),
                    origin.x,
                )?;
                let extent_y = axis_extent(
                    supports.iter().flat_map(|support| {
                        sample_triangle_vertices(support)
                            .iter()
                            .map(|point| point.y)
                    }),
                    origin.y,
                )?;
                AxisFrame::from_origin_extents(origin, extent_x, extent_y)
            });
            let placed = sample_weighted(
                count,
                seed,
                &supports,
                |support| {
                    let Some(frame) = frame else {
                        return 0.0;
                    };
                    let [a, b, c] = sample_triangle_vertices(support);
                    let a = frame.frame_point(*a);
                    let b = frame.frame_point(*b);
                    let c = frame.frame_point(*c);
                    ((b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x)).abs()
                },
                |support, state| {
                    let [base, mid, apex] = sample_triangle_vertices(support);
                    // P = (1 - s) A + s (1 - t) B + s t C with s = sqrt(u):
                    // uniform over the triangle.
                    let spread = uniform_f64(state).sqrt();
                    let blend = uniform_f64(state);
                    let x =
                        (1.0 - spread) * base.x + spread * ((1.0 - blend) * mid.x + blend * apex.x);
                    let y =
                        (1.0 - spread) * base.y + spread * ((1.0 - blend) * mid.y + blend * apex.y);
                    Ok((Point::new(x, y)?, SampleRecipe::Triangle { spread, blend }))
                },
            )?;
            if let Some(draws) = placed {
                return Ok(PreparedSampling::from_draws(
                    logical_row,
                    seed,
                    domain,
                    supports,
                    draws,
                ));
            }
        }
        if self.topological_dimension() >= Dimension::Curve {
            let placed = if let Some(supports) = geographic_segments.take() {
                let TopologyDomain::Geographic(frame) = domain else {
                    unreachable!("retained sample segments require a geographic domain")
                };
                let scale = weight_scale(supports.iter().fold(0.0_f64, |largest, support| {
                    let SampleSupport::GeographicSegment { segment } = support else {
                        unreachable!("geographic support contains retained segments")
                    };
                    [segment.start, segment.end]
                        .into_iter()
                        .flat_map(|point| [point.x.abs(), point.y.abs()])
                        .fold(largest, f64::max)
                }));
                sample_weighted(
                    count,
                    seed,
                    &supports,
                    |support| {
                        let SampleSupport::GeographicSegment { segment } = support else {
                            unreachable!("geographic support contains retained segments")
                        };
                        let dx = segment.delta_x * scale;
                        let dy = segment.delta_y * scale;
                        (dx * dx + dy * dy).sqrt()
                    },
                    |support, state| {
                        let SampleSupport::GeographicSegment { segment } = support else {
                            unreachable!("geographic support contains retained segments")
                        };
                        let along = uniform_f64(state);
                        Ok((segment.sample(along, frame)?, SampleRecipe::Segment {
                            along,
                        }))
                    },
                )?
                .map(|draws| (supports, draws))
            } else {
                let segment_count = ShapeCountQuery::default().segments(self)?;
                let mut segments: Vec<SampleSupport> = Vec::with_capacity(segment_count);
                let mut component = 0_usize;
                self.try_for_each_segment_chain(|chain| -> Result<()> {
                    for (occurrence, segment) in line_segments(chain).enumerate() {
                        segments.push(SampleSupport::Segment {
                            occurrence: SampleOccurrenceId {
                                component,
                                occurrence,
                            },
                            segment,
                        });
                    }
                    component = component
                        .checked_add(1)
                        .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    Ok(())
                })?;
                let scale = weight_scale(
                    segments
                        .iter()
                        .flat_map(|support| {
                            let SampleSupport::Segment { segment, .. } = support else {
                                unreachable!("Cartesian support contains line segments")
                            };
                            [
                                segment.start.x.abs(),
                                segment.start.y.abs(),
                                segment.end.x.abs(),
                                segment.end.y.abs(),
                            ]
                        })
                        .fold(0.0_f64, f64::max),
                );
                sample_weighted(
                    count,
                    seed,
                    &segments,
                    |support| {
                        let SampleSupport::Segment { segment, .. } = support else {
                            unreachable!("Cartesian support contains line segments")
                        };
                        let dx = (segment.end.x - segment.start.x) * scale;
                        let dy = (segment.end.y - segment.start.y) * scale;
                        (dx * dx + dy * dy).sqrt()
                    },
                    |support, state| {
                        let SampleSupport::Segment { segment, .. } = support else {
                            unreachable!("Cartesian support contains line segments")
                        };
                        let along = uniform_f64(state);
                        let x = (1.0 - along) * segment.start.x + along * segment.end.x;
                        let y = (1.0 - along) * segment.start.y + along * segment.end.y;
                        Ok((Point::new(x, y)?, SampleRecipe::Segment { along }))
                    },
                )?
                .map(|draws| (segments, draws))
            };
            if let Some((supports, draws)) = placed {
                return Ok(PreparedSampling::from_draws(
                    logical_row,
                    seed,
                    domain,
                    supports,
                    draws,
                ));
            }
        }
        let atom_count = ShapeCountQuery::default().coordinates(point_source)?;
        let mut supports = Vec::with_capacity(atom_count);
        collect_sample_atoms(point_source, &mut supports)?;
        let draws = sample_weighted(
            count,
            seed,
            &supports,
            |_| 1.0,
            |support, _| {
                let SampleSupport::PointAtom { point, .. } = support else {
                    unreachable!("atomic support contains points")
                };
                Ok((Point::new(point.x, point.y)?, SampleRecipe::Atom))
            },
        )?;
        Ok(draws.map_or_else(
            || PreparedSampling::empty(logical_row, seed, domain),
            |draws| PreparedSampling::from_draws(logical_row, seed, domain, supports, draws),
        ))
    }
}

#[cfg(test)]
#[path = "sample_tests.rs"]
mod geographic_sample_tests;
